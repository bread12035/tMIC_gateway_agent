"""CronScheduler engine (SDD §16.9).

Pure thread-based scheduler that fires :class:`ScheduledTask` s by
publishing a Pub/Sub message to the task's topic. Business logic lives in
the Gateway pipeline; this module only decides *when* to publish.

Design notes:

* Each enabled task gets its own daemon thread (``_interval_loop`` or
  ``_cron_loop``). Daemon threads let the process exit cleanly without
  explicit join on SIGTERM.
* A single :class:`threading.Event` acts as the global stop flag, so
  :meth:`stop` wakes every loop immediately instead of waiting for the
  next ``time.sleep`` to return.
* :meth:`_fire` never raises: publish failures are logged so one broken
  topic doesn't kill the thread and silently stop firing.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from typing import Iterable, List, Optional

from .cron_registry import ScheduledTask
from .services import Publisher

logger = logging.getLogger(__name__)


class CronScheduler:
    """Owns the daemon threads that drive scheduled Pub/Sub publishes."""

    def __init__(
        self,
        publisher: Publisher,
        project_id: str,
        *,
        clock: Optional[object] = None,
    ) -> None:
        self._publisher = publisher
        self._project_id = project_id
        # ``clock`` is only used by tests to freeze "now" for cron loops.
        self._clock = clock or time
        self._tasks: List[ScheduledTask] = []
        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self._started = False

    # ── public API ────────────────────────────────────────────────────
    def register(self, task: ScheduledTask) -> None:
        task.validate()
        if any(t.name == task.name for t in self._tasks):
            raise ValueError(f"Duplicate scheduled task name: {task.name!r}")
        self._tasks.append(task)

    def register_all(self, tasks: Iterable[ScheduledTask]) -> None:
        for task in tasks:
            self.register(task)

    @property
    def tasks(self) -> List[ScheduledTask]:
        return list(self._tasks)

    def start(self) -> None:
        if self._started:
            raise RuntimeError("CronScheduler.start() called twice")
        self._started = True

        for task in self._tasks:
            if not task.enabled:
                logger.info("CronScheduler: skip disabled task %r", task.name)
                continue

            if task.interval_seconds is not None:
                target = self._interval_loop
            else:
                target = self._cron_loop

            thread = threading.Thread(
                target=target,
                args=(task,),
                name=f"cron-{task.name}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)
            logger.info(
                "CronScheduler: started task %r (topic=%s, %s)",
                task.name,
                task.topic,
                f"every {task.interval_seconds}s"
                if task.interval_seconds is not None
                else f"cron={task.cron}",
            )

    def stop(self, timeout: Optional[float] = 5.0) -> None:
        self._stop_event.set()
        for thread in self._threads:
            thread.join(timeout=timeout)
        self._threads.clear()
        self._started = False

    # ── loops ─────────────────────────────────────────────────────────
    def _interval_loop(self, task: ScheduledTask) -> None:
        interval = float(task.interval_seconds or 0)
        while not self._stop_event.wait(interval):
            self._fire(task)

    def _cron_loop(self, task: ScheduledTask) -> None:
        # Import lazily so tests that only touch interval loops / _fire()
        # don't need croniter installed.
        from croniter import croniter

        base = datetime.now()
        iterator = croniter(task.cron, base)
        while not self._stop_event.is_set():
            next_fire = iterator.get_next(datetime)
            wait_seconds = max(0.0, (next_fire - datetime.now()).total_seconds())
            if self._stop_event.wait(wait_seconds):
                return
            self._fire(task)

    # ── fire ──────────────────────────────────────────────────────────
    def _fire(self, task: ScheduledTask) -> None:
        if not task.enabled:
            # Shouldn't happen (we don't start threads for disabled tasks),
            # but keep a guard so manual _fire() calls still honour it.
            return

        try:
            data = json.dumps(task.payload).encode("utf-8")
        except (TypeError, ValueError):
            logger.exception(
                "CronScheduler: cannot serialise payload for %r", task.name
            )
            return

        attributes = {
            "source": "scheduler",
            "task_name": task.name,
        }

        # The Publisher protocol (see gateway.services.Publisher) takes a
        # topic *name*; main._publish resolves it to a full path via
        # ``projects/{project_id}/topics/{topic}`` internally.
        try:
            self._publisher(task.topic, data, attributes)
            logger.info(
                "CronScheduler: fired task %r → projects/%s/topics/%s",
                task.name,
                self._project_id,
                task.topic,
            )
        except Exception:
            logger.exception(
                "CronScheduler: publish failed for task %r", task.name
            )
