"""CRON_REGISTRY — scheduled task definitions (SDD §16).

Parallel to :mod:`gateway.route_registry`, this module ships in-code with
the container image. CRON_REGISTRY maps a task ``name`` to a
:class:`ScheduledTask`, which tells :class:`gateway.scheduler.CronScheduler`
*when* to fire and *what* to publish. When the scheduler fires a task it
simply publishes the task's ``payload`` to the configured Pub/Sub ``topic``;
the Gateway pull loop then receives it and walks the same pipeline as a
department-triggered message.

Updates go through CI/CD: edit the registry, ship a new image, and Pod
rolling-restart picks up the change. No hot-reload mechanism is provided
on purpose — this mirrors how ROUTE_REGISTRY is managed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ScheduledTask:
    """A single scheduled task definition.

    Exactly one of ``interval_seconds`` or ``cron`` must be set.
    """

    name: str
    topic: str                                   # Pub/Sub topic name
    payload: Dict[str, Any] = field(default_factory=dict)
    interval_seconds: Optional[int] = None       # fixed interval, picks one
    cron: Optional[str] = None                   # cron expression, picks one
    enabled: bool = True
    description: str = ""

    def validate(self) -> None:
        """Raise ``ValueError`` if the task is not well-formed.

        Called by :meth:`CronScheduler.register` so Pod startup fails fast
        on a broken registry entry.
        """
        if not self.name:
            raise ValueError("ScheduledTask.name must be non-empty")
        if not self.topic:
            raise ValueError(
                f"ScheduledTask {self.name!r}: topic must be non-empty"
            )

        has_interval = self.interval_seconds is not None
        has_cron = self.cron is not None and self.cron.strip() != ""
        if has_interval and has_cron:
            raise ValueError(
                f"ScheduledTask {self.name!r}: "
                "interval_seconds and cron are mutually exclusive"
            )
        if not has_interval and not has_cron:
            raise ValueError(
                f"ScheduledTask {self.name!r}: "
                "must set exactly one of interval_seconds or cron"
            )
        if has_interval and self.interval_seconds <= 0:
            raise ValueError(
                f"ScheduledTask {self.name!r}: "
                f"interval_seconds must be > 0 (got {self.interval_seconds})"
            )


CRON_REGISTRY: Dict[str, ScheduledTask] = {
    "daily-earnings-summary": ScheduledTask(
        name="daily-earnings-summary",
        description="Weekday 09:00 — generate TSMC earnings-call summary",
        topic="earnings-call-analysis",
        cron="0 9 * * 1-5",
        payload={
            "company": "TSMC",
            "fiscal_year": 2026,
            "fiscal_quarter": 1,
        },
    ),
    "risk-check-interval": ScheduledTask(
        name="risk-check-interval",
        description="Every 30 minutes — run risk assessment",
        topic="risk-assessment",
        interval_seconds=1800,
        payload={
            "company": "TSMC",
            "report_type": "daily_risk",
        },
    ),
}
