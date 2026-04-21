from __future__ import annotations

import json
import time

import pytest

from gateway.cron_registry import ScheduledTask
from gateway.scheduler import CronScheduler


class FakePublisher:
    """Captures publish calls for assertions."""

    def __init__(self):
        self.calls = []

    def __call__(self, topic, data, attrs):
        self.calls.append({"topic": topic, "data": data, "attrs": dict(attrs)})


# ── register / validate ───────────────────────────────────────────────

def test_register_rejects_invalid_task():
    scheduler = CronScheduler(FakePublisher(), project_id="proj")
    with pytest.raises(ValueError):
        scheduler.register(ScheduledTask(name="x", topic="t"))  # neither set


def test_register_rejects_duplicate_names():
    scheduler = CronScheduler(FakePublisher(), project_id="proj")
    scheduler.register(ScheduledTask(name="x", topic="t", interval_seconds=5))
    with pytest.raises(ValueError, match="Duplicate"):
        scheduler.register(
            ScheduledTask(name="x", topic="u", interval_seconds=10)
        )


def test_register_all_accepts_mix_of_cron_and_interval():
    scheduler = CronScheduler(FakePublisher(), project_id="proj")
    scheduler.register_all(
        [
            ScheduledTask(name="a", topic="t1", interval_seconds=10),
            ScheduledTask(name="b", topic="t2", cron="* * * * *"),
        ]
    )
    assert [t.name for t in scheduler.tasks] == ["a", "b"]


# ── _fire publishes with the correct shape ────────────────────────────

def test_fire_publishes_topic_payload_and_attrs():
    publisher = FakePublisher()
    scheduler = CronScheduler(publisher, project_id="proj")
    task = ScheduledTask(
        name="daily",
        topic="earnings-call-analysis",
        interval_seconds=60,
        payload={"company": "TSMC", "fiscal_year": 2026},
    )
    scheduler.register(task)

    scheduler._fire(task)

    assert len(publisher.calls) == 1
    call = publisher.calls[0]
    assert call["topic"] == "earnings-call-analysis"
    assert json.loads(call["data"].decode("utf-8")) == {
        "company": "TSMC",
        "fiscal_year": 2026,
    }
    assert call["attrs"] == {"source": "scheduler", "task_name": "daily"}


def test_fire_swallows_publisher_errors():
    def boom(topic, data, attrs):
        raise RuntimeError("pubsub down")

    scheduler = CronScheduler(boom, project_id="proj")
    task = ScheduledTask(name="x", topic="t", interval_seconds=60)
    scheduler.register(task)

    # Must not raise — the daemon thread would otherwise die silently.
    scheduler._fire(task)


def test_fire_respects_enabled_flag_when_called_directly():
    publisher = FakePublisher()
    scheduler = CronScheduler(publisher, project_id="proj")
    task = ScheduledTask(
        name="x", topic="t", interval_seconds=60, enabled=False
    )
    # Skip register() (which doesn't forbid disabled tasks) and fire directly.
    scheduler._fire(task)
    assert publisher.calls == []


def test_fire_logs_but_does_not_raise_on_non_serialisable_payload():
    publisher = FakePublisher()
    scheduler = CronScheduler(publisher, project_id="proj")

    class NotJson:
        pass

    task = ScheduledTask(
        name="x",
        topic="t",
        interval_seconds=60,
        payload={"bad": NotJson()},
    )
    scheduler._fire(task)
    assert publisher.calls == []


# ── start() only spawns threads for enabled tasks ─────────────────────

def test_start_does_not_spawn_thread_for_disabled_task():
    publisher = FakePublisher()
    scheduler = CronScheduler(publisher, project_id="proj")
    scheduler.register(
        ScheduledTask(name="on", topic="t", interval_seconds=3600)
    )
    scheduler.register(
        ScheduledTask(
            name="off", topic="t", interval_seconds=3600, enabled=False
        )
    )

    try:
        scheduler.start()
        assert len(scheduler._threads) == 1
        assert scheduler._threads[0].name == "cron-on"
    finally:
        scheduler.stop(timeout=1.0)


def test_start_twice_raises():
    scheduler = CronScheduler(FakePublisher(), project_id="proj")
    scheduler.register(
        ScheduledTask(name="x", topic="t", interval_seconds=3600)
    )
    try:
        scheduler.start()
        with pytest.raises(RuntimeError):
            scheduler.start()
    finally:
        scheduler.stop(timeout=1.0)


# ── interval loop actually fires ──────────────────────────────────────

def test_interval_loop_fires_task_at_least_once():
    """Use a very short interval and wait briefly; the loop should fire."""
    publisher = FakePublisher()
    scheduler = CronScheduler(publisher, project_id="proj")
    task = ScheduledTask(
        name="fast", topic="t", interval_seconds=1, payload={"hello": "world"}
    )
    scheduler.register(task)
    scheduler.start()
    try:
        # Wait up to ~2.5s for the first fire (interval=1s).
        deadline = time.time() + 2.5
        while time.time() < deadline and not publisher.calls:
            time.sleep(0.1)
    finally:
        scheduler.stop(timeout=1.0)

    assert publisher.calls, "interval loop should have published at least once"
    assert publisher.calls[0]["topic"] == "t"
    assert publisher.calls[0]["attrs"]["task_name"] == "fast"


# ── E2E: register from registry, fire all, confirm payloads ───────────

def test_register_all_from_registry_and_fire_each():
    from gateway.cron_registry import CRON_REGISTRY

    publisher = FakePublisher()
    scheduler = CronScheduler(publisher, project_id="proj")
    scheduler.register_all(list(CRON_REGISTRY.values()))

    for task in scheduler.tasks:
        scheduler._fire(task)

    assert len(publisher.calls) == len(CRON_REGISTRY)
    topics = {c["topic"] for c in publisher.calls}
    assert "earnings-call-analysis" in topics
    assert "risk-assessment" in topics
