from __future__ import annotations

import pytest

from gateway.cron_registry import CRON_REGISTRY, ScheduledTask


def test_all_registry_entries_validate():
    # Every shipped entry must be well-formed at startup.
    for name, task in CRON_REGISTRY.items():
        assert name == task.name, (
            f"Registry key {name!r} does not match task.name {task.name!r}"
        )
        task.validate()


def test_interval_and_cron_are_mutually_exclusive():
    bad = ScheduledTask(
        name="both",
        topic="t",
        interval_seconds=60,
        cron="* * * * *",
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        bad.validate()


def test_must_set_one_of_interval_or_cron():
    bad = ScheduledTask(name="none", topic="t")
    with pytest.raises(ValueError, match="exactly one of"):
        bad.validate()


def test_interval_must_be_positive():
    bad = ScheduledTask(name="neg", topic="t", interval_seconds=0)
    with pytest.raises(ValueError, match="> 0"):
        bad.validate()


def test_topic_required():
    bad = ScheduledTask(name="n", topic="", interval_seconds=10)
    with pytest.raises(ValueError, match="topic"):
        bad.validate()


def test_name_required():
    bad = ScheduledTask(name="", topic="t", interval_seconds=10)
    with pytest.raises(ValueError, match="name"):
        bad.validate()


def test_default_registry_has_earnings_task():
    task = CRON_REGISTRY["daily-earnings-summary"]
    assert task.topic == "earnings-call-analysis"
    assert task.cron == "0 9 * * 1-5"
    assert task.payload["company"] == "TSMC"


def test_default_registry_has_risk_task():
    task = CRON_REGISTRY["risk-check-interval"]
    assert task.topic == "risk-assessment"
    assert task.interval_seconds == 1800
    assert task.payload["report_type"] == "daily_risk"


def test_enabled_default_true():
    task = ScheduledTask(name="x", topic="t", interval_seconds=5)
    assert task.enabled is True
