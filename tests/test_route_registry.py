from __future__ import annotations

import pytest

from gateway.route_registry import (
    DataSource,
    ROUTE_REGISTRY,
    RouteConfig,
    get_route,
    register_route,
)


def test_default_earnings_route_renders_task():
    route = get_route("earnings-summary-sub")
    task = route.render_task(
        {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1}
    )
    assert "TSMC" in task
    assert "2026" in task


def test_default_earnings_route_renders_data_sources():
    route = get_route("earnings-summary-sub")
    ds = route.render_data_sources(
        {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1}
    )
    assert ds[0]["bucket"] == "earnings-data"
    assert ds[0]["gcs_path"] == "transcripts/TSMC/2026/Q1.txt"


def test_missing_field_raises_value_error():
    route = get_route("earnings-summary-sub")
    with pytest.raises(ValueError):
        route.render_task({"company": "TSMC"})


def test_register_custom_route():
    register_route(
        "custom-sub",
        RouteConfig(
            agent_id="custom-agent",
            workspace="workspaces/custom",
            skills=[],
            llm_model="claude-sonnet-4-20250514",
            task_template="do {thing}",
        ),
    )
    try:
        route = get_route("custom-sub")
        assert route.render_task({"thing": "x"}) == "do x"
    finally:
        ROUTE_REGISTRY.pop("custom-sub", None)


def test_unknown_subscription_raises():
    with pytest.raises(KeyError):
        get_route("missing-sub")
