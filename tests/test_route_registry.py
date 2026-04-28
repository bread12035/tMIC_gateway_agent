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


def test_ects_route_tolerates_missing_period_fields():
    """ECTS now uses a programmatic builder that should not raise on
    missing period fields — the company / period bits are folded into a
    "(unspecified)" placeholder so the agent can still proceed."""
    route = get_route("earnings-summary-sub")
    task = route.render_task({"company": "TSMC"})
    assert "TSMC" in task
    assert "unspecified" in task


def test_ects_route_includes_manual_template_marker():
    route = get_route("earnings-summary-sub")
    task = route.render_task(
        {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1}
    )
    assert "[ECTS_TEMPLATE]" in task
    assert "[/ECTS_TEMPLATE]" in task


def test_ects_route_uses_web_search_branch_when_flag_true(monkeypatch):
    monkeypatch.setenv("WEB_SEARCH_FLAG", "true")
    monkeypatch.setenv("STOCKTITAN_URL", "https://example.com/stocktitan/{ticker}")
    monkeypatch.setenv("MOTLEYFOOL_URL", "https://example.com/motleyfool/")
    route = get_route("earnings-summary-sub")
    task = route.render_task(
        {
            "company": "TSMC",
            "ticker": "TSM",
            "fiscal_year": 2026,
            "fiscal_quarter": 1,
        }
    )
    assert "WEB_SEARCH_FLAG=true" in task
    assert "https://example.com/stocktitan/TSM" in task
    assert "https://example.com/motleyfool/" in task


def test_ects_route_uses_gcs_branch_when_flag_false(monkeypatch):
    monkeypatch.setenv("WEB_SEARCH_FLAG", "false")
    route = get_route("earnings-summary-sub")
    task = route.render_task(
        {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1}
    )
    assert "WEB_SEARCH_FLAG=false" in task
    assert "inputs/" in task


def test_pre_earnings_route_accepts_no_company(monkeypatch):
    monkeypatch.setenv("STOCKTITAN_URL", "https://example.com/st/{ticker}")
    monkeypatch.setenv("COMPANY_IR_URL", "")
    monkeypatch.setenv("COMPANY_TOPIC", "")
    route = get_route("pre-earnings-sub")
    task = route.render_task({})
    assert "stocktitan" in task.lower()
    assert "未指定" in task


def test_pre_earnings_route_uses_stocktitan_url_from_configmap(monkeypatch):
    monkeypatch.setenv("STOCKTITAN_URL", "https://stocktitan.example/{ticker}")
    route = get_route("pre-earnings-sub")
    task = route.render_task({"ticker": "TSM"})
    assert "https://stocktitan.example/TSM" in task


def test_pre_earnings_route_includes_ir_topic_fallback(monkeypatch):
    monkeypatch.setenv("STOCKTITAN_URL", "https://stocktitan.example/")
    monkeypatch.setenv("COMPANY_IR_URL", "https://ir.example.com/")
    monkeypatch.setenv("COMPANY_TOPIC", "earnings-2026-q1")
    route = get_route("pre-earnings-sub")
    task = route.render_task({"company": "TSMC"})
    assert "Fallback" in task
    assert "https://ir.example.com/" in task
    assert "earnings-2026-q1" in task


def test_pre_earnings_route_has_no_data_sources():
    route = get_route("pre-earnings-sub")
    assert route.data_sources == []
    assert route.render_data_sources({"company": "TSMC"}) == []


def test_risk_eval_route_still_uses_string_template():
    route = get_route("risk-eval-sub")
    task = route.render_task({"company": "TSMC", "report_type": "daily_risk"})
    assert "TSMC" in task
    assert "daily_risk" in task


def test_missing_field_for_string_template_raises_value_error():
    route = get_route("risk-eval-sub")
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
