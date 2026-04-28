from __future__ import annotations

from gateway.external_sources import ExternalSourcesContext, render_format_dict


def test_from_env_defaults_when_unset(monkeypatch):
    for key in (
        "STOCKTITAN_URL",
        "MOTLEYFOOL_URL",
        "WEB_SEARCH_FLAG",
        "COMPANY_IR_URL",
        "COMPANY_TOPIC",
    ):
        monkeypatch.delenv(key, raising=False)

    ctx = ExternalSourcesContext.from_env()
    assert ctx.stocktitan_url_template == ""
    assert ctx.motleyfool_url_template == ""
    assert ctx.web_search_flag is False
    assert ctx.company_ir_url == ""
    assert ctx.company_topic == ""


def test_web_search_flag_truthy_tokens(monkeypatch):
    for token in ("true", "True", "1", "yes", "on"):
        monkeypatch.setenv("WEB_SEARCH_FLAG", token)
        assert ExternalSourcesContext.from_env().web_search_flag is True


def test_web_search_flag_falsy_tokens(monkeypatch):
    for token in ("false", "0", "no", "off", ""):
        monkeypatch.setenv("WEB_SEARCH_FLAG", token)
        assert ExternalSourcesContext.from_env().web_search_flag is False


def test_url_templates_substitute_fields(monkeypatch):
    monkeypatch.setenv(
        "STOCKTITAN_URL", "https://stocktitan.example/{ticker}/{fiscal_year}"
    )
    ctx = ExternalSourcesContext.from_env()
    assert ctx.stocktitan_url({"ticker": "TSM", "fiscal_year": 2026}) == (
        "https://stocktitan.example/TSM/2026"
    )


def test_url_templates_tolerate_missing_keys(monkeypatch):
    monkeypatch.setenv("STOCKTITAN_URL", "https://stocktitan.example/{ticker}")
    ctx = ExternalSourcesContext.from_env()
    # No ticker provided — placeholder renders empty rather than raising.
    assert ctx.stocktitan_url({}) == "https://stocktitan.example/"


def test_render_format_dict_message_overrides_runtime(monkeypatch):
    monkeypatch.setenv("COMPANY_IR_URL", "https://from-configmap/")
    merged = render_format_dict({"company_ir_url": "https://from-message/"})
    # Message-supplied value wins so departments can override per-task.
    assert merged["company_ir_url"] == "https://from-message/"


def test_render_format_dict_includes_runtime_keys(monkeypatch):
    monkeypatch.setenv("STOCKTITAN_URL", "https://stocktitan.example/{ticker}")
    monkeypatch.setenv("MOTLEYFOOL_URL", "https://fool.example/")
    monkeypatch.setenv("COMPANY_IR_URL", "https://ir.example/")
    monkeypatch.setenv("COMPANY_TOPIC", "earnings-2026-q1")

    merged = render_format_dict({"ticker": "TSM"})
    assert merged["stocktitan_url"] == "https://stocktitan.example/TSM"
    assert merged["motleyfool_url"] == "https://fool.example/"
    assert merged["company_ir_url"] == "https://ir.example/"
    assert merged["company_topic"] == "earnings-2026-q1"
    assert merged["ticker"] == "TSM"
