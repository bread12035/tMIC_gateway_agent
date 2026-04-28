"""Runtime context for external data-source URLs (configmap.yaml).

The values in this module mirror the ``data:`` section of ``configmap.yaml``
which is mounted into each Pod as environment variables (``envFrom``). The
earnings prompt builders read them at task-render time so a Config-only
change can flip the agent between GCS-prefetched inputs and live web
queries without touching the image.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping


_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_TOKENS


def _safe_format(template: str, fields: Mapping[str, Any]) -> str:
    """``str.format`` that tolerates missing keys (renders them as empty).

    The pre-earnings prompt accepts requests where ``company`` /
    ``fiscal_year`` / ``fiscal_quarter`` are not specified — the primary
    query goes against stocktitan regardless — so a missing placeholder
    must not raise.
    """
    class _SafeDict(dict):
        def __missing__(self, key: str) -> str:  # type: ignore[override]
            return ""

    if not template:
        return ""
    return template.format_map(_SafeDict(dict(fields)))


@dataclass(frozen=True)
class ExternalSourcesContext:
    """Snapshot of configmap.yaml URLs / flags taken at render time."""

    stocktitan_url_template: str
    motleyfool_url_template: str
    web_search_flag: bool
    company_ir_url: str
    company_topic: str

    @classmethod
    def from_env(cls) -> "ExternalSourcesContext":
        return cls(
            stocktitan_url_template=os.environ.get("STOCKTITAN_URL", "").strip(),
            motleyfool_url_template=os.environ.get("MOTLEYFOOL_URL", "").strip(),
            web_search_flag=_bool_env("WEB_SEARCH_FLAG", default=False),
            company_ir_url=os.environ.get("COMPANY_IR_URL", "").strip(),
            company_topic=os.environ.get("COMPANY_TOPIC", "").strip(),
        )

    # ── render helpers ────────────────────────────────────────────────
    def stocktitan_url(self, fields: Mapping[str, Any]) -> str:
        return _safe_format(self.stocktitan_url_template, fields)

    def motleyfool_url(self, fields: Mapping[str, Any]) -> str:
        return _safe_format(self.motleyfool_url_template, fields)


def render_format_dict(fields: Mapping[str, Any]) -> Dict[str, Any]:
    """Build the merged template-rendering context.

    The runtime context wins only for keys the inbound message does not
    already provide, so a department can still override e.g. a default
    ticker by including it in the Pub/Sub payload.
    """
    ctx = ExternalSourcesContext.from_env()
    merged: Dict[str, Any] = {
        "stocktitan_url": ctx.stocktitan_url(fields),
        "motleyfool_url": ctx.motleyfool_url(fields),
        "company_ir_url": ctx.company_ir_url,
        "company_topic": ctx.company_topic,
    }
    merged.update(fields)
    return merged
