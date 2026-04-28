"""Route Registry — subscription → agent configuration mapping (SDD §3).

The registry lives in-tree and ships with the container image. Departments
publish messages containing only domain fields (company, fiscal_year, …);
the registry turns those into a full agent execution config, including the
task description template and the `data_sources` used for prefetching.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .external_sources import ExternalSourcesContext, render_format_dict


@dataclass
class DataSource:
    name: str
    bucket: str
    path_template: str  # rendered with the message fields
    description: str = ""
    # v0.4.0: optional deterministic transform applied before landing.
    processor: Optional[Callable[[bytes, Dict[str, Any]], bytes]] = None


# Builder receives the merged (env + domain) format dict and returns the
# fully rendered task description. Routes that need conditional logic
# (e.g. branching on WEB_SEARCH_FLAG) prefer this over `task_template`.
TaskBuilder = Callable[[Dict[str, Any]], str]


@dataclass
class RouteConfig:
    agent_id: str
    workspace: str
    skills: List[str]
    llm_model: str
    task_template: str = ""
    task_builder: Optional[TaskBuilder] = None
    data_sources: List[DataSource] = field(default_factory=list)
    max_iterations: Optional[int] = None
    allowed_topics: List[str] = field(default_factory=list)

    def render_task(self, fields: Dict[str, Any]) -> str:
        merged = render_format_dict(fields)
        if self.task_builder is not None:
            return self.task_builder(merged)
        try:
            return self.task_template.format(**merged)
        except KeyError as e:
            raise ValueError(
                f"Missing domain field {e} for agent {self.agent_id}"
            ) from None

    def render_data_sources(self, fields: Dict[str, Any]) -> List[Dict[str, str]]:
        rendered: List[Dict[str, str]] = []
        for ds in self.data_sources:
            try:
                path = ds.path_template.format(**fields)
            except KeyError as e:
                raise ValueError(
                    f"Missing domain field {e} for data_source {ds.name}"
                ) from None
            rendered.append(
                {
                    "name": ds.name,
                    "bucket": ds.bucket,
                    "gcs_path": path,
                    "description": ds.description,
                    "processor": ds.processor,
                }
            )
        return rendered


def _flatten_financial_json(raw: bytes, fields: Dict[str, Any]) -> bytes:
    """Flatten nested financial JSON into an agent-friendly shape.

    Uses `fields` (domain fields from the Pub/Sub message) so the processor
    can tag the output with company / fiscal context for downstream tools.
    """
    data = json.loads(raw)
    flat = {
        "company": fields.get("company"),
        "fiscal_year": fields.get("fiscal_year"),
        "fiscal_quarter": fields.get("fiscal_quarter"),
        "revenue": data["financials"]["income"]["revenue"],
        "eps": data["financials"]["income"]["eps"],
        "gross_margin": data["financials"]["ratios"]["gross_margin"],
    }
    return json.dumps(flat, ensure_ascii=False).encode("utf-8")


# ── Pre-earnings prompt builder ──────────────────────────────────────
def _build_pre_earnings_task(merged: Dict[str, Any]) -> str:
    """Compose the pre-earnings prompt.

    Primary query source is stocktitan (URL injected from configmap.yaml).
    Company IR URL and company topic from the ConfigMap remain as
    fallbacks. The company / fiscal-period fields are optional — when the
    inbound message omits them, the agent should still query stocktitan
    for the latest financial results / statements.
    """
    company = (merged.get("company") or "").strip()
    fiscal_year = merged.get("fiscal_year")
    fiscal_quarter = merged.get("fiscal_quarter")
    stocktitan_url = (merged.get("stocktitan_url") or "").strip()
    ir_url = (merged.get("company_ir_url") or "").strip()
    ir_topic = (merged.get("company_topic") or "").strip()

    target_lines: List[str] = ["## 分析目標"]
    if company:
        period_bits: List[str] = []
        if fiscal_year:
            period_bits.append(str(fiscal_year))
        if fiscal_quarter:
            period_bits.append(f"Q{fiscal_quarter}")
        period = " ".join(period_bits)
        if period:
            target_lines.append(f"- 公司：{company}（{period}）")
        else:
            target_lines.append(f"- 公司：{company}")
    else:
        target_lines.append(
            "- 公司：未指定 — 直接以 stocktitan 提供的最新財報資料為主。"
        )

    primary_lines: List[str] = ["## 主要查詢來源（必先嘗試）"]
    if stocktitan_url:
        primary_lines.append(
            f"1. **Stocktitan**（主要 URL，由 configmap.yaml 帶入）：{stocktitan_url}"
        )
    else:
        primary_lines.append(
            "1. **Stocktitan**（主要 URL）：未在 configmap.yaml 設定 STOCKTITAN_URL，"
            "請先確認 ConfigMap 後再執行。"
        )
    primary_lines.append(
        "   請使用 `web_search` 工具自上述網址擷取最新的 financial results / "
        "financial statements，包含 revenue、EPS、gross margin、operating "
        "margin、guidance 等關鍵指標。"
    )

    fallback_lines: List[str] = ["## Fallback（僅在主要來源查無資料時使用）"]
    has_fallback = False
    if ir_url:
        fallback_lines.append(f"- 公司 IR 頁面：{ir_url}")
        has_fallback = True
    if ir_topic:
        fallback_lines.append(f"- 設定的 IR 主題（topic）：{ir_topic}")
        has_fallback = True
    if not has_fallback:
        fallback_lines.append(
            "- configmap.yaml 未提供 COMPANY_IR_URL 或 COMPANY_TOPIC；"
            "若 stocktitan 取不到資料，請直接回報失敗並結束。"
        )

    instruction_lines = [
        "## 步驟",
        "1. 先用 stocktitan URL 查詢 financial results / statements。",
        "2. 若 stocktitan 無相關資料，再依序嘗試上方列出的 fallback 來源。",
        "3. 將取得的數據整理成 pre-earnings 摘要，存放於 `outputs/pre_earnings.md`，"
        "並在報告結尾標註資料來源（Stocktitan / IR / Topic）。",
    ]

    return "\n".join(
        [
            "你是 pre-earnings 分析助手，目的是在 earnings call 之前產出"
            "公司財務狀況快照。",
            "",
            *target_lines,
            "",
            *primary_lines,
            "",
            *fallback_lines,
            "",
            *instruction_lines,
        ]
    )


# ── ECTS (Earnings-Call Transcript Summary) prompt builder ──────────
_ECTS_MANUAL_TEMPLATE = (
    "<!-- BEGIN MANUAL ECTS TEMPLATE — please fill in -->\n"
    "[ECTS_TEMPLATE]\n"
    "TODO: 由開發者手動補上 ECTS 細節 prompt（例如重點章節、輸出欄位、"
    "summary 風格等）。本段於 CD 套用 configmap.yaml 後仍會保留，請於"
    "`gateway/route_registry.py::_build_ects_task` 內覆寫此區塊。\n"
    "[/ECTS_TEMPLATE]\n"
    "<!-- END MANUAL ECTS TEMPLATE -->"
)


def _build_ects_task(merged: Dict[str, Any]) -> str:
    """Compose the ECTS prompt.

    Behaviour is driven by ``WEB_SEARCH_FLAG`` from configmap.yaml:

    * ``true``  — instruct the agent to use stocktitan for financial
      statements / results and motley fool for the transcript, both URLs
      injected from the ConfigMap.
    * ``false`` — keep the legacy GCS-prefetched flow (`inputs/...`).

    A clearly-marked manual TEMPLATE block is preserved at the end so
    the operator can hand-tune the prompt without touching the surrounding
    scaffolding.
    """
    company = (merged.get("company") or "").strip()
    fiscal_year = merged.get("fiscal_year")
    fiscal_quarter = merged.get("fiscal_quarter")
    web_search_flag = ExternalSourcesContext.from_env().web_search_flag

    period_bits: List[str] = []
    if fiscal_year:
        period_bits.append(str(fiscal_year))
    if fiscal_quarter:
        period_bits.append(f"Q{fiscal_quarter}")
    period = " ".join(period_bits) or "(period unspecified)"
    company_label = company or "(company unspecified)"

    header = (
        f"請分析 {company_label} 的 {period} Earnings Call Transcript，"
        "產生摘要報告。"
    )

    if web_search_flag:
        stocktitan_url = (merged.get("stocktitan_url") or "").strip()
        motleyfool_url = (merged.get("motleyfool_url") or "").strip()

        body_lines = [
            "## 資料來源（WEB_SEARCH_FLAG=true）",
            "本次 ECTS 啟用網路查詢模式，請以下列來源為主，使用 `web_search` 工具擷取資料：",
            "",
            "1. **Financial statements / results — Stocktitan**",
            (
                f"   - URL（由 configmap.yaml 帶入）：{stocktitan_url}"
                if stocktitan_url
                else "   - URL：未在 configmap.yaml 設定 STOCKTITAN_URL，請先補上後再執行。"
            ),
            "",
            "2. **Earnings call transcript — Motley Fool**",
            (
                f"   - URL（由 configmap.yaml 帶入）：{motleyfool_url}"
                if motleyfool_url
                else "   - URL：未在 configmap.yaml 設定 MOTLEYFOOL_URL，請先補上後再執行。"
            ),
            "",
            "若兩處皆查無資料，再退回讀取 `inputs/` 之既有檔案。",
        ]
    else:
        body_lines = [
            "## 資料來源（WEB_SEARCH_FLAG=false）",
            "請優先讀取 Gateway 已預先下載到 `inputs/` 的檔案"
            "（transcript / financials），再進行摘要與重點抽取。",
        ]

    return "\n".join(
        [
            header,
            "",
            *body_lines,
            "",
            _ECTS_MANUAL_TEMPLATE,
        ]
    )


# Default registry — overridable at runtime via register_route().
ROUTE_REGISTRY: Dict[str, RouteConfig] = {
    "pre-earnings-sub": RouteConfig(
        agent_id="earnings-agent",
        workspace="workspaces/earnings-agent",
        skills=["financial_extraction"],
        llm_model="claude-sonnet-4-20250514",
        task_builder=_build_pre_earnings_task,
        # No GCS prefetch — the prompt directs the agent to fetch from
        # stocktitan via web_search. Company / fiscal fields are optional.
        data_sources=[],
        allowed_topics=["agent-results"],
    ),
    "earnings-summary-sub": RouteConfig(
        agent_id="earnings-agent",
        workspace="workspaces/earnings-agent",
        skills=["transcript_summary", "financial_extraction"],
        llm_model="claude-sonnet-4-20250514",
        task_builder=_build_ects_task,
        data_sources=[
            DataSource(
                name="transcript",
                bucket="earnings-data",
                path_template="transcripts/{company}/{fiscal_year}/Q{fiscal_quarter}.txt",
                description="Earnings call raw transcript",
            ),
            DataSource(
                name="financials",
                bucket="earnings-data",
                path_template="financials/{company}/{fiscal_year}/Q{fiscal_quarter}.json",
                description="Financial statements JSON",
                processor=_flatten_financial_json,
            ),
        ],
        allowed_topics=["agent-results"],
    ),
    "risk-eval-sub": RouteConfig(
        agent_id="risk-agent",
        workspace="workspaces/risk-agent",
        skills=["risk_scoring", "compliance_check"],
        llm_model="claude-sonnet-4-20250514",
        task_template=(
            "請對 {company} 的 {report_type} 執行風險評估。"
        ),
        data_sources=[],
        allowed_topics=["agent-results"],
    ),
}


def get_route(subscription_id: str) -> RouteConfig:
    try:
        return ROUTE_REGISTRY[subscription_id]
    except KeyError:
        raise KeyError(f"Unknown subscription {subscription_id!r}") from None


def register_route(subscription_id: str, config: RouteConfig) -> None:
    """Primarily used by tests and by dynamic-registry extensions."""
    ROUTE_REGISTRY[subscription_id] = config
