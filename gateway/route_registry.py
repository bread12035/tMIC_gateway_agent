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


@dataclass
class DataSource:
    name: str
    bucket: str
    path_template: str  # rendered with the message fields
    description: str = ""
    # v0.4.0: optional deterministic transform applied before landing.
    processor: Optional[Callable[[bytes, Dict[str, Any]], bytes]] = None


@dataclass
class RouteConfig:
    agent_id: str
    workspace: str
    skills: List[str]
    llm_model: str
    task_template: str
    data_sources: List[DataSource] = field(default_factory=list)
    max_iterations: Optional[int] = None
    allowed_topics: List[str] = field(default_factory=list)

    def render_task(self, fields: Dict[str, Any]) -> str:
        try:
            return self.task_template.format(**fields)
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


# Default registry — overridable at runtime via register_route().
ROUTE_REGISTRY: Dict[str, RouteConfig] = {
    "earnings-summary-sub": RouteConfig(
        agent_id="earnings-agent",
        workspace="workspaces/earnings-agent",
        skills=["transcript_summary", "financial_extraction"],
        llm_model="claude-sonnet-4-20250514",
        task_template=(
            "請分析 {company} 的 {fiscal_year} Q{fiscal_quarter} "
            "Earnings Call Transcript，產生摘要報告。"
        ),
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
