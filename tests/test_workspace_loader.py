"""Tests for load_workspace filtering by RouteConfig.skills."""
from __future__ import annotations

from gateway.context import AgentContext
from gateway.services import GatewayServices
from gateway.storage_backend import InMemoryStorageBackend
from gateway.workspace_loader import load_workspace


def _services(storage, workspace_prefix):
    ctx = AgentContext(
        agent_id="earnings-agent",
        task_id="t1",
        workspace_bucket="ws",
        output_bucket="out",
        allowed_read_prefixes=[workspace_prefix, "shared/"],
        allowed_write_prefixes=[f"{workspace_prefix}/memory/"],
        allowed_topics=[],
    )
    return GatewayServices(ctx, storage)


def _seed(storage, prefix):
    storage.seed("ws", f"{prefix}/SOUL.md", "soul")
    storage.seed("ws", f"{prefix}/AGENTS.md", "agents")
    storage.seed("ws", f"{prefix}/skills/transcript-summary/SKILL.md", "summary")
    storage.seed("ws", f"{prefix}/skills/financial-extraction/SKILL.md", "extract")
    storage.seed("ws", f"{prefix}/skills/unused-skill/SKILL.md", "unused")


def test_load_workspace_loads_all_skills_when_no_filter():
    storage = InMemoryStorageBackend()
    prefix = "workspaces/earnings-agent"
    _seed(storage, prefix)

    ws = load_workspace(_services(storage, prefix), prefix, "earnings-agent")

    assert set(ws.skills) == {
        "transcript-summary",
        "financial-extraction",
        "unused-skill",
    }


def test_load_workspace_only_loads_registered_skills():
    storage = InMemoryStorageBackend()
    prefix = "workspaces/earnings-agent"
    _seed(storage, prefix)

    ws = load_workspace(
        _services(storage, prefix),
        prefix,
        "earnings-agent",
        enabled_skills=["transcript_summary", "financial_extraction"],
    )

    # Hyphen/underscore variants are treated as equivalent.
    assert set(ws.skills) == {"transcript-summary", "financial-extraction"}
    assert "unused-skill" not in ws.skills


def test_load_workspace_empty_enabled_loads_no_skills():
    storage = InMemoryStorageBackend()
    prefix = "workspaces/earnings-agent"
    _seed(storage, prefix)

    ws = load_workspace(
        _services(storage, prefix),
        prefix,
        "earnings-agent",
        enabled_skills=[],
    )
    assert ws.skills == {}
