"""Tests for invoke_sub_agent MD-file resolution logic (SDD §7.5).

We exercise `_resolve_sub_agent_workspace` directly rather than running a
full LangGraph, so the test is independent of LLM plumbing.
"""
from __future__ import annotations

import tools
from gateway.context import AgentContext
from gateway.services import GatewayServices
from gateway.storage_backend import InMemoryStorageBackend
from tools.sub_agent_tools import _resolve_sub_agent_workspace


def _setup(storage):
    # Parent workspace MD files
    storage.seed("ws", "workspaces/earnings-agent/SOUL.md", "earnings soul")
    storage.seed("ws", "workspaces/earnings-agent/AGENTS.md", "earnings agents")
    storage.seed(
        "ws", "workspaces/earnings-agent/skills/a/SKILL.md", "earnings skill"
    )
    # Sub agent workspace
    storage.seed("ws", "workspaces/risk-agent/SOUL.md", "risk soul")
    storage.seed("ws", "workspaces/risk-agent/AGENTS.md", "risk agents")
    storage.seed("ws", "workspaces/risk-agent/skills/b/SKILL.md", "risk skill")
    storage.seed("ws", "shared/policies.md", "global policies")


def _ctx_services(storage, agent_id, workspace_prefix):
    ctx = AgentContext(
        agent_id=agent_id,
        task_id=f"{agent_id}-t1",
        workspace_bucket="ws",
        output_bucket="out",
        allowed_read_prefixes=[workspace_prefix, "shared/"],
        allowed_write_prefixes=[f"{workspace_prefix}/memory/"],
        allowed_topics=[],
    )
    return GatewayServices(ctx, storage)


def test_share_workspace_copies_parent_md():
    storage = InMemoryStorageBackend()
    _setup(storage)
    parent = _ctx_services(storage, "earnings-agent", "workspaces/earnings-agent")
    sub = _ctx_services(storage, "risk-agent", "workspaces/risk-agent")

    ws = _resolve_sub_agent_workspace(
        parent_services=parent,
        sub_services=sub,
        sub_agent_id="risk-agent",
        share_workspace=True,
        extra_md_files=None,
    )
    # Inherited from parent
    assert ws.md_files["SOUL.md"] == "earnings soul"
    # Sub-agent's own skills are loaded regardless
    assert "b" in ws.skills


def test_isolated_workspace_loads_sub_agent_md():
    storage = InMemoryStorageBackend()
    _setup(storage)
    parent = _ctx_services(storage, "earnings-agent", "workspaces/earnings-agent")
    sub = _ctx_services(storage, "risk-agent", "workspaces/risk-agent")

    ws = _resolve_sub_agent_workspace(
        parent_services=parent,
        sub_services=sub,
        sub_agent_id="risk-agent",
        share_workspace=False,
        extra_md_files=None,
    )
    assert ws.md_files["SOUL.md"] == "risk soul"
    assert "b" in ws.skills


def test_extra_md_files_merged():
    storage = InMemoryStorageBackend()
    _setup(storage)
    parent = _ctx_services(storage, "earnings-agent", "workspaces/earnings-agent")
    sub = _ctx_services(storage, "risk-agent", "workspaces/risk-agent")

    ws = _resolve_sub_agent_workspace(
        parent_services=parent,
        sub_services=sub,
        sub_agent_id="risk-agent",
        share_workspace=False,
        extra_md_files=["shared/policies.md"],
    )
    assert "extra:policies.md" in ws.md_files
    assert ws.md_files["extra:policies.md"] == "global policies"
