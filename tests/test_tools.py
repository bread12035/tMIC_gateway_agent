"""Tests for the agent tool wrappers."""
from __future__ import annotations

import os
import tempfile

import pytest

import tools
from gateway.context import AgentContext
from gateway.services import GatewayServices
from gateway.storage_backend import InMemoryStorageBackend


@pytest.fixture
def setup_tools():
    storage = InMemoryStorageBackend()
    tmp = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp, "inputs"))
    with open(os.path.join(tmp, "inputs", "Q1.txt"), "w") as fh:
        fh.write("hello world")

    ctx = AgentContext(
        agent_id="earnings-agent",
        task_id="task-1",
        workspace_bucket="ws",
        output_bucket="out",
        local_workspace=tmp,
        allowed_read_prefixes=["workspaces/earnings-agent", "shared/"],
        allowed_write_prefixes=["workspaces/earnings-agent/memory/"],
        allowed_topics=["agent-results"],
    )
    svc = GatewayServices(ctx, storage)
    tools.init_tools(svc, workspace_dir=tmp)
    yield svc, tmp
    # Cleanup
    import shutil

    shutil.rmtree(tmp, ignore_errors=True)


def test_read_data_tool_reads_local_input(setup_tools):
    svc, _ = setup_tools
    result = tools.read_data("inputs/Q1.txt")
    assert result["success"]
    assert result["content"] == "hello world"


def test_write_data_tool_allows_memory_note(setup_tools):
    svc, _ = setup_tools
    result = tools.write_data(
        "workspaces/earnings-agent/memory/2026-04-11.md", "note", mode="overwrite"
    )
    assert result["success"]


def test_write_output_tool_lands_file(setup_tools):
    svc, _ = setup_tools
    result = tools.write_output("summary.json", '{"a":1}')
    assert result["success"]
    assert "summary.json" in result["gcs_path"]


def test_send_task_publishes_to_subagent_topic(setup_tools):
    svc, _ = setup_tools
    # Subagent topic is NOT in the allowed topics — permission should fail.
    result = tools.send_task("risk-agent", "evaluate")
    assert not result["success"]

    # Expand allowed topics and retry
    published = []

    def pub(topic, data, attrs):
        published.append(topic)

    svc.publisher = pub
    svc.ctx.allowed_topics.append("agent-tasks-risk-agent")
    result = tools.send_task("risk-agent", "evaluate")
    assert result["success"]
    assert published == ["agent-tasks-risk-agent"]


def test_tools_require_initialisation():
    # Clear module singletons
    tools._services = None
    tools._workspace_dir = None
    with pytest.raises(RuntimeError):
        tools.read_data("x")


def test_web_search_tool_has_expected_shape():
    spec = tools.WEB_SEARCH_TOOL
    assert spec["type"] == "web_search_20260209"
    assert spec["name"] == "web_search"
    assert spec["max_uses"] >= 1
    assert tools.is_server_tool(spec)


def test_build_web_search_tool_accepts_overrides():
    spec = tools.build_web_search_tool(
        max_uses=3,
        allowed_domains=["example.com"],
        user_location={"type": "approximate", "country": "TW"},
    )
    assert spec["max_uses"] == 3
    assert spec["allowed_domains"] == ["example.com"]
    assert spec["user_location"]["country"] == "TW"
    assert "blocked_domains" not in spec


def test_build_web_search_tool_rejects_conflicting_domain_lists():
    with pytest.raises(ValueError):
        tools.build_web_search_tool(
            allowed_domains=["a.com"], blocked_domains=["b.com"]
        )


def test_build_tools_includes_web_search():
    selected = tools.build_tools(enabled_skills=[])
    assert tools.WEB_SEARCH_TOOL in selected
    server_tools = [t for t in selected if tools.is_server_tool(t)]
    assert server_tools == [tools.WEB_SEARCH_TOOL]


def test_is_server_tool_rejects_non_server_entries():
    assert tools.is_server_tool({"type": "custom_tool"}) is False
    assert tools.is_server_tool(tools.read_data) is False
