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
