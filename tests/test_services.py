"""Tests for GatewayServices — permission checks, limits, audit log."""
from __future__ import annotations

import os
import tempfile

import pytest

from gateway.context import AgentContext
from gateway.services import (
    GatewayServices,
    OperationLimitExceeded,
    OperationLimits,
    PermissionDenied,
)
from gateway.storage_backend import InMemoryStorageBackend


def _ctx(tmp_path=None):
    return AgentContext(
        agent_id="earnings-agent",
        task_id="task-1",
        workspace_bucket="ws",
        output_bucket="out",
        local_workspace=str(tmp_path) if tmp_path else None,
        allowed_read_prefixes=["workspaces/earnings-agent", "shared/"],
        allowed_write_prefixes=[
            "workspaces/earnings-agent/MEMORY.md",
            "workspaces/earnings-agent/memory/",
        ],
        allowed_topics=["agent-results"],
    )


def _services(ctx, storage=None, publisher=None, limits=None):
    return GatewayServices(
        ctx, storage or InMemoryStorageBackend(), publisher=publisher, limits=limits
    )


def test_read_from_workspace_success():
    storage = InMemoryStorageBackend()
    storage.seed("ws", "workspaces/earnings-agent/MEMORY.md", "hello")
    svc = _services(_ctx(), storage)

    result = svc.read_data("workspaces/earnings-agent/MEMORY.md")
    assert result["success"]
    assert result["content"] == "hello"


def test_read_blocked_by_prefix_whitelist():
    storage = InMemoryStorageBackend()
    storage.seed("ws", "workspaces/risk-agent/SECRET.md", "nope")
    svc = _services(_ctx(), storage)

    result = svc.read_data("workspaces/risk-agent/SECRET.md")
    assert not result["success"]
    assert "not in allowed prefixes" in result["error"]


def test_read_from_local_input_overrides_gcs():
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "inputs"))
        with open(os.path.join(tmp, "inputs", "Q1.txt"), "w") as fh:
            fh.write("local content")
        ctx = _ctx(tmp_path=tmp)
        svc = _services(ctx)
        result = svc.read_data("inputs/Q1.txt")
        assert result["success"]
        assert result["content"] == "local content"
        assert result["source"] == "local"


def test_write_data_append_mode():
    storage = InMemoryStorageBackend()
    storage.seed("ws", "workspaces/earnings-agent/MEMORY.md", "line1")
    svc = _services(_ctx(), storage)

    result = svc.write_data("workspaces/earnings-agent/MEMORY.md", "line2", mode="append")
    assert result["success"]
    content = storage.read("ws", "workspaces/earnings-agent/MEMORY.md").decode()
    assert "line1" in content and "line2" in content


def test_write_data_prefix_denied():
    svc = _services(_ctx())
    result = svc.write_data("workspaces/risk-agent/MEMORY.md", "x")
    assert not result["success"]
    assert "not in allowed prefixes" in result["error"]


def test_write_output_auto_prefixes():
    storage = InMemoryStorageBackend()
    svc = _services(_ctx(), storage)
    result = svc.write_output("summary.json", '{"ok":1}')
    assert result["success"]
    # Path pattern: outputs/<agent>/<date>/<task>/summary.json
    keys = [p for (b, p) in storage._store if b == "out"]
    assert len(keys) == 1
    assert keys[0].startswith("outputs/earnings-agent/")
    assert keys[0].endswith("/task-1/summary.json")


def test_write_output_rejects_nested_filename():
    svc = _services(_ctx())
    result = svc.write_output("../evil.json", "x")
    assert not result["success"]


def test_publish_requires_topic_whitelist():
    published = []

    def pub(topic, data, attrs):
        published.append((topic, data, attrs))

    svc = _services(_ctx(), publisher=pub)
    ok = svc.publish_message("agent-results", {"k": "v"})
    assert ok["success"]
    assert published[0][0] == "agent-results"

    denied = svc.publish_message("forbidden-topic", {"k": "v"})
    assert not denied["success"]


def test_operation_limits_enforced():
    svc = _services(_ctx(), limits=OperationLimits(max_reads=1))
    # First read (missing but counts toward the limit) — ok
    svc.read_data("workspaces/earnings-agent/SOUL.md")
    # Second read should explode internally and return error dict
    second = svc.read_data("workspaces/earnings-agent/SOUL.md")
    assert not second["success"]
    assert "read operation limit" in second["error"]


def test_audit_log_records_success_and_failure():
    storage = InMemoryStorageBackend()
    storage.seed("ws", "workspaces/earnings-agent/SOUL.md", "soul")
    ctx = _ctx()
    svc = _services(ctx, storage)

    svc.read_data("workspaces/earnings-agent/SOUL.md")          # success
    svc.read_data("workspaces/risk-agent/OTHER.md")             # permission denied

    tools = [e["tool"] for e in ctx.audit_log]
    assert tools == ["read_data", "read_data"]
    assert ctx.audit_log[0]["success"] is True
    assert ctx.audit_log[1]["success"] is False
    assert "error" in ctx.audit_log[1]
