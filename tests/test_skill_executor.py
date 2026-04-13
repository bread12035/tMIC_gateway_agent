"""Tests for run_skill — whitelist, path traversal, env scrubbing, execution."""
from __future__ import annotations

import os
import tempfile

import pytest

import tools
from gateway.context import AgentContext
from gateway.services import GatewayServices
from gateway.storage_backend import InMemoryStorageBackend
from tools import skill_executor


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def initialised_tools(monkeypatch):
    monkeypatch.setenv("SKILLS_BASE_PATH", os.path.join(PROJECT_ROOT, "skills"))
    storage = InMemoryStorageBackend()
    tmp = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp, "inputs"))
    transcript = os.path.join(tmp, "inputs", "Q1.txt")
    with open(transcript, "w") as fh:
        fh.write("Revenue was USD 1,234. Profit was USD 500. End.")

    ctx = AgentContext(
        agent_id="earnings-agent",
        task_id="t1",
        workspace_bucket="ws",
        output_bucket="out",
        local_workspace=tmp,
        allowed_read_prefixes=["workspaces/earnings-agent"],
        allowed_write_prefixes=["workspaces/earnings-agent/memory/"],
        allowed_topics=[],
    )
    svc = GatewayServices(ctx, storage)
    tools.init_tools(svc, workspace_dir=tmp)
    yield svc, tmp
    import shutil

    shutil.rmtree(tmp, ignore_errors=True)


def test_run_skill_rejects_unregistered(initialised_tools):
    result = tools.run_skill("not_a_skill")
    assert not result["success"]
    assert "registry" in result["error"]


def test_run_skill_executes_transcript_summary(initialised_tools):
    svc, tmp = initialised_tools
    result = tools.run_skill(
        "transcript_summary",
        params={"input_path": "inputs/Q1.txt", "max_sentences": 2},
    )
    assert result["success"], result
    assert result["parsed"]["word_count"] > 0
    # The skill should have written summary.json into outputs/
    assert os.path.exists(os.path.join(tmp, "outputs", "summary.json"))


def test_run_skill_executes_financial_extraction(initialised_tools):
    svc, tmp = initialised_tools
    result = tools.run_skill(
        "financial_extraction", params={"input_path": "inputs/Q1.txt"}
    )
    assert result["success"], result
    assert result["parsed"]["metrics_found"] >= 1
    assert os.path.exists(os.path.join(tmp, "outputs", "key_metrics.csv"))


def test_run_skill_path_traversal_blocked(initialised_tools, monkeypatch):
    # Temporarily add a malicious entry pointing outside SKILLS_BASE_PATH
    monkeypatch.setitem(
        skill_executor.SKILL_REGISTRY,
        "evil",
        {"script": "../../../etc/passwd", "timeout": 5},
    )
    result = tools.run_skill("evil")
    assert not result["success"]
    assert ("escapes" in result["error"]) or ("not found" in result["error"])


def test_sensitive_env_vars_are_scrubbed(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-secret")
    monkeypatch.setenv("DATA_API_KEY", "d-secret")
    monkeypatch.setenv("UNRELATED", "safe")
    clean = skill_executor._sanitise_env()
    assert "ANTHROPIC_API_KEY" not in clean
    assert "OPENAI_API_KEY" not in clean
    assert "DATA_API_KEY" not in clean
    assert clean.get("UNRELATED") == "safe"
    assert clean["PATH"] == skill_executor.RESTRICTED_PATH
