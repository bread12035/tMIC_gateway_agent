"""Tests for the kubectl-friendly manual trigger.

Confirms that `trigger_task` walks the same pipeline as Pub/Sub delivery by
reusing `Gateway.handle_message` with an internal fake message wrapper.
"""
from __future__ import annotations

import json
import os

import pytest

import tools  # noqa: F401 — ensures package import side-effects
from gateway.main import Gateway, GatewayConfig
from gateway.manual_trigger import trigger_task
from gateway.route_registry import DataSource, RouteConfig, register_route
from gateway.storage_backend import InMemoryStorageBackend


@pytest.fixture
def wired_gateway(tmp_path):
    storage = InMemoryStorageBackend()

    ws_prefix = "workspaces/earnings-agent"
    storage.seed("my-agent-workspaces", f"{ws_prefix}/SOUL.md", "soul")
    storage.seed("my-agent-workspaces", f"{ws_prefix}/AGENTS.md", "agents")
    storage.seed(
        "my-agent-workspaces",
        f"{ws_prefix}/skills/transcript-summary/SKILL.md",
        "summary",
    )
    storage.seed(
        "earnings-data",
        "transcripts/TSMC/2026/Q1.txt",
        "Revenue was USD 1,000. End.",
    )

    runner_calls = {"count": 0}

    def fake_runner(system_prompt, task_description, tool_list, config):
        runner_calls["count"] += 1
        return {
            "status": "completed",
            "output_files": [],
            "memory_updated": False,
            "summary": "manual ok",
            "error": None,
            "llm_calls": 0,
            "iteration_count": 0,
        }

    register_route(
        "earnings-summary-sub",
        RouteConfig(
            agent_id="earnings-agent",
            workspace="workspaces/earnings-agent",
            skills=["transcript_summary"],
            llm_model="claude-sonnet-4-20250514",
            task_template="Analyse {company} {fiscal_year} Q{fiscal_quarter}",
            data_sources=[
                DataSource(
                    name="transcript",
                    bucket="earnings-data",
                    path_template="transcripts/{company}/{fiscal_year}/Q{fiscal_quarter}.txt",
                )
            ],
            allowed_topics=["agent-results"],
        ),
    )

    config = GatewayConfig(
        workspace_bucket="my-agent-workspaces",
        output_bucket="my-agent-outputs",
        tasklog_bucket="my-agent-task-logs",
        tmp_dir=str(tmp_path / "workspaces"),
    )
    gateway = Gateway(
        config=config,
        storage=storage,
        publisher=lambda t, d, a: None,
        agent_runner=fake_runner,
    )
    os.environ["SKILLS_BASE_PATH"] = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "skills")
    )
    return gateway, storage, runner_calls


def test_trigger_task_runs_pipeline_without_pubsub(wired_gateway):
    gateway, storage, runner_calls = wired_gateway

    state = trigger_task(
        "earnings-summary-sub",
        {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1},
        gateway=gateway,
    )

    assert runner_calls["count"] == 1
    assert state["output"]["status"] == "completed"
    assert state["subscription_id"] == "earnings-summary-sub"

    tasklog_keys = [p for (b, p) in storage._store.keys() if b == "my-agent-task-logs"]
    assert tasklog_keys, "task state was not persisted"


def test_trigger_task_bubbles_runner_failure(wired_gateway):
    gateway, _storage, _ = wired_gateway

    def bad_runner(*args, **kwargs):
        raise RuntimeError("manual kaboom")

    gateway._agent_runner = bad_runner
    state = trigger_task(
        "earnings-summary-sub",
        {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1},
        gateway=gateway,
    )
    assert state["output"]["status"] == "failed"
    assert "manual kaboom" in state["output"]["error"]
