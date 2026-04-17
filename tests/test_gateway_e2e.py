"""End-to-end test: Gateway.handle_message with a fake agent runner.

Exercises the entire task lifecycle — Pub/Sub message → route lookup →
prefetch → workspace load → prompt assembly → agent runner → output
collection → task state JSONL writeback — using in-memory fakes.
"""
from __future__ import annotations

import json
import os

import pytest

import tools  # noqa: F401 — ensures package import side-effects
from gateway.main import Gateway, GatewayConfig
from gateway.route_registry import RouteConfig, register_route, ROUTE_REGISTRY
from gateway.storage_backend import InMemoryStorageBackend


class FakeMessage:
    def __init__(self, data: bytes):
        self.data = data
        self.acked = False
        self.nacked = False

    def ack(self):
        self.acked = True

    def nack(self):
        self.nacked = True


@pytest.fixture
def wired_gateway(tmp_path):
    storage = InMemoryStorageBackend()

    # Seed the workspace bucket with the boot-sequence MD files
    ws_prefix = "workspaces/earnings-agent"
    storage.seed("my-agent-workspaces", f"{ws_prefix}/SOUL.md", "I am the earnings agent.")
    storage.seed("my-agent-workspaces", f"{ws_prefix}/AGENTS.md", "Follow all rules.")
    storage.seed(
        "my-agent-workspaces",
        f"{ws_prefix}/skills/transcript-summary/SKILL.md",
        "Summarise transcripts.",
    )

    # Seed the upstream data source
    storage.seed(
        "earnings-data",
        "transcripts/TSMC/2026/Q1.txt",
        "Revenue was USD 1,000. End.",
    )

    # Fake agent runner: read prefetched file via read_data, run a skill,
    # and write a result file to outputs/.
    runner_calls = {"count": 0}

    def fake_runner(system_prompt, task_description, tool_list, config):
        runner_calls["count"] += 1
        # Verify system prompt contains boot sections and input listing
        assert "[SOUL.md]" in system_prompt
        assert "[INPUTS]" in system_prompt
        assert "Q1.txt" in system_prompt

        # Use the wrappers directly — they're bound to the current services
        result = tools.read_data("inputs/Q1.txt")
        assert result["success"], result
        script_result = tools.run_safe_script(
            "transcript_summary", params={"input_path": "inputs/Q1.txt"}
        )
        assert script_result["success"], script_result
        tools.write_output("final.txt", f"len={len(result['content'])}")

        return {
            "status": "completed",
            "output_files": ["summary.json", "final.txt"],
            "memory_updated": False,
            "summary": "done",
            "error": None,
            "llm_calls": 2,
            "iteration_count": 2,
        }

    # Override the default registry with bucket names matching the config
    register_route(
        "earnings-summary-sub",
        RouteConfig(
            agent_id="earnings-agent",
            workspace="workspaces/earnings-agent",
            skills=["transcript_summary"],
            llm_model="claude-sonnet-4-20250514",
            task_template=(
                "Analyse {company} {fiscal_year} Q{fiscal_quarter} transcript"
            ),
            data_sources=[
                __import__("gateway.route_registry", fromlist=["DataSource"]).DataSource(
                    name="transcript",
                    bucket="earnings-data",
                    path_template="transcripts/{company}/{fiscal_year}/Q{fiscal_quarter}.txt",
                    description="Earnings call transcript",
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
    yield gateway, storage, runner_calls


def test_handle_message_full_lifecycle(wired_gateway):
    gateway, storage, runner_calls = wired_gateway

    msg = FakeMessage(
        json.dumps(
            {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1}
        ).encode("utf-8")
    )
    state = gateway.handle_message(msg, "earnings-summary-sub")

    # Agent was invoked exactly once
    assert runner_calls["count"] == 1
    # Message acked
    assert msg.acked

    # Task state recorded
    assert state["output"]["status"] == "completed"
    assert state["input"]["prefetched_files"][0]["name"] == "transcript"
    assert any("transcript_summary" in str(tc.get("args", {})) for tc in state["execution"]["tool_calls"])

    # Landed files were written to output bucket
    output_keys = [p for (b, p) in storage._store.items() if b == "my-agent-outputs"]
    # Keys come back as ((bucket, path), _) from items() — fix:
    output_keys = [p for (b, p) in storage._store.keys() if b == "my-agent-outputs"]
    assert any("final.txt" in p for p in output_keys)
    assert any("summary.json" in p for p in output_keys)

    # Task state JSONL persisted
    tasklog_keys = [p for (b, p) in storage._store.keys() if b == "my-agent-task-logs"]
    assert tasklog_keys, "task state JSONL was not written"
    line = storage.read("my-agent-task-logs", tasklog_keys[0]).decode().strip()
    parsed = json.loads(line.splitlines()[0])
    assert parsed["agent_id"] == "earnings-agent"
    assert parsed["subscription_id"] == "earnings-summary-sub"


def test_handle_message_records_failure(wired_gateway):
    gateway, storage, runner_calls = wired_gateway

    def failing_runner(*args, **kwargs):
        raise RuntimeError("llm kaboom")

    gateway._agent_runner = failing_runner

    msg = FakeMessage(
        json.dumps(
            {"company": "TSMC", "fiscal_year": 2026, "fiscal_quarter": 1}
        ).encode("utf-8")
    )
    state = gateway.handle_message(msg, "earnings-summary-sub")

    assert state["output"]["status"] == "failed"
    assert "llm kaboom" in state["output"]["error"]
    assert msg.acked  # still acked so we don't redeliver
