"""Tests for agent.nodes — the finalize node + should_continue edge.

These tests exercise the graph node logic WITHOUT importing LangGraph, so
the suite runs even when LLM dependencies aren't installed.
"""
from __future__ import annotations

import os
import tempfile

from agent.nodes import finalize_node, should_continue


class FakeAI:
    """Minimal stand-in for an AIMessage."""

    def __init__(self, content="", tool_calls=None):
        self.content = content
        self.tool_calls = tool_calls or []


# Register type name so _is_ai_message recognises it
FakeAI.__name__ = "AIMessage"


class FakeHuman:
    def __init__(self, content=""):
        self.content = content


def test_finalize_marks_completed_when_below_limit(tmp_path):
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "summary.json").write_text("{}")

    state = {
        "messages": [FakeHuman("task"), FakeAI("done")],
        "tool_call_log": [],
    }
    result = finalize_node(state, {"outputs_dir": str(outputs), "max_iterations": 10})
    outcome = result["outcome"]
    assert outcome["status"] == "completed"
    assert "summary.json" in outcome["output_files"]
    assert outcome["memory_updated"] is False
    assert outcome["summary"] == "done"


def test_finalize_detects_memory_updates():
    state = {
        "messages": [FakeAI("ok")],
        "tool_call_log": [
            {"tool": "write_data", "success": True},
        ],
    }
    outcome = finalize_node(state, {"max_iterations": 10})["outcome"]
    assert outcome["memory_updated"] is True


def test_finalize_reports_max_iterations():
    state = {
        "messages": [FakeAI(f"msg{i}", [{"name": "t"}]) for i in range(10)],
        "tool_call_log": [],
    }
    outcome = finalize_node(state, {"max_iterations": 10})["outcome"]
    assert outcome["status"] == "max_iterations"


def test_should_continue_routes_to_tools_when_tool_calls_present():
    ai = FakeAI("calling", [{"name": "read_data"}])
    assert should_continue({"messages": [ai]}, {"max_iterations": 10}) == "tools"


def test_should_continue_routes_to_finalize_when_no_tool_calls():
    ai = FakeAI("done")
    assert should_continue({"messages": [ai]}, {"max_iterations": 10}) == "finalize"


def test_should_continue_respects_iteration_cap():
    ai = FakeAI("calling", [{"name": "t"}])
    messages = [FakeAI("m", [{"name": "t"}]) for _ in range(10)] + [ai]
    assert should_continue({"messages": messages}, {"max_iterations": 10}) == "finalize"
