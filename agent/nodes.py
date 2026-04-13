"""Graph nodes: model / tools / finalize (SDD §10.3).

These are kept in plain Python so the test suite can exercise the
`finalize_node` logic without having LangChain installed. The real LangGraph
graph in `agent.graph` wires them together and delegates `tools_node` to
LangGraph's `ToolNode`.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

MAX_ITERATIONS_DEFAULT = int(os.environ.get("AGENT_MAX_ITERATIONS", "10"))


def finalize_node(state: Dict[str, Any], config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Produce the AgentOutcome for the end of the agent loop.

    This function does not call an LLM. It looks at the accumulated
    messages and tool call log plus the local `outputs/` directory to
    summarize what happened.
    """
    cfg = config or {}
    outputs_dir = cfg.get("outputs_dir")

    messages: List[Any] = state.get("messages", [])
    tool_call_log: List[Dict[str, Any]] = state.get("tool_call_log", [])

    # Count "LLM iterations" as messages that were produced by the model
    # (tool-call messages from the model count; tool results do not).
    iteration_count = 0
    for m in messages:
        if _is_ai_message(m):
            iteration_count += 1

    max_iterations = cfg.get("max_iterations", MAX_ITERATIONS_DEFAULT)
    if iteration_count >= max_iterations:
        status = "max_iterations"
    else:
        status = "completed"

    output_files: List[str] = []
    if outputs_dir and os.path.isdir(outputs_dir):
        for p in Path(outputs_dir).rglob("*"):
            if p.is_file():
                output_files.append(str(p.relative_to(outputs_dir)))

    memory_updated = any(
        entry.get("tool") == "write_data" and entry.get("success")
        for entry in tool_call_log
    )

    last_msg = messages[-1] if messages else None
    summary = _message_text(last_msg)

    outcome = {
        "status": status,
        "output_files": output_files,
        "memory_updated": memory_updated,
        "summary": summary,
        "error": None,
        "iteration_count": iteration_count,
    }

    return {"outcome": outcome}


def should_continue(state: Dict[str, Any], config: Dict[str, Any] | None = None) -> str:
    """Conditional edge from `model` — decide the next node.

    Returns `"tools"` if the most recent AI message has pending tool_calls
    and we're still below the iteration cap, `"finalize"` otherwise.
    """
    cfg = config or {}
    max_iterations = cfg.get("max_iterations", MAX_ITERATIONS_DEFAULT)

    messages: List[Any] = state.get("messages", [])
    if not messages:
        return "finalize"

    iteration_count = sum(1 for m in messages if _is_ai_message(m))
    if iteration_count >= max_iterations:
        return "finalize"

    last = messages[-1]
    tool_calls = _tool_calls(last)
    if tool_calls:
        return "tools"
    return "finalize"


# ── Message helpers that work on both real LC messages and dict fakes ─
def _is_ai_message(msg: Any) -> bool:
    if msg is None:
        return False
    cls = type(msg).__name__
    if cls == "AIMessage":
        return True
    if isinstance(msg, dict):
        return msg.get("role") == "assistant" or msg.get("type") == "ai"
    return False


def _tool_calls(msg: Any) -> List[Any]:
    if msg is None:
        return []
    if hasattr(msg, "tool_calls"):
        return list(getattr(msg, "tool_calls") or [])
    if isinstance(msg, dict):
        return list(msg.get("tool_calls") or [])
    return []


def _message_text(msg: Any) -> str:
    if msg is None:
        return ""
    if hasattr(msg, "content"):
        content = getattr(msg, "content")
        if isinstance(content, list):
            return " ".join(
                part.get("text", "") if isinstance(part, dict) else str(part)
                for part in content
            )
        return str(content or "")
    if isinstance(msg, dict):
        return str(msg.get("content", ""))
    return ""
