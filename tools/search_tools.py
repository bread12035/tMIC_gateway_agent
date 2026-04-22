"""Web search tool — Anthropic native `web_search_20260209` server tool.

Unlike the rest of the tools in this package, `web_search` is a *server-side*
tool executed by the Claude API itself. Clients never see an invocation: the
model emits a tool use, the Anthropic backend runs the search, and the
results are returned inline as additional content blocks on the same
assistant turn.

Because the API handles execution, the descriptor exposed here is a plain
dict matching Anthropic's tool schema rather than a Python callable. The
agent graph binds it to the LLM alongside the client-side `@tool` functions,
but it is deliberately excluded from the LangGraph `ToolNode` — there is
nothing for the client runtime to execute.

Reference: the `web_search_20260209` version follows the 2026-02-09 update
to Anthropic's web search tool.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

WEB_SEARCH_TOOL_TYPE = "web_search_20260209"
WEB_SEARCH_TOOL_NAME = "web_search"


def build_web_search_tool(
    max_uses: int = 5,
    allowed_domains: Optional[List[str]] = None,
    blocked_domains: Optional[List[str]] = None,
    user_location: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return an Anthropic web_search server-tool descriptor.

    Args:
        max_uses: Cap on the number of searches the model can perform per turn.
        allowed_domains: If set, results are restricted to these hosts.
        blocked_domains: If set, results from these hosts are excluded.
            `allowed_domains` and `blocked_domains` are mutually exclusive.
        user_location: Optional approximate location hint passed to the API,
            e.g. ``{"type": "approximate", "country": "TW"}``.
    """
    if allowed_domains and blocked_domains:
        raise ValueError(
            "web_search: allowed_domains and blocked_domains are mutually exclusive"
        )

    spec: Dict[str, Any] = {
        "type": WEB_SEARCH_TOOL_TYPE,
        "name": WEB_SEARCH_TOOL_NAME,
        "max_uses": max_uses,
    }
    if allowed_domains:
        spec["allowed_domains"] = list(allowed_domains)
    if blocked_domains:
        spec["blocked_domains"] = list(blocked_domains)
    if user_location:
        spec["user_location"] = dict(user_location)
    return spec


def is_server_tool(tool: Any) -> bool:
    """Return True if `tool` is an Anthropic server-side tool descriptor.

    Server tools are represented as dicts with a versioned `type` such as
    `web_search_20260209`. They are executed by the Claude API and must be
    excluded from LangGraph's client-side `ToolNode`.
    """
    if not isinstance(tool, dict):
        return False
    tool_type = tool.get("type")
    return isinstance(tool_type, str) and tool_type.startswith("web_search_")


WEB_SEARCH_TOOL: Dict[str, Any] = build_web_search_tool()


__all__ = [
    "WEB_SEARCH_TOOL",
    "WEB_SEARCH_TOOL_NAME",
    "WEB_SEARCH_TOOL_TYPE",
    "build_web_search_tool",
    "is_server_tool",
]
