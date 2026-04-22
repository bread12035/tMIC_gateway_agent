"""LangGraph StateGraph for the Agent Runtime (SDD §10).

The full graph is constructed lazily — LangChain / LangGraph only get
imported when `run_agent()` is actually called. Tests can side-step this
entirely by passing a synthetic `AgentRunner` to `Gateway`.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from tools.search_tools import is_server_tool

from .nodes import finalize_node, should_continue

logger = logging.getLogger(__name__)


def _split_tools(tools: List[Any]) -> Tuple[List[Any], List[Any]]:
    """Partition the tool list into (client-side callables, server-side specs).

    Server tools (e.g. Anthropic's ``web_search_20260209``) are executed by
    the Claude API and must be declared to the LLM but excluded from
    LangGraph's ``ToolNode``, which only dispatches client-side callables.
    """
    client_tools: List[Any] = []
    server_tools: List[Any] = []
    for t in tools:
        if is_server_tool(t):
            server_tools.append(t)
        else:
            client_tools.append(t)
    return client_tools, server_tools


def _build_graph(tools: List[Any], llm_model: str, max_iterations: int):
    """Construct the LangGraph StateGraph described in SDD §10.1."""
    from langchain_anthropic import ChatAnthropic  # type: ignore
    from langchain_core.messages import AIMessage  # type: ignore  # noqa: F401
    from langgraph.checkpoint.memory import MemorySaver  # type: ignore
    from langgraph.graph import END, StateGraph  # type: ignore
    from langgraph.graph.message import MessagesState  # type: ignore
    from langgraph.prebuilt import ToolNode  # type: ignore

    class AgentState(MessagesState):  # type: ignore[misc]
        outcome: Dict[str, Any] | None
        tool_call_log: List[Dict[str, Any]]

    client_tools, server_tools = _split_tools(tools)

    # Bind every tool (client + server) to the LLM so Claude can invoke any
    # of them; server tools are passed through as raw dict specs.
    llm = ChatAnthropic(model=llm_model).bind_tools([*client_tools, *server_tools])

    def model_node(state):  # AgentState
        response = llm.invoke(state["messages"])
        return {"messages": [response]}

    # ToolNode only dispatches client-side callables — server tools are
    # executed by the Claude API and come back as inline content blocks.
    tool_node = ToolNode(client_tools)

    def _should_continue(state):
        return should_continue(state, {"max_iterations": max_iterations})

    def _finalize(state):
        return finalize_node(state, {"max_iterations": max_iterations})

    graph = StateGraph(AgentState)
    graph.add_node("model", model_node)
    graph.add_node("tools", tool_node)
    graph.add_node("finalize", _finalize)

    graph.set_entry_point("model")
    graph.add_conditional_edges(
        "model",
        _should_continue,
        {"tools": "tools", "finalize": "finalize"},
    )
    graph.add_edge("tools", "model")
    graph.add_edge("finalize", END)

    return graph.compile(checkpointer=MemorySaver())


def run_agent(
    system_prompt: str,
    task_description: str,
    tools: List[Any],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Entry point used by Gateway._run_agent.

    Returns an AgentOutcome dict. Any exception propagates so Gateway can
    record the task as failed.
    """
    llm_model = config.get("llm_model", "claude-sonnet-4-20250514")
    max_iterations = int(config.get("max_iterations", 10))

    try:
        agent = _build_graph(tools, llm_model, max_iterations)
    except ModuleNotFoundError as e:  # pragma: no cover — dev convenience
        logger.error("LangGraph/LangChain not installed: %s", e)
        return {
            "status": "error",
            "output_files": [],
            "memory_updated": False,
            "summary": "",
            "error": str(e),
        }

    from langchain_core.messages import HumanMessage, SystemMessage  # type: ignore

    initial_state = {
        "messages": [
            SystemMessage(content=system_prompt),
            HumanMessage(content=task_description),
        ],
        "outcome": None,
        "tool_call_log": [],
    }
    invoke_config = {"configurable": {"thread_id": "single"}}
    result = agent.invoke(initial_state, config=invoke_config)
    return result.get("outcome") or {
        "status": "error",
        "output_files": [],
        "memory_updated": False,
        "summary": "",
        "error": "finalize node did not produce outcome",
    }
