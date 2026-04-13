"""Agent tools package (SDD §7).

This package exposes three categories of tools:

1. Thin wrappers over GatewayServices (`data_tools`).
2. Messaging / orchestration (`messaging_tools`, `sub_agent_tools`).
3. Subprocess skill execution (`skill_executor`).

Tools are regular Python callables so the same code can be used both from
the LangGraph runtime (via `@tool` decorators) and from the unit tests.

At agent boot, Gateway calls `init_tools(services, workspace_dir)` which
installs a module-level reference so the individual tool functions don't
each need the context passed explicitly.
"""
from __future__ import annotations

from typing import List, Optional

from gateway.services import GatewayServices  # noqa: F401 (re-export convenience)

# Global singletons — deliberately mutable so tests can reset them.
_services: Optional[GatewayServices] = None
_workspace_dir: Optional[str] = None


def init_tools(services: GatewayServices, workspace_dir: str) -> None:
    """Install the per-task services + workspace root (SDD §7.3)."""
    global _services, _workspace_dir
    _services = services
    _workspace_dir = workspace_dir


def get_services() -> GatewayServices:
    if _services is None:
        raise RuntimeError("Tools not initialised — call init_tools() first.")
    return _services


def get_workspace_dir() -> str:
    if _workspace_dir is None:
        raise RuntimeError("Tools not initialised — call init_tools() first.")
    return _workspace_dir


from .data_tools import read_data, write_data, write_output  # noqa: E402
from .messaging_tools import send_task  # noqa: E402
from .skill_executor import SKILL_REGISTRY, run_skill  # noqa: E402
from .sub_agent_tools import invoke_sub_agent  # noqa: E402


ALL_TOOLS = [
    read_data,
    write_data,
    write_output,
    run_skill,
    send_task,
    invoke_sub_agent,
]


def build_tools(enabled_skills: List[str]) -> List:
    """Return the list of tools the agent should be given for this task.

    `enabled_skills` is informational only — `run_skill` itself enforces the
    whitelist via `SKILL_REGISTRY`. We still pass the list so the agent's
    system prompt / TOOLS.md can present the right subset.
    """
    return list(ALL_TOOLS)


__all__ = [
    "init_tools",
    "get_services",
    "get_workspace_dir",
    "read_data",
    "write_data",
    "write_output",
    "run_skill",
    "send_task",
    "invoke_sub_agent",
    "SKILL_REGISTRY",
    "ALL_TOOLS",
    "build_tools",
]
