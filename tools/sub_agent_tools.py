"""invoke_sub_agent — synchronous sub-agent execution (SDD §7.4–§7.6).

Starts an independent LangGraph instance for the sub-agent inside the same
Python process. A fresh `AgentContext` / `GatewayServices` is created for
the sub-agent so its operation counts and audit log stay isolated from the
parent. MD files are resolved according to the `share_workspace` /
`extra_md_files` flags described in §7.5.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from gateway.context import AgentContext
from gateway.route_registry import get_route
from gateway.services import GatewayServices
from gateway.workspace_loader import LoadedWorkspace, load_workspace
from gateway.prompt_assembler import assemble_system_prompt

logger = logging.getLogger(__name__)


def invoke_sub_agent(
    sub_agent_id: str,
    task: str,
    input_data: Optional[Dict[str, Any]] = None,
    share_workspace: bool = True,
    extra_md_files: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run another agent synchronously inside this process.

    Args:
        sub_agent_id: The agent registry key (matches `route.agent_id`).
        task: Task description handed to the sub-agent.
        input_data: Extra structured data passed to the sub-agent as inputs.
        share_workspace: If True, inherit all MD files from the parent
            workspace; if False, load the sub-agent's own workspace.
        extra_md_files: Additional MD files to load (either shared/... or
            workspaces/<other>/...). Merged on top of the resolved set.
    """
    from . import get_services, get_workspace_dir

    parent_services = get_services()
    parent_ctx = parent_services.ctx
    parent_workspace_dir = get_workspace_dir()

    sub_route = _find_route_for_agent(sub_agent_id)
    sub_task_id = f"{parent_ctx.task_id}:sub:{sub_agent_id}"

    # Fresh context — independent counters + audit log
    sub_ctx = AgentContext(
        agent_id=sub_agent_id,
        task_id=sub_task_id,
        workspace_bucket=parent_ctx.workspace_bucket,
        output_bucket=parent_ctx.output_bucket,
        local_workspace=parent_workspace_dir,
        allowed_read_prefixes=[
            sub_route.workspace if sub_route else parent_ctx.allowed_read_prefixes[0],
            "shared/",
        ],
        allowed_write_prefixes=[
            f"{sub_route.workspace}/MEMORY.md" if sub_route else parent_ctx.allowed_write_prefixes[0],
            f"{sub_route.workspace}/memory/" if sub_route else parent_ctx.allowed_write_prefixes[0],
        ],
        allowed_topics=list(sub_route.allowed_topics) if sub_route else [],
    )
    sub_services = GatewayServices(
        sub_ctx,
        parent_services.storage,
        publisher=parent_services.publisher,
    )

    workspace = _resolve_sub_agent_workspace(
        parent_services=parent_services,
        sub_services=sub_services,
        sub_agent_id=sub_agent_id,
        share_workspace=share_workspace,
        extra_md_files=extra_md_files,
    )

    system_prompt = assemble_system_prompt(workspace, task, prefetched=[])
    if input_data:
        system_prompt += f"\n\n## [SUB-AGENT INPUT]\n{input_data}"

    from . import build_tools, init_tools  # avoid import cycle
    from agent.graph import run_agent  # type: ignore

    # Re-init module-level services to the sub-agent, then restore.
    init_tools(sub_services, workspace_dir=parent_workspace_dir)
    try:
        outcome = run_agent(
            system_prompt=system_prompt,
            task_description=task,
            tools=build_tools(sub_route.skills if sub_route else []),
            config={
                "llm_model": sub_route.llm_model if sub_route else "claude-sonnet-4-20250514",
                "max_iterations": 10,
                "outputs_dir": f"{parent_workspace_dir}/outputs",
            },
        )
    finally:
        # Restore parent bindings so subsequent tool calls are scoped to parent.
        init_tools(parent_services, workspace_dir=parent_workspace_dir)

    # Surface sub-agent audit entries to parent task state.
    parent_ctx.record_audit(
        "invoke_sub_agent",
        {"sub_agent_id": sub_agent_id},
        success=outcome.get("status") == "completed",
        duration_ms=0,
        error=outcome.get("error"),
    )
    return outcome


def _find_route_for_agent(agent_id: str):
    from gateway.route_registry import ROUTE_REGISTRY

    for route in ROUTE_REGISTRY.values():
        if route.agent_id == agent_id:
            return route
    return None


def _resolve_sub_agent_workspace(
    parent_services: GatewayServices,
    sub_services: GatewayServices,
    sub_agent_id: str,
    share_workspace: bool,
    extra_md_files: Optional[List[str]],
) -> LoadedWorkspace:
    """Implements §7.5 load strategy."""
    sub_route = _find_route_for_agent(sub_agent_id)
    sub_workspace_prefix = (
        sub_route.workspace if sub_route else f"workspaces/{sub_agent_id}"
    )

    if share_workspace:
        # Shallow copy of parent workspace
        parent_ws = load_workspace(
            parent_services,
            parent_services.ctx.allowed_read_prefixes[0]
            if parent_services.ctx.allowed_read_prefixes
            else sub_workspace_prefix,
            parent_services.ctx.agent_id,
        )
        workspace = LoadedWorkspace(
            agent_id=sub_agent_id,
            workspace_prefix=sub_workspace_prefix,
            md_files=dict(parent_ws.md_files),
            skills={},
        )
    else:
        workspace = load_workspace(sub_services, sub_workspace_prefix, sub_agent_id)

    # Always load the sub-agent's own skills
    sub_ws = load_workspace(sub_services, sub_workspace_prefix, sub_agent_id)
    workspace.skills.update(sub_ws.skills)

    # Extra MD files
    if extra_md_files:
        for path in extra_md_files:
            result = sub_services.read_data(path)
            if result.get("success"):
                key = f"extra:{path.rsplit('/', 1)[-1]}"
                workspace.md_files[key] = result["content"]
            else:
                logger.warning("extra_md_files load failed for %s: %s", path, result)

    return workspace
