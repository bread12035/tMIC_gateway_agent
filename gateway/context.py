"""AgentContext — per-task execution boundary (SDD §6.2).

The context is created by Gateway for every Pub/Sub message and passed to
GatewayServices, which enforces path / topic whitelists and per-session
operation limits.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class AgentContext:
    """Execution boundary for a single task/session."""

    agent_id: str
    task_id: str
    workspace_bucket: str
    output_bucket: str

    # Path whitelists are expressed as GCS-relative prefixes. They are compared
    # against the "resolved" path after the Services layer has prepended the
    # workspace/output prefix.
    allowed_read_prefixes: List[str] = field(default_factory=list)
    allowed_write_prefixes: List[str] = field(default_factory=list)
    allowed_topics: List[str] = field(default_factory=list)

    # Local workspace directory (created by Gateway, shared with skills)
    local_workspace: Optional[str] = None

    # Per-session operation counters (bumped by Services)
    operation_counts: Dict[str, int] = field(
        default_factory=lambda: {"read": 0, "write": 0, "publish": 0}
    )

    # Structured audit log for Task State (§5.5 — GatewayServices records
    # every operation and the Gateway integrates it into execution.tool_calls).
    audit_log: List[Dict] = field(default_factory=list)

    # ── Convenience helpers ────────────────────────────────────────────
    def record_audit(self, tool: str, args: Dict, success: bool,
                     duration_ms: int, error: Optional[str] = None) -> None:
        entry = {
            "tool": tool,
            "args": args,
            "success": success,
            "duration_ms": duration_ms,
        }
        if error:
            entry["error"] = error
        self.audit_log.append(entry)
