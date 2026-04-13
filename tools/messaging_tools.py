"""Messaging tool — fire-and-forget sub-agent dispatch via Pub/Sub (SDD §7.6)."""
from __future__ import annotations

from typing import Dict, Optional


def send_task(target_agent: str, task: str, input_data: Optional[Dict] = None) -> Dict:
    """Asynchronously dispatch a task to another agent.

    Use when the result is *not* required to continue. For synchronous
    sub-agent execution inside the same process, use `invoke_sub_agent`.
    """
    from . import get_services

    return get_services().dispatch_sub_agent(target_agent, task, input_data)
