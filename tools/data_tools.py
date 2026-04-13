"""Data-access tools exposed to the agent (SDD §7.2).

These are thin wrappers over `GatewayServices` — no business logic lives
here. Permission checks, limits, and audit logging all happen in Services.
"""
from __future__ import annotations

from typing import Dict


def read_data(path: str) -> Dict:
    """Read a file from the agent's workspace or prefetched inputs.

    Args:
        path: Either `inputs/<name>` for prefetched files, or a path under
              the agent's own workspace (e.g. `MEMORY.md`,
              `memory/2026-04-11.md`).
    """
    from . import get_services

    return get_services().read_data(path)


def write_data(path: str, content: str, mode: str = "overwrite") -> Dict:
    """Write to the agent's workspace (memory or daily notes).

    Args:
        path: Path under the agent's workspace; whitelist-enforced.
        content: UTF-8 text content.
        mode: `overwrite` (default) or `append`.
    """
    from . import get_services

    return get_services().write_data(path, content, mode=mode)


def write_output(filename: str, content: str) -> Dict:
    """Write a result file to the output bucket.

    Args:
        filename: Flat filename — no slashes. Gateway auto-prefixes with
                  `outputs/{agent_id}/{date}/{task_id}/`.
    """
    from . import get_services

    return get_services().write_output(filename, content)
