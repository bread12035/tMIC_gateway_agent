"""Workspace loader — fetches the MD file bundle that becomes the system
prompt (SDD §4.4 "Boot Sequence" + §9 "Workspace structure").

The workspace lives at `gs://{workspace_bucket}/{route.workspace}/`. We load
the required and optional MD files in a fixed order and also enumerate any
`skills/*/SKILL.md` files so the agent knows which skills it can invoke.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .services import GatewayServices

logger = logging.getLogger(__name__)


# Boot-sequence file list: (filename, required)
BOOT_FILES: List[tuple[str, bool]] = [
    ("SOUL.md", True),
    ("AGENTS.md", True),
    ("IDENTITY.md", False),
    ("USER.md", False),
    ("TOOLS.md", False),
    ("MEMORY.md", False),
]


@dataclass
class LoadedWorkspace:
    agent_id: str
    workspace_prefix: str
    md_files: Dict[str, str] = field(default_factory=dict)  # name → content
    skills: Dict[str, str] = field(default_factory=dict)    # skill_name → SKILL.md

    def missing_required(self) -> List[str]:
        return [name for name, required in BOOT_FILES
                if required and name not in self.md_files]


def load_workspace(
    services: GatewayServices,
    workspace_prefix: str,
    agent_id: str,
) -> LoadedWorkspace:
    """Load all boot-sequence MD files plus skill descriptors."""
    ws = LoadedWorkspace(agent_id=agent_id, workspace_prefix=workspace_prefix)

    for name, required in BOOT_FILES:
        path = f"{workspace_prefix}/{name}"
        content = services.read_workspace_file(path)
        if content is not None:
            ws.md_files[name] = content
        elif required:
            logger.warning(
                "Required workspace file missing: %s (agent=%s)", path, agent_id
            )

    # Enumerate skills/<skill>/SKILL.md under this workspace
    skill_prefix = f"{workspace_prefix}/skills/"
    for path in services.list_workspace(skill_prefix):
        if path.endswith("/SKILL.md"):
            # workspaces/earnings-agent/skills/transcript-summary/SKILL.md
            relative = path[len(skill_prefix):]            # transcript-summary/SKILL.md
            skill_name = relative.split("/", 1)[0]
            content = services.read_workspace_file(path)
            if content is not None:
                ws.skills[skill_name] = content

    missing = ws.missing_required()
    if missing:
        logger.warning(
            "Workspace %s missing required files: %s", workspace_prefix, missing
        )
    return ws
