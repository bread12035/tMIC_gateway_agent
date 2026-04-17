"""Workspace loader — fetches the MD file bundle that becomes the system
prompt (SDD §4.4 "Boot Sequence" + §9 "Workspace structure").

The workspace lives at `gs://{workspace_bucket}/{route.workspace}/`. We load
the required and optional MD files in a fixed order and also enumerate the
`skills/*/SKILL.md` files **that the Route Registry has explicitly enabled
for this agent** (so the system prompt only advertises the skills the agent
is actually allowed to run).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

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


def _normalise(name: str) -> str:
    """Normalise skill identifiers so hyphen/underscore variants compare equal."""
    return name.replace("-", "_")


def load_workspace(
    services: GatewayServices,
    workspace_prefix: str,
    agent_id: str,
    enabled_skills: Optional[Iterable[str]] = None,
) -> LoadedWorkspace:
    """Load all boot-sequence MD files plus the enabled skill descriptors.

    When `enabled_skills` is provided, only skills whose directory name
    matches one of those entries (hyphen/underscore variants are treated as
    equivalent) are loaded into `ws.skills`. Passing `None` preserves the
    legacy behaviour of loading every SKILL.md under the workspace — this is
    still used by `invoke_sub_agent` when inheriting a parent workspace and
    by tests that seed a known fixture.
    """
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

    enabled_set: Optional[set[str]] = None
    if enabled_skills is not None:
        enabled_set = {_normalise(s) for s in enabled_skills}

    # Enumerate skills/<skill>/SKILL.md under this workspace
    skill_prefix = f"{workspace_prefix}/skills/"
    for path in services.list_workspace(skill_prefix):
        if not path.endswith("/SKILL.md"):
            continue
        # workspaces/earnings-agent/skills/transcript-summary/SKILL.md
        relative = path[len(skill_prefix):]                 # transcript-summary/SKILL.md
        skill_name = relative.split("/", 1)[0]

        if enabled_set is not None and _normalise(skill_name) not in enabled_set:
            logger.info(
                "Skipping skill %s — not registered for agent %s",
                skill_name, agent_id,
            )
            continue

        content = services.read_workspace_file(path)
        if content is not None:
            ws.skills[skill_name] = content

    missing = ws.missing_required()
    if missing:
        logger.warning(
            "Workspace %s missing required files: %s", workspace_prefix, missing
        )
    return ws
