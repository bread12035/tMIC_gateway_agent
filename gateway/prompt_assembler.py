"""Boot-sequence system prompt assembler (SDD §4.4).

Assembles the workspace MD files in the fixed boot order, appends skill
descriptors, and finally adds a section describing the files prefetched by
Gateway so the agent knows exactly what is available under `inputs/`.
"""
from __future__ import annotations

from typing import List

from .data_prefetcher import PrefetchedFile
from .workspace_loader import BOOT_FILES, LoadedWorkspace


# Optional MD files (in addition to the required ones in BOOT_FILES) that
# the boot sequence should load when present.
OPTIONAL_FILES = ["IDENTITY.md", "USER.md", "TOOLS.md", "MEMORY.md"]


def assemble_system_prompt(
    workspace: LoadedWorkspace,
    task_description: str,
    prefetched: List[PrefetchedFile],
) -> str:
    sections: List[str] = []

    sections.append(
        f"# Agent: {workspace.agent_id}\n"
        "You are running inside the GCP Agent Gateway. Follow the operating "
        "rules and use the tools exposed to you."
    )

    for name, _required in BOOT_FILES:
        content = workspace.md_files.get(name)
        if content:
            sections.append(f"## [{name}]\n{content.strip()}")

    if workspace.skills:
        skill_lines = ["## [SKILLS]"]
        for skill_name, content in sorted(workspace.skills.items()):
            skill_lines.append(f"### skill: {skill_name}\n{content.strip()}")
        sections.append("\n".join(skill_lines))

    if prefetched:
        lines = ["## [INPUTS]",
                 "The Gateway has pre-fetched the following files into your "
                 "local workspace. Read them with `read_data('inputs/…')`:"]
        for pf in prefetched:
            lines.append(
                f"- `{pf.local_path}` ({pf.size_bytes} bytes) — source: {pf.gcs_source}"
            )
        sections.append("\n".join(lines))
    else:
        sections.append(
            "## [INPUTS]\nNo files were pre-fetched for this task."
        )

    sections.append(f"## [TASK]\n{task_description.strip()}")
    return "\n\n".join(sections)
