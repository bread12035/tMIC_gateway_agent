"""Output collector — scans `outputs/` under the local workspace after the
agent loop finishes and uploads everything to the output bucket via
GatewayServices (SDD §4.3)."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

from .services import GatewayServices

logger = logging.getLogger(__name__)


@dataclass
class LandedFile:
    local: str
    gcs: str
    size_bytes: int

    def to_task_state(self) -> Dict[str, Any]:
        return {"local": self.local, "gcs": self.gcs, "size_bytes": self.size_bytes}


class OutputCollector:
    def __init__(self, services: GatewayServices) -> None:
        self.services = services

    def collect(self, workspace_dir: str) -> List[LandedFile]:
        outputs_dir = os.path.join(workspace_dir, "outputs")
        if not os.path.isdir(outputs_dir):
            return []

        ctx = self.services.ctx
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        prefix = f"outputs/{ctx.agent_id}/{date}/{ctx.task_id}"

        landed: List[LandedFile] = []
        for root, _, files in os.walk(outputs_dir):
            for name in files:
                abs_path = os.path.join(root, name)
                rel = os.path.relpath(abs_path, outputs_dir)
                gcs_path = f"{prefix}/{rel}"
                with open(abs_path, "rb") as fh:
                    data = fh.read()
                self.services.put_output(gcs_path, data)
                landed.append(
                    LandedFile(
                        local=rel,
                        gcs=f"gs://{ctx.output_bucket}/{gcs_path}",
                        size_bytes=len(data),
                    )
                )
                logger.info("Landed %s → %s", abs_path, gcs_path)
        return landed
