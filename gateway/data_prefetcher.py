"""Data prefetcher — deterministic input fetch driven by the Route
Registry's `data_sources` list (SDD §4.2).

Downloads from GCS to `/tmp/workspace/{task_id}/inputs/` so the agent can
read via a simple `read_data('inputs/…')` call without knowing about GCS.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List

from .storage_backend import StorageBackend

logger = logging.getLogger(__name__)


@dataclass
class PrefetchedFile:
    name: str
    gcs_source: str
    local_path: str       # relative to workspace dir, e.g. "inputs/Q1.txt"
    absolute_path: str
    size_bytes: int

    def to_task_state(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "gcs_source": self.gcs_source,
            "local_path": self.local_path,
            "size_bytes": self.size_bytes,
        }


class DataPrefetcher:
    def __init__(self, storage: StorageBackend) -> None:
        self.storage = storage

    def prefetch(
        self,
        data_sources: List[Dict[str, str]],
        workspace_dir: str,
    ) -> List[PrefetchedFile]:
        inputs_dir = os.path.join(workspace_dir, "inputs")
        os.makedirs(inputs_dir, exist_ok=True)

        results: List[PrefetchedFile] = []
        for ds in data_sources:
            bucket = ds["bucket"]
            gcs_path = ds["gcs_path"]
            name = ds["name"]

            data = self.storage.read(bucket, gcs_path)
            if data is None:
                logger.warning(
                    "Prefetch skipped (not found): gs://%s/%s", bucket, gcs_path
                )
                continue

            # Flatten: inputs/<basename> (keep the original filename so the
            # agent sees something familiar).
            basename = os.path.basename(gcs_path) or name
            abs_path = os.path.join(inputs_dir, basename)
            with open(abs_path, "wb") as fh:
                fh.write(data)

            results.append(
                PrefetchedFile(
                    name=name,
                    gcs_source=f"gs://{bucket}/{gcs_path}",
                    local_path=f"inputs/{basename}",
                    absolute_path=abs_path,
                    size_bytes=len(data),
                )
            )
            logger.info("Prefetched %s → %s (%d bytes)", name, abs_path, len(data))
        return results
