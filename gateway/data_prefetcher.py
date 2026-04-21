"""Data prefetcher — deterministic input fetch driven by the Route
Registry's `data_sources` list (SDD §4.2).

Downloads from GCS to `/tmp/workspace/{task_id}/inputs/` so the agent can
read via a simple `read_data('inputs/…')` call without knowing about GCS.

v0.4.0: Each `DataSource` may carry an optional `processor` callable that
transforms the raw bytes before they land. Processor failures are logged
and skip the affected source, mirroring GCS read failures.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .storage_backend import StorageBackend

logger = logging.getLogger(__name__)


@dataclass
class PrefetchedFile:
    name: str
    gcs_source: str
    local_path: str       # relative to workspace dir, e.g. "inputs/Q1.txt"
    absolute_path: str
    size_bytes: int
    processed: bool = False  # v0.4.0: True if a processor transformed the bytes

    def to_task_state(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "gcs_source": self.gcs_source,
            "local_path": self.local_path,
            "size_bytes": self.size_bytes,
            "processed": self.processed,
        }


class DataPrefetcher:
    def __init__(self, storage: StorageBackend) -> None:
        self.storage = storage

    def prefetch(
        self,
        data_sources: List[Dict[str, Any]],
        workspace_dir: str,
        domain_fields: Optional[Dict[str, Any]] = None,
    ) -> List[PrefetchedFile]:
        domain_fields = domain_fields or {}
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

            processed = False
            processor = ds.get("processor")
            if processor is not None:
                try:
                    data = processor(data, domain_fields)
                    processed = True
                except Exception as exc:
                    logger.warning(
                        "Processor failed for %s (gs://%s/%s): %s",
                        name, bucket, gcs_path, exc,
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
                    processed=processed,
                )
            )
            logger.info("Prefetched %s → %s (%d bytes)", name, abs_path, len(data))
        return results
