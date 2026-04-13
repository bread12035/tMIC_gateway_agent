"""GatewayServices — the sole layer allowed to touch external resources.

Per SDD §6, GatewayServices is the single choke point for GCS / Pub/Sub
access. Gateway's prefetch/output collection and Agent tools both go
through it. Responsibilities:

* Path + topic whitelist enforcement against `AgentContext`.
* Per-session operation limits (reads / writes / publishes).
* Structured audit log (consumed later by Task State, §5.5).
* Automatic prefix handling (agents never see raw GCS paths).

The class is deliberately backend-agnostic: `StorageBackend` can be the
real GCS client or the in-memory fake used in tests.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from .context import AgentContext
from .storage_backend import StorageBackend


# ── Default per-session limits (can be overridden via env) ─────────────
DEFAULT_MAX_READS = int(os.environ.get("MAX_READS_PER_SESSION", "50"))
DEFAULT_MAX_WRITES = int(os.environ.get("MAX_WRITES_PER_SESSION", "20"))
DEFAULT_MAX_PUBLISHES = int(os.environ.get("MAX_PUBLISHES_PER_SESSION", "10"))
DEFAULT_MAX_READ_BYTES = int(os.environ.get("MAX_FILE_READ_SIZE_MB", "10")) * 1024 * 1024


@dataclass
class OperationLimits:
    max_reads: int = DEFAULT_MAX_READS
    max_writes: int = DEFAULT_MAX_WRITES
    max_publishes: int = DEFAULT_MAX_PUBLISHES
    max_read_bytes: int = DEFAULT_MAX_READ_BYTES


class OperationLimitExceeded(Exception):
    pass


class PermissionDenied(Exception):
    pass


# A Publisher is any callable(topic, data_bytes, attrs) -> None — concrete
# Pub/Sub client or in-memory fake.
Publisher = Callable[[str, bytes, Dict[str, str]], None]


def _noop_publisher(topic: str, data: bytes, attrs: Dict[str, str]) -> None:
    """Fallback publisher used when Pub/Sub isn't configured (local dev)."""
    return None


class GatewayServices:
    """Single entry point for GCS + Pub/Sub operations."""

    def __init__(
        self,
        ctx: AgentContext,
        storage: StorageBackend,
        publisher: Optional[Publisher] = None,
        limits: Optional[OperationLimits] = None,
    ) -> None:
        self.ctx = ctx
        self.storage = storage
        self.publisher = publisher or _noop_publisher
        self.limits = limits or OperationLimits()

    # ── internal helpers ──────────────────────────────────────────────
    def _check_read_prefix(self, path: str) -> None:
        if not self.ctx.allowed_read_prefixes:
            return  # no whitelist configured → allow all
        if any(path.startswith(p) for p in self.ctx.allowed_read_prefixes):
            return
        raise PermissionDenied(
            f"Read path {path!r} not in allowed prefixes "
            f"{self.ctx.allowed_read_prefixes}"
        )

    def _check_write_prefix(self, path: str) -> None:
        if not self.ctx.allowed_write_prefixes:
            return
        if any(path.startswith(p) for p in self.ctx.allowed_write_prefixes):
            return
        raise PermissionDenied(
            f"Write path {path!r} not in allowed prefixes "
            f"{self.ctx.allowed_write_prefixes}"
        )

    def _check_topic(self, topic: str) -> None:
        if not self.ctx.allowed_topics:
            raise PermissionDenied("No Pub/Sub topics allowed for this agent")
        if topic not in self.ctx.allowed_topics:
            raise PermissionDenied(
                f"Topic {topic!r} not in allowed topics {self.ctx.allowed_topics}"
            )

    def _bump(self, kind: str, limit: int) -> None:
        self.ctx.operation_counts[kind] = self.ctx.operation_counts.get(kind, 0) + 1
        if self.ctx.operation_counts[kind] > limit:
            raise OperationLimitExceeded(
                f"{kind} operation limit ({limit}) exceeded for session "
                f"{self.ctx.task_id}"
            )

    def _resolve_local_input(self, path: str) -> Optional[str]:
        """Allow the agent to read pre-fetched local inputs via a virtual
        `inputs/...` path. Returns the absolute file path if it resolves."""
        if not self.ctx.local_workspace:
            return None
        if path.startswith("inputs/"):
            return os.path.join(self.ctx.local_workspace, path)
        return None

    # ── public API (SDD §6.3) ─────────────────────────────────────────
    def read_data(self, path: str) -> Dict:
        """Read a file from the workspace / inputs area.

        Returns a dict with `success`, `content` (str) or `error`.
        """
        start = time.perf_counter()
        try:
            self._bump("read", self.limits.max_reads)

            # 1) Local prefetched inputs take priority
            local = self._resolve_local_input(path)
            if local and os.path.exists(local):
                with open(local, "rb") as fh:
                    data = fh.read()
                if len(data) > self.limits.max_read_bytes:
                    raise ValueError(
                        f"File {path!r} exceeds max read size "
                        f"({self.limits.max_read_bytes} bytes)"
                    )
                content = data.decode("utf-8", errors="replace")
                self.ctx.record_audit(
                    "read_data", {"path": path}, True,
                    int((time.perf_counter() - start) * 1000),
                )
                return {"success": True, "content": content, "source": "local"}

            # 2) Otherwise read from workspace bucket
            self._check_read_prefix(path)
            blob = self.storage.read(self.ctx.workspace_bucket, path)
            if blob is None:
                raise FileNotFoundError(f"gs://{self.ctx.workspace_bucket}/{path}")
            if len(blob) > self.limits.max_read_bytes:
                raise ValueError(
                    f"File {path!r} exceeds max read size "
                    f"({self.limits.max_read_bytes} bytes)"
                )
            content = blob.decode("utf-8", errors="replace")
            self.ctx.record_audit(
                "read_data", {"path": path}, True,
                int((time.perf_counter() - start) * 1000),
            )
            return {"success": True, "content": content, "source": "gcs"}
        except Exception as e:
            self.ctx.record_audit(
                "read_data", {"path": path}, False,
                int((time.perf_counter() - start) * 1000),
                error=str(e),
            )
            return {"success": False, "error": str(e)}

    def write_data(self, path: str, content: str, mode: str = "overwrite") -> Dict:
        """Write to the agent's workspace (memory / daily notes)."""
        start = time.perf_counter()
        try:
            self._bump("write", self.limits.max_writes)
            self._check_write_prefix(path)

            if mode == "append":
                existing = self.storage.read(self.ctx.workspace_bucket, path) or b""
                payload = existing
                if payload and not payload.endswith(b"\n"):
                    payload += b"\n"
                payload += content.encode("utf-8")
            else:
                payload = content.encode("utf-8")

            self.storage.write(self.ctx.workspace_bucket, path, payload)
            self.ctx.record_audit(
                "write_data", {"path": path, "mode": mode, "size": len(payload)},
                True, int((time.perf_counter() - start) * 1000),
            )
            return {"success": True, "path": path, "size_bytes": len(payload)}
        except Exception as e:
            self.ctx.record_audit(
                "write_data", {"path": path, "mode": mode}, False,
                int((time.perf_counter() - start) * 1000),
                error=str(e),
            )
            return {"success": False, "error": str(e)}

    def write_output(self, filename: str, content: str | bytes) -> Dict:
        """Write a result file to the output bucket with auto-prefixing.

        Final path: `outputs/{agent_id}/{YYYY-MM-DD}/{task_id}/{filename}`.
        """
        start = time.perf_counter()
        try:
            self._bump("write", self.limits.max_writes)
            if "/" in filename or ".." in filename:
                raise ValueError(f"Invalid output filename: {filename!r}")

            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            full_path = (
                f"outputs/{self.ctx.agent_id}/{date}/{self.ctx.task_id}/{filename}"
            )
            data = content.encode("utf-8") if isinstance(content, str) else content
            self.storage.write(self.ctx.output_bucket, full_path, data)

            self.ctx.record_audit(
                "write_output", {"filename": filename, "size": len(data)},
                True, int((time.perf_counter() - start) * 1000),
            )
            return {
                "success": True,
                "gcs_path": f"gs://{self.ctx.output_bucket}/{full_path}",
                "size_bytes": len(data),
            }
        except Exception as e:
            self.ctx.record_audit(
                "write_output", {"filename": filename}, False,
                int((time.perf_counter() - start) * 1000),
                error=str(e),
            )
            return {"success": False, "error": str(e)}

    def publish_message(
        self, topic: str, data: Dict, attributes: Optional[Dict[str, str]] = None
    ) -> Dict:
        """Publish to a whitelisted Pub/Sub topic."""
        start = time.perf_counter()
        try:
            self._bump("publish", self.limits.max_publishes)
            self._check_topic(topic)

            import json

            payload = json.dumps(data).encode("utf-8")
            self.publisher(topic, payload, attributes or {})
            self.ctx.record_audit(
                "publish_message", {"topic": topic}, True,
                int((time.perf_counter() - start) * 1000),
            )
            return {"success": True, "topic": topic}
        except Exception as e:
            self.ctx.record_audit(
                "publish_message", {"topic": topic}, False,
                int((time.perf_counter() - start) * 1000),
                error=str(e),
            )
            return {"success": False, "error": str(e)}

    def dispatch_sub_agent(
        self, agent_id: str, task: str, input_data: Optional[Dict] = None
    ) -> Dict:
        """Asynchronously hand a task to another agent via Pub/Sub (SDD §7.6)."""
        topic = f"agent-tasks-{agent_id}"
        return self.publish_message(
            topic,
            {
                "target_agent": agent_id,
                "task": task,
                "input_data": input_data or {},
                "dispatched_by": self.ctx.agent_id,
                "parent_task_id": self.ctx.task_id,
            },
        )

    # ── Gateway-only helpers (not exposed to agents) ──────────────────
    def read_workspace_file(self, path: str) -> Optional[str]:
        """Un-checked read used by Gateway itself for boot-sequence MD loading."""
        data = self.storage.read(self.ctx.workspace_bucket, path)
        return data.decode("utf-8") if data is not None else None

    def list_workspace(self, prefix: str) -> List[str]:
        return self.storage.list_prefix(self.ctx.workspace_bucket, prefix)

    def put_output(self, path: str, data: bytes) -> None:
        """Raw output write used by output_collector (bypasses limits)."""
        self.storage.write(self.ctx.output_bucket, path, data)

    def append_task_log(self, bucket: str, path: str, line: str) -> None:
        """Append a JSONL line to the task log bucket."""
        self.storage.append_line(bucket, path, line)
