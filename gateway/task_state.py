"""Task State — per-task JSONL record written to GCS (SDD §5.2–5.5).

A `TaskState` instance is created at the start of every `handle_message`
call and updated as the lifecycle progresses. In the `finally` block the
Gateway calls `save_to_gcs()`, guaranteeing both success and failure cases
are recorded.

The schema intentionally mirrors §5.3 so future BigQuery ingestion is a
zero-transformation step.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .services import GatewayServices


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class TaskState:
    task_id: str
    agent_id: str
    subscription_id: str
    raw_message: Dict[str, Any]
    llm_model: str
    gateway_version: str
    pod_name: str

    task_description: str = ""
    prefetched_files: List[Dict[str, Any]] = field(default_factory=list)

    started_at: str = field(default_factory=_utcnow)
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None

    llm_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    iteration_count: int = 0

    status: str = "running"   # running → completed | failed | max_iterations
    landed_files: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None

    _start_monotonic: float = field(default_factory=lambda: datetime.now().timestamp())

    # ── mutation helpers ──────────────────────────────────────────────
    def set_task_description(self, description: str) -> None:
        self.task_description = description

    def record_prefetch(self, files: List[Dict[str, Any]]) -> None:
        self.prefetched_files = files

    def set_outcome(self, outcome: Dict[str, Any]) -> None:
        """Accept an AgentOutcome dict from the finalize node."""
        status = outcome.get("status") or "completed"
        self.status = status
        self.error = outcome.get("error")
        self.llm_calls = outcome.get("llm_calls", self.llm_calls)
        self.iteration_count = outcome.get("iteration_count", self.iteration_count)
        self.total_input_tokens = outcome.get("total_input_tokens", self.total_input_tokens)
        self.total_output_tokens = outcome.get("total_output_tokens", self.total_output_tokens)

    def set_landed_files(self, landed: List[Dict[str, Any]]) -> None:
        self.landed_files = landed

    def merge_audit_log(self, audit_log: List[Dict[str, Any]]) -> None:
        """Pull structured audit entries from GatewayServices."""
        self.tool_calls.extend(audit_log)

    def mark_completed(self) -> None:
        if self.status == "running":
            self.status = "completed"
        self.completed_at = _utcnow()
        self.duration_seconds = round(
            datetime.now().timestamp() - self._start_monotonic, 3
        )

    def mark_failed(self, exc: BaseException) -> None:
        self.status = "failed"
        self.error = f"{type(exc).__name__}: {exc}"
        self.completed_at = _utcnow()
        self.duration_seconds = round(
            datetime.now().timestamp() - self._start_monotonic, 3
        )

    # ── serialization (§5.3 schema) ───────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "subscription_id": self.subscription_id,
            "input": {
                "raw_message": self.raw_message,
                "task_description": self.task_description,
                "prefetched_files": self.prefetched_files,
            },
            "execution": {
                "started_at": self.started_at,
                "completed_at": self.completed_at,
                "duration_seconds": self.duration_seconds,
                "llm_calls": self.llm_calls,
                "total_input_tokens": self.total_input_tokens,
                "total_output_tokens": self.total_output_tokens,
                "tool_calls": self.tool_calls,
                "iteration_count": self.iteration_count,
            },
            "output": {
                "status": self.status,
                "landed_files": self.landed_files,
                "error": self.error,
            },
            "metadata": {
                "gateway_version": self.gateway_version,
                "llm_model": self.llm_model,
                "pod_name": self.pod_name,
            },
        }

    def save_to_gcs(self, services: GatewayServices, bucket: str) -> None:
        """Append as one JSONL line to `task_states/{YYYY-MM-DD}.jsonl`."""
        if self.completed_at is None:
            # Failed before mark_completed/mark_failed was called.
            self.mark_completed()
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = f"task_states/{date}.jsonl"
        line = json.dumps(self.to_dict(), ensure_ascii=False)
        services.append_task_log(bucket, path, line)


def new_task_state(
    task_id: str,
    agent_id: str,
    subscription_id: str,
    raw_message: Dict[str, Any],
    llm_model: str,
) -> TaskState:
    return TaskState(
        task_id=task_id,
        agent_id=agent_id,
        subscription_id=subscription_id,
        raw_message=raw_message,
        llm_model=llm_model,
        gateway_version=os.environ.get("GATEWAY_VERSION", "0.3.0"),
        pod_name=os.environ.get("POD_NAME", "local"),
    )
