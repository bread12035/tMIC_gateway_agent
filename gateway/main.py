"""Gateway Pod entry point (SDD §2.5, §4, §11).

Responsibilities of this module:

1. Start a Pub/Sub streaming-pull subscriber for every subscription in
   `PUBSUB_SUBSCRIPTIONS` (each with `max_messages=1` so concurrency is
   controlled by HPA).
2. For each received message, invoke `Gateway.handle_message`, which walks
   the full task lifecycle: route lookup → prefetch → workspace load →
   prompt assembly → agent runtime → output collection → task state
   writeback → workspace cleanup → ack.

The `Gateway` class is structured so it can be driven directly from tests
with a fake message object and an `InMemoryStorageBackend`.
"""
from __future__ import annotations

import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .context import AgentContext
from .data_prefetcher import DataPrefetcher
from .output_collector import OutputCollector
from .prompt_assembler import assemble_system_prompt
from .route_registry import RouteConfig, get_route
from .services import GatewayServices, Publisher
from .storage_backend import StorageBackend, default_backend
from .task_state import new_task_state
from .workspace_loader import load_workspace

logger = logging.getLogger(__name__)


# Signature of the agent-runtime entry point (imported lazily at runtime so
# tests can inject a fake without pulling LangChain).
AgentRunner = Callable[[str, str, List, Dict[str, Any]], Dict[str, Any]]
"""(system_prompt, task_description, tools, config) -> AgentOutcome dict"""


@dataclass
class GatewayConfig:
    workspace_bucket: str
    output_bucket: str
    tasklog_bucket: str
    tmp_dir: str = "/tmp/workspace"
    default_llm_model: str = "claude-sonnet-4-20250514"
    max_iterations: int = 10

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        return cls(
            workspace_bucket=os.environ.get("GCS_WORKSPACE_BUCKET", "my-agent-workspaces"),
            output_bucket=os.environ.get("GCS_OUTPUT_BUCKET", "my-agent-outputs"),
            tasklog_bucket=os.environ.get("GCS_TASKLOG_BUCKET", "my-agent-task-logs"),
            tmp_dir=os.environ.get("WORKSPACE_TMP_DIR", "/tmp/workspace"),
            default_llm_model=os.environ.get("DEFAULT_LLM_MODEL", "claude-sonnet-4-20250514"),
            max_iterations=int(os.environ.get("AGENT_MAX_ITERATIONS", "10")),
        )


class Gateway:
    """Gateway controller — one instance per Pod."""

    def __init__(
        self,
        config: GatewayConfig,
        storage: StorageBackend,
        publisher: Optional[Publisher] = None,
        agent_runner: Optional[AgentRunner] = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.publisher = publisher
        self._agent_runner = agent_runner  # lazily loaded in _run_agent()

    # ── public API ─────────────────────────────────────────────────────
    def handle_message(self, message: Any, subscription_id: str) -> Dict[str, Any]:
        """Process one Pub/Sub message end-to-end.

        `message` may be a real `pubsub_v1.subscriber.message.Message` or a
        simple object exposing `.data` (bytes) and `.ack()/.nack()`.
        Returns the serialized task state for inspection in tests.
        """
        import json

        route = get_route(subscription_id)
        raw = json.loads(message.data.decode("utf-8"))

        task_id = self._make_task_id(route, raw)
        workspace_dir = os.path.join(self.config.tmp_dir, task_id)
        os.makedirs(workspace_dir, exist_ok=True)

        ctx = self._build_context(task_id, route, workspace_dir)
        services = GatewayServices(ctx, self.storage, publisher=self.publisher)

        state = new_task_state(
            task_id=task_id,
            agent_id=route.agent_id,
            subscription_id=subscription_id,
            raw_message=raw,
            llm_model=route.llm_model,
        )

        try:
            # 1. Render task + data_sources from domain message fields
            task_description = route.render_task(raw)
            data_sources = route.render_data_sources(raw)
            state.set_task_description(task_description)

            # 2. Prefetch inputs to local workspace
            prefetcher = DataPrefetcher(self.storage)
            prefetched = prefetcher.prefetch(data_sources, workspace_dir)
            state.record_prefetch([p.to_task_state() for p in prefetched])

            # 3. Load workspace MD files (only skills registered on the route)
            workspace = load_workspace(
                services,
                route.workspace,
                route.agent_id,
                enabled_skills=route.skills,
            )

            # 4. Assemble boot-sequence system prompt
            system_prompt = assemble_system_prompt(
                workspace, task_description, prefetched
            )

            # 5. Run the agent
            outcome = self._run_agent(
                services=services,
                route=route,
                system_prompt=system_prompt,
                task_description=task_description,
                workspace_dir=workspace_dir,
            )
            state.set_outcome(outcome)

            # 6. Collect outputs → GCS
            collector = OutputCollector(services)
            landed = collector.collect(workspace_dir)
            state.set_landed_files([l.to_task_state() for l in landed])

            state.mark_completed()
        except Exception as exc:
            logger.exception("Task %s failed", task_id)
            state.mark_failed(exc)
        finally:
            # 7. Merge audit log + write task state
            state.merge_audit_log(ctx.audit_log)
            try:
                state.save_to_gcs(services, self.config.tasklog_bucket)
            except Exception:  # pragma: no cover — best-effort
                logger.exception("Failed to persist task state %s", task_id)

            # 8. Cleanup local tmp + ack
            self._cleanup_workspace(workspace_dir)
            try:
                message.ack()
            except Exception:  # pragma: no cover
                logger.exception("Ack failed for %s", task_id)

        return state.to_dict()

    # ── helpers ────────────────────────────────────────────────────────
    def _make_task_id(self, route: RouteConfig, raw: Dict[str, Any]) -> str:
        # e.g. earnings-agent-TSMC-2026Q1-a3f8
        parts = [route.agent_id]
        for key in ("company", "fiscal_year", "fiscal_quarter", "report_type"):
            if key in raw:
                parts.append(str(raw[key]))
        parts.append(uuid.uuid4().hex[:4])
        return "-".join(parts)

    def _build_context(
        self,
        task_id: str,
        route: RouteConfig,
        workspace_dir: str,
    ) -> AgentContext:
        workspace_prefix = route.workspace  # e.g. "workspaces/earnings-agent"
        return AgentContext(
            agent_id=route.agent_id,
            task_id=task_id,
            workspace_bucket=self.config.workspace_bucket,
            output_bucket=self.config.output_bucket,
            local_workspace=workspace_dir,
            allowed_read_prefixes=[
                workspace_prefix,
                "shared/",
            ],
            allowed_write_prefixes=[
                f"{workspace_prefix}/MEMORY.md",
                f"{workspace_prefix}/memory/",
            ],
            allowed_topics=list(route.allowed_topics),
        )

    def _run_agent(
        self,
        services: GatewayServices,
        route: RouteConfig,
        system_prompt: str,
        task_description: str,
        workspace_dir: str,
    ) -> Dict[str, Any]:
        # Ensure an outputs/ dir exists so skills can drop files.
        os.makedirs(os.path.join(workspace_dir, "outputs"), exist_ok=True)

        if self._agent_runner is None:
            # Lazy import — avoids pulling LangChain unless we actually run
            # the real runtime (e.g. unit tests use an injected runner).
            from agent.graph import run_agent  # type: ignore

            self._agent_runner = run_agent

        from tools import build_tools, init_tools  # local import

        init_tools(services, workspace_dir=workspace_dir)
        tools = build_tools(route.skills)

        config = {
            "llm_model": route.llm_model or self.config.default_llm_model,
            "max_iterations": route.max_iterations or self.config.max_iterations,
            "outputs_dir": os.path.join(workspace_dir, "outputs"),
        }
        return self._agent_runner(system_prompt, task_description, tools, config)

    def _cleanup_workspace(self, workspace_dir: str) -> None:
        if os.path.isdir(workspace_dir):
            shutil.rmtree(workspace_dir, ignore_errors=True)


# ── Streaming-pull entry point ────────────────────────────────────────
def run_forever() -> None:  # pragma: no cover — infra glue
    """Start streaming-pull subscribers for every managed subscription."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    subscriptions = [
        s.strip() for s in os.environ.get("PUBSUB_SUBSCRIPTIONS", "").split(",")
        if s.strip()
    ]
    project_id = os.environ["GCP_PROJECT_ID"]

    from google.cloud import pubsub_v1  # type: ignore

    subscriber = pubsub_v1.SubscriberClient()
    publisher_client = pubsub_v1.PublisherClient()

    def _publish(topic: str, data: bytes, attrs: Dict[str, str]) -> None:
        topic_path = publisher_client.topic_path(project_id, topic)
        publisher_client.publish(topic_path, data=data, **attrs).result()

    gateway = Gateway(
        config=GatewayConfig.from_env(),
        storage=default_backend(),
        publisher=_publish,
    )

    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    futures = []
    for sub in subscriptions:
        sub_path = subscriber.subscription_path(project_id, sub)

        def _callback(msg, _sub=sub):
            try:
                gateway.handle_message(msg, _sub)
            except Exception:
                logger.exception("Unhandled error in handle_message")
                msg.nack()

        future = subscriber.subscribe(sub_path, callback=_callback, flow_control=flow_control)
        logger.info("Subscribed to %s", sub_path)
        futures.append(future)

    # Block forever
    for f in futures:
        try:
            f.result()
        except KeyboardInterrupt:
            f.cancel()


if __name__ == "__main__":  # pragma: no cover
    run_forever()
