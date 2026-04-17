# GCP Agent Gateway

Runtime service that implements the **GCP Agent Gateway System Design Document
(v0.3.0)** — an enterprise AI agent platform modeled after OpenClaw's
Gateway-centric architecture and deployed on GKE.

## Architecture

Five logical layers, all running in the **same Python process** inside a single
GKE container:

| Layer            | Component                                    |
|------------------|----------------------------------------------|
| Input            | Pub/Sub pull subscriptions                   |
| Control          | `gateway/` — Gateway main loop + Route Registry |
| Services         | `gateway/services.py` — GCS/Pub/Sub access   |
| Execution        | `agent/` — LangGraph StateGraph runtime      |
| Persistence      | GCS (workspaces, outputs, task state JSONL)  |

### Key design principles

1. Gateway owns the **control plane** (routing, prefetch, output collection,
   task state). Agent Runtime owns **reasoning & tool execution**.
2. Gateway never calls LLMs; Agent Runtime never directly touches GCS/Pub/Sub.
3. Agents have **no shell** — only whitelisted Python scripts via `run_safe_script`.
4. Each message = **stateless session** (LangGraph `MemorySaver` is
   in-memory only and discarded after each invocation).
5. Department-facing Pub/Sub messages contain only **domain fields**; the
   Route Registry maps subscription → internal agent configuration.

## Project layout

See `gateway/`, `agent/`, `tools/`, `skills/`, `tests/`, plus the Dockerfile
and `.env.example`.

## Running locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env       # fill in values
python -m gateway.main     # starts the Pub/Sub pull loop
```

## Manually triggering a task (kubectl)

For operational use on GKE, a task can be driven without publishing a
Pub/Sub message. The Route Registry is still consulted the usual way —
you only supply the subscription key and the domain payload:

```bash
kubectl exec -it <gateway-pod> -- \
    python -m gateway.manual_trigger earnings-summary-sub \
        '{"company":"TSMC","fiscal_year":2026,"fiscal_quarter":1}'
```

The same pipeline as a Pub/Sub delivery runs end-to-end (prefetch →
workspace load → prompt assembly → agent runtime → output collection →
task state writeback). The serialised task state is printed to stdout.

## Running tests

```bash
python -m pytest tests/ -q
```

The unit tests use in-memory fakes for GCS/Pub/Sub so the suite runs without
network access and without GCP credentials.
