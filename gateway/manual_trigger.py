"""Manual trigger entry point for Gateway tasks (kubectl-friendly).

In addition to the default Pub/Sub-driven loop in `gateway.main`, this module
lets operators drive a single task end-to-end without publishing a message.
The Route Registry is still consulted the same way — callers just supply the
`subscription_id` (which picks the route) and the domain payload that would
otherwise live in the Pub/Sub message body.

Two invocation surfaces are provided:

1. `trigger_task(subscription_id, payload)` — Python API. Returns the same
   serialised task state dict that `Gateway.handle_message` returns.
2. A CLI entry point: `python -m gateway.manual_trigger <subscription_id>
   <payload_json> [--payload-file FILE]` — intended for:

       kubectl exec -it <gateway-pod> -- \
           python -m gateway.manual_trigger earnings-summary-sub \
               '{"company":"TSMC","fiscal_year":2026,"fiscal_quarter":1}'

The underlying pipeline (route lookup → prefetch → workspace load → prompt
assembly → agent runtime → output collection → task state writeback) is
exactly the one used for Pub/Sub messages; we reuse `Gateway.handle_message`
by wrapping the payload in a fake message object that implements the
`.data`/`.ack()`/`.nack()` protocol.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .main import Gateway, GatewayConfig
from .services import Publisher
from .storage_backend import StorageBackend, default_backend

logger = logging.getLogger(__name__)


@dataclass
class _ManualMessage:
    """Minimal stand-in for a Pub/Sub message."""

    data: bytes
    acked: bool = False
    nacked: bool = False

    def ack(self) -> None:
        self.acked = True

    def nack(self) -> None:
        self.nacked = True


def trigger_task(
    subscription_id: str,
    payload: Dict[str, Any],
    *,
    gateway: Optional[Gateway] = None,
    storage: Optional[StorageBackend] = None,
    publisher: Optional[Publisher] = None,
) -> Dict[str, Any]:
    """Run a single task through the Gateway pipeline without Pub/Sub.

    The `subscription_id` must match a key in the Route Registry so Gateway
    can resolve the `RouteConfig`. `payload` is the domain-message body
    (same schema that would normally be published to Pub/Sub).
    """
    if gateway is None:
        gateway = Gateway(
            config=GatewayConfig.from_env(),
            storage=storage or default_backend(),
            publisher=publisher,
        )

    msg = _ManualMessage(data=json.dumps(payload).encode("utf-8"))
    return gateway.handle_message(msg, subscription_id)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m gateway.manual_trigger",
        description=(
            "Manually drive a Gateway task without publishing a Pub/Sub "
            "message. Intended for kubectl exec invocations."
        ),
    )
    parser.add_argument(
        "subscription_id",
        help="Route Registry key (same value that a Pub/Sub subscription would carry).",
    )
    parser.add_argument(
        "payload",
        nargs="?",
        default=None,
        help="Inline JSON payload. Mutually exclusive with --payload-file.",
    )
    parser.add_argument(
        "--payload-file",
        dest="payload_file",
        default=None,
        help="Path to a JSON file containing the payload.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:  # pragma: no cover — CLI glue
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _build_arg_parser().parse_args(argv)

    if args.payload and args.payload_file:
        print("Specify either <payload> or --payload-file, not both.", file=sys.stderr)
        return 2
    if not args.payload and not args.payload_file:
        print("Missing payload. Pass JSON inline or via --payload-file.", file=sys.stderr)
        return 2

    if args.payload_file:
        with open(args.payload_file, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    else:
        payload = json.loads(args.payload)

    state = trigger_task(args.subscription_id, payload)
    json.dump(state, sys.stdout, ensure_ascii=False, indent=2, default=str)
    sys.stdout.write("\n")
    return 0 if state.get("output", {}).get("status") == "completed" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
