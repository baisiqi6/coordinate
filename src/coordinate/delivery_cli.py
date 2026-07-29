"""Delivery, policy, and worker CLI registration and handlers."""
from __future__ import annotations

import argparse
import json
import sys

from .bus import pump_deliveries, send_delivery
from .db import create_delivery, list_deliveries, recover_sending_deliveries, row_to_dict
from .policy import create_deliveries_for_event, create_delivery_for_event, pump_events, render_event
from .worker import run_delivery_worker
from .cli_support import open_connection, print_json


# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json


def register_delivery_commands(subcommands) -> None:
    delivery = subcommands.add_parser("delivery", help="Create, send, and list bus deliveries")
    delivery_subcommands = delivery.add_subparsers(dest="delivery_command")

    delivery_create = delivery_subcommands.add_parser("create", help="Create a pending delivery")
    delivery_create.add_argument("--event-id")
    delivery_create.add_argument("--platform", required=True)
    delivery_create.add_argument("--destination", required=True)
    delivery_create.add_argument("--message-key", required=True)
    delivery_create.add_argument("--payload-json", required=True)
    delivery_create.set_defaults(handler=handle_delivery_create)

    delivery_list = delivery_subcommands.add_parser("list", help="List deliveries")
    delivery_list.add_argument("--status")
    delivery_list.add_argument("--platform")
    delivery_list.add_argument("--type", choices=["dry_run", "live"], dest="delivery_type")
    delivery_list.set_defaults(handler=handle_delivery_list)

    delivery_send = delivery_subcommands.add_parser("send", help="Send one pending/failed delivery")
    delivery_send.add_argument("delivery_id")
    delivery_send.set_defaults(handler=handle_delivery_send)

    delivery_pump = delivery_subcommands.add_parser("pump", help="Send pending deliveries")
    delivery_pump.add_argument("--platform")
    delivery_pump.add_argument("--limit", type=int, default=10)
    delivery_pump.add_argument(
        "--recover-sending",
        action="store_true",
        help="Reset sending deliveries to pending before pumping; use only after restart/no active sender",
    )
    delivery_pump.set_defaults(handler=handle_delivery_pump)

    delivery_recover = delivery_subcommands.add_parser(
        "recover-sending",
        help="Reset sending deliveries to pending after a crashed sender",
    )
    delivery_recover.add_argument("--platform")
    delivery_recover.set_defaults(handler=handle_delivery_recover_sending)

    policy = subcommands.add_parser("policy", help="Render workflow events into visible deliveries")
    policy_subcommands = policy.add_subparsers(dest="policy_command")

    policy_render = policy_subcommands.add_parser("render-event", help="Render a supported event")
    policy_render.add_argument("event_id")
    policy_render.add_argument("--platform", required=True)
    policy_render.add_argument("--destination", required=True)
    policy_render.set_defaults(handler=handle_policy_render_event)

    policy_create = policy_subcommands.add_parser(
        "create-delivery",
        help="Create an idempotent visible-message delivery for a supported event",
    )
    policy_create.add_argument("event_id")
    policy_create.add_argument("--platform", required=True)
    policy_create.add_argument("--destination", required=True)
    policy_create.set_defaults(handler=handle_policy_create_delivery)

    policy_create_many = policy_subcommands.add_parser(
        "create-deliveries",
        help="Create all idempotent visible-message deliveries for a supported event",
    )
    policy_create_many.add_argument("event_id")
    policy_create_many.add_argument("--platform", required=True)
    policy_create_many.add_argument("--destination", required=True)
    policy_create_many.set_defaults(handler=handle_policy_create_deliveries)

    policy_pump = policy_subcommands.add_parser(
        "pump-events",
        help="Create visible-message deliveries from supported workspace events",
    )
    policy_pump.add_argument("--workspace-id", required=True)
    policy_pump.add_argument("--platform", required=True)
    policy_pump.add_argument("--destination", required=True)
    policy_pump.add_argument("--limit", type=int, default=20)
    policy_pump.add_argument("--task-id")
    policy_pump.add_argument("--event-type")
    policy_pump.add_argument(
        "--allow-backfill",
        action="store_true",
        help="Allow broad live-platform backfill. Without this, live pumps require --task-id or --event-type.",
    )
    policy_pump.set_defaults(handler=handle_policy_pump_events)

    worker = subcommands.add_parser("worker", help="Run coordinator worker loops")
    worker_subcommands = worker.add_subparsers(dest="worker_command")

    worker_delivery = worker_subcommands.add_parser("delivery", help="Continuously pump pending deliveries")
    worker_delivery.add_argument("--platform")
    worker_delivery.add_argument("--limit", type=int, default=10)
    worker_delivery.add_argument("--interval", type=float, default=5.0)
    worker_delivery.add_argument("--once", action="store_true", help="Run exactly one pump iteration")
    worker_delivery.add_argument("--max-iterations", type=int)
    worker_delivery.add_argument(
        "--recover-sending",
        action="store_true",
        help="Reset sending deliveries to pending on the first iteration; use only after restart/no active sender",
    )
    worker_delivery.set_defaults(handler=handle_worker_delivery)


def handle_delivery_create(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print("error: --payload-json must decode to an object", file=sys.stderr)
        return 1
    with _conn(args) as conn:
        row, created = create_delivery(
            conn,
            event_id=args.event_id,
            platform=args.platform,
            destination=args.destination,
            message_key=args.message_key,
            payload=payload,
        )
        delivery = row_to_dict(row)
    _print_json({"created": created, "delivery": delivery})
    return 0


def handle_delivery_list(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        deliveries = [
            row_to_dict(row)
            for row in list_deliveries(
                conn,
                status=args.status,
                platform=args.platform,
                delivery_type=args.delivery_type,
            )
        ]
    _print_json({"deliveries": deliveries})
    return 0


def handle_delivery_send(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = send_delivery(conn, args.delivery_id, output_stream=sys.stderr)
    _print_json({"result": result.to_dict()})
    return 0


def handle_delivery_pump(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = pump_deliveries(
            conn,
            platform=args.platform,
            limit=args.limit,
            output_stream=sys.stderr,
            recover_sending=args.recover_sending,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_delivery_recover_sending(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        rows = recover_sending_deliveries(conn, platform=args.platform)
        deliveries = [row_to_dict(row) for row in rows]
    _print_json({"recovered": len(deliveries), "deliveries": deliveries})
    return 0


def handle_policy_render_event(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = render_event(
            conn,
            args.event_id,
            platform=args.platform,
            destination=args.destination,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_policy_create_delivery(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = create_delivery_for_event(
            conn,
            args.event_id,
            platform=args.platform,
            destination=args.destination,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_policy_create_deliveries(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        results = create_deliveries_for_event(
            conn,
            args.event_id,
            platform=args.platform,
            destination=args.destination,
        )
    _print_json({"results": [result.to_dict() for result in results]})
    return 0


def handle_policy_pump_events(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = pump_events(
            conn,
            workspace_id=args.workspace_id,
            platform=args.platform,
            destination=args.destination,
            limit=args.limit,
            task_id=args.task_id,
            event_type=args.event_type,
            allow_backfill=args.allow_backfill,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_worker_delivery(args: argparse.Namespace) -> int:
    max_iterations = 1 if args.once else args.max_iterations
    with _conn(args) as conn:
        result = run_delivery_worker(
            conn,
            platform=args.platform,
            limit=args.limit,
            interval=args.interval,
            max_iterations=max_iterations,
            output_stream=sys.stderr,
            recover_sending=args.recover_sending,
        )
    _print_json({"result": result.to_dict()})
    return 0
