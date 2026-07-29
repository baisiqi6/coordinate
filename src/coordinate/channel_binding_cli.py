"""CLI handlers for the channel binding authority.

Coordinate is the sole persistent authority for
``(platform, channel_id) -> workspace_id``. These handlers are thin wrappers
over the ``coordinate.db`` channel binding API; they own argument validation,
JSON output and exit codes only. The registrar is attached to the existing
``workspace channel`` subcommand group from
``workspace_cli.register_workspace_commands()``; root ``cli.py`` is unchanged.
"""
from __future__ import annotations

import argparse
import sys

from .cli_support import open_connection, print_json
from .db import (
    bind_channel_workspace,
    list_channel_bindings,
    release_channel_workspace,
    resolve_channel_workspace,
)


# Compatibility aliases so handlers read like the other registrars.
_conn = open_connection
_print_json = print_json


def handle_workspace_channel_bind(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = bind_channel_workspace(
                conn,
                platform=args.platform,
                channel_id=args.channel_id,
                workspace_id=args.workspace_id,
                actor=args.actor,
                reason=args.reason,
                idempotency_key=args.idempotency_key,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    _print_json(result)
    return 0


def handle_workspace_channel_resolve(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            binding = resolve_channel_workspace(
                conn,
                platform=args.platform,
                channel_id=args.channel_id,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    if binding is None:
        _print_json({"binding": None, "status": "unbound"})
    else:
        _print_json({"binding": binding.to_dict(), "status": "bound"})
    return 0


def handle_workspace_channel_release(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = release_channel_workspace(
                conn,
                platform=args.platform,
                channel_id=args.channel_id,
                expected_workspace_id=args.expected_workspace_id,
                actor=args.actor,
                reason=args.reason,
                idempotency_key=args.idempotency_key,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    _print_json(result)
    return 0


def handle_workspace_channel_list(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            bindings = list_channel_bindings(
                conn,
                platform=args.platform,
                workspace_id=args.workspace_id,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    _print_json({"bindings": [binding.to_dict() for binding in bindings]})
    return 0


def register_workspace_channel_commands(workspace_subcommands) -> None:
    """Register the ``workspace channel`` subcommand group."""
    workspace_channel = workspace_subcommands.add_parser(
        "channel",
        help="Manage the (platform, channel_id) -> workspace_id binding authority",
    )
    channel_sub = workspace_channel.add_subparsers(dest="workspace_channel_command")

    channel_bind = channel_sub.add_parser(
        "bind", help="Bind a channel to a workspace (event-first, atomic)"
    )
    channel_bind.add_argument("platform")
    channel_bind.add_argument("channel_id")
    channel_bind.add_argument("workspace_id")
    channel_bind.add_argument("--actor", required=True, help="Actor performing the bind")
    channel_bind.add_argument("--reason", required=True, help="Non-empty reason for the bind")
    channel_bind.add_argument(
        "--idempotency-key", required=True, help="Idempotency key for exact-replay safety"
    )
    channel_bind.set_defaults(handler=handle_workspace_channel_bind)

    channel_resolve = channel_sub.add_parser(
        "resolve", help="Resolve the active workspace binding for a channel"
    )
    channel_resolve.add_argument("platform")
    channel_resolve.add_argument("channel_id")
    channel_resolve.set_defaults(handler=handle_workspace_channel_resolve)

    channel_release = channel_sub.add_parser(
        "release", help="Release a channel binding (event-first, atomic)"
    )
    channel_release.add_argument("platform")
    channel_release.add_argument("channel_id")
    channel_release.add_argument(
        "--expected-workspace-id",
        required=True,
        help="Workspace the channel is expected to be bound to (fail-closed check)",
    )
    channel_release.add_argument("--actor", required=True, help="Actor performing the release")
    channel_release.add_argument("--reason", required=True, help="Non-empty reason for the release")
    channel_release.add_argument(
        "--idempotency-key", required=True, help="Idempotency key for exact-replay safety"
    )
    channel_release.set_defaults(handler=handle_workspace_channel_release)

    channel_list = channel_sub.add_parser("list", help="List active channel bindings")
    channel_list.add_argument("--platform", help="Filter by platform (discord|kook)")
    channel_list.add_argument("--workspace-id", help="Filter by workspace id")
    channel_list.set_defaults(handler=handle_workspace_channel_list)
