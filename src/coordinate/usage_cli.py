"""CLI handlers for task-scoped usage evidence and warning policies (issue #12).

Leaves:

- ``coordinate runtime usage policy-set <workspace> --task-id ... --revision N
  --warn-observed-tokens N [--disable]``
- ``coordinate runtime usage status <workspace> --task-id ...``

Warning V1 is only observable through ``status`` and the append-only event
audit; ``usage.warning`` never enters the Discord/KOOK renderer.
"""
from __future__ import annotations

import sys

from .cli_support import open_connection, print_json
from .usage_policy import (
    UsagePolicyError,
    build_usage_status,
    set_task_usage_warning_policy,
)

# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json


def register_runtime_usage_commands(runtime_subcommands) -> None:
    runtime_usage = runtime_subcommands.add_parser(
        "usage", help="Task-scoped usage evidence and warning policy"
    )
    usage_subcommands = runtime_usage.add_subparsers(dest="runtime_usage_command")

    policy_set = usage_subcommands.add_parser(
        "policy-set", help="Set the task-scoped usage warning policy revision"
    )
    policy_set.add_argument("workspace_id")
    policy_set.add_argument("--task-id", required=True)
    policy_set.add_argument(
        "--revision", type=int, required=True, help="Monotonic policy revision"
    )
    policy_set.add_argument(
        "--warn-observed-tokens",
        type=int,
        required=True,
        dest="warn_observed_tokens",
        help="Warn when an accepted attempt observes >= this many tokens",
    )
    policy_set.add_argument(
        "--disable",
        action="store_true",
        help="Disable warnings for this policy revision",
    )
    policy_set.add_argument("--actor", default="operator")
    policy_set.set_defaults(handler=handle_runtime_usage_policy_set)

    status = usage_subcommands.add_parser(
        "status", help="Show task-scoped usage aggregate, attempt ledger and warnings"
    )
    status.add_argument("workspace_id")
    status.add_argument("--task-id", required=True)
    status.set_defaults(handler=handle_runtime_usage_status)


def handle_runtime_usage_policy_set(args) -> int:
    try:
        with open_connection(args) as conn:
            policy = set_task_usage_warning_policy(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                revision=args.revision,
                observed_tokens_threshold=args.warn_observed_tokens,
                enabled=not args.disable,
                actor=getattr(args, "actor", "operator"),
            )
    except UsagePolicyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    _print_json({"policy": policy})
    return 0


def handle_runtime_usage_status(args) -> int:
    try:
        with open_connection(args) as conn:
            status = build_usage_status(
                conn, workspace_id=args.workspace_id, task_id=args.task_id
            )
    except UsagePolicyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    _print_json(status)
    return 0
