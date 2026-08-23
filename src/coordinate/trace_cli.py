"""Issue #11 trace CLI: ``coordinate trace task`` / ``coordinate trace job``."""
from __future__ import annotations

import argparse
import sys

from .cli_support import open_connection, print_json
from .trace_projection import (
    DEFAULT_HISTORY_LIMIT,
    MAX_HISTORY_LIMIT,
    TraceQueryError,
    build_job_trace,
    build_task_trace,
)


def handle_trace_task(args: argparse.Namespace) -> int:
    try:
        with _conn(args) as conn:
            trace = build_task_trace(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                history_limit=args.history_limit,
            )
    except TraceQueryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    _print_json({"trace": trace})
    return 0


def handle_trace_job(args: argparse.Namespace) -> int:
    try:
        with _conn(args) as conn:
            trace = build_job_trace(
                conn,
                job_id=args.job_id,
                workspace_id=args.workspace_id,
            )
    except TraceQueryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    _print_json({"trace": trace})
    return 0


_conn = open_connection
_print_json = print_json


def register_trace_command(subcommands) -> None:
    """Register the read-only trace projection parser (Issue #11 R1)."""
    trace = subcommands.add_parser(
        "trace", help="Read-only task/job trace projection (Issue #11)"
    )
    trace_subcommands = trace.add_subparsers(dest="trace_command")

    trace_task = trace_subcommands.add_parser(
        "task", help="Project the trace for a workspace task"
    )
    trace_task.add_argument("workspace_id")
    trace_task.add_argument("task_id")
    trace_task.add_argument(
        "--history-limit",
        type=int,
        default=DEFAULT_HISTORY_LIMIT,
        help=f"Bounded related-job history (default {DEFAULT_HISTORY_LIMIT}, max {MAX_HISTORY_LIMIT})",
    )
    trace_task.set_defaults(handler=handle_trace_task)

    trace_job = trace_subcommands.add_parser(
        "job", help="Project the trace for one job"
    )
    trace_job.add_argument("job_id")
    trace_job.add_argument("--workspace-id", help="Optional exact workspace guard")
    trace_job.set_defaults(handler=handle_trace_job)
