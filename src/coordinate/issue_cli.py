"""Issue command registration, handlers, and host/server split seams."""
from __future__ import annotations

import argparse

from .cli_support import open_connection, print_json
from .issues import (
    IssueTriageError,
    materialize_issue,
    materialize_issue_files,
    materialize_issue_record,
    scan_github_issues,
    scan_github_issues_via_event_cli,
    triage_issue,
)


# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json


def register_issue_commands(subcommands) -> None:

    issue = subcommands.add_parser("issue", help="Scan and triage GitHub issues")
    issue_subcommands = issue.add_subparsers(dest="issue_command")

    issue_scan = issue_subcommands.add_parser("scan", help="Scan GitHub issues into issue.spotted events")
    issue_scan.add_argument("workspace_id")
    issue_scan.add_argument("--repo", required=True, help="GitHub repository in owner/name form")
    issue_scan.add_argument("--label", help="Optional label filter")
    issue_scan.add_argument("--limit", type=int, default=50)
    issue_scan.add_argument("--actor", default="github")
    issue_scan.add_argument(
        "--event-cli-path",
        help="Append issue.spotted events through another coordinate CLI, e.g. a remote coord-ssh wrapper",
    )
    issue_scan.set_defaults(handler=handle_issue_scan)

    issue_triage = issue_subcommands.add_parser(
        "triage",
        help="Triage an issue.spotted event: accept creates a task mirror, reject/defer does not",
    )
    issue_triage.add_argument("workspace_id")
    issue_triage.add_argument("--event-id", required=True)
    issue_triage.add_argument(
        "--decision", required=True, choices=["accept", "reject", "defer"]
    )
    issue_triage.add_argument("--task-id")
    issue_triage.add_argument("--title")
    issue_triage.add_argument("--owner")
    issue_triage.add_argument("--phase", default="phase-8")
    issue_triage.add_argument("--actor", default="operator")
    issue_triage.add_argument("--reason")
    issue_triage.add_argument("--platform")
    issue_triage.add_argument("--destination")
    issue_triage.set_defaults(handler=handle_issue_triage)

    issue_materialize = issue_subcommands.add_parser(
        "materialize",
        help="Materialize an accepted issue.triaged event into a plan-backed harness task",
    )
    issue_materialize.add_argument("workspace_id")
    issue_materialize.add_argument("--event-id", required=True)
    issue_materialize.add_argument(
        "--plan-doc", required=True, help="Workspace-relative path to an operator-provided plan file"
    )
    issue_materialize.add_argument("--task-id")
    issue_materialize.add_argument("--title")
    issue_materialize.add_argument("--owner")
    issue_materialize.add_argument("--branch")
    issue_materialize.add_argument("--phase", default="ready")
    issue_materialize.add_argument("--actor", default="operator")
    issue_materialize.add_argument("--platform")
    issue_materialize.add_argument("--destination")
    issue_materialize.add_argument(
        "--allow-runtime-copy",
        action="store_true",
        help="Override the /opt runtime-copy guard (operator must set explicitly)",
    )
    issue_materialize.set_defaults(handler=handle_issue_materialize)

    issue_materialize_files = issue_subcommands.add_parser(
        "materialize-files",
        help="Coding-host half of host-aware materialize: checklist file half only (no DB write)",
    )
    issue_materialize_files.add_argument("--workspace-path", required=True)
    issue_materialize_files.add_argument("--harness-root", required=True)
    issue_materialize_files.add_argument("--workspace-id", required=True)
    issue_materialize_files.add_argument("--operation-id", required=True)
    issue_materialize_files.add_argument("--event-id", required=True, help="Accepted issue.triaged event UUID")
    issue_materialize_files.add_argument("--task-id", required=True)
    issue_materialize_files.add_argument("--plan-doc", required=True)
    issue_materialize_files.add_argument("--title")
    issue_materialize_files.add_argument("--phase", default="ready")
    issue_materialize_files.add_argument("--priority", default="p1")
    issue_materialize_files.add_argument(
        "--allow-runtime-copy", action="store_true",
        help="Override the /opt runtime-copy guard",
    )
    issue_materialize_files.set_defaults(handler=handle_issue_materialize_files)

    issue_materialize_record = issue_subcommands.add_parser(
        "materialize-record",
        help="Server half of host-aware materialize: write DB plan.ready/issue.materialized only (no harness write)",
    )
    issue_materialize_record.add_argument("workspace_id")
    issue_materialize_record.add_argument("--event-id", required=True)
    issue_materialize_record.add_argument("--plan-doc", required=True)
    issue_materialize_record.add_argument("--operation-id", required=True)
    issue_materialize_record.add_argument("--input-fingerprint", required=True)
    issue_materialize_record.add_argument("--before-fingerprint", required=True)
    issue_materialize_record.add_argument("--after-fingerprint", required=True)
    issue_materialize_record.add_argument("--task-id")
    issue_materialize_record.add_argument("--title")
    issue_materialize_record.add_argument("--owner")
    issue_materialize_record.add_argument("--branch")
    issue_materialize_record.add_argument("--phase", default="ready")
    issue_materialize_record.add_argument("--actor", default="operator")
    issue_materialize_record.add_argument("--platform")
    issue_materialize_record.add_argument("--destination")
    issue_materialize_record.set_defaults(handler=handle_issue_materialize_record)


def handle_issue_scan(args: argparse.Namespace) -> int:
    if args.event_cli_path:
        result = scan_github_issues_via_event_cli(
            workspace_id=args.workspace_id,
            repo=args.repo,
            event_cli_path=args.event_cli_path,
            label=args.label,
            limit=args.limit,
            actor=args.actor,
        )
        _print_json({"result": result.to_dict()})
        return 0
    with _conn(args) as conn:
        result = scan_github_issues(
            conn,
            workspace_id=args.workspace_id,
            repo=args.repo,
            label=args.label,
            limit=args.limit,
            actor=args.actor,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_issue_triage(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = triage_issue(
                conn,
                workspace_id=args.workspace_id,
                event_id=args.event_id,
                decision=args.decision,
                task_id=args.task_id,
                title=args.title,
                owner=args.owner,
                phase=args.phase,
                actor=args.actor,
                reason=args.reason,
                platform=args.platform,
                destination=args.destination,
            )
        except (IssueTriageError, ValueError) as exc:
            _print_json({"error": {"message": str(exc)}})
            return 1
    _print_json({"result": result.to_dict()})
    return 0


def _error_dict(exc: BaseException) -> dict[str, object]:
    """Build an error response, preserving a C2 split-operation reason if present."""
    error: dict[str, object] = {"message": str(exc)}
    reason = getattr(exc, "reason", None)
    if reason is not None:
        error["reason"] = reason
    return error


def handle_issue_materialize(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = materialize_issue(
                conn,
                workspace_id=args.workspace_id,
                event_id=args.event_id,
                plan_doc=args.plan_doc,
                task_id=args.task_id,
                title=args.title,
                owner=args.owner,
                branch=args.branch,
                phase=args.phase,
                actor=args.actor,
                platform=args.platform,
                destination=args.destination,
                allow_runtime_copy=args.allow_runtime_copy,
            )
        except (IssueTriageError, ValueError) as exc:
            # Legacy combined `issue materialize` must keep the original
            # {"error": {"message": ...}} shape; do not inject C2 reasons here.
            _print_json({"error": {"message": str(exc)}})
            return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_issue_materialize_files(args: argparse.Namespace) -> int:
    try:
        result = materialize_issue_files(
            workspace_path=args.workspace_path,
            harness_root=args.harness_root,
            workspace_id=args.workspace_id,
            operation_id=args.operation_id,
            event_id=args.event_id,
            task_id=args.task_id,
            plan_doc=args.plan_doc,
            title=args.title,
            phase=args.phase,
            priority=args.priority,
            allow_runtime_copy=args.allow_runtime_copy,
        )
    except (IssueTriageError, ValueError) as exc:
        _print_json({"error": _error_dict(exc)})
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_issue_materialize_record(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = materialize_issue_record(
                conn,
                workspace_id=args.workspace_id,
                event_id=args.event_id,
                plan_doc=args.plan_doc,
                operation_id=args.operation_id,
                input_fingerprint=args.input_fingerprint,
                before_fingerprint=args.before_fingerprint,
                after_fingerprint=args.after_fingerprint,
                task_id=args.task_id,
                title=args.title,
                owner=args.owner,
                branch=args.branch,
                phase=args.phase,
                actor=args.actor,
                platform=args.platform,
                destination=args.destination,
            )
        except (IssueTriageError, ValueError) as exc:
            _print_json({"error": _error_dict(exc)})
            return 1
    _print_json({"result": result.to_dict()})
    return 0
