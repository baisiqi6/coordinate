from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .cli_support import open_connection, print_json
from .db import append_event, list_events, row_to_dict
from .handoff import prepare_handoff
from .onboarding import (
    TaskAdoptRecoveryError,
    TaskCreateRecoveryError,
    adopt_plan_task,
    adopt_plan_task_files,
    adopt_plan_task_record,
    create_plan_task,
    create_plan_task_files,
    create_plan_task_record,
)
from .operator import list_pending_actions, pending_snapshot_metadata
from .plan_gate import approve_plan, reject_plan, review_request_plan
from .split_operations import SplitOperationError
from .task_dependencies import (
    DependencyUpdateError,
    TaskDependenciesRecoveryError,
    apply_dependency_mutation,
    update_task_dependencies,
)


# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json


def handle_event_append(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print("error: --payload-json must decode to an object", file=sys.stderr)
        return 1
    with _conn(args) as conn:
        result = append_event(
            conn,
            event_type=args.event_type,
            actor=args.actor,
            workspace_id=args.workspace_id,
            target=args.target,
            task_id=args.task_id,
            causation_id=args.causation_id,
            idempotency_key=args.idempotency_key,
            payload=payload,
        )
        row = row_to_dict(result.row)
    _print_json({"created": result.created, "event": row})
    return 0


def handle_event_list(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        events = [row_to_dict(row) for row in list_events(conn, args.workspace_id)]
    _print_json({"events": events})
    return 0


def handle_task_create(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print("error: --payload-json must decode to an object", file=sys.stderr)
        return 1
    try:
        with _conn(args) as conn:
            result = create_plan_task(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                plan_doc=args.plan_doc,
                title=args.title,
                owner=args.owner,
                branch=args.branch,
                phase=args.phase,
                priority=args.priority,
                actor=args.actor,
                target=args.target,
                payload=payload,
                idempotency_key=args.idempotency_key,
                operation_id=args.operation_id,
                allow_runtime_copy=args.allow_runtime_copy,
            )
    except TaskCreateRecoveryError as exc:
        # File half committed; DB half failed. Emit the same-operation recovery
        # material as structured JSON and a copyable display command.
        _print_json({"error": {"message": str(exc), **exc.recovery.to_dict()}})
        return 1
    except (SplitOperationError, ValueError) as exc:
        _print_json({
            "error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", None),
            }
        })
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_task_create_files(args: argparse.Namespace) -> int:
    try:
        result = create_plan_task_files(
            workspace_path=args.workspace_path,
            harness_root=args.harness_root,
            workspace_id=args.workspace_id,
            operation_id=args.operation_id,
            task_id=args.task_id,
            plan_doc=args.plan_doc,
            title=args.title,
            phase=args.phase,
            priority=args.priority,
            allow_runtime_copy=args.allow_runtime_copy,
        )
    except ValueError as exc:
        _print_json({"error": {"message": str(exc)}})
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_task_create_record(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print("error: --payload-json must decode to an object", file=sys.stderr)
        return 1
    try:
        with _conn(args) as conn:
            result = create_plan_task_record(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                plan_doc=args.plan_doc,
                title=args.title,
                owner=args.owner,
                branch=args.branch,
                phase=args.phase,
                actor=args.actor,
                target=args.target,
                payload=payload,
                idempotency_key=args.idempotency_key,
                operation_id=args.operation_id,
                input_fingerprint=args.input_fingerprint,
                before_fingerprint=args.before_fingerprint,
                after_fingerprint=args.after_fingerprint,
            )
    except ValueError as exc:
        _print_json({"error": {"message": str(exc)}})
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_task_adopt(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print("error: --payload-json must decode to an object", file=sys.stderr)
        return 1
    try:
        with _conn(args) as conn:
            result = adopt_plan_task(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                plan_doc=args.plan_doc,
                owner=args.owner,
                branch=args.branch,
                actor=args.actor,
                target=args.target,
                payload=payload,
                idempotency_key=args.idempotency_key,
                operation_id=args.operation_id,
                allow_runtime_copy=args.allow_runtime_copy,
                prepare_only=args.prepare_only,
            )
    except TaskAdoptRecoveryError as exc:
        # File half committed; DB half failed. Emit the same-operation recovery
        # material as structured JSON and a copyable display command.
        _print_json({"error": {"message": str(exc), **exc.recovery.to_dict()}})
        return 1
    except (SplitOperationError, ValueError) as exc:
        _print_json({
            "error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", None),
            }
        })
        return 1
    # --prepare-only returns the read-only prepare dict; the combined path
    # returns a TaskAdoptResult dataclass.
    _print_json({"result": result if isinstance(result, dict) else result.to_dict()})
    return 0


def handle_task_adopt_files(args: argparse.Namespace) -> int:
    try:
        result = adopt_plan_task_files(
            workspace_path=args.workspace_path,
            harness_root=args.harness_root,
            workspace_id=args.workspace_id,
            operation_id=args.operation_id,
            task_id=args.task_id,
            plan_doc=args.plan_doc,
            expected_item_fingerprint=args.expected_item_fingerprint,
            expected_plan_sha256=args.expected_plan_sha256,
            allow_runtime_copy=args.allow_runtime_copy,
        )
    except (SplitOperationError, ValueError) as exc:
        _print_json({
            "error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", None),
            }
        })
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_task_adopt_record(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print("error: --payload-json must decode to an object", file=sys.stderr)
        return 1
    try:
        with _conn(args) as conn:
            result = adopt_plan_task_record(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                plan_doc=args.plan_doc,
                operation_id=args.operation_id,
                input_fingerprint=args.input_fingerprint,
                before_fingerprint=args.before_fingerprint,
                after_fingerprint=args.after_fingerprint,
                owner=args.owner,
                branch=args.branch,
                actor=args.actor,
                target=args.target,
                payload=payload,
                idempotency_key=args.idempotency_key,
            )
    except (SplitOperationError, ValueError) as exc:
        _print_json({
            "error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", None),
            }
        })
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_task_update_dependencies(args: argparse.Namespace) -> int:
    try:
        with _conn(args) as conn:
            result = update_task_dependencies(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                add=args.add,
                remove=args.remove,
                allow_runtime_copy=args.allow_runtime_copy,
            )
    except TaskDependenciesRecoveryError as exc:
        # File half committed; DB half failed. Emit the structured recovery:
        # checklist-committed marker plus same-command retry and targeted
        # reconcile commands.
        _print_json({"error": {"message": str(exc), **exc.recovery.to_dict()}})
        return 1
    except (DependencyUpdateError, ValueError) as exc:
        _print_json({
            "error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", None),
            }
        })
        return 1
    _print_json({"result": result})
    return 0


def handle_task_update_dependencies_files(args: argparse.Namespace) -> int:
    """Coding-host file-only half: canonical checklist only, no DB, and
    deliberately no harnessctl preflight (the coding host does not need a
    harness runtime; the caller owns the commit/deploy/refresh/reconcile
    boundary). Service-boundary guards (absolute paths, /opt runtime copy)
    run inside ``apply_dependency_mutation`` before any mutation."""
    try:
        result = apply_dependency_mutation(
            workspace_path=args.workspace_path,
            harness_root=args.harness_root,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            add=args.add,
            remove=args.remove,
            allow_runtime_copy=args.allow_runtime_copy,
        )
    except (DependencyUpdateError, ValueError) as exc:
        _print_json({
            "error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", None),
            }
        })
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_task_handoff(args: argparse.Namespace) -> int:
    coordinator_path = str(Path(__file__).resolve().parents[2])

    with _conn(args) as conn:
        result = prepare_handoff(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            role=args.role,
            required_scope=args.required_scope,
            actor=args.actor,
            idempotency_key=args.idempotency_key,
            db_path=args.db,
            coordinator_path=coordinator_path,
            target_agent=args.target_agent,
            review_type=args.review_type,
        )

    bootstrap_file = None
    if args.write_bootstrap and result.bootstrap_text:
        workspace_path = result.workspace.path
        bootstrap_abs = os.path.join(workspace_path, result.bootstrap_recommended_path)
        os.makedirs(os.path.dirname(bootstrap_abs), exist_ok=True)
        with open(bootstrap_abs, "w") as f:
            f.write(result.bootstrap_text)
        bootstrap_file = bootstrap_abs

    output = result.to_dict()
    if bootstrap_file:
        output["bootstrap_file"] = bootstrap_file
    _print_json({"result": output})
    return 0


def handle_plan_revise(args: argparse.Namespace) -> int:
    payload = None
    if args.payload_json is not None:
        try:
            payload = json.loads(args.payload_json)
        except json.JSONDecodeError as exc:
            print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
            return 1
        if not isinstance(payload, dict):
            print("error: --payload-json must decode to an object", file=sys.stderr)
            return 1
    try:
        with _conn(args) as conn:
            # No operation_id/fingerprints: the existing service revision branch
            # appends a superseding plan.ready while carrying split metadata.
            result = create_plan_task_record(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                plan_doc=args.plan_doc,
                title=args.title,
                owner=args.owner,
                branch=args.branch,
                phase=args.phase,
                actor=args.actor,
                target=args.target,
                payload=payload,
                idempotency_key=args.idempotency_key,
            )
    except ValueError as exc:
        _print_json({"error": {"message": str(exc)}})
        return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_plan_review_request(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = review_request_plan(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            actor=args.actor,
            idempotency_key=args.idempotency_key,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_plan_approve(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = approve_plan(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            scope=args.scope,
            reviewer=args.reviewer,
            notes=args.notes,
            actor=args.actor,
            idempotency_key=args.idempotency_key,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_plan_reject(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = reject_plan(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            scope=args.scope,
            reviewer=args.reviewer,
            reason=args.reason,
            actor=args.actor,
            idempotency_key=args.idempotency_key,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_operator_pending(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        actions = list_pending_actions(conn, workspace_id=args.workspace_id)
        snapshot = pending_snapshot_metadata(conn, workspace_id=args.workspace_id)
    _print_json({
        "pending_actions": [a.to_dict() for a in actions],
        "snapshot": snapshot,
    })
    return 0


def register_planning_commands(subcommands) -> None:
    """Register the event, task, and plan parsers in their canonical positions."""
    event = subcommands.add_parser("event", help="Append or inspect normalized events")
    event_subcommands = event.add_subparsers(dest="event_command")

    event_append = event_subcommands.add_parser("append", help="Append an idempotent event")
    event_append.add_argument("event_type")
    event_append.add_argument("--workspace-id")
    event_append.add_argument("--actor", default="system")
    event_append.add_argument("--target")
    event_append.add_argument("--task-id")
    event_append.add_argument("--causation-id")
    event_append.add_argument("--idempotency-key")
    event_append.add_argument("--payload-json", default="{}")
    event_append.set_defaults(handler=handle_event_append)

    event_list = event_subcommands.add_parser("list", help="List events")
    event_list.add_argument("--workspace-id")
    event_list.set_defaults(handler=handle_event_list)

    task = subcommands.add_parser("task", help="Create and inspect coordinator task mirrors")
    task_subcommands = task.add_subparsers(dest="task_command")

    task_create = task_subcommands.add_parser(
        "create",
        help="Combined managed create: checklist file half first, DB record half second (idempotent; --operation-id to pin)",
    )
    task_create.add_argument("workspace_id")
    task_create.add_argument("--task-id", required=True)
    task_create.add_argument("--plan-doc", required=True)
    task_create.add_argument("--title")
    task_create.add_argument("--owner")
    task_create.add_argument("--branch")
    task_create.add_argument("--phase", default="ready")
    task_create.add_argument("--priority", default="p1", choices=["p0", "p1", "p2"])
    task_create.add_argument("--actor", default="operator")
    task_create.add_argument("--target", default="worker")
    task_create.add_argument("--payload-json", default="{}")
    task_create.add_argument("--idempotency-key")
    task_create.add_argument("--operation-id", help="Pin the split operation id (default: fresh UUIDv4 or reuse the deployed envelope)")
    task_create.add_argument(
        "--allow-runtime-copy",
        action="store_true",
        help="Override the /opt runtime-copy guard",
    )
    task_create.set_defaults(handler=handle_task_create)

    task_create_files = task_subcommands.add_parser(
        "create-files",
        help="Coding-host half of host-aware task create: checklist file half only (no DB write)",
    )
    task_create_files.add_argument("--workspace-path", required=True)
    task_create_files.add_argument("--harness-root", required=True)
    task_create_files.add_argument("--workspace-id", required=True)
    task_create_files.add_argument("--operation-id", required=True)
    task_create_files.add_argument("--task-id", required=True)
    task_create_files.add_argument("--plan-doc", required=True)
    task_create_files.add_argument("--title")
    task_create_files.add_argument("--phase", default="ready")
    task_create_files.add_argument("--priority", default="p1")
    task_create_files.add_argument(
        "--allow-runtime-copy",
        action="store_true",
        help="Override the /opt runtime-copy guard",
    )
    task_create_files.set_defaults(handler=handle_task_create_files)

    task_create_record = task_subcommands.add_parser(
        "create-record",
        help="Server half of host-aware task create: write DB task mirror + plan.ready only (no checklist write)",
    )
    task_create_record.add_argument("workspace_id")
    task_create_record.add_argument("--operation-id", required=True)
    task_create_record.add_argument("--input-fingerprint", required=True)
    task_create_record.add_argument("--before-fingerprint", required=True)
    task_create_record.add_argument("--after-fingerprint", required=True)
    task_create_record.add_argument("--task-id", required=True)
    task_create_record.add_argument("--plan-doc", required=True)
    task_create_record.add_argument("--title")
    task_create_record.add_argument("--owner")
    task_create_record.add_argument("--branch")
    task_create_record.add_argument("--phase", default="ready")
    task_create_record.add_argument("--actor", default="operator")
    task_create_record.add_argument("--target", default="worker")
    task_create_record.add_argument("--payload-json", default="{}")
    task_create_record.add_argument("--idempotency-key")
    task_create_record.set_defaults(handler=handle_task_create_record)

    task_adopt = task_subcommands.add_parser(
        "adopt",
        help="Combined explicit legacy first-adoption: bind an existing unbound checklist item into the managed lifecycle (file half first, DB record half second; idempotent; --prepare-only for the read-only expected fingerprints)",
    )
    task_adopt.add_argument("workspace_id")
    task_adopt.add_argument("--task-id", required=True)
    task_adopt.add_argument("--plan-doc", required=True)
    task_adopt.add_argument("--owner")
    task_adopt.add_argument("--branch")
    task_adopt.add_argument("--actor", default="operator")
    task_adopt.add_argument("--target", default="worker")
    task_adopt.add_argument("--payload-json", default="{}")
    task_adopt.add_argument("--idempotency-key")
    task_adopt.add_argument("--operation-id", help="Pin the adoption operation id (default: fresh UUIDv4 or reuse the deployed envelope)")
    task_adopt.add_argument("--allow-runtime-copy", action="store_true", help="Override the /opt runtime-copy guard")
    task_adopt.add_argument("--prepare-only", action="store_true", help="Read-only: emit the expected item fingerprint and plan digest without mutating anything")
    task_adopt.set_defaults(handler=handle_task_adopt)

    task_adopt_files = task_subcommands.add_parser(
        "adopt-files",
        help="Coding-host half of host-aware adoption: write the task.adopt envelope only (no DB write; requires the prepare-phase expected fingerprints)",
    )
    task_adopt_files.add_argument("--workspace-path", required=True)
    task_adopt_files.add_argument("--harness-root", required=True)
    task_adopt_files.add_argument("--workspace-id", required=True)
    task_adopt_files.add_argument("--operation-id", required=True)
    task_adopt_files.add_argument("--task-id", required=True)
    task_adopt_files.add_argument("--plan-doc", required=True)
    task_adopt_files.add_argument("--expected-item-fingerprint", required=True)
    task_adopt_files.add_argument("--expected-plan-sha256", required=True)
    task_adopt_files.add_argument("--allow-runtime-copy", action="store_true", help="Override the /opt runtime-copy guard")
    task_adopt_files.set_defaults(handler=handle_task_adopt_files)

    task_adopt_record = task_subcommands.add_parser(
        "adopt-record",
        help="Server half of host-aware adoption: write DB ledger + task mirror + plan.ready only (no checklist write)",
    )
    task_adopt_record.add_argument("workspace_id")
    task_adopt_record.add_argument("--operation-id", required=True)
    task_adopt_record.add_argument("--input-fingerprint", required=True)
    task_adopt_record.add_argument("--before-fingerprint", required=True)
    task_adopt_record.add_argument("--after-fingerprint", required=True)
    task_adopt_record.add_argument("--task-id", required=True)
    task_adopt_record.add_argument("--plan-doc", required=True)
    task_adopt_record.add_argument("--owner")
    task_adopt_record.add_argument("--branch")
    task_adopt_record.add_argument("--actor", default="operator")
    task_adopt_record.add_argument("--target", default="worker")
    task_adopt_record.add_argument("--payload-json", default="{}")
    task_adopt_record.add_argument("--idempotency-key")
    task_adopt_record.set_defaults(handler=handle_task_adopt_record)

    task_update_dependencies = task_subcommands.add_parser(
        "update-dependencies",
        help="Combined managed dependency update: checklist file mutation first, state refresh + targeted reconcile second (idempotent)",
    )
    task_update_dependencies.add_argument("workspace_id")
    task_update_dependencies.add_argument("--task-id", required=True)
    task_update_dependencies.add_argument(
        "--add",
        action="append",
        default=[],
        help="Existing dependency id to add (repeatable; order preserved, duplicates deduped)",
    )
    task_update_dependencies.add_argument(
        "--remove",
        action="append",
        default=[],
        help="Dependency id to remove (repeatable; order preserved, duplicates deduped)",
    )
    task_update_dependencies.add_argument(
        "--allow-runtime-copy",
        action="store_true",
        help="Override the /opt runtime-copy guard",
    )
    task_update_dependencies.set_defaults(handler=handle_task_update_dependencies)

    task_update_dependencies_files = task_subcommands.add_parser(
        "update-dependencies-files",
        help="Coding-host half of dependency update: canonical checklist file only (no DB write, no harnessctl preflight)",
    )
    task_update_dependencies_files.add_argument(
        "--workspace-path",
        required=True,
        help="Absolute path to the coding-host checkout (relative paths rejected)",
    )
    task_update_dependencies_files.add_argument(
        "--harness-root",
        required=True,
        help="Absolute harness root path (relative paths rejected; external roots supported)",
    )
    task_update_dependencies_files.add_argument("--workspace-id", required=True)
    task_update_dependencies_files.add_argument("--task-id", required=True)
    task_update_dependencies_files.add_argument(
        "--add",
        action="append",
        default=[],
        help="Existing dependency id to add (repeatable; order preserved, duplicates deduped)",
    )
    task_update_dependencies_files.add_argument(
        "--remove",
        action="append",
        default=[],
        help="Dependency id to remove (repeatable; order preserved, duplicates deduped)",
    )
    task_update_dependencies_files.add_argument(
        "--allow-runtime-copy",
        action="store_true",
        help="Override the /opt runtime-copy guard",
    )
    task_update_dependencies_files.set_defaults(handler=handle_task_update_dependencies_files)

    task_handoff = task_subcommands.add_parser("handoff", help="Generate a structured worker handoff")
    task_handoff.add_argument("workspace_id")
    task_handoff.add_argument("--task-id", required=True)
    task_handoff.add_argument("--role", required=True, choices=["worker", "reviewer"])
    task_handoff.add_argument("--required-scope", default="implementation plan", help="Gate scope to check (default: implementation plan)")
    task_handoff.add_argument("--actor", default="operator")
    task_handoff.add_argument("--idempotency-key")
    task_handoff.add_argument("--write-bootstrap", action=argparse.BooleanOptionalAction, default=True)
    task_handoff.add_argument("--target-agent", default=None, help="Target agent name for handoff delivery (e.g. mac-codex)")
    task_handoff.add_argument("--review-type", default="code", choices=["plan", "code"], help="Reviewer bootstrap mode: plan (read-only plan/spec review, no worktree/self_test) or code (review implementation, requires worktree + self_test). Default: code")
    task_handoff.set_defaults(handler=handle_task_handoff)

    plan = subcommands.add_parser("plan", help="Plan review and approval gate")
    plan_subcommands = plan.add_subparsers(dest="plan_command")

    plan_revise = plan_subcommands.add_parser(
        "revise",
        help="Revise an existing task's plan: append a superseding plan.ready (preserves split-operation metadata; no auto-approve)",
    )
    plan_revise.add_argument("workspace_id")
    plan_revise.add_argument("--task-id", required=True)
    plan_revise.add_argument("--plan-doc", required=True)
    plan_revise.add_argument("--title")
    plan_revise.add_argument("--owner")
    plan_revise.add_argument("--branch")
    plan_revise.add_argument("--phase", default="ready")
    plan_revise.add_argument("--actor", default="operator")
    plan_revise.add_argument("--target", default="worker")
    plan_revise.add_argument("--payload-json", default=None, help="Optional payload object overlaid on the stored task payload (default: keep stored fields)")
    plan_revise.add_argument("--idempotency-key")
    plan_revise.set_defaults(handler=handle_plan_revise)

    plan_review_request = plan_subcommands.add_parser("review-request", help="Request plan review")
    plan_review_request.add_argument("workspace_id")
    plan_review_request.add_argument("--task-id", required=True)
    plan_review_request.add_argument("--actor", default="operator")
    plan_review_request.add_argument("--idempotency-key")
    plan_review_request.set_defaults(handler=handle_plan_review_request)

    plan_approve = plan_subcommands.add_parser("approve", help="Approve a plan")
    plan_approve.add_argument("workspace_id")
    plan_approve.add_argument("--task-id", required=True)
    plan_approve.add_argument("--scope", required=True, help="Explicit approval scope (e.g. 'implementation plan', 'harness.reviewed')")
    plan_approve.add_argument("--reviewer")
    plan_approve.add_argument("--notes")
    plan_approve.add_argument("--actor", default="operator")
    plan_approve.add_argument("--idempotency-key")
    plan_approve.set_defaults(handler=handle_plan_approve)

    plan_reject = plan_subcommands.add_parser("reject", help="Reject a plan")
    plan_reject.add_argument("workspace_id")
    plan_reject.add_argument("--task-id", required=True)
    plan_reject.add_argument("--scope", required=True, help="Explicit rejection scope")
    plan_reject.add_argument("--reviewer")
    plan_reject.add_argument("--reason")
    plan_reject.add_argument("--actor", default="operator")
    plan_reject.add_argument("--idempotency-key")
    plan_reject.set_defaults(handler=handle_plan_reject)


def register_operator_command(subcommands) -> None:
    """Register the operator parser in its canonical position."""
    operator = subcommands.add_parser("operator", help="Operator-facing pending-action queries")
    operator_subcommands = operator.add_subparsers(dest="operator_command")

    operator_pending = operator_subcommands.add_parser("pending", help="List tasks awaiting operator action")
    operator_pending.add_argument("workspace_id")
    operator_pending.set_defaults(handler=handle_operator_pending)
