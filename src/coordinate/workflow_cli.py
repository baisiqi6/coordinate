"""Workflow control CLI registration and handlers."""
from __future__ import annotations

import argparse

from .assignments import request_assignment
from .branches import allocate_branch
from .ci import check_ci
from .completion_cli import register_completion_commands
from .handoff import latest_prepared_handoff_bootstrap
from .reviews import check_merge_gate, check_pr_review
from .transitions import (
    accept_task,
    blocker_task,
    closeout_task,
    handoff_task,
    mark_done_task,
    review_result_task,
    unblock_task,
)
from .cli_support import open_connection, print_json


# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json


def register_branch_command(subcommands) -> None:
    """Register the branch allocation command."""
    branch = subcommands.add_parser("branch", help="Manage branch allocations")
    branch_subcommands = branch.add_subparsers(dest="branch_command")

    branch_allocate = branch_subcommands.add_parser("allocate", help="Allocate a branch for a task")
    branch_allocate.add_argument("workspace_id")
    branch_allocate.add_argument("--task-id", required=True)
    branch_allocate.add_argument("--owner")
    branch_allocate.add_argument("--actor", default="operator")
    branch_allocate.set_defaults(handler=handle_branch_allocate)


def register_forge_commands(subcommands) -> None:
    """Register CI, review, and merge gate commands."""
    ci = subcommands.add_parser("ci", help="Check CI status")
    ci_subcommands = ci.add_subparsers(dest="ci_command")

    ci_check = ci_subcommands.add_parser("check", help="Check CI status for a task's PR")
    ci_check.add_argument("workspace_id")
    ci_check.add_argument("--task-id", required=True)
    ci_check.add_argument("--pr-url")
    ci_check.add_argument("--branch")
    ci_check.add_argument("--actor", default="operator")
    ci_check.set_defaults(handler=handle_ci_check)

    review = subcommands.add_parser("review", help="Check PR review status")
    review_subcommands = review.add_subparsers(dest="review_command")

    review_check = review_subcommands.add_parser("check", help="Check PR review status for a task's PR")
    review_check.add_argument("workspace_id")
    review_check.add_argument("--task-id", required=True)
    review_check.add_argument("--pr-url")
    review_check.add_argument("--branch")
    review_check.add_argument("--actor", default="operator")
    review_check.set_defaults(handler=handle_review_check)

    merge = subcommands.add_parser("merge", help="Check merge readiness")
    merge_subcommands = merge.add_subparsers(dest="merge_command")

    merge_gate = merge_subcommands.add_parser("gate", help="Check merge gate for a task")
    merge_gate.add_argument("workspace_id")
    merge_gate.add_argument("--task-id", required=True)
    merge_gate.set_defaults(handler=handle_merge_gate)


def register_assignment_commands(subcommands) -> None:
    """Register assignment workflow commands and delegate receipt leaves."""
    assignment = subcommands.add_parser("assignment", help="Manage task assignments")
    assignment_subcommands = assignment.add_subparsers(dest="assignment_command")

    assignment_request = assignment_subcommands.add_parser("request", help="Request a task assignment")
    assignment_request.add_argument("workspace_id")
    assignment_request.add_argument("--task-id", required=True)
    assignment_request.add_argument("--owner", required=True)
    assignment_request.add_argument("--session", required=True)
    assignment_request.add_argument("--actor", default="operator")
    assignment_request.add_argument("--branch")
    assignment_request.add_argument("--platform")
    assignment_request.add_argument("--destination")
    assignment_request.add_argument("--idempotency-hint")
    assignment_request.set_defaults(handler=handle_assignment_request)

    assignment_accept = assignment_subcommands.add_parser("accept", help="Accept a task assignment")
    assignment_accept.add_argument("workspace_id")
    assignment_accept.add_argument("--task-id", required=True)
    assignment_accept.add_argument("--owner", required=True)
    assignment_accept.add_argument("--session", required=True)
    assignment_accept.add_argument("--actor", default=None)
    assignment_accept.add_argument("--branch")
    assignment_accept.add_argument("--idempotency-hint")
    assignment_accept.set_defaults(handler=handle_assignment_accept)

    assignment_handoff = assignment_subcommands.add_parser("handoff", help="Hand off a task to another agent")
    assignment_handoff.add_argument("workspace_id")
    assignment_handoff.add_argument("--task-id", required=True)
    assignment_handoff.add_argument("--target", required=True)
    assignment_handoff.add_argument("--actor", default="operator")
    assignment_handoff.add_argument("--reason")
    assignment_handoff.add_argument("--idempotency-hint")
    assignment_handoff.set_defaults(handler=handle_assignment_handoff)

    assignment_blocker = assignment_subcommands.add_parser("blocker", help="Raise a blocker on a task")
    assignment_blocker.add_argument("workspace_id")
    assignment_blocker.add_argument("--task-id", required=True)
    assignment_blocker.add_argument("--actor", default="operator")
    assignment_blocker.add_argument("--reason")
    assignment_blocker.add_argument("--idempotency-hint")
    assignment_blocker.set_defaults(handler=handle_assignment_blocker)

    assignment_unblock = assignment_subcommands.add_parser("unblock", help="Resolve a blocker on a task")
    assignment_unblock.add_argument("workspace_id")
    assignment_unblock.add_argument("--task-id", required=True)
    assignment_unblock.add_argument("--actor", required=True)
    assignment_unblock.add_argument("--decision", required=True)
    assignment_unblock.add_argument("--force", action="store_true")
    assignment_unblock.add_argument("--reason")
    assignment_unblock.add_argument("--idempotency-hint")
    assignment_unblock.set_defaults(handler=handle_assignment_unblock)

    assignment_closeout = assignment_subcommands.add_parser("closeout", help="Request closeout review for a task")
    assignment_closeout.add_argument("workspace_id")
    assignment_closeout.add_argument("--task-id", required=True)
    assignment_closeout.add_argument("--reviewer", required=True)
    assignment_closeout.add_argument("--self-test-evidence", default="")
    assignment_closeout.add_argument("--actor", default="operator")
    assignment_closeout.add_argument("--idempotency-hint")
    assignment_closeout.set_defaults(handler=handle_assignment_closeout)

    assignment_review_result = assignment_subcommands.add_parser("review-result", help="Submit a review result for a task")
    assignment_review_result.add_argument("workspace_id")
    assignment_review_result.add_argument("--task-id", required=True)
    assignment_review_result.add_argument("--reviewer", required=True)
    assignment_review_result.add_argument("--decision", required=True)
    assignment_review_result.add_argument("--summary")
    assignment_review_result.add_argument("--actor", default="operator")
    assignment_review_result.add_argument("--idempotency-hint")
    assignment_review_result.set_defaults(handler=handle_assignment_review_result)

    assignment_mark_done = assignment_subcommands.add_parser("mark-done", help="Mark a task as done")
    assignment_mark_done.add_argument("workspace_id")
    assignment_mark_done.add_argument("--task-id", required=True)
    assignment_mark_done.add_argument("--actor", default="operator")
    assignment_mark_done.add_argument("--idempotency-hint")
    assignment_mark_done.set_defaults(handler=handle_assignment_mark_done)

    register_completion_commands(assignment_subcommands)


def handle_branch_allocate(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = allocate_branch(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                owner=args.owner,
                actor=args.actor,
            )
        except ValueError as exc:
            _print_json({"error": {"message": str(exc)}})
            return 1
    _print_json({
        "workspace_id": result.workspace_id,
        "task_id": result.task_id,
        "branch": result.branch,
        "owner": result.owner,
        "event_created": result.event_created,
        "existing": result.existing,
    })
    return 0


def handle_ci_check(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = check_ci(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                pr_url=args.pr_url,
                branch=args.branch,
                actor=args.actor,
            )
        except ValueError as exc:
            _print_json({"error": {"message": str(exc)}})
            return 1
    _print_json(result.to_dict())
    return 0


def handle_review_check(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = check_pr_review(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                pr_url=args.pr_url,
                branch=args.branch,
                actor=args.actor,
            )
        except ValueError as exc:
            _print_json({"error": {"message": str(exc)}})
            return 1
    _print_json(result.to_dict())
    return 0


def handle_merge_gate(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = check_merge_gate(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
            )
        except ValueError as exc:
            _print_json({"error": {"message": str(exc)}})
            return 1
    _print_json(result.to_dict())
    return 0


def handle_assignment_request(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = request_assignment(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            owner=args.owner,
            session=args.session,
            actor=args.actor,
            branch=args.branch,
            platform=args.platform,
            destination=args.destination,
            idempotency_hint=args.idempotency_hint,
        )
    output = {
        "result": {
            "mutation": result.mutation.to_dict() if result.mutation else None,
            "event": result.event,
            "event_created": result.event_created,
            "delivery": result.delivery,
            "delivery_created": result.delivery_created,
        }
    }
    _print_json(output)
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0


def handle_assignment_accept(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = accept_task(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            owner=args.owner,
            session=args.session,
            actor=args.actor,
            branch=args.branch,
            idempotency_hint=args.idempotency_hint,
        )
        bootstrap = latest_prepared_handoff_bootstrap(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            target_agent=args.owner,
        )
    output = {
        "result": {
            "mutation": result.mutation.to_dict() if result.mutation else None,
            "event": result.event,
            "event_created": result.event_created,
            "bootstrap_text": bootstrap.get("bootstrap_text") if bootstrap else None,
            "bootstrap_path": bootstrap.get("bootstrap_path") if bootstrap else None,
            "bootstrap_event_id": bootstrap.get("event_id") if bootstrap else None,
            "execution_profile": bootstrap.get("execution_profile") if bootstrap else None,
        }
    }
    _print_json(output)
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0


def handle_assignment_handoff(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = handoff_task(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            target=args.target,
            actor=args.actor,
            reason=args.reason,
            idempotency_hint=args.idempotency_hint,
        )
    output = {
        "result": {
            "mutation": result.mutation.to_dict() if result.mutation else None,
            "event": result.event,
            "event_created": result.event_created,
        }
    }
    _print_json(output)
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0


def handle_assignment_blocker(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = blocker_task(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            actor=args.actor,
            reason=args.reason,
            idempotency_hint=args.idempotency_hint,
        )
    output = {
        "result": {
            "mutation": result.mutation.to_dict() if result.mutation else None,
            "event": result.event,
            "event_created": result.event_created,
        }
    }
    _print_json(output)
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0


def handle_assignment_unblock(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = unblock_task(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            actor=args.actor,
            decision=args.decision,
            force=args.force,
            reason=args.reason,
            idempotency_hint=args.idempotency_hint,
        )
    output = {
        "result": {
            "mutation": result.mutation.to_dict() if result.mutation else None,
            "event": result.event,
            "event_created": result.event_created,
        }
    }
    _print_json(output)
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0


def handle_assignment_closeout(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = closeout_task(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            reviewer=args.reviewer,
            actor=args.actor,
            idempotency_hint=args.idempotency_hint,
            self_test_evidence=args.self_test_evidence or None,
        )
    output = {
        "result": {
            "mutation": result.mutation.to_dict() if result.mutation else None,
            "event": result.event,
            "event_created": result.event_created,
        }
    }
    _print_json(output)
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0


def handle_assignment_review_result(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = review_result_task(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            reviewer=args.reviewer,
            decision=args.decision,
            actor=args.actor,
            summary=args.summary,
            idempotency_hint=args.idempotency_hint,
        )
    output = {
        "result": {
            "mutation": result.mutation.to_dict() if result.mutation else None,
            "event": result.event,
            "event_created": result.event_created,
        }
    }
    _print_json(output)
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0


def handle_assignment_mark_done(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = mark_done_task(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            actor=args.actor,
            idempotency_hint=args.idempotency_hint,
        )
    output = {"result": result.to_dict()}
    _print_json(output)
    if result.gate is not None and not result.gate.passed:
        return 1
    if result.event.get("event_type") == "harness.mutation_failed":
        return 1
    return 0
