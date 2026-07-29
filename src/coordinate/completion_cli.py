"""Receipt-aware completion CLI registration and handlers."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import re
import subprocess
import sys
from typing import Any

from .completion import (
    CompletionReceiptError,
    ReceiptEvidence,
    apply_completion_receipt,
    claim_completion_receipt,
    compute_mark_done_fingerprints,
    consume_completion_receipt,
    parse_iso_timestamp,
    prepare_completion_receipt,
)
from .transitions import mark_done_files, mark_done_record
from .cli_support import open_connection, print_json
from .db import find_events

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _is_canonical_sha256(value: Any) -> bool:
    """Return True if *value* is a canonical SHA-256 hex digest (64 lowercase)."""
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json

STATUS_AUTHORIZED = "authorized"
STATUS_CLAIMED = "claimed"
STATUS_APPLIED = "applied"
STATUS_CONSUMED = "consumed"


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def register_completion_commands(assignment_subcommands) -> None:
    """Register the six receipt-aware mark-done leaves under assignment."""
    assignment_mark_done_prepare = assignment_subcommands.add_parser(
        "mark-done-prepare",
        help=(
            "Validate the closeout/review gate on the control plane and issue "
            "a one-time completion.authorized receipt binding the host-aware "
            "mark-done files + record pair."
        ),
    )
    assignment_mark_done_prepare.add_argument("workspace_id")
    assignment_mark_done_prepare.add_argument("--task-id", required=True)
    assignment_mark_done_prepare.add_argument("--actor", default="operator",
        help="Requester of the receipt; also the authorized_actor unless --authorized-actor is set.")
    assignment_mark_done_prepare.add_argument("--authorized-actor", default=None,
        help="Explicit authorized actor if it differs from the requester.")
    assignment_mark_done_prepare.add_argument(
        "--ttl-seconds", type=int, default=None,
        help="Receipt validity window in seconds (default 6h).",
    )
    assignment_mark_done_prepare.set_defaults(handler=handle_assignment_mark_done_prepare)

    assignment_mark_done_preflight = assignment_subcommands.add_parser(
        "mark-done-preflight",
        help=(
            "Read-only: re-query a receipt from the control-plane DB and "
            "return its authoritative workspace/task/status/expiry. The "
            "coding host calls this through coord-ssh before mutating the "
            "canonical checklist so it never trusts its own claims."
        ),
    )
    assignment_mark_done_preflight.add_argument("receipt_id")
    assignment_mark_done_preflight.add_argument("--workspace-id")
    assignment_mark_done_preflight.add_argument("--task-id")
    assignment_mark_done_preflight.set_defaults(handler=handle_assignment_mark_done_preflight)

    assignment_mark_done_claim = assignment_subcommands.add_parser(
        "mark-done-claim",
        help=(
            "Atomically reserve a receipt authorized -> claimed on the control "
            "plane, recording before/expected-after fingerprints. Server-side "
            "sink invoked by the coding host through coord-ssh BEFORE the "
            "checklist mutation (two-phase reserve step)."
        ),
    )
    assignment_mark_done_claim.add_argument("receipt_id")
    assignment_mark_done_claim.add_argument("--workspace-id", required=True)
    assignment_mark_done_claim.add_argument("--task-id", required=True)
    assignment_mark_done_claim.add_argument("--actor", default="operator")
    assignment_mark_done_claim.add_argument("--before-fingerprint", required=True)
    assignment_mark_done_claim.add_argument("--after-fingerprint", required=True,
                                           dest="expected_after_fingerprint")
    assignment_mark_done_claim.set_defaults(handler=handle_assignment_mark_done_claim)

    assignment_mark_done_apply = assignment_subcommands.add_parser(
        "mark-done-apply",
        help=(
            "Acknowledge a claimed receipt -> applied on the control plane, "
            "recording the actual after-fingerprint. Server-side sink invoked "
            "by the coding host through coord-ssh AFTER the canonical checklist "
            "write lands (two-phase apply step)."
        ),
    )
    assignment_mark_done_apply.add_argument("receipt_id")
    assignment_mark_done_apply.add_argument("--workspace-id", required=True)
    assignment_mark_done_apply.add_argument("--task-id", required=True)
    assignment_mark_done_apply.add_argument("--actor", default="operator")
    assignment_mark_done_apply.add_argument("--after-fingerprint", required=True)
    assignment_mark_done_apply.set_defaults(handler=handle_assignment_mark_done_apply)

    assignment_mark_done_files = assignment_subcommands.add_parser(
        "mark-done-files",
        help=(
            "Coding-host half of host-aware mark-done. Writes local "
            "mvp-checklist.json only. Normal path requires --receipt and a "
            "remote coord CLI (--event-cli-path) to verify/claim the receipt "
            "online before any file mutation. --repair-reason selects the "
            "explicit repair-only path."
        ),
    )
    assignment_mark_done_files.add_argument("--workspace-path", required=True)
    assignment_mark_done_files.add_argument("--harness-root", required=True)
    assignment_mark_done_files.add_argument("--task-id", required=True)
    assignment_mark_done_files.add_argument("--workspace-id")
    assignment_mark_done_files.add_argument("--actor", default="operator")
    assignment_mark_done_files.add_argument("--verification")
    assignment_mark_done_files.add_argument("--receipt")
    assignment_mark_done_files.add_argument(
        "--event-cli-path",
        help=(
            "Path to a coord CLI that runs mark-done-preflight / "
            "mark-done-claim against the control-plane DB "
            "(e.g. <HOME>/.local/bin/coord-ssh). Required for the "
            "normal receipt path so the host verifies the receipt online "
            "before mutating files."
        ),
    )
    assignment_mark_done_files.add_argument("--repair-reason")
    assignment_mark_done_files.add_argument(
        "--allow-runtime-copy",
        action="store_true",
        help="Allow mutation of /opt deployment copy",
    )
    assignment_mark_done_files.set_defaults(handler=handle_assignment_mark_done_files)

    assignment_mark_done_record = assignment_subcommands.add_parser(
        "mark-done-record",
        help=(
            "Server half of host-aware mark-done. Writes the control-plane "
            "task.done event after re-verifying the receipt and the deployed "
            "harness. Normal path requires --receipt; --repair-reason "
            "selects the explicit repair-only path."
        ),
    )
    assignment_mark_done_record.add_argument("workspace_id")
    assignment_mark_done_record.add_argument("--task-id")
    assignment_mark_done_record.add_argument("--receipt")
    assignment_mark_done_record.add_argument("--actor", default="operator")
    assignment_mark_done_record.add_argument("--verification")
    assignment_mark_done_record.add_argument("--idempotency-hint")
    assignment_mark_done_record.add_argument("--repair-reason")
    assignment_mark_done_record.set_defaults(handler=handle_assignment_mark_done_record)


def handle_assignment_mark_done_files(args: argparse.Namespace) -> int:
    """Coding-host mark-done-files handler.

    Two paths:

    - **Normal (receipt)**: requires ``--receipt`` and ``--event-cli-path``.
      Before any canonical file mutation, forwards a read-only
      ``mark-done-preflight`` and then an atomic ``mark-done-claim`` to the
      control-plane coord CLI. The remote re-derives workspace/task/expiry
      from the ledger; the host never trusts its own claims.
    - **Repair-only**: requires non-empty ``--repair-reason``. Bypasses the
      receipt protocol and stamps ``repair_only=true`` plus the reason into
      the result for audit.

    No path mutates the file before the receipt is claimed. Without either a
    receipt+remote-CLI pair or an explicit repair reason, the command fails
    closed.
    """
    receipt_id = args.receipt
    repair_reason = (args.repair_reason or "").strip() or None

    if receipt_id and repair_reason:
        _print_json({"error": {
            "message": "specify either --receipt or --repair-reason, not both",
            "reason": "conflicting_mode",
        }})
        return 1

    if not receipt_id and not repair_reason:
        _print_json({"error": {
            "message": (
                "mark-done-files requires either --receipt (normal path) or "
                "--repair-reason (repair-only path)"
            ),
            "reason": "missing_authorization",
        }})
        return 1

    if receipt_id and not args.event_cli_path:
        _print_json({"error": {
            "message": (
                "normal mark-done-files path requires --event-cli-path to "
                "verify/claim the receipt online before file mutation"
            ),
            "reason": "no_remote_verification_path",
        }})
        return 1

    try:
        if receipt_id:
            result, claim = _run_mark_done_files_receipt(args, receipt_id)
            result_dict = result.to_dict()
            result_dict["claim"] = claim
        else:
            result = mark_done_files(
                workspace_path=args.workspace_path,
                harness_root=args.harness_root,
                task_id=args.task_id,
                actor=args.actor,
                verification=args.verification,
                allow_runtime_copy=args.allow_runtime_copy,
                repair_reason=repair_reason,
            )
            result_dict = result.to_dict()
    except (ValueError, CompletionReceiptError) as exc:
        _print_json({"error": {
            "message": str(exc),
            "reason": getattr(exc, "reason", "mark_done_files_failed"),
        }})
        return 1
    _print_json({"result": result_dict})
    return 0


def _run_mark_done_files_receipt(args: argparse.Namespace, receipt_id: str) -> Any:
    """Receipt-path orchestration (two-phase):

    preflight (remote read-only)
      -> reserve/claim (remote: authorized -> claimed, records before +
         deterministic expected-after)        [BEFORE any canonical write]
      -> local canonical write + verify       (writes completion_receipt metadata)
      -> apply ack (remote: claimed -> applied, records actual-after)
                                                 [AFTER the write lands]

    The canonical mutation runs ONLY between the reserve and the apply. If the
    host crashes after the write but before the apply ack, the ledger shows
    ``claimed`` (not ``applied``) — a diagnosable partial state. Retrying the
    whole files command converges idempotently: reserve (idempotent on matching
    expected-after), local write (idempotent no-op with matching metadata),
    apply (fresh or idempotent). The record side requires ``applied``; it will
    not consume a merely-``claimed`` receipt.
    """
    workspace_id = args.workspace_id
    task_id = args.task_id

    pre = _forward_mark_done_preflight(
        args.event_cli_path,
        receipt_id=receipt_id,
        workspace_id=workspace_id,
        task_id=task_id,
    )
    if pre.get("ok") is not True:
        raise CompletionReceiptError(
            f"remote preflight rejected receipt: {pre.get('message') or pre.get('reason')}",
            reason=pre.get("reason") or "preflight_failed",
        )
    authoritative_workspace = pre.get("workspace_id")
    authoritative_task = pre.get("task_id")
    if workspace_id and authoritative_workspace and workspace_id != authoritative_workspace:
        raise CompletionReceiptError(
            f"host workspace_id {workspace_id!r} does not match receipt "
            f"{authoritative_workspace!r}",
            reason="workspace_mismatch",
        )
    if task_id and authoritative_task and task_id != authoritative_task:
        raise CompletionReceiptError(
            f"host task_id {task_id!r} does not match receipt "
            f"{authoritative_task!r}",
            reason="task_mismatch",
        )
    claim_workspace = authoritative_workspace or workspace_id
    claim_task = authoritative_task or task_id
    if not (claim_workspace and claim_task):
        raise CompletionReceiptError(
            "preflight did not return workspace_id/task_id and the host "
            "supplied none",
            reason="invalid_preflight",
        )

    fps = compute_mark_done_fingerprints(
        harness_root=args.harness_root, task_id=claim_task,
    )

    # Reserve: authorized -> claimed, recording before + expected-after. The
    # server rejects if the local before does not match the receipt's
    # harness_fingerprint (binding the coding host's state to prepare time).
    claim = _forward_mark_done_claim(
        args.event_cli_path,
        receipt_id=receipt_id,
        workspace_id=claim_workspace,
        task_id=claim_task,
        actor=args.actor,
        before_fingerprint=fps.before_fingerprint,
        expected_after_fingerprint=fps.after_fingerprint,
    )

    # Canonical write, threaded with receipt evidence so the service layer
    # records structured completion_receipt metadata and cannot be invoked
    # without authorization. The evidence MUST be built from the
    # AUTHORITATIVE remote claim result (the server-confirmed
    # receipt_id / before_fingerprint / expected_after_fingerprint), not
    # from the untrusted local fps — otherwise a local drift between reserve
    # and write could re-authorize a state the receipt never bound. Fail
    # closed if the remote result omits any required field.
    evidence_receipt_id = claim.get("receipt_id")
    evidence_before = claim.get("before_fingerprint")
    evidence_after = claim.get("expected_after_fingerprint")
    if not (evidence_receipt_id and evidence_before and evidence_after):
        raise CompletionReceiptError(
            f"remote claim result missing required fields "
            f"(receipt_id={evidence_receipt_id!r}, "
            f"before_fingerprint={'set' if evidence_before else 'missing'}, "
            f"expected_after_fingerprint={'set' if evidence_after else 'missing'})",
            reason="invalid_claim_result",
        )
    evidence = ReceiptEvidence(
        receipt_id=evidence_receipt_id,
        before_fingerprint=evidence_before,
        after_fingerprint=evidence_after,
    )
    result = mark_done_files(
        workspace_path=args.workspace_path,
        harness_root=args.harness_root,
        task_id=claim_task,
        actor=args.actor,
        verification=args.verification,
        allow_runtime_copy=args.allow_runtime_copy,
        receipt=evidence,
    )
    if result.after_fingerprint != evidence_after:
        raise CompletionReceiptError(
            f"post-write fingerprint {result.after_fingerprint!r} does not "
            f"match claimed expected-after {evidence_after!r}",
            reason="post_write_fingerprint_mismatch",
        )

    # Apply ack: claimed -> applied. On failure the receipt stays claimed
    # (diagnosable partial); retrying files re-runs reserve (idempotent) and
    # re-runs apply (idempotent).
    apply = _forward_mark_done_apply(
        args.event_cli_path,
        receipt_id=receipt_id,
        workspace_id=claim_workspace,
        task_id=claim_task,
        actor=args.actor,
        after_fingerprint=fps.after_fingerprint,
    )
    return result, {"claim": claim, "apply": apply}


def handle_assignment_mark_done_record(args: argparse.Namespace) -> int:
    """Server-side mark-done-record handler.

    - **Normal (receipt)**: requires ``--receipt``. Re-queries the receipt,
      verifies the deployed harness is done/closed with a matching
      fingerprint, and atomically consumes the receipt while appending
      ``task.done``.
    - **Repair-only**: requires non-empty ``--repair-reason`` (and
      ``--task-id``); writes a plain ``task.done`` stamped
      ``repair_only=true`` with the reason, for drift reconciliation.
    """
    receipt_id = args.receipt
    repair_reason = (args.repair_reason or "").strip() or None

    if receipt_id and repair_reason:
        _print_json({"error": {
            "message": "specify either --receipt or --repair-reason, not both",
            "reason": "conflicting_mode",
        }})
        return 1

    if not receipt_id and not repair_reason:
        _print_json({"error": {
            "message": (
                "mark-done-record requires either --receipt (normal path) "
                "or --repair-reason (repair-only path)"
            ),
            "reason": "missing_authorization",
        }})
        return 1

    with _conn(args) as conn:
        try:
            if receipt_id:
                result = consume_completion_receipt(
                    conn,
                    receipt_id=receipt_id,
                    actor=args.actor,
                    verification=args.verification,
                )
                output_key = "result"
                output_val = result.to_dict()
            else:
                if not args.task_id:
                    raise CompletionReceiptError(
                        "repair path requires --task-id",
                        reason="missing_task_id",
                    )
                record_result = mark_done_record(
                    conn,
                    workspace_id=args.workspace_id,
                    task_id=args.task_id,
                    actor=args.actor,
                    verification=_stamp_repair_verification(args.verification, repair_reason),
                    idempotency_hint=args.idempotency_hint,
                    repair_reason=repair_reason,
                )
                output_key = "result"
                output_val = record_result.to_dict()
        except (ValueError, CompletionReceiptError) as exc:
            _print_json({"error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", "mark_done_record_failed"),
            }})
            return 1
    _print_json({output_key: output_val})
    return 0


def _stamp_repair_verification(verification: str | None, repair_reason: str | None) -> str:
    """Repair-path verification text carries the reason so audit/doctor
    readers can see why the receipt protocol was bypassed."""
    reason = (repair_reason or "").strip()
    base = verification or ""
    if not reason:
        return base
    stamp = f"[repair_only reason={reason}]"
    return f"{base} {stamp}".strip()


def handle_assignment_mark_done_prepare(args: argparse.Namespace) -> int:
    """Server-side: validate gate + evidence and issue a one-time receipt."""
    kwargs = {
        "workspace_id": args.workspace_id,
        "task_id": args.task_id,
        "requester": args.actor,
    }
    if args.authorized_actor:
        kwargs["authorized_actor"] = args.authorized_actor
    if args.ttl_seconds is not None:
        kwargs["ttl_seconds"] = args.ttl_seconds
    with _conn(args) as conn:
        try:
            receipt = prepare_completion_receipt(conn, **kwargs)
        except (ValueError, CompletionReceiptError) as exc:
            _print_json({"error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", "prepare_failed"),
            }})
            return 1
    _print_json({"result": receipt.to_dict()})
    return 0


def handle_assignment_mark_done_preflight(args: argparse.Namespace) -> int:
    """Read-only: return the authoritative receipt binding for the host."""
    with _conn(args) as conn:
        state = _lookup_receipt_for_preflight(conn, args.receipt_id)
    if state is None:
        _print_json({"result": {
            "ok": False, "reason": "unknown_receipt",
            "message": f"no completion.authorized event for receipt {args.receipt_id}",
        }})
        return 1
    if state.get("broken"):
        _print_json({"result": {
            "ok": False,
            "reason": state.get("reason", "receipt_chain_broken"),
            "workspace_id": state.get("workspace_id"),
            "task_id": state.get("task_id"),
            "message": state.get("message"),
        }})
        return 1
    workspace_id = state.get("workspace_id")
    task_id = state.get("task_id")
    expires_at = state.get("expires_at")
    status = state.get("status")
    # Expiry only invalidates an unused authorized receipt.  A claimed,
    # applied, or consumed chain is authoritative regardless of the original
    # authorization expiry window.
    expired = False
    if status == STATUS_AUTHORIZED and expires_at:
        try:
            expired = datetime.now(timezone.utc) > parse_iso_timestamp(expires_at)
        except ValueError:
            expired = True
    if args.workspace_id and workspace_id and args.workspace_id != workspace_id:
        _print_json({"result": {
            "ok": False, "reason": "workspace_mismatch",
            "workspace_id": workspace_id, "task_id": task_id,
        }})
        return 1
    if args.task_id and task_id and args.task_id != task_id:
        _print_json({"result": {
            "ok": False, "reason": "task_mismatch",
            "workspace_id": workspace_id, "task_id": task_id,
        }})
        return 1
    if expired:
        _print_json({"result": {
            "ok": False, "reason": "expired",
            "workspace_id": workspace_id, "task_id": task_id,
            "expires_at": expires_at,
        }})
        return 1
    _print_json({"result": {
        "ok": True,
        "receipt_id": args.receipt_id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "status": status,
        "issued_at": state.get("issued_at"),
        "expires_at": expires_at,
        "actor": state.get("actor"),
        "terminal_event_id": state.get("terminal_event_id"),
    }})
    return 0


def handle_assignment_mark_done_claim(args: argparse.Namespace) -> int:
    """Server-side sink: atomic authorized -> claimed reserve."""
    with _conn(args) as conn:
        try:
            result = claim_completion_receipt(
                conn,
                receipt_id=args.receipt_id,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                actor=args.actor,
                before_fingerprint=args.before_fingerprint,
                expected_after_fingerprint=args.expected_after_fingerprint,
            )
        except (ValueError, CompletionReceiptError) as exc:
            _print_json({"error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", "claim_failed"),
            }})
            return 1
    _print_json({"result": result.to_dict()})
    return 0


def handle_assignment_mark_done_apply(args: argparse.Namespace) -> int:
    """Server-side sink: claimed -> applied acknowledgement."""
    with _conn(args) as conn:
        try:
            result = apply_completion_receipt(
                conn,
                receipt_id=args.receipt_id,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                actor=args.actor,
                after_fingerprint=args.after_fingerprint,
            )
        except (ValueError, CompletionReceiptError) as exc:
            _print_json({"error": {
                "message": str(exc),
                "reason": getattr(exc, "reason", "apply_failed"),
            }})
            return 1
    _print_json({"result": result.to_dict()})
    return 0


def _lookup_receipt_for_preflight(conn, receipt_id: str) -> dict[str, Any] | None:
    """Derive the authoritative receipt state from its event chain.

    Precedence is ``consumed > applied > claimed > authorized``.  Partial,
    duplicate, or inconsistent chains fail closed.  All immutable links
    (workspace, task, actor, fingerprints, required task.done) are verified.
    """
    from .db import get_event, row_to_dict
    rows = find_events(
        conn,
        event_type=None,
        workspace_id=None,
        task_id=None,
        payload_key="receipt_id",
        payload_value=receipt_id,
    )
    events = [
        row_to_dict(row)
        for row in rows
        if (row_to_dict(row).get("event_type") or "").startswith("completion.")
    ]
    events.sort(key=lambda e: e.get("rowid", 0))

    if not events:
        return None

    if events[0].get("event_type") != "completion.authorized":
        return {
            "broken": True,
            "reason": "receipt_chain_incomplete",
            "message": f"receipt {receipt_id} chain does not start with completion.authorized",
        }

    status_order = ["authorized", "claimed", "applied", "consumed"]
    seen: set[str] = set()
    workspace_id: str | None = None
    task_id: str | None = None
    actor: str | None = None
    state: dict[str, Any] = {}
    authorized_harness_fingerprint: str | None = None
    claimed_expected_after: str | None = None
    applied_after: str | None = None

    for event in events:
        payload = event.get("payload") or {}
        ev_status = payload.get("status")
        if ev_status not in status_order:
            return {
                "broken": True,
                "reason": "receipt_chain_conflict",
                "message": f"unknown receipt status {ev_status!r} in chain",
            }
        idx = status_order.index(ev_status)
        required = set(status_order[:idx])
        if not required.issubset(seen):
            return {
                "broken": True,
                "reason": "receipt_chain_incomplete",
                "message": f"{ev_status} event missing required predecessors",
            }
        if ev_status in seen:
            return {
                "broken": True,
                "reason": "receipt_chain_conflict",
                "message": f"duplicate {ev_status} transition in chain",
            }
        seen.add(ev_status)

        ev_ws = payload.get("workspace_id")
        ev_task = payload.get("task_id")
        ev_actor = payload.get("authorized_actor") or payload.get("actor")
        if workspace_id is None:
            workspace_id = ev_ws
        if task_id is None:
            task_id = ev_task
        if actor is None:
            actor = ev_actor
        if ev_ws and ev_ws != workspace_id:
            return {
                "broken": True,
                "reason": "receipt_chain_conflict",
                "message": f"workspace_id mismatch in receipt chain: {ev_ws!r} != {workspace_id!r}",
            }
        if ev_task and ev_task != task_id:
            return {
                "broken": True,
                "reason": "receipt_chain_conflict",
                "message": f"task_id mismatch in receipt chain: {ev_task!r} != {task_id!r}",
            }
        if ev_actor and ev_actor != actor:
            return {
                "broken": True,
                "reason": "receipt_chain_conflict",
                "message": f"actor mismatch in receipt chain: {ev_actor!r} != {actor!r}",
            }

        if ev_status == STATUS_AUTHORIZED:
            fp = payload.get("harness_fingerprint")
            if not fp:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": "authorized event missing harness_fingerprint",
                }
            if not _is_canonical_sha256(fp):
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"authorized harness_fingerprint {fp!r} is not a canonical SHA-256",
                }
            authorized_harness_fingerprint = fp
        elif ev_status == STATUS_CLAIMED:
            before_fingerprint = payload.get("before_fingerprint")
            if not before_fingerprint:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": "claimed event missing before_fingerprint",
                }
            if not _is_canonical_sha256(before_fingerprint):
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"claimed before_fingerprint {before_fingerprint!r} is not a canonical SHA-256",
                }
            if before_fingerprint != authorized_harness_fingerprint:
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": (
                        f"claimed before_fingerprint {before_fingerprint!r} does not match "
                        f"authorized harness_fingerprint {authorized_harness_fingerprint!r}"
                    ),
                }
            expected_after = payload.get("expected_after_fingerprint")
            if not expected_after:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": "claimed event missing expected_after_fingerprint",
                }
            if not _is_canonical_sha256(expected_after):
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"claimed expected_after_fingerprint {expected_after!r} is not a canonical SHA-256",
                }
            claimed_expected_after = expected_after
        elif ev_status == STATUS_APPLIED:
            before_fp = payload.get("before_fingerprint")
            if not before_fp:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": "applied event missing before_fingerprint",
                }
            if not _is_canonical_sha256(before_fp):
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"applied before_fingerprint {before_fp!r} is not a canonical SHA-256",
                }
            if before_fp != authorized_harness_fingerprint:
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": (
                        f"applied before_fingerprint {before_fp!r} does not match "
                        f"claimed before_fingerprint {authorized_harness_fingerprint!r}"
                    ),
                }
            after_fp = payload.get("after_fingerprint")
            if not after_fp:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": "applied event missing after_fingerprint",
                }
            if not _is_canonical_sha256(after_fp):
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"applied after_fingerprint {after_fp!r} is not a canonical SHA-256",
                }
            if after_fp != claimed_expected_after:
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": (
                        f"applied after_fingerprint {after_fp!r} does not match "
                        f"claimed expected_after_fingerprint {claimed_expected_after!r}"
                    ),
                }
            applied_after = after_fp
        elif ev_status == STATUS_CONSUMED:
            task_done_event_id = payload.get("task_done_event_id")
            if not task_done_event_id:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": "consumed event missing task_done_event_id",
                }
            if not applied_after:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": "consumed event missing applied after_fingerprint predecessor",
                }
            try:
                task_done_row = get_event(conn, task_done_event_id)
            except KeyError:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": f"consumed event references missing task.done {task_done_event_id}",
                }
            if task_done_row["event_type"] != "task.done":
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": f"consumed event references non-task.done event {task_done_event_id}",
                }
            if (
                task_done_row["workspace_id"] != workspace_id
                or task_done_row["task_id"] != task_id
            ):
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"consumed event task.done {task_done_event_id} has wrong workspace/task",
                }
            task_done_payload = _json_loads(task_done_row["payload_json"])
            if task_done_payload.get("receipt_id") != receipt_id:
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"consumed event task.done {task_done_event_id} references another receipt",
                }
            applied_fingerprint = task_done_payload.get("applied_fingerprint")
            if not applied_fingerprint:
                return {
                    "broken": True,
                    "reason": "receipt_chain_incomplete",
                    "message": f"task.done {task_done_event_id} missing applied_fingerprint",
                }
            if not _is_canonical_sha256(applied_fingerprint):
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": f"task.done applied_fingerprint {applied_fingerprint!r} is not a canonical SHA-256",
                }
            if applied_fingerprint != applied_after:
                return {
                    "broken": True,
                    "reason": "receipt_chain_conflict",
                    "message": (
                        f"consumed task.done applied_fingerprint {applied_fingerprint!r} does not match "
                        f"applied after_fingerprint {applied_after!r}"
                    ),
                }

        state["status"] = ev_status
        if ev_status == "consumed":
            state["terminal_event_id"] = event.get("id")

    authorized_payload = events[0].get("payload") or {}
    return {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "actor": actor,
        "status": state.get("status", "authorized"),
        "issued_at": authorized_payload.get("issued_at"),
        "expires_at": authorized_payload.get("expires_at"),
        "terminal_event_id": state.get("terminal_event_id"),
    }


def _build_mark_done_event_cli_argv(event_cli_path: str, sub_args: list[str]) -> list[str]:
    """Build a subprocess argv for a coordinate coord CLI (mirrors pr_cli)."""
    if event_cli_path.lower().endswith(".py"):
        return [sys.executable, event_cli_path, *sub_args]
    return [event_cli_path, *sub_args]


def _forward_mark_done_preflight(
    event_cli_path: str,
    *,
    receipt_id: str,
    workspace_id: str | None,
    task_id: str | None,
) -> dict[str, Any]:
    argv = _build_mark_done_event_cli_argv(
        event_cli_path, ["assignment", "mark-done-preflight", receipt_id],
    )
    if workspace_id:
        argv += ["--workspace-id", workspace_id]
    if task_id:
        argv += ["--task-id", task_id]
    return _run_remote_cli_json(event_cli_path, argv, "mark-done-preflight")


def _forward_mark_done_claim(
    event_cli_path: str,
    *,
    receipt_id: str,
    workspace_id: str,
    task_id: str,
    actor: str,
    before_fingerprint: str,
    expected_after_fingerprint: str,
) -> dict[str, Any]:
    argv = _build_mark_done_event_cli_argv(
        event_cli_path,
        [
            "assignment", "mark-done-claim", receipt_id,
            "--workspace-id", workspace_id,
            "--task-id", task_id,
            "--actor", actor,
            "--before-fingerprint", before_fingerprint,
            "--after-fingerprint", expected_after_fingerprint,
        ],
    )
    return _run_remote_cli_json(event_cli_path, argv, "mark-done-claim")


def _forward_mark_done_apply(
    event_cli_path: str,
    *,
    receipt_id: str,
    workspace_id: str,
    task_id: str,
    actor: str,
    after_fingerprint: str,
) -> dict[str, Any]:
    argv = _build_mark_done_event_cli_argv(
        event_cli_path,
        [
            "assignment", "mark-done-apply", receipt_id,
            "--workspace-id", workspace_id,
            "--task-id", task_id,
            "--actor", actor,
            "--after-fingerprint", after_fingerprint,
        ],
    )
    return _run_remote_cli_json(event_cli_path, argv, "mark-done-apply")


def _run_remote_cli_json(
    event_cli_path: str, argv: list[str], op: str,
) -> dict[str, Any]:
    """Run a remote coord CLI subcommand and parse its JSON envelope.

    Returns the inner ``result`` object on success. Raises
    ``CompletionReceiptError`` with a machine-readable ``reason`` on any
    non-zero exit, empty stdout, or invalid JSON.
    """
    completed = subprocess.run(  # noqa: S603 - controlled argv list
        argv, capture_output=True, text=True, encoding="utf-8",
    )
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    if completed.returncode != 0:
        reason = f"{op}_failed"
        message = stderr or completed.returncode
        if stdout:
            try:
                parsed = json.loads(stdout)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                if isinstance(parsed.get("error"), dict):
                    err = parsed["error"]
                    reason = err.get("reason") or reason
                    if err.get("message"):
                        message = err["message"]
                elif isinstance(parsed.get("result"), dict):
                    payload = parsed["result"]
                    if payload.get("ok") is False:
                        reason = payload.get("reason") or reason
                        message = payload.get("message") or message
        raise CompletionReceiptError(
            f"remote {op} failed: {message}", reason=reason,
        )
    if not stdout:
        raise CompletionReceiptError(
            f"remote {op} returned empty stdout",
            reason=f"{op}_invalid_json",
        )
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise CompletionReceiptError(
            f"remote {op} returned invalid JSON: {exc}",
            reason=f"{op}_invalid_json",
        ) from exc
    if isinstance(parsed, dict) and isinstance(parsed.get("result"), dict):
        return parsed["result"]
    raise CompletionReceiptError(
        f"remote {op} JSON missing 'result' object",
        reason=f"{op}_invalid_json",
    )
