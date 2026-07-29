"""Slice 3: completion authorization receipt protocol.

This module owns the receipt lifecycle that binds a coding host's canonical
``mvp-checklist.json`` mutation to the control-plane ``task.done`` terminal
under one server-issued, one-time authorization. It also owns the gate check
and the lifecycle fingerprint helpers shared with ``transitions``.

Authority and state machine
---------------------------

The receipt lives entirely in the control-plane event ledger:

    completion.authorized   (prepare; server-issued, one-time)
        |
        v
    completion.claimed      (file-side reserve, BEFORE any canonical write;
        |                    records before + deterministic expected-after)
        v
    completion.applied      (file-side ack, AFTER the canonical write lands;
        |                    records actual-after)
        v
    task.done + completion.consumed   (record side; verifies the deployed
                                       harness, then consumes atomically)

The server re-derives workspace/task/expiry/actor/fingerprints from the
ledger on every claim/apply/consume; nothing the client sends is trusted.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .db import append_event, find_events, get_workspace, latest_event, row_to_dict
from .harness import HarnessAdapter, HarnessError


# --------------------------------------------------------------------------
# Gate (shared with transitions.mark_done_task)
# --------------------------------------------------------------------------

ALLOWED_MARK_DONE_STATUSES: frozenset[str] = frozenset({"review_approved", "closed"})


@dataclass(frozen=True)
class MarkDoneGateResult:
    passed: bool
    reason: str | None
    task_status: str | None


def _evaluate_gate_status(item: dict, task_id: str) -> MarkDoneGateResult:
    workflow = item.get("workflow") if isinstance(item.get("workflow"), dict) else {}
    wf_status = workflow.get("status")
    coarse_status = item.get("status")
    if wf_status is not None:
        if wf_status in ALLOWED_MARK_DONE_STATUSES:
            return MarkDoneGateResult(passed=True, reason=None, task_status=wf_status)
        return MarkDoneGateResult(
            passed=False,
            reason=(
                f"workflow status is {wf_status}, "
                f"expected one of {sorted(ALLOWED_MARK_DONE_STATUSES)}"
            ),
            task_status=wf_status,
        )
    if coarse_status == "done":
        return MarkDoneGateResult(passed=True, reason=None, task_status=coarse_status)
    display_status = coarse_status or "unknown"
    return MarkDoneGateResult(
        passed=False,
        reason=f"coarse status is {display_status}, expected 'done'",
        task_status=coarse_status,
    )


def check_mark_done_gate(adapter, task_id: str) -> MarkDoneGateResult:
    """Refresh harness state and return whether the task is clear to complete."""
    try:
        state = adapter.refresh_state()
    except (HarnessError, OSError) as exc:
        return MarkDoneGateResult(
            passed=False,
            reason=f"harness state unavailable: {exc}",
            task_status=None,
        )
    current = state.get("current_item")
    if isinstance(current, dict) and current.get("id") == task_id:
        return _evaluate_gate_status(current, task_id)
    try:
        checklist = adapter.read_checklist()
    except HarnessError as exc:
        return MarkDoneGateResult(
            passed=False,
            reason=f"checklist unavailable: {exc}",
            task_status=None,
        )
    items = checklist.get("items") if isinstance(checklist, dict) else None
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict) and item.get("id") == task_id:
                return _evaluate_gate_status(item, task_id)
    return MarkDoneGateResult(
        passed=False, reason="task not found in harness state", task_status=None,
    )


# --------------------------------------------------------------------------
# Lifecycle fingerprints
# --------------------------------------------------------------------------


def _fingerprint_fields(item: dict[str, Any]) -> dict[str, Any]:
    """Project a checklist item to the lifecycle fields that bind the receipt.

    Free-text ``verification`` AND structured ``completion_receipt`` metadata
    are both excluded: verification is descriptive only, and the metadata is
    the receipt's own bookkeeping (admitting it would let metadata author the
    fingerprint it is supposed to be bound by)."""
    workflow = item.get("workflow") if isinstance(item.get("workflow"), dict) else {}
    return {
        "id": item.get("id"),
        "status": item.get("status"),
        "workflow": {"status": workflow.get("status"), "branch": workflow.get("branch")},
    }


def compute_item_fingerprint(item: dict[str, Any]) -> str:
    canonical = json.dumps(
        _fingerprint_fields(item),
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _would_be_done_item(item: dict[str, Any]) -> dict[str, Any]:
    workflow = item.get("workflow") if isinstance(item.get("workflow"), dict) else {}
    projected = dict(item)
    projected["status"] = "done"
    projected_wv = dict(workflow)
    projected_wv["status"] = "closed"
    projected["workflow"] = projected_wv
    return projected


@dataclass(frozen=True)
class MarkDoneFingerprints:
    before_fingerprint: str
    after_fingerprint: str
    task_id: str


def compute_mark_done_fingerprints(*, harness_root: str, task_id: str) -> MarkDoneFingerprints:
    """Read-only (before, expected-after) for a task.

    ``before`` is the current item fingerprint; ``after`` is the deterministic
    fingerprint the item will have once mark-done writes status=done /
    workflow.status=closed. The file side computes this before mutating so the
    reserve can record the expected-after ahead of the write.
    """
    item = _read_checklist_item(harness_root, task_id)
    return MarkDoneFingerprints(
        before_fingerprint=compute_item_fingerprint(item),
        after_fingerprint=compute_item_fingerprint(_would_be_done_item(item)),
        task_id=task_id,
    )


def read_checklist_item(harness_root: str, task_id: str) -> dict[str, Any]:
    """Public read-only accessor for a single canonical checklist item."""
    return _read_checklist_item(harness_root, task_id)


def _read_checklist_item(harness_root: str, task_id: str) -> dict[str, Any]:
    checklist_path = Path(harness_root) / "mvp-checklist.json"
    if not checklist_path.is_file():
        raise CompletionReceiptError(
            f"mvp-checklist.json not found at {checklist_path}",
            reason="harness_fingerprint_unavailable",
        )
    try:
        checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CompletionReceiptError(
            f"mvp-checklist.json at {checklist_path} cannot be read: {exc}",
            reason="harness_fingerprint_unavailable",
        ) from exc
    items = checklist.get("items") if isinstance(checklist, dict) else None
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict) and item.get("id") == task_id:
                return item
    raise CompletionReceiptError(
        f"task {task_id} not found in mvp-checklist.json at {checklist_path}",
        reason="harness_item_missing",
    )


# --------------------------------------------------------------------------
# Error + result types
# --------------------------------------------------------------------------


class CompletionReceiptError(ValueError):
    """Receipt claim/apply/consume/prepare rejection. ``reason`` is a
    machine-readable short code surfaced in CLI JSON."""

    def __init__(self, message: str, *, reason: str):
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class ReceiptEvidence:
    """Authorization evidence threaded from the reserve (claim) result into
    ``transitions.mark_done_files`` so the canonical write carries receipt
    binding and the service layer cannot be invoked without it."""

    receipt_id: str
    before_fingerprint: str
    after_fingerprint: str


@dataclass(frozen=True)
class CompletionReceipt:
    receipt_id: str
    workspace_id: str
    task_id: str
    requester: str
    authorized_actor: str
    status: str
    issued_at: str
    expires_at: str
    nonce: str
    gate: dict[str, Any]
    review_evidence: dict[str, Any]
    forge_evidence: dict[str, Any]
    harness_fingerprint: str
    event: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "requester": self.requester,
            "authorized_actor": self.authorized_actor,
            "status": self.status,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "nonce": self.nonce,
            "gate": self.gate,
            "review_evidence": self.review_evidence,
            "forge_evidence": self.forge_evidence,
            "harness_fingerprint": self.harness_fingerprint,
            "event": self.event,
        }


@dataclass(frozen=True)
class CompletionClaimResult:
    receipt_id: str
    workspace_id: str
    task_id: str
    authorized_actor: str
    status: str
    before_fingerprint: str
    expected_after_fingerprint: str
    claimed_at: str
    event: dict[str, Any]
    idempotent: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "authorized_actor": self.authorized_actor,
            "status": self.status,
            "before_fingerprint": self.before_fingerprint,
            "expected_after_fingerprint": self.expected_after_fingerprint,
            "claimed_at": self.claimed_at,
            "event": self.event,
            "idempotent": self.idempotent,
        }

    def as_evidence(self) -> ReceiptEvidence:
        return ReceiptEvidence(
            receipt_id=self.receipt_id,
            before_fingerprint=self.before_fingerprint,
            after_fingerprint=self.expected_after_fingerprint,
        )


@dataclass(frozen=True)
class CompletionApplyResult:
    receipt_id: str
    workspace_id: str
    task_id: str
    authorized_actor: str
    status: str
    before_fingerprint: str
    after_fingerprint: str
    applied_at: str
    event: dict[str, Any]
    idempotent: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "authorized_actor": self.authorized_actor,
            "status": self.status,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "applied_at": self.applied_at,
            "event": self.event,
            "idempotent": self.idempotent,
        }


@dataclass(frozen=True)
class CompletionConsumeResult:
    receipt_id: str
    workspace_id: str
    task_id: str
    authorized_actor: str
    event: dict[str, Any]
    event_created: bool
    deployed_verification: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "authorized_actor": self.authorized_actor,
            "event": self.event,
            "event_created": self.event_created,
            "deployed_verification": self.deployed_verification,
        }


# --------------------------------------------------------------------------
# Status constants + timestamp helper
# --------------------------------------------------------------------------

STATUS_AUTHORIZED = "authorized"
STATUS_CLAIMED = "claimed"
STATUS_APPLIED = "applied"
STATUS_CONSUMED = "consumed"
DEFAULT_RECEIPT_TTL_SECONDS = 6 * 3600


def parse_iso_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (Z or offset) to an aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# --------------------------------------------------------------------------
# Event-ledger lookups (single source of truth for receipt state)
# --------------------------------------------------------------------------


def _event_by_receipt(conn, event_type: str, receipt_id: str) -> dict[str, Any] | None:
    row = latest_event(
        conn, event_type=event_type,
        payload_key="receipt_id", payload_value=receipt_id,
    )
    return row_to_dict(row) if row is not None else None


# --------------------------------------------------------------------------
# Evidence gathering (review + forge) — honest, never fabricated
# --------------------------------------------------------------------------


def _gather_review_evidence(conn, workspace_id, task_id) -> dict[str, Any]:
    row = latest_event(
        conn, event_type="review.completed",
        workspace_id=workspace_id, task_id=task_id,
    )
    if row is None:
        return {"not_applicable": True, "reason": "no review.completed event for task"}
    d = row_to_dict(row)
    payload = d.get("payload") or {}
    return {
        "event_id": d.get("id"),
        "decision": payload.get("decision"),
        "reviewer": payload.get("reviewer"),
        "summary": payload.get("summary"),
    }


def _latest_ci_event(conn, workspace_id, task_id) -> dict[str, Any] | None:
    rows = find_events(conn, workspace_id=workspace_id, task_id=task_id)
    ci_rows = [r for r in rows if row_to_dict(r).get("event_type", "").startswith("ci.")]
    if not ci_rows:
        return None
    return row_to_dict(ci_rows[-1])


def _gather_forge_evidence(conn, workspace_id, task_id) -> dict[str, Any]:
    """Honest forge evidence. Returns ``not_applicable`` only when the project
    exposes no ci.* evidence at all. Caller enforces the pass/fail gate."""
    latest = _latest_ci_event(conn, workspace_id, task_id)
    if latest is None:
        return {"not_applicable": True,
                "reason": "no forge gate configured for completion path"}
    event_type = latest.get("event_type")
    return {
        "event_id": latest.get("id"),
        "event_type": event_type,
        "status": "passed" if event_type == "ci.passed" else event_type.split(".", 1)[-1],
    }


# --------------------------------------------------------------------------
# prepare
# --------------------------------------------------------------------------


def prepare_completion_receipt(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    requester: str = "operator",
    authorized_actor: str | None = None,
    adapter: HarnessAdapter | None = None,
    ttl_seconds: int = DEFAULT_RECEIPT_TTL_SECONDS,
) -> CompletionReceipt:
    """Validate the closeout/review/forge gate and issue a one-time receipt.

    Fail-closed policy:
    - workspace must exist;
    - gate (workflow status) must currently pass;
    - the canonical checklist item must be readable so its lifecycle
      fingerprint can bind the later claim (no silent ``None``);
    - if any ci.* evidence exists, the latest must be ``ci.passed``;
    - ``authorized_actor`` defaults to ``requester``; an explicit split is
      written verbatim, never inferred.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise CompletionReceiptError(
            f"unknown workspace: {workspace_id}", reason="unknown_workspace",
        )
    if adapter is None:
        adapter = HarnessAdapter(workspace)

    gate = check_mark_done_gate(adapter, task_id)
    if not gate.passed:
        raise CompletionReceiptError(
            f"completion gate not passed for task {task_id}: {gate.reason}",
            reason="gate_not_passed",
        )

    # Bind the receipt to the canonical checklist item's lifecycle fingerprint.
    # Read from the same authoritative adapter the gate used; do not silently
    # degrade to None.
    try:
        checklist = adapter.read_checklist()
    except (HarnessError, OSError) as exc:
        raise CompletionReceiptError(
            f"cannot read canonical checklist for fingerprint: {exc}",
            reason="harness_fingerprint_unavailable",
        ) from exc
    items = checklist.get("items") if isinstance(checklist, dict) else None
    item = None
    if isinstance(items, list):
        for candidate in items:
            if isinstance(candidate, dict) and candidate.get("id") == task_id:
                item = candidate
                break
    if item is None:
        raise CompletionReceiptError(
            f"task {task_id} not found in canonical checklist; cannot bind fingerprint",
            reason="harness_item_missing",
        )
    harness_fingerprint = compute_item_fingerprint(item)

    # Forge gate: only ``not_applicable`` when there is no ci.* evidence.
    forge_evidence = _gather_forge_evidence(conn, workspace_id, task_id)
    if not forge_evidence.get("not_applicable"):
        if forge_evidence.get("status") != "passed":
            raise CompletionReceiptError(
                f"forge gate not satisfied: latest ci evidence is "
                f"{forge_evidence.get('event_type')!r}, expected ci.passed",
                reason="forge_gate_failed",
            )

    review_evidence = _gather_review_evidence(conn, workspace_id, task_id)

    authorized = authorized_actor if authorized_actor is not None else requester
    issued_at = datetime.now(timezone.utc)
    expires_at = issued_at + timedelta(seconds=ttl_seconds)
    receipt_id = str(uuid.uuid4())
    nonce = str(uuid.uuid4())

    payload = {
        "receipt_id": receipt_id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "requester": requester,
        "authorized_actor": authorized,
        "nonce": nonce,
        "issued_at": _utc_stamp_from(issued_at),
        "expires_at": _utc_stamp_from(expires_at),
        "status": STATUS_AUTHORIZED,
        "gate": {
            "passed": gate.passed, "task_status": gate.task_status, "reason": gate.reason,
        },
        "review_evidence": review_evidence,
        "forge_evidence": forge_evidence,
        "harness_fingerprint": harness_fingerprint,
    }
    event_result = append_event(
        conn,
        event_type="completion.authorized",
        actor=requester,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=f"receipt:{receipt_id}:authorized",
        payload=payload,
    )
    event_dict = row_to_dict(event_result.row)
    return CompletionReceipt(
        receipt_id=receipt_id,
        workspace_id=workspace_id,
        task_id=task_id,
        requester=requester,
        authorized_actor=authorized,
        status=STATUS_AUTHORIZED,
        issued_at=payload["issued_at"],
        expires_at=payload["expires_at"],
        nonce=nonce,
        gate=payload["gate"],
        review_evidence=review_evidence,
        forge_evidence=forge_evidence,
        harness_fingerprint=harness_fingerprint,
        event=event_dict,
    )


def _utc_stamp_from(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


# --------------------------------------------------------------------------
# Shared claim/apply/consume validation
# --------------------------------------------------------------------------


def _receipt_payload(conn, receipt_id: str) -> dict[str, Any]:
    row = _event_by_receipt(conn, "completion.authorized", receipt_id)
    if row is None:
        raise CompletionReceiptError(
            f"unknown receipt: {receipt_id}", reason="unknown_receipt",
        )
    return row.get("payload") or {}


def _validate_binding(payload, *, workspace_id, task_id, actor) -> None:
    if payload.get("workspace_id") != workspace_id:
        raise CompletionReceiptError(
            f"receipt workspace {payload.get('workspace_id')!r} does not match "
            f"{workspace_id!r}",
            reason="workspace_mismatch",
        )
    if payload.get("task_id") != task_id:
        raise CompletionReceiptError(
            f"receipt task {payload.get('task_id')!r} does not match {task_id!r}",
            reason="task_mismatch",
        )
    if payload.get("authorized_actor") != actor:
        raise CompletionReceiptError(
            f"actor {actor!r} does not match receipt authorized_actor "
            f"{payload.get('authorized_actor')!r}",
            reason="actor_mismatch",
        )


def _validate_expiry(payload) -> None:
    expires_at = payload.get("expires_at")
    if not expires_at:
        return
    try:
        expired = datetime.now(timezone.utc) > parse_iso_timestamp(expires_at)
    except ValueError:
        raise CompletionReceiptError(
            f"receipt has malformed expires_at: {expires_at}",
            reason="malformed_expiry",
        )
    if expired:
        raise CompletionReceiptError(
            f"receipt expired at {expires_at}", reason="expired",
        )


def _validate_idempotent_before(receipt_id, before_fingerprint, *, allowed) -> None:
    """On an idempotent claim retry, the before_fingerprint the caller reports
    must be one of the recorded lifecycle fingerprints for this receipt —
    either the original reserved before (the write has not happened yet) or
    the after/expected-after fingerprint (the local write already landed and
    the host is about to ack apply). Any third value means the local
    checklist drifted between reserve and apply; accepting it would let the
    host authorize a mutation of a state the receipt never bound."""
    if before_fingerprint not in allowed:
        raise CompletionReceiptError(
            f"receipt {receipt_id} retry before_fingerprint "
            f"{before_fingerprint!r} does not match an allowed prior-claim "
            f"lifecycle fingerprint (expected one of {sorted(x for x in allowed if x)}); "
            f"possible drift between reserve and apply",
            reason="before_fingerprint_mismatch",
        )


# --------------------------------------------------------------------------
# claim (reserve): authorized -> claimed, BEFORE the canonical write
# --------------------------------------------------------------------------


def claim_completion_receipt(
    conn: sqlite3.Connection,
    *,
    receipt_id: str,
    workspace_id: str,
    task_id: str,
    actor: str,
    before_fingerprint: str,
    expected_after_fingerprint: str,
) -> CompletionClaimResult:
    payload = _receipt_payload(conn, receipt_id)
    _validate_binding(payload, workspace_id=workspace_id, task_id=task_id, actor=actor)
    _validate_expiry(payload)

    if _event_by_receipt(conn, "completion.consumed", receipt_id) is not None:
        raise CompletionReceiptError(
            f"receipt {receipt_id} already consumed", reason="already_consumed",
        )

    prior = _event_by_receipt(conn, "completion.applied", receipt_id)
    if prior is not None:
        # Fully applied already — idempotent only if BOTH the expected-after
        # AND the retry before_fingerprint are consistent with the recorded
        # lifecycle. The retry before may legitimately be either the original
        # before (reserve replayed before any write) or the after-fingerprint
        # (local write already landed). Any third drift value — even with the
        # same expected-after — is an authorization drift and is rejected.
        prior_payload = prior.get("payload") or {}
        if prior_payload.get("after_fingerprint") != expected_after_fingerprint:
            raise CompletionReceiptError(
                f"receipt {receipt_id} already applied with a different after-fingerprint",
                reason="fingerprint_mismatch",
            )
        _validate_idempotent_before(
            receipt_id, before_fingerprint,
            allowed={prior_payload.get("before_fingerprint"),
                     prior_payload.get("after_fingerprint")},
        )
        return _claim_result_from(prior, idempotent=True,
                                  expected_after=expected_after_fingerprint)

    prior_claimed = _event_by_receipt(conn, "completion.claimed", receipt_id)
    if prior_claimed is not None:
        prior_payload = prior_claimed.get("payload") or {}
        if prior_payload.get("expected_after_fingerprint") != expected_after_fingerprint:
            raise CompletionReceiptError(
                f"receipt {receipt_id} already claimed with a different expected-after",
                reason="fingerprint_mismatch",
            )
        # Same before-allow-list as the prior-applied branch: the retry
        # before must be the original claimed before (write not yet done)
        # or the expected-after (write done, about to ack apply). A drifted
        # before with matching expected-after is rejected.
        _validate_idempotent_before(
            receipt_id, before_fingerprint,
            allowed={prior_payload.get("before_fingerprint"),
                     prior_payload.get("expected_after_fingerprint")},
        )
        return _claim_result_from(prior_claimed, idempotent=True,
                                  expected_after=expected_after_fingerprint)

    # Fresh claim: the local before MUST match the authoritative fingerprint
    # the server recorded at prepare time. This binds the coding host's
    # checklist state to the control-plane view.
    if before_fingerprint != payload.get("harness_fingerprint"):
        raise CompletionReceiptError(
            f"before_fingerprint {before_fingerprint!r} does not match receipt "
            f"harness_fingerprint {payload.get('harness_fingerprint')!r}",
            reason="before_fingerprint_mismatch",
        )

    claimed_at = _utc_stamp()
    claim_payload = {
        "receipt_id": receipt_id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "authorized_actor": actor,
        "before_fingerprint": before_fingerprint,
        "expected_after_fingerprint": expected_after_fingerprint,
        "claimed_at": claimed_at,
        "status": STATUS_CLAIMED,
    }
    event_result = append_event(
        conn,
        event_type="completion.claimed",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=f"receipt:{receipt_id}:claimed",
        payload=claim_payload,
    )
    return CompletionClaimResult(
        receipt_id=receipt_id,
        workspace_id=workspace_id,
        task_id=task_id,
        authorized_actor=actor,
        status=STATUS_CLAIMED,
        before_fingerprint=before_fingerprint,
        expected_after_fingerprint=expected_after_fingerprint,
        claimed_at=claimed_at,
        event=row_to_dict(event_result.row),
        idempotent=False,
    )


def _claim_result_from(event_dict, *, idempotent, expected_after) -> CompletionClaimResult:
    payload = event_dict.get("payload") or {}
    # applied events store after_fingerprint; claimed events store expected_after.
    after = payload.get("expected_after_fingerprint") or payload.get("after_fingerprint")
    return CompletionClaimResult(
        receipt_id=payload.get("receipt_id"),
        workspace_id=payload.get("workspace_id"),
        task_id=payload.get("task_id"),
        authorized_actor=payload.get("authorized_actor"),
        status=payload.get("status"),
        before_fingerprint=payload.get("before_fingerprint"),
        expected_after_fingerprint=after or expected_after,
        claimed_at=payload.get("claimed_at") or payload.get("applied_at"),
        event=event_dict,
        idempotent=idempotent,
    )


# --------------------------------------------------------------------------
# apply (ack): claimed -> applied, AFTER the canonical write
# --------------------------------------------------------------------------


def apply_completion_receipt(
    conn: sqlite3.Connection,
    *,
    receipt_id: str,
    workspace_id: str,
    task_id: str,
    actor: str,
    after_fingerprint: str,
) -> CompletionApplyResult:
    payload = _receipt_payload(conn, receipt_id)
    _validate_binding(payload, workspace_id=workspace_id, task_id=task_id, actor=actor)
    _validate_expiry(payload)

    if _event_by_receipt(conn, "completion.consumed", receipt_id) is not None:
        raise CompletionReceiptError(
            f"receipt {receipt_id} already consumed", reason="already_consumed",
        )

    prior_applied = _event_by_receipt(conn, "completion.applied", receipt_id)
    if prior_applied is not None:
        prior_payload = prior_applied.get("payload") or {}
        if prior_payload.get("after_fingerprint") != after_fingerprint:
            raise CompletionReceiptError(
                f"receipt {receipt_id} already applied with a different after-fingerprint",
                reason="fingerprint_mismatch",
            )
        return _apply_result_from(prior_applied, idempotent=True)

    claimed = _event_by_receipt(conn, "completion.claimed", receipt_id)
    if claimed is None:
        raise CompletionReceiptError(
            f"receipt {receipt_id} has no completion.claimed event; reserve before apply",
            reason="not_claimed",
        )
    claimed_payload = claimed.get("payload") or {}
    if claimed_payload.get("expected_after_fingerprint") != after_fingerprint:
        raise CompletionReceiptError(
            f"after_fingerprint {after_fingerprint!r} does not match claimed "
            f"expected-after {claimed_payload.get('expected_after_fingerprint')!r}",
            reason="after_fingerprint_mismatch",
        )

    applied_at = _utc_stamp()
    apply_payload = {
        "receipt_id": receipt_id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "authorized_actor": actor,
        "before_fingerprint": claimed_payload.get("before_fingerprint"),
        "after_fingerprint": after_fingerprint,
        "applied_at": applied_at,
        "status": STATUS_APPLIED,
    }
    event_result = append_event(
        conn,
        event_type="completion.applied",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=f"receipt:{receipt_id}:applied",
        payload=apply_payload,
    )
    return CompletionApplyResult(
        receipt_id=receipt_id,
        workspace_id=workspace_id,
        task_id=task_id,
        authorized_actor=actor,
        status=STATUS_APPLIED,
        before_fingerprint=claimed_payload.get("before_fingerprint"),
        after_fingerprint=after_fingerprint,
        applied_at=applied_at,
        event=row_to_dict(event_result.row),
        idempotent=False,
    )


def _apply_result_from(event_dict, *, idempotent) -> CompletionApplyResult:
    payload = event_dict.get("payload") or {}
    return CompletionApplyResult(
        receipt_id=payload.get("receipt_id"),
        workspace_id=payload.get("workspace_id"),
        task_id=payload.get("task_id"),
        authorized_actor=payload.get("authorized_actor"),
        status=payload.get("status"),
        before_fingerprint=payload.get("before_fingerprint"),
        after_fingerprint=payload.get("after_fingerprint"),
        applied_at=payload.get("applied_at"),
        event=event_dict,
        idempotent=idempotent,
    )


# --------------------------------------------------------------------------
# consume: applied -> task.done + consumed, after deployed harness verification
# --------------------------------------------------------------------------


def consume_completion_receipt(
    conn: sqlite3.Connection,
    *,
    receipt_id: str,
    actor: str,
    deployed_adapter: HarnessAdapter | None = None,
    verification: str | None = None,
) -> CompletionConsumeResult:
    payload = _receipt_payload(conn, receipt_id)
    workspace_id = payload.get("workspace_id")
    task_id = payload.get("task_id")
    _validate_binding(
        payload, workspace_id=workspace_id, task_id=task_id, actor=actor,
    )
    _validate_expiry(payload)  # P1-4: re-check expiry before terminal write

    consumed = _event_by_receipt(conn, "completion.consumed", receipt_id)
    if consumed is not None:
        prior_done = _event_by_receipt(conn, "task.done", receipt_id)
        if prior_done is None:
            raise CompletionReceiptError(
                f"receipt {receipt_id} marked consumed but task.done missing",
                reason="consumed_without_task_done",
            )
        return CompletionConsumeResult(
            receipt_id=receipt_id,
            workspace_id=workspace_id,
            task_id=task_id,
            authorized_actor=actor,
            event=prior_done,
            event_created=False,
            deployed_verification={
                "idempotent": True,
                "fingerprint": (prior_done.get("payload") or {}).get("applied_fingerprint"),
            },
        )

    applied = _event_by_receipt(conn, "completion.applied", receipt_id)
    if applied is None:
        raise CompletionReceiptError(
            f"receipt {receipt_id} has no completion.applied event; the file side "
            f"must acknowledge before record can consume",
            reason="not_applied",
        )
    applied_payload = applied.get("payload") or {}
    after_fingerprint = applied_payload.get("after_fingerprint")

    # Defence in depth: no second terminal under a different authority.
    for prior_row in find_events(
        conn, event_type="task.done", workspace_id=workspace_id, task_id=task_id,
    ):
        prior = row_to_dict(prior_row)
        prior_payload = prior.get("payload") or {}
        if prior_payload.get("receipt_id") == receipt_id:
            continue
        raise CompletionReceiptError(
            f"task {task_id} already has task.done {prior.get('id')} under a "
            f"different authority; use the repair path to reconcile",
            reason="task_already_done_other_authority",
        )

    if deployed_adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise CompletionReceiptError(
                f"unknown workspace: {workspace_id}", reason="unknown_workspace",
            )
        deployed_adapter = HarnessAdapter(workspace)

    deployed_item = _deployed_task_item(deployed_adapter, task_id)
    workflow = deployed_item.get("workflow") if isinstance(deployed_item.get("workflow"), dict) else {}
    if deployed_item.get("status") != "done" or workflow.get("status") != "closed":
        raise CompletionReceiptError(
            f"deployed harness for task {task_id} is status="
            f"{deployed_item.get('status')!r}/workflow={workflow.get('status')!r}, "
            f"expected done/closed",
            reason="deployed_not_done",
        )
    deployed_fingerprint = compute_item_fingerprint(deployed_item)
    if not after_fingerprint or deployed_fingerprint != after_fingerprint:
        raise CompletionReceiptError(
            f"deployed fingerprint {deployed_fingerprint!r} does not match applied "
            f"after-fingerprint {after_fingerprint!r}",
            reason="fingerprint_mismatch",
        )

    deployed_verification = {
        "fingerprint": deployed_fingerprint,
        "status": deployed_item.get("status"),
        "workflow_status": workflow.get("status"),
    }
    task_done_payload = {
        "task_id": task_id,
        "receipt_id": receipt_id,
        "host_aware": "receipt",
        "applied_fingerprint": after_fingerprint,
        "deployed_verification": deployed_verification,
        "review_evidence": payload.get("review_evidence"),
        "forge_evidence": payload.get("forge_evidence"),
    }
    if verification:
        task_done_payload["verification"] = verification

    consumed_at = _utc_stamp()
    try:
        conn.execute("SAVEPOINT consume_receipt")
        done_result = append_event(
            conn,
            event_type="task.done",
            actor=actor,
            workspace_id=workspace_id,
            target=task_id,
            task_id=task_id,
            idempotency_key=f"receipt:{receipt_id}:task.done",
            payload=task_done_payload,
            commit=False,
        )
        consumed_payload = {
            "receipt_id": receipt_id,
            "workspace_id": workspace_id,
            "task_id": task_id,
            "authorized_actor": actor,
            "task_done_event_id": row_to_dict(done_result.row).get("id"),
            "consumed_at": consumed_at,
            "status": STATUS_CONSUMED,
        }
        consumed_result = append_event(
            conn,
            event_type="completion.consumed",
            actor=actor,
            workspace_id=workspace_id,
            target=task_id,
            task_id=task_id,
            idempotency_key=f"receipt:{receipt_id}:consumed",
            payload=consumed_payload,
            commit=False,
        )
        conn.execute("RELEASE SAVEPOINT consume_receipt")
        conn.commit()
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT consume_receipt")
        conn.execute("RELEASE SAVEPOINT consume_receipt")
        conn.commit()
        raise

    return CompletionConsumeResult(
        receipt_id=receipt_id,
        workspace_id=workspace_id,
        task_id=task_id,
        authorized_actor=actor,
        event=row_to_dict(done_result.row),
        event_created=done_result.created and consumed_result.created,
        deployed_verification=deployed_verification,
    )


def _deployed_task_item(deployed_adapter, task_id: str) -> dict[str, Any]:
    try:
        checklist = deployed_adapter.read_checklist()
    except (HarnessError, OSError) as exc:
        raise CompletionReceiptError(
            f"deployed harness checklist unreadable: {exc}",
            reason="deployed_harness_unavailable",
        ) from exc
    items = checklist.get("items") if isinstance(checklist, dict) else None
    if not isinstance(items, list):
        raise CompletionReceiptError(
            "deployed harness checklist has no items list",
            reason="deployed_harness_unavailable",
        )
    for item in items:
        if isinstance(item, dict) and item.get("id") == task_id:
            return item
    raise CompletionReceiptError(
        f"task {task_id} not found in deployed harness",
        reason="deployed_task_missing",
    )
