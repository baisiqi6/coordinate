"""Slice 3 receipt-protocol contract tests.

Authority is the Coordinate control-plane event ledger. These tests pin the
correctness contract raised in review round 1 (P1-1 .. P1-7) and prove that
every rejection path leaves the canonical checklist and the `task.done`
terminal untouched.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from coordinate import completion
from coordinate.completion import (
    CompletionReceiptError,
    apply_completion_receipt,
    claim_completion_receipt,
    compute_item_fingerprint,
    compute_mark_done_fingerprints,
    consume_completion_receipt,
    parse_iso_timestamp,
    prepare_completion_receipt,
)
from coordinate.db import (
    append_event,
    find_events,
    get_workspace,
    initialize,
    list_events,
    row_to_dict,
    upsert_workspace,
)
from coordinate.harness import HarnessError


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


class _Adapter:
    """Combined gate + checklist fake for prepare/consume tests."""

    def __init__(
        self,
        workspace,
        *,
        refresh_state_result=None,
        refresh_state_error=None,
        checklist_items=None,
        checklist_error=None,
    ):
        self.workspace = workspace
        self._refresh_state_result = refresh_state_result
        self._refresh_state_error = refresh_state_error
        self._checklist_items = checklist_items
        self._checklist_error = checklist_error

    def refresh_state(self):
        if self._refresh_state_error is not None:
            raise self._refresh_state_error
        return self._refresh_state_result or {}

    def read_state(self):
        return self.refresh_state()

    def read_checklist(self):
        if self._checklist_error is not None:
            raise self._checklist_error
        return {"items": list(self._checklist_items or [])}


def _item(task_id="mvp-001", status="doing", workflow_status="review_approved",
          branch="feat-x"):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "status": status,
        "owner": None,
        "verification": "",
        "updated_at": "2026-01-01T00:00:00Z",
        "workflow": {"status": workflow_status, "branch": branch,
                     "updated_at": "2026-01-01T00:00:00Z"},
    }


def _done_fingerprint(task_id="mvp-001", branch="feat-x"):
    return compute_item_fingerprint(
        {"id": task_id, "status": "done",
         "workflow": {"status": "closed", "branch": branch}}
    )


class _ReceiptHarness(unittest.TestCase):
    """Shared fixtures: a workspace + a passing gate adapter that also
    exposes a readable checklist item."""

    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path=".", harness_root=".",
        )
        return conn

    def _adapter(self, conn, *, item=None, items=None, refresh=None,
                 refresh_error=None, checklist_error=None):
        workspace = get_workspace(conn, "demo")
        if items is None:
            items = [item or _item()]
        return _Adapter(
            workspace,
            refresh_state_result=refresh,
            refresh_state_error=refresh_error,
            checklist_items=items,
            checklist_error=checklist_error,
        )

    def _gate_refresh(self, task_id="mvp-001", workflow_status="review_approved"):
        return {"current_item": {
            "id": task_id,
            "workflow": {"status": workflow_status, "branch": "feat-x"},
            "status": "doing",
        }}

    def _prepare(self, conn, *, task_id="mvp-001", **kwargs):
        adapter = kwargs.pop("adapter", None) or self._adapter(
            conn, refresh=self._gate_refresh(task_id),
            items=[_item(task_id=task_id)],
        )
        return prepare_completion_receipt(
            conn, workspace_id="demo", task_id=task_id,
            adapter=adapter, **kwargs,
        )

    def _count_events(self, conn, event_type):
        return sum(
            1 for r in list_events(conn, "demo")
            if row_to_dict(r)["event_type"] == event_type
        )


# --------------------------------------------------------------------------
# P1-1: forge gate fail-closed
# --------------------------------------------------------------------------


class ForgeGateTests(_ReceiptHarness):
    def _add_ci(self, conn, event_type, *, task_id="mvp-001"):
        append_event(
            conn, event_type=event_type, actor="ci",
            workspace_id="demo", task_id=task_id, target=task_id,
            idempotency_key=f"ci:{task_id}:{event_type}:{event_type}",
            payload={"task_id": task_id, "status": event_type.split(".", 1)[1]},
        )

    def test_prepare_ok_when_no_ci_evidence_records_not_applicable(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        self.assertTrue(receipt.forge_evidence.get("not_applicable"))

    def test_prepare_ok_when_latest_ci_is_passed(self):
        conn = self._make_conn()
        self._add_ci(conn, "ci.pending")
        self._add_ci(conn, "ci.passed")
        receipt = self._prepare(conn)
        self.assertEqual(receipt.forge_evidence.get("status"), "passed")
        self.assertNotIn("not_applicable", receipt.forge_evidence)

    def test_prepare_rejects_when_latest_ci_failed(self):
        conn = self._make_conn()
        self._add_ci(conn, "ci.passed")
        self._add_ci(conn, "ci.failed")
        with self.assertRaises(CompletionReceiptError) as ctx:
            self._prepare(conn)
        self.assertEqual(ctx.exception.reason, "forge_gate_failed")
        self.assertEqual(self._count_events(conn, "completion.authorized"), 0)

    def test_prepare_rejects_when_latest_ci_pending(self):
        conn = self._make_conn()
        self._add_ci(conn, "ci.passed")
        self._add_ci(conn, "ci.pending")
        with self.assertRaises(CompletionReceiptError) as ctx:
            self._prepare(conn)
        self.assertEqual(ctx.exception.reason, "forge_gate_failed")


# --------------------------------------------------------------------------
# P1-2: actor / requester binding
# --------------------------------------------------------------------------


class ActorBindingTests(_ReceiptHarness):
    def test_authorized_actor_defaults_to_requester(self):
        conn = self._make_conn()
        receipt = self._prepare(conn, requester="operator")
        self.assertEqual(receipt.authorized_actor, "operator")
        self.assertEqual(receipt.requester, "operator")

    def test_prepare_explicit_authorized_actor_split(self):
        conn = self._make_conn()
        receipt = self._prepare(conn, requester="alice", authorized_actor="bob")
        self.assertEqual(receipt.requester, "alice")
        self.assertEqual(receipt.authorized_actor, "bob")

    def test_claim_rejects_actor_mismatch(self):
        conn = self._make_conn()
        receipt = self._prepare(conn, requester="operator")
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="intruder",
                before_fingerprint=receipt.harness_fingerprint,
                expected_after_fingerprint=_done_fingerprint(),
            )
        self.assertEqual(ctx.exception.reason, "actor_mismatch")

    def test_consume_rejects_actor_mismatch(self):
        conn = self._make_conn()
        receipt = self._prepare(conn, requester="operator")
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        deployed = self._adapter(conn, items=[_item(status="done",
                                                    workflow_status="closed")])
        with self.assertRaises(CompletionReceiptError) as ctx:
            consume_completion_receipt(
                conn, receipt_id=receipt.receipt_id, actor="intruder",
                deployed_adapter=deployed,
            )
        self.assertEqual(ctx.exception.reason, "actor_mismatch")
        self.assertEqual(self._count_events(conn, "task.done"), 0)


# --------------------------------------------------------------------------
# P1-3: prepare harness fingerprint binds the claim
# --------------------------------------------------------------------------


class HarnessFingerprintBindingTests(_ReceiptHarness):
    def test_prepare_records_harness_fingerprint_from_checklist_item(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        expected = compute_item_fingerprint(_item())
        self.assertEqual(receipt.harness_fingerprint, expected)
        self.assertIsNotNone(receipt.harness_fingerprint)

    def test_claim_rejects_before_fingerprint_mismatch(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="operator",
                before_fingerprint="0" * 64,  # != receipt.harness_fingerprint
                expected_after_fingerprint=_done_fingerprint(),
            )
        self.assertEqual(ctx.exception.reason, "before_fingerprint_mismatch")
        self.assertEqual(self._count_events(conn, "completion.claimed"), 0)

    def test_prepare_fails_closed_when_checklist_item_missing(self):
        conn = self._make_conn()
        # Gate passes via current_item, but checklist has a DIFFERENT task.
        adapter = self._adapter(
            conn, refresh=self._gate_refresh(task_id="mvp-001"),
            items=[_item(task_id="mvp-999")],
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            prepare_completion_receipt(
                conn, workspace_id="demo", task_id="mvp-001", adapter=adapter,
            )
        self.assertEqual(ctx.exception.reason, "harness_item_missing")

    def test_prepare_fails_closed_when_checklist_unreadable(self):
        conn = self._make_conn()
        adapter = self._adapter(
            conn, refresh=self._gate_refresh(),
            checklist_error=HarnessError("checklist gone"),
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            prepare_completion_receipt(
                conn, workspace_id="demo", task_id="mvp-001", adapter=adapter,
            )
        self.assertIn(ctx.exception.reason, {"harness_item_missing",
                                             "harness_fingerprint_unavailable"})


# --------------------------------------------------------------------------
# P1-4: consume re-validates expiry
# --------------------------------------------------------------------------


class ConsumeExpiryTests(_ReceiptHarness):
    def test_consume_rejects_expired_receipt(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        # Expire the receipt AFTER claim/apply succeeded.
        conn.execute(
            "UPDATE events SET payload_json = json_set("
            "payload_json, '$.expires_at', ?) "
            "WHERE id = ?",
            ("2020-01-01T00:00:00Z", receipt.event["id"]),
        )
        conn.commit()
        deployed = self._adapter(conn, items=[_item(status="done",
                                                    workflow_status="closed")])
        with self.assertRaises(CompletionReceiptError) as ctx:
            consume_completion_receipt(
                conn, receipt_id=receipt.receipt_id, actor="operator",
                deployed_adapter=deployed,
            )
        self.assertEqual(ctx.exception.reason, "expired")
        self.assertEqual(self._count_events(conn, "task.done"), 0)

    def test_consume_rejects_malformed_expiry(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        conn.execute(
            "UPDATE events SET payload_json = json_set("
            "payload_json, '$.expires_at', ?) WHERE id = ?",
            ("not-a-date", receipt.event["id"]),
        )
        conn.commit()
        deployed = self._adapter(conn, items=[_item(status="done",
                                                    workflow_status="closed")])
        with self.assertRaises(CompletionReceiptError) as ctx:
            consume_completion_receipt(
                conn, receipt_id=receipt.receipt_id, actor="operator",
                deployed_adapter=deployed,
            )
        self.assertEqual(ctx.exception.reason, "malformed_expiry")


# --------------------------------------------------------------------------
# P1-5: two-phase claimed -> applied -> consumed
# --------------------------------------------------------------------------


class TwoPhaseProtocolTests(_ReceiptHarness):
    def _full_claim_apply(self, conn, receipt, *, actor="operator"):
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor=actor,
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor=actor,
            after_fingerprint=_done_fingerprint(),
        )

    def test_claim_creates_claimed_event_with_expected_after(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claimed = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        self.assertEqual(claimed.status, "claimed")
        self.assertEqual(claimed.expected_after_fingerprint, _done_fingerprint())
        self.assertEqual(self._count_events(conn, "completion.claimed"), 1)
        self.assertEqual(self._count_events(conn, "completion.applied"), 0)

    def test_apply_creates_applied_event_with_actual_after(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        applied = apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        self.assertEqual(applied.status, "applied")
        self.assertEqual(applied.after_fingerprint, _done_fingerprint())
        self.assertEqual(self._count_events(conn, "completion.applied"), 1)

    def test_apply_rejects_when_not_claimed(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        with self.assertRaises(CompletionReceiptError) as ctx:
            apply_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="operator",
                after_fingerprint=_done_fingerprint(),
            )
        self.assertEqual(ctx.exception.reason, "not_claimed")

    def test_consume_requires_applied_not_claimed(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(  # claimed only, no apply
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        deployed = self._adapter(conn, items=[_item(status="done",
                                                    workflow_status="closed")])
        with self.assertRaises(CompletionReceiptError) as ctx:
            consume_completion_receipt(
                conn, receipt_id=receipt.receipt_id, actor="operator",
                deployed_adapter=deployed,
            )
        self.assertEqual(ctx.exception.reason, "not_applied")
        self.assertEqual(self._count_events(conn, "task.done"), 0)

    def test_apply_rejects_after_fingerprint_mismatch(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            apply_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="operator",
                after_fingerprint="0" * 64,
            )
        self.assertEqual(ctx.exception.reason, "after_fingerprint_mismatch")
        # apply must NOT have created a partial applied event.
        self.assertEqual(self._count_events(conn, "completion.applied"), 0)

    def test_claim_idempotent_on_same_expected_after(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        first = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        second = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        self.assertFalse(first.idempotent)
        self.assertTrue(second.idempotent)
        self.assertEqual(self._count_events(conn, "completion.claimed"), 1)

    def test_apply_idempotent_on_retry_after_callback_loss(self):
        """If the host crashes after the local write but before the apply
        ack lands, retrying claim+apply must converge idempotently."""
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        first_apply = apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        # Retry: claim again (idempotent) then apply again (idempotent).
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        second_apply = apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        self.assertFalse(first_apply.idempotent)
        self.assertTrue(second_apply.idempotent)
        self.assertEqual(self._count_events(conn, "completion.applied"), 1)


# --------------------------------------------------------------------------
# P1 round-3: idempotent claim must bind the retry's before_fingerprint to
# the prior-claimed / prior-applied lifecycle. A third drift fingerprint
# (same expected-after) must be rejected, not treated as idempotent.
# --------------------------------------------------------------------------


def _drift_before_fingerprint():
    """A third lifecycle fingerprint (running, same branch) — neither the
    review_approved harness_fingerprint (A) nor the done/closed
    after-fingerprint (E), but sharing the same branch so the deterministic
    expected-after is unchanged."""
    return compute_item_fingerprint(
        {"id": "mvp-001", "status": "doing",
         "workflow": {"status": "running", "branch": "feat-x"}}
    )


class ClaimIdempotentBeforeBindingTests(_ReceiptHarness):
    def test_claim_rejects_drifted_before_under_prior_claimed(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)  # harness_fingerprint = A
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="operator",
                before_fingerprint=_drift_before_fingerprint(),  # C, not A or E
                expected_after_fingerprint=_done_fingerprint(),   # same E
            )
        self.assertEqual(ctx.exception.reason, "before_fingerprint_mismatch")
        # No new claimed event; no applied event.
        self.assertEqual(self._count_events(conn, "completion.claimed"), 1)
        self.assertEqual(self._count_events(conn, "completion.applied"), 0)

    def test_claim_rejects_drifted_before_under_prior_applied(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="operator",
                before_fingerprint=_drift_before_fingerprint(),
                expected_after_fingerprint=_done_fingerprint(),
            )
        self.assertEqual(ctx.exception.reason, "before_fingerprint_mismatch")
        self.assertEqual(self._count_events(conn, "completion.applied"), 1)

    def test_claim_idempotent_allows_retry_with_original_before(self):
        """Legit recovery: claim landed, write NOT yet done — retrying claim
        with the original before_fingerprint (A) is idempotent."""
        conn = self._make_conn()
        receipt = self._prepare(conn)
        first = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        second = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        self.assertFalse(first.idempotent)
        self.assertTrue(second.idempotent)
        self.assertEqual(self._count_events(conn, "completion.claimed"), 1)

    def test_claim_idempotent_allows_retry_with_done_state_before_then_apply(self):
        """Legit recovery: claim landed, local write already done — retrying
        claim with before == expected_after (the done-state fp, E) is
        idempotent, and a following apply ack succeeds."""
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        # Local write has landed: retry claim with before = done-state fp (E).
        retry = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=_done_fingerprint(),
            expected_after_fingerprint=_done_fingerprint(),
        )
        self.assertTrue(retry.idempotent)
        # The returned claim carries the AUTHORITATIVE original before (A),
        # not the caller's done-state before.
        self.assertEqual(retry.before_fingerprint, receipt.harness_fingerprint)
        # Apply ack succeeds.
        applied = apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        self.assertFalse(applied.idempotent)
        self.assertEqual(self._count_events(conn, "completion.applied"), 1)

    def test_claim_idempotent_allows_retry_under_prior_applied(self):
        """After applied, a recovery retry with either original-before (A) or
        done-state (E) is idempotent; a drift before is rejected."""
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            after_fingerprint=_done_fingerprint(),
        )
        retry_a = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,  # A
            expected_after_fingerprint=_done_fingerprint(),
        )
        retry_e = claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=_done_fingerprint(),  # E
            expected_after_fingerprint=_done_fingerprint(),
        )
        self.assertTrue(retry_a.idempotent)
        self.assertTrue(retry_e.idempotent)
        self.assertEqual(self._count_events(conn, "completion.applied"), 1)


# --------------------------------------------------------------------------
# Consume correctness + carry-over rejection paths
# --------------------------------------------------------------------------


class ConsumeCorrectnessTests(_ReceiptHarness):
    def _claim_apply(self, conn, receipt, *, actor="operator"):
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor=actor,
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        apply_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor=actor,
            after_fingerprint=_done_fingerprint(),
        )

    def test_consume_happy_path_atomic_task_done_and_consumed(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        self._claim_apply(conn, receipt)
        deployed = self._adapter(conn, items=[_item(status="done",
                                                    workflow_status="closed")])
        result = consume_completion_receipt(
            conn, receipt_id=receipt.receipt_id, actor="operator",
            deployed_adapter=deployed,
        )
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "task.done")
        self.assertEqual(result.event["payload"]["receipt_id"], receipt.receipt_id)
        self.assertEqual(self._count_events(conn, "task.done"), 1)
        self.assertEqual(self._count_events(conn, "completion.consumed"), 1)

    def test_consume_replay_no_second_terminal(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        self._claim_apply(conn, receipt)
        deployed = self._adapter(conn, items=[_item(status="done",
                                                    workflow_status="closed")])
        first = consume_completion_receipt(
            conn, receipt_id=receipt.receipt_id, actor="operator",
            deployed_adapter=deployed,
        )
        second = consume_completion_receipt(
            conn, receipt_id=receipt.receipt_id, actor="operator",
            deployed_adapter=deployed,
        )
        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(self._count_events(conn, "task.done"), 1)

    def test_consume_rejects_deployed_not_done(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        self._claim_apply(conn, receipt)
        deployed = self._adapter(conn, items=[_item(status="doing",
                                                    workflow_status="todo")])
        with self.assertRaises(CompletionReceiptError) as ctx:
            consume_completion_receipt(
                conn, receipt_id=receipt.receipt_id, actor="operator",
                deployed_adapter=deployed,
            )
        self.assertEqual(ctx.exception.reason, "deployed_not_done")
        self.assertEqual(self._count_events(conn, "task.done"), 0)

    def test_consume_rejects_deployed_fingerprint_mismatch(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        self._claim_apply(conn, receipt)
        # Deployed done/closed but on a DIFFERENT branch.
        deployed = self._adapter(
            conn, items=[_item(status="done", workflow_status="closed",
                              branch="other-branch")],
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            consume_completion_receipt(
                conn, receipt_id=receipt.receipt_id, actor="operator",
                deployed_adapter=deployed,
            )
        self.assertEqual(ctx.exception.reason, "fingerprint_mismatch")
        self.assertEqual(self._count_events(conn, "task.done"), 0)

    def test_consume_rejects_when_task_done_under_other_authority(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        self._claim_apply(conn, receipt)
        append_event(
            conn, event_type="task.done", actor="omp",
            workspace_id="demo", task_id="mvp-001", target="mvp-001",
            idempotency_key="legacy:mvp-001:task.done",
            payload={"task_id": "mvp-001", "host_aware": "legacy"},
        )
        deployed = self._adapter(conn, items=[_item(status="done",
                                                    workflow_status="closed")])
        with self.assertRaises(CompletionReceiptError) as ctx:
            consume_completion_receipt(
                conn, receipt_id=receipt.receipt_id, actor="operator",
                deployed_adapter=deployed,
            )
        self.assertEqual(ctx.exception.reason, "task_already_done_other_authority")
        self.assertEqual(self._count_events(conn, "task.done"), 1)  # only the legacy one


# --------------------------------------------------------------------------
# Cross-binding rejections + expiry at claim
# --------------------------------------------------------------------------


class ClaimRejectionTests(_ReceiptHarness):
    def test_claim_rejects_cross_workspace(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="other", task_id="mvp-001", actor="operator",
                before_fingerprint=receipt.harness_fingerprint,
                expected_after_fingerprint=_done_fingerprint(),
            )
        self.assertEqual(ctx.exception.reason, "workspace_mismatch")

    def test_claim_rejects_cross_task(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-999", actor="operator",
                before_fingerprint=receipt.harness_fingerprint,
                expected_after_fingerprint=_done_fingerprint(),
            )
        self.assertEqual(ctx.exception.reason, "task_mismatch")

    def test_claim_rejects_unknown_receipt(self):
        conn = self._make_conn()
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id="missing", workspace_id="demo",
                task_id="mvp-001", actor="operator",
                before_fingerprint="x", expected_after_fingerprint="y",
            )
        self.assertEqual(ctx.exception.reason, "unknown_receipt")

    def test_claim_rejects_expired_receipt(self):
        conn = self._make_conn()
        receipt = self._prepare(conn, ttl_seconds=-1)
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="operator",
                before_fingerprint=receipt.harness_fingerprint,
                expected_after_fingerprint=_done_fingerprint(),
            )
        self.assertEqual(ctx.exception.reason, "expired")
        self.assertEqual(self._count_events(conn, "completion.claimed"), 0)

    def test_claim_rejects_mismatched_expected_after_replay(self):
        conn = self._make_conn()
        receipt = self._prepare(conn)
        claim_completion_receipt(
            conn, receipt_id=receipt.receipt_id,
            workspace_id="demo", task_id="mvp-001", actor="operator",
            before_fingerprint=receipt.harness_fingerprint,
            expected_after_fingerprint=_done_fingerprint(),
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            claim_completion_receipt(
                conn, receipt_id=receipt.receipt_id,
                workspace_id="demo", task_id="mvp-001", actor="operator",
                before_fingerprint=receipt.harness_fingerprint,
                expected_after_fingerprint="0" * 64,
            )
        self.assertEqual(ctx.exception.reason, "fingerprint_mismatch")


# --------------------------------------------------------------------------
# prepare gate fail-closed
# --------------------------------------------------------------------------


class PrepareGateTests(_ReceiptHarness):
    def test_prepare_fail_closed_when_gate_not_passed(self):
        conn = self._make_conn()
        adapter = self._adapter(
            conn, refresh={"current_item": {
                "id": "mvp-001",
                "workflow": {"status": "todo", "branch": "feat-x"},
                "status": "doing",
            }},
            items=[_item(workflow_status="todo")],
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            prepare_completion_receipt(
                conn, workspace_id="demo", task_id="mvp-001", adapter=adapter,
            )
        self.assertEqual(ctx.exception.reason, "gate_not_passed")

    def test_prepare_fail_closed_when_harness_unavailable(self):
        conn = self._make_conn()
        adapter = self._adapter(
            conn, refresh_error=HarnessError("no harness"),
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            prepare_completion_receipt(
                conn, workspace_id="demo", task_id="mvp-001", adapter=adapter,
            )
        self.assertIn(ctx.exception.reason,
                      {"gate_not_passed", "harness_item_missing"})


# --------------------------------------------------------------------------
# fingerprint helper
# --------------------------------------------------------------------------


class FingerprintTests(unittest.TestCase):
    def test_excludes_verification_freetext(self):
        a = compute_item_fingerprint(
            {"id": "t", "status": "done",
             "workflow": {"status": "closed", "branch": "x"},
             "verification": "first", "completion_receipt": {"receipt_id": "r"}}
        )
        b = compute_item_fingerprint(
            {"id": "t", "status": "done",
             "workflow": {"status": "closed", "branch": "x"},
             "verification": "other", "completion_receipt": {"receipt_id": "s"}}
        )
        self.assertEqual(a, b)

    def test_changes_on_branch(self):
        a = compute_item_fingerprint(
            {"id": "t", "status": "done",
             "workflow": {"status": "closed", "branch": "x"}}
        )
        b = compute_item_fingerprint(
            {"id": "t", "status": "done",
             "workflow": {"status": "closed", "branch": "y"}}
        )
        self.assertNotEqual(a, b)

    def test_compute_mark_done_fingerprints_before_and_expected_after(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        Path(tmp, "mvp-checklist.json").write_text(
            json.dumps({"items": [_item()]}), encoding="utf-8",
        )
        fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
        self.assertEqual(fps.before_fingerprint, compute_item_fingerprint(_item()))
        self.assertEqual(fps.after_fingerprint, _done_fingerprint())

    def test_parse_iso_timestamp_helper(self):
        dt = parse_iso_timestamp("2026-01-02T03:04:05Z")
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.year, 2026)


if __name__ == "__main__":
    unittest.main()
