"""Tests for ``coordinate.onboarding`` — esp. backlog #9c (plan.ready versioning)."""
import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinate.checklist_contract import validate_checklist
from coordinate.checklist_io import (
    CHECKLIST_LEGACY_NAME,
    CHECKLIST_NEW_NAME,
    REASON_DUAL_AUTHORITY,
    REASON_VALIDATION_ERROR,
    ChecklistError,
)
from coordinate.db import (
    get_workspace,
    initialize,
    list_events,
    list_split_operations,
    list_task_mirrors,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.onboarding import (
    REASON_RUNTIME_DESTINATION_MISMATCH,
    REASON_RUNTIME_ROOT_INCOMPATIBLE,
    REASON_RUNTIME_SOURCE_INCOMPLETE,
    REASON_RUNTIME_TEMPLATE_PLACEHOLDER,
    RuntimeSourceError,
    create_plan_task,
    create_plan_task_record,
    init_file_harness,
    init_full_harness,
)
from tests.fixtures.runtime_template import (
    coordinate_runtime_dir,
    make_template_source,
    rendered_runtime_values,
)
from coordinate.plan_gate import approve_plan, reject_plan
from coordinate.projection_doctor import diagnose_projections
from coordinate.split_operations import (
    CONTRACT_VERSION,
    OPERATION_KIND_TASK_CREATE,
    REASON_OPERATION_CONFLICT,
    SplitOperationError,
    apply_task_create_files,
    apply_task_create_record,
)


class CreatePlanTaskTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.plan_path = Path(self.tmp.name) / "plan.md"
        self.plan_path.write_text("# Plan v1\n")
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        (Path(self.tmp.name) / "mvp-checklist.json").write_text(
            json.dumps({
                "project": "demo",
                "harness_root": ".",
                "version": 1,
                "updated_at": "2026-07-13",
                "items": [],
            }, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def _plan_ready_events(self):
        return [
            row_to_dict(e)
            for e in list_events(self.conn, "demo")
            if row_to_dict(e)["event_type"] == "plan.ready"
        ]

    def test_plan_content_change_creates_new_plan_ready_event(self):
        # 9c: revising the plan doc (same task) must produce a new plan.ready
        # event — through the explicit revision entry (create_plan_task_record
        # without an operation), not through the combined create, which fails
        # closed on changed inputs.
        create_plan_task(
            self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path)
        )

        self.plan_path.write_text("# Plan v2 — revised\n")  # content changed
        with self.assertRaises(SplitOperationError) as ctx:
            create_plan_task(
                self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path)
            )
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
        self.assertEqual(len(self._plan_ready_events()), 1)

        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="t1",
            plan_doc="plan.md",
            title="t1",
            phase="ready",
        )
        plan_ready = self._plan_ready_events()
        self.assertEqual(len(plan_ready), 2, "revised plan must create a new plan.ready event")
        self.assertNotEqual(plan_ready[0]["id"], plan_ready[1]["id"])
        self.assertNotEqual(
            plan_ready[0]["payload"].get("plan_content_hash"),
            plan_ready[1]["payload"].get("plan_content_hash"),
        )

    def test_same_content_is_idempotent(self):
        create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path))
        create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path))

        plan_ready = self._plan_ready_events()
        self.assertEqual(len(plan_ready), 1, "same content → idempotent (one event)")

    def test_idempotency_key_contains_hash(self):
        create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path))
        plan_ready = self._plan_ready_events()
        self.assertEqual(len(plan_ready), 1)
        key = plan_ready[0]["idempotency_key"]
        self.assertTrue(key.startswith("demo:t1:plan.ready:"))
        # the hash suffix is present (not the old fixed key)
        self.assertNotEqual(key, "demo:t1:plan.ready")

    def test_missing_plan_doc_fails_closed_with_no_writes(self):
        missing = Path(self.tmp.name) / "missing.md"
        with self.assertRaises(ValueError):
            create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(missing))
        self.assertEqual(self._plan_ready_events(), [])
        self.assertEqual(list_task_mirrors(self.conn, "demo"), [])

    def test_directory_plan_doc_fails_closed_with_no_writes(self):
        dir_path = Path(self.tmp.name) / "plans_dir"
        dir_path.mkdir()
        with self.assertRaises(ValueError):
            create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(dir_path))
        self.assertEqual(self._plan_ready_events(), [])
        self.assertEqual(list_task_mirrors(self.conn, "demo"), [])

    def test_unreadable_plan_doc_fails_closed_with_no_writes(self):
        with patch(
            "coordinate.onboarding.compute_plan_sha256",
            side_effect=OSError("mocked read failure"),
        ):
            with self.assertRaises(ValueError):
                create_plan_task(
                    self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path)
                )
        self.assertEqual(self._plan_ready_events(), [])
        self.assertEqual(list_task_mirrors(self.conn, "demo"), [])

    def test_plan_sha256_is_full_and_content_hash_is_prefix(self):
        create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path))
        plan_ready = self._plan_ready_events()
        self.assertEqual(len(plan_ready), 1)
        payload = plan_ready[0]["payload"]
        sha256 = payload["plan_sha256"]
        self.assertEqual(len(sha256), 64)
        self.assertEqual(payload["plan_content_hash"], sha256[:16])


class SplitOperationPreservationTests(unittest.TestCase):
    """P9-2A: non-split create_plan_task_record must preserve existing split_operation metadata."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
        self.plan_path = Path(self.tmp.name) / "plan.md"
        self.plan_path.write_text("# Plan v1\n")
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        (Path(self.tmp.name) / "mvp-checklist.json").write_text(
            json.dumps({
                "project": "demo",
                "harness_root": ".",
                "version": 1,
                "updated_at": "2026-07-13",
                "items": [],
            }, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def _split_operation_id(self):
        return "12345678-1234-1234-1234-123456789abc"

    def _operation_meta(self, operation_id=None):
        return {
            "contract_version": CONTRACT_VERSION,
            "operation_id": operation_id or self._split_operation_id(),
            "operation_kind": OPERATION_KIND_TASK_CREATE,
            "input_fingerprint": "a" * 64,
            "before_fingerprint": "b" * 64,
            "after_fingerprint": "c" * 64,
        }

    def _create_split_task(self, operation_id=None, content="# Plan v1\n"):
        operation_id = operation_id or self._split_operation_id()
        self.plan_path.write_text(content)
        files = apply_task_create_files(
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            priority="p1",
            operation_id=operation_id,
        )
        apply_task_create_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            owner=None,
            branch=None,
            actor="operator",
            target=None,
            payload=None,
            operation_id=operation_id,
            input_fingerprint=files.input_fingerprint,
            before_fingerprint=files.before_fingerprint,
            after_fingerprint=files.after_fingerprint,
        )
        return files

    def _create_legacy_task(self, content="# Legacy plan\n"):
        # Legacy tasks are pre-registered mirrors; the revision branch must
        # never be used as a task-create path (U2 root cause regression).
        # Register the mirror explicitly, then author its initial plan.ready
        # through the revision entry.
        self.plan_path.write_text(content)
        upsert_task_mirror(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            phase="ready",
            owner=None,
            branch=None,
            pr=None,
            payload={
                "task_id": "task-1",
                "title": "Legacy task",
                "plan_doc": "plan.md",
                "absolute_plan_doc": str(self.plan_path.resolve()),
                "status": "ready",
            },
        )
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Legacy task",
            phase="ready",
        )

    def _task_payload(self):
        row = list_task_mirrors(self.conn, "demo")[0]
        return json.loads(row["payload_json"])

    def _plan_ready_events(self):
        return [
            row_to_dict(e)
            for e in list_events(self.conn, "demo")
            if row_to_dict(e)["event_type"] == "plan.ready"
        ]

    def _diagnose(self):
        ws = get_workspace(self.conn, "demo")
        return diagnose_projections(self.conn, ws)

    def _assert_no_metadata_drift(self, report):
        for f in report.findings:
            self.assertNotEqual(
                f.kind,
                "operation_task_mirror_metadata_drift",
                f"unexpected metadata drift: {f.evidence}",
            )

    def test_split_create_then_legacy_revision_preserves_metadata(self):
        self._create_split_task()
        before = self._task_payload()["split_operation"]

        # Reproduce the deployment finding: revise the plan through the
        # compatibility path, then approve that exact superseding ready event.
        self.plan_path.write_text("# Plan v1 revised\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
        )
        approve_plan(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            scope="implementation",
            reviewer="reviewer-1",
        )

        after = self._task_payload()["split_operation"]
        self.assertEqual(after, before)
        self.assertEqual(after["operation_id"], self._split_operation_id())
        self.assertEqual(after["contract_version"], CONTRACT_VERSION)
        self.assertEqual(len(self._plan_ready_events()), 2)

        report = self._diagnose()
        self._assert_no_metadata_drift(report)
        self.assertEqual(report.summary["errors"], 0)
        self.assertIn(
            "operation_plan_superseded",
            {finding.kind for finding in report.findings},
        )

    def test_conflicting_caller_split_operation_rejected_no_mutation(self):
        self._create_split_task()
        before_events = self._plan_ready_events()
        before_payload = self._task_payload()

        forged = self._operation_meta()
        forged["operation_id"] = "87654321-4321-4321-4321-210987654321"
        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plan.md",
                title="Task 1",
                phase="ready",
                payload={"split_operation": forged},
            )

        self.assertEqual(self._plan_ready_events(), before_events)
        self.assertEqual(self._task_payload(), before_payload)

    def test_unbound_legacy_task_forged_split_operation_rejected(self):
        self._create_legacy_task()
        before_events = self._plan_ready_events()
        before_payload = self._task_payload()

        forged = self._operation_meta()
        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plan.md",
                title="Legacy task",
                phase="ready",
                payload={"split_operation": forged},
            )

        self.assertEqual(self._plan_ready_events(), before_events)
        self.assertEqual(self._task_payload(), before_payload)

    def test_normal_legacy_revision_without_split_metadata_unchanged(self):
        self._create_legacy_task()
        self.assertIsNone(self._task_payload().get("split_operation"))
        self.assertNotIn("phase", self._task_payload())

        self.plan_path.write_text("# Legacy plan v2\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Legacy task revised",
            phase="ready",
        )

        self.assertIsNone(self._task_payload().get("split_operation"))
        self.assertNotIn("phase", self._task_payload())
        self.assertEqual(self._task_payload()["title"], "Legacy task revised")
        self.assertEqual(len(self._plan_ready_events()), 2)

    def test_malformed_stored_split_operation_fails_before_mutation(self):
        self._create_legacy_task()
        malformed = self._operation_meta()
        del malformed["input_fingerprint"]  # missing required key
        payload = self._task_payload()
        payload["split_operation"] = malformed
        self.conn.execute(
            "UPDATE tasks SET payload_json = ? WHERE workspace_id = ? AND task_id = ?",
            (json.dumps(payload), "demo", "task-1"),
        )
        self.conn.commit()

        before_events = self._plan_ready_events()
        before_payload = self._task_payload()
        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plan.md",
                title="Legacy task",
                phase="ready",
            )
        self.assertEqual(self._plan_ready_events(), before_events)
        self.assertEqual(self._task_payload(), before_payload)

    def test_caller_null_split_operation_rejected(self):
        # Key presence (not value) marks the reserved key as caller-supplied;
        # a null value must still fail closed with zero mirror/event mutation.
        self._create_split_task()
        before_events = self._plan_ready_events()
        before_payload = self._task_payload()

        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plan.md",
                title="Task 1",
                phase="ready",
                payload={"split_operation": None},
            )

        self.assertEqual(self._plan_ready_events(), before_events)
        self.assertEqual(self._task_payload(), before_payload)

    def test_stored_null_split_operation_fails_closed(self):
        # A stored payload with the split_operation key present but null is
        # malformed and must fail closed rather than be silently dropped.
        self._create_legacy_task()
        payload = self._task_payload()
        payload["split_operation"] = None
        self.conn.execute(
            "UPDATE tasks SET payload_json = ? WHERE workspace_id = ? AND task_id = ?",
            (json.dumps(payload), "demo", "task-1"),
        )
        self.conn.commit()

        before_events = self._plan_ready_events()
        before_payload = self._task_payload()
        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plan.md",
                title="Legacy task",
                phase="ready",
            )
        self.assertEqual(self._plan_ready_events(), before_events)
        self.assertEqual(self._task_payload(), before_payload)

    def test_equal_caller_split_operation_accepted_and_still_stored(self):
        self._create_split_task()
        stored = self._task_payload()["split_operation"]

        self.plan_path.write_text("# Plan v1 revised again\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            payload={"split_operation": stored.copy()},
        )

        self.assertEqual(self._task_payload()["split_operation"], stored)

    def test_approve_and_reject_preserve_metadata(self):
        self._create_split_task()
        self.plan_path.write_text("# Plan v1 revised\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
        )
        stored = self._task_payload()["split_operation"]

        approve_plan(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            scope="worker",
            reviewer="reviewer-1",
        )
        self.assertEqual(self._task_payload()["split_operation"], stored)

        reject_plan(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            scope="worker",
            reason="needs rework",
        )
        self.assertEqual(self._task_payload()["split_operation"], stored)

    def test_revision_unknown_task_fails_closed_zero_mutation(self):
        # U2 root-cause regression: the revision entry must never INSERT a
        # DB-only task mirror for an unregistered task.
        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="never-registered",
                plan_doc="plan.md",
                title="Ghost",
                phase="ready",
            )
        self.assertEqual(list_task_mirrors(self.conn, "demo"), [])
        self.assertEqual(self._plan_ready_events(), [])

    def test_revision_different_plan_doc_fails_closed_state_unchanged(self):
        self._create_split_task()
        before_events = self._plan_ready_events()
        before_payload = self._task_payload()
        (Path(self.tmp.name) / "other.md").write_text("# Other plan\n")

        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="other.md",
                title="Task 1",
                phase="ready",
            )

        self.assertEqual(self._plan_ready_events(), before_events)
        self.assertEqual(self._task_payload(), before_payload)

    def test_revision_explicit_key_conflict_rolls_back_atomically(self):
        self._create_split_task()
        self.plan_path.write_text("# Plan v1 revised\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            idempotency_key="demo:task-1:revise:round-1",
        )
        before_events = self._plan_ready_events()
        before_payload = self._task_payload()
        before_last_event_id = list_task_mirrors(self.conn, "demo")[0]["last_event_id"]

        # Reuse the same key with a different intent: must fail closed and roll
        # back the mirror upsert performed before the conflicting event write.
        self.plan_path.write_text("# Plan v1 revised again\n")
        with self.assertRaises(ValueError):
            create_plan_task_record(
                self.conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plan.md",
                title="Changed title",
                phase="ready",
                idempotency_key="demo:task-1:revise:round-1",
            )

        self.assertEqual(self._plan_ready_events(), before_events)
        self.assertEqual(self._task_payload(), before_payload)
        self.assertEqual(
            list_task_mirrors(self.conn, "demo")[0]["last_event_id"],
            before_last_event_id,
        )

    def test_revision_explicit_key_exact_replay_idempotent(self):
        self._create_split_task()
        self.plan_path.write_text("# Plan v1 revised\n")
        kwargs = dict(
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            idempotency_key="demo:task-1:revise:round-1",
        )
        first = create_plan_task_record(self.conn, **kwargs)
        second = create_plan_task_record(self.conn, **kwargs)

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(second.event["id"], first.event["id"])
        self.assertEqual(len(self._plan_ready_events()), 2)

    def test_revision_relative_legacy_identity_no_drift(self):
        # Legacy mirror stores only a workspace-relative plan_doc; the
        # revision must resolve it against the workspace and keep the
        # registered expression (no drift from equivalent ./x or absolute
        # spellings).
        self.plan_path.write_text("# Legacy plan\n")
        upsert_task_mirror(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            phase="ready",
            owner=None,
            branch=None,
            pr=None,
            payload={
                "task_id": "task-1",
                "title": "Legacy task",
                "plan_doc": "plan.md",
                "status": "ready",
            },
        )

        self.plan_path.write_text("# Legacy plan revised\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="./plan.md",
            title="Legacy task",
            phase="ready",
        )
        after = self._task_payload()
        self.assertEqual(after["plan_doc"], "plan.md")
        self.assertEqual(after["absolute_plan_doc"], str(self.plan_path.resolve()))
        self.assertEqual(len(self._plan_ready_events()), 1)

    def test_revision_omitted_optional_args_preserve_metadata(self):
        self._create_split_task()
        payload = self._task_payload()
        payload.update({"custom_field": "keep-me", "title": "Custom Title"})
        self.conn.execute(
            "UPDATE tasks SET payload_json = ?, owner = ?, branch = ?, pr = ? "
            "WHERE workspace_id = ? AND task_id = ?",
            (json.dumps(payload), "owner-a", "agents/owner-a/task-1", "owner-a/task-1#1", "demo", "task-1"),
        )
        self.conn.commit()

        self.plan_path.write_text("# Plan v1 revised\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            phase="ready",
            # no title/owner/branch/payload: stored values must survive.
        )
        row = list_task_mirrors(self.conn, "demo")[0]
        after = json.loads(row["payload_json"])
        self.assertEqual(after["title"], "Custom Title")
        self.assertEqual(after["custom_field"], "keep-me")
        self.assertEqual(after["split_operation"], payload["split_operation"])
        self.assertEqual(row["owner"], "owner-a")
        self.assertEqual(row["branch"], "agents/owner-a/task-1")
        self.assertEqual(row["pr"], "owner-a/task-1#1")
        self.assertEqual(len(self._plan_ready_events()), 2)

        # Explicit empty strings normalize like omission: stored values stay.
        self.plan_path.write_text("# Plan v1 revised again\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="",
            owner="",
            branch="",
            phase="ready",
        )
        row = list_task_mirrors(self.conn, "demo")[0]
        after = json.loads(row["payload_json"])
        self.assertEqual(after["title"], "Custom Title")
        self.assertEqual(row["owner"], "owner-a")
        self.assertEqual(row["branch"], "agents/owner-a/task-1")
        self.assertEqual(len(self._plan_ready_events()), 3)

    def test_revision_explicit_overlay_updates_only_specified_fields(self):
        self._create_split_task()
        payload_before = self._task_payload()
        self.plan_path.write_text("# Plan v1 revised\n")
        create_plan_task_record(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="New Title",
            owner="owner-b",
            branch="agents/owner-b/task-1",
            payload={
                "custom_field": "overlaid",
                "task_id": "forged",
                "status": "forged",
            },
        )
        row = list_task_mirrors(self.conn, "demo")[0]
        after = json.loads(row["payload_json"])
        self.assertEqual(after["title"], "New Title")
        self.assertEqual(after["custom_field"], "overlaid")
        # Reserved/canonical fields stay system-controlled.
        self.assertEqual(after["task_id"], "task-1")
        self.assertEqual(after["status"], "ready")
        self.assertEqual(after["split_operation"], payload_before["split_operation"])
        self.assertEqual(row["owner"], "owner-b")
        self.assertEqual(row["branch"], "agents/owner-b/task-1")

    def test_revision_savepoint_failure_not_masked_by_rollback(self):
        # External failure while establishing the SAVEPOINT: the rollback
        # branch must not mask the original exception (no savepoint exists).
        # Single-point fault injection, not a second SQLite state machine.
        self._create_split_task()
        original_execute = self.conn.execute

        def failing_execute(sql, *args, **kwargs):
            if sql == "SAVEPOINT plan_revise":
                raise sqlite3.OperationalError("simulated external failure")
            return original_execute(sql, *args, **kwargs)

        self.plan_path.write_text("# Plan v1 revised\n")
        with patch.object(self.conn, "execute", side_effect=failing_execute):
            with self.assertRaisesRegex(
                sqlite3.OperationalError, "simulated external failure"
            ):
                create_plan_task_record(
                    self.conn,
                    workspace_id="demo",
                    task_id="task-1",
                    plan_doc="plan.md",
                    title="Task 1",
                    phase="ready",
                )
        # No partial writes escaped the failed revision.
        self.assertEqual(len(self._plan_ready_events()), 1)


def _valid_empty_checklist_body() -> str:
    return (
        json.dumps({
            "project": "demo",
            "harness_root": ".",
            "version": 1,
            "updated_at": "2026-07-13",
            "items": [],
        }, ensure_ascii=False, indent=2)
        + "\n"
    )


def _workspace_snapshot(ws: Path, conn) -> dict:
    """Full file-tree digests plus DB task/event/operation counts.

    Symlinks (including dangling ones) are recorded by target, never
    followed, so a symlink collision is part of the zero-mutation proof.
    """
    files = {}
    for p in sorted(ws.rglob("*")):
        rel = str(p.relative_to(ws))
        if p.is_symlink():
            files[rel] = "symlink:" + os.readlink(p)
        elif p.is_dir():
            files[rel + "/"] = None
        else:
            files[rel] = hashlib.sha256(p.read_bytes()).hexdigest()
    return {
        "files": files,
        "events": len(list(list_events(conn, "demo"))),
        "mirrors": len(list_task_mirrors(conn, "demo")),
        "ops": len(list_split_operations(conn, workspace_id="demo")),
    }


class InitHarnessPreflightTests(unittest.TestCase):
    """P1-2: minimal/full init must complete every predictable preflight
    BEFORE any mkdir/copy/protocol write/workspace upsert/event/DB mutation.
    Dual authority or invalid input must leave the full file tree and all DB
    task/event/operation counts byte-identical.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.ws = Path(self.tmp.name)
        self.root = self.ws / "docs" / "project-harness"
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.ws),
            harness_root=str(self.ws / "docs"),
        )
        self.plan = self.ws / "docs" / "plan.md"
        self.plan.parent.mkdir(parents=True, exist_ok=True)
        self.plan.write_text("# Plan\n", encoding="utf-8")

    def _snapshot(self):
        files = {}
        for p in sorted(self.ws.rglob("*")):
            rel = str(p.relative_to(self.ws))
            if p.is_dir():
                files[rel + "/"] = None
            else:
                files[rel] = hashlib.sha256(p.read_bytes()).hexdigest()
        return {
            "files": files,
            "events": len(list(list_events(self.conn, "demo"))),
            "mirrors": len(list_task_mirrors(self.conn, "demo")),
            "ops": len(list_split_operations(self.conn, workspace_id="demo")),
        }

    def _write_checklist(self, name: str, root: Path | None = None) -> Path:
        root = root or self.root
        root.mkdir(parents=True, exist_ok=True)
        path = root / name
        path.write_text(_valid_empty_checklist_body(), encoding="utf-8")
        return path

    def _minimal_init(self, **overrides):
        kwargs = {
            "workspace_id": "demo",
            "root": str(self.root),
            "task_id": "t1",
            "plan_doc": str(self.plan),
        }
        kwargs.update(overrides)
        return init_file_harness(self.conn, **kwargs)

    def _assert_minimal_init_fails_without_mutation(self, exc_type, *, reason=None, **overrides) -> None:
        before = self._snapshot()
        with self.assertRaises(exc_type) as ctx:
            self._minimal_init(**overrides)
        if reason is not None:
            self.assertEqual(ctx.exception.reason, reason)
        self.assertEqual(self._snapshot(), before)

    # --- minimal: authority matrix -------------------------------------------------

    def test_minimal_dual_authority_zero_mutation(self):
        self._write_checklist(CHECKLIST_NEW_NAME)
        self._write_checklist(CHECKLIST_LEGACY_NAME)
        self._assert_minimal_init_fails_without_mutation(
            ChecklistError, reason=REASON_DUAL_AUTHORITY
        )

    def test_minimal_non_regular_candidate_zero_mutation(self):
        # A directory named like the new checklist must fail closed even
        # though is_file() is False — nothing may be created around it.
        (self.root / CHECKLIST_NEW_NAME).mkdir(parents=True)
        self._assert_minimal_init_fails_without_mutation(
            ChecklistError, reason=REASON_VALIDATION_ERROR
        )

    def test_minimal_invalid_existing_candidate_zero_mutation(self):
        path = self._write_checklist(CHECKLIST_NEW_NAME)
        data = json.loads(path.read_text(encoding="utf-8"))
        del data["items"]
        path.write_text(json.dumps(data), encoding="utf-8")
        self._assert_minimal_init_fails_without_mutation(
            ChecklistError, reason=REASON_VALIDATION_ERROR
        )

    # --- minimal: plan preflight ---------------------------------------------------

    def test_minimal_missing_plan_zero_mutation(self):
        self._assert_minimal_init_fails_without_mutation(
            ValueError, plan_doc=str(self.ws / "docs" / "missing.md")
        )

    def test_minimal_directory_plan_zero_mutation(self):
        plan_dir = self.ws / "docs" / "plans"
        plan_dir.mkdir(parents=True)
        self._assert_minimal_init_fails_without_mutation(
            ValueError, plan_doc=str(plan_dir)
        )

    def test_minimal_unreadable_plan_zero_mutation(self):
        before = self._snapshot()
        os.chmod(self.plan, 0)
        if os.access(self.plan, os.R_OK):
            os.chmod(self.plan, 0o644)
            self.skipTest("file remains readable (running as root); cannot simulate")
        try:
            with self.assertRaises(ValueError):
                self._minimal_init()
        finally:
            os.chmod(self.plan, 0o644)
        self.assertEqual(self._snapshot(), before)

    def test_minimal_outside_workspace_plan_zero_mutation(self):
        outside = Path(tempfile.mkdtemp()) / "plan.md"
        self.addCleanup(lambda: __import__("shutil").rmtree(outside.parent, ignore_errors=True))
        outside.write_text("# Plan\n", encoding="utf-8")
        self._assert_minimal_init_fails_without_mutation(
            ValueError, plan_doc=str(outside)
        )

    def test_minimal_external_absolute_root_supported(self):
        # The formal contract keeps minimal external-root capability: operator
        # policy may avoid it, but the implementation must not forbid it. The
        # plan stays workspace-relative; only the harness root lives outside
        # the workspace, and the stored paths match the original capability.
        external = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(external, ignore_errors=True))
        result = self._minimal_init(root=str(external / "harness"))
        root = external / "harness"
        self.assertTrue((root / CHECKLIST_NEW_NAME).is_file())
        self.assertFalse((root / CHECKLIST_LEGACY_NAME).exists())
        data = json.loads((root / CHECKLIST_NEW_NAME).read_text(encoding="utf-8"))
        errors, _ = validate_checklist(data)
        self.assertEqual(errors, [])
        item = data["items"][0]
        # The plan locator contract is unchanged: workspace-relative.
        self.assertEqual(item["plan_path"], "docs/plan.md")
        self.assertEqual(item["artifacts"]["plan"], "docs/plan.md")
        # Workspace registration and state point at the external root.
        self.assertEqual(result.workspace.harness_root, str(root.resolve()))
        state = json.loads((root / "harness-state.json").read_text(encoding="utf-8"))
        self.assertEqual(
            state["source"]["checklist_path"],
            str((root / CHECKLIST_NEW_NAME).resolve()),
        )
        self.assertEqual(
            state["source"]["checklist_sha256"],
            hashlib.sha256((root / CHECKLIST_NEW_NAME).read_bytes()).hexdigest(),
        )
        # DB/event recording is normal against the external root.
        events = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        self.assertTrue(any(e["event_type"] == "harness.initialized" for e in events))
        mirrors = list_task_mirrors(self.conn, "demo")
        self.assertEqual([m["task_id"] for m in mirrors], ["t1"])
        # Nothing leaks into the workspace tree.
        self.assertFalse((self.ws / "harness-checklist.json").exists())
        self.assertFalse((self.ws / "tasks" / "t1").exists())

    # --- minimal: phase/priority create-time contract -----------------------------

    def test_minimal_reserved_phase_zero_mutation(self):
        before = self._snapshot()
        for phase in ("running", "blocked", "done"):
            with self.assertRaises(ChecklistError):
                self._minimal_init(status=phase)
            self.assertEqual(self._snapshot(), before, phase)

    def test_minimal_whitespace_phase_zero_mutation(self):
        self._assert_minimal_init_fails_without_mutation(ChecklistError, status="done ")

    def test_minimal_invalid_priority_zero_mutation(self):
        self._assert_minimal_init_fails_without_mutation(ChecklistError, priority="p9")

    # --- minimal: success / compatibility paths ------------------------------------

    def test_minimal_none_creates_only_new_checklist(self):
        result = self._minimal_init()
        new_path = self.root / CHECKLIST_NEW_NAME
        self.assertTrue(new_path.is_file())
        self.assertFalse((self.root / CHECKLIST_LEGACY_NAME).exists())
        data = json.loads(new_path.read_text(encoding="utf-8"))
        errors, _ = validate_checklist(data)
        self.assertEqual(errors, [])
        self.assertEqual([item["id"] for item in data["items"]], ["t1"])
        self.assertEqual(result.workspace.harness_root, str(self.root.resolve()))
        self.assertTrue((self.root / "harness-state.json").is_file())
        self.assertTrue((self.root / "tasks" / "t1" / "plan.md").is_file())
        state = json.loads((self.root / "harness-state.json").read_text(encoding="utf-8"))
        self.assertEqual(
            state["source"]["checklist_sha256"],
            hashlib.sha256(new_path.read_bytes()).hexdigest(),
        )
        self.assertEqual(
            state["source"]["checklist_path"],
            "docs/project-harness/harness-checklist.json",
        )

    def test_minimal_new_only_reuses_existing_authority(self):
        self._write_checklist(CHECKLIST_NEW_NAME)
        self._minimal_init()
        self.assertFalse((self.root / CHECKLIST_LEGACY_NAME).exists())
        data = json.loads((self.root / CHECKLIST_NEW_NAME).read_text(encoding="utf-8"))
        self.assertEqual([item["id"] for item in data["items"]], ["t1"])

    def test_minimal_legacy_only_stays_compatible(self):
        legacy = self._write_checklist(CHECKLIST_LEGACY_NAME)
        self._minimal_init()
        self.assertFalse((self.root / CHECKLIST_NEW_NAME).exists())
        data = json.loads(legacy.read_text(encoding="utf-8"))
        self.assertEqual([item["id"] for item in data["items"]], ["t1"])

    def test_minimal_atomic_writer_failure_leaves_no_authority_or_corruption(self):
        # Failpoint on the unified atomic writer: the run must not leave an
        # authority file, must not corrupt pre-existing bytes, and must not
        # touch the DB.
        sentinel = self.root / "progress.md"
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text("sentinel\n", encoding="utf-8")
        before = self._snapshot()
        with patch(
            "coordinate.checklist_io.atomic_write_bytes",
            side_effect=OSError("write refused"),
        ):
            with self.assertRaises(OSError):
                self._minimal_init()
        after = self._snapshot()
        self.assertFalse((self.root / CHECKLIST_NEW_NAME).exists())
        self.assertFalse((self.root / CHECKLIST_LEGACY_NAME).exists())
        self.assertEqual(
            after["files"][str(sentinel.relative_to(self.ws))],
            before["files"][str(sentinel.relative_to(self.ws))],
        )
        self.assertEqual(list(self.root.glob(".*.tmp")), [])
        self.assertEqual(after["events"], before["events"])
        self.assertEqual(after["mirrors"], before["mirrors"])
        self.assertEqual(after["ops"], before["ops"])


class FullInitPreflightTests(unittest.TestCase):
    """P1-2: full init must preflight the checklist authority BEFORE copying
    the runtime or writing protocol files; failures leave zero mutation.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.ws = Path(self.tmp.name)
        self.hr = self.ws / "docs" / "project-harness"
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.ws),
            harness_root=str(self.hr),
        )
        # A real U1-style template source derived from Coordinate's vendored
        # runtime (never a fake two-file fixture: full init must render and
        # the copied runtime must really run validate/state).
        self.source = make_template_source(self.ws)

    def _snapshot(self):
        return _workspace_snapshot(self.ws, self.conn)

    def _write_checklist(self, name: str) -> Path:
        self.hr.mkdir(parents=True, exist_ok=True)
        path = self.hr / name
        path.write_text(_valid_empty_checklist_body(), encoding="utf-8")
        return path

    def _full_init(self, **overrides):
        kwargs = {"workspace_id": "demo", "source": str(self.source)}
        kwargs.update(overrides)
        return init_full_harness(self.conn, **kwargs)

    def _assert_full_init_fails_without_mutation(self, reason: str) -> None:
        before = self._snapshot()
        with self.assertRaises(ChecklistError) as ctx:
            self._full_init()
        self.assertEqual(ctx.exception.reason, reason)
        self.assertEqual(self._snapshot(), before)

    def test_full_dual_authority_zero_mutation(self):
        self._write_checklist(CHECKLIST_NEW_NAME)
        self._write_checklist(CHECKLIST_LEGACY_NAME)
        self._assert_full_init_fails_without_mutation(REASON_DUAL_AUTHORITY)
        # Nothing may reach the runtime/protocol destinations before refusal.
        self.assertFalse((self.ws / "scripts" / "harness" / "harnessctl").exists())
        self.assertFalse((self.hr / "scope.md").exists())
        self.assertFalse((self.hr / "harness-config.json").exists())

    def test_full_non_regular_candidate_zero_mutation(self):
        (self.hr / CHECKLIST_NEW_NAME).mkdir(parents=True)
        self._assert_full_init_fails_without_mutation(REASON_VALIDATION_ERROR)
        self.assertFalse((self.ws / "scripts" / "harness" / "harnessctl").exists())
        self.assertFalse((self.hr / "scope.md").exists())
        self.assertFalse((self.hr / "harness-config.json").exists())

    def test_full_invalid_existing_candidate_zero_mutation(self):
        path = self._write_checklist(CHECKLIST_NEW_NAME)
        data = json.loads(path.read_text(encoding="utf-8"))
        del data["items"]
        path.write_text(json.dumps(data), encoding="utf-8")
        self._assert_full_init_fails_without_mutation(REASON_VALIDATION_ERROR)
        self.assertFalse((self.ws / "scripts" / "harness" / "harnessctl").exists())

    def test_full_none_creates_only_new_checklist(self):
        result = self._full_init()
        new_path = self.hr / CHECKLIST_NEW_NAME
        self.assertTrue(new_path.is_file())
        self.assertFalse((self.hr / CHECKLIST_LEGACY_NAME).exists())
        data = json.loads(new_path.read_text(encoding="utf-8"))
        errors, _ = validate_checklist(data)
        self.assertEqual(errors, [])
        self.assertEqual(data["items"], [])
        self.assertTrue((self.ws / "scripts" / "harness" / "harnessctl").is_file())
        self.assertTrue((self.ws / "scripts" / "harness" / "harness_common.py").is_file())
        self.assertTrue((self.hr / "scope.md").is_file())
        state = json.loads((self.hr / "harness-state.json").read_text(encoding="utf-8"))
        self.assertEqual(
            state["source"]["checklist_sha256"],
            hashlib.sha256(new_path.read_bytes()).hexdigest(),
        )
        self.assertEqual(
            state["source"]["checklist_path"],
            "docs/project-harness/harness-checklist.json",
        )
        self.assertEqual(result.harness_root, str(self.hr.resolve()))
        self.assertTrue(result.harnessctl_path_updated)

    def test_full_legacy_only_stays_compatible(self):
        legacy = self._write_checklist(CHECKLIST_LEGACY_NAME)
        legacy_bytes = legacy.read_bytes()
        result = self._full_init()
        self.assertFalse((self.hr / CHECKLIST_NEW_NAME).exists())
        self.assertEqual(legacy.read_bytes(), legacy_bytes)
        self.assertTrue(any("legacy" in w.lower() for w in result.warnings))
        state = json.loads((self.hr / "harness-state.json").read_text(encoding="utf-8"))
        self.assertEqual(
            state["source"]["checklist_path"],
            "docs/project-harness/mvp-checklist.json",
        )


class FullInitRuntimeContractTests(unittest.TestCase):
    """P1-3: full init must render the U1 template against the registered
    harness root (or prove an already-rendered source compatible) BEFORE any
    mutation; the copied runtime must really run validate/state against the
    registered checklist. Unknown/residual placeholders, unprovable rendered
    sources, missing key runtime files and incompatible existing destinations
    all fail closed with zero file/DB/event mutation.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.ws = Path(self.tmp.name)

    def _make_workspace(self, harness_root_rel: str = "docs/project-harness") -> Path:
        hr = self.ws / harness_root_rel
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.ws),
            harness_root=str(hr),
        )
        return hr

    def _snapshot(self):
        return _workspace_snapshot(self.ws, self.conn)

    def _full_init(self, source, **overrides):
        kwargs = {"workspace_id": "demo", "source": str(source)}
        kwargs.update(overrides)
        return init_full_harness(self.conn, **kwargs)

    def _run_harnessctl(self, *args) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", "scripts/harness/harnessctl", *args],
            cwd=str(self.ws),
            capture_output=True,
            text=True,
            timeout=120,
        )

    def test_template_source_full_init_runs_validate_and_state(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        result = self._full_init(source)

        # No residual placeholder may survive into the copied runtime.
        self.assertGreater(len(result.scripts_copied), 0)
        for rel in result.scripts_copied:
            self.assertNotIn(
                "{{", (self.ws / rel).read_text(encoding="utf-8"), f"{rel} still templated"
            )

        checklist = hr / CHECKLIST_NEW_NAME
        self.assertTrue(checklist.is_file())
        data = json.loads(checklist.read_text(encoding="utf-8"))
        self.assertEqual(data["harness_root"], "docs/project-harness")

        validated = self._run_harnessctl("validate")
        self.assertEqual(validated.returncode, 0, msg=validated.stderr)

        help_text = self._run_harnessctl("--help")
        self.assertEqual(help_text.returncode, 0, msg=help_text.stderr)
        self.assertIn(
            "mark-done <item-id> [actor] [--verification TEXT] [--force --reason TEXT]",
            help_text.stdout,
        )

        refreshed = self._run_harnessctl("state")
        self.assertEqual(refreshed.returncode, 0, msg=refreshed.stderr)

        state = json.loads((hr / "harness-state.json").read_text(encoding="utf-8"))
        self.assertEqual(
            state["source"]["checklist_path"],
            "docs/project-harness/harness-checklist.json",
        )
        self.assertEqual(
            state["source"]["checklist_sha256"],
            hashlib.sha256(checklist.read_bytes()).hexdigest(),
        )

    def test_rendered_docs_source_against_project_harness_target_fails_closed(self):
        # The exact P1-3 repro: source is an already-rendered runtime (the
        # repo's own, embedding its own root) and the target harness root is
        # docs/project-harness. Must fail before any mutation.
        hr = self._make_workspace("docs/project-harness")
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(coordinate_runtime_dir())
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
        self.assertIn("U1 template source", str(ctx.exception))
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((self.ws / "scripts").exists())
        self.assertFalse((hr / "scope.md").exists())
        self.assertFalse((hr / "harness-config.json").exists())
        self.assertFalse((hr / CHECKLIST_NEW_NAME).exists())

    def test_rendered_source_matching_target_root_succeeds(self):
        # docs target + an already-rendered runtime that provably embeds docs:
        # the legacy compatibility path stays successful.
        values = rendered_runtime_values()
        hr = self._make_workspace(values["{{HARNESS_ROOT}}"])
        result = self._full_init(coordinate_runtime_dir())
        self.assertTrue((self.ws / "scripts" / "harness" / "harnessctl").is_file())
        self.assertTrue((hr / CHECKLIST_NEW_NAME).is_file())
        self.assertTrue(result.harnessctl_path_updated)

    def test_existing_destination_root_mismatch_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        scripts = self.ws / "scripts" / "harness"
        scripts.mkdir(parents=True)
        (scripts / "harness_common.py").write_text(
            'from pathlib import Path\n'
            'def project_root(): return Path(__file__).resolve().parents[2]\n'
            'def harness_root(): return project_root() / "docs"\n',
            encoding="utf-8",
        )
        (scripts / "harnessctl").write_text(
            'HARNESS_ROOT="$root/docs"\n', encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
        self.assertIn("refresh/clean", str(ctx.exception))
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((hr / "scope.md").exists())
        self.assertFalse((hr / "harness-config.json").exists())
        self.assertFalse((hr / CHECKLIST_NEW_NAME).exists())

    def test_existing_destination_unrendered_template_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        scripts = self.ws / "scripts" / "harness"
        scripts.mkdir(parents=True)
        (scripts / "harnessctl").write_text(
            'HARNESS_ROOT="$root/{{HARNESS_ROOT}}"\n', encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_TEMPLATE_PLACEHOLDER)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((hr / "harness-config.json").exists())

    def test_existing_destination_compatible_is_kept_not_overwritten(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        # First init renders the runtime for the target root; a second init
        # must treat the compatible existing runtime as existing (no blind
        # overwrite, no re-copy) and still succeed.
        first = self._full_init(source)
        self.assertIn("scripts/harness/harnessctl", first.scripts_copied)
        ctl_path = self.ws / "scripts" / "harness" / "harnessctl"
        ctl_bytes = ctl_path.read_bytes()

        second = self._full_init(source)
        self.assertEqual(second.scripts_copied, [])
        self.assertIn("scripts/harness/harnessctl", second.scripts_existing)
        self.assertEqual(ctl_path.read_bytes(), ctl_bytes)

    def test_unknown_placeholder_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        (source / "extra_plugin.py").write_text(
            "# {{BOGUS_PLACEHOLDER}}\n", encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_TEMPLATE_PLACEHOLDER)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((self.ws / "scripts").exists())
        self.assertFalse(hr.exists())

    def test_missing_key_runtime_file_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        (source / "build_harness_state.py").unlink()
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_SOURCE_INCOMPLETE)
        self.assertIn("build_harness_state.py", str(ctx.exception))
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((self.ws / "scripts").exists())

    def test_partially_rendered_key_file_fails_closed(self):
        # A template source whose harness_common.py was already rendered for a
        # different root must fail closed per file, never silently mix.
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        (source / "harness_common.py").write_text(
            'from pathlib import Path\n'
            'def project_root(): return Path(__file__).resolve().parents[2]\n'
            'def harness_root(): return project_root() / "docs"\n',
            encoding="utf-8",
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((self.ws / "scripts").exists())

    def test_dry_run_preflights_but_writes_nothing(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        before = self._snapshot()
        result = self._full_init(source, dry_run=True)
        self.assertGreater(len(result.scripts_copied), 0)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((self.ws / "scripts").exists())
        self.assertFalse(hr.exists())

    def test_dry_run_still_runs_compatibility_preflight(self):
        self._make_workspace("docs/project-harness")
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(coordinate_runtime_dir(), dry_run=True)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
        self.assertEqual(self._snapshot(), before)

    # --- R7: every existing destination collision needs the same proof -----

    def test_existing_key_file_bytes_mismatch_fails_closed(self):
        # validate_checklist.py is required by `harnessctl validate`; a stale
        # or damaged existing copy must never be silently kept.
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        scripts = self.ws / "scripts" / "harness"
        scripts.mkdir(parents=True)
        (scripts / "validate_checklist.py").write_text(
            "# stale copy\n", encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_DESTINATION_MISMATCH)
        self.assertIn("refresh/clean", str(ctx.exception))
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((hr / "scope.md").exists())
        self.assertFalse((hr / "harness-config.json").exists())
        self.assertFalse((hr / CHECKLIST_NEW_NAME).exists())

    def test_existing_other_runtime_file_mismatch_fails_closed(self):
        # A non-key runtime file that differs from the rendered bytes is also
        # unprovable as "the same runtime" and must fail closed.
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        scripts = self.ws / "scripts" / "harness"
        scripts.mkdir(parents=True)
        (scripts / "session_init.py").write_text(
            "# unrelated content\n", encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_DESTINATION_MISMATCH)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((hr / "scope.md").exists())
        self.assertFalse((hr / CHECKLIST_NEW_NAME).exists())

    def test_existing_collision_directory_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        scripts = self.ws / "scripts" / "harness"
        scripts.mkdir(parents=True)
        (scripts / "harnessctl").mkdir()
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((hr / "scope.md").exists())
        self.assertFalse((hr / CHECKLIST_NEW_NAME).exists())

    def test_existing_collision_dangling_symlink_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        scripts = self.ws / "scripts" / "harness"
        scripts.mkdir(parents=True)
        (scripts / "build_harness_state.py").symlink_to(self.ws / "missing-target")
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((hr / "scope.md").exists())
        self.assertFalse((hr / CHECKLIST_NEW_NAME).exists())

    def test_existing_collision_live_symlink_fails_closed(self):
        # P1-3: a live symlink whose target bytes are IDENTICAL to the rendered
        # runtime must still fail closed — the collision itself has to be a
        # regular file, so the non-following lstat/S_ISREG check refuses it.
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        # First init proves the rendered bytes, so the symlink target really is
        # byte-identical to what the second init would render.
        first = self._full_init(source)
        self.assertGreater(len(first.scripts_copied), 0)
        dst = self.ws / "scripts" / "harness" / "build_harness_state.py"
        self.assertTrue(dst.is_file())
        twin = self.ws / "twin-build_harness_state.py"
        twin.write_bytes(dst.read_bytes())
        dst.unlink()
        dst.symlink_to(twin)
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
        # The first init's protocol/checklist files and the symlink itself are
        # all untouched: the rejection happens in preflight, before any write.
        self.assertEqual(self._snapshot(), before)
        self.assertTrue(dst.is_symlink())
        self.assertEqual(os.readlink(dst), str(twin))

    # --- R7: any {{...}} shape is a placeholder ----------------------------

    def test_lowercase_placeholder_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        (source / "extra_plugin.py").write_text(
            "# {{lowercase}} must not be copied verbatim\n", encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_TEMPLATE_PLACEHOLDER)
        self.assertIn("{{lowercase}}", str(ctx.exception))
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((self.ws / "scripts").exists())
        self.assertFalse(hr.exists())

    def test_mixed_name_placeholder_fails_closed(self):
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        (source / "extra_plugin.py").write_text(
            "# {{MIXED-NAME}} must not be copied verbatim\n", encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_TEMPLATE_PLACEHOLDER)
        self.assertIn("{{MIXED-NAME}}", str(ctx.exception))
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((self.ws / "scripts").exists())
        self.assertFalse(hr.exists())

    def test_existing_destination_lowercase_placeholder_fails_closed(self):
        # The same residual rule applies to existing destination files.
        hr = self._make_workspace("docs/project-harness")
        source = make_template_source(self.ws)
        scripts = self.ws / "scripts" / "harness"
        scripts.mkdir(parents=True)
        (scripts / "harnessctl").write_text(
            'HARNESS_ROOT="$root/{{lowercase}}"\n', encoding="utf-8"
        )
        before = self._snapshot()
        with self.assertRaises(RuntimeSourceError) as ctx:
            self._full_init(source)
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_TEMPLATE_PLACEHOLDER)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse((hr / "harness-config.json").exists())


if __name__ == "__main__":
    unittest.main()
