"""Tests for ``coordinate.onboarding`` — esp. backlog #9c (plan.ready versioning)."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinate.db import (
    get_workspace,
    initialize,
    list_events,
    list_task_mirrors,
    row_to_dict,
    upsert_workspace,
)
from coordinate.onboarding import create_plan_task, create_plan_task_record
from coordinate.plan_gate import approve_plan, reject_plan
from coordinate.projection_doctor import diagnose_projections
from coordinate.split_operations import (
    CONTRACT_VERSION,
    OPERATION_KIND_TASK_CREATE,
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

    def _plan_ready_events(self):
        return [
            row_to_dict(e)
            for e in list_events(self.conn, "demo")
            if row_to_dict(e)["event_type"] == "plan.ready"
        ]

    def test_plan_content_change_creates_new_plan_ready_event(self):
        # 9c: revising the plan doc (same task) must produce a new plan.ready
        # event, not be de-duped by a fixed idempotency key.
        create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path))

        self.plan_path.write_text("# Plan v2 — revised\n")  # content changed
        create_plan_task(self.conn, workspace_id="demo", task_id="t1", plan_doc=str(self.plan_path))

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
            priority="high",
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
        self.plan_path.write_text(content)
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


if __name__ == "__main__":
    unittest.main()
