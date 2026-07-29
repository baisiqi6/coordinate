"""Focused tests for the S4-C1 split-operation contract and task.create halves."""
from __future__ import annotations

import json
import os
import sqlite3
import stat
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from coordinate.db import initialize, row_to_dict, upsert_workspace
from coordinate.split_operations import (
    CONTRACT_VERSION,
    OPERATION_KIND_ISSUE_MATERIALIZE,
    OPERATION_KIND_TASK_CREATE,
    REASON_FILES_NOT_DEPLOYED,
    REASON_FINGERPRINT_DRIFT,
    REASON_LOCK_TIMEOUT,
    REASON_OPERATION_CONFLICT,
    REASON_VALIDATION_ERROR,
    SOURCE_KIND_ISSUE_TRIAGED_EVENT,
    STATUS_RECORD_APPLIED,
    ChecklistLock,
    IssueMaterializeRecordResult,
    SplitOperationError,
    _atomic_write_json,
    apply_issue_materialize_files,
    apply_issue_materialize_record,
    apply_task_create_files,
    apply_task_create_record,
    build_issue_materialize_envelope,
    build_issue_materialize_input_fingerprint,
    build_task_create_envelope,
    build_task_create_input_fingerprint,
    compute_plan_sha256,
    compute_task_item_fingerprint,
    project_checklist_item_for_fingerprint,
    validate_sha256,
    validate_uuid,
    validate_workspace_relative_path,
)

_KNOWN_INPUT_FINGERPRINT = (
    "0651e86d7266749d9ee1e3a4b2c8869724fa0846bb987fbc2ac7db436bfd6f05"
)
_KNOWN_ABSENT_FINGERPRINT = (
    "214b3c56ee5b4aed0e1e12eeff4537a6ade4f537480fb32e2337c002b00f56f3"
)


def _make_plan(path: Path, content: bytes = b"# plan\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


class ContractValidationTests(unittest.TestCase):
    def test_validate_uuid_accepts_canonical_lowercase(self) -> None:
        self.assertEqual(
            validate_uuid("12345678-1234-1234-1234-123456789ABC"),
            "12345678-1234-1234-1234-123456789abc",
        )

    def test_validate_uuid_rejects_non_canonical(self) -> None:
        with self.assertRaises(SplitOperationError) as ctx:
            validate_uuid("not-a-uuid")
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)

    def test_validate_sha256_accepts_lowercase(self) -> None:
        self.assertEqual(validate_sha256("a" * 64), "a" * 64)

    def test_validate_sha256_rejects_uppercase(self) -> None:
        with self.assertRaises(SplitOperationError) as ctx:
            validate_sha256("A" * 64)
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)

    def test_validate_workspace_relative_path_normalizes(self) -> None:
        self.assertEqual(
            validate_workspace_relative_path("plans/foo.md"),
            "plans/foo.md",
        )

    def test_validate_workspace_relative_path_rejects_dangerous_paths(self) -> None:
        for bad in ["../foo.md", "/abs/foo.md", "foo//bar.md", "foo\\bar.md", ""]:
            with self.subTest(path=bad):
                with self.assertRaises(SplitOperationError) as ctx:
                    validate_workspace_relative_path(bad)
                self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)


class CanonicalFingerprintTests(unittest.TestCase):
    def test_input_fingerprint_is_stable(self) -> None:
        fp = build_task_create_input_fingerprint(
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plans/foo.md",
            plan_sha256="a" * 64,
            title="Task Title",
            phase="ready",
            priority="p1",
        )
        self.assertEqual(fp, _KNOWN_INPUT_FINGERPRINT)

    def test_absent_item_fingerprint_is_stable(self) -> None:
        fp = compute_task_item_fingerprint(item=None, task_id="task-1")
        self.assertEqual(fp, _KNOWN_ABSENT_FINGERPRINT)

    def test_excluded_keys_do_not_affect_fingerprint(self) -> None:
        base = {
            "id": "task-1",
            "title": "T",
            "status": "todo",
            "phase": "ready",
            "priority": "p1",
            "owner": None,
            "human_gate_required": True,
            "plan_path": "plans/foo.md",
            "acceptance": "a",
            "blocked_by": [],
            "blocked_reason": "",
            "dependencies": [],
            "handoff": {"from": None, "to": None, "reason": None},
            "selected_in_session": None,
            "updated_at": "2026-07-13T12:00:00Z",
            "workflow": {"status": "todo", "branch": None, "updated_at": "2026-07-13T12:00:00Z"},
            "artifacts": {"plan": "plans/foo.md"},
            "verification": "",
            "review": {},
            "split_operation": {"operation_id": "x"},
            "completion_receipt": {"receipt_id": "y"},
        }
        fp_base = compute_task_item_fingerprint(item=base, task_id="task-1")

        mutated = dict(base)
        mutated["updated_at"] = "2099-01-01T00:00:00Z"
        mutated["verification"] = "different"
        mutated["split_operation"] = {"operation_id": "z"}
        mutated["completion_receipt"] = {"receipt_id": "w"}
        mutated["workflow"]["updated_at"] = "2099-01-01T00:00:00Z"
        fp_mutated = compute_task_item_fingerprint(item=mutated, task_id="task-1")

        self.assertEqual(fp_base, fp_mutated)

    def test_projection_sorts_keys(self) -> None:
        item = {"z": 1, "a": 2, "m": {"b": 1, "a": 2}}
        projected = project_checklist_item_for_fingerprint(item, "task-1")
        self.assertEqual(list(projected.keys()), ["a", "m", "z"])
        self.assertEqual(list(projected["m"].keys()), ["a", "b"])


class EnvelopeTests(unittest.TestCase):
    def test_envelope_has_expected_shape(self) -> None:
        envelope = build_task_create_envelope(
            operation_id="12345678-1234-1234-1234-123456789abc",
            workspace_id="demo",
            task_id="task-1",
            input_fingerprint="in",
            before_fingerprint="bef",
            after_fingerprint="aft",
            files_applied_at="2026-07-13T12:00:00Z",
        )
        self.assertEqual(envelope["contract_version"], CONTRACT_VERSION)
        self.assertEqual(envelope["operation_kind"], OPERATION_KIND_TASK_CREATE)
        self.assertEqual(envelope["target_kind"], "checklist_task")
        self.assertEqual(envelope["source_kind"], None)
        self.assertEqual(envelope["source_id"], None)


class ChecklistLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.checklist = self.tmp / "mvp-checklist.json"
        self.checklist.write_text("{}")

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_acquire_creates_and_release_removes_lock(self) -> None:
        lock = ChecklistLock(self.checklist, timeout=0.1, poll_interval=0.001)
        lock.acquire()
        self.assertTrue(lock.lock_path.exists())
        lock.release()
        self.assertFalse(lock.lock_path.exists())

    def test_live_owner_causes_timeout(self) -> None:
        # Pre-create a lock owned by a live process.
        lock_path = self.checklist.with_suffix(".json.lock")
        lock_path.write_text(json.dumps({"owner_pid": os.getpid(), "created_at": "x"}))

        lock = ChecklistLock(
            self.checklist,
            timeout=0.05,
            poll_interval=0.001,
            _process_alive=lambda _pid: True,
        )
        with self.assertRaises(SplitOperationError) as ctx:
            lock.acquire()
        self.assertEqual(ctx.exception.reason, REASON_LOCK_TIMEOUT)

    def test_stale_lock_is_broken(self) -> None:
        lock_path = self.checklist.with_suffix(".json.lock")
        lock_path.write_text(json.dumps({"owner_pid": 99999999, "created_at": "x"}))

        lock = ChecklistLock(
            self.checklist,
            timeout=0.1,
            poll_interval=0.001,
            _process_alive=lambda _pid: False,
        )
        lock.acquire()
        self.assertTrue(lock._owned)
        # Lock content now reflects current pid.
        content = json.loads(lock_path.read_text())
        self.assertEqual(content["owner_pid"], os.getpid())
        lock.release()

    def test_context_manager_releases_on_exception(self) -> None:
        lock = ChecklistLock(self.checklist, timeout=0.1, poll_interval=0.001)
        with self.assertRaises(RuntimeError):
            with lock:
                raise RuntimeError("boom")
        self.assertFalse(lock.lock_path.exists())

    def test_no_sleep_in_tests(self) -> None:
        # If _process_alive says live, the loop must hit the deadline without
        # relying on real wall-clock sleeps.
        lock_path = self.checklist.with_suffix(".json.lock")
        lock_path.write_text(json.dumps({"owner_pid": 1, "created_at": "x"}))

        sleeps: list[float] = []

        def fake_sleep(d: float) -> None:
            sleeps.append(d)
            # Advance fake time enough to trigger the timeout immediately.
            now[0] += 1.0

        now = [0.0]
        lock = ChecklistLock(
            self.checklist,
            timeout=0.5,
            poll_interval=0.05,
            _now=lambda: now[0],
            _process_alive=lambda _pid: True,
        )
        with patch("time.sleep", fake_sleep):
            with self.assertRaises(SplitOperationError) as ctx:
                lock.acquire()
        self.assertEqual(ctx.exception.reason, REASON_LOCK_TIMEOUT)
        self.assertTrue(sleeps)
        self.assertLess(sum(sleeps), 1.0)


class FileHalfTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.workspace_path = self.tmp / "workspace"
        self.workspace_path.mkdir()
        self.harness_root = self.tmp / "docs"
        self.harness_root.mkdir()
        self.plan = self.workspace_path / "plans" / "foo.md"
        _make_plan(self.plan)
        self.operation_id = str(uuid.uuid4())

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(self.tmp, ignore_errors=True)

    def _apply_files(self, **overrides: object) -> dict[str, object]:
        kwargs: dict[str, object] = dict(
            workspace_path=self.workspace_path,
            harness_root=self.harness_root,
            task_id="task-1",
            plan_doc="plans/foo.md",
            title="Task Title",
            phase="ready",
            priority="p1",
            operation_id=self.operation_id,
            workspace_id="demo",
        )
        kwargs.update(overrides)
        result = apply_task_create_files(**kwargs)
        return result.to_dict()

    def test_happy_path_creates_envelope_and_fingerprints(self) -> None:
        result = self._apply_files()
        self.assertTrue(result["checklist_changed"])
        self.assertEqual(result["operation_id"], self.operation_id)
        self.assertEqual(result["contract_version"], CONTRACT_VERSION)

        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist = json.loads(checklist_path.read_text())
        self.assertEqual(len(checklist["items"]), 1)
        item = checklist["items"][0]
        self.assertEqual(item["id"], "task-1")
        envelope = item["split_operation"]
        self.assertEqual(envelope["operation_id"], self.operation_id)
        self.assertEqual(envelope["input_fingerprint"], result["input_fingerprint"])
        self.assertEqual(envelope["before_fingerprint"], result["before_fingerprint"])
        self.assertEqual(envelope["after_fingerprint"], result["after_fingerprint"])

    def test_idempotent_retry_ignores_new_timestamp(self) -> None:
        # Explicit cross-second now values: retry must return the original
        # files_applied_at and must not rewrite the checklist.
        first = self._apply_files(now="2026-01-01T00:00:00Z")
        second = self._apply_files(now="2026-01-01T00:00:01Z")
        mtime_before = (self.harness_root / "mvp-checklist.json").stat().st_mtime

        # Force a filesystem-level change attempt; a correct retry leaves mtime alone.
        time.sleep(0.02)
        mtime_after = (self.harness_root / "mvp-checklist.json").stat().st_mtime

        self.assertFalse(second["checklist_changed"])
        self.assertEqual(first["input_fingerprint"], second["input_fingerprint"])
        self.assertEqual(first["after_fingerprint"], second["after_fingerprint"])
        self.assertEqual(first["files_applied_at"], "2026-01-01T00:00:00Z")
        self.assertEqual(second["files_applied_at"], "2026-01-01T00:00:00Z")
        self.assertEqual(mtime_before, mtime_after)

    def test_retry_with_different_operation_conflicts(self) -> None:
        self._apply_files()
        other_id = str(uuid.uuid4())
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files(operation_id=other_id)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_preexisting_unbound_task_conflicts(self) -> None:
        checklist = {
            "project": "p",
            "harness_root": str(self.harness_root),
            "version": 1,
            "items": [
                {
                    "id": "task-1",
                    "title": "Legacy",
                    "status": "todo",
                    "phase": "ready",
                    "priority": "p1",
                    "owner": None,
                    "human_gate_required": True,
                    "plan_path": "plans/foo.md",
                    "acceptance": "a",
                    "blocked_by": [],
                    "blocked_reason": "",
                    "dependencies": [],
                    "handoff": {"from": None, "to": None, "reason": None},
                    "selected_in_session": None,
                    "updated_at": "2026-07-13T12:00:00Z",
                    "workflow": {"status": "todo", "branch": None, "updated_at": "2026-07-13T12:00:00Z"},
                    "artifacts": {"plan": "plans/foo.md"},
                    "verification": "",
                    "review": {},
                }
            ],
        }
        (self.harness_root / "mvp-checklist.json").write_text(
            json.dumps(checklist, ensure_ascii=False, indent=2) + "\n"
        )
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_same_id_drift_is_conflict(self) -> None:
        self._apply_files()
        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist = json.loads(checklist_path.read_text())
        checklist["items"][0]["title"] = "Drifted"
        checklist_path.write_text(json.dumps(checklist, ensure_ascii=False, indent=2) + "\n")

        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files()
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_missing_plan_raises_files_not_deployed(self) -> None:
        self.plan.unlink()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files()
        self.assertEqual(ctx.exception.reason, REASON_FILES_NOT_DEPLOYED)

    def test_invalid_operation_id_raises_validation_error(self) -> None:
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files(operation_id="not-a-uuid")
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)

    def test_atomic_write_preserves_target_mode(self) -> None:
        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist_path.write_text("{}")
        os.chmod(checklist_path, 0o640)
        self._apply_files()
        self.assertEqual(stat.S_IMODE(checklist_path.stat().st_mode), 0o640)

    def test_atomic_write_cleans_temp_on_replace_failure(self) -> None:
        def boom(_src: str, _dst: str) -> None:
            raise OSError("replace refused")

        with patch("os.replace", boom):
            with self.assertRaises(OSError):
                self._apply_files()

        tmp_path = self.harness_root / ".mvp-checklist.json.tmp"
        self.assertFalse(tmp_path.exists())

    def test_two_tasks_are_independent(self) -> None:
        plan2 = self.workspace_path / "plans" / "bar.md"
        _make_plan(plan2)
        first = self._apply_files(task_id="task-1")
        second = self._apply_files(
            task_id="task-2",
            plan_doc="plans/bar.md",
            operation_id=str(uuid.uuid4()),
        )
        self.assertNotEqual(first["operation_id"], second["operation_id"])
        checklist = json.loads((self.harness_root / "mvp-checklist.json").read_text())
        self.assertEqual(len(checklist["items"]), 2)

    def test_concurrent_live_lock_blocks_apply(self) -> None:
        # Simulate another process owning the lock.
        lock_path = self.harness_root / "mvp-checklist.json.lock"
        lock_path.write_text(json.dumps({"owner_pid": 1, "created_at": "x"}))

        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files(_lock_timeout=0.05)
        self.assertEqual(ctx.exception.reason, REASON_LOCK_TIMEOUT)


class RecordHalfTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.workspace_path = self.tmp / "workspace"
        self.workspace_path.mkdir()
        self.harness_root = self.tmp / "docs"
        self.harness_root.mkdir()
        self.plan = self.workspace_path / "plans" / "foo.md"
        _make_plan(self.plan)

        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
        )
        self.operation_id = str(uuid.uuid4())
        files_result = apply_task_create_files(
            workspace_path=self.workspace_path,
            harness_root=self.harness_root,
            task_id="task-1",
            plan_doc="plans/foo.md",
            title="Task Title",
            phase="ready",
            priority="p1",
            operation_id=self.operation_id,
            workspace_id="demo",
        )
        self.files_result = files_result.to_dict()

    def tearDown(self) -> None:
        self.conn.close()
        import shutil

        shutil.rmtree(self.tmp, ignore_errors=True)

    def _apply_record(
        self,
        conn: sqlite3.Connection | None = None,
        **overrides: object,
    ) -> dict[str, object]:
        c = conn or self.conn
        kwargs: dict[str, object] = dict(
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plans/foo.md",
            title=None,
            phase="ready",
            owner=None,
            branch=None,
            actor="operator",
            target="worker",
            payload=None,
            operation_id=self.operation_id,
            input_fingerprint=self.files_result["input_fingerprint"],
            before_fingerprint=self.files_result["before_fingerprint"],
            after_fingerprint=self.files_result["after_fingerprint"],
        )
        kwargs.update(overrides)
        result = apply_task_create_record(c, **kwargs)
        return result.to_dict()

    def test_apply_record_creates_ledger_task_and_event(self) -> None:
        result = self._apply_record()
        self.assertTrue(result["event_created"])
        self.assertEqual(result["operation"]["operation_id"], self.operation_id)
        self.assertEqual(result["operation"]["status"], "record_applied")

        task = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("demo", "task-1"),
        ).fetchone()
        self.assertIsNotNone(task)
        payload = json.loads(task["payload_json"])
        self.assertEqual(payload["split_operation"]["operation_id"], self.operation_id)

        event = self.conn.execute(
            "SELECT * FROM events WHERE id = ?",
            (result["operation"]["record_event_id"],),
        ).fetchone()
        self.assertIsNotNone(event)
        event_payload = json.loads(event["payload_json"])
        self.assertEqual(event_payload["split_operation"]["operation_id"], self.operation_id)

    def test_idempotent_record_retry_returns_existing(self) -> None:
        first = self._apply_record()
        second = self._apply_record()
        self.assertFalse(second["event_created"])
        self.assertEqual(first["event"]["id"], second["event"]["id"])
        self.assertEqual(first["task"]["task_id"], second["task"]["task_id"])
        self.assertEqual(first["operation"]["operation_id"], second["operation"]["operation_id"])

    def test_files_not_deployed_when_checklist_missing(self) -> None:
        (self.harness_root / "mvp-checklist.json").unlink()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_FILES_NOT_DEPLOYED)

    def test_files_not_deployed_when_envelope_missing(self) -> None:
        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist = json.loads(checklist_path.read_text())
        del checklist["items"][0]["split_operation"]
        checklist_path.write_text(json.dumps(checklist, ensure_ascii=False, indent=2) + "\n")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_FILES_NOT_DEPLOYED)

    def test_operation_conflict_when_envelope_has_other_id(self) -> None:
        other_id = str(uuid.uuid4())
        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist = json.loads(checklist_path.read_text())
        checklist["items"][0]["split_operation"]["operation_id"] = other_id
        checklist_path.write_text(json.dumps(checklist, ensure_ascii=False, indent=2) + "\n")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_fingerprint_drift_on_supplied_input_mismatch(self) -> None:
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(input_fingerprint="b" * 64)
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_fingerprint_drift_on_plan_byte_change(self) -> None:
        self.plan.write_bytes(b"# changed plan\n")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_record_title_mismatch_is_drift(self) -> None:
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(title="Wrong Title")
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_record_phase_mismatch_is_drift(self) -> None:
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(phase="planned")
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_missing_promised_event_artifact_conflicts(self) -> None:
        result = self._apply_record()
        self.conn.execute("DELETE FROM events WHERE id = ?", (result["event"]["id"],))
        self.conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_with_changed_owner_conflicts(self) -> None:
        self._apply_record(owner="alice")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(owner="bob")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_with_changed_branch_conflicts(self) -> None:
        self._apply_record(branch="feature-1")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(branch="feature-2")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_with_changed_actor_conflicts(self) -> None:
        self._apply_record(actor="operator")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(actor="worker")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_with_changed_target_conflicts(self) -> None:
        self._apply_record(target="worker")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(target="reviewer")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_with_changed_payload_conflicts(self) -> None:
        self._apply_record(payload={"note": "a"})
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(payload={"note": "b"})
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_with_changed_custom_idempotency_key_conflicts(self) -> None:
        self._apply_record(idempotency_key="first-key")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(idempotency_key="second-key")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_custom_idempotency_key_is_operation_bound(self) -> None:
        # Two different operations using the same bare custom key must each
        # get a distinct plan.ready event, not collide through the bare key.
        first = self._apply_record(idempotency_key="shared-key")

        plan2 = self.workspace_path / "plans" / "bar.md"
        plan2.parent.mkdir(parents=True, exist_ok=True)
        plan2.write_bytes(b"# second plan\n")
        op2 = str(uuid.uuid4())
        files2 = apply_task_create_files(
            workspace_path=self.workspace_path,
            harness_root=self.harness_root,
            task_id="task-2",
            plan_doc="plans/bar.md",
            title="Task Two",
            phase="ready",
            priority="p1",
            operation_id=op2,
            workspace_id="demo",
        )
        second = apply_task_create_record(
            self.conn,
            workspace_id="demo",
            task_id="task-2",
            plan_doc="plans/bar.md",
            title=None,
            phase="ready",
            owner=None,
            branch=None,
            actor="operator",
            target="worker",
            payload=None,
            operation_id=op2,
            input_fingerprint=files2.input_fingerprint,
            before_fingerprint=files2.before_fingerprint,
            after_fingerprint=files2.after_fingerprint,
            idempotency_key="shared-key",
        )

        self.assertNotEqual(first["event"]["id"], second.event["id"])
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
            2,
        )
        # The ready key embeds the operation id so the bare custom key cannot
        # collide with an unrelated existing event.
        preexisting_event_id = str(uuid.uuid4())
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, 'plan.ready', 'operator', 'worker', 'task-1', ?, '{}', '2026-01-01T00:00:00Z')",
            (preexisting_event_id, "demo", "shared-key"),
        )
        self.conn.commit()
        third = self._apply_record(idempotency_key="shared-key")
        self.assertNotEqual(third["event"]["id"], preexisting_event_id)

    def test_envelope_missing_field_is_drift(self) -> None:
        self._patch_envelope({"after_fingerprint": None})  # remove key
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_envelope_extra_field_is_drift(self) -> None:
        self._patch_envelope({"extra_field": "x"})
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_envelope_wrong_operation_kind_is_drift(self) -> None:
        self._patch_envelope({"operation_kind": "issue.materialize"})
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_envelope_non_null_source_is_drift(self) -> None:
        self._patch_envelope({"source_kind": "issue_triaged_event", "source_id": "abc"})
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_envelope_files_applied_at_format_is_drift(self) -> None:
        bad_values = [
            "2026-01-01T00:00:00+00:00",  # offset instead of Z
            "2026-01-01T00:00:00",          # missing Z
            "2026-01-01T00:00:00.000Z",     # fractional seconds
            "not-a-timestamp",              # garbage
            123456,                          # wrong type
        ]
        for value in bad_values:
            with self.subTest(value=value):
                self._patch_envelope({"files_applied_at": value})
                before = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                with self.assertRaises(SplitOperationError) as ctx:
                    self._apply_record()
                self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)
                after = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                self.assertEqual(before, after, "envelope format failures must not write events")

    def test_retry_after_last_event_id_drift_conflicts(self) -> None:
        self._apply_record()
        wrong_event_id = "00000000-0000-0000-0000-000000000000"
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, 'plan.ready', 'operator', 'worker', 'task-1', ?, '{}', '2026-01-01T00:00:00Z')",
            (wrong_event_id, "demo", "wrong-key"),
        )
        self.conn.execute(
            "UPDATE tasks SET last_event_id = ? WHERE workspace_id = ? AND task_id = ?",
            (wrong_event_id, "demo", "task-1"),
        )
        self.conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_after_last_event_id_missing_conflicts(self) -> None:
        self._apply_record()
        self.conn.execute(
            "UPDATE tasks SET last_event_id = NULL WHERE workspace_id = ? AND task_id = ?",
            ("demo", "task-1"),
        )
        self.conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_after_pr_drift_conflicts(self) -> None:
        self._apply_record()
        self.conn.execute(
            "UPDATE tasks SET pr = ? WHERE workspace_id = ? AND task_id = ?",
            ("99", "demo", "task-1"),
        )
        self.conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_bound_key_unrelated_existing_event_is_conflict(self) -> None:
        # First apply creates the operation-bound event, then we strip the
        # ledger and task and poison the event to simulate an unrelated
        # pre-existing collision.
        first = self._apply_record(idempotency_key="custom")
        event_id = first["event"]["id"]
        self.conn.execute("DELETE FROM split_operations")
        self.conn.execute(
            "DELETE FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("demo", "task-1"),
        )
        self.conn.execute(
            "UPDATE events SET event_type = ?, actor = ?, target = ?, task_id = ?, payload_json = ? WHERE id = ?",
            ("wrong.type", "evil", "worker", "other", "{}", event_id),
        )
        self.conn.commit()

        before_events = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(idempotency_key="custom")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

        # The pre-existing event must survive unchanged and no partial state
        # (ledger or task) may be fabricated.
        after_events = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(after_events, before_events)
        retained = self.conn.execute(
            "SELECT event_type, actor, task_id, payload_json FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        self.assertEqual(retained["event_type"], "wrong.type")
        self.assertEqual(retained["actor"], "evil")
        self.assertEqual(retained["task_id"], "other")
        self.assertEqual(retained["payload_json"], "{}")
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
            0,
        )
        self.assertIsNone(
            self.conn.execute(
                "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                ("demo", "task-1"),
            ).fetchone(),
        )

    def test_bound_key_exact_event_without_ledger_is_conflict(self) -> None:
        # A matching-looking event without a ledger is still partial state and
        # must not be used to repair or bootstrap a new ledger.
        first = self._apply_record(idempotency_key="custom")
        event_id = first["event"]["id"]
        self.conn.execute("DELETE FROM split_operations")
        self.conn.execute(
            "DELETE FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("demo", "task-1"),
        )
        self.conn.commit()

        before_events = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(idempotency_key="custom")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

        after_events = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(after_events, before_events)
        self.assertEqual(
            self.conn.execute("SELECT id FROM events WHERE id = ?", (event_id,)).fetchone()["id"],
            event_id,
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
            0,
        )
        self.assertIsNone(
            self.conn.execute(
                "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                ("demo", "task-1"),
            ).fetchone(),
        )

    def test_different_ledger_for_same_target_conflicts(self) -> None:
        self._apply_record()
        # Same checklist task, different operation id and its own envelope.
        op2 = str(uuid.uuid4())
        self._rewrite_envelope_for_op(op2)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(operation_id=op2)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def _patch_envelope(self, overrides: dict[str, object]) -> None:
        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist = json.loads(checklist_path.read_text())
        envelope = checklist["items"][0]["split_operation"]
        for key in list(overrides.keys()):
            if overrides[key] is None:
                envelope.pop(key, None)
            else:
                envelope[key] = overrides[key]
        # Recompute supplied fingerprints to match the (possibly changed) envelope.
        if "input_fingerprint" in overrides and overrides["input_fingerprint"] is not None:
            self.files_result["input_fingerprint"] = str(overrides["input_fingerprint"])
        if "before_fingerprint" in overrides and overrides["before_fingerprint"] is not None:
            self.files_result["before_fingerprint"] = str(overrides["before_fingerprint"])
        if "after_fingerprint" in overrides and overrides["after_fingerprint"] is not None:
            self.files_result["after_fingerprint"] = str(overrides["after_fingerprint"])
        checklist_path.write_text(json.dumps(checklist, ensure_ascii=False, indent=2) + "\n")

    def _rewrite_envelope_for_op(self, operation_id: str) -> None:
        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist = json.loads(checklist_path.read_text())
        envelope = checklist["items"][0]["split_operation"]
        envelope["operation_id"] = operation_id
        checklist_path.write_text(json.dumps(checklist, ensure_ascii=False, indent=2) + "\n")

    def _insert_manual_plan_ready(
        self,
        conn: sqlite3.Connection,
        task_id: str = "task-1",
        payload: dict[str, object] | None = None,
    ) -> str:
        """Insert a standalone plan.ready event and return its id."""
        event_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, 'plan.ready', 'operator', 'worker', ?, ?, ?, ?)",
            (
                event_id,
                "demo",
                task_id,
                f"demo:manual:{task_id}:{event_id}",
                json.dumps(payload or {"note": "manual"}),
                "2026-01-01T00:00:00Z",
            ),
        )
        conn.commit()
        return event_id

    def _set_ready_supersedes(
        self,
        conn: sqlite3.Connection,
        ready_event_id: str,
        supersedes: str | None,
    ) -> None:
        """Overwrite the supersedes link in a plan.ready event payload."""
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM events WHERE id = ?", (ready_event_id,)
            ).fetchone()["payload_json"]
        )
        if supersedes is None:
            payload.pop("supersedes_plan_ready_event_id", None)
        else:
            payload["supersedes_plan_ready_event_id"] = supersedes
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), ready_event_id),
        )
        conn.commit()

    def test_retry_forged_supersedes_plan_ready_conflicts(self) -> None:
        """A stored supersedes link that does not match the derived prior ready is a forged provenance and must fail closed."""
        self._insert_manual_plan_ready(self.conn)
        result = self._apply_record()
        ready_id = result["event"]["id"]
        forged_id = str(uuid.uuid4())
        self._set_ready_supersedes(self.conn, ready_id, forged_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_cross_task_supersedes_plan_ready_conflicts(self) -> None:
        """A stored supersedes link pointing to a ready event for a different task is cross-task provenance and must fail closed."""
        self._insert_manual_plan_ready(self.conn, task_id="task-1")
        # Create a plan.ready for a different task to use as the cross-task id.
        cross_task_id = self._insert_manual_plan_ready(self.conn, task_id="task-2")
        result = self._apply_record()
        ready_id = result["event"]["id"]
        self._set_ready_supersedes(self.conn, ready_id, cross_task_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_missing_supersedes_when_prior_ready_existed_conflicts(self) -> None:
        """Deleting the supersedes link when a prior ready event existed is a missing-provenance tamper and must fail closed."""
        self._insert_manual_plan_ready(self.conn)
        result = self._apply_record()
        ready_id = result["event"]["id"]
        self._set_ready_supersedes(self.conn, ready_id, None)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_spurious_supersedes_when_none_existed_conflicts(self) -> None:
        """Adding a supersedes link when no prior ready existed is a spurious-provenance tamper and must fail closed."""
        result = self._apply_record()
        ready_id = result["event"]["id"]
        forged_id = str(uuid.uuid4())
        self._set_ready_supersedes(self.conn, ready_id, forged_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_retry_later_unrelated_ready_still_idempotent(self) -> None:
        """A later unrelated plan.ready event must not break an otherwise exact idempotent retry."""
        result = self._apply_record()
        ready_id = result["event"]["id"]
        # Simulate a later unrelated ready event for the same task.
        self._insert_manual_plan_ready(self.conn, task_id="task-1", payload={"note": "later"})
        second = self._apply_record()
        self.assertFalse(second["event_created"])
        self.assertEqual(second["event"]["id"], ready_id)

    def test_injected_failure_after_each_step_rolls_back(self) -> None:
        steps = [
            "insert_ledger",
            "upsert_mirror_initial",
            "append_event",
            "upsert_mirror_final",
            "link_ledger_event",
        ]
        for step in steps:
            with self.subTest(step=step):
                conn = initialize(":memory:")
                upsert_workspace(
                    conn,
                    workspace_id="demo",
                    name="Demo",
                    path=str(self.workspace_path),
                    harness_root=str(self.harness_root),
                )

                def fail_after(name: str) -> None:
                    if name == step:
                        raise RuntimeError(f"injected failure after {name}")

                with self.assertRaises(RuntimeError):
                    self._apply_record(
                        conn=conn,
                        _inject_after_step=fail_after,
                    )

                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                    0,
                )
                conn.close()


class CrossTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.workspace_path = self.tmp / "workspace"
        self.workspace_path.mkdir()
        self.harness_root = self.tmp / "docs"
        self.harness_root.mkdir()

        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
        )

    def tearDown(self) -> None:
        self.conn.close()
        import shutil

        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_two_different_tasks_record_independently(self) -> None:
        results = []
        for n in (1, 2):
            plan = self.workspace_path / f"plans/plan-{n}.md"
            _make_plan(plan)
            operation_id = str(uuid.uuid4())
            files = apply_task_create_files(
                workspace_path=self.workspace_path,
                harness_root=self.harness_root,
                task_id=f"task-{n}",
                plan_doc=f"plans/plan-{n}.md",
                title=f"Task {n}",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
                workspace_id="demo",
            )
            record = apply_task_create_record(
                self.conn,
                workspace_id="demo",
                task_id=f"task-{n}",
                plan_doc=f"plans/plan-{n}.md",
                title=None,
                phase="ready",
                owner=None,
                branch=None,
                actor="operator",
                target="worker",
                payload=None,
                operation_id=operation_id,
                input_fingerprint=files.input_fingerprint,
                before_fingerprint=files.before_fingerprint,
                after_fingerprint=files.after_fingerprint,
            )
            results.append(record)

        self.assertEqual(len(results), 2)
        self.assertNotEqual(results[0].operation["operation_id"], results[1].operation["operation_id"])
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
            2,
        )




class AtomicWriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.target = self.tmp / "mvp-checklist.json"
        self.target.write_text("ORIGINAL\n")

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_open_failure_leaves_target_unchanged_and_cleans_temp(self) -> None:
        def fail_open(path: str, *args: object, **kwargs: object) -> object:
            raise OSError("open refused")

        with patch("builtins.open", fail_open):
            with self.assertRaises(OSError):
                _atomic_write_json(self.target, {"items": []})

        self.assertEqual(self.target.read_text(), "ORIGINAL\n")
        self.assertFalse((self.tmp / ".mvp-checklist.json.tmp").exists())

    def test_fsync_failure_leaves_target_unchanged_and_cleans_temp(self) -> None:
        def fail_fsync(fd: int) -> None:
            raise OSError("fsync refused")

        with patch("os.fsync", fail_fsync):
            with self.assertRaises(OSError):
                _atomic_write_json(self.target, {"items": []})

        self.assertEqual(self.target.read_text(), "ORIGINAL\n")
        self.assertFalse((self.tmp / ".mvp-checklist.json.tmp").exists())

    def test_replace_failure_leaves_target_unchanged_and_cleans_temp(self) -> None:
        def boom(_src: str, _dst: str) -> None:
            raise OSError("replace refused")

        with patch("os.replace", boom):
            with self.assertRaises(OSError):
                _atomic_write_json(self.target, {"items": []})

        self.assertEqual(self.target.read_text(), "ORIGINAL\n")
        self.assertFalse((self.tmp / ".mvp-checklist.json.tmp").exists())


class IssueMaterializeOperationTests(unittest.TestCase):
    """C2 split-operation contract for issue.materialize."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.workspace_path = self.tmp / "workspace"
        self.workspace_path.mkdir()
        self.harness_root = self.tmp / "docs"
        self.harness_root.mkdir()
        self.plan = self.workspace_path / "plans" / "foo.md"
        _make_plan(self.plan)
        self.operation_id = str(uuid.uuid4())
        self.source_event_id = str(uuid.uuid4())

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(self.tmp, ignore_errors=True)

    def _apply_files(
        self,
        operation_id: str | None = None,
        source_event_id: str | None = None,
        **overrides: object,
    ) -> dict[str, object]:
        kwargs: dict[str, object] = dict(
            workspace_path=self.workspace_path,
            harness_root=self.harness_root,
            task_id="task-1",
            plan_doc="plans/foo.md",
            title="Task Title",
            phase="ready",
            priority="p1",
            operation_id=operation_id or self.operation_id,
            workspace_id="demo",
            source_event_id=source_event_id or self.source_event_id,
        )
        kwargs.update(overrides)
        result = apply_issue_materialize_files(**kwargs)
        return result.to_dict()

    def _seed_accepted_triage(self, conn: sqlite3.Connection, task_id: str = "task-1") -> str:
        from coordinate.db import upsert_workspace
        from coordinate.issues import triage_issue

        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
        )
        conn.execute(
            """
            INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at)
            VALUES (?, ?, 'issue.spotted', 'github', 'acme/repo', NULL, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                "demo",
                f"demo:github_issue:acme/repo:1:{task_id}",
                json.dumps(
                    {
                        "repo": "acme/repo",
                        "number": 1,
                        "url": "https://github.com/acme/repo/issues/1",
                        "title": "Bug",
                        "content_trust": "untrusted",
                    }
                ),
                "2026-07-13T12:00:00Z",
            ),
        )
        conn.commit()
        spotted_id = conn.execute(
            "SELECT id FROM events WHERE event_type = 'issue.spotted' AND workspace_id = ?",
            ("demo",),
        ).fetchone()["id"]
        triage = triage_issue(
            conn,
            workspace_id="demo",
            event_id=spotted_id,
            decision="accept",
            task_id=task_id,
        )
        return triage.event["id"]

    def _apply_record(
        self,
        conn: sqlite3.Connection,
        triage_event_id: str,
        files_result: dict[str, object],
        **overrides: object,
    ) -> IssueMaterializeRecordResult:
        kwargs: dict[str, object] = dict(
            workspace_id="demo",
            source_event_id=triage_event_id,
            task_id="task-1",
            plan_doc="plans/foo.md",
            operation_id=files_result["operation_id"],
            input_fingerprint=files_result["input_fingerprint"],
            before_fingerprint=files_result["before_fingerprint"],
            after_fingerprint=files_result["after_fingerprint"],
            actor="operator",
            target="worker",
        )
        kwargs.update(overrides)
        return apply_issue_materialize_record(conn, **kwargs)

    def test_input_fingerprint_is_stable(self) -> None:
        fp = build_issue_materialize_input_fingerprint(
            workspace_id="demo",
            task_id="task-1",
            source_id="12345678-1234-1234-1234-123456789abc",
            plan_doc="plans/foo.md",
            plan_sha256="a" * 64,
            title="Task Title",
            phase="ready",
            priority="p1",
        )
        self.assertEqual(
            fp,
            "3c9015454875e443edbac0d848b4f96f7a80f9114912e580dcfaa2101a31ae81",
        )

    def test_envelope_has_expected_shape(self) -> None:
        envelope = build_issue_materialize_envelope(
            operation_id="12345678-1234-1234-1234-123456789abc",
            workspace_id="demo",
            task_id="task-1",
            source_id="22345678-1234-1234-1234-123456789abc",
            input_fingerprint="in",
            before_fingerprint="bef",
            after_fingerprint="aft",
            files_applied_at="2026-07-13T12:00:00Z",
        )
        self.assertEqual(envelope["contract_version"], CONTRACT_VERSION)
        self.assertEqual(envelope["operation_kind"], OPERATION_KIND_ISSUE_MATERIALIZE)
        self.assertEqual(envelope["target_kind"], "checklist_task")
        self.assertEqual(envelope["source_kind"], SOURCE_KIND_ISSUE_TRIAGED_EVENT)
        self.assertEqual(envelope["source_id"], "22345678-1234-1234-1234-123456789abc")

    def test_files_happy_path_creates_envelope(self) -> None:
        result = self._apply_files()
        self.assertTrue(result["checklist_changed"])
        self.assertEqual(result["operation_kind"], OPERATION_KIND_ISSUE_MATERIALIZE)

        checklist = json.loads(
            (self.harness_root / "mvp-checklist.json").read_text(encoding="utf-8")
        )
        item = checklist["items"][0]
        envelope = item["split_operation"]
        self.assertEqual(envelope["operation_id"], self.operation_id)
        self.assertEqual(envelope["source_id"], self.source_event_id)
        self.assertEqual(envelope["input_fingerprint"], result["input_fingerprint"])

    def test_files_idempotent_retry(self) -> None:
        first = self._apply_files(now="2026-01-01T00:00:00Z")
        second = self._apply_files(now="2026-01-01T00:00:01Z")
        self.assertFalse(second["checklist_changed"])
        self.assertEqual(first["files_applied_at"], "2026-01-01T00:00:00Z")
        self.assertEqual(second["files_applied_at"], "2026-01-01T00:00:00Z")

    def test_files_missing_plan_raises_files_not_deployed(self) -> None:
        self.plan.unlink()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files()
        self.assertEqual(ctx.exception.reason, REASON_FILES_NOT_DEPLOYED)

    def test_files_preexisting_unbound_task_conflicts(self) -> None:
        checklist = {
            "project": "p",
            "items": [
                {
                    "id": "task-1",
                    "title": "Legacy",
                    "status": "todo",
                    "phase": "ready",
                    "priority": "p1",
                    "owner": None,
                    "human_gate_required": True,
                    "plan_path": "plans/foo.md",
                    "acceptance": "a",
                    "blocked_by": [],
                    "blocked_reason": "",
                    "dependencies": [],
                    "handoff": {"from": None, "to": None, "reason": None},
                    "selected_in_session": None,
                    "updated_at": "2026-07-13T12:00:00Z",
                    "workflow": {"status": "todo", "branch": None, "updated_at": "2026-07-13T12:00:00Z"},
                    "artifacts": {"plan": "plans/foo.md"},
                    "verification": "",
                    "review": {},
                }
            ],
        }
        (self.harness_root / "mvp-checklist.json").write_text(
            json.dumps(checklist, ensure_ascii=False, indent=2) + "\n"
        )
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_files_different_operation_conflicts(self) -> None:
        self._apply_files()
        other_op = str(uuid.uuid4())
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files(operation_id=other_op)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_files_different_source_conflicts(self) -> None:
        self._apply_files()
        other_source = str(uuid.uuid4())
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files(source_event_id=other_source)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_happy_path(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "issue.materialized")
        self.assertEqual(result.plan_ready_event["event_type"], "plan.ready")
        self.assertEqual(result.operation["operation_kind"], OPERATION_KIND_ISSUE_MATERIALIZE)
        self.assertEqual(result.operation["source_id"], triage_id)

    def test_record_idempotent(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        first = self._apply_record(conn, triage_id, files_result)
        second = self._apply_record(conn, triage_id, files_result)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])

    def test_record_files_not_deployed(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        (self.harness_root / "mvp-checklist.json").unlink()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_FILES_NOT_DEPLOYED)

    def test_record_reject_decision_fails(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps({"decision": "reject", "task_id": "task-1"}), triage_id),
        )
        conn.commit()
        files_result = self._apply_files(source_event_id=triage_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_fingerprint_drift_on_plan_change(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self.plan.write_bytes(b"# changed\n")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_record_title_mismatch_is_drift(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result, title="Wrong")
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_record_phase_mismatch_is_drift(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result, phase="planned")
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_injected_failure_rolls_back_and_preserves_accepted_mirror(self) -> None:
        steps = [
            "insert_ledger",
            "upsert_mirror_initial",
            "append_plan_ready",
            "append_materialized",
            "upsert_mirror_final",
            "link_ledger_event",
            "create_delivery",
        ]
        for step in steps:
            with self.subTest(step=step):
                conn = initialize(":memory:")
                task_id = f"task-{step}"
                triage_id = self._seed_accepted_triage(conn, task_id=task_id)
                accepted_before = row_to_dict(
                    conn.execute(
                        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                        ("demo", task_id),
                    ).fetchone()
                )
                operation_id = str(uuid.uuid4())
                files_result = self._apply_files(
                    operation_id=operation_id,
                    source_event_id=triage_id,
                    task_id=task_id,
                )

                def fail_after(name: str) -> None:
                    if name == step:
                        raise RuntimeError(f"injected failure after {name}")

                with self.assertRaises(RuntimeError):
                    self._apply_record(
                        conn,
                        triage_id,
                        files_result,
                        task_id=task_id,
                        _inject_after_step=fail_after,
                    )

                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM events WHERE event_type IN ('plan.ready', 'issue.materialized')"
                    ).fetchone()[0],
                    0,
                )
                accepted_after = row_to_dict(
                    conn.execute(
                        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                        ("demo", task_id),
                    ).fetchone()
                )
                self.assertEqual(accepted_before, accepted_after)
                conn.close()

    def test_two_issues_independent(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
        )
        from coordinate.issues import triage_issue

        results = []
        for n in (1, 2):
            plan = self.workspace_path / f"plans/plan-{n}.md"
            _make_plan(plan)
            spotted_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at)
                VALUES (?, ?, 'issue.spotted', 'github', 'acme/repo', NULL, ?, ?, ?)
                """,
                (
                    spotted_id,
                    "demo",
                    f"demo:spotted:{n}",
                    json.dumps({"repo": "acme/repo", "number": n, "title": f"Bug {n}", "content_trust": "untrusted"}),
                    "2026-07-13T12:00:00Z",
                ),
            )
            conn.commit()
            triage_id = triage_issue(
                conn,
                workspace_id="demo",
                event_id=spotted_id,
                decision="accept",
                task_id=f"task-{n}",
            ).event["id"]
            files = apply_issue_materialize_files(
                workspace_path=self.workspace_path,
                harness_root=self.harness_root,
                task_id=f"task-{n}",
                plan_doc=f"plans/plan-{n}.md",
                title=f"Task {n}",
                phase="ready",
                priority="p1",
                operation_id=str(uuid.uuid4()),
                workspace_id="demo",
                source_event_id=triage_id,
            )
            record = apply_issue_materialize_record(
                conn,
                workspace_id="demo",
                source_event_id=triage_id,
                task_id=f"task-{n}",
                plan_doc=f"plans/plan-{n}.md",
                operation_id=files.operation_id,
                input_fingerprint=files.input_fingerprint,
                before_fingerprint=files.before_fingerprint,
                after_fingerprint=files.after_fingerprint,
            )
            results.append(record)

        self.assertEqual(len(results), 2)
        self.assertNotEqual(
            results[0].operation["operation_id"], results[1].operation["operation_id"]
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
            2,
        )

    def test_same_source_with_different_operation_fails(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result)
        # Attempt a second ledger for the same triage source with a different
        # operation. The deployed envelope belongs to the first operation, so
        # the record half refuses before any write.
        conflicting = dict(files_result)
        conflicting["operation_id"] = str(uuid.uuid4())
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, conflicting)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_same_target_with_different_source_fails(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result)
        other_source = str(uuid.uuid4())
        conflicting = dict(files_result)
        conflicting["operation_id"] = str(uuid.uuid4())
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, other_source, conflicting)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    # -----------------------------------------------------------------------
    # File-half boundary tests beyond the baseline 19.
    # -----------------------------------------------------------------------

    def test_files_current_projection_drift_is_fingerprint_drift(self) -> None:
        """Changing the deployed checklist item under the same operation is drift."""
        self._apply_files()
        checklist_path = self.harness_root / "mvp-checklist.json"
        checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
        checklist["items"][0]["title"] = "Tampered"
        checklist_path.write_text(json.dumps(checklist, ensure_ascii=False, indent=2) + "\n")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files()
        self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_files_C1_envelope_on_same_task_conflicts(self) -> None:
        """A C1 task.create envelope is a different operation/source and conflicts."""
        c1_envelope = build_task_create_envelope(
            operation_id=str(uuid.uuid4()),
            workspace_id="demo",
            task_id="task-1",
            input_fingerprint="a" * 64,
            before_fingerprint="b" * 64,
            after_fingerprint="c" * 64,
            files_applied_at="2026-07-13T12:00:00Z",
        )
        checklist = {
            "project": "p",
            "items": [
                {
                    "id": "task-1",
                    "title": "Legacy",
                    "status": "todo",
                    "phase": "ready",
                    "priority": "p1",
                    "owner": None,
                    "human_gate_required": True,
                    "plan_path": "plans/foo.md",
                    "acceptance": "a",
                    "blocked_by": [],
                    "blocked_reason": "",
                    "dependencies": [],
                    "handoff": {"from": None, "to": None, "reason": None},
                    "selected_in_session": None,
                    "updated_at": "2026-07-13T12:00:00Z",
                    "workflow": {"status": "todo", "branch": None, "updated_at": "2026-07-13T12:00:00Z"},
                    "artifacts": {"plan": "plans/foo.md"},
                    "verification": "",
                    "review": {},
                    "split_operation": c1_envelope,
                }
            ],
        }
        (self.harness_root / "mvp-checklist.json").write_text(
            json.dumps(checklist, ensure_ascii=False, indent=2) + "\n"
        )
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files()
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_files_same_source_with_different_operation_conflicts(self) -> None:
        """The same triage source bound to two different operations is a conflict."""
        triage_id = str(uuid.uuid4())
        self._apply_files(source_event_id=triage_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files(operation_id=str(uuid.uuid4()), source_event_id=triage_id)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_files_lock_timeout_blocks_apply(self) -> None:
        """C2 reuses the shared advisory lock; a live owner causes lock_timeout."""
        lock_path = self.harness_root / "mvp-checklist.json.lock"
        lock_path.write_text(json.dumps({"owner_pid": 1, "created_at": "x"}))
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_files(_lock_timeout=0.05)
        self.assertEqual(ctx.exception.reason, REASON_LOCK_TIMEOUT)

    def test_files_atomic_write_failure_leaves_checklist_unchanged(self) -> None:
        """Shared atomic-write failure path: replace refusal leaves target unchanged."""
        self._apply_files()
        original = (self.harness_root / "mvp-checklist.json").read_bytes()

        with patch(
            "coordinate.split_operations.os.replace",
            side_effect=OSError("replace refused"),
        ):
            with self.assertRaises(OSError):
                self._apply_files(task_id="task-2", plan_doc="plans/foo.md")

        self.assertEqual((self.harness_root / "mvp-checklist.json").read_bytes(), original)
        self.assertFalse((self.harness_root / ".mvp-checklist.json.tmp").exists())

    # -----------------------------------------------------------------------
    # Record-half boundary tests beyond the baseline 19.
    # -----------------------------------------------------------------------

    def test_record_wrong_workspace_is_conflict(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        upsert_workspace(
            conn,
            workspace_id="other",
            name="Other",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
        )
        files_result = self._apply_files(source_event_id=triage_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result, workspace_id="other")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_wrong_event_type_is_conflict(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        conn.execute(
            "UPDATE events SET event_type = 'issue.spotted' WHERE id = ?",
            (triage_id,),
        )
        conn.commit()
        files_result = self._apply_files(source_event_id=triage_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_missing_triage_task_mirror_is_files_not_deployed(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        conn.execute(
            "DELETE FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("demo", "task-1"),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_FILES_NOT_DEPLOYED)

    def test_record_source_target_mismatch_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result, task_id="task-99")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_each_fingerprint_mismatch_is_drift(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        for field in ("input_fingerprint", "before_fingerprint", "after_fingerprint"):
            with self.subTest(field=field):
                bad = dict(files_result)
                bad[field] = "0" * 64
                with self.assertRaises(SplitOperationError) as ctx:
                    self._apply_record(conn, triage_id, bad)
                self.assertEqual(ctx.exception.reason, REASON_FINGERPRINT_DRIFT)

    def test_record_triage_payload_drift_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result)
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (
                json.dumps(
                    {
                        "decision": "accept",
                        "task_id": "task-1",
                        "source_event_id": str(uuid.uuid4()),
                        "repo": "acme/other",
                        "number": 99,
                        "url": "https://github.com/acme/other/issues/99",
                        "title": "Changed",
                        "content_trust": "untrusted",
                    }
                ),
                triage_id,
            ),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_missing_plan_ready_event_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result)
        conn.execute(
            "DELETE FROM events WHERE event_type = 'plan.ready' AND task_id = ?",
            ("task-1",),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_missing_materialized_event_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        conn.execute(
            "DELETE FROM events WHERE id = ?",
            (result.event["id"],),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_changed_destination_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(
            conn, triage_id, files_result, platform="discord", destination="#ops"
        )
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(
                conn, triage_id, files_result, platform="discord", destination="#other"
            )
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_no_delivery_intent(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        self.assertTrue(result.event_created)
        self.assertIsNone(result.delivery)
        self.assertIsNone(result.delivery_created)
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM deliveries WHERE event_id = ?",
                (result.event["id"],),
            ).fetchone()[0],
            0,
        )

    def test_record_delivery_created_in_same_transaction(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
            default_bus="discord",
            default_destination="#ops",
        )
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        self.assertTrue(result.event_created)
        self.assertTrue(result.delivery_created)
        self.assertIsNotNone(result.delivery)
        self.assertEqual(result.delivery["event_id"], result.event["id"])

    def test_record_retry_preserves_progressed_delivery(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
            default_bus="discord",
            default_destination="#ops",
        )
        files_result = self._apply_files(source_event_id=triage_id)
        first = self._apply_record(conn, triage_id, files_result)
        delivery_id = first.delivery["id"]
        conn.execute(
            "UPDATE deliveries SET status = ?, attempt_count = ? WHERE id = ?",
            ("sent", 3, delivery_id),
        )
        conn.commit()
        second = self._apply_record(conn, triage_id, files_result)
        self.assertFalse(second.event_created)
        self.assertEqual(second.delivery["status"], "sent")
        self.assertEqual(second.delivery["attempt_count"], 3)

    def test_record_delivery_drift_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
            default_bus="discord",
            default_destination="#ops",
        )
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        conn.execute(
            "UPDATE deliveries SET payload_json = ? WHERE id = ?",
            (json.dumps({"tampered": True}), result.delivery["id"]),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_later_delivery_args_do_not_create_absent_delivery(self) -> None:
        """A retry that would newly create a delivery is a changed intent."""
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(
                conn, triage_id, files_result, platform="discord", destination="#ops"
            )
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_preexisting_plan_ready_key_without_ledger_is_conflict(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        plan_content_hash = compute_plan_sha256(self.plan)[:16]
        ready_key = (
            f"demo:task-1:plan.ready:{files_result['operation_id']}:"
            f"{triage_id}:{plan_content_hash}"
        )
        from coordinate.db import append_event as db_append_event

        db_append_event(
            conn,
            workspace_id="demo",
            event_type="plan.ready",
            actor="operator",
            target="worker",
            task_id="task-1",
            idempotency_key=ready_key,
            payload={"note": "unrelated"},
        )
        prior = conn.execute(
            "SELECT * FROM events WHERE event_type = 'plan.ready' AND task_id = ?",
            ("task-1",),
        ).fetchone()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
        after = conn.execute(
            "SELECT * FROM events WHERE id = ?",
            (prior["id"],),
        ).fetchone()
        self.assertEqual(prior["payload_json"], after["payload_json"])

    def test_record_preexisting_materialized_key_without_ledger_is_conflict(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        materialized_key = (
            f"demo:issue.materialized:{triage_id}:"
            f"{files_result['operation_id']}:task-1:plans/foo.md"
        )
        from coordinate.db import append_event as db_append_event

        db_append_event(
            conn,
            workspace_id="demo",
            event_type="issue.materialized",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=materialized_key,
            payload={"note": "unrelated"},
        )
        prior = conn.execute(
            "SELECT * FROM events WHERE event_type = 'issue.materialized' AND task_id = ?",
            ("task-1",),
        ).fetchone()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
        after = conn.execute(
            "SELECT * FROM events WHERE id = ?",
            (prior["id"],),
        ).fetchone()
        self.assertEqual(prior["payload_json"], after["payload_json"])


    # -----------------------------------------------------------------------
    # Exact-replay acceptance: any drift in immutable intent is a hard conflict.
    # -----------------------------------------------------------------------

    def test_record_changed_owner_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result, owner="alice")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result, owner="bob")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_changed_branch_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result, branch="feature-1")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result, branch="feature-2")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_changed_actor_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result, actor="operator")
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result, actor="worker")
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_changed_platform_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
        )
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(
            conn, triage_id, files_result, platform="discord", destination="#ops"
        )
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(
                conn, triage_id, files_result, platform="kook", destination="#ops"
            )
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_task_mirror_payload_drift_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        self._apply_record(conn, triage_id, files_result)
        conn.execute(
            "UPDATE tasks SET payload_json = ? WHERE workspace_id = ? AND task_id = ?",
            (json.dumps({"tampered": True}), "demo", "task-1"),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_task_mirror_column_drift_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        # last_event_id is a foreign key; use a real decoy event id.
        decoy_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, 'issue.spotted', 'github', 'acme/repo', NULL, ?, ?, ?)",
            (decoy_id, "demo", "demo:decoy:last_event", json.dumps({}), "2026-07-13T12:00:00Z"),
        )
        conn.commit()
        drift_cases = [
            ("phase", "planned"),
            ("owner", "other-owner"),
            ("branch", "other-branch"),
            ("pr", "123"),
            ("last_event_id", decoy_id),
        ]
        for column, value in drift_cases:
            with self.subTest(column=column):
                conn.execute(
                    f"UPDATE tasks SET {column} = ? WHERE workspace_id = ? AND task_id = ?",
                    (value, "demo", "task-1"),
                )
                conn.commit()
                with self.assertRaises(SplitOperationError) as ctx:
                    self._apply_record(conn, triage_id, files_result)
                self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
                # Restore accepted state for the next subtest.
                conn.execute(
                    "UPDATE tasks SET phase = ?, owner = NULL, branch = NULL, pr = NULL, last_event_id = ? "
                    "WHERE workspace_id = ? AND task_id = ?",
                    ("ready", result.event["id"], "demo", "task-1"),
                )
                conn.commit()

    def test_record_ledger_field_drift_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        operation_id = files_result["operation_id"]

        # Insert a decoy event to use as a wrong record_event_id.
        decoy_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, 'issue.spotted', 'github', 'acme/repo', NULL, ?, ?, ?)",
            (decoy_id, "demo", "demo:decoy", json.dumps({}), "2026-07-13T12:00:00Z"),
        )
        conn.commit()

        drift_cases = [
            ("status", "'applied'"),
            ("input_fingerprint", "'" + "0" * 64 + "'"),
            ("before_fingerprint", "'" + "0" * 64 + "'"),
            ("after_fingerprint", "'" + "0" * 64 + "'"),
            ("source_id", "'" + str(uuid.uuid4()) + "'"),
            ("target_id", "'task-99'"),
            ("record_event_id", "'" + decoy_id + "'"),
        ]
        for column, literal in drift_cases:
            with self.subTest(column=column):
                conn.execute(
                    f"UPDATE split_operations SET {column} = {literal} WHERE operation_id = ?",
                    (operation_id,),
                )
                conn.commit()
                with self.assertRaises(SplitOperationError) as ctx:
                    self._apply_record(conn, triage_id, files_result)
                self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
                # Restore accepted ledger state.
                conn.execute(
                    """
                    UPDATE split_operations SET
                        status = ?,
                        input_fingerprint = ?,
                        before_fingerprint = ?,
                        after_fingerprint = ?,
                        source_id = ?,
                        target_id = ?,
                        record_event_id = ?
                    WHERE operation_id = ?
                    """,
                    (
                        STATUS_RECORD_APPLIED,
                        files_result["input_fingerprint"],
                        files_result["before_fingerprint"],
                        files_result["after_fingerprint"],
                        triage_id,
                        "task-1",
                        result.event["id"],
                        operation_id,
                    ),
                )
                conn.commit()

    def test_record_plan_ready_wrong_metadata_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        ready_id = result.plan_ready_event["id"]
        original = conn.execute(
            "SELECT actor, target, idempotency_key FROM events WHERE id = ?", (ready_id,)
        ).fetchone()
        baseline = {
            "actor": original["actor"],
            "target": original["target"],
            "idempotency_key": original["idempotency_key"],
        }

        metadata_cases = [
            ("actor", "attacker"),
            ("target", "attacker"),
            ("idempotency_key", "tampered-key"),
        ]
        for column, value in metadata_cases:
            with self.subTest(column=column):
                conn.execute(
                    f"UPDATE events SET {column} = ? WHERE id = ?",
                    (value, ready_id),
                )
                conn.commit()
                with self.assertRaises(SplitOperationError) as ctx:
                    self._apply_record(conn, triage_id, files_result)
                self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
                # Restore the single drifted column from the saved baseline so
                # each subtest isolates exactly one metadata field.
                conn.execute(
                    "UPDATE events SET actor = ?, target = ?, idempotency_key = ? WHERE id = ?",
                    (baseline["actor"], baseline["target"], baseline["idempotency_key"], ready_id),
                )
                conn.commit()

    def test_record_plan_ready_wrong_payload_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        ready_id = result.plan_ready_event["id"]
        original = conn.execute("SELECT * FROM events WHERE id = ?", (ready_id,)).fetchone()
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps({"tampered": True}), ready_id),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
        # Restore for clarity if the test is read as a narrative.
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (original["payload_json"], ready_id),
        )
        conn.commit()

    def test_record_materialized_wrong_metadata_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        event_id = result.event["id"]
        original = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()

        metadata_cases = [
            ("actor", "attacker"),
            ("target", "attacker"),
            ("idempotency_key", "tampered-key"),
        ]
        for column, value in metadata_cases:
            with self.subTest(column=column):
                conn.execute(
                    f"UPDATE events SET {column} = ? WHERE id = ?",
                    (value, event_id),
                )
                conn.commit()
                with self.assertRaises(SplitOperationError) as ctx:
                    self._apply_record(conn, triage_id, files_result)
                self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
                conn.execute(
                    "UPDATE events SET actor = ?, target = ?, idempotency_key = ? WHERE id = ?",
                    (original["actor"], original["target"], original["idempotency_key"], event_id),
                )
                conn.commit()

    def test_record_materialized_wrong_payload_conflicts(self) -> None:
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        event_id = result.event["id"]
        original = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps({"tampered": True}), event_id),
        )
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (original["payload_json"], event_id),
        )
        conn.commit()

    def test_record_promised_delivery_missing_conflicts(self) -> None:
        """Deleting a rendered delivery row makes the exact replay fail closed."""
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
            default_bus="discord",
            default_destination="#ops",
        )
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        self.assertIsNotNone(result.delivery)
        conn.execute("DELETE FROM deliveries WHERE id = ?", (result.delivery["id"],))
        conn.commit()
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_preexisting_delivery_without_ledger_fails_closed(self) -> None:
        """A pre-existing delivery row returned by policy on a fresh record
        transaction is a hard idempotency collision: fail closed, rollback
        the ledger/task/events, and leave both the accepted mirror and the
        pre-existing delivery untouched.
        """
        from coordinate.policy import PolicyDeliveryResult

        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
            default_bus="discord",
            default_destination="#ops",
        )
        files_result = self._apply_files(source_event_id=triage_id)

        accepted_before = row_to_dict(
            conn.execute(
                "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                ("demo", "task-1"),
            ).fetchone()
        )

        # Pre-create a decoy delivery row. create_delivery_for_event will be
        # patched to claim it already existed for the new event's message_key.
        decoy_delivery_id = str(uuid.uuid4())
        now = "2026-07-13T12:00:00Z"
        conn.execute(
            """
            INSERT INTO deliveries (
                id, event_id, platform, destination, message_key, status,
                platform_message_id, attempt_count, last_error, payload_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decoy_delivery_id,
                triage_id,
                "discord",
                "#ops",
                "demo:decoy:message-key",
                "pending",
                None,
                0,
                None,
                json.dumps({"note": "pre-existing"}),
                now,
                now,
            ),
        )
        conn.commit()
        decoy_before = row_to_dict(
            conn.execute("SELECT * FROM deliveries WHERE id = ?", (decoy_delivery_id,)).fetchone()
        )

        def fake_create_delivery_for_event(
            c: sqlite3.Connection,
            event_id: str,
            *,
            platform: str,
            destination: str,
            commit: bool = True,
        ) -> PolicyDeliveryResult:
            del c, event_id, platform, destination, commit
            decoy_row = conn.execute(
                "SELECT * FROM deliveries WHERE id = ?", (decoy_delivery_id,)
            ).fetchone()
            return PolicyDeliveryResult(
                supported=True,
                created=False,
                skipped=False,
                event=None,
                delivery=row_to_dict(decoy_row),
                payload=None,
                message_key="demo:decoy:message-key",
                reason=None,
            )

        with patch(
            "coordinate.policy.create_delivery_for_event",
            fake_create_delivery_for_event,
        ):
            with self.assertRaises(SplitOperationError) as ctx:
                self._apply_record(conn, triage_id, files_result)

        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)
        self.assertIn("without an exact ledger", str(ctx.exception))

        # Accepted task mirror is preserved byte/column-for-column.
        accepted_after = row_to_dict(
            conn.execute(
                "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                ("demo", "task-1"),
            ).fetchone()
        )
        self.assertEqual(accepted_before, accepted_after)

        # No ledger or new materialize artifacts were written.
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0],
            0,
        )
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type IN ('plan.ready', 'issue.materialized')"
            ).fetchone()[0],
            0,
        )

        # Pre-existing delivery row is untouched.
        decoy_after = row_to_dict(
            conn.execute("SELECT * FROM deliveries WHERE id = ?", (decoy_delivery_id,)).fetchone()
        )
        self.assertEqual(decoy_before, decoy_after)

    def _insert_manual_plan_ready(
        self,
        conn: sqlite3.Connection,
        task_id: str = "task-1",
        payload: dict[str, object] | None = None,
    ) -> str:
        """Insert a standalone plan.ready event and return its id."""
        event_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO events (id, workspace_id, event_type, actor, target, task_id, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, 'plan.ready', 'operator', 'worker', ?, ?, ?, ?)",
            (
                event_id,
                "demo",
                task_id,
                f"demo:manual:{task_id}:{event_id}",
                json.dumps(payload or {"note": "manual"}),
                "2026-01-01T00:00:00Z",
            ),
        )
        conn.commit()
        return event_id

    def _set_ready_supersedes(
        self,
        conn: sqlite3.Connection,
        ready_event_id: str,
        supersedes: str | None,
    ) -> None:
        """Overwrite the supersedes link in a plan.ready event payload."""
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM events WHERE id = ?", (ready_event_id,)
            ).fetchone()["payload_json"]
        )
        if supersedes is None:
            payload.pop("supersedes_plan_ready_event_id", None)
        else:
            payload["supersedes_plan_ready_event_id"] = supersedes
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), ready_event_id),
        )
        conn.commit()

    def test_record_retry_forged_supersedes_plan_ready_conflicts(self) -> None:
        """A stored supersedes link that does not match the derived prior ready is a forged provenance and must fail closed."""
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        self._insert_manual_plan_ready(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        ready_id = result.plan_ready_event["id"]
        forged_id = str(uuid.uuid4())
        self._set_ready_supersedes(conn, ready_id, forged_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_retry_cross_task_supersedes_plan_ready_conflicts(self) -> None:
        """A stored supersedes link pointing to a ready event for a different task is cross-task provenance and must fail closed."""
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        self._insert_manual_plan_ready(conn, task_id="task-1")
        cross_task_id = self._insert_manual_plan_ready(conn, task_id="task-2")
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        ready_id = result.plan_ready_event["id"]
        self._set_ready_supersedes(conn, ready_id, cross_task_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_retry_missing_supersedes_when_prior_ready_existed_conflicts(self) -> None:
        """Deleting the supersedes link when a prior ready event existed is a missing-provenance tamper and must fail closed."""
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        self._insert_manual_plan_ready(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        ready_id = result.plan_ready_event["id"]
        self._set_ready_supersedes(conn, ready_id, None)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_retry_spurious_supersedes_when_none_existed_conflicts(self) -> None:
        """Adding a supersedes link when no prior ready existed is a spurious-provenance tamper and must fail closed."""
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        ready_id = result.plan_ready_event["id"]
        forged_id = str(uuid.uuid4())
        self._set_ready_supersedes(conn, ready_id, forged_id)
        with self.assertRaises(SplitOperationError) as ctx:
            self._apply_record(conn, triage_id, files_result)
        self.assertEqual(ctx.exception.reason, REASON_OPERATION_CONFLICT)

    def test_record_retry_later_unrelated_ready_still_idempotent(self) -> None:
        """A later unrelated plan.ready event must not break an otherwise exact idempotent retry."""
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        triage_id = self._seed_accepted_triage(conn)
        files_result = self._apply_files(source_event_id=triage_id)
        result = self._apply_record(conn, triage_id, files_result)
        ready_id = result.plan_ready_event["id"]
        self._insert_manual_plan_ready(conn, task_id="task-1", payload={"note": "later"})
        second = self._apply_record(conn, triage_id, files_result)
        self.assertFalse(second.event_created)
        self.assertEqual(second.plan_ready_event["id"], ready_id)


if __name__ == "__main__":
    unittest.main()
