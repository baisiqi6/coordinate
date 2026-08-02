"""S4-D projection doctor focused tests."""
from __future__ import annotations

import dataclasses
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from collections import UserDict
from pathlib import Path
from types import MappingProxyType
from typing import Any

from coordinate.db import (
    Workspace,
    append_event,
    initialize,
    insert_split_operation,
    list_split_operations,
    set_workspace_agent,
    sync_workspace_agents,
    upsert_workspace,
)
from coordinate.projection_doctor import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    Finding,
    ProjectionReport,
    diagnose_projections,
)
from coordinate.agent_registry import parse_agents_toml
from coordinate.split_operations import (
    OPERATION_KIND_TASK_CREATE,
    STATUS_RECORD_APPLIED,
    TARGET_KIND_CHECKLIST_TASK,
    apply_task_create_files,
    apply_task_create_record,
    compute_plan_sha256,
)


class ProjectionDoctorTestBase(unittest.TestCase):
    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        return conn

    def _make_workspace(self, conn, tmp, harness_root=None):
        hr = harness_root or os.path.join(tmp, "harness")
        Path(hr).mkdir(parents=True, exist_ok=True)
        # Seed a validator-passing empty checklist (the init contract shape) so
        # file-half mutations resolve and validate the current checklist.
        (Path(hr) / "mvp-checklist.json").write_text(
            json.dumps({
                "project": "demo",
                "harness_root": ".",
                "version": 1,
                "updated_at": "2026-07-13",
                "items": [],
            }, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=tmp,
            harness_root=hr,
            default_bus="discord",
            default_destination="#general",
        )
        return Workspace(
            id="demo",
            name="Demo",
            path=tmp,
            harness_root=hr,
            default_bus="discord",
            default_destination="#general",
        )

    def _make_plan(self, workspace_path, rel_path="plans/plan.md", content=b"# plan\n"):
        plan = Path(workspace_path) / rel_path
        plan.parent.mkdir(parents=True, exist_ok=True)
        plan.write_bytes(content)
        return plan

    def _manifest(self, root: Path) -> dict[str, bytes]:
        manifest: dict[str, bytes] = {}
        for path in sorted(root.rglob("*")):
            if path.is_file():
                manifest[str(path.relative_to(root))] = path.read_bytes()
        return manifest

    @staticmethod
    def _evidence_dict(finding: Finding) -> dict[str, Any]:
        """Local helper: map a Finding's immutable evidence to a key/value dict."""
        return {e["key"]: e["value"] for e in finding.evidence}


class RegistryFindingsTest(ProjectionDoctorTestBase):
    def test_clean_registry_produces_no_findings(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            source = Path(tmp) / "agents.toml"
            source.write_text(
                '[registry]\nid = "multinexus.discord"\nversion = 1\n\n'
                '[[agents]]\nid = "mac-codex"\ndisplay_name = "Mac Codex"\n'
                'discord_user_id = "1001"\nagent_type = "managed"\n',
                encoding="utf-8",
            )
            parsed = parse_agents_toml(source)
            sync_workspace_agents(
                conn,
                workspace_id="demo",
                source_id="multinexus.discord",
                source_version=1,
                source_hash=parsed.source.source_hash or compute_plan_sha256(source),
                source_path=str(source),
                entries=[{
                    "id": "mac-codex",
                    "discord_user_id": "1001",
                    "display_name": "Mac Codex",
                    "agent_type": "managed",
                }],
                replace=True,
                synced_by="operator",
            )
            report = diagnose_projections(conn, ws, now="2099-01-01T00:00:00Z")
            self.assertTrue(report.ok)
            self.assertEqual(report.summary["findings"], 0)

    def test_registry_source_missing(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            conn.execute(
                "INSERT INTO workspace_agent_registry_entries "
                "(workspace_id, agent_name, entry_kind, discord_user_id, display_name, agent_type, created_at, updated_at) "
                "VALUES (?, ?, 'authoritative', ?, ?, ?, 'x', 'x')",
                ("demo", "mac-codex", "1001", "Mac Codex", "managed"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "registry_source_missing")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_registry_source_unreadable(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            source_path = "/nonexistent/agents.toml"
            conn.execute(
                "INSERT INTO workspace_agent_registry_sources "
                "(workspace_id, source_id, source_version, source_hash, source_path, synced_by, synced_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("demo", "multinexus.discord", 1, "a" * 64, source_path, "op", "x"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "registry_source_unreadable")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_WARNING)

    def test_registry_source_identity_mismatch(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            source = Path(tmp) / "agents.toml"
            source.write_text(
                '[registry]\nid = "multinexus.discord"\nversion = 2\n\n'
                '[[agents]]\nid = "mac-codex"\ndisplay_name = "Mac Codex"\n'
                'discord_user_id = "1001"\nagent_type = "managed"\n',
                encoding="utf-8",
            )
            conn.execute(
                "INSERT INTO workspace_agent_registry_sources "
                "(workspace_id, source_id, source_version, source_hash, source_path, synced_by, synced_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("demo", "multinexus.discord", 1, "b" * 64, str(source), "op", "x"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "registry_source_identity_mismatch")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertTrue(f.repairable)

    def test_registry_projection_stale(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            source = Path(tmp) / "agents.toml"
            source.write_text(
                '[registry]\nid = "multinexus.discord"\nversion = 1\n\n'
                '[[agents]]\nid = "mac-codex"\ndisplay_name = "Mac Codex"\n'
                'discord_user_id = "1001"\nagent_type = "managed"\n',
                encoding="utf-8",
            )
            sync_workspace_agents(
                conn,
                workspace_id="demo",
                source_id="multinexus.discord",
                source_version=1,
                source_hash="a" * 64,
                source_path=str(source),
                entries=[{
                    "id": "mac-codex",
                    "discord_user_id": "1001",
                    "display_name": "Mac Codex",
                    "agent_type": "managed",
                }],
                replace=True,
                synced_by="operator",
            )
            # Corrupt agents_json projection manually (no schema change).
            conn.execute(
                "UPDATE workspaces SET agents_json = ? WHERE id = ?",
                (json.dumps({"other": {"discord_user_id": "1"}}), "demo"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "registry_projection_stale")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_registry_override_shadowed(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            source = Path(tmp) / "agents.toml"
            source.write_text(
                '[registry]\nid = "multinexus.discord"\nversion = 1\n\n'
                '[[agents]]\nid = "mac-codex"\ndisplay_name = "Mac Codex"\n'
                'discord_user_id = "1001"\nagent_type = "managed"\n',
                encoding="utf-8",
            )
            sync_workspace_agents(
                conn,
                workspace_id="demo",
                source_id="multinexus.discord",
                source_version=1,
                source_hash="a" * 64,
                source_path=str(source),
                entries=[{
                    "id": "mac-codex",
                    "discord_user_id": "1001",
                    "display_name": "Mac Codex",
                    "agent_type": "managed",
                }],
                replace=True,
                synced_by="operator",
            )
            set_workspace_agent(
                conn,
                workspace_id="demo",
                agent_name="mac-codex",
                discord_user_id="999999999999999999",
                actor="operator",
                reason="test",
            )
            report = diagnose_projections(conn, ws, now="2099-01-01T00:00:00Z")
            f = self._find(report, "registry_override_shadowed")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_INFO)

    def test_registry_expired_override_retained(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            set_workspace_agent(
                conn,
                workspace_id="demo",
                agent_name="mac-codex",
                discord_user_id="1001",
                actor="operator",
                reason="test",
                expires_at="2099-01-01T00:00:00Z",
            )
            conn.execute(
                "UPDATE workspace_agent_registry_entries SET expires_at = ? WHERE workspace_id = ? AND agent_name = ? AND entry_kind = 'override'",
                ("2020-01-01T00:00:00Z", "demo", "mac-codex"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws, now="2020-01-01T00:00:00Z")
            f = self._find(report, "registry_expired_override_retained")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_INFO)

    def _find(self, report: ProjectionReport, kind: str) -> Finding | None:
        for f in report.findings:
            if f.kind == kind:
                return f
        return None


class SplitOperationFindingsTest(ProjectionDoctorTestBase):
    def test_clean_c1_recorded_operation_produces_no_errors(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            report = diagnose_projections(conn, ws)
            error_kinds = {f.kind for f in report.findings if f.severity == SEVERITY_ERROR}
            self.assertEqual(error_kinds, set())

    def test_operation_file_pending(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_file_pending")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_WARNING)
            self.assertFalse(f.repairable)

    def test_operation_ledger_orphaned(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            now = "2026-01-01T00:00:00Z"
            insert_split_operation(
                conn,
                operation_id="12345678-1234-1234-1234-123456789abc",
                contract_version=1,
                operation_kind=OPERATION_KIND_TASK_CREATE,
                workspace_id="demo",
                target_kind=TARGET_KIND_CHECKLIST_TASK,
                target_id="task-1",
                source_kind=None,
                source_id=None,
                input_fingerprint="a" * 64,
                before_fingerprint="b" * 64,
                after_fingerprint="c" * 64,
                status=STATUS_RECORD_APPLIED,
                created_at=now,
                updated_at=now,
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_ledger_orphaned")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_operation_envelope_drift(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            # Mutate deployed checklist title to create drift.
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            checklist["items"][0]["title"] = "Changed"
            checklist_path.write_text(json.dumps(checklist), encoding="utf-8")
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_operation_record_event_missing(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            # Delete the bound record event.
            ops = list_split_operations(conn, workspace_id="demo")
            record_event_id = ops[0].record_event_id
            conn.execute("DELETE FROM events WHERE id = ?", (record_event_id,))
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_record_event_missing")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_operation_target_conflict(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            now = "2026-01-01T00:00:00Z"
            for op_id in ["12345678-1234-1234-1234-123456789abc", "12345678-1234-1234-1234-123456789abd"]:
                insert_split_operation(
                    conn,
                    operation_id=op_id,
                    contract_version=1,
                    operation_kind=OPERATION_KIND_TASK_CREATE,
                    workspace_id="demo",
                    target_kind=TARGET_KIND_CHECKLIST_TASK,
                    target_id="task-1",
                    source_kind=None,
                    source_id=None,
                    input_fingerprint="a" * 64,
                    before_fingerprint="b" * 64,
                    after_fingerprint="c" * 64,
                    status=STATUS_RECORD_APPLIED,
                    created_at=now,
                    updated_at=now,
                )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_target_conflict")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def _find(self, report: ProjectionReport, kind: str) -> Finding | None:
        for f in report.findings:
            if f.kind == kind:
                return f
        return None


class TaskMirrorFindingsTest(ProjectionDoctorTestBase):
    def test_clean_mirror_with_later_task_done(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            later = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="task.done",
                target="task-1",
                task_id="task-1",
                idempotency_key="demo:done:task-1",
                payload={"task_id": "task-1"},
            )
            conn.execute(
                "UPDATE tasks SET last_event_id = ? WHERE workspace_id = ? AND task_id = ?",
                (later.row["id"], "demo", "task-1"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            error_kinds = {f.kind for f in report.findings if f.severity == SEVERITY_ERROR}
            self.assertNotIn("operation_task_event_regression", error_kinds)

    def test_operation_task_mirror_missing(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            conn.execute("DELETE FROM tasks WHERE workspace_id = ? AND task_id = ?", ("demo", "task-1"))
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_task_mirror_missing")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_operation_task_event_regression(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            # Point mirror last_event_id at a different task's event.
            other = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-2",
                task_id="task-2",
                idempotency_key="demo:ready:task-2",
                payload={"task_id": "task-2"},
            )
            conn.execute(
                "UPDATE tasks SET last_event_id = ? WHERE workspace_id = ? AND task_id = ?",
                (other.row["id"], "demo", "task-1"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_task_event_regression")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def _find(self, report: ProjectionReport, kind: str) -> Finding | None:
        for f in report.findings:
            if f.kind == kind:
                return f
        return None


class ReceiptFindingsTest(ProjectionDoctorTestBase):
    def test_receipt_terminal(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            self._build_full_receipt_chain(conn, ws, receipt_id)
            report = diagnose_projections(conn, ws)
            f = self._find(report, "receipt_terminal")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_INFO)
            self.assertEqual(f.receipt_id, receipt_id)

    def test_receipt_chain_incomplete(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.applied",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:applied",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "after_fingerprint": "c" * 64,
                    "applied_at": "2026-01-01T00:00:00Z",
                    "status": "applied",
                },
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "receipt_chain_incomplete")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_receipt_chain_conflict(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            self._build_full_receipt_chain(conn, ws, receipt_id)
            # Add a second claim with wrong workspace.
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.claimed",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:claimed:2",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "other",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "before_fingerprint": "b" * 64,
                    "expected_after_fingerprint": "c" * 64,
                    "claimed_at": "2026-01-01T00:00:02Z",
                    "status": "claimed",
                },
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "receipt_chain_conflict")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_receipt_authorization_unused_superseded(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            first_receipt = "12345678-1234-1234-1234-123456789abc"
            second_receipt = "12345678-1234-1234-1234-123456789abd"
            # First receipt authorized only.
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.authorized",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{first_receipt}:authorized",
                payload={
                    "receipt_id": first_receipt,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "issued_at": "2026-01-01T00:00:00Z",
                    "expires_at": "2099-01-01T00:00:00Z",
                    "status": "authorized",
                    "harness_fingerprint": "a" * 64,
                },
            )
            # Second receipt consumed for the same task.
            self._build_full_receipt_chain(conn, ws, second_receipt)
            report = diagnose_projections(conn, ws)
            f = self._find(report, "receipt_authorization_unused")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_WARNING)
            self.assertTrue(any(e.get("value") is True for e in f.evidence if e.get("key") == "superseded"))

    def _build_full_receipt_chain(self, conn, ws, receipt_id):
        harness_fingerprint = "a" * 64
        expected_after_fingerprint = "c" * 64
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.authorized",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:authorized",
            payload={
                "receipt_id": receipt_id,
                "workspace_id": "demo",
                "task_id": "task-1",
                "authorized_actor": "operator",
                "issued_at": "2026-01-01T00:00:00Z",
                "expires_at": "2099-01-01T00:00:00Z",
                "status": "authorized",
                "harness_fingerprint": harness_fingerprint,
            },
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.claimed",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:claimed",
            payload={
                "receipt_id": receipt_id,
                "workspace_id": "demo",
                "task_id": "task-1",
                "authorized_actor": "operator",
                "before_fingerprint": harness_fingerprint,
                "expected_after_fingerprint": expected_after_fingerprint,
                "claimed_at": "2026-01-01T00:00:01Z",
                "status": "claimed",
            },
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.applied",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:applied",
            payload={
                "receipt_id": receipt_id,
                "workspace_id": "demo",
                "task_id": "task-1",
                "authorized_actor": "operator",
                "before_fingerprint": harness_fingerprint,
                "after_fingerprint": expected_after_fingerprint,
                "applied_at": "2026-01-01T00:00:02Z",
                "status": "applied",
            },
        )
        done = append_event(
            conn,
            workspace_id="demo",
            event_type="task.done",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"demo:done:task-1:{receipt_id}",
            payload={
                "task_id": "task-1",
                "receipt_id": receipt_id,
                "applied_fingerprint": expected_after_fingerprint,
            },
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.consumed",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:consumed",
            payload={
                "receipt_id": receipt_id,
                "workspace_id": "demo",
                "task_id": "task-1",
                "authorized_actor": "operator",
                "task_done_event_id": done.row["id"],
                "consumed_at": "2026-01-01T00:00:04Z",
                "status": "consumed",
            },
        )
        return None
    def _find(self, report: ProjectionReport, kind: str) -> Finding | None:
        for f in report.findings:
            if f.kind == kind:
                return f
        return None


class OrderingAndSerializationTest(ProjectionDoctorTestBase):
    def test_findings_sorted_by_severity_then_lexical(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            # Create an info finding (expired override).
            set_workspace_agent(
                conn,
                workspace_id="demo",
                agent_name="a-agent",
                discord_user_id="1001",
                actor="operator",
                reason="test",
                expires_at="2099-01-01T00:00:00Z",
            )
            conn.execute(
                "UPDATE workspace_agent_registry_entries SET expires_at = ? WHERE workspace_id = ? AND agent_name = ? AND entry_kind = 'override'",
                ("2020-01-01T00:00:00Z", "demo", "a-agent"),
            )
            conn.commit()
            # Create an error finding (source missing).
            conn.execute(
                "INSERT INTO workspace_agent_registry_entries "
                "(workspace_id, agent_name, entry_kind, discord_user_id, display_name, agent_type, created_at, updated_at) "
                "VALUES (?, ?, 'authoritative', ?, ?, ?, 'x', 'x')",
                ("demo", "z-agent", "1001", "Z", "managed"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws, now="2020-01-01T00:00:00Z")
            severities = [f.severity for f in report.findings]
            self.assertEqual(severities, sorted(severities, key=lambda s: {"error": 0, "warning": 1, "info": 2}[s]))

    def test_report_json_serializable(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            report = diagnose_projections(conn, ws)
            serialized = json.dumps(report.to_dict())
            self.assertIsInstance(serialized, str)


class NoWriteProofTest(ProjectionDoctorTestBase):
    def test_diagnose_projections_does_not_mutate_db_or_files(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            before_data_version = conn.execute("PRAGMA data_version").fetchone()[0]
            before_total_changes = conn.total_changes
            before_manifest = self._manifest(Path(ws.harness_root))
            diagnose_projections(conn, ws)
            after_data_version = conn.execute("PRAGMA data_version").fetchone()[0]
            after_total_changes = conn.total_changes
            after_manifest = self._manifest(Path(ws.harness_root))
            self.assertEqual(before_data_version, after_data_version)
            self.assertEqual(before_total_changes, after_total_changes)
            self.assertEqual(before_manifest, after_manifest)

class PreflightCorrectnessTest(ProjectionDoctorTestBase):
    def test_preflight_returns_consumed_status(self):
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            ReceiptFindingsTest._build_full_receipt_chain(self, conn, ws, receipt_id)
            state = _lookup_receipt_for_preflight(conn, receipt_id)
            self.assertIsNotNone(state)
            self.assertEqual(state["status"], "consumed")
            self.assertIn("terminal_event_id", state)

    def test_preflight_consumed_ignores_authorization_expiry(self):
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            # Build a full consumed chain but with a past authorization expiry.
            ReceiptFindingsTest._build_full_receipt_chain(self, conn, ws, receipt_id)
            conn.execute(
                "UPDATE events SET payload_json = REPLACE(payload_json, '2099-01-01T00:00:00Z', '2020-01-01T00:00:00Z') "
                "WHERE event_type = 'completion.authorized' AND json_extract(payload_json, '$.receipt_id') = ?",
                (receipt_id,),
            )
            conn.commit()
            state = _lookup_receipt_for_preflight(conn, receipt_id)
            self.assertIsNotNone(state)
            self.assertEqual(state["status"], "consumed")
            self.assertIn("terminal_event_id", state)

    def test_preflight_returns_applied_status(self):
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.authorized",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:authorized",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "issued_at": "2026-01-01T00:00:00Z",
                    "expires_at": "2099-01-01T00:00:00Z",
                    "status": "authorized",
                    "harness_fingerprint": "a" * 64,
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.claimed",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:claimed",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "before_fingerprint": "a" * 64,
                    "expected_after_fingerprint": "c" * 64,
                    "claimed_at": "2026-01-01T00:00:01Z",
                    "status": "claimed",
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.applied",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:applied",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "before_fingerprint": "a" * 64,
                    "after_fingerprint": "c" * 64,
                    "applied_at": "2026-01-01T00:00:02Z",
                    "status": "applied",
                },
            )
            state = _lookup_receipt_for_preflight(conn, receipt_id)
            self.assertEqual(state["status"], "applied")


class AdditionalRequiredChecksTest(ProjectionDoctorTestBase):
    """R1-7 and additional required checks: no-write, source, manifest, C2, fingerprints."""

    def test_unsupported_contract_version_in_ledger(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            conn.execute(
                "UPDATE split_operations SET contract_version = 99 WHERE operation_id = ?",
                (operation_id,),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_contract_unsupported")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertEqual(self._evidence_dict(f).get("contract_version"), 99)

    def test_unsupported_contract_version_in_file_pending_envelope(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            checklist["items"][0]["split_operation"]["contract_version"] = 99
            checklist_path.write_text(json.dumps(checklist), encoding="utf-8")
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_contract_unsupported")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertEqual(self._evidence_dict(f).get("contract_version"), 99)

    def test_receipt_claimed_before_fingerprint_mismatch(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.authorized",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:authorized",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "issued_at": "2026-01-01T00:00:00Z",
                    "expires_at": "2099-01-01T00:00:00Z",
                    "status": "authorized",
                    "harness_fingerprint": "a" * 64,
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.claimed",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:claimed",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "before_fingerprint": "b" * 64,
                    "expected_after_fingerprint": "c" * 64,
                    "claimed_at": "2026-01-01T00:00:01Z",
                    "status": "claimed",
                },
            )
            report = diagnose_projections(conn, Workspace(
                id="demo", name="Demo", path=tmp, harness_root=tmp,
                default_bus="discord", default_destination="#general",
            ))
            f = self._find(report, "receipt_chain_conflict")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertTrue(any("before_fingerprint" in str(e.get("value", "")) for e in f.evidence))

    def test_receipt_applied_after_fingerprint_mismatch(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.authorized",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:authorized",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "issued_at": "2026-01-01T00:00:00Z",
                    "expires_at": "2099-01-01T00:00:00Z",
                    "status": "authorized",
                    "harness_fingerprint": "a" * 64,
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.claimed",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:claimed",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "before_fingerprint": "a" * 64,
                    "expected_after_fingerprint": "c" * 64,
                    "claimed_at": "2026-01-01T00:00:01Z",
                    "status": "claimed",
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.applied",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:applied",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "after_fingerprint": "d" * 64,
                    "applied_at": "2026-01-01T00:00:02Z",
                    "status": "applied",
                },
            )
            report = diagnose_projections(conn, Workspace(
                id="demo", name="Demo", path=tmp, harness_root=tmp,
                default_bus="discord", default_destination="#general",
            ))
            f = self._find(report, "receipt_chain_conflict")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertTrue(any("after_fingerprint" in str(e.get("value", "")) for e in f.evidence))

    def test_receipt_duplicate_consumed_is_conflict(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            receipt_id = "12345678-1234-1234-1234-123456789abc"
            ReceiptFindingsTest._build_full_receipt_chain(self, conn, ws, receipt_id)
            # Add a second consumed event for the same receipt.
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="completion.consumed",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"receipt:{receipt_id}:consumed:2",
                payload={
                    "receipt_id": receipt_id,
                    "workspace_id": "demo",
                    "task_id": "task-1",
                    "authorized_actor": "operator",
                    "task_done_event_id": "00000000-0000-0000-0000-000000000000",
                    "consumed_at": "2026-01-01T00:00:05Z",
                    "status": "consumed",
                },
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "receipt_chain_conflict")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_envelope_drift_operation_id_mismatch(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            # Change the deployed envelope to bind a different operation id.
            other_id = "12345678-1234-1234-1234-123456789abd"
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            checklist["items"][0]["split_operation"]["operation_id"] = other_id
            checklist_path.write_text(json.dumps(checklist), encoding="utf-8")
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertEqual(self._evidence_dict(f).get("ledger_operation_id"), operation_id)
            self.assertEqual(self._evidence_dict(f).get("envelope_operation_id"), other_id)
            # Should NOT be classified as orphaned.
            self.assertIsNone(self._find(report, "operation_ledger_orphaned"))

    def test_record_event_operation_kind_mismatch(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            event = conn.execute(
                "SELECT * FROM events WHERE event_type = 'plan.ready' AND json_extract(payload_json, '$.split_operation.operation_id') = ?",
                (operation_id,),
            ).fetchone()
            payload = json.loads(event["payload_json"])
            payload["split_operation"]["operation_kind"] = "issue.materialize"
            conn.execute(
                "UPDATE events SET payload_json = ? WHERE id = ?",
                (json.dumps(payload), event["id"]),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_record_event_mismatch")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_record_event_fingerprint_mismatch(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            event = conn.execute(
                "SELECT * FROM events WHERE event_type = 'plan.ready' AND json_extract(payload_json, '$.split_operation.operation_id') = ?",
                (operation_id,),
            ).fetchone()
            payload = json.loads(event["payload_json"])
            payload["split_operation"]["after_fingerprint"] = "c" * 64
            conn.execute(
                "UPDATE events SET payload_json = ? WHERE id = ?",
                (json.dumps(payload), event["id"]),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_record_event_mismatch")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_task_mirror_contract_version_mismatch(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            task = conn.execute(
                "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                ("demo", "task-1"),
            ).fetchone()
            payload = json.loads(task["payload_json"])
            payload["split_operation"]["contract_version"] = 99
            conn.execute(
                "UPDATE tasks SET payload_json = ? WHERE workspace_id = ? AND task_id = ?",
                (json.dumps(payload), "demo", "task-1"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_task_mirror_metadata_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_diagnose_projections_does_not_invoke_shell_or_mutation_helpers(self):
        from unittest.mock import patch
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            with patch("subprocess.run") as mock_run, \
                 patch("coordinate.harness.HarnessAdapter.refresh_state") as mock_refresh, \
                 patch("coordinate.split_operations.apply_task_create_files") as mock_apply_files, \
                 patch("coordinate.split_operations.apply_task_create_record") as mock_apply_record:
                diagnose_projections(conn, ws)
            mock_run.assert_not_called()
            mock_refresh.assert_not_called()
            mock_apply_files.assert_not_called()
            mock_apply_record.assert_not_called()

    def test_diagnose_projections_failure_path_no_write(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            # Inject a failure: unsupported contract version.
            conn.execute(
                "UPDATE split_operations SET contract_version = 99 WHERE operation_id = ?",
                (operation_id,),
            )
            conn.commit()
            before_data_version = conn.execute("PRAGMA data_version").fetchone()[0]
            before_total_changes = conn.total_changes
            before_manifest = self._manifest(Path(ws.harness_root))
            report = diagnose_projections(conn, ws)
            self.assertFalse(report.ok)
            after_data_version = conn.execute("PRAGMA data_version").fetchone()[0]
            after_total_changes = conn.total_changes
            after_manifest = self._manifest(Path(ws.harness_root))
            self.assertEqual(before_data_version, after_data_version)
            self.assertEqual(before_total_changes, after_total_changes)
            self.assertEqual(before_manifest, after_manifest)

    def test_registry_source_bytes_outside_harness_root_unchanged(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            source = Path(tmp) / "agents.toml"
            source.write_text(
                '[registry]\nid = "multinexus.discord"\nversion = 1\n\n'
                '[[agents]]\nid = "mac-codex"\ndisplay_name = "Mac Codex"\n'
                'discord_user_id = "1001"\nagent_type = "managed"\n',
                encoding="utf-8",
            )
            parsed = parse_agents_toml(source)
            sync_workspace_agents(
                conn,
                workspace_id="demo",
                source_id="multinexus.discord",
                source_version=1,
                source_hash=parsed.source.source_hash or compute_plan_sha256(source),
                source_path=str(source),
                entries=[{
                    "id": "mac-codex",
                    "discord_user_id": "1001",
                    "display_name": "Mac Codex",
                    "agent_type": "managed",
                }],
                replace=True,
                synced_by="operator",
            )
            before_source_bytes = source.read_bytes()
            before_data_version = conn.execute("PRAGMA data_version").fetchone()[0]
            before_total_changes = conn.total_changes
            report = diagnose_projections(conn, ws, now="2099-01-01T00:00:00Z")
            self.assertTrue(report.ok)
            after_source_bytes = source.read_bytes()
            after_data_version = conn.execute("PRAGMA data_version").fetchone()[0]
            after_total_changes = conn.total_changes
            self.assertEqual(before_source_bytes, after_source_bytes)
            self.assertEqual(before_data_version, after_data_version)
            self.assertEqual(before_total_changes, after_total_changes)

    def test_c2_clean_issue_materialize_operation(self):
        from coordinate.split_operations import (
            OPERATION_KIND_ISSUE_MATERIALIZE,
            SOURCE_KIND_ISSUE_TRIAGED_EVENT,
            STATUS_RECORD_APPLIED,
            apply_issue_materialize_files,
        )
        from coordinate.db import insert_split_operation, upsert_task_mirror
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            # Create a triaged issue event and the file envelope.
            triage = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="issue.triaged",
                target="github:repo/1",
                task_id="issue-1",
                idempotency_key="demo:issue:1",
                payload={
                    "repo": "owner/repo",
                    "number": 1,
                    "url": "https://github.com/owner/repo/issues/1",
                    "source_event_id": "src-1",
                    "decision": "accept",
                    "task_id": "issue-1",
                },
            )
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_issue_materialize_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="issue-1",
                source_event_id=triage.row["id"],
                plan_doc="plans/plan.md",
                title="Issue 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            # Manually create the DB record state (ledger + mirror + record event).
            plan_sha256 = compute_plan_sha256(Path(tmp) / "plans" / "plan.md")
            plan_ready = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="issue-1",
                task_id="issue-1",
                idempotency_key=f"demo:plan.ready:{operation_id}",
                payload={
                    "task_id": "issue-1",
                    "title": "Issue 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": plan_sha256,
                    "split_operation": {
                        "contract_version": 1,
                        "operation_id": operation_id,
                        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
                        "input_fingerprint": files.input_fingerprint,
                        "before_fingerprint": files.before_fingerprint,
                        "after_fingerprint": files.after_fingerprint,
                    },
                },
            )
            materialized = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="issue.materialized",
                target="issue-1",
                task_id="issue-1",
                idempotency_key=f"demo:issue.materialized:{operation_id}",
                payload={
                    "task_id": "issue-1",
                    "title": "Issue 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "triage_event_id": triage.row["id"],
                    "source": "github_issue",
                    "plan_ready_event_id": plan_ready.row["id"],
                    "split_operation": {
                        "contract_version": 1,
                        "operation_id": operation_id,
                        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
                        "input_fingerprint": files.input_fingerprint,
                        "before_fingerprint": files.before_fingerprint,
                        "after_fingerprint": files.after_fingerprint,
                    },
                },
            )
            insert_split_operation(
                conn,
                operation_id=operation_id,
                contract_version=1,
                operation_kind=OPERATION_KIND_ISSUE_MATERIALIZE,
                workspace_id="demo",
                target_kind="checklist_task",
                target_id="issue-1",
                source_kind=SOURCE_KIND_ISSUE_TRIAGED_EVENT,
                source_id=triage.row["id"],
                input_fingerprint=files.input_fingerprint,
                before_fingerprint=files.before_fingerprint,
                after_fingerprint=files.after_fingerprint,
                status=STATUS_RECORD_APPLIED,
                record_event_id=materialized.row["id"],
                created_at="2026-01-01T00:00:00Z",
                updated_at="2026-01-01T00:00:00Z",
            )
            upsert_task_mirror(
                conn,
                workspace_id="demo",
                task_id="issue-1",
                phase="ready",
                owner=None,
                branch=None,
                pr=None,
                payload={
                    "task_id": "issue-1",
                    "title": "Issue 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "triage_event_id": triage.row["id"],
                    "source": "github_issue",
                    "split_operation": {
                        "contract_version": 1,
                        "operation_id": operation_id,
                        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
                        "input_fingerprint": files.input_fingerprint,
                        "before_fingerprint": files.before_fingerprint,
                        "after_fingerprint": files.after_fingerprint,
                    },
                },
                last_event_id=materialized.row["id"],
            )
            report = diagnose_projections(conn, ws)
            error_kinds = {f.kind for f in report.findings if f.severity == SEVERITY_ERROR}
            self.assertEqual(error_kinds, set())

    def test_c2_envelope_drift_detected(self):
        from coordinate.split_operations import (
            OPERATION_KIND_ISSUE_MATERIALIZE,
            SOURCE_KIND_ISSUE_TRIAGED_EVENT,
            STATUS_RECORD_APPLIED,
            apply_issue_materialize_files,
        )
        from coordinate.db import insert_split_operation, upsert_task_mirror
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            self._make_plan(tmp)
            triage = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="issue.triaged",
                target="github:repo/1",
                task_id="issue-1",
                idempotency_key="demo:issue:1",
                payload={
                    "repo": "owner/repo",
                    "number": 1,
                    "url": "https://github.com/owner/repo/issues/1",
                    "source_event_id": "src-1",
                    "decision": "accept",
                    "task_id": "issue-1",
                },
            )
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_issue_materialize_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="issue-1",
                source_event_id=triage.row["id"],
                plan_doc="plans/plan.md",
                title="Issue 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            # Manually create the DB record state (ledger + mirror + record event).
            plan_sha256 = compute_plan_sha256(Path(tmp) / "plans" / "plan.md")
            plan_ready = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="issue-1",
                task_id="issue-1",
                idempotency_key=f"demo:plan.ready:{operation_id}",
                payload={
                    "task_id": "issue-1",
                    "title": "Issue 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": plan_sha256,
                    "split_operation": {
                        "contract_version": 1,
                        "operation_id": operation_id,
                        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
                        "input_fingerprint": files.input_fingerprint,
                        "before_fingerprint": files.before_fingerprint,
                        "after_fingerprint": files.after_fingerprint,
                    },
                },
            )
            materialized = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="issue.materialized",
                target="issue-1",
                task_id="issue-1",
                idempotency_key=f"demo:issue.materialized:{operation_id}",
                payload={
                    "task_id": "issue-1",
                    "title": "Issue 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "triage_event_id": triage.row["id"],
                    "source": "github_issue",
                    "plan_ready_event_id": plan_ready.row["id"],
                    "split_operation": {
                        "contract_version": 1,
                        "operation_id": operation_id,
                        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
                        "input_fingerprint": files.input_fingerprint,
                        "before_fingerprint": files.before_fingerprint,
                        "after_fingerprint": files.after_fingerprint,
                    },
                },
            )
            insert_split_operation(
                conn,
                operation_id=operation_id,
                contract_version=1,
                operation_kind=OPERATION_KIND_ISSUE_MATERIALIZE,
                workspace_id="demo",
                target_kind="checklist_task",
                target_id="issue-1",
                source_kind=SOURCE_KIND_ISSUE_TRIAGED_EVENT,
                source_id=triage.row["id"],
                input_fingerprint=files.input_fingerprint,
                before_fingerprint=files.before_fingerprint,
                after_fingerprint=files.after_fingerprint,
                status=STATUS_RECORD_APPLIED,
                record_event_id=materialized.row["id"],
                created_at="2026-01-01T00:00:00Z",
                updated_at="2026-01-01T00:00:00Z",
            )
            upsert_task_mirror(
                conn,
                workspace_id="demo",
                task_id="issue-1",
                phase="ready",
                owner=None,
                branch=None,
                pr=None,
                payload={
                    "task_id": "issue-1",
                    "title": "Issue 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "triage_event_id": triage.row["id"],
                    "source": "github_issue",
                    "split_operation": {
                        "contract_version": 1,
                        "operation_id": operation_id,
                        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
                        "input_fingerprint": files.input_fingerprint,
                        "before_fingerprint": files.before_fingerprint,
                        "after_fingerprint": files.after_fingerprint,
                    },
                },
                last_event_id=materialized.row["id"],
            )
            # Change the deployed title to create drift.
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            checklist["items"][0]["title"] = "Changed"
            checklist_path.write_text(json.dumps(checklist), encoding="utf-8")
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_production_like_c1_and_later_task_done_is_clean(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            source = Path(tmp) / "agents.toml"
            source.write_text(
                '[registry]\nid = "multinexus.discord"\nversion = 1\n\n'
                '[[agents]]\nid = "mac-codex"\ndisplay_name = "Mac Codex"\n'
                'discord_user_id = "1001"\nagent_type = "managed"\n',
                encoding="utf-8",
            )
            parsed = parse_agents_toml(source)
            sync_workspace_agents(
                conn,
                workspace_id="demo",
                source_id="multinexus.discord",
                source_version=1,
                source_hash=parsed.source.source_hash or compute_plan_sha256(source),
                source_path=str(source),
                entries=[{
                    "id": "mac-codex",
                    "discord_user_id": "1001",
                    "display_name": "Mac Codex",
                    "agent_type": "managed",
                }],
                replace=True,
                synced_by="operator",
            )
            self._make_plan(tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files = apply_task_create_files(
                workspace_path=tmp,
                harness_root=ws.harness_root,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
                title="Task 1",
                phase="ready",
                priority="p1",
                operation_id=operation_id,
            )
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            # Mutate lifecycle-owned fields on the deployed checklist item.
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            item = checklist["items"][0]
            item["status"] = "doing"
            item["owner"] = "worker"
            item["workflow"] = {
                "status": "running",
                "branch": "feature/task-1",
                "updated_at": "2026-06-01T00:00:00Z",
            }
            item["review"] = {"decision": "approved", "reviewer": "lead"}
            item["selected_in_session"] = "session-1"
            item["updated_at"] = "2026-06-01T00:00:00Z"
            item["verification"] = "verified"
            # Representative lease written by supported harness transitions.
            item["lease"] = {
                "owner": "worker",
                "session": "session-1",
                "acquired_at": "2026-06-01T00:00:00Z",
                "expires_at": "2026-06-01T02:00:00Z",
                "ttl_minutes": 120,
            }
            item["completion_receipt"] = {"receipt_id": "r1", "status": "consumed"}
            checklist_path.write_text(
                json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            # Later legitimate lifecycle event.
            later = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="task.done",
                target="task-1",
                task_id="task-1",
                idempotency_key="demo:done:task-1",
                payload={"task_id": "task-1"},
            )
            conn.execute(
                "UPDATE tasks SET last_event_id = ? WHERE workspace_id = ? AND task_id = ?",
                (later.row["id"], "demo", "task-1"),
            )
            conn.commit()
            report = diagnose_projections(conn, ws, now="2099-01-01T00:00:00Z")
            error_kinds = {f.kind for f in report.findings if f.severity == SEVERITY_ERROR}
            self.assertEqual(error_kinds, set())

    def _apply_split_create(
        self,
        conn: sqlite3.Connection,
        ws: Workspace,
        tmp: str,
        operation_id: str,
        *,
        task_id: str = "task-1",
        plan_doc: str = "plans/plan.md",
        title: str = "Task 1",
        content: bytes = b"# original plan\n",
    ) -> tuple[Any, str]:
        self._make_plan(tmp, rel_path=plan_doc, content=content)
        files = apply_task_create_files(
            workspace_path=tmp,
            harness_root=ws.harness_root,
            workspace_id="demo",
            task_id=task_id,
            plan_doc=plan_doc,
            title=title,
            phase="ready",
            priority="p1",
            operation_id=operation_id,
        )
        apply_task_create_record(
            conn,
            workspace_id="demo",
            task_id=task_id,
            plan_doc=plan_doc,
            title=title,
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
        ops = [op for op in list_split_operations(conn, workspace_id="demo") if op.operation_id == operation_id]
        if not ops:
            raise AssertionError("operation not found")
        return files, ops[0].record_event_id

    def test_plan_change_without_approved_supersession_is_drift_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            self._apply_split_create(conn, ws, tmp, operation_id)
            Path(tmp, "plans", "plan.md").write_bytes(b"# changed plan\n")
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertIsNone(self._find(report, "operation_plan_superseded"))

    def test_plan_change_with_approved_supersession_is_info(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            _, base_ready_event_id = self._apply_split_create(conn, ws, tmp, operation_id)
            plan = Path(tmp, "plans", "plan.md")
            plan.write_bytes(b"# changed plan\n")
            new_sha = compute_plan_sha256(plan)
            new_ready = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.ready:revision:{new_sha}",
                payload={
                    "task_id": "task-1",
                    "title": "Task 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": new_sha,
                    "supersedes_plan_ready_event_id": base_ready_event_id,
                },
            )
            approved = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.approved",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.approved:{new_ready.row['id']}",
                payload={
                    "task_id": "task-1",
                    "decision": "approved",
                    "scope": "implementation",
                    "plan_ready_event_id": new_ready.row["id"],
                },
            )
            report = diagnose_projections(conn, ws)
            error_kinds = {f.kind for f in report.findings if f.severity == SEVERITY_ERROR}
            self.assertEqual(error_kinds, set())
            f = self._find(report, "operation_plan_superseded")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_INFO)
            ev = self._evidence_dict(f)
            self.assertEqual(ev["current_plan_sha256"], new_sha)
            self.assertEqual(ev["base_ready_event_id"], new_ready.row["id"])
            self.assertEqual(ev["approved_event_id"], approved.row["id"])

    def test_plan_change_with_rejected_supersession_is_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            _, base_ready_event_id = self._apply_split_create(conn, ws, tmp, operation_id)
            plan = Path(tmp, "plans", "plan.md")
            plan.write_bytes(b"# changed plan\n")
            new_sha = compute_plan_sha256(plan)
            new_ready = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.ready:revision:{new_sha}",
                payload={
                    "task_id": "task-1",
                    "title": "Task 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": new_sha,
                    "supersedes_plan_ready_event_id": base_ready_event_id,
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.rejected",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.rejected:{new_ready.row['id']}",
                payload={"task_id": "task-1", "decision": "rejected", "scope": "implementation"},
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertIsNone(self._find(report, "operation_plan_superseded"))

    def test_plan_approval_referencing_wrong_ready_event_is_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            _, base_ready_event_id = self._apply_split_create(conn, ws, tmp, operation_id)
            plan = Path(tmp, "plans", "plan.md")
            plan.write_bytes(b"# changed plan\n")
            new_sha = compute_plan_sha256(plan)
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.ready:revision:{new_sha}",
                payload={
                    "task_id": "task-1",
                    "title": "Task 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": new_sha,
                    "supersedes_plan_ready_event_id": base_ready_event_id,
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.approved",
                target="task-1",
                task_id="task-1",
                idempotency_key="demo:task-1:plan.approved:wrong",
                payload={
                    "task_id": "task-1",
                    "decision": "approved",
                    "scope": "implementation",
                    "plan_ready_event_id": base_ready_event_id,  # approves old revision, not new
                },
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            self.assertIsNone(self._find(report, "operation_plan_superseded"))

    def test_supersession_cycle_is_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            _, base_ready_event_id = self._apply_split_create(conn, ws, tmp, operation_id)
            plan = Path(tmp, "plans", "plan.md")
            plan.write_bytes(b"# changed plan\n")
            new_sha = compute_plan_sha256(plan)
            _ = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.ready:revision:{new_sha}",
                payload={
                    "task_id": "task-1",
                    "title": "Task 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": new_sha,
                    "supersedes_plan_ready_event_id": base_ready_event_id,
                },
            )
            cycle_ready = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.ready:cycle:{new_sha}",
                payload={
                    "task_id": "task-1",
                    "title": "Task 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": new_sha,
                    "supersedes_plan_ready_event_id": base_ready_event_id,
                },
            )
            # Create a real cycle by making the earlier revision point back at itself.
            cycle_payload = json.loads(
                conn.execute(
                    "SELECT payload_json FROM events WHERE id = ?", (cycle_ready.row["id"],)
                ).fetchone()["payload_json"]
            )
            cycle_payload["supersedes_plan_ready_event_id"] = cycle_ready.row["id"]
            conn.execute(
                "UPDATE events SET payload_json = ? WHERE id = ?",
                (json.dumps(cycle_payload), cycle_ready.row["id"]),
            )
            conn.commit()
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.approved",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.approved:{cycle_ready.row['id']}",
                payload={
                    "task_id": "task-1",
                    "decision": "approved",
                    "scope": "implementation",
                    "plan_ready_event_id": cycle_ready.row["id"],
                },
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_cross_task_supersedes_is_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            # task-1 base ready
            operation_id_1 = "12345678-1234-1234-1234-123456789abc"
            _, base_ready_1 = self._apply_split_create(conn, ws, tmp, operation_id_1)
            # task-2 base ready
            operation_id_2 = "12345678-1234-1234-1234-123456789abd"
            _, base_ready_2 = self._apply_split_create(
                conn,
                ws,
                tmp,
                operation_id_2,
                task_id="task-2",
                plan_doc="plans/plan2.md",
                title="Task 2",
                content=b"# plan 2\n",
            )
            # task-1 plan changes but its revision claims to supersede task-2's ready event.
            plan = Path(tmp, "plans", "plan.md")
            plan.write_bytes(b"# changed plan\n")
            new_sha = compute_plan_sha256(plan)
            new_ready = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.ready:xtask:{new_sha}",
                payload={
                    "task_id": "task-1",
                    "title": "Task 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    "plan_sha256": new_sha,
                    "supersedes_plan_ready_event_id": base_ready_2,  # wrong task: cross-task link
                },
            )
            # Approve the task-1 revision with a same-task approval.
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.approved",
                target="task-1",
                task_id="task-1",
                idempotency_key="demo:task-1:plan.approved:xtask",
                payload={
                    "task_id": "task-1",
                    "decision": "approved",
                    "scope": "implementation",
                    "plan_ready_event_id": new_ready.row["id"],
                },
            )
            report = diagnose_projections(conn, ws)
            f1 = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f1)
            self.assertEqual(f1.severity, SEVERITY_ERROR)

    def test_legacy_ready_event_missing_full_sha_remains_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            _, base_ready_event_id = self._apply_split_create(conn, ws, tmp, operation_id)
            plan = Path(tmp, "plans", "plan.md")
            plan.write_bytes(b"# changed plan\n")
            new_sha = compute_plan_sha256(plan)
            new_ready = append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.ready",
                target="task-1",
                task_id="task-1",
                idempotency_key=f"demo:task-1:plan.ready:legacy:{new_sha}",
                payload={
                    "task_id": "task-1",
                    "title": "Task 1",
                    "plan_doc": "plans/plan.md",
                    "phase": "ready",
                    "status": "ready",
                    "priority": "p1",
                    # missing plan_sha256 and supersedes link -> fail-closed
                    "supersedes_plan_ready_event_id": base_ready_event_id,
                },
            )
            append_event(
                conn,
                workspace_id="demo",
                actor="operator",
                event_type="plan.approved",
                target="task-1",
                task_id="task-1",
                idempotency_key="demo:task-1:plan.approved:legacy",
                payload={
                    "task_id": "task-1",
                    "decision": "approved",
                    "scope": "implementation",
                    "plan_ready_event_id": new_ready.row["id"],
                },
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_split_record_event_includes_full_plan_sha256_and_supersedes(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            operation_id = "12345678-1234-1234-1234-123456789abc"
            self._apply_split_create(conn, self._make_workspace(conn, tmp), tmp, operation_id)
            event = conn.execute(
                "SELECT payload_json FROM events WHERE event_type = 'plan.ready' AND task_id = ?",
                ("task-1",),
            ).fetchone()
            payload = json.loads(event["payload_json"])
            self.assertTrue(payload.get("plan_sha256"))
            self.assertEqual(len(payload["plan_sha256"]), 64)
            self.assertIsNone(payload.get("supersedes_plan_ready_event_id"))

    def test_split_record_retry_is_idempotent(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            files, _ = self._apply_split_create(conn, ws, tmp, operation_id)
            first = conn.execute(
                "SELECT COUNT(*) AS cnt FROM events WHERE event_type = 'plan.ready' AND task_id = ?",
                ("task-1",),
            ).fetchone()["cnt"]
            apply_task_create_record(
                conn,
                workspace_id="demo",
                task_id="task-1",
                plan_doc="plans/plan.md",
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
            second = conn.execute(
                "SELECT COUNT(*) AS cnt FROM events WHERE event_type = 'plan.ready' AND task_id = ?",
                ("task-1",),
            ).fetchone()["cnt"]
            self.assertEqual(first, second)

    def test_unknown_top_level_field_is_drift_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            self._apply_split_create(conn, ws, tmp, operation_id)
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            checklist["items"][0]["unrecognized_creation_identity"] = "tampered"
            checklist_path.write_text(
                json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)
            ev = self._evidence_dict(f)
            self.assertIn("identity_errors", ev)
            self.assertTrue(
                any(
                    "unknown top-level field: 'unrecognized_creation_identity'" in e
                    for e in ev["identity_errors"]
                ),
                ev["identity_errors"],
            )

    def test_tampered_artifacts_plan_is_drift_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            self._apply_split_create(conn, ws, tmp, operation_id)
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            checklist["items"][0]["artifacts"]["plan"] = "plans/other.md"
            checklist_path.write_text(
                json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_missing_artifacts_plan_is_drift_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            self._apply_split_create(conn, ws, tmp, operation_id)
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            del checklist["items"][0]["artifacts"]["plan"]
            checklist_path.write_text(
                json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_plan_path_and_artifacts_plan_conflict_is_drift_error(self):
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            ws = self._make_workspace(conn, tmp)
            operation_id = "12345678-1234-1234-1234-123456789abc"
            self._apply_split_create(conn, ws, tmp, operation_id)
            Path(tmp, "plans", "other.md").write_bytes(b"# other\n")
            checklist_path = Path(ws.harness_root) / "mvp-checklist.json"
            checklist = json.loads(checklist_path.read_text())
            checklist["items"][0]["artifacts"]["plan"] = "plans/other.md"
            checklist_path.write_text(
                json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            report = diagnose_projections(conn, ws)
            f = self._find(report, "operation_envelope_drift")
            self.assertIsNotNone(f)
            self.assertEqual(f.severity, SEVERITY_ERROR)

    def test_non_split_record_event_includes_full_plan_sha256_and_supersedes(self):
        from coordinate.onboarding import create_plan_task_record
        conn = self._make_conn()
        with tempfile.TemporaryDirectory() as tmp:
            self._make_workspace(conn, tmp)
            plan = Path(tmp, "plans", "plan.md")
            plan.parent.mkdir(parents=True, exist_ok=True)
            plan.write_bytes(b"# v1\n")
            create_plan_task_record(
                conn,
                workspace_id="demo",
                task_id="task-ns",
                plan_doc=str(plan),
                title="Task NS",
                phase="ready",
            )
            plan.write_bytes(b"# v2\n")
            create_plan_task_record(
                conn,
                workspace_id="demo",
                task_id="task-ns",
                plan_doc=str(plan),
                title="Task NS",
                phase="ready",
            )
            rows = conn.execute(
                "SELECT id, payload_json FROM events WHERE event_type = 'plan.ready' AND task_id = ? ORDER BY rowid",
                ("task-ns",),
            ).fetchall()
            self.assertEqual(len(rows), 2)
            payloads = [json.loads(row["payload_json"]) for row in rows]
            self.assertTrue(payloads[0].get("plan_sha256"))
            self.assertEqual(len(payloads[0]["plan_sha256"]), 64)
            self.assertTrue(payloads[1].get("plan_sha256"))
            self.assertEqual(payloads[1]["supersedes_plan_ready_event_id"], rows[0]["id"])

    def _find(self, report: ProjectionReport, kind: str) -> Finding | None:
        for f in report.findings:
            if f.kind == kind:
                return f
        return None


class ReportImmutabilityTest(ProjectionDoctorTestBase):
    """Nested structures in Finding/ProjectionReport must refuse mutation."""

    def _make_finding(self) -> Finding:
        return Finding(
            finding_id="f1",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=[{"key": "k", "value": "v"}],
        )

    def test_finding_evidence_is_tuple_of_mapping_proxies(self):
        f = self._make_finding()
        self.assertIsInstance(f.evidence, tuple)
        self.assertIsInstance(f.evidence[0], MappingProxyType)

    def test_finding_evidence_refuses_item_assignment(self):
        f = self._make_finding()
        with self.assertRaises(TypeError):
            f.evidence[0] = {"key": "x", "value": "y"}

    def test_finding_evidence_item_refuses_key_mutation(self):
        f = self._make_finding()
        with self.assertRaises(TypeError):
            f.evidence[0]["key"] = "mutated"

    def test_finding_evidence_refuses_append(self):
        f = self._make_finding()
        with self.assertRaises(AttributeError):
            f.evidence.append({"key": "x", "value": "y"})

    def test_finding_attribute_refuses_rebinding(self):
        f = self._make_finding()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            f.finding_id = "mutated"

    def test_projection_report_findings_are_tuple(self):
        f = self._make_finding()
        report = ProjectionReport(
            workspace_id="demo",
            findings=[f],
            ok=True,
            summary={"errors": 0},
        )
        self.assertIsInstance(report.findings, tuple)
        with self.assertRaises(AttributeError):
            report.findings.append(self._make_finding())

    def test_projection_report_summary_is_mapping_proxy(self):
        report = ProjectionReport(
            workspace_id="demo",
            findings=[self._make_finding()],
            ok=True,
            summary={"errors": 0},
        )
        self.assertIsInstance(report.summary, MappingProxyType)
        with self.assertRaises(TypeError):
            report.summary["errors"] = 1

    def test_projection_report_summary_refuses_rebinding(self):
        report = ProjectionReport(
            workspace_id="demo",
            findings=[self._make_finding()],
            ok=True,
            summary={"errors": 0},
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            report.summary = {"errors": 1}

    def test_to_dict_returns_mutable_copies(self):
        f = self._make_finding()
        report = ProjectionReport(
            workspace_id="demo",
            findings=[f],
            ok=True,
            summary={"errors": 0},
        )
        d = report.to_dict()
        d["findings"][0]["evidence"][0]["key"] = "mutated"
        self.assertEqual(f.evidence[0]["key"], "k")
        d["summary"]["errors"] = 99
        self.assertEqual(report.summary["errors"], 0)

    def test_nested_dict_in_evidence_is_frozen(self):
        f = Finding(
            finding_id="f2",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=[{"key": "nested", "value": {"items": [1, 2], "map": {"a": "b"}}}],
        )
        with self.assertRaises(AttributeError):
            f.evidence[0]["value"]["items"].append(3)
        with self.assertRaises(TypeError):
            f.evidence[0]["value"]["map"]["a"] = "mutated"

    def test_nested_list_in_evidence_is_frozen(self):
        f = Finding(
            finding_id="f3",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=[{"key": "list", "value": [{"nested": "x"}]}],
        )
        with self.assertRaises(TypeError):
            f.evidence[0]["value"][0]["nested"] = "mutated"
        with self.assertRaises(AttributeError):
            f.evidence[0]["value"].append({})

    def test_set_in_evidence_becomes_frozenset(self):
        f = Finding(
            finding_id="f4",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=[{"key": "set", "value": {"a", "b"}}],
        )
        self.assertIsInstance(f.evidence[0]["value"], frozenset)
        with self.assertRaises(AttributeError):
            f.evidence[0]["value"].add("c")

    def test_to_dict_thaws_nested_structures(self):
        f = Finding(
            finding_id="f5",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=[{"key": "nested", "value": {"items": [1, 2], "set": {"a"}}}],
        )
        d = f.to_dict()
        self.assertIsInstance(d["evidence"][0]["value"], dict)
        self.assertIsInstance(d["evidence"][0]["value"]["items"], list)
        self.assertIsInstance(d["evidence"][0]["value"]["set"], list)
        d["evidence"][0]["value"]["items"].append(3)
        self.assertEqual(list(f.evidence[0]["value"]["items"]), [1, 2])

    def test_to_dict_isolation_from_original(self):
        original = {"items": [1, 2]}
        f = Finding(
            finding_id="f6",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=[{"key": "orig", "value": original}],
        )
        d = f.to_dict()
        d["evidence"][0]["value"]["items"][0] = 99
        self.assertEqual(original["items"][0], 1)

    def test_prefrozen_mapping_evidence_is_recursively_frozen(self):
        """R4-1: tuple-of-MappingProxyType evidence must be re-frozen deeply."""
        inner = {"items": [1, 2]}
        proxy = MappingProxyType({"nested": inner})
        f = Finding(
            finding_id="f7",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=(proxy,),
        )
        # The input proxy is not trusted; a new recursively frozen proxy is used.
        self.assertIsInstance(f.evidence[0], MappingProxyType)
        self.assertIsNot(f.evidence[0], proxy)
        with self.assertRaises(AttributeError):
            f.evidence[0]["nested"]["items"].append(3)
        # The original mutable nested object must remain reachable through the
        # original proxy but not through the finding's evidence.
        inner["items"].append(99)
        self.assertEqual(list(f.evidence[0]["nested"]["items"]), [1, 2])

    def test_prefrozen_summary_is_recursively_frozen(self):
        """R4-1: already-proxied summary must be re-frozen deeply."""
        errors: list[str] = []
        summary_proxy = MappingProxyType({"errors": errors})
        report = ProjectionReport(
            workspace_id="demo",
            findings=[self._make_finding()],
            ok=True,
            summary=summary_proxy,
        )
        self.assertIsInstance(report.summary, MappingProxyType)
        self.assertIsNot(report.summary, summary_proxy)
        with self.assertRaises(AttributeError):
            report.summary["errors"].append("mutated")
        errors.append("original")
        self.assertEqual(list(report.summary["errors"]), [])

    def test_non_dict_mapping_is_recursively_frozen(self):
        """R4-1: UserDict and other Mapping implementations must be frozen."""
        user = UserDict({"key": "nested", "value": {"items": [1]}})
        f = Finding(
            finding_id="f8",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=[user],
        )
        self.assertIsInstance(f.evidence[0], MappingProxyType)
        with self.assertRaises(AttributeError):
            f.evidence[0]["value"]["items"].append(2)

    def test_to_dict_copy_isolation_for_prefrozen_structures(self):
        """R4-1: to_dict() yields independent mutable copies for pre-frozen inputs."""
        inner = {"items": [1, 2]}
        evidence_proxy = MappingProxyType({"nested": inner})
        summary_errors: list[str] = []
        summary_proxy = MappingProxyType({"errors": summary_errors})
        f = Finding(
            finding_id="f9",
            kind="test_kind",
            severity=SEVERITY_INFO,
            scope="test",
            workspace_id="demo",
            evidence=(evidence_proxy,),
        )
        report = ProjectionReport(
            workspace_id="demo",
            findings=[f],
            ok=True,
            summary=summary_proxy,
        )
        d = report.to_dict()
        # Mutate the returned copies.
        d["findings"][0]["evidence"][0]["nested"]["items"].append(3)
        d["summary"]["errors"].append("mutated")
        # Internal state must be unchanged.
        self.assertEqual(list(f.evidence[0]["nested"]["items"]), [1, 2])
        self.assertEqual(list(report.summary["errors"]), [])
        # Original mutable objects must also remain independent.
        inner["items"].append(99)
        summary_errors.append("original")
        self.assertEqual(list(f.evidence[0]["nested"]["items"]), [1, 2])
        self.assertEqual(list(report.summary["errors"]), [])


class ReceiptRequiredLinkFindingsTest(ProjectionDoctorTestBase):
    """R2-1: every required receipt transition link must be present and consistent."""

    def _build_chain_with(self, overrides: dict[str, Any]) -> tuple[sqlite3.Connection, Workspace, str]:
        """Return (conn, ws, receipt_id) for a chain customized by *overrides*.

        *overrides* maps a transition name to a dict of payload fields to add
        or remove.  A value of ``None`` removes the field.
        """
        conn = self._make_conn()
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        ws = self._make_workspace(conn, tmp)
        receipt_id = "12345678-1234-1234-1234-123456789abc"
        harness_fingerprint = "a" * 64
        expected_after = "c" * 64

        authorized = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "issued_at": "2026-01-01T00:00:00Z",
            "expires_at": "2099-01-01T00:00:00Z",
            "status": "authorized",
            "harness_fingerprint": harness_fingerprint,
        }
        authorized.update(overrides.get("authorized", {}))
        self._drop_nones(authorized)

        claimed = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "before_fingerprint": harness_fingerprint,
            "expected_after_fingerprint": expected_after,
            "claimed_at": "2026-01-01T00:00:01Z",
            "status": "claimed",
        }
        claimed.update(overrides.get("claimed", {}))
        self._drop_nones(claimed)

        applied = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "before_fingerprint": harness_fingerprint,
            "after_fingerprint": expected_after,
            "applied_at": "2026-01-01T00:00:02Z",
            "status": "applied",
        }
        applied.update(overrides.get("applied", {}))
        self._drop_nones(applied)

        done_payload = {
            "task_id": "task-1",
            "receipt_id": receipt_id,
            "applied_fingerprint": expected_after,
        }
        done_payload.update(overrides.get("task_done", {}))
        self._drop_nones(done_payload)

        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.authorized",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:authorized",
            payload=authorized,
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.claimed",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:claimed",
            payload=claimed,
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.applied",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:applied",
            payload=applied,
        )
        done = append_event(
            conn,
            workspace_id="demo",
            event_type="task.done",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"demo:done:task-1:{receipt_id}",
            payload=done_payload,
        )

        consumed = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "consumed_at": "2026-01-01T00:00:04Z",
            "status": "consumed",
        }
        consumed.update(overrides.get("consumed", {}))
        if "task_done_event_id" not in consumed:
            consumed["task_done_event_id"] = done.row["id"]
        self._drop_nones(consumed)

        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.consumed",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:consumed",
            payload=consumed,
        )

        return conn, ws, receipt_id

    @staticmethod
    def _drop_nones(mapping: dict[str, Any]) -> None:
        for key in list(mapping):
            if mapping[key] is None:
                del mapping[key]

    def _expect_error(self, report: ProjectionReport, receipt_id: str, kind: str) -> None:
        findings = [f for f in report.findings if f.receipt_id == receipt_id and f.kind == kind]
        self.assertTrue(findings, f"expected {kind} finding for {receipt_id}")
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)

    def test_missing_authorized_harness_fingerprint_is_incomplete(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"authorized": {"harness_fingerprint": None}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_incomplete")
        self.assertIsNone(self._find(report, "receipt_terminal"))

    def test_missing_claimed_before_fingerprint_is_incomplete(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"claimed": {"before_fingerprint": None}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_incomplete")

    def test_missing_claimed_expected_after_fingerprint_is_incomplete(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"claimed": {"expected_after_fingerprint": None}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_incomplete")

    def test_claimed_before_fingerprint_must_backlink_to_authorized(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"claimed": {"before_fingerprint": "b" * 64}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_missing_applied_after_fingerprint_is_incomplete(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"applied": {"after_fingerprint": None}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_incomplete")

    def test_applied_after_fingerprint_must_match_claimed_expected(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"applied": {"after_fingerprint": "d" * 64}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_missing_consumed_task_done_event_id_is_incomplete(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"consumed": {"task_done_event_id": None}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_incomplete")

    def test_missing_task_done_applied_fingerprint_is_incomplete(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"task_done": {"applied_fingerprint": None}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_incomplete")

    def test_task_done_applied_fingerprint_must_equal_applied_after(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"task_done": {"applied_fingerprint": "d" * 64}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_missing_applied_before_fingerprint_is_incomplete(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"applied": {"before_fingerprint": None}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_incomplete")
        self.assertIsNone(self._find(report, "receipt_terminal"))

    def test_applied_before_fingerprint_must_backlink_to_claimed(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"applied": {"before_fingerprint": "b" * 64}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_malformed_authorized_harness_fingerprint_is_conflict(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"authorized": {"harness_fingerprint": "not-a-sha"}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")
        self.assertIsNone(self._find(report, "receipt_terminal"))

    def test_malformed_claimed_before_fingerprint_is_conflict(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"claimed": {"before_fingerprint": "not-a-sha"}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_malformed_claimed_expected_after_fingerprint_is_conflict(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"claimed": {"expected_after_fingerprint": "not-a-sha"}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_malformed_applied_before_fingerprint_is_conflict(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"applied": {"before_fingerprint": "not-a-sha"}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_malformed_applied_after_fingerprint_is_conflict(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"applied": {"after_fingerprint": "not-a-sha"}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_malformed_task_done_applied_fingerprint_is_conflict(self):
        conn, ws, receipt_id = self._build_chain_with(
            {"task_done": {"applied_fingerprint": "not-a-sha"}}
        )
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")

    def test_mutually_equal_malformed_chain_fails_closed(self):
        # Every fingerprint is the same malformed value; equality must not bypass format validation.
        conn, ws, receipt_id = self._build_chain_with({
            "authorized": {"harness_fingerprint": "also-not-a-sha"},
            "claimed": {
                "before_fingerprint": "also-not-a-sha",
                "expected_after_fingerprint": "also-not-a-sha",
            },
            "applied": {
                "before_fingerprint": "also-not-a-sha",
                "after_fingerprint": "also-not-a-sha",
            },
            "task_done": {"applied_fingerprint": "also-not-a-sha"},
        })
        report = diagnose_projections(conn, ws)
        self._expect_error(report, receipt_id, "receipt_chain_conflict")
        self.assertIsNone(self._find(report, "receipt_terminal"))

    def test_clean_chain_with_all_required_links_is_terminal(self):
        conn, ws, receipt_id = self._build_chain_with({})
        report = diagnose_projections(conn, ws)
        self.assertIsNone(self._find(report, "receipt_chain_incomplete"))
        self.assertIsNone(self._find(report, "receipt_chain_conflict"))
        f = self._find(report, "receipt_terminal")
        self.assertIsNotNone(f)
        self.assertEqual(f.severity, SEVERITY_INFO)

    def _find(self, report: ProjectionReport, kind: str) -> Finding | None:
        for f in report.findings:
            if f.kind == kind:
                return f
        return None


class PreflightRequiredLinkTest(ProjectionDoctorTestBase):
    """R2-1: preflight must fail-closed on every missing required receipt link."""

    def _make_chain(self, overrides: dict[str, Any]) -> tuple[sqlite3.Connection, str]:
        conn = self._make_conn()
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        self._make_workspace(conn, tmp)
        receipt_id = "12345678-1234-1234-1234-123456789abc"
        harness_fingerprint = "a" * 64
        expected_after = "c" * 64

        authorized = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "issued_at": "2026-01-01T00:00:00Z",
            "expires_at": "2099-01-01T00:00:00Z",
            "status": "authorized",
            "harness_fingerprint": harness_fingerprint,
        }
        authorized.update(overrides.get("authorized", {}))
        self._drop_nones(authorized)

        claimed = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "before_fingerprint": harness_fingerprint,
            "expected_after_fingerprint": expected_after,
            "claimed_at": "2026-01-01T00:00:01Z",
            "status": "claimed",
        }
        claimed.update(overrides.get("claimed", {}))
        self._drop_nones(claimed)

        applied = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "before_fingerprint": harness_fingerprint,
            "after_fingerprint": expected_after,
            "applied_at": "2026-01-01T00:00:02Z",
            "status": "applied",
        }
        applied.update(overrides.get("applied", {}))
        self._drop_nones(applied)

        done_payload = {
            "task_id": "task-1",
            "receipt_id": receipt_id,
            "applied_fingerprint": expected_after,
        }
        done_payload.update(overrides.get("task_done", {}))
        self._drop_nones(done_payload)

        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.authorized",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:authorized",
            payload=authorized,
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.claimed",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:claimed",
            payload=claimed,
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.applied",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:applied",
            payload=applied,
        )
        done = append_event(
            conn,
            workspace_id="demo",
            event_type="task.done",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"demo:done:task-1:{receipt_id}",
            payload=done_payload,
        )

        consumed = {
            "receipt_id": receipt_id,
            "workspace_id": "demo",
            "task_id": "task-1",
            "authorized_actor": "operator",
            "consumed_at": "2026-01-01T00:00:04Z",
            "status": "consumed",
        }
        consumed.update(overrides.get("consumed", {}))
        if "task_done_event_id" not in consumed:
            consumed["task_done_event_id"] = done.row["id"]
        self._drop_nones(consumed)

        append_event(
            conn,
            workspace_id="demo",
            event_type="completion.consumed",
            actor="operator",
            target="task-1",
            task_id="task-1",
            idempotency_key=f"receipt:{receipt_id}:consumed",
            payload=consumed,
        )
        return conn, receipt_id

    @staticmethod
    def _drop_nones(mapping: dict[str, Any]) -> None:
        for key in list(mapping):
            if mapping[key] is None:
                del mapping[key]

    def _expect_broken(self, state: dict[str, Any], reason: str) -> None:
        self.assertTrue(state.get("broken"))
        self.assertEqual(state.get("reason"), reason)

    def test_preflight_missing_authorized_harness_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"authorized": {"harness_fingerprint": None}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_incomplete")

    def test_preflight_missing_claimed_before_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"claimed": {"before_fingerprint": None}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_incomplete")

    def test_preflight_missing_claimed_expected_after_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"claimed": {"expected_after_fingerprint": None}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_incomplete")

    def test_preflight_claimed_before_backlinks_to_authorized(self):
        conn, receipt_id = self._make_chain(
            {"claimed": {"before_fingerprint": "b" * 64}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_missing_applied_after_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"applied": {"after_fingerprint": None}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_incomplete")

    def test_preflight_applied_after_matches_claimed_expected(self):
        conn, receipt_id = self._make_chain(
            {"applied": {"after_fingerprint": "d" * 64}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_missing_consumed_task_done_event_id(self):
        conn, receipt_id = self._make_chain(
            {"consumed": {"task_done_event_id": None}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_incomplete")

    def test_preflight_missing_task_done_applied_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"task_done": {"applied_fingerprint": None}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_incomplete")

    def test_preflight_task_done_applied_fingerprint_equals_applied_after(self):
        conn, receipt_id = self._make_chain(
            {"task_done": {"applied_fingerprint": "d" * 64}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_missing_applied_before_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"applied": {"before_fingerprint": None}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_incomplete")

    def test_preflight_applied_before_backlinks_to_claimed(self):
        conn, receipt_id = self._make_chain(
            {"applied": {"before_fingerprint": "b" * 64}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_malformed_authorized_harness_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"authorized": {"harness_fingerprint": "not-a-sha"}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_malformed_claimed_before_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"claimed": {"before_fingerprint": "not-a-sha"}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_malformed_claimed_expected_after_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"claimed": {"expected_after_fingerprint": "not-a-sha"}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_malformed_applied_before_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"applied": {"before_fingerprint": "not-a-sha"}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_malformed_applied_after_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"applied": {"after_fingerprint": "not-a-sha"}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_malformed_task_done_applied_fingerprint(self):
        conn, receipt_id = self._make_chain(
            {"task_done": {"applied_fingerprint": "not-a-sha"}}
        )
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_mutually_equal_malformed_chain_fails_closed(self):
        conn, receipt_id = self._make_chain({
            "authorized": {"harness_fingerprint": "also-not-a-sha"},
            "claimed": {
                "before_fingerprint": "also-not-a-sha",
                "expected_after_fingerprint": "also-not-a-sha",
            },
            "applied": {
                "before_fingerprint": "also-not-a-sha",
                "after_fingerprint": "also-not-a-sha",
            },
            "task_done": {"applied_fingerprint": "also-not-a-sha"},
        })
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self._expect_broken(state, "receipt_chain_conflict")

    def test_preflight_clean_chain_returns_consumed(self):
        conn, receipt_id = self._make_chain({})
        from coordinate.completion_cli import _lookup_receipt_for_preflight
        state = _lookup_receipt_for_preflight(conn, receipt_id)
        self.assertFalse(state.get("broken", False))
        self.assertEqual(state.get("status"), "consumed")


if __name__ == "__main__":
    unittest.main()
