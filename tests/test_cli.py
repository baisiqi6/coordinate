import contextlib
import hashlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import uuid
from pathlib import Path

from coordinate.cli import main
from coordinate.db import (
    append_event,
    initialize,
    mark_delivery_sending,
    upsert_task_mirror,
    upsert_workspace,
)


def _write_harness_state_with_source(
    root,
    *,
    checklist_name="mvp-checklist.json",
    project="demo",
    current_item=None,
):
    """Write harness-state.json whose source matches the actual checklist bytes."""
    checklist_path = Path(root) / checklist_name
    checklist_bytes = checklist_path.read_bytes()
    (Path(root) / "harness-state.json").write_text(
        json.dumps({
            "project": project,
            "current_item": current_item,
            "source": {
                "checklist_path": checklist_name,
                "checklist_sha256": hashlib.sha256(checklist_bytes).hexdigest(),
            },
        }),
        encoding="utf-8",
    )


def _audit_item(task_id="mvp-001", status="doing", workflow_status="running",
                owner="codex"):
    """A validator-passing checklist item for reconcile/audit fixtures."""
    return {
        "id": task_id,
        "title": "Build core",
        "status": status,
        "priority": "p1",
        "owner": owner,
        "selected_in_session": "session-1" if status == "doing" else None,
        "verification": "verified" if status == "done" else "",
        "updated_at": "2026-01-01T00:00:00Z",
        "dependencies": [],
        "blocked_by": [],
        "blocked_reason": "",
        "acceptance": "Acceptance",
        "handoff": {"from": None, "to": None, "reason": None},
        "workflow": {"status": workflow_status, "branch": None,
                     "updated_at": "2026-01-01T00:00:00Z"},
    }


def _mark_done_item(task_id="mvp-001", status="doing", workflow_status="review_approved",
                    branch=None, verification="evidence"):
    """A validator-passing checklist item for mark-done/receipt fixtures."""
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "status": status,
        "priority": "p1",
        "owner": "codex",
        "selected_in_session": "session-1",
        "verification": verification,
        "updated_at": "2026-01-01T00:00:00Z",
        "dependencies": [],
        "blocked_by": [],
        "blocked_reason": "",
        "acceptance": f"Acceptance for {task_id}",
        "handoff": {"from": None, "to": None, "reason": None},
        "workflow": {"status": workflow_status, "branch": branch,
                     "updated_at": "2026-01-01T00:00:00Z"},
    }


class CliTests(unittest.TestCase):
    def run_cli(self, *args):
        code, stdout, _ = self.run_cli_raw(*args)
        return code, json.loads(stdout)

    def run_cli_raw(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(args))
        return code, stdout.getvalue(), stderr.getvalue()

    def test_workspace_add_and_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            add_code, add_payload = self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
                "--default-bus",
                "discord",
                "--default-destination",
                "channel-1",
            )
            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "workspace",
                "list",
            )

            self.assertEqual(add_code, 0)
            self.assertEqual(add_payload["workspace"]["id"], "demo")
            self.assertEqual(list_code, 0)
            self.assertEqual(list_payload["workspaces"][0]["default_bus"], "discord")

    def test_operator_pending_reports_snapshot_version_and_staleness(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
            )
            upsert_task_mirror(
                conn,
                workspace_id="demo",
                task_id="t1",
                phase="running",
                owner="mac-codex",
                branch=None,
                pr=None,
                payload={},
            )
            append_event(
                conn,
                workspace_id="demo",
                event_type="agent.reported",
                actor="mac-codex",
                task_id="t1",
                payload={"action": "done"},
            )
            conn.close()

            code, payload = self.run_cli(
                "--db", db_path, "operator", "pending", "demo"
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["pending_actions"][0]["action"], "review_code")
            snapshot = payload["snapshot"]
            self.assertEqual(snapshot["source"], "task_mirror+event_ledger")
            self.assertFalse(snapshot["harness_refreshed"])
            self.assertTrue(snapshot["may_be_stale"])
            self.assertIsInstance(snapshot["latest_event_rowid"], int)
            self.assertIsNotNone(snapshot["task_mirror_updated_at"])

    def test_event_append_returns_existing_row_for_same_idempotency_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            first_code, first_payload = self.run_cli(
                "--db",
                db_path,
                "event",
                "append",
                "assignment.requested",
                "--workspace-id",
                "demo",
                "--actor",
                "operator",
                "--task-id",
                "mvp-001",
                "--idempotency-key",
                "demo:mvp-001:assign",
                "--payload-json",
                '{"owner":"codex"}',
            )
            second_code, second_payload = self.run_cli(
                "--db",
                db_path,
                "event",
                "append",
                "assignment.requested",
                "--workspace-id",
                "demo",
                "--actor",
                "operator",
                "--task-id",
                "mvp-001",
                "--idempotency-key",
                "demo:mvp-001:assign",
                "--payload-json",
                '{"owner":"codex"}',
            )

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["created"])
            self.assertFalse(second_payload["created"])
            self.assertEqual(first_payload["event"]["id"], second_payload["event"]["id"])

    def test_runtime_request_claim_report_cli_flow(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            self.run_cli(
                "--db", db_path, "workspace", "host-profile", "set", "demo",
                "--host-id", "mac", "--workspace-path", tmp, "--harness-root", tmp,
            )
            register_code, register_payload = self.run_cli(
                "--db",
                db_path,
                "runtime",
                "agent",
                "register",
                "--agent-id",
                "mac-codex",
                "--host-id",
                "mac",
                "--capabilities-json",
                '{"models":["codex"]}',
            )
            request_code, request_payload = self.run_cli(
                "--db",
                db_path,
                "runtime",
                "request",
                "submit",
                "demo",
                "--target-agent",
                "mac-codex",
                "--prompt",
                "hello",
                "--origin-json",
                '{"platform":"discord","destination":"channel-1","message_id":"m1","session_scope_id":"discord:channel-1"}',
                "--reply-json",
                '{"platform":"discord","destination":"channel-1"}',
            )
            claim_code, claim_payload = self.run_cli(
                "--db",
                db_path,
                "runtime",
                "job",
                "claim",
                "--agent-id",
                "mac-codex",
            )
            report_code, report_payload = self.run_cli(
                "--db",
                db_path,
                "runtime",
                "job",
                "report",
                request_payload["result"]["job"]["id"],
                "--agent-id",
                "mac-codex",
                "--status",
                "done",
                "--result-json",
                '{"response_text":"done"}',
            )

            self.assertEqual(register_code, 0)
            self.assertEqual(request_code, 0)
            self.assertEqual(claim_code, 0)
            self.assertEqual(report_code, 0)
            self.assertEqual(register_payload["result"]["agent"]["id"], "mac-codex")
            self.assertTrue(request_payload["result"]["job_created"])
            self.assertTrue(claim_payload["result"]["claimed"])
            self.assertEqual(report_payload["result"]["job"]["status"], "done")
            self.assertEqual(report_payload["result"]["delivery"]["payload"]["text"], "done")

    def test_runtime_job_claim_default_vs_recoverable(self):
        """8.4.3 P1 #1 CLI: default claim leaves timed_out+recoverable alone; --recoverable reclaims."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli("--db", db_path, "workspace", "add", "demo", "--path", tmp, "--harness-root", tmp)
            self.run_cli(
                "--db", db_path, "workspace", "host-profile", "set", "demo",
                "--host-id", "mac", "--workspace-path", tmp, "--harness-root", tmp,
            )
            self.run_cli(
                "--db", db_path, "runtime", "agent", "register",
                "--agent-id", "mac-codex", "--host-id", "mac",
                "--capabilities-json", '{"models":["codex"]}',
            )
            _, request_payload = self.run_cli(
                "--db", db_path, "runtime", "request", "submit", "demo",
                "--target-agent", "mac-codex", "--prompt", "x",
                "--origin-json", '{"platform":"discord","destination":"ch","message_id":"m1","session_scope_id":"discord:ch"}',
                "--reply-json", '{"platform":"discord","destination":"ch"}',
            )
            job_id = request_payload["result"]["job"]["id"]
            self.run_cli("--db", db_path, "runtime", "job", "claim", "--agent-id", "mac-codex")
            self.run_cli(
                "--db", db_path, "runtime", "job", "report", job_id,
                "--agent-id", "mac-codex", "--status", "timed_out",
                "--result-json", '{"recoverable": true}',
            )
            _, claim_default = self.run_cli(
                "--db", db_path, "runtime", "job", "claim", "--agent-id", "mac-codex",
            )
            self.assertEqual(claim_default["result"]["claimed"], False)
            _, claim_recoverable = self.run_cli(
                "--db", db_path, "runtime", "job", "claim",
                "--agent-id", "mac-codex", "--recoverable",
            )
            self.assertEqual(claim_recoverable["result"]["claimed"], True)

    def test_runtime_job_report_attempt_token_cli(self):
        """8.4.3 P1 #2 CLI: --attempt-token rejects stale, accepts current."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli("--db", db_path, "workspace", "add", "demo", "--path", tmp, "--harness-root", tmp)
            self.run_cli(
                "--db", db_path, "workspace", "host-profile", "set", "demo",
                "--host-id", "mac", "--workspace-path", tmp, "--harness-root", tmp,
            )
            self.run_cli(
                "--db", db_path, "runtime", "agent", "register",
                "--agent-id", "mac-codex", "--host-id", "mac",
                "--capabilities-json", '{"models":["codex"]}',
            )
            _, request_payload = self.run_cli(
                "--db", db_path, "runtime", "request", "submit", "demo",
                "--target-agent", "mac-codex", "--prompt", "x",
                "--origin-json", '{"platform":"discord","destination":"ch","message_id":"m1","session_scope_id":"discord:ch"}',
                "--reply-json", '{"platform":"discord","destination":"ch"}',
            )
            job_id = request_payload["result"]["job"]["id"]
            self.run_cli("--db", db_path, "runtime", "job", "claim", "--agent-id", "mac-codex")
            self.run_cli(
                "--db", db_path, "runtime", "job", "report", job_id,
                "--agent-id", "mac-codex", "--status", "timed_out",
                "--result-json", '{"recoverable": true}',
            )
            # reclaim → attempt 2; claim returns attempt_token
            _, claim2 = self.run_cli(
                "--db", db_path, "runtime", "job", "claim",
                "--agent-id", "mac-codex", "--recoverable",
            )
            self.assertEqual(claim2["result"]["attempt_token"], 2)
            # stale token 1 → rejected (non-zero exit + error on stderr; stdout empty)
            stale_code, _, stale_stderr = self.run_cli_raw(
                "--db", db_path, "runtime", "job", "report", job_id,
                "--agent-id", "mac-codex", "--status", "done",
                "--result-json", '{"response_text":"late"}',
                "--attempt-token", "1",
            )
            self.assertNotEqual(stale_code, 0)
            self.assertIn("attempt", stale_stderr)
            # current token 2 → accepted
            ok_code, ok_payload = self.run_cli(
                "--db", db_path, "runtime", "job", "report", job_id,
                "--agent-id", "mac-codex", "--status", "done",
                "--result-json", '{"response_text":"final"}',
                "--attempt-token", "2",
            )
            self.assertEqual(ok_code, 0)
            self.assertEqual(ok_payload["result"]["job"]["status"], "done")

    def test_runtime_job_progress_attempt_token_cli(self):
        """8.4.3 P1 #2 CLI: runtime job progress --attempt-token rejects stale."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli("--db", db_path, "workspace", "add", "demo", "--path", tmp, "--harness-root", tmp)
            self.run_cli(
                "--db", db_path, "workspace", "host-profile", "set", "demo",
                "--host-id", "mac", "--workspace-path", tmp, "--harness-root", tmp,
            )
            self.run_cli(
                "--db", db_path, "runtime", "agent", "register",
                "--agent-id", "mac-codex", "--host-id", "mac",
                "--capabilities-json", '{"models":["codex"]}',
            )
            _, request_payload = self.run_cli(
                "--db", db_path, "runtime", "request", "submit", "demo",
                "--target-agent", "mac-codex", "--prompt", "x",
                "--origin-json", '{"platform":"discord","destination":"ch","message_id":"m1","session_scope_id":"discord:ch"}',
                "--reply-json", '{"platform":"discord","destination":"ch"}',
            )
            job_id = request_payload["result"]["job"]["id"]
            self.run_cli("--db", db_path, "runtime", "job", "claim", "--agent-id", "mac-codex")
            self.run_cli(
                "--db", db_path, "runtime", "job", "report", job_id,
                "--agent-id", "mac-codex", "--status", "timed_out",
                "--result-json", '{"recoverable": true}',
            )
            self.run_cli(
                "--db", db_path, "runtime", "job", "claim",
                "--agent-id", "mac-codex", "--recoverable",
            )  # reclaimed → attempt 2
            # stale progress token 1 → rejected (non-zero exit)
            stale_code, _, stale_stderr = self.run_cli_raw(
                "--db", db_path, "runtime", "job", "progress", job_id,
                "--agent-id", "mac-codex", "--stage", "editing",
                "--attempt-token", "1",
            )
            self.assertNotEqual(stale_code, 0)
            self.assertIn("attempt", stale_stderr)
            # current progress token 2 → accepted
            ok_code, ok_payload = self.run_cli(
                "--db", db_path, "runtime", "job", "progress", job_id,
                "--agent-id", "mac-codex", "--stage", "editing",
                "--attempt-token", "2",
            )
            self.assertEqual(ok_code, 0)
            self.assertEqual(ok_payload["result"]["job"]["progress"]["stage"], "editing")

    def test_state_no_refresh_reads_registered_harness_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            # A fresh cache carries the real checklist source path + digest.
            checklist_bytes = json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-07-13",
                "items": [],
            }).encode("utf-8")
            (Path(tmp) / "mvp-checklist.json").write_bytes(checklist_bytes)
            state_path = Path(tmp) / "harness-state.json"
            state_path.write_text(
                json.dumps({
                    "project": "demo",
                    "current_item": None,
                    "source": {
                        "checklist_path": "mvp-checklist.json",
                        "checklist_sha256": hashlib.sha256(checklist_bytes).hexdigest(),
                    },
                }),
                encoding="utf-8",
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "state",
                "demo",
                "--no-refresh",
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["state"]["project"], "demo")
            self.assertTrue(payload["authoritative"])

    def test_state_no_refresh_stale_reports_non_authoritative_nonzero(self):
        """--no-refresh on a stale cache is a diagnostic: JSON marks
        authoritative=false and the exit code is nonzero."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            # No checklist source at all → stale.
            (Path(tmp) / "harness-state.json").write_text(
                json.dumps({"project": "demo", "current_item": None}),
                encoding="utf-8",
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "state",
                "demo",
                "--no-refresh",
            )

            self.assertEqual(code, 1)
            self.assertFalse(payload["authoritative"])
            self.assertTrue(payload["stale_reasons"])

    def test_state_refresh_adapter_error_reports_diagnostic_json(self):
        """A HarnessAdapter failure during refresh must surface as the
        documented error JSON with exit code 1, never as a NameError from
        the CLI handler."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            harnessctl_path.write_text(
                "#!/bin/bash\necho 'harnessctl exploded' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
                "--harnessctl-path",
                str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "state",
                "demo",
            )

            self.assertEqual(code, 1)
            self.assertIn("harnessctl state failed", payload["error"]["message"])

    def test_workspace_init_harness_creates_minimal_file_backed_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            repo = Path(tmp) / "repo"
            repo.mkdir()
            (repo / "docs").mkdir()
            (repo / "docs" / "plan.md").write_text("# Plan\n", encoding="utf-8")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                str(repo),
                "--harness-root",
                str(repo / "docs"),
                "--base-branch",
                "main",
                "--branch-namespace",
                "agents",
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "workspace",
                "init-harness",
                "demo",
                "--root",
                "docs/project-harness",
                "--task-id",
                "phase-001",
                "--plan-doc",
                "docs/plan.md",
                "--title",
                "Phase 001",
            )
            state_code, state_payload = self.run_cli(
                "--db",
                db_path,
                "state",
                "demo",
                "--no-refresh",
            )

            harness_root = (repo / "docs" / "project-harness").resolve()
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["workspace"]["harness_root"], str(harness_root))
            self.assertTrue((harness_root / "harness-checklist.json").exists())
            self.assertTrue((harness_root / "harness-state.json").exists())
            self.assertTrue((harness_root / "tasks" / "phase-001" / "plan.md").exists())
            self.assertEqual(payload["result"]["event"]["event_type"], "harness.initialized")
            self.assertEqual(payload["result"]["task"]["task_id"], "phase-001")
            self.assertEqual(state_code, 0)
            self.assertEqual(state_payload["state"]["current_item"]["id"], "phase-001")
            # The config must name the real workspace-relative events path, not
            # a bare filename that silently resolves elsewhere.
            config = json.loads((harness_root / "harness-config.json").read_text(encoding="utf-8"))
            self.assertEqual(
                config["message_bus"]["event_log"],
                "docs/project-harness/events.jsonl",
            )
            # The state must carry a real checklist source path + digest.
            state = state_payload["state"]
            self.assertEqual(
                state["source"]["checklist_path"],
                "docs/project-harness/harness-checklist.json",
            )
            checklist_digest = hashlib.sha256(
                (harness_root / "harness-checklist.json").read_bytes()
            ).hexdigest()
            self.assertEqual(state["source"]["checklist_sha256"], checklist_digest)
            # The checklist item carries a single canonical plan locator.
            checklist_data = json.loads(
                (harness_root / "harness-checklist.json").read_text(encoding="utf-8")
            )
            item = checklist_data["items"][0]
            self.assertEqual(item["plan_path"], "docs/plan.md")
            self.assertEqual(item["artifacts"]["plan"], "docs/plan.md")
            self.assertEqual(item["status"], "todo")
            self.assertEqual(item["workflow"]["status"], "todo")

    def test_workspace_audit_no_refresh_reports_file_state_without_harnessctl(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            root = Path(tmp)
            (root / "mvp-checklist.json").write_text(
                json.dumps({"project": "demo", "harness_root": ".", "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            _write_harness_state_with_source(root)
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "workspace",
                "audit",
                "demo",
                "--no-refresh",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["harness_available"])
            self.assertTrue(payload["file_state_available"])
            self.assertTrue(payload["checklist_available"])
            self.assertFalse(payload["harnessctl_available"])
            self.assertFalse(payload["assignment_lifecycle_available"])

    def test_task_create_writes_plan_ready_event_and_task_mirror(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "task",
                "create",
                "demo",
                "--task-id",
                "phase-001",
                "--plan-doc",
                "plan.md",
                "--title",
                "Phase 001",
                "--owner",
                "claude",
                "--branch",
                "agents/claude/phase-001",
                "--payload-json",
                '{"test_baseline":"python -m unittest"}',
            )

            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertEqual(result["task"]["task_id"], "phase-001")
            self.assertEqual(result["task"]["phase"], "ready")
            self.assertEqual(result["task"]["last_event_id"], result["event"]["id"])
            self.assertEqual(result["event"]["event_type"], "plan.ready")
            self.assertEqual(result["event"]["payload"]["plan_doc"], "plan.md")
            self.assertEqual(result["event"]["payload"]["test_baseline"], "python -m unittest")
            checklist_payload = json.loads(checklist.read_text(encoding="utf-8"))
            items = checklist_payload["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["id"], "phase-001")
            self.assertEqual(items[0]["title"], "Phase 001")
            self.assertEqual(items[0]["plan_path"], "plan.md")
            self.assertEqual(items[0]["status"], "todo")
            self.assertEqual(items[0]["workflow"]["status"], "todo")
            self.assertIn("plan.md", items[0]["acceptance"])

    def test_task_create_repairs_missing_checklist_item_for_existing_plan_event(self):
        """A lost checklist item under an existing DB operation is NOT re-created:
        combined create fails closed and writes neither file nor DB."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            base_args = (
                "--db", db_path,
                "task", "create", "demo",
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--title", "Phase 001",
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            first_code, first_payload = self.run_cli(*base_args)
            self.assertEqual(first_code, 0)
            first_event_id = first_payload["result"]["event"]["id"]
            # Simulate the lost checklist item: wipe items but keep the DB.
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )

            code, payload = self.run_cli(*base_args)

            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "operation_conflict")
            # Zero file write: the checklist stays empty.
            checklist_payload = json.loads(checklist.read_text(encoding="utf-8"))
            self.assertEqual(checklist_payload["items"], [])
            # Zero new events: the original plan.ready remains the only one.
            _, events_payload = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo"
            )
            plan_ready = [
                e for e in events_payload["events"] if e["event_type"] == "plan.ready"
            ]
            self.assertEqual(len(plan_ready), 1)
            self.assertEqual(plan_ready[0]["id"], first_event_id)

    def test_task_create_repairs_invalid_existing_checklist_item(self):
        """A legacy unbound checklist item is NOT silently adopted: combined
        create fails closed with legacy_unbound_item and zero DB writes."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({
                    "project": "demo",
                    "harness_root": ".",
                    "version": 1,
                    "updated_at": "2026-07-13",
                    "items": [
                        {
                            "id": "phase-001",
                            "title": "Phase 001",
                            "status": "todo",
                            "priority": "p1",
                            "owner": None,
                            "human_gate_required": True,
                            "plan_path": "plan.md",
                            "acceptance": "acceptance text",
                            "blocked_by": [],
                            "blocked_reason": "",
                            "dependencies": [],
                            "handoff": {"from": None, "to": None, "reason": None},
                            "selected_in_session": None,
                            "updated_at": "2026-05-31T04:00:00Z",
                            "workflow": {"status": "todo", "branch": None, "updated_at": "2026-05-31T04:00:00Z"},
                            "artifacts": {"plan": "plan.md"},
                            "verification": "",
                            "review": {},
                        }
                    ],
                }),
                encoding="utf-8",
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "create", "demo",
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--title", "Phase 001",
            )

            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "legacy_unbound_item")
            # Zero DB writes: no plan.ready event.
            _, events_payload = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo"
            )
            plan_ready = [
                e for e in events_payload["events"] if e["event_type"] == "plan.ready"
            ]
            self.assertEqual(plan_ready, [])
            # File unchanged.
            checklist_payload = json.loads(checklist.read_text(encoding="utf-8"))
            self.assertEqual(len(checklist_payload["items"]), 1)
            self.assertNotIn("split_operation", checklist_payload["items"][0])

    def test_task_create_legacy_returns_host_aware_warning(self):
        """Combined create returns a combined receipt (file half + record half),
        with no legacy host-aware warning: the combined path IS the managed path."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "create", "demo",
                "--task-id", "phase-legacy",
                "--plan-doc", "plan.md",
                "--title", "Phase Legacy",
            )

            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertEqual(result["task"]["task_id"], "phase-legacy")
            self.assertEqual(result["event"]["event_type"], "plan.ready")
            self.assertNotIn("host_aware_warning", result)
            # Combined receipt: the file half result and the shared operation.
            self.assertEqual(
                result["files"]["operation_id"],
                result["operation"]["operation_id"],
            )
            self.assertTrue(result["files"]["checklist_changed"])

    def test_task_create_files_writes_checklist_without_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "coordinator.sqlite3"
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            operation_id = str(uuid.uuid4())

            code, payload = self.run_cli(
                "--db",
                str(db_path),
                "task",
                "create-files",
                "--workspace-path",
                tmp,
                "--harness-root",
                tmp,
                "--workspace-id",
                "demo",
                "--operation-id",
                operation_id,
                "--task-id",
                "phase-001",
                "--plan-doc",
                "plan.md",
                "--title",
                "Phase 001",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["checklist_changed"])
            self.assertFalse(db_path.exists())
            self.assertEqual(payload["result"]["operation_id"], operation_id)
            checklist_payload = json.loads(checklist.read_text(encoding="utf-8"))
            self.assertEqual(checklist_payload["items"][0]["id"], "phase-001")
            self.assertEqual(checklist_payload["items"][0]["split_operation"]["operation_id"], operation_id)

    def test_task_create_record_writes_db_without_checklist(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            operation_id = str(uuid.uuid4())
            _, files_payload = self.run_cli(
                "--db",
                ":memory:",
                "task",
                "create-files",
                "--workspace-path",
                tmp,
                "--harness-root",
                tmp,
                "--workspace-id",
                "demo",
                "--operation-id",
                operation_id,
                "--task-id",
                "phase-001",
                "--plan-doc",
                "plan.md",
                "--title",
                "Phase 001",
            )
            files_result = files_payload["result"]

            code, payload = self.run_cli(
                "--db",
                db_path,
                "task",
                "create-record",
                "demo",
                "--operation-id",
                operation_id,
                "--input-fingerprint",
                files_result["input_fingerprint"],
                "--before-fingerprint",
                files_result["before_fingerprint"],
                "--after-fingerprint",
                files_result["after_fingerprint"],
                "--task-id",
                "phase-001",
                "--plan-doc",
                "plan.md",
                "--title",
                "Phase 001",
            )

            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertEqual(result["task"]["task_id"], "phase-001")
            self.assertEqual(result["event"]["event_type"], "plan.ready")
            self.assertEqual(result["operation"]["operation_id"], operation_id)
            checklist_payload = json.loads(checklist.read_text(encoding="utf-8"))
            self.assertEqual(checklist_payload["items"][0]["id"], "phase-001")

    def test_task_create_files_refuses_runtime_copy_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            code, payload = self.run_cli(
                "task",
                "create-files",
                "--workspace-path",
                "/opt/multinexus",
                "--harness-root",
                tmp,
                "--workspace-id",
                "demo",
                "--operation-id",
                str(uuid.uuid4()),
                "--task-id",
                "phase-001",
                "--plan-doc",
                str(plan),
            )

            self.assertEqual(code, 1)
            self.assertIn("coding-host git checkout", payload["error"]["message"])

    def test_runner_add_and_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            add_code, add_payload = self.run_cli(
                "--db",
                db_path,
                "runner",
                "add",
                "codex",
                "--runner-type",
                "codex_cli",
                "--command",
                "codex",
                "--working-directory-strategy",
                "git_worktree",
                "--supports-stream-attach",
                "--env-json",
                '{"CODEX_HOME":"/tmp/codex"}',
            )
            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "runner",
                "list",
            )

            self.assertEqual(add_code, 0)
            self.assertEqual(add_payload["runner_profile"]["runner_type"], "codex_cli")
            self.assertTrue(add_payload["runner_profile"]["supports_stream_attach"])
            self.assertEqual(list_code, 0)
            self.assertEqual(list_payload["runner_profiles"][0]["command"], "codex")

    def test_runner_examples_are_available_without_registering_profiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "runner",
                "examples",
            )
            show_code, show_payload = self.run_cli(
                "--db",
                db_path,
                "runner",
                "example",
                "codex-wrapper",
            )

            self.assertEqual(list_code, 0)
            self.assertEqual(
                [example["id"] for example in list_payload["runner_profile_examples"]],
                ["codex-wrapper", "claude-wrapper"],
            )
            self.assertEqual(show_code, 0)
            profile = show_payload["runner_profile_example"]["runner_profile"]
            self.assertEqual(profile["runner_type"], "generic_subprocess")
            self.assertIn("{result_path}", profile["command"])

    def test_reconcile_no_refresh_syncs_task_mirror(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            root = Path(tmp)
            (root / "mvp-checklist.json").write_text(
                json.dumps(
                    {
                        "project": "demo",
                        "harness_root": ".",
                        "updated_at": "2026-07-13",
                        "items": [_audit_item()],
                    }
                ),
                encoding="utf-8",
            )
            _write_harness_state_with_source(root)
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "reconcile",
                "demo",
                "--no-refresh",
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["reconciliation"]["created"], 1)
            self.assertEqual(payload["reconciliation"]["tasks"][0]["phase"], "running")

    def test_job_create_run_and_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            self.run_cli(
                "--db",
                db_path,
                "runner",
                "add",
                "subprocess",
                "--runner-type",
                "generic_subprocess",
                "--command",
                "printf 'cli {task_id}'",
            )

            create_code, create_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "create",
                "demo",
                "--runner-profile-id",
                "subprocess",
                "--task-id",
                "mvp-001",
            )
            job_id = create_payload["job"]["id"]
            run_code, run_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "run",
                job_id,
            )
            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "list",
                "--workspace-id",
                "demo",
            )

            self.assertEqual(create_code, 0)
            self.assertEqual(create_payload["job"]["status"], "pending")
            self.assertEqual(run_code, 0)
            self.assertEqual(run_payload["result"]["job"]["status"], "done")
            self.assertEqual(list_code, 0)
            self.assertEqual(list_payload["jobs"][0]["status"], "done")
            self.assertTrue(Path(run_payload["result"]["log_path"]).exists())

    def test_job_pump_runs_pending_jobs_with_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            self.run_cli(
                "--db",
                db_path,
                "runner",
                "add",
                "subprocess",
                "--runner-type",
                "generic_subprocess",
                "--command",
                "printf 'pump {task_id}'",
            )
            for index in range(3):
                self.run_cli(
                    "--db",
                    db_path,
                    "job",
                    "create",
                    "demo",
                    "--runner-profile-id",
                    "subprocess",
                    "--task-id",
                    f"mvp-00{index}",
                )

            pump_code, pump_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "pump",
                "--workspace-id",
                "demo",
                "--limit",
                "2",
            )
            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "list",
                "--workspace-id",
                "demo",
            )

            self.assertEqual(pump_code, 0)
            self.assertEqual(pump_payload["result"]["processed"], 2)
            self.assertEqual(pump_payload["result"]["done"], 2)
            self.assertEqual(list_code, 0)
            self.assertEqual(
                [job["status"] for job in list_payload["jobs"]],
                ["done", "done", "pending"],
            )

    def test_job_cancel_and_retry_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            self.run_cli(
                "--db",
                db_path,
                "runner",
                "add",
                "subprocess",
                "--runner-type",
                "generic_subprocess",
                "--command",
                "python3 -c 'import sys; sys.exit(3)'",
            )

            _, cancel_create = self.run_cli(
                "--db",
                db_path,
                "job",
                "create",
                "demo",
                "--runner-profile-id",
                "subprocess",
                "--task-id",
                "mvp-cancel",
            )
            cancel_code, cancel_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "cancel",
                cancel_create["job"]["id"],
                "--reason",
                "duplicate",
            )
            _, failed_create = self.run_cli(
                "--db",
                db_path,
                "job",
                "create",
                "demo",
                "--runner-profile-id",
                "subprocess",
                "--task-id",
                "mvp-retry",
            )
            self.run_cli(
                "--db",
                db_path,
                "job",
                "run",
                failed_create["job"]["id"],
            )
            retry_code, retry_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "retry",
                failed_create["job"]["id"],
                "--reason",
                "retry after failure",
            )

            self.assertEqual(cancel_code, 0)
            self.assertEqual(cancel_payload["result"]["job"]["status"], "cancelled")
            self.assertEqual(
                cancel_payload["result"]["event"]["event_type"],
                "job.cancelled",
            )
            self.assertEqual(retry_code, 0)
            self.assertEqual(retry_payload["result"]["source_job"]["status"], "failed")
            self.assertEqual(retry_payload["result"]["retry_job"]["status"], "pending")
            self.assertEqual(
                retry_payload["result"]["event"]["event_type"],
                "job.retry_requested",
            )

    def test_job_create_result_path_is_used_by_runner_output_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            command = (
                "python3 -c "
                + repr(
                    "import json, os; "
                    "json.dump(dict(status='done', summary='CLI response'), "
                    "open(os.environ['COORDINATOR_RESULT_PATH'], 'w'))"
                )
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            self.run_cli(
                "--db",
                db_path,
                "runner",
                "add",
                "subprocess",
                "--runner-type",
                "generic_subprocess",
                "--command",
                command,
            )

            _, create_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "create",
                "demo",
                "--runner-profile-id",
                "subprocess",
                "--task-id",
                "mvp-001",
                "--result-path",
                "custom-result.json",
            )
            run_code, run_payload = self.run_cli(
                "--db",
                db_path,
                "job",
                "run",
                create_payload["job"]["id"],
            )

            self.assertEqual(run_code, 0)
            result = run_payload["result"]["job"]["result"]
            self.assertEqual(result["summary"], "CLI response")
            self.assertTrue(result["result_path"].endswith("custom-result.json"))

    def test_delivery_create_and_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            create_code, create_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "create",
                "--platform",
                "stdout",
                "--destination",
                "local",
                "--message-key",
                "demo:message:1",
                "--payload-json",
                '{"text":"[ASSIGN] mvp-001"}',
            )
            second_code, second_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "create",
                "--platform",
                "stdout",
                "--destination",
                "local",
                "--message-key",
                "demo:message:1",
                "--payload-json",
                '{"text":"[ASSIGN] mvp-001"}',
            )
            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "list",
                "--status",
                "pending",
            )

            self.assertEqual(create_code, 0)
            self.assertTrue(create_payload["created"])
            self.assertEqual(second_code, 0)
            self.assertFalse(second_payload["created"])
            self.assertEqual(list_code, 0)
            self.assertEqual(len(list_payload["deliveries"]), 1)

    def test_delivery_send_keeps_cli_stdout_json_parseable(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            _, create_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "create",
                "--platform",
                "stdout",
                "--destination",
                "local",
                "--message-key",
                "demo:message:1",
                "--payload-json",
                '{"text":"[RESULT] mvp-001"}',
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "send",
                create_payload["delivery"]["id"],
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["sent"])
            self.assertEqual(payload["result"]["delivery"]["status"], "sent")

    def test_policy_create_delivery_is_idempotent_and_pumpable(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            _, event_payload = self.run_cli(
                "--db",
                db_path,
                "event",
                "append",
                "job.completed",
                "--workspace-id",
                "demo",
                "--actor",
                "runner",
                "--task-id",
                "mvp-001",
                "--payload-json",
                '{"job_id":"job-1","logs_path":"/tmp/job-1.log"}',
            )
            event_id = event_payload["event"]["id"]

            create_code, create_payload = self.run_cli(
                "--db",
                db_path,
                "policy",
                "create-delivery",
                event_id,
                "--platform",
                "stdout",
                "--destination",
                "local",
            )
            second_code, second_payload = self.run_cli(
                "--db",
                db_path,
                "policy",
                "create-delivery",
                event_id,
                "--platform",
                "stdout",
                "--destination",
                "local",
            )
            pump_code, pump_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "pump",
                "--platform",
                "stdout",
            )

            self.assertEqual(create_code, 0)
            self.assertTrue(create_payload["result"]["created"])
            self.assertEqual(create_payload["result"]["payload"]["visible_header"], "[RESULT]")
            self.assertEqual(second_code, 0)
            self.assertFalse(second_payload["result"]["created"])
            self.assertEqual(
                create_payload["result"]["delivery"]["id"],
                second_payload["result"]["delivery"]["id"],
            )
            self.assertEqual(pump_code, 0)
            self.assertEqual(pump_payload["result"]["sent"], 1)

    def test_policy_create_deliveries_creates_agent_handoff_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "agent",
                "add",
                "demo",
                "--name",
                "mac-claude",
                "--discord-user-id",
                "123456789",
                "--reason",
                "test",
            )
            _, event_payload = self.run_cli(
                "--db",
                db_path,
                "event",
                "append",
                "worker.handoff.prepared",
                "--workspace-id",
                "demo",
                "--actor",
                "operator",
                "--target",
                "worker",
                "--task-id",
                "phase-001",
                "--payload-json",
                json.dumps({
                    "task_id": "phase-001",
                    "role": "worker",
                    "target_agent": "mac-claude",
                    "bootstrap_path": "docs/tasks/phase-001/worker-bootstrap.md",
                    "handoff_text": "handoff text",
                }),
            )
            event_id = event_payload["event"]["id"]

            code, payload = self.run_cli(
                "--db",
                db_path,
                "policy",
                "create-deliveries",
                event_id,
                "--platform",
                "discord_webhook",
                "--destination",
                "channel-1",
            )
            second_code, second_payload = self.run_cli(
                "--db",
                db_path,
                "policy",
                "create-deliveries",
                event_id,
                "--platform",
                "discord_webhook",
                "--destination",
                "channel-1",
            )

            self.assertEqual(code, 0)
            self.assertEqual(len(payload["results"]), 2)
            self.assertTrue(all(result["created"] for result in payload["results"]))
            self.assertEqual(
                payload["results"][0]["payload"]["visible_header"],
                "[HANDOFF_STATUS]",
            )
            self.assertIn(
                "[handoff] <@123456789>",
                payload["results"][1]["payload"]["text"],
            )
            self.assertEqual(second_code, 0)
            self.assertEqual(len(second_payload["results"]), 2)
            self.assertFalse(any(result["created"] for result in second_payload["results"]))

    def test_policy_render_unsupported_event_reports_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db",
                db_path,
                "workspace",
                "add",
                "demo",
                "--path",
                tmp,
                "--harness-root",
                tmp,
            )
            _, event_payload = self.run_cli(
                "--db",
                db_path,
                "event",
                "append",
                "unknown.event",
                "--workspace-id",
                "demo",
                "--actor",
                "operator",
                "--task-id",
                "mvp-001",
            )

            code, payload = self.run_cli(
                "--db",
                db_path,
                "policy",
                "render-event",
                event_payload["event"]["id"],
                "--platform",
                "stdout",
                "--destination",
                "local",
            )

            self.assertEqual(code, 0)
            self.assertFalse(payload["result"]["supported"])
            self.assertIn("unsupported event type", payload["result"]["reason"])

    def test_discord_send_without_token_fails_without_losing_delivery(self):
        old_token = os.environ.pop("DISCORD_BOT_TOKEN", None)
        self.addCleanup(self._restore_env, "DISCORD_BOT_TOKEN", old_token)
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            _, create_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "create",
                "--platform",
                "discord",
                "--destination",
                "channel-1",
                "--message-key",
                "demo:message:discord:1",
                "--payload-json",
                '{"text":"[RESULT] mvp-001"}',
            )

            code, stdout, stderr = self.run_cli_raw(
                "--db",
                db_path,
                "delivery",
                "send",
                create_payload["delivery"]["id"],
            )
            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "list",
                "--platform",
                "discord",
            )

            self.assertEqual(code, 1)
            self.assertEqual(stdout, "")
            self.assertIn("DISCORD_BOT_TOKEN", stderr)
            self.assertEqual(list_code, 0)
            self.assertEqual(list_payload["deliveries"][0]["status"], "pending")

    def test_worker_delivery_once_pumps_pending_stdout_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            _, create_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "create",
                "--platform",
                "stdout",
                "--destination",
                "local",
                "--message-key",
                "demo:message:worker:1",
                "--payload-json",
                '{"text":"[STATE] reconciled"}',
            )
            delivery_id = create_payload["delivery"]["id"]

            code, stdout, stderr = self.run_cli_raw(
                "--db",
                db_path,
                "worker",
                "delivery",
                "--platform",
                "stdout",
                "--once",
            )
            payload = json.loads(stdout)
            list_code, list_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "list",
                "--platform",
                "stdout",
            )

            self.assertEqual(code, 0)
            self.assertIn("[STATE] reconciled", stderr)
            self.assertEqual(payload["result"]["iterations"], 1)
            self.assertEqual(payload["result"]["sent"], 1)
            self.assertEqual(list_code, 0)
            self.assertEqual(list_payload["deliveries"][0]["id"], delivery_id)
            self.assertEqual(list_payload["deliveries"][0]["status"], "sent")

    def test_delivery_recover_sending_command_resets_crashed_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            _, create_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "create",
                "--platform",
                "stdout",
                "--destination",
                "local",
                "--message-key",
                "demo:message:recover",
                "--payload-json",
                '{"text":"[STATE] recover"}',
            )
            conn = initialize(db_path)
            try:
                mark_delivery_sending(conn, create_payload["delivery"]["id"])
            finally:
                conn.close()

            recover_code, recover_payload = self.run_cli(
                "--db",
                db_path,
                "delivery",
                "recover-sending",
                "--platform",
                "stdout",
            )

            self.assertEqual(recover_code, 0)
            self.assertEqual(recover_payload["recovered"], 1)
            self.assertEqual(recover_payload["deliveries"][0]["status"], "pending")
            self.assertIn("recovered from sending", recover_payload["deliveries"][0]["last_error"])

    def _restore_env(self, key, value):
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value

    # --- assignment request CLI tests ---

    def _setup_assignment_workspace(self, tmp, *, success=True,
                                   default_bus=None, default_destination=None):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            harnessctl_path.write_text("#!/bin/bash\necho 'error' >&2\nexit 1\n")
        harnessctl_path.chmod(0o755)
        ws_args = [
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        ]
        if default_bus:
            ws_args.extend(["--default-bus", default_bus])
        if default_destination:
            ws_args.extend(["--default-destination", default_destination])
        self.run_cli(*ws_args)
        return db_path

    def test_assignment_request_success_creates_event_and_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_assignment_workspace(
                tmp, default_bus="stdout", default_destination="local",
            )
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "request", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "assignment.requested")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertIsNotNone(result["delivery"])
            self.assertTrue(result["delivery_created"])
            self.assertEqual(result["delivery"]["platform"], "stdout")
            self.assertEqual(result["delivery"]["destination"], "local")
            self.assertEqual(result["delivery"]["status"], "pending")

    def test_assignment_request_idempotent_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_assignment_workspace(
                tmp, default_bus="stdout", default_destination="local",
            )
            args = [
                "--db", db_path,
                "assignment", "request", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(
                first_payload["result"]["delivery"]["id"],
                second_payload["result"]["delivery"]["id"],
            )
            _, list_payload = self.run_cli("--db", db_path, "delivery", "list")
            self.assertEqual(len(list_payload["deliveries"]), 1)

    def test_assignment_request_platform_destination_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_assignment_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "request", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
                "--platform", "discord",
                "--destination", "ch-1",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["delivery"])
            self.assertTrue(result["delivery_created"])
            self.assertEqual(result["delivery"]["platform"], "discord")
            self.assertEqual(result["delivery"]["destination"], "ch-1")

    def test_assignment_request_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_assignment_workspace(
                tmp, success=False,
                default_bus="stdout", default_destination="local",
            )
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "request", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIsNotNone(result["delivery"])
            self.assertTrue(result["delivery_created"])
            self.assertEqual(result["delivery"]["platform"], "stdout")
            self.assertEqual(result["delivery"]["destination"], "local")
            self.assertEqual(result["delivery"]["status"], "pending")
            self.assertEqual(result["delivery"]["payload"]["visible_header"], "[BLOCKER]")
            _, list_payload = self.run_cli("--db", db_path, "delivery", "list")
            self.assertEqual(len(list_payload["deliveries"]), 1)

    def test_assignment_request_failure_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\necho 'error' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
                "--default-bus", "stdout",
                "--default-destination", "local",
            )
            args = [
                "--db", db_path,
                "assignment", "request", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 1)
            _, list_payload = self.run_cli("--db", db_path, "delivery", "list")
            self.assertEqual(len(list_payload["deliveries"]), 1)

    def test_assignment_request_missing_harnessctl_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = Path(tmp) / "missing-harnessctl"
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(missing_harnessctl),
                "--default-bus", "stdout",
                "--default-destination", "local",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "request", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIn(
                "configured harnessctl_path not found",
                result["event"]["payload"]["stderr"],
            )
            self.assertIsNotNone(result["delivery"])
            self.assertEqual(result["delivery"]["payload"]["visible_header"], "[BLOCKER]")

    def test_assignment_request_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
                "--default-bus", "stdout",
                "--default-destination", "local",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "request", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIn("missing-workspace", result["event"]["payload"]["stderr"])
            self.assertIsNotNone(result["delivery"])
            self.assertEqual(result["delivery"]["payload"]["visible_header"], "[BLOCKER]")

    # --- assignment accept CLI tests ---

    def _setup_accept_workspace(self, tmp, *, success=True):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            harnessctl_path.write_text("#!/bin/bash\necho 'error' >&2\nexit 1\n")
        harnessctl_path.chmod(0o755)
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_assignment_accept_success_creates_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_accept_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "assignment.accepted")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertEqual(result["event"]["actor"], "codex")
            self.assertEqual(result["event"]["target"], "codex")

    def test_assignment_accept_returns_latest_prepared_bootstrap_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_accept_workspace(tmp)
            self.run_cli(
                "--db", db_path,
                "event", "append", "worker.handoff.prepared",
                "--workspace-id", "demo",
                "--task-id", "mvp-001",
                "--actor", "operator",
                "--target", "worker",
                "--idempotency-key", "demo:mvp-001:prepared:codex",
                "--payload-json", json.dumps({
                    "task_id": "mvp-001",
                    "target_agent": "codex",
                    "bootstrap_path": "docs/project-harness/tasks/mvp-001/worker-bootstrap.md",
                    "bootstrap_text": "# Worker Bootstrap\ncd /host/workspace",
                    "execution_profile": {
                        "host_id": "mac",
                        "workspace_path": "/host/workspace",
                    },
                }),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )

            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertEqual(result["bootstrap_text"], "# Worker Bootstrap\ncd /host/workspace")
            self.assertEqual(
                result["bootstrap_path"],
                "docs/project-harness/tasks/mvp-001/worker-bootstrap.md",
            )
            self.assertEqual(result["execution_profile"]["workspace_path"], "/host/workspace")

    def test_assignment_accept_default_actor_is_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_accept_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )
            self.assertEqual(payload["result"]["event"]["actor"], "codex")

    def test_assignment_accept_actor_overrides_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_accept_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
                "--actor", "operator",
            )
            self.assertEqual(payload["result"]["event"]["actor"], "operator")

    def test_assignment_accept_branch_in_mutation_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_accept_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
                "--branch", "agent/codex/mvp-001",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["branch"], "agent/codex/mvp-001")
            self.assertIn("--branch", result["mutation"]["command"])
            self.assertIn("agent/codex/mvp-001", result["mutation"]["command"])

    def test_assignment_accept_idempotent_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_accept_workspace(tmp)
            args = [
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            _, event_payload = self.run_cli("--db", db_path, "event", "list", "--workspace-id", "demo")
            self.assertEqual(len(event_payload["events"]), 1)

    def test_assignment_accept_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_accept_workspace(tmp, success=False)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "accept")

    def test_assignment_accept_failure_idempotent_no_repeat_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\necho 'error' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            args = [
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 1)

    def test_assignment_accept_missing_harnessctl_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", missing_harnessctl,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIn(
                "not found",
                result["event"]["payload"]["stderr"],
            )

    def test_assignment_accept_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "accept", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
                "--session", "sess-1",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIn("missing-workspace", result["event"]["payload"]["stderr"])

    # --- assignment handoff CLI tests ---

    def _setup_handoff_workspace(self, tmp, *, success=True):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            harnessctl_path.write_text("#!/bin/bash\necho 'error' >&2\nexit 1\n")
        harnessctl_path.chmod(0o755)
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_assignment_handoff_success_creates_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_handoff_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "handoff.requested")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertEqual(result["event"]["actor"], "operator")
            self.assertEqual(result["event"]["target"], "claude")

    def test_assignment_handoff_default_actor_is_operator(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_handoff_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
            )
            self.assertEqual(payload["result"]["event"]["actor"], "operator")

    def test_assignment_handoff_explicit_actor_overrides_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_handoff_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
                "--actor", "codex",
            )
            self.assertEqual(payload["result"]["event"]["actor"], "codex")

    def test_assignment_handoff_reason_in_payload_and_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_handoff_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
                "--reason", "codex busy",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["reason"], "codex busy")
            self.assertIn("--reason", result["mutation"]["command"])
            self.assertIn("codex busy", result["mutation"]["command"])

    def test_assignment_handoff_idempotent_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_handoff_workspace(tmp)
            args = [
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            _, event_payload = self.run_cli("--db", db_path, "event", "list", "--workspace-id", "demo")
            self.assertEqual(len(event_payload["events"]), 1)

    def test_assignment_handoff_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_handoff_workspace(tmp, success=False)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "handoff")

    def test_assignment_handoff_failure_idempotent_no_repeat_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\necho 'error' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            args = [
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 1)

    def test_assignment_handoff_missing_harnessctl_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", missing_harnessctl,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "handoff")
            self.assertIn(
                "not found",
                result["event"]["payload"]["stderr"],
            )

    def test_assignment_handoff_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "handoff", "demo",
                "--task-id", "mvp-001",
                "--target", "claude",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "handoff")
            self.assertIn("missing-workspace", result["event"]["payload"]["stderr"])

    # --- assignment blocker CLI tests ---

    def _setup_blocker_workspace(self, tmp, *, success=True):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            harnessctl_path.write_text("#!/bin/bash\necho 'error' >&2\nexit 1\n")
        harnessctl_path.chmod(0o755)
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_assignment_blocker_success_creates_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_blocker_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "blocker.raised")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertEqual(result["event"]["actor"], "operator")

    def test_assignment_blocker_default_actor_is_operator(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_blocker_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(payload["result"]["event"]["actor"], "operator")

    def test_assignment_blocker_explicit_actor_overrides_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_blocker_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
            )
            self.assertEqual(payload["result"]["event"]["actor"], "codex")

    def test_assignment_blocker_reason_in_payload_and_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_blocker_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
                "--reason", "stuck on dependency",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["reason"], "stuck on dependency")
            self.assertIn("--reason", result["mutation"]["command"])
            self.assertIn("stuck on dependency", result["mutation"]["command"])

    def test_assignment_blocker_idempotent_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_blocker_workspace(tmp)
            args = [
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            _, event_payload = self.run_cli("--db", db_path, "event", "list", "--workspace-id", "demo")
            self.assertEqual(len(event_payload["events"]), 1)

    def test_assignment_blocker_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_blocker_workspace(tmp, success=False)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "blocker")

    def test_assignment_blocker_failure_idempotent_no_repeat_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\necho 'error' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            args = [
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 1)

    def test_assignment_blocker_missing_harnessctl_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", missing_harnessctl,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "blocker")
            self.assertIn(
                "not found",
                result["event"]["payload"]["stderr"],
            )

    def test_assignment_blocker_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "blocker", "demo",
                "--task-id", "mvp-001",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "blocker")
            self.assertIn("missing-workspace", result["event"]["payload"]["stderr"])

    # --- assignment unblock CLI tests ---

    def _setup_unblock_workspace(self, tmp, *, success=True):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            harnessctl_path.write_text("#!/bin/bash\necho 'error' >&2\nexit 1\n")
        harnessctl_path.chmod(0o755)
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_assignment_unblock_success_creates_blocker_resolved_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_unblock_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "proceed",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "blocker.resolved")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertEqual(result["event"]["actor"], "codex")

    def test_assignment_unblock_actor_and_decision_in_event_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_unblock_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "proceed",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["decision"], "proceed")
            self.assertEqual(result["mutation"]["actor"], "codex")
            self.assertIn("--decision", result["mutation"]["command"])
            self.assertIn("proceed", result["mutation"]["command"])

    def test_assignment_unblock_force_and_reason_in_payload_and_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_unblock_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "override",
                "--force",
                "--reason", "urgent fix needed",
            )
            result = payload["result"]
            self.assertTrue(result["event"]["payload"]["force"])
            self.assertEqual(result["event"]["payload"]["reason"], "urgent fix needed")
            self.assertIn("--force", result["mutation"]["command"])
            self.assertIn("--reason", result["mutation"]["command"])
            self.assertIn("urgent fix needed", result["mutation"]["command"])

    def test_assignment_unblock_idempotent_success_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_unblock_workspace(tmp)
            args = [
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "proceed",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            _, event_payload = self.run_cli("--db", db_path, "event", "list", "--workspace-id", "demo")
            self.assertEqual(len(event_payload["events"]), 1)

    def test_assignment_unblock_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_unblock_workspace(tmp, success=False)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "proceed",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "unblock")

    def test_assignment_unblock_failure_idempotent_no_repeat_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\necho 'error' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            args = [
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "proceed",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 1)

    def test_assignment_unblock_missing_harnessctl_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", missing_harnessctl,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "proceed",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "unblock")
            self.assertIn(
                "not found",
                result["event"]["payload"]["stderr"],
            )

    def test_assignment_unblock_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "unblock", "demo",
                "--task-id", "mvp-001",
                "--actor", "codex",
                "--decision", "proceed",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "unblock")
            self.assertIn("missing-workspace", result["event"]["payload"]["stderr"])

    # --- assignment closeout CLI tests ---

    def _setup_closeout_workspace(self, tmp, *, success=True):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            harnessctl_path.write_text("#!/bin/bash\necho 'error' >&2\nexit 1\n")
        harnessctl_path.chmod(0o755)
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_assignment_closeout_success_creates_closeout_requested_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_closeout_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "closeout.requested")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["actor"], "operator")

    def test_assignment_closeout_self_test_evidence_in_payload_and_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_closeout_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--self-test-evidence", "Deploy SHA: abc123; E2E: passed",
            )
            result = payload["result"]
            self.assertEqual(
                result["event"]["payload"]["self_test_evidence"],
                "Deploy SHA: abc123; E2E: passed",
            )
            self.assertIn("--self-test-evidence", result["mutation"]["command"])
            self.assertIn("Deploy SHA: abc123; E2E: passed", result["mutation"]["command"])

    def test_assignment_closeout_default_self_test_evidence_is_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_closeout_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["self_test_evidence"], "")

    def test_assignment_closeout_reviewer_in_event_payload_and_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_closeout_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["reviewer"], "alice")
            self.assertIn("alice", result["mutation"]["command"])

    def test_assignment_closeout_actor_default_and_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_closeout_workspace(tmp)
            # default actor
            _, default_payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            )
            self.assertEqual(
                default_payload["result"]["event"]["actor"], "operator"
            )
            # explicit actor
            _, explicit_payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-002",
                "--reviewer", "bob",
                "--actor", "codex",
            )
            self.assertEqual(
                explicit_payload["result"]["event"]["actor"], "codex"
            )

    def test_assignment_closeout_idempotent_success_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_closeout_workspace(tmp)
            args = [
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            _, event_payload = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo"
            )
            self.assertEqual(len(event_payload["events"]), 1)

    def test_assignment_closeout_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_closeout_workspace(tmp, success=False)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "closeout")

    def test_assignment_closeout_failure_idempotent_no_repeat_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\necho 'error' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            args = [
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 1)

    def test_assignment_closeout_missing_harnessctl_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", missing_harnessctl,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIn(
                "not found",
                result["event"]["payload"]["stderr"],
            )

    def test_assignment_closeout_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "closeout", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIn("missing-workspace", result["event"]["payload"]["stderr"])

    # --- assignment review-result CLI tests ---

    def _setup_review_result_workspace(self, tmp, *, success=True):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            harnessctl_path.write_text("#!/bin/bash\necho 'error' >&2\nexit 1\n")
        harnessctl_path.chmod(0o755)
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_assignment_review_result_success_creates_review_completed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_review_result_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "review.completed")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["actor"], "operator")

    def test_assignment_review_result_reviewer_decision_in_payload_and_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_review_result_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["reviewer"], "alice")
            self.assertEqual(result["event"]["payload"]["decision"], "approved")
            self.assertIn("alice", result["mutation"]["command"])
            self.assertIn("approved", result["mutation"]["command"])

    def test_assignment_review_result_summary_in_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_review_result_workspace(tmp)
            _, payload = self.run_cli(
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
                "--summary", "looks good",
            )
            result = payload["result"]
            self.assertEqual(result["event"]["payload"]["summary"], "looks good")
            self.assertIn("--summary", result["mutation"]["command"])
            self.assertIn("looks good", result["mutation"]["command"])

    def test_assignment_review_result_idempotent_success_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_review_result_workspace(tmp)
            args = [
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            _, event_payload = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo"
            )
            self.assertEqual(len(event_payload["events"]), 1)

    def test_assignment_review_result_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_review_result_workspace(tmp, success=False)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "review-result")

    def test_assignment_review_result_failure_idempotent_no_repeat_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\necho 'error' >&2\nexit 1\n"
            )
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            args = [
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 1)

    def test_assignment_review_result_missing_harnessctl_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", missing_harnessctl,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertIn(
                "not found",
                result["event"]["payload"]["stderr"],
            )

    def test_assignment_review_result_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "review-result", "demo",
                "--task-id", "mvp-001",
                "--reviewer", "alice",
                "--decision", "approved",
            )

            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "review-result")
            self.assertIn("missing-workspace", result["event"]["payload"]["stderr"])

    # --- assignment mark-done CLI tests ---

    def _setup_mark_done_workspace(self, tmp, *, success=True):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        if success:
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        else:
            # Gate needs harnessctl state to succeed; mutation should fail
            harnessctl_path.write_text(
                "#!/bin/bash\nif [ \"$1\" = \"state\" ]; then echo 'ok'; exit 0; "
                "else echo 'error' >&2; exit 1; fi\n"
            )
        harnessctl_path.chmod(0o755)
        # Provide harness-state.json and mvp-checklist.json so gate precheck passes
        import json as _json
        (Path(tmp) / "mvp-checklist.json").write_text(_json.dumps({
            "project": "demo",
            "harness_root": ".",
            "updated_at": "2026-01-01",
            "items": [
                _mark_done_item("mvp-001"),
                _mark_done_item("mvp-002"),
            ]
        }))
        _write_harness_state_with_source(Path(tmp))
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_assignment_mark_done_success_creates_task_done_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_mark_done_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertTrue(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "task.done")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertEqual(result["event"]["workspace_id"], "demo")
            self.assertEqual(result["event"]["actor"], "operator")

    def test_assignment_mark_done_actor_default_and_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_mark_done_workspace(tmp)
            # default actor
            _, default_payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(
                default_payload["result"]["event"]["actor"], "operator"
            )
            # explicit actor
            _, explicit_payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-002",
                "--actor", "codex",
            )
            self.assertEqual(
                explicit_payload["result"]["event"]["actor"], "codex"
            )

    def test_assignment_mark_done_idempotent_success_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_mark_done_workspace(tmp)
            args = [
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            _, event_payload = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo"
            )
            task_done_events = [
                e for e in event_payload["events"]
                if e["event_type"] == "task.done"
            ]
            self.assertEqual(len(task_done_events), 1)

    def test_assignment_mark_done_failure_returns_nonzero_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_mark_done_workspace(tmp, success=False)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 1)
            result = payload["result"]
            self.assertIsNotNone(result["mutation"])
            self.assertFalse(result["mutation"]["success"])
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "harness.mutation_failed")
            self.assertEqual(result["event"]["payload"]["operation"], "mark-done")

    def test_assignment_mark_done_failure_idempotent_no_repeat_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            counter_path = Path(tmp) / "runs.txt"
            harnessctl_path.write_text(
                f"#!/bin/bash\necho run >> {counter_path}\n"
                f"if [ \"$1\" = \"state\" ]; then echo 'ok'; exit 0; "
                f"else echo 'error' >&2; exit 1; fi\n"
            )
            harnessctl_path.chmod(0o755)
            import json as _json
            (Path(tmp) / "mvp-checklist.json").write_text(_json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [
                    _mark_done_item("mvp-001"),
                ]
            }))
            _write_harness_state_with_source(Path(tmp))
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            args = [
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            ]

            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 1)
            self.assertEqual(second_code, 1)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])
            self.assertIsNone(second_payload["result"]["mutation"])
            self.assertEqual(
                first_payload["result"]["event"]["id"],
                second_payload["result"]["event"]["id"],
            )
            self.assertEqual(counter_path.read_text().count("run"), 2)

    def test_assignment_mark_done_missing_harnessctl_gate_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", missing_harnessctl,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )

            # Fail-closed: missing harnessctl → gate fails, CLI returns 1
            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNone(result["mutation"])
            self.assertFalse(result["event_created"])
            self.assertIn("gate", result)
            self.assertFalse(result["gate"]["passed"])

    def test_assignment_mark_done_invalid_workspace_path_returns_json_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", str(missing_workspace),
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )

            # Fail-closed: invalid workspace path → gate fails, CLI returns 1
            result = payload["result"]
            self.assertEqual(code, 1)
            self.assertIsNone(result["mutation"])
            self.assertFalse(result["event_created"])
            self.assertIn("gate", result)
            self.assertFalse(result["gate"]["passed"])

    # --- A1 host-aware mark-done CLI tests ---

    def test_assignment_mark_done_legacy_returns_host_aware_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_mark_done_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertIn("host_aware_warning", result)
            self.assertIn("mark-done-files", result["host_aware_warning"])
            self.assertIn("mark-done-record", result["host_aware_warning"])

    def test_assignment_mark_done_files_cli_writes_checklist(self):
        with tempfile.TemporaryDirectory() as tmp:
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [
                    _mark_done_item("mvp-001"),
                ]
            }), encoding="utf-8")

            code, payload = self.run_cli(
                "assignment", "mark-done-files",
                "--workspace-path", tmp,
                "--harness-root", tmp,
                "--task-id", "mvp-001",
                "--repair-reason", "legacy single-host reconcile",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertTrue(result["checklist_changed"])
            self.assertEqual(result["task_id"], "mvp-001")

            updated = json.loads(checklist.read_text(encoding="utf-8"))
            item = updated["items"][0]
            self.assertEqual(item["status"], "done")
            self.assertEqual(item["workflow"]["status"], "closed")

    def test_assignment_mark_done_files_cli_rejects_opt_path(self):
        code, payload = self.run_cli(
            "assignment", "mark-done-files",
            "--workspace-path", "/opt/coordinate",
            "--harness-root", "/opt/coordinate/docs/project-harness",
            "--task-id", "mvp-001",
            "--repair-reason", "must still hit the /opt guard",
        )
        self.assertEqual(code, 1)
        self.assertIn("/opt", payload["error"]["message"])

    def test_assignment_mark_done_files_cli_rejects_exact_opt_path(self):
        """Guard must reject /opt exactly (not just /opt/...)."""
        code, payload = self.run_cli(
            "assignment", "mark-done-files",
            "--workspace-path", "/opt",
            "--harness-root", "/opt",
            "--task-id", "mvp-001",
            "--repair-reason", "must still hit the /opt guard",
        )
        self.assertEqual(code, 1)
        self.assertIn("/opt", payload["error"]["message"])

    def test_assignment_mark_done_record_cli_creates_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-record", "demo",
                "--task-id", "mvp-001",
                "--repair-reason", "drift reconciliation",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "task.done")
            self.assertEqual(result["event"]["task_id"], "mvp-001")
            self.assertTrue(result["event"]["payload"]["repair_only"])
            self.assertEqual(
                result["event"]["payload"]["repair_reason"], "drift reconciliation"
            )

    def test_assignment_mark_done_record_cli_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            args = [
                "--db", db_path,
                "assignment", "mark-done-record", "demo",
                "--task-id", "mvp-001",
                "--repair-reason", "drift reconciliation",
            ]
            first_code, first_payload = self.run_cli(*args)
            second_code, second_payload = self.run_cli(*args)

            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first_payload["result"]["event_created"])
            self.assertFalse(second_payload["result"]["event_created"])

    # --- Slice 3 receipt-protocol CLI tests ---

    def _setup_receipt_workspace(self, tmp):
        """Workspace whose gate passes for mvp-001 (review_approved)."""
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        harnessctl_path.chmod(0o755)
        (Path(tmp) / "harness-state.json").write_text(json.dumps({
            "project": "demo", "current_item": None,
        }))
        (Path(tmp) / "mvp-checklist.json").write_text(json.dumps({
            "project": "demo",
            "harness_root": ".",
            "updated_at": "2026-01-01",
            "items": [
                _mark_done_item("mvp-001", branch="feat-x"),
            ]
        }))
        _write_harness_state_with_source(Path(tmp))
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp, "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path



    def _prepare_claim_apply_receipt(self, db_path, tmp, *, task_id="mvp-001"):
        """Drive a receipt authorized -> claimed -> applied via the CLI."""
        _, prep = self.run_cli(
            "--db", db_path,
            "assignment", "mark-done-prepare", "demo",
            "--task-id", task_id,
        )
        receipt_id = prep["result"]["receipt_id"]
        from coordinate.completion import compute_mark_done_fingerprints
        fps = compute_mark_done_fingerprints(harness_root=tmp, task_id=task_id)
        self.run_cli(
            "--db", db_path,
            "assignment", "mark-done-claim", receipt_id,
            "--workspace-id", "demo",
            "--task-id", task_id,
            "--actor", "operator",
            "--before-fingerprint", fps.before_fingerprint,
            "--after-fingerprint", fps.after_fingerprint,
        )
        self.run_cli(
            "--db", db_path,
            "assignment", "mark-done-apply", receipt_id,
            "--workspace-id", "demo",
            "--task-id", task_id,
            "--actor", "operator",
            "--after-fingerprint", fps.after_fingerprint,
        )
        return receipt_id, fps

    def test_assignment_mark_done_prepare_cli_happy_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertEqual(result["workspace_id"], "demo")
            self.assertEqual(result["task_id"], "mvp-001")
            self.assertEqual(result["status"], "authorized")
            self.assertEqual(result["authorized_actor"], "operator")
            self.assertTrue(result["harness_fingerprint"])
            # No review.completed / ci.* events yet → recorded honestly.
            self.assertTrue(result["review_evidence"].get("not_applicable"))
            self.assertTrue(result["forge_evidence"].get("not_applicable"))

    def test_assignment_mark_done_prepare_cli_gate_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            (Path(tmp) / "harness-state.json").write_text(json.dumps({
                "project": "demo", "current_item": None,
            }))
            (Path(tmp) / "mvp-checklist.json").write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [
                    _mark_done_item("mvp-001", workflow_status="todo", branch="feat-x"),
                ]
            }))
            _write_harness_state_with_source(Path(tmp))
            self.run_cli(
                "--db", db_path, "workspace", "add", "demo",
                "--path", tmp, "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 1)
            self.assertIn("error", payload)

    def test_assignment_mark_done_prepare_cli_rejects_when_latest_ci_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            from coordinate.db import append_event, connect
            conn = connect(db_path)
            append_event(conn, event_type="ci.passed", actor="ci", workspace_id="demo",
                         task_id="mvp-001", target="mvp-001",
                         idempotency_key="ci:mvp-001:passed",
                         payload={"task_id": "mvp-001", "status": "passed"})
            append_event(conn, event_type="ci.failed", actor="ci", workspace_id="demo",
                         task_id="mvp-001", target="mvp-001",
                         idempotency_key="ci:mvp-001:failed",
                         payload={"task_id": "mvp-001", "status": "failed"})
            conn.commit()
            conn.close()
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "forge_gate_failed")




    def test_assignment_mark_done_preflight_cli_returns_authoritative_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            _, prep = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            receipt_id = prep["result"]["receipt_id"]
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-preflight", receipt_id,
                "--workspace-id", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["ok"])
            self.assertEqual(payload["result"]["workspace_id"], "demo")
            self.assertEqual(payload["result"]["task_id"], "mvp-001")

    def test_assignment_mark_done_preflight_cli_rejects_cross_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            _, prep = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-preflight", prep["result"]["receipt_id"],
                "--workspace-id", "demo",
                "--task-id", "mvp-999",
            )
            self.assertEqual(code, 1)
            self.assertFalse(payload["result"]["ok"])
            self.assertEqual(payload["result"]["reason"], "task_mismatch")

    def test_assignment_mark_done_claim_cli_records_claimed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            _, prep = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            receipt_id = prep["result"]["receipt_id"]
            from coordinate.completion import compute_mark_done_fingerprints
            fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-claim", receipt_id,
                "--workspace-id", "demo",
                "--task-id", "mvp-001",
                "--actor", "operator",
                "--before-fingerprint", fps.before_fingerprint,
                "--after-fingerprint", fps.after_fingerprint,
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["status"], "claimed")
            _, events = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo",
            )
            claimed = [e for e in events["events"]
                       if e["event_type"] == "completion.claimed"]
            self.assertEqual(len(claimed), 1)
            self.assertEqual(
                claimed[0]["payload"]["expected_after_fingerprint"],
                fps.after_fingerprint,
            )

    def test_assignment_mark_done_claim_cli_rejects_actor_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            _, prep = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            from coordinate.completion import compute_mark_done_fingerprints
            fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-claim", prep["result"]["receipt_id"],
                "--workspace-id", "demo",
                "--task-id", "mvp-001",
                "--actor", "intruder",  # != authorized_actor "operator"
                "--before-fingerprint", fps.before_fingerprint,
                "--after-fingerprint", fps.after_fingerprint,
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "actor_mismatch")

    def test_assignment_mark_done_apply_cli_creates_applied_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            receipt_id, fps = self._prepare_claim_apply_receipt(db_path, tmp)
            _, events = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo",
            )
            applied = [e for e in events["events"]
                       if e["event_type"] == "completion.applied"]
            self.assertEqual(len(applied), 1)
            self.assertEqual(applied[0]["payload"]["receipt_id"], receipt_id)
            self.assertEqual(
                applied[0]["payload"]["after_fingerprint"], fps.after_fingerprint
            )

    def test_assignment_mark_done_apply_cli_rejects_when_not_claimed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            _, prep = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            from coordinate.completion import compute_mark_done_fingerprints
            fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-apply", prep["result"]["receipt_id"],
                "--workspace-id", "demo", "--task-id", "mvp-001",
                "--actor", "operator",
                "--after-fingerprint", fps.after_fingerprint,
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "not_claimed")

    def test_mark_done_files_requires_receipt_or_repair(self):
        with tempfile.TemporaryDirectory() as tmp:
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item("mvp-001")],
            }))
            code, payload = self.run_cli(
                "assignment", "mark-done-files",
                "--workspace-path", tmp, "--harness-root", tmp,
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "missing_authorization")
            item = json.loads(checklist.read_text())["items"][0]
            self.assertEqual(item["status"], "doing")

    def test_mark_done_files_receipt_requires_event_cli_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, payload = self.run_cli(
                "assignment", "mark-done-files",
                "--workspace-path", tmp, "--harness-root", tmp,
                "--task-id", "mvp-001",
                "--receipt", "rec-1",
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "no_remote_verification_path")

    def test_mark_done_files_repair_without_reason_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item("mvp-001")],
            }))
            code, payload = self.run_cli(
                "assignment", "mark-done-files",
                "--workspace-path", tmp, "--harness-root", tmp,
                "--task-id", "mvp-001",
                "--repair-reason", "   ",
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "missing_authorization")
            item = json.loads(checklist.read_text())["items"][0]
            self.assertEqual(item["status"], "doing")

    def test_mark_done_files_repair_happy_path_stamps_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item("mvp-001", branch="feat-x")],
            }))
            code, payload = self.run_cli(
                "assignment", "mark-done-files",
                "--workspace-path", tmp, "--harness-root", tmp,
                "--task-id", "mvp-001",
                "--repair-reason", "drift fix",
            )
            self.assertEqual(code, 0)
            result = payload["result"]
            self.assertTrue(result["checklist_changed"])
            self.assertTrue(result["repair_only"])
            self.assertEqual(result["repair_reason"], "drift fix")
            self.assertIsNone(result["receipt_id"])

    def test_mark_done_files_receipt_happy_path_two_phase_remote(self):
        """End-to-end files path: preflight -> claim(reserve) -> write ->
        apply(ack), all forwarded to a mocked remote coord CLI."""
        import subprocess
        from coordinate import cli as cli_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            receipt_id, fps = self._prepare_claim_apply_receipt(db_path, tmp)
            # Reset checklist to pre-done so the canonical write is observable.
            (Path(tmp) / "mvp-checklist.json").write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item("mvp-001", branch="feat-x")],
            }))

            captured = []

            def fake_run(argv, **_kwargs):
                captured.append(list(argv))
                if argv[1:3] == ["assignment", "mark-done-preflight"]:
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0,
                        stdout=json.dumps({"result": {
                            "ok": True, "receipt_id": receipt_id,
                            "workspace_id": "demo", "task_id": "mvp-001",
                            "status": "claimed",
                        }}),
                        stderr="",
                    )
                if argv[1:3] == ["assignment", "mark-done-claim"]:
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0,
                        stdout=json.dumps({"result": {
                            "receipt_id": receipt_id, "status": "claimed",
                            "before_fingerprint": fps.before_fingerprint,
                            "expected_after_fingerprint": fps.after_fingerprint,
                            "idempotent": False,
                        }}),
                        stderr="",
                    )
                if argv[1:3] == ["assignment", "mark-done-apply"]:
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0,
                        stdout=json.dumps({"result": {
                            "receipt_id": receipt_id, "status": "applied",
                            "after_fingerprint": fps.after_fingerprint,
                            "idempotent": False,
                        }}),
                        stderr="",
                    )
                return subprocess.CompletedProcess(
                    args=argv, returncode=1, stdout="", stderr="unexpected",
                )

            original_run = cli_module.subprocess.run
            cli_module.subprocess.run = fake_run
            try:
                code, payload = self.run_cli(
                    "assignment", "mark-done-files",
                    "--workspace-path", tmp, "--harness-root", tmp,
                    "--workspace-id", "demo",
                    "--task-id", "mvp-001",
                    "--receipt", receipt_id,
                    "--event-cli-path", "/fake/coord-ssh",
                )
            finally:
                cli_module.subprocess.run = original_run

            self.assertEqual(code, 0, msg=str(payload))
            result = payload["result"]
            self.assertTrue(result["checklist_changed"])
            self.assertEqual(result["receipt_id"], receipt_id)
            self.assertEqual(result["after_fingerprint"], fps.after_fingerprint)
            # Ordering: preflight -> claim(reserve) -> [write] -> apply(ack).
            seq = [a[1:3] for a in captured]
            self.assertEqual(seq[0], ["assignment", "mark-done-preflight"])
            self.assertEqual(seq[1], ["assignment", "mark-done-claim"])
            self.assertEqual(seq[-1], ["assignment", "mark-done-apply"])
            # Canonical file mutated with structured receipt metadata.
            item = json.loads(
                (Path(tmp) / "mvp-checklist.json").read_text()
            )["items"][0]
            self.assertEqual(item["status"], "done")
            self.assertEqual(item["workflow"]["status"], "closed")
            self.assertEqual(item["completion_receipt"]["receipt_id"], receipt_id)

    def test_mark_done_files_receipt_rejects_when_preflight_fails(self):
        """If the remote preflight rejects the receipt, no file mutation."""
        import subprocess
        from coordinate import cli as cli_module

        with tempfile.TemporaryDirectory() as tmp:
            self._setup_receipt_workspace(tmp)
            checklist_text = (Path(tmp) / "mvp-checklist.json").read_text()

            def fake_run(argv, **_kwargs):
                return subprocess.CompletedProcess(
                    args=argv, returncode=1,
                    stdout=json.dumps({"result": {
                        "ok": False, "reason": "expired",
                        "message": "receipt expired",
                    }}),
                    stderr="",
                )

            original_run = cli_module.subprocess.run
            cli_module.subprocess.run = fake_run
            try:
                code, payload = self.run_cli(
                    "assignment", "mark-done-files",
                    "--workspace-path", tmp, "--harness-root", tmp,
                    "--workspace-id", "demo",
                    "--task-id", "mvp-001",
                    "--receipt", "rec-expired",
                    "--event-cli-path", "/fake/coord-ssh",
                )
            finally:
                cli_module.subprocess.run = original_run

            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "expired")
            self.assertEqual(
                (Path(tmp) / "mvp-checklist.json").read_text(), checklist_text
            )

    def test_mark_done_files_receipt_fails_closed_when_remote_claim_missing_fields(self):
        """The receipt evidence is built from the AUTHORITATIVE remote claim
        result. If that result omits before_fingerprint /
        expected_after_fingerprint / receipt_id, the CLI fails closed and
        performs no canonical file mutation."""
        import subprocess
        from coordinate import cli as cli_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            _, prep = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            receipt_id = prep["result"]["receipt_id"]
            checklist_text = (Path(tmp) / "mvp-checklist.json").read_text()

            def fake_run(argv, **_kwargs):
                if argv[1:3] == ["assignment", "mark-done-preflight"]:
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0,
                        stdout=json.dumps({"result": {
                            "ok": True, "receipt_id": receipt_id,
                            "workspace_id": "demo", "task_id": "mvp-001",
                            "status": "authorized",
                        }}),
                        stderr="",
                    )
                if argv[1:3] == ["assignment", "mark-done-claim"]:
                    # Omit before_fingerprint + expected_after_fingerprint.
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0,
                        stdout=json.dumps({"result": {
                            "receipt_id": receipt_id, "status": "claimed",
                            "idempotent": False,
                        }}),
                        stderr="",
                    )
                return subprocess.CompletedProcess(
                    args=argv, returncode=1, stdout="", stderr="unexpected",
                )

            original_run = cli_module.subprocess.run
            cli_module.subprocess.run = fake_run
            try:
                code, payload = self.run_cli(
                    "assignment", "mark-done-files",
                    "--workspace-path", tmp, "--harness-root", tmp,
                    "--workspace-id", "demo",
                    "--task-id", "mvp-001",
                    "--receipt", receipt_id,
                    "--event-cli-path", "/fake/coord-ssh",
                )
            finally:
                cli_module.subprocess.run = original_run

            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "invalid_claim_result")
            # Zero canonical file change.
            self.assertEqual(
                (Path(tmp) / "mvp-checklist.json").read_text(), checklist_text
            )

    def test_mark_done_record_receipt_rejects_when_not_applied(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            _, prep = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-prepare", "demo",
                "--task-id", "mvp-001",
            )
            receipt_id = prep["result"]["receipt_id"]
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-record", "demo",
                "--receipt", receipt_id,
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "not_applied")
            _, events = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo",
            )
            self.assertFalse(
                any(e["event_type"] == "task.done" for e in events["events"])
            )

    def test_mark_done_record_receipt_rejects_when_deployed_not_done(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            receipt_id, _ = self._prepare_claim_apply_receipt(db_path, tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-record", "demo",
                "--receipt", receipt_id,
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "deployed_not_done")

    def test_mark_done_record_receipt_happy_path_atomic_consume(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_receipt_workspace(tmp)
            receipt_id, fps = self._prepare_claim_apply_receipt(db_path, tmp)
            (Path(tmp) / "mvp-checklist.json").write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item("mvp-001", status="done", workflow_status="closed", branch="feat-x")],
            }))

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-record", "demo",
                "--receipt", receipt_id,
            )
            self.assertEqual(code, 0, msg=str(payload))
            result = payload["result"]
            self.assertTrue(result["event_created"])
            self.assertEqual(result["event"]["event_type"], "task.done")
            self.assertEqual(result["event"]["payload"]["receipt_id"], receipt_id)
            self.assertEqual(
                result["event"]["payload"]["applied_fingerprint"],
                fps.after_fingerprint,
            )

            code2, payload2 = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-record", "demo",
                "--receipt", receipt_id,
            )
            self.assertEqual(code2, 0)
            self.assertFalse(payload2["result"]["event_created"])
            _, events = self.run_cli(
                "--db", db_path, "event", "list", "--workspace-id", "demo",
            )
            dones = [e for e in events["events"] if e["event_type"] == "task.done"]
            self.assertEqual(len(dones), 1)

    def test_mark_done_record_repair_requires_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli("--db", db_path, "workspace", "add", "demo",
                         "--path", tmp, "--harness-root", tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done-record", "demo",
                "--task-id", "mvp-001",
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "missing_authorization")



    # --- workspace audit CLI tests ---

    def _setup_audit_workspace(self, tmp):
        """Set up a workspace with fake harnessctl, harness-state.json and mvp-checklist.json."""
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        harnessctl_path = Path(tmp) / "fake-harnessctl"
        harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
        harnessctl_path.chmod(0o755)
        (Path(tmp) / "mvp-checklist.json").write_text(
            json.dumps({"project": "demo", "harness_root": ".", "updated_at": "2026-07-13", "items": []}),
            encoding="utf-8",
        )
        _write_harness_state_with_source(Path(tmp))
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
            "--harnessctl-path", str(harnessctl_path),
        )
        return db_path

    def test_workspace_audit_clean_no_drifts(self):
        """Clean audit with matching mirrors and harness state: exit code 0, empty drifts."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_audit_workspace(tmp)
            # Reconcile to create matching task mirrors
            (Path(tmp) / "mvp-checklist.json").write_text(
                json.dumps({
                    "project": "demo",
                    "harness_root": ".",
                    "updated_at": "2026-07-13",
                    "items": [
                        _audit_item(),
                    ],
                }),
                encoding="utf-8",
            )
            _write_harness_state_with_source(Path(tmp))
            self.run_cli("--db", db_path, "reconcile", "demo", "--no-refresh")

            code, payload = self.run_cli("--db", db_path, "workspace", "audit", "demo")

            self.assertEqual(code, 0)
            self.assertTrue(payload["harness_available"])
            self.assertEqual(payload["drifts"], [])
            self.assertEqual(payload["mutation_failures"], [])
            self.assertEqual(payload["summary"]["drifts"], 0)

    def test_workspace_audit_with_drifts(self):
        """Audit with status mismatch: exit code 1, drifts present."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_audit_workspace(tmp)
            # Create a mirror, then make harness state diverge
            (Path(tmp) / "mvp-checklist.json").write_text(
                json.dumps({
                    "project": "demo",
                    "harness_root": ".",
                    "updated_at": "2026-07-13",
                    "items": [
                        _audit_item(),
                    ],
                }),
                encoding="utf-8",
            )
            _write_harness_state_with_source(Path(tmp))
            self.run_cli("--db", db_path, "reconcile", "demo", "--no-refresh")
            # Now change harness state to create a mismatch
            (Path(tmp) / "mvp-checklist.json").write_text(
                json.dumps({
                    "project": "demo",
                    "harness_root": ".",
                    "updated_at": "2026-07-13",
                    "items": [
                        _audit_item(status="done", workflow_status="closed"),
                    ],
                }),
                encoding="utf-8",
            )
            # Refresh harness-state.json to reflect new checklist
            _write_harness_state_with_source(Path(tmp))

            code, payload = self.run_cli("--db", db_path, "workspace", "audit", "demo")

            self.assertEqual(code, 1)
            self.assertGreaterEqual(len(payload["drifts"]), 1)
            drift_kinds = [d["kind"] for d in payload["drifts"]]
            self.assertIn("status_mismatch", drift_kinds)

    def test_workspace_audit_with_mutation_failures(self):
        """Audit with mutation_failed event: exit code 1, mutation_failures in report."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_audit_workspace(tmp)
            self.run_cli(
                "--db", db_path,
                "event", "append",
                "harness.mutation_failed",
                "--workspace-id", "demo",
                "--actor", "operator",
                "--task-id", "mvp-001",
                "--idempotency-key", "demo:assign:mvp-001:failed",
                "--payload-json", '{"operation": "assign", "exit_code": 1}',
            )

            code, payload = self.run_cli("--db", db_path, "workspace", "audit", "demo")

            self.assertEqual(code, 1)
            self.assertGreaterEqual(len(payload["mutation_failures"]), 1)
            failure = payload["mutation_failures"][0]
            self.assertEqual(failure["task_id"], "mvp-001")
            self.assertEqual(failure["payload"]["operation"], "assign")

    def test_workspace_audit_unknown_workspace(self):
        """Audit unknown workspace: exit code != 0 (error)."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            # No workspace registered

            code, stdout, stderr = self.run_cli_raw(
                "--db", db_path, "workspace", "audit", "nonexistent",
            )

            self.assertNotEqual(code, 0)
            self.assertIn("unknown workspace", stderr)

    def test_workspace_audit_harness_unavailable(self):
        """Audit with no harness-state.json: report has harness_available=False."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            harnessctl_path.write_text("#!/bin/bash\nexit 0\n")
            harnessctl_path.chmod(0o755)
            # Register workspace with harnessctl but no harness-state.json / mvp-checklist.json
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli("--db", db_path, "workspace", "audit", "demo")

            self.assertFalse(payload["harness_available"])
            self.assertIsNotNone(payload["harness_error"])

    # --- branch allocate tests ---

    def test_branch_allocate_success(self):
        """Successful branch allocation returns JSON with branch/event/existing."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--base-branch", "main",
                "--branch-namespace", "agents",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "branch", "allocate", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["branch"], "agents/codex/mvp-001")
            self.assertEqual(payload["owner"], "codex")
            self.assertFalse(payload["existing"])
            self.assertTrue(payload["event_created"])

    def test_branch_allocate_idempotent(self):
        """Second allocation returns existing=True."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--base-branch", "main",
                "--branch-namespace", "agents",
            )

            self.run_cli(
                "--db", db_path,
                "branch", "allocate", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "branch", "allocate", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["existing"])
            self.assertFalse(payload["event_created"])

    def test_branch_allocate_conflict(self):
        """Conflict: another task holds the same branch name."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--base-branch", "main",
                "--branch-namespace", "agents",
            )

            # Allocate for mvp-001 with owner codex
            self.run_cli(
                "--db", db_path,
                "branch", "allocate", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
            )

            # Manually insert another task mirror claiming the target branch for mvp-002
            code_init, _ = self.run_cli("--db", db_path, "event", "append", "test.init", "--workspace-id", "demo", "--idempotency-key", "demo:init")
            # We need to insert a mirror directly; use reconcile or direct DB
            import sqlite3
            from coordinate.db import initialize
            conn = initialize(Path(db_path))
            conn.execute(
                "INSERT INTO tasks (workspace_id, task_id, phase, owner, branch, pr, payload_json, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                ("demo", "mvp-002", None, "codex", "agents/codex/mvp-003", None, "{}"),
            )
            conn.commit()
            conn.close()

            # Try to allocate mvp-003 with owner codex -> conflict
            code, payload = self.run_cli(
                "--db", db_path,
                "branch", "allocate", "demo",
                "--task-id", "mvp-003",
                "--owner", "codex",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("already allocated to task", payload["error"]["message"])

    def test_branch_allocate_unknown_workspace(self):
        """Unknown workspace returns JSON error, exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            code, payload = self.run_cli(
                "--db", db_path,
                "branch", "allocate", "nonexistent",
                "--task-id", "mvp-001",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("unknown workspace", payload["error"]["message"])

    def test_branch_allocate_existing_different_branch(self):
        """Task has a different branch already -> JSON error, exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--base-branch", "main",
                "--branch-namespace", "agents",
            )

            # Allocate with owner=codex
            self.run_cli(
                "--db", db_path,
                "branch", "allocate", "demo",
                "--task-id", "mvp-001",
                "--owner", "codex",
            )

            # Try again with different owner -> would generate different branch
            code, payload = self.run_cli(
                "--db", db_path,
                "branch", "allocate", "demo",
                "--task-id", "mvp-001",
                "--owner", "claude",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("already has branch", payload["error"]["message"])

    # --- pr link tests ---

    def test_pr_link_explicit_success(self):
        """pr link --pr-url succeeds, returns JSON."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["pr_url"], "https://github.com/example/repo/pull/1")
            self.assertIsNone(payload["branch"])
            self.assertFalse(payload["existing"])
            self.assertTrue(payload["event_created"])

    def test_pr_link_explicit_with_branch(self):
        """pr link --pr-url --branch updates both pr and branch in mirror."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
                "--branch", "agents/codex/mvp-001",
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["pr_url"], "https://github.com/example/repo/pull/1")
            self.assertEqual(payload["branch"], "agents/codex/mvp-001")

    def test_pr_link_idempotent(self):
        """Second link with same pr_url returns existing=True."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["existing"])

    def test_pr_link_conflict_different_pr(self):
        """Task already has a different PR -> error."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/2",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("cannot relink", payload["error"]["message"])

    def test_pr_link_unknown_workspace(self):
        """Unknown workspace returns JSON error, exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "link", "nonexistent",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("unknown workspace", payload["error"]["message"])

    def test_pr_link_no_branch_no_pr_url(self):
        """No branch, no pr_url, no mirror branch -> error."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("no branch", payload["error"]["message"])

    def test_pr_link_same_pr_on_other_task(self):
        """Same PR URL already on another task -> error."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            # Link PR to mvp-001
            self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            # Try to link same PR to mvp-002
            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-002",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("already linked to task", payload["error"]["message"])


    # --- ci check ---

    def test_ci_check_no_pr_returns_error(self):
        """ci check with no PR URL and no mirror PR -> exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "ci", "check", "demo",
                "--task-id", "mvp-001",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)

    def test_ci_check_unknown_workspace_returns_error(self):
        """ci check with unknown workspace -> exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            code, payload = self.run_cli(
                "--db", db_path,
                "ci", "check", "nonexistent",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("unknown workspace", payload["error"]["message"])

    def test_ci_check_mirror_pr_conflict_returns_error(self):
        """ci check with pr_url that conflicts with mirror PR -> exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            # Link PR to mvp-001 via pr link
            self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            # Try ci check with a different pr_url
            code, payload = self.run_cli(
                "--db", db_path,
                "ci", "check", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/2",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("already has pr", payload["error"]["message"])

    # --- review check ---

    def test_review_check_no_pr_returns_error(self):
        """review check with no PR URL and no mirror PR -> exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "review", "check", "demo",
                "--task-id", "mvp-001",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)

    def test_review_check_unknown_workspace_returns_error(self):
        """review check with unknown workspace -> exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            code, payload = self.run_cli(
                "--db", db_path,
                "review", "check", "nonexistent",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("unknown workspace", payload["error"]["message"])

    def test_review_check_mirror_pr_conflict_returns_error(self):
        """review check with pr_url that conflicts with mirror PR -> exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            self.run_cli(
                "--db", db_path,
                "pr", "link", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/1",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "review", "check", "demo",
                "--task-id", "mvp-001",
                "--pr-url", "https://github.com/example/repo/pull/2",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("already has pr", payload["error"]["message"])

    # --- merge gate ---

    def test_merge_gate_unknown_workspace_returns_error(self):
        """merge gate with unknown workspace -> exit 1."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            code, payload = self.run_cli(
                "--db", db_path,
                "merge", "gate", "nonexistent",
                "--task-id", "mvp-001",
            )

            self.assertEqual(code, 1)
            self.assertIn("error", payload)
            self.assertIn("unknown workspace", payload["error"]["message"])

    # --- plan gate CLI tests ---

    def _setup_plan_workspace(self, tmp):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
        )
        return db_path

    def _write_harness_task(self, tmp, task_id):
        root = Path(tmp)
        (root / "mvp-checklist.json").write_text(
            json.dumps(
                {
                    "project": "demo",
                    "harness_root": ".",
                    "updated_at": "2026-07-13",
                    "items": [
                        {
                            "id": task_id,
                            "title": f"Task {task_id}",
                            "status": "todo",
                            "priority": "p1",
                            "owner": None,
                            "selected_in_session": None,
                            "verification": "",
                            "updated_at": "2026-07-13T12:00:00Z",
                            "dependencies": [],
                            "blocked_by": [],
                            "blocked_reason": "",
                            "acceptance": "Acceptance",
                            "handoff": {"from": None, "to": None, "reason": None},
                            "workflow": {"status": "todo", "branch": None,
                                         "updated_at": "2026-07-13T12:00:00Z"},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        _write_harness_state_with_source(root, current_item={"id": task_id, "status": "todo"})

    def _create_task(self, db_path, tmp):
        plan = Path(tmp) / "plan.md"
        plan.write_text("# Plan\n", encoding="utf-8")
        checklist = Path(tmp) / "mvp-checklist.json"
        if not checklist.exists():
            checklist.write_text(
                json.dumps(
                    {
                        "project": "demo",
                        "harness_root": ".",
                        "version": 1,
                        "updated_at": "2026-07-13",
                        "items": [],
                    }
                ),
                encoding="utf-8",
            )
        self.run_cli(
            "--db", db_path,
            "task", "create", "demo",
            "--task-id", "phase-001",
            "--plan-doc", "plan.md",
            "--title", "Phase 001",
        )
        self._write_harness_task(tmp, "phase-001")

    def test_plan_review_request_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)

            code, payload = self.run_cli(
                "--db", db_path,
                "plan", "review-request", "demo",
                "--task-id", "phase-001",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            self.assertEqual(payload["result"]["event"]["event_type"], "plan.review_requested")

    def test_plan_approve_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)

            code, payload = self.run_cli(
                "--db", db_path,
                "plan", "approve", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
                "--reviewer", "alice",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            self.assertEqual(payload["result"]["task"]["phase"], "ready")

    def test_plan_reject_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)

            code, payload = self.run_cli(
                "--db", db_path,
                "plan", "reject", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
                "--reason", "scope too broad",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            self.assertEqual(payload["result"]["task"]["phase"], "ready")

    def test_plan_reject_then_approve_cycle_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)

            reject_code, reject_payload = self.run_cli(
                "--db", db_path,
                "plan", "reject", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
                "--reason", "first attempt",
            )
            approve_code, approve_payload = self.run_cli(
                "--db", db_path,
                "plan", "approve", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
                "--reviewer", "alice",
            )

            self.assertEqual(reject_code, 0)
            self.assertEqual(approve_code, 0)
            self.assertTrue(reject_payload["result"]["event_created"])
            self.assertTrue(approve_payload["result"]["event_created"])
            self.assertNotEqual(
                reject_payload["result"]["event"]["id"],
                approve_payload["result"]["event"]["id"],
            )

    def test_task_handoff_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)
            self.run_cli(
                "--db", db_path,
                "plan", "approve", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "handoff", "demo",
                "--task-id", "phase-001",
                "--role", "worker",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            handoff_text = payload["result"]["handoff_text"]
            self.assertIn("phase-001", handoff_text)

    def test_task_handoff_without_approval_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)
            # No plan approve step

            code, stdout, stderr = self.run_cli_raw(
                "--db", db_path,
                "task", "handoff", "demo",
                "--task-id", "phase-001",
                "--role", "worker",
            )

            self.assertEqual(code, 1)
            self.assertIn("no plan gate event", stderr)

    def test_task_handoff_writes_bootstrap_file_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)
            self.run_cli(
                "--db", db_path,
                "plan", "approve", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "handoff", "demo",
                "--task-id", "phase-001",
                "--role", "worker",
            )

            self.assertEqual(code, 0)
            self.assertIn("bootstrap_file", payload["result"])
            bootstrap_path = payload["result"]["bootstrap_file"]
            self.assertTrue(Path(bootstrap_path).exists())
            content = Path(bootstrap_path).read_text()
            self.assertIn("Worker Bootstrap", content)
            self.assertIn("phase-001", content)
            self.assertIn("Coordinator CLI", content)

    def test_task_handoff_no_write_bootstrap_skips_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup_plan_workspace(tmp)
            self._create_task(db_path, tmp)
            self.run_cli(
                "--db", db_path,
                "plan", "approve", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "handoff", "demo",
                "--task-id", "phase-001",
                "--role", "worker",
                "--no-write-bootstrap",
            )

            self.assertEqual(code, 0)
            self.assertNotIn("bootstrap_file", payload["result"])

    def test_delivery_list_includes_delivery_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            self.run_cli(
                "--db", db_path,
                "delivery", "create",
                "--platform", "stdout",
                "--destination", "local",
                "--message-key", "demo:type-test:1",
                "--payload-json", '{"text":"test"}',
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "delivery", "list",
            )

            self.assertEqual(code, 0)
            self.assertGreater(len(payload["deliveries"]), 0)
            self.assertIn("delivery_type", payload["deliveries"][0])
            self.assertEqual(payload["deliveries"][0]["delivery_type"], "dry_run")

    def test_delivery_list_filter_by_type_dry_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")

            self.run_cli(
                "--db", db_path,
                "delivery", "create",
                "--platform", "stdout",
                "--destination", "local",
                "--message-key", "demo:filter-dry:1",
                "--payload-json", '{"text":"dry"}',
            )
            self.run_cli(
                "--db", db_path,
                "delivery", "create",
                "--platform", "discord",
                "--destination", "123",
                "--message-key", "demo:filter-live:1",
                "--payload-json", '{"text":"live"}',
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "delivery", "list",
                "--type", "dry_run",
            )

            self.assertEqual(code, 0)
            self.assertEqual(len(payload["deliveries"]), 1)
            self.assertEqual(payload["deliveries"][0]["delivery_type"], "dry_run")

    # --- Phase 4.5: workspace agent add and task handoff --target-agent ---

    def test_workspace_agent_add(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "workspace", "agent", "add", "demo",
                "--name", "mac-claude",
                "--discord-user-id", "123",
                "--reason", "test",
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["workspace_id"], "demo")
            self.assertEqual(payload["agent_name"], "mac-claude")
            self.assertEqual(payload["discord_user_id"], "123")
            self.assertEqual(payload["status"], "registered")

            # Verify via DB
            from coordinate.db import initialize as _init, get_agent_discord_id
            conn = _init(db_path)
            try:
                self.assertEqual(get_agent_discord_id(conn, "demo", "mac-claude"), "123")
            finally:
                conn.close()

    def test_task_handoff_target_agent_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            (Path(tmp) / "mvp-checklist.json").write_text(
                json.dumps({
                    "project": "demo",
                    "harness_root": ".",
                    "version": 1,
                    "updated_at": "2026-07-13",
                    "items": [],
                }),
                encoding="utf-8",
            )
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            self.run_cli(
                "--db", db_path,
                "task", "create", "demo",
                "--task-id", "T1",
                "--plan-doc", "plan.md",
                "--title", "Phase",
            )
            self._write_harness_task(tmp, "T1")
            self.run_cli(
                "--db", db_path,
                "plan", "approve", "demo",
                "--task-id", "T1",
                "--scope", "implementation plan",
            )
            self.run_cli(
                "--db", db_path,
                "workspace", "agent", "add", "demo",
                "--name", "mac-codex",
                "--discord-user-id", "123456789",
                "--reason", "test",
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "handoff", "demo",
                "--task-id", "T1",
                "--role", "worker",
                "--target-agent", "mac-codex",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            self.assertEqual(
                payload["result"]["event"]["payload"]["target_agent"],
                "mac-codex",
            )

    def test_workspace_agent_sync_authoritative(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            toml_path = Path(tmp) / "agents.toml"
            toml_path.write_text("""
[registry]
id = "multinexus.discord"
version = 1

[[agents]]
id = "mac-claude"
display_name = "Mac Claude"
discord_user_id = 11111

[[external_agents]]
id = "mac-openclaw"
display_name = "小龙虾"
discord_user_id = 22222
""")

            code, payload = self.run_cli(
                "--db", db_path,
                "workspace", "agent", "sync", "demo",
                "--source", str(toml_path),
                "--replace",
            )

            self.assertEqual(code, 0)
            self.assertEqual(len(payload["added"]), 2)
            self.assertEqual(payload["workspace_id"], "demo")
            self.assertEqual(payload["skipped"], [])
            self.assertEqual(payload["source_id"], "multinexus.discord")
            self.assertEqual(payload["source_version"], 1)
            self.assertIsNotNone(payload["source_hash"])

    def test_workspace_agent_sync_duplicate_id_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            toml_path = Path(tmp) / "agents.toml"
            toml_path.write_text("""
[registry]
id = "multinexus.discord"
version = 1

[[agents]]
id = "mac-claude"
discord_user_id = 11111

[[external_agents]]
id = "mac-claude"
discord_user_id = 22222
""")

            code, stdout, stderr = self.run_cli_raw(
                "--db", db_path,
                "workspace", "agent", "sync", "demo",
                "--source", str(toml_path),
                "--replace",
            )

            self.assertEqual(code, 1)
            self.assertIn("duplicate agent id", stderr)

    def test_workspace_agent_sync_replace(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            # Add a manual override first
            self.run_cli(
                "--db", db_path,
                "workspace", "agent", "add", "demo",
                "--name", "manual-agent",
                "--discord-user-id", "999",
                "--reason", "manual",
            )

            toml_path = Path(tmp) / "agents.toml"
            toml_path.write_text("""
[registry]
id = "multinexus.discord"
version = 1

[[agents]]
id = "mac-claude"
display_name = "Mac Claude"
discord_user_id = 11111
""")

            code, payload = self.run_cli(
                "--db", db_path,
                "workspace", "agent", "sync", "demo",
                "--source", str(toml_path),
                "--replace",
            )

            self.assertEqual(code, 0)
            self.assertEqual(len(payload["added"]), 1)
            self.assertEqual(len(payload["removed"]), 0)
            self.assertEqual(payload["workspace_id"], "demo")

    def test_workspace_agent_sync_no_token_in_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            toml_path = Path(tmp) / "agents.toml"
            toml_path.write_text("""
[registry]
id = "multinexus.discord"
version = 1

[[agents]]
id = "mac-claude"
display_name = "Mac Claude"
token_env = "SECRET_TOKEN_HERE"
discord_user_id = 11111
""")

            code, payload = self.run_cli(
                "--db", db_path,
                "workspace", "agent", "sync", "demo",
                "--source", str(toml_path),
                "--replace",
            )

            self.assertEqual(code, 0)
            output_str = json.dumps(payload)
            self.assertNotIn("SECRET_TOKEN_HERE", output_str)
            self.assertNotIn("token_env", output_str)

    def test_workspace_host_profile_set_and_list_preserves_foreign_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "workspace", "host-profile", "set", "demo",
                "--host-id", "win-admin",
                "--workspace-path", r"C:\Users\ADMIN\projects\multinexus",
                "--harness-root", r"C:\Users\ADMIN\projects\multinexus\docs\project-harness",
                "--coordinator-cli-path", r"C:\Users\ADMIN\projects\multinexus\scripts\coord-ssh-win.py",
                "--shell", "powershell",
                "--metadata-json", json.dumps({"os": "windows"}),
            )
            list_code, list_payload = self.run_cli(
                "--db", db_path,
                "workspace", "host-profile", "list", "demo",
            )

            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["host_id"], "win-admin")
            self.assertEqual(
                payload["result"]["workspace_path"],
                r"C:\Users\ADMIN\projects\multinexus",
            )
            self.assertEqual(list_code, 0)
            self.assertEqual(list_payload["profiles"][0]["metadata"], {"os": "windows"})


class LoadDotenvTests(unittest.TestCase):
    """Verify `coordinate` main() loads .env from the current working directory
    (or any ancestor) so that `coordinate serve` and other subcommands pick
    up COORDINATOR_BOT_TOKEN etc. when launched from a launchd-managed
    process whose WorkingDirectory already exists."""

    # Keys that other test classes may have set. We clear them before and
    # after each test so dotenv load semantics are observable in isolation.
    _TRACKED_KEYS = (
        "COORDINATOR_BOT_TOKEN",
        "COORDINATOR_CHANNEL_ID",
        "COORDINATOR_ALLOWED_USER_IDS",
    )

    def setUp(self):
        for key in self._TRACKED_KEYS:
            os.environ.pop(key, None)

    def tearDown(self):
        for key in self._TRACKED_KEYS:
            os.environ.pop(key, None)

    def test_main_loads_dotenv_from_cwd(self):
        with tempfile.TemporaryDirectory() as tmp:
            # macOS temp dirs go through /var -> /private/var symlink;
            # resolve to realpath so Path.cwd() (used by python-dotenv)
            # agrees with our written .env path.
            tmp = os.path.realpath(tmp)
            (Path(tmp) / ".env").write_text(
                "COORDINATOR_BOT_TOKEN=dotenv-bot-token\n"
                "COORDINATOR_CHANNEL_ID=1234567890\n"
                "COORDINATOR_ALLOWED_USER_IDS=42\n",
                encoding="utf-8",
            )
            cwd = os.getcwd()
            try:
                os.chdir(tmp)
                # Run any subcommand; --help triggers SystemExit after
                # argparse, but load_dotenv() runs before parse_args so
                # the dotenv effect is observable either way.
                try:
                    main(["--help"])
                except SystemExit:
                    pass
            finally:
                os.chdir(cwd)
            # dotenv values should now be in os.environ.
            self.assertEqual(
                os.environ.get("COORDINATOR_BOT_TOKEN"), "dotenv-bot-token"
            )
            self.assertEqual(os.environ.get("COORDINATOR_CHANNEL_ID"), "1234567890")
            self.assertEqual(os.environ.get("COORDINATOR_ALLOWED_USER_IDS"), "42")

    def test_main_does_not_override_existing_env(self):
        # Existing process env wins over .env (override=False is the
        # python-dotenv default; this test pins that contract).
        os.environ["COORDINATOR_BOT_TOKEN"] = "process-env-token"
        with tempfile.TemporaryDirectory() as tmp:
            tmp = os.path.realpath(tmp)
            (Path(tmp) / ".env").write_text(
                "COORDINATOR_BOT_TOKEN=dotenv-token\n",
                encoding="utf-8",
            )
            cwd = os.getcwd()
            try:
                os.chdir(tmp)
                try:
                    main(["--help"])
                except SystemExit:
                    pass
            finally:
                os.chdir(cwd)
            self.assertEqual(
                os.environ.get("COORDINATOR_BOT_TOKEN"), "process-env-token"
            )


class CombinedCreateFaultMatrixTests(unittest.TestCase):
    """U2 §11.1 fault matrix for the combined task create: file-first/
    record-second ordering, idempotency, recovery, and the initial phase table."""

    def run_cli(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(args))
        return code, json.loads(stdout.getvalue()) if stdout.getvalue().strip() else {}

    def _setup(self, tmp):
        db_path = str(Path(tmp) / "coordinator.sqlite3")
        plan = Path(tmp) / "plan.md"
        plan.write_text("# Plan\n", encoding="utf-8")
        (Path(tmp) / "mvp-checklist.json").write_text(
            json.dumps({"project": "demo", "harness_root": ".", "version": 1,
                        "updated_at": "2026-07-13", "items": []}),
            encoding="utf-8",
        )
        self.run_cli("--db", db_path, "workspace", "add", "demo",
                     "--path", tmp, "--harness-root", tmp)
        return db_path

    def _create(self, db_path, task_id="t1", **extra):
        argv = ["--db", db_path, "task", "create", "demo",
                "--task-id", task_id, "--plan-doc", "plan.md", "--title", "T"]
        for key, value in extra.items():
            flag = "--operation-id" if key == "operation_id" else f"--{key}"
            argv += [flag, str(value)]
        return self.run_cli(*argv)

    def _plan_ready_count(self, db_path):
        _, payload = self.run_cli("--db", db_path, "event", "list", "--workspace-id", "demo")
        return sum(1 for e in payload.get("events", []) if e["event_type"] == "plan.ready")

    def test_file_half_phase_rejection_leaves_db_and_file_untouched(self):
        """Reserved lifecycle/terminal phases fail closed at the file half:
        zero DB writes and zero checklist mutation."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            checklist_path = Path(tmp) / "mvp-checklist.json"
            before = checklist_path.read_bytes()
            for phase in ("awaiting_operator", "running", "blocked", "done", "released"):
                with self.subTest(phase=phase):
                    code, payload = self._create(db_path, task_id=f"t-{phase}", phase=phase)
                    self.assertEqual(code, 1)
                    self.assertEqual(payload["error"]["reason"], "phase_not_creatable")
                    self.assertEqual(self._plan_ready_count(db_path), 0)
                    self.assertEqual(checklist_path.read_bytes(), before)

    def test_arbitrary_planning_label_creates_todo(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            code, payload = self._create(db_path, task_id="t1", phase="phase-8")
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["task"]["task_id"], "t1")
            checklist = json.loads(
                (Path(tmp) / "mvp-checklist.json").read_text(encoding="utf-8")
            )
            item = checklist["items"][0]
            self.assertEqual(item["status"], "todo")
            self.assertEqual(item["workflow"]["status"], "todo")
            self.assertEqual(item["phase"], "phase-8")

    def test_full_success_retry_reuses_operation_no_new_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            first_code, first = self._create(db_path)
            self.assertEqual(first_code, 0)
            first_op = first["result"]["operation"]["operation_id"]
            first_event = first["result"]["event"]["id"]

            second_code, second = self._create(db_path)
            self.assertEqual(second_code, 0)
            self.assertEqual(second["result"]["operation"]["operation_id"], first_op)
            self.assertEqual(second["result"]["event"]["id"], first_event)
            self.assertFalse(second["result"]["event_created"])
            self.assertEqual(self._plan_ready_count(db_path), 1)

    def test_input_change_fails_closed_before_any_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            first_code, _ = self._create(db_path, title="Original")
            self.assertEqual(first_code, 0)
            checklist_path = Path(tmp) / "mvp-checklist.json"
            before = checklist_path.read_bytes()

            code, payload = self._create(db_path, title="Changed Title")
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "operation_conflict")
            # Zero writes: file bytes unchanged and still one plan.ready.
            self.assertEqual(checklist_path.read_bytes(), before)
            self.assertEqual(self._plan_ready_count(db_path), 1)

    def _db_counts(self, db_path):
        """(task mirror, event, split-operation) rows for the demo workspace."""
        conn = sqlite3.connect(db_path)
        try:
            return (
                conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE workspace_id = 'demo'"
                ).fetchone()[0],
                conn.execute(
                    "SELECT COUNT(*) FROM events WHERE workspace_id = 'demo'"
                ).fetchone()[0],
                conn.execute(
                    "SELECT COUNT(*) FROM split_operations WHERE workspace_id = 'demo'"
                ).fetchone()[0],
            )
        finally:
            conn.close()

    def test_lost_item_with_new_explicit_operation_id_conflicts(self):
        """P1-1: after a successful create, deleting the checklist item and
        retrying with a NEW explicit --operation-id must fail closed: the DB
        ledger still binds the target, so re-authoring under a second
        authority is refused with operation_conflict and zero mutation
        (checklist bytes, task mirror, events, split-operation ledger)."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            first_code, first = self._create(db_path)
            self.assertEqual(first_code, 0)
            self.assertEqual(first["result"]["event_created"], True)
            checklist_path = Path(tmp) / "mvp-checklist.json"
            # The DB ledger row survives the file-side deletion.
            checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
            checklist["items"] = []
            checklist_path.write_text(json.dumps(checklist), encoding="utf-8")
            before_bytes = checklist_path.read_bytes()
            before_counts = self._db_counts(db_path)

            code, payload = self._create(db_path, operation_id=str(uuid.uuid4()))
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "operation_conflict")
            self.assertEqual(checklist_path.read_bytes(), before_bytes)
            self.assertEqual(self._db_counts(db_path), before_counts)

    def test_explicit_operation_id_mismatch_conflicts_with_zero_mutation(self):
        """P1-1: an item already deployed under an envelope with identical
        inputs, retried with a DIFFERENT explicit --operation-id, must fail
        closed with operation_conflict and zero mutation; the deployed
        envelope stays bound to the original operation."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            first_code, first = self._create(db_path)
            self.assertEqual(first_code, 0)
            deployed_op = first["result"]["operation"]["operation_id"]
            checklist_path = Path(tmp) / "mvp-checklist.json"
            before_bytes = checklist_path.read_bytes()
            before_counts = self._db_counts(db_path)

            other = str(uuid.uuid4())
            self.assertNotEqual(other, deployed_op)
            code, payload = self._create(db_path, operation_id=other)
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "operation_conflict")
            self.assertEqual(checklist_path.read_bytes(), before_bytes)
            self.assertEqual(self._db_counts(db_path), before_counts)
            checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
            self.assertEqual(
                checklist["items"][0]["split_operation"]["operation_id"],
                deployed_op,
            )

    def test_phase_whitespace_rejected_with_zero_mutation(self):
        """P2-1: surrounding whitespace must not smuggle lifecycle/terminal
        phases past the combined create ("done ", " running") — same
        phase_not_creatable contract as the plain phase table, with zero
        checklist and DB mutation."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            checklist_path = Path(tmp) / "mvp-checklist.json"
            before = checklist_path.read_bytes()
            before_counts = self._db_counts(db_path)
            for phase in ("done ", " running"):
                with self.subTest(phase=phase):
                    code, payload = self._create(
                        db_path, task_id=f"t-{phase.strip()}", phase=phase
                    )
                    self.assertEqual(code, 1)
                    self.assertEqual(payload["error"]["reason"], "phase_not_creatable")
                    self.assertEqual(checklist_path.read_bytes(), before)
                    self.assertEqual(self._db_counts(db_path), before_counts)

    def test_record_half_failure_emits_structured_recovery(self):
        """A record-half failure keeps the file authority and returns the
        same-operation recovery argv (recovery_required JSON + copyable command)."""
        import coordinate.onboarding as onboarding_module

        def boom(*_args, **_kwargs):
            raise RuntimeError("injected record failure")

        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._setup(tmp)
            checklist_path = Path(tmp) / "mvp-checklist.json"
            with patch.object(onboarding_module, "apply_task_create_record", side_effect=boom):
                code, payload = self.run_cli(
                    "--db", db_path,
                    "task", "create", "demo",
                    "--task-id", "t1",
                    "--plan-doc", "plan.md",
                    "--title", "T",
                )
            self.assertEqual(code, 1)
            error = payload["error"]
            self.assertTrue(error["recovery_required"])
            self.assertIn("operation_id", error)
            self.assertIn("input_fingerprint", error)
            self.assertIn("before_fingerprint", error)
            self.assertIn("after_fingerprint", error)
            self.assertIn("recovery_argv", error)
            self.assertEqual(error["recovery_argv"][:3], ["coordinate", "task", "create-record"])
            self.assertIn("recovery_command", error)
            # File authority is preserved: the checklist item + envelope remain.
            checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
            self.assertEqual(len(checklist["items"]), 1)
            self.assertEqual(
                checklist["items"][0]["split_operation"]["operation_id"],
                error["operation_id"],
            )
            # The recovery argv completes the same operation idempotently.
            with patch.object(onboarding_module, "apply_task_create_record") as mock_record:
                mock_record.return_value = None
                argv = error["recovery_argv"]
                self.assertIn("--operation-id", argv)
                self.assertEqual(
                    argv[argv.index("--operation-id") + 1],
                    error["operation_id"],
                )

    def test_assignment_mark_done_empty_verification_rejected(self):
        """Monolithic mark-done E2E: a new item with verification='' is rejected
        before harnessctl runs (no task.done event)."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            (Path(tmp) / "mvp-checklist.json").write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item(verification="")],
            }), encoding="utf-8")
            _write_harness_state_with_source(Path(tmp))
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp, "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )

            self.assertEqual(code, 1)
            self.assertFalse(payload["result"]["gate"]["passed"])
            self.assertIn("verification", payload["result"]["gate"]["reason"])
            self.assertIsNone(payload["result"]["mutation"])

            _, events_payload = self.run_cli("--db", db_path, "event", "list", "--workspace-id", "demo")
            task_done = [e for e in events_payload["events"] if e["event_type"] == "task.done"]
            self.assertEqual(task_done, [])

    def test_assignment_mark_done_existing_item_verification_success(self):
        """Monolithic mark-done E2E: an item with an existing non-empty
        verification succeeds and passes --verification to harnessctl."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            (Path(tmp) / "mvp-checklist.json").write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item(verification="already in item")],
            }), encoding="utf-8")
            _write_harness_state_with_source(Path(tmp))
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp, "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            self.assertEqual(
                payload["result"]["event"]["payload"].get("verification"),
                "already in item",
            )

    def test_assignment_mark_done_explicit_verification_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            (Path(tmp) / "mvp-checklist.json").write_text(json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [_mark_done_item(verification="")],
            }), encoding="utf-8")
            _write_harness_state_with_source(Path(tmp))
            self.run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp, "--harness-root", tmp,
                "--harnessctl-path", str(harnessctl_path),
            )

            code, payload = self.run_cli(
                "--db", db_path,
                "assignment", "mark-done", "demo",
                "--task-id", "mvp-001",
                "--verification", "explicit evidence",
            )

            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            self.assertEqual(
                payload["result"]["event"]["payload"].get("verification"),
                "explicit evidence",
            )
