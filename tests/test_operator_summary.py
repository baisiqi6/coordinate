"""Operator recovery must surface ledger feedback without refreshing authority."""

import contextlib
import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinate.cli import main
from coordinate.db import append_event, initialize, upsert_task_mirror, upsert_workspace


def event(conn, kind, payload, *, task="t1", workspace="demo"):
    return append_event(conn, event_type=kind, workspace_id=workspace,
                        task_id=task, actor="reviewer-c", payload=payload).row["id"]


def run_cli(path, *argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(["--db", str(path), *argv])
        except SystemExit as exc:
            code = exc.code
    return code, json.loads(out.getvalue()) if out.getvalue() else None, err.getvalue()


def summary(path):
    code, output, err = run_cli(path, "operator", "task", "demo", "t1")
    assert code == 0, err
    return output["task_summary"]


class OperatorSummaryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        tmp_path = Path(directory.name)
        self.tmp_path = tmp_path
        path = tmp_path / "coordinate.sqlite3"
        conn = initialize(path)
        for workspace in ("demo", "other"):
            upsert_workspace(conn, workspace_id=workspace, name=workspace,
                             path=str(tmp_path / "unavailable"),
                             harness_root=str(tmp_path / "unavailable"))
        payload = {
            "blocked_reason": "Waiting for interface review",
            "workflow": {"status": "blocked", "unblock_owner": "maintainer-b"},
            "lease": {"owner": "maintainer-a", "session": "old-session",
                      "expires_at": "2099-01-01T00:00:00Z",
                      "released_at": "2026-10-15T00:00:00Z"},
            "artifacts": {"plan": "docs/tasks/t1/plan.md",
                          "blocker_packet": "docs/current/blocker-packet.md"},
        }
        upsert_task_mirror(conn, workspace_id="demo", task_id="t1", phase="blocked",
                           owner=None, branch=None, pr=None, payload=payload)
        self.addCleanup(conn.close)
        self.task_db = path, conn, payload


    def test_unknown_latest_decision_does_not_resurrect_older_approval(self):
        for decision in ("not_a_known_decision", {"nested": "approved"}, None):
            with self.subTest(decision=decision):
                self._assert_unknown_latest_decision(decision)


    def test_lease_state_and_current_executor_are_separate(self):
        for lease, expected in (
            ({"owner": "a", "expires_at": "2099-01-01T00:00:00Z"}, "active"),
            ({"owner": "a", "expires_at": "2000-01-01T00:00:00Z"}, "expired"),
            ({"owner": "a", "expires_at": "malformed"}, "unknown"),
            (None, "absent"),
        ):
            with self.subTest(expected=expected):
                self._assert_lease_state(lease, expected)


    def test_blocked_task_recovers_later_domain_feedback_without_job_or_pr(self):
        task_db = self.task_db
        path, conn, _ = task_db
        review_id = event(conn, "domain.review", {
            "decision": "changes_requested", "summary": "Update the interface contract",
            "evidence_locator": "docs/reviews/interface.md",
        })
        result = summary(path)
        assert result["phase"] == "blocked"
        assert result["owner"] is None
        assert result["roles"]["current_executor"]["value"] is None
        assert result["roles"]["last_executor"] == {
            "value": "maintainer-a", "source": "task_mirror.lease.owner"}
        assert result["roles"]["unblock_owner"]["value"] == "maintainer-b"
        assert result["lease_state"] == "released"
        assert result["blocker"]["reason"] == "Waiting for interface review"
        assert result["latest_feedback"]["id"] == review_id
        assert result["latest_feedback"]["decision"] == "changes_requested"
        assert result["latest_feedback"]["evidence_locators"] == ["docs/reviews/interface.md"]
        assert result["snapshot"]["latest_task_event_id"] == review_id
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


    def test_unknown_responsibility_is_not_guessed_from_any_actor(self):
        task_db = self.task_db
        path, conn, _ = task_db
        event(conn, "assignment.accepted", {"owner": "executor-d"})
        result = summary(path)
        assert result["roles"]["responsibility"] == {
            "state": "unknown", "operator": None,
            "evidence_locator": "docs/tasks/t1/plan.md"}
        assert any("dispatch" in warning.lower() for warning in result["warnings"])


    def test_feedback_and_assignment_survive_recent_window_noise(self):
        task_db = self.task_db
        path, conn, payload = task_db
        payload.pop("lease")
        upsert_task_mirror(conn, workspace_id="demo", task_id="t1", phase="blocked",
                           owner=None, branch=None, pr=None, payload=payload)
        assignment_id = event(conn, "assignment.accepted", {"owner": "executor-d"})
        review_id = event(conn, "custom.domain.feedback", {"decision": "blocked"})
        for index in range(24):
            event(conn, "domain.note", {"summary": str(index)})
        result = summary(path)
        assert len(result["recent_events"]) == 20
        assert result["snapshot"]["events_truncated"] is True
        assert result["latest_feedback"]["id"] == review_id
        assert result["roles"]["last_executor"] == {
            "value": "executor-d", "source": assignment_id}
        assert all(row["id"] != review_id for row in result["recent_events"])


    def test_task_and_workspace_are_exactly_scoped_and_ledger_order_wins(self):
        task_db = self.task_db
        path, conn, _ = task_db
        event(conn, "domain.review", {"decision": "approved"})
        latest = event(conn, "domain.review", {"decision": "changes_requested"})
        conn.execute("UPDATE events SET created_at = '2000-01-01T00:00:00Z' WHERE id = ?",
                     (latest,))
        conn.commit()
        event(conn, "domain.review", {"decision": "approved", "summary": "FOREIGN"}, task="t2")
        event(conn, "domain.review", {"decision": "approved", "summary": "FOREIGN"}, workspace="other")
        result = summary(path)
        assert result["latest_feedback"]["id"] == latest
        assert result["recent_events"][0]["id"] == latest
        assert result["snapshot"]["latest_task_event_id"] == latest
        assert "FOREIGN" not in json.dumps(result)


    def _assert_unknown_latest_decision(self, decision):
        task_db = self.task_db
        path, conn, _ = task_db
        event(conn, "domain.review", {"decision": "approved"})
        latest = event(conn, "domain.review", {"decision": decision})
        result = summary(path)
        assert result["latest_feedback"]["id"] == latest
        assert result["latest_feedback"]["decision"] == "unknown"
        assert result["latest_feedback"]["decision_source"] == "payload.decision"
        assert result["latest_feedback"]["decision_known"] is False


    def test_projection_is_bounded_and_never_copies_arbitrary_payload(self):
        task_db = self.task_db
        path, conn, _ = task_db
        event(conn, "domain.review", {
            "decision": "changes_requested", "summary": "x" * 700,
            "evidence_locators": ["docs/" + str(i) for i in range(12)],
            "prompt": "SECRET_PROMPT", "response": "SECRET_RESPONSE",
            "env": {"token": "SECRET_TOKEN"}, "mutation": {"stdout": "SECRET_STDOUT"},
            "artifacts": [{"transcript": "SECRET_TRANSCRIPT"}],
        })
        result = summary(path)
        feedback = result["latest_feedback"]
        assert len(feedback["summary"]) == 500
        assert len(feedback["evidence_locators"]) == 8
        assert feedback["truncated"] is True
        assert "SECRET_" not in json.dumps(result)


    def _assert_lease_state(self, lease, expected):
        task_db = self.task_db
        path, conn, payload = task_db
        payload["lease"] = lease
        upsert_task_mirror(conn, workspace_id="demo", task_id="t1", phase="blocked",
                           owner=None, branch=None, pr=None, payload=payload)
        result = summary(path)
        assert result["lease_state"] == expected
        assert result["roles"]["current_executor"]["value"] == ("a" if expected == "active" else None)


    def test_readonly_query_changes_no_database_or_workspace_bytes(self):
        task_db = self.task_db
        path, conn, _ = task_db
        event(conn, "domain.review", {"decision": "blocked"})
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        before_files = sorted(str(p.relative_to(path.parent)) for p in path.parent.rglob("*"))
        with patch("coordinate.cli_support.initialize", side_effect=AssertionError("must not initialize")):
            result = summary(path)
        assert result["snapshot"]["harness_refreshed"] is False
        assert result["snapshot"]["ledger_included"] is True
        assert result["snapshot"]["mirror_may_be_stale"] is True
        assert hashlib.sha256(path.read_bytes()).hexdigest() == before
        assert sorted(str(p.relative_to(path.parent)) for p in path.parent.rglob("*")) == before_files


    def test_missing_database_is_not_created(self):
        tmp_path = self.tmp_path
        path = tmp_path / "missing-parent" / "missing.sqlite3"
        code, _, err = run_cli(path, "operator", "task", "demo", "t1")
        assert code == 1
        assert "read-only" in err
        assert not path.parent.exists()


    def test_old_database_schema_is_not_migrated(self):
        tmp_path = self.tmp_path
        path = tmp_path / "old.sqlite3"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE old_marker (value TEXT)")
        conn.commit()
        conn.close()
        before = path.read_bytes()
        code, _, _ = run_cli(path, "operator", "task", "demo", "t1")
        assert code == 1
        assert path.read_bytes() == before


    def test_unknown_task_fails_without_guessing_a_different_task(self):
        task_db = self.task_db
        path, _, _ = task_db
        code, _, err = run_cli(path, "operator", "task", "demo", "missing")
        assert code == 1
        assert "task" in err


    def test_no_refresh_marks_cache_authority_and_uses_readonly_connection(self):
        tmp_path = self.tmp_path
        task_db = self.task_db
        path, conn, _ = task_db
        root = tmp_path / "harness"
        root.mkdir()
        checklist = {"project": "demo", "harness_root": str(root),
                     "updated_at": "2026-10-15", "items": []}
        checklist_bytes = json.dumps(checklist).encode()
        (root / "mvp-checklist.json").write_bytes(checklist_bytes)
        (root / "harness-state.json").write_text(json.dumps({"project": "demo", "current_item": None,
            "source": {"checklist_path": "mvp-checklist.json",
                       "checklist_sha256": hashlib.sha256(checklist_bytes).hexdigest()}}))
        upsert_workspace(conn, workspace_id="demo", name="demo", path=str(root), harness_root=str(root))
        with patch("coordinate.cli_support.initialize", side_effect=AssertionError("must not initialize")):
            code, output, err = run_cli(path, "state", "demo", "--no-refresh")
        assert code == 0, err
        assert output["authoritative"] is True
        assert output["authority_scope"] == "checklist_cache"
        assert output["coordinate_ledger_included"] is False
