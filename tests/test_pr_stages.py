from __future__ import annotations

import sqlite3
import unittest
from unittest import mock

from coordinate.db import connect, migrate
from coordinate.pr_publishing import (
    PublishResult,
    discover_existing_target,
    discover_publish_target,
    persist_publish_outcome,
    validate_publish_request,
    validate_publish_request_existing,
)


class _StageTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = connect(":memory:")
        migrate(self.conn)
        self.conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, base_branch, "
            "branch_namespace, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
            ("ws", "ws", "/tmp/ws", "/tmp/ws/docs", "main", "agents"),
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()


class TestValidatePublishRequest(_StageTestCase):
    def _call(self, **overrides):
        defaults = {
            "conn": self.conn,
            "workspace_id": "ws",
            "task_id": "task-1",
            "repo": "owner/repo",
            "branch": "agents/mac-claude/task-1",
            "head_owner": "owner",
            "base": "main",
            "title": "title",
            "body": "body",
            "commit": "a" * 40,
            "pushed": True,
            "actor": "operator",
            "remote": None,
            "validation": None,
            "run": None,
        }
        defaults.update(overrides)
        return validate_publish_request(**defaults)

    def test_valid_request_returns_no_outcome(self):
        validated = self._call()
        self.assertIsNone(validated.outcome)
        self.assertEqual(validated.repo, "owner/repo")
        self.assertEqual(validated.branch, "agents/mac-claude/task-1")
        self.assertTrue(validated.pushed)

    def test_invalid_repo_returns_blocked_outcome(self):
        validated = self._call(repo="not-a-repo")
        self.assertEqual(validated.outcome.action, "blocked")
        self.assertEqual(validated.outcome.event_type, "publish.blocked")
        self.assertEqual(validated.outcome.extra_idem, "validation")

    def test_head_owner_mismatch_returns_blocked_outcome(self):
        validated = self._call(head_owner="other")
        self.assertEqual(validated.outcome.action, "blocked")
        self.assertEqual(validated.outcome.reason, "head_owner_mismatch")

    def test_not_pushed_returns_push_required_outcome(self):
        validated = self._call(pushed=False)
        self.assertEqual(validated.outcome.action, "push_required")
        self.assertEqual(validated.outcome.event_type, "push.required")
        self.assertEqual(validated.outcome.reason, "not_pushed")

    def test_mirror_conflict_returns_blocked_outcome(self):
        from coordinate.db import upsert_task_mirror
        upsert_task_mirror(
            self.conn,
            workspace_id="ws",
            task_id="task-1",
            phase="running",
            owner="x",
            branch="other-branch",
            pr=None,
            payload={},
        )
        validated = self._call()
        self.assertEqual(validated.outcome.action, "blocked")
        self.assertEqual(validated.outcome.reason, "mirror_conflict")


class TestDiscoverPublishTarget(_StageTestCase):
    def _validated(self):
        return validate_publish_request(
            self.conn,
            "ws",
            "task-1",
            repo="owner/repo",
            branch="agents/mac-claude/task-1",
            head_owner="owner",
            base="main",
            title="title",
            body="body",
            commit="a" * 40,
            pushed=True,
            actor="operator",
            remote=None,
            validation=None,
            run=None,
        )

    def test_ref_missing_returns_push_required(self):
        validated = self._validated()
        import coordinate.pr_publishing as publishing
        with mock.patch.object(
            publishing.github_module, "fetch_remote_ref", return_value=None
        ):
            target = discover_publish_target(validated)
        self.assertEqual(target.action, "push_required")
        self.assertEqual(target.reason, "ref_missing")

    def test_sha_mismatch_returns_blocked(self):
        validated = self._validated()
        import coordinate.pr_publishing as publishing
        with mock.patch.object(
            publishing.github_module, "fetch_remote_ref", return_value="b" * 40
        ):
            target = discover_publish_target(validated)
        self.assertEqual(target.action, "blocked")
        self.assertEqual(target.reason, "sha_mismatch")
        self.assertEqual(target.remote_sha, "b" * 40)

    def test_existing_pr_returns_linked(self):
        validated = self._validated()
        import coordinate.pr_publishing as publishing
        with mock.patch.object(
            publishing.github_module, "fetch_remote_ref", return_value="a" * 40
        ), mock.patch.object(
            publishing.github_module,
            "discover_open_pr_for_head",
            return_value={"url": "https://github.com/owner/repo/pull/1"},
        ):
            target = discover_publish_target(validated)
        self.assertEqual(target.action, "linked")
        self.assertEqual(
            target.pr_url, "https://github.com/owner/repo/pull/1",
        )

    def test_no_existing_pr_creates(self):
        validated = self._validated()
        import coordinate.pr_publishing as publishing
        with mock.patch.object(
            publishing.github_module, "fetch_remote_ref", return_value="a" * 40
        ), mock.patch.object(
            publishing.github_module, "discover_open_pr_for_head", return_value=None
        ), mock.patch.object(
            publishing.github_module,
            "create_pr",
            return_value="https://github.com/owner/repo/pull/2",
        ):
            target = discover_publish_target(validated)
        self.assertEqual(target.action, "created")
        self.assertEqual(
            target.pr_url, "https://github.com/owner/repo/pull/2",
        )


class TestPersistPublishOutcome(_StageTestCase):
    def _validated(self, action="created", pr_url=None):
        return validate_publish_request(
            self.conn,
            "ws",
            "task-1",
            repo="owner/repo",
            branch="agents/mac-claude/task-1",
            head_owner="owner",
            base="main",
            title="title",
            body="body",
            commit="a" * 40,
            pushed=True,
            actor="operator",
            remote="origin",
            validation="strict",
            run=None,
        )

    def test_blocked_outcome_emits_blocked_event(self):
        from coordinate.pr_publishing import _PublishOutcome
        validated = self._validated()
        outcome = _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem="validation",
            reason="invalid_repo",
            message="bad repo",
        )
        result = persist_publish_outcome(
            self.conn, validated, outcome,
            actor="operator", remote="origin", validation="strict",
        )
        self.assertIsInstance(result, PublishResult)
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["event_type"], "publish.blocked")

    def test_push_required_outcome_emits_push_required_event(self):
        from coordinate.pr_publishing import _PublishOutcome
        validated = self._validated()
        outcome = _PublishOutcome(
            action="push_required",
            event_type="push.required",
            extra_idem="not_pushed",
            reason="not_pushed",
            message="",
            detail="branch missing",
        )
        result = persist_publish_outcome(
            self.conn, validated, outcome,
            actor="operator", remote="origin", validation="strict",
        )
        self.assertEqual(result.action, "push_required")
        self.assertEqual(result.event["event_type"], "push.required")

    def test_linked_outcome_finalizes_link(self):
        from coordinate.pr_publishing import _PublishOutcome
        validated = self._validated()
        outcome = _PublishOutcome(
            action="linked",
            pr_url="https://github.com/owner/repo/pull/1",
            existing_pr_payload={"url": "https://github.com/owner/repo/pull/1"},
            remote_sha="a" * 40,
        )
        result = persist_publish_outcome(
            self.conn, validated, outcome,
            actor="operator", remote="origin", validation="strict",
        )
        self.assertEqual(result.action, "linked")
        self.assertEqual(result.pr_url, "https://github.com/owner/repo/pull/1")
        mirror = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws", "task-1"),
        ).fetchone()
        self.assertEqual(
            mirror["pr"], "https://github.com/owner/repo/pull/1",
        )

    def test_created_outcome_finalizes_created(self):
        from coordinate.pr_publishing import _PublishOutcome
        validated = self._validated()
        outcome = _PublishOutcome(
            action="created",
            pr_url="https://github.com/owner/repo/pull/2",
            remote_sha="a" * 40,
        )
        result = persist_publish_outcome(
            self.conn, validated, outcome,
            actor="operator", remote="origin", validation="strict",
        )
        self.assertEqual(result.action, "created")
        self.assertEqual(result.event["event_type"], "pr.created")


class TestValidatePublishRequestExisting(_StageTestCase):
    def test_existing_pr_mismatch_returns_blocked(self):
        from coordinate.db import upsert_task_mirror
        upsert_task_mirror(
            self.conn,
            workspace_id="ws",
            task_id="task-1",
            phase="running",
            owner="x",
            branch="agents/mac-claude/task-1",
            pr="https://github.com/owner/repo/pull/1",
            payload={},
        )
        validated = validate_publish_request_existing(
            self.conn,
            "ws",
            "task-1",
            repo="owner/repo",
            branch="agents/mac-claude/task-1",
            head_owner="owner",
            base="main",
            commit="a" * 40,
            expected_pr_url="https://github.com/owner/repo/pull/2",
            actor="operator",
            remote=None,
            validation=None,
            run=None,
        )
        self.assertEqual(validated.outcome.action, "blocked")
        self.assertEqual(validated.outcome.reason, "pr_already_linked")


if __name__ == "__main__":
    unittest.main()
