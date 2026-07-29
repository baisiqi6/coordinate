from __future__ import annotations

import sqlite3
import unittest

from coordinate import pr_contracts, pr_publishing, pr_recording, prs
from coordinate.db import connect, migrate


class TestPrContracts(unittest.TestCase):
    def test_imports_create_no_cycles(self):
        """The contract layer must be importable from both host and sink."""
        self.assertTrue(hasattr(pr_contracts, "publish_idempotency_key"))
        self.assertTrue(hasattr(pr_publishing, "publish_pr"))
        self.assertTrue(hasattr(pr_recording, "record_publish_result"))

    def test_recording_does_not_depend_on_publishing(self):
        """pr_recording must resolve shared invariants through contracts,
        not through the publishing module.
        """
        self.assertIsNone(pr_recording.__dict__.get("pr_publishing"))

    def test_action_to_event_type_matches_existing_behavior(self):
        self.assertEqual(
            pr_contracts.ACTION_TO_EVENT_TYPE,
            {
                "created": "pr.created",
                "linked": "pr.linked",
                "push_required": "push.required",
                "blocked": "publish.blocked",
            },
        )
        self.assertEqual(pr_contracts.PUBLISH_ACTIONS, {
            "created", "linked", "push_required", "blocked",
        })

    def test_publish_idempotency_key_shape(self):
        key = pr_contracts.publish_idempotency_key(
            "ws", "task", "pr.created", "owner/repo", "branch", "sha",
        )
        self.assertEqual(
            key, "ws:task:pr.created:owner/repo:branch:sha",
        )

    def test_publish_idempotency_key_with_extra(self):
        key = pr_contracts.publish_idempotency_key(
            "ws", "task", "pr.created", "owner/repo", "branch", "sha",
            extra="extra-bit",
        )
        self.assertEqual(
            key, "ws:task:pr.created:owner/repo:branch:sha:extra-bit",
        )

    def test_extract_mirror_publish_identity_current_payload(self):
        mirror = {
            "payload": {
                "publish_metadata": {
                    "repo": "owner/repo",
                    "reported_commit": "abc123",
                },
            },
        }
        self.assertEqual(
            pr_contracts.extract_mirror_publish_identity(mirror),
            ("owner/repo", "abc123"),
        )

    def test_extract_mirror_publish_identity_legacy_payload(self):
        mirror = {"payload": {"repo": "owner/repo", "commit": "def456"}}
        self.assertEqual(
            pr_contracts.extract_mirror_publish_identity(mirror),
            ("owner/repo", "def456"),
        )

    def test_extract_mirror_publish_identity_no_mirror(self):
        self.assertEqual(
            pr_contracts.extract_mirror_publish_identity(None),
            (None, None),
        )

    def test_check_mirror_conflict_no_mirror(self):
        self.assertIsNone(
            pr_contracts.check_mirror_conflict(
                mirror=None,
                repo="owner/repo",
                branch="main",
                commit="sha",
            )
        )

    def test_check_mirror_conflict_branch(self):
        mirror = {"branch": "other", "payload": {}}
        conflict = pr_contracts.check_mirror_conflict(
            mirror=mirror,
            repo="owner/repo",
            branch="main",
            commit="sha",
        )
        self.assertIn("mirror branch", conflict)
        self.assertIn("other", conflict)

    def test_check_mirror_conflict_repo(self):
        mirror = {"branch": "main", "payload": {"repo": "other/repo"}}
        conflict = pr_contracts.check_mirror_conflict(
            mirror=mirror,
            repo="owner/repo",
            branch="main",
            commit="sha",
        )
        self.assertIn("mirror repo", conflict)

    def test_check_mirror_conflict_commit(self):
        mirror = {"branch": "main", "payload": {"commit": "oldsha"}}
        conflict = pr_contracts.check_mirror_conflict(
            mirror=mirror,
            repo="owner/repo",
            branch="main",
            commit="newsha",
        )
        self.assertIn("mirror commit", conflict)

    def test_check_mirror_conflict_allows_commit_change(self):
        mirror = {"branch": "main", "payload": {"commit": "oldsha"}}
        self.assertIsNone(
            pr_contracts.check_mirror_conflict(
                mirror=mirror,
                repo="owner/repo",
                branch="main",
                commit="newsha",
                allow_commit_change=True,
            )
        )


class TestPrContractDatabaseChecks(unittest.TestCase):
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

    def test_check_cross_task_conflict_excludes_closed(self):
        self.conn.execute(
            "INSERT INTO tasks (workspace_id, task_id, phase, owner, branch, pr, "
            "payload_json, last_event_id, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("ws", "other", "closed", "x", "main", None, "{}", None),
        )
        self.conn.commit()
        self.assertIsNone(
            pr_contracts.check_cross_task_conflict(
                self.conn, workspace_id="ws", task_id="task",
                branch="main", pr_url=None,
            )
        )

    def test_check_cross_task_conflict_detects_active_branch(self):
        self.conn.execute(
            "INSERT INTO tasks (workspace_id, task_id, phase, owner, branch, pr, "
            "payload_json, last_event_id, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("ws", "other", "running", "x", "main", None, "{}", None),
        )
        self.conn.commit()
        conflict = pr_contracts.check_cross_task_conflict(
            self.conn, workspace_id="ws", task_id="task",
            branch="main", pr_url=None,
        )
        self.assertIn("already allocated", conflict)

    def test_check_existing_pr_rebind_detects_same_task_different_pr(self):
        self.conn.execute(
            "INSERT INTO tasks (workspace_id, task_id, phase, owner, branch, pr, "
            "payload_json, last_event_id, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("ws", "task", "running", "x", "main",
             "https://github.com/owner/repo/pull/1", "{}", None),
        )
        self.conn.commit()
        conflict = pr_contracts.check_existing_pr_rebind(
            self.conn, workspace_id="ws", task_id="task",
            pr_url="https://github.com/owner/repo/pull/2",
        )
        self.assertIn("already has pr", conflict)


class TestPrsContractAliases(unittest.TestCase):
    def test_legacy_aliases_point_at_contracts(self):
        self.assertIs(
            prs._idempotency_key,
            pr_contracts.publish_idempotency_key,
        )
        self.assertIs(
            prs._read_task_mirror,
            pr_contracts.read_task_mirror,
        )
        self.assertIs(
            prs._mirror_publish_identity,
            pr_contracts.extract_mirror_publish_identity,
        )
        self.assertIs(
            prs._mirror_conflict_check,
            pr_contracts.check_mirror_conflict,
        )
        self.assertIs(
            prs._cross_task_conflict_check,
            pr_contracts.check_cross_task_conflict,
        )


class TestValidatePublishSuccessFacts(unittest.TestCase):
    def _valid_envelope(self):
        return {
            "workspace_id": "ws",
            "repo": "owner/repo",
            "branch": "main",
            "reported_commit": "a" * 40,
            "head_ref": "owner:main",
            "base": "main",
            "pr_url": "https://github.com/owner/repo/pull/1",
            "remote_sha": "a" * 40,
            "result": {"commit": "a" * 40},
        }

    def test_valid_envelope_passes(self):
        pr_contracts.validate_publish_success_facts(**self._valid_envelope())

    def _run_with(self, **overrides):
        envelope = self._valid_envelope()
        envelope.update(overrides)
        pr_contracts.validate_publish_success_facts(**envelope)

    def test_mismatched_head_ref_raises(self):
        with self.assertRaises(pr_contracts.github_module.GitHubCommandError) as ctx:
            self._run_with(head_ref="wrong:main")
        self.assertEqual(ctx.exception.reason, "head_ref_mismatch")

    def test_mismatched_remote_sha_raises(self):
        with self.assertRaises(pr_contracts.github_module.GitHubCommandError) as ctx:
            self._run_with(remote_sha="b" * 40)
        self.assertEqual(ctx.exception.reason, "sha_mismatch")

    def test_missing_remote_sha_raises(self):
        with self.assertRaises(pr_contracts.github_module.GitHubCommandError) as ctx:
            self._run_with(remote_sha=None)
        self.assertEqual(ctx.exception.reason, "invalid_commit")


if __name__ == "__main__":
    unittest.main()
