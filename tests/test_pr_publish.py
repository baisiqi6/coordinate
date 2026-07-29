from __future__ import annotations

import json
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from coordinate.db import (
    append_event,
    connect,
    list_events,
    migrate,
    row_to_dict,
    upsert_task_mirror,
)
from coordinate.prs import (
    LinkPrResult,
    PublishError,
    RecordPublishError,
    _discover_pr,
    link_pr,
    publish_pr,
    publish_pr_existing,
    record_publish_preflight,
    record_publish_result,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_db():
    conn = connect(":memory:")
    migrate(conn)
    conn.execute(
        "INSERT INTO workspaces (id, name, path, harness_root, base_branch, branch_namespace, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
        ("ws-1", "test-ws", "/tmp/test", "/tmp/test/docs", "main", "agents"),
    )
    conn.commit()
    return conn


def _mock_run_factory(stdout='[]', returncode=0, side_effect=None):
    """Create a mock subprocess.run function."""
    def mock_run(*args, **kwargs):
        if side_effect:
            raise side_effect
        return subprocess.CompletedProcess(
            args=args[0], returncode=returncode, stdout=stdout, stderr='',
        )
    return mock_run


def _mock_run_discover():
    """Mock that returns a single open PR."""
    return _mock_run_factory(
        stdout='[{"url": "https://github.com/example/repo/pull/1"}]',
    )


# ---------------------------------------------------------------------------
# Phase 8.4 — publish_pr() decision tree
# ---------------------------------------------------------------------------


SHA = "0123456789abcdef0123456789abcdef01234567"


class _RunnerScript:
    """Records every gh argv + dispatches to a queue of canned responses."""

    def __init__(self, responses):
        # responses: list of dicts keyed by command substring, last-write-wins.
        self.responses = list(responses)
        self.calls = []

    def __call__(self, cmd):
        self.calls.append(list(cmd))
        for entry in self.responses:
            if entry["match"](cmd):
                return entry["make"](cmd)
        raise AssertionError(f"unexpected gh call: {cmd}")


def _ok(stdout="", returncode=0, stderr=""):
    parsed = stdout
    if isinstance(stdout, str) and stdout.lstrip().startswith("["):
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = stdout
    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict) and "headRefOid" in item:
                item.setdefault("headRefName", "agents/mac-claude/task-1")
                item.setdefault(
                    "headRepository",
                    {"nameWithOwner": "acme/repo"},
                )
                item.setdefault("headRepositoryOwner", {"login": "acme"})
                item.setdefault("isCrossRepository", False)
        stdout = json.dumps(parsed)
    elif isinstance(parsed, dict):
        stdout = json.dumps(parsed)
    return subprocess.CompletedProcess(
        args=["gh"], returncode=returncode, stdout=stdout, stderr=stderr,
    )


def _match_argv(*needles):
    """Match a gh command by required argv substrings."""
    def predicate(cmd):
        joined = " ".join(cmd)
        return all(n in joined for n in needles)
    return predicate


class TestPublishPr(unittest.TestCase):
    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _args(self, **over):
        base = dict(
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            title="Fix",
            body="Body",
            commit=SHA,
            pushed=True,
            actor="operator",
        )
        base.update(over)
        return base

    def _publish(self, runner, **over):
        return publish_pr(self.conn, run=runner, **self._args(**over))

    # ----- validation fail-closed -----

    def test_invalid_repo_blocks(self):
        runner = _RunnerScript([])
        result = self._publish(runner, repo="BAD/REPO")
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["event_type"], "publish.blocked")
        self.assertEqual(result.event["payload"]["reason"], "invalid_repo")
        self.assertEqual(self.runner_calls(runner), 0)

    def test_invalid_commit_blocks(self):
        runner = _RunnerScript([])
        result = self._publish(runner, commit="not-40-hex")
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "invalid_commit")
        self.assertEqual(self.runner_calls(runner), 0)

    def test_invalid_pushed_blocks(self):
        runner = _RunnerScript([])
        result = self._publish(runner, pushed="yes-please")
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "invalid_pushed")
        self.assertEqual(self.runner_calls(runner), 0)

    def test_unknown_workspace_raises(self):
        runner = _RunnerScript([])
        with self.assertRaises(PublishError) as ctx:
            publish_pr(
                self.conn, run=runner,
                workspace_id="missing", task_id="task-1",
                repo="a/b", branch="main", head_owner="a", base="main",
                title="t", body="", commit=SHA, pushed=True,
            )
        self.assertEqual(ctx.exception.reason, "unknown_workspace")

    # ----- mirror conflict -----

    def test_mirror_conflict_blocks(self):
        # Need a real event row for the FK; insert a placeholder.
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, actor, "
            "target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-1", "ws-1", "task-1", "branch.allocated", "operator", "acme/repo",
             "ws-1:branch:task-1:other/branch",
             json.dumps({"task_id": "task-1", "branch": "other/branch"})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="ready", owner="mac-claude", branch="other/branch",
            pr=None, payload={"repo": "acme/repo"},
            last_event_id="evt-1",
        )
        result = self._publish(_RunnerScript([]))
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "mirror_conflict")

    # ----- pushed=false -----

    def test_not_pushed_emits_push_required(self):
        runner = _RunnerScript([])
        result = self._publish(runner, pushed=False)
        self.assertEqual(result.action, "push_required")
        self.assertEqual(result.event["event_type"], "push.required")
        self.assertEqual(result.event["payload"]["repo"], "acme/repo")
        self.assertEqual(result.event["payload"]["branch"], "agents/mac-claude/task-1")
        self.assertEqual(result.event["payload"]["reported_commit"], SHA)
        self.assertEqual(result.event["payload"]["remote"], "origin")
        self.assertIn("next_action", result.event["payload"])
        # No gh calls expected
        self.assertEqual(self.runner_calls(runner), 0)

    # ----- remote ref missing -----

    def test_ref_missing_emits_push_required(self):
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(returncode=1, stderr="Not Found (HTTP 404)"),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "push_required")
        self.assertEqual(result.event["payload"]["detail"], "remote ref not found on GitHub")
        self.assertEqual(self.runner_calls(runner), 1)

    # ----- SHA mismatch -----

    def test_sha_mismatch_blocks(self):
        remote_sha = "ffffffffffffffffffffffffffffffffffffffff"
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": remote_sha}})),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "sha_mismatch")
        self.assertEqual(result.event["payload"]["reported_commit"], SHA)
        self.assertEqual(result.event["payload"]["remote_sha"], remote_sha)
        self.assertEqual(self.runner_calls(runner), 1)

    # ----- exact match + discover existing PR (link) -----

    def test_exact_sha_match_links_existing_pr(self):
        pr_url = "https://github.com/acme/repo/pull/9"
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([
                    {"number": 9, "url": pr_url, "headRefOid": SHA,
                     "baseRefName": "main", "title": "fix", "state": "OPEN"}
                ])),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "linked")
        self.assertEqual(result.pr_url, pr_url)
        self.assertEqual(result.event["event_type"], "pr.linked")
        # No gh pr create call
        self.assertFalse(any("create" in c for c in runner.calls))
        # Task mirror updated with PR
        row = self.conn.execute(
            "SELECT pr, branch FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], pr_url)
        self.assertEqual(row["branch"], "agents/mac-claude/task-1")

    def test_first_publish_rejects_noncanonical_discovered_pr_url(self):
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok({"object": {"sha": SHA}}),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok([{
                    "number": 9,
                    "url": "https://evil.invalid/acme/repo/pull/9",
                    "headRefOid": SHA,
                    "baseRefName": "main",
                    "title": "malicious",
                    "state": "OPEN",
                }]),
            ),
        ])

        result = self._publish(runner)

        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.reason, "discovery_mismatch")
        mirror = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(mirror)

    # ----- exact match + create new PR -----

    def test_exact_sha_match_creates_pr(self):
        pr_url = "https://github.com/acme/repo/pull/10"
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([])),
            ),
            dict(
                match=_match_argv("gh", "pr", "create"),
                make=lambda _cmd: _ok(stdout=f"https://github.com/acme/repo/pull/10\n"),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "created")
        self.assertEqual(result.pr_url, pr_url)
        self.assertEqual(result.event["event_type"], "pr.created")
        # Verify create argv shape (no shell interpolation)
        create_call = next(c for c in runner.calls if "create" in c)
        self.assertEqual(create_call[:5], ["gh", "pr", "create", "--repo", "acme/repo"])
        self.assertIn("--head", create_call)
        self.assertIn("acme:agents/mac-claude/task-1", create_call)
        self.assertIn("--base", create_call)
        self.assertIn("main", create_call)
        self.assertIn("--title", create_call)
        self.assertIn("Fix", create_call)
        self.assertIn("--body", create_call)
        self.assertIn("Body", create_call)
        # Task mirror updated
        row = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], pr_url)

    # ----- rerun idempotency: same call twice never duplicates -----

    def test_rerun_same_call_no_duplicate_event_or_pr(self):
        pr_url = "https://github.com/acme/repo/pull/11"

        def ref_call(cmd):
            return _ok(json.dumps({"object": {"sha": SHA}}))

        list_calls = {"n": 0}
        create_calls = {"n": 0}

        def list_call(cmd):
            list_calls["n"] += 1
            return _ok(json.dumps([
                {"number": 11, "url": pr_url, "headRefOid": SHA,
                 "baseRefName": "main", "title": "x", "state": "OPEN"}
            ]))

        def create_call(cmd):
            create_calls["n"] += 1
            return _ok(stdout=f"{pr_url}\n")

        responses = [
            dict(match=_match_argv("repos/acme/repo/git/ref/heads"),
                 make=ref_call),
            dict(match=_match_argv("gh", "pr", "list"), make=list_call),
            dict(match=_match_argv("gh", "pr", "create"), make=create_call),
        ]
        runner = _RunnerScript(responses)
        first = self._publish(runner)
        second = self._publish(runner)
        self.assertEqual(first.action, "linked")
        self.assertEqual(second.action, "linked")
        self.assertEqual(first.event["id"], second.event["id"])
        # Only one create call across the whole sequence, and none in the
        # rerun because discover found the existing PR.
        self.assertEqual(create_calls["n"], 0)
        self.assertEqual(list_calls["n"], 2)
        # Only one event per type in the DB
        rows = list_events(self.conn, "ws-1")
        pr_events = [r for r in rows if r["event_type"] == "pr.linked"]
        self.assertEqual(len(pr_events), 1)

    # ----- gh failures fail closed -----

    def test_gh_ref_lookup_failure_blocks(self):
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(returncode=1, stderr="auth required"),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "gh_failed")

    def test_gh_pr_create_failure_blocks(self):
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([])),
            ),
            dict(
                match=_match_argv("gh", "pr", "create"),
                make=lambda _cmd: _ok(returncode=1, stderr="title invalid"),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "gh_failed")
        # Task mirror must NOT exist (blocked path doesn't upsert mirror)
        row = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def runner_calls(self, runner):
        return len(runner.calls)


# ---------------------------------------------------------------------------
# Phase 8.4 review-fix coverage — P1-B (mirror payload) + P1-C (discover)
# ---------------------------------------------------------------------------


class TestPublishPrReviewFixes(unittest.TestCase):
    """Regression coverage for the 2026-06-19 reviewer findings.

    P1-B: blocked/push.required paths must NOT upsert the task mirror
    (no branch overwrite, no payload wipe).

    P1-C: discover_open_pr_for_head must reject PRs whose headRefOid or
    baseRefName do not match the requested commit / base.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _args(self, **over):
        base = dict(
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            title="Fix",
            body="Body",
            commit=SHA,
            pushed=True,
            actor="operator",
        )
        base.update(over)
        return base

    def _publish(self, runner, **over):
        return publish_pr(self.conn, run=runner, **self._args(**over))

    def _seed_mirror(self, branch, payload=None):
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-1", "ws-1", "task-1", "branch.allocated", "operator",
             "acme/repo", "ws-1:branch:task-1:" + branch,
             json.dumps({"task_id": "task-1", "branch": branch})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude", branch=branch,
            pr=None, payload=payload or {"metadata": {"foo": "bar"}},
            last_event_id="evt-1",
        )

    # ---- P1-B: blocked paths must not touch the task mirror ----

    def test_invalid_repo_does_not_create_mirror(self):
        runner = _RunnerScript([])
        result = self._publish(runner, repo="BAD/REPO")
        self.assertEqual(result.action, "blocked")
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def test_pushed_false_does_not_create_mirror(self):
        runner = _RunnerScript([])
        result = self._publish(runner, pushed=False)
        self.assertEqual(result.action, "push_required")
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def test_ref_missing_does_not_create_mirror(self):
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(
                    returncode=1, stderr="Not Found (HTTP 404)",
                ),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "push_required")
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def test_sha_mismatch_does_not_overwrite_existing_mirror(self):
        # Mirror already has the trusted branch + payload; publish must
        # record the mismatch as an event but leave the mirror alone.
        self._seed_mirror("trusted/branch", payload={
            "metadata": {"foo": "bar"}, "phase": "running",
        })
        remote_sha = "f" * 40
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps(
                    {"object": {"sha": remote_sha}},
                )),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "blocked")
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["branch"], "trusted/branch")
        payload = json.loads(row["payload_json"])
        self.assertEqual(payload.get("metadata", {}).get("foo"), "bar")
        self.assertEqual(payload.get("phase"), "running")

    def test_successful_link_preserves_existing_payload(self):
        # Mirror already has trusted metadata; a successful link must
        # merge publish_metadata in without erasing the original keys.
        self._seed_mirror(
            "agents/mac-claude/task-1",
            payload={"metadata": {"foo": "bar"}, "phase": "running"},
        )
        pr_url = "https://github.com/acme/repo/pull/9"
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps(
                    {"object": {"sha": SHA}},
                )),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 9, "url": pr_url, "headRefOid": SHA,
                    "baseRefName": "main", "title": "fix", "state": "OPEN",
                }])),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "linked")
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], pr_url)
        payload = json.loads(row["payload_json"])
        # Original keys preserved.
        self.assertEqual(payload.get("metadata", {}).get("foo"), "bar")
        self.assertEqual(payload.get("phase"), "running")
        # Publish metadata merged.
        self.assertEqual(payload["publish_metadata"]["repo"], "acme/repo")
        self.assertEqual(
            payload["publish_metadata"]["reported_commit"], SHA,
        )

    # ---- P1-C: discover_open_pr_for_head requires SHA + base match ----

    def test_discover_mismatch_head_oid_blocks(self):
        from coordinate.github import GitHubCommandError
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps(
                    {"object": {"sha": SHA}},
                )),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                # headRefOid points to a different commit.
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 9, "url": "https://github.com/acme/repo/pull/9",
                    "headRefOid": "f" * 40, "baseRefName": "main",
                    "title": "stale", "state": "OPEN",
                }])),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "discovery_mismatch")
        # Mirror must not be touched.
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def test_discover_mismatch_base_blocks(self):
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps(
                    {"object": {"sha": SHA}},
                )),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                # PR targets release branch, not main.
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 9, "url": "https://github.com/acme/repo/pull/9",
                    "headRefOid": SHA, "baseRefName": "release",
                    "title": "release-track", "state": "OPEN",
                }])),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "discovery_mismatch")
        # payload records the requested base + head_ref so the operator
        # sees what was requested vs. what GitHub had.
        self.assertEqual(result.event["payload"]["base"], "main")
        self.assertEqual(
            result.event["payload"]["head_ref"], "acme:agents/mac-claude/task-1",
        )

    def test_discover_match_links(self):
        pr_url = "https://github.com/acme/repo/pull/9"
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps(
                    {"object": {"sha": SHA}},
                )),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 9, "url": pr_url, "headRefOid": SHA,
                    "baseRefName": "main", "title": "fix", "state": "OPEN",
                }])),
            ),
        ])
        result = self._publish(runner)
        self.assertEqual(result.action, "linked")
        self.assertEqual(result.pr_url, pr_url)


class Phase84CrossDbCloseoutTests(unittest.TestCase):
    def setUp(self):
        self.remote_conn = _setup_db()
        self.host_conn = _setup_db()

    def tearDown(self):
        self.remote_conn.close()
        self.host_conn.close()

    def _result_envelope(self):
        return {
            "workspace_id": "ws-1",
            "task_id": "task-1",
            "repo": "acme/repo",
            "branch": "agents/mac-claude/task-1",
            "head_ref": "acme:agents/mac-claude/task-1",
            "base": "main",
            "commit": SHA,
            "reported_commit": SHA,
            "remote_sha": SHA,
            "pr_url": "https://github.com/acme/repo/pull/5",
            "action": "created",
        }

    def test_fresh_host_links_remote_expected_pr_and_repairs_local_mirror(self):
        record_publish_result(
            self.remote_conn,
            workspace_id="ws-1",
            result=self._result_envelope(),
        )
        preflight = record_publish_preflight(
            self.remote_conn,
            workspace_id="ws-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            reported_commit=SHA,
            task_id="task-1",
        )
        runner = _RunnerScript([
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 5,
                    "url": "https://github.com/acme/repo/pull/5",
                    "headRefOid": SHA,
                    "baseRefName": "main",
                    "title": "existing",
                    "state": "OPEN",
                }])),
            ),
        ])

        result = publish_pr_existing(
            self.host_conn,
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            commit=SHA,
            expected_pr_url=preflight["expected_pr_url"],
            run=runner,
        )

        self.assertEqual(result.action, "linked")
        self.assertEqual(len(runner.calls), 1)
        self.assertIn("list", runner.calls[0])
        local = self.host_conn.execute(
            "SELECT branch, pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(local["pr"], "https://github.com/acme/repo/pull/5")

        remote_replay = record_publish_result(
            self.remote_conn,
            workspace_id="ws-1",
            result=result.to_dict(),
        )
        self.assertEqual(remote_replay.action, "linked")
        self.assertTrue(remote_replay.event_created)
        remote = self.remote_conn.execute(
            "SELECT branch, pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(remote["branch"], "agents/mac-claude/task-1")
        self.assertEqual(remote["pr"], "https://github.com/acme/repo/pull/5")
        second_remote_replay = record_publish_result(
            self.remote_conn,
            workspace_id="ws-1",
            result=result.to_dict(),
        )
        self.assertFalse(second_remote_replay.event_created)
        self.assertEqual(
            second_remote_replay.event["id"],
            remote_replay.event["id"],
        )

    def test_sink_repo_mismatch_fails_but_existing_pr_commit_can_advance(self):
        record_publish_result(
            self.remote_conn,
            workspace_id="ws-1",
            result=self._result_envelope(),
        )

        wrong_repo = record_publish_preflight(
            self.remote_conn,
            workspace_id="ws-1",
            repo="other/repo",
            branch="agents/mac-claude/task-1",
            reported_commit=SHA,
            task_id="task-1",
        )
        wrong_commit = record_publish_preflight(
            self.remote_conn,
            workspace_id="ws-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            reported_commit="f" * 40,
            task_id="task-1",
        )

        self.assertFalse(wrong_repo["ok"])
        self.assertEqual(wrong_repo["reason"], "mirror_conflict")
        self.assertTrue(wrong_commit["ok"])
        self.assertEqual(wrong_commit["mode"], "link_existing")
        self.assertEqual(
            wrong_commit["expected_pr_url"],
            "https://github.com/acme/repo/pull/5",
        )


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Phase 8.4 review-fix round 2 — head_owner mismatch + record-only sink
# ---------------------------------------------------------------------------


class TestHeadOwnerMismatch(unittest.TestCase):
    """`head_owner` must equal the repo owner — fork workflow is out of
    scope. Mismatches fail closed as `publish.blocked (head_owner_mismatch)`.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _publish(self, **over):
        base = dict(
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            title="Fix",
            body="Body",
            commit=SHA,
            pushed=True,
            actor="operator",
        )
        base.update(over)
        return publish_pr(self.conn, run=_RunnerScript([]), **base)

    def test_head_owner_mismatch_blocks(self):
        # Realistic scenario from review: ref was verified at acme/repo,
        # but CLI passes --head-owner attacker; we must refuse even if
        # the SHA would match.
        result = self._publish(head_owner="attacker")
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "head_owner_mismatch")

    def test_head_owner_match_passes_validation(self):
        # Run with a realistic gh runner that does SHA match + create
        # so we exercise the success path with the equality check.
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps(
                    {"object": {"sha": SHA}},
                )),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([])),
            ),
            dict(
                match=_match_argv("gh", "pr", "create"),
                make=lambda _cmd: _ok(
                    stdout="https://github.com/acme/repo/pull/42\n",
                ),
            ),
        ])
        base = dict(
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            title="Fix",
            body="Body",
            commit=SHA,
            pushed=True,
            actor="operator",
        )
        result = publish_pr(self.conn, run=runner, **base)
        self.assertEqual(result.action, "created")


class TestRecordPublishSink(unittest.TestCase):
    """Cross-DB integration: the host publishes into a local DB; the
    record-only sink writes the host's PublishResult into a *remote*
    DB (second connection), so `merge gate` on the remote side sees
    the PR.
    """

    def setUp(self):
        # Two independent in-memory connections — they share the schema
        # but not the data; this mirrors two DB files on two hosts.
        self.host_conn = _setup_db()
        self.remote_conn = _setup_db()

    def tearDown(self):
        self.host_conn.close()
        self.remote_conn.close()

    def _host_publish(self):
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps(
                    {"object": {"sha": SHA}},
                )),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([])),
            ),
            dict(
                match=_match_argv("gh", "pr", "create"),
                make=lambda _cmd: _ok(
                    stdout="https://github.com/acme/repo/pull/99\n",
                ),
            ),
        ])
        return publish_pr(
            self.host_conn,
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            title="Fix",
            body="Body",
            commit=SHA,
            pushed=True,
            actor="operator",
            run=runner,
        )

    def test_host_publish_then_remote_sink_makes_merge_gate_happy(self):
        # Step 1: host runs publish_pr.
        host_result = self._host_publish()
        self.assertEqual(host_result.action, "created")
        self.assertEqual(host_result.event["event_type"], "pr.created")

        # Step 2: forward to remote record-only sink.
        from coordinate.prs import record_publish_result
        recorded = record_publish_result(
            self.remote_conn,
            workspace_id="ws-1",
            result=host_result.to_dict(),
            actor="host-cli",
        )
        self.assertEqual(recorded.action, "created")
        self.assertTrue(recorded.mirror_updated)
        self.assertTrue(recorded.event_created)

        # Step 3: the remote `tasks` row must now show the PR URL.
        row = self.remote_conn.execute(
            "SELECT pr, branch FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], "https://github.com/acme/repo/pull/99")
        self.assertEqual(row["branch"], "agents/mac-claude/task-1")

        # Step 4: simulate merge gate reading remote DB — must see PR.
        from coordinate.reviews import check_merge_gate
        gate = check_merge_gate(
            self.remote_conn,
            workspace_id="ws-1",
            task_id="task-1",
        )
        # The task has a PR but no approval/CI events yet → not ready,
        # but the `has_pr` check must be present and pass so the gate
        # sees the PR exists in the remote mirror.
        self.assertIn("has_pr", gate.checks)
        self.assertTrue(gate.checks["has_pr"].get("passed"))
        self.assertEqual(
            gate.checks["has_pr"].get("pr"),
            "https://github.com/acme/repo/pull/99",
        )

    def test_remote_sink_preserves_existing_trusted_branch(self):
        # Seed remote mirror with a trusted branch.
        self.remote_conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-seed", "ws-1", "task-1", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-1:trusted/x",
             json.dumps({"task_id": "task-1", "branch": "trusted/x"})),
        )
        self.remote_conn.commit()
        upsert_task_mirror(
            self.remote_conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude", branch="trusted/x",
            pr=None, payload={"metadata": {"foo": "bar"}},
            last_event_id="evt-seed",
        )

        host_result = self._host_publish()
        from coordinate.prs import record_publish_result, RecordPublishError
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.remote_conn,
                workspace_id="ws-1",
                result=host_result.to_dict(),
            )
        self.assertEqual(ctx.exception.reason, "mirror_conflict")
        # Mirror branch must NOT have been overwritten.
        row = self.remote_conn.execute(
            "SELECT branch FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["branch"], "trusted/x")
        # And no event was appended either.
        from coordinate.db import list_events
        events = [row_to_dict(r) for r in list_events(self.remote_conn, "ws-1")]
        pr_events = [e for e in events if e["event_type"] in
                     {"pr.created", "pr.linked"}]
        self.assertEqual(pr_events, [])

    def test_remote_sink_push_required_does_not_upsert_mirror(self):
        from coordinate.prs import record_publish_result
        host_result = publish_pr(
            self.host_conn,
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            title="Fix",
            body="Body",
            commit=SHA,
            pushed=False,
            actor="operator",
            run=_RunnerScript([]),
        )
        self.assertEqual(host_result.action, "push_required")
        recorded = record_publish_result(
            self.remote_conn,
            workspace_id="ws-1",
            result=host_result.to_dict(),
        )
        self.assertEqual(recorded.action, "push_required")
        self.assertFalse(recorded.mirror_updated)
        # Remote has no task mirror row.
        row = self.remote_conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def test_remote_sink_replay_is_idempotent(self):
        host_result = self._host_publish()
        from coordinate.prs import record_publish_result
        first = record_publish_result(
            self.remote_conn, workspace_id="ws-1",
            result=host_result.to_dict(),
        )
        second = record_publish_result(
            self.remote_conn, workspace_id="ws-1",
            result=host_result.to_dict(),
        )
        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertTrue(first.mirror_updated)
        self.assertFalse(second.mirror_updated)


class TestPublishPrExisting(unittest.TestCase):
    """`publish_pr_existing` is the read-only idempotent path used when the
    remote mirror already records a PR for this task.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _seed_mirror_with_pr(self, pr_url, payload=None):
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-seed", "ws-1", "task-1", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-1:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude",
            branch="agents/mac-claude/task-1",
            pr=pr_url, payload=payload or {"repo": "acme/repo"},
            last_event_id="evt-seed",
        )

    def test_links_existing_pr_when_discover_matches(self):
        pr_url = "https://github.com/acme/repo/pull/5"
        self._seed_mirror_with_pr(pr_url)
        runner = _RunnerScript([
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 5, "url": pr_url, "headRefOid": SHA,
                    "baseRefName": "main", "title": "same", "state": "OPEN",
                }])),
            ),
        ])
        result = publish_pr_existing(
            self.conn,
            workspace_id="ws-1", task_id="task-1",
            repo="acme/repo", branch="agents/mac-claude/task-1",
            head_owner="acme", base="main", commit=SHA,
            expected_pr_url=pr_url,
            run=runner,
        )
        self.assertEqual(result.action, "linked")
        self.assertEqual(result.pr_url, pr_url)
        # No create call; only list.
        self.assertFalse(any("create" in c for c in runner.calls))
        self.assertTrue(any("list" in c for c in runner.calls))
        row = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], pr_url)

    def test_links_same_pr_after_verified_commit_advance(self):
        pr_url = "https://github.com/acme/repo/pull/5"
        self._seed_mirror_with_pr(pr_url, payload={"publish_metadata": {
            "repo": "acme/repo",
            "reported_commit": SHA,
            "pr_url": pr_url,
        }})
        next_sha = "f" * 40
        runner = _RunnerScript([
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 5,
                    "url": pr_url,
                    "headRefOid": next_sha,
                    "baseRefName": "main",
                    "title": "updated",
                    "state": "OPEN",
                }])),
            ),
        ])

        result = publish_pr_existing(
            self.conn,
            workspace_id="ws-1",
            task_id="task-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            head_owner="acme",
            base="main",
            commit=next_sha,
            expected_pr_url=pr_url,
            run=runner,
        )

        self.assertEqual(result.action, "linked")
        row = self.conn.execute(
            "SELECT payload_json FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(
            json.loads(row["payload_json"])["publish_metadata"]["reported_commit"],
            next_sha,
        )

    def test_blocks_when_discovered_url_mismatches(self):
        pr_url = "https://github.com/acme/repo/pull/5"
        self._seed_mirror_with_pr(pr_url)
        runner = _RunnerScript([
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 6, "url": "https://github.com/acme/repo/pull/6",
                    "headRefOid": SHA, "baseRefName": "main",
                    "title": "different", "state": "OPEN",
                }])),
            ),
        ])
        result = publish_pr_existing(
            self.conn,
            workspace_id="ws-1", task_id="task-1",
            repo="acme/repo", branch="agents/mac-claude/task-1",
            head_owner="acme", base="main", commit=SHA,
            expected_pr_url=pr_url,
            run=runner,
        )
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "discover_url_mismatch")

    def test_blocks_when_no_open_pr_found(self):
        pr_url = "https://github.com/acme/repo/pull/5"
        self._seed_mirror_with_pr(pr_url)
        runner = _RunnerScript([
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([])),
            ),
        ])
        result = publish_pr_existing(
            self.conn,
            workspace_id="ws-1", task_id="task-1",
            repo="acme/repo", branch="agents/mac-claude/task-1",
            head_owner="acme", base="main", commit=SHA,
            expected_pr_url=pr_url,
            run=runner,
        )
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "discover_missing_pr")

    def test_blocks_when_sha_mismatches(self):
        pr_url = "https://github.com/acme/repo/pull/5"
        self._seed_mirror_with_pr(pr_url)
        runner = _RunnerScript([
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 5, "url": pr_url, "headRefOid": "f" * 40,
                    "baseRefName": "main", "title": "stale", "state": "OPEN",
                }])),
            ),
        ])
        result = publish_pr_existing(
            self.conn,
            workspace_id="ws-1", task_id="task-1",
            repo="acme/repo", branch="agents/mac-claude/task-1",
            head_owner="acme", base="main", commit=SHA,
            expected_pr_url=pr_url,
            run=runner,
        )
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "discovery_mismatch")

    def test_blocks_when_base_mismatches(self):
        pr_url = "https://github.com/acme/repo/pull/5"
        self._seed_mirror_with_pr(pr_url)
        runner = _RunnerScript([
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 5, "url": pr_url, "headRefOid": SHA,
                    "baseRefName": "release", "title": "release", "state": "OPEN",
                }])),
            ),
        ])
        result = publish_pr_existing(
            self.conn,
            workspace_id="ws-1", task_id="task-1",
            repo="acme/repo", branch="agents/mac-claude/task-1",
            head_owner="acme", base="main", commit=SHA,
            expected_pr_url=pr_url,
            run=runner,
        )
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "discovery_mismatch")


# ---------------------------------------------------------------------------
# Phase 8.4 review-fix round 3 — preflight + server-recomputed event +
# SAVEPOINT atomicity. These are the regressions for the 2026-06-21
# codex review.
# ---------------------------------------------------------------------------


class TestRecordPublishPreflight(unittest.TestCase):
    """`record_publish_preflight` is the read-only check the host CLI
    must run BEFORE any `gh` call.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _call(self, **over):
        base = dict(
            workspace_id="ws-1",
            repo="acme/repo",
            branch="agents/mac-claude/task-1",
            reported_commit=SHA,
            task_id="task-1",
        )
        base.update(over)
        return record_publish_preflight(self.conn, **base)

    def test_ok_when_no_mirror(self):
        result = self._call()
        self.assertTrue(result["ok"])
        self.assertIsNone(result["reason"])

    def test_ok_when_mirror_matches(self):
        # Seed a mirror consistent with the worker's claim.
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-seed", "ws-1", "task-1", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-1:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude",
            branch="agents/mac-claude/task-1",
            pr=None, payload={"repo": "acme/repo"},
            last_event_id="evt-seed",
        )
        result = self._call()
        self.assertTrue(result["ok"])

    def test_mirror_conflict_returns_reason(self):
        # Seed mirror with conflicting branch.
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-seed", "ws-1", "task-1", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-1:trusted/x",
             json.dumps({"task_id": "task-1", "branch": "trusted/x"})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude", branch="trusted/x",
            pr=None, payload={"repo": "acme/repo"},
            last_event_id="evt-seed",
        )
        result = self._call()
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "mirror_conflict")
        self.assertEqual(result["mirror_branch"], "trusted/x")

    def test_unknown_workspace_returns_reason(self):
        result = self._call(workspace_id="missing")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "unknown_workspace")

    def test_preflight_existing_pr_returns_link_existing_mode(self):
        # Seed a mirror that already has a PR. The host must NOT call
        # `gh pr create`; preflight should tell it to discover/link the
        # existing PR read-only.
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-seed", "ws-1", "task-1", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-1:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude",
            branch="agents/mac-claude/task-1",
            pr="https://github.com/acme/repo/pull/5",
            payload={"repo": "acme/repo"},
            last_event_id="evt-seed",
        )
        result = self._call()
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "link_existing")
        self.assertEqual(result["expected_pr_url"], "https://github.com/acme/repo/pull/5")

    def test_preflight_existing_pr_mismatched_branch_blocks(self):
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-seed", "ws-1", "task-1", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-1:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude",
            branch="agents/mac-claude/task-1",
            pr="https://github.com/acme/repo/pull/5",
            payload={"repo": "acme/repo"},
            last_event_id="evt-seed",
        )
        result = self._call(branch="agents/other/task-1")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "mirror_conflict")

    def test_preflight_existing_pr_allows_commit_advance_for_same_binding(self):
        upsert_task_mirror(
            self.conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase="running",
            owner="mac-claude",
            branch="agents/mac-claude/task-1",
            pr="https://github.com/acme/repo/pull/5",
            payload={"publish_metadata": {
                "repo": "acme/repo",
                "reported_commit": SHA,
                "pr_url": "https://github.com/acme/repo/pull/5",
            }},
        )

        result = self._call(reported_commit="f" * 40)

        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "link_existing")
        self.assertEqual(
            result["expected_pr_url"],
            "https://github.com/acme/repo/pull/5",
        )

    def test_current_publish_metadata_precedes_legacy_top_level_identity(self):
        upsert_task_mirror(
            self.conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase="running",
            owner="mac-claude",
            branch="agents/mac-claude/task-1",
            pr=None,
            payload={
                "repo": "legacy/repo",
                "commit": "f" * 40,
                "publish_metadata": {
                    "repo": "acme/repo",
                    "reported_commit": SHA,
                },
            },
        )

        result = self._call()

        self.assertTrue(result["ok"])

    def test_preflight_writes_nothing(self):
        # preflight must not append events or touch mirror.
        from coordinate.db import list_events
        result = self._call()
        self.assertTrue(result["ok"])
        events = list_events(self.conn, "ws-1")
        self.assertEqual(events, [])
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)


class TestRecordPublishServerRecompute(unittest.TestCase):
    """The remote sink must NOT trust the host's event_type /
    idempotency_key / event_payload. It recomputes them from the
    minimal facts.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _publish_envelope(self, action="created", **over):
        envelope = {
            "workspace_id": "ws-1",
            "task_id": "task-1",
            "repo": "acme/repo",
            "branch": "agents/mac-claude/task-1",
            "head_ref": "acme:agents/mac-claude/task-1",
            "base": "main",
            "commit": SHA,
            "reported_commit": SHA,
            "remote_sha": SHA,
            "pr_url": "https://github.com/acme/repo/pull/99",
            "action": action,
        }
        envelope.update(over)
        return envelope

    def test_action_created_yields_pr_created_event(self):
        result = record_publish_result(
            self.conn, workspace_id="ws-1",
            result=self._publish_envelope(action="created"),
        )
        self.assertEqual(result.action, "created")
        self.assertEqual(result.event["event_type"], "pr.created")
        self.assertEqual(
            result.event["idempotency_key"],
            f"ws-1:task-1:pr.created:acme/repo:agents/mac-claude/task-1:{SHA}:publish:https://github.com/acme/repo/pull/99",
        )

    def test_action_push_required_yields_push_required_event(self):
        result = record_publish_result(
            self.conn, workspace_id="ws-1",
            result=self._publish_envelope(
                action="push_required", pr_url="", remote_sha=None,
            ),
        )
        self.assertEqual(result.event["event_type"], "push.required")
        self.assertNotIn("publish:https", result.event["idempotency_key"])

    def test_action_blocked_yields_publish_blocked_event(self):
        result = record_publish_result(
            self.conn, workspace_id="ws-1",
            result=self._publish_envelope(
                action="blocked", pr_url="", remote_sha=None,
            ),
        )
        self.assertEqual(result.event["event_type"], "publish.blocked")

    def test_host_event_field_is_ignored(self):
        # Host tries to inject a wrong event_type; server must ignore.
        result = record_publish_result(
            self.conn, workspace_id="ws-1",
            result={
                **self._publish_envelope(action="created"),
                "event": {
                    "event_type": "publish.blocked",
                    "idempotency_key": "host-supplied-bogus",
                    "payload": {"host": "tried"},
                },
            },
        )
        # Server's canonical event_type wins.
        self.assertEqual(result.event["event_type"], "pr.created")
        # The server's idempotency key is what we computed, not the
        # host's "host-supplied-bogus".
        self.assertNotEqual(
            result.event["idempotency_key"], "host-supplied-bogus",
        )
        self.assertEqual(
            result.event["payload"]["pr"],
            "https://github.com/acme/repo/pull/99",
        )

    def test_created_without_pr_url_raises(self):
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn, workspace_id="ws-1",
                result=self._publish_envelope(action="created", pr_url=""),
            )
        self.assertEqual(ctx.exception.reason, "invalid_result")

    def test_success_action_rejects_noncanonical_host_facts(self):
        cases = {
            "repo": {"repo": "not-a-repo"},
            "branch": {"branch": "-unsafe"},
            "reported_commit": {"reported_commit": "not-a-sha"},
            "head_ref": {"head_ref": "attacker:agents/mac-claude/task-1"},
            "base": {"base": "-unsafe"},
            "pr_url": {"pr_url": "https://evil.example/acme/repo/pull/99"},
            "remote_sha": {"remote_sha": "f" * 40},
            "workspace_id": {"workspace_id": "other-workspace"},
        }
        for label, overrides in cases.items():
            with self.subTest(label=label):
                conn = _setup_db()
                try:
                    with self.assertRaises(RecordPublishError) as ctx:
                        record_publish_result(
                            conn,
                            workspace_id="ws-1",
                            result=self._publish_envelope(**overrides),
                        )
                    self.assertEqual(ctx.exception.reason, "invalid_result")
                    self.assertEqual(list_events(conn, "ws-1"), [])
                    self.assertIsNone(conn.execute(
                        "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
                        ("ws-1", "task-1"),
                    ).fetchone())
                finally:
                    conn.close()

    def test_success_pr_url_edge_cases_leave_remote_state_empty(self):
        for pr_url in [
            "https://github.com/acme/repo/pull/١",
            "https://github.com/acme/repo/pull/²",
            "https://github.com/acme/repo/pull/99?",
            "https://github.com/acme/repo/pull/99#",
            " https://github.com/acme/repo/pull/99",
            "\x00https://github.com/acme/repo/pull/99",
            "https://git\thub.com/acme/repo/pull/99",
            "https://github.com/ac\nme/repo/pull/99",
            "https://github.com/acme/repo/pull/99\n",
            "https://github.com/acme/repo/pull/99;",
        ]:
            with self.subTest(pr_url=pr_url):
                conn = _setup_db()
                try:
                    with self.assertRaises(RecordPublishError) as ctx:
                        record_publish_result(
                            conn,
                            workspace_id="ws-1",
                            result=self._publish_envelope(pr_url=pr_url),
                        )
                    self.assertEqual(ctx.exception.reason, "invalid_result")
                    self.assertEqual(list_events(conn, "ws-1"), [])
                    self.assertIsNone(conn.execute(
                        "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
                        ("ws-1", "task-1"),
                    ).fetchone())
                finally:
                    conn.close()

    def test_success_repo_dot_segments_leave_remote_state_empty(self):
        cases = [
            ("acme/..", "https://github.com/acme/../pull/1"),
            ("../..", "https://github.com/../../pull/1"),
        ]
        for repo, pr_url in cases:
            with self.subTest(repo=repo):
                conn = _setup_db()
                try:
                    with self.assertRaises(RecordPublishError) as ctx:
                        record_publish_result(
                            conn,
                            workspace_id="ws-1",
                            result=self._publish_envelope(
                                repo=repo,
                                pr_url=pr_url,
                            ),
                        )
                    self.assertEqual(ctx.exception.reason, "invalid_result")
                    self.assertEqual(list_events(conn, "ws-1"), [])
                    self.assertIsNone(conn.execute(
                        "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
                        ("ws-1", "task-1"),
                    ).fetchone())
                finally:
                    conn.close()

    def test_unknown_action_raises(self):
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn, workspace_id="ws-1",
                result=self._publish_envelope(action="frobnicate"),
            )
        self.assertEqual(ctx.exception.reason, "invalid_result")

    def test_workspace_mismatch_rejected_for_blocked_audit(self):
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn,
                workspace_id="ws-1",
                result=self._publish_envelope(
                    action="blocked",
                    workspace_id="other-workspace",
                    pr_url="",
                    remote_sha=None,
                ),
            )
        self.assertEqual(ctx.exception.reason, "invalid_result")
        self.assertEqual(list_events(self.conn, "ws-1"), [])


class TestRecordPublishAtomicity(unittest.TestCase):
    """SAVEPOINT-scoped `record_publish_result`: append_event +
    mirror upsert are atomic. A failure inside the wrapper leaves no
    half-state.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _envelope(self, action="created", **over):
        envelope = {
            "workspace_id": "ws-1",
            "task_id": "task-1",
            "repo": "acme/repo",
            "branch": "agents/mac-claude/task-1",
            "head_ref": "acme:agents/mac-claude/task-1",
            "base": "main",
            "commit": SHA,
            "reported_commit": SHA,
            "remote_sha": SHA,
            "pr_url": "https://github.com/acme/repo/pull/99",
            "action": action,
        }
        envelope.update(over)
        return envelope

    def test_failure_rolls_back_event_and_mirror(self):
        import coordinate.pr_recording as recording_module

        # Patch the upsert_task_mirror reference that record_publish_result
        # imports directly in coordinate.pr_recording.
        original_upsert = recording_module.upsert_task_mirror
        call_count = {"n": 0}

        def boom(*args, **kwargs):
            call_count["n"] += 1
            raise sqlite3.OperationalError("simulated mirror upsert failure")

        recording_module.upsert_task_mirror = boom
        try:
            with self.assertRaises(sqlite3.OperationalError):
                record_publish_result(
                    self.conn, workspace_id="ws-1",
                    result=self._envelope(),
                )
        finally:
            recording_module.upsert_task_mirror = original_upsert

        # Atomic guarantee: no event, no mirror.
        events = list_events(self.conn, "ws-1")
        self.assertEqual(events, [])
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def test_replay_repairs_missing_mirror(self):
        """Review scenario: a transient half-completion leaves an
        event but no mirror row. A subsequent replay with the same
        inputs must repair the mirror — the sink is not allowed to
        skip the upsert just because the event already exists.
        """
        import coordinate.pr_recording as recording_module

        envelope = self._envelope()

        # First call: simulate append_event commits but mirror upsert
        # fails. We do this by patching upsert_task_mirror to raise
        # on the *first* call only.
        original_upsert = recording_module.upsert_task_mirror
        state = {"raised": False}

        def boom_once(*args, **kwargs):
            if not state["raised"]:
                state["raised"] = True
                raise sqlite3.OperationalError("simulated first-time failure")
            return original_upsert(*args, **kwargs)

        recording_module.upsert_task_mirror = boom_once
        try:
            with self.assertRaises(sqlite3.OperationalError):
                record_publish_result(
                    self.conn, workspace_id="ws-1",
                    result=envelope,
                )
            # Half-state: nothing should be on disk because SAVEPOINT
            # rolled back. We assert this to make the regression
            # contract explicit before continuing to the replay.
            events_before = list_events(self.conn, "ws-1")
            self.assertEqual(events_before, [])
            mirror_before = self.conn.execute(
                "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
                ("ws-1", "task-1"),
            ).fetchone()
            self.assertIsNone(mirror_before)

            # Now replay the same envelope: server must append the
            # event AND upsert the mirror atomically.
            result = record_publish_result(
                self.conn, workspace_id="ws-1",
                result=envelope,
            )
        finally:
            recording_module.upsert_task_mirror = original_upsert

        self.assertEqual(result.action, "created")
        # Event was created (first time this run); mirror was upserted.
        self.assertTrue(result.event_created)
        self.assertTrue(result.mirror_updated)
        # DB state matches the post-success path.
        events_after = list_events(self.conn, "ws-1")
        self.assertEqual(len(events_after), 1)
        self.assertEqual(events_after[0]["event_type"], "pr.created")
        mirror_after = self.conn.execute(
            "SELECT pr, branch FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(mirror_after["pr"],
                         "https://github.com/acme/repo/pull/99")

    def test_replay_no_duplicate_event(self):
        envelope = self._envelope()
        first = record_publish_result(
            self.conn, workspace_id="ws-1", result=envelope,
        )
        second = record_publish_result(
            self.conn, workspace_id="ws-1", result=envelope,
        )
        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        # Mirror is the same row on the second call.
        events = list_events(self.conn, "ws-1")
        self.assertEqual(len(events), 1)

    def test_replay_preserves_newer_lifecycle_last_event(self):
        envelope = self._envelope()
        first = record_publish_result(
            self.conn,
            workspace_id="ws-1",
            result=envelope,
        )
        mirror = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        later = append_event(
            self.conn,
            event_type="assignment.started",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-1",
            payload={"task_id": "task-1"},
        )
        upsert_task_mirror(
            self.conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase=mirror["phase"],
            owner=mirror["owner"],
            branch=mirror["branch"],
            pr=mirror["pr"],
            payload=json.loads(mirror["payload_json"]),
            last_event_id=later.row["id"],
        )

        replay = record_publish_result(
            self.conn,
            workspace_id="ws-1",
            result=envelope,
        )

        self.assertFalse(replay.event_created)
        self.assertFalse(replay.mirror_updated)
        after = self.conn.execute(
            "SELECT last_event_id FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(after["last_event_id"], later.row["id"])
        self.assertNotEqual(after["last_event_id"], first.event["id"])

    def test_failure_releases_transaction_lock(self):
        """After a SAVEPOINT rollback the connection must not hold a
        transaction lock; a second connection can read the DB.
        """
        import coordinate.pr_recording as recording_module

        original_upsert = recording_module.upsert_task_mirror
        recording_module.upsert_task_mirror = lambda *args, **kwargs: (_
            for _ in ()).throw(sqlite3.OperationalError("simulated"))
        try:
            with self.assertRaises(sqlite3.OperationalError):
                record_publish_result(
                    self.conn, workspace_id="ws-1", result=self._envelope(),
                )
        finally:
            recording_module.upsert_task_mirror = original_upsert

        self.assertFalse(self.conn.in_transaction)
        events = list_events(self.conn, "ws-1")
        self.assertEqual(events, [])
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNone(row)

    def test_cross_task_branch_conflict_blocked(self):
        """A branch already allocated to another task is rejected."""
        from coordinate.db import upsert_task_mirror as _upm
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-other", "ws-1", "task-other", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-other:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-other",
                         "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        _upm(
            self.conn, workspace_id="ws-1", task_id="task-other",
            phase="running", owner="x", branch="agents/mac-claude/task-1",
            pr=None, payload={},
            last_event_id="evt-other",
        )
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn, workspace_id="ws-1", result=self._envelope(),
            )
        self.assertEqual(ctx.exception.reason, "cross_task_conflict")

    def test_cross_task_pr_conflict_blocked(self):
        """A PR already linked to another task is rejected."""
        from coordinate.db import upsert_task_mirror as _upm
        pr_url = "https://github.com/acme/repo/pull/99"
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-other", "ws-1", "task-other", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-other:other/branch",
             json.dumps({"task_id": "task-other", "branch": "other/branch"})),
        )
        self.conn.commit()
        _upm(
            self.conn, workspace_id="ws-1", task_id="task-other",
            phase="running", owner="x", branch="other/branch",
            pr=pr_url, payload={},
            last_event_id="evt-other",
        )
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn, workspace_id="ws-1", result=self._envelope(),
            )
        self.assertEqual(ctx.exception.reason, "cross_task_conflict")

    def test_audit_fields_round_trip_in_blocked_payload(self):
        """Remote/validation/message/detail from the host must survive
        server-side payload recompute.
        """
        result = record_publish_result(
            self.conn,
            workspace_id="ws-1",
            result={
                "workspace_id": "ws-1",
                "task_id": "task-1",
                "repo": "acme/repo",
                "branch": "agents/mac-claude/task-1",
                "head_ref": "acme:agents/mac-claude/task-1",
                "base": "main",
                "commit": SHA,
                "reported_commit": SHA,
                "remote_sha": None,
                "pr_url": "",
                "action": "blocked",
                "remote": "upstream",
                "validation": "314 OK",
                "message": "remote SHA mismatch",
                "detail": "ref missing",
                "reason": "sha_mismatch",
            },
        )
        self.assertEqual(result.action, "blocked")
        payload = result.event["payload"]
        self.assertEqual(payload["remote"], "upstream")
        self.assertEqual(payload["validation"], "314 OK")
        self.assertEqual(payload["message"], "remote SHA mismatch")
        self.assertEqual(payload["detail"], "ref missing")


class TestPrPublishPreflightCLI(unittest.TestCase):
    """The host CLI must run the remote preflight BEFORE invoking gh,
    and must exit 1 without calling gh when the preflight returns
    ok=false.
    """

    def setUp(self):
        import contextlib
        import io
        from coordinate.cli import main
        from coordinate.db import (
            connect, migrate, upsert_workspace,
        )
        self._main = main
        self._connect = connect
        self._migrate = migrate
        self._upsert_workspace = upsert_workspace
        self._ctxlib = contextlib
        self._io = io

    def _setup_db(self, db_path, tmp):
        conn = self._connect(db_path)
        self._migrate(conn)
        self._upsert_workspace(
            conn, workspace_id="demo", name="d", path=tmp,
            harness_root=tmp,
        )
        conn.close()

    def _run_cli(self, db_path, argv):
        stdout = self._io.StringIO()
        stderr = self._io.StringIO()
        with self._ctxlib.redirect_stdout(stdout), self._ctxlib.redirect_stderr(stderr):
            code = self._main(["--db", db_path] + argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_preflight_failure_returns_nonzero_and_skips_gh(self):
        import json as _json
        import subprocess

        from coordinate import cli as cli_module
        from coordinate import prs as prs_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "c.sqlite3")
            self._setup_db(db_path, tmp)

            # Seed a mirror with a conflicting branch.
            conn = self._connect(db_path)
            self._migrate(conn)
            conn.execute(
                "INSERT INTO events (id, workspace_id, task_id, event_type, "
                "actor, target, idempotency_key, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                ("evt-seed", "demo", "task-1", "branch.allocated", "op",
                 "acme/repo", "demo:branch:task-1:trusted/x",
                 _json.dumps({"task_id": "task-1", "branch": "trusted/x"})),
            )
            conn.commit()
            from coordinate.db import upsert_task_mirror as _upm
            _upm(conn, workspace_id="demo", task_id="task-1",
                 phase="running", owner="mac-claude", branch="trusted/x",
                 pr=None, payload={"repo": "acme/repo"},
                 last_event_id="evt-seed")
            conn.close()

            gh_calls = {"ref": 0, "list": 0, "create": 0}

            def fail_if_gh_called(cmd):
                joined = " ".join(cmd)
                if "git/ref/heads" in joined:
                    gh_calls["ref"] += 1
                if "pr" in joined and "list" in joined:
                    gh_calls["list"] += 1
                if "pr" in joined and "create" in joined:
                    gh_calls["create"] += 1
                # Force-fail the very first gh call to detect leaks.
                raise AssertionError(
                    f"gh was called after preflight failed: {cmd}"
                )

            # Simulate the remote preflight returning ok=false WITHOUT
            # actually invoking `/usr/local/bin/coord-ssh`. We patch
            # _forward_publish_preflight directly.
            original_forward = cli_module._forward_publish_preflight

            def fake_forward(event_cli_path, **kwargs):
                return {
                    "ok": False,
                    "reason": "mirror_conflict",
                    "message": "mirror branch 'trusted/x' != worker branch",
                    "mirror_branch": "trusted/x",
                }

            cli_module._forward_publish_preflight = fake_forward
            original_runner = prs_module.github_module._run_gh
            prs_module.github_module._run_gh = fail_if_gh_called
            try:
                code, stdout, stderr = self._run_cli(db_path, [
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "acme/repo",
                    "--branch", "agents/mac-claude/task-1",
                    "--head-owner", "acme",
                    "--base", "main",
                    "--title", "Fix", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567",
                    "--pushed", "true",
                    "--preflight-event-cli-path", "/usr/local/bin/coord-ssh",
                ])
            finally:
                cli_module._forward_publish_preflight = original_forward
                prs_module.github_module._run_gh = original_runner

            # Exit code is non-zero (preflight failure surfaces as
            # a publish.blocked / mirror_conflict envelope).
            self.assertEqual(code, 1, msg=f"stdout={stdout!r} stderr={stderr!r}")
            envelope = _json.loads(stdout)
            self.assertEqual(envelope["error"]["reason"], "mirror_conflict")
            # Critically: gh was NOT called.
            self.assertEqual(gh_calls["ref"], 0)
            self.assertEqual(gh_calls["list"], 0)
            self.assertEqual(gh_calls["create"], 0)

    def test_preflight_pr_already_linked_skips_gh(self):
        import json as _json
        import subprocess

        from coordinate import cli as cli_module
        from coordinate import prs as prs_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "c.sqlite3")
            self._setup_db(db_path, tmp)

            # Seed a mirror with an existing PR.
            conn = self._connect(db_path)
            self._migrate(conn)
            conn.execute(
                "INSERT INTO events (id, workspace_id, task_id, event_type, "
                "actor, target, idempotency_key, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                ("evt-seed", "demo", "task-1", "branch.allocated", "op",
                 "acme/repo", "demo:branch:task-1:agents/mac-claude/task-1",
                 _json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
            )
            conn.commit()
            from coordinate.db import upsert_task_mirror as _upm
            _upm(conn, workspace_id="demo", task_id="task-1",
                 phase="running", owner="mac-claude",
                 branch="agents/mac-claude/task-1",
                 pr="https://github.com/acme/repo/pull/5",
                 payload={"repo": "acme/repo"},
                 last_event_id="evt-seed")
            conn.close()

            gh_calls = {"list": 0, "create": 0}

            def fake_gh(cmd):
                joined = " ".join(cmd)
                if "pr" in joined and "list" in joined:
                    gh_calls["list"] += 1
                    return _ok(json.dumps([{
                        "number": 5,
                        "url": "https://github.com/acme/repo/pull/5",
                        "headRefOid": "0123456789abcdef0123456789abcdef01234567",
                        "baseRefName": "main",
                        "title": "existing",
                        "state": "OPEN",
                    }]))
                if "pr" in joined and "create" in joined:
                    gh_calls["create"] += 1
                    raise AssertionError(
                        f"gh pr create must not be called when linking existing PR: {cmd}"
                    )
                raise AssertionError(f"unexpected gh call: {cmd}")

            original_forward = cli_module._forward_publish_preflight

            def fake_forward(event_cli_path, **kwargs):
                return {
                    "ok": True,
                    "reason": None,
                    "message": "task task-1 already has pr; host must verify it read-only and link",
                    "mode": "link_existing",
                    "expected_pr_url": "https://github.com/acme/repo/pull/5",
                }

            cli_module._forward_publish_preflight = fake_forward
            original_runner = prs_module.github_module._run_gh
            prs_module.github_module._run_gh = fake_gh
            try:
                code, stdout, stderr = self._run_cli(db_path, [
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "acme/repo",
                    "--branch", "agents/mac-claude/task-1",
                    "--head-owner", "acme",
                    "--base", "main",
                    "--title", "Fix", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567",
                    "--pushed", "true",
                    "--preflight-event-cli-path", "/usr/local/bin/coord-ssh",
                ])
            finally:
                cli_module._forward_publish_preflight = original_forward
                prs_module.github_module._run_gh = original_runner

            self.assertEqual(code, 0, msg=f"stdout={stdout!r} stderr={stderr!r}")
            envelope = _json.loads(stdout)
            self.assertEqual(envelope["result"]["action"], "linked")
            self.assertEqual(envelope["result"]["pr_url"], "https://github.com/acme/repo/pull/5")
            # Only a read-only list call; no create.
            self.assertEqual(gh_calls["list"], 1)
            self.assertEqual(gh_calls["create"], 0)

    def test_preflight_pr_already_linked_url_mismatch_blocks(self):
        import json as _json
        import subprocess

        from coordinate import cli as cli_module
        from coordinate import prs as prs_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "c.sqlite3")
            self._setup_db(db_path, tmp)

            # Seed a mirror with an existing PR.
            conn = self._connect(db_path)
            self._migrate(conn)
            conn.execute(
                "INSERT INTO events (id, workspace_id, task_id, event_type, "
                "actor, target, idempotency_key, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                ("evt-seed", "demo", "task-1", "branch.allocated", "op",
                 "acme/repo", "demo:branch:task-1:agents/mac-claude/task-1",
                 _json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
            )
            conn.commit()
            from coordinate.db import upsert_task_mirror as _upm
            _upm(conn, workspace_id="demo", task_id="task-1",
                 phase="running", owner="mac-claude",
                 branch="agents/mac-claude/task-1",
                 pr="https://github.com/acme/repo/pull/5",
                 payload={"repo": "acme/repo"},
                 last_event_id="evt-seed")
            conn.close()

            gh_calls = {"ref": 0, "list": 0, "create": 0}

            def fail_if_gh_called(cmd):
                joined = " ".join(cmd)
                if "git/ref/heads" in joined:
                    gh_calls["ref"] += 1
                if "pr" in joined and "list" in joined:
                    gh_calls["list"] += 1
                if "pr" in joined and "create" in joined:
                    gh_calls["create"] += 1
                raise AssertionError(
                    f"gh was called after preflight reported existing PR: {cmd}"
                )

            original_forward = cli_module._forward_publish_preflight

            def fake_forward(event_cli_path, **kwargs):
                return {
                    "ok": False,
                    "reason": "pr_already_linked",
                    "message": "task task-1 already has pr 'https://github.com/acme/repo/pull/5'",
                    "pr_url": "https://github.com/acme/repo/pull/5",
                }

            cli_module._forward_publish_preflight = fake_forward
            original_runner = prs_module.github_module._run_gh
            prs_module.github_module._run_gh = fail_if_gh_called
            try:
                code, stdout, stderr = self._run_cli(db_path, [
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "acme/repo",
                    "--branch", "agents/mac-claude/task-1",
                    "--head-owner", "acme",
                    "--base", "main",
                    "--title", "Fix", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567",
                    "--pushed", "true",
                    "--preflight-event-cli-path", "/usr/local/bin/coord-ssh",
                ])
            finally:
                cli_module._forward_publish_preflight = original_forward
                prs_module.github_module._run_gh = original_runner

            self.assertEqual(code, 1, msg=f"stdout={stdout!r} stderr={stderr!r}")
            envelope = _json.loads(stdout)
            self.assertEqual(envelope["error"]["reason"], "pr_already_linked")
            self.assertEqual(envelope["preflight"]["pr_url"],
                             "https://github.com/acme/repo/pull/5")
            # No GitHub write happened.
            self.assertEqual(gh_calls["ref"], 0)
            self.assertEqual(gh_calls["list"], 0)
            self.assertEqual(gh_calls["create"], 0)


# ---------------------------------------------------------------------------
# Phase 8.4 review-fix round 5 — TOCTOU + PR rebind + audit round-trip
# ---------------------------------------------------------------------------


    def test_preflight_pr_already_linked_url_mismatch_blocks(self):
        import json as _json

        from coordinate import cli as cli_module
        from coordinate import prs as prs_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "c.sqlite3")
            self._setup_db(db_path, tmp)

            conn = self._connect(db_path)
            self._migrate(conn)
            conn.execute(
                "INSERT INTO events (id, workspace_id, task_id, event_type, "
                "actor, target, idempotency_key, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                ("evt-seed", "demo", "task-1", "branch.allocated", "op",
                 "acme/repo", "demo:branch:task-1:agents/mac-claude/task-1",
                 _json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
            )
            conn.commit()
            from coordinate.db import upsert_task_mirror as _upm
            _upm(conn, workspace_id="demo", task_id="task-1",
                 phase="running", owner="mac-claude",
                 branch="agents/mac-claude/task-1",
                 pr="https://github.com/acme/repo/pull/5",
                 payload={"repo": "acme/repo"},
                 last_event_id="evt-seed")
            conn.close()

            def fake_gh(cmd):
                joined = " ".join(cmd)
                if "pr" in joined and "list" in joined:
                    return _ok(_json.dumps([{
                        "number": 6,
                        "url": "https://github.com/acme/repo/pull/6",
                        "headRefOid": "0123456789abcdef0123456789abcdef01234567",
                        "baseRefName": "main",
                        "title": "different",
                        "state": "OPEN",
                    }]))
                raise AssertionError(f"unexpected gh call: {cmd}")

            original_forward = cli_module._forward_publish_preflight

            def fake_forward(event_cli_path, **kwargs):
                return {
                    "ok": True,
                    "reason": None,
                    "message": "task task-1 already has pr; host must verify it read-only and link",
                    "mode": "link_existing",
                    "expected_pr_url": "https://github.com/acme/repo/pull/5",
                }

            cli_module._forward_publish_preflight = fake_forward
            original_runner = prs_module.github_module._run_gh
            prs_module.github_module._run_gh = fake_gh
            try:
                code, stdout, stderr = self._run_cli(db_path, [
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "acme/repo",
                    "--branch", "agents/mac-claude/task-1",
                    "--head-owner", "acme",
                    "--base", "main",
                    "--title", "Fix", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567",
                    "--pushed", "true",
                    "--preflight-event-cli-path", "/usr/local/bin/coord-ssh",
                ])
            finally:
                cli_module._forward_publish_preflight = original_forward
                prs_module.github_module._run_gh = original_runner

            self.assertEqual(code, 1, msg=f"stdout={stdout!r} stderr={stderr!r}")
            envelope = _json.loads(stdout)
            self.assertEqual(envelope["result"]["action"], "blocked")
            self.assertEqual(envelope["result"]["reason"], "discover_url_mismatch")


class TestPublishPrRebindProtection(unittest.TestCase):
    """A task that already has a PR must not be silently relinked to a
    different PR through `publish_pr`.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _seed_mirror_with_pr(self, pr_url):
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-seed", "ws-1", "task-1", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-1:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-1", "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        upsert_task_mirror(
            self.conn, workspace_id="ws-1", task_id="task-1",
            phase="running", owner="mac-claude",
            branch="agents/mac-claude/task-1",
            pr=pr_url, payload={"repo": "acme/repo"},
            last_event_id="evt-seed",
        )

    def test_existing_pr_different_pr_blocks(self):
        self._seed_mirror_with_pr("https://github.com/acme/repo/pull/1")
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 2,
                    "url": "https://github.com/acme/repo/pull/2",
                    "headRefOid": SHA,
                    "baseRefName": "main",
                    "title": "different",
                    "state": "OPEN",
                }])),
            ),
        ])
        result = publish_pr(
            self.conn, run=runner,
            workspace_id="ws-1", task_id="task-1", repo="acme/repo",
            branch="agents/mac-claude/task-1", head_owner="acme", base="main",
            title="Fix", body="", commit=SHA, pushed=True,
        )
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "pr_already_linked")
        self.assertIn("already has pr", result.event["payload"]["message"])
        # Mirror PR must remain unchanged.
        row = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], "https://github.com/acme/repo/pull/1")

    def test_existing_pr_same_pr_links(self):
        pr_url = "https://github.com/acme/repo/pull/1"
        self._seed_mirror_with_pr(pr_url)
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 1, "url": pr_url, "headRefOid": SHA,
                    "baseRefName": "main", "title": "same", "state": "OPEN",
                }])),
            ),
        ])
        result = publish_pr(
            self.conn, run=runner,
            workspace_id="ws-1", task_id="task-1", repo="acme/repo",
            branch="agents/mac-claude/task-1", head_owner="acme", base="main",
            title="Fix", body="", commit=SHA, pushed=True,
        )
        self.assertEqual(result.action, "linked")
        self.assertEqual(result.pr_url, pr_url)

    def test_existing_pr_create_different_pr_blocks(self):
        """If the mirror already has /pull/1, a new `gh pr create` returning
        /pull/2 must be rejected before the mirror is overwritten.
        """
        self._seed_mirror_with_pr("https://github.com/acme/repo/pull/1")
        runner = _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([])),
            ),
            dict(
                match=_match_argv("gh", "pr", "create"),
                make=lambda _cmd: _ok(
                    stdout="https://github.com/acme/repo/pull/2\n",
                ),
            ),
        ])
        result = publish_pr(
            self.conn, run=runner,
            workspace_id="ws-1", task_id="task-1", repo="acme/repo",
            branch="agents/mac-claude/task-1", head_owner="acme", base="main",
            title="Fix", body="", commit=SHA, pushed=True,
        )
        self.assertEqual(result.action, "blocked")
        self.assertEqual(result.event["payload"]["reason"], "pr_already_linked")
        row = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], "https://github.com/acme/repo/pull/1")


class TestRecordPublishSinkRebindAndAudit(unittest.TestCase):
    """`record_publish_result` must reject same-task PR rebind and must
    preserve remote/validation audit fields on created/linked events.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _envelope(self, action="created", **over):
        envelope = {
            "workspace_id": "ws-1",
            "task_id": "task-1",
            "repo": "acme/repo",
            "branch": "agents/mac-claude/task-1",
            "head_ref": "acme:agents/mac-claude/task-1",
            "base": "main",
            "commit": SHA,
            "reported_commit": SHA,
            "remote_sha": SHA,
            "pr_url": "https://github.com/acme/repo/pull/99",
            "action": action,
            "remote": "upstream",
            "validation": "314 tests OK",
        }
        envelope.update(over)
        return envelope

    def test_created_event_preserves_remote_and_validation(self):
        result = record_publish_result(
            self.conn, workspace_id="ws-1", result=self._envelope("created"),
        )
        self.assertEqual(result.action, "created")
        payload = result.event["payload"]
        self.assertEqual(payload["remote"], "upstream")
        self.assertEqual(payload["validation"], "314 tests OK")

    def test_linked_event_preserves_remote_and_validation(self):
        result = record_publish_result(
            self.conn, workspace_id="ws-1", result=self._envelope("linked"),
        )
        self.assertEqual(result.action, "linked")
        payload = result.event["payload"]
        self.assertEqual(payload["remote"], "upstream")
        self.assertEqual(payload["validation"], "314 tests OK")

    def test_remote_sink_rejects_same_task_pr_rebind(self):
        # First record /pull/99.
        record_publish_result(
            self.conn, workspace_id="ws-1", result=self._envelope("created"),
        )
        # Then try to record /pull/100 for the same task.
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn, workspace_id="ws-1",
                result=self._envelope(
                    "linked", pr_url="https://github.com/acme/repo/pull/100",
                ),
            )
        self.assertEqual(ctx.exception.reason, "pr_already_linked")
        # Mirror must still point at the original PR.
        row = self.conn.execute(
            "SELECT pr FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], "https://github.com/acme/repo/pull/99")

    def test_linked_same_pr_advances_verified_commit(self):
        record_publish_result(
            self.conn,
            workspace_id="ws-1",
            result=self._envelope("created"),
        )
        next_sha = "f" * 40

        result = record_publish_result(
            self.conn,
            workspace_id="ws-1",
            result=self._envelope(
                "linked",
                commit=next_sha,
                reported_commit=next_sha,
                remote_sha=next_sha,
            ),
        )

        self.assertTrue(result.event_created)
        self.assertTrue(result.mirror_updated)
        row = self.conn.execute(
            "SELECT pr, payload_json FROM tasks WHERE workspace_id=? AND task_id=?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(row["pr"], "https://github.com/acme/repo/pull/99")
        metadata = json.loads(row["payload_json"])["publish_metadata"]
        self.assertEqual(metadata["reported_commit"], next_sha)
        self.assertEqual(metadata["remote_sha"], next_sha)
        replay = record_publish_result(
            self.conn,
            workspace_id="ws-1",
            result=self._envelope(
                "linked",
                commit=next_sha,
                reported_commit=next_sha,
                remote_sha=next_sha,
            ),
        )
        self.assertFalse(replay.event_created)
        self.assertFalse(replay.mirror_updated)

    def test_unique_index_catches_cross_task_branch_conflict(self):
        """The application-level `_cross_task_conflict_check` should catch
        this, but if it is bypassed or races, the partial unique index is
        the final guard.
        """
        from coordinate.db import upsert_task_mirror as _upm
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-other", "ws-1", "task-other", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-other:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-other",
                         "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        _upm(
            self.conn, workspace_id="ws-1", task_id="task-other",
            phase="running", owner="x", branch="agents/mac-claude/task-1",
            pr=None, payload={},
            last_event_id="evt-other",
        )
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn, workspace_id="ws-1", result=self._envelope("created"),
            )
        self.assertEqual(ctx.exception.reason, "cross_task_conflict")
        self.assertIn("branch", str(ctx.exception).lower())

    def test_unique_index_catches_cross_task_pr_conflict(self):
        """Same as above but for PR URL."""
        from coordinate.db import upsert_task_mirror as _upm
        pr_url = "https://github.com/acme/repo/pull/99"
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-other", "ws-1", "task-other", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-other:other/branch",
             json.dumps({"task_id": "task-other", "branch": "other/branch"})),
        )
        self.conn.commit()
        _upm(
            self.conn, workspace_id="ws-1", task_id="task-other",
            phase="running", owner="x", branch="other/branch",
            pr=pr_url, payload={},
            last_event_id="evt-other",
        )
        with self.assertRaises(RecordPublishError) as ctx:
            record_publish_result(
                self.conn, workspace_id="ws-1", result=self._envelope("created"),
            )
        self.assertEqual(ctx.exception.reason, "cross_task_conflict")
        self.assertIn("pr", str(ctx.exception).lower())

    def test_unique_index_branch_conflict_converts_integrity_error(self):
        """Bypass the application-level check and force SQLite's unique
        index to fire. Verifies the raw IntegrityError message
        ``UNIQUE constraint failed: tasks.workspace_id, tasks.branch`` is
        converted to ``RecordPublishError(reason='cross_task_conflict')``.
        """
        from coordinate.db import upsert_task_mirror as _upm
        import coordinate.pr_contracts as contracts_module
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-other", "ws-1", "task-other", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-other:agents/mac-claude/task-1",
             json.dumps({"task_id": "task-other",
                         "branch": "agents/mac-claude/task-1"})),
        )
        self.conn.commit()
        _upm(
            self.conn, workspace_id="ws-1", task_id="task-other",
            phase="running", owner="x", branch="agents/mac-claude/task-1",
            pr=None, payload={},
            last_event_id="evt-other",
        )
        with mock.patch.object(
            contracts_module, "check_cross_task_conflict", lambda *a, **k: None
        ):
            with self.assertRaises(RecordPublishError) as ctx:
                record_publish_result(
                    self.conn, workspace_id="ws-1",
                    result=self._envelope("created"),
                )
        self.assertEqual(ctx.exception.reason, "cross_task_conflict")
        self.assertIn("branch", str(ctx.exception).lower())

    def test_unique_index_pr_conflict_converts_integrity_error(self):
        """Same as above but for the (workspace_id, pr) unique index."""
        from coordinate.db import upsert_task_mirror as _upm
        import coordinate.pr_contracts as contracts_module
        pr_url = "https://github.com/acme/repo/pull/99"
        self.conn.execute(
            "INSERT INTO events (id, workspace_id, task_id, event_type, "
            "actor, target, idempotency_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            ("evt-other", "ws-1", "task-other", "branch.allocated", "op",
             "acme/repo", "ws-1:branch:task-other:other/branch",
             json.dumps({"task_id": "task-other", "branch": "other/branch"})),
        )
        self.conn.commit()
        _upm(
            self.conn, workspace_id="ws-1", task_id="task-other",
            phase="running", owner="x", branch="other/branch",
            pr=pr_url, payload={},
            last_event_id="evt-other",
        )
        with mock.patch.object(
            contracts_module, "check_cross_task_conflict", lambda *a, **k: None
        ):
            with self.assertRaises(RecordPublishError) as ctx:
                record_publish_result(
                    self.conn, workspace_id="ws-1",
                    result=self._envelope("created"),
                )
        self.assertEqual(ctx.exception.reason, "cross_task_conflict")
        self.assertIn("pr", str(ctx.exception).lower())


class TestPublishResultAuditRoundTrip(unittest.TestCase):
    """`PublishResult.to_dict()` must carry remote/validation on the
    created/linked success paths so the remote sink preserves them.
    """

    def setUp(self):
        self.conn = _setup_db()

    def tearDown(self):
        self.conn.close()

    def _make_runner(self, action, pr_url):
        if action == "created":
            return _RunnerScript([
                dict(
                    match=_match_argv("repos/acme/repo/git/ref/heads"),
                    make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
                ),
                dict(
                    match=_match_argv("gh", "pr", "list"),
                    make=lambda _cmd: _ok(json.dumps([])),
                ),
                dict(
                    match=_match_argv("gh", "pr", "create"),
                    make=lambda _cmd: _ok(stdout=f"{pr_url}\n"),
                ),
            ])
        return _RunnerScript([
            dict(
                match=_match_argv("repos/acme/repo/git/ref/heads"),
                make=lambda _cmd: _ok(json.dumps({"object": {"sha": SHA}})),
            ),
            dict(
                match=_match_argv("gh", "pr", "list"),
                make=lambda _cmd: _ok(json.dumps([{
                    "number": 1, "url": pr_url, "headRefOid": SHA,
                    "baseRefName": "main", "title": "x", "state": "OPEN",
                }])),
            ),
        ])

    def test_created_to_dict_includes_remote_and_validation(self):
        pr_url = "https://github.com/acme/repo/pull/10"
        result = publish_pr(
            self.conn,
            run=self._make_runner("created", pr_url),
            workspace_id="ws-1", task_id="task-1", repo="acme/repo",
            branch="agents/mac-claude/task-1", head_owner="acme", base="main",
            title="Fix", body="", commit=SHA, pushed=True,
            remote="upstream", validation="314 tests OK",
        )
        self.assertEqual(result.action, "created")
        d = result.to_dict()
        self.assertEqual(d["remote"], "upstream")
        self.assertEqual(d["validation"], "314 tests OK")
        # And the local event payload also carries them.
        self.assertEqual(result.event["payload"]["remote"], "upstream")
        self.assertEqual(result.event["payload"]["validation"], "314 tests OK")

    def test_linked_to_dict_includes_remote_and_validation(self):
        pr_url = "https://github.com/acme/repo/pull/11"
        result = publish_pr(
            self.conn,
            run=self._make_runner("linked", pr_url),
            workspace_id="ws-1", task_id="task-1", repo="acme/repo",
            branch="agents/mac-claude/task-1", head_owner="acme", base="main",
            title="Fix", body="", commit=SHA, pushed=True,
            remote="origin", validation="805 tests OK",
        )
        self.assertEqual(result.action, "linked")
        d = result.to_dict()
        self.assertEqual(d["remote"], "origin")
        self.assertEqual(d["validation"], "805 tests OK")
        self.assertEqual(result.event["payload"]["remote"], "origin")
        self.assertEqual(result.event["payload"]["validation"], "805 tests OK")


if __name__ == "__main__":
    unittest.main()
