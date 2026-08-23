"""Tests for the P9-1 ExecutionContext v1 authority and contract."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinate.db import (
    get_workspace_host_profile,
    initialize,
    row_to_dict,
    upsert_runner_profile,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.execution_context import (
    CONTRACT_VERSION,
    ContextError,
    ExecutionContextV1,
    _compute_context_id,
    _host_separator,
    _join_host_path,
    _map_foreign_path,
    _parse_origin_scope,
    context_matches_origin,
    execution_context_dict_matches,
    resolve_execution_context_v1,
    validate_execution_context_snapshot,
)
from coordinate.execution_resources import build_worktree_resource, compute_resource_key
from coordinate.runtime import RuntimeError, claim_job, register_agent, submit_request


class ExecutionContextValueTests(unittest.TestCase):
    def test_serialize_and_digest_are_deterministic(self) -> None:
        ctx = ExecutionContextV1(
            context_id="sha256:placeholder",
            job_id="request:e1",
            workspace_id="ws",
            task_id="t1",
            assigned_agent="agent",
            host_id="host",
            workspace_path="/ws",
            worktree_path="/ws",
            harness_root="/ws/harness",
            branch="main",
            session_scope_id="task:ws:t1",
            legacy_scope_ids=("legacy:a",),
            log_handle={"kind": "coordinate_job", "job_id": "request:e1", "logs_path": None},
        )
        expected_id = _compute_context_id(ctx.canonical_snapshot_dict())
        ctx2 = ExecutionContextV1(
            context_id=expected_id,
            job_id=ctx.job_id,
            workspace_id=ctx.workspace_id,
            task_id=ctx.task_id,
            assigned_agent=ctx.assigned_agent,
            host_id=ctx.host_id,
            workspace_path=ctx.workspace_path,
            worktree_path=ctx.worktree_path,
            harness_root=ctx.harness_root,
            branch=ctx.branch,
            session_scope_id=ctx.session_scope_id,
            legacy_scope_ids=ctx.legacy_scope_ids,
            log_handle=ctx.log_handle,
        )
        self.assertEqual(ctx2.context_id, expected_id)
        self.assertEqual(ctx2.to_dict()["context_id"], expected_id)

    def test_digest_changes_when_semantic_field_changes(self) -> None:
        base = ExecutionContextV1(
            context_id="sha256:x",
            job_id="request:e1",
            workspace_id="ws",
            task_id=None,
            assigned_agent="agent",
            host_id="host",
            workspace_path="/ws",
            worktree_path="/ws",
            harness_root="/ws/harness",
            branch=None,
            session_scope_id="scope:1",
            legacy_scope_ids=(),
            log_handle={"kind": "coordinate_job", "job_id": "request:e1", "logs_path": None},
        )
        id1 = _compute_context_id(base.canonical_snapshot_dict())
        changed = ExecutionContextV1(
            context_id="sha256:x",
            job_id=base.job_id,
            workspace_id=base.workspace_id,
            task_id=base.task_id,
            assigned_agent=base.assigned_agent,
            host_id=base.host_id,
            workspace_path="/ws2",
            worktree_path=base.worktree_path,
            harness_root=base.harness_root,
            branch=base.branch,
            session_scope_id=base.session_scope_id,
            legacy_scope_ids=base.legacy_scope_ids,
            log_handle=base.log_handle,
        )
        id2 = _compute_context_id(changed.canonical_snapshot_dict())
        self.assertNotEqual(id1, id2)


class ScopeParsingTests(unittest.TestCase):
    def test_task_scope_canonicalization_ignores_bridge_scope(self) -> None:
        scope, legacy = _parse_origin_scope(
            {"session_scope_id": "bridge:scope", "legacy_scope_ids": ["legacy:a"]},
            workspace_id="ws",
            task_id="t1",
        )
        self.assertEqual(scope, "task:ws:t1")
        self.assertEqual(legacy, ("legacy:a",))

    def test_non_task_scope_required_and_bounded(self) -> None:
        with self.assertRaisesRegex(ContextError, "session_scope_id is required"):
            _parse_origin_scope({}, workspace_id="ws", task_id=None)
        with self.assertRaisesRegex(ContextError, "unsafe characters"):
            _parse_origin_scope(
                {"session_scope_id": "scope with spaces"},
                workspace_id="ws",
                task_id=None,
            )

    def test_legacy_scope_dedup_and_bounds(self) -> None:
        scope, legacy = _parse_origin_scope(
            {
                "session_scope_id": "scope:1",
                "legacy_scope_ids": ["a", "a", "b", "scope:1"],
            },
            workspace_id="ws",
            task_id=None,
        )
        self.assertEqual(scope, "scope:1")
        self.assertEqual(legacy, ("a", "b"))

    def test_legacy_scope_too_many_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "exceeds"):
            _parse_origin_scope(
                {"session_scope_id": "scope:1", "legacy_scope_ids": ["s"] * 11},
                workspace_id="ws",
                task_id=None,
            )


class ForeignPathMappingTests(unittest.TestCase):
    def test_posix_absolute_rebase(self) -> None:
        self.assertEqual(
            _map_foreign_path("/Users/ws", "/host/ws", "/Users/ws/src/foo.py"),
            "/host/ws/src/foo.py",
        )

    def test_relative_path_is_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "path must be absolute"):
            _map_foreign_path("/Users/ws", "/host/ws", "src/foo.py")

    def test_windows_host_separator(self) -> None:
        self.assertEqual(
            _map_foreign_path("/Users/ws", "C:\\Users\\ws", "/Users/ws/src/foo.py"),
            "C:\\Users\\ws\\src\\foo.py",
        )

    def test_spaces_and_backslashes_quoted_by_host(self) -> None:
        host = "C:\\Users\\My User"
        self.assertEqual(
            _map_foreign_path("/Users/ws", host, "/Users/ws/src with spaces"),
            "C:\\Users\\My User\\src with spaces",
        )

    def test_host_separator_inference(self) -> None:
        self.assertEqual(_host_separator("/posix/path"), "/")
        self.assertEqual(_host_separator("C:\\windows\\path"), "\\")

    def test_no_local_pathlib_resolve_on_foreign_root(self) -> None:
        # The hard reviewer note: foreign host roots are mapped with pure
        # string/segment joining, never through local pathlib resolution.
        result = _map_foreign_path(
            "/Users/ws", "C:\\not\\a\\real\\windows", "/Users/ws/src"
        )
        self.assertEqual(result, "C:\\not\\a\\real\\windows\\src")


class ExecutionContextResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root="harness",
            base_branch="main",
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        register_agent(
            self.conn,
            agent_id="agent1",
            host_id="host1",
            capabilities={},
        )

    def _task(self, task_id: str, branch: str | None = None) -> dict:
        from coordinate.db import upsert_task_mirror

        row, _ = upsert_task_mirror(
            self.conn,
            workspace_id="ws",
            task_id=task_id,
            phase="open",
            owner="owner",
            branch=branch,
            pr=None,
            payload={},
        )
        return row_to_dict(row)

    def test_task_scope_overrides_origin(self) -> None:
        from coordinate.db import Workspace, WorkspaceHostProfile, get_workspace, get_workspace_host_profile

        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(self.conn, workspace_id="ws", host_id="host1")
        ctx = resolve_execution_context_v1(
            job_id="request:e1",
            workspace=workspace,
            task=self._task("t1"),
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "discord:ch", "legacy_scope_ids": ["legacy:a"]},
        )
        self.assertEqual(ctx.session_scope_id, "task:ws:t1")
        self.assertEqual(ctx.workspace_path, "/host/ws")
        self.assertEqual(ctx.harness_root, "/host/harness")

    def test_branch_precedence(self) -> None:
        from coordinate.db import Workspace, WorkspaceHostProfile, get_workspace, get_workspace_host_profile

        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(self.conn, workspace_id="ws", host_id="host1")
        # task branch wins over workspace base branch
        ctx = resolve_execution_context_v1(
            job_id="request:e1",
            workspace=workspace,
            task=self._task("t1", branch="feature/t1"),
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "discord:ch"},
        )
        self.assertEqual(ctx.branch, "feature/t1")
        # explicit job branch wins over task branch
        ctx2 = resolve_execution_context_v1(
            job_id="request:e1",
            workspace=workspace,
            task=self._task("t1", branch="feature/t1"),
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "discord:ch"},
            job_branch="job/branch",
        )
        self.assertEqual(ctx2.branch, "job/branch")

    def test_explicit_job_worktree_maps_control_absolute_path(self) -> None:
        from coordinate.db import get_workspace, get_workspace_host_profile

        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(self.conn, workspace_id="ws", host_id="host1")
        ctx = resolve_execution_context_v1(
            job_id="request:e1",
            workspace=workspace,
            task=None,
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "discord:ch"},
        )
        self.assertEqual(ctx.worktree_path, "/host/ws")
        ctx2 = resolve_execution_context_v1(
            job_id="request:e1",
            workspace=workspace,
            task=None,
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "discord:ch"},
            job_worktree_path=f"{workspace.path}/feature",
        )
        self.assertEqual(ctx2.worktree_path, "/host/ws/feature")

    def test_windows_worktree_path_is_canonical_before_context_freeze(self) -> None:
        from coordinate.db import get_workspace, get_workspace_host_profile

        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="C:\\Users\\ADMIN\\projects\\multinexus",
            harness_root="C:\\Users\\ADMIN\\projects\\multinexus\\docs\\project-harness",
        )
        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(
            self.conn, workspace_id="ws", host_id="host1"
        )
        ctx = resolve_execution_context_v1(
            job_id="request:windows-path",
            workspace=workspace,
            task=None,
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "local:windows-path"},
        )

        self.assertEqual(
            ctx.worktree_path,
            "c:\\users\\admin\\projects\\multinexus",
        )
        self.assertEqual(
            build_worktree_resource(ctx.host_id, ctx.worktree_path).normalized_path,
            ctx.worktree_path,
        )

    def test_explicit_job_worktree_rejects_relative_path(self) -> None:
        from coordinate.db import get_workspace, get_workspace_host_profile

        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(self.conn, workspace_id="ws", host_id="host1")
        with self.assertRaisesRegex(ContextError, "path must be absolute"):
            resolve_execution_context_v1(
                job_id="request:e1",
                workspace=workspace,
                task=None,
                assigned_agent="agent1",
                host_id="host1",
                profile=profile,
                origin={"session_scope_id": "discord:ch"},
                job_worktree_path="feature",
            )

    def test_missing_profile_raises_context_error(self) -> None:
        from coordinate.db import get_workspace

        workspace = get_workspace(self.conn, "ws")
        with self.assertRaisesRegex(ContextError, "workspace_path is required"):
            resolve_execution_context_v1(
                job_id="request:e1",
                workspace=workspace,
                task=None,
                assigned_agent="agent1",
                host_id="host1",
                profile=type("P", (), {"workspace_path": "", "harness_root": None})(),
                origin={"session_scope_id": "discord:ch"},
            )

    def test_relative_profile_workspace_path_rejected(self) -> None:
        from coordinate.db import Workspace, WorkspaceHostProfile, get_workspace, get_workspace_host_profile

        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(self.conn, workspace_id="ws", host_id="host1")
        unsafe_profile = WorkspaceHostProfile(
            workspace_id=profile.workspace_id,
            host_id=profile.host_id,
            workspace_path="relative/ws",
            harness_root=profile.harness_root,
        )
        with self.assertRaisesRegex(ContextError, "workspace_path must be absolute"):
            resolve_execution_context_v1(
                job_id="request:e1",
                workspace=workspace,
                task=None,
                assigned_agent="agent1",
                host_id="host1",
                profile=unsafe_profile,
                origin={"session_scope_id": "discord:ch"},
            )

    def test_relative_profile_harness_root_rejected(self) -> None:
        from coordinate.db import Workspace, WorkspaceHostProfile, get_workspace, get_workspace_host_profile

        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(self.conn, workspace_id="ws", host_id="host1")
        unsafe_profile = WorkspaceHostProfile(
            workspace_id=profile.workspace_id,
            host_id=profile.host_id,
            workspace_path=profile.workspace_path,
            harness_root="relative/h",
        )
        with self.assertRaisesRegex(ContextError, "harness_root must be absolute"):
            resolve_execution_context_v1(
                job_id="request:e1",
                workspace=workspace,
                task=None,
                assigned_agent="agent1",
                host_id="host1",
                profile=unsafe_profile,
                origin={"session_scope_id": "discord:ch"},
            )


class RuntimeContextIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root="harness",
            base_branch="main",
        )
        self.workspace_path = str(Path(self.tmp.name).resolve())
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        register_agent(
            self.conn,
            agent_id="agent1",
            host_id="host1",
            capabilities={},
        )

    def test_submit_request_persists_context_and_claim_returns_it(self) -> None:
        origin = {"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"}
        reply = {"platform": "discord", "destination": "ch"}
        req = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="hello",
            origin=origin,
            reply=reply,
        )
        self.assertTrue(req.job_created)
        ctx = req.job["payload"]["execution_context"]
        self.assertEqual(ctx["contract_version"], CONTRACT_VERSION)
        self.assertTrue(ctx["context_id"].startswith("sha256:"))
        self.assertEqual(ctx["workspace_path"], "/host/ws")
        self.assertEqual(ctx["session_scope_id"], "discord:ch")

        claim = claim_job(self.conn, agent_id="agent1")
        self.assertTrue(claim.claimed)
        self.assertIsNotNone(claim.execution_context)
        self.assertEqual(
            claim.execution_context["context_id"],
            ctx["context_id"],
        )
        self.assertEqual(claim.execution_context["worktree_path"], "/host/ws")

    def test_exact_requests_freeze_distinct_per_request_worktrees(self) -> None:
        reply = {"platform": "discord", "destination": "ch"}
        results = []
        for suffix, agent in (("e1", "agent1"), ("e2", "agent2")):
            if agent == "agent2":
                register_agent(
                    self.conn, agent_id=agent, host_id="host1", capabilities={}
                )
            control_path = str(Path(self.workspace_path) / suffix)
            result = submit_request(
                self.conn,
                workspace_id="ws",
                target_agent=agent,
                prompt="quiet",
                origin={
                    "platform": "discord",
                    "destination": "ch",
                    "message_id": suffix,
                    "session_scope_id": f"discord:{suffix}",
                },
                reply=reply,
                worktree_path=control_path,
            )
            self.assertEqual(result.job["worktree_path"], control_path)
            self.assertEqual(
                result.event["payload"]["worktree_path"], control_path
            )
            self.assertEqual(
                result.job["payload"]["execution_context"]["worktree_path"],
                f"/host/ws/{suffix}",
            )
            results.append(result)

        keys = {
            compute_resource_key(
                build_worktree_resource(
                    "host1",
                    result.job["payload"]["execution_context"]["worktree_path"],
                )
            )
            for result in results
        }
        self.assertEqual(len(keys), 2)

    def test_worktree_replay_same_is_idempotent_and_different_conflicts(self) -> None:
        origin = {
            "platform": "discord",
            "destination": "ch",
            "message_id": "worktree-replay",
            "session_scope_id": "discord:worktree-replay",
        }
        reply = {"platform": "discord", "destination": "ch"}
        first_path = str(Path(self.workspace_path) / "e1")
        second_path = str(Path(self.workspace_path) / "e2")
        first = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="quiet",
            origin=origin,
            reply=reply,
            worktree_path=first_path,
        )
        before = tuple(
            self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("events", "jobs")
        )
        replay = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="quiet",
            origin=origin,
            reply=reply,
            worktree_path=first_path,
        )
        self.assertFalse(replay.event_created)
        self.assertFalse(replay.job_created)
        self.assertEqual(replay.job["id"], first.job["id"])
        self.assertEqual(
            before,
            tuple(
                self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("events", "jobs")
            ),
        )
        with self.assertRaisesRegex(RuntimeError, "worktree_path conflicts"):
            submit_request(
                self.conn,
                workspace_id="ws",
                target_agent="agent1",
                prompt="quiet",
                origin=origin,
                reply=reply,
                worktree_path=second_path,
            )
        self.assertEqual(
            before,
            tuple(
                self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("events", "jobs")
            ),
        )

    def test_invalid_worktree_paths_fail_before_durable_write(self) -> None:
        reply = {"platform": "discord", "destination": "ch"}
        invalid_paths = (
            "relative/e1",
            str(Path(self.tmp.name) / "e1" / ".." / "escape"),
            str(Path(self.tmp.name).parent / "outside"),
        )
        for index, worktree_path in enumerate(invalid_paths):
            with self.subTest(worktree_path=worktree_path):
                before = tuple(
                    self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                    for table in ("events", "jobs")
                )
                with self.assertRaisesRegex(RuntimeError, "invalid execution context"):
                    submit_request(
                        self.conn,
                        workspace_id="ws",
                        target_agent="agent1",
                        prompt="quiet",
                        origin={
                            "platform": "discord",
                            "destination": "ch",
                            "message_id": f"invalid-{index}",
                            "session_scope_id": f"discord:invalid-{index}",
                        },
                        reply=reply,
                        worktree_path=worktree_path,
                    )
                self.assertEqual(
                    before,
                    tuple(
                        self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                        for table in ("events", "jobs")
                    ),
                )

    def test_worktree_path_is_canonicalized_before_freeze(self) -> None:
        canonical = str(Path(self.workspace_path) / "e1")
        result = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="quiet",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "canonical-worktree",
                "session_scope_id": "discord:canonical-worktree",
            },
            reply={"platform": "discord", "destination": "ch"},
            worktree_path=canonical + "/",
        )
        self.assertEqual(result.event["payload"]["worktree_path"], canonical)
        self.assertEqual(result.job["worktree_path"], canonical)

    def test_symlink_escape_fails_before_durable_write(self) -> None:
        with tempfile.TemporaryDirectory() as outside:
            link = Path(self.workspace_path) / "link-outside"
            link.symlink_to(outside, target_is_directory=True)
            before = tuple(
                self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("events", "jobs")
            )
            with self.assertRaisesRegex(RuntimeError, "invalid execution context"):
                submit_request(
                    self.conn,
                    workspace_id="ws",
                    target_agent="agent1",
                    prompt="quiet",
                    origin={
                        "platform": "discord",
                        "destination": "ch",
                        "message_id": "symlink-escape",
                        "session_scope_id": "discord:symlink-escape",
                    },
                    reply={"platform": "discord", "destination": "ch"},
                    worktree_path=str(link / "e1"),
                )
            self.assertEqual(
                before,
                tuple(
                    self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                    for table in ("events", "jobs")
                ),
            )

    def test_routed_request_rejects_worktree_before_durable_write(self) -> None:
        before = tuple(
            self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("events", "jobs")
        )
        with self.assertRaisesRegex(RuntimeError, "does not support worktree_path"):
            submit_request(
                self.conn,
                workspace_id="ws",
                routing_request={"contract_version": 1},
                prompt="quiet",
                origin={
                    "platform": "discord",
                    "destination": "ch",
                    "message_id": "routed-worktree",
                    "session_scope_id": "discord:routed-worktree",
                },
                reply={"platform": "discord", "destination": "ch"},
                worktree_path=str(Path(self.tmp.name) / "e1"),
            )
        self.assertEqual(
            before,
            tuple(
                self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("events", "jobs")
            ),
        )

    def test_submit_request_rejects_conflicting_replay(self) -> None:
        origin = {"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"}
        reply = {"platform": "discord", "destination": "ch"}
        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="hello",
            origin=origin,
            reply=reply,
        )
        with self.assertRaisesRegex(RuntimeError, "execution_context conflicts"):
            submit_request(
                self.conn,
                workspace_id="ws",
                target_agent="agent1",
                prompt="hello",
                origin={**origin, "session_scope_id": "discord:other"},
                reply=reply,
            )

    def test_submit_request_missing_profile_fails_before_job_creation(self) -> None:
        register_agent(self.conn, agent_id="agent2", host_id="host2", capabilities={})
        with self.assertRaisesRegex(RuntimeError, "no host profile"):
            submit_request(
                self.conn,
                workspace_id="ws",
                target_agent="agent2",
                prompt="hello",
                origin={"platform": "discord", "destination": "ch", "message_id": "m2", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        # No job should have been created for the missing profile.
        jobs = self.conn.execute("SELECT * FROM jobs WHERE assigned_agent = ?", ("agent2",)).fetchall()
        self.assertEqual(len(jobs), 0)

    def test_claim_backfills_pre_upgrade_pending_job(self) -> None:
        # Manually create a pre-upgrade job without execution_context.
        from coordinate.db import create_job

        job = create_job(
            self.conn,
            workspace_id="ws",
            task_id=None,
            runner_profile_id="agent1",
            assigned_agent="agent1",
            payload={
                "prompt": "legacy",
                "origin": {"session_scope_id": "discord:legacy", "legacy_scope_ids": []},
                "reply": {"platform": "discord", "destination": "ch"},
            },
        )
        claim = claim_job(self.conn, agent_id="agent1")
        self.assertTrue(claim.claimed)
        self.assertEqual(claim.job["id"], job["id"])
        self.assertIsNotNone(claim.execution_context)
        self.assertEqual(claim.execution_context["session_scope_id"], "discord:legacy")
        # Snapshot persisted in job row.
        stored = claim.job["payload"]
        self.assertIn("execution_context", stored)
        self.assertEqual(
            stored["execution_context"]["context_id"],
            claim.execution_context["context_id"],
        )

    def test_claim_refuses_host_mismatch_without_mutation(self) -> None:
        origin = {"platform": "discord", "destination": "ch", "message_id": "m3", "session_scope_id": "discord:ch"}
        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="hello",
            origin=origin,
            reply={"platform": "discord", "destination": "ch"},
        )
        # Register the same agent id on a different host and re-register as online.
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host2",
            workspace_path="/host2/ws",
            harness_root="/host2/harness",
        )
        register_agent(self.conn, agent_id="agent1", host_id="host2", capabilities={})
        from coordinate.runtime import heartbeat_agent
        heartbeat_agent(self.conn, agent_id="agent1", host_id="host2")

        before = row_to_dict(self.conn.execute("SELECT * FROM jobs").fetchone())
        with self.assertRaisesRegex(RuntimeError, "host_id mismatch"):
            claim_job(self.conn, agent_id="agent1")
        after = row_to_dict(self.conn.execute("SELECT * FROM jobs").fetchone())
        self.assertEqual(after["status"], before["status"])
        self.assertEqual(after["attempt_count"], before["attempt_count"])


class SnapshotValidationTests(unittest.TestCase):
    def test_validate_snapshot_detects_digest_mismatch(self) -> None:
        snapshot = {
            "contract_version": 1,
            "context_id": "sha256:" + "0" * 64,
            "job_id": "request:e1",
            "workspace_id": "ws",
            "task_id": None,
            "assigned_agent": "a",
            "host_id": "h",
            "workspace_path": "/ws",
            "worktree_path": "/ws",
            "harness_root": "/ws/h",
            "branch": None,
            "session_scope_id": "scope:1",
            "legacy_scope_ids": [],
            "log_handle": {"kind": "coordinate_job", "job_id": "request:e1", "logs_path": None},
        }
        with self.assertRaisesRegex(ContextError, "digest mismatch"):
            validate_execution_context_snapshot(snapshot)

    def test_validate_snapshot_detects_identity_mismatch(self) -> None:
        ctx = ExecutionContextV1(
            context_id="sha256:x",
            job_id="request:e1",
            workspace_id="ws",
            task_id=None,
            assigned_agent="a",
            host_id="h",
            workspace_path="/ws",
            worktree_path="/ws",
            harness_root="/ws/h",
            branch=None,
            session_scope_id="scope:1",
            legacy_scope_ids=(),
            log_handle={"kind": "coordinate_job", "job_id": "request:e1", "logs_path": None},
        )
        ctx = ExecutionContextV1(
            context_id=_compute_context_id(ctx.canonical_snapshot_dict()),
            job_id=ctx.job_id,
            workspace_id=ctx.workspace_id,
            task_id=ctx.task_id,
            assigned_agent=ctx.assigned_agent,
            host_id=ctx.host_id,
            workspace_path=ctx.workspace_path,
            worktree_path=ctx.worktree_path,
            harness_root=ctx.harness_root,
            branch=ctx.branch,
            session_scope_id=ctx.session_scope_id,
            legacy_scope_ids=ctx.legacy_scope_ids,
            log_handle=ctx.log_handle,
        )
        with self.assertRaisesRegex(ContextError, "host_id mismatch"):
            validate_execution_context_snapshot(
                ctx.to_dict(),
                job_id=ctx.job_id,
                host_id="other-host",
            )

    def test_execution_context_dict_matches_ignores_context_id(self) -> None:
        ctx1 = ExecutionContextV1(
            context_id="sha256:x",
            job_id="request:e1",
            workspace_id="ws",
            task_id=None,
            assigned_agent="a",
            host_id="h",
            workspace_path="/ws",
            worktree_path="/ws",
            harness_root="/ws/h",
            branch=None,
            session_scope_id="scope:1",
            legacy_scope_ids=(),
            log_handle={"kind": "coordinate_job", "job_id": "request:e1", "logs_path": None},
        )
        ctx1 = ExecutionContextV1(
            context_id=_compute_context_id(ctx1.canonical_snapshot_dict()),
            job_id=ctx1.job_id,
            workspace_id=ctx1.workspace_id,
            task_id=ctx1.task_id,
            assigned_agent=ctx1.assigned_agent,
            host_id=ctx1.host_id,
            workspace_path=ctx1.workspace_path,
            worktree_path=ctx1.worktree_path,
            harness_root=ctx1.harness_root,
            branch=ctx1.branch,
            session_scope_id=ctx1.session_scope_id,
            legacy_scope_ids=ctx1.legacy_scope_ids,
            log_handle=ctx1.log_handle,
        )
        # A second valid snapshot of the identical content will carry the same
        # digest; execution_context_dict_matches recomputes and ignores the
        # self-referential context_id value itself.
        ctx2 = ExecutionContextV1(
            context_id=ctx1.context_id,
            job_id=ctx1.job_id,
            workspace_id=ctx1.workspace_id,
            task_id=ctx1.task_id,
            assigned_agent=ctx1.assigned_agent,
            host_id=ctx1.host_id,
            workspace_path=ctx1.workspace_path,
            worktree_path=ctx1.worktree_path,
            harness_root=ctx1.harness_root,
            branch=ctx1.branch,
            session_scope_id=ctx1.session_scope_id,
            legacy_scope_ids=ctx1.legacy_scope_ids,
            log_handle=ctx1.log_handle,
        )
        self.assertTrue(
            execution_context_dict_matches(ctx1.to_dict(), ctx2.to_dict())
        )


class FixtureTests(unittest.TestCase):
    def test_v1_fixture_loads_and_validates(self) -> None:
        fixture = Path(__file__).resolve().parent / "fixtures" / "execution_context_v1.json"
        data = json.loads(fixture.read_text(encoding="utf-8"))
        ctx = validate_execution_context_snapshot(data)
        self.assertEqual(ctx.context_id, data["context_id"])
        self.assertEqual(ctx.session_scope_id, "task:discord-nexus:p9-1-task")

    def test_v1_fixture_sha_is_pinned(self) -> None:
        fixture = Path(__file__).resolve().parent / "fixtures" / "execution_context_v1.json"
        expected_sha = (
            "9a9b15d5f1e4e07f0792985bc589e5cc2c0b1edf4df4125c696100c6f2e365f2"
        )
        actual = hashlib.sha256(fixture.read_bytes()).hexdigest()
        self.assertEqual(actual, expected_sha)


class StrictMutationMatrixTests(unittest.TestCase):
    """R1-3: every mutation of the v1 fixture must be rejected."""

    def _fixture(self) -> dict[str, object]:
        fixture = Path(__file__).resolve().parent / "fixtures" / "execution_context_v1.json"
        return json.loads(fixture.read_text(encoding="utf-8"))

    def _mutate(self, mutation: dict[str, object]) -> None:
        data = self._fixture()
        data.update(mutation)
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_extra_top_level_key_rejected(self):
        self._mutate({"extra": "surprise"})

    def test_missing_required_key_rejected(self):
        data = self._fixture()
        del data["host_id"]
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_wrong_contract_version_rejected(self):
        self._mutate({"contract_version": 2})

    def test_context_id_short_digest_rejected(self):
        self._mutate({"context_id": "sha256:deadbeef"})

    def test_context_id_uppercase_hex_rejected(self):
        data = self._fixture()
        data["context_id"] = data["context_id"].upper()
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_context_id_missing_prefix_rejected(self):
        data = self._fixture()
        data["context_id"] = data["context_id"][7:]
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_job_id_integer_rejected(self):
        self._mutate({"job_id": 123})

    def test_task_id_integer_rejected(self):
        self._mutate({"task_id": 123})

    def test_assigned_agent_empty_rejected(self):
        self._mutate({"assigned_agent": ""})

    def test_host_id_empty_rejected(self):
        self._mutate({"host_id": ""})

    def test_workspace_path_relative_rejected(self):
        self._mutate({"workspace_path": "relative/path"})

    def test_workspace_path_traversal_rejected(self):
        self._mutate({"workspace_path": "/host/../other"})

    def test_workspace_path_with_null_rejected(self):
        data = self._fixture()
        data["workspace_path"] = "/host\x00/other"
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_workspace_path_with_newline_rejected(self):
        self._mutate({"workspace_path": "/host\n/other"})

    def test_worktree_path_relative_rejected(self):
        self._mutate({"worktree_path": "./foo"})

    def test_harness_root_windows_traversal_rejected(self):
        data = self._fixture()
        data["harness_root"] = "C:\\host\\..\\other"
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_session_scope_id_empty_rejected(self):
        self._mutate({"session_scope_id": ""})

    def test_session_scope_id_with_space_rejected(self):
        self._mutate({"session_scope_id": "scope with space"})

    def test_legacy_scope_ids_duplicate_rejected(self):
        data = self._fixture()
        sid = data["session_scope_id"]
        data["legacy_scope_ids"] = [sid, sid]
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_legacy_scope_ids_tuple_rejected(self):
        data = self._fixture()
        data["legacy_scope_ids"] = ("legacy:a",)
        with self.assertRaisesRegex(ContextError, "must be a list"):
            validate_execution_context_snapshot(data)

    def test_legacy_scope_ids_non_string_rejected(self):
        self._mutate({"legacy_scope_ids": ["legacy:a", 1]})

    def test_legacy_scope_ids_exceeds_max_rejected(self):
        data = self._fixture()
        data["legacy_scope_ids"] = [f"legacy:{i}" for i in range(11)]
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_log_handle_wrong_kind_rejected(self):
        data = self._fixture()
        data["log_handle"] = {"kind": "other", "job_id": data["job_id"], "logs_path": None}
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_log_handle_job_id_mismatch_rejected(self):
        data = self._fixture()
        data["log_handle"] = {"kind": "coordinate_job", "job_id": "other", "logs_path": None}
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_log_handle_extra_key_rejected(self):
        data = self._fixture()
        data["log_handle"] = {
            "kind": "coordinate_job",
            "job_id": data["job_id"],
            "logs_path": None,
            "extra": "surprise",
        }
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_log_handle_logs_path_not_string_or_null_rejected(self):
        data = self._fixture()
        data["log_handle"] = {"kind": "coordinate_job", "job_id": data["job_id"], "logs_path": 123}
        with self.assertRaises(ContextError):
            validate_execution_context_snapshot(data)

    def test_digest_mismatch_rejected(self):
        data = self._fixture()
        data["context_id"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ContextError, "digest mismatch"):
            validate_execution_context_snapshot(data)




class WorktreeRootsAuthorityTests(unittest.TestCase):
    """Issue #18: allowlisted host-native sibling worktree roots.

    The allowlist is the job-creation authority for paths outside the control
    workspace root. Resolution is pure lexical: no control-host
    ``Path.resolve()``/``realpath``/stat ever runs on the host-native branch.
    """

    SIBLING_ROOT = "/host/worktrees"

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        # upsert_workspace stores the control root control-host-resolved; raw
        # sibling tests must use the same resolved spelling for classification.
        self.control_root = str(Path(self.tmp.name).resolve())
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root="harness",
            base_branch="main",
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[self.SIBLING_ROOT],
        )
        register_agent(self.conn, agent_id="agent1", host_id="host1", capabilities={})

    def _resolve(self, worktree_path: str, **overrides):
        from coordinate.db import get_workspace, get_workspace_host_profile

        workspace = get_workspace(self.conn, "ws")
        profile = get_workspace_host_profile(
            self.conn, workspace_id="ws", host_id="host1"
        )
        return resolve_execution_context_v1(
            job_id="request:e1",
            workspace=workspace,
            task=None,
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "discord:ch"},
            job_worktree_path=worktree_path,
            **overrides,
        )

    def test_allowlisted_sibling_resolves_to_normalized_host_path(self) -> None:
        ctx = self._resolve(f"{self.SIBLING_ROOT}/issue-18/feature")
        self.assertEqual(ctx.worktree_path, f"{self.SIBLING_ROOT}/issue-18/feature")

    def test_sibling_trailing_slash_is_canonicalized(self) -> None:
        ctx = self._resolve(f"{self.SIBLING_ROOT}/issue-18/feature/")
        self.assertEqual(ctx.worktree_path, f"{self.SIBLING_ROOT}/issue-18/feature")

    def test_sibling_root_itself_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "worktree_path is invalid"):
            self._resolve(self.SIBLING_ROOT)

    def test_similar_prefix_sibling_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "worktree_path is invalid"):
            self._resolve(f"{self.SIBLING_ROOT}-other/issue-18")

    def test_sibling_traversal_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "traversal"):
            self._resolve(f"{self.SIBLING_ROOT}/issue-18/../escape")

    def test_sibling_dot_segment_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "traversal"):
            self._resolve(f"{self.SIBLING_ROOT}/./issue-18")

    def test_relative_sibling_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "worktree_path is invalid"):
            self._resolve("worktrees/issue-18")

    def test_outside_allowlist_rejected(self) -> None:
        with self.assertRaisesRegex(ContextError, "worktree_path is invalid"):
            self._resolve("/other/worktrees/issue-18")

    def test_control_branch_still_maps_under_control_root(self) -> None:
        # Classification is control-root-first: a path under the control
        # workspace root keeps the legacy mapping even when roots exist.
        ctx = self._resolve(f"{self.control_root}/feature")
        self.assertEqual(ctx.worktree_path, "/host/ws/feature")

    def test_canonical_checkout_with_roots_configured(self) -> None:
        workspace, profile = self._profile()
        ctx = resolve_execution_context_v1(
            job_id="request:e1",
            workspace=workspace,
            task=None,
            assigned_agent="agent1",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "discord:ch"},
        )
        self.assertEqual(ctx.worktree_path, "/host/ws")

    def _profile(self):
        from coordinate.db import get_workspace, get_workspace_host_profile

        return (
            get_workspace(self.conn, "ws"),
            get_workspace_host_profile(self.conn, workspace_id="ws", host_id="host1"),
        )

    def test_windows_sibling_child_is_allowlisted(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="C:\\Users\\ADMIN\\projects\\multinexus",
            harness_root="C:\\Users\\ADMIN\\projects\\multinexus\\harness",
            worktree_roots=["C:\\Users\\Admin\\projects\\WorkTrees"],
        )
        ctx = self._resolve("C:\\Users\\Admin\\projects\\WorkTrees\\issue-18")
        self.assertEqual(
            ctx.worktree_path,
            "c:\\users\\admin\\projects\\worktrees\\issue-18",
        )
        from coordinate.execution_resources import build_worktree_resource

        self.assertEqual(
            build_worktree_resource(ctx.host_id, ctx.worktree_path).normalized_path,
            ctx.worktree_path,
        )

    def test_windows_sibling_never_mapped_through_control_root(self) -> None:
        # A Windows host path must not be treated as a control-path candidate
        # even when the control root is a POSIX prefix of nothing.
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="C:\\Users\\ADMIN\\projects\\multinexus",
            harness_root="C:\\Users\\ADMIN\\projects\\multinexus\\harness",
            worktree_roots=["C:\\Users\\Admin\\projects\\WorkTrees"],
        )
        ctx = self._resolve("C:/Users/Admin/projects/WorkTrees/issue-18/feature")
        self.assertEqual(
            ctx.worktree_path,
            "c:\\users\\admin\\projects\\worktrees\\issue-18\\feature",
        )

    def test_digest_and_resource_identity_use_normalized_sibling_path(self) -> None:
        ctx = self._resolve(f"{self.SIBLING_ROOT}/issue-18/feature")
        resource = build_worktree_resource(ctx.host_id, ctx.worktree_path)
        self.assertEqual(resource.normalized_path, ctx.worktree_path)
        key = compute_resource_key(resource)
        self.assertTrue(key.startswith("sha256:"))
        # Same lexical path with a trailing slash must bind the same identity.
        ctx2 = self._resolve(f"{self.SIBLING_ROOT}/issue-18/feature/")
        self.assertEqual(ctx2.worktree_path, ctx.worktree_path)
        self.assertEqual(ctx2.context_id, ctx.context_id)


class WorktreeRootsProfileValidationTests(unittest.TestCase):
    """Issue #18: allowlist storage validation and fail-closed reads."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )

    def test_explicit_roots_normalized_deduped_order_stable(self) -> None:
        profile = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[
                "/host/worktrees/b",
                "/host/worktrees/a",
                "/host/worktrees/a/",
                "/host/worktrees/b",
            ],
        )
        self.assertEqual(
            profile.worktree_roots,
            ("/host/worktrees/b", "/host/worktrees/a"),
        )
        self.assertEqual(
            json.loads(
                self.conn.execute(
                    "SELECT worktree_roots_json FROM workspace_host_profiles"
                ).fetchone()[0]
            ),
            ["/host/worktrees/b", "/host/worktrees/a"],
        )

    def test_none_preserves_existing_roots(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/worktrees/a"],
        )
        preserved = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        self.assertEqual(preserved.worktree_roots, ("/host/worktrees/a",))

    def test_none_on_new_row_writes_empty_allowlist(self) -> None:
        profile = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        self.assertEqual(profile.worktree_roots, ())

    def test_explicit_list_replaces_whole_allowlist(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/worktrees/a", "/host/worktrees/b"],
        )
        replaced = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/worktrees/c"],
        )
        self.assertEqual(replaced.worktree_roots, ("/host/worktrees/c",))

    def test_explicit_empty_list_clears_allowlist(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/worktrees/a"],
        )
        cleared = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[],
        )
        self.assertEqual(cleared.worktree_roots, ())

    def test_metadata_update_does_not_touch_roots(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/worktrees/a"],
        )
        updated = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            metadata={"note": "x"},
        )
        self.assertEqual(updated.worktree_roots, ("/host/worktrees/a",))
        self.assertEqual(updated.metadata, {"note": "x"})

    def test_invalid_roots_rejected(self) -> None:
        for invalid in (
            "relative/root",
            "/",
            "C:\\",
            "/host/worktrees/..",
            "/host/worktrees/.",
            "C:\\Users\\x",  # cross-flavour against POSIX workspace_path
        ):
            with self.subTest(root=invalid):
                with self.assertRaisesRegex(ValueError, "invalid worktree root"):
                    upsert_workspace_host_profile(
                        self.conn,
                        workspace_id="ws",
                        host_id="mac",
                        workspace_path="/host/ws",
                        harness_root="/host/harness",
                        worktree_roots=[invalid],
                    )

    def test_unc_share_root_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid worktree root"):
            upsert_workspace_host_profile(
                self.conn,
                workspace_id="ws",
                host_id="mac",
                workspace_path="\\\\server\\share\\ws",
                harness_root="\\\\server\\share\\ws\\harness",
                worktree_roots=["\\\\server\\share"],
            )

    def test_non_string_root_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "entries must be strings"):
            upsert_workspace_host_profile(
                self.conn,
                workspace_id="ws",
                host_id="mac",
                workspace_path="/host/ws",
                harness_root="/host/harness",
                worktree_roots=["/host/worktrees/a", 42],
            )

    def test_string_roots_argument_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "worktree_roots must be a list"):
            upsert_workspace_host_profile(
                self.conn,
                workspace_id="ws",
                host_id="mac",
                workspace_path="/host/ws",
                harness_root="/host/harness",
                worktree_roots="/host/worktrees/a",
            )

    def test_malformed_stored_json_fails_closed_on_read(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        self.conn.execute(
            "UPDATE workspace_host_profiles SET worktree_roots_json = 'not-json'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(ValueError, "invalid JSON"):
            get_workspace_host_profile(self.conn, workspace_id="ws", host_id="mac")

    def test_non_canonical_stored_json_fails_closed_on_read(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        self.conn.execute(
            "UPDATE workspace_host_profiles SET worktree_roots_json = '[\"/host/worktrees/a/\"]'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(ValueError, "not canonical"):
            get_workspace_host_profile(self.conn, workspace_id="ws", host_id="mac")

    def test_duplicate_stored_json_fails_closed_on_read(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        self.conn.execute(
            "UPDATE workspace_host_profiles SET worktree_roots_json = "
            "'[\"/host/worktrees/a\", \"/host/worktrees/a\"]'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            get_workspace_host_profile(self.conn, workspace_id="ws", host_id="mac")

    def test_non_list_stored_json_fails_closed_on_read(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
        )
        self.conn.execute(
            "UPDATE workspace_host_profiles SET worktree_roots_json = '{}'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(ValueError, "must be a JSON array"):
            get_workspace_host_profile(self.conn, workspace_id="ws", host_id="mac")




class SiblingSubmitIntegrationTests(unittest.TestCase):
    """Issue #18: runtime submit of allowlisted host-native sibling worktrees."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.sibling = tempfile.TemporaryDirectory()
        self.addCleanup(self.sibling.cleanup)
        self.conn = initialize(":memory:")
        self.control_root = str(Path(self.tmp.name).resolve())
        self.sibling_root = str(Path(self.sibling.name).resolve())
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root="harness",
            base_branch="main",
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[self.sibling_root],
        )
        register_agent(self.conn, agent_id="agent1", host_id="host1", capabilities={})

    def _submit(self, worktree_path: str | None, message_id: str, **overrides):
        return submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="quiet",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": message_id,
                "session_scope_id": f"discord:{message_id}",
            },
            reply={"platform": "discord", "destination": "ch"},
            worktree_path=worktree_path,
            **overrides,
        )

    def _counts(self) -> tuple[int, int]:
        return tuple(
            self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("events", "jobs")
        )

    def test_sibling_submit_persists_same_normalized_path_everywhere(self) -> None:
        raw = f"{self.sibling_root}/issue-18/feature/"
        request = self._submit(raw, "m-sibling")
        expected = f"{self.sibling_root}/issue-18/feature"
        self.assertEqual(request.event["payload"]["worktree_path"], expected)
        self.assertEqual(request.job["worktree_path"], expected)
        self.assertEqual(
            request.job["payload"]["execution_context"]["worktree_path"], expected
        )

    def test_sibling_submit_then_claim_uses_same_worktree(self) -> None:
        request = self._submit(f"{self.sibling_root}/issue-18", "m-claim")
        claim = claim_job(self.conn, agent_id="agent1")
        self.assertTrue(claim.claimed)
        self.assertEqual(claim.job["worktree_path"], request.job["worktree_path"])
        self.assertEqual(
            claim.execution_context["worktree_path"],
            f"{self.sibling_root}/issue-18",
        )
        self.assertEqual(
            claim.execution_context["context_id"],
            request.job["payload"]["execution_context"]["context_id"],
        )

    def test_sibling_replay_normalizes_raw_before_comparison(self) -> None:
        first = self._submit(f"{self.sibling_root}/issue-18", "m-replay")
        before = self._counts()
        replay = self._submit(f"{self.sibling_root}/issue-18/", "m-replay")
        self.assertFalse(replay.event_created)
        self.assertFalse(replay.job_created)
        self.assertEqual(replay.job["id"], first.job["id"])
        self.assertEqual(self._counts(), before)

    def test_sibling_replay_different_path_conflicts(self) -> None:
        self._submit(f"{self.sibling_root}/issue-18", "m-replay-diff")
        before = self._counts()
        with self.assertRaisesRegex(RuntimeError, "worktree_path conflicts"):
            self._submit(f"{self.sibling_root}/issue-19", "m-replay-diff")
        self.assertEqual(self._counts(), before)

    def test_sibling_submit_without_allowlist_rejected_zero_mutation(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[],
        )
        before = self._counts()
        with self.assertRaisesRegex(RuntimeError, "invalid execution context"):
            self._submit(f"{self.sibling_root}/issue-18", "m-no-allowlist")
        self.assertEqual(self._counts(), before)

    def test_revoked_allowlist_rejects_new_submit_and_replay(self) -> None:
        self._submit(f"{self.sibling_root}/issue-18", "m-revoked")
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[],
        )
        before = self._counts()
        with self.assertRaisesRegex(RuntimeError, "invalid execution context"):
            self._submit(f"{self.sibling_root}/issue-19", "m-revoked-new")
        # The same raw replay is rejected at the preflight (allowlist is the
        # job-creation authority) before any replay comparison runs.
        with self.assertRaisesRegex(RuntimeError, "invalid execution context"):
            self._submit(f"{self.sibling_root}/issue-18", "m-revoked")
        self.assertEqual(self._counts(), before)

    def test_sibling_durable_path_ignores_control_host_symlink(self) -> None:
        # The sibling path lexically contains a control-host symlink. The
        # durable value must stay the normalized lexical path and must never
        # be resolved through the control host (Path.resolve would follow the
        # link to ``actual``).
        target = Path(self.sibling_root) / "actual"
        target.mkdir()
        link = Path(self.sibling_root) / "linked"
        link.symlink_to(target, target_is_directory=True)
        raw = f"{link}/issue-18"
        request = self._submit(raw, "m-symlink")
        expected = f"{self.sibling_root}/linked/issue-18"
        self.assertEqual(request.job["worktree_path"], expected)
        self.assertEqual(request.event["payload"]["worktree_path"], expected)
        self.assertEqual(
            request.job["payload"]["execution_context"]["worktree_path"], expected
        )

    def test_windows_foreign_sibling_submit_skips_control_resolve(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="C:\\Users\\ADMIN\\projects\\multinexus",
            harness_root="C:\\Users\\ADMIN\\projects\\multinexus\\harness",
            worktree_roots=["C:\\Users\\Admin\\projects\\WorkTrees"],
        )
        request = self._submit(
            "C:\\Users\\Admin\\projects\\WorkTrees\\issue-18\\feature",
            "m-windows",
        )
        expected = "c:\\users\\admin\\projects\\worktrees\\issue-18\\feature"
        self.assertEqual(request.job["worktree_path"], expected)
        self.assertEqual(request.event["payload"]["worktree_path"], expected)
        self.assertEqual(
            request.job["payload"]["execution_context"]["worktree_path"], expected
        )
        # Claim must derive the same worktree resource identity.
        claim = claim_job(self.conn, agent_id="agent1")
        self.assertTrue(claim.claimed)
        self.assertEqual(claim.execution_context["worktree_path"], expected)
        key = compute_resource_key(
            build_worktree_resource("host1", claim.execution_context["worktree_path"])
        )
        self.assertTrue(key.startswith("sha256:"))

    def test_canonical_submit_unaffected_by_allowlist(self) -> None:
        request = self._submit(None, "m-canonical")
        self.assertEqual(request.job["worktree_path"], None)
        self.assertEqual(
            request.job["payload"]["execution_context"]["worktree_path"], "/host/ws"
        )
        claim = claim_job(self.conn, agent_id="agent1")
        self.assertTrue(claim.claimed)
        self.assertEqual(claim.execution_context["worktree_path"], "/host/ws")




class ControlPathSeparatorSymmetryTests(unittest.TestCase):
    """R3: control-root/raw comparison must be separator-symmetric.

    ``classify_worktree_raw_path`` and ``_map_foreign_path`` share one
    separator canonicalization so a Windows-style control root with either
    separator spelling never drifts between classification and mapping.
    """

    def test_classify_windows_control_root_backslash_child(self):
        from coordinate.execution_resources import classify_worktree_raw_path

        self.assertEqual(
            classify_worktree_raw_path("C:\\ws", "C:\\ws\\feature"), "control"
        )

    def test_classify_windows_control_root_forward_slash_child(self):
        from coordinate.execution_resources import classify_worktree_raw_path

        self.assertEqual(
            classify_worktree_raw_path("C:\\ws", "C:/ws/feature"), "control"
        )

    def test_classify_windows_control_root_itself(self):
        from coordinate.execution_resources import classify_worktree_raw_path

        self.assertEqual(classify_worktree_raw_path("C:\\ws", "C:\\ws"), "control")

    def test_classify_windows_similar_prefix_is_host_native(self):
        from coordinate.execution_resources import classify_worktree_raw_path

        self.assertEqual(
            classify_worktree_raw_path("C:\\ws", "C:\\wsx\\feature"),
            "host_native",
        )

    def test_classify_posix_control_root_backslash_child_matches_map(self):
        # _map_foreign_path has always canonicalized backslashes to slashes
        # for the control-root comparison, so classify must agree: a
        # backslash child of a POSIX control root is control-branch material
        # (never drift between the two comparisons).
        from coordinate.execution_resources import classify_worktree_raw_path

        self.assertEqual(
            classify_worktree_raw_path("/ws", "/ws\\\\feature"), "control"
        )
        self.assertEqual(
            _map_foreign_path("/ws", "/host/ws", "/ws\\\\feature"),
            "/host/ws/feature",
        )

    def test_map_foreign_path_windows_control_root_backslash_child(self):
        self.assertEqual(
            _map_foreign_path("C:\\ws", "/host/ws", "C:\\ws\\feature"),
            "/host/ws/feature",
        )

    def test_map_foreign_path_windows_control_root_mixed_separators(self):
        self.assertEqual(
            _map_foreign_path("C:/ws", "/host/ws", "C:\\ws\\a\\b"),
            "/host/ws/a/b",
        )

    def test_map_foreign_path_windows_control_root_similar_prefix_rejected(self):
        with self.assertRaisesRegex(ContextError, "outside control workspace"):
            _map_foreign_path("C:\\ws", "/host/ws", "C:\\wsx\\feature")


class WorktreeRootWidthTests(unittest.TestCase):
    """R3: allowlisted roots must not equal or contain the canonical checkout."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )

    def _upsert(self, root: str):
        return upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[root],
        )

    def test_root_equal_to_canonical_rejected(self):
        with self.assertRaisesRegex(ValueError, "canonical workspace_path or an ancestor"):
            self._upsert("/host/ws")

    def test_root_ancestor_of_canonical_rejected(self):
        with self.assertRaisesRegex(ValueError, "canonical workspace_path or an ancestor"):
            self._upsert("/host")

    def test_root_ancestor_of_canonical_rejected_with_windows_flavour(self):
        with self.assertRaisesRegex(ValueError, "canonical workspace_path or an ancestor"):
            upsert_workspace_host_profile(
                self.conn,
                workspace_id="ws",
                host_id="mac",
                workspace_path="C:\\\\Users\\\\Admin\\\\projects\\\\multinexus",
                harness_root="C:\\\\Users\\\\Admin\\\\projects\\\\multinexus\\\\harness",
                worktree_roots=["C:\\\\Users\\\\Admin\\\\projects"],
            )

    def test_root_descendant_of_canonical_allowed(self):
        profile = self._upsert("/host/ws/worktrees")
        self.assertEqual(profile.worktree_roots, ("/host/ws/worktrees",))

    def test_root_true_sibling_of_canonical_allowed(self):
        profile = self._upsert("/host/worktrees")
        self.assertEqual(profile.worktree_roots, ("/host/worktrees",))

    def test_root_ancestor_of_canonical_preserve_path_also_rejected(self):
        """Preserve re-validation applies the same width rule against the NEW
        workspace_path: a stored root that becomes the new canonical or its
        ancestor fails before mutation."""
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/ws/worktrees"],
        )
        before = tuple(
            self.conn.execute(
                "SELECT * FROM workspace_host_profiles WHERE host_id = 'mac'"
            ).fetchone()
        )
        # New canonical moves INTO the stored root's subtree -> the root
        # becomes an ancestor of the canonical checkout.
        with self.assertRaisesRegex(ValueError, "canonical workspace_path or an ancestor"):
            upsert_workspace_host_profile(
                self.conn,
                workspace_id="ws",
                host_id="mac",
                workspace_path="/host/ws/worktrees/checkout",
                harness_root="/host/harness",
            )
        after = tuple(
            self.conn.execute(
                "SELECT * FROM workspace_host_profiles WHERE host_id = 'mac'"
            ).fetchone()
        )
        self.assertEqual(after, before)


class MalformedStoredRootsRepairTests(unittest.TestCase):
    """R3: explicit clear/replace must repair malformed stored roots JSON."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )

    def _corrupt(self) -> None:
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/worktrees/a"],
        )
        self.conn.execute(
            "UPDATE workspace_host_profiles SET worktree_roots_json = 'not-json'"
        )
        self.conn.commit()

    def test_ordinary_read_still_fails_closed(self) -> None:
        self._corrupt()
        with self.assertRaisesRegex(ValueError, "invalid JSON"):
            get_workspace_host_profile(self.conn, workspace_id="ws", host_id="mac")

    def test_preserve_on_malformed_still_fails_closed(self) -> None:
        self._corrupt()
        with self.assertRaisesRegex(ValueError, "invalid JSON"):
            upsert_workspace_host_profile(
                self.conn,
                workspace_id="ws",
                host_id="mac",
                workspace_path="/host/ws",
                harness_root="/host/harness",
            )

    def test_explicit_clear_repairs_malformed_stored_json(self) -> None:
        self._corrupt()
        repaired = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=[],
        )
        self.assertEqual(repaired.worktree_roots, ())
        loaded = get_workspace_host_profile(
            self.conn, workspace_id="ws", host_id="mac"
        )
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.worktree_roots, ())

    def test_explicit_replace_repairs_malformed_stored_json(self) -> None:
        self._corrupt()
        repaired = upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path="/host/ws",
            harness_root="/host/harness",
            worktree_roots=["/host/worktrees/b"],
        )
        self.assertEqual(repaired.worktree_roots, ("/host/worktrees/b",))
        loaded = get_workspace_host_profile(
            self.conn, workspace_id="ws", host_id="mac"
        )
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.worktree_roots, ("/host/worktrees/b",))


class LegacyClaimTransactionGateTests(unittest.TestCase):
    """R3: the legacy claim containment gate runs inside BEGIN IMMEDIATE and
    re-reads the CURRENT profile, so a root mutation cannot land between the
    gate and the CAS; failures roll back."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.sibling = tempfile.TemporaryDirectory()
        self.addCleanup(self.sibling.cleanup)
        self.conn = initialize(":memory:")
        self.control_root = str(Path(self.tmp.name).resolve())
        self.sibling_root = str(Path(self.sibling.name).resolve())
        upsert_workspace(
            self.conn,
            workspace_id="ws",
            name="WS",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path=self.control_root,
            harness_root=self.control_root,
            worktree_roots=[self.sibling_root],
        )
        register_agent(self.conn, agent_id="agent1", host_id="host1", capabilities={})

    def test_gate_reruns_inside_transaction_and_rolls_back(self) -> None:
        import coordinate.runtime as runtime_module

        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="quiet",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "m-gate",
                "session_scope_id": "discord:m-gate",
            },
            reply={"platform": "discord", "destination": "ch"},
            worktree_path=f"{self.sibling_root}/issue-18",
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path=self.control_root,
            harness_root=self.control_root,
            worktree_roots=[],
        )

        calls: list[bool] = []
        original = runtime_module.get_workspace_host_profile

        def recording(conn_, **kwargs):
            calls.append(conn_.in_transaction)
            return original(conn_, **kwargs)

        with patch(
            "coordinate.runtime.get_workspace_host_profile", side_effect=recording
        ):
            with self.assertRaisesRegex(
                RuntimeError, "worktree containment policy rejected claim"
            ):
                claim_job(self.conn, agent_id="agent1")

        self.assertTrue(calls, "claim must re-read the host profile")
        self.assertTrue(
            calls[-1],
            "containment gate profile read must happen inside BEGIN IMMEDIATE",
        )
        self.assertFalse(
            self.conn.in_transaction,
            "gate failure must roll back the transaction",
        )
        job = self.conn.execute("SELECT status, attempt_count FROM jobs").fetchone()
        self.assertEqual((job["status"], job["attempt_count"]), ("pending", 0))
        claimed = self.conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'job.claimed'"
        ).fetchone()[0]
        self.assertEqual(claimed, 0)
        # The gated job can still be claimed after the root is restored.
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path=self.control_root,
            harness_root=self.control_root,
            worktree_roots=[self.sibling_root],
        )
        result = claim_job(self.conn, agent_id="agent1")
        self.assertTrue(result.claimed)
        self.assertEqual(
            result.execution_context["worktree_path"],
            f"{self.sibling_root}/issue-18",
        )

    def test_same_roots_claim_passes_inside_transaction(self) -> None:
        import coordinate.runtime as runtime_module

        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="agent1",
            prompt="quiet",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "m-gate-ok",
                "session_scope_id": "discord:m-gate-ok",
            },
            reply={"platform": "discord", "destination": "ch"},
            worktree_path=f"{self.sibling_root}/issue-18",
        )
        calls: list[bool] = []
        original = runtime_module.get_workspace_host_profile

        def recording(conn_, **kwargs):
            calls.append(conn_.in_transaction)
            return original(conn_, **kwargs)

        with patch(
            "coordinate.runtime.get_workspace_host_profile", side_effect=recording
        ):
            result = claim_job(self.conn, agent_id="agent1")
        self.assertTrue(result.claimed)
        self.assertTrue(calls[-1])
        self.assertFalse(self.conn.in_transaction)

if __name__ == "__main__":
    unittest.main()
