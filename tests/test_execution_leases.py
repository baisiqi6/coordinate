"""Tests for transaction-aware execution attempt lease primitives."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import unittest
from typing import Any

from coordinate.db import (
    create_job,
    get_workspace,
    get_workspace_host_profile,
    upsert_workspace_host_profile,
)
from coordinate.db_support import utc_now
from coordinate.execution_context import resolve_execution_context_v1
from coordinate.execution_leases import (
    LeaseError,
    count_active_leases_for_agent,
    expire_attempt_lease,
    expire_due_attempt_leases,
    get_attempt_lease,
    list_active_leases_for_agent,
    release_attempt_lease,
    renew_attempt_lease,
    reserve_attempt_lease,
)
from coordinate.executor_capacity import (
    CapacityCatalog,
    CapacityError,
    CapacityPolicy,
    compute_capacity_catalog_hash,
    sync_capacity_catalog,
)
from coordinate.executor_identity import (
    ExecutorCatalog,
    ExecutorDefinition,
    ExecutorInstanceBinding,
    compute_executor_catalog_hash,
    sync_executor_catalog,
)
from coordinate.schema import migrate


LEASE_TABLES = {
    "executor_capacity_sources",
    "executor_capacity_policies",
    "execution_attempt_leases",
}


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    migrate(conn)
    return conn


def _seed_agent_runner(conn: sqlite3.Connection, agent_id: str) -> None:
    now = utc_now()
    conn.execute(
        "INSERT INTO agents (id, name, role, capabilities_json, online_state, current_load, client_type, created_at, updated_at) "
        "VALUES (?, ?, 'agent', '[]', 'offline', 0, 'agentd', ?, ?)",
        (agent_id, agent_id, now, now),
    )
    conn.execute(
        "INSERT INTO runner_profiles (id, name, runner_type, command, working_directory_strategy, supports_stream_attach, env_json, created_at, updated_at) "
        "VALUES (?, ?, 'agentd', 'agent', 'current_dir', 0, '{}', ?, ?)",
        (agent_id, agent_id, now, now),
    )
    conn.commit()


def _sync_executor_and_capacity(conn: sqlite3.Connection, agent_id: str, max_jobs: int = 2) -> None:
    executor_catalog = ExecutorCatalog(
        source_id="multinexus.discord",
        source_version=2,
        catalog_hash="",
        source_path=None,
        definitions=(
            ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
        ),
        bindings=(
            ExecutorInstanceBinding(agent_id=agent_id, executor_definition_id="omp-code", runner_profile_id=agent_id, enabled=True),
        ),
    )
    executor_catalog = executor_catalog.__class__(
        source_id=executor_catalog.source_id,
        source_version=executor_catalog.source_version,
        catalog_hash=compute_executor_catalog_hash(executor_catalog),
        source_path=executor_catalog.source_path,
        definitions=executor_catalog.definitions,
        bindings=executor_catalog.bindings,
    )
    sync_executor_catalog(conn, executor_catalog)
    conn.commit()

    capacity_catalog = CapacityCatalog(
        source_id="multinexus.discord.capacity",
        source_version=1,
        catalog_hash="",
        source_path=None,
        policies=(CapacityPolicy(agent_id=agent_id, max_concurrent_jobs=max_jobs),),
    )
    capacity_catalog = capacity_catalog.__class__(
        source_id=capacity_catalog.source_id,
        source_version=capacity_catalog.source_version,
        catalog_hash=compute_capacity_catalog_hash(capacity_catalog),
        source_path=capacity_catalog.source_path,
        policies=capacity_catalog.policies,
    )
    sync_capacity_catalog(conn, capacity_catalog)
    conn.commit()


def _create_workspace(conn: sqlite3.Connection) -> None:
    now = utc_now()
    conn.execute(
        "INSERT OR IGNORE INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("ws", "ws", "/tmp", "/tmp/docs", now, now),
    )


def _create_job(
    conn: sqlite3.Connection,
    agent_id: str,
    *,
    host_id: str = "host1",
    worktree_path: str = "/tmp/ws1",
    attempt: int = 1,
) -> str:
    _create_workspace(conn)
    conn.commit()
    upsert_workspace_host_profile(
        conn,
        workspace_id="ws",
        host_id=host_id,
        workspace_path=worktree_path,
        harness_root="/tmp/docs",
    )
    conn.commit()
    job = create_job(
        conn,
        workspace_id="ws",
        task_id=None,
        runner_profile_id=agent_id,
        assigned_agent=agent_id,
        payload={},
        worktree_path=worktree_path,
    )
    conn.execute(
        "UPDATE jobs SET attempt_count = ?, assigned_agent = ?, runner_profile_id = ? WHERE id = ?",
        (attempt, agent_id, agent_id, job["id"]),
    )
    conn.commit()

    workspace = get_workspace(conn, "ws")
    profile = get_workspace_host_profile(conn, workspace_id="ws", host_id=host_id)
    assert workspace is not None and profile is not None
    ctx = resolve_execution_context_v1(
        job_id=job["id"],
        workspace=workspace,
        task=None,
        assigned_agent=agent_id,
        host_id=host_id,
        profile=profile,
        origin={"session_scope_id": "test"},
    )
    payload = {"execution_context": ctx.to_dict()}
    conn.execute(
        "UPDATE jobs SET payload_json = ? WHERE id = ?",
        (json.dumps(payload), job["id"]),
    )
    conn.commit()
    return job["id"]


class LeaseSchemaTests(unittest.TestCase):
    def test_lease_tables_created(self):
        conn = _make_conn()
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        self.assertTrue(LEASE_TABLES.issubset(tables))


class LeaseReserveTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()
        _seed_agent_runner(self.conn, "mac-omp")
        _sync_executor_and_capacity(self.conn, "mac-omp", max_jobs=2)
        self.job_id = _create_job(self.conn, "mac-omp")

    def tearDown(self):
        self.conn.close()

    def test_reserve_succeeds_and_snapshots_capacity(self):
        result = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        self.assertEqual(result["status"], "active")
        self.assertFalse(result["replayed"])
        self.assertEqual(count_active_leases_for_agent(self.conn, "mac-omp"), 1)
        row = self.conn.execute(
            "SELECT capacity_policy_id, max_concurrent_jobs, resource_key FROM execution_attempt_leases WHERE lease_id = ?",
            (result["lease_id"],),
        ).fetchone()
        self.assertTrue(row["capacity_policy_id"].startswith("sha256:"))
        self.assertEqual(row["max_concurrent_jobs"], 2)
        self.assertTrue(row["resource_key"].startswith("sha256:"))

    def test_reserve_ttl_bounds(self):
        for ttl in (29, 601, True):
            with self.assertRaisesRegex(LeaseError, "TTL"):
                reserve_attempt_lease(
                    self.conn,
                    job_id=self.job_id,
                    attempt_token=1,
                    agent_id="mac-omp",
                    runner_profile_id="mac-omp",
                    host_id="host1",
                    worktree_path="/tmp/ws1",
                    ttl_seconds=ttl,  # type: ignore[arg-type]
                )

    def test_exact_replay_idempotent(self):
        result1 = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        result2 = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        self.assertEqual(result1["lease_id"], result2["lease_id"])
        self.assertTrue(result2["replayed"])

    def test_conflicting_replay_fails(self):
        result1 = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        self.conn.commit()

        # Snapshot capacity state so we can prove zero mutation after the rejection.
        pre_source = dict(
            self.conn.execute(
                "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
                ("multinexus.discord.capacity",),
            ).fetchone()
        )
        pre_policies = [
            dict(row)
            for row in self.conn.execute(
                "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
                ("multinexus.discord.capacity",),
            ).fetchall()
        ]

        # A version/hash bump would replace the active lease's exact capacity_policy_id.
        # Under the new authority contract this fails closed at sync time with zero mutation.
        new_catalog = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=2,
            catalog_hash="",
            source_path=None,
            policies=(CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),),
        )
        new_catalog = new_catalog.__class__(
            source_id=new_catalog.source_id,
            source_version=new_catalog.source_version,
            catalog_hash=compute_capacity_catalog_hash(new_catalog),
            source_path=new_catalog.source_path,
            policies=new_catalog.policies,
        )
        with self.assertRaisesRegex(CapacityError, "referenced by active lease"):
            sync_capacity_catalog(self.conn, new_catalog)
        self.conn.commit()

        # Source and policy rows must be field-for-field unchanged.
        post_source = dict(
            self.conn.execute(
                "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
                ("multinexus.discord.capacity",),
            ).fetchone()
        )
        post_policies = [
            dict(row)
            for row in self.conn.execute(
                "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
                ("multinexus.discord.capacity",),
            ).fetchall()
        ]
        self.assertEqual(post_source, pre_source)
        self.assertEqual(post_policies, pre_policies)

        # The original lease must remain active, and replay against the unchanged
        # policy must still be idempotent (same lease, replayed=True).
        self.assertEqual(count_active_leases_for_agent(self.conn, "mac-omp"), 1)
        result2 = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        self.assertEqual(result1["lease_id"], result2["lease_id"])
        self.assertTrue(result2["replayed"])
        self.assertEqual(count_active_leases_for_agent(self.conn, "mac-omp"), 1)

    def test_capacity_exhaustion(self):
        for i in range(2):
            job_id = _create_job(self.conn, "mac-omp", worktree_path=f"/tmp/ws{i}")
            reserve_attempt_lease(
                self.conn,
                job_id=job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path=f"/tmp/ws{i}",
                ttl_seconds=60,
            )
        job_id = _create_job(self.conn, "mac-omp", worktree_path="/tmp/ws3")
        with self.assertRaisesRegex(LeaseError, "capacity exhausted"):
            reserve_attempt_lease(
                self.conn,
                job_id=job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws3",
                ttl_seconds=60,
            )

    def test_same_resource_collision(self):
        reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        job2 = _create_job(self.conn, "mac-omp")
        with self.assertRaisesRegex(LeaseError, "resource collision"):
            reserve_attempt_lease(
                self.conn,
                job_id=job2,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1/",
                ttl_seconds=60,
            )

    def test_distinct_resources_up_to_capacity(self):
        for i in range(2):
            job_id = _create_job(self.conn, "mac-omp", worktree_path=f"/tmp/ws{i}")
            reserve_attempt_lease(
                self.conn,
                job_id=job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path=f"/tmp/ws{i}",
                ttl_seconds=60,
            )
        self.assertEqual(count_active_leases_for_agent(self.conn, "mac-omp"), 2)

    def test_due_leases_block_reserve_for_same_resource_until_reap(self):
        reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=30,
        )
        # Artificially set the whole lease to the past so it is due for expiry.
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE job_id = ?",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.job_id),
        )
        self.conn.commit()
        job2 = _create_job(self.conn, "mac-omp")
        # P9-3B: reserve no longer expires due leases inline. A due lease for the
        # same resource still blocks selection until the caller-owned reap drains
        # the backlog.
        with self.assertRaisesRegex(LeaseError, "due lease"):
            reserve_attempt_lease(
                self.conn,
                job_id=job2,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )
        # The due lease is still active (not silently expired by reserve).
        active = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE status = 'active'"
        ).fetchone()["n"]
        self.assertEqual(active, 1)


class LeaseRenewReleaseExpireTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()
        _seed_agent_runner(self.conn, "mac-omp")
        _sync_executor_and_capacity(self.conn, "mac-omp", max_jobs=2)
        self.job_id = _create_job(self.conn, "mac-omp")
        self.lease = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )

    def tearDown(self):
        self.conn.close()

    def test_renew_advances_expires_monotonically(self):
        old_expires = self.lease["expires_at"]
        time.sleep(0.1)
        result = renew_attempt_lease(
            self.conn,
            lease_id=self.lease["lease_id"],
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            ttl_seconds=120,
        )
        self.assertGreater(result["expires_at"], old_expires)

    def test_renew_rejects_non_monotonic_ttl(self):
        with self.assertRaisesRegex(LeaseError, "renewal must advance expires_at"):
            renew_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                ttl_seconds=30,
            )

    def test_renew_rejects_wrong_identity(self):
        with self.assertRaisesRegex(LeaseError, "lease job_id mismatch"):
            renew_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id="wrong",
                attempt_token=1,
                agent_id="mac-omp",
                ttl_seconds=60,
            )

    def test_release_idempotent(self):
        result1 = release_attempt_lease(
            self.conn,
            lease_id=self.lease["lease_id"],
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            reason="done",
        )
        result2 = release_attempt_lease(
            self.conn,
            lease_id=self.lease["lease_id"],
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            reason="done",
        )
        self.assertEqual(result1["released_at"], result2["released_at"])

    def test_release_different_reason_fails(self):
        release_attempt_lease(
            self.conn,
            lease_id=self.lease["lease_id"],
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            reason="done",
        )
        with self.assertRaisesRegex(LeaseError, "already released"):
            release_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                reason="cancelled",
            )

    def test_expire_idempotent(self):
        result1 = expire_attempt_lease(
            self.conn,
            lease_id=self.lease["lease_id"],
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
        )
        result2 = expire_attempt_lease(
            self.conn,
            lease_id=self.lease["lease_id"],
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
        )
        self.assertEqual(result1["status"], "expired")
        self.assertEqual(result2["status"], "expired")

    def test_expire_rejects_wrong_identity(self):
        with self.assertRaisesRegex(LeaseError, "expire job_id mismatch"):
            expire_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id="wrong",
                attempt_token=1,
                agent_id="mac-omp",
            )

    def test_expire_due_leaves_job_state(self):
        # Set lease as expired by moving all timestamps to the past.
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.lease["lease_id"]),
        )
        self.conn.commit()
        expire_due_attempt_leases(self.conn)
        job = self.conn.execute("SELECT status FROM jobs WHERE id = ?", (self.job_id,)).fetchone()
        self.assertNotEqual(job["status"], "expired")  # job state is not mutated by lease expiry


class LeaseTwoConnectionRaceTests(unittest.TestCase):
    def test_two_connections_reserve_same_resource_race(self):
        # Use a file-backed DB so two connections can race.
        import tempfile
        db_path = tempfile.mktemp(suffix=".sqlite3")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        migrate(conn)
        _seed_agent_runner(conn, "mac-omp")
        _sync_executor_and_capacity(conn, "mac-omp", max_jobs=2)
        job1 = _create_job(conn, "mac-omp")
        job2 = _create_job(conn, "mac-omp")
        conn.close()

        results: list[Any] = []
        errors: list[Exception] = []

        def worker(job_id: str):
            c = sqlite3.connect(db_path)
            c.row_factory = sqlite3.Row
            try:
                c.execute("BEGIN IMMEDIATE")
                result = reserve_attempt_lease(
                    c,
                    job_id=job_id,
                    attempt_token=1,
                    agent_id="mac-omp",
                    runner_profile_id="mac-omp",
                    host_id="host1",
                    worktree_path="/tmp/ws1",
                    ttl_seconds=60,
                )
                c.commit()
                results.append(result)
            except Exception as exc:
                c.rollback()
                errors.append(exc)
            finally:
                c.close()

        t1 = threading.Thread(target=worker, args=(job1,))
        t2 = threading.Thread(target=worker, args=(job2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # One succeeds, one fails due to resource collision.
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertRegex(str(errors[0]), "resource collision")

        # Clean up.
        import os
        os.unlink(db_path)


class LeaseContextCrossLinkTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()
        _seed_agent_runner(self.conn, "mac-omp")
        _sync_executor_and_capacity(self.conn, "mac-omp", max_jobs=2)
        self.job_id = _create_job(self.conn, "mac-omp")

    def tearDown(self):
        self.conn.close()

    def test_reserve_rejects_missing_execution_context(self):
        self.conn.execute("UPDATE jobs SET payload_json = '{}' WHERE id = ?", (self.job_id,))
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "execution_context is required"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_attempt_count_mismatch(self):
        self.conn.execute("UPDATE jobs SET attempt_count = 2 WHERE id = ?", (self.job_id,))
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "attempt_count 2 != 1"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_bool_attempt_token(self):
        with self.assertRaisesRegex(LeaseError, "attempt_token must be an integer"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=True,  # type: ignore[arg-type]
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_context_digest_tamper(self):
        payload = json.loads(
            self.conn.execute("SELECT payload_json FROM jobs WHERE id = ?", (self.job_id,)).fetchone()["payload_json"]
        )
        payload["execution_context"]["context_id"] = "sha256:" + "0" * 64
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), self.job_id),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "invalid execution context"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_forged_host(self):
        with self.assertRaisesRegex(LeaseError, "execution_context host_id mismatch"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host2",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_forged_worktree_path(self):
        with self.assertRaisesRegex(LeaseError, "worktree_path resource mismatch"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/other",
                ttl_seconds=60,
            )

    def test_reserve_rejects_context_job_id_mismatch(self):
        payload = json.loads(
            self.conn.execute("SELECT payload_json FROM jobs WHERE id = ?", (self.job_id,)).fetchone()["payload_json"]
        )
        payload["execution_context"]["job_id"] = "not-this-job"
        payload["execution_context"]["context_id"] = "sha256:" + "0" * 64
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), self.job_id),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "invalid execution context"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_malformed_payload_json(self):
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            ("not-json", self.job_id),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "job payload_json is not valid JSON"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_payload_json_scalar(self):
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            ('"string"', self.job_id),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "job payload_json must be an object"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_reserve_rejects_payload_json_list(self):
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            ("[1, 2, 3]", self.job_id),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "job payload_json must be an object"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )


class LeaseBulkDecisionTamperTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()
        _seed_agent_runner(self.conn, "mac-omp")
        _sync_executor_and_capacity(self.conn, "mac-omp", max_jobs=2)
        self.job_id1 = _create_job(self.conn, "mac-omp", worktree_path="/tmp/ws1")
        self.job_id2 = _create_job(self.conn, "mac-omp", worktree_path="/tmp/ws2")
        self.lease1 = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id1,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        self.lease2 = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id2,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws2",
            ttl_seconds=60,
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _make_due(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE status = 'active'",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z"),
        )
        self.conn.commit()

    def test_count_active_rejects_tampered_resource(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
            ("sha256:" + "0" * 64, self.lease1["lease_id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            count_active_leases_for_agent(self.conn, "mac-omp")

    def test_find_active_resource_rejects_tampered_resource(self):
        tampered_key = "sha256:" + "0" * 64
        self.conn.execute(
            "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
            (tampered_key, self.lease1["lease_id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            from coordinate.execution_leases import _find_active_resource_lease
            _find_active_resource_lease(self.conn, tampered_key)

    def test_expire_due_leaves_all_active_when_one_row_tampered(self):
        # Tamper the resource key of one due lease; the bulk expire must fail
        # closed and leave every lease active.
        self._make_due()
        self.conn.execute(
            "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
            ("sha256:" + "0" * 64, self.lease1["lease_id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            expire_due_attempt_leases(self.conn)
        active = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE status = 'active'"
        ).fetchone()["n"]
        self.assertEqual(active, 2)

    def test_reserve_allows_different_resource_while_same_agent_has_due_lease(self):
        # P9-3B: reserve is not an expiry authority. A due lease for the same
        # agent (different resource) does not block a different resource; the
        # caller-owned reap will drain the backlog separately.
        self._make_due()
        job3 = _create_job(self.conn, "mac-omp", worktree_path="/tmp/ws3")
        reserve_attempt_lease(
            self.conn,
            job_id=job3,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws3",
            ttl_seconds=60,
        )

    def test_reap_rejects_tampered_due_lease(self):
        # A due active lease is tampered; bounded global reap must fail closed
        # and leave every lease active.
        self._make_due()
        self.conn.execute(
            "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
            ("sha256:" + "0" * 64, self.lease1["lease_id"]),
        )
        self.conn.commit()
        from coordinate.execution_leases import _find_due_active_leases
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            _find_due_active_leases(self.conn, utc_now(), 100)
        active = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE status = 'active'"
        ).fetchone()["n"]
        self.assertEqual(active, 2)


class LeaseStoredResourceTamperTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()
        _seed_agent_runner(self.conn, "mac-omp")
        _sync_executor_and_capacity(self.conn, "mac-omp", max_jobs=2)
        self.job_id = _create_job(self.conn, "mac-omp")
        self.lease = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_replay_rejects_tampered_stored_resource_key(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
            ("sha256:" + "0" * 64, self.lease["lease_id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            reserve_attempt_lease(
                self.conn,
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                runner_profile_id="mac-omp",
                host_id="host1",
                worktree_path="/tmp/ws1",
                ttl_seconds=60,
            )

    def test_renew_rejects_tampered_stored_path(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET normalized_path = ? WHERE lease_id = ?",
            ("/tmp/tampered", self.lease["lease_id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            renew_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                ttl_seconds=120,
            )

    def test_get_lease_rejects_tampered_resource(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET host_id = ? WHERE lease_id = ?",
            ("host2", self.lease["lease_id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            get_attempt_lease(self.conn, self.lease["lease_id"])

    def test_list_active_leases_rejects_tampered_resource(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET host_id = ? WHERE lease_id = ?",
            ("host2", self.lease["lease_id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(LeaseError, "stored lease resource snapshot is tampered"):
            list_active_leases_for_agent(self.conn, "mac-omp")


class LeaseAttemptAndReasonBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()
        _seed_agent_runner(self.conn, "mac-omp")
        _sync_executor_and_capacity(self.conn, "mac-omp", max_jobs=2)
        self.job_id = _create_job(self.conn, "mac-omp")
        self.lease = reserve_attempt_lease(
            self.conn,
            job_id=self.job_id,
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws1",
            ttl_seconds=60,
        )

    def tearDown(self):
        self.conn.close()

    def test_release_rejects_long_reason(self):
        with self.assertRaisesRegex(LeaseError, "exceeds 256"):
            release_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                reason="x" * 257,
            )

    def test_release_rejects_control_reason(self):
        with self.assertRaisesRegex(LeaseError, "control characters"):
            release_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                reason="bad\x00",
            )

    def test_release_rejects_empty_reason(self):
        with self.assertRaisesRegex(LeaseError, "must not be empty"):
            release_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                reason="",
            )

    def test_release_rejects_whitespace_reason(self):
        with self.assertRaisesRegex(LeaseError, "must not have surrounding whitespace"):
            release_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=1,
                agent_id="mac-omp",
                reason=" done ",
            )

    def test_release_rejects_bool_attempt(self):
        with self.assertRaisesRegex(LeaseError, "attempt_token must be an integer"):
            release_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=True,  # type: ignore[arg-type]
                agent_id="mac-omp",
                reason="done",
            )

    def test_expire_rejects_bool_attempt(self):
        with self.assertRaisesRegex(LeaseError, "attempt_token must be an integer"):
            expire_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=True,  # type: ignore[arg-type]
                agent_id="mac-omp",
            )

    def test_renew_rejects_bool_attempt(self):
        with self.assertRaisesRegex(LeaseError, "attempt_token must be an integer"):
            renew_attempt_lease(
                self.conn,
                lease_id=self.lease["lease_id"],
                job_id=self.job_id,
                attempt_token=True,  # type: ignore[arg-type]
                agent_id="mac-omp",
                ttl_seconds=120,
            )


if __name__ == "__main__":
    unittest.main()
