"""Tests for the Coordinate-side capacity authority projection and sync."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sqlite3
import stat
import tempfile
import textwrap
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from coordinate.db import initialize
from coordinate.execution_cli import (
    handle_runtime_capacity_list,
    handle_runtime_capacity_show,
    handle_runtime_capacity_sync,
)
from coordinate.executor_capacity import (
    CapacityCatalog,
    CapacityError,
    CapacityPolicy,
    EXPECTED_CAPACITY_SOURCE_ID,
    canonical_capacity_catalog_dict,
    capture_capacity_snapshot,
    compute_capacity_catalog_hash,
    compute_capacity_policy_id,
    get_capacity_policy,
    get_capacity_source,
    list_capacity_policies,
    list_capacity_sources,
    parse_capacity_catalog,
    resolve_capacity_policy,
    restore_capacity_snapshot,
    sync_capacity_catalog,
)
from coordinate.executor_capacity import (
    _snapshot_canonical_bytes as _capacity_snapshot_canonical_bytes,
)
from coordinate.executor_identity import (
    ExecutorCatalog,
    ExecutorDefinition,
    ExecutorInstanceBinding,
    compute_executor_catalog_hash,
    sync_executor_catalog,
)
from coordinate.execution_leases import reserve_attempt_lease
from coordinate.schema import SCHEMA_VERSION, migrate


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures"


def _fixture_capacity_catalog() -> dict[str, object]:
    return json.loads((FIXTURES / "capacity_catalog_v1.json").read_text(encoding="utf-8"))


def _write_toml(content: str) -> Path:
    tmp = Path(tempfile.mkdtemp()) / "registry.toml"
    tmp.write_text(textwrap.dedent(content), encoding="utf-8")
    return tmp


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    migrate(conn)
    return conn


class CapacitySchemaTests(unittest.TestCase):
    def test_fresh_initialize_is_v14(self):
        conn = initialize(":memory:")
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        self.assertEqual(SCHEMA_VERSION, 14)

    def test_capacity_tables_exist(self):
        conn = _make_conn()
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        self.assertIn("executor_capacity_sources", tables)
        self.assertIn("executor_capacity_policies", tables)
        self.assertIn("execution_attempt_leases", tables)

    def test_v12_upgrade_creates_capacity_tables(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE agents (id TEXT PRIMARY KEY, name TEXT NOT NULL,
              capabilities_json TEXT NOT NULL, online_state TEXT NOT NULL,
              current_load INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL);
            CREATE TABLE runner_profiles (id TEXT PRIMARY KEY, name TEXT NOT NULL,
              runner_type TEXT NOT NULL, command TEXT NOT NULL,
              working_directory_strategy TEXT NOT NULL, supports_stream_attach INTEGER NOT NULL DEFAULT 0,
              env_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE executor_catalog_sources (source_id TEXT PRIMARY KEY,
              source_version INTEGER NOT NULL, catalog_hash TEXT NOT NULL,
              source_path TEXT, updated_at TEXT NOT NULL);
            CREATE TABLE executor_definitions (id TEXT PRIMARY KEY, source_id TEXT NOT NULL,
              provider TEXT NOT NULL, adapter TEXT NOT NULL, capabilities_json TEXT NOT NULL,
              metadata_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE executor_instance_bindings (agent_id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL, executor_definition_id TEXT NOT NULL,
              runner_profile_id TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            PRAGMA user_version = 12;
            """
        )
        conn.commit()
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'execution_attempt_leases'"
            ).fetchall()
        }
        self.assertIn("idx_execution_attempt_leases_active_resource", indexes)


class CapacityCanonicalTests(unittest.TestCase):
    def test_fixture_hash_matches_computed(self):
        fixture = _fixture_capacity_catalog()
        expected_hash = hashlib.sha256(
            json.dumps(fixture, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        policies = [CapacityPolicy(agent_id=p["agent_id"], max_concurrent_jobs=p["max_concurrent_jobs"]) for p in fixture["policies"]]
        catalog = CapacityCatalog(
            source_id=fixture["source_id"],
            source_version=fixture["source_version"],
            catalog_hash="",
            source_path=None,
            policies=tuple(policies),
        )
        self.assertEqual(compute_capacity_catalog_hash(catalog), expected_hash)

    def test_policy_id_matches_cross_repository_fixture(self):
        fixture = _fixture_capacity_catalog()
        catalog_hash = hashlib.sha256(
            json.dumps(fixture, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        policy_id = compute_capacity_policy_id(
            agent_id="mac-claude",
            catalog_hash=catalog_hash,
            max_concurrent_jobs=1,
            source_id="multinexus.discord.capacity",
            source_version=1,
        )
        self.assertEqual(
            policy_id,
            "sha256:2bb3f41503e8eaf997269a2ee950f87a16d56cd2c2966f72c4207ab764355765",
        )

    def test_parse_accepts_full_shared_registry(self):
        path = _write_toml("""\
[registry]
id = "multinexus.discord"
version = 2

[[executor_definitions]]
id = "claude-code"
provider = "anthropic-claude"
adapter = "claude"
capabilities = ["coding"]

[[agents]]
id = "mac-claude"
display_name = "Mac Claude"
discord_user_id = "1001"
executor_definition_id = "claude-code"
runner_profile_id = "mac-claude"

[[external_agents]]
id = "server-hermes"
display_name = "Hermes"
discord_user_id = "1002"

[capacity_registry]
id = "multinexus.discord.capacity"
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 1
""")
        catalog = parse_capacity_catalog(path)
        self.assertEqual(catalog.source_id, "multinexus.discord.capacity")
        self.assertEqual(catalog.source_version, 1)
        expected_hash = compute_capacity_catalog_hash(catalog)
        self.assertEqual(catalog.catalog_hash, expected_hash)

    def test_parse_rejects_unknown_capacity_root_keys(self):
        path = _write_toml("""\
[capacity_registry]
id = "multinexus.discord.capacity"
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 1

[unknown]
""")
        with self.assertRaisesRegex(CapacityError, "unknown root keys"):
            parse_capacity_catalog(path)

    def test_parse_rejects_secret_bearing_root(self):
        path = _write_toml("""\
[capacity_registry]
id = "multinexus.discord.capacity"
version = 1

[secrets]
token = "x"

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 1
""")
        with self.assertRaisesRegex(CapacityError, "unknown root keys"):
            parse_capacity_catalog(path)

    def test_parse_rejects_non_string_source_id(self):
        path = _write_toml("""\
[capacity_registry]
id = 123
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 1
""")
        with self.assertRaisesRegex(CapacityError, "must be a string"):
            parse_capacity_catalog(path)

    def test_parse_rejects_whitespace_source_id(self):
        path = _write_toml("""\
[capacity_registry]
id = " x "
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 1
""")
        with self.assertRaisesRegex(CapacityError, "must not have surrounding whitespace"):
            parse_capacity_catalog(path)

        path = _write_toml("""\
[capacity_registry]
id = "multinexus.discord.capacity"
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 1

[unknown]
""")
        with self.assertRaisesRegex(CapacityError, "unknown root keys"):
            parse_capacity_catalog(path)

    def test_parse_rejects_duplicate_agent_id(self):
        path = _write_toml("""\
[capacity_registry]
id = "multinexus.discord.capacity"
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 2
""")
        with self.assertRaisesRegex(CapacityError, "duplicate executor_capacity agent_id"):
            parse_capacity_catalog(path)

    def test_parse_rejects_boolean_capacity(self):
        path = _write_toml("""\
[capacity_registry]
id = "multinexus.discord.capacity"
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = true
""")
        with self.assertRaisesRegex(CapacityError, "must be an integer"):
            parse_capacity_catalog(path)

    def test_parse_rejects_out_of_range_capacity(self):
        path = _write_toml("""\
[capacity_registry]
id = "multinexus.discord.capacity"
version = 1

[[executor_capacities]]
agent_id = "mac-claude"
max_concurrent_jobs = 33
""")
        with self.assertRaisesRegex(CapacityError, "between 1 and 32"):
            parse_capacity_catalog(path)


class CapacitySyncTests(unittest.TestCase):
    def _valid_capacity_catalog(self) -> CapacityCatalog:
        # Match the executor catalog bindings: mac-omp and mac-claude at capacity 1.
        policies = (CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1), CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        catalog = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path=None,
            policies=policies,
        )
        return catalog.__class__(
            source_id=catalog.source_id,
            source_version=catalog.source_version,
            catalog_hash=compute_capacity_catalog_hash(catalog),
            source_path=catalog.source_path,
            policies=catalog.policies,
        )

    def _valid_executor_catalog(self) -> ExecutorCatalog:
        return ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=2,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=(
                ExecutorInstanceBinding(agent_id="mac-omp", executor_definition_id="omp-code", runner_profile_id="mac-omp", enabled=True),
                ExecutorInstanceBinding(agent_id="mac-claude", executor_definition_id="omp-code", runner_profile_id="mac-claude", enabled=True),
            ),
        )

    def _seed_agents_and_profiles(self, conn: sqlite3.Connection) -> None:
        from coordinate.db_support import utc_now
        now = utc_now()
        for agent_id in ("mac-omp", "mac-claude"):
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

    def test_sync_succeeds_with_full_coverage(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        capacity_catalog = self._valid_capacity_catalog()
        result = sync_capacity_catalog(conn, capacity_catalog)
        self.assertTrue(result["changed"])
        self.assertEqual(len(list_capacity_policies(conn)), 2)
        policy = get_capacity_policy(conn, "mac-claude")
        self.assertIsNotNone(policy)
        self.assertEqual(policy["max_concurrent_jobs"], 1)

    def test_sync_fails_when_missing_capacity_for_enabled_binding(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        # Capacity catalog only covers some of the enabled bindings.
        partial = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path=None,
            policies=(CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),),
        )
        partial = partial.__class__(
            source_id=partial.source_id,
            source_version=partial.source_version,
            catalog_hash=compute_capacity_catalog_hash(partial),
            source_path=partial.source_path,
            policies=partial.policies,
        )
        with self.assertRaisesRegex(CapacityError, "missing for enabled typed agents"):
            sync_capacity_catalog(conn, partial)
        self.assertEqual(len(list_capacity_policies(conn)), 0)

    def test_sync_fails_when_capacity_for_untyped_agent(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        extra = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path=None,
            policies=(
                CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
                CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
                CapacityPolicy(agent_id="unknown", max_concurrent_jobs=1),
            ),
        )
        extra = extra.__class__(
            source_id=extra.source_id,
            source_version=extra.source_version,
            catalog_hash=compute_capacity_catalog_hash(extra),
            source_path=extra.source_path,
            policies=extra.policies,
        )
        with self.assertRaisesRegex(CapacityError, "present for unknown/untyped agents"):
            sync_capacity_catalog(conn, extra)
        self.assertEqual(len(list_capacity_policies(conn)), 0)

    def test_sync_same_version_same_hash_is_noop(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        capacity_catalog = self._valid_capacity_catalog()
        sync_capacity_catalog(conn, capacity_catalog)
        result = sync_capacity_catalog(conn, capacity_catalog)
        self.assertFalse(result["changed"])

    def test_sync_version_downgrade_fails(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        capacity_catalog = self._valid_capacity_catalog()
        sync_capacity_catalog(conn, capacity_catalog)
        downgraded = capacity_catalog.__class__(
            source_id=capacity_catalog.source_id,
            source_version=0,
            catalog_hash=capacity_catalog.catalog_hash,
            source_path=capacity_catalog.source_path,
            policies=capacity_catalog.policies,
        )
        with self.assertRaisesRegex(CapacityError, "version downgrade"):
            sync_capacity_catalog(conn, downgraded)

    def test_sync_same_version_different_hash_fails(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        capacity_catalog = self._valid_capacity_catalog()
        sync_capacity_catalog(conn, capacity_catalog)
        mutated = capacity_catalog.__class__(
            source_id=capacity_catalog.source_id,
            source_version=capacity_catalog.source_version,
            catalog_hash="0" * 64,
            source_path=capacity_catalog.source_path,
            policies=capacity_catalog.policies,
        )
        with self.assertRaisesRegex(CapacityError, "hash changed without version bump"):
            sync_capacity_catalog(conn, mutated)

    def test_sync_rejects_removal_of_policy_with_active_lease(self):
        from coordinate.db import create_job, get_workspace, get_workspace_host_profile, upsert_workspace_host_profile
        from coordinate.db_support import utc_now
        from coordinate.execution_context import resolve_execution_context_v1

        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        capacity_catalog = self._valid_capacity_catalog()
        sync_capacity_catalog(conn, capacity_catalog)

        # Create a job for mac-omp and reserve a lease so the policy is referenced.
        now = utc_now()
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("ws", "ws", "/tmp", "/tmp/docs", now, now),
        )
        conn.commit()
        upsert_workspace_host_profile(
            conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/tmp/ws",
            harness_root="/tmp/docs",
        )
        conn.commit()
        job = create_job(
            conn,
            workspace_id="ws",
            task_id=None,
            runner_profile_id="mac-omp",
            assigned_agent="mac-omp",
            payload={},
            worktree_path="/tmp/ws",
        )
        conn.execute("UPDATE jobs SET attempt_count = 1, assigned_agent = 'mac-omp', runner_profile_id = 'mac-omp' WHERE id = ?", (job["id"],))
        conn.commit()

        workspace = get_workspace(conn, "ws")
        profile = get_workspace_host_profile(conn, workspace_id="ws", host_id="host1")
        assert workspace is not None and profile is not None
        ctx = resolve_execution_context_v1(
            job_id=job["id"],
            workspace=workspace,
            task=None,
            assigned_agent="mac-omp",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "test"},
        )
        payload = {"execution_context": ctx.to_dict()}
        conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), job["id"]),
        )
        conn.commit()

        reserve_attempt_lease(
            conn,
            job_id=job["id"],
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws",
            ttl_seconds=60,
        )
        conn.commit()

        # Disable the mac-omp binding so the new catalog no longer needs to cover it,
        # but an active lease still references its policy.
        conn.execute(
            "UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?",
            ("mac-omp",),
        )
        conn.commit()

        # Attempt to sync a catalog that removes the mac-omp policy.
        reduced = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=2,
            catalog_hash="",
            source_path=None,
            policies=(CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),),
        )
        reduced = reduced.__class__(
            source_id=reduced.source_id,
            source_version=reduced.source_version,
            catalog_hash=compute_capacity_catalog_hash(reduced),
            source_path=reduced.source_path,
            policies=reduced.policies,
        )
        with self.assertRaisesRegex(CapacityError, "referenced by active lease"):
            sync_capacity_catalog(conn, reduced)
        self.assertIsNotNone(get_capacity_policy(conn, "mac-omp"))

    def test_exact_retry_validates_coverage_after_disabled_binding(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        capacity_catalog = self._valid_capacity_catalog()
        sync_capacity_catalog(conn, capacity_catalog)
        conn.commit()

        # Disable one binding; policies for disabled typed bindings are allowed,
        # so the exact retry succeeds (changed=false) and leaves rows untouched.
        conn.execute(
            "UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?",
            ("mac-omp",),
        )
        conn.commit()

        before_source = get_capacity_source(conn, capacity_catalog.source_id)
        before_policies = list_capacity_policies(conn)
        assert before_source is not None
        result = sync_capacity_catalog(conn, capacity_catalog)
        self.assertFalse(result["changed"])
        after_source = get_capacity_source(conn, capacity_catalog.source_id)
        after_policies = list_capacity_policies(conn)
        self.assertEqual(after_source, before_source)
        self.assertEqual(after_policies, before_policies)

    def test_exact_retry_validates_coverage_after_new_binding(self):
        conn = _make_conn()
        self._seed_agents_and_profiles(conn)
        # Start with only mac-omp enabled and covered.
        single_binding_executor = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=2,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=(
                ExecutorInstanceBinding(agent_id="mac-omp", executor_definition_id="omp-code", runner_profile_id="mac-omp", enabled=True),
            ),
        )
        single_binding_executor = single_binding_executor.__class__(
            source_id=single_binding_executor.source_id,
            source_version=single_binding_executor.source_version,
            catalog_hash=compute_executor_catalog_hash(single_binding_executor),
            source_path=single_binding_executor.source_path,
            definitions=single_binding_executor.definitions,
            bindings=single_binding_executor.bindings,
        )
        sync_executor_catalog(conn, single_binding_executor)
        single_capacity = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path=None,
            policies=(CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),),
        )
        single_capacity = single_capacity.__class__(
            source_id=single_capacity.source_id,
            source_version=single_capacity.source_version,
            catalog_hash=compute_capacity_catalog_hash(single_capacity),
            source_path=single_capacity.source_path,
            policies=single_capacity.policies,
        )
        sync_capacity_catalog(conn, single_capacity)
        conn.commit()

        # Add a second enabled typed binding without updating capacity.
        full_executor = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=3,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=(
                ExecutorInstanceBinding(agent_id="mac-omp", executor_definition_id="omp-code", runner_profile_id="mac-omp", enabled=True),
                ExecutorInstanceBinding(agent_id="mac-claude", executor_definition_id="omp-code", runner_profile_id="mac-claude", enabled=True),
            ),
        )
        full_executor = full_executor.__class__(
            source_id=full_executor.source_id,
            source_version=full_executor.source_version,
            catalog_hash=compute_executor_catalog_hash(full_executor),
            source_path=full_executor.source_path,
            definitions=full_executor.definitions,
            bindings=full_executor.bindings,
        )
        sync_executor_catalog(conn, full_executor)
        conn.commit()

        with self.assertRaisesRegex(CapacityError, "missing for enabled typed agents"):
            sync_capacity_catalog(conn, single_capacity)


class CapacitySourceDecouplingTests(unittest.TestCase):
    """Capacity-source decoupling invariants: disjoint ownership, union coverage,
    active-lease guards, and deterministic results."""

    def _seed_two_agents(self, conn: sqlite3.Connection) -> None:
        from coordinate.db_support import utc_now
        now = utc_now()
        for agent_id in ("mac-omp", "mac-claude"):
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

    def _sync_executor_catalog(self, conn: sqlite3.Connection, *agent_ids: str, source_version: int = 2) -> None:
        catalog = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=source_version,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=tuple(
                ExecutorInstanceBinding(
                    agent_id=agent_id,
                    executor_definition_id="omp-code",
                    runner_profile_id=agent_id,
                    enabled=True,
                )
                for agent_id in agent_ids
            ),
        )
        catalog = catalog.__class__(
            source_id=catalog.source_id,
            source_version=catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(catalog),
            source_path=catalog.source_path,
            definitions=catalog.definitions,
            bindings=catalog.bindings,
        )
        sync_executor_catalog(conn, catalog)

    def _capacity_catalog(self, source_id: str, source_version: int, *policies: CapacityPolicy) -> CapacityCatalog:
        catalog = CapacityCatalog(
            source_id=source_id,
            source_version=source_version,
            catalog_hash="",
            source_path=None,
            policies=policies,
        )
        return catalog.__class__(
            source_id=catalog.source_id,
            source_version=catalog.source_version,
            catalog_hash=compute_capacity_catalog_hash(catalog),
            source_path=catalog.source_path,
            policies=catalog.policies,
        )

    def _set_binding_enabled(self, conn: sqlite3.Connection, agent_id: str, enabled: bool) -> None:
        conn.execute(
            "UPDATE executor_instance_bindings SET enabled = ? WHERE agent_id = ?",
            (1 if enabled else 0, agent_id),
        )
        conn.commit()

    def _snapshot_capacity_rows(self, conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Snapshot all capacity source and policy rows, field-for-field."""
        return list_capacity_sources(conn), list_capacity_policies(conn)

    def test_two_disjoint_sources_cover_enabled_bindings(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        # Create both bindings, but keep mac-claude disabled while main covers mac-omp.
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        self._set_binding_enabled(conn, "mac-claude", False)
        main = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, main)
        # Enable mac-claude; fixture covers it. Union now covers both enabled bindings.
        self._set_binding_enabled(conn, "mac-claude", True)
        fixture = self._capacity_catalog(
            "fixture.capacity", 1,
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        result = sync_capacity_catalog(conn, fixture)
        self.assertTrue(result["changed"])
        self.assertEqual(result["added_policy_ids"], ["mac-claude"])
        policies = {p["agent_id"]: p["source_id"] for p in list_capacity_policies(conn)}
        self.assertEqual(policies, {"mac-claude": "fixture.capacity", "mac-omp": "multinexus.discord.capacity"})

    def test_partial_source_fails_when_union_misses_enabled_binding(self):
        conn = _make_conn()
        # Seed three agents; main covers mac-omp, fixture covers mac-claude,
        # leaving mac-extra enabled but uncovered.
        self._seed_two_agents(conn)
        _insert_agents(conn, "mac-extra")
        _insert_runner_profiles(conn, "mac-extra")
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude", "mac-extra")
        self._set_binding_enabled(conn, "mac-claude", False)
        self._set_binding_enabled(conn, "mac-extra", False)
        main = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, main)
        fixture = self._capacity_catalog(
            "fixture.capacity", 1,
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, fixture)
        # Enable mac-extra; no source covers it.
        self._set_binding_enabled(conn, "mac-extra", True)
        partial_fixture = self._capacity_catalog(
            "fixture.capacity", 2,
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "missing for enabled typed agents"):
            sync_capacity_catalog(conn, partial_fixture)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_cross_source_takeover_fails_with_zero_mutation(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        # Main source covers both agents.
        main = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, main)
        # Fixture source tries to take over mac-omp.
        takeover_fixture = self._capacity_catalog(
            "fixture.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=2),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "owned by source"):
            sync_capacity_catalog(conn, takeover_fixture)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_empty_fixture_source_succeeds_and_removes_only_its_policies(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        # Create both bindings, but keep mac-claude disabled while main covers mac-omp.
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        self._set_binding_enabled(conn, "mac-claude", False)
        main = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, main)
        # Enable mac-claude; fixture covers it.
        self._set_binding_enabled(conn, "mac-claude", True)
        fixture = self._capacity_catalog(
            "fixture.capacity", 1,
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, fixture)
        # Disable mac-claude, then empty the fixture source while main covers mac-omp.
        self._set_binding_enabled(conn, "mac-claude", False)
        empty_fixture = self._capacity_catalog("fixture.capacity", 2)
        result = sync_capacity_catalog(conn, empty_fixture)
        self.assertTrue(result["changed"])
        self.assertEqual(result["removed_policy_ids"], ["mac-claude"])
        policies = {p["agent_id"]: p["source_id"] for p in list_capacity_policies(conn)}
        self.assertEqual(policies, {"mac-omp": "multinexus.discord.capacity"})

    def test_empty_source_fails_union_coverage(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        # Main covers only mac-omp while mac-claude is disabled.
        self._set_binding_enabled(conn, "mac-claude", False)
        main = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, main)
        # Enable mac-claude; emptying main leaves mac-claude uncovered.
        self._set_binding_enabled(conn, "mac-claude", True)
        empty = self._capacity_catalog("multinexus.discord.capacity", 2)
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "missing for enabled typed agents"):
            sync_capacity_catalog(conn, empty)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def _reserve_active_lease(self, conn: sqlite3.Connection, agent_id: str) -> None:
        from coordinate.db import create_job, get_workspace, get_workspace_host_profile, upsert_workspace_host_profile
        from coordinate.db_support import utc_now
        from coordinate.execution_context import resolve_execution_context_v1
        now = utc_now()
        ws_id = f"ws-{agent_id}"
        host_id = f"host-{agent_id}"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (ws_id, ws_id, "/tmp", "/tmp/docs", now, now),
        )
        conn.commit()
        upsert_workspace_host_profile(
            conn,
            workspace_id=ws_id,
            host_id=host_id,
            workspace_path="/tmp/ws",
            harness_root="/tmp/docs",
        )
        conn.commit()
        job = create_job(
            conn,
            workspace_id=ws_id,
            task_id=None,
            runner_profile_id=agent_id,
            assigned_agent=agent_id,
            payload={},
            worktree_path="/tmp/ws",
        )
        conn.execute(
            "UPDATE jobs SET attempt_count = 1, assigned_agent = ?, runner_profile_id = ? WHERE id = ?",
            (agent_id, agent_id, job["id"]),
        )
        conn.commit()
        workspace = get_workspace(conn, ws_id)
        profile = get_workspace_host_profile(conn, workspace_id=ws_id, host_id=host_id)
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
        reserve_attempt_lease(
            conn,
            job_id=job["id"],
            attempt_token=1,
            agent_id=agent_id,
            runner_profile_id=agent_id,
            host_id=host_id,
            worktree_path="/tmp/ws",
            ttl_seconds=60,
        )
        conn.commit()

    def _reserve_active_leases(self, conn: sqlite3.Connection, *agent_ids: str) -> None:
        for agent_id in agent_ids:
            self._reserve_active_lease(conn, agent_id)

    def test_active_lease_replacement_by_version_bump_fails_zero_mutation(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        self._reserve_active_lease(conn, "mac-omp")
        bumped = self._capacity_catalog(
            "multinexus.discord.capacity", 2,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "referenced by active lease"):
            sync_capacity_catalog(conn, bumped)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_active_lease_replacement_by_capacity_change_fails_zero_mutation(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        self._reserve_active_lease(conn, "mac-omp")
        changed = self._capacity_catalog(
            "multinexus.discord.capacity", 2,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=2),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "referenced by active lease"):
            sync_capacity_catalog(conn, changed)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_active_lease_removal_fails_zero_mutation(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        self._reserve_active_lease(conn, "mac-omp")
        # Disable mac-omp so the new catalog doesn't need to cover it, but an
        # active lease still references its old policy id.
        conn.execute("UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?", ("mac-omp",))
        conn.commit()
        reduced = self._capacity_catalog(
            "multinexus.discord.capacity", 2,
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "referenced by active lease"):
            sync_capacity_catalog(conn, reduced)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_exact_retry_revalidates_ownership_drift(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        # Another source takes over mac-claude outside the catalog (simulated drift).
        conn.execute(
            "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("fixture.capacity", 1, "0" * 64, None, "2026-01-01T00:00:00Z"),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET source_id = ?, source_version = ?, catalog_hash = ?, capacity_policy_id = ? "
            "WHERE agent_id = ?",
            ("fixture.capacity", 1, "0" * 64, "sha256:" + "0" * 64, "mac-claude"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "owned by source"):
            sync_capacity_catalog(conn, catalog)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_exact_retry_revalidates_unknown_binding_drift(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        # Drop mac-claude binding outside the catalog (simulated drift).
        conn.execute("DELETE FROM executor_instance_bindings WHERE agent_id = ?", ("mac-claude",))
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "present for unknown/untyped agents"):
            sync_capacity_catalog(conn, catalog)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_result_fields_are_deterministically_sorted(self):
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        result = sync_capacity_catalog(conn, catalog)
        self.assertEqual(result["added_policy_ids"], ["mac-claude", "mac-omp"])
        self.assertEqual(result["removed_policy_ids"], [])
        self.assertEqual(result["updated_policy_ids"], [])
        self.assertEqual(result["unchanged_policy_ids"], [])

    def test_multiple_active_lease_replacements_report_smallest_agent_id(self):
        """When multiple old policy ids are blocked, report the lexicographically
        smallest agent_id deterministically.
        """
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        self._reserve_active_leases(conn, "mac-omp", "mac-claude")
        bumped = self._capacity_catalog(
            "multinexus.discord.capacity", 2,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "cannot replace capacity policy for 'mac-claude': referenced by active lease"):
            sync_capacity_catalog(conn, bumped)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_multiple_cross_source_takeovers_report_smallest_agent_id(self):
        """When another source tries to take over multiple agent_ids at once,
        ownership failure reports the lexicographically smallest agent_id and
        leaves every capacity row untouched.
        """
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        main = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, main)
        # Fixture source attempts takeovers in non-sorted order.
        takeover_fixture = self._capacity_catalog(
            "fixture.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=2),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=2),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(
            CapacityError,
            "capacity agent_id 'mac-claude' is owned by source 'multinexus.discord.capacity'",
        ):
            sync_capacity_catalog(conn, takeover_fixture)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_version_downgrade_precedes_coverage_drift(self):
        """Even when a new enabled typed binding is uncovered, downgrade is
        reported before the coverage error.
        """
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 2,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        # Add a third enabled typed binding (mac-extra) without a capacity policy,
        # creating a real coverage drift.
        _insert_agents(conn, "mac-extra")
        _insert_runner_profiles(conn, "mac-extra")
        extra_executor = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=3,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=(
                ExecutorInstanceBinding(agent_id="mac-omp", executor_definition_id="omp-code", runner_profile_id="mac-omp", enabled=True),
                ExecutorInstanceBinding(agent_id="mac-claude", executor_definition_id="omp-code", runner_profile_id="mac-claude", enabled=True),
                ExecutorInstanceBinding(agent_id="mac-extra", executor_definition_id="omp-code", runner_profile_id="mac-extra", enabled=True),
            ),
        )
        extra_executor = extra_executor.__class__(
            source_id=extra_executor.source_id,
            source_version=extra_executor.source_version,
            catalog_hash=compute_executor_catalog_hash(extra_executor),
            source_path=extra_executor.source_path,
            definitions=extra_executor.definitions,
            bindings=extra_executor.bindings,
        )
        sync_executor_catalog(conn, extra_executor)
        # Request a version downgrade; coverage drift must not be reported first.
        downgrade = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "version downgrade"):
            sync_capacity_catalog(conn, downgrade)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_same_version_different_hash_precedes_known_binding_drift(self):
        """Same-version/different-hash is reported before the known-binding
        invariant failure caused by deleting a typed binding.
        """
        conn = _make_conn()
        self._seed_two_agents(conn)
        self._sync_executor_catalog(conn, "mac-omp", "mac-claude")
        catalog = self._capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)
        # Delete mac-omp binding outside the catalog to create a known-binding drift.
        conn.execute("DELETE FROM executor_instance_bindings WHERE agent_id = ?", ("mac-omp",))
        conn.commit()
        mutated = catalog.__class__(
            source_id=catalog.source_id,
            source_version=catalog.source_version,
            catalog_hash="0" * 64,
            source_path=catalog.source_path,
            policies=catalog.policies,
        )
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "hash changed without version bump"):
            sync_capacity_catalog(conn, mutated)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)


def _insert_agents(conn: sqlite3.Connection, *agent_ids: str) -> None:
    from coordinate.db_support import utc_now
    now = utc_now()
    for agent_id in agent_ids:
        conn.execute(
            "INSERT INTO agents (id, name, role, capabilities_json, online_state, current_load, client_type, created_at, updated_at) "
            "VALUES (?, ?, 'agent', '[]', 'offline', 0, 'agentd', ?, ?)",
            (agent_id, agent_id, now, now),
        )
    conn.commit()


def _insert_runner_profiles(conn: sqlite3.Connection, *profile_ids: str) -> None:
    from coordinate.db_support import utc_now
    now = utc_now()
    for profile_id in profile_ids:
        conn.execute(
            "INSERT INTO runner_profiles (id, name, runner_type, command, working_directory_strategy, supports_stream_attach, env_json, created_at, updated_at) "
            "VALUES (?, ?, 'agentd', 'agent', 'current_dir', 0, '{}', ?, ?)",
            (profile_id, profile_id, now, now),
        )
    conn.commit()


def _sync_minimal_executor_catalog(conn: sqlite3.Connection, agent_id: str) -> None:
    catalog = ExecutorCatalog(
        source_id="multinexus.discord",
        source_version=2,
        catalog_hash="",
        source_path=None,
        definitions=(
            ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
        ),
        bindings=(
            ExecutorInstanceBinding(
                agent_id=agent_id,
                executor_definition_id="omp-code",
                runner_profile_id=agent_id,
                enabled=True,
            ),
        ),
    )
    sync_executor_catalog(conn, catalog)


class CapacityCLIDelegationTests(unittest.TestCase):
    """Thin CLI handler tests for ``runtime capacity`` commands."""

    def _args(self, **kwargs) -> SimpleNamespace:
        return SimpleNamespace(**kwargs)

    def _capture(self, func, args) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = func(args)
        return code, stdout.getvalue(), stderr.getvalue()

    def _capture_json(self, func, args) -> tuple[int, dict]:
        code, stdout, _stderr = self._capture(func, args)
        return code, json.loads(stdout)

    def _seed_capacity(self, conn: sqlite3.Connection, agent_id: str, content: str) -> None:
        """Set up a minimal executor catalog, agent, runner profile, and capacity policy."""
        _insert_agents(conn, agent_id)
        _insert_runner_profiles(conn, agent_id)
        _sync_minimal_executor_catalog(conn, agent_id)
        tmp = _write_toml(content)
        catalog = parse_capacity_catalog(tmp)
        sync_capacity_catalog(conn, catalog)

    def test_capacity_sync_delegation(self) -> None:
        tmp = _write_toml(
            """
            [capacity_registry]
            id = "multinexus.discord.capacity"
            version = 1
            [[executor_capacities]]
            agent_id = "mac-omp"
            max_concurrent_jobs = 1
            """
        )
        conn = _make_conn()
        _insert_agents(conn, "mac-omp")
        _insert_runner_profiles(conn, "mac-omp")
        _sync_minimal_executor_catalog(conn, "mac-omp")
        args = self._args(source=str(tmp), synced_by="cli-test", db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, payload = self._capture_json(handle_runtime_capacity_sync, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload["source_id"], "multinexus.discord.capacity")
        self.assertEqual(payload["source_version"], 1)
        self.assertEqual(payload["synced_by"], "cli-test")

    def test_capacity_sync_rejects_invalid_source_path(self) -> None:
        args = self._args(source="/nonexistent/path.toml", synced_by="cli-test", db=":memory:")
        code, _stdout, stderr = self._capture(handle_runtime_capacity_sync, args)
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)

    def test_capacity_list_delegation(self) -> None:
        conn = _make_conn()
        self._seed_capacity(
            conn,
            "mac-omp",
            """
            [capacity_registry]
            id = "multinexus.discord.capacity"
            version = 1
            [[executor_capacities]]
            agent_id = "mac-omp"
            max_concurrent_jobs = 1
            """,
        )
        args = self._args(db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, payload = self._capture_json(handle_runtime_capacity_list, args)
        self.assertEqual(code, 0)
        self.assertEqual(len(payload["sources"]), 1)
        self.assertEqual(len(payload["policies"]), 1)
        self.assertEqual(payload["policies"][0]["agent_id"], "mac-omp")

    def test_capacity_show_delegation(self) -> None:
        conn = _make_conn()
        self._seed_capacity(
            conn,
            "mac-omp",
            """
            [capacity_registry]
            id = "multinexus.discord.capacity"
            version = 1
            [[executor_capacities]]
            agent_id = "mac-omp"
            max_concurrent_jobs = 1
            """,
        )
        args = self._args(agent_id="mac-omp", db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, payload = self._capture_json(handle_runtime_capacity_show, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload["agent_id"], "mac-omp")
        self.assertEqual(payload["policy"]["agent_id"], "mac-omp")

    def test_capacity_show_unknown_agent_returns_error(self) -> None:
        conn = _make_conn()
        args = self._args(agent_id="unknown-agent", db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, _stdout, stderr = self._capture(handle_runtime_capacity_show, args)
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)

    def test_capacity_sync_rejects_unknown_agent_with_error(self) -> None:
        conn = _make_conn()
        _insert_agents(conn, "mac-omp")
        _insert_runner_profiles(conn, "mac-omp")
        _sync_minimal_executor_catalog(conn, "mac-omp")
        tmp = _write_toml(
            """
            [capacity_registry]
            id = "multinexus.discord.capacity"
            version = 1
            [[executor_capacities]]
            agent_id = "mac-omp"
            max_concurrent_jobs = 1
            [[executor_capacities]]
            agent_id = "unknown-agent"
            max_concurrent_jobs = 1
            """
        )
        args = self._args(source=str(tmp), synced_by="cli-test", db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, _stdout, stderr = self._capture(handle_runtime_capacity_sync, args)
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)
        self.assertIn("unknown/untyped", stderr)

    def test_capacity_sync_rejects_union_coverage_miss_with_error(self) -> None:
        conn = _make_conn()
        _insert_agents(conn, "mac-omp", "mac-claude")
        _insert_runner_profiles(conn, "mac-omp", "mac-claude")
        _sync_minimal_executor_catalog(conn, "mac-omp")
        # Bind mac-claude via a separate executor catalog version.
        second = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=3,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=(
                ExecutorInstanceBinding(agent_id="mac-omp", executor_definition_id="omp-code", runner_profile_id="mac-omp", enabled=True),
                ExecutorInstanceBinding(agent_id="mac-claude", executor_definition_id="omp-code", runner_profile_id="mac-claude", enabled=True),
            ),
        )
        second = second.__class__(
            source_id=second.source_id,
            source_version=second.source_version,
            catalog_hash=compute_executor_catalog_hash(second),
            source_path=second.source_path,
            definitions=second.definitions,
            bindings=second.bindings,
        )
        sync_executor_catalog(conn, second)
        # Capacity catalog only covers mac-omp.
        tmp = _write_toml(
            """
            [capacity_registry]
            id = "multinexus.discord.capacity"
            version = 1
            [[executor_capacities]]
            agent_id = "mac-omp"
            max_concurrent_jobs = 1
            """
        )
        args = self._args(source=str(tmp), synced_by="cli-test", db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, _stdout, stderr = self._capture(handle_runtime_capacity_sync, args)
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)
        self.assertIn("missing for enabled typed agents", stderr)

    def test_capacity_sync_rejects_cross_source_takeover_with_error(self) -> None:
        conn = _make_conn()
        _insert_agents(conn, "mac-omp")
        _insert_runner_profiles(conn, "mac-omp")
        _sync_minimal_executor_catalog(conn, "mac-omp")
        # Pre-seed a policy for mac-omp owned by another source.
        _seed_capacity_catalog = CapacityCatalog(
            source_id="fixture.capacity",
            source_version=1,
            catalog_hash="",
            source_path=None,
            policies=(CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),),
        )
        _seed_capacity_catalog = _seed_capacity_catalog.__class__(
            source_id=_seed_capacity_catalog.source_id,
            source_version=_seed_capacity_catalog.source_version,
            catalog_hash=compute_capacity_catalog_hash(_seed_capacity_catalog),
            source_path=_seed_capacity_catalog.source_path,
            policies=_seed_capacity_catalog.policies,
        )
        sync_capacity_catalog(conn, _seed_capacity_catalog)
        # CLI tries to sync the same agent_id from the canonical source.
        tmp = _write_toml(
            """
            [capacity_registry]
            id = "multinexus.discord.capacity"
            version = 1
            [[executor_capacities]]
            agent_id = "mac-omp"
            max_concurrent_jobs = 1
            """
        )
        args = self._args(source=str(tmp), synced_by="cli-test", db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, _stdout, stderr = self._capture(handle_runtime_capacity_sync, args)
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)
        self.assertIn("owned by source", stderr)

    def test_capacity_sync_rejects_active_lease_policy_replacement_with_error(self) -> None:
        """CLI propagates active-lease replacement failure concisely."""
        from coordinate.db import create_job, get_workspace, get_workspace_host_profile, upsert_workspace_host_profile
        from coordinate.db_support import utc_now
        from coordinate.execution_context import resolve_execution_context_v1

        def _make_capacity_catalog(
            source_id: str, source_version: int, *policies: CapacityPolicy
        ) -> CapacityCatalog:
            catalog = CapacityCatalog(
                source_id=source_id,
                source_version=source_version,
                catalog_hash="",
                source_path=None,
                policies=policies,
            )
            return catalog.__class__(
                source_id=catalog.source_id,
                source_version=catalog.source_version,
                catalog_hash=compute_capacity_catalog_hash(catalog),
                source_path=catalog.source_path,
                policies=catalog.policies,
            )

        conn = _make_conn()
        _insert_agents(conn, "mac-omp")
        _insert_runner_profiles(conn, "mac-omp")
        _sync_minimal_executor_catalog(conn, "mac-omp")
        catalog = _make_capacity_catalog(
            "multinexus.discord.capacity", 1,
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
        )
        sync_capacity_catalog(conn, catalog)

        # Reserve an active lease for mac-omp.
        now = utc_now()
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("ws", "ws", "/tmp", "/tmp/docs", now, now),
        )
        conn.commit()
        upsert_workspace_host_profile(
            conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/tmp/ws",
            harness_root="/tmp/docs",
        )
        conn.commit()
        job = create_job(
            conn,
            workspace_id="ws",
            task_id=None,
            runner_profile_id="mac-omp",
            assigned_agent="mac-omp",
            payload={},
            worktree_path="/tmp/ws",
        )
        conn.execute(
            "UPDATE jobs SET attempt_count = 1, assigned_agent = ?, runner_profile_id = ? WHERE id = ?",
            ("mac-omp", "mac-omp", job["id"]),
        )
        conn.commit()
        workspace = get_workspace(conn, "ws")
        profile = get_workspace_host_profile(conn, workspace_id="ws", host_id="host1")
        assert workspace is not None and profile is not None
        ctx = resolve_execution_context_v1(
            job_id=job["id"],
            workspace=workspace,
            task=None,
            assigned_agent="mac-omp",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "test"},
        )
        payload = {"execution_context": ctx.to_dict()}
        conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), job["id"]),
        )
        conn.commit()
        reserve_attempt_lease(
            conn,
            job_id=job["id"],
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws",
            ttl_seconds=60,
        )
        conn.commit()

        # Version bump would replace the active lease's exact capacity_policy_id.
        tmp = _write_toml(
            """
            [capacity_registry]
            id = "multinexus.discord.capacity"
            version = 2
            [[executor_capacities]]
            agent_id = "mac-omp"
            max_concurrent_jobs = 1
            """
        )
        args = self._args(source=str(tmp), synced_by="cli-test", db=":memory:")
        with patch("coordinate.execution_cli._conn") as mock_conn:
            mock_conn.return_value.__enter__.return_value = conn
            code, _stdout, stderr = self._capture(handle_runtime_capacity_sync, args)
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)
        self.assertIn("referenced by active lease", stderr)


class CapacitySnapshotTests(unittest.TestCase):
    """Digest-bound capacity-only snapshot capture/restore tests."""

    def _make_conn(self) -> sqlite3.Connection:
        return _make_conn()

    def _seed_agents_and_profiles(self, conn: sqlite3.Connection) -> None:
        from coordinate.db_support import utc_now
        now = utc_now()
        for agent_id in ("mac-omp", "mac-claude"):
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

    def _valid_executor_catalog(self) -> ExecutorCatalog:
        return ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=2,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=(
                ExecutorInstanceBinding(agent_id="mac-omp", executor_definition_id="omp-code", runner_profile_id="mac-omp", enabled=True),
                ExecutorInstanceBinding(agent_id="mac-claude", executor_definition_id="omp-code", runner_profile_id="mac-claude", enabled=True),
            ),
        )

    def _sync_capacity(self, conn: sqlite3.Connection) -> None:
        executor_catalog = self._valid_executor_catalog()
        executor_catalog = executor_catalog.__class__(
            source_id=executor_catalog.source_id,
            source_version=executor_catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(executor_catalog),
            source_path=executor_catalog.source_path,
            definitions=executor_catalog.definitions,
            bindings=executor_catalog.bindings,
        )
        sync_executor_catalog(conn, executor_catalog)
        policies = (
            CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1),
            CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1),
        )
        catalog = CapacityCatalog(
            source_id=EXPECTED_CAPACITY_SOURCE_ID,
            source_version=1,
            catalog_hash="",
            source_path=None,
            policies=policies,
        )
        catalog = catalog.__class__(
            source_id=catalog.source_id,
            source_version=catalog.source_version,
            catalog_hash=compute_capacity_catalog_hash(catalog),
            source_path=catalog.source_path,
            policies=catalog.policies,
        )
        sync_capacity_catalog(conn, catalog)
        conn.commit()

    def _expected_prior_absence_envelope(self) -> dict[str, Any]:
        inner = {
            "contract_version": 2,
            "target_source_id": EXPECTED_CAPACITY_SOURCE_ID,
            "captured_state": None,
            "preserved_state": {"sources": [], "policies": []},
        }
        digest = hashlib.sha256(
            json.dumps(inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return {
            "snapshot": inner,
            "snapshot_sha256": digest,
        }

    def test_capture_prior_absence_exact_bytes_and_digest(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "prior_absence.json"
        envelope = capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        expected = self._expected_prior_absence_envelope()
        self.assertEqual(envelope, expected)
        self.assertEqual(
            snapshot_path.read_bytes(),
            json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)

    def test_capture_existing_capacity_exact_bytes_and_digest(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        # Fix timestamps so bytes are deterministic.
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "existing.json"
        envelope = capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        source = get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID)
        assert source is not None
        policies = list_capacity_policies(conn, EXPECTED_CAPACITY_SOURCE_ID)
        expected_inner = {
            "contract_version": 2,
            "target_source_id": EXPECTED_CAPACITY_SOURCE_ID,
            "captured_state": {
                "source": {
                    "source_id": EXPECTED_CAPACITY_SOURCE_ID,
                    "source_version": 1,
                    "catalog_hash": source["catalog_hash"],
                    "source_path": source["source_path"],
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                "policies": sorted(
                    [
                        {
                            "agent_id": p["agent_id"],
                            "source_id": EXPECTED_CAPACITY_SOURCE_ID,
                            "source_version": 1,
                            "catalog_hash": p["catalog_hash"],
                            "capacity_policy_id": p["capacity_policy_id"],
                            "max_concurrent_jobs": p["max_concurrent_jobs"],
                            "created_at": "2026-01-01T00:00:00Z",
                            "updated_at": "2026-01-01T00:00:00Z",
                        }
                        for p in policies
                    ],
                    key=lambda p: p["agent_id"],
                ),
            },
            "preserved_state": {"sources": [], "policies": []},
        }
        expected_digest = hashlib.sha256(
            json.dumps(expected_inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        expected_envelope = {
            "snapshot": expected_inner,
            "snapshot_sha256": expected_digest,
        }
        self.assertEqual(envelope, expected_envelope)
        self.assertEqual(
            snapshot_path.read_bytes(),
            json.dumps(expected_envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)

    def test_capture_rejects_unexpected_capacity_source(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        conn.execute(
            "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("unexpected.other.capacity", 1, "0" * 64, None, "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "unexpected.json"
        # With v2, other sources become preserved witnesses. The unexpected source
        # is not rejected; it is captured in preserved_state.
        envelope = capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        preserved_sources = envelope["snapshot"]["preserved_state"]["sources"]
        self.assertEqual([s["source_id"] for s in preserved_sources], ["unexpected.other.capacity"])
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)

    def test_capture_rejects_mismatched_policy_id(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        conn.execute(
            "UPDATE executor_capacity_policies SET capacity_policy_id = ? WHERE agent_id = ?",
            ("sha256:" + "0" * 64, "mac-omp"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "mismatch.json"
        with self.assertRaisesRegex(CapacityError, "capacity_policy_id mismatch"):
            capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_existing_capacity_exact_match(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "existing.json"
        envelope = capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        # Mutate the DB to a different but internally consistent state
        # (both source_version and policy_ids must be consistent so the
        # current-DB strict pre-delete validation still passes).
        new_hash = "1" * 64
        new_version = 99
        conn.execute(
            "UPDATE executor_capacity_sources SET source_version = ?, catalog_hash = ?",
            (new_version, new_hash),
        )
        # Recompute policy_ids for the new hash/version.
        for agent_id in ("mac-omp", "mac-claude"):
            new_pid = compute_capacity_policy_id(
                agent_id=agent_id,
                catalog_hash=new_hash,
                max_concurrent_jobs=1,
                source_id=EXPECTED_CAPACITY_SOURCE_ID,
                source_version=new_version,
            )
            conn.execute(
                "UPDATE executor_capacity_policies SET source_version = ?, catalog_hash = ?, capacity_policy_id = ? "
                "WHERE agent_id = ?",
                (new_version, new_hash, new_pid, agent_id),
            )
        conn.commit()

        restored = restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        self.assertEqual(restored, envelope)
        source = get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID)
        self.assertIsNotNone(source)
        self.assertEqual(source["source_version"], 1)
        self.assertEqual(source["catalog_hash"], envelope["snapshot"]["captured_state"]["source"]["catalog_hash"])
        policies = list_capacity_policies(conn, EXPECTED_CAPACITY_SOURCE_ID)
        self.assertEqual(len(policies), 2)
        for p in policies:
            self.assertEqual(p["source_version"], 1)
            self.assertEqual(p["created_at"], "2026-01-01T00:00:00Z")

    def test_restore_prior_absence_deletes_new_capacity(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "prior_absence.json"
        capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        # Simulate first rollout: sync new capacity.
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        self.assertIsNotNone(get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID))

        # Disable all typed bindings so prior-absence restore does not fail coverage.
        conn.execute("UPDATE executor_instance_bindings SET enabled = 0")
        conn.commit()

        # Restore prior absence.
        restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        self.assertIsNone(get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID))
        self.assertEqual(len(list_capacity_policies(conn, EXPECTED_CAPACITY_SOURCE_ID)), 0)

    def test_restore_rejects_active_lease(self):
        from coordinate.db import create_job, get_workspace, get_workspace_host_profile, upsert_workspace_host_profile
        from coordinate.db_support import utc_now
        from coordinate.execution_context import resolve_execution_context_v1
        from coordinate.execution_leases import reserve_attempt_lease

        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "existing.json"
        capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        # Create a job and reserve an active lease.
        now = utc_now()
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("ws", "ws", "/tmp", "/tmp/docs", now, now),
        )
        conn.commit()
        upsert_workspace_host_profile(
            conn,
            workspace_id="ws",
            host_id="host1",
            workspace_path="/tmp/ws",
            harness_root="/tmp/docs",
        )
        conn.commit()
        job = create_job(
            conn,
            workspace_id="ws",
            task_id=None,
            runner_profile_id="mac-omp",
            assigned_agent="mac-omp",
            payload={},
            worktree_path="/tmp/ws",
        )
        conn.execute("UPDATE jobs SET attempt_count = 1, assigned_agent = 'mac-omp', runner_profile_id = 'mac-omp' WHERE id = ?", (job["id"],))
        conn.commit()

        workspace = get_workspace(conn, "ws")
        profile = get_workspace_host_profile(conn, workspace_id="ws", host_id="host1")
        assert workspace is not None and profile is not None
        ctx = resolve_execution_context_v1(
            job_id=job["id"],
            workspace=workspace,
            task=None,
            assigned_agent="mac-omp",
            host_id="host1",
            profile=profile,
            origin={"session_scope_id": "test"},
        )
        payload = {"execution_context": ctx.to_dict()}
        conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), job["id"]),
        )
        conn.commit()

        reserve_attempt_lease(
            conn,
            job_id=job["id"],
            attempt_token=1,
            agent_id="mac-omp",
            runner_profile_id="mac-omp",
            host_id="host1",
            worktree_path="/tmp/ws",
            ttl_seconds=60,
        )
        conn.commit()

        with self.assertRaisesRegex(CapacityError, "active lease"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        # DB should remain unchanged.
        self.assertIsNotNone(get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID))

    def test_restore_rejects_tampered_digest(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "tampered.json"
        envelope = capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        envelope["snapshot_sha256"] = "0" * 64
        snapshot_path.write_text(
            json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(CapacityError, "digest mismatch"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_wrong_target_source_id(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "wrong.json"
        envelope = capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        inner = envelope["snapshot"]
        inner["target_source_id"] = "other.capacity"
        new_digest = hashlib.sha256(
            json.dumps(inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        envelope["snapshot_sha256"] = new_digest
        snapshot_path.write_text(
            json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(CapacityError, "target_source_id mismatch"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_malformed_envelope(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "malformed.json"
        snapshot_path.write_text("not json", encoding="utf-8")
        with self.assertRaisesRegex(CapacityError, "malformed JSON"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_unexpected_capacity_sources(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "existing.json"
        capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        conn.execute(
            "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("unexpected.other.capacity", 1, "0" * 64, None, "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        with self.assertRaisesRegex(CapacityError, "witness mismatch|unexpected capacity sources"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    # --- R3-1: strict validation adversarial tests ---

    def _capture_existing(self):
        """Capture a valid existing-capacity snapshot with fixed timestamps."""
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "existing.json"
        envelope = capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        return conn, snapshot_path, envelope

    def _write_adversarial(self, snapshot_path, inner):
        """Write a snapshot with arbitrary inner state and a correctly recomputed digest."""
        digest = hashlib.sha256(
            json.dumps(inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        envelope = {"snapshot": inner, "snapshot_sha256": digest}
        snapshot_path.write_bytes(
            json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )

    def test_restore_rejects_noncanonical_raw_bytes(self):
        conn, snapshot_path, envelope = self._capture_existing()
        raw = snapshot_path.read_bytes()
        parsed = json.loads(raw)
        pretty = json.dumps(parsed, indent=2)
        snapshot_path.write_text(pretty, encoding="utf-8")
        with self.assertRaisesRegex(CapacityError, "not canonical"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        self.assertIsNotNone(get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID))

    def test_restore_rejects_unknown_field_in_captured_state(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        inner["captured_state"]["extra"] = True
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "unknown or missing keys"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_unknown_field_in_source(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        inner["captured_state"]["source"]["extra"] = True
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "unknown or missing keys"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_unknown_field_in_policy(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        inner["captured_state"]["policies"][0]["extra"] = True
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "unknown or missing keys"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_modified_source_id_with_valid_digest(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        inner["captured_state"]["source"]["source_id"] = "other.capacity"
        for p in inner["captured_state"]["policies"]:
            p["source_id"] = "other.capacity"
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "source_id mismatch"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_modified_source_version_with_valid_digest(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        inner["captured_state"]["source"]["source_version"] = 99
        for p in inner["captured_state"]["policies"]:
            p["source_version"] = 99
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "capacity_policy_id mismatch"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_unsorted_policies_with_valid_digest(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        polys = inner["captured_state"]["policies"]
        inner["captured_state"]["policies"] = [polys[1], polys[0]]
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "not strictly increasing"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_duplicate_agent_policies_with_valid_digest(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        polys = inner["captured_state"]["policies"]
        inner["captured_state"]["policies"] = [polys[0], polys[0]]
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "not strictly increasing"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_policy_id_tamper_with_valid_digest(self):
        conn, snapshot_path, envelope = self._capture_existing()
        inner = json.loads(json.dumps(envelope["snapshot"]))
        inner["captured_state"]["policies"][0]["capacity_policy_id"] = "sha256:" + "f" * 64
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "capacity_policy_id mismatch"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_orphan_policy_in_current_db(self):
        conn, snapshot_path, envelope = self._capture_existing()
        # Insert orphan policy (source_id does not match target).
        conn.execute(
            "INSERT INTO executor_capacity_policies "
            "(agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("orphan-agent", "other.source", 1, "0" * 64, "sha256:" + "0" * 64, 1,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        with self.assertRaisesRegex(CapacityError, "orphan current policy|orphan policies from other sources"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        # Orphan must survive (zero-write before DELETE).
        row = conn.execute(
            "SELECT agent_id FROM executor_capacity_policies WHERE agent_id = ?",
            ("orphan-agent",),
        ).fetchone()
        self.assertIsNotNone(row)

    def test_restore_rejects_mismatched_policy_in_current_db(self):
        conn, snapshot_path, envelope = self._capture_existing()
        # Corrupt a policy's source_version so it mismatches the current source.
        conn.execute(
            "UPDATE executor_capacity_policies SET source_version = 777 WHERE agent_id = ?",
            ("mac-omp",),
        )
        conn.commit()
        with self.assertRaisesRegex(CapacityError, r"current policy\[1\] source_version mismatch|source_version != current source"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        # The corrupt row must survive (zero-write before DELETE).
        row = conn.execute(
            "SELECT source_version FROM executor_capacity_policies WHERE agent_id = ?",
            ("mac-omp",),
        ).fetchone()
        self.assertEqual(row["source_version"], 777)

    def test_capture_rejects_orphan_policy_when_source_absent(self):
        conn = self._make_conn()
        conn.execute(
            "INSERT INTO executor_capacity_policies "
            "(agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("orphan", "other.source", 1, "0" * 64, "sha256:" + "0" * 64, 1,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "orphan.json"
        with self.assertRaisesRegex(CapacityError, "orphan"):
            capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        self.assertFalse(snapshot_path.exists())

    def test_capture_rejects_coverage_drift(self):
        conn, _snapshot_path, _envelope = self._capture_existing()
        # Add a third enabled typed binding with no capacity policy.
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO agents (id, name, role, capabilities_json, online_state, current_load, client_type, created_at, updated_at) "
            "VALUES ('mac-new', 'Mac New', 'agent', '[]', 'offline', 0, 'agentd', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO runner_profiles (id, name, runner_type, command, working_directory_strategy, supports_stream_attach, env_json, created_at, updated_at) "
            "VALUES ('mac-new', 'mac-new', 'agentd', 'agent', 'current_dir', 0, '{}', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO executor_instance_bindings (agent_id, source_id, executor_definition_id, runner_profile_id, enabled, created_at, updated_at) "
            "VALUES ('mac-new', 'multinexus.discord', 'omp-code', 'mac-new', 1, ?, ?)",
            (now, now),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "drift.json"
        with self.assertRaisesRegex(CapacityError, "coverage drift"):
            capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        self.assertFalse(snapshot_path.exists())

    def test_capture_commit_failure_cleans_up_file(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "commit_fail.json"

        class _CommitFailConn:
            def __init__(self, real):
                self._real = real
            def commit(self):
                raise sqlite3.OperationalError("injected commit failure")
            def __getattr__(self, name):
                return getattr(self._real, name)

        wrapper = _CommitFailConn(conn)
        with self.assertRaises(sqlite3.OperationalError):
            capture_capacity_snapshot(wrapper, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        self.assertFalse(snapshot_path.exists(), "snapshot file must be cleaned up on commit failure")

    # --- C5-1: snapshot coverage drift adversarial tests ---

    def test_restore_rejects_snapshot_coverage_missing_policy(self):
        """C5-1: Removing one otherwise-valid policy from snapshot, recomputing digest,
        must reject with zero writes — DB fully unchanged."""
        conn, snapshot_path, _envelope = self._capture_existing()
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        # Remove mac-claude policy.
        inner["captured_state"]["policies"] = [
            p for p in inner["captured_state"]["policies"]
            if p["agent_id"] != "mac-claude"
        ]
        self._write_adversarial(snapshot_path, inner)

        # Snapshot pre-state.
        pre_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())
        pre_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "snapshot coverage drift"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        # Prove zero writes — every row identical.
        post_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())
        post_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchall()]
        self.assertEqual(pre_source, post_source)
        self.assertEqual(pre_policies, post_policies)

    def test_restore_rejects_snapshot_coverage_extra_policy(self):
        """C5-1: Adding an otherwise-valid policy to snapshot, recomputing digest,
        must reject with zero writes."""
        conn, snapshot_path, _envelope = self._capture_existing()
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        # Clone mac-omp policy as mac-extra (same fields, different agent_id).
        base = inner["captured_state"]["policies"][0].copy()
        base["agent_id"] = "mac-extra"
        base["capacity_policy_id"] = compute_capacity_policy_id(
            agent_id="mac-extra",
            catalog_hash=base["catalog_hash"],
            max_concurrent_jobs=base["max_concurrent_jobs"],
            source_id=base["source_id"],
            source_version=base["source_version"],
        )
        inner["captured_state"]["policies"].append(base)
        inner["captured_state"]["policies"].sort(key=lambda p: p["agent_id"])
        self._write_adversarial(snapshot_path, inner)

        pre_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())
        pre_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "snapshot present for unknown/untyped agents|snapshot coverage drift"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        post_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())
        post_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchall()]
        self.assertEqual(pre_source, post_source)
        self.assertEqual(pre_policies, post_policies)

    # --- C5-2: strict timestamp adversarial tests ---

    def _build_adversarial_timestamp_snapshot(self, conn, snapshot_path, field_path, bad_value):
        """Modify a timestamp field in the existing snapshot and recompute digest."""
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        obj = inner["captured_state"]
        for key in field_path[:-1]:
            obj = obj[key]
        obj[field_path[-1]] = bad_value
        self._write_adversarial(snapshot_path, inner)

    def test_restore_rejects_invalid_month_timestamp(self):
        conn, snapshot_path, _envelope = self._capture_existing()
        self._build_adversarial_timestamp_snapshot(
            conn, snapshot_path,
            ["source", "updated_at"], "2026-13-01T00:00:00Z",
        )
        with self.assertRaisesRegex(CapacityError, "not a valid UTC datetime"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_invalid_day_timestamp(self):
        conn, snapshot_path, _envelope = self._capture_existing()
        self._build_adversarial_timestamp_snapshot(
            conn, snapshot_path,
            ["source", "updated_at"], "2026-02-30T00:00:00Z",
        )
        with self.assertRaisesRegex(CapacityError, "not a valid UTC datetime"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_invalid_hour_timestamp(self):
        conn, snapshot_path, _envelope = self._capture_existing()
        self._build_adversarial_timestamp_snapshot(
            conn, snapshot_path,
            ["source", "updated_at"], "2026-01-01T25:00:00Z",
        )
        with self.assertRaisesRegex(CapacityError, "not a valid UTC datetime"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_fractional_timestamp(self):
        conn, snapshot_path, _envelope = self._capture_existing()
        self._build_adversarial_timestamp_snapshot(
            conn, snapshot_path,
            ["source", "updated_at"], "2026-01-01T00:00:00.123Z",
        )
        with self.assertRaisesRegex(CapacityError, "invalid timestamp shape"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_offset_timestamp(self):
        conn, snapshot_path, _envelope = self._capture_existing()
        self._build_adversarial_timestamp_snapshot(
            conn, snapshot_path,
            ["source", "updated_at"], "2026-01-01T00:00:00+00:00",
        )
        with self.assertRaisesRegex(CapacityError, "invalid timestamp shape"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_policy_bad_timestamp(self):
        """C5-2: Bad timestamp in a policy field must also be rejected."""
        conn, snapshot_path, _envelope = self._capture_existing()
        self._build_adversarial_timestamp_snapshot(
            conn, snapshot_path,
            ["policies", 0, "created_at"], "2026-99-99T99:99:99Z",
        )
        with self.assertRaisesRegex(CapacityError, "not a valid UTC datetime"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    # --- C5-2: source_path adversarial tests ---

    def test_restore_rejects_source_path_del_char(self):
        """C5-2: DEL (U+007F) is a Cc control and must be rejected."""
        conn, snapshot_path, _envelope = self._capture_existing()
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        inner["captured_state"]["source"]["source_path"] = "/path/\x7f/evil"
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "control character"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_rejects_source_path_c1_control(self):
        """C5-2: C1 control (U+009F) must be rejected."""
        conn, snapshot_path, _envelope = self._capture_existing()
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        inner["captured_state"]["source"]["source_path"] = "/path/\x9f/evil"
        self._write_adversarial(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "control character"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

    def test_restore_accepts_source_path_unicode(self):
        """C5-2: Ordinary Unicode path (non-ASCII, non-Latin) must be accepted."""
        conn, snapshot_path, _envelope = self._capture_existing()
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        inner["captured_state"]["source"]["source_path"] = "/home/synthetic-user/projects/项目/コード"
        self._write_adversarial(snapshot_path, inner)
        # Should succeed — not a control character, just Unicode.
        restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        src = get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID)
        self.assertEqual(src["source_path"], "/home/synthetic-user/projects/项目/コード")

    # --- C5-3: current DB strict pre-delete validation adversarial tests ---

    def test_restore_rejects_current_target_orphan(self):
        """C5-3: Target source absent + target policy exists = reject, zero-write."""
        conn, snapshot_path, _envelope = self._capture_existing()
        # Delete the source but leave a policy.
        conn.execute("DELETE FROM executor_capacity_sources WHERE source_id = ?",
                      (EXPECTED_CAPACITY_SOURCE_ID,))
        conn.commit()

        pre_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "orphan current policy|target source absent"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        # Zero write — corrupt rows exact unchanged.
        post_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]
        self.assertEqual(pre_policies, post_policies)
        self.assertIsNone(get_capacity_source(conn, EXPECTED_CAPACITY_SOURCE_ID))

    def test_restore_rejects_current_bad_policy_id(self):
        """C5-3: Current policy has shaped-but-recomputation-invalid capacity_policy_id."""
        conn, snapshot_path, _envelope = self._capture_existing()
        conn.execute(
            "UPDATE executor_capacity_policies SET capacity_policy_id = ? WHERE agent_id = ?",
            ("sha256:" + "b" * 64, "mac-omp"),
        )
        conn.commit()

        pre_state = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "capacity_policy_id mismatch"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        post_state = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]
        self.assertEqual(pre_state, post_state)

    def test_restore_rejects_current_bad_capacity(self):
        """C5-3: Current policy has a valid-range max_concurrent_jobs but
        the capacity_policy_id was computed for a different capacity value,
        so recomputation fails."""
        conn, snapshot_path, _envelope = self._capture_existing()
        # Change max_concurrent_jobs but NOT the capacity_policy_id.
        # The SQL CHECK still passes (value=2 is in 1..32), but the
        # recomputed policy_id won't match.
        conn.execute(
            "UPDATE executor_capacity_policies SET max_concurrent_jobs = 2 WHERE agent_id = ?",
            ("mac-omp",),
        )
        conn.commit()

        pre_state = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "capacity_policy_id mismatch"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        post_state = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]
        self.assertEqual(pre_state, post_state)

    def test_restore_rejects_current_bad_timestamp(self):
        """C5-3: Current policy has invalid timestamp."""
        conn, snapshot_path, _envelope = self._capture_existing()
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ? WHERE agent_id = ?",
            ("2026-99-99T99:99:99Z", "mac-omp"),
        )
        conn.commit()

        pre_state = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "not a valid UTC datetime"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        post_state = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]
        self.assertEqual(pre_state, post_state)

    def test_restore_rejects_current_bad_source_path(self):
        """C5-3: Current source has a path with Cc control character."""
        conn, snapshot_path, _envelope = self._capture_existing()
        conn.execute(
            "UPDATE executor_capacity_sources SET source_path = ? WHERE source_id = ?",
            ("/path/\x00/bad", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.commit()

        pre_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())

        with self.assertRaisesRegex(CapacityError, "control character"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        post_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())
        self.assertEqual(pre_source, post_source)

    def test_restore_rejects_current_coverage_missing(self):
        """C5-3: Current DB has an enabled binding with no capacity policy."""
        conn, snapshot_path, _envelope = self._capture_existing()
        # Add a new enabled typed binding without a capacity policy.
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO agents (id, name, role, capabilities_json, online_state, current_load, client_type, created_at, updated_at) "
            "VALUES ('mac-new2', 'Mac New2', 'agent', '[]', 'offline', 0, 'agentd', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO runner_profiles (id, name, runner_type, command, working_directory_strategy, supports_stream_attach, env_json, created_at, updated_at) "
            "VALUES ('mac-new2', 'mac-new2', 'agentd', 'agent', 'current_dir', 0, '{}', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO executor_instance_bindings (agent_id, source_id, executor_definition_id, runner_profile_id, enabled, created_at, updated_at) "
            "VALUES ('mac-new2', 'multinexus.discord', 'omp-code', 'mac-new2', 1, ?, ?)",
            (now, now),
        )
        conn.commit()

        pre_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "current coverage drift"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        post_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]
        self.assertEqual(pre_policies, post_policies)

    def test_restore_rejects_current_coverage_extra(self):
        """C5-3: Current DB has a capacity policy for a disabled/nonexistent agent."""
        conn, snapshot_path, _envelope = self._capture_existing()
        # Add an extra capacity policy for an agent with no binding.
        catalog_hash = conn.execute(
            "SELECT catalog_hash FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone()["catalog_hash"]
        extra_pid = compute_capacity_policy_id(
            agent_id="mac-ghost",
            catalog_hash=catalog_hash,
            max_concurrent_jobs=1,
            source_id=EXPECTED_CAPACITY_SOURCE_ID,
            source_version=1,
        )
        conn.execute(
            "INSERT INTO executor_capacity_policies "
            "(agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("mac-ghost", EXPECTED_CAPACITY_SOURCE_ID, 1, catalog_hash, extra_pid, 1,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()

        pre_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]

        with self.assertRaisesRegex(CapacityError, "current capacity present for unknown/untyped agent|current coverage drift"):
            restore_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        post_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()]
        self.assertEqual(pre_policies, post_policies)

    # --- C5-4: atomic write chmod failure test ---

    def test_atomic_write_chmod_failure_removes_final_output(self):
        """C5-4: If os.chmod raises after os.replace, final output must be removed."""
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn)
        self._sync_capacity(conn)
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", EXPECTED_CAPACITY_SOURCE_ID),
        )
        conn.commit()

        snapshot_path = Path(tempfile.mkdtemp()) / "chmod_fail.json"
        pre_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())
        pre_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchall()]

        def _failing_chmod(path, mode):
            raise PermissionError("injected chmod failure")

        with patch("os.chmod", side_effect=_failing_chmod):
            with self.assertRaises(PermissionError):
                capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)

        # Final output must not exist.
        self.assertFalse(snapshot_path.exists(), "final output must be removed on chmod failure")
        # DB must be unchanged (transaction rolled back).
        post_source = dict(conn.execute(
            "SELECT * FROM executor_capacity_sources WHERE source_id = ?",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchone())
        post_policies = [dict(r) for r in conn.execute(
            "SELECT * FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
            (EXPECTED_CAPACITY_SOURCE_ID,),
        ).fetchall()]
        self.assertEqual(pre_source, post_source)
        self.assertEqual(pre_policies, post_policies)

    def test_atomic_write_success_mode_is_exact_0600(self):
        """C5-4: Successful capture must produce exact 0600 mode."""
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "prior_absence.json"
        capture_capacity_snapshot(conn, EXPECTED_CAPACITY_SOURCE_ID, snapshot_path)
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)


class CapacitySnapshotMultiSourceTests(unittest.TestCase):
    """v2 snapshot contract: preserved_state witness, multi-source restore, v1 compatibility."""

    TARGET = EXPECTED_CAPACITY_SOURCE_ID
    FIXTURE_SOURCE_ID = "fixture.capacity"

    def _make_conn(self) -> sqlite3.Connection:
        return _make_conn()

    def _seed_agents_and_profiles(self, conn: sqlite3.Connection, *agent_ids: str) -> None:
        from coordinate.db_support import utc_now
        now = utc_now()
        for agent_id in agent_ids:
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

    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self._requested_binding_enabled: dict[str, bool] = {}

    def _sync_executor_catalog(self, conn: sqlite3.Connection, *bindings: tuple[str, bool]) -> None:
        catalog = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=2,
            catalog_hash="",
            source_path=None,
            definitions=(
                ExecutorDefinition(id="omp-code", provider="kimi-code", adapter="omp", capabilities=("coding",)),
            ),
            bindings=tuple(
                ExecutorInstanceBinding(
                    agent_id=agent_id,
                    executor_definition_id="omp-code",
                    runner_profile_id=agent_id,
                    enabled=enabled,
                )
                for agent_id, enabled in bindings
            ),
        )
        catalog = catalog.__class__(
            source_id=catalog.source_id,
            source_version=catalog.source_version,
            catalog_hash=compute_executor_catalog_hash(catalog),
            source_path=catalog.source_path,
            definitions=catalog.definitions,
            bindings=catalog.bindings,
        )
        sync_executor_catalog(conn, catalog)
        self._requested_binding_enabled = {agent_id: enabled for agent_id, enabled in bindings}

    def _capacity_catalog(self, source_id: str, source_version: int, *policies: CapacityPolicy) -> CapacityCatalog:
        catalog = CapacityCatalog(
            source_id=source_id,
            source_version=source_version,
            catalog_hash="",
            source_path=None,
            policies=policies,
        )
        return catalog.__class__(
            source_id=catalog.source_id,
            source_version=catalog.source_version,
            catalog_hash=compute_capacity_catalog_hash(catalog),
            source_path=catalog.source_path,
            policies=catalog.policies,
        )

    def _sync_capacity(self, conn: sqlite3.Connection, source_id: str, source_version: int, *policies: CapacityPolicy) -> None:
        catalog = self._capacity_catalog(source_id, source_version, *policies)
        # sync_capacity_catalog owns a transaction; ensure we are not inside one.
        try:
            conn.commit()
        except Exception:
            pass
        proposed_agents = {p.agent_id for p in policies}
        other_policy_agents = {
            row[0] for row in conn.execute(
                "SELECT DISTINCT agent_id FROM executor_capacity_policies WHERE source_id != ?",
                (source_id,),
            )
        }
        covered_agents = proposed_agents | other_policy_agents
        for agent_id, requested_enabled in self._requested_binding_enabled.items():
            if requested_enabled and agent_id not in covered_agents:
                conn.execute(
                    "UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?",
                    (agent_id,),
                )
        conn.commit()
        sync_capacity_catalog(conn, catalog)
        conn.commit()
        all_policy_agents = {
            row[0] for row in conn.execute("SELECT DISTINCT agent_id FROM executor_capacity_policies")
        }
        for agent_id, requested_enabled in self._requested_binding_enabled.items():
            enabled = 1 if (requested_enabled and agent_id in all_policy_agents) else 0
            conn.execute(
                "UPDATE executor_instance_bindings SET enabled = ? WHERE agent_id = ?",
                (enabled, agent_id),
            )
        conn.commit()

    def _snapshot_capacity_rows(self, conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return list_capacity_sources(conn), list_capacity_policies(conn)

    def _reserve_active_lease(self, conn: sqlite3.Connection, agent_id: str) -> None:
        from coordinate.db import create_job, get_workspace, get_workspace_host_profile, upsert_workspace_host_profile
        from coordinate.db_support import utc_now
        from coordinate.execution_context import resolve_execution_context_v1
        now = utc_now()
        ws_id = f"ws-{agent_id}"
        host_id = f"host-{agent_id}"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (ws_id, ws_id, "/tmp", "/tmp/docs", now, now),
        )
        conn.commit()
        upsert_workspace_host_profile(
            conn,
            workspace_id=ws_id,
            host_id=host_id,
            workspace_path="/tmp/ws",
            harness_root="/tmp/docs",
        )
        conn.commit()
        job = create_job(
            conn,
            workspace_id=ws_id,
            task_id=None,
            runner_profile_id=agent_id,
            assigned_agent=agent_id,
            payload={},
            worktree_path="/tmp/ws",
        )
        conn.execute(
            "UPDATE jobs SET attempt_count = 1, assigned_agent = ?, runner_profile_id = ? WHERE id = ?",
            (agent_id, agent_id, job["id"]),
        )
        conn.commit()
        workspace = get_workspace(conn, ws_id)
        profile = get_workspace_host_profile(conn, workspace_id=ws_id, host_id=host_id)
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
        from coordinate.execution_leases import reserve_attempt_lease
        reserve_attempt_lease(
            conn,
            job_id=job["id"],
            attempt_token=1,
            agent_id=agent_id,
            runner_profile_id=agent_id,
            host_id=host_id,
            worktree_path="/tmp/ws",
            ttl_seconds=60,
        )
        conn.commit()

    def _write_adversarial_snapshot(self, snapshot_path: Path, inner: dict[str, Any]) -> None:
        digest = hashlib.sha256(
            json.dumps(inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        envelope = {"snapshot": inner, "snapshot_sha256": digest}
        snapshot_path.write_bytes(
            json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )

    def _build_v1_envelope(self, target_source_id: str, captured_state: dict[str, Any] | None) -> dict[str, Any]:
        inner = {
            "contract_version": 1,
            "target_source_id": target_source_id,
            "captured_state": captured_state,
        }
        digest = hashlib.sha256(
            json.dumps(inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return {"snapshot": inner, "snapshot_sha256": digest}

    def test_v2_capture_single_source_exact_bytes_digest_and_empty_witness(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp")
        self._sync_executor_catalog(conn, ("mac-omp", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", self.TARGET),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE source_id = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", self.TARGET),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "single.json"
        envelope = capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        expected_inner = {
            "contract_version": 2,
            "target_source_id": self.TARGET,
            "captured_state": {
                "source": {
                    "source_id": self.TARGET,
                    "source_version": 1,
                    "catalog_hash": envelope["snapshot"]["captured_state"]["source"]["catalog_hash"],
                    "source_path": None,
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                "policies": [
                    {
                        "agent_id": "mac-omp",
                        "source_id": self.TARGET,
                        "source_version": 1,
                        "catalog_hash": envelope["snapshot"]["captured_state"]["source"]["catalog_hash"],
                        "capacity_policy_id": compute_capacity_policy_id(
                            agent_id="mac-omp",
                            catalog_hash=envelope["snapshot"]["captured_state"]["source"]["catalog_hash"],
                            max_concurrent_jobs=1,
                            source_id=self.TARGET,
                            source_version=1,
                        ),
                        "max_concurrent_jobs": 1,
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z",
                    },
                ],
            },
            "preserved_state": {"sources": [], "policies": []},
        }
        expected_digest = hashlib.sha256(
            json.dumps(expected_inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        expected_envelope = {"snapshot": expected_inner, "snapshot_sha256": expected_digest}
        self.assertEqual(envelope, expected_envelope)
        self.assertEqual(
            snapshot_path.read_bytes(),
            json.dumps(expected_envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)

    def test_v2_capture_two_sources_exact_bytes_digest_and_witness(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        envelope = capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertEqual(envelope["snapshot"]["contract_version"], 2)
        self.assertEqual(envelope["snapshot"]["target_source_id"], self.TARGET)
        self.assertIsNotNone(envelope["snapshot"]["captured_state"])
        self.assertEqual(
            [p["agent_id"] for p in envelope["snapshot"]["captured_state"]["policies"]],
            ["mac-omp"],
        )
        preserved = envelope["snapshot"]["preserved_state"]
        self.assertEqual([s["source_id"] for s in preserved["sources"]], [self.FIXTURE_SOURCE_ID])
        self.assertEqual([p["agent_id"] for p in preserved["policies"]], ["mac-claude"])
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)

        # Digest verifies against canonical bytes.
        canonical = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.assertEqual(
            envelope["snapshot_sha256"],
            hashlib.sha256(
                _capacity_snapshot_canonical_bytes(envelope["snapshot"])
            ).hexdigest(),
        )
        self.assertEqual(snapshot_path.read_bytes(), canonical)

    def test_v2_capture_target_absence_with_other_source_zero_policies(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1)
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "absent_zero.json"
        envelope = capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertIsNone(envelope["snapshot"]["captured_state"])
        preserved = envelope["snapshot"]["preserved_state"]
        self.assertEqual([s["source_id"] for s in preserved["sources"]], [self.FIXTURE_SOURCE_ID])
        self.assertEqual(preserved["policies"], [])
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)

    def test_v2_capture_target_absence_with_other_source_nonzero_policies(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "absent_nonzero.json"
        envelope = capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertIsNone(envelope["snapshot"]["captured_state"])
        preserved = envelope["snapshot"]["preserved_state"]
        self.assertEqual([s["source_id"] for s in preserved["sources"]], [self.FIXTURE_SOURCE_ID])
        self.assertEqual([p["agent_id"] for p in preserved["policies"]], ["mac-claude"])
        self.assertEqual(stat.S_IMODE(snapshot_path.stat().st_mode), 0o600)

    def test_v2_capture_rejects_unknown_binding_in_target(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp")
        self._sync_executor_catalog(conn, ("mac-omp", True))
        conn.execute(
            "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (self.TARGET, 1, "0" * 64, None, "2026-01-01T00:00:00Z"),
        )
        conn.execute(
            "INSERT INTO executor_capacity_policies "
            "(agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("unknown-target", self.TARGET, 1, "0" * 64, compute_capacity_policy_id(
                agent_id="unknown-target",
                catalog_hash="0" * 64,
                max_concurrent_jobs=1,
                source_id=self.TARGET,
                source_version=1,
            ), 1,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "unknown_target.json"
        with self.assertRaisesRegex(CapacityError, "current capacity present for unknown/untyped agent"):
            capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertFalse(snapshot_path.exists())

    def test_v2_capture_rejects_unknown_binding_in_any_source(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp")
        self._sync_executor_catalog(conn, ("mac-omp", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        conn.execute(
            "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (self.FIXTURE_SOURCE_ID, 1, "0" * 64, None, "2026-01-01T00:00:00Z"),
        )
        conn.execute(
            "INSERT INTO executor_capacity_policies "
            "(agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("unknown-other", self.FIXTURE_SOURCE_ID, 1, "0" * 64, compute_capacity_policy_id(
                agent_id="unknown-other",
                catalog_hash="0" * 64,
                max_concurrent_jobs=1,
                source_id=self.FIXTURE_SOURCE_ID,
                source_version=1,
            ), 1,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "unknown_other.json"
        with self.assertRaisesRegex(CapacityError, "current capacity present for unknown/untyped agent"):
            capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertFalse(snapshot_path.exists())

    def test_v2_capture_rejects_orphan_policy_without_source(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp")
        self._sync_executor_catalog(conn, ("mac-omp", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        conn.execute(
            "INSERT INTO executor_capacity_policies "
            "(agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("orphan", self.FIXTURE_SOURCE_ID, 1, "0" * 64, "sha256:" + "0" * 64, 1,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "orphan.json"
        with self.assertRaisesRegex(CapacityError, "orphan current policy"):
            capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertFalse(snapshot_path.exists())

    def test_v2_capture_rejects_union_coverage_miss(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_instance_bindings SET enabled = 1 WHERE agent_id = ?",
            ("mac-claude",),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "coverage.json"
        with self.assertRaisesRegex(CapacityError, "current coverage drift"):
            capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertFalse(snapshot_path.exists())

    def test_v2_capture_rejects_corrupt_non_target_policy_id(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_policies SET capacity_policy_id = ? WHERE agent_id = ?",
            ("sha256:" + "b" * 64, "mac-claude"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "corrupt.json"
        with self.assertRaisesRegex(CapacityError, "capacity_policy_id mismatch"):
            capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertFalse(snapshot_path.exists())

    def test_v2_restore_two_sources_preserves_witness(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        envelope = capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        before_sources, before_policies = self._snapshot_capacity_rows(conn)

        # Mutate target only to a new consistent state.
        new_hash = "1" * 64
        new_version = 2
        conn.execute(
            "UPDATE executor_capacity_sources SET source_version = ?, catalog_hash = ? WHERE source_id = ?",
            (new_version, new_hash, self.TARGET),
        )
        for agent_id in ("mac-omp",):
            new_pid = compute_capacity_policy_id(
                agent_id=agent_id,
                catalog_hash=new_hash,
                max_concurrent_jobs=1,
                source_id=self.TARGET,
                source_version=new_version,
            )
            conn.execute(
                "UPDATE executor_capacity_policies SET source_version = ?, catalog_hash = ?, capacity_policy_id = ? "
                "WHERE agent_id = ?",
                (new_version, new_hash, new_pid, agent_id),
            )
        conn.commit()

        restored = restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertEqual(restored["snapshot"]["contract_version"], 2)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)

        # Target restored to snapshot; non-target exact/value-identical.
        target_source = next(s for s in after_sources if s["source_id"] == self.TARGET)
        self.assertEqual(target_source["source_version"], 1)
        self.assertEqual(target_source["catalog_hash"], envelope["snapshot"]["captured_state"]["source"]["catalog_hash"])
        non_target_before = [s for s in before_sources if s["source_id"] != self.TARGET]
        non_target_after = [s for s in after_sources if s["source_id"] != self.TARGET]
        self.assertEqual(non_target_after, non_target_before)
        non_target_policies_before = [p for p in before_policies if p["source_id"] != self.TARGET]
        non_target_policies_after = [p for p in after_policies if p["source_id"] != self.TARGET]
        self.assertEqual(non_target_policies_after, non_target_policies_before)

    def test_v2_restore_prior_absence_deletes_target_only(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", False), ("mac-claude", True))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "prior_absence.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)

        # Deploy creates target capacity.
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        before_sources, before_policies = self._snapshot_capacity_rows(conn)

        restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertIsNone(get_capacity_source(conn, self.TARGET))
        self.assertEqual(len(list_capacity_policies(conn, self.TARGET)), 0)
        non_target_before = [s for s in before_sources if s["source_id"] != self.TARGET]
        non_target_after = [s for s in after_sources if s["source_id"] != self.TARGET]
        self.assertEqual(non_target_after, non_target_before)
        non_target_policies_before = [p for p in before_policies if p["source_id"] != self.TARGET]
        non_target_policies_after = [p for p in after_policies if p["source_id"] != self.TARGET]
        self.assertEqual(non_target_policies_after, non_target_policies_before)

    def test_v2_restore_rejects_active_lease_on_other_source(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self._reserve_active_lease(conn, "mac-claude")
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "active lease"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_active_lease_on_target_source(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self._reserve_active_lease(conn, "mac-omp")
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "active lease"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_source_added(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        conn.execute(
            "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("extra.capacity", 1, "0" * 64, None, "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_source_removed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        conn.execute("DELETE FROM executor_capacity_policies WHERE source_id = ?", (self.FIXTURE_SOURCE_ID,))
        conn.execute("DELETE FROM executor_capacity_sources WHERE source_id = ?", (self.FIXTURE_SOURCE_ID,))
        conn.execute("UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?", ("mac-claude",))
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_source_version_changed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        catalog_hash = conn.execute(
            "SELECT catalog_hash FROM executor_capacity_sources WHERE source_id = ?",
            (self.FIXTURE_SOURCE_ID,),
        ).fetchone()["catalog_hash"]
        new_version = 2
        new_pid = compute_capacity_policy_id(
            agent_id="mac-claude",
            catalog_hash=catalog_hash,
            max_concurrent_jobs=1,
            source_id=self.FIXTURE_SOURCE_ID,
            source_version=new_version,
        )
        conn.execute(
            "UPDATE executor_capacity_sources SET source_version = ? WHERE source_id = ?",
            (new_version, self.FIXTURE_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET source_version = ?, capacity_policy_id = ? WHERE agent_id = ?",
            (new_version, new_pid, "mac-claude"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_source_hash_changed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        new_hash = "1" * 64
        new_pid = compute_capacity_policy_id(
            agent_id="mac-claude",
            catalog_hash=new_hash,
            max_concurrent_jobs=1,
            source_id=self.FIXTURE_SOURCE_ID,
            source_version=1,
        )
        conn.execute(
            "UPDATE executor_capacity_sources SET catalog_hash = ? WHERE source_id = ?",
            (new_hash, self.FIXTURE_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET catalog_hash = ?, capacity_policy_id = ? WHERE agent_id = ?",
            (new_hash, new_pid, "mac-claude"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_policy_added(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude", "mac-extra")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True), ("mac-extra", False))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        # Add a disabled-binding policy owned by the fixture source.
        catalog_hash = conn.execute(
            "SELECT catalog_hash FROM executor_capacity_sources WHERE source_id = ?",
            (self.FIXTURE_SOURCE_ID,),
        ).fetchone()["catalog_hash"]
        extra_pid = compute_capacity_policy_id(
            agent_id="mac-extra",
            catalog_hash=catalog_hash,
            max_concurrent_jobs=1,
            source_id=self.FIXTURE_SOURCE_ID,
            source_version=1,
        )
        conn.execute(
            "INSERT INTO executor_capacity_policies "
            "(agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("mac-extra", self.FIXTURE_SOURCE_ID, 1, catalog_hash, extra_pid, 1,
             "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_policy_removed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        conn.execute("DELETE FROM executor_capacity_policies WHERE agent_id = ?", ("mac-claude",))
        conn.execute("UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?", ("mac-claude",))
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_policy_capacity_changed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        catalog_hash = conn.execute(
            "SELECT catalog_hash FROM executor_capacity_sources WHERE source_id = ?",
            (self.FIXTURE_SOURCE_ID,),
        ).fetchone()["catalog_hash"]
        new_max = 2
        new_pid = compute_capacity_policy_id(
            agent_id="mac-claude",
            catalog_hash=catalog_hash,
            max_concurrent_jobs=new_max,
            source_id=self.FIXTURE_SOURCE_ID,
            source_version=1,
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET max_concurrent_jobs = ?, capacity_policy_id = ? WHERE agent_id = ?",
            (new_max, new_pid, "mac-claude"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_policy_hash_changed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        new_hash = "1" * 64
        new_pid = compute_capacity_policy_id(
            agent_id="mac-claude",
            catalog_hash=new_hash,
            max_concurrent_jobs=1,
            source_id=self.FIXTURE_SOURCE_ID,
            source_version=1,
        )
        conn.execute(
            "UPDATE executor_capacity_sources SET catalog_hash = ? WHERE source_id = ?",
            (new_hash, self.FIXTURE_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET catalog_hash = ?, capacity_policy_id = ? WHERE agent_id = ?",
            (new_hash, new_pid, "mac-claude"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_policy_version_changed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        catalog_hash = conn.execute(
            "SELECT catalog_hash FROM executor_capacity_sources WHERE source_id = ?",
            (self.FIXTURE_SOURCE_ID,),
        ).fetchone()["catalog_hash"]
        new_version = 2
        new_pid = compute_capacity_policy_id(
            agent_id="mac-claude",
            catalog_hash=catalog_hash,
            max_concurrent_jobs=1,
            source_id=self.FIXTURE_SOURCE_ID,
            source_version=new_version,
        )
        conn.execute(
            "UPDATE executor_capacity_sources SET source_version = ? WHERE source_id = ?",
            (new_version, self.FIXTURE_SOURCE_ID),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET source_version = ?, capacity_policy_id = ? WHERE agent_id = ?",
            (new_version, new_pid, "mac-claude"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_witness_policy_timestamp_changed(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ? WHERE agent_id = ?",
            ("2026-02-01T00:00:00Z", "2026-02-01T00:00:00Z", "mac-claude"),
        )
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "witness mismatch"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_target_ownership_takeover_by_preserved(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)

        # Mutate snapshot target to claim mac-claude, which is owned by fixture.
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        catalog_hash = inner["captured_state"]["source"]["catalog_hash"]
        source_version = inner["captured_state"]["source"]["source_version"]
        inner["captured_state"]["policies"] = [
            {
                "agent_id": "mac-claude",
                "source_id": self.TARGET,
                "source_version": source_version,
                "catalog_hash": catalog_hash,
                "capacity_policy_id": compute_capacity_policy_id(
                    agent_id="mac-claude",
                    catalog_hash=catalog_hash,
                    max_concurrent_jobs=1,
                    source_id=self.TARGET,
                    source_version=source_version,
                ),
                "max_concurrent_jobs": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ]
        self._write_adversarial_snapshot(snapshot_path, inner)
        conn.execute("UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?", ("mac-omp",))
        conn.commit()
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "owned by preserved source"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_post_restore_union_coverage_miss(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)

        # Remove target policy from snapshot so proposed union would miss the enabled mac-omp binding.
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        inner["captured_state"]["policies"] = [
            p for p in inner["captured_state"]["policies"] if p["agent_id"] != "mac-omp"
        ]
        self._write_adversarial_snapshot(snapshot_path, inner)

        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "snapshot coverage drift"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_unknown_agent_in_target_snapshot(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp")
        self._sync_executor_catalog(conn, ("mac-omp", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "target.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        catalog_hash = inner["captured_state"]["source"]["catalog_hash"]
        source_version = inner["captured_state"]["source"]["source_version"]
        inner["captured_state"]["policies"].append(
            {
                "agent_id": "unknown-agent",
                "source_id": self.TARGET,
                "source_version": source_version,
                "catalog_hash": catalog_hash,
                "capacity_policy_id": compute_capacity_policy_id(
                    agent_id="unknown-agent",
                    catalog_hash=catalog_hash,
                    max_concurrent_jobs=1,
                    source_id=self.TARGET,
                    source_version=source_version,
                ),
                "max_concurrent_jobs": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        )
        inner["captured_state"]["policies"].sort(key=lambda p: p["agent_id"])
        self._write_adversarial_snapshot(snapshot_path, inner)
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "snapshot present for unknown/untyped agents"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_restore_rejects_v1_snapshot_on_multi_source_db(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "v1.json"
        # Handcraft a valid v1 envelope for the target source.
        inner = {
            "contract_version": 1,
            "target_source_id": self.TARGET,
            "captured_state": {
                "source": {
                    "source_id": self.TARGET,
                    "source_version": 1,
                    "catalog_hash": "0" * 64,
                    "source_path": None,
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                "policies": [
                    {
                        "agent_id": "mac-omp",
                        "source_id": self.TARGET,
                        "source_version": 1,
                        "catalog_hash": "0" * 64,
                        "capacity_policy_id": compute_capacity_policy_id(
                            agent_id="mac-omp",
                            catalog_hash="0" * 64,
                            max_concurrent_jobs=1,
                            source_id=self.TARGET,
                            source_version=1,
                        ),
                        "max_concurrent_jobs": 1,
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z",
                    },
                ],
            },
        }
        self._write_adversarial_snapshot(snapshot_path, inner)
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with self.assertRaisesRegex(CapacityError, "v1 snapshot cannot restore a multi-source"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v1_restore_rejects_unknown_version(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp")
        self._sync_executor_catalog(conn, ("mac-omp", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        snapshot_path = Path(tempfile.mkdtemp()) / "bad_version.json"
        base_inner = {
            "contract_version": 0,
            "target_source_id": self.TARGET,
            "captured_state": None,
        }
        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        for version in (0, 3, True, 1.0, 2.0, "1", "2", None):
            with self.subTest(version=version):
                inner = dict(base_inner)
                inner["contract_version"] = version
                self._write_adversarial_snapshot(snapshot_path, inner)
                with self.assertRaisesRegex(CapacityError, "contract_version must be 1 or 2"):
                    restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
                after_sources, after_policies = self._snapshot_capacity_rows(conn)
                self.assertEqual(after_sources, before_sources)
                self.assertEqual(after_policies, before_policies)

    def test_v1_restore_rejects_v2_key_shape(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "v1_with_preserved.json"
        inner = {
            "contract_version": 1,
            "target_source_id": self.TARGET,
            "captured_state": None,
            "preserved_state": {"sources": [], "policies": []},
        }
        self._write_adversarial_snapshot(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "v1 snapshot.snapshot has unknown or missing keys"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)

    def test_v2_restore_rejects_v1_key_shape(self):
        conn = self._make_conn()
        snapshot_path = Path(tempfile.mkdtemp()) / "v2_without_preserved.json"
        inner = {
            "contract_version": 2,
            "target_source_id": self.TARGET,
            "captured_state": None,
        }
        self._write_adversarial_snapshot(snapshot_path, inner)
        with self.assertRaisesRegex(CapacityError, "v2 snapshot.snapshot has unknown or missing keys"):
            restore_capacity_snapshot(conn, self.TARGET, snapshot_path)

    def test_v1_restore_succeeds_on_single_source_db(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp")
        self._sync_executor_catalog(conn, ("mac-omp", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        # Handcraft a valid v1 envelope.
        catalog_hash = get_capacity_source(conn, self.TARGET)["catalog_hash"]
        captured_state = {
            "source": {
                "source_id": self.TARGET,
                "source_version": 1,
                "catalog_hash": catalog_hash,
                "source_path": None,
                "updated_at": "2026-01-01T00:00:00Z",
            },
            "policies": [
                {
                    "agent_id": "mac-omp",
                    "source_id": self.TARGET,
                    "source_version": 1,
                    "catalog_hash": catalog_hash,
                    "capacity_policy_id": compute_capacity_policy_id(
                        agent_id="mac-omp",
                        catalog_hash=catalog_hash,
                        max_concurrent_jobs=1,
                        source_id=self.TARGET,
                        source_version=1,
                    ),
                    "max_concurrent_jobs": 1,
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            ],
        }
        snapshot_path = Path(tempfile.mkdtemp()) / "v1.json"
        inner = {
            "contract_version": 1,
            "target_source_id": self.TARGET,
            "captured_state": captured_state,
        }
        self._write_adversarial_snapshot(snapshot_path, inner)
        v1_envelope = {
            "snapshot": inner,
            "snapshot_sha256": hashlib.sha256(
                json.dumps(inner, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        }

        # Mutate current target.
        new_hash = "1" * 64
        new_version = 2
        conn.execute(
            "UPDATE executor_capacity_sources SET source_version = ?, catalog_hash = ? WHERE source_id = ?",
            (new_version, new_hash, self.TARGET),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET source_version = ?, catalog_hash = ?, capacity_policy_id = ? WHERE agent_id = ?",
            (new_version, new_hash, compute_capacity_policy_id(
                agent_id="mac-omp",
                catalog_hash=new_hash,
                max_concurrent_jobs=1,
                source_id=self.TARGET,
                source_version=new_version,
            ), "mac-omp"),
        )
        conn.commit()

        restored = restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertEqual(restored, v1_envelope)
        source = get_capacity_source(conn, self.TARGET)
        self.assertEqual(source["source_version"], 1)
        self.assertEqual(source["catalog_hash"], catalog_hash)
        policies = list_capacity_policies(conn, self.TARGET)
        self.assertEqual(len(policies), 1)
        self.assertEqual(policies[0]["agent_id"], "mac-omp")

    def test_v2_deterministic_bytes_order_and_digest(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        path1 = Path(tempfile.mkdtemp()) / "a.json"
        path2 = Path(tempfile.mkdtemp()) / "b.json"
        envelope1 = capture_capacity_snapshot(conn, self.TARGET, path1)
        envelope2 = capture_capacity_snapshot(conn, self.TARGET, path2)
        self.assertEqual(path1.read_bytes(), path2.read_bytes())
        self.assertEqual(envelope1["snapshot_sha256"], envelope2["snapshot_sha256"])

    def test_v2_atomic_write_failure_cleans_output(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "chmod_fail.json"
        before_sources, before_policies = self._snapshot_capacity_rows(conn)

        def _failing_chmod(path, mode):
            raise PermissionError("injected chmod failure")

        with patch("os.chmod", side_effect=_failing_chmod):
            with self.assertRaises(PermissionError):
                capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        self.assertFalse(snapshot_path.exists())
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def _make_snapshot_claim_mac_claude(self, snapshot_path: Path) -> None:
        inner = json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshot"]
        catalog_hash = inner["captured_state"]["source"]["catalog_hash"]
        source_version = inner["captured_state"]["source"]["source_version"]
        inner["captured_state"]["policies"] = [
            {
                "agent_id": "mac-claude",
                "source_id": self.TARGET,
                "source_version": source_version,
                "catalog_hash": catalog_hash,
                "capacity_policy_id": compute_capacity_policy_id(
                    agent_id="mac-claude",
                    catalog_hash=catalog_hash,
                    max_concurrent_jobs=1,
                    source_id=self.TARGET,
                    source_version=source_version,
                ),
                "max_concurrent_jobs": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ]
        self._write_adversarial_snapshot(snapshot_path, inner)

    def test_v2_zero_mutation_before_delete_on_every_validation_failure(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)

        failure_cases = [
            ("witness mismatch: source added", lambda c: c.execute(
                "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at) VALUES (?, ?, ?, ?, ?)",
                ("extra.capacity", 1, "0" * 64, None, "2026-01-01T00:00:00Z"),
            )),
            ("witness mismatch: source version changed", lambda c: c.execute(
                "UPDATE executor_capacity_sources SET source_version = 2 WHERE source_id = ?",
                (self.FIXTURE_SOURCE_ID,),
            )),
            ("snapshot claims preserved agent", lambda c: self._make_snapshot_claim_mac_claude(snapshot_path)),
        ]
        for label, mutate in failure_cases:
            with self.subTest(case=label):
                mutate(conn)
                conn.commit()
                before_sources, before_policies = self._snapshot_capacity_rows(conn)
                with self.assertRaises(CapacityError):
                    restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
                after_sources, after_policies = self._snapshot_capacity_rows(conn)
                self.assertEqual(after_sources, before_sources, label)
                self.assertEqual(after_policies, before_policies, label)
                # Rollback the mutate for the next case by restoring DB state.
                conn.execute("DELETE FROM executor_capacity_sources WHERE source_id = ?", ("extra.capacity",))
                conn.execute(
                    "UPDATE executor_capacity_sources SET source_version = 1 WHERE source_id = ?",
                    (self.FIXTURE_SOURCE_ID,),
                )
                conn.execute("UPDATE executor_instance_bindings SET enabled = 1 WHERE agent_id = ?", ("mac-claude",))
                conn.commit()

    def test_v2_post_write_target_verification_failure_rolls_back(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)

        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with patch("coordinate.executor_capacity._assert_exact_state_match", side_effect=CapacityError("injected target verification failure")):
            with self.assertRaisesRegex(CapacityError, "injected target verification failure"):
                restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_post_write_witness_verification_failure_rolls_back(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        capture_capacity_snapshot(conn, self.TARGET, snapshot_path)

        before_sources, before_policies = self._snapshot_capacity_rows(conn)
        with patch("coordinate.executor_capacity._assert_exact_witness_match", side_effect=CapacityError("injected witness verification failure")):
            with self.assertRaisesRegex(CapacityError, "injected witness verification failure"):
                restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
        after_sources, after_policies = self._snapshot_capacity_rows(conn)
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_policies, before_policies)

    def test_v2_malformed_preserved_state_rejected(self):
        conn = self._make_conn()
        self._seed_agents_and_profiles(conn, "mac-omp", "mac-claude")
        self._sync_executor_catalog(conn, ("mac-omp", True), ("mac-claude", True))
        self._sync_capacity(conn, self.TARGET, 1, CapacityPolicy(agent_id="mac-omp", max_concurrent_jobs=1))
        self._sync_capacity(conn, self.FIXTURE_SOURCE_ID, 1, CapacityPolicy(agent_id="mac-claude", max_concurrent_jobs=1))
        conn.execute(
            "UPDATE executor_capacity_sources SET updated_at = ?",
            ("2026-01-01T00:00:00Z",),
        )
        conn.execute(
            "UPDATE executor_capacity_policies SET created_at = ?, updated_at = ?",
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()
        snapshot_path = Path(tempfile.mkdtemp()) / "two.json"
        envelope = capture_capacity_snapshot(conn, self.TARGET, snapshot_path)
        base_inner = envelope["snapshot"]

        malformed_cases = [
            ("extra key", lambda i: i["preserved_state"].update({"extra": True})),
            ("missing policies key", lambda i: i["preserved_state"].pop("policies")),
            ("sources not list", lambda i: i["preserved_state"].__setitem__("sources", "not-a-list")),
            ("policies not list", lambda i: i["preserved_state"].__setitem__("policies", "not-a-list")),
            ("sources duplicate", lambda i: i["preserved_state"].__setitem__("sources", [
                base_inner["preserved_state"]["sources"][0],
                base_inner["preserved_state"]["sources"][0],
            ])),
            ("sources not increasing", lambda i: i["preserved_state"].__setitem__("sources", [
                {**base_inner["preserved_state"]["sources"][0], "source_id": "a.a"},
                {**base_inner["preserved_state"]["sources"][0], "source_id": "a.a"},
            ])),
            ("policies duplicate", lambda i: i["preserved_state"].__setitem__("policies", [
                base_inner["preserved_state"]["policies"][0],
                base_inner["preserved_state"]["policies"][0],
            ])),
            ("policies not increasing", lambda i: i["preserved_state"].__setitem__("policies", [
                {**base_inner["preserved_state"]["policies"][0], "agent_id": "a"},
                {**base_inner["preserved_state"]["policies"][0], "agent_id": "a"},
            ])),
            ("tampered source hash recomputed digest", lambda i: i["preserved_state"]["sources"][0].__setitem__("catalog_hash", "1" * 64)),
        ]

        for label, mutate in malformed_cases:
            with self.subTest(case=label):
                inner = json.loads(json.dumps(base_inner))
                mutate(inner)
                snapshot_path.unlink(missing_ok=True)
                self._write_adversarial_snapshot(snapshot_path, inner)
                before_sources, before_policies = self._snapshot_capacity_rows(conn)
                with self.assertRaises(CapacityError):
                    restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
                after_sources, after_policies = self._snapshot_capacity_rows(conn)
                self.assertEqual(after_sources, before_sources, label)
                self.assertEqual(after_policies, before_policies, label)

        with self.subTest(case="tampered source hash with original digest"):
            inner = json.loads(json.dumps(base_inner))
            inner["preserved_state"]["sources"][0]["catalog_hash"] = "1" * 64
            tampered_envelope = {
                "snapshot": inner,
                "snapshot_sha256": envelope["snapshot_sha256"],
            }
            snapshot_path.unlink(missing_ok=True)
            snapshot_path.write_bytes(
                json.dumps(tampered_envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
            )
            before_sources, before_policies = self._snapshot_capacity_rows(conn)
            with self.assertRaisesRegex(CapacityError, "snapshot digest mismatch"):
                restore_capacity_snapshot(conn, self.TARGET, snapshot_path)
            after_sources, after_policies = self._snapshot_capacity_rows(conn)
            self.assertEqual(after_sources, before_sources)
            self.assertEqual(after_policies, before_policies)


if __name__ == "__main__":
    unittest.main()
