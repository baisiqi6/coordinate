"""Tests for the P9-2A executor identity catalog and binding snapshot contract."""
from __future__ import annotations

import dataclasses
import hashlib
import json
import sqlite3
import tempfile
import textwrap
import unittest
from pathlib import Path

from coordinate.db import (
    initialize,
    row_to_dict,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.executor_capacity import (
    CapacityCatalog,
    CapacityPolicy,
    compute_capacity_catalog_hash,
    sync_capacity_catalog,
)
from coordinate.executor_identity import (
    ExecutorCatalog,
    ExecutorDefinition,
    ExecutorIdentityError,
    ExecutorInstanceBinding,
    compute_executor_binding_id,
    compute_executor_catalog_hash,
    get_executor_catalog_source,
    parse_executor_catalog,
    sync_executor_catalog,
)
from coordinate.runtime import RuntimeError, claim_job, register_agent, submit_request
from coordinate.schema import SCHEMA_VERSION, migrate


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures"


def _fixture_binding() -> dict[str, object]:
    return json.loads((FIXTURES / "executor_binding_v1.json").read_text(encoding="utf-8"))


def _fixture_catalog() -> dict[str, object]:
    return json.loads((FIXTURES / "executor_catalog_v1.json").read_text(encoding="utf-8"))


def _write_toml(content: str) -> Path:
    tmp = Path(tempfile.mkdtemp()) / "registry.toml"
    tmp.write_text(textwrap.dedent(content), encoding="utf-8")
    return tmp


class SchemaV12Tests(unittest.TestCase):
    def test_fresh_initialize_is_v14(self):
        conn = initialize(":memory:")
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        self.assertIn("executor_catalog_sources", tables)
        self.assertIn("executor_definitions", tables)
        self.assertIn("executor_instance_bindings", tables)
        self.assertIn("executor_capacity_sources", tables)
        self.assertIn("executor_capacity_policies", tables)
        self.assertIn("execution_attempt_leases", tables)

    def test_v11_upgrade_creates_executor_tables(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE workspaces (id TEXT PRIMARY KEY, name TEXT NOT NULL,
              path TEXT NOT NULL, harness_root TEXT NOT NULL, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL);
            CREATE TABLE agents (id TEXT PRIMARY KEY, name TEXT NOT NULL,
              capabilities_json TEXT NOT NULL, online_state TEXT NOT NULL,
              current_load INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL);
            CREATE TABLE runner_profiles (id TEXT PRIMARY KEY, name TEXT NOT NULL,
              runner_type TEXT NOT NULL, command TEXT NOT NULL,
              working_directory_strategy TEXT NOT NULL, supports_stream_attach INTEGER NOT NULL DEFAULT 0,
              env_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            """
        )
        conn.execute("PRAGMA user_version = 11")
        conn.commit()
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name LIKE 'executor_%'"
            ).fetchall()
        }
        self.assertIn("idx_executor_definitions_source_id", indexes)
        self.assertIn("idx_executor_instance_bindings_source_id", indexes)
        self.assertIn("idx_executor_instance_bindings_definition_id", indexes)
        self.assertIn("idx_executor_instance_bindings_profile_id", indexes)
        self.assertIn("idx_executor_capacity_policies_source_id", indexes)
        lease_indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'execution_attempt_leases'"
            ).fetchall()
        }
        self.assertIn("idx_execution_attempt_leases_active_resource", lease_indexes)
        self.assertIn("idx_execution_attempt_leases_agent_active", lease_indexes)
        self.assertIn("idx_execution_attempt_leases_expires", lease_indexes)

    def test_v11_upgrade_failure_rolls_back_all_v13_objects(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("CREATE TABLE executor_definitions (legacy_only TEXT)")
        conn.execute("PRAGMA user_version = 11")
        conn.commit()

        with self.assertRaisesRegex(sqlite3.OperationalError, "source_id"):
            migrate(conn)

        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 11)
        objects = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE name LIKE 'executor_%'"
            ).fetchall()
        }
        self.assertEqual(objects, {"executor_definitions"})
        self.assertFalse(conn.in_transaction)

    def test_schema_version_constant_matches_migration(self):
        self.assertEqual(SCHEMA_VERSION, 14)


class CanonicalBytesTests(unittest.TestCase):
    def test_catalog_fixture_hash_matches_computed(self):
        fixture = _fixture_catalog()
        expected_hash = hashlib.sha256(
            json.dumps(fixture, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        catalog = ExecutorCatalog(
            source_id=fixture["source_id"],
            source_version=fixture["source_version"],
            catalog_hash="",
            source_path=None,
            definitions=tuple(
                ExecutorDefinition(
                    id=d["id"],
                    provider=d["provider"],
                    adapter=d["adapter"],
                    capabilities=tuple(d["capabilities"]),
                )
                for d in fixture["executor_definitions"]
            ),
            bindings=tuple(
                ExecutorInstanceBinding(
                    agent_id=b["agent_id"],
                    executor_definition_id=b["executor_definition_id"],
                    runner_profile_id=b["runner_profile_id"],
                    enabled=b["enabled"],
                )
                for b in fixture["executor_instance_bindings"]
            ),
        )
        self.assertEqual(compute_executor_catalog_hash(catalog), expected_hash)

    def test_binding_fixture_digest_matches_computed(self):
        fixture = _fixture_binding()
        snapshot_without_id = {k: v for k, v in fixture.items() if k != "binding_id"}
        self.assertEqual(compute_executor_binding_id(snapshot_without_id), fixture["binding_id"])

    def test_catalog_hash_excludes_roster_fields(self):
        base = """
        [registry]
        id = "x"
        version = 1

        [[executor_definitions]]
        id = "d1"
        provider = "p"
        adapter = "a"
        capabilities = ["coding"]

        [[agents]]
        id = "a1"
        display_name = "A1"
        discord_user_id = "1"
        executor_definition_id = "d1"
        runner_profile_id = "a1"
        """
        p1 = _write_toml(base)
        p2 = _write_toml(base.replace('display_name = "A1"', 'display_name = "Changed"'))
        self.assertEqual(
            parse_executor_catalog(p1).catalog_hash,
            parse_executor_catalog(p2).catalog_hash,
        )


class ParserStrictnessTests(unittest.TestCase):
    def _valid_base(self) -> str:
        return textwrap.dedent("""
        [registry]
        id = "multinexus.discord"
        version = 2

        [[executor_definitions]]
        id = "omp-code"
        provider = "kimi-code"
        adapter = "omp"
        capabilities = ["coding"]

        [[agents]]
        id = "mac-omp"
        display_name = "Mac OMP"
        discord_user_id = "1001"
        executor_definition_id = "omp-code"
        runner_profile_id = "mac-omp"
        """).strip()

    def test_valid_catalog_parses(self):
        catalog = parse_executor_catalog(_write_toml(self._valid_base()))
        self.assertEqual(catalog.source_id, "multinexus.discord")
        self.assertEqual(catalog.source_version, 2)
        self.assertEqual(len(catalog.definitions), 1)
        self.assertEqual(len(catalog.bindings), 1)
        self.assertTrue(catalog.catalog_hash)

    def test_unknown_root_key_rejected(self):
        path = _write_toml(self._valid_base() + "\n[[secrets]]\nid = \"x\"\n")
        with self.assertRaisesRegex(ExecutorIdentityError, "unknown root keys"):
            parse_executor_catalog(path)

    def test_unknown_registry_key_rejected(self):
        path = _write_toml(
            self._valid_base().replace(
                '[registry]\nid = "multinexus.discord"\nversion = 2',
                '[registry]\nid = "multinexus.discord"\nversion = 2\nhash = "abc"',
            )
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "unknown \\[registry\\] keys"):
            parse_executor_catalog(path)

    def test_secret_entry_keys_rejected(self):
        for key in ("token", "token_env", "system_prompt", "command", "model", "bin"):
            path = _write_toml(self._valid_base() + f'\n        {key} = "secret"\n')
            with self.subTest(key=key):
                with self.assertRaisesRegex(ExecutorIdentityError, "unknown keys"):
                    parse_executor_catalog(path)

    def test_path_separator_in_provider_rejected(self):
        path = _write_toml(
            self._valid_base().replace('provider = "kimi-code"', 'provider = "kimi/code"')
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "unsafe characters"):
            parse_executor_catalog(path)

    def test_shell_metacharacter_in_adapter_rejected(self):
        for bad in ('a;b', 'a|b', 'a\u0026b', 'a`b', 'a$(b)', 'a\u003cb\u003e'):
            path = _write_toml(
                self._valid_base().replace('adapter = "omp"', f'adapter = "{bad}"')
            )
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(ExecutorIdentityError, "unsafe characters"):
                    parse_executor_catalog(path)

    def test_whitespace_in_capability_rejected(self):
        path = _write_toml(
            self._valid_base().replace('capabilities = ["coding"]', 'capabilities = ["coding review"]')
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "unsafe characters"):
            parse_executor_catalog(path)

    def test_duplicate_definition_rejected(self):
        base = self._valid_base()
        path = _write_toml(base + '\n        [[executor_definitions]]\n        id = "omp-code"\n        provider = "p2"\n        adapter = "a2"\n        capabilities = ["review"]\n')
        with self.assertRaisesRegex(ExecutorIdentityError, "duplicate executor_definition id"):
            parse_executor_catalog(path)

    def test_duplicate_capability_rejected(self):
        path = _write_toml(
            self._valid_base().replace('capabilities = ["coding"]', 'capabilities = ["coding", "coding"]')
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "duplicate capability"):
            parse_executor_catalog(path)

    def test_unsorted_capabilities_rejected(self):
        path = _write_toml(
            self._valid_base().replace('capabilities = ["coding"]', 'capabilities = ["review", "coding"]')
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "must be sorted"):
            parse_executor_catalog(path)

    def test_external_agent_binding_rejected(self):
        base = self._valid_base()
        extra = """
        [[external_agents]]
        id = "server-hermes"
        display_name = "Hermes"
        discord_user_id = "1002"
        executor_definition_id = "omp-code"
        runner_profile_id = "server-hermes"
        """
        path = _write_toml(base + extra)
        with self.assertRaisesRegex(ExecutorIdentityError, "external agent.*must not carry executor bindings"):
            parse_executor_catalog(path)

    def test_external_agent_enabled_flag_rejected(self):
        base = self._valid_base()
        extra = """
        [[external_agents]]
        id = "server-hermes"
        display_name = "Hermes"
        discord_user_id = "1002"
        enabled = false
        """
        path = _write_toml(base + extra)
        with self.assertRaisesRegex(ExecutorIdentityError, "external agent.*must not carry executor bindings"):
            parse_executor_catalog(path)

    def test_enabled_must_be_boolean(self):
        path = _write_toml(self._valid_base() + "\nenabled = 1\n")
        with self.assertRaisesRegex(ExecutorIdentityError, "enabled must be a boolean"):
            parse_executor_catalog(path)

    def test_missing_runner_profile_id_rejected(self):
        path = _write_toml(
            self._valid_base().replace('runner_profile_id = "mac-omp"', "")
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "must set both"):
            parse_executor_catalog(path)

    def test_runner_profile_id_must_equal_agent_id(self):
        path = _write_toml(
            self._valid_base().replace('runner_profile_id = "mac-omp"', 'runner_profile_id = "other"')
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "runner_profile_id must equal agent_id"):
            parse_executor_catalog(path)


class SyncAtomicityTests(unittest.TestCase):
    def setUp(self):
        self.conn = initialize(":memory:")
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        register_agent(self.conn, agent_id="mac-claude", host_id="mac", capabilities={})

    def _catalog(self, version: int = 2) -> ExecutorCatalog:
        return parse_executor_catalog(
            _write_toml(
                f"""
                [registry]
                id = "multinexus.discord"
                version = {version}

                [[executor_definitions]]
                id = "omp-code"
                provider = "kimi-code"
                adapter = "omp"
                capabilities = ["coding", "review"]

                [[agents]]
                id = "mac-omp"
                display_name = "Mac OMP"
                discord_user_id = "1001"
                executor_definition_id = "omp-code"
                runner_profile_id = "mac-omp"
                """
            )
        )

    def test_sync_creates_source_definitions_and_bindings(self):
        catalog = self._catalog()
        result = sync_executor_catalog(self.conn, catalog)
        self.assertTrue(result["changed"])
        self.assertEqual(result["added_binding_ids"], ["mac-omp"])
        self.assertEqual(result["added_definition_ids"], ["omp-code"])
        self.assertEqual(
            get_executor_catalog_source(self.conn, "multinexus.discord")["catalog_hash"],
            catalog.catalog_hash,
        )

    def test_sync_is_idempotent(self):
        catalog = self._catalog()
        sync_executor_catalog(self.conn, catalog)
        result = sync_executor_catalog(self.conn, catalog)
        self.assertFalse(result["changed"])

    def test_sync_replaces_definition_capabilities(self):
        catalog = self._catalog()
        sync_executor_catalog(self.conn, catalog)
        catalog2 = parse_executor_catalog(
            _write_toml(
                """
                [registry]
                id = "multinexus.discord"
                version = 3

                [[executor_definitions]]
                id = "omp-code"
                provider = "kimi-code"
                adapter = "omp"
                capabilities = ["coding"]

                [[agents]]
                id = "mac-omp"
                display_name = "Mac OMP"
                discord_user_id = "1001"
                executor_definition_id = "omp-code"
                runner_profile_id = "mac-omp"
                """
            )
        )
        result = sync_executor_catalog(self.conn, catalog2)
        self.assertTrue(result["changed"])
        self.assertEqual(result["updated_definition_ids"], ["omp-code"])

    def test_sync_delta_covers_definition_labels_and_preserves_binding(self):
        sync_executor_catalog(self.conn, self._catalog(version=2))
        catalog2 = parse_executor_catalog(
            _write_toml(
                textwrap.dedent(
                    """
                    [registry]
                    id = "multinexus.discord"
                    version = 3

                    [[executor_definitions]]
                    id = "omp-code"
                    provider = "kimi-code"
                    adapter = "opencode"
                    capabilities = ["coding", "review"]

                    [[agents]]
                    id = "mac-omp"
                    display_name = "Mac OMP"
                    discord_user_id = "1001"
                    executor_definition_id = "omp-code"
                    runner_profile_id = "mac-omp"
                    """
                )
            )
        )

        result = sync_executor_catalog(self.conn, catalog2)

        self.assertEqual(result["updated_definition_ids"], ["omp-code"])
        self.assertEqual(result["unchanged_binding_ids"], ["mac-omp"])
        self.assertEqual(result["updated_binding_ids"], [])

    def test_sync_rejects_version_downgrade(self):
        sync_executor_catalog(self.conn, self._catalog(version=3))
        with self.assertRaisesRegex(ExecutorIdentityError, "version downgrade"):
            sync_executor_catalog(self.conn, self._catalog(version=2))

    def test_sync_rejects_same_version_different_hash(self):
        sync_executor_catalog(self.conn, self._catalog(version=2))
        catalog2 = parse_executor_catalog(
            _write_toml(
                """
                [registry]
                id = "multinexus.discord"
                version = 2

                [[executor_definitions]]
                id = "omp-code"
                provider = "kimi-code"
                adapter = "omp"
                capabilities = ["coding", "different"]

                [[agents]]
                id = "mac-omp"
                display_name = "Mac OMP"
                discord_user_id = "1001"
                executor_definition_id = "omp-code"
                runner_profile_id = "mac-omp"
                """
            )
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "hash changed without version bump"):
            sync_executor_catalog(self.conn, catalog2)

    def test_sync_rejects_takeover_of_definition_id(self):
        sync_executor_catalog(self.conn, self._catalog())
        other = parse_executor_catalog(
            _write_toml(
                """
                [registry]
                id = "other.source"
                version = 1

                [[executor_definitions]]
                id = "omp-code"
                provider = "x"
                adapter = "x"
                capabilities = ["coding"]
                """
            )
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "owned by source"):
            sync_executor_catalog(self.conn, other)

    def test_sync_preserves_other_source(self):
        sync_executor_catalog(self.conn, self._catalog())
        other = parse_executor_catalog(
            _write_toml(
                """
                [registry]
                id = "other.source"
                version = 1

                [[executor_definitions]]
                id = "other-def"
                provider = "x"
                adapter = "x"
                capabilities = ["coding"]
                """
            )
        )
        sync_executor_catalog(self.conn, other)
        sources = self.conn.execute(
            "SELECT source_id FROM executor_catalog_sources ORDER BY source_id"
        ).fetchall()
        self.assertEqual([r["source_id"] for r in sources], ["multinexus.discord", "other.source"])

    def test_sync_rejects_missing_agent(self):
        catalog = parse_executor_catalog(
            _write_toml(
                """
                [registry]
                id = "multinexus.discord"
                version = 2

                [[executor_definitions]]
                id = "omp-code"
                provider = "kimi-code"
                adapter = "omp"
                capabilities = ["coding"]

                [[agents]]
                id = "missing-agent"
                display_name = "Missing"
                discord_user_id = "1"
                executor_definition_id = "omp-code"
                runner_profile_id = "missing-agent"
                """
            )
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "references unknown agent"):
            sync_executor_catalog(self.conn, catalog)

    def test_sync_rejects_missing_runner_profile(self):
        self.conn.execute("DELETE FROM runner_profiles WHERE id = ?", ("mac-omp",))
        self.conn.commit()
        with self.assertRaisesRegex(ExecutorIdentityError, "references unknown runner profile"):
            sync_executor_catalog(self.conn, self._catalog())

    def test_sync_rejects_non_agentd_instance_without_mutation(self):
        self.conn.execute(
            "UPDATE agents SET client_type = 'bridge' WHERE id = ?", ("mac-omp",)
        )
        self.conn.commit()

        with self.assertRaisesRegex(ExecutorIdentityError, "non-agentd instance"):
            sync_executor_catalog(self.conn, self._catalog())

        self.assertIsNone(get_executor_catalog_source(self.conn, "multinexus.discord"))

    def test_sync_rejects_non_agentd_runner_profile_without_mutation(self):
        self.conn.execute(
            "UPDATE runner_profiles SET runner_type = 'command' WHERE id = ?",
            ("mac-omp",),
        )
        self.conn.commit()

        with self.assertRaisesRegex(ExecutorIdentityError, "non-agentd runner profile"):
            sync_executor_catalog(self.conn, self._catalog())

        self.assertIsNone(get_executor_catalog_source(self.conn, "multinexus.discord"))

    def test_sync_blocks_catalog_change_with_in_flight_typed_job(self):
        sync_executor_catalog(self.conn, self._catalog())
        upsert_workspace(
            self.conn, workspace_id="ws", name="WS", path="/ws", harness_root="/ws"
        )
        upsert_workspace_host_profile(
            self.conn, workspace_id="ws", host_id="mac", workspace_path="/ws", harness_root="/ws"
        )
        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        catalog2 = parse_executor_catalog(
            _write_toml(
                """
                [registry]
                id = "multinexus.discord"
                version = 3

                [[executor_definitions]]
                id = "omp-code"
                provider = "kimi-code"
                adapter = "omp"
                capabilities = ["coding", "newcap", "review"]

                [[agents]]
                id = "mac-omp"
                display_name = "Mac OMP"
                discord_user_id = "1001"
                executor_definition_id = "omp-code"
                runner_profile_id = "mac-omp"
                """
            )
        )
        with self.assertRaisesRegex(ExecutorIdentityError, "in-flight typed jobs"):
            sync_executor_catalog(self.conn, catalog2)

    def test_sync_allows_same_hash_retry_with_in_flight_job(self):
        sync_executor_catalog(self.conn, self._catalog())
        upsert_workspace(
            self.conn, workspace_id="ws", name="WS", path="/ws", harness_root="/ws"
        )
        upsert_workspace_host_profile(
            self.conn, workspace_id="ws", host_id="mac", workspace_path="/ws", harness_root="/ws"
        )
        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        result = sync_executor_catalog(self.conn, self._catalog())
        self.assertFalse(result["changed"])


class RuntimeBindingIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn, workspace_id="ws", name="WS", path=self.tmp.name, harness_root=self.tmp.name
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="ws",
            host_id="mac",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        catalog = parse_executor_catalog(
            _write_toml(
                """
                [registry]
                id = "multinexus.discord"
                version = 2

                [[executor_definitions]]
                id = "omp-code"
                provider = "kimi-code"
                adapter = "omp"
                capabilities = ["coding", "review"]

                [[agents]]
                id = "mac-omp"
                display_name = "Mac OMP"
                discord_user_id = "1001"
                executor_definition_id = "omp-code"
                runner_profile_id = "mac-omp"
                """
            )
        )
        sync_executor_catalog(self.conn, catalog)
        self._sync_capacity(["mac-omp"])

    def _sync_capacity(self, agent_ids: list[str]):
        policies = tuple(
            CapacityPolicy(agent_id=aid, max_concurrent_jobs=2) for aid in agent_ids
        )
        catalog = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path="/dev/null",
            policies=policies,
        )
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_capacity_catalog_hash(catalog)
        )
        sync_capacity_catalog(self.conn, catalog)

    def test_typed_submit_creates_binding_snapshot(self):
        result = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        payload = result.job["payload"]
        binding = payload["executor_binding"]
        self.assertIsNotNone(binding)
        self.assertEqual(binding["executor_instance_id"], "mac-omp")
        self.assertEqual(binding["runner_profile_id"], "mac-omp")
        self.assertEqual(binding["adapter"], "omp")
        self.assertEqual(result.job["runner_profile_id"], "mac-omp")
        self.assertTrue(binding["binding_id"].startswith("sha256:"))

    def test_legacy_untyped_submit_has_null_binding(self):
        register_agent(self.conn, agent_id="mac-claude", host_id="mac", capabilities={})
        result = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-claude",
            prompt="go",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        self.assertIsNone(result.job["payload"]["executor_binding"])

    def test_replay_returns_same_binding_when_unchanged(self):
        origin = {"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"}
        first = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin=origin,
            reply={"platform": "discord", "destination": "ch"},
        )
        second = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin=origin,
            reply={"platform": "discord", "destination": "ch"},
        )
        self.assertFalse(second.job_created)
        self.assertEqual(
            first.job["payload"]["executor_binding"]["binding_id"],
            second.job["payload"]["executor_binding"]["binding_id"],
        )

    def test_replay_rejects_changed_catalog(self):
        origin = {"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"}
        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin=origin,
            reply={"platform": "discord", "destination": "ch"},
        )
        # Mutate the current catalog definition directly (bypassing the sync
        # guard) so the stored binding no longer matches the resolved binding.
        self.conn.execute(
            "UPDATE executor_definitions SET adapter = ? WHERE id = ?",
            ("changed", "omp-code"),
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeError, "request replay: executor_binding conflicts"):
            submit_request(
                self.conn,
                workspace_id="ws",
                target_agent="mac-omp",
                prompt="go",
                origin=origin,
                reply={"platform": "discord", "destination": "ch"},
            )

    def test_claim_validates_binding_before_cas(self):
        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        result = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(result.claimed)
        self.assertEqual(result.job["status"], "running")

    def test_claim_rejects_binding_mismatch(self):
        result = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        # Tamper with the stored binding so it no longer matches the current catalog.
        payload = dict(result.job["payload"])
        payload["executor_binding"]["adapter"] = "tampered"
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")), result.job["id"]),
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeError, "executor_binding_mismatch"):
            claim_job(self.conn, agent_id="mac-omp")

    def test_claim_extra_field_error_is_bounded(self):
        result = submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "bounded",
                "session_scope_id": "discord:test",
            },
            reply={"platform": "discord", "destination": "ch"},
        )
        payload = dict(result.job["payload"])
        for index in range(100):
            payload["executor_binding"][f"unexpected_{index}"] = "x"
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), result.job["id"]),
        )
        self.conn.commit()

        with self.assertRaises(RuntimeError) as caught:
            claim_job(self.conn, agent_id="mac-omp")

        self.assertLess(len(str(caught.exception)), 512)
        self.assertIn("unexpected_count=100", str(caught.exception))
        row = self.conn.execute(
            "SELECT status, attempt_count FROM jobs WHERE id = ?", (result.job["id"],)
        ).fetchone()
        self.assertEqual((row["status"], row["attempt_count"]), ("pending", 0))

    def test_claimed_event_has_redacted_binding_evidence(self):
        from coordinate.db import list_events

        submit_request(
            self.conn,
            workspace_id="ws",
            target_agent="mac-omp",
            prompt="go",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        claim_job(self.conn, agent_id="mac-omp")
        claimed = [
            row_to_dict(row)
            for row in list_events(self.conn, "ws")
            if row["event_type"] == "job.claimed"
        ]
        self.assertEqual(len(claimed), 1)
        payload = claimed[0]["payload"]
        self.assertIn("executor_binding_id", payload)
        self.assertIn("executor_definition_id", payload)
        self.assertIn("executor_instance_id", payload)
        self.assertIn("runner_profile_id", payload)
        self.assertIn("catalog_hash", payload)
        self.assertNotIn("prompt", payload)
        self.assertNotIn("command", payload)
        self.assertNotIn("env", payload)


if __name__ == "__main__":
    unittest.main()
