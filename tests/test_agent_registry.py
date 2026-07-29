import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from coordinate.agent_registry import parse_agents_toml
from coordinate.db import (
    build_agent_registry_map,
    get_agent_discord_id,
    initialize,
    list_events,
    remove_workspace_agent_override,
    resolve_effective_agents,
    row_to_dict,
    set_workspace_agent,
    sync_workspace_agents,
    upsert_workspace,
)


class AgentRegistryParseTests(unittest.TestCase):
    def _write_toml(self, content: str) -> str:
        tmp = tempfile.NamedTemporaryFile(suffix=".toml", mode="w", delete=False)
        tmp.write(content)
        tmp.flush()
        tmp.close()
        return tmp.name

    def test_managed_and_external_agents(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
display_name = "Mac Claude"
discord_user_id = 12345

[[external_agents]]
id = "mac-openclaw"
display_name = "小龙虾"
discord_user_id = 67890
""")
        result = parse_agents_toml(path)
        self.assertEqual(len(result.agents), 2)
        self.assertEqual(result.agents[0].id, "mac-claude")
        self.assertEqual(result.agents[0].agent_type, "managed")
        self.assertEqual(result.agents[1].id, "mac-openclaw")
        self.assertEqual(result.agents[1].agent_type, "external")
        self.assertEqual(len(result.skipped), 0)
        self.assertEqual(len(result.errors), 0)

    def test_missing_discord_user_id_skipped(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
display_name = "Mac Claude"

[[agents]]
id = "mac-codex"
display_name = "Mac Codex"
discord_user_id = 11111
""")
        result = parse_agents_toml(path)
        self.assertEqual(len(result.agents), 1)
        self.assertEqual(result.agents[0].id, "mac-codex")
        self.assertEqual(len(result.skipped), 1)
        self.assertEqual(result.skipped[0]["id"], "mac-claude")
        self.assertEqual(result.skipped[0]["reason"], "missing discord_user_id")
        self.assertEqual(len(result.errors), 0)

    def test_duplicate_id_fail_closed(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
display_name = "Mac Claude"
discord_user_id = 11111

[[external_agents]]
id = "mac-claude"
display_name = "Duplicate"
discord_user_id = 22222
""")
        result = parse_agents_toml(path)
        self.assertTrue(any("duplicate agent id 'mac-claude'" in e for e in result.errors))
        self.assertEqual(len(result.agents), 1)

    def test_duplicate_id_with_skipped_entry_fail_closed(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
display_name = "Mac Claude"

[[external_agents]]
id = "mac-claude"
display_name = "Duplicate"
discord_user_id = 22222
""")
        result = parse_agents_toml(path)
        self.assertTrue(any("duplicate agent id 'mac-claude'" in e for e in result.errors))
        self.assertEqual(len(result.agents), 0)
        self.assertEqual(len(result.skipped), 1)

    def test_duplicate_discord_user_id_fail_closed(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
display_name = "Mac Claude"
discord_user_id = 11111

[[agents]]
id = "mac-codex"
display_name = "Mac Codex"
discord_user_id = 11111
""")
        result = parse_agents_toml(path)
        self.assertTrue(any("duplicate discord_user_id '11111'" in e for e in result.errors))
        self.assertEqual(len(result.agents), 1)

    def test_missing_id_is_error(self):
        path = self._write_toml("""
[[agents]]
display_name = "No ID"
discord_user_id = 11111
""")
        result = parse_agents_toml(path)
        self.assertEqual(len(result.errors), 1)
        self.assertIn("missing 'id'", result.errors[0])

    def test_missing_id_error_does_not_echo_raw_entry(self):
        path = self._write_toml("""
[[agents]]
display_name = "No ID"
token_env = "SECRET_TOKEN_HERE"
discord_user_id = 11111
""")
        result = parse_agents_toml(path)
        self.assertEqual(len(result.errors), 1)
        self.assertNotIn("SECRET_TOKEN_HERE", result.errors[0])
        self.assertNotIn("token_env", result.errors[0])

    def test_defaults_display_name_to_id(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
discord_user_id = 11111
""")
        result = parse_agents_toml(path)
        self.assertEqual(result.agents[0].display_name, "mac-claude")

    def test_registry_source_parsed(self):
        path = self._write_toml("""
[registry]
id = "multinexus.discord"
version = 1

[[agents]]
id = "mac-claude"
discord_user_id = 11111
""")
        result = parse_agents_toml(path)
        self.assertIsNotNone(result.source)
        self.assertEqual(result.source.source_id, "multinexus.discord")
        self.assertEqual(result.source.source_version, 1)
        self.assertIsNotNone(result.source.source_hash)
        self.assertEqual(len(result.source.source_hash), 64)

    def test_registry_version_must_be_an_integer(self):
        for value in ("true", "1.5"):
            path = self._write_toml(f"""
[registry]
id = "multinexus.discord"
version = {value}

[[agents]]
id = "mac-claude"
discord_user_id = 11111
""")
            result = parse_agents_toml(path)
            self.assertIsNone(result.source.source_version)
            self.assertIsNone(result.source.source_hash)

    def test_invalid_discord_user_id_is_error(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
discord_user_id = "not-a-number"
""")
        result = parse_agents_toml(path)
        self.assertEqual(len(result.agents), 0)
        self.assertTrue(any("invalid discord_user_id" in e for e in result.errors))

    def test_non_ascii_decimal_discord_user_id_is_error(self):
        path = self._write_toml("""
[[agents]]
id = "mac-claude"
discord_user_id = "١٢٣"
""")
        result = parse_agents_toml(path)
        self.assertEqual(result.agents, [])
        self.assertTrue(any("invalid discord_user_id" in e for e in result.errors))

    def test_canonical_hash_excludes_source_and_secrets(self):
        path = self._write_toml("""
[registry]
id = "a"
version = 1

[[agents]]
id = "z"
display_name = "Z"
discord_user_id = 1

[[agents]]
id = "a"
display_name = "A"
discord_user_id = 2
token_env = "SECRET"
""")
        result = parse_agents_toml(path)
        self.assertEqual(len(result.agents), 2)
        agents_by_id = {a.id: a for a in result.agents}
        self.assertIn("a", agents_by_id)
        self.assertIn("z", agents_by_id)
        payload = [
            {
                "id": "a",
                "discord_user_id": "2",
                "display_name": "A",
                "agent_type": "managed",
            },
            {
                "id": "z",
                "discord_user_id": "1",
                "display_name": "Z",
                "agent_type": "managed",
            },
        ]
        expected = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        self.assertEqual(result.source.source_hash, expected)


class SyncWorkspaceAgentsTests(unittest.TestCase):
    def setUp(self):
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

    def tearDown(self):
        self.conn.close()

    def _source(self, version: int = 1, id_: str = "multinexus.discord", hash_: str = "a" * 64):
        return {"source_id": id_, "source_version": version, "source_hash": hash_}

    def test_sync_adds_authoritative_and_increments_revision(self):
        result = sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[
                {"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"},
                {"id": "mac-codex", "discord_user_id": "222", "display_name": "Codex", "agent_type": "managed"},
            ],
            **self._source(),
        )
        self.assertEqual(result["added"], ["mac-claude", "mac-codex"])
        self.assertEqual(result["removed"], [])
        self.assertEqual(result["updated"], [])
        self.assertEqual(result["unchanged"], [])
        self.assertEqual(result["source_id"], "multinexus.discord")
        self.assertEqual(result["revision"], 1)
        self.assertEqual(get_agent_discord_id(self.conn, "demo", "mac-claude"), "111")

    def test_sync_preserves_existing_overrides(self):
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="manual-agent",
            discord_user_id="999",
            actor="fixture",
            reason="test override",
        )
        sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[
                {"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"},
            ],
            **self._source(),
        )
        self.assertEqual(get_agent_discord_id(self.conn, "demo", "manual-agent"), "999")

    def test_sync_replaces_previous_authoritative(self):
        sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}],
            **self._source(version=1),
        )
        result = sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[{"id": "mac-codex", "discord_user_id": "222", "display_name": "Codex", "agent_type": "managed"}],
            **self._source(version=2),
        )
        self.assertEqual(result["added"], ["mac-codex"])
        self.assertEqual(result["removed"], ["mac-claude"])
        self.assertIsNone(get_agent_discord_id(self.conn, "demo", "mac-claude"))
        self.assertEqual(get_agent_discord_id(self.conn, "demo", "mac-codex"), "222")

    def test_sync_idempotent_same_version_and_hash(self):
        entries = [{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}]
        sync_workspace_agents(self.conn, workspace_id="demo", entries=entries, **self._source(version=1, hash_="a" * 64))
        result = sync_workspace_agents(self.conn, workspace_id="demo", entries=entries, **self._source(version=1, hash_="a" * 64))
        self.assertEqual(result["added"], [])
        self.assertEqual(result["updated"], [])
        self.assertEqual(result["removed"], [])
        self.assertEqual(result["unchanged"], ["mac-claude"])

    def test_sync_rejects_version_conflict(self):
        entries = [{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}]
        sync_workspace_agents(self.conn, workspace_id="demo", entries=entries, **self._source(version=1, hash_="a" * 64))
        with self.assertRaises(ValueError) as ctx:
            sync_workspace_agents(self.conn, workspace_id="demo", entries=entries, **self._source(version=1, hash_="b" * 64))
        self.assertIn("version conflict", str(ctx.exception))

    def test_sync_rejects_rollback(self):
        entries = [{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}]
        sync_workspace_agents(self.conn, workspace_id="demo", entries=entries, **self._source(version=2))
        with self.assertRaises(ValueError) as ctx:
            sync_workspace_agents(self.conn, workspace_id="demo", entries=entries, **self._source(version=1))
        self.assertIn("rollback", str(ctx.exception))

    def test_sync_rejects_source_takeover(self):
        sync_workspace_agents(self.conn, workspace_id="demo", entries=[], **self._source(version=1, id_="first"))
        with self.assertRaises(ValueError) as ctx:
            sync_workspace_agents(self.conn, workspace_id="demo", entries=[], **self._source(version=1, id_="second"))
        self.assertIn("takeover", str(ctx.exception))

    def test_sync_rejects_missing_source(self):
        # parse_agents_toml would have failed to produce source metadata.
        with self.assertRaises(ValueError) as ctx:
            sync_workspace_agents(
                self.conn,
                workspace_id="demo",
                source_id="",
                source_version=0,
                source_hash="a" * 64,
                entries=[{"id": "x", "discord_user_id": "1", "display_name": "X", "agent_type": "managed"}],
            )
        self.assertIn("source_id is required", str(ctx.exception))

    def test_sync_rejects_boolean_source_version(self):
        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            sync_workspace_agents(
                self.conn,
                workspace_id="demo",
                source_id="s",
                source_version=True,
                source_hash="a" * 64,
                entries=[],
            )

    def test_sync_rejects_duplicate_discord_id_in_entries(self):
        with self.assertRaises(ValueError) as ctx:
            sync_workspace_agents(
                self.conn,
                workspace_id="demo",
                entries=[
                    {"id": "a", "discord_user_id": "111", "display_name": "A", "agent_type": "managed"},
                    {"id": "b", "discord_user_id": "111", "display_name": "B", "agent_type": "managed"},
                ],
                **self._source(),
            )
        self.assertIn("duplicate discord_user_id", str(ctx.exception))

    def test_sync_rejects_duplicate_discord_id_with_existing_override(self):
        set_workspace_agent(self.conn, workspace_id="demo", agent_name="existing", discord_user_id="111", actor="fixture", reason="x")
        with self.assertRaises(ValueError) as ctx:
            sync_workspace_agents(
                self.conn,
                workspace_id="demo",
                entries=[{"id": "new", "discord_user_id": "111", "display_name": "New", "agent_type": "managed"}],
                **self._source(),
            )
        self.assertIn("duplicate effective", str(ctx.exception).lower())

    def test_sync_reports_shadowed_names(self):
        set_workspace_agent(self.conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="999", actor="fixture", reason="override")
        result = sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}],
            **self._source(),
        )
        self.assertEqual(result["shadowed"], ["mac-claude"])
        self.assertEqual(result["added"], ["mac-claude"])
        self.assertEqual(result["unchanged"], [])
        self.assertEqual(get_agent_discord_id(self.conn, "demo", "mac-claude"), "999")
        event = row_to_dict(list(list_events(self.conn, "demo"))[-1])
        self.assertEqual(event["event_type"], "workspace.agent_registry.synced")
        self.assertEqual(event["payload"]["shadowed"], result["shadowed"])

    def test_unknown_workspace_raises(self):
        with self.assertRaises(ValueError) as ctx:
            sync_workspace_agents(
                self.conn,
                workspace_id="nonexistent",
                entries=[],
                **self._source(),
            )
        self.assertIn("unknown workspace", str(ctx.exception))


class OverrideRegistryTests(unittest.TestCase):
    def setUp(self):
        self.conn = initialize(":memory:")
        upsert_workspace(self.conn, workspace_id="demo", name="Demo", path=".", harness_root=".")

    def tearDown(self):
        self.conn.close()

    def test_set_workspace_agent_requires_actor_and_reason(self):
        with self.assertRaises(TypeError):
            set_workspace_agent(self.conn, workspace_id="demo", agent_name="x", discord_user_id="1")
        with self.assertRaises(ValueError):
            set_workspace_agent(
                self.conn,
                workspace_id="demo",
                agent_name="x",
                discord_user_id="1",
                actor="fixture",
                reason="",
            )

    def test_set_workspace_agent_with_reason_appends_audit(self):
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="x",
            discord_user_id="1",
            actor="fixture",
            reason="test",
        )
        events = list(list_events(self.conn, "demo"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "workspace.agent_override.set")

    def test_override_shadows_authoritative(self):
        sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}],
            source_id="s", source_version=1, source_hash="a" * 64,
        )
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-claude",
            discord_user_id="999",
            actor="fixture",
            reason="manual",
        )
        self.assertEqual(get_agent_discord_id(self.conn, "demo", "mac-claude"), "999")
        effective = resolve_effective_agents(self.conn, "demo")
        self.assertEqual(effective["mac-claude"]["agent_type"], "override")

    def test_remove_override_reveals_authoritative(self):
        sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}],
            source_id="s", source_version=1, source_hash="a" * 64,
        )
        set_workspace_agent(self.conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="999", actor="fixture", reason="manual")
        remove_workspace_agent_override(
            self.conn,
            workspace_id="demo",
            agent_name="mac-claude",
            actor="operator",
            reason="done",
        )
        self.assertEqual(get_agent_discord_id(self.conn, "demo", "mac-claude"), "111")

    def test_remove_missing_override_fails(self):
        with self.assertRaises(ValueError):
            remove_workspace_agent_override(
                self.conn,
                workspace_id="demo",
                agent_name="missing",
                actor="operator",
                reason="cleanup",
            )

    def test_expired_override_ignored(self):
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        expiry = (t0 + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        t1 = t0 + timedelta(seconds=2)
        sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[{"id": "mac-claude", "discord_user_id": "111", "display_name": "Claude", "agent_type": "managed"}],
            source_id="s", source_version=1, source_hash="a" * 64,
            now_utc=t0,
        )
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-claude",
            discord_user_id="999",
            actor="fixture",
            reason="manual",
            expires_at=expiry,
            now_utc=t0,
        )
        effective = resolve_effective_agents(self.conn, "demo", now_utc=t1)
        self.assertEqual(effective["mac-claude"]["discord_user_id"], "111")

    def test_duplicate_effective_discord_id_rejected(self):
        set_workspace_agent(self.conn, workspace_id="demo", agent_name="a", discord_user_id="111", actor="fixture", reason="x")
        with self.assertRaises(ValueError):
            set_workspace_agent(self.conn, workspace_id="demo", agent_name="b", discord_user_id="111", actor="fixture", reason="y")

    def test_override_audit_failure_rolls_back_all_state(self):
        before = self.conn.execute(
            "SELECT agent_registry_revision, agents_json FROM workspaces WHERE id = 'demo'"
        ).fetchone()
        with patch("coordinate.db.append_event", side_effect=RuntimeError("audit down")):
            with self.assertRaisesRegex(RuntimeError, "audit down"):
                set_workspace_agent(
                    self.conn,
                    workspace_id="demo",
                    agent_name="x",
                    discord_user_id="1",
                    actor="fixture",
                    reason="atomicity",
                )
        after = self.conn.execute(
            "SELECT agent_registry_revision, agents_json FROM workspaces WHERE id = 'demo'"
        ).fetchone()
        self.assertEqual(tuple(after), tuple(before))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM workspace_agent_registry_entries WHERE workspace_id = 'demo'"
            ).fetchone()[0],
            0,
        )

    def test_sync_audit_failure_rolls_back_all_state(self):
        before = self.conn.execute(
            "SELECT agent_registry_revision, agents_json FROM workspaces WHERE id = 'demo'"
        ).fetchone()
        with patch("coordinate.db.append_event", side_effect=RuntimeError("audit down")):
            with self.assertRaisesRegex(RuntimeError, "audit down"):
                sync_workspace_agents(
                    self.conn,
                    workspace_id="demo",
                    source_id="s",
                    source_version=1,
                    source_hash="a" * 64,
                    entries=[
                        {"id": "x", "discord_user_id": "1", "display_name": "X", "agent_type": "managed"}
                    ],
                )
        after = self.conn.execute(
            "SELECT agent_registry_revision, agents_json FROM workspaces WHERE id = 'demo'"
        ).fetchone()
        self.assertEqual(tuple(after), tuple(before))
        self.assertIsNone(
            self.conn.execute(
                "SELECT workspace_id FROM workspace_agent_registry_sources WHERE workspace_id = 'demo'"
            ).fetchone()
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM workspace_agent_registry_entries WHERE workspace_id = 'demo'"
            ).fetchone()[0],
            0,
        )

    def test_remove_audit_failure_rolls_back_all_state(self):
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="x",
            discord_user_id="1",
            actor="fixture",
            reason="setup",
        )
        before = self.conn.execute(
            "SELECT agent_registry_revision, agents_json FROM workspaces WHERE id = 'demo'"
        ).fetchone()
        with patch("coordinate.db.append_event", side_effect=RuntimeError("audit down")):
            with self.assertRaisesRegex(RuntimeError, "audit down"):
                remove_workspace_agent_override(
                    self.conn,
                    workspace_id="demo",
                    agent_name="x",
                    actor="fixture",
                    reason="atomicity",
                )
        after = self.conn.execute(
            "SELECT agent_registry_revision, agents_json FROM workspaces WHERE id = 'demo'"
        ).fetchone()
        self.assertEqual(tuple(after), tuple(before))
        self.assertEqual(get_agent_discord_id(self.conn, "demo", "x"), "1")

    def test_build_agent_registry_map_is_workspace_scoped(self):
        upsert_workspace(self.conn, workspace_id="other", name="Other", path=".", harness_root=".")
        sync_workspace_agents(
            self.conn,
            workspace_id="demo",
            entries=[{"id": "ag", "discord_user_id": "123", "display_name": "AG", "agent_type": "managed"}],
            source_id="s", source_version=1, source_hash="a" * 64,
        )
        sync_workspace_agents(
            self.conn,
            workspace_id="other",
            entries=[{"id": "ag", "discord_user_id": "123", "display_name": "AG", "agent_type": "managed"}],
            source_id="s", source_version=1, source_hash="a" * 64,
        )
        registry = build_agent_registry_map(self.conn)
        self.assertEqual(registry, {123: {"demo": "ag", "other": "ag"}})


class LegacyMigrationTests(unittest.TestCase):
    def test_v9_agents_json_backfilled_as_legacy(self):
        from coordinate.schema import migrate
        conn = initialize(":memory:")
        # Fresh initialize already at v10 with no legacy rows.
        conn.close()

        conn = initialize(":memory:")
        # Simulate a v9 workspace by reverting user_version and inserting agents_json.
        conn.execute("PRAGMA user_version = 9")
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at, agents_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("legacy-ws", "Legacy", ".", ".", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", '{"old-agent": {"discord_user_id": "12345"}}'),
        )
        conn.commit()
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        rows = conn.execute(
            "SELECT agent_name, entry_kind, discord_user_id FROM workspace_agent_registry_entries WHERE workspace_id = ?",
            ("legacy-ws",),
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["agent_name"], "old-agent")
        self.assertEqual(rows[0]["entry_kind"], "legacy")
        self.assertEqual(rows[0]["discord_user_id"], "12345")
        self.assertEqual(get_agent_discord_id(conn, "legacy-ws", "old-agent"), "12345")
        conn.close()

    def test_v9_invalid_agents_json_blocks_migration(self):
        from coordinate.schema import migrate
        conn = initialize(":memory:")
        conn.execute("PRAGMA user_version = 9")
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at, agents_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("bad-ws", "Bad", ".", ".", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", '["not-an-object"]'),
        )
        conn.commit()
        with self.assertRaises(ValueError):
            migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 9)
        conn.close()

    def test_v10_reopen_is_idempotent(self):
        from coordinate.schema import migrate
        conn = initialize(":memory:")
        conn.execute("PRAGMA user_version = 9")
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at, agents_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("ws", "WS", ".", ".", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", '{"a": {"discord_user_id": "1"}}'),
        )
        conn.commit()
        migrate(conn)
        migrate(conn)
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM workspace_agent_registry_entries WHERE workspace_id = ?",
            ("ws",),
        ).fetchone()
        self.assertEqual(rows["n"], 1)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        conn.close()


if __name__ == "__main__":
    unittest.main()
