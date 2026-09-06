import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinate.db import (
    append_event,
    assert_schema_compatible,
    create_delivery,
    create_job,
    create_decision_request,
    create_task_group,
    connect,
    connect_readonly,
    get_agent_discord_id,
    get_workspace,
    get_workspace_host_profile,
    initialize,
    list_events,
    list_deliveries,
    list_jobs,
    list_runner_profiles,
    list_task_mirrors,
    list_workspace_host_profiles,
    list_workspaces,
    migrate,
    ReadOnlyConnectionError,
    row_to_dict,
    SchemaCompatibilityError,
    set_workspace_agent as _set_workspace_agent,
    upsert_workspace_host_profile,
    upsert_runner_profile,
    upsert_task_mirror,
    upsert_workspace,
)


def set_workspace_agent(conn, **kwargs):
    """Create an explicit fixture override without setup-event side effects."""
    result = _set_workspace_agent(
        conn, actor="test-fixture", reason="database test fixture", **kwargs
    )
    conn.execute("DELETE FROM events WHERE event_type = 'workspace.agent_override.set'")
    conn.commit()
    return result


class DatabaseTests(unittest.TestCase):
    def test_migration_creates_core_tables(self):
        conn = initialize(":memory:")

        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual(user_version, 16)
        self.assertTrue(
            {
                "workspaces",
                "events",
                "jobs",
                "deliveries",
                "agents",
                "runner_profiles",
                "tasks",
                "task_groups",
                "task_group_items",
                "decision_requests",
                "workspace_agent_registry_sources",
                "workspace_agent_registry_entries",
                "split_operations",
                "executor_catalog_sources",
                "executor_definitions",
                "executor_instance_bindings",
                "executor_capacity_sources",
                "executor_capacity_policies",
                "execution_attempt_leases",
            }.issubset(tables)
        )
        agent_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(agents)").fetchall()
        }
        self.assertTrue({"host_id", "client_type", "last_seen_at"}.issubset(agent_columns))

    def test_partial_unique_indexes_exclude_closed_tasks(self):
        conn = initialize(":memory:")
        # The unique branch index must be partial: it excludes phase='closed'
        # so historical reuse is allowed. The PR index remains globally unique
        # because PR URLs are immutable historical associations.
        indexes = {
            row["name"]: row["sql"]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'tasks'"
            ).fetchall()
        }
        self.assertIn("idx_tasks_workspace_branch", indexes)
        self.assertIn("idx_tasks_workspace_pr", indexes)
        self.assertIn("WHERE phase IS NOT 'closed'", indexes["idx_tasks_workspace_branch"])
        self.assertNotIn("WHERE phase IS NOT 'closed'", indexes["idx_tasks_workspace_pr"])

    def test_migration_from_v7_with_duplicate_closed_branch_succeeds(self):
        """Production DB had two closed tasks sharing a branch. v8 migration
        must succeed because uniqueness is only enforced for active tasks.
        """
        conn = connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE workspaces (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              path TEXT NOT NULL,
              harness_root TEXT NOT NULL,
              harnessctl_path TEXT,
              default_bus TEXT,
              default_destination TEXT,
              base_branch TEXT,
              branch_namespace TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE tasks (
              workspace_id TEXT NOT NULL,
              task_id TEXT NOT NULL,
              phase TEXT,
              owner TEXT,
              branch TEXT,
              pr TEXT,
              last_event_id TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (workspace_id, task_id)
            );
            """
        )
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))",
            ("ws-1", "Test", "/tmp/test", "/tmp/test/docs"),
        )
        for task_id in ("phase-3.3-runtime-launchd", "phase-4-coordinator-integration"):
            conn.execute(
                "INSERT INTO tasks (workspace_id, task_id, phase, owner, branch, pr, payload_json, updated_at) "
                "VALUES (?, ?, 'closed', 'worker', 'feature/multi-bot', NULL, '{}', datetime('now'))",
                ("ws-1", task_id),
            )
        conn.commit()

        migrate(conn)

        self.assertEqual(
            conn.execute("PRAGMA user_version").fetchone()[0], 16
        )
        # Active tasks must still be unique.
        conn.execute(
            "INSERT INTO tasks (workspace_id, task_id, phase, branch, pr, payload_json, updated_at) "
            "VALUES (?, ?, 'ready', 'feature/active-1', NULL, '{}', datetime('now'))",
            ("ws-1", "active-1"),
        )
        conn.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO tasks (workspace_id, task_id, phase, branch, pr, payload_json, updated_at) "
                "VALUES (?, ?, 'ready', 'feature/active-1', NULL, '{}', datetime('now'))",
                ("ws-1", "active-2"),
            )
            conn.commit()

    def test_migration_from_v8_global_indexes_recreated_as_v9_partial(self):
        """Round 5/6 schema v8 may have left global branch index or partial
        PR index. v9 migration must drop/recreate to partial branch + global PR.
        """
        conn = connect(":memory:")
        # Simulate a Round 5/6 schema v8 state: global branch index and
        # partial PR index (the wrong shapes).
        conn.executescript(
            """
            CREATE TABLE workspaces (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              path TEXT NOT NULL,
              harness_root TEXT NOT NULL,
              harnessctl_path TEXT,
              default_bus TEXT,
              default_destination TEXT,
              base_branch TEXT,
              branch_namespace TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE tasks (
              workspace_id TEXT NOT NULL,
              task_id TEXT NOT NULL,
              phase TEXT,
              owner TEXT,
              branch TEXT,
              pr TEXT,
              last_event_id TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (workspace_id, task_id)
            );
            CREATE UNIQUE INDEX idx_tasks_workspace_branch
              ON tasks(workspace_id, branch);
            CREATE UNIQUE INDEX idx_tasks_workspace_pr
              ON tasks(workspace_id, pr) WHERE phase IS NOT 'closed';
            """
        )
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))",
            ("ws-1", "Test", "/tmp/test", "/tmp/test/docs"),
        )
        migrate(conn)

        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        indexes = {
            row["name"]: row["sql"]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'tasks'"
            ).fetchall()
        }
        # Branch index is partial; PR index is global.
        self.assertIn("WHERE phase IS NOT 'closed'", indexes["idx_tasks_workspace_branch"])
        self.assertNotIn("WHERE phase IS NOT 'closed'", indexes["idx_tasks_workspace_pr"])
        # Active tasks still enforce branch uniqueness.
        conn.execute(
            "INSERT INTO tasks (workspace_id, task_id, phase, branch, pr, payload_json, updated_at) "
            "VALUES (?, ?, 'ready', 'feature/active-1', NULL, '{}', datetime('now'))",
            ("ws-1", "active-1"),
        )
        conn.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO tasks (workspace_id, task_id, phase, branch, pr, payload_json, updated_at) "
                "VALUES (?, ?, 'ready', 'feature/active-1', NULL, '{}', datetime('now'))",
                ("ws-1", "active-2"),
            )
            conn.commit()

    def test_migration_upgrades_v1_jobs_table_columns(self):
        conn = connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE jobs (
              id TEXT PRIMARY KEY,
              workspace_id TEXT,
              task_id TEXT,
              assigned_agent TEXT,
              status TEXT NOT NULL,
              prompt_path TEXT,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              timeout_seconds INTEGER,
              payload_json TEXT NOT NULL,
              result_json TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            """
        )

        migrate(conn)

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        self.assertTrue(
            {
                "runner_profile_id",
                "branch",
                "worktree_path",
                "terminal_session_id",
                "logs_path",
            }.issubset(columns)
        )

    def test_migration_creates_split_operations_table_v11(self):
        """v11 adds the split_operations ledger and its supporting indexes."""
        conn = initialize(":memory:")
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(split_operations)").fetchall()
        }
        self.assertTrue(
            {
                "operation_id",
                "contract_version",
                "operation_kind",
                "workspace_id",
                "target_kind",
                "target_id",
                "source_kind",
                "source_id",
                "input_fingerprint",
                "before_fingerprint",
                "after_fingerprint",
                "status",
                "record_event_id",
                "created_at",
                "updated_at",
            }.issubset(columns)
        )

        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'split_operations'"
            ).fetchall()
        }
        self.assertIn("idx_split_operations_workspace_target", indexes)
        self.assertIn("idx_split_operations_status", indexes)

    def test_migration_from_v10_to_v11_is_additive(self):
        """v11 migration must not fabricate rows or break v10 tables."""
        conn = connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE workspaces (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              path TEXT NOT NULL,
              harness_root TEXT NOT NULL,
              harnessctl_path TEXT,
              default_bus TEXT,
              default_destination TEXT,
              base_branch TEXT,
              branch_namespace TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE events (
              id TEXT PRIMARY KEY,
              workspace_id TEXT,
              event_type TEXT NOT NULL,
              actor TEXT NOT NULL,
              target TEXT,
              task_id TEXT,
              causation_id TEXT,
              idempotency_key TEXT NOT NULL UNIQUE,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE tasks (
              workspace_id TEXT NOT NULL,
              task_id TEXT NOT NULL,
              phase TEXT,
              owner TEXT,
              branch TEXT,
              pr TEXT,
              last_event_id TEXT,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (workspace_id, task_id)
            );
            """
        )
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))",
            ("ws-1", "Test", "/tmp/test", "/tmp/test/docs"),
        )
        conn.execute("PRAGMA user_version = 10")
        conn.commit()

        migrate(conn)

        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        rows = conn.execute("SELECT COUNT(*) FROM split_operations").fetchone()[0]
        self.assertEqual(rows, 0)
        # Existing workspace data survives.
        self.assertIsNotNone(get_workspace(conn, "ws-1"))

    def test_upsert_workspace_is_stable_registry_entry(self):
        conn = initialize(":memory:")

        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
            default_bus="kook",
            default_destination="room-1",
            base_branch="main",
            branch_namespace="agent",
        )
        updated = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo Project",
            path=".",
            harness_root=".",
            default_bus="discord",
            default_destination="channel-1",
            base_branch="main",
            branch_namespace="agent",
        )

        self.assertEqual(workspace.id, "demo")
        self.assertEqual(updated.name, "Demo Project")
        self.assertEqual(updated.default_bus, "discord")
        self.assertEqual(get_workspace(conn, "demo").default_destination, "channel-1")
        self.assertEqual(len(list_workspaces(conn)), 1)

    def test_append_event_is_idempotent_by_key(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

        first = append_event(
            conn,
            workspace_id="demo",
            event_type="assignment.requested",
            actor="operator",
            task_id="mvp-001",
            idempotency_key="demo:mvp-001:assign",
            payload={"owner": "codex"},
        )
        second = append_event(
            conn,
            workspace_id="demo",
            event_type="assignment.requested",
            actor="operator",
            task_id="mvp-001",
            idempotency_key="demo:mvp-001:assign",
            payload={"owner": "codex"},
        )

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.row["id"], second.row["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)
        self.assertEqual(row_to_dict(first.row)["payload"], {"owner": "codex"})

    def test_append_event_rejects_unknown_workspace(self):
        conn = initialize(":memory:")

        with self.assertRaises(sqlite3.IntegrityError):
            append_event(
                conn,
                workspace_id="missing",
                event_type="assignment.requested",
                actor="operator",
            )

    def test_runner_profile_registry(self):
        conn = initialize(":memory:")

        profile = upsert_runner_profile(
            conn,
            profile_id="codex",
            name="Codex CLI",
            runner_type="codex_cli",
            command="codex",
            working_directory_strategy="git_worktree",
            supports_stream_attach=True,
            env={"CODEX_HOME": "/tmp/codex"},
        )

        self.assertEqual(profile.runner_type, "codex_cli")
        self.assertTrue(profile.supports_stream_attach)
        self.assertEqual(profile.env["CODEX_HOME"], "/tmp/codex")
        self.assertEqual(len(list_runner_profiles(conn)), 1)

    def test_task_group_and_decision_request_records(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

        group = create_task_group(
            conn,
            workspace_id="demo",
            title="MVP Round",
            task_ids=["mvp-001", "mvp-002"],
            payload={"goal": "ship"},
        )
        decision = create_decision_request(
            conn,
            workspace_id="demo",
            request_type="review",
            requester="coordinator",
            reviewer="human",
            summary="Review mvp-001",
            task_id="mvp-001",
            context={"packet": "current/review-packet.md"},
        )

        self.assertEqual(group["title"], "MVP Round")
        self.assertEqual(row_to_dict(decision)["context"]["packet"], "current/review-packet.md")

    def test_upsert_task_mirror_tracks_changes(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

        _, first_action = upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="todo",
            owner=None,
            branch=None,
            pr=None,
            payload={"id": "mvp-001", "status": "todo"},
        )
        _, second_action = upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="todo",
            owner=None,
            branch=None,
            pr=None,
            payload={"id": "mvp-001", "status": "todo"},
        )
        _, third_action = upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="running",
            owner="codex",
            branch="agents/mvp-001",
            pr=None,
            payload={"id": "mvp-001", "status": "doing"},
        )

        mirrors = [row_to_dict(row) for row in list_task_mirrors(conn, "demo")]
        self.assertEqual(first_action, "created")
        self.assertEqual(second_action, "unchanged")
        self.assertEqual(third_action, "updated")
        self.assertEqual(mirrors[0]["branch"], "agents/mvp-001")

    def test_create_job_requires_known_workspace_and_runner(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
            )
            upsert_runner_profile(
                conn,
                profile_id="subprocess",
                name="Subprocess",
                runner_type="generic_subprocess",
                command="true",
            )

            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
                prompt_path="README.md",
                branch="agents/mvp-001",
                worktree_path="worktrees/mvp-001",
                logs_path="logs/mvp-001.log",
                payload={"purpose": "test"},
            )

            jobs = [row_to_dict(row) for row in list_jobs(conn, workspace_id="demo")]
            self.assertEqual(job["status"], "pending")
            self.assertEqual(jobs[0]["runner_profile_id"], "subprocess")
            root = Path(tmp).resolve()
            self.assertEqual(jobs[0]["prompt_path"], str(root / "README.md"))
            self.assertEqual(jobs[0]["worktree_path"], str(root / "worktrees" / "mvp-001"))
            self.assertEqual(jobs[0]["logs_path"], str(root / "logs" / "mvp-001.log"))
            self.assertEqual(jobs[0]["payload"], {"purpose": "test"})
            with self.assertRaisesRegex(ValueError, "unknown runner profile"):
                create_job(
                    conn,
                    workspace_id="demo",
                    task_id="mvp-001",
                    runner_profile_id="missing",
                )
            with self.assertRaisesRegex(ValueError, "unknown workspace"):
                create_job(
                    conn,
                    workspace_id="missing",
                    task_id="mvp-001",
                runner_profile_id="subprocess",
            )

    def test_create_delivery_is_idempotent_by_message_key(self):
        conn = initialize(":memory:")
        event = append_event(
            conn,
            event_type="assignment.requested",
            actor="operator",
        ).row

        first, first_created = create_delivery(
            conn,
            event_id=event["id"],
            platform="stdout",
            destination="local",
            message_key="demo:assign:1",
            payload={"text": "[ASSIGN] mvp-001"},
        )
        second, second_created = create_delivery(
            conn,
            event_id=event["id"],
            platform="stdout",
            destination="local",
            message_key="demo:assign:1",
            payload={"text": "[ASSIGN] mvp-001"},
        )

        deliveries = [row_to_dict(row) for row in list_deliveries(conn, status="pending")]
        self.assertTrue(first_created)
        self.assertFalse(second_created)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["payload"], {"text": "[ASSIGN] mvp-001"})

    def test_agents_json_migration(self):
        conn = initialize(":memory:")

        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(workspaces)").fetchall()
        }
        self.assertIn("agents_json", columns)

    def test_workspace_host_profiles_schema_and_roundtrip(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/opt/multinexus",
            harness_root="/opt/multinexus/docs/project-harness",
        )

        profile = upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="win-admin",
            workspace_path=r"C:\Users\ADMIN\projects\multinexus",
            harness_root=r"C:\Users\ADMIN\projects\multinexus\docs\project-harness",
            coordinator_cli_path=r"C:\Users\ADMIN\projects\multinexus\scripts\coord-ssh-win.py",
            shell="powershell",
            metadata={"os": "windows"},
        )

        self.assertEqual(profile.workspace_path, r"C:\Users\ADMIN\projects\multinexus")
        self.assertEqual(profile.metadata, {"os": "windows"})
        loaded = get_workspace_host_profile(conn, workspace_id="demo", host_id="win-admin")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.to_dict(), profile.to_dict())
        self.assertEqual(
            [p.host_id for p in list_workspace_host_profiles(conn, workspace_id="demo")],
            ["win-admin"],
        )

    def test_set_workspace_agent_and_get(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

        set_workspace_agent(
            conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="111111",
        )
        set_workspace_agent(
            conn,
            workspace_id="demo",
            agent_name="mac-claude",
            discord_user_id="222222",
        )

        self.assertEqual(get_agent_discord_id(conn, "demo", "mac-codex"), "111111")
        self.assertEqual(get_agent_discord_id(conn, "demo", "mac-claude"), "222222")

    def test_get_agent_discord_id_not_found(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )
        set_workspace_agent(
            conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="111111",
        )

        self.assertIsNone(get_agent_discord_id(conn, "demo", "unknown-agent"))

    def test_set_workspace_agent_preserves_existing(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

        set_workspace_agent(
            conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="111111",
        )
        set_workspace_agent(
            conn,
            workspace_id="demo",
            agent_name="mac-claude",
            discord_user_id="222222",
        )

        # agent A still exists after setting agent B
        self.assertEqual(get_agent_discord_id(conn, "demo", "mac-codex"), "111111")
        self.assertEqual(get_agent_discord_id(conn, "demo", "mac-claude"), "222222")

class SchemaV9SafetyTests(unittest.TestCase):
    def test_reopening_v9_does_not_drop_or_recreate_task_indexes(self):
        conn = initialize(":memory:")
        statements = []
        conn.set_trace_callback(statements.append)

        migrate(conn)

        task_index_ddl = [
            " ".join(statement.split()).upper()
            for statement in statements
            if "IDX_TASKS_WORKSPACE_BRANCH" in statement.upper()
            or "IDX_TASKS_WORKSPACE_PR" in statement.upper()
        ]
        self.assertFalse(any(sql.startswith("DROP INDEX") for sql in task_index_ddl))
        self.assertFalse(any(sql.startswith("CREATE UNIQUE INDEX") for sql in task_index_ddl))

    def test_failed_v8_to_v9_index_rebuild_restores_previous_indexes(self):
        conn = initialize(":memory:")
        conn.executescript(
            """
            DROP INDEX idx_tasks_workspace_branch;
            DROP INDEX idx_tasks_workspace_pr;
            CREATE UNIQUE INDEX idx_tasks_workspace_branch
              ON tasks(workspace_id, branch) WHERE phase IS NOT 'closed';
            CREATE UNIQUE INDEX idx_tasks_workspace_pr
              ON tasks(workspace_id, pr) WHERE phase IS NOT 'closed';
            PRAGMA user_version = 8;
            """
        )
        upsert_workspace(
            conn,
            workspace_id="ws",
            name="Workspace",
            path=".",
            harness_root=".",
        )
        for task_id in ("closed-1", "closed-2"):
            conn.execute(
                "INSERT INTO tasks (workspace_id, task_id, phase, branch, pr, "
                "payload_json, updated_at) VALUES (?, ?, 'closed', ?, ?, '{}', "
                "datetime('now'))",
                ("ws", task_id, f"branch/{task_id}", "https://github.com/acme/repo/pull/1"),
            )
        conn.commit()

        with self.assertRaises(sqlite3.IntegrityError):
            migrate(conn)

        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 8)
        indexes = {
            row["name"]: row["sql"]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' "
                "AND name IN ('idx_tasks_workspace_branch', 'idx_tasks_workspace_pr')"
            )
        }
        self.assertEqual(set(indexes), {
            "idx_tasks_workspace_branch",
            "idx_tasks_workspace_pr",
        })
        self.assertIn("WHERE phase IS NOT 'closed'", indexes["idx_tasks_workspace_pr"])

    def test_v8_to_v9_rebuild_blocks_concurrent_duplicate_writer(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            setup = initialize(db_path)
            upsert_workspace(
                setup,
                workspace_id="ws",
                name="Workspace",
                path=".",
                harness_root=".",
            )
            setup.execute(
                "INSERT INTO tasks (workspace_id, task_id, phase, branch, "
                "payload_json, updated_at) VALUES "
                "('ws', 'existing', 'doing', 'agents/shared', '{}', datetime('now'))"
            )
            setup.execute("PRAGMA user_version = 8")
            setup.commit()
            setup.close()

            drop_started = threading.Event()
            writer_started = threading.Event()
            outcomes = {}

            def migrate_worker():
                conn = connect(db_path)

                def trace(statement):
                    if statement.upper().startswith(
                        "DROP INDEX IF EXISTS IDX_TASKS_WORKSPACE_BRANCH"
                    ):
                        drop_started.set()
                        writer_started.wait(timeout=2)

                conn.set_trace_callback(trace)
                try:
                    migrate(conn)
                    outcomes["migration"] = "ok"
                except Exception as exc:  # pragma: no cover - assertion reports it
                    outcomes["migration"] = exc
                finally:
                    conn.close()

            def writer_worker():
                if not drop_started.wait(timeout=2):
                    outcomes["writer"] = "migration did not reach index rebuild"
                    return
                conn = connect(db_path)
                writer_started.set()
                try:
                    conn.execute(
                        "INSERT INTO tasks (workspace_id, task_id, phase, branch, "
                        "payload_json, updated_at) VALUES "
                        "('ws', 'concurrent', 'doing', 'agents/shared', '{}', "
                        "datetime('now'))"
                    )
                    conn.commit()
                    outcomes["writer"] = "unexpected success"
                except sqlite3.IntegrityError:
                    outcomes["writer"] = "unique constraint enforced"
                except Exception as exc:  # pragma: no cover - assertion reports it
                    outcomes["writer"] = exc
                finally:
                    conn.close()

            migration_thread = threading.Thread(target=migrate_worker)
            writer_thread = threading.Thread(target=writer_worker)
            migration_thread.start()
            writer_thread.start()
            migration_thread.join(timeout=5)
            writer_thread.join(timeout=5)

            self.assertFalse(migration_thread.is_alive())
            self.assertFalse(writer_thread.is_alive())
            self.assertEqual(outcomes.get("migration"), "ok")
            self.assertEqual(outcomes.get("writer"), "unique constraint enforced")

            check = connect(db_path)
            rows = check.execute(
                "SELECT task_id FROM tasks WHERE workspace_id='ws' "
                "AND branch='agents/shared'"
            ).fetchall()
            self.assertEqual([row["task_id"] for row in rows], ["existing"])
            check.close()


class SchemaV13Tests(unittest.TestCase):
    def _v12_schema_script(self) -> str:
        return """
        CREATE TABLE agents (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT,
          capabilities_json TEXT NOT NULL, online_state TEXT NOT NULL,
          current_load INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE runner_profiles (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, runner_type TEXT NOT NULL,
          command TEXT NOT NULL, working_directory_strategy TEXT NOT NULL,
          supports_stream_attach INTEGER NOT NULL DEFAULT 0, env_json TEXT NOT NULL,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE executor_catalog_sources (
          source_id TEXT PRIMARY KEY, source_version INTEGER NOT NULL,
          catalog_hash TEXT NOT NULL, source_path TEXT, updated_at TEXT NOT NULL
        );
        CREATE TABLE executor_definitions (
          id TEXT PRIMARY KEY, source_id TEXT NOT NULL, provider TEXT NOT NULL,
          adapter TEXT NOT NULL, capabilities_json TEXT NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE executor_instance_bindings (
          agent_id TEXT PRIMARY KEY, source_id TEXT NOT NULL,
          executor_definition_id TEXT NOT NULL, runner_profile_id TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        PRAGMA user_version = 12;
        """

    def test_v13_lease_table_constraints_present(self):
        conn = initialize(":memory:")
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'execution_attempt_leases'"
        ).fetchone()["sql"]
        self.assertIn("capacity_policy_id TEXT NOT NULL", sql)
        self.assertIn("CHECK(length(host_id) BETWEEN 1 AND 64)", sql)
        self.assertIn("CHECK(length(normalized_path) BETWEEN 1 AND 4096)", sql)
        self.assertIn("CHECK(resource_key GLOB 'sha256:*'", sql)
        self.assertIn("CHECK(capacity_policy_id GLOB 'sha256:*'", sql)
        self.assertIn("CHECK (status != 'released' OR", sql)
        self.assertIn("CHECK (status = 'released' OR", sql)
        self.assertIn("CHECK (release_reason IS NULL OR length(release_reason) BETWEEN 1 AND 256)", sql)
        self.assertIn("CHECK (renewed_at >= acquired_at AND expires_at >= renewed_at)", sql)
        self.assertIn("CHECK (released_at IS NULL OR released_at >= acquired_at)", sql)
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'execution_attempt_leases'"
            ).fetchall()
        }
        self.assertIn("idx_execution_attempt_leases_active_resource", indexes)
        self.assertIn("idx_execution_attempt_leases_job", indexes)

    def test_v13_repeated_migration_is_idempotent(self):
        conn = connect(":memory:")
        conn.executescript(self._v12_schema_script())
        conn.commit()
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)

    def test_failed_v13_migration_rolls_back_to_v12(self):
        conn = connect(":memory:")
        conn.executescript(self._v12_schema_script())
        conn.commit()
        # Pre-create a malformed table with the same name so the index creation in the
        # v13 script fails, forcing the entire migration to roll back.
        conn.execute("CREATE TABLE execution_attempt_leases (lease_id TEXT PRIMARY KEY)")
        conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 12)
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            .fetchall()
        }
        self.assertNotIn("executor_capacity_sources", tables)
        self.assertNotIn("executor_capacity_policies", tables)

    def test_v13_lease_state_constraints_fail(self):
        conn = initialize(":memory:")
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("ws", "ws", "/tmp", "/tmp/docs", now, now),
        )
        conn.execute(
            "INSERT INTO agents (id, name, role, capabilities_json, online_state, current_load, created_at, updated_at) "
            "VALUES (?, ?, 'agent', '[]', 'offline', 0, ?, ?)",
            ("mac-omp", "mac-omp", now, now),
        )
        conn.execute(
            "INSERT INTO runner_profiles (id, name, runner_type, command, working_directory_strategy, supports_stream_attach, env_json, created_at, updated_at) "
            "VALUES (?, ?, 'agentd', 'agent', 'current_dir', 0, '{}', ?, ?)",
            ("mac-omp", "mac-omp", now, now),
        )
        conn.execute(
            "INSERT INTO jobs (id, workspace_id, status, attempt_count, payload_json, created_at, updated_at, runner_profile_id, assigned_agent) "
            "VALUES (?, ?, 'pending', 1, '{}', ?, ?, ?, ?)",
            ("job1", "ws", now, now, "mac-omp", "mac-omp"),
        )
        conn.execute(
            "INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, updated_at) VALUES (?, ?, ?, ?)",
            ("src", 1, "a" * 64, now),
        )
        conn.execute(
            "INSERT INTO executor_capacity_policies (agent_id, source_id, source_version, catalog_hash, capacity_policy_id, max_concurrent_jobs, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("mac-omp", "src", 1, "a" * 64, "sha256:" + "a" * 64, 1, now, now),
        )
        conn.commit()

        base_values = (
            "lease1", "job1", 1, "mac-omp", "mac-omp", "host1", "worktree",
            "sha256:" + "a" * 64, "/tmp/ws", "sha256:" + "a" * 64, 1,
            "active", now, now, "2026-01-01T00:01:00Z", None, None,
        )

        # Active lease with released_at violates state shape.
        with self.assertRaises(sqlite3.IntegrityError):
            values = list(base_values)
            values[15] = now  # released_at
            conn.execute(
                "INSERT INTO execution_attempt_leases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(values),
            )
            conn.commit()
        conn.rollback()

        # Released lease without release_reason violates state shape.
        with self.assertRaises(sqlite3.IntegrityError):
            values = list(base_values)
            values[11] = "released"
            values[15] = now
            conn.execute(
                "INSERT INTO execution_attempt_leases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(values),
            )
            conn.commit()
        conn.rollback()

        # capacity_policy_id NULL violates NOT NULL.
        with self.assertRaises(sqlite3.IntegrityError):
            values = list(base_values)
            values[9] = None
            conn.execute(
                "INSERT INTO execution_attempt_leases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(values),
            )
            conn.commit()
        conn.rollback()

        # Timestamp order violation.
        with self.assertRaises(sqlite3.IntegrityError):
            values = list(base_values)
            values[13] = "2026-01-01T00:02:00Z"  # renewed_at after expires_at
            conn.execute(
                "INSERT INTO execution_attempt_leases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(values),
            )
            conn.commit()
        conn.rollback()


class SchemaV14Tests(unittest.TestCase):
    def _v13_fresh_conn(self) -> sqlite3.Connection:
        """A connection migrated to v13, then rewound to simulate a pre-v14 file DB."""
        conn = initialize(":memory:")
        conn.execute("DROP TABLE channel_bindings")
        conn.execute("PRAGMA user_version = 13")
        conn.commit()
        return conn

    def test_v14_channel_bindings_table_present(self):
        conn = initialize(":memory:")
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'channel_bindings'"
        ).fetchone()["sql"]
        self.assertIn("platform TEXT NOT NULL", sql)
        self.assertIn("channel_id TEXT NOT NULL", sql)
        self.assertIn("workspace_id TEXT NOT NULL", sql)
        self.assertIn("REFERENCES workspaces(id) ON DELETE RESTRICT", sql)
        self.assertIn("PRIMARY KEY (platform, channel_id)", sql)

    def test_v13_to_v14_migration_adds_table(self):
        conn = self._v13_fresh_conn()
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 13)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        self.assertIn("channel_bindings", tables)

    def test_v14_repeated_migration_is_idempotent(self):
        conn = self._v13_fresh_conn()
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)

    def test_composite_pk_blocks_second_workspace_for_channel(self):
        conn = initialize(":memory:")
        now = "2026-01-01T00:00:00Z"
        for ws in ("ws-a", "ws-b"):
            conn.execute(
                "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (ws, ws, "/tmp", "/tmp/docs", now, now),
            )
        conn.execute(
            "INSERT INTO channel_bindings (platform, channel_id, workspace_id, bound_at) VALUES (?, ?, ?, ?)",
            ("discord", "123", "ws-a", now),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO channel_bindings (platform, channel_id, workspace_id, bound_at) VALUES (?, ?, ?, ?)",
                ("discord", "123", "ws-b", now),
            )

    def test_workspace_delete_restricted_while_bound(self):
        conn = initialize(":memory:")
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("ws-a", "ws-a", "/tmp", "/tmp/docs", now, now),
        )
        conn.execute(
            "INSERT INTO channel_bindings (platform, channel_id, workspace_id, bound_at) VALUES (?, ?, ?, ?)",
            ("discord", "123", "ws-a", now),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM workspaces WHERE id = ?", ("ws-a",))




class SchemaV15Tests(unittest.TestCase):
    """Issue #18: v15 adds workspace_host_profiles.worktree_roots_json."""

    _V14_PROFILES_DDL = """
        CREATE TABLE workspace_host_profiles (
          workspace_id TEXT NOT NULL,
          host_id TEXT NOT NULL,
          workspace_path TEXT NOT NULL,
          harness_root TEXT,
          harnessctl_path TEXT,
          coordinator_cli_path TEXT,
          coordinator_db_path TEXT,
          shell TEXT,
          metadata_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (workspace_id, host_id),
          FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
        );
    """

    def _v14_fresh_conn(self) -> sqlite3.Connection:
        """A connection migrated to v15, then rebuilt as a pre-v15 file DB."""
        conn = initialize(":memory:")
        conn.execute("DROP TABLE workspace_host_profiles")
        conn.executescript(self._V14_PROFILES_DDL)
        conn.execute("PRAGMA user_version = 14")
        conn.commit()
        return conn

    def test_v15_fresh_db_has_column_and_version(self):
        conn = initialize(":memory:")
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(workspace_host_profiles)"
            ).fetchall()
        }
        self.assertIn("worktree_roots_json", columns)

    def test_v14_to_v15_migration_backfills_empty_allowlist(self):
        conn = self._v14_fresh_conn()
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "Demo", "/ws", "/ws/docs", now, now),
        )
        conn.execute(
            "INSERT INTO workspace_host_profiles "
            "(workspace_id, host_id, workspace_path, metadata_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "mac", "/ws", "{}", now, now),
        )
        conn.commit()
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        row = conn.execute(
            "SELECT worktree_roots_json FROM workspace_host_profiles WHERE host_id = 'mac'"
        ).fetchone()
        self.assertEqual(row["worktree_roots_json"], "[]")
        profile = get_workspace_host_profile(conn, workspace_id="demo", host_id="mac")
        self.assertEqual(profile.worktree_roots, ())

    def test_v15_repeated_migration_is_idempotent(self):
        conn = self._v14_fresh_conn()
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)

    def test_failed_v15_migration_preserves_version_and_data(self):
        conn = self._v14_fresh_conn()
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "Demo", "/ws", "/ws/docs", now, now),
        )
        conn.execute(
            "INSERT INTO workspace_host_profiles "
            "(workspace_id, host_id, workspace_path, metadata_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "mac", "/ws", "{}", now, now),
        )
        conn.commit()
        with patch("coordinate.schema._add_column_if_missing", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        rows = conn.execute("SELECT COUNT(*) FROM workspace_host_profiles").fetchone()[0]
        self.assertEqual(rows, 1)
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(workspace_host_profiles)"
            ).fetchall()
        }
        self.assertNotIn("worktree_roots_json", columns)


    def test_failed_v15_migration_after_column_add_rolls_back(self):
        """P1-1: a failure AFTER the real ADD COLUMN but BEFORE the version
        bump must roll back both, leaving the file DB at v14 with the original
        row intact and no column."""
        import coordinate.schema as schema_module

        conn = self._v14_fresh_conn()
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "Demo", "/ws", "/ws/docs", now, now),
        )
        conn.execute(
            "INSERT INTO workspace_host_profiles "
            "(workspace_id, host_id, workspace_path, metadata_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "mac", "/ws", '{"note":"keep"}', now, now),
        )
        conn.commit()

        original_add_column = schema_module._add_column_if_missing

        def _boom_after_alter(conn_, table, column, definition):
            original_add_column(conn_, table, column, definition)
            raise RuntimeError("boom after alter")

        with patch(
            "coordinate.schema._add_column_if_missing", side_effect=_boom_after_alter
        ):
            with self.assertRaisesRegex(RuntimeError, "boom after alter"):
                migrate(conn)

        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(workspace_host_profiles)"
            ).fetchall()
        }
        self.assertNotIn("worktree_roots_json", columns)
        row = conn.execute(
            "SELECT workspace_path, metadata_json FROM workspace_host_profiles "
            "WHERE host_id = 'mac'"
        ).fetchone()
        self.assertEqual(row["workspace_path"], "/ws")
        self.assertEqual(row["metadata_json"], '{"note":"keep"}')
        # The migration is still rerunnable from the same v14 state.
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(workspace_host_profiles)"
            ).fetchall()
        }
        self.assertIn("worktree_roots_json", columns)
        row = conn.execute(
            "SELECT workspace_path, metadata_json FROM workspace_host_profiles "
            "WHERE host_id = 'mac'"
        ).fetchone()
        self.assertEqual(row["workspace_path"], "/ws")
        self.assertEqual(row["metadata_json"], '{"note":"keep"}')

    def test_preserve_revalidates_roots_against_new_workspace_path(self):
        """P1-2: None=preserve still runs the stored roots through the same
        validator against the NEW workspace_path; a flavour-changing update
        fails BEFORE any DB mutation."""
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/ws",
            harness_root="/ws/docs",
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path="/ws",
            harness_root="/ws/docs",
            worktree_roots=["/ws/worktrees/a"],
        )
        before = tuple(
            conn.execute(
                "SELECT * FROM workspace_host_profiles WHERE host_id = 'mac'"
            ).fetchone()
        )
        with self.assertRaisesRegex(ValueError, "flavour"):
            upsert_workspace_host_profile(
                conn,
                workspace_id="demo",
                host_id="mac",
                workspace_path="C:\\Users\\Admin\\projects\\multinexus",
                harness_root="C:\\Users\\Admin\\projects\\multinexus\\harness",
            )
        after = tuple(
            conn.execute(
                "SELECT * FROM workspace_host_profiles WHERE host_id = 'mac'"
            ).fetchone()
        )
        self.assertEqual(after, before)

    def test_preserve_revalidates_same_flavour_roots_ok(self):
        """P1-2 positive: preserve keeps working when the new workspace_path
        keeps the same path flavour."""
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/ws",
            harness_root="/ws/docs",
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path="/ws",
            harness_root="/ws/docs",
            worktree_roots=["/ws/worktrees/a"],
        )
        updated = upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path="/ws/other",
            harness_root="/ws/docs",
        )
        self.assertEqual(updated.worktree_roots, ("/ws/worktrees/a",))
        self.assertEqual(updated.workspace_path, "/ws/other")

    def test_stored_cross_flavour_roots_fail_closed_on_read(self):
        """Corrupted stored state: roots whose flavour no longer matches the
        row's workspace_path must fail closed on read."""
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/ws",
            harness_root="/ws/docs",
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path="/ws",
            harness_root="/ws/docs",
            worktree_roots=["/ws/worktrees/a"],
        )
        conn.execute(
            "UPDATE workspace_host_profiles SET workspace_path = "
            "'C:\\Users\\Admin\\projects\\multinexus' WHERE host_id = 'mac'"
        )
        conn.commit()
        with self.assertRaisesRegex(ValueError, "flavour"):
            get_workspace_host_profile(conn, workspace_id="demo", host_id="mac")

    def test_stored_cross_flavour_roots_windows_side_fails_closed_on_read(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/ws",
            harness_root="/ws/docs",
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path="C:\\Users\\Admin\\projects\\multinexus",
            harness_root="C:\\Users\\Admin\\projects\\multinexus\\harness",
            worktree_roots=["C:\\Users\\Admin\\projects\\WorkTrees"],
        )
        conn.execute(
            "UPDATE workspace_host_profiles SET workspace_path = '/ws' "
            "WHERE host_id = 'mac'"
        )
        conn.commit()
        with self.assertRaisesRegex(ValueError, "flavour"):
            get_workspace_host_profile(conn, workspace_id="demo", host_id="mac")
    def test_profile_roundtrip_preserves_roots(self):
        conn = initialize(":memory:")
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/ws",
            harness_root="/ws/docs",
        )
        profile = upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path="/ws",
            harness_root="/ws/docs",
            worktree_roots=["/ws/worktrees/a", "/ws/worktrees/b"],
        )
        loaded = get_workspace_host_profile(conn, workspace_id="demo", host_id="mac")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.worktree_roots, ("/ws/worktrees/a", "/ws/worktrees/b"))
        self.assertEqual(loaded.to_dict(), profile.to_dict())
        self.assertEqual(
            loaded.to_dict()["worktree_roots"],
            ["/ws/worktrees/a", "/ws/worktrees/b"],
        )


class SchemaV16Tests(unittest.TestCase):
    """Issue #12: v16 adds job_attempt_usage + task_usage_warning_policies."""

    def _v15_fresh_conn(self) -> sqlite3.Connection:
        """A v16-migrated connection rebuilt as a pre-v16 file DB."""
        conn = initialize(":memory:")
        for table in (
            "task_usage_warning_policies",
            "job_attempt_usage",
        ):
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute("PRAGMA user_version = 15")
        conn.commit()
        return conn

    def test_v16_fresh_db_has_tables_and_version(self):
        conn = initialize(":memory:")
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        self.assertIn("job_attempt_usage", tables)
        self.assertIn("task_usage_warning_policies", tables)

    def test_job_attempt_usage_columns_and_keys(self):
        conn = initialize(":memory:")
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(job_attempt_usage)").fetchall()
        }
        self.assertIn("job_id", columns)
        self.assertIn("attempt_token", columns)
        self.assertIn("workspace_id", columns)
        self.assertIn("task_id", columns)
        self.assertIn("evidence_json", columns)
        self.assertIn("evidence_digest", columns)
        self.assertIn("observed_tokens", columns)
        self.assertIn("provider_cost_microusd", columns)
        self.assertIn("completeness", columns)
        self.assertIn("terminal_event_id", columns)
        self.assertIn("event_created", columns)
        self.assertIn("recorded_at", columns)
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'job_attempt_usage'"
        ).fetchone()[0]
        self.assertIn("PRIMARY KEY (job_id, attempt_token)", sql)
        self.assertIn("FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE RESTRICT", sql)
        self.assertIn("FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE RESTRICT", sql)
        self.assertIn("FOREIGN KEY(terminal_event_id) REFERENCES events(id) ON DELETE SET NULL", sql)

    def test_warning_policy_columns_and_keys(self):
        conn = initialize(":memory:")
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(task_usage_warning_policies)"
            ).fetchall()
        }
        self.assertIn("workspace_id", columns)
        self.assertIn("task_id", columns)
        self.assertIn("revision", columns)
        self.assertIn("observed_tokens_threshold", columns)
        self.assertIn("enabled", columns)
        self.assertIn("created_at", columns)
        self.assertIn("updated_at", columns)
        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'task_usage_warning_policies'"
        ).fetchone()[0]
        self.assertIn("PRIMARY KEY (workspace_id, task_id)", sql)
        self.assertIn("FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE RESTRICT", sql)

    def test_v15_to_v16_migration_creates_tables(self):
        conn = self._v15_fresh_conn()
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 15)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        self.assertIn("job_attempt_usage", tables)
        self.assertIn("task_usage_warning_policies", tables)

    def test_v16_repeated_migration_is_idempotent(self):
        conn = self._v15_fresh_conn()
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 16)

    def test_failed_v16_migration_rolls_back_to_v15(self):
        """A failure mid-migration must roll back both tables AND the version
        bump, leaving the v15 file DB byte-semantically intact and rerunnable.
        The injected failure lands AFTER both CREATE TABLEs and the version
        bump but BEFORE COMMIT — proving atomicity of the whole block."""
        import tempfile

        import coordinate.schema as schema_module

        class _BoomAfterVersionBump(sqlite3.Connection):
            def executescript(self, script):
                if "job_attempt_usage" in script:
                    script = script.replace(
                        "PRAGMA user_version = 16;\n            COMMIT;",
                        "PRAGMA user_version = 16;\n            SELECT broken_zzz;",
                    )
                return super().executescript(script)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}/v15.db"
            conn = initialize(db_path)
            conn.close()
            # Downgrade the migrated file to v15 semantics: drop the new tables.
            downgrade = sqlite3.connect(db_path)
            downgrade.execute("DROP TABLE task_usage_warning_policies")
            downgrade.execute("DROP TABLE job_attempt_usage")
            downgrade.execute("PRAGMA user_version = 15")
            now = "2026-01-01T00:00:00Z"
            downgrade.execute(
                "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("demo", "Demo", "/ws", "/ws/docs", now, now),
            )
            downgrade.commit()
            downgrade.close()

            conn = sqlite3.connect(db_path, factory=_BoomAfterVersionBump)
            conn.row_factory = sqlite3.Row
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 15)
            with self.assertRaisesRegex(sqlite3.OperationalError, "broken_zzz"):
                schema_module.migrate(conn)
            if conn.in_transaction:
                conn.rollback()
            conn.close()

            verify = sqlite3.connect(db_path)
            verify.row_factory = sqlite3.Row
            self.assertEqual(verify.execute("PRAGMA user_version").fetchone()[0], 15)
            tables = {
                row["name"]
                for row in verify.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            self.assertNotIn("job_attempt_usage", tables)
            self.assertNotIn("task_usage_warning_policies", tables)
            rows = verify.execute("SELECT COUNT(*) FROM workspaces").fetchone()[0]
            self.assertEqual(rows, 1)
            verify.close()
            # Rerunnable from the same v15 state.
            migrate(initialize(db_path))
            final = sqlite3.connect(db_path)
            final.row_factory = sqlite3.Row
            self.assertEqual(final.execute("PRAGMA user_version").fetchone()[0], 16)
            tables = {
                row["name"]
                for row in final.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            self.assertIn("job_attempt_usage", tables)
            self.assertIn("task_usage_warning_policies", tables)
            final.close()

    def test_usage_rows_restrict_job_delete(self):
        """RESTRICT retention: a job with an attempt usage row cannot be deleted."""
        conn = initialize(":memory:")
        from coordinate.db import create_job, get_job

        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "Demo", "/ws", "/ws/docs", now, now),
        )
        conn.execute(
            "INSERT INTO runner_profiles (id, name, runner_type, command, working_directory_strategy, env_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("rp", "rp", "agentd", "", "current_dir", "{}", now, now),
        )
        job = create_job(
            conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="rp",
            payload={},
        )
        conn.execute(
            """
            INSERT INTO job_attempt_usage (
              job_id, attempt_token, workspace_id, task_id, evidence_json,
              evidence_digest, observed_tokens, provider_cost_microusd, completeness,
              terminal_event_id, event_created, recorded_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1, ?)
            """,
            (job["id"], 1, "demo", None, "{}", "digest", 1, 1, "complete", now),
        )
        conn.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM jobs WHERE id = ?", (job["id"],))

    def test_policy_rows_restrict_workspace_delete(self):
        """RESTRICT retention: a workspace with a policy row cannot be deleted."""
        conn = initialize(":memory:")
        now = "2026-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("demo", "Demo", "/ws", "/ws/docs", now, now),
        )
        conn.execute(
            """
            INSERT INTO task_usage_warning_policies (
              workspace_id, task_id, revision, observed_tokens_threshold, enabled,
              created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("demo", "task-1", 1, 100, 1, now, now),
        )
        conn.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM workspaces WHERE id = ?", ("demo",))


class ReadOnlyConnectionTests(unittest.TestCase):
    """Strict read-only registry query path: mode=ro + query_only=ON + exact
    schema gate. Zero mutation is proven by DB bytes (primary), mtime
    (auxiliary, same-platform stable), and sidecar absence (discrete fact)."""

    def _snapshot(self, db_path: str) -> dict[str, object]:
        p = Path(db_path)
        return {
            "bytes": p.read_bytes() if p.exists() else None,
            "mtime_ns": p.stat().st_mtime_ns if p.exists() else None,
            "journal": Path(f"{db_path}-journal").exists(),
            "wal": Path(f"{db_path}-wal").exists(),
            "shm": Path(f"{db_path}-shm").exists(),
        }

    def _assert_zero_mutation(self, before: dict[str, object], after: dict[str, object]) -> None:
        self.assertEqual(after["bytes"], before["bytes"], "DB bytes changed")
        self.assertEqual(after["mtime_ns"], before["mtime_ns"], "DB mtime changed")
        self.assertFalse(after["journal"] or after["wal"] or after["shm"], "sidecar created")

    def _assert_sidecars_absent(self, db_path: str) -> None:
        for suffix in ("-journal", "-wal", "-shm"):
            self.assertFalse(Path(f"{db_path}{suffix}").exists(), f"unexpected sidecar {suffix}")

    # T1: missing DB fails closed with zero file creation.
    def test_connect_readonly_missing_db_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing_parent = str(Path(tmp) / "missing" / "coordinator.sqlite3")
            with self.assertRaises(ReadOnlyConnectionError):
                connect_readonly(missing_parent)
            self.assertFalse(Path(missing_parent).exists())
            self.assertFalse(Path(tmp).joinpath("missing").exists())
            self._assert_sidecars_absent(missing_parent)

            absent_file = str(Path(tmp) / "absent.sqlite3")
            with self.assertRaises(ReadOnlyConnectionError):
                connect_readonly(absent_file)
            self.assertFalse(Path(absent_file).exists())
            self._assert_sidecars_absent(absent_file)

    def test_connect_readonly_rejects_memory(self) -> None:
        with self.assertRaises(ReadOnlyConnectionError):
            connect_readonly(":memory:")

    # T2: current-schema queries succeed, file untouched.
    def test_connect_readonly_current_schema_queries_ok(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            upsert_workspace(
                conn, workspace_id="demo", name="Demo", path="/ws", harness_root="/ws/docs"
            )
            upsert_workspace_host_profile(
                conn, workspace_id="demo", host_id="mac", workspace_path="/ws"
            )
            conn.close()

            ro = connect_readonly(db_path)
            try:
                assert_schema_compatible(ro)
                self.assertEqual(ro.execute("PRAGMA query_only").fetchone()[0], 1)
                self.assertEqual([w.id for w in list_workspaces(ro)], ["demo"])
                self.assertEqual(
                    [p.host_id for p in list_workspace_host_profiles(ro, workspace_id="demo")],
                    ["mac"],
                )
            finally:
                ro.close()

    def test_readonly_query_leaves_file_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            upsert_workspace(
                conn, workspace_id="demo", name="Demo", path="/ws", harness_root="/ws/docs"
            )
            conn.close()
            before = self._snapshot(db_path)
            ro = connect_readonly(db_path)
            assert_schema_compatible(ro)
            list_workspaces(ro)
            ro.close()
            self._assert_zero_mutation(before, self._snapshot(db_path))

    # T3: legacy/unknown/spoofed schemas fail closed without migration.
    def _downgrade_to_v15(self, db_path: str) -> None:
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE task_usage_warning_policies")
        conn.execute("DROP TABLE job_attempt_usage")
        conn.execute("PRAGMA user_version = 15")
        conn.commit()
        conn.close()

    def test_schema_gate_rejects_v15_without_migration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            conn.close()
            self._downgrade_to_v15(db_path)
            before = self._snapshot(db_path)
            ro = connect_readonly(db_path)
            with self.assertRaisesRegex(SchemaCompatibilityError, "schema version 15"):
                assert_schema_compatible(ro)
            self._assert_zero_mutation(before, self._snapshot(db_path))
            # No migration ran: v16-only tables are still absent.
            tables = {
                row["name"]
                for row in ro.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            self.assertNotIn("job_attempt_usage", tables)
            ro.close()

    def test_schema_gate_rejects_unknown_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            conn.execute("PRAGMA user_version = 17")
            conn.commit()
            conn.close()
            before = self._snapshot(db_path)
            ro = connect_readonly(db_path)
            with self.assertRaisesRegex(SchemaCompatibilityError, "schema version 17"):
                assert_schema_compatible(ro)
            self._assert_zero_mutation(before, self._snapshot(db_path))
            ro.close()

    def test_schema_gate_rejects_spoofed_version_missing_registry_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            conn.execute("DROP TABLE workspaces")
            conn.commit()
            conn.close()
            ro = connect_readonly(db_path)
            with self.assertRaisesRegex(SchemaCompatibilityError, "workspaces"):
                assert_schema_compatible(ro)
            ro.close()

    # T4: writes on the read-only connection fail and leave the file untouched.
    def test_readonly_connection_rejects_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            conn.close()
            before = self._snapshot(db_path)
            ro = connect_readonly(db_path)
            try:
                assert_schema_compatible(ro)
                self.assertEqual(ro.execute("PRAGMA query_only").fetchone()[0], 1)
                with self.assertRaises(sqlite3.OperationalError):
                    ro.execute(
                        "INSERT INTO workspaces "
                        "(id, name, path, harness_root, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        ("demo", "Demo", "/ws", "/ws/docs", "2026-01-01T00:00:00Z",
                         "2026-01-01T00:00:00Z"),
                    )
                self.assertEqual(ro.execute("PRAGMA query_only").fetchone()[0], 1)
            finally:
                ro.close()
            self._assert_zero_mutation(before, self._snapshot(db_path))

    # F1: a failure after the initial open (any post-open configuration step)
    # must close the connection, never register it in _OPEN_CONNECTIONS, and
    # surface a stable ReadOnlyConnectionError (sqlite3.Error) or the original
    # exception (other BaseException) -- never a leaked sqlite3.Error.
    def test_post_open_sqlite_failure_closes_and_fails_closed(self) -> None:
        import coordinate.db as db_module

        closed: list[bool] = []

        class _Broken(sqlite3.Connection):
            def close(self) -> None:
                closed.append(True)
                super().close()

            def execute(self, sql, *args):
                raise sqlite3.OperationalError("simulated post-open failure")

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            conn.close()
            before = list(db_module._OPEN_CONNECTIONS)
            with patch("coordinate.db.sqlite3.connect", return_value=_Broken(":memory:")):
                with self.assertRaisesRegex(ReadOnlyConnectionError, "failed to configure"):
                    connect_readonly(db_path)
            self.assertEqual(closed, [True], "connection must be closed on failure")
            self.assertEqual(db_module._OPEN_CONNECTIONS, before, "must not register a failed connection")

    def test_post_open_verification_failure_closes_and_fails_closed(self) -> None:
        import coordinate.db as db_module

        closed: list[bool] = []

        class _FakeRow:
            def fetchone(self):
                return (0,)

        class _QueryOnlyZero(sqlite3.Connection):
            def close(self) -> None:
                closed.append(True)
                super().close()

            def execute(self, sql, *args):
                return _FakeRow()

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            conn.close()
            before = list(db_module._OPEN_CONNECTIONS)
            with patch("coordinate.db.sqlite3.connect", return_value=_QueryOnlyZero(":memory:")):
                with self.assertRaisesRegex(ReadOnlyConnectionError, "query_only"):
                    connect_readonly(db_path)
            self.assertEqual(closed, [True], "connection must be closed on failure")
            self.assertEqual(db_module._OPEN_CONNECTIONS, before, "must not register a failed connection")

    def test_post_open_non_sqlite_failure_closes_and_re_raises(self) -> None:
        import coordinate.db as db_module

        closed: list[bool] = []

        class _Boom(sqlite3.Connection):
            def close(self) -> None:
                closed.append(True)
                super().close()

            def execute(self, sql, *args):
                raise RuntimeError("simulated non-sqlite failure")

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            conn.close()
            before = list(db_module._OPEN_CONNECTIONS)
            with patch("coordinate.db.sqlite3.connect", return_value=_Boom(":memory:")):
                with self.assertRaisesRegex(RuntimeError, "simulated non-sqlite failure"):
                    connect_readonly(db_path)
            self.assertEqual(closed, [True], "connection must be closed on failure")
            self.assertEqual(db_module._OPEN_CONNECTIONS, before, "must not register a failed connection")

    # T5: concurrent/live DB boundaries.
    def test_readonly_reads_committed_snapshot_under_pending_writer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            upsert_workspace(
                conn, workspace_id="demo", name="Demo", path="/ws", harness_root="/ws/docs"
            )
            conn.close()
            before = self._snapshot(db_path)
            writer = sqlite3.connect(db_path)
            try:
                writer.execute("BEGIN IMMEDIATE")
                writer.execute(
                    "INSERT INTO workspaces "
                    "(id, name, path, harness_root, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    ("pending", "Pending", "/p", "/p/docs", "2026-01-01T00:00:00Z",
                     "2026-01-01T00:00:00Z"),
                )
                ro = connect_readonly(db_path)
                try:
                    assert_schema_compatible(ro)
                    # Uncommitted writer row must not be visible.
                    self.assertEqual([w.id for w in list_workspaces(ro)], ["demo"])
                finally:
                    ro.close()
            finally:
                writer.rollback()
                writer.close()
            self._assert_zero_mutation(before, self._snapshot(db_path))

    def test_concurrent_readonly_readers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            upsert_workspace(
                conn, workspace_id="demo", name="Demo", path="/ws", harness_root="/ws/docs"
            )
            conn.close()
            before = self._snapshot(db_path)
            errors: list[BaseException] = []
            counts: list[int] = []

            def reader() -> None:
                try:
                    ro = connect_readonly(db_path)
                    try:
                        assert_schema_compatible(ro)
                        counts.append(len(list_workspaces(ro)))
                    finally:
                        ro.close()
                except BaseException as exc:  # pragma: no cover - failure path
                    errors.append(exc)

            threads = [threading.Thread(target=reader) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            self.assertEqual(sorted(counts), [1, 1])
            self._assert_zero_mutation(before, self._snapshot(db_path))

    def test_wal_boundary_environment_aware(self) -> None:
        """WAL-mode DB is readable while the writer keeps -wal/-shm sidecars.
        The fail-closed missing-sidecar state is only asserted when this
        environment deterministically produces it; the read-only path itself
        never creates or modifies sidecars."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            upsert_workspace(
                conn, workspace_id="demo", name="Demo", path="/ws", harness_root="/ws/docs"
            )
            conn.commit()
            mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            self.assertEqual(mode, "wal")
            conn.execute(
                "INSERT INTO workspaces "
                "(id, name, path, harness_root, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("wal-ws", "Wal", "/w", "/w/docs", "2026-01-01T00:00:00Z",
                 "2026-01-01T00:00:00Z"),
            )
            conn.commit()
            # Baseline AFTER the writer's own WAL switch/commit: the read-only
            # path must leave these exact bytes untouched (the WAL-mode header
            # rewrite is the writer's action, not the read-only path's).
            before_wal = self._snapshot(db_path)
            writer_sidecars = (
                Path(f"{db_path}-wal").exists(),
                Path(f"{db_path}-shm").exists(),
            )
            self.assertTrue(any(writer_sidecars), "writer must hold WAL sidecars")
            ro = connect_readonly(db_path)
            try:
                assert_schema_compatible(ro)
                self.assertEqual(len(list_workspaces(ro)), 2)
            finally:
                ro.close()
            # The read-only path itself never modifies the DB file or creates
            # sidecars: bytes stay identical to the pre-WAL state (uncommitted
            # WAL content lives in -wal, not the DB file) and the sidecar set
            # is exactly the writer's.
            after_read = self._snapshot(db_path)
            self.assertEqual(after_read["bytes"], before_wal["bytes"], "DB bytes changed")
            self.assertEqual(after_read["mtime_ns"], before_wal["mtime_ns"], "DB mtime changed")
            self.assertEqual(
                (Path(f"{db_path}-wal").exists(), Path(f"{db_path}-shm").exists()),
                writer_sidecars,
            )
            conn.close()
            # After the last writer closes, SQLite checkpoints and removes the
            # sidecars. Fail-closed on a sidecar-less WAL DB is SQLite-version
            # dependent (>= 3.50 can read WAL without shared memory); assert
            # the result the environment actually produces and never gate on
            # internal cleanup timing.
            if not Path(f"{db_path}-wal").exists() and not Path(f"{db_path}-shm").exists():
                try:
                    ro2 = connect_readonly(db_path)
                except ReadOnlyConnectionError:
                    self.assertFalse(Path(f"{db_path}-wal").exists())
                    self.assertFalse(Path(f"{db_path}-shm").exists())
                else:
                    # SQLite >= 3.50: sidecar-less read-only WAL read works;
                    # still run the gate and a real query before closing.
                    try:
                        assert_schema_compatible(ro2)
                        self.assertEqual(len(list_workspaces(ro2)), 2)
                    finally:
                        ro2.close()
                    # SQLite versions that can read a sidecar-less WAL
                    # database may materialize runtime sidecars while opening
                    # the read-only connection. They are SQLite artifacts, not
                    # a Coordinate write; the DB byte/mtime checks above remain
                    # the mutation boundary.


if __name__ == "__main__":
    unittest.main()
