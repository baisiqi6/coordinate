import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from coordinate.db import (
    append_event,
    create_delivery,
    create_job,
    create_decision_request,
    create_task_group,
    connect,
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
    row_to_dict,
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

        self.assertEqual(user_version, 14)
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
            conn.execute("PRAGMA user_version").fetchone()[0], 14
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

        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
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
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)

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

        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
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
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)

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
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
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
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
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
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)
        migrate(conn)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 14)

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


if __name__ == "__main__":
    unittest.main()
