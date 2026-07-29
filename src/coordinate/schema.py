"""SQLite schema creation and versioned migrations."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


SCHEMA_VERSION = 14


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row["name"] for row in rows}


def _add_column_if_missing(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    if column not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _detect_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Detect duplicate JSON object keys and fail migration rather than silently pick the last value."""
    seen: set[str] = set()
    for key, _value in pairs:
        if key in seen:
            raise ValueError(f"duplicate key {key!r}")
        seen.add(key)
    return dict(pairs)


def _backfill_agent_registry_legacy(conn: sqlite3.Connection) -> None:
    """Migrate v9 agents_json into legacy registry entries.

    Validates every workspace's agents_json before writing any row. On failure
    the caller rolls back the transaction, leaving user_version and legacy rows
    unchanged.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rows = conn.execute(
        "SELECT id, agents_json FROM workspaces WHERE agents_json IS NOT NULL AND agents_json != ''"
    ).fetchall()

    for workspace_id, agents_json in rows:
        stripped = agents_json.strip()
        if stripped == "{}" or stripped == "":
            continue

        try:
            agents = json.loads(agents_json, object_pairs_hook=_detect_duplicate_keys)
        except Exception as exc:
            raise ValueError(f"workspace {workspace_id!r}: invalid agents_json") from exc

        if not isinstance(agents, dict):
            raise ValueError(f"workspace {workspace_id!r}: agents_json must be an object")

        legacy_entries: list[tuple[str, ...]] = []
        for agent_name, info in agents.items():
            if not isinstance(info, dict):
                raise ValueError(
                    f"workspace {workspace_id!r}: agent {agent_name!r} entry must be an object"
                )

            discord_user_id = info.get("discord_user_id")
            if not discord_user_id:
                raise ValueError(
                    f"workspace {workspace_id!r}: agent {agent_name!r} missing discord_user_id"
                )
            did = str(discord_user_id).strip()
            if not did or not did.isascii() or not did.isdigit() or int(did) <= 0:
                raise ValueError(
                    f"workspace {workspace_id!r}: agent {agent_name!r} has invalid discord_user_id"
                )

            display_name = str(info.get("display_name", "")).strip() or agent_name
            legacy_entries.append(
                (workspace_id, agent_name, "legacy", did, display_name, "legacy", now, now)
            )

        if legacy_entries:
            conn.execute(
                "DELETE FROM workspace_agent_registry_entries "
                "WHERE workspace_id = ? AND entry_kind = 'legacy'",
                (workspace_id,),
            )
            conn.executemany(
                """
                INSERT INTO workspace_agent_registry_entries (
                  workspace_id, agent_name, entry_kind, discord_user_id,
                  display_name, agent_type, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                legacy_entries,
            )


def migrate(conn: sqlite3.Connection) -> None:
    starting_version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS workspaces (
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

        CREATE TABLE IF NOT EXISTS events (
          id TEXT PRIMARY KEY,
          workspace_id TEXT,
          event_type TEXT NOT NULL,
          actor TEXT NOT NULL,
          target TEXT,
          task_id TEXT,
          causation_id TEXT,
          idempotency_key TEXT NOT NULL UNIQUE,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_workspace_created
          ON events(workspace_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_events_task
          ON events(workspace_id, task_id);

        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY,
          workspace_id TEXT,
          task_id TEXT,
          runner_profile_id TEXT,
          assigned_agent TEXT,
          status TEXT NOT NULL,
          prompt_path TEXT,
          branch TEXT,
          worktree_path TEXT,
          terminal_session_id TEXT,
          logs_path TEXT,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          timeout_seconds INTEGER,
          payload_json TEXT NOT NULL,
          result_json TEXT,
          created_at TEXT NOT NULL,
          started_at TEXT,
          completed_at TEXT,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_workspace_task ON jobs(workspace_id, task_id);

        CREATE TABLE IF NOT EXISTS deliveries (
          id TEXT PRIMARY KEY,
          event_id TEXT,
          platform TEXT NOT NULL,
          destination TEXT NOT NULL,
          message_key TEXT NOT NULL UNIQUE,
          status TEXT NOT NULL,
          platform_message_id TEXT,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);

        CREATE TABLE IF NOT EXISTS agents (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          role TEXT,
          capabilities_json TEXT NOT NULL,
          online_state TEXT NOT NULL,
          current_load INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runner_profiles (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          runner_type TEXT NOT NULL,
          command TEXT NOT NULL,
          working_directory_strategy TEXT NOT NULL,
          supports_stream_attach INTEGER NOT NULL DEFAULT 0,
          env_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
          workspace_id TEXT NOT NULL,
          task_id TEXT NOT NULL,
          phase TEXT,
          owner TEXT,
          branch TEXT,
          pr TEXT,
          last_event_id TEXT,
          payload_json TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (workspace_id, task_id),
          FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
          FOREIGN KEY(last_event_id) REFERENCES events(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_workspace_task ON tasks(workspace_id, task_id);

        CREATE TABLE IF NOT EXISTS task_groups (
          id TEXT PRIMARY KEY,
          workspace_id TEXT NOT NULL,
          title TEXT NOT NULL,
          status TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_task_groups_workspace_status
          ON task_groups(workspace_id, status);

        CREATE TABLE IF NOT EXISTS task_group_items (
          group_id TEXT NOT NULL,
          task_id TEXT NOT NULL,
          position INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (group_id, task_id),
          FOREIGN KEY(group_id) REFERENCES task_groups(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS decision_requests (
          id TEXT PRIMARY KEY,
          workspace_id TEXT NOT NULL,
          task_id TEXT,
          request_type TEXT NOT NULL,
          status TEXT NOT NULL,
          requester TEXT NOT NULL,
          reviewer TEXT,
          severity TEXT,
          summary TEXT NOT NULL,
          context_json TEXT NOT NULL,
          decision_json TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_decision_requests_workspace_status
          ON decision_requests(workspace_id, status);

        CREATE TABLE IF NOT EXISTS workspace_host_profiles (
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
    )
    for column, definition in {
        "runner_profile_id": "TEXT",
        "branch": "TEXT",
        "worktree_path": "TEXT",
        "terminal_session_id": "TEXT",
        "logs_path": "TEXT",
        "started_at": "TEXT",
        "completed_at": "TEXT",
        "last_activity_at": "TEXT",
        "progress_json": "TEXT",
        "recoverable": "INTEGER NOT NULL DEFAULT 0",
    }.items():
        _add_column_if_missing(conn, "jobs", column, definition)
    _add_column_if_missing(conn, "workspaces", "agents_json", "TEXT")
    _add_column_if_missing(conn, "agents", "host_id", "TEXT")
    _add_column_if_missing(conn, "agents", "client_type", "TEXT")
    _add_column_if_missing(conn, "agents", "last_seen_at", "TEXT")
    if starting_version < 9:
        conn.commit()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DROP INDEX IF EXISTS idx_tasks_workspace_branch")
            conn.execute("DROP INDEX IF EXISTS idx_tasks_workspace_pr")
            conn.execute(
                "CREATE UNIQUE INDEX idx_tasks_workspace_branch "
                "ON tasks(workspace_id, branch) WHERE phase IS NOT 'closed'"
            )
            conn.execute(
                "CREATE UNIQUE INDEX idx_tasks_workspace_pr "
                "ON tasks(workspace_id, pr)"
            )
            conn.execute("PRAGMA user_version = 9")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if starting_version < 10:
        _add_column_if_missing(
            conn, "workspaces", "agent_registry_revision", "INTEGER NOT NULL DEFAULT 0"
        )
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS workspace_agent_registry_sources (
              workspace_id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL,
              source_version INTEGER NOT NULL,
              source_hash TEXT NOT NULL,
              source_path TEXT,
              synced_by TEXT,
              synced_at TEXT NOT NULL,
              FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS workspace_agent_registry_entries (
              workspace_id TEXT NOT NULL,
              agent_name TEXT NOT NULL,
              entry_kind TEXT NOT NULL CHECK(entry_kind IN ('authoritative', 'override', 'legacy')),
              discord_user_id TEXT NOT NULL,
              display_name TEXT NOT NULL,
              agent_type TEXT NOT NULL,
              actor TEXT,
              reason TEXT,
              expires_at TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (workspace_id, agent_name, entry_kind),
              FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_workspace_agent_registry_entries_kind
              ON workspace_agent_registry_entries(workspace_id, entry_kind);
            """
        )
        conn.commit()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _backfill_agent_registry_legacy(conn)
            conn.execute("PRAGMA user_version = 10")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    else:
        conn.commit()

    if starting_version < 11:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS split_operations (
              operation_id TEXT PRIMARY KEY,
              contract_version INTEGER NOT NULL,
              operation_kind TEXT NOT NULL,
              workspace_id TEXT NOT NULL,
              target_kind TEXT NOT NULL,
              target_id TEXT NOT NULL,
              source_kind TEXT,
              source_id TEXT,
              input_fingerprint TEXT NOT NULL,
              before_fingerprint TEXT NOT NULL,
              after_fingerprint TEXT NOT NULL,
              status TEXT NOT NULL,
              record_event_id TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
              FOREIGN KEY(record_event_id) REFERENCES events(id) ON DELETE SET NULL
            );
            CREATE INDEX IF NOT EXISTS idx_split_operations_workspace_target
              ON split_operations(workspace_id, target_kind, target_id);
            CREATE INDEX IF NOT EXISTS idx_split_operations_status
              ON split_operations(status);
            """
        )
        conn.execute("PRAGMA user_version = 11")
        conn.commit()

    if starting_version < 12:
        try:
            conn.executescript(
                """
            BEGIN IMMEDIATE;

            CREATE TABLE IF NOT EXISTS executor_catalog_sources (
              source_id TEXT PRIMARY KEY,
              source_version INTEGER NOT NULL,
              catalog_hash TEXT NOT NULL,
              source_path TEXT,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS executor_definitions (
              id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL,
              provider TEXT NOT NULL,
              adapter TEXT NOT NULL,
              capabilities_json TEXT NOT NULL,
              metadata_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(source_id) REFERENCES executor_catalog_sources(source_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_executor_definitions_source_id
              ON executor_definitions(source_id);

            CREATE TABLE IF NOT EXISTS executor_instance_bindings (
              agent_id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL,
              executor_definition_id TEXT NOT NULL,
              runner_profile_id TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(source_id) REFERENCES executor_catalog_sources(source_id) ON DELETE CASCADE,
              FOREIGN KEY(agent_id) REFERENCES agents(id) ON DELETE CASCADE,
              FOREIGN KEY(executor_definition_id) REFERENCES executor_definitions(id) ON DELETE CASCADE,
              FOREIGN KEY(runner_profile_id) REFERENCES runner_profiles(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_executor_instance_bindings_source_id
              ON executor_instance_bindings(source_id);
            CREATE INDEX IF NOT EXISTS idx_executor_instance_bindings_definition_id
              ON executor_instance_bindings(executor_definition_id);
            CREATE INDEX IF NOT EXISTS idx_executor_instance_bindings_profile_id
              ON executor_instance_bindings(runner_profile_id);

            PRAGMA user_version = 12;
            COMMIT;
            """
            )
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise

    if starting_version < 13:
        try:
            conn.executescript(
                """
            BEGIN IMMEDIATE;

            CREATE TABLE IF NOT EXISTS executor_capacity_sources (
              source_id TEXT PRIMARY KEY,
              source_version INTEGER NOT NULL,
              catalog_hash TEXT NOT NULL,
              source_path TEXT,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS executor_capacity_policies (
              agent_id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL,
              source_version INTEGER NOT NULL,
              catalog_hash TEXT NOT NULL,
              capacity_policy_id TEXT NOT NULL,
              max_concurrent_jobs INTEGER NOT NULL CHECK(max_concurrent_jobs BETWEEN 1 AND 32),
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(source_id) REFERENCES executor_capacity_sources(source_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_executor_capacity_policies_source_id
              ON executor_capacity_policies(source_id);

            CREATE TABLE IF NOT EXISTS execution_attempt_leases (
              lease_id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              attempt_token INTEGER NOT NULL CHECK(attempt_token > 0),
              agent_id TEXT NOT NULL,
              runner_profile_id TEXT NOT NULL,
              host_id TEXT NOT NULL CHECK(length(host_id) BETWEEN 1 AND 64),
              resource_kind TEXT NOT NULL CHECK(resource_kind = 'worktree'),
              resource_key TEXT NOT NULL CHECK(resource_key GLOB 'sha256:*' AND length(resource_key) = 71),
              normalized_path TEXT NOT NULL CHECK(length(normalized_path) BETWEEN 1 AND 4096),
              capacity_policy_id TEXT NOT NULL CHECK(capacity_policy_id GLOB 'sha256:*' AND length(capacity_policy_id) = 71),
              max_concurrent_jobs INTEGER NOT NULL CHECK(max_concurrent_jobs BETWEEN 1 AND 32),
              status TEXT NOT NULL CHECK(status IN ('active', 'released', 'expired')),
              acquired_at TEXT NOT NULL,
              renewed_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              released_at TEXT,
              release_reason TEXT,
              CHECK (status != 'released' OR (released_at IS NOT NULL AND release_reason IS NOT NULL)),
              CHECK (status = 'released' OR (released_at IS NULL AND release_reason IS NULL)),
              CHECK (release_reason IS NULL OR length(release_reason) BETWEEN 1 AND 256),
              CHECK (renewed_at >= acquired_at AND expires_at >= renewed_at),
              CHECK (released_at IS NULL OR released_at >= acquired_at),
              UNIQUE(job_id, attempt_token),
              FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE RESTRICT,
              FOREIGN KEY(agent_id) REFERENCES agents(id) ON DELETE RESTRICT,
              FOREIGN KEY(runner_profile_id) REFERENCES runner_profiles(id) ON DELETE RESTRICT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_attempt_leases_active_resource
              ON execution_attempt_leases(resource_key) WHERE status = 'active';
            CREATE INDEX IF NOT EXISTS idx_execution_attempt_leases_agent_active
              ON execution_attempt_leases(agent_id) WHERE status = 'active';
            CREATE INDEX IF NOT EXISTS idx_execution_attempt_leases_expires
              ON execution_attempt_leases(expires_at) WHERE status = 'active';
            CREATE INDEX IF NOT EXISTS idx_execution_attempt_leases_job
              ON execution_attempt_leases(job_id, attempt_token);
            CREATE INDEX IF NOT EXISTS idx_execution_attempt_leases_resource
              ON execution_attempt_leases(resource_key, status);

            PRAGMA user_version = 13;
            COMMIT;
            """
            )
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise

    if starting_version < 14:
        try:
            conn.executescript(
                """
            BEGIN IMMEDIATE;

            CREATE TABLE IF NOT EXISTS channel_bindings (
              platform TEXT NOT NULL,
              channel_id TEXT NOT NULL,
              workspace_id TEXT NOT NULL
                REFERENCES workspaces(id) ON DELETE RESTRICT,
              bound_at TEXT NOT NULL,
              PRIMARY KEY (platform, channel_id)
            );

            PRAGMA user_version = 14;
            COMMIT;
            """
            )
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise

    conn.commit()
