from __future__ import annotations

import json
import atexit
import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from coordinate.schema import (
    SCHEMA_VERSION,  # noqa: F401
    _add_column_if_missing,  # noqa: F401
    _table_columns,  # noqa: F401
    migrate,
)
from coordinate.db_support import _absolute_path, _json_dumps, utc_now
from coordinate.job_repository import (  # noqa: F401
    create_job,
    get_job,
    list_jobs,
    mark_job_cancelled,
    mark_job_completed,
    mark_job_started,
)

_OPEN_CONNECTIONS: list[sqlite3.Connection] = []


def _unregister_connection(conn: sqlite3.Connection) -> None:
    try:
        _OPEN_CONNECTIONS.remove(conn)
    except ValueError:
        pass


def _close_open_connections() -> None:
    for conn in list(_OPEN_CONNECTIONS):
        try:
            conn.close()
        except sqlite3.Error:
            pass
        finally:
            _unregister_connection(conn)


class CoordinatorConnection(sqlite3.Connection):
    def close(self) -> None:
        try:
            super().close()
        finally:
            _unregister_connection(self)


atexit.register(_close_open_connections)


def connect(db_path: str | Path) -> sqlite3.Connection:
    if str(db_path) != ":memory:":
        Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), factory=CoordinatorConnection)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # P9-3B: explicit 30-second busy timeout for production DB connections.
    # WAL is deliberately not enabled; rollback/backup behavior remains unchanged.
    conn.execute("PRAGMA busy_timeout = 30000")
    _OPEN_CONNECTIONS.append(conn)
    return conn


def initialize(db_path: str | Path) -> sqlite3.Connection:
    conn = connect(db_path)
    migrate(conn)
    return conn


@dataclass(frozen=True)
class Workspace:
    id: str
    name: str
    path: str
    harness_root: str
    harnessctl_path: str | None = None
    default_bus: str | None = None
    default_destination: str | None = None
    base_branch: str | None = None
    branch_namespace: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Workspace":
        return cls(
            id=row["id"],
            name=row["name"],
            path=row["path"],
            harness_root=row["harness_root"],
            harnessctl_path=row["harnessctl_path"],
            default_bus=row["default_bus"],
            default_destination=row["default_destination"],
            base_branch=row["base_branch"],
            branch_namespace=row["branch_namespace"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "path": self.path,
            "harness_root": self.harness_root,
            "harnessctl_path": self.harnessctl_path,
            "default_bus": self.default_bus,
            "default_destination": self.default_destination,
            "base_branch": self.base_branch,
            "branch_namespace": self.branch_namespace,
        }


@dataclass(frozen=True)
class WorkspaceHostProfile:
    workspace_id: str
    host_id: str
    workspace_path: str
    harness_root: str | None = None
    harnessctl_path: str | None = None
    coordinator_cli_path: str | None = None
    coordinator_db_path: str | None = None
    shell: str | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "WorkspaceHostProfile":
        return cls(
            workspace_id=row["workspace_id"],
            host_id=row["host_id"],
            workspace_path=row["workspace_path"],
            harness_root=row["harness_root"],
            harnessctl_path=row["harnessctl_path"],
            coordinator_cli_path=row["coordinator_cli_path"],
            coordinator_db_path=row["coordinator_db_path"],
            shell=row["shell"],
            metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else {},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "host_id": self.host_id,
            "workspace_path": self.workspace_path,
            "harness_root": self.harness_root,
            "harnessctl_path": self.harnessctl_path,
            "coordinator_cli_path": self.coordinator_cli_path,
            "coordinator_db_path": self.coordinator_db_path,
            "shell": self.shell,
            "metadata": self.metadata or {},
        }


@dataclass(frozen=True)
class AppendEventResult:
    row: sqlite3.Row
    created: bool


@dataclass(frozen=True)
class RunnerProfile:
    id: str
    name: str
    runner_type: str
    command: str
    working_directory_strategy: str
    supports_stream_attach: bool
    env: dict[str, Any]

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "RunnerProfile":
        return cls(
            id=row["id"],
            name=row["name"],
            runner_type=row["runner_type"],
            command=row["command"],
            working_directory_strategy=row["working_directory_strategy"],
            supports_stream_attach=bool(row["supports_stream_attach"]),
            env=json.loads(row["env_json"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "runner_type": self.runner_type,
            "command": self.command,
            "working_directory_strategy": self.working_directory_strategy,
            "supports_stream_attach": self.supports_stream_attach,
            "env": self.env,
        }


def upsert_workspace(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    name: str,
    path: str | Path,
    harness_root: str | Path,
    harnessctl_path: str | Path | None = None,
    default_bus: str | None = None,
    default_destination: str | None = None,
    base_branch: str | None = None,
    branch_namespace: str | None = None,
) -> Workspace:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO workspaces (
          id, name, path, harness_root, harnessctl_path, default_bus,
          default_destination, base_branch, branch_namespace, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          name = excluded.name,
          path = excluded.path,
          harness_root = excluded.harness_root,
          harnessctl_path = excluded.harnessctl_path,
          default_bus = excluded.default_bus,
          default_destination = excluded.default_destination,
          base_branch = excluded.base_branch,
          branch_namespace = excluded.branch_namespace,
          updated_at = excluded.updated_at
        """,
        (
            workspace_id,
            name,
            _absolute_path(path),
            _absolute_path(harness_root),
            _absolute_path(harnessctl_path) if harnessctl_path else None,
            default_bus,
            default_destination,
            base_branch,
            branch_namespace,
            now,
            now,
        ),
    )
    conn.commit()
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise RuntimeError(f"workspace was not written: {workspace_id}")
    return workspace


def get_workspace(conn: sqlite3.Connection, workspace_id: str) -> Workspace | None:
    row = conn.execute("SELECT * FROM workspaces WHERE id = ?", (workspace_id,)).fetchone()
    return Workspace.from_row(row) if row else None


def list_workspaces(conn: sqlite3.Connection) -> list[Workspace]:
    rows = conn.execute("SELECT * FROM workspaces ORDER BY id").fetchall()
    return [Workspace.from_row(row) for row in rows]


def upsert_workspace_host_profile(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    host_id: str,
    workspace_path: str,
    harness_root: str | None = None,
    harnessctl_path: str | None = None,
    coordinator_cli_path: str | None = None,
    coordinator_db_path: str | None = None,
    shell: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> WorkspaceHostProfile:
    if get_workspace(conn, workspace_id) is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    if not host_id.strip():
        raise ValueError("host_id is required")
    if not workspace_path.strip():
        raise ValueError("workspace_path is required")

    now = utc_now()
    conn.execute(
        """
        INSERT INTO workspace_host_profiles (
          workspace_id, host_id, workspace_path, harness_root, harnessctl_path,
          coordinator_cli_path, coordinator_db_path, shell, metadata_json,
          created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(workspace_id, host_id) DO UPDATE SET
          workspace_path = excluded.workspace_path,
          harness_root = excluded.harness_root,
          harnessctl_path = excluded.harnessctl_path,
          coordinator_cli_path = excluded.coordinator_cli_path,
          coordinator_db_path = excluded.coordinator_db_path,
          shell = excluded.shell,
          metadata_json = excluded.metadata_json,
          updated_at = excluded.updated_at
        """,
        (
            workspace_id,
            host_id,
            workspace_path,
            harness_root,
            harnessctl_path,
            coordinator_cli_path,
            coordinator_db_path,
            shell,
            _json_dumps(metadata),
            now,
            now,
        ),
    )
    conn.commit()
    profile = get_workspace_host_profile(conn, workspace_id=workspace_id, host_id=host_id)
    if profile is None:
        raise RuntimeError(f"workspace host profile was not written: {workspace_id}/{host_id}")
    return profile


def get_workspace_host_profile(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    host_id: str,
) -> WorkspaceHostProfile | None:
    row = conn.execute(
        """
        SELECT * FROM workspace_host_profiles
        WHERE workspace_id = ? AND host_id = ?
        """,
        (workspace_id, host_id),
    ).fetchone()
    return WorkspaceHostProfile.from_row(row) if row else None


def list_workspace_host_profiles(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
) -> list[WorkspaceHostProfile]:
    rows = conn.execute(
        """
        SELECT * FROM workspace_host_profiles
        WHERE workspace_id = ?
        ORDER BY host_id
        """,
        (workspace_id,),
    ).fetchall()
    return [WorkspaceHostProfile.from_row(row) for row in rows]


_EXPIRES_AT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _validate_discord_user_id(value: Any) -> str:
    """Return a normalized Discord user id or raise ValueError."""
    did = str(value).strip()
    if not did or not did.isascii() or not did.isdigit() or int(did) <= 0:
        raise ValueError(f"invalid discord_user_id: {value!r}")
    return did


def _utc_now_str(now: datetime | str | None = None) -> str:
    """Return a strict UTC timestamp string, optionally from a fixed clock."""
    if now is None:
        return utc_now()
    if isinstance(now, datetime):
        return now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return now


def _parse_expires_at(value: str | None, now: datetime | str | None = None) -> str | None:
    """Validate strict UTC expiry format and reject past or malformed values."""
    if value is None:
        return None
    if not isinstance(value, str) or not _EXPIRES_AT_RE.match(value):
        raise ValueError(
            "expires_at must be exactly YYYY-MM-DDTHH:MM:SSZ in UTC"
        )
    # Ensure the timestamp is a real calendar value.
    datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    if value <= _utc_now_str(now):
        raise ValueError("expires_at must be in the future")
    return value


def _write_agents_json_projection(
    conn: sqlite3.Connection,
    workspace_id: str,
    *,
    increment_revision: bool = True,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    """Regenerate workspaces.agents_json from the effective registry.

    This is a write helper: callers are responsible for transaction/commit.
    """
    effective = resolve_effective_agents(conn, workspace_id, now_utc=now)
    agents_json = _json_dumps(effective)
    ts = _utc_now_str(now)
    if increment_revision:
        conn.execute(
            """
            UPDATE workspaces
            SET agents_json = ?, agent_registry_revision = agent_registry_revision + 1, updated_at = ?
            WHERE id = ?
            """,
            (agents_json, ts, workspace_id),
        )
    else:
        conn.execute(
            "UPDATE workspaces SET agents_json = ?, updated_at = ? WHERE id = ?",
            (agents_json, ts, workspace_id),
        )
    return effective


def resolve_effective_agents(
    conn: sqlite3.Connection,
    workspace_id: str,
    *,
    now_utc: datetime | str | None = None,
) -> dict[str, dict[str, Any]]:
    """Return the currently effective agent map for a workspace.

    Resolution order: active override > authoritative > legacy. Expired
    overrides are retained for audit but ignored. Duplicate effective Discord
    ids fail closed.
    """
    now = _utc_now_str(now_utc)
    rows = conn.execute(
        """
        SELECT agent_name, entry_kind, discord_user_id, display_name, agent_type, expires_at
        FROM workspace_agent_registry_entries
        WHERE workspace_id = ?
        ORDER BY
          CASE entry_kind
            WHEN 'override' THEN 1
            WHEN 'authoritative' THEN 2
            WHEN 'legacy' THEN 3
            ELSE 4
          END
        """,
        (workspace_id,),
    ).fetchall()

    effective: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = row["agent_name"]
        if name in effective:
            continue
        if row["entry_kind"] == "override" and row["expires_at"] is not None and row["expires_at"] <= now:
            continue
        effective[name] = {
            "discord_user_id": row["discord_user_id"],
            "display_name": row["display_name"],
            "agent_type": row["agent_type"],
        }

    seen: dict[str, str] = {}
    for name, info in effective.items():
        did = info["discord_user_id"]
        if did in seen:
            raise ValueError(
                f"duplicate effective discord_user_id {did} for {seen[did]} and {name}"
            )
        seen[did] = name

    return effective


def build_agent_registry_map(
    conn: sqlite3.Connection,
    *,
    now_utc: datetime | str | None = None,
) -> dict[int, dict[str, str]]:
    """Build the daemon's global {discord_id: {workspace_id: agent_name}} map."""
    registry: dict[int, dict[str, str]] = {}
    for workspace in list_workspaces(conn):
        effective = resolve_effective_agents(conn, workspace.id, now_utc=now_utc)
        for agent_name, info in effective.items():
            discord_id = int(info["discord_user_id"])
            registry.setdefault(discord_id, {})[workspace.id] = agent_name
    return registry


def get_agent_discord_id(
    conn: sqlite3.Connection,
    workspace_id: str,
    agent_name: str,
) -> str | None:
    effective = resolve_effective_agents(conn, workspace_id)
    entry = effective.get(agent_name)
    return entry["discord_user_id"] if entry else None


def set_workspace_agent(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    agent_name: str,
    discord_user_id: str,
    actor: str,
    reason: str,
    expires_at: str | None = None,
    now_utc: datetime | str | None = None,
) -> None:
    """Upsert a manual override entry for an agent.

    Actor and reason are mandatory. The override, revision, compatibility
    projection and audit event are committed atomically.
    """
    if get_workspace(conn, workspace_id) is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    agent_name = str(agent_name).strip()
    if not agent_name:
        raise ValueError("agent_name is required")
    if not reason or not str(reason).strip():
        raise ValueError("reason is required")
    if not actor or not str(actor).strip():
        raise ValueError("actor is required")

    did = _validate_discord_user_id(discord_user_id)
    expiry = _parse_expires_at(expires_at, now=now_utc)
    now = _utc_now_str(now_utc)

    existing = conn.execute(
        """
        SELECT discord_user_id FROM workspace_agent_registry_entries
        WHERE workspace_id = ? AND agent_name = ? AND entry_kind = 'override'
        """,
        (workspace_id, agent_name),
    ).fetchone()

    # Reject duplicate effective Discord ids across different names.
    current_effective = resolve_effective_agents(conn, workspace_id, now_utc=now_utc)
    current_effective.pop(agent_name, None)
    if did in {info["discord_user_id"] for info in current_effective.values()}:
        raise ValueError(
            f"discord_user_id {did} is already effective for another agent"
        )

    conn.execute("SAVEPOINT workspace_agent_override_set")
    try:
        if existing is None:
            conn.execute(
                """
                INSERT INTO workspace_agent_registry_entries (
                  workspace_id, agent_name, entry_kind, discord_user_id, display_name,
                  agent_type, actor, reason, expires_at, created_at, updated_at
                )
                VALUES (?, ?, 'override', ?, ?, 'override', ?, ?, ?, ?, ?)
                """,
                (workspace_id, agent_name, did, agent_name, actor, reason, expiry, now, now),
            )
        else:
            conn.execute(
                """
                UPDATE workspace_agent_registry_entries
                SET discord_user_id = ?, actor = ?, reason = ?, expires_at = ?, updated_at = ?
                WHERE workspace_id = ? AND agent_name = ? AND entry_kind = 'override'
                """,
                (did, actor, reason, expiry, now, workspace_id, agent_name),
            )

        _write_agents_json_projection(conn, workspace_id, increment_revision=True, now=now_utc)
        append_event(
            conn,
            event_type="workspace.agent_override.set",
            actor=actor,
            target=workspace_id,
            workspace_id=workspace_id,
            idempotency_key=str(uuid.uuid4()),
            payload={
                "agent_name": agent_name,
                "discord_user_id": did,
                "reason": reason,
                "expires_at": expiry,
            },
            commit=False,
        )
    except Exception:
        conn.execute("ROLLBACK TO workspace_agent_override_set")
        conn.execute("RELEASE workspace_agent_override_set")
        raise
    conn.execute("RELEASE workspace_agent_override_set")
    conn.commit()


def remove_workspace_agent_override(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    agent_name: str,
    actor: str,
    reason: str,
    now_utc: datetime | str | None = None,
) -> dict[str, Any]:
    """Remove only the override row for an agent, revealing roster/legacy entries.

    Returns a dict describing the removed override. Raises ValueError if the
    override does not exist.
    """
    if get_workspace(conn, workspace_id) is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    agent_name = str(agent_name).strip()
    if not agent_name:
        raise ValueError("agent_name is required")
    if not reason or not str(reason).strip():
        raise ValueError("reason is required")
    if not actor or not str(actor).strip():
        raise ValueError("actor is required")

    row = conn.execute(
        """
        SELECT discord_user_id, expires_at FROM workspace_agent_registry_entries
        WHERE workspace_id = ? AND agent_name = ? AND entry_kind = 'override'
        """,
        (workspace_id, agent_name),
    ).fetchone()
    if row is None:
        raise ValueError(f"no override found for {agent_name!r} in workspace {workspace_id!r}")

    conn.execute("SAVEPOINT workspace_agent_override_remove")
    try:
        conn.execute(
            """
            DELETE FROM workspace_agent_registry_entries
            WHERE workspace_id = ? AND agent_name = ? AND entry_kind = 'override'
            """,
            (workspace_id, agent_name),
        )

        _write_agents_json_projection(conn, workspace_id, increment_revision=True, now=now_utc)
        append_event(
            conn,
            event_type="workspace.agent_override.removed",
            actor=actor,
            target=workspace_id,
            workspace_id=workspace_id,
            idempotency_key=str(uuid.uuid4()),
            payload={
                "agent_name": agent_name,
                "discord_user_id": row["discord_user_id"],
                "reason": reason,
                "expires_at": row["expires_at"],
            },
            commit=False,
        )
    except Exception:
        conn.execute("ROLLBACK TO workspace_agent_override_remove")
        conn.execute("RELEASE workspace_agent_override_remove")
        raise
    conn.execute("RELEASE workspace_agent_override_remove")
    conn.commit()
    return {
        "workspace_id": workspace_id,
        "agent_name": agent_name,
        "discord_user_id": row["discord_user_id"],
    }


def sync_workspace_agents(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    source_id: str,
    source_version: int,
    source_hash: str,
    source_path: str | None = None,
    entries: list[dict[str, Any]],
    replace: bool = True,
    synced_by: str = "operator",
    now_utc: datetime | str | None = None,
) -> dict[str, Any]:
    """Authoritative roster sync: replace authoritative/legacy rows, preserve overrides.

    The source identity, version and canonical roster hash are validated
    against conflict/rollback/takeover rules before any write. The mutation is
    atomic with source metadata, entries, revision, compatibility projection
    and audit event.
    """
    if get_workspace(conn, workspace_id) is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    if not replace:
        raise ValueError("authoritative sync requires --replace")

    if not source_id or not str(source_id).strip():
        raise ValueError("source_id is required")
    source_id = str(source_id).strip()
    if not synced_by or not str(synced_by).strip():
        raise ValueError("synced_by is required")
    if isinstance(source_version, bool) or not isinstance(source_version, int) or source_version < 0:
        raise ValueError("source_version must be a non-negative integer")
    if not source_hash or len(source_hash) != 64 or any(c not in "0123456789abcdef" for c in source_hash):
        raise ValueError("source_hash must be a lowercase 64-character SHA-256 hex string")

    # Normalize and validate input entries.
    seen_ids: set[str] = set()
    seen_discord_ids: set[str] = set()
    normalized_entries: list[dict[str, Any]] = []
    for raw in entries:
        name = str(raw.get("id", "")).strip()
        if not name:
            raise ValueError("entry missing id")
        if name in seen_ids:
            raise ValueError(f"duplicate agent id {name!r}")
        seen_ids.add(name)

        did = _validate_discord_user_id(raw.get("discord_user_id"))
        if did in seen_discord_ids:
            raise ValueError(f"duplicate discord_user_id {did!r}")
        seen_discord_ids.add(did)

        display_name = str(raw.get("display_name", name)).strip() or name
        agent_type = str(raw.get("agent_type", "managed")).strip().lower()
        if agent_type not in {"managed", "external"}:
            raise ValueError(f"invalid agent_type {agent_type!r} for {name!r}")

        normalized_entries.append({
            "id": name,
            "discord_user_id": did,
            "display_name": display_name,
            "agent_type": agent_type,
        })

    # Conflict/rollback/takeover rules against the existing source row.
    source_row = conn.execute(
        "SELECT source_id, source_version, source_hash FROM workspace_agent_registry_sources WHERE workspace_id = ?",
        (workspace_id,),
    ).fetchone()

    if source_row is not None:
        existing_id = source_row["source_id"]
        existing_version = source_row["source_version"]
        existing_hash = source_row["source_hash"]
        if source_id != existing_id:
            raise ValueError(
                f"source takeover rejected: existing source is {existing_id!r}, got {source_id!r}"
            )
        if source_version < existing_version:
            raise ValueError(
                f"rollback rejected: existing version {existing_version}, got {source_version}"
            )
        if source_version == existing_version and source_hash != existing_hash:
            raise ValueError(
                f"version conflict: source version {source_version} already has hash {existing_hash}"
            )
        if source_version == existing_version and source_hash == existing_hash:
            # Idempotent no-op: report the current authoritative roster unchanged.
            revision = conn.execute(
                "SELECT agent_registry_revision FROM workspaces WHERE id = ?",
                (workspace_id,),
            ).fetchone()["agent_registry_revision"]
            authoritative_names = [
                row["agent_name"]
                for row in conn.execute(
                    """
                    SELECT agent_name
                    FROM workspace_agent_registry_entries
                    WHERE workspace_id = ? AND entry_kind = 'authoritative'
                    ORDER BY agent_name
                    """,
                    (workspace_id,),
                ).fetchall()
            ]
            shadowed_rows = conn.execute(
                """
                SELECT authoritative.agent_name
                FROM workspace_agent_registry_entries authoritative
                JOIN workspace_agent_registry_entries override
                  ON authoritative.workspace_id = override.workspace_id
                 AND authoritative.agent_name = override.agent_name
                 AND override.entry_kind = 'override'
                 AND (override.expires_at IS NULL OR override.expires_at > ?)
                WHERE authoritative.workspace_id = ?
                  AND authoritative.entry_kind = 'authoritative'
                """,
                (_utc_now_str(now_utc), workspace_id),
            ).fetchall()
            return {
                "workspace_id": workspace_id,
                "source_id": source_id,
                "source_version": source_version,
                "source_hash": source_hash,
                "revision": revision,
                "added": [],
                "updated": [],
                "removed": [],
                "unchanged": authoritative_names,
                "shadowed": sorted({row["agent_name"] for row in shadowed_rows}),
            }

    before_source: dict[str, dict[str, Any]] = {}
    for row in conn.execute(
        """
        SELECT agent_name, entry_kind, discord_user_id, display_name, agent_type
        FROM workspace_agent_registry_entries
        WHERE workspace_id = ? AND entry_kind IN ('authoritative', 'legacy')
        ORDER BY CASE entry_kind WHEN 'authoritative' THEN 1 ELSE 2 END
        """,
        (workspace_id,),
    ).fetchall():
        before_source.setdefault(
            row["agent_name"],
            {
                "discord_user_id": row["discord_user_id"],
                "display_name": row["display_name"],
                "agent_type": row["agent_type"],
            },
        )
    after_source = {
        entry["id"]: {
            "discord_user_id": entry["discord_user_id"],
            "display_name": entry["display_name"],
            "agent_type": entry["agent_type"],
        }
        for entry in normalized_entries
    }

    conn.execute("SAVEPOINT workspace_agent_registry_sync")
    try:
        conn.execute(
            "DELETE FROM workspace_agent_registry_entries WHERE workspace_id = ? AND entry_kind IN ('authoritative', 'legacy')",
            (workspace_id,),
        )

        now = _utc_now_str(now_utc)
        for entry in normalized_entries:
            conn.execute(
                """
                INSERT INTO workspace_agent_registry_entries (
                  workspace_id, agent_name, entry_kind, discord_user_id, display_name,
                  agent_type, created_at, updated_at
                )
                VALUES (?, ?, 'authoritative', ?, ?, ?, ?, ?)
                """,
                (workspace_id, entry["id"], entry["discord_user_id"], entry["display_name"], entry["agent_type"], now, now),
            )

        conn.execute(
            """
            INSERT INTO workspace_agent_registry_sources (
              workspace_id, source_id, source_version, source_hash, source_path, synced_by, synced_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_id) DO UPDATE SET
              source_id = excluded.source_id,
              source_version = excluded.source_version,
              source_hash = excluded.source_hash,
              source_path = excluded.source_path,
              synced_by = excluded.synced_by,
              synced_at = excluded.synced_at
            """,
            (workspace_id, source_id, source_version, source_hash, source_path, synced_by, now),
        )

        _write_agents_json_projection(conn, workspace_id, increment_revision=True, now=now_utc)
        shadowed_rows = conn.execute(
            """
            SELECT authoritative.agent_name
            FROM workspace_agent_registry_entries authoritative
            JOIN workspace_agent_registry_entries override
              ON authoritative.workspace_id = override.workspace_id
             AND authoritative.agent_name = override.agent_name
             AND override.entry_kind = 'override'
             AND (override.expires_at IS NULL OR override.expires_at > ?)
            WHERE authoritative.workspace_id = ?
              AND authoritative.entry_kind = 'authoritative'
            """,
            (_utc_now_str(now_utc), workspace_id),
        ).fetchall()
        shadowed = sorted({row["agent_name"] for row in shadowed_rows})
        added = sorted(set(after_source) - set(before_source))
        updated = sorted(
            name
            for name in (set(before_source) & set(after_source))
            if before_source[name] != after_source[name]
        )
        removed = sorted(set(before_source) - set(after_source))
        unchanged = sorted(
            name
            for name in (set(before_source) & set(after_source))
            if before_source[name] == after_source[name]
        )
        append_event(
            conn,
            event_type="workspace.agent_registry.synced",
            actor=synced_by,
            target=workspace_id,
            workspace_id=workspace_id,
            idempotency_key=f"{workspace_id}:agent-registry-sync:{source_id}:{source_version}:{source_hash}",
            payload={
                "source_id": source_id,
                "source_version": source_version,
                "source_hash": source_hash,
                "added": added,
                "updated": updated,
                "removed": removed,
                "unchanged": unchanged,
                "shadowed": shadowed,
            },
            commit=False,
        )
        revision = conn.execute(
            "SELECT agent_registry_revision FROM workspaces WHERE id = ?",
            (workspace_id,),
        ).fetchone()["agent_registry_revision"]
    except Exception:
        conn.execute("ROLLBACK TO workspace_agent_registry_sync")
        conn.execute("RELEASE workspace_agent_registry_sync")
        raise
    conn.execute("RELEASE workspace_agent_registry_sync")
    conn.commit()

    return {
        "workspace_id": workspace_id,
        "source_id": source_id,
        "source_version": source_version,
        "source_hash": source_hash,
        "revision": revision,
        "added": added,
        "updated": updated,
        "removed": removed,
        "unchanged": unchanged,
        "shadowed": shadowed,
    }


def upsert_runner_profile(
    conn: sqlite3.Connection,
    *,
    profile_id: str,
    name: str,
    runner_type: str,
    command: str,
    working_directory_strategy: str = "current_dir",
    supports_stream_attach: bool = False,
    env: dict[str, Any] | None = None,
) -> RunnerProfile:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO runner_profiles (
          id, name, runner_type, command, working_directory_strategy,
          supports_stream_attach, env_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          name = excluded.name,
          runner_type = excluded.runner_type,
          command = excluded.command,
          working_directory_strategy = excluded.working_directory_strategy,
          supports_stream_attach = excluded.supports_stream_attach,
          env_json = excluded.env_json,
          updated_at = excluded.updated_at
        """,
        (
            profile_id,
            name,
            runner_type,
            command,
            working_directory_strategy,
            1 if supports_stream_attach else 0,
            _json_dumps(env),
            now,
            now,
        ),
    )
    conn.commit()
    profile = get_runner_profile(conn, profile_id)
    if profile is None:
        raise RuntimeError(f"runner profile was not written: {profile_id}")
    return profile


def get_runner_profile(conn: sqlite3.Connection, profile_id: str) -> RunnerProfile | None:
    row = conn.execute("SELECT * FROM runner_profiles WHERE id = ?", (profile_id,)).fetchone()
    return RunnerProfile.from_row(row) if row else None


def list_runner_profiles(conn: sqlite3.Connection) -> list[RunnerProfile]:
    rows = conn.execute("SELECT * FROM runner_profiles ORDER BY id").fetchall()
    return [RunnerProfile.from_row(row) for row in rows]


def create_delivery(
    conn: sqlite3.Connection,
    *,
    platform: str,
    destination: str,
    message_key: str,
    payload: dict[str, Any],
    event_id: str | None = None,
    delivery_id: str | None = None,
    commit: bool = True,
) -> tuple[sqlite3.Row, bool]:
    if event_id is not None:
        get_event(conn, event_id)
    now = utc_now()
    did = delivery_id or str(uuid.uuid4())
    try:
        conn.execute(
            """
            INSERT INTO deliveries (
              id, event_id, platform, destination, message_key, status,
              platform_message_id, attempt_count, last_error, payload_json,
              created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                did,
                event_id,
                platform,
                destination,
                message_key,
                "pending",
                None,
                0,
                None,
                _json_dumps(payload),
                now,
                now,
            ),
        )
        if commit:
            conn.commit()
        return get_delivery(conn, did), True
    except sqlite3.IntegrityError:
        row = conn.execute(
            "SELECT * FROM deliveries WHERE message_key = ?",
            (message_key,),
        ).fetchone()
        if row is None:
            raise
        return row, False


def get_delivery(conn: sqlite3.Connection, delivery_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
    if row is None:
        raise KeyError(delivery_id)
    return row


def list_deliveries(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    platform: str | None = None,
    delivery_type: str | None = None,
) -> list[sqlite3.Row]:
    where: list[str] = []
    params: list[str] = []
    if status:
        where.append("status = ?")
        params.append(status)
    if platform:
        where.append("platform = ?")
        params.append(platform)
    if delivery_type == "dry_run":
        where.append("platform = ? AND destination = ?")
        params.extend(["stdout", "local"])
    elif delivery_type == "live":
        where.append("(platform != ? OR destination != ?)")
        params.extend(["stdout", "local"])
    sql = "SELECT * FROM deliveries"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY rowid"
    return conn.execute(sql, params).fetchall()


def mark_delivery_sending(conn: sqlite3.Connection, delivery_id: str) -> sqlite3.Row:
    now = utc_now()
    delivery = get_delivery(conn, delivery_id)
    conn.execute(
        """
        UPDATE deliveries
        SET status = ?, attempt_count = ?, last_error = ?, updated_at = ?
        WHERE id = ?
        """,
        ("sending", int(delivery["attempt_count"]) + 1, None, now, delivery_id),
    )
    conn.commit()
    return get_delivery(conn, delivery_id)


def mark_delivery_sent(
    conn: sqlite3.Connection,
    delivery_id: str,
    *,
    platform_message_id: str,
) -> sqlite3.Row:
    now = utc_now()
    conn.execute(
        """
        UPDATE deliveries
        SET status = ?, platform_message_id = ?, last_error = ?, updated_at = ?
        WHERE id = ?
        """,
        ("sent", platform_message_id, None, now, delivery_id),
    )
    conn.commit()
    return get_delivery(conn, delivery_id)


def mark_delivery_failed(
    conn: sqlite3.Connection,
    delivery_id: str,
    *,
    error: str,
    dead: bool = False,
) -> sqlite3.Row:
    now = utc_now()
    conn.execute(
        """
        UPDATE deliveries
        SET status = ?, last_error = ?, updated_at = ?
        WHERE id = ?
        """,
        ("dead" if dead else "failed", error, now, delivery_id),
    )
    conn.commit()
    return get_delivery(conn, delivery_id)


def recover_sending_deliveries(
    conn: sqlite3.Connection,
    *,
    platform: str | None = None,
    reason: str = "recovered from sending state; platform delivery may need audit",
) -> list[sqlite3.Row]:
    where = ["status = ?"]
    params: list[str] = ["sending"]
    if platform:
        where.append("platform = ?")
        params.append(platform)
    rows = conn.execute(
        "SELECT * FROM deliveries WHERE " + " AND ".join(where) + " ORDER BY rowid",
        params,
    ).fetchall()
    now = utc_now()
    for row in rows:
        conn.execute(
            """
            UPDATE deliveries
            SET status = ?, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            ("pending", reason, now, row["id"]),
        )
    conn.commit()
    return [get_delivery(conn, row["id"]) for row in rows]


def create_task_group(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    title: str,
    task_ids: list[str] | None = None,
    group_id: str | None = None,
    status: str = "open",
    payload: dict[str, Any] | None = None,
) -> sqlite3.Row:
    now = utc_now()
    gid = group_id or str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO task_groups (
          id, workspace_id, title, status, payload_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (gid, workspace_id, title, status, _json_dumps(payload), now, now),
    )
    for index, task_id in enumerate(task_ids or []):
        conn.execute(
            """
            INSERT INTO task_group_items (group_id, task_id, position)
            VALUES (?, ?, ?)
            """,
            (gid, task_id, index),
        )
    conn.commit()
    return conn.execute("SELECT * FROM task_groups WHERE id = ?", (gid,)).fetchone()


def create_decision_request(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    request_type: str,
    requester: str,
    summary: str,
    task_id: str | None = None,
    reviewer: str | None = None,
    severity: str | None = None,
    context: dict[str, Any] | None = None,
    request_id: str | None = None,
    status: str = "pending",
) -> sqlite3.Row:
    now = utc_now()
    rid = request_id or str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO decision_requests (
          id, workspace_id, task_id, request_type, status, requester, reviewer,
          severity, summary, context_json, decision_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rid,
            workspace_id,
            task_id,
            request_type,
            status,
            requester,
            reviewer,
            severity,
            summary,
            _json_dumps(context),
            None,
            now,
            now,
        ),
    )
    conn.commit()
    return conn.execute("SELECT * FROM decision_requests WHERE id = ?", (rid,)).fetchone()


def upsert_task_mirror(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    phase: str | None,
    owner: str | None,
    branch: str | None,
    pr: str | None,
    payload: dict[str, Any] | None,
    last_event_id: str | None = None,
    commit: bool = True,
) -> tuple[sqlite3.Row, str]:
    payload_json = _json_dumps(payload)
    existing = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if existing is not None:
        unchanged = (
            existing["phase"] == phase
            and existing["owner"] == owner
            and existing["branch"] == branch
            and existing["pr"] == pr
            and existing["last_event_id"] == last_event_id
            and existing["payload_json"] == payload_json
        )
        if unchanged:
            return existing, "unchanged"

    now = utc_now()
    conn.execute(
        """
        INSERT INTO tasks (
          workspace_id, task_id, phase, owner, branch, pr, last_event_id,
          payload_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(workspace_id, task_id) DO UPDATE SET
          phase = excluded.phase,
          owner = excluded.owner,
          branch = excluded.branch,
          pr = excluded.pr,
          last_event_id = excluded.last_event_id,
          payload_json = excluded.payload_json,
          updated_at = excluded.updated_at
        """,
        (workspace_id, task_id, phase, owner, branch, pr, last_event_id, payload_json, now),
    )
    if commit:
        conn.commit()
    row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    return row, "created" if existing is None else "updated"


def list_task_mirrors(conn: sqlite3.Connection, workspace_id: str | None = None) -> list[sqlite3.Row]:
    if workspace_id:
        return conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? ORDER BY task_id",
            (workspace_id,),
        ).fetchall()
    return conn.execute("SELECT * FROM tasks ORDER BY workspace_id, task_id").fetchall()


@dataclass(frozen=True)
class SplitOperation:
    operation_id: str
    contract_version: int
    operation_kind: str
    workspace_id: str
    target_kind: str
    target_id: str
    source_kind: str | None
    source_id: str | None
    input_fingerprint: str
    before_fingerprint: str
    after_fingerprint: str
    status: str
    record_event_id: str | None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "SplitOperation":
        return cls(
            operation_id=row["operation_id"],
            contract_version=row["contract_version"],
            operation_kind=row["operation_kind"],
            workspace_id=row["workspace_id"],
            target_kind=row["target_kind"],
            target_id=row["target_id"],
            source_kind=row["source_kind"],
            source_id=row["source_id"],
            input_fingerprint=row["input_fingerprint"],
            before_fingerprint=row["before_fingerprint"],
            after_fingerprint=row["after_fingerprint"],
            status=row["status"],
            record_event_id=row["record_event_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


def insert_split_operation(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    contract_version: int,
    operation_kind: str,
    workspace_id: str,
    target_kind: str,
    target_id: str,
    source_kind: str | None,
    source_id: str | None,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    status: str,
    record_event_id: str | None = None,
    created_at: str,
    updated_at: str,
) -> sqlite3.Row:
    conn.execute(
        """
        INSERT INTO split_operations (
          operation_id, contract_version, operation_kind, workspace_id,
          target_kind, target_id, source_kind, source_id,
          input_fingerprint, before_fingerprint, after_fingerprint,
          status, record_event_id, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            operation_id,
            contract_version,
            operation_kind,
            workspace_id,
            target_kind,
            target_id,
            source_kind,
            source_id,
            input_fingerprint,
            before_fingerprint,
            after_fingerprint,
            status,
            record_event_id,
            created_at,
            updated_at,
        ),
    )
    return conn.execute(
        "SELECT * FROM split_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()


def get_split_operation(
    conn: sqlite3.Connection,
    operation_id: str,
) -> SplitOperation | None:
    row = conn.execute(
        "SELECT * FROM split_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    return SplitOperation.from_row(row) if row else None


def update_split_operation_event(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    event_id: str,
) -> sqlite3.Row:
    now = utc_now()
    conn.execute(
        """
        UPDATE split_operations
        SET record_event_id = ?, updated_at = ?
        WHERE operation_id = ?
        """,
        (event_id, now, operation_id),
    )
    return conn.execute(
        "SELECT * FROM split_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()


def list_split_operations(
    conn: sqlite3.Connection,
    *,
    workspace_id: str | None = None,
    target_kind: str | None = None,
    target_id: str | None = None,
    source_kind: str | None = None,
    source_id: str | None = None,
    status: str | None = None,
) -> list[SplitOperation]:
    where: list[str] = []
    params: list[Any] = []
    if workspace_id is not None:
        where.append("workspace_id = ?")
        params.append(workspace_id)
    if target_kind is not None:
        where.append("target_kind = ?")
        params.append(target_kind)
    if target_id is not None:
        where.append("target_id = ?")
        params.append(target_id)
    if source_kind is not None:
        where.append("source_kind = ?")
        params.append(source_kind)
    if source_id is not None:
        where.append("source_id = ?")
        params.append(source_id)
    if status is not None:
        where.append("status = ?")
        params.append(status)
    sql = "SELECT * FROM split_operations"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at"
    rows = conn.execute(sql, params).fetchall()
    return [SplitOperation.from_row(row) for row in rows]


def append_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    actor: str,
    workspace_id: str | None = None,
    target: str | None = None,
    task_id: str | None = None,
    causation_id: str | None = None,
    idempotency_key: str | None = None,
    payload: dict[str, Any] | None = None,
    commit: bool = True,
) -> AppendEventResult:
    event_id = str(uuid.uuid4())
    key = idempotency_key or event_id
    now = utc_now()
    try:
        conn.execute(
            """
            INSERT INTO events (
              id, workspace_id, event_type, actor, target, task_id, causation_id,
              idempotency_key, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                workspace_id,
                event_type,
                actor,
                target,
                task_id,
                causation_id,
                key,
                _json_dumps(payload),
                now,
            ),
        )
        if commit:
            conn.commit()
        return AppendEventResult(get_event(conn, event_id), True)
    except sqlite3.IntegrityError:
        row = conn.execute("SELECT * FROM events WHERE idempotency_key = ?", (key,)).fetchone()
        if row is None:
            raise
        return AppendEventResult(row, False)


def get_event(conn: sqlite3.Connection, event_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    if row is None:
        raise KeyError(event_id)
    return row


def list_events(conn: sqlite3.Connection, workspace_id: str | None = None) -> Iterable[sqlite3.Row]:
    if workspace_id:
        return conn.execute(
            "SELECT rowid, * FROM events WHERE workspace_id = ? ORDER BY rowid",
            (workspace_id,),
        ).fetchall()
    return conn.execute("SELECT rowid, * FROM events ORDER BY rowid").fetchall()


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for key in (
        "payload_json",
        "result_json",
        "progress_json",
        "capabilities_json",
        "env_json",
        "context_json",
        "decision_json",
    ):
        if key in result and result[key] is not None:
            result[key.removesuffix("_json")] = json.loads(result.pop(key))
    if "platform" in result and "destination" in result:
        result["delivery_type"] = (
            "dry_run" if result["platform"] == "stdout" and result["destination"] == "local" else "live"
        )
    return result


_PAYLOAD_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_payload_key(payload_key: str) -> str:
    """Payload keys are interpolated into a json_extract path; they must be
    plain identifiers so no user-controlled string can reach the SQL text."""
    if not payload_key or not _PAYLOAD_KEY_RE.match(payload_key):
        raise ValueError(f"invalid payload key: {payload_key!r}")
    return payload_key


def find_events(
    conn: sqlite3.Connection,
    *,
    event_type: str | None = None,
    workspace_id: str | None = None,
    task_id: str | None = None,
    payload_key: str | None = None,
    payload_value: Any | None = None,
) -> list[sqlite3.Row]:
    """Find events matching filters, oldest-first (by rowid).

    When ``payload_key``/``payload_value`` are given, the JSON payload field
    ``payload_key`` is matched for equality via ``json_extract``. The key is
    validated to a plain identifier; the value is parameterized.
    """
    clauses: list[str] = []
    params: list[Any] = []
    if event_type is not None:
        clauses.append("event_type = ?")
        params.append(event_type)
    if workspace_id is not None:
        clauses.append("workspace_id = ?")
        params.append(workspace_id)
    if task_id is not None:
        clauses.append("task_id = ?")
        params.append(task_id)
    if payload_key is not None:
        _validate_payload_key(payload_key)
        clauses.append(f"json_extract(payload_json, '$.{payload_key}') = ?")
        params.append(json.dumps(payload_value) if not isinstance(payload_value, str) else payload_value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(
        f"SELECT rowid, * FROM events {where} ORDER BY rowid",
        params,
    ).fetchall()


def latest_event(
    conn: sqlite3.Connection,
    *,
    event_type: str | None = None,
    workspace_id: str | None = None,
    task_id: str | None = None,
    payload_key: str | None = None,
    payload_value: Any | None = None,
) -> sqlite3.Row | None:
    """Return the most recent matching event (rowid DESC) or None."""
    rows = find_events(
        conn,
        event_type=event_type,
        workspace_id=workspace_id,
        task_id=task_id,
        payload_key=payload_key,
        payload_value=payload_value,
    )
    return rows[-1] if rows else None


# ---------------------------------------------------------------------------
# Channel binding authority: (platform, channel_id) -> workspace_id
#
# Coordinate is the sole persistent authority. Only the active row is stored;
# actor/reason/history live in events. Mutations are event-first and commit the
# audit event and the active-row change in the same SAVEPOINT.
# ---------------------------------------------------------------------------

_CHANNEL_BINDING_PLATFORMS = ("discord", "kook")
_CHANNEL_BINDING_MAX_CODE_POINTS = 128

_CHANNEL_BINDING_BOUND_EVENT = "channel.binding.bound"
_CHANNEL_BINDING_RELEASED_EVENT = "channel.binding.released"


@dataclass(frozen=True)
class ChannelBinding:
    platform: str
    channel_id: str
    workspace_id: str
    bound_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ChannelBinding":
        return cls(
            platform=row["platform"],
            channel_id=row["channel_id"],
            workspace_id=row["workspace_id"],
            bound_at=row["bound_at"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "channel_id": self.channel_id,
            "workspace_id": self.workspace_id,
            "bound_at": self.bound_at,
        }


def _normalize_channel_platform(value: Any) -> str:
    """Return the canonical platform or raise ValueError (fail loud)."""
    platform = str(value).strip().lower()
    if platform not in _CHANNEL_BINDING_PLATFORMS:
        raise ValueError(f"invalid platform: {value!r}")
    return platform


def _normalize_channel_id(value: Any) -> str:
    """Return the canonical channel id or raise ValueError (fail loud).

    The id is an opaque platform-SDK identifier: we only enforce the outer
    bounds (non-empty, no surrounding whitespace, no control characters,
    bounded length). We deliberately do not invent a narrower per-platform
    regex.
    """
    channel_id = str(value)
    if not channel_id:
        raise ValueError("channel_id is required")
    if channel_id != channel_id.strip():
        raise ValueError(f"channel_id must not have leading/trailing whitespace: {value!r}")
    if len(channel_id) > _CHANNEL_BINDING_MAX_CODE_POINTS:
        raise ValueError(
            f"channel_id exceeds {_CHANNEL_BINDING_MAX_CODE_POINTS} code points: {value!r}"
        )
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in channel_id):
        raise ValueError(f"channel_id must not contain control characters: {value!r}")
    return channel_id


def _canonical_channel_target(platform: str, channel_id: str) -> str:
    return f"{platform}:{channel_id}"


def _require_non_empty(value: Any, field: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _get_channel_binding_row(
    conn: sqlite3.Connection,
    *,
    platform: str,
    channel_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM channel_bindings WHERE platform = ? AND channel_id = ?",
        (platform, channel_id),
    ).fetchone()


def resolve_channel_workspace(
    conn: sqlite3.Connection,
    *,
    platform: Any,
    channel_id: Any,
) -> ChannelBinding | None:
    """Return the active binding for a canonical channel, or None when unbound.

    Invalid keys fail loud (ValueError); unbound is a normal None result and is
    never used to mask an invalid key.
    """
    canonical_platform = _normalize_channel_platform(platform)
    canonical_channel_id = _normalize_channel_id(channel_id)
    row = _get_channel_binding_row(
        conn, platform=canonical_platform, channel_id=canonical_channel_id
    )
    return ChannelBinding.from_row(row) if row else None


def list_channel_bindings(
    conn: sqlite3.Connection,
    *,
    platform: Any | None = None,
    workspace_id: Any | None = None,
) -> list[ChannelBinding]:
    """List active bindings, optionally filtered. Filter keys fail loud."""
    where: list[str] = []
    params: list[str] = []
    if platform is not None:
        where.append("platform = ?")
        params.append(_normalize_channel_platform(platform))
    if workspace_id is not None:
        ws = _require_non_empty(workspace_id, "workspace_id")
        where.append("workspace_id = ?")
        params.append(ws)
    sql = "SELECT * FROM channel_bindings"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY platform, channel_id"
    rows = conn.execute(sql, params).fetchall()
    return [ChannelBinding.from_row(row) for row in rows]


def _check_channel_binding_idempotency(
    conn: sqlite3.Connection,
    *,
    idempotency_key: str,
    event_type: str,
    workspace_id: str,
    target: str,
    payload: dict[str, Any],
) -> sqlite3.Row | None:
    """Validate an idempotency key against any prior event.

    Returns the prior event row on exact replay; returns None when the key is
    fresh; raises ValueError on cross-operation/cross-payload reuse.
    """
    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?",
        (idempotency_key,),
    ).fetchone()
    if existing is None:
        return None
    _assert_channel_binding_event_matches(
        existing,
        idempotency_key=idempotency_key,
        event_type=event_type,
        workspace_id=workspace_id,
        target=target,
        payload=payload,
    )
    return existing


def _assert_channel_binding_event_matches(
    event_row: sqlite3.Row,
    *,
    idempotency_key: str,
    event_type: str,
    workspace_id: str,
    target: str,
    payload: dict[str, Any],
) -> None:
    """Fail closed unless an existing event exactly matches this mutation.

    Used both for pre-mutation idempotency checks and to re-validate an event
    that ``append_event`` returns with ``created=False`` after a concurrent
    insert won the unique-key race.
    """
    exact = (
        event_row["event_type"] == event_type
        and event_row["workspace_id"] == workspace_id
        and event_row["target"] == target
        and event_row["payload_json"] == _json_dumps(payload)
    )
    if not exact:
        raise ValueError(
            f"idempotency key conflict: {idempotency_key!r} was already used with different parameters"
        )


def bind_channel_workspace(
    conn: sqlite3.Connection,
    *,
    platform: Any,
    channel_id: Any,
    workspace_id: Any,
    actor: Any,
    reason: Any,
    idempotency_key: Any,
) -> dict[str, Any]:
    """Bind a canonical channel to a workspace, event-first and atomic.

    Returns a receipt dict with a ``status`` of ``bound``, ``already_bound`` or
    ``replayed``. Raises ValueError on invalid key, unknown workspace,
    cross-workspace conflict, or idempotency reuse.
    """
    canonical_platform = _normalize_channel_platform(platform)
    canonical_channel_id = _normalize_channel_id(channel_id)
    ws = _require_non_empty(workspace_id, "workspace_id")
    actor_text = _require_non_empty(actor, "actor")
    reason_text = _require_non_empty(reason, "reason")
    key = _require_non_empty(idempotency_key, "idempotency_key")

    if get_workspace(conn, ws) is None:
        raise ValueError(f"unknown workspace: {ws}")

    target = _canonical_channel_target(canonical_platform, canonical_channel_id)
    payload = {
        "platform": canonical_platform,
        "channel_id": canonical_channel_id,
        "workspace_id": ws,
        "reason": reason_text,
    }

    receipt = {
        "platform": canonical_platform,
        "channel_id": canonical_channel_id,
        "workspace_id": ws,
        "target": target,
        "idempotency_key": key,
    }

    prior = _check_channel_binding_idempotency(
        conn,
        idempotency_key=key,
        event_type=_CHANNEL_BINDING_BOUND_EVENT,
        workspace_id=ws,
        target=target,
        payload=payload,
    )
    if prior is not None:
        return {**receipt, "status": "replayed", "event_id": prior["id"]}

    existing_row = _get_channel_binding_row(
        conn, platform=canonical_platform, channel_id=canonical_channel_id
    )
    if existing_row is not None:
        if existing_row["workspace_id"] == ws:
            return {**receipt, "status": "already_bound", "event_id": None}
        raise ValueError(
            f"channel {target!r} is already bound to workspace "
            f"{existing_row['workspace_id']!r}; release it before rebinding to {ws!r}"
        )

    conn.execute("SAVEPOINT channel_binding_bind")
    try:
        event = append_event(
            conn,
            event_type=_CHANNEL_BINDING_BOUND_EVENT,
            actor=actor_text,
            workspace_id=ws,
            target=target,
            idempotency_key=key,
            payload=payload,
            commit=False,
        )
        if not event.created:
            # A concurrent mutation won the idempotency-key race. Re-validate the
            # winning event before deciding: exact replay returns the receipt
            # without re-mutating; anything else fails closed. We never treat the
            # pre-existing event as our own and continue to INSERT.
            _assert_channel_binding_event_matches(
                event.row,
                idempotency_key=key,
                event_type=_CHANNEL_BINDING_BOUND_EVENT,
                workspace_id=ws,
                target=target,
                payload=payload,
            )
            conn.execute("RELEASE channel_binding_bind")
            conn.commit()
            return {**receipt, "status": "replayed", "event_id": event.row["id"]}
        conn.execute(
            """
            INSERT INTO channel_bindings (platform, channel_id, workspace_id, bound_at)
            VALUES (?, ?, ?, ?)
            """,
            (canonical_platform, canonical_channel_id, ws, utc_now()),
        )
    except Exception:
        conn.execute("ROLLBACK TO channel_binding_bind")
        conn.execute("RELEASE channel_binding_bind")
        raise
    conn.execute("RELEASE channel_binding_bind")
    conn.commit()
    return {**receipt, "status": "bound", "event_id": event.row["id"]}


def release_channel_workspace(
    conn: sqlite3.Connection,
    *,
    platform: Any,
    channel_id: Any,
    expected_workspace_id: Any,
    actor: Any,
    reason: Any,
    idempotency_key: Any,
) -> dict[str, Any]:
    """Release a channel binding, event-first and atomic.

    Returns a receipt dict with a ``status`` of ``released``,
    ``already_unbound`` or ``replayed``. Replaying a historical release only
    returns the receipt and never deletes a later rebind's active row. Raises
    ValueError on invalid key, unknown/expected workspace mismatch, or
    idempotency reuse.
    """
    canonical_platform = _normalize_channel_platform(platform)
    canonical_channel_id = _normalize_channel_id(channel_id)
    expected_ws = _require_non_empty(expected_workspace_id, "expected_workspace_id")
    actor_text = _require_non_empty(actor, "actor")
    reason_text = _require_non_empty(reason, "reason")
    key = _require_non_empty(idempotency_key, "idempotency_key")

    if get_workspace(conn, expected_ws) is None:
        raise ValueError(f"unknown workspace: {expected_ws}")

    target = _canonical_channel_target(canonical_platform, canonical_channel_id)
    payload = {
        "platform": canonical_platform,
        "channel_id": canonical_channel_id,
        "workspace_id": expected_ws,
        "reason": reason_text,
    }

    receipt = {
        "platform": canonical_platform,
        "channel_id": canonical_channel_id,
        "workspace_id": expected_ws,
        "target": target,
        "idempotency_key": key,
    }

    prior = _check_channel_binding_idempotency(
        conn,
        idempotency_key=key,
        event_type=_CHANNEL_BINDING_RELEASED_EVENT,
        workspace_id=expected_ws,
        target=target,
        payload=payload,
    )
    if prior is not None:
        # Exact replay of a historical release: return the receipt only. Do NOT
        # delete the active row, which may belong to a later rebind.
        return {**receipt, "status": "replayed", "event_id": prior["id"]}

    existing_row = _get_channel_binding_row(
        conn, platform=canonical_platform, channel_id=canonical_channel_id
    )
    if existing_row is None:
        return {**receipt, "status": "already_unbound", "event_id": None}
    if existing_row["workspace_id"] != expected_ws:
        raise ValueError(
            f"channel {target!r} is bound to workspace "
            f"{existing_row['workspace_id']!r}, not expected workspace {expected_ws!r}"
        )

    conn.execute("SAVEPOINT channel_binding_release")
    try:
        event = append_event(
            conn,
            event_type=_CHANNEL_BINDING_RELEASED_EVENT,
            actor=actor_text,
            workspace_id=expected_ws,
            target=target,
            idempotency_key=key,
            payload=payload,
            commit=False,
        )
        if not event.created:
            # A concurrent mutation won the idempotency-key race. Re-validate the
            # winning event: an exact replay of a historical release returns the
            # receipt only and must NOT delete a later rebind's active row;
            # anything else fails closed.
            _assert_channel_binding_event_matches(
                event.row,
                idempotency_key=key,
                event_type=_CHANNEL_BINDING_RELEASED_EVENT,
                workspace_id=expected_ws,
                target=target,
                payload=payload,
            )
            conn.execute("RELEASE channel_binding_release")
            conn.commit()
            return {**receipt, "status": "replayed", "event_id": event.row["id"]}
        # Compare-and-delete: only remove the row if it is still bound to the
        # expected workspace, and require exactly one row. If a concurrent
        # release/rebind changed or removed the row, this deletes zero rows and
        # we roll back the whole event + mutation and fail closed.
        cursor = conn.execute(
            "DELETE FROM channel_bindings "
            "WHERE platform = ? AND channel_id = ? AND workspace_id = ?",
            (canonical_platform, canonical_channel_id, expected_ws),
        )
        if cursor.rowcount != 1:
            raise ValueError(
                f"channel {target!r} active row changed concurrently; "
                f"release of expected workspace {expected_ws!r} aborted"
            )
    except Exception:
        conn.execute("ROLLBACK TO channel_binding_release")
        conn.execute("RELEASE channel_binding_release")
        raise
    conn.execute("RELEASE channel_binding_release")
    conn.commit()
    return {**receipt, "status": "released", "event_id": event.row["id"]}
