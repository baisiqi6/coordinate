"""Job CRUD primitives extracted from coordinate.db.

This module intentionally does NOT import coordinate.db so that coordinate.db
can statically re-export these symbols without creating an import cycle.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from coordinate.db_support import _absolute_path, _json_dumps, utc_now


def create_job(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str | None,
    runner_profile_id: str,
    assigned_agent: str | None = None,
    prompt_path: str | None = None,
    branch: str | None = None,
    worktree_path: str | None = None,
    terminal_session_id: str | None = None,
    logs_path: str | None = None,
    timeout_seconds: int | None = None,
    payload: dict[str, Any] | None = None,
    job_id: str | None = None,
    commit: bool = True,
) -> sqlite3.Row:
    workspace_row = conn.execute(
        "SELECT path FROM workspaces WHERE id = ?", (workspace_id,)
    ).fetchone()
    if workspace_row is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    workspace_path = workspace_row["path"]

    runner_row = conn.execute(
        "SELECT id FROM runner_profiles WHERE id = ?", (runner_profile_id,)
    ).fetchone()
    if runner_row is None:
        raise ValueError(f"unknown runner profile: {runner_profile_id}")

    now = utc_now()
    jid = job_id or str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO jobs (
          id, workspace_id, task_id, runner_profile_id, assigned_agent, status,
          prompt_path, branch, worktree_path, terminal_session_id, logs_path,
          attempt_count, timeout_seconds, payload_json, result_json, created_at,
          started_at, completed_at, last_activity_at, progress_json, recoverable,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            jid,
            workspace_id,
            task_id,
            runner_profile_id,
            assigned_agent or runner_profile_id,
            "pending",
            _absolute_path(prompt_path, base=workspace_path) if prompt_path else None,
            branch,
            _absolute_path(worktree_path, base=workspace_path) if worktree_path else None,
            terminal_session_id,
            _absolute_path(logs_path, base=workspace_path) if logs_path else None,
            0,
            timeout_seconds,
            _json_dumps(payload),
            None,
            now,
            None,
            None,
            None,
            None,
            0,
            now,
        ),
    )
    if commit:
        conn.commit()
    return get_job(conn, jid)


def get_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise KeyError(job_id)
    return row


def list_jobs(
    conn: sqlite3.Connection,
    *,
    workspace_id: str | None = None,
    status: str | None = None,
) -> list[sqlite3.Row]:
    where: list[str] = []
    params: list[str] = []
    if workspace_id:
        where.append("workspace_id = ?")
        params.append(workspace_id)
    if status:
        where.append("status = ?")
        params.append(status)
    sql = "SELECT * FROM jobs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at, id"
    return conn.execute(sql, params).fetchall()


def mark_job_started(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    logs_path: str,
) -> sqlite3.Row:
    now = utc_now()
    job = get_job(conn, job_id)
    conn.execute(
        """
        UPDATE jobs
        SET status = ?, attempt_count = ?, logs_path = ?, started_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            "running",
            int(job["attempt_count"]) + 1,
            _absolute_path(logs_path),
            now,
            now,
            job_id,
        ),
    )
    conn.commit()
    return get_job(conn, job_id)


def mark_job_completed(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    status: str,
    result: dict[str, Any],
) -> sqlite3.Row:
    if status not in {"done", "failed"}:
        raise ValueError("job completion status must be done or failed")
    now = utc_now()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = ?, result_json = ?, completed_at = ?, updated_at = ?
        WHERE id = ? AND status = ?
        """,
        (status, _json_dumps(result), now, now, job_id, "running"),
    )
    conn.commit()
    if cursor.rowcount == 0:
        job = get_job(conn, job_id)
        if job["status"] == "cancelled":
            return job
        raise ValueError(f"job {job_id} is {job['status']}; only running jobs can be completed")
    return get_job(conn, job_id)


def mark_job_cancelled(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    reason: str | None = None,
) -> sqlite3.Row:
    job = get_job(conn, job_id)
    if job["status"] == "cancelled":
        return job
    if job["status"] in {"done", "failed"}:
        raise ValueError(f"job {job_id} is {job['status']}; completed jobs cannot be cancelled")
    result = json.loads(job["result_json"]) if job["result_json"] else {}
    if not isinstance(result, dict):
        result = {}
    result.update(
        {
            "cancelled": True,
            "cancel_reason": reason,
            "previous_status": job["status"],
        }
    )
    now = utc_now()
    conn.execute(
        """
        UPDATE jobs
        SET status = ?, result_json = ?, completed_at = ?, updated_at = ?
        WHERE id = ?
        """,
        ("cancelled", _json_dumps(result), now, now, job_id),
    )
    conn.commit()
    return get_job(conn, job_id)
