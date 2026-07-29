from __future__ import annotations

import json
import os
import shlex
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import (
    append_event,
    create_job,
    get_job,
    get_runner_profile,
    get_workspace,
    list_jobs,
    mark_job_cancelled,
    mark_job_completed,
    mark_job_started,
    row_to_dict,
)
from .executor_routing import is_routed_job


class JobError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunJobResult:
    job: dict[str, Any]
    log_path: str

    def to_dict(self) -> dict[str, Any]:
        return {"job": self.job, "log_path": self.log_path}


@dataclass(frozen=True)
class CancelJobResult:
    job: dict[str, Any]
    event: dict[str, Any]
    event_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": self.job,
            "event": self.event,
            "event_created": self.event_created,
        }


@dataclass(frozen=True)
class RetryJobResult:
    source_job: dict[str, Any]
    retry_job: dict[str, Any]
    event: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_job": self.source_job,
            "retry_job": self.retry_job,
            "event": self.event,
        }


@dataclass(frozen=True)
class PumpJobsResult:
    processed: int
    done: int
    failed: int
    errors: int
    jobs: list[dict[str, Any]]
    error_details: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "processed": self.processed,
            "done": self.done,
            "failed": self.failed,
            "errors": self.errors,
            "jobs": self.jobs,
            "error_details": self.error_details,
        }


class _QuotedFormat(dict):
    def __missing__(self, key: str) -> str:
        return "''"


def run_job(conn: sqlite3.Connection, job_id: str) -> RunJobResult:
    job = get_job(conn, job_id)
    if job["status"] != "pending":
        raise JobError(f"job {job_id} is {job['status']}; only pending jobs can be run")

    workspace = get_workspace(conn, job["workspace_id"])
    if workspace is None:
        raise JobError(f"unknown workspace for job {job_id}: {job['workspace_id']}")
    runner = get_runner_profile(conn, job["runner_profile_id"])
    if runner is None:
        raise JobError(f"unknown runner profile for job {job_id}: {job['runner_profile_id']}")
    if runner.runner_type != "generic_subprocess":
        raise JobError(
            f"runner profile {runner.id} has type {runner.runner_type}; "
            "only generic_subprocess is supported by job run"
        )

    cwd = _resolve_cwd(workspace.path, job["worktree_path"], runner.working_directory_strategy)
    job_payload = _job_payload(job)
    log_path = _resolve_log_path(workspace.path, job_id, job["logs_path"])
    result_path = _resolve_result_path(workspace.path, job_id, job_payload)
    _prepare_result_path(result_path)
    command = _render_command(runner.command, workspace.path, job, log_path, result_path)
    env = _build_env(runner.env, workspace.path, job, log_path, result_path)

    mark_job_started(conn, job_id=job_id, logs_path=str(log_path))
    started_event = append_event(
        conn,
        workspace_id=workspace.id,
        event_type="job.started",
        actor="runner",
        task_id=job["task_id"],
        payload={"job_id": job_id, "runner_profile_id": runner.id, "command": runner.command},
    )

    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            shell=True,
            text=True,
            capture_output=True,
            timeout=job["timeout_seconds"],
            check=False,
        )
        result = {
            "exit_code": completed.returncode,
            "timeout": False,
            "event_id": started_event.row["id"],
            "logs_path": str(log_path),
            "result_path": str(result_path),
        }
        _write_log(log_path, command, cwd, completed.returncode, completed.stdout, completed.stderr)
        final_status = "done" if completed.returncode == 0 else "failed"
    except subprocess.TimeoutExpired as exc:
        stdout = _coerce_output(exc.stdout)
        stderr = _coerce_output(exc.stderr)
        result = {
            "exit_code": None,
            "timeout": True,
            "timeout_seconds": job["timeout_seconds"],
            "failure_reason": "timeout",
            "event_id": started_event.row["id"],
            "logs_path": str(log_path),
            "result_path": str(result_path),
        }
        _write_log(log_path, command, cwd, None, stdout, stderr, timed_out=True)
        final_status = "failed"

    response, response_error = _read_agent_response(result_path)
    if response_error:
        result["agent_response_error"] = response_error
        final_status = "failed"
    elif response:
        result.update(response)
        if response.get("agent_status") in {"blocked", "failed", "declined"}:
            final_status = "failed"

    final_job = mark_job_completed(conn, job_id=job_id, status=final_status, result=result)
    if final_job["status"] == "cancelled":
        return RunJobResult(row_to_dict(final_job), str(log_path))
    append_event(
        conn,
        workspace_id=workspace.id,
        event_type="job.completed" if final_status == "done" else "job.failed",
        actor="runner",
        task_id=job["task_id"],
        payload={"job_id": job_id, "status": final_status, **result},
    )
    return RunJobResult(row_to_dict(final_job), str(log_path))


def cancel_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    reason: str | None = None,
    actor: str = "operator",
) -> CancelJobResult:
    job = get_job(conn, job_id)
    if job["status"] in {"done", "failed"}:
        raise JobError(f"job {job_id} is {job['status']}; completed jobs cannot be cancelled")

    cancelled = mark_job_cancelled(conn, job_id=job_id, reason=reason)
    event = append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type="job.cancelled",
        actor=actor,
        task_id=job["task_id"],
        idempotency_key=f"job:{job_id}:cancelled",
        payload={
            "job_id": job_id,
            "status": "cancelled",
            "previous_status": job["status"],
            "reason": reason,
        },
    )
    return CancelJobResult(
        job=row_to_dict(cancelled),
        event=row_to_dict(event.row),
        event_created=event.created,
    )


def retry_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    reason: str | None = None,
    actor: str = "operator",
) -> RetryJobResult:
    source = get_job(conn, job_id)
    if source["status"] not in {"failed", "cancelled"}:
        raise JobError(
            f"job {job_id} is {source['status']}; only failed or cancelled jobs can be retried"
        )

    payload = dict(_job_payload(source))
    if is_routed_job(payload):
        raise JobError("routed_runtime_retry_requires_explicit_resubmission")
    payload.pop("result_path", None)
    payload["retry_of_job_id"] = job_id
    if reason:
        payload["retry_reason"] = reason

    retry = create_job(
        conn,
        workspace_id=source["workspace_id"],
        task_id=source["task_id"],
        runner_profile_id=source["runner_profile_id"],
        prompt_path=source["prompt_path"],
        branch=source["branch"],
        worktree_path=source["worktree_path"],
        timeout_seconds=source["timeout_seconds"],
        payload=payload,
    )
    event = append_event(
        conn,
        workspace_id=source["workspace_id"],
        event_type="job.retry_requested",
        actor=actor,
        task_id=source["task_id"],
        payload={
            "source_job_id": job_id,
            "retry_job_id": retry["id"],
            "source_status": source["status"],
            "runner_profile_id": source["runner_profile_id"],
            "reason": reason,
        },
    )
    return RetryJobResult(
        source_job=row_to_dict(source),
        retry_job=row_to_dict(retry),
        event=row_to_dict(event.row),
    )


def pump_jobs(
    conn: sqlite3.Connection,
    *,
    workspace_id: str | None = None,
    limit: int = 10,
) -> PumpJobsResult:
    if limit < 1:
        raise JobError("limit must be at least 1")

    pending = list_jobs(conn, workspace_id=workspace_id, status="pending")[:limit]
    done_count = 0
    failed_count = 0
    errors: list[dict[str, Any]] = []
    jobs: list[dict[str, Any]] = []

    for job in pending:
        try:
            result = run_job(conn, job["id"])
        except JobError as exc:
            errors.append({"job_id": job["id"], "error": str(exc)})
            continue

        jobs.append(result.to_dict())
        status = result.job["status"]
        if status == "done":
            done_count += 1
        elif status == "failed":
            failed_count += 1

    return PumpJobsResult(
        processed=len(pending),
        done=done_count,
        failed=failed_count,
        errors=len(errors),
        jobs=jobs,
        error_details=errors,
    )


def _resolve_cwd(workspace_path: str, worktree_path: str | None, strategy: str) -> Path:
    if strategy == "git_worktree":
        if not worktree_path:
            raise JobError("runner uses git_worktree strategy but job has no worktree_path")
        cwd = Path(worktree_path)
    else:
        cwd = Path(worktree_path) if worktree_path else Path(workspace_path)
    if not cwd.exists():
        raise JobError(f"job working directory does not exist: {cwd}")
    return cwd


def _resolve_log_path(workspace_path: str, job_id: str, logs_path: str | None) -> Path:
    if logs_path:
        path = Path(logs_path)
    else:
        path = Path(workspace_path) / ".coordinator" / "logs" / "jobs" / f"{job_id}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_result_path(workspace_path: str, job_id: str, payload: dict[str, Any]) -> Path:
    configured = payload.get("result_path")
    if configured:
        path = Path(str(configured)).expanduser()
        if not path.is_absolute():
            path = Path(workspace_path) / path
    else:
        path = Path(workspace_path) / ".coordinator" / "results" / "jobs" / f"{job_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _prepare_result_path(result_path: Path) -> None:
    if not result_path.exists():
        return
    if result_path.is_dir():
        raise JobError(f"job result_path points to a directory: {result_path}")
    result_path.unlink()


def _render_command(
    command: str,
    workspace_path: str,
    job: sqlite3.Row,
    log_path: Path,
    result_path: Path,
) -> str:
    values = _placeholder_values(workspace_path, job, log_path, result_path)
    return command.format_map(_QuotedFormat(values))


def _build_env(
    env: dict[str, Any],
    workspace_path: str,
    job: sqlite3.Row,
    log_path: Path,
    result_path: Path,
) -> dict[str, str]:
    merged = os.environ.copy()
    merged.update({key: str(value) for key, value in env.items()})
    raw_values = {
        key.upper(): "" if value is None else str(value)
        for key, value in _raw_placeholder_values(workspace_path, job, log_path, result_path).items()
    }
    merged.update({f"COORDINATOR_{key}": value for key, value in raw_values.items()})
    return merged


def _placeholder_values(
    workspace_path: str,
    job: sqlite3.Row,
    log_path: Path,
    result_path: Path,
) -> dict[str, str]:
    return {
        key: shlex.quote("" if value is None else str(value))
        for key, value in _raw_placeholder_values(workspace_path, job, log_path, result_path).items()
    }


def _raw_placeholder_values(
    workspace_path: str,
    job: sqlite3.Row,
    log_path: Path,
    result_path: Path,
) -> dict[str, Any]:
    return {
        "job_id": job["id"],
        "workspace_id": job["workspace_id"],
        "workspace_path": workspace_path,
        "task_id": job["task_id"],
        "prompt_path": job["prompt_path"],
        "branch": job["branch"],
        "worktree_path": job["worktree_path"],
        "logs_path": str(log_path),
        "result_path": str(result_path),
    }


def _write_log(
    log_path: Path,
    command: str,
    cwd: Path,
    exit_code: int | None,
    stdout: str,
    stderr: str,
    *,
    timed_out: bool = False,
) -> None:
    log_path.write_text(
        "\n".join(
            [
                f"command: {command}",
                f"cwd: {cwd}",
                f"exit_code: {exit_code}",
                f"timed_out: {timed_out}",
                "",
                "=== STDOUT ===",
                stdout,
                "",
                "=== STDERR ===",
                stderr,
                "",
            ]
        ),
        encoding="utf-8",
    )


def _coerce_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _job_payload(job: sqlite3.Row) -> dict[str, Any]:
    value = json.loads(job["payload_json"])
    if not isinstance(value, dict):
        raise JobError(f"job {job['id']} payload_json must decode to an object")
    return value


def _read_agent_response(result_path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not result_path.exists():
        return None, None
    raw = result_path.read_text(encoding="utf-8").strip()
    if not raw:
        return None, None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid AgentResponse JSON at {result_path}: {exc}"
    if not isinstance(value, dict):
        return None, f"AgentResponse at {result_path} must be a JSON object"
    try:
        return _normalize_agent_response(value), None
    except JobError as exc:
        return None, str(exc)


def _normalize_agent_response(value: dict[str, Any]) -> dict[str, Any]:
    response: dict[str, Any] = {}
    if value.get("status") is not None:
        response["agent_status"] = str(value["status"])
    if value.get("summary") is not None:
        response["summary"] = str(value["summary"])
    if value.get("artifact_paths") is not None:
        response["artifact_paths"] = _string_list(value["artifact_paths"], field="artifact_paths")
    if value.get("branch") is not None:
        response["branch"] = str(value["branch"])
    if value.get("commit") is not None:
        response["commit"] = str(value["commit"])
    if value.get("pr") is not None:
        response["pr"] = str(value["pr"])
    if value.get("logs_path") is not None:
        response["agent_logs_path"] = str(value["logs_path"])
    return response


def _string_list(value: Any, *, field: str) -> list[str]:
    if not isinstance(value, list):
        raise JobError(f"AgentResponse field {field} must be a list")
    return [str(item) for item in value]
