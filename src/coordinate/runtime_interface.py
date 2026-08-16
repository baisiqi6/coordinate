"""Bounded runtime data-plane facade shared by the MCP (R1) and HTTP (R2A) adapters.

This module is the single error-sanitization boundary between every remote
adapter and the existing Coordinate domain/application core. It deliberately
does NOT:

- open a connection on the server startup thread; every call creates and closes
  its own SQLite connection inside the calling thread;
- re-implement domain validation or transaction boundaries;
- call ``print``/``print_json``, the CLI ``main()``, shell, SSH or subprocesses;
- hold process-level mutable business state.

It carries exactly the seven use cases with real consumers (§3 of the R2A
plan): channel resolve, request submit, job get, normal job claim, progress
checkpoint, terminal report and managed lease renew. Every public method
returns the stable envelope::

    {"ok": true, "data": {...}, "error": null}
    {"ok": false, "data": null, "error": {"code": "...", "message": "..."}}

``actor`` is fixed from adapter startup configuration; callers can never
inject it through request bodies.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Callable

from .db import get_workspace, resolve_channel_workspace, row_to_dict
from .execution_leases import LEASE_DEFAULT_TTL_SECONDS
from .executor_routing import ExecutorRoutingError, build_routing_request
from .job_repository import get_job
from .runtime import (
    claim_job,
    record_job_progress,
    report_job_result,
    submit_request,
)
from .runtime_lease import renew_managed_lease

logger = logging.getLogger("coordinate.runtime_interface")

# Raw routing builder fields accepted from a caller. ``routing_request_id``
# and contract/policy versions are computed by ``build_routing_request()`` on
# the server side, never accepted from the caller.
_ROUTING_BUILDER_FIELDS = frozenset(
    {
        "required_capabilities",
        "executor_definition_id",
        "preferred_host_id",
        "operator_override_agent_id",
        "operator_override_reason",
    }
)

# Static, bounded wire messages. The facade is the only error-sanitization
# boundary: caller/domain values, paths, field names and long inputs are never
# echoed back onto the wire; the full diagnostic for unexpected exceptions goes
# to stderr only.
MESSAGE_UNKNOWN_WORKSPACE = "unknown workspace"
MESSAGE_UNKNOWN_AGENT = "unknown agent"
MESSAGE_TASK_MIRROR_NOT_FOUND = "task mirror not found"
MESSAGE_HOST_PROFILE_NOT_FOUND = "host profile not found"
MESSAGE_JOB_NOT_FOUND = "job not found"
MESSAGE_INVALID_REQUEST = "invalid request"
MESSAGE_INVALID_ROUTING_REQUEST = "invalid routing request"
MESSAGE_REPLAY_CONFLICT = "request replay conflict"
MESSAGE_CONFLICT = "state conflict"
MESSAGE_UNAVAILABLE = "coordinate storage or filesystem is unavailable"
MESSAGE_INTERNAL = "internal error"

# Domain errors whose message signature marks them as idempotency replay
# conflicts (fail closed, never silently replaying a different payload).
# Covers both the plain ``request replay:`` prefix and the context-conflict
# signature raised in three places by runtime.py.
_CONFLICT_PREFIXES = ("request replay:", "request replay context conflict:")

# Domain error text signatures for known-missing resources, mapped to a static
# wire message. These signatures are stable domain contract text already
# asserted by existing suite tests.
_NOT_FOUND_MARKERS = (
    ("unknown workspace", MESSAGE_UNKNOWN_WORKSPACE),
    ("unknown agent", MESSAGE_UNKNOWN_AGENT),
    ("task mirror not found", MESSAGE_TASK_MIRROR_NOT_FOUND),
    ("has no host_id", MESSAGE_HOST_PROFILE_NOT_FOUND),
    ("has no host profile", MESSAGE_HOST_PROFILE_NOT_FOUND),
    ("disappeared", MESSAGE_JOB_NOT_FOUND),
)

# Domain error text signatures for state/authority conflicts mapped to the
# static ``conflict`` code (HTTP 409): stale/late/reclaimed attempts, identity
# mismatches, expired/lost/mismatched leases, CAS failures and non-running
# state reports. These are stable domain contract texts asserted by existing
# suite tests; ordering matters — not-found markers are checked first so true
# missing resources stay 404, then conflict markers, then the generic
# invalid-request fallback.
_CONFLICT_MARKERS = (
    # R1c: a late result for a timed_out+recoverable job whose current attempt
    # is managed (any lease row) is fail-closed authority conflict, not a 400
    # shape error: ``late-result rejected: current attempt is managed``.
    "late-result rejected",
    "rejected: cas failed",
    "is assigned to",
    "requires attempt_token",
    "stale attempt_token",
    "lease renewal rejected",
    "renewal must advance expires_at",
    "lease job_id mismatch",
    "lease attempt_token mismatch",
    "lease agent_id mismatch",
    "release job_id mismatch",
    "release attempt_token mismatch",
    "release agent_id mismatch",
    "expire job_id mismatch",
    "expire attempt_token mismatch",
    "expire agent_id mismatch",
    "conflicting lease replay",
    "only running jobs can",
    "has expired",
    "already released",
    "already expired",
)

# Prefix conflict markers: exact lease-row messages such as
# ``lease '…' not found`` / ``lease '…' has expired`` / ``lease '…' is …``.
# Prefix matching deliberately excludes integrity failures like ``stored lease
# resource snapshot is tampered``, which stay on the generic path instead of
# being misreported as a 409 state conflict.
_CONFLICT_PREFIXES_MARKERS = ("lease ",)


def ok_envelope(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data, "error": None}


def error_envelope(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "data": None, "error": {"code": code, "message": message}}


def classify_error(exc: Exception, *, logger: logging.Logger | None = None) -> tuple[str, str]:
    """Map a domain/storage failure to a bounded (code, static message) pair.

    Wire messages are drawn from a small static set: exception text, caller
    values, paths, field names and long inputs are never echoed back. Unknown
    exceptions are logged to stderr (via the R1/R2A facade logger) and reduced
    to ``internal`` on the wire.
    """
    if isinstance(exc, KeyError):
        return "not_found", MESSAGE_JOB_NOT_FOUND
    if isinstance(exc, (sqlite3.Error, OSError)):
        return "unavailable", MESSAGE_UNAVAILABLE
    message = str(exc) or exc.__class__.__name__
    lowered = message.lower()
    if any(lowered.startswith(prefix) for prefix in _CONFLICT_PREFIXES):
        return "conflict", MESSAGE_REPLAY_CONFLICT
    for marker, static_message in _NOT_FOUND_MARKERS:
        if marker in lowered:
            return "not_found", static_message
    for marker in _CONFLICT_MARKERS:
        if marker in lowered:
            return "conflict", MESSAGE_CONFLICT
    for prefix in _CONFLICT_PREFIXES_MARKERS:
        if lowered.startswith(prefix):
            return "conflict", MESSAGE_CONFLICT
    if isinstance(exc, ExecutorRoutingError):
        return "invalid_request", MESSAGE_INVALID_ROUTING_REQUEST
    if isinstance(exc, ValueError):
        return "invalid_request", MESSAGE_INVALID_REQUEST
    (logger or globals()["logger"]).exception("unexpected error in runtime interface call")
    return "internal", MESSAGE_INTERNAL


# Compatibility alias for the R1 facade: keep the historical logger name so
# existing ``assertLogs("coordinate.agent_interface")`` tests keep working.
def _classify_error(exc: Exception) -> tuple[str, str]:
    return classify_error(
        exc, logger=logging.getLogger("coordinate.agent_interface")
    )


@dataclass(frozen=True)
class RuntimeInterfaceConfig:
    """Startup configuration for the runtime data-plane facade."""

    db_path: str
    actor: str = "runtime-http"


class RuntimeInterface:
    """Bounded runtime data-plane facade over existing Coordinate domain functions."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], sqlite3.Connection],
        actor: str = "runtime-http",
    ) -> None:
        self._connection_factory = connection_factory
        self._actor = actor

    @property
    def connection_factory(self) -> Callable[[], sqlite3.Connection]:
        """Expose the factory so adapters can derive per-principal interfaces."""
        return self._connection_factory

    @classmethod
    def from_config(cls, config: RuntimeInterfaceConfig) -> "RuntimeInterface":
        from pathlib import Path

        from .db import connect

        db_path = Path(config.db_path).expanduser()

        def _factory() -> sqlite3.Connection:
            # HTTP data plane must never create or migrate the DB: atomic
            # existing-only open (SQLite URI mode=rw) fails closed on a
            # missing file with no check-then-open TOCTOU window, preserving
            # row factory / foreign keys / busy_timeout.
            return connect(db_path, must_exist=True)

        return cls(connection_factory=_factory, actor=config.actor)

    # -- use case 1: channel -> workspace resolve ---------------------------

    def resolve_channel_workspace(
        self,
        *,
        platform: str | None,
        channel_id: str | None,
    ) -> dict[str, Any]:
        try:
            if platform is None or not str(platform).strip():
                return error_envelope("invalid_request", "platform is required")
            if channel_id is None or not str(channel_id).strip():
                return error_envelope("invalid_request", "channel_id is required")
            with closing(self._connection_factory()) as conn, conn:
                binding = resolve_channel_workspace(
                    conn, platform=platform, channel_id=channel_id
                )
            if binding is None:
                return ok_envelope({"bound": False, "binding": None})
            return ok_envelope(
                {"bound": True, "binding": binding.to_dict()}
            )
        except Exception as exc:
            return error_envelope(*classify_error(exc))

    # -- use case 2: request submit -----------------------------------------

    def submit_request(
        self,
        *,
        workspace_id: str | None,
        prompt: str | None,
        origin: dict[str, Any] | None,
        reply: dict[str, Any] | None,
        task_id: str | None = None,
        target_agent: str | None = None,
        routing_request: dict[str, Any] | None = None,
        worktree_path: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        try:
            if idempotency_key is None or not str(idempotency_key).strip():
                return error_envelope(
                    "invalid_request", "idempotency_key is required and must be non-empty"
                )
            if workspace_id is None or not str(workspace_id).strip():
                return error_envelope("invalid_request", "workspace_id is required")
            if prompt is None or not str(prompt).strip():
                return error_envelope("invalid_request", "prompt is required")
            if not isinstance(origin, dict):
                return error_envelope("invalid_request", "origin must be an object")
            if not isinstance(reply, dict):
                return error_envelope("invalid_request", "reply must be an object")
            if target_agent is not None and routing_request is not None:
                return error_envelope(
                    "invalid_request",
                    "target_agent and routing_request are mutually exclusive",
                )
            if target_agent is None and routing_request is None:
                return error_envelope(
                    "invalid_request", "target_agent or routing_request is required"
                )

            routing = None
            if routing_request is not None:
                if not isinstance(routing_request, dict):
                    return error_envelope(
                        "invalid_request", "routing_request must be an object"
                    )
                unknown = set(routing_request) - _ROUTING_BUILDER_FIELDS
                if unknown:
                    return error_envelope(
                        "invalid_request", "unknown routing_request field"
                    )
                capabilities = routing_request.get("required_capabilities")
                if not isinstance(capabilities, list):
                    return error_envelope(
                        "invalid_request",
                        "routing_request.required_capabilities is required and must be a list",
                    )
                try:
                    routing = build_routing_request(
                        required_capabilities=capabilities,
                        executor_definition_id=routing_request.get(
                            "executor_definition_id"
                        ),
                        preferred_host_id=routing_request.get("preferred_host_id"),
                        operator_override_agent_id=routing_request.get(
                            "operator_override_agent_id"
                        ),
                        operator_override_reason=routing_request.get(
                            "operator_override_reason"
                        ),
                    )
                except ExecutorRoutingError:
                    return error_envelope(
                        "invalid_request", MESSAGE_INVALID_ROUTING_REQUEST
                    )

            with closing(self._connection_factory()) as conn, conn:
                result = submit_request(
                    conn,
                    workspace_id=workspace_id,
                    target_agent=target_agent,
                    prompt=prompt,
                    origin=origin,
                    reply=reply,
                    actor=self._actor,
                    task_id=task_id,
                    idempotency_key=idempotency_key,
                    routing_request=routing,
                    worktree_path=worktree_path,
                )
            return ok_envelope(result.to_dict())
        except Exception as exc:
            return error_envelope(*classify_error(exc))

    # -- use case 3: job get (job-id-only or workspace-bound) ---------------

    def get_job(
        self,
        job_id: str | None,
        workspace_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            if job_id is None or not str(job_id).strip():
                return error_envelope("invalid_request", "job_id is required")
            with closing(self._connection_factory()) as conn, conn:
                job = get_job(conn, job_id)
                if workspace_id is not None:
                    if not str(workspace_id).strip():
                        return error_envelope(
                            "invalid_request", "workspace_id is required"
                        )
                    if job["workspace_id"] != str(workspace_id):
                        return error_envelope(
                            "not_found", MESSAGE_JOB_NOT_FOUND
                        )
            return ok_envelope(row_to_dict(job))
        except Exception as exc:
            return error_envelope(*classify_error(exc))

    # -- use case 4: normal job claim ---------------------------------------

    def claim_job(
        self,
        *,
        agent_id: str | None,
        ttl_seconds: int | None = None,
        reap_mode: str | None = None,
        reap_reason: str | None = None,
    ) -> dict[str, Any]:
        try:
            if agent_id is None or not str(agent_id).strip():
                return error_envelope("invalid_request", "agent_id is required")
            if reap_mode is None:
                reap_mode = "global"
            if reap_mode not in {"global", "none"}:
                return error_envelope(
                    "invalid_request", "reap_mode must be 'global' or 'none'"
                )
            if ttl_seconds is not None and (
                not isinstance(ttl_seconds, int) or isinstance(ttl_seconds, bool)
                or ttl_seconds <= 0
            ):
                return error_envelope(
                    "invalid_request", "ttl_seconds must be a positive integer"
                )
            with closing(self._connection_factory()) as conn, conn:
                result = claim_job(
                    conn,
                    agent_id=agent_id,
                    ttl_seconds=(
                        ttl_seconds if ttl_seconds is not None else LEASE_DEFAULT_TTL_SECONDS
                    ),
                    reap_mode=reap_mode,
                    reap_reason=reap_reason,
                )
            return ok_envelope(result.to_dict())
        except Exception as exc:
            return error_envelope(*classify_error(exc))

    # -- use case 5: progress checkpoint ------------------------------------

    def record_job_progress(
        self,
        *,
        job_id: str | None,
        agent_id: str | None,
        stage: str | None = None,
        summary: str | None = None,
        session_id: str | None = None,
        attempt_token: int | None = None,
        lease_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            if job_id is None or not str(job_id).strip():
                return error_envelope("invalid_request", "job_id is required")
            if agent_id is None or not str(agent_id).strip():
                return error_envelope("invalid_request", "agent_id is required")
            with closing(self._connection_factory()) as conn, conn:
                result = record_job_progress(
                    conn,
                    job_id=job_id,
                    agent_id=agent_id,
                    stage=stage,
                    summary=summary,
                    session_id=session_id,
                    actor=self._actor,
                    attempt_token=attempt_token,
                    lease_id=lease_id,
                )
            return ok_envelope(result.to_dict())
        except Exception as exc:
            return error_envelope(*classify_error(exc))

    # -- use case 6: terminal/timeout report --------------------------------

    def report_job_result(
        self,
        *,
        job_id: str | None,
        agent_id: str | None,
        status: str | None,
        result: dict[str, Any] | None,
        attempt_token: int | None = None,
        lease_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            if job_id is None or not str(job_id).strip():
                return error_envelope("invalid_request", "job_id is required")
            if agent_id is None or not str(agent_id).strip():
                return error_envelope("invalid_request", "agent_id is required")
            if status is None or status not in {"done", "failed", "timed_out"}:
                return error_envelope(
                    "invalid_request", "status must be done, failed, or timed_out"
                )
            if not isinstance(result, dict):
                return error_envelope("invalid_request", "result must be an object")
            with closing(self._connection_factory()) as conn, conn:
                outcome = report_job_result(
                    conn,
                    job_id=job_id,
                    agent_id=agent_id,
                    status=status,
                    result=result,
                    actor=self._actor,
                    attempt_token=attempt_token,
                    lease_id=lease_id,
                )
            return ok_envelope(outcome.to_dict())
        except Exception as exc:
            return error_envelope(*classify_error(exc))

    # -- use case 7: managed lease renew ------------------------------------

    def renew_managed_lease(
        self,
        *,
        lease_id: str | None,
        job_id: str | None,
        attempt_token: int | None,
        agent_id: str | None,
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        try:
            if lease_id is None or not str(lease_id).strip():
                return error_envelope("invalid_request", "lease_id is required")
            if job_id is None or not str(job_id).strip():
                return error_envelope("invalid_request", "job_id is required")
            if agent_id is None or not str(agent_id).strip():
                return error_envelope("invalid_request", "agent_id is required")
            if not isinstance(attempt_token, int) or isinstance(attempt_token, bool):
                return error_envelope(
                    "invalid_request", "attempt_token must be an integer"
                )
            if ttl_seconds is not None and (
                not isinstance(ttl_seconds, int) or isinstance(ttl_seconds, bool)
                or ttl_seconds <= 0
            ):
                return error_envelope(
                    "invalid_request", "ttl_seconds must be a positive integer"
                )
            with closing(self._connection_factory()) as conn, conn:
                outcome = renew_managed_lease(
                    conn,
                    lease_id=lease_id,
                    job_id=job_id,
                    attempt_token=attempt_token,
                    agent_id=agent_id,
                    ttl_seconds=(
                        ttl_seconds if ttl_seconds is not None else LEASE_DEFAULT_TTL_SECONDS
                    ),
                )
            return ok_envelope(outcome)
        except Exception as exc:
            return error_envelope(*classify_error(exc))

    # -- readiness (short DB probe, no schema/path leakage) -----------------

    def readiness(self) -> bool:
        """Return True only when a fresh connection opens AND all three
        required Coordinate schema tables (events, jobs, workspaces) exist.
        A missing file, a schema-less file or a partial schema all fail
        closed (503), and the file itself is never created."""
        try:
            with closing(self._connection_factory()) as conn:
                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name IN ('events', 'jobs', 'workspaces')"
                ).fetchall()
            present = {row["name"] for row in rows}
            return {"events", "jobs", "workspaces"}.issubset(present)
        except Exception as exc:
            logger.error("readiness probe failed type=%s", type(exc).__name__)
            return False
