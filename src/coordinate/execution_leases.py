"""Transaction-aware execution attempt lease primitives.

All public mutation functions accept a caller-owned transaction and never
commit. Test-only wrappers may open ``BEGIN IMMEDIATE``.
"""
from __future__ import annotations

import json
import re
import sqlite3
import uuid
from typing import Any

from coordinate.db_support import utc_now
from coordinate.execution_context import (
    ContextError,
    validate_execution_context_snapshot,
)
from coordinate.execution_resources import (
    RESOURCE_CONTRACT_VERSION,
    ResourceIdentityError,
    build_worktree_resource,
    compute_resource_key,
    validate_resource_key_matches,
)
from coordinate.executor_capacity import (
    resolve_capacity_policy,
)


MIN_TTL_SECONDS = 30
MAX_TTL_SECONDS = 600
MAX_RELEASE_REASON_LEN = 256


class LeaseError(ValueError):
    """Raised when a lease operation violates capacity, identity, or state rules."""


LEASE_STATUS = {"active", "released", "expired"}

_RESOURCE_KEY_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_CAPACITY_POLICY_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


LEASE_CONTRACT_VERSION = 1
LEASE_DEFAULT_TTL_SECONDS = 120
LEASE_DEFAULT_RENEW_INTERVAL_SECONDS = 30
LEASE_REAP_BATCH_SIZE = 100


def _validate_attempt_token(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LeaseError("attempt_token must be an integer")
    if value <= 0:
        raise LeaseError("attempt_token must be a positive integer")
    return value


def _validate_release_reason(value: Any) -> str:
    if not isinstance(value, str):
        raise LeaseError("release reason must be a Unicode string")
    if not value:
        raise LeaseError("release reason must not be empty")
    if value != value.strip():
        raise LeaseError("release reason must not have surrounding whitespace")
    if len(value) > MAX_RELEASE_REASON_LEN:
        raise LeaseError(f"release reason exceeds {MAX_RELEASE_REASON_LEN} characters")
    if _CONTROL_RE.search(value):
        raise LeaseError("release reason contains control characters")
    return value


def _validate_stored_resource(lease: dict[str, Any]) -> dict[str, Any]:
    """Rebuild and validate the stored resource snapshot.

    Raises ``LeaseError`` if the stored row has been tampered with.
    """
    try:
        resource = {
            "contract_version": RESOURCE_CONTRACT_VERSION,
            "resource_kind": lease.get("resource_kind"),
            "host_id": lease.get("host_id"),
            "normalized_path": lease.get("normalized_path"),
        }
        return validate_resource_key_matches(resource, lease["resource_key"])
    except ResourceIdentityError as exc:
        raise LeaseError(f"stored lease resource snapshot is tampered: {exc}") from exc


def _utc_now() -> str:
    return utc_now()


def _validate_ttl(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LeaseError("TTL must be an integer")
    if value < MIN_TTL_SECONDS or value > MAX_TTL_SECONDS:
        raise LeaseError(f"TTL must be between {MIN_TTL_SECONDS} and {MAX_TTL_SECONDS} seconds")
    return value


_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _validate_lease_id(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise LeaseError("lease_id must be a non-empty string")
    return value


def _validate_status(value: Any) -> str:
    if not isinstance(value, str) or value not in LEASE_STATUS:
        raise LeaseError(f"status must be one of {LEASE_STATUS}")
    return value


def _lease_id() -> str:
    return str(uuid.uuid4())


def count_active_leases_for_agent(conn: sqlite3.Connection, agent_id: str) -> int:
    """Count active, unexpired leases for one agent."""
    rows = conn.execute(
        "SELECT * FROM execution_attempt_leases "
        "WHERE agent_id = ? AND status = 'active' AND expires_at > ?",
        (agent_id, _utc_now()),
    ).fetchall()
    for row in rows:
        _validate_stored_resource(dict(row))
    return len(rows)


def _find_active_resource_lease(
    conn: sqlite3.Connection, resource_key: str
) -> dict[str, Any] | None:
    """Return the active (status='active') lease for a resource, regardless of expiry.

    P9-3B: reserve is not an expiry authority, so a due active lease still blocks
    the resource until the caller-owned reap drains it.
    """
    rows = conn.execute(
        "SELECT * FROM execution_attempt_leases "
        "WHERE resource_key = ? AND status = 'active'",
        (resource_key,),
    ).fetchall()
    if not rows:
        return None
    for row in rows:
        _validate_stored_resource(dict(row))
    return dict(rows[0])


def _find_active_unexpired_resource_lease(
    conn: sqlite3.Connection, resource_key: str
) -> dict[str, Any] | None:
    """Return the active, unexpired lease for a resource (legacy helpers)."""
    rows = conn.execute(
        "SELECT * FROM execution_attempt_leases "
        "WHERE resource_key = ? AND status = 'active' AND expires_at > ?",
        (resource_key, _utc_now()),
    ).fetchall()
    if not rows:
        return None
    for row in rows:
        _validate_stored_resource(dict(row))
    return dict(rows[0])


def _find_due_active_leases(
    conn: sqlite3.Connection,
    now: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Return up to ``limit`` due active leases ordered by expires_at ASC."""
    rows = conn.execute(
        """
        SELECT * FROM execution_attempt_leases
        WHERE status = 'active' AND expires_at <= ?
        ORDER BY expires_at ASC
        LIMIT ?
        """,
        (now, limit),
    ).fetchall()
    leases = []
    for row in rows:
        lease = dict(row)
        _validate_stored_resource(lease)
        leases.append(lease)
    return leases


def attempt_has_any_lease(conn: sqlite3.Connection, job_id: str, attempt_token: int) -> bool:
    """Managed-attempt predicate: any lease row for (job_id, attempt_token) means managed."""
    row = conn.execute(
        "SELECT 1 FROM execution_attempt_leases WHERE job_id = ? AND attempt_token = ?",
        (job_id, attempt_token),
    ).fetchone()
    return row is not None


def require_active_unexpired_lease(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    attempt_token: int,
    agent_id: str,
) -> dict[str, Any]:
    """Return the active, unexpired lease if every tuple field matches.

    Raises ``LeaseError`` on mismatch, expired, released, or missing lease.
    """
    lease_id = _validate_lease_id(lease_id)
    attempt_token = _validate_attempt_token(attempt_token)
    row = conn.execute(
        "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
        (lease_id,),
    ).fetchone()
    if row is None:
        raise LeaseError(f"lease {lease_id!r} not found")
    lease = dict(row)
    _validate_stored_resource(lease)
    if lease["job_id"] != job_id:
        raise LeaseError("lease job_id mismatch")
    if lease["attempt_token"] != attempt_token:
        raise LeaseError("lease attempt_token mismatch")
    if lease["agent_id"] != agent_id:
        raise LeaseError("lease agent_id mismatch")
    if lease["status"] != "active":
        raise LeaseError(f"lease {lease_id!r} is {lease['status']}")
    now = _utc_now()
    if lease["expires_at"] <= now:
        raise LeaseError(f"lease {lease_id!r} has expired")
    return lease


def build_lease_envelope(
    lease: dict[str, Any],
    *,
    server_now: str,
    ttl_seconds: int,
    renew_interval_seconds: int,
) -> dict[str, Any]:
    """Return the strict v1 execution_lease envelope from a committed lease row."""
    return {
        "contract_version": LEASE_CONTRACT_VERSION,
        "lease_id": lease["lease_id"],
        "job_id": lease["job_id"],
        "attempt_token": lease["attempt_token"],
        "agent_id": lease["agent_id"],
        "runner_profile_id": lease["runner_profile_id"],
        "host_id": lease["host_id"],
        "resource_kind": lease["resource_kind"],
        "resource_key": lease["resource_key"],
        "normalized_path": lease["normalized_path"],
        "capacity_policy_id": lease["capacity_policy_id"],
        "max_concurrent_jobs": lease["max_concurrent_jobs"],
        "acquired_at": lease["acquired_at"],
        "expires_at": lease["expires_at"],
        "server_now": server_now,
        "ttl_seconds": ttl_seconds,
        "renew_interval_seconds": renew_interval_seconds,
    }


def _job_attempt_lease(
    conn: sqlite3.Connection, job_id: str, attempt_token: int
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM execution_attempt_leases WHERE job_id = ? AND attempt_token = ?",
        (job_id, attempt_token),
    ).fetchone()
    return dict(row) if row else None


def _immutable_lease_fields_match(
    existing: dict[str, Any],
    *,
    agent_id: str,
    runner_profile_id: str,
    host_id: str,
    resource_key: str,
    capacity_policy_id: str,
    max_concurrent_jobs: int,
) -> bool:
    return (
        existing["agent_id"] == agent_id
        and existing["runner_profile_id"] == runner_profile_id
        and existing["host_id"] == host_id
        and existing["resource_key"] == resource_key
        and existing["capacity_policy_id"] == capacity_policy_id
        and existing["max_concurrent_jobs"] == max_concurrent_jobs
    )


def reserve_attempt_lease(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_token: int,
    agent_id: str,
    runner_profile_id: str,
    host_id: str,
    worktree_path: str,
    ttl_seconds: int,
) -> dict[str, Any]:
    """Reserve one attempt lease for an existing job/attempt/context.

    - TTL must be an integer 30..600 before any write.
    - Requires a valid v1 execution context snapshot bound to the job/attempt/host/path.
    - Expires due active leases for the target agent or resource first.
    - Rejects capacity exhaustion and active resource collision.
    - Exact replay with matching immutable fields returns the existing lease.
    - Conflicting replay raises ``LeaseError`` and leaves no new row.
    - Requires the caller to own the transaction; no commit is performed.
    """
    ttl_seconds = _validate_ttl(ttl_seconds)
    attempt_token = _validate_attempt_token(attempt_token)

    # P9-3B: jobs without a runner_profile_id fail closed before claim.
    job = conn.execute(
        """
        SELECT id, assigned_agent, runner_profile_id, attempt_count, workspace_id, payload_json
        FROM jobs WHERE id = ?
        """,
        (job_id,),
    ).fetchone()
    if job is None:
        raise LeaseError(f"job {job_id!r} not found")
    if not job["runner_profile_id"]:
        raise LeaseError(f"job {job_id!r} has no runner_profile_id")
    if job["attempt_count"] != attempt_token:
        raise LeaseError(
            f"job {job_id!r} attempt_count {job['attempt_count']} != {attempt_token}"
        )
    if job["assigned_agent"] != agent_id:
        raise LeaseError(
            f"job {job_id!r} assigned_agent {job['assigned_agent']!r} != {agent_id!r}"
        )
    if job["runner_profile_id"] != runner_profile_id:
        raise LeaseError(
            f"job {job_id!r} runner_profile_id {job['runner_profile_id']!r} != {runner_profile_id!r}"
        )

    # Validate execution context snapshot against job/host/attempt authority.
    try:
        payload = json.loads(job["payload_json"])
    except (json.JSONDecodeError, TypeError) as exc:
        raise LeaseError(f"job payload_json is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise LeaseError("job payload_json must be an object")
    execution_context = payload.get("execution_context")
    if execution_context is None:
        raise LeaseError("job payload_json.execution_context is required")
    try:
        snapshot = validate_execution_context_snapshot(
            execution_context,
            job_id=job_id,
            workspace_id=job["workspace_id"],
            assigned_agent=agent_id,
            host_id=host_id,
        )
    except ContextError as exc:
        raise LeaseError(f"invalid execution context: {exc}") from exc

    # Resolve capacity policy snapshot.
    policy = resolve_capacity_policy(conn, agent_id)
    if policy is None:
        raise LeaseError(f"no capacity policy for agent {agent_id!r}")
    capacity_policy_id = policy["capacity_policy_id"]
    max_concurrent_jobs = policy["max_concurrent_jobs"]

    # Resolve resource identity from caller input.
    try:
        resource = build_worktree_resource(host_id, worktree_path)
    except ResourceIdentityError as exc:
        raise LeaseError(f"invalid resource identity: {exc}") from exc
    resource_key = compute_resource_key(resource)

    # The caller-supplied resource must match the context snapshot.
    try:
        expected_resource = build_worktree_resource(host_id, snapshot.worktree_path)
    except ResourceIdentityError as exc:
        raise LeaseError(
            f"execution context worktree_path is invalid: {exc}"
        ) from exc
    if resource.normalized_path != expected_resource.normalized_path:
        raise LeaseError(
            f"worktree_path resource mismatch: input {resource.normalized_path!r} != "
            f"context {expected_resource.normalized_path!r}"
        )

    now = _utc_now()

    # Exact replay check first.
    existing = _job_attempt_lease(conn, job_id, attempt_token)
    if existing is not None:
        _validate_stored_resource(existing)
        if not _immutable_lease_fields_match(
            existing,
            agent_id=agent_id,
            runner_profile_id=runner_profile_id,
            host_id=host_id,
            resource_key=resource_key,
            capacity_policy_id=capacity_policy_id,
            max_concurrent_jobs=max_concurrent_jobs,
        ):
            raise LeaseError(
                "conflicting lease replay: immutable fields differ from existing lease"
            )
        return {
            "lease_id": existing["lease_id"],
            "job_id": existing["job_id"],
            "attempt_token": existing["attempt_token"],
            "agent_id": existing["agent_id"],
            "resource_key": existing["resource_key"],
            "status": existing["status"],
            "expires_at": existing["expires_at"],
            "acquired_at": existing["acquired_at"],
            "replayed": True,
        }

    # P9-3B: reserve is no longer an expiry authority. After the caller-owned
    # bounded global reap, an active due lease for this resource is a distinct
    # conflict; capacity exhaustion is a distinct conflict. We never silently
    # expire a lease here because that would orphan its running job.
    active_resource = _find_active_resource_lease(conn, resource_key)
    if active_resource is not None:
        if active_resource["expires_at"] <= now:
            raise LeaseError(
                f"resource blocked by due lease {active_resource['lease_id']!r}; "
                "reap backlog must be drained before claiming this resource"
            )
        raise LeaseError(
            f"resource collision: {resource_key!r} is held by lease {active_resource['lease_id']!r}"
        )

    # Capacity exhaustion check.
    active_count = count_active_leases_for_agent(conn, agent_id)
    if active_count >= max_concurrent_jobs:
        raise LeaseError(
            f"capacity exhausted for agent {agent_id!r}: "
            f"{active_count} active >= {max_concurrent_jobs} max"
        )

    # Verify agent and runner rows exist.
    agent = conn.execute("SELECT id FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if agent is None:
        raise LeaseError(f"agent {agent_id!r} not found")
    profile = conn.execute(
        "SELECT id FROM runner_profiles WHERE id = ?", (runner_profile_id,)
    ).fetchone()
    if profile is None:
        raise LeaseError(f"runner profile {runner_profile_id!r} not found")

    lease_id = _lease_id()
    expires_at = _compute_expires(now, ttl_seconds)
    conn.execute(
        """
        INSERT INTO execution_attempt_leases (
          lease_id, job_id, attempt_token, agent_id, runner_profile_id, host_id,
          resource_kind, resource_key, normalized_path, capacity_policy_id,
          max_concurrent_jobs, status, acquired_at, renewed_at, expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lease_id,
            job_id,
            attempt_token,
            agent_id,
            runner_profile_id,
            host_id,
            "worktree",
            resource_key,
            resource.normalized_path,
            capacity_policy_id,
            max_concurrent_jobs,
            "active",
            now,
            now,
            expires_at,
        ),
    )
    return {
        "lease_id": lease_id,
        "job_id": job_id,
        "attempt_token": attempt_token,
        "agent_id": agent_id,
        "resource_key": resource_key,
        "status": "active",
        "expires_at": expires_at,
        "acquired_at": now,
        "replayed": False,
    }


def _compute_expires(acquired_at: str, ttl_seconds: int) -> str:
    """Compute expires_at from an ISO timestamp string and a TTL in seconds."""
    from datetime import datetime, timedelta, timezone

    dt = datetime.fromisoformat(acquired_at.replace("Z", "+00:00"))
    expires = dt + timedelta(seconds=ttl_seconds)
    return expires.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def renew_attempt_lease(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    attempt_token: int,
    agent_id: str,
    ttl_seconds: int,
) -> dict[str, Any]:
    """Renew one exact active/unexpired lease.

    - Requires exact lease/job/attempt/agent identity.
    - Active status and current unexpired lease required.
    - Advances ``renewed_at/expires_at`` monotonically within TTL 30..600.
    - Raises ``LeaseError`` for wrong token, agent, job, or stale lease.
    """
    ttl_seconds = _validate_ttl(ttl_seconds)
    # P9-3B: shared tuple validation.
    lease = require_active_unexpired_lease(
        conn,
        lease_id=lease_id,
        job_id=job_id,
        attempt_token=attempt_token,
        agent_id=agent_id,
    )

    now = _utc_now()
    new_expires = _compute_expires(now, ttl_seconds)
    if new_expires <= lease["expires_at"]:
        raise LeaseError("renewal must advance expires_at")

    conn.execute(
        """
        UPDATE execution_attempt_leases
        SET renewed_at = ?, expires_at = ?
        WHERE lease_id = ?
        """,
        (now, new_expires, lease_id),
    )
    return {
        "lease_id": lease_id,
        "job_id": job_id,
        "attempt_token": attempt_token,
        "agent_id": agent_id,
        "status": "active",
        "expires_at": new_expires,
        "renewed_at": now,
    }


def release_attempt_lease(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    attempt_token: Any,
    agent_id: str,
    reason: Any,
) -> dict[str, Any]:
    """Release one exact active lease.

    - Idempotent only for the same final state/reason.
    - Rejects wrong token, agent, job, or newer-attempt safety violations.
    - Does not release an already terminal lease with a different reason.
    """
    lease_id = _validate_lease_id(lease_id)
    reason = _validate_release_reason(reason)
    attempt_token = _validate_attempt_token(attempt_token)

    now = _utc_now()
    row = conn.execute(
        "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
        (lease_id,),
    ).fetchone()
    if row is None:
        raise LeaseError(f"lease {lease_id!r} not found")
    lease = dict(row)
    _validate_stored_resource(lease)

    if lease["job_id"] != job_id:
        raise LeaseError("release job_id mismatch")
    if lease["attempt_token"] != attempt_token:
        raise LeaseError("release attempt_token mismatch")
    if lease["agent_id"] != agent_id:
        raise LeaseError("release agent_id mismatch")

    if lease["status"] == "released":
        if lease["release_reason"] != reason:
            raise LeaseError(
                f"lease {lease_id!r} already released with different reason"
            )
        return {
            "lease_id": lease_id,
            "job_id": job_id,
            "attempt_token": attempt_token,
            "agent_id": agent_id,
            "status": "released",
            "released_at": lease["released_at"],
            "release_reason": reason,
        }

    if lease["status"] == "expired":
        raise LeaseError(f"lease {lease_id!r} is already expired")

    released_at = now
    conn.execute(
        """
        UPDATE execution_attempt_leases
        SET status = 'released', released_at = ?, release_reason = ?
        WHERE lease_id = ?
        """,
        (released_at, reason, lease_id),
    )
    return {
        "lease_id": lease_id,
        "job_id": job_id,
        "attempt_token": attempt_token,
        "agent_id": agent_id,
        "status": "released",
        "released_at": released_at,
        "release_reason": reason,
    }


def expire_attempt_lease(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    attempt_token: Any,
    agent_id: str,
) -> dict[str, Any]:
    """Expire one exact lease without mutating job state.

    - Idempotent only for the same final state.
    - Rejects wrong token, agent, job.
    - Does not expire a newer attempt's lease.
    """
    # P9-3B: shared tuple validation; an already expired lease is idempotent.
    lease_id = _validate_lease_id(lease_id)
    attempt_token = _validate_attempt_token(attempt_token)
    row = conn.execute(
        "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
        (lease_id,),
    ).fetchone()
    if row is None:
        raise LeaseError(f"lease {lease_id!r} not found")
    lease = dict(row)
    _validate_stored_resource(lease)

    if lease["job_id"] != job_id:
        raise LeaseError("expire job_id mismatch")
    if lease["attempt_token"] != attempt_token:
        raise LeaseError("expire attempt_token mismatch")
    if lease["agent_id"] != agent_id:
        raise LeaseError("expire agent_id mismatch")

    if lease["status"] == "expired":
        return {
            "lease_id": lease_id,
            "job_id": job_id,
            "attempt_token": attempt_token,
            "agent_id": agent_id,
            "status": "expired",
        }

    if lease["status"] == "released":
        raise LeaseError(f"lease {lease_id!r} is already released")

    conn.execute(
        """
        UPDATE execution_attempt_leases
        SET status = 'expired'
        WHERE lease_id = ?
        """,
        (lease_id,),
    )
    return {
        "lease_id": lease_id,
        "job_id": job_id,
        "attempt_token": attempt_token,
        "agent_id": agent_id,
        "status": "expired",
    }


def expire_due_attempt_leases(
    conn: sqlite3.Connection,
    *,
    due_before: str | None = None,
) -> list[str]:
    """Expire all active leases whose ``expires_at <= due_before``.

    Returns the list of affected ``lease_id`` values. Does not mutate job state.
    """
    now = due_before or _utc_now()
    rows = conn.execute(
        """
        SELECT * FROM execution_attempt_leases
        WHERE status = 'active' AND expires_at <= ?
        """,
        (now,),
    ).fetchall()
    lease_ids = []
    for row in rows:
        lease = dict(row)
        _validate_stored_resource(lease)
        lease_ids.append(lease["lease_id"])
    if not lease_ids:
        return []
    placeholders = ",".join("?" for _ in lease_ids)
    cur = conn.execute(
        f"""
        UPDATE execution_attempt_leases
        SET status = 'expired'
        WHERE lease_id IN ({placeholders})
        RETURNING lease_id
        """,
        tuple(lease_ids),
    )
    return [row["lease_id"] for row in cur.fetchall()]


def get_attempt_lease(
    conn: sqlite3.Connection, lease_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM execution_attempt_leases WHERE lease_id = ?", (lease_id,)
    ).fetchone()
    if row is None:
        return None
    lease = dict(row)
    _validate_stored_resource(lease)
    return lease


def list_active_leases_for_agent(
    conn: sqlite3.Connection, agent_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM execution_attempt_leases "
        "WHERE agent_id = ? AND status = 'active' ORDER BY acquired_at",
        (agent_id,),
    ).fetchall()
    leases = [dict(row) for row in rows]
    for lease in leases:
        _validate_stored_resource(lease)
    return leases
