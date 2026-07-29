"""Strict v1 parser for execution_lease envelopes.

Coordinate owns this validator so canonical fixtures can be tested without
importing MultiNexus code. The format is the contract boundary between the
Coordinate runtime and agentd consumers.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .execution_resources import (
    ResourceIdentityError,
    build_worktree_resource,
    compute_resource_key,
)

LEASE_CONTRACT_VERSION = 1
MIN_TTL_SECONDS = 30
MAX_TTL_SECONDS = 600
MAX_CONCURRENT_JOBS = 32
MAX_LABEL_LEN = 256

_LEASE_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_WINDOWS_ABS_RE = re.compile(r"^([A-Za-z]:[\\/]|\\\\|//)")

_V1_ENVELOPE_KEYS = frozenset({
    "contract_version",
    "lease_id",
    "job_id",
    "attempt_token",
    "agent_id",
    "runner_profile_id",
    "host_id",
    "resource_kind",
    "resource_key",
    "normalized_path",
    "capacity_policy_id",
    "max_concurrent_jobs",
    "acquired_at",
    "expires_at",
    "server_now",
    "ttl_seconds",
    "renew_interval_seconds",
})


class LeaseEnvelopeError(ValueError):
    """Raised when a lease envelope is missing, malformed, or conflicts with authority."""


@dataclass(frozen=True)
class ExecutionLeaseV1:
    contract_version: int
    lease_id: str
    job_id: str
    attempt_token: int
    agent_id: str
    runner_profile_id: str
    host_id: str
    resource_kind: str
    resource_key: str
    normalized_path: str
    capacity_policy_id: str
    max_concurrent_jobs: int
    acquired_at: str
    expires_at: str
    server_now: str
    ttl_seconds: int
    renew_interval_seconds: int


def _isabs(path: str) -> bool:
    return path.startswith("/") or _WINDOWS_ABS_RE.match(path) is not None


def _has_traversal(segments: list[str]) -> bool:
    return any(seg in {"..", "."} for seg in segments)


def _validate_required_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise LeaseEnvelopeError(f"{label} is required")
    if not value.strip():
        raise LeaseEnvelopeError(f"{label} must be a non-empty string")
    return value


def _validate_bounded_label(value: Any, label: str) -> str:
    s = _validate_required_string(value, label)
    if len(s) > MAX_LABEL_LEN:
        raise LeaseEnvelopeError(f"{label} exceeds {MAX_LABEL_LEN} characters")
    if not _SAFE_LABEL_RE.match(s):
        raise LeaseEnvelopeError(f"{label} contains unsafe characters: {s!r}")
    return s


def _validate_lease_id(value: Any) -> str:
    s = _validate_required_string(value, "lease_id")
    if not _LEASE_ID_RE.match(s):
        raise LeaseEnvelopeError(f"lease_id must be a lowercase UUID: {s!r}")
    return s


def _validate_digest(value: Any, label: str) -> str:
    s = _validate_required_string(value, label)
    if not _DIGEST_RE.match(s):
        raise LeaseEnvelopeError(f"{label} must be sha256:<64-hex>: {s!r}")
    return s


def _validate_timestamp(value: Any, label: str) -> str:
    s = _validate_required_string(value, label)
    if not _TS_RE.match(s):
        raise LeaseEnvelopeError(f"{label} must be ISO-UTC timestamp ending in Z: {s!r}")
    try:
        datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise LeaseEnvelopeError(f"{label} is not a valid timestamp: {s!r}") from exc
    return s


def _validate_path(value: Any, label: str) -> str:
    s = _validate_required_string(value, label)
    if "\x00" in s or "\n" in s or "\r" in s:
        raise LeaseEnvelopeError(f"{label} contains NUL or newline")
    if not _isabs(s):
        raise LeaseEnvelopeError(f"{label} must be absolute: {s!r}")
    segments = s.replace("\\", "/").split("/")
    if _has_traversal(segments):
        raise LeaseEnvelopeError(f"{label} contains traversal: {s!r}")
    return s


def _validate_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LeaseEnvelopeError(f"{label} must be an integer")
    if value <= 0:
        raise LeaseEnvelopeError(f"{label} must be positive")
    return value


def _validate_ttl(value: Any) -> int:
    ttl = _validate_positive_int(value, "ttl_seconds")
    if ttl < MIN_TTL_SECONDS or ttl > MAX_TTL_SECONDS:
        raise LeaseEnvelopeError(
            f"ttl_seconds must be between {MIN_TTL_SECONDS} and {MAX_TTL_SECONDS}: {ttl}"
        )
    return ttl


def _validate_max_concurrent_jobs(value: Any) -> int:
    n = _validate_positive_int(value, "max_concurrent_jobs")
    if n > MAX_CONCURRENT_JOBS:
        raise LeaseEnvelopeError(
            f"max_concurrent_jobs must be <= {MAX_CONCURRENT_JOBS}: {n}"
        )
    return n


def _parse_iso(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def parse_execution_lease(data: Any) -> ExecutionLeaseV1:
    """Strictly parse a v1 execution_lease envelope dict.

    Raises ``LeaseEnvelopeError`` on any structural, type, or consistency violation.
    """
    if not isinstance(data, dict):
        raise LeaseEnvelopeError("execution_lease must be an object")
    if set(data.keys()) != _V1_ENVELOPE_KEYS:
        missing = sorted(_V1_ENVELOPE_KEYS - set(data.keys()))
        unexpected = sorted(set(data.keys()) - _V1_ENVELOPE_KEYS)
        raise LeaseEnvelopeError(
            f"execution_lease has incorrect keys: missing={missing}, unexpected={unexpected}"
        )

    contract_version = data["contract_version"]
    if isinstance(contract_version, bool) or not isinstance(contract_version, int):
        raise LeaseEnvelopeError("contract_version must be an integer")
    if contract_version != LEASE_CONTRACT_VERSION:
        raise LeaseEnvelopeError(
            f"execution_lease contract_version {contract_version} is not supported"
        )

    lease_id = _validate_lease_id(data["lease_id"])
    job_id = _validate_required_string(data["job_id"], "job_id")
    attempt_token = _validate_positive_int(data["attempt_token"], "attempt_token")
    agent_id = _validate_bounded_label(data["agent_id"], "agent_id")
    runner_profile_id = _validate_bounded_label(data["runner_profile_id"], "runner_profile_id")
    host_id = _validate_bounded_label(data["host_id"], "host_id")
    resource_kind = _validate_required_string(data["resource_kind"], "resource_kind")
    if resource_kind != "worktree":
        raise LeaseEnvelopeError(f"resource_kind must be 'worktree': {resource_kind!r}")
    resource_key = _validate_digest(data["resource_key"], "resource_key")
    normalized_path = _validate_path(data["normalized_path"], "normalized_path")
    capacity_policy_id = _validate_digest(data["capacity_policy_id"], "capacity_policy_id")
    max_concurrent_jobs = _validate_max_concurrent_jobs(data["max_concurrent_jobs"])
    acquired_at = _validate_timestamp(data["acquired_at"], "acquired_at")
    expires_at = _validate_timestamp(data["expires_at"], "expires_at")
    server_now = _validate_timestamp(data["server_now"], "server_now")
    ttl_seconds = _validate_ttl(data["ttl_seconds"])
    renew_interval_seconds = _validate_positive_int(
        data["renew_interval_seconds"], "renew_interval_seconds"
    )
    if renew_interval_seconds >= ttl_seconds:
        raise LeaseEnvelopeError(
            f"renew_interval_seconds ({renew_interval_seconds}) must be < ttl_seconds ({ttl_seconds})"
        )

    acquired_dt = _parse_iso(acquired_at)
    expires_dt = _parse_iso(expires_at)
    server_dt = _parse_iso(server_now)
    if expires_dt <= acquired_dt:
        raise LeaseEnvelopeError("expires_at must be after acquired_at")

    expected_ttl = int((expires_dt - acquired_dt).total_seconds())
    if expected_ttl != ttl_seconds:
        raise LeaseEnvelopeError(
            f"ttl_seconds ({ttl_seconds}) does not match acquired_at/expires_at interval ({expected_ttl})"
        )

    if server_dt < acquired_dt:
        raise LeaseEnvelopeError("server_now must not be before acquired_at")
    if server_dt >= expires_dt:
        raise LeaseEnvelopeError("server_now must be before expires_at")

    return ExecutionLeaseV1(
        contract_version=LEASE_CONTRACT_VERSION,
        lease_id=lease_id,
        job_id=job_id,
        attempt_token=attempt_token,
        agent_id=agent_id,
        runner_profile_id=runner_profile_id,
        host_id=host_id,
        resource_kind=resource_kind,
        resource_key=resource_key,
        normalized_path=normalized_path,
        capacity_policy_id=capacity_policy_id,
        max_concurrent_jobs=max_concurrent_jobs,
        acquired_at=acquired_at,
        expires_at=expires_at,
        server_now=server_now,
        ttl_seconds=ttl_seconds,
        renew_interval_seconds=renew_interval_seconds,
    )


def validate_execution_lease(
    data: Any,
    *,
    expected_agent_id: str | None = None,
    expected_job_id: str | None = None,
    expected_attempt_token: int | None = None,
    execution_context: Any = None,
    executor_binding: Any = None,
) -> ExecutionLeaseV1:
    """Validate a lease envelope and its cross-links to context/binding/resource.

    Raises ``LeaseEnvelopeError`` on any mismatch. The resource key is recomputed
    from ``host_id`` and ``normalized_path`` to detect mismatched path/digest pairs.
    """
    lease = parse_execution_lease(data)

    if expected_agent_id is not None and lease.agent_id != expected_agent_id:
        raise LeaseEnvelopeError(
            f"lease agent_id mismatch: {lease.agent_id!r} != {expected_agent_id!r}"
        )
    if expected_job_id is not None and lease.job_id != expected_job_id:
        raise LeaseEnvelopeError(
            f"lease job_id mismatch: {lease.job_id!r} != {expected_job_id!r}"
        )
    if expected_attempt_token is not None and lease.attempt_token != expected_attempt_token:
        raise LeaseEnvelopeError(
            f"lease attempt_token mismatch: {lease.attempt_token} != {expected_attempt_token}"
        )

    try:
        resource = build_worktree_resource(lease.host_id, lease.normalized_path)
    except ResourceIdentityError as exc:
        raise LeaseEnvelopeError(f"invalid resource identity: {exc}") from exc
    if resource.normalized_path != lease.normalized_path:
        raise LeaseEnvelopeError(
            f"normalized_path is not canonical: {lease.normalized_path!r}"
        )
    expected_resource_key = compute_resource_key(resource)
    if lease.resource_key != expected_resource_key:
        raise LeaseEnvelopeError(
            f"lease resource_key does not match normalized_path: "
            f"expected {expected_resource_key}, got {lease.resource_key}"
        )

    if execution_context is not None:
        if not isinstance(execution_context, dict):
            raise LeaseEnvelopeError("execution_context must be a dict for lease cross-check")
        if execution_context.get("job_id") != lease.job_id:
            raise LeaseEnvelopeError("lease job_id does not match execution_context.job_id")
        if execution_context.get("assigned_agent") != lease.agent_id:
            raise LeaseEnvelopeError(
                "lease agent_id does not match execution_context.assigned_agent"
            )
        if execution_context.get("host_id") != lease.host_id:
            raise LeaseEnvelopeError("lease host_id does not match execution_context.host_id")
        ctx_worktree = execution_context.get("worktree_path")
        if ctx_worktree != lease.normalized_path:
            raise LeaseEnvelopeError(
                f"lease normalized_path does not match execution_context.worktree_path: "
                f"{lease.normalized_path!r} != {ctx_worktree!r}"
            )

    if executor_binding is not None:
        if not isinstance(executor_binding, dict):
            raise LeaseEnvelopeError("executor_binding must be a dict for lease cross-check")
        if executor_binding.get("executor_instance_id") != lease.agent_id:
            raise LeaseEnvelopeError(
                "lease agent_id does not match executor_binding.executor_instance_id"
            )
        if executor_binding.get("runner_profile_id") != lease.runner_profile_id:
            raise LeaseEnvelopeError(
                "lease runner_profile_id does not match executor_binding.runner_profile_id"
            )

    return lease
