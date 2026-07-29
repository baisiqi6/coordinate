"""P9-2B executor routing: deterministic candidate selection for routed requests.

This module owns the routing request/decision contract, hard eligibility
filters, observed-load ordering, and redacted claim evidence. No routing
policy lives in the CLI, runtime orchestration, MultiNexus, or executor
catalog identity code.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from typing import Any

from coordinate.db import (
    get_runner_profile,
    get_workspace,
    get_workspace_host_profile,
    resolve_effective_agents,
)
from coordinate.executor_identity import (
    MAX_CAPABILITIES,
    MAX_CAPABILITY_LEN,
    ExecutorIdentityError,
    resolve_exact_executor_binding,
)

ROUTING_CONTRACT_VERSION = 1
ROUTING_POLICY_VERSION = 1
MAX_CANDIDATES = 256
MAX_REASON_LEN = 512

# Routing labels reuse the executor identity grammar: no control characters,
# shell metacharacters, path separators, or whitespace.
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_ROUTING_REQUEST_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")


class ExecutorRoutingError(ValueError):
    """Raised for malformed routing contracts or ineligible routing decisions."""


@dataclass(frozen=True)
class Candidate:
    """One eligible typed agent instance for a routed request."""

    agent_id: str
    host_id: str
    runner_profile_id: str
    executor_definition_id: str
    binding_id: str
    source_id: str
    source_version: int
    catalog_hash: str
    capabilities: tuple[str, ...]
    online_state: str
    last_seen_at: str | None
    routing_load: int
    binding_snapshot: dict[str, Any]
    host_rank: int
    preferred_host: bool = False

    def sort_key(self) -> tuple[int, int, str, str]:
        return (self.host_rank, self.routing_load, self.executor_definition_id, self.agent_id)

    def to_decision_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "host_id": self.host_id,
            "runner_profile_id": self.runner_profile_id,
            "executor_definition_id": self.executor_definition_id,
            "binding_id": self.binding_id,
            "source_id": self.source_id,
            "source_version": self.source_version,
            "catalog_hash": self.catalog_hash,
            "capabilities": list(self.capabilities),
            "online_state": self.online_state,
            "last_seen_at": self.last_seen_at,
            "preferred_host": self.preferred_host,
            "routing_load": self.routing_load,
        }


@dataclass(frozen=True)
class RoutingRequest:
    """Validated deterministic routing request v1."""

    required_capabilities: tuple[str, ...]
    executor_definition_id: str | None
    preferred_host_id: str | None
    operator_override_agent_id: str | None
    operator_override_reason: str | None
    routing_request_id: str

    def selection_kind(self) -> str | None:
        if self.operator_override_agent_id is not None:
            return "operator_override"
        return None


@dataclass(frozen=True)
class RoutingDecision:
    """Validated deterministic routing decision v1."""

    routing_request_id: str
    routing_decision_id: str
    selection_kind: str
    selected_agent_id: str
    selected_host_id: str
    selected_runner_profile_id: str
    selected_executor_definition_id: str
    selected_binding_id: str
    eligible_candidates: tuple[Candidate, ...]


# ---------------------------------------------------------------------------
# Canonical JSON and digest helpers
# ---------------------------------------------------------------------------


def _canonical_json(value: dict[str, Any]) -> str:
    """Deterministic JSON used for routing digests and byte-equivalent fixtures."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compute_routing_request_id(body: dict[str, Any]) -> str:
    """SHA-256 digest over the canonical request body excluding its own id."""
    canonical = _canonical_json(body)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _compute_routing_decision_id(body: dict[str, Any]) -> str:
    """SHA-256 digest over the canonical decision body excluding its own id."""
    canonical = _canonical_json(body)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


# ---------------------------------------------------------------------------
# Label and reason validation
# ---------------------------------------------------------------------------


def _validate_int(value: Any, label: str) -> int:
    """Reject booleans-as-integers and non-integer values."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExecutorRoutingError(f"{label} must be an integer")
    return value


def _validate_safe_label(value: Any, label: str, max_len: int = 64) -> str:
    if not isinstance(value, str):
        raise ExecutorRoutingError(f"{label} must be a string")
    if not value:
        raise ExecutorRoutingError(f"{label} is required")
    if len(value) > max_len:
        raise ExecutorRoutingError(f"{label} exceeds {max_len} characters")
    if not _SAFE_LABEL_RE.match(value):
        raise ExecutorRoutingError(f"{label} contains unsafe characters: {value!r}")
    return value


def _validate_optional_safe_label(value: Any, label: str, max_len: int = 64) -> str | None:
    if value is None:
        return None
    return _validate_safe_label(value, label, max_len=max_len)


def _normalize_capabilities(value: Any, label: str = "required_capabilities") -> tuple[str, ...]:
    """Caller-side normalization: sort, deduplicate, and validate labels."""
    if not isinstance(value, list):
        raise ExecutorRoutingError(f"{label} must be a list")
    if len(value) == 0:
        raise ExecutorRoutingError(f"{label} must contain at least one capability")
    seen: set[str] = set()
    for item in value:
        cap = _validate_safe_label(item, f"{label} item", max_len=MAX_CAPABILITY_LEN)
        seen.add(cap)
    if len(seen) > MAX_CAPABILITIES:
        raise ExecutorRoutingError(
            f"{label} exceeds maximum cardinality: {len(seen)} > {MAX_CAPABILITIES}"
        )
    return tuple(sorted(seen))


def _validate_canonical_capabilities(value: Any, label: str = "required_capabilities") -> tuple[str, ...]:
    """Strict stored-envelope validation: exact sorted-unique list of strings."""
    if not isinstance(value, list):
        raise ExecutorRoutingError(f"{label} must be a list")
    if len(value) == 0:
        raise ExecutorRoutingError(f"{label} must contain at least one capability")
    for item in value:
        if not isinstance(item, str) or isinstance(item, bool):
            raise ExecutorRoutingError(f"{label} item must be a string")
        if not _SAFE_LABEL_RE.match(item):
            raise ExecutorRoutingError(f"{label} item contains unsafe characters: {item!r}")
    if any(len(item) > MAX_CAPABILITY_LEN for item in value):
        raise ExecutorRoutingError(
            f"{label} exceeds maximum item length: {MAX_CAPABILITY_LEN}"
        )
    if len(value) != len(set(value)):
        raise ExecutorRoutingError(f"{label} contains duplicate values")
    if len(value) > MAX_CAPABILITIES:
        raise ExecutorRoutingError(
            f"{label} exceeds maximum cardinality: {len(value)} > {MAX_CAPABILITIES}"
        )
    if value != sorted(value):
        raise ExecutorRoutingError(f"{label} is not in canonical sorted order")
    return tuple(value)


def _validate_reason_text(value: str, label: str) -> str:
    if len(value) > MAX_REASON_LEN:
        raise ExecutorRoutingError(f"{label} exceeds {MAX_REASON_LEN} characters")
    if any(unicodedata.category(c).startswith("C") for c in value):
        raise ExecutorRoutingError(f"{label} contains control characters")
    return value


def _normalize_override_reason(value: Any, label: str = "operator_override_reason") -> str:
    """Caller-side normalization: strip, validate, and require non-empty audit text."""
    if not isinstance(value, str):
        raise ExecutorRoutingError(f"{label} must be a string")
    stripped = value.strip()
    if not stripped:
        raise ExecutorRoutingError(f"{label} is required when override is supplied")
    _validate_reason_text(stripped, label)
    return stripped


def _validate_canonical_reason(value: Any, label: str = "operator_override_reason") -> str:
    """Strict stored-envelope validation: exact canonical stripped text."""
    if not isinstance(value, str):
        raise ExecutorRoutingError(f"{label} must be a string")
    if value != value.strip():
        raise ExecutorRoutingError(f"{label} must be canonical stripped text")
    if not value:
        raise ExecutorRoutingError(f"{label} is required when override is supplied")
    _validate_reason_text(value, label)
    return value


def _validate_override_pair(
    agent_id: Any,
    reason: Any,
    *,
    reason_validator: Any = _normalize_override_reason,
) -> tuple[str | None, str | None]:
    if agent_id is None and reason is None:
        return None, None
    if agent_id is None or reason is None:
        raise ExecutorRoutingError(
            "operator_override_agent_id and operator_override_reason must both be supplied or both absent"
        )
    return (
        _validate_safe_label(agent_id, "operator_override_agent_id", max_len=256),
        reason_validator(reason),
    )


# ---------------------------------------------------------------------------
# Routing request construction / parsing
# ---------------------------------------------------------------------------


def build_routing_request(
    *,
    required_capabilities: list[str],
    executor_definition_id: str | None = None,
    preferred_host_id: str | None = None,
    operator_override_agent_id: str | None = None,
    operator_override_reason: str | None = None,
) -> RoutingRequest:
    """Build a deterministic routing request from caller-supplied values.

    Validates all labels, normalizes capabilities, and computes the self
    digest so the returned object is ready to persist.
    """
    capabilities = _normalize_capabilities(required_capabilities)
    definition_id = _validate_optional_safe_label(
        executor_definition_id, "executor_definition_id", max_len=256
    )
    host_id = _validate_optional_safe_label(preferred_host_id, "preferred_host_id", max_len=64)
    override_id, override_reason = _validate_override_pair(
        operator_override_agent_id, operator_override_reason
    )

    body = {
        "contract_version": ROUTING_CONTRACT_VERSION,
        "mode": "deterministic",
        "required_capabilities": list(capabilities),
        "executor_definition_id": definition_id,
        "preferred_host_id": host_id,
        "operator_override_agent_id": override_id,
        "operator_override_reason": override_reason,
        "policy_version": ROUTING_POLICY_VERSION,
    }
    request_id = _compute_routing_request_id(body)
    return RoutingRequest(
        required_capabilities=capabilities,
        executor_definition_id=definition_id,
        preferred_host_id=host_id,
        operator_override_agent_id=override_id,
        operator_override_reason=override_reason,
        routing_request_id=request_id,
    )


def parse_routing_request(data: Any) -> RoutingRequest:
    """Strictly parse a stored routing request dict.

    Fails closed on unknown keys, version mismatches, malformed digests,
    unsorted or duplicate capabilities, noncanonical reason text, and
    booleans passed as integers.
    """
    if not isinstance(data, dict):
        raise ExecutorRoutingError("routing_request must be an object")

    expected_keys = {
        "contract_version",
        "mode",
        "required_capabilities",
        "executor_definition_id",
        "preferred_host_id",
        "operator_override_agent_id",
        "operator_override_reason",
        "policy_version",
        "routing_request_id",
    }
    if set(data.keys()) != expected_keys:
        raise ExecutorRoutingError(
            f"routing_request has incorrect keys: expected {sorted(expected_keys)}, got {sorted(data.keys())}"
        )

    contract_version = _validate_int(data.get("contract_version"), "contract_version")
    if contract_version != ROUTING_CONTRACT_VERSION:
        raise ExecutorRoutingError(
            f"routing_request contract_version must be {ROUTING_CONTRACT_VERSION}"
        )

    policy_version = _validate_int(data.get("policy_version"), "policy_version")
    if policy_version != ROUTING_POLICY_VERSION:
        raise ExecutorRoutingError(
            f"routing_request policy_version must be {ROUTING_POLICY_VERSION}"
        )

    mode = data.get("mode")
    if mode != "deterministic":
        raise ExecutorRoutingError(f"routing_request mode unsupported: {mode!r}")

    capabilities = _validate_canonical_capabilities(data.get("required_capabilities"))
    definition_id = _validate_optional_safe_label(
        data.get("executor_definition_id"), "executor_definition_id", max_len=256
    )
    host_id = _validate_optional_safe_label(
        data.get("preferred_host_id"), "preferred_host_id", max_len=64
    )
    override_id, override_reason = _validate_override_pair(
        data.get("operator_override_agent_id"),
        data.get("operator_override_reason"),
        reason_validator=_validate_canonical_reason,
    )

    request_id = data.get("routing_request_id")
    if not isinstance(request_id, str) or not _ROUTING_REQUEST_ID_RE.match(request_id):
        raise ExecutorRoutingError("routing_request_id must be sha256:<64-lowercase-hex>")

    body = {
        "contract_version": ROUTING_CONTRACT_VERSION,
        "mode": "deterministic",
        "required_capabilities": list(capabilities),
        "executor_definition_id": definition_id,
        "preferred_host_id": host_id,
        "operator_override_agent_id": override_id,
        "operator_override_reason": override_reason,
        "policy_version": ROUTING_POLICY_VERSION,
    }
    expected_id = _compute_routing_request_id(body)
    if request_id != expected_id:
        raise ExecutorRoutingError(
            f"routing_request_id digest mismatch: expected {expected_id}, got {request_id}"
        )

    return RoutingRequest(
        required_capabilities=capabilities,
        executor_definition_id=definition_id,
        preferred_host_id=host_id,
        operator_override_agent_id=override_id,
        operator_override_reason=override_reason,
        routing_request_id=request_id,
    )


def routing_request_to_dict(routing_request: RoutingRequest) -> dict[str, Any]:
    """Return the exact canonical routing request dict including its self digest."""
    return {
        "contract_version": ROUTING_CONTRACT_VERSION,
        "mode": "deterministic",
        "required_capabilities": list(routing_request.required_capabilities),
        "executor_definition_id": routing_request.executor_definition_id,
        "preferred_host_id": routing_request.preferred_host_id,
        "operator_override_agent_id": routing_request.operator_override_agent_id,
        "operator_override_reason": routing_request.operator_override_reason,
        "policy_version": ROUTING_POLICY_VERSION,
        "routing_request_id": routing_request.routing_request_id,
    }


# ---------------------------------------------------------------------------
# Candidate resolution and load
# ---------------------------------------------------------------------------


def compute_routing_load(conn: sqlite3.Connection, agent_id: str) -> int:
    """Count non-terminal Coordinate jobs assigned to this agent.

    Includes pending, running, and recoverable timed_out jobs. Excludes
    terminal (done/failed) and non-recoverable timed_out jobs.
    """
    row = conn.execute(
        """
        SELECT COUNT(*) FROM jobs
        WHERE assigned_agent = ?
          AND (
            status IN ('pending', 'running')
            OR (status = 'timed_out' AND recoverable = 1)
          )
        """,
        (agent_id,),
    ).fetchone()
    return int(row[0])


def _resolve_candidate(
    conn: sqlite3.Connection,
    workspace_id: str,
    effective_agent_names: set[str],
    binding_row: sqlite3.Row,
    routing_request: RoutingRequest,
) -> Candidate | None:
    """Return a Candidate if the binding row passes every hard filter."""
    agent_id = binding_row["agent_id"]

    # 1. Workspace authorization is keyed by agent name (same as agent_id in
    #    runtime registrations). Fail closed if not effective for workspace.
    if agent_id not in effective_agent_names:
        return None

    # 2. Runtime agent must be agentd and online.
    agent_row = conn.execute(
        "SELECT client_type, online_state, host_id, last_seen_at FROM agents WHERE id = ?",
        (agent_id,),
    ).fetchone()
    if agent_row is None:
        return None
    if agent_row["client_type"] != "agentd" or agent_row["online_state"] != "online":
        return None

    host_id = agent_row["host_id"]
    if not host_id:
        return None

    # 3. Workspace must have a host profile for the candidate's host.
    if get_workspace_host_profile(conn, workspace_id=workspace_id, host_id=host_id) is None:
        return None

    # 4. The runner profile must exist and be agentd.
    runner = get_runner_profile(conn, binding_row["runner_profile_id"])
    if runner is None or runner.runner_type != "agentd":
        return None

    # 5. The full P9-2A binding snapshot must resolve successfully.
    try:
        binding_snapshot = resolve_exact_executor_binding(conn, agent_id)
    except ExecutorIdentityError:
        return None
    if binding_snapshot is None:
        return None

    # 6. Optional executor definition filter.
    definition_id = binding_snapshot["executor_definition_id"]
    if routing_request.executor_definition_id is not None:
        if definition_id != routing_request.executor_definition_id:
            return None

    # 7. Required capabilities must be a subset of the definition's capabilities.
    required = set(routing_request.required_capabilities)
    if not required.issubset(binding_snapshot["capabilities"]):
        return None

    load = compute_routing_load(conn, agent_id)
    preferred_host = (
        routing_request.preferred_host_id is not None
        and host_id == routing_request.preferred_host_id
    )
    host_rank = (
        0
        if routing_request.preferred_host_id is None
        or host_id == routing_request.preferred_host_id
        else 1
    )

    return Candidate(
        agent_id=agent_id,
        host_id=host_id,
        runner_profile_id=binding_row["runner_profile_id"],
        executor_definition_id=definition_id,
        binding_id=binding_snapshot["binding_id"],
        source_id=binding_snapshot["source_id"],
        source_version=binding_snapshot["source_version"],
        catalog_hash=binding_snapshot["catalog_hash"],
        capabilities=tuple(binding_snapshot["capabilities"]),
        online_state=agent_row["online_state"],
        last_seen_at=agent_row["last_seen_at"],
        routing_load=load,
        binding_snapshot=binding_snapshot,
        host_rank=host_rank,
        preferred_host=preferred_host,
    )


def resolve_routing_candidates(
    conn: sqlite3.Connection,
    workspace_id: str,
    routing_request: RoutingRequest,
) -> tuple[Candidate, ...]:
    """Resolve all hard-eligible candidates for a routing request.

    Returns candidates sorted by the deterministic routing tuple. Raises if
    the workspace is unknown.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ExecutorRoutingError(f"unknown workspace: {workspace_id}")

    effective = resolve_effective_agents(conn, workspace_id)
    effective_names = set(effective.keys())

    rows = conn.execute(
        """
        SELECT b.agent_id, b.source_id, b.executor_definition_id, b.runner_profile_id, b.enabled
        FROM executor_instance_bindings b
        WHERE b.enabled = 1
        """
    ).fetchall()

    candidates: list[Candidate] = []
    for row in rows:
        candidate = _resolve_candidate(conn, workspace_id, effective_names, row, routing_request)
        if candidate is not None:
            candidates.append(candidate)

    candidates.sort(key=lambda c: c.sort_key())
    return tuple(candidates)


# ---------------------------------------------------------------------------
# Decision selection
# ---------------------------------------------------------------------------


def select_routing_decision(
    routing_request: RoutingRequest,
    candidates: tuple[Candidate, ...],
) -> RoutingDecision:
    """Select the winning candidate and build an immutable decision.

    Raises ``ExecutorRoutingError`` for zero candidates or an ineligible override.
    """
    if len(candidates) > MAX_CANDIDATES:
        raise ExecutorRoutingError(
            f"executor_route_candidate_cap_exceeded: {len(candidates)} > {MAX_CANDIDATES}"
        )
    if not candidates:
        raise ExecutorRoutingError("executor_route_no_candidate")

    if routing_request.operator_override_agent_id is not None:
        selected = next(
            (c for c in candidates if c.agent_id == routing_request.operator_override_agent_id),
            None,
        )
        if selected is None:
            raise ExecutorRoutingError(
                f"executor_route_override_ineligible: {routing_request.operator_override_agent_id!r}"
            )
        selection_kind = "operator_override"
    else:
        selected = candidates[0]
        selection_kind = "automatic"

    decision_body = {
        "contract_version": ROUTING_CONTRACT_VERSION,
        "policy_version": ROUTING_POLICY_VERSION,
        "routing_request_id": routing_request.routing_request_id,
        "selection_kind": selection_kind,
        "selected_agent_id": selected.agent_id,
        "selected_host_id": selected.host_id,
        "selected_runner_profile_id": selected.runner_profile_id,
        "selected_executor_definition_id": selected.executor_definition_id,
        "selected_binding_id": selected.binding_id,
        "eligible_candidates": [c.to_decision_dict() for c in candidates],
    }
    decision_id = _compute_routing_decision_id(decision_body)

    return RoutingDecision(
        routing_request_id=routing_request.routing_request_id,
        routing_decision_id=decision_id,
        selection_kind=selection_kind,
        selected_agent_id=selected.agent_id,
        selected_host_id=selected.host_id,
        selected_runner_profile_id=selected.runner_profile_id,
        selected_executor_definition_id=selected.executor_definition_id,
        selected_binding_id=selected.binding_id,
        eligible_candidates=candidates,
    )


def routing_decision_to_dict(decision: RoutingDecision) -> dict[str, Any]:
    """Return the canonical routing decision dict including its self digest."""
    body = {
        "contract_version": ROUTING_CONTRACT_VERSION,
        "policy_version": ROUTING_POLICY_VERSION,
        "routing_request_id": decision.routing_request_id,
        "selection_kind": decision.selection_kind,
        "selected_agent_id": decision.selected_agent_id,
        "selected_host_id": decision.selected_host_id,
        "selected_runner_profile_id": decision.selected_runner_profile_id,
        "selected_executor_definition_id": decision.selected_executor_definition_id,
        "selected_binding_id": decision.selected_binding_id,
        "eligible_candidates": [c.to_decision_dict() for c in decision.eligible_candidates],
    }
    body["routing_decision_id"] = decision.routing_decision_id
    return body


# ---------------------------------------------------------------------------
# Stored decision validation
# ---------------------------------------------------------------------------


def _validate_routing_decision_keys(data: dict[str, Any]) -> None:
    expected = {
        "contract_version",
        "policy_version",
        "routing_request_id",
        "selection_kind",
        "selected_agent_id",
        "selected_host_id",
        "selected_runner_profile_id",
        "selected_executor_definition_id",
        "selected_binding_id",
        "eligible_candidates",
        "routing_decision_id",
    }
    if set(data.keys()) != expected:
        raise ExecutorRoutingError(
            f"routing_decision has incorrect keys: expected {sorted(expected)}, got {sorted(data.keys())}"
        )


def _candidate_sort_tuple(
    candidate: dict[str, Any],
    routing_request: RoutingRequest,
) -> tuple[int, int, str, str]:
    host_rank = 0 if (
        routing_request.preferred_host_id is None
        or candidate["host_id"] == routing_request.preferred_host_id
    ) else 1
    return (
        host_rank,
        candidate["routing_load"],
        candidate["executor_definition_id"],
        candidate["agent_id"],
    )


def _validate_candidate(value: Any, routing_request: RoutingRequest) -> dict[str, Any]:
    """Strictly validate one candidate dict and return it."""
    if not isinstance(value, dict):
        raise ExecutorRoutingError("eligible_candidates item must be an object")

    expected_keys = {
        "agent_id",
        "host_id",
        "runner_profile_id",
        "executor_definition_id",
        "binding_id",
        "source_id",
        "source_version",
        "catalog_hash",
        "capabilities",
        "online_state",
        "last_seen_at",
        "preferred_host",
        "routing_load",
    }
    if set(value.keys()) != expected_keys:
        raise ExecutorRoutingError(
            f"candidate has incorrect keys: expected {sorted(expected_keys)}, got {sorted(value.keys())}"
        )

    _validate_safe_label(value["agent_id"], "candidate agent_id")
    _validate_safe_label(value["host_id"], "candidate host_id")
    _validate_safe_label(value["runner_profile_id"], "candidate runner_profile_id")
    _validate_safe_label(value["executor_definition_id"], "candidate executor_definition_id")
    binding_id = value["binding_id"]
    if not isinstance(binding_id, str) or not _ROUTING_REQUEST_ID_RE.match(binding_id):
        raise ExecutorRoutingError("candidate binding_id must be sha256:<64-lowercase-hex>")
    _validate_safe_label(value["source_id"], "candidate source_id")
    _validate_safe_label(value["online_state"], "candidate online_state")

    if value["online_state"] != "online":
        raise ExecutorRoutingError("candidate online_state must be 'online'")

    _validate_int(value["source_version"], "candidate source_version")
    if value["source_version"] < 1:
        raise ExecutorRoutingError("candidate source_version must be positive")

    catalog_hash = value["catalog_hash"]
    if not isinstance(catalog_hash, str) or not _HEX64_RE.match(catalog_hash):
        raise ExecutorRoutingError("candidate catalog_hash must be a 64-character lowercase hex string")

    capabilities = _validate_canonical_capabilities(value["capabilities"], "candidate capabilities")
    required = set(routing_request.required_capabilities)
    if not required.issubset(capabilities):
        raise ExecutorRoutingError(
            f"candidate capabilities {capabilities} do not satisfy required {routing_request.required_capabilities}"
        )

    if routing_request.executor_definition_id is not None:
        if value["executor_definition_id"] != routing_request.executor_definition_id:
            raise ExecutorRoutingError(
                "candidate executor_definition_id does not match routing_request filter"
            )

    load = _validate_int(value["routing_load"], "candidate routing_load")
    if load < 0:
        raise ExecutorRoutingError("candidate routing_load must be non-negative")

    if not isinstance(value["preferred_host"], bool):
        raise ExecutorRoutingError("candidate preferred_host must be a boolean")
    expected_preferred = (
        routing_request.preferred_host_id is not None
        and value["host_id"] == routing_request.preferred_host_id
    )
    if value["preferred_host"] != expected_preferred:
        raise ExecutorRoutingError(
            "candidate preferred_host does not match routing_request preferred_host_id"
        )

    last_seen_at = value["last_seen_at"]
    if last_seen_at is not None and (not isinstance(last_seen_at, str) or not last_seen_at):
        raise ExecutorRoutingError("candidate last_seen_at must be a string or null")

    return value


def validate_routing_decision(
    data: Any,
    *,
    routing_request: RoutingRequest,
) -> dict[str, Any]:
    """Strictly validate a stored routing decision dict.

    Checks version, request link, digest, candidate schema, exact policy
    ordering, selection kind, and that every selected field matches one
    eligible candidate. Returns the validated dict. Fails closed for any
    structural or cryptographic violation.
    """
    if not isinstance(data, dict):
        raise ExecutorRoutingError("routing_decision must be an object")
    _validate_routing_decision_keys(data)

    if _validate_int(data.get("contract_version"), "contract_version") != ROUTING_CONTRACT_VERSION:
        raise ExecutorRoutingError(
            f"routing_decision contract_version must be {ROUTING_CONTRACT_VERSION}"
        )
    if _validate_int(data.get("policy_version"), "policy_version") != ROUTING_POLICY_VERSION:
        raise ExecutorRoutingError(
            f"routing_decision policy_version must be {ROUTING_POLICY_VERSION}"
        )
    if data.get("routing_request_id") != routing_request.routing_request_id:
        raise ExecutorRoutingError("routing_decision routing_request_id mismatch")

    selection_kind = data.get("selection_kind")
    if selection_kind not in {"automatic", "operator_override"}:
        raise ExecutorRoutingError(
            f"routing_decision selection_kind invalid: {selection_kind!r}"
        )

    override_id = routing_request.operator_override_agent_id
    if override_id is None:
        if selection_kind != "automatic":
            raise ExecutorRoutingError(
                "routing_decision selection_kind must be automatic for non-override request"
            )
    else:
        if selection_kind != "operator_override":
            raise ExecutorRoutingError(
                "routing_decision selection_kind must be operator_override for override request"
            )
        if data.get("selected_agent_id") != override_id:
            raise ExecutorRoutingError(
                "routing_decision selected_agent_id does not match operator override"
            )

    candidates = data.get("eligible_candidates")
    if not isinstance(candidates, list):
        raise ExecutorRoutingError("routing_decision eligible_candidates must be a list")
    if len(candidates) > MAX_CANDIDATES:
        raise ExecutorRoutingError(
            f"routing_decision eligible_candidates exceed cap: {len(candidates)} > {MAX_CANDIDATES}"
        )
    if len(candidates) == 0:
        raise ExecutorRoutingError("routing_decision eligible_candidates must not be empty")

    parsed_candidates: list[dict[str, Any]] = []
    seen_agent_ids: set[str] = set()
    for candidate in candidates:
        parsed = _validate_candidate(candidate, routing_request)
        if parsed["agent_id"] in seen_agent_ids:
            raise ExecutorRoutingError("routing_decision eligible_candidates contains duplicate agent_id")
        seen_agent_ids.add(parsed["agent_id"])
        parsed_candidates.append(parsed)

    # Exact policy ordering for the stored request.
    for i in range(len(parsed_candidates) - 1):
        if _candidate_sort_tuple(parsed_candidates[i], routing_request) > _candidate_sort_tuple(parsed_candidates[i + 1], routing_request):
            raise ExecutorRoutingError("routing_decision eligible_candidates are not in policy order")

    selected = None
    for candidate in parsed_candidates:
        if (
            candidate["agent_id"] == data["selected_agent_id"]
            and candidate["host_id"] == data["selected_host_id"]
            and candidate["runner_profile_id"] == data["selected_runner_profile_id"]
            and candidate["executor_definition_id"] == data["selected_executor_definition_id"]
            and candidate["binding_id"] == data["selected_binding_id"]
        ):
            selected = candidate
            break
    if selected is None:
        raise ExecutorRoutingError("routing_decision selected candidate not in eligible_candidates")

    if selection_kind == "automatic" and data["selected_agent_id"] != parsed_candidates[0]["agent_id"]:
        raise ExecutorRoutingError(
            "routing_decision automatic selection does not match first candidate"
        )

    decision_id = data.get("routing_decision_id")
    if not isinstance(decision_id, str) or not _ROUTING_REQUEST_ID_RE.match(decision_id):
        raise ExecutorRoutingError("routing_decision_id must be sha256:<64-lowercase-hex>")

    body = {k: v for k, v in data.items() if k != "routing_decision_id"}
    expected_id = _compute_routing_decision_id(body)
    if decision_id != expected_id:
        raise ExecutorRoutingError(
            f"routing_decision_id digest mismatch: expected {expected_id}, got {decision_id}"
        )

    return data


# ---------------------------------------------------------------------------
# Claim evidence and replay helpers
# ---------------------------------------------------------------------------


def _validate_routing_cross_links(
    request: RoutingRequest,
    decision: dict[str, Any],
    *,
    job: dict[str, Any],
    binding_snapshot: dict[str, Any] | None = None,
    execution_context: dict[str, Any] | None = None,
) -> None:
    """Fail-closed validation that selected decision, binding, context, and job agree.

    Raises ExecutorRoutingError if any internal link is forged or inconsistent.
    Does not reroute or consult current load; it only checks the stored snapshot.
    """
    if job.get("assigned_agent") != decision["selected_agent_id"]:
        raise ExecutorRoutingError(
            f"routing decision selected_agent_id {decision['selected_agent_id']!r} "
            f"does not match job assignment {job.get('assigned_agent')!r}"
        )
    if job.get("runner_profile_id") != decision["selected_runner_profile_id"]:
        raise ExecutorRoutingError(
            f"routing decision selected_runner_profile_id {decision['selected_runner_profile_id']!r} "
            f"does not match job runner_profile_id {job.get('runner_profile_id')!r}"
        )

    selected_candidate = None
    for candidate in decision["eligible_candidates"]:
        if candidate["agent_id"] == decision["selected_agent_id"]:
            selected_candidate = candidate
            break
    if selected_candidate is None:
        raise ExecutorRoutingError("routing decision selected candidate missing from eligible_candidates")

    if binding_snapshot is not None:
        if binding_snapshot.get("binding_id") != decision["selected_binding_id"]:
            raise ExecutorRoutingError("routing decision selected_binding_id does not match stored binding")
        if binding_snapshot.get("executor_instance_id") != decision["selected_agent_id"]:
            raise ExecutorRoutingError("routing decision selected_agent_id does not match stored binding")
        if binding_snapshot.get("runner_profile_id") != decision["selected_runner_profile_id"]:
            raise ExecutorRoutingError("routing decision selected_runner_profile_id does not match stored binding")
        if binding_snapshot.get("executor_definition_id") != decision["selected_executor_definition_id"]:
            raise ExecutorRoutingError("routing decision selected_executor_definition_id does not match stored binding")
        if selected_candidate["source_id"] != binding_snapshot.get("source_id"):
            raise ExecutorRoutingError("routing decision candidate source_id does not match stored binding")
        if selected_candidate["source_version"] != binding_snapshot.get("source_version"):
            raise ExecutorRoutingError("routing decision candidate source_version does not match stored binding")
        if selected_candidate["catalog_hash"] != binding_snapshot.get("catalog_hash"):
            raise ExecutorRoutingError("routing decision candidate catalog_hash does not match stored binding")
        if selected_candidate["capabilities"] != binding_snapshot.get("capabilities"):
            raise ExecutorRoutingError("routing decision candidate capabilities do not match stored binding")

    if execution_context is not None:
        if job.get("workspace_id") != execution_context.get("workspace_id"):
            raise ExecutorRoutingError("routing execution_context workspace_id does not match job")
        if job.get("task_id") != execution_context.get("task_id"):
            raise ExecutorRoutingError("routing execution_context task_id does not match job")
        if job.get("id") != execution_context.get("job_id"):
            raise ExecutorRoutingError("routing execution_context job_id does not match job")
        if execution_context.get("assigned_agent") != decision["selected_agent_id"]:
            raise ExecutorRoutingError("routing execution_context assigned_agent does not match decision")
        if execution_context.get("host_id") != decision["selected_host_id"]:
            raise ExecutorRoutingError("routing execution_context host_id does not match decision")


def routing_claim_evidence(
    payload: dict[str, Any],
    *,
    job: dict[str, Any],
) -> dict[str, Any]:
    """Return additive redacted routing evidence for a ``job.claimed`` event.

    Validates that the stored routing links match the job assignment, the
    stored P9-2A binding snapshot, and the stored P9-1 execution context.
    Fails closed if any cross-link is malformed or forged.
    """
    request_data = payload.get("routing_request")
    decision_data = payload.get("routing_decision")
    if request_data is None and decision_data is None:
        return {}
    if request_data is None or decision_data is None:
        raise ExecutorRoutingError(
            "routing_request and routing_decision must both be present or both absent"
        )

    request = parse_routing_request(request_data)
    decision = validate_routing_decision(decision_data, routing_request=request)

    binding_snapshot = payload.get("executor_binding")
    execution_context = payload.get("execution_context")
    if binding_snapshot is None or execution_context is None:
        raise ExecutorRoutingError(
            "routed job requires executor_binding and execution_context snapshots"
        )

    _validate_routing_cross_links(
        request,
        decision,
        job=job,
        binding_snapshot=binding_snapshot,
        execution_context=execution_context,
    )

    return {
        "routing_request_id": request.routing_request_id,
        "routing_decision_id": decision["routing_decision_id"],
        "selection_kind": decision["selection_kind"],
    }


def is_routed_job(payload: dict[str, Any]) -> bool:
    """Return True when a job payload carries a routed snapshot."""
    return payload.get("routing_request") is not None or payload.get("routing_decision") is not None
