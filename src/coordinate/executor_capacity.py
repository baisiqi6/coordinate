"""Capacity authority projection: parse, canonical digests, sync, and read.

This module owns the separately versioned capacity projection from
``config/agent-registry.toml``. It lives next to ``executor_identity.py`` but
keeps its own tables and contract.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
import tomllib
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from coordinate.db_support import _json_dumps, utc_now


CAPACITY_CONTRACT_VERSION = 1
MAX_CONCURRENT_JOBS = 32
MAX_LABEL_LEN = 64

EXPECTED_CAPACITY_SOURCE_ID = "multinexus.discord.capacity"
SNAPSHOT_CONTRACT_VERSION = 2
_SNAPSHOT_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SNAPSHOT_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SNAPSHOT_POLICY_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

_SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_POLICY_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_SOURCE_PATH_LEN = 4096
_EXPECTED_ENVELOPE_KEYS = frozenset({"snapshot", "snapshot_sha256"})
_EXPECTED_INNER_SNAPSHOT_KEYS_V1 = frozenset({"contract_version", "target_source_id", "captured_state"})
_EXPECTED_INNER_SNAPSHOT_KEYS_V2 = frozenset({"contract_version", "target_source_id", "captured_state", "preserved_state"})
_EXPECTED_CAPTURED_STATE_KEYS = frozenset({"source", "policies"})
_EXPECTED_PRESERVED_STATE_KEYS = frozenset({"sources", "policies"})
_EXPECTED_SNAPSHOT_SOURCE_KEYS = frozenset({"source_id", "source_version", "catalog_hash", "source_path", "updated_at"})
_EXPECTED_SNAPSHOT_POLICY_KEYS = frozenset({
    "agent_id", "source_id", "source_version", "catalog_hash",
    "capacity_policy_id", "max_concurrent_jobs", "created_at", "updated_at",
})

_ALLOWED_CAPACITY_ROOT_KEYS = {"capacity_registry", "executor_capacities"}
_ALLOWED_CAPACITY_REGISTRY_KEYS = {"id", "version"}
_ALLOWED_CAPACITY_POLICY_KEYS = {"agent_id", "max_concurrent_jobs"}
# Shared agent-registry.toml may contain P9-2A roster/executor roots; the capacity
# parser ignores them but must reject any unknown or secret-bearing root.
_ALLOWED_SHARED_ROOT_KEYS = {
    "registry",
    "executor_definitions",
    "agents",
    "external_agents",
    "capacity_registry",
    "executor_capacities",
}


class CapacityError(ValueError):
    """Raised when the capacity catalog or a policy snapshot is invalid."""


def _canonical_json(value: dict[str, Any]) -> str:
    """Deterministic JSON used for digests and byte-identical fixtures."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now() -> str:
    return utc_now()


def _validate_bounded_label(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise CapacityError(f"{label} must be a string")
    if value != value.strip():
        raise CapacityError(f"{label} must not have surrounding whitespace")
    if not value:
        raise CapacityError(f"{label} is required")
    if len(value) > MAX_LABEL_LEN:
        raise CapacityError(f"{label} exceeds {MAX_LABEL_LEN} characters")
    if not _SAFE_LABEL_RE.match(value):
        raise CapacityError(f"{label} contains unsafe characters: {value!r}")
    return value


def _validate_max_concurrent_jobs(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CapacityError(f"{label} must be an integer")
    if value < 1 or value > MAX_CONCURRENT_JOBS:
        raise CapacityError(f"{label} must be between 1 and {MAX_CONCURRENT_JOBS}: got {value}")
    return value


@dataclass(frozen=True)
class CapacityPolicy:
    agent_id: str
    max_concurrent_jobs: int

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "max_concurrent_jobs": self.max_concurrent_jobs,
        }


@dataclass(frozen=True)
class CapacityCatalog:
    source_id: str
    source_version: int
    catalog_hash: str
    source_path: str | None
    policies: tuple[CapacityPolicy, ...]


def canonical_capacity_catalog_dict(catalog: CapacityCatalog) -> dict[str, Any]:
    """Return the exact canonical object whose UTF-8 JSON is hashed."""
    policies = sorted(
        [p.canonical_dict() for p in catalog.policies],
        key=lambda p: p["agent_id"],
    )
    return {
        "contract_version": CAPACITY_CONTRACT_VERSION,
        "source_id": catalog.source_id,
        "source_version": catalog.source_version,
        "policies": policies,
    }


def compute_capacity_catalog_hash(catalog: CapacityCatalog) -> str:
    """SHA-256 of the canonical UTF-8 JSON capacity catalog bytes."""
    canonical = _canonical_json(canonical_capacity_catalog_dict(catalog))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_capacity_policy_id(
    *,
    agent_id: str,
    catalog_hash: str,
    max_concurrent_jobs: int,
    source_id: str,
    source_version: int,
) -> str:
    """Return ``sha256:<digest>`` for a capacity policy snapshot."""
    canonical = _canonical_json({
        "agent_id": agent_id,
        "catalog_hash": catalog_hash,
        "contract_version": CAPACITY_CONTRACT_VERSION,
        "max_concurrent_jobs": max_concurrent_jobs,
        "source_id": source_id,
        "source_version": source_version,
    })
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def parse_capacity_catalog(source: str | Path) -> CapacityCatalog:
    """Parse the capacity projection from a TOML authority file.

    Unknown capacity keys, duplicate policies, non-integer capacities, and
    values outside ``1..MAX_CONCURRENT_JOBS`` raise ``CapacityError``.
    """
    path = Path(source).expanduser()
    with open(path, "rb") as f:
        data = tomllib.load(f)

    unknown_root = set(data.keys()) - _ALLOWED_SHARED_ROOT_KEYS
    if unknown_root:
        raise CapacityError(f"unknown root keys: {sorted(unknown_root)}")

    capacity_registry = data.get("capacity_registry")
    if not isinstance(capacity_registry, dict):
        raise CapacityError("missing [capacity_registry] metadata")
    unknown_registry = set(capacity_registry.keys()) - _ALLOWED_CAPACITY_REGISTRY_KEYS
    if unknown_registry:
        raise CapacityError(f"unknown [capacity_registry] keys: {sorted(unknown_registry)}")

    source_id = capacity_registry.get("id")
    _validate_bounded_label(source_id, "[capacity_registry].id")

    version = capacity_registry.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise CapacityError("[capacity_registry].version must be a non-negative integer")
    source_version = version

    policies: list[CapacityPolicy] = []
    seen_agent_ids: set[str] = set()
    for raw in data.get("executor_capacities", []):
        if not isinstance(raw, dict):
            raise CapacityError("executor_capacities entry must be a table")
        unknown = set(raw.keys()) - _ALLOWED_CAPACITY_POLICY_KEYS
        if unknown:
            raise CapacityError(f"unknown keys in executor_capacities entry: {sorted(unknown)}")
        agent_id = _validate_bounded_label(raw.get("agent_id"), "executor_capacity.agent_id")
        if agent_id in seen_agent_ids:
            raise CapacityError(f"duplicate executor_capacity agent_id: {agent_id!r}")
        seen_agent_ids.add(agent_id)
        max_jobs = _validate_max_concurrent_jobs(
            raw.get("max_concurrent_jobs"), f"executor_capacity '{agent_id}'.max_concurrent_jobs"
        )
        policies.append(CapacityPolicy(agent_id=agent_id, max_concurrent_jobs=max_jobs))

    policies_tuple = tuple(policies)
    catalog = CapacityCatalog(
        source_id=source_id,
        source_version=source_version,
        catalog_hash="",
        source_path=str(path),
        policies=policies_tuple,
    )
    catalog_hash = compute_capacity_catalog_hash(catalog)
    return CapacityCatalog(
        source_id=source_id,
        source_version=source_version,
        catalog_hash=catalog_hash,
        source_path=catalog.source_path,
        policies=policies_tuple,
    )


def _dict_diff(
    old: dict[str, dict[str, Any]],
    new: dict[str, dict[str, Any]],
) -> tuple[set[str], set[str], set[str], set[str]]:
    added = set(new) - set(old)
    removed = set(old) - set(new)
    common = set(old) & set(new)
    updated = {k for k in common if old[k] != new[k]}
    unchanged = common - updated
    return added, updated, removed, unchanged


def _enabled_typed_agent_ids(conn: sqlite3.Connection) -> set[str]:
    """Return the set of agent ids that have enabled typed executor bindings."""
    rows = conn.execute(
        "SELECT agent_id FROM executor_instance_bindings WHERE enabled = 1"
    ).fetchall()
    return {row["agent_id"] for row in rows}


def _all_typed_agent_ids(conn: sqlite3.Connection) -> set[str]:
    """Return the set of agent ids that have typed executor bindings (enabled or disabled)."""
    rows = conn.execute("SELECT agent_id FROM executor_instance_bindings").fetchall()
    return {row["agent_id"] for row in rows}


def _other_source_policy_agents(conn: sqlite3.Connection, source_id: str) -> set[str]:
    """Return agent ids currently owned by capacity sources other than source_id."""
    rows = conn.execute(
        "SELECT agent_id FROM executor_capacity_policies WHERE source_id != ?",
        (source_id,),
    ).fetchall()
    return {row["agent_id"] for row in rows}


def _this_source_policy_ids(conn: sqlite3.Connection, source_id: str) -> dict[str, str]:
    """Return {agent_id: capacity_policy_id} for policies owned by source_id."""
    rows = conn.execute(
        "SELECT agent_id, capacity_policy_id FROM executor_capacity_policies WHERE source_id = ?",
        (source_id,),
    ).fetchall()
    return {row["agent_id"]: row["capacity_policy_id"] for row in rows}


def _active_lease_policy_ids(conn: sqlite3.Connection) -> set[str]:
    """Return the set of capacity_policy_ids referenced by active leases."""
    rows = conn.execute(
        "SELECT DISTINCT capacity_policy_id FROM execution_attempt_leases WHERE status = 'active'"
    ).fetchall()
    return {row["capacity_policy_id"] for row in rows if row["capacity_policy_id"]}


def sync_capacity_catalog(
    conn: sqlite3.Connection,
    catalog: CapacityCatalog,
    *,
    source_path: str | None = None,
    synced_by: str = "operator",
) -> dict[str, Any]:
    """Atomically sync one capacity catalog source.

    - Same-version/same-hash syncs are idempotent and allowed even when active
      leases exist.
    - Same-version/different-hash and version downgrades fail with zero mutation.
    - A source may own a disjoint partial policy set; the post-sync union across
      all sources must cover every enabled typed executor binding.
    - A policy may target an existing enabled or disabled typed binding, but
      never an unknown/untyped id.
    - Another source may not take over an existing ``agent_id`` policy.
    - Any mutation that would replace or remove a policy id referenced by an
      active lease fails with zero mutation.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = _now()
        existing = conn.execute(
            "SELECT source_id, source_version, catalog_hash FROM executor_capacity_sources WHERE source_id = ?",
            (catalog.source_id,),
        ).fetchone()

        # Version/hash gate: for an already-known source, downgrade and same-
        # version/different-hash take precedence over global invariant checks so
        # that deterministic structural errors are reported first.
        if existing is not None:
            if catalog.source_version < existing["source_version"]:
                raise CapacityError(
                    f"capacity version downgrade for {catalog.source_id!r}: "
                    f"{catalog.source_version} < {existing['source_version']}"
                )
            if (
                catalog.source_version == existing["source_version"]
                and catalog.catalog_hash != existing["catalog_hash"]
            ):
                raise CapacityError(
                    f"capacity hash changed without version bump for {catalog.source_id!r}"
                )

        # Global invariants are revalidated before any write, including the
        # no-write exact-retry path, so that drift in bindings or ownership is
        # never silently accepted.
        all_typed = _all_typed_agent_ids(conn)
        enabled = _enabled_typed_agent_ids(conn)
        catalog_agents = {p.agent_id for p in catalog.policies}

        # Known-binding guard applies globally: every existing policy (any source)
        # and every proposed policy must target a typed binding.
        existing_policy_agents = {
            row["agent_id"]
            for row in conn.execute("SELECT DISTINCT agent_id FROM executor_capacity_policies").fetchall()
        }
        unknown = (catalog_agents | existing_policy_agents) - all_typed
        if unknown:
            raise CapacityError(
                f"capacity present for unknown/untyped agents: {sorted(unknown)}"
            )

        # Ownership guard: use the existing source_id column; cross-source
        # takeover of an agent_id must fail before any write.
        if catalog_agents:
            sorted_catalog_agents = sorted(catalog_agents)
            placeholders = ",".join("?" * len(sorted_catalog_agents))
            owners = conn.execute(
                f"SELECT agent_id, source_id FROM executor_capacity_policies "
                f"WHERE agent_id IN ({placeholders}) ORDER BY agent_id",
                tuple(sorted_catalog_agents),
            ).fetchall()
            for row in owners:
                if row["source_id"] != catalog.source_id:
                    raise CapacityError(
                        f"capacity agent_id {row['agent_id']!r} is owned by source {row['source_id']!r}"
                    )

        # Proposed post-sync union must cover every enabled typed binding.
        other_agents = _other_source_policy_agents(conn, catalog.source_id)
        union = catalog_agents | other_agents
        missing = enabled - union
        if missing:
            raise CapacityError(
                f"capacity missing for enabled typed agents: {sorted(missing)}"
            )

        # Same-version/same-hash idempotent retry is only allowed after global
        # ownership/known-binding/union invariants are revalidated.
        if existing is not None:
            if (
                catalog.source_version == existing["source_version"]
                and catalog.catalog_hash == existing["catalog_hash"]
            ):
                conn.commit()
                return {
                    "source_id": catalog.source_id,
                    "source_version": catalog.source_version,
                    "catalog_hash": catalog.catalog_hash,
                    "changed": False,
                    "added_policy_ids": [],
                    "updated_policy_ids": [],
                    "removed_policy_ids": [],
                    "unchanged_policy_ids": sorted(p.agent_id for p in catalog.policies),
                }

        # Active-lease guard: compare exact old policy-id set for this source
        # with the exact new policy-id set. Any old id absent from the new set
        # that is referenced by an active lease blocks the sync.
        old_policy_ids = _this_source_policy_ids(conn, catalog.source_id)
        new_policy_ids = {
            compute_capacity_policy_id(
                agent_id=p.agent_id,
                catalog_hash=catalog.catalog_hash,
                max_concurrent_jobs=p.max_concurrent_jobs,
                source_id=catalog.source_id,
                source_version=catalog.source_version,
            )
            for p in catalog.policies
        }
        active_policy_ids = _active_lease_policy_ids(conn)
        doomed_policy_ids = set(old_policy_ids.values()) - new_policy_ids
        blocked = doomed_policy_ids & active_policy_ids
        if blocked:
            # Deterministic reporting: sort by agent_id so multiple blocked leases
            # produce a stable message.
            blocked_pairs = sorted(
                ((a, pid) for a, pid in old_policy_ids.items() if pid in blocked),
                key=lambda pair: pair[0],
            )
            agent_id, _doomed_pid = blocked_pairs[0]
            raise CapacityError(
                f"cannot replace capacity policy for {agent_id!r}: referenced by active lease"
            )

        # Compute diff for result fields.
        old_policies = {
            row["agent_id"]: {
                "agent_id": row["agent_id"],
                "capacity_policy_id": row["capacity_policy_id"],
                "max_concurrent_jobs": row["max_concurrent_jobs"],
            }
            for row in conn.execute(
                "SELECT agent_id, capacity_policy_id, max_concurrent_jobs FROM executor_capacity_policies WHERE source_id = ?",
                (catalog.source_id,),
            ).fetchall()
        }
        new_policies = {
            p.agent_id: {
                "agent_id": p.agent_id,
                "capacity_policy_id": compute_capacity_policy_id(
                    agent_id=p.agent_id,
                    catalog_hash=catalog.catalog_hash,
                    max_concurrent_jobs=p.max_concurrent_jobs,
                    source_id=catalog.source_id,
                    source_version=catalog.source_version,
                ),
                "max_concurrent_jobs": p.max_concurrent_jobs,
            }
            for p in catalog.policies
        }
        added, updated, removed, unchanged = _dict_diff(old_policies, new_policies)

        # Upsert source row.
        conn.execute(
            """
            INSERT INTO executor_capacity_sources (source_id, source_version, catalog_hash, source_path, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
              source_version = excluded.source_version,
              catalog_hash = excluded.catalog_hash,
              source_path = excluded.source_path,
              updated_at = excluded.updated_at
            """,
            (
                catalog.source_id,
                catalog.source_version,
                catalog.catalog_hash,
                source_path or catalog.source_path,
                now,
            ),
        )

        # Replace policies atomically.
        conn.execute(
            "DELETE FROM executor_capacity_policies WHERE source_id = ?",
            (catalog.source_id,),
        )
        for p in catalog.policies:
            policy_id = compute_capacity_policy_id(
                agent_id=p.agent_id,
                catalog_hash=catalog.catalog_hash,
                max_concurrent_jobs=p.max_concurrent_jobs,
                source_id=catalog.source_id,
                source_version=catalog.source_version,
            )
            conn.execute(
                """
                INSERT INTO executor_capacity_policies (
                  agent_id, source_id, source_version, catalog_hash, capacity_policy_id,
                  max_concurrent_jobs, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p.agent_id,
                    catalog.source_id,
                    catalog.source_version,
                    catalog.catalog_hash,
                    policy_id,
                    p.max_concurrent_jobs,
                    now,
                    now,
                ),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "source_id": catalog.source_id,
        "source_version": catalog.source_version,
        "catalog_hash": catalog.catalog_hash,
        "changed": True,
        "added_policy_ids": sorted(added),
        "updated_policy_ids": sorted(updated),
        "removed_policy_ids": sorted(removed),
        "unchanged_policy_ids": sorted(unchanged),
    }


def get_capacity_source(
    conn: sqlite3.Connection, source_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT source_id, source_version, catalog_hash, source_path, updated_at "
        "FROM executor_capacity_sources WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    return dict(row) if row else None


def list_capacity_sources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT source_id, source_version, catalog_hash, source_path, updated_at "
        "FROM executor_capacity_sources ORDER BY source_id"
    ).fetchall()
    return [dict(row) for row in rows]


def get_capacity_policy(conn: sqlite3.Connection, agent_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT agent_id, source_id, source_version, catalog_hash, capacity_policy_id, "
        "max_concurrent_jobs, created_at, updated_at "
        "FROM executor_capacity_policies WHERE agent_id = ?",
        (agent_id,),
    ).fetchone()
    return dict(row) if row else None


def list_capacity_policies(
    conn: sqlite3.Connection, source_id: str | None = None
) -> list[dict[str, Any]]:
    if source_id is not None:
        rows = conn.execute(
            "SELECT agent_id, source_id, source_version, catalog_hash, capacity_policy_id, "
            "max_concurrent_jobs, created_at, updated_at "
            "FROM executor_capacity_policies WHERE source_id = ? ORDER BY agent_id",
            (source_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT agent_id, source_id, source_version, catalog_hash, capacity_policy_id, "
            "max_concurrent_jobs, created_at, updated_at "
            "FROM executor_capacity_policies ORDER BY agent_id"
        ).fetchall()
    return [dict(row) for row in rows]


def resolve_capacity_policy(
    conn: sqlite3.Connection, agent_id: str
) -> dict[str, Any] | None:
    """Return the current capacity policy for an enabled typed agent, or None."""
    policy = get_capacity_policy(conn, agent_id)
    if policy is None:
        return None
    return {
        "agent_id": policy["agent_id"],
        "capacity_policy_id": policy["capacity_policy_id"],
        "max_concurrent_jobs": policy["max_concurrent_jobs"],
        "source_id": policy["source_id"],
        "source_version": policy["source_version"],
        "catalog_hash": policy["catalog_hash"],
    }


def _validate_snapshot_timestamp(value: Any, label: str) -> str:
    """Validate exact ``YYYY-MM-DDTHH:MM:SSZ`` with real calendar/time parse.

    Rejects non-canonical shapes (fraction/fractional seconds, offsets, spaces),
    out-of-range month/day/hour/minute/second, and non-string types.
    """
    if not isinstance(value, str):
        raise CapacityError(f"{label} must be a string")
    if not _SNAPSHOT_TS_RE.match(value):
        raise CapacityError(f"{label} has invalid timestamp shape: {value!r}")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise CapacityError(f"{label} is not a valid UTC datetime: {value!r} ({exc})") from exc
    # Reject canonicalized forms that parsed successfully but aren't Z-suffixed UTC.
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise CapacityError(f"{label} is not canonical UTC Z-suffix: {value!r}")
    return value


def _validate_snapshot_hash(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise CapacityError(f"{label} must be a string")
    if not _SNAPSHOT_HASH_RE.match(value):
        raise CapacityError(f"{label} must be 64 lowercase hex digits: {value!r}")
    return value


def _validate_snapshot_policy_id(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise CapacityError(f"{label} must be a string")
    if not _SNAPSHOT_POLICY_ID_RE.match(value):
        raise CapacityError(f"{label} must be sha256:<64 lowercase hex>: {value!r}")
    return value


def _validate_snapshot_source_version(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CapacityError(f"{label} must be a non-negative integer")
    return value


def _snapshot_canonical_bytes(value: dict[str, Any]) -> bytes:
    return _canonical_json(value).encode("utf-8")


def _validate_source_path(value: Any, label: str) -> None:
    """Validate source_path is None or a bounded string free of Unicode Cc characters.

    Rejects characters in Unicode category Cc (U+0000-U+001F, U+007F DEL,
    U+0080-U+009F C1 controls). Accepts ordinary Unicode paths (including
    non-ASCII/non-Latin scripts) as long as they contain no control characters.
    """
    if value is None:
        return
    if not isinstance(value, str):
        raise CapacityError(f"{label} must be null or a string")
    if len(value) > _MAX_SOURCE_PATH_LEN:
        raise CapacityError(f"{label} exceeds {_MAX_SOURCE_PATH_LEN} characters")
    for ch in value:
        if unicodedata.category(ch) == "Cc":
            raise CapacityError(f"{label} contains control character U+{ord(ch):04X}")


def _safe_unlink(path: Path) -> None:
    """Best-effort delete of a snapshot file that must not remain as valid-looking output."""
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _strict_validate_captured_state(
    captured_state: Any,
    target_source_id: str,
) -> None:
    """Validate captured_state structure, field shapes, and source/policy consistency.

    Raises CapacityError on any violation. Shared by capture and restore so that
    an attacker who modifies the state and recomputes the digest is still rejected.
    """
    if captured_state is None:
        return
    if not isinstance(captured_state, dict):
        raise CapacityError("captured_state must be an object or null")
    if set(captured_state.keys()) != _EXPECTED_CAPTURED_STATE_KEYS:
        raise CapacityError(
            f"captured_state has unknown or missing keys: {sorted(captured_state.keys())}"
        )
    source = captured_state["source"]
    if not isinstance(source, dict):
        raise CapacityError("captured_state.source must be an object")
    if set(source.keys()) != _EXPECTED_SNAPSHOT_SOURCE_KEYS:
        raise CapacityError(
            f"captured_state.source has unknown or missing keys: {sorted(source.keys())}"
        )
    _validate_bounded_label(target_source_id, "target_source_id")
    source_id = source["source_id"]
    if source_id != target_source_id:
        raise CapacityError(
            f"source.source_id mismatch: {source_id!r} != {target_source_id!r}"
        )
    _validate_bounded_label(source_id, "source.source_id")
    source_version = _validate_snapshot_source_version(
        source["source_version"], "source.source_version"
    )
    catalog_hash = _validate_snapshot_hash(
        source["catalog_hash"], "source.catalog_hash"
    )
    _validate_source_path(source["source_path"], "source.source_path")
    _validate_snapshot_timestamp(source["updated_at"], "source.updated_at")
    policies = captured_state["policies"]
    if not isinstance(policies, list):
        raise CapacityError("captured_state.policies must be a list")
    prev_agent_id: str | None = None
    for i, p in enumerate(policies):
        if not isinstance(p, dict):
            raise CapacityError(f"policy[{i}] must be an object")
        if set(p.keys()) != _EXPECTED_SNAPSHOT_POLICY_KEYS:
            raise CapacityError(
                f"policy[{i}] has unknown or missing keys: {sorted(p.keys())}"
            )
        agent_id = _validate_bounded_label(p["agent_id"], f"policy[{i}].agent_id")
        if prev_agent_id is not None and agent_id <= prev_agent_id:
            raise CapacityError(
                f"policies not strictly increasing by agent_id at index {i}: "
                f"{agent_id!r} <= {prev_agent_id!r}"
            )
        prev_agent_id = agent_id
        if p["source_id"] != target_source_id:
            raise CapacityError(
                f"policy[{i}] source_id mismatch: {p['source_id']!r} != {target_source_id!r}"
            )
        _validate_bounded_label(p["source_id"], f"policy[{i}].source_id")
        p_version = _validate_snapshot_source_version(
            p["source_version"], f"policy[{i}].source_version"
        )
        if p_version != source_version:
            raise CapacityError(
                f"policy[{i}] source_version mismatch: {p_version} != {source_version}"
            )
        p_hash = _validate_snapshot_hash(
            p["catalog_hash"], f"policy[{i}].catalog_hash"
        )
        if p_hash != catalog_hash:
            raise CapacityError(
                f"policy[{i}] catalog_hash mismatch"
            )
        p_cap = _validate_max_concurrent_jobs(
            p["max_concurrent_jobs"], f"policy[{i}].max_concurrent_jobs"
        )
        _validate_snapshot_policy_id(
            p["capacity_policy_id"], f"policy[{i}].capacity_policy_id"
        )
        expected_pid = compute_capacity_policy_id(
            agent_id=agent_id,
            catalog_hash=p_hash,
            max_concurrent_jobs=p_cap,
            source_id=p["source_id"],
            source_version=p_version,
        )
        if expected_pid != p["capacity_policy_id"]:
            raise CapacityError(
                f"policy[{i}] capacity_policy_id mismatch: "
                f"expected {expected_pid}, got {p['capacity_policy_id']}"
            )
        _validate_snapshot_timestamp(p["created_at"], f"policy[{i}].created_at")
        _validate_snapshot_timestamp(p["updated_at"], f"policy[{i}].updated_at")


def _strict_validate_current_projection(conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Fail-closed validation of every capacity source and policy row.

    Returns the canonical sorted source and policy lists. Raises CapacityError on
    any structural violation, ownership conflict, orphan policy, or coverage drift.
    """
    sources = list_capacity_sources(conn)
    policies = list_capacity_policies(conn)
    all_typed = _all_typed_agent_ids(conn)
    enabled = _enabled_typed_agent_ids(conn)

    seen_source_ids: set[str] = set()
    source_by_id: dict[str, dict[str, Any]] = {}
    for i, s in enumerate(sources):
        if set(s.keys()) != _EXPECTED_SNAPSHOT_SOURCE_KEYS:
            raise CapacityError(
                f"current source[{i}] has unknown or missing keys: {sorted(s.keys())}"
            )
        sid = _validate_bounded_label(s["source_id"], f"current source[{i}].source_id")
        if sid in seen_source_ids:
            raise CapacityError(f"duplicate current source_id: {sid!r}")
        seen_source_ids.add(sid)
        source_by_id[sid] = s
        _validate_snapshot_source_version(s["source_version"], f"current source[{i}].source_version")
        _validate_snapshot_hash(s["catalog_hash"], f"current source[{i}].catalog_hash")
        _validate_source_path(s["source_path"], f"current source[{i}].source_path")
        _validate_snapshot_timestamp(s["updated_at"], f"current source[{i}].updated_at")

    seen_agent_ids: set[str] = set()
    union_agents: set[str] = set()
    for i, p in enumerate(policies):
        if set(p.keys()) != _EXPECTED_SNAPSHOT_POLICY_KEYS:
            raise CapacityError(
                f"current policy[{i}] has unknown or missing keys: {sorted(p.keys())}"
            )
        agent_id = _validate_bounded_label(p["agent_id"], f"current policy[{i}].agent_id")
        if agent_id in seen_agent_ids:
            raise CapacityError(f"duplicate current agent_id across policies: {agent_id!r}")
        seen_agent_ids.add(agent_id)
        union_agents.add(agent_id)
        sid = _validate_bounded_label(p["source_id"], f"current policy[{i}].source_id")
        source = source_by_id.get(sid)
        if source is None:
            raise CapacityError(f"orphan current policy: agent {agent_id!r} has no source {sid!r}")
        p_version = _validate_snapshot_source_version(
            p["source_version"], f"current policy[{i}].source_version"
        )
        if p_version != source["source_version"]:
            raise CapacityError(
                f"current policy[{i}] source_version mismatch: {p_version} != {source['source_version']}"
            )
        p_hash = _validate_snapshot_hash(p["catalog_hash"], f"current policy[{i}].catalog_hash")
        if p_hash != source["catalog_hash"]:
            raise CapacityError(f"current policy[{i}] catalog_hash mismatch")
        p_cap = _validate_max_concurrent_jobs(
            p["max_concurrent_jobs"], f"current policy[{i}].max_concurrent_jobs"
        )
        _validate_snapshot_policy_id(p["capacity_policy_id"], f"current policy[{i}].capacity_policy_id")
        expected_pid = compute_capacity_policy_id(
            agent_id=agent_id,
            catalog_hash=p_hash,
            max_concurrent_jobs=p_cap,
            source_id=sid,
            source_version=p_version,
        )
        if expected_pid != p["capacity_policy_id"]:
            raise CapacityError(
                f"current policy[{i}] capacity_policy_id mismatch: "
                f"expected {expected_pid}, got {p['capacity_policy_id']}"
            )
        _validate_snapshot_timestamp(p["created_at"], f"current policy[{i}].created_at")
        _validate_snapshot_timestamp(p["updated_at"], f"current policy[{i}].updated_at")
        if agent_id not in all_typed:
            raise CapacityError(
                f"current capacity present for unknown/untyped agent: {agent_id!r}"
            )

    missing = enabled - union_agents
    if missing:
        raise CapacityError(
            f"current coverage drift: enabled bindings {sorted(missing)} have no capacity policy"
        )

    return sources, policies


def _canonical_source_dict(source: dict[str, Any]) -> dict[str, Any]:
    """Canonical source object for the preserved_state witness."""
    return {
        "source_id": source["source_id"],
        "source_version": source["source_version"],
        "catalog_hash": source["catalog_hash"],
        "source_path": source["source_path"],
        "updated_at": source["updated_at"],
    }


def _canonical_policy_dict(policy: dict[str, Any]) -> dict[str, Any]:
    """Canonical policy object for the preserved_state witness."""
    return {
        "agent_id": policy["agent_id"],
        "source_id": policy["source_id"],
        "source_version": policy["source_version"],
        "catalog_hash": policy["catalog_hash"],
        "capacity_policy_id": policy["capacity_policy_id"],
        "max_concurrent_jobs": policy["max_concurrent_jobs"],
        "created_at": policy["created_at"],
        "updated_at": policy["updated_at"],
    }


def _build_preserved_state(
    sources: list[dict[str, Any]],
    policies: list[dict[str, Any]],
    target_source_id: str,
) -> dict[str, Any]:
    """Build deterministic non-target witness from current rows."""
    preserved_sources = sorted(
        (_canonical_source_dict(s) for s in sources if s["source_id"] != target_source_id),
        key=lambda s: s["source_id"],
    )
    preserved_policies = sorted(
        (_canonical_policy_dict(p) for p in policies if p["source_id"] != target_source_id),
        key=lambda p: p["agent_id"],
    )
    # Strictly increasing uniqueness is guaranteed by DB primary keys, but assert
    # for defense-in-depth since the witness is digest-bound.
    prev: str | None = None
    for s in preserved_sources:
        if prev is not None and s["source_id"] <= prev:
            raise CapacityError(f"preserved sources not strictly increasing: {s['source_id']!r}")
        prev = s["source_id"]
    prev = None
    for p in preserved_policies:
        if prev is not None and p["agent_id"] <= prev:
            raise CapacityError(f"preserved policies not strictly increasing: {p['agent_id']!r}")
        prev = p["agent_id"]
    return {"sources": preserved_sources, "policies": preserved_policies}


def _validate_preserved_state(preserved_state: Any) -> None:
    """Validate the shape and ordering of a preserved_state witness."""
    if not isinstance(preserved_state, dict):
        raise CapacityError("preserved_state must be an object")
    if set(preserved_state.keys()) != _EXPECTED_PRESERVED_STATE_KEYS:
        raise CapacityError(
            f"preserved_state has unknown or missing keys: {sorted(preserved_state.keys())}"
        )
    sources = preserved_state["sources"]
    policies = preserved_state["policies"]
    if not isinstance(sources, list):
        raise CapacityError("preserved_state.sources must be a list")
    if not isinstance(policies, list):
        raise CapacityError("preserved_state.policies must be a list")
    prev: str | None = None
    for i, s in enumerate(sources):
        if not isinstance(s, dict):
            raise CapacityError(f"preserved_state.source[{i}] must be an object")
        if set(s.keys()) != _EXPECTED_SNAPSHOT_SOURCE_KEYS:
            raise CapacityError(
                f"preserved_state.source[{i}] has unknown or missing keys: {sorted(s.keys())}"
            )
        sid = _validate_bounded_label(s["source_id"], f"preserved_state.source[{i}].source_id")
        if prev is not None and sid <= prev:
            raise CapacityError(
                f"preserved_state.sources not strictly increasing at index {i}: {sid!r} <= {prev!r}"
            )
        prev = sid
        _validate_snapshot_source_version(s["source_version"], f"preserved_state.source[{i}].source_version")
        _validate_snapshot_hash(s["catalog_hash"], f"preserved_state.source[{i}].catalog_hash")
        _validate_source_path(s["source_path"], f"preserved_state.source[{i}].source_path")
        _validate_snapshot_timestamp(s["updated_at"], f"preserved_state.source[{i}].updated_at")
    prev = None
    for i, p in enumerate(policies):
        if not isinstance(p, dict):
            raise CapacityError(f"preserved_state.policy[{i}] must be an object")
        if set(p.keys()) != _EXPECTED_SNAPSHOT_POLICY_KEYS:
            raise CapacityError(
                f"preserved_state.policy[{i}] has unknown or missing keys: {sorted(p.keys())}"
            )
        agent_id = _validate_bounded_label(p["agent_id"], f"preserved_state.policy[{i}].agent_id")
        if prev is not None and agent_id <= prev:
            raise CapacityError(
                f"preserved_state.policies not strictly increasing at index {i}: {agent_id!r} <= {prev!r}"
            )
        prev = agent_id
        _validate_bounded_label(p["source_id"], f"preserved_state.policy[{i}].source_id")
        _validate_snapshot_source_version(p["source_version"], f"preserved_state.policy[{i}].source_version")
        _validate_snapshot_hash(p["catalog_hash"], f"preserved_state.policy[{i}].catalog_hash")
        _validate_snapshot_policy_id(p["capacity_policy_id"], f"preserved_state.policy[{i}].capacity_policy_id")
        _validate_max_concurrent_jobs(p["max_concurrent_jobs"], f"preserved_state.policy[{i}].max_concurrent_jobs")
        _validate_snapshot_timestamp(p["created_at"], f"preserved_state.policy[{i}].created_at")
        _validate_snapshot_timestamp(p["updated_at"], f"preserved_state.policy[{i}].updated_at")


def _build_snapshot_envelope(
    captured_state: dict[str, Any] | None,
    preserved_state: dict[str, Any],
    target_source_id: str,
) -> tuple[dict[str, Any], bytes]:
    """Build the v2 canonical snapshot envelope and its UTF-8 bytes."""
    inner_snapshot = {
        "contract_version": SNAPSHOT_CONTRACT_VERSION,
        "target_source_id": target_source_id,
        "captured_state": captured_state,
        "preserved_state": preserved_state,
    }
    inner_bytes = _snapshot_canonical_bytes(inner_snapshot)
    digest = hashlib.sha256(inner_bytes).hexdigest()
    envelope = {
        "snapshot": inner_snapshot,
        "snapshot_sha256": digest,
    }
    return envelope, _snapshot_canonical_bytes(envelope)


def _atomic_write_snapshot(path: Path, snapshot_bytes: bytes) -> None:
    """Write snapshot_bytes to path atomically with mode 0600.

    On any post-replace failure (e.g. ``os.chmod``), the final output is removed.
    The caller's capture transaction is independently responsible for rollback.
    """
    path = path.expanduser()
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=parent, prefix=".capacity-snapshot-", suffix=".tmp")
    tmp_path = Path(tmp_path)
    replaced = False
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(snapshot_bytes)
        os.replace(str(tmp_path), str(path))
        replaced = True
        os.chmod(str(path), 0o600)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        if replaced:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
        raise


def _assert_exact_state_match(
    conn: sqlite3.Connection, expected_state: dict[str, Any]
) -> None:
    """Re-read the DB and assert it matches the expected captured_state exactly."""
    expected_source = expected_state["source"]
    source_id = expected_source["source_id"]
    actual_source = get_capacity_source(conn, source_id)
    if actual_source is None:
        raise CapacityError("restore verification failed: source missing after restore")
    if (
        actual_source["source_version"] != expected_source["source_version"]
        or actual_source["catalog_hash"] != expected_source["catalog_hash"]
        or actual_source["source_path"] != expected_source["source_path"]
        or actual_source["updated_at"] != expected_source["updated_at"]
    ):
        raise CapacityError(
            f"restore verification failed: source mismatch: expected {expected_source!r}, got {actual_source!r}"
        )

    expected_policies = {p["agent_id"]: p for p in expected_state["policies"]}
    actual_policies = {p["agent_id"]: p for p in list_capacity_policies(conn, source_id)}
    if set(expected_policies) != set(actual_policies):
        raise CapacityError(
            "restore verification failed: policy set mismatch: "
            f"expected {sorted(expected_policies)!r}, got {sorted(actual_policies)!r}"
        )
    for agent_id, expected_policy in expected_policies.items():
        actual_policy = actual_policies[agent_id]
        for key in (
            "agent_id",
            "source_id",
            "source_version",
            "catalog_hash",
            "capacity_policy_id",
            "max_concurrent_jobs",
            "created_at",
            "updated_at",
        ):
            if actual_policy[key] != expected_policy[key]:
                raise CapacityError(
                    f"restore verification failed: policy {agent_id!r} {key} mismatch: "
                    f"expected {expected_policy[key]!r}, got {actual_policy[key]!r}"
                )


def _assert_exact_witness_match(
    conn: sqlite3.Connection,
    expected_witness: dict[str, Any],
    target_source_id: str,
) -> None:
    """Re-read non-target rows and assert they match the witness exactly."""
    actual_sources = list_capacity_sources(conn)
    actual_policies = list_capacity_policies(conn)
    actual_witness = _build_preserved_state(actual_sources, actual_policies, target_source_id)
    if actual_witness != expected_witness:
        raise CapacityError(
            f"restore verification failed: witness mismatch: expected {expected_witness!r}, got {actual_witness!r}"
        )


def _active_lease_count(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE status = 'active'"
    ).fetchone()["n"]


def capture_capacity_snapshot(
    conn: sqlite3.Connection,
    target_source_id: str,
    output_path: str | Path,
) -> dict[str, Any]:
    """Capture a capacity-only snapshot of the expected target source.

    The snapshot is a digest-bound JSON envelope. It is written atomically with
    mode 0600. The caller must not hold a transaction.
    """
    _validate_bounded_label(target_source_id, "target_source_id")
    output_path = Path(output_path)
    conn.execute("BEGIN IMMEDIATE")
    file_written = False
    try:
        # Read and strictly validate the complete current projection.
        all_sources, all_policies = _strict_validate_current_projection(conn)

        target_source = next((s for s in all_sources if s["source_id"] == target_source_id), None)
        target_policies = [p for p in all_policies if p["source_id"] == target_source_id]

        if target_source is None:
            if target_policies:
                raise CapacityError(
                    f"target source absent but {len(target_policies)} target polic(ies) exist"
                )
            captured_state = None
        else:
            captured_policies = sorted(
                (_canonical_policy_dict(p) for p in target_policies),
                key=lambda p: p["agent_id"],
            )
            captured_state = {
                "source": _canonical_source_dict(target_source),
                "policies": captured_policies,
            }
            _strict_validate_captured_state(captured_state, target_source_id)

        preserved_state = _build_preserved_state(all_sources, all_policies, target_source_id)
        envelope, envelope_bytes = _build_snapshot_envelope(
            captured_state, preserved_state, target_source_id
        )
        _atomic_write_snapshot(output_path, envelope_bytes)
        file_written = True
        conn.commit()
        return envelope
    except Exception:
        conn.rollback()
        if file_written:
            _safe_unlink(output_path)
        raise


def _parse_restore_request(
    raw: bytes,
    target_source_id: str,
) -> tuple[int, dict[str, Any] | None, dict[str, Any], dict[str, Any]]:
    """Parse and validate a snapshot envelope before any DB mutation.

    Returns (contract_version, captured_state, witness_state, envelope).
    """
    try:
        envelope = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise CapacityError(f"snapshot is malformed JSON: {exc}") from exc
    if not isinstance(envelope, dict):
        raise CapacityError("snapshot envelope must be a JSON object")
    if raw != _snapshot_canonical_bytes(envelope):
        raise CapacityError("snapshot raw bytes are not canonical")
    if set(envelope.keys()) != _EXPECTED_ENVELOPE_KEYS:
        raise CapacityError(
            f"snapshot envelope has unknown or missing keys: {sorted(envelope.keys())}"
        )
    _validate_snapshot_hash(envelope["snapshot_sha256"], "snapshot_sha256")
    snapshot = envelope["snapshot"]
    if not isinstance(snapshot, dict):
        raise CapacityError("snapshot.snapshot must be a JSON object")

    contract_version = snapshot.get("contract_version")
    if type(contract_version) is not int or contract_version not in (1, 2):
        raise CapacityError(
            f"snapshot contract_version must be 1 or 2: got {contract_version!r}"
        )
    if contract_version == 1:
        if set(snapshot.keys()) != _EXPECTED_INNER_SNAPSHOT_KEYS_V1:
            raise CapacityError(
                f"v1 snapshot.snapshot has unknown or missing keys: {sorted(snapshot.keys())}"
            )
        witness_state: dict[str, Any] = {"sources": [], "policies": []}
    else:
        if set(snapshot.keys()) != _EXPECTED_INNER_SNAPSHOT_KEYS_V2:
            raise CapacityError(
                f"v2 snapshot.snapshot has unknown or missing keys: {sorted(snapshot.keys())}"
            )
        witness_state = snapshot["preserved_state"]
        _validate_preserved_state(witness_state)

    inner_target = snapshot["target_source_id"]
    if inner_target != target_source_id:
        raise CapacityError(
            f"snapshot target_source_id mismatch: expected {target_source_id!r}, "
            f"got {inner_target!r}"
        )
    _validate_bounded_label(inner_target, "snapshot.target_source_id")

    inner_snapshot = {
        "contract_version": snapshot["contract_version"],
        "target_source_id": snapshot["target_source_id"],
        "captured_state": snapshot["captured_state"],
    }
    if contract_version == 2:
        inner_snapshot["preserved_state"] = snapshot["preserved_state"]
    expected_digest = hashlib.sha256(_snapshot_canonical_bytes(inner_snapshot)).hexdigest()
    if envelope["snapshot_sha256"] != expected_digest:
        raise CapacityError(
            f"snapshot digest mismatch: expected {expected_digest}, "
            f"got {envelope['snapshot_sha256']!r}"
        )

    captured_state = snapshot["captured_state"]
    _strict_validate_captured_state(captured_state, target_source_id)
    return contract_version, captured_state, witness_state, envelope


def restore_capacity_snapshot(
    conn: sqlite3.Connection,
    target_source_id: str,
    snapshot_path: str | Path,
) -> dict[str, Any]:
    """Restore capacity projection from a digest-bound snapshot.

    Owns a BEGIN IMMEDIATE transaction. Fails closed (rollback) on any mismatch,
    active lease, unexpected source, or restore-self failure.
    """
    _validate_bounded_label(target_source_id, "target_source_id")
    snapshot_path = Path(snapshot_path).expanduser()
    raw = snapshot_path.read_bytes()
    contract_version, captured_state, witness_state, envelope = _parse_restore_request(raw, target_source_id)

    conn.execute("BEGIN IMMEDIATE")
    try:
        # Strictly validate the complete current projection before any mutation.
        current_sources, current_policies = _strict_validate_current_projection(conn)

        # Reject any active lease on any source.
        if _active_lease_count(conn) != 0:
            raise CapacityError(
                "cannot restore capacity snapshot while active lease(s) exist"
            )

        # v1 contract predates multi-source awareness.
        if contract_version == 1:
            non_target_exists = any(s["source_id"] != target_source_id for s in current_sources)
            non_target_policies = any(p["source_id"] != target_source_id for p in current_policies)
            if non_target_exists or non_target_policies:
                raise CapacityError(
                    "v1 snapshot cannot restore a multi-source capacity projection"
                )

        # Build canonical witness from current non-target rows.
        current_witness = _build_preserved_state(current_sources, current_policies, target_source_id)

        # Witness equality: any drift in non-target rows is a loud fail-closed error.
        if current_witness != witness_state:
            raise CapacityError(
                "witness mismatch: non-target capacity state changed since snapshot"
            )

        # Construct proposed post-restore union.
        proposed_target_agents = {p["agent_id"] for p in (captured_state["policies"] if captured_state else [])}
        proposed_witness_agents = {p["agent_id"] for p in witness_state["policies"]}
        proposed_union_agents = proposed_target_agents | proposed_witness_agents
        all_typed = _all_typed_agent_ids(conn)
        enabled = _enabled_typed_agent_ids(conn)

        # Every proposed policy agent must be a typed binding.
        unknown = proposed_union_agents - all_typed
        if unknown:
            raise CapacityError(
                f"snapshot present for unknown/untyped agents: {sorted(unknown)}"
            )

        # Every enabled typed binding must be covered by the proposed union.
        missing = enabled - proposed_union_agents
        if missing:
            raise CapacityError(
                f"snapshot coverage drift: missing from proposed union: {sorted(missing)}"
            )

        # No target captured policy may claim an agent owned by a preserved source.
        preserved_owners = {p["agent_id"]: p["source_id"] for p in witness_state["policies"]}
        for agent_id in proposed_target_agents:
            if agent_id in preserved_owners:
                raise CapacityError(
                    f"snapshot target claims agent {agent_id!r} owned by preserved source {preserved_owners[agent_id]!r}"
                )

        # Safe to mutate: delete target only.
        conn.execute(
            "DELETE FROM executor_capacity_policies WHERE source_id = ?",
            (target_source_id,),
        )
        conn.execute(
            "DELETE FROM executor_capacity_sources WHERE source_id = ?",
            (target_source_id,),
        )
        if captured_state is not None:
            source = captured_state["source"]
            policies = captured_state["policies"]
            conn.execute(
                """
                INSERT INTO executor_capacity_sources (
                  source_id, source_version, catalog_hash, source_path, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    source["source_id"],
                    source["source_version"],
                    source["catalog_hash"],
                    source["source_path"],
                    source["updated_at"],
                ),
            )
            for p in policies:
                conn.execute(
                    """
                    INSERT INTO executor_capacity_policies (
                      agent_id, source_id, source_version, catalog_hash, capacity_policy_id,
                      max_concurrent_jobs, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        p["agent_id"],
                        p["source_id"],
                        p["source_version"],
                        p["catalog_hash"],
                        p["capacity_policy_id"],
                        p["max_concurrent_jobs"],
                        p["created_at"],
                        p["updated_at"],
                    ),
                )

        # Post-write verification before commit.
        if captured_state is not None:
            _assert_exact_state_match(conn, captured_state)
        else:
            if get_capacity_source(conn, target_source_id) is not None:
                raise CapacityError("restore verification failed: target source still present")
        _assert_exact_witness_match(conn, witness_state, target_source_id)
        if _active_lease_count(conn) != 0:
            raise CapacityError("restore verification failed: active lease appeared during restore")

        conn.commit()
        return envelope
    except Exception:
        conn.rollback()
        raise
