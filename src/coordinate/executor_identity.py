"""Executor identity catalog: canonical v1 catalog and binding snapshots.

This module owns the Coordinate-side projection of the single source-controlled
authority file (``config/agent-registry.toml``) into durable v12 schema tables.
It does not import MultiNexus code.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from coordinate.db_support import _json_dumps, utc_now


EXECUTOR_CONTRACT_VERSION = 1
MAX_LABEL_LEN = 64
MAX_ID_LEN = 256
MAX_CAPABILITY_LEN = 64
MAX_CAPABILITIES = 32

# Bounded identity labels: no path separators, shell metacharacters, whitespace,
# or control characters.  This deliberately excludes ``/``, ``\``, ``:``, ``;``,
# ``|``, ``&``, ``$``, backticks, parentheses, braces, brackets, ``<``, ``>``,
# ``*``, ``?``, quotes, and spaces.
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_BINDING_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

_ALLOWED_ROOT_KEYS = {"registry", "agents", "external_agents", "executor_definitions", "capacity_registry", "executor_capacities"}
_ALLOWED_REGISTRY_KEYS = {"id", "version"}
_ALLOWED_DEFINITION_KEYS = {"id", "provider", "adapter", "capabilities"}
_ALLOWED_AGENT_KEYS = {
    "id",
    "display_name",
    "discord_user_id",
    "executor_definition_id",
    "runner_profile_id",
    "enabled",
}


class ExecutorIdentityError(ValueError):
    """Raised when the executor catalog or a binding snapshot is invalid."""


def _field_mismatch_message(
    label: str, actual_keys: set[Any], expected_keys: set[str]
) -> str:
    missing = sorted(expected_keys - actual_keys)
    unexpected_count = len(actual_keys - expected_keys)
    return (
        f"{label} has incorrect fields: missing={missing}, "
        f"unexpected_count={unexpected_count}, total_count={len(actual_keys)}"
    )


@dataclass(frozen=True)
class ExecutorDefinition:
    id: str
    provider: str
    adapter: str
    capabilities: tuple[str, ...]

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "provider": self.provider,
            "adapter": self.adapter,
            "capabilities": list(self.capabilities),
        }


@dataclass(frozen=True)
class ExecutorInstanceBinding:
    agent_id: str
    executor_definition_id: str
    runner_profile_id: str
    enabled: bool = True

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "executor_definition_id": self.executor_definition_id,
            "runner_profile_id": self.runner_profile_id,
            "enabled": self.enabled,
        }


@dataclass(frozen=True)
class ExecutorCatalog:
    source_id: str
    source_version: int
    catalog_hash: str
    source_path: str | None
    definitions: tuple[ExecutorDefinition, ...]
    bindings: tuple[ExecutorInstanceBinding, ...]


def _canonical_json(value: dict[str, Any]) -> str:
    """Deterministic JSON used for digests and byte-identical fixtures."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now() -> str:
    return utc_now()


def _validate_bounded_label(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ExecutorIdentityError(f"{label} must be a string")
    if value != value.strip():
        raise ExecutorIdentityError(f"{label} must not have surrounding whitespace")
    if not value:
        raise ExecutorIdentityError(f"{label} is required")
    if len(value) > MAX_LABEL_LEN:
        raise ExecutorIdentityError(f"{label} exceeds {MAX_LABEL_LEN} characters")
    if not _SAFE_LABEL_RE.match(value):
        raise ExecutorIdentityError(
            f"{label} contains unsafe characters: {value!r}"
        )
    return value


def _validate_id(value: Any, label: str) -> str:
    # Catalog ids reuse the same bounded label grammar as adapter/provider labels.
    return _validate_bounded_label(value, label)


def _validate_capabilities(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ExecutorIdentityError(f"{label} must be a list")
    if len(value) > MAX_CAPABILITIES:
        raise ExecutorIdentityError(f"{label} exceeds {MAX_CAPABILITIES} items")
    seen: set[str] = set()
    normalized: list[str] = []
    for item in value:
        cap = _validate_bounded_label(item, f"{label} item")
        if len(cap) > MAX_CAPABILITY_LEN:
            raise ExecutorIdentityError(f"{label} item exceeds {MAX_CAPABILITY_LEN} characters")
        if cap in seen:
            raise ExecutorIdentityError(f"{label} contains duplicate capability: {cap!r}")
        seen.add(cap)
        normalized.append(cap)
    if normalized != sorted(normalized):
        raise ExecutorIdentityError(f"{label} must be sorted")
    return tuple(normalized)


def _validate_boolish(value: Any, label: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ExecutorIdentityError(f"{label} must be a boolean")


def canonical_executor_catalog_dict(catalog: ExecutorCatalog) -> dict[str, Any]:
    """Return the exact canonical object whose UTF-8 JSON is hashed.

    The root contains ``contract_version``, ``source_id``, ``source_version``,
    ``executor_definitions`` and ``executor_instance_bindings``.  Definitions are
    sorted by ``id`` and contain exactly ``id``, ``provider``, ``adapter`` and
    ``capabilities``.  Bindings are sorted by ``agent_id`` and contain exactly
    ``agent_id``, ``executor_definition_id``, ``runner_profile_id`` and
    ``enabled``.
    """
    definitions = sorted(
        [d.canonical_dict() for d in catalog.definitions],
        key=lambda d: d["id"],
    )
    bindings = sorted(
        [b.canonical_dict() for b in catalog.bindings],
        key=lambda b: b["agent_id"],
    )
    return {
        "contract_version": EXECUTOR_CONTRACT_VERSION,
        "source_id": catalog.source_id,
        "source_version": catalog.source_version,
        "executor_definitions": definitions,
        "executor_instance_bindings": bindings,
    }


def compute_executor_catalog_hash(catalog: ExecutorCatalog) -> str:
    """SHA-256 of the canonical UTF-8 JSON catalog bytes."""
    canonical = _canonical_json(canonical_executor_catalog_dict(catalog))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _binding_snapshot_canonical_dict(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Canonical form of a binding snapshot used for its own digest.

    The ``binding_id`` field is excluded because it is a self-referential digest.
    """
    keys = {
        "contract_version",
        "source_id",
        "source_version",
        "catalog_hash",
        "executor_definition_id",
        "executor_instance_id",
        "runner_profile_id",
        "provider",
        "adapter",
        "capabilities",
    }
    if set(snapshot.keys()) - {"binding_id"} != keys:
        raise ExecutorIdentityError(_field_mismatch_message(
            "binding snapshot", set(snapshot.keys()) - {"binding_id"}, keys
        ))
    return {k: snapshot[k] for k in sorted(keys)}


def compute_executor_binding_id(snapshot: dict[str, Any]) -> str:
    """Return ``sha256:<digest>`` for a binding snapshot (excluding its own id)."""
    canonical = _canonical_json(_binding_snapshot_canonical_dict(snapshot))
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _build_executor_binding_snapshot(
    *,
    source_id: str,
    source_version: int,
    catalog_hash: str,
    executor_definition_id: str,
    executor_instance_id: str,
    runner_profile_id: str,
    provider: str,
    adapter: str,
    capabilities: tuple[str, ...],
) -> dict[str, Any]:
    """Create a complete snapshot dict including its self-referential binding_id."""
    snapshot: dict[str, Any] = {
        "contract_version": EXECUTOR_CONTRACT_VERSION,
        "source_id": source_id,
        "source_version": source_version,
        "catalog_hash": catalog_hash,
        "executor_definition_id": executor_definition_id,
        "executor_instance_id": executor_instance_id,
        "runner_profile_id": runner_profile_id,
        "provider": provider,
        "adapter": adapter,
        "capabilities": list(capabilities),
    }
    snapshot["binding_id"] = compute_executor_binding_id(snapshot)
    return snapshot


def parse_executor_catalog(source: str | Path) -> ExecutorCatalog:
    """Parse the executor projection from a TOML authority file.

    The roster projection is ignored except to locate managed agents carrying
    executor bindings.  Unknown root keys, secret-bearing keys, malformed
    labels, and unsafe payloads raise ``ExecutorIdentityError``.
    """
    path = Path(source).expanduser()
    with open(path, "rb") as f:
        data = tomllib.load(f)

    unknown_root = set(data.keys()) - _ALLOWED_ROOT_KEYS
    if unknown_root:
        raise ExecutorIdentityError(f"unknown root keys: {sorted(unknown_root)}")

    registry = data.get("registry")
    if not isinstance(registry, dict):
        raise ExecutorIdentityError("missing [registry] metadata")
    unknown_registry = set(registry.keys()) - _ALLOWED_REGISTRY_KEYS
    if unknown_registry:
        raise ExecutorIdentityError(f"unknown [registry] keys: {sorted(unknown_registry)}")

    source_id = str(registry.get("id", "")).strip()
    if not source_id:
        raise ExecutorIdentityError("[registry].id is required")
    _validate_id(source_id, "[registry].id")

    version = registry.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ExecutorIdentityError("[registry].version must be a non-negative integer")
    source_version = version

    definitions: list[ExecutorDefinition] = []
    seen_def_ids: set[str] = set()
    for raw in data.get("executor_definitions", []):
        if not isinstance(raw, dict):
            raise ExecutorIdentityError("executor_definitions entry must be a table")
        unknown = set(raw.keys()) - _ALLOWED_DEFINITION_KEYS
        if unknown:
            raise ExecutorIdentityError(
                f"unknown keys in executor_definitions entry: {sorted(unknown)}"
            )
        def_id = _validate_id(raw.get("id"), "executor_definition.id")
        if def_id in seen_def_ids:
            raise ExecutorIdentityError(f"duplicate executor_definition id: {def_id!r}")
        seen_def_ids.add(def_id)
        provider = _validate_bounded_label(raw.get("provider"), "executor_definition.provider")
        adapter = _validate_bounded_label(raw.get("adapter"), "executor_definition.adapter")
        capabilities = _validate_capabilities(raw.get("capabilities", []), "executor_definition.capabilities")
        definitions.append(ExecutorDefinition(
            id=def_id,
            provider=provider,
            adapter=adapter,
            capabilities=capabilities,
        ))

    definition_ids = {d.id for d in definitions}

    bindings: list[ExecutorInstanceBinding] = []
    seen_agent_ids: set[str] = set()

    def _process_agent_entry(raw: dict[str, Any], agent_type: str) -> None:
        agent_id = _validate_id(raw.get("id"), f"{agent_type}.id")
        unknown = set(raw.keys()) - _ALLOWED_AGENT_KEYS
        if unknown:
            raise ExecutorIdentityError(
                f"unknown keys in {agent_type} entry '{agent_id}': {sorted(unknown)}"
            )
        executor_definition_id = raw.get("executor_definition_id")
        runner_profile_id = raw.get("runner_profile_id")

        if agent_type == "external_agents":
            if (
                executor_definition_id is not None
                or runner_profile_id is not None
                or "enabled" in raw
            ):
                raise ExecutorIdentityError(
                    f"external agent '{agent_id}' must not carry executor bindings"
                )
            return

        if executor_definition_id is None and runner_profile_id is None:
            return
        if executor_definition_id is None or runner_profile_id is None:
            raise ExecutorIdentityError(
                f"agent '{agent_id}' must set both executor_definition_id and runner_profile_id"
            )
        executor_definition_id = _validate_id(
            executor_definition_id, f"agent '{agent_id}'.executor_definition_id"
        )
        runner_profile_id = _validate_id(
            runner_profile_id, f"agent '{agent_id}'.runner_profile_id"
        )
        if executor_definition_id not in definition_ids:
            raise ExecutorIdentityError(
                f"agent '{agent_id}' references unknown executor_definition_id: {executor_definition_id!r}"
            )
        if runner_profile_id != agent_id:
            raise ExecutorIdentityError(
                f"agent '{agent_id}' runner_profile_id must equal agent_id in P9-2A: got {runner_profile_id!r}"
            )
        if agent_id in seen_agent_ids:
            raise ExecutorIdentityError(f"duplicate agent id in executor bindings: {agent_id!r}")
        seen_agent_ids.add(agent_id)
        enabled = _validate_boolish(raw.get("enabled", True), f"agent '{agent_id}'.enabled")
        bindings.append(ExecutorInstanceBinding(
            agent_id=agent_id,
            executor_definition_id=executor_definition_id,
            runner_profile_id=runner_profile_id,
            enabled=enabled,
        ))

    for raw in data.get("agents", []):
        _process_agent_entry(raw, "agents")
    for raw in data.get("external_agents", []):
        _process_agent_entry(raw, "external_agents")

    catalog = ExecutorCatalog(
        source_id=source_id,
        source_version=source_version,
        catalog_hash="",
        source_path=str(path),
        definitions=tuple(definitions),
        bindings=tuple(bindings),
    )
    catalog_hash = compute_executor_catalog_hash(catalog)
    return ExecutorCatalog(
        source_id=catalog.source_id,
        source_version=catalog.source_version,
        catalog_hash=catalog_hash,
        source_path=catalog.source_path,
        definitions=catalog.definitions,
        bindings=catalog.bindings,
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


def _has_in_flight_typed_jobs(conn: sqlite3.Connection, source_id: str) -> bool:
    """True if any pending/running job carries a typed binding for this source."""
    row = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE status IN ('pending', 'running')
          AND json_extract(payload_json, '$.executor_binding.source_id') = ?
        LIMIT 1
        """,
        (source_id,),
    ).fetchone()
    return row is not None


def sync_executor_catalog(
    conn: sqlite3.Connection,
    catalog: ExecutorCatalog,
    *,
    source_path: str | None = None,
    synced_by: str = "operator",
) -> dict[str, Any]:
    """Atomically sync one executor catalog source.

    - Same-version/same-hash syncs are idempotent and allowed even when typed
      jobs are in flight.
    - Same-version/different-hash and version downgrades fail with zero mutation.
    - Takeover of definition ids or agent bindings owned by another source fails
      with zero mutation.
    - Catalog changes are refused while any pending or running typed job for the
      source exists; the caller must drain or terminally resolve those jobs.
    - References to missing agents or runner profiles fail with zero mutation.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = _now()
        existing = conn.execute(
            "SELECT source_id, source_version, catalog_hash FROM executor_catalog_sources WHERE source_id = ?",
            (catalog.source_id,),
        ).fetchone()

        if existing is not None:
            if catalog.source_version < existing["source_version"]:
                raise ExecutorIdentityError(
                    f"catalog version downgrade for {catalog.source_id!r}: "
                    f"{catalog.source_version} < {existing['source_version']}"
                )
            if (
                catalog.source_version == existing["source_version"]
                and catalog.catalog_hash != existing["catalog_hash"]
            ):
                raise ExecutorIdentityError(
                    f"catalog hash changed without version bump for {catalog.source_id!r}"
                )
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
                    "added_definition_ids": [],
                    "updated_definition_ids": [],
                    "removed_definition_ids": [],
                    "unchanged_definition_ids": sorted(d.id for d in catalog.definitions),
                    "added_binding_ids": [],
                    "updated_binding_ids": [],
                    "removed_binding_ids": [],
                    "unchanged_binding_ids": sorted(b.agent_id for b in catalog.bindings),
                }

        # Ownership takeover guard.
        if catalog.definitions:
            placeholders = ",".join("?" for _ in catalog.definitions)
            rows = conn.execute(
                f"SELECT id, source_id FROM executor_definitions WHERE id IN ({placeholders})",
                tuple(d.id for d in catalog.definitions),
            ).fetchall()
            for row in rows:
                if row["source_id"] != catalog.source_id:
                    raise ExecutorIdentityError(
                        f"executor_definition id {row['id']!r} is owned by source {row['source_id']!r}"
                    )
        if catalog.bindings:
            placeholders = ",".join("?" for _ in catalog.bindings)
            rows = conn.execute(
                f"SELECT agent_id, source_id FROM executor_instance_bindings WHERE agent_id IN ({placeholders})",
                tuple(b.agent_id for b in catalog.bindings),
            ).fetchall()
            for row in rows:
                if row["source_id"] != catalog.source_id:
                    raise ExecutorIdentityError(
                        f"executor_instance_binding {row['agent_id']!r} is owned by source {row['source_id']!r}"
                    )

        # Reference validation.
        for binding in catalog.bindings:
            agent = conn.execute(
                "SELECT id, client_type FROM agents WHERE id = ?", (binding.agent_id,)
            ).fetchone()
            if agent is None:
                raise ExecutorIdentityError(
                    f"executor binding references unknown agent: {binding.agent_id!r}"
                )
            if agent["client_type"] != "agentd":
                raise ExecutorIdentityError(
                    f"executor binding references non-agentd instance: {binding.agent_id!r}"
                )
            profile = conn.execute(
                "SELECT id, runner_type FROM runner_profiles WHERE id = ?",
                (binding.runner_profile_id,),
            ).fetchone()
            if profile is None:
                raise ExecutorIdentityError(
                    f"executor binding references unknown runner profile: {binding.runner_profile_id!r}"
                )
            if profile["runner_type"] != "agentd":
                raise ExecutorIdentityError(
                    "executor binding references non-agentd runner profile: "
                    f"{binding.runner_profile_id!r}"
                )

        # In-flight typed-job guard.  Not needed for idempotent same-hash syncs
        # (handled above), but required before any actual mutation.
        if _has_in_flight_typed_jobs(conn, catalog.source_id):
            raise ExecutorIdentityError(
                f"catalog {catalog.source_id!r} has in-flight typed jobs; drain or resolve before sync"
            )

        old_defs = {
            row["id"]: {
                "id": row["id"],
                "provider": row["provider"],
                "adapter": row["adapter"],
                "capabilities": json.loads(row["capabilities_json"]),
            }
            for row in conn.execute(
                "SELECT id, provider, adapter, capabilities_json "
                "FROM executor_definitions WHERE source_id = ?",
                (catalog.source_id,),
            ).fetchall()
        }
        new_defs = {d.id: d.canonical_dict() for d in catalog.definitions}
        def_added, def_updated, def_removed, def_unchanged = _dict_diff(old_defs, new_defs)

        old_bindings = {
            row["agent_id"]: {
                "agent_id": row["agent_id"],
                "executor_definition_id": row["executor_definition_id"],
                "runner_profile_id": row["runner_profile_id"],
                "enabled": bool(row["enabled"]),
            }
            for row in conn.execute(
                "SELECT agent_id, executor_definition_id, runner_profile_id, enabled "
                "FROM executor_instance_bindings WHERE source_id = ?",
                (catalog.source_id,),
            ).fetchall()
        }
        new_bindings = {b.agent_id: b.canonical_dict() for b in catalog.bindings}
        bind_added, bind_updated, bind_removed, bind_unchanged = _dict_diff(
            old_bindings, new_bindings
        )

        # Upsert the source row first so child inserts can reference it.
        conn.execute(
            """
            INSERT INTO executor_catalog_sources (source_id, source_version, catalog_hash, source_path, updated_at)
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

        conn.execute(
            "DELETE FROM executor_instance_bindings WHERE source_id = ?",
            (catalog.source_id,),
        )
        conn.execute(
            "DELETE FROM executor_definitions WHERE source_id = ?",
            (catalog.source_id,),
        )

        for d in catalog.definitions:
            conn.execute(
                """
                INSERT INTO executor_definitions (
                  id, source_id, provider, adapter, capabilities_json, metadata_json,
                  created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    d.id,
                    catalog.source_id,
                    d.provider,
                    d.adapter,
                    _json_dumps(list(d.capabilities)),
                    _json_dumps({}),
                    now,
                    now,
                ),
            )

        for b in catalog.bindings:
            conn.execute(
                """
                INSERT INTO executor_instance_bindings (
                  agent_id, source_id, executor_definition_id, runner_profile_id,
                  enabled, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    b.agent_id,
                    catalog.source_id,
                    b.executor_definition_id,
                    b.runner_profile_id,
                    1 if b.enabled else 0,
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
        "added_definition_ids": sorted(def_added),
        "updated_definition_ids": sorted(def_updated),
        "removed_definition_ids": sorted(def_removed),
        "unchanged_definition_ids": sorted(def_unchanged),
        "added_binding_ids": sorted(bind_added),
        "updated_binding_ids": sorted(bind_updated),
        "removed_binding_ids": sorted(bind_removed),
        "unchanged_binding_ids": sorted(bind_unchanged),
    }


def get_executor_catalog_source(
    conn: sqlite3.Connection, source_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT source_id, source_version, catalog_hash, source_path, updated_at "
        "FROM executor_catalog_sources WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    return dict(row) if row else None


def list_executor_catalog_sources(
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT source_id, source_version, catalog_hash, source_path, updated_at "
        "FROM executor_catalog_sources ORDER BY source_id"
    ).fetchall()
    return [dict(row) for row in rows]


def list_executor_definitions(
    conn: sqlite3.Connection, source_id: str | None = None
) -> list[dict[str, Any]]:
    if source_id is not None:
        rows = conn.execute(
            "SELECT id, source_id, provider, adapter, capabilities_json, metadata_json, created_at, updated_at "
            "FROM executor_definitions WHERE source_id = ? ORDER BY id",
            (source_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, source_id, provider, adapter, capabilities_json, metadata_json, created_at, updated_at "
            "FROM executor_definitions ORDER BY id"
        ).fetchall()
    return [dict(row) for row in rows]


def list_executor_instance_bindings(
    conn: sqlite3.Connection, source_id: str | None = None
) -> list[dict[str, Any]]:
    if source_id is not None:
        rows = conn.execute(
            "SELECT agent_id, source_id, executor_definition_id, runner_profile_id, enabled, created_at, updated_at "
            "FROM executor_instance_bindings WHERE source_id = ? ORDER BY agent_id",
            (source_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT agent_id, source_id, executor_definition_id, runner_profile_id, enabled, created_at, updated_at "
            "FROM executor_instance_bindings ORDER BY agent_id"
        ).fetchall()
    return [
        {**dict(row), "enabled": bool(row["enabled"])} for row in rows
    ]


def get_executor_instance_binding(
    conn: sqlite3.Connection, agent_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT agent_id, source_id, executor_definition_id, runner_profile_id, enabled, created_at, updated_at "
        "FROM executor_instance_bindings WHERE agent_id = ?",
        (agent_id,),
    ).fetchone()
    if row is None:
        return None
    return {**dict(row), "enabled": bool(row["enabled"])}


def resolve_exact_executor_binding(
    conn: sqlite3.Connection,
    agent_id: str,
) -> dict[str, Any] | None:
    """Return the current v1 binding snapshot for an agent, or None if untyped.

    Raises ``ExecutorIdentityError`` if the catalog binding is internally
    inconsistent (missing definition, profile mismatch, etc.).
    """
    binding = get_executor_instance_binding(conn, agent_id)
    if binding is None or not binding["enabled"]:
        return None

    if binding["runner_profile_id"] != agent_id:
        raise ExecutorIdentityError(
            f"binding for {agent_id!r} has runner_profile_id {binding['runner_profile_id']!r} != agent_id"
        )

    profile = conn.execute(
        "SELECT id FROM runner_profiles WHERE id = ?", (binding["runner_profile_id"],)
    ).fetchone()
    if profile is None:
        raise ExecutorIdentityError(
            f"binding for {agent_id!r} references missing runner profile {binding['runner_profile_id']!r}"
        )

    def_row = conn.execute(
        "SELECT id, provider, adapter, capabilities_json FROM executor_definitions "
        "WHERE id = ? AND source_id = ?",
        (binding["executor_definition_id"], binding["source_id"]),
    ).fetchone()
    if def_row is None:
        raise ExecutorIdentityError(
            f"binding for {agent_id!r} references missing definition {binding['executor_definition_id']!r}"
        )

    source = get_executor_catalog_source(conn, binding["source_id"])
    if source is None:
        raise ExecutorIdentityError(
            f"binding for {agent_id!r} references missing source {binding['source_id']!r}"
        )

    capabilities = tuple(json.loads(def_row["capabilities_json"]))
    return _build_executor_binding_snapshot(
        source_id=binding["source_id"],
        source_version=source["source_version"],
        catalog_hash=source["catalog_hash"],
        executor_definition_id=def_row["id"],
        executor_instance_id=agent_id,
        runner_profile_id=binding["runner_profile_id"],
        provider=def_row["provider"],
        adapter=def_row["adapter"],
        capabilities=capabilities,
    )


def _validate_binding_snapshot(snapshot: Any) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        raise ExecutorIdentityError("executor_binding must be an object")
    keys = {
        "contract_version",
        "binding_id",
        "source_id",
        "source_version",
        "catalog_hash",
        "executor_definition_id",
        "executor_instance_id",
        "runner_profile_id",
        "provider",
        "adapter",
        "capabilities",
    }
    if set(snapshot.keys()) != keys:
        raise ExecutorIdentityError(
            _field_mismatch_message("executor_binding", set(snapshot.keys()), keys)
        )
    if snapshot["contract_version"] != EXECUTOR_CONTRACT_VERSION:
        raise ExecutorIdentityError("executor_binding contract_version must be 1")
    binding_id = snapshot["binding_id"]
    if not isinstance(binding_id, str) or not _BINDING_ID_RE.match(binding_id):
        raise ExecutorIdentityError(
            "executor_binding.binding_id must be sha256:<64-lowercase-hex>"
        )
    expected = compute_executor_binding_id(snapshot)
    if binding_id != expected:
        raise ExecutorIdentityError(
            f"executor_binding digest mismatch: expected {expected}, got {binding_id}"
        )
    return snapshot


def _current_snapshot_matches(
    conn: sqlite3.Connection,
    stored_snapshot: dict[str, Any],
) -> bool:
    """True if the stored binding still matches the current catalog binding."""
    current = resolve_exact_executor_binding(
        conn, stored_snapshot["executor_instance_id"]
    )
    if current is None:
        return False
    return _binding_snapshot_canonical_dict(current) == _binding_snapshot_canonical_dict(
        stored_snapshot
    )


def validate_stored_executor_binding(
    conn: sqlite3.Connection,
    snapshot: Any,
    *,
    job: dict[str, Any],
) -> None:
    """Validate a stored executor binding before a claim CAS.

    Raises ``ExecutorIdentityError`` (a subclass of ``ValueError``) with a
    machine-readable ``executor_binding_mismatch:`` prefix on any mismatch so
    callers can distinguish it from an empty queue.
    """
    def _fail(reason: str) -> None:
        raise ExecutorIdentityError(f"executor_binding_mismatch: {reason}")

    try:
        snapshot = _validate_binding_snapshot(snapshot)
    except ExecutorIdentityError as exc:
        _fail(str(exc))

    job_id = job.get("id")
    assigned_agent = job.get("assigned_agent")
    runner_profile_id = job.get("runner_profile_id")

    if assigned_agent != snapshot["executor_instance_id"]:
        _fail(
            f"job assigned_agent {assigned_agent!r} != binding instance {snapshot['executor_instance_id']!r}"
        )
    if runner_profile_id != snapshot["runner_profile_id"]:
        _fail(
            f"job runner_profile_id {runner_profile_id!r} != binding runner_profile_id {snapshot['runner_profile_id']!r}"
        )

    if not _current_snapshot_matches(conn, snapshot):
        _fail("stored binding no longer matches current catalog")


def executor_binding_claim_evidence(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Redacted evidence for a ``job.claimed`` event.

    Excludes prompt, command, env, token, credentials, and capability lists are
    fine to include (they are public authority metadata).
    """
    return {
        "executor_binding_id": snapshot["binding_id"],
        "executor_definition_id": snapshot["executor_definition_id"],
        "executor_instance_id": snapshot["executor_instance_id"],
        "runner_profile_id": snapshot["runner_profile_id"],
        "source_id": snapshot["source_id"],
        "source_version": snapshot["source_version"],
        "catalog_hash": snapshot["catalog_hash"],
    }
