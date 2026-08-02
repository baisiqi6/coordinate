"""S4-D read-only projection diagnostic.

Produces an immutable report comparing registry source/effective/agents_json,
split-operation ledger/envelope/event state, task-mirror linkage, and completion
receipt event chains against their respective authorities.  No mutation, no
subprocess, no repair execution.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

from .agent_registry import parse_agents_toml
from .db import (
    Workspace,
    get_workspace,
    list_events,
    list_split_operations,
    list_task_mirrors,
    resolve_effective_agents,
    row_to_dict,
)
from .split_operations import (
    CONTRACT_VERSION,
    LIFECYCLE_OWNED_ITEM_FIELDS,
    OPERATION_KIND_ISSUE_MATERIALIZE,
    OPERATION_KIND_TASK_CREATE,
    SOURCE_KIND_ISSUE_TRIAGED_EVENT,
    STANDARD_CREATION_ITEM_FIELDS,
    STATUS_RECORD_APPLIED,
    build_issue_materialize_input_fingerprint,
    build_task_create_input_fingerprint,
    compute_plan_sha256,
    compute_task_item_fingerprint,
    load_deployed_item_readonly,
    reconstruct_creation_time_checklist_item,
    resolve_workspace_path,
    verify_issue_materialize_envelope_readonly,
    verify_task_create_envelope_readonly,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"

SEVERITY_RANK = {
    SEVERITY_ERROR: 0,
    SEVERITY_WARNING: 1,
    SEVERITY_INFO: 2,
}

OPERATION_EVENT_TYPE = {
    OPERATION_KIND_TASK_CREATE: "plan.ready",
    OPERATION_KIND_ISSUE_MATERIALIZE: "issue.materialized",
}

# Recognized top-level checklist item fields: creation-time projection plus the
# small, named allowlist of fields added by supported lifecycle transitions.
RECOGNIZED_CHECKLIST_ITEM_FIELDS = (
    STANDARD_CREATION_ITEM_FIELDS | LIFECYCLE_OWNED_ITEM_FIELDS
)

RECEIPT_EVENT_TYPES = frozenset({
    "completion.authorized",
    "completion.claimed",
    "completion.applied",
    "completion.consumed",
})

STATUS_AUTHORIZED = "authorized"
STATUS_CLAIMED = "claimed"
STATUS_APPLIED = "applied"
STATUS_CONSUMED = "consumed"

RECEIPT_TRANSITION_ORDER = [
    STATUS_AUTHORIZED,
    STATUS_CLAIMED,
    STATUS_APPLIED,
    STATUS_CONSUMED,
]


def _is_canonical_sha256(value: Any) -> bool:
    """Return True if *value* is a canonical SHA-256 hex digest (64 lowercase)."""
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def _freeze(value: Any) -> Any:
    """Recursively freeze mappings, sequences, and sets into immutable views."""
    if isinstance(value, Mapping):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(_freeze(v) for v in value)
    return value


def _thaw(value: Any) -> Any:
    """Return a fresh, JSON-serializable mutable copy of a frozen value."""
    if isinstance(value, Mapping):
        return {k: _thaw(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw(v) for v in value]
    if isinstance(value, (frozenset, set)):
        return [_thaw(v) for v in value]
    return value


@dataclass(frozen=True)
class Finding:
    finding_id: str
    kind: str
    severity: str
    scope: str
    workspace_id: str
    task_id: str | None = None
    operation_id: str | None = None
    receipt_id: str | None = None
    authority: str = ""
    evidence: tuple[MappingProxyType[str, Any], ...] = field(default_factory=tuple)
    repairable: bool = False
    next_action: str = ""

    def __post_init__(self) -> None:
        # Enforce deeply immutable evidence: tuple of recursively frozen items.
        object.__setattr__(
            self, "evidence", tuple(_freeze(item) for item in self.evidence)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "kind": self.kind,
            "severity": self.severity,
            "scope": self.scope,
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "operation_id": self.operation_id,
            "receipt_id": self.receipt_id,
            "authority": self.authority,
            "evidence": [_thaw(item) for item in self.evidence],
            "repairable": self.repairable,
            "next_action": self.next_action,
        }


@dataclass(frozen=True)
class ProjectionReport:
    workspace_id: str
    findings: tuple[Finding, ...]
    ok: bool
    summary: MappingProxyType[str, int]

    def __post_init__(self) -> None:
        # Enforce deeply immutable findings list and summary mapping.
        if not isinstance(self.findings, tuple):
            object.__setattr__(self, "findings", tuple(self.findings))
        object.__setattr__(self, "summary", _freeze(self.summary))

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "findings": [f.to_dict() for f in self.findings],
            "ok": self.ok,
            "summary": _thaw(self.summary),
        }


def _make_finding_id(
    kind: str,
    workspace_id: str,
    task_id: str | None = None,
    operation_id: str | None = None,
    receipt_id: str | None = None,
) -> str:
    return json.dumps(
        [kind, workspace_id, task_id or "", operation_id or "", receipt_id or ""],
        ensure_ascii=False,
        sort_keys=False,
        separators=(",", ":"),
    )


def _evidence(**kwargs: Any) -> list[dict[str, Any]]:
    return [{"key": k, "value": v} for k, v in sorted(kwargs.items())]


def _utc_now_str(now: str | None) -> str:
    if now:
        return now
    from .db import utc_now
    return utc_now()


def _read_checklist(workspace: Workspace) -> dict[str, Any] | None:
    """Parse-only read of the resolved checklist (new/legacy; none -> None)."""
    from .checklist_io import ChecklistError, read_checklist_bytes

    try:
        raw, _resolved = read_checklist_bytes(workspace.harness_root, purpose="read")
    except (ChecklistError, OSError):
        return None
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _checklist_authority_label(workspace: Workspace) -> str:
    """The actual checklist filename (or the new default when absent) for
    finding evidence labels — never a hardcoded legacy name."""
    from .checklist_io import CHECKLIST_LEGACY_NAME, CHECKLIST_NEW_NAME, checklist_candidates

    candidates = checklist_candidates(workspace.harness_root)
    if candidates["legacy"].is_file() and not candidates["new"].is_file():
        return CHECKLIST_LEGACY_NAME
    return CHECKLIST_NEW_NAME


def _items_from_checklist(checklist: dict[str, Any] | None) -> list[dict[str, Any]]:
    if checklist is None:
        return []
    items = checklist.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict) and item.get("id")]


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def diagnose_projections(
    conn: sqlite3.Connection,
    workspace: Workspace,
    *,
    checklist: dict[str, Any] | None = None,
    now: str | None = None,
) -> ProjectionReport:
    """Return a deterministic read-only projection diagnostic report."""
    if get_workspace(conn, workspace.id) is None:
        raise ValueError(f"unknown workspace: {workspace.id}")

    checklist_data = checklist if checklist is not None else _read_checklist(workspace)
    items = _items_from_checklist(checklist_data)
    item_lookup = {item["id"]: item for item in items}

    findings: list[Finding] = []
    findings.extend(_diagnose_registry(conn, workspace, now=now))
    findings.extend(_diagnose_split_operations(conn, workspace, item_lookup))
    findings.extend(_diagnose_task_mirrors(conn, workspace, item_lookup))
    findings.extend(_diagnose_receipts(conn, workspace, now=now))

    findings = _sort_findings(findings)
    has_error = any(f.severity == SEVERITY_ERROR for f in findings)
    summary = {
        "findings": len(findings),
        "errors": sum(1 for f in findings if f.severity == SEVERITY_ERROR),
        "warnings": sum(1 for f in findings if f.severity == SEVERITY_WARNING),
        "infos": sum(1 for f in findings if f.severity == SEVERITY_INFO),
    }
    return ProjectionReport(
        workspace_id=workspace.id,
        findings=findings,
        ok=not has_error,
        summary=summary,
    )


def _sort_findings(findings: list[Finding]) -> list[Finding]:
    def key(f: Finding) -> tuple[Any, ...]:
        return (
            SEVERITY_RANK.get(f.severity, 99),
            f.kind,
            f.scope,
            f.workspace_id or "",
            f.task_id or "",
            f.operation_id or "",
            f.receipt_id or "",
            f.finding_id,
        )

    return sorted(findings, key=key)


# ---------------------------------------------------------------------------
# Registry findings
# ---------------------------------------------------------------------------


def _diagnose_registry(
    conn: sqlite3.Connection,
    workspace: Workspace,
    *,
    now: str | None = None,
) -> list[Finding]:
    findings: list[Finding] = []
    ws_row = conn.execute(
        "SELECT agents_json FROM workspaces WHERE id = ?", (workspace.id,)
    ).fetchone()
    agents_json_str = ws_row["agents_json"] if ws_row else None
    try:
        agents_json = json.loads(agents_json_str) if agents_json_str else {}
    except json.JSONDecodeError:
        agents_json = {}
    if not isinstance(agents_json, dict):
        agents_json = {}

    effective = resolve_effective_agents(conn, workspace.id, now_utc=now)

    source_row = conn.execute(
        "SELECT source_id, source_version, source_hash, source_path "
        "FROM workspace_agent_registry_sources WHERE workspace_id = ?",
        (workspace.id,),
    ).fetchone()

    entries = conn.execute(
        "SELECT agent_name, entry_kind, discord_user_id, display_name, agent_type, expires_at "
        "FROM workspace_agent_registry_entries WHERE workspace_id = ?",
        (workspace.id,),
    ).fetchall()

    has_authoritative = any(row["entry_kind"] == "authoritative" for row in entries)

    if has_authoritative and source_row is None:
        findings.append(Finding(
            finding_id=_make_finding_id("registry_source_missing", workspace.id),
            kind="registry_source_missing",
            severity=SEVERITY_ERROR,
            scope="registry",
            workspace_id=workspace.id,
            authority="workspace_agent_registry_sources",
            evidence=_evidence(authoritative_entries=len([r for r in entries if r["entry_kind"] == "authoritative"])),
            repairable=False,
            next_action="Run `workspace agent sync --source <agents.toml> --replace` to record authoritative source metadata.",
        ))

    if source_row is not None:
        source_path = source_row["source_path"]
        if source_path:
            path = Path(source_path).expanduser()
            if not path.is_file():
                findings.append(Finding(
                    finding_id=_make_finding_id("registry_source_unreadable", workspace.id),
                    kind="registry_source_unreadable",
                    severity=SEVERITY_WARNING,
                    scope="registry",
                    workspace_id=workspace.id,
                    authority="workspace_agent_registry_sources",
                    evidence=_evidence(source_path=source_path, reason="file_not_found"),
                    repairable=False,
                    next_action="Make the recorded source path readable on this host, then re-run doctor.",
                ))
            else:
                parsed = parse_agents_toml(path)
                if parsed.errors:
                    findings.append(Finding(
                        finding_id=_make_finding_id("registry_source_unreadable", workspace.id),
                        kind="registry_source_unreadable",
                        severity=SEVERITY_WARNING,
                        scope="registry",
                        workspace_id=workspace.id,
                        authority="workspace_agent_registry_sources",
                        evidence=_evidence(source_path=source_path, parse_errors=parsed.errors),
                        repairable=False,
                        next_action="Fix agents.toml syntax, then re-run doctor.",
                    ))
                else:
                    src = parsed.source
                    mismatches: list[str] = []
                    if src and src.source_id != source_row["source_id"]:
                        mismatches.append(
                            f"source_id: recorded={source_row['source_id']!r} file={src.source_id!r}"
                        )
                    if src and src.source_version != source_row["source_version"]:
                        mismatches.append(
                            f"source_version: recorded={source_row['source_version']!r} file={src.source_version!r}"
                        )
                    if src and src.source_hash != source_row["source_hash"]:
                        mismatches.append(
                            f"source_hash: recorded={source_row['source_hash']!r} file={src.source_hash!r}"
                        )
                    if mismatches:
                        repairable = bool(
                            source_path and src and src.source_id and src.source_version and src.source_hash
                        )
                        findings.append(Finding(
                            finding_id=_make_finding_id("registry_source_identity_mismatch", workspace.id),
                            kind="registry_source_identity_mismatch",
                            severity=SEVERITY_ERROR,
                            scope="registry",
                            workspace_id=workspace.id,
                            authority="workspace_agent_registry_sources",
                            evidence=_evidence(
                                source_path=source_path,
                                recorded_source_id=source_row["source_id"],
                                recorded_source_version=source_row["source_version"],
                                recorded_source_hash=source_row["source_hash"],
                                file_source_id=src.source_id if src else None,
                                file_source_version=src.source_version if src else None,
                                file_source_hash=src.source_hash if src else None,
                                mismatches=mismatches,
                            ),
                            repairable=repairable,
                            next_action=(
                                f"Run `workspace agent sync --source {source_path} --replace` to align with the readable source."
                                if repairable else
                                "Escalate: recorded source metadata is incomplete or file identity cannot be verified."
                            ),
                        ))

    if agents_json != effective:
        findings.append(Finding(
            finding_id=_make_finding_id("registry_projection_stale", workspace.id),
            kind="registry_projection_stale",
            severity=SEVERITY_ERROR,
            scope="registry",
            workspace_id=workspace.id,
            authority="resolve_effective_agents",
            evidence=_evidence(
                agents_json=agents_json,
                effective=effective,
                diff_keys=sorted(set(agents_json) ^ set(effective)),
            ),
            repairable=bool(source_row and source_row["source_path"]),
            next_action=(
                f"Run `workspace agent sync --source {source_row['source_path']} --replace` to regenerate agents_json from the effective registry."
                if source_row and source_row["source_path"] else
                "Escalate: agents_json projection differs from effective registry and no recorded source path exists."
            ),
        ))

    # Shadowed active overrides and retained expired overrides.
    now = _utc_now_str(now)
    for row in entries:
        if row["entry_kind"] == "override":
            expired = row["expires_at"] is not None and row["expires_at"] <= now
            if expired:
                findings.append(Finding(
                    finding_id=_make_finding_id(
                        "registry_expired_override_retained",
                        workspace.id,
                        operation_id=row["agent_name"],
                    ),
                    kind="registry_expired_override_retained",
                    severity=SEVERITY_INFO,
                    scope="registry",
                    workspace_id=workspace.id,
                    task_id=None,
                    authority="workspace_agent_registry_entries",
                    evidence=_evidence(
                        agent_name=row["agent_name"],
                        discord_user_id=row["discord_user_id"],
                        expires_at=row["expires_at"],
                    ),
                    repairable=False,
                    next_action="Expired override is retained for audit and correctly excluded from effective authorization.",
                ))
            authoritative = next(
                (r for r in entries if r["entry_kind"] == "authoritative" and r["agent_name"] == row["agent_name"]),
                None,
            )
            if authoritative is not None and not expired:
                findings.append(Finding(
                    finding_id=_make_finding_id(
                        "registry_override_shadowed",
                        workspace.id,
                        operation_id=row["agent_name"],
                    ),
                    kind="registry_override_shadowed",
                    severity=SEVERITY_INFO,
                    scope="registry",
                    workspace_id=workspace.id,
                    task_id=None,
                    authority="workspace_agent_registry_entries",
                    evidence=_evidence(
                        agent_name=row["agent_name"],
                        override_discord_user_id=row["discord_user_id"],
                        authoritative_discord_user_id=authoritative["discord_user_id"],
                    ),
                    repairable=False,
                    next_action="Review override with `workspace agent remove-override` if the authoritative identity should be effective.",
                ))

    return findings


# ---------------------------------------------------------------------------
# Split-operation findings
# ---------------------------------------------------------------------------


def _diagnose_split_operations(
    conn: sqlite3.Connection,
    workspace: Workspace,
    item_lookup: dict[str, dict[str, Any]],
) -> list[Finding]:
    findings: list[Finding] = []
    operations = list_split_operations(conn, workspace_id=workspace.id)

    # Target/source conflict detection.
    target_ops: dict[tuple[str, str], list[Any]] = {}
    source_ops: dict[tuple[str, str], list[Any]] = {}
    for op in operations:
        target_ops.setdefault((op.target_kind, op.target_id), []).append(op)
        if op.source_kind and op.source_id:
            source_ops.setdefault((op.source_kind, op.source_id), []).append(op)

    for (target_kind, target_id), ops in target_ops.items():
        if len(ops) > 1:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_target_conflict",
                    workspace.id,
                    task_id=target_id,
                ),
                kind="operation_target_conflict",
                severity=SEVERITY_ERROR,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=target_id,
                authority="split_operations",
                evidence=_evidence(
                    target_kind=target_kind,
                    target_id=target_id,
                    operation_ids=sorted(o.operation_id for o in ops),
                ),
                repairable=False,
                next_action="Escalate: multiple ledger rows bind the same target.",
            ))

    for (source_kind, source_id), ops in source_ops.items():
        if len(ops) > 1:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_target_conflict",
                    workspace.id,
                    operation_id=f"source:{source_id}",
                ),
                kind="operation_target_conflict",
                severity=SEVERITY_ERROR,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=None,
                operation_id=ops[0].operation_id,
                authority="split_operations",
                evidence=_evidence(
                    source_kind=source_kind,
                    source_id=source_id,
                    operation_ids=sorted(o.operation_id for o in ops),
                ),
                repairable=False,
                next_action="Escalate: multiple ledger rows bind the same source.",
            ))

    # Ledger rows.
    for op in operations:
        findings.extend(_diagnose_one_split_operation(conn, workspace, op, item_lookup))

    # File-pending envelopes: checklist has envelope but no ledger row.
    ledger_ids = {op.operation_id for op in operations}
    for task_id, item in item_lookup.items():
        envelope = item.get("split_operation")
        if not isinstance(envelope, dict):
            continue
        op_id = envelope.get("operation_id")
        if not op_id or op_id in ledger_ids:
            continue
        op_kind = envelope.get("operation_kind")
        contract_version = envelope.get("contract_version")
        unsupported = (
            op_kind not in {OPERATION_KIND_TASK_CREATE, OPERATION_KIND_ISSUE_MATERIALIZE}
            or contract_version != CONTRACT_VERSION
        )
        if unsupported:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_contract_unsupported", workspace.id, task_id=task_id, operation_id=op_id
                ),
                kind="operation_contract_unsupported",
                severity=SEVERITY_ERROR,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op_id,
                authority=_checklist_authority_label(workspace),
                evidence=_evidence(
                    operation_id=op_id,
                    operation_kind=op_kind,
                    contract_version=contract_version,
                    supported_kinds=[OPERATION_KIND_TASK_CREATE, OPERATION_KIND_ISSUE_MATERIALIZE],
                    supported_contract_version=CONTRACT_VERSION,
                ),
                repairable=False,
                next_action="Escalate: unsupported contract version or operation kind in deployed envelope.",
            ))
            continue

        # File-pending: report envelope evidence.  Repairable only when the
        # envelope carries every record-half input consumed by apply_*_record.
        repairable, record_inputs = _file_pending_record_inputs(item, envelope, workspace, task_id, op_kind)
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_file_pending", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_file_pending",
            severity=SEVERITY_WARNING,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority=_checklist_authority_label(workspace),
            evidence=_evidence(
                operation_id=op_id,
                operation_kind=op_kind,
                input_fingerprint=envelope.get("input_fingerprint"),
                before_fingerprint=envelope.get("before_fingerprint"),
                after_fingerprint=envelope.get("after_fingerprint"),
                files_applied_at=envelope.get("files_applied_at"),
                record_inputs=record_inputs,
            ),
            repairable=repairable,
            next_action=(
                f"Run the original record half with the recorded inputs: {record_inputs}"
                if repairable else
                "Envelope-only evidence is not a runnable repair; collect the full record-half intent before running the record command."
            ),
        ))

    return findings


def _file_pending_record_inputs(
    item: dict[str, Any],
    envelope: dict[str, Any],
    workspace: Workspace,
    task_id: str,
    op_kind: str,
) -> tuple[bool, dict[str, Any]]:
    """Return (repairable, inputs) for a file-pending operation.

    Repairable only when every argument consumed by the record half is known
    from authoritative evidence (the deployed item plus the ledger envelope).
    """
    title = item.get("title")
    phase = item.get("phase")
    plan_doc = _plan_doc_from_envelope_or_item(envelope, item)
    plan_sha256: str | None = None
    if plan_doc:
        plan_path = resolve_workspace_path(workspace, plan_doc)
        if plan_path.is_file():
            plan_sha256 = compute_plan_sha256(plan_path)

    if op_kind == OPERATION_KIND_TASK_CREATE:
        # task.create record consumes: workspace_id, task_id, plan_doc, title,
        # phase, owner, branch, actor, target, payload, operation_id, and fingerprints.
        # The envelope gives us operation_id and fingerprints.  The item gives
        # title/phase.  We still need actor at minimum; owner/branch/target/payload
        # are optional at runtime but must not be guessed.
        actor = envelope.get("record_actor")  # not part of envelope today
        inputs: dict[str, Any] = {
            "workspace_id": workspace.id,
            "task_id": task_id,
            "plan_doc": plan_doc,
            "title": title,
            "phase": phase,
            "operation_id": envelope.get("operation_id"),
            "input_fingerprint": envelope.get("input_fingerprint"),
            "before_fingerprint": envelope.get("before_fingerprint"),
            "after_fingerprint": envelope.get("after_fingerprint"),
        }
        if not actor:
            return False, inputs
        inputs["actor"] = actor
        # Verify the fingerprint we can recompute matches the envelope.
        if not (title and phase and plan_doc and plan_sha256):
            return False, inputs
        expected_input = build_task_create_input_fingerprint(
            workspace_id=workspace.id,
            task_id=task_id,
            plan_doc=plan_doc,
            plan_sha256=plan_sha256,
            title=title,
            phase=phase,
            priority=item.get("priority", ""),
        )
        expected_before = compute_task_item_fingerprint(item=None, task_id=task_id)
        expected_after = compute_task_item_fingerprint(item=item, task_id=task_id)
        if (
            expected_input != envelope.get("input_fingerprint")
            or expected_before != envelope.get("before_fingerprint")
            or expected_after != envelope.get("after_fingerprint")
        ):
            return False, inputs
        return True, inputs

    if op_kind == OPERATION_KIND_ISSUE_MATERIALIZE:
        source_id = envelope.get("source_id")
        # issue.materialize record consumes actor at minimum plus title/phase/owner/branch/target/platform/destination.
        actor = envelope.get("record_actor")
        inputs = {
            "workspace_id": workspace.id,
            "task_id": task_id,
            "source_event_id": source_id,
            "plan_doc": plan_doc,
            "title": title,
            "phase": phase,
            "operation_id": envelope.get("operation_id"),
            "input_fingerprint": envelope.get("input_fingerprint"),
            "before_fingerprint": envelope.get("before_fingerprint"),
            "after_fingerprint": envelope.get("after_fingerprint"),
        }
        if not actor:
            return False, inputs
        inputs["actor"] = actor
        if not (title and phase and plan_doc and plan_sha256 and source_id):
            return False, inputs
        expected_input = build_issue_materialize_input_fingerprint(
            workspace_id=workspace.id,
            task_id=task_id,
            source_id=source_id,
            plan_doc=plan_doc,
            plan_sha256=plan_sha256,
            title=title,
            phase=phase,
            priority=item.get("priority", ""),
        )
        expected_before = compute_task_item_fingerprint(item=None, task_id=task_id)
        expected_after = compute_task_item_fingerprint(item=item, task_id=task_id)
        if (
            expected_input != envelope.get("input_fingerprint")
            or expected_before != envelope.get("before_fingerprint")
            or expected_after != envelope.get("after_fingerprint")
        ):
            return False, inputs
        return True, inputs

    return False, {}


def _diagnose_one_split_operation(
    conn: sqlite3.Connection,
    workspace: Workspace,
    op: Any,
    item_lookup: dict[str, dict[str, Any]],
) -> list[Finding]:
    findings: list[Finding] = []
    op_id = op.operation_id
    task_id = op.target_id
    op_kind = op.operation_kind

    if op.status != STATUS_RECORD_APPLIED:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_status_invalid", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_status_invalid",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority="split_operations",
            evidence=_evidence(status=op.status, supported_statuses=[STATUS_RECORD_APPLIED]),
            repairable=False,
            next_action="Escalate: ledger status is outside the supported state machine.",
        ))

    if (
        op_kind not in {OPERATION_KIND_TASK_CREATE, OPERATION_KIND_ISSUE_MATERIALIZE}
        or op.contract_version != CONTRACT_VERSION
    ):
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_contract_unsupported", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_contract_unsupported",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority="split_operations",
            evidence=_evidence(
                operation_kind=op_kind,
                contract_version=op.contract_version,
                supported_kinds=[OPERATION_KIND_TASK_CREATE, OPERATION_KIND_ISSUE_MATERIALIZE],
                supported_contract_version=CONTRACT_VERSION,
            ),
            repairable=False,
            next_action="Escalate: unsupported contract version or operation kind in ledger.",
        ))
        return findings

    # Load the item by task id alone, independent of expected operation id,
    # so we can distinguish missing item/envelope from present-but-drifted.
    item, item_errors = load_deployed_item_readonly(
        workspace=workspace,
        task_id=task_id,
    )
    if item is None:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_ledger_orphaned", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_ledger_orphaned",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority=_checklist_authority_label(workspace),
            evidence=_evidence(errors=item_errors),
            repairable=False,
            next_action="Escalate: ledger target/envelope is missing.",
        ))
        return findings

    envelope = item.get("split_operation")
    if not isinstance(envelope, dict):
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_ledger_orphaned", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_ledger_orphaned",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority=_checklist_authority_label(workspace),
            evidence=_evidence(reason="deployed_item_has_no_split_operation_envelope"),
            repairable=False,
            next_action="Escalate: ledger target/envelope is missing.",
        ))
        return findings

    if envelope.get("operation_id") != op_id:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_envelope_drift", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_envelope_drift",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority=_checklist_authority_label(workspace),
            evidence=_evidence(
                ledger_operation_id=op_id,
                envelope_operation_id=envelope.get("operation_id"),
            ),
            repairable=False,
            next_action="Escalate: deployed envelope binds a different operation id.",
        ))
        return findings

    source_id = op.source_id if op.source_kind == SOURCE_KIND_ISSUE_TRIAGED_EVENT else None
    if op_kind == OPERATION_KIND_TASK_CREATE:
        shape_errors = verify_task_create_envelope_readonly(
            envelope=envelope,
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            input_fingerprint=op.input_fingerprint,
            before_fingerprint=op.before_fingerprint,
            after_fingerprint=op.after_fingerprint,
        )
    else:
        shape_errors = verify_issue_materialize_envelope_readonly(
            envelope=envelope,
            workspace_id=workspace.id,
            task_id=task_id,
            source_event_id=source_id or "",
            operation_id=op_id,
            input_fingerprint=op.input_fingerprint,
            before_fingerprint=op.before_fingerprint,
            after_fingerprint=op.after_fingerprint,
        )

    if shape_errors:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_envelope_drift", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_envelope_drift",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority=_checklist_authority_label(workspace),
            evidence=_evidence(shape_errors=shape_errors),
            repairable=False,
            next_action="Escalate: deployed envelope shape or identity fields drift from ledger.",
        ))

    # Load the authoritative ready event and validate ledger/envelope/record intent.
    ready_event_row, ready_payload, ready_findings = _operation_ready_event(
        conn, workspace, op, item, envelope
    )
    if ready_findings:
        findings.extend(ready_findings)
        return findings

    # Creation-time fingerprint proof from the immutable record payload.
    proof_errors = _creation_time_proof_errors(op, envelope, ready_payload)
    if proof_errors:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_envelope_drift", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_envelope_drift",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority="events",
            evidence=_evidence(proof_errors=proof_errors),
            repairable=False,
            next_action="Escalate: creation-time fingerprint proof does not match the ledger/envelope.",
        ))
        return findings

    # Immutable creation identity: current item may evolve only in lifecycle-owned fields.
    identity_errors = _immutable_identity_errors(
        item=item,
        envelope=envelope,
        ready_payload=ready_payload,
        task_id=task_id,
        operation_id=op_id,
    )
    if identity_errors:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_envelope_drift", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_envelope_drift",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority=_checklist_authority_label(workspace),
            evidence=_evidence(identity_errors=identity_errors),
            repairable=False,
            next_action="Escalate: immutable creation identity field has drifted.",
        ))
        return findings

    # Current plan bytes vs historical plan SHA.
    plan_doc = _plan_doc_from_envelope_or_item(envelope, item)
    current_plan_sha256: str | None = None
    if plan_doc:
        try:
            plan_path = resolve_workspace_path(workspace, plan_doc)
            if plan_path.is_file():
                current_plan_sha256 = compute_plan_sha256(plan_path)
        except Exception:
            current_plan_sha256 = None

    if current_plan_sha256 is None:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_envelope_drift", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_envelope_drift",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority=_checklist_authority_label(workspace),
            evidence=_evidence(plan_doc=plan_doc, reason="plan_file_unavailable_for_fingerprint_check"),
            repairable=False,
            next_action="Deploy the referenced plan file, then re-run doctor.",
        ))
    elif current_plan_sha256 != ready_payload["plan_sha256"]:
        supersession = _find_approved_plan_supersession(
            conn,
            workspace_id=workspace.id,
            task_id=task_id,
            base_ready_event_id=ready_event_row["id"],
            base_ready_rowid=ready_event_row["rowid"],
            canonical_plan_doc=ready_payload["plan_doc"],
            current_plan_sha256=current_plan_sha256,
        )
        if supersession:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_plan_superseded", workspace.id, task_id=task_id, operation_id=op_id
                ),
                kind="operation_plan_superseded",
                severity=SEVERITY_INFO,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op_id,
                authority="events",
                evidence=_evidence(
                    plan_doc=plan_doc,
                    historical_plan_sha256=supersession["old_plan_sha256"],
                    current_plan_sha256=supersession["new_plan_sha256"],
                    base_ready_event_id=supersession["superseding_ready_event_id"],
                    approved_event_id=supersession["approved_event_id"],
                ),
                repairable=False,
                next_action="Plan revision is approved; no action required.",
            ))
        else:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_envelope_drift", workspace.id, task_id=task_id, operation_id=op_id
                ),
                kind="operation_envelope_drift",
                severity=SEVERITY_ERROR,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op_id,
                authority=_checklist_authority_label(workspace),
                evidence=_evidence(
                    plan_doc=plan_doc,
                    historical_plan_sha256=ready_payload["plan_sha256"],
                    current_plan_sha256=current_plan_sha256,
                    reason="current_plan_sha256_does_not_match_creation_record_and_no_approved_supersession",
                ),
                repairable=False,
                next_action="Correct the plan or obtain an approved plan revision with exact SHA linkage.",
            ))

    return findings


def _plan_doc_from_envelope_or_item(envelope: dict[str, Any], item: dict[str, Any]) -> str | None:
    # The ledger does not store plan_doc directly, but the deployed item's
    # plan_path is the authoritative record-half plan reference.
    plan_doc = item.get("plan_path")
    if isinstance(plan_doc, str) and plan_doc:
        return plan_doc
    return None


def _operation_ready_event(
    conn: sqlite3.Connection,
    workspace: Workspace,
    op: Any,
    item: dict[str, Any],
    envelope: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[Finding]]:
    """Load the authoritative plan.ready event for a recorded split operation.

    For ``task.create`` the record event itself is the ``plan.ready``.  For
    ``issue.materialize`` the record event is ``issue.materialized`` and the
    authoritative ready event is the one referenced by
    ``plan_ready_event_id``.

    Returns ``(ready_event_row, ready_payload, findings)``.  When *findings* is
    non-empty the caller should return them and stop processing this operation.
    """
    findings: list[Finding] = []
    op_id = op.operation_id
    task_id = op.target_id
    op_kind = op.operation_kind

    record_event_id = op.record_event_id
    if not record_event_id:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_record_event_missing", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_record_event_missing",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority="split_operations",
            evidence=_evidence(record_event_id=record_event_id),
            repairable=False,
            next_action="Escalate: ledger row has no bound record event.",
        ))
        return None, None, findings

    expected_record_type = OPERATION_EVENT_TYPE.get(op_kind)
    record_row = conn.execute(
        "SELECT rowid, * FROM events WHERE id = ?", (record_event_id,)
    ).fetchone()
    if record_row is None:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_record_event_missing", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_record_event_missing",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority="events",
            evidence=_evidence(record_event_id=record_event_id, reason="event_not_found"),
            repairable=False,
            next_action="Escalate: bound record event is missing.",
        ))
        return None, None, findings

    mismatches: list[str] = []
    if record_row["workspace_id"] != workspace.id:
        mismatches.append(
            f"workspace_id: event={record_row['workspace_id']!r} expected={workspace.id!r}"
        )
    if record_row["task_id"] != task_id:
        mismatches.append(
            f"task_id: event={record_row['task_id']!r} expected={task_id!r}"
        )
    if expected_record_type and record_row["event_type"] != expected_record_type:
        mismatches.append(
            f"event_type: event={record_row['event_type']!r} expected={expected_record_type!r}"
        )
    record_payload = _json_loads(record_row["payload_json"])
    payload_op_meta = record_payload.get("split_operation") if isinstance(record_payload.get("split_operation"), dict) else {}
    if payload_op_meta.get("operation_id") != op_id:
        mismatches.append(
            f"operation_id: payload={payload_op_meta.get('operation_id')!r} expected={op_id!r}"
        )
    if payload_op_meta.get("operation_kind") != op_kind:
        mismatches.append(
            f"operation_kind: payload={payload_op_meta.get('operation_kind')!r} expected={op_kind!r}"
        )
    if payload_op_meta.get("contract_version") != op.contract_version:
        mismatches.append(
            f"contract_version: payload={payload_op_meta.get('contract_version')!r} expected={op.contract_version!r}"
        )
    for fp_key in ("input_fingerprint", "before_fingerprint", "after_fingerprint"):
        if payload_op_meta.get(fp_key) != getattr(op, fp_key):
            mismatches.append(
                f"{fp_key}: payload={payload_op_meta.get(fp_key)!r} expected={getattr(op, fp_key)!r}"
            )
    if op_kind == OPERATION_KIND_TASK_CREATE:
        if record_payload.get("task_id") != task_id:
            mismatches.append(
                f"record_task_id: payload={record_payload.get('task_id')!r} expected={task_id!r}"
            )
    elif op_kind == OPERATION_KIND_ISSUE_MATERIALIZE:
        if record_payload.get("triage_event_id") != op.source_id:
            mismatches.append(
                f"triage_event_id: payload={record_payload.get('triage_event_id')!r} expected={op.source_id!r}"
            )
    if mismatches:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "operation_record_event_mismatch", workspace.id, task_id=task_id, operation_id=op_id
            ),
            kind="operation_record_event_mismatch",
            severity=SEVERITY_ERROR,
            scope="split_operation",
            workspace_id=workspace.id,
            task_id=task_id,
            operation_id=op_id,
            authority="events",
            evidence=_evidence(
                record_event_id=record_event_id,
                event_rowid=record_row["rowid"],
                event_workspace_id=record_row["workspace_id"],
                event_task_id=record_row["task_id"],
                event_type=record_row["event_type"],
                mismatches=mismatches,
            ),
            repairable=False,
            next_action="Escalate: bound record event has wrong workspace/task/type/operation intent.",
        ))
        return None, None, findings

    if op_kind == OPERATION_KIND_TASK_CREATE:
        ready_event_row = record_row
        ready_payload = record_payload
    else:
        ready_event_id = record_payload.get("plan_ready_event_id")
        if not ready_event_id:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_record_event_mismatch", workspace.id, task_id=task_id, operation_id=op_id
                ),
                kind="operation_record_event_mismatch",
                severity=SEVERITY_ERROR,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op_id,
                authority="events",
                evidence=_evidence(record_event_id=record_event_id, reason="missing_plan_ready_event_id"),
                repairable=False,
                next_action="Escalate: issue.materialized record has no linked plan.ready event.",
            ))
            return None, None, findings
        ready_event_row = conn.execute(
            "SELECT rowid, * FROM events WHERE id = ?", (ready_event_id,)
        ).fetchone()
        if ready_event_row is None:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_record_event_mismatch", workspace.id, task_id=task_id, operation_id=op_id
                ),
                kind="operation_record_event_mismatch",
                severity=SEVERITY_ERROR,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op_id,
                authority="events",
                evidence=_evidence(record_event_id=record_event_id, plan_ready_event_id=ready_event_id, reason="linked_ready_event_not_found"),
                repairable=False,
                next_action="Escalate: issue.materialized references a missing plan.ready event.",
            ))
            return None, None, findings
        if (
            ready_event_row["event_type"] != "plan.ready"
            or ready_event_row["workspace_id"] != workspace.id
            or ready_event_row["task_id"] != task_id
        ):
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_record_event_mismatch", workspace.id, task_id=task_id, operation_id=op_id
                ),
                kind="operation_record_event_mismatch",
                severity=SEVERITY_ERROR,
                scope="split_operation",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op_id,
                authority="events",
                evidence=_evidence(
                    plan_ready_event_id=ready_event_id,
                    event_type=ready_event_row["event_type"],
                    workspace_id=ready_event_row["workspace_id"],
                    task_id=ready_event_row["task_id"],
                ),
                repairable=False,
                next_action="Escalate: linked plan.ready event has wrong type/workspace/task.",
            ))
            return None, None, findings
        ready_payload = _json_loads(ready_event_row["payload_json"])
        # The materialized payload and the ready payload must agree on immutable intent.
        for field in ("title", "phase", "plan_doc"):
            if record_payload.get(field) != ready_payload.get(field):
                findings.append(Finding(
                    finding_id=_make_finding_id(
                        "operation_record_event_mismatch", workspace.id, task_id=task_id, operation_id=op_id
                    ),
                    kind="operation_record_event_mismatch",
                    severity=SEVERITY_ERROR,
                    scope="split_operation",
                    workspace_id=workspace.id,
                    task_id=task_id,
                    operation_id=op_id,
                    authority="events",
                    evidence=_evidence(
                        record_event_id=record_event_id,
                        plan_ready_event_id=ready_event_row["id"],
                        field=field,
                        materialized_value=record_payload.get(field),
                        ready_value=ready_payload.get(field),
                    ),
                    repairable=False,
                    next_action="Escalate: issue.materialized payload disagrees with linked plan.ready payload.",
                ))
                return None, None, findings

    return dict(ready_event_row), ready_payload, []


def _creation_time_proof_errors(
    op: Any,
    envelope: dict[str, Any],
    ready_payload: dict[str, Any],
) -> list[str]:
    """Return errors validating the creation-time fingerprint proof.

    Recomputes the input fingerprint and the creation-time checklist projection
    from the immutable ready payload and compares them to the ledger/envelope
    fingerprints.  Any mismatch means the historical record is corrupted.
    """
    errors: list[str] = []
    op_kind = op.operation_kind
    workspace_id = op.workspace_id
    task_id = op.target_id

    required = ("plan_doc", "plan_sha256", "title", "phase", "priority")
    for key in required:
        value = ready_payload.get(key)
        if not isinstance(value, str) or not value:
            errors.append(f"ready payload missing canonical {key}")
    if not _is_canonical_sha256(ready_payload.get("plan_sha256")):
        errors.append("ready payload plan_sha256 is not a canonical SHA-256")
    if errors:
        return errors

    try:
        if op_kind == OPERATION_KIND_TASK_CREATE:
            expected_input = build_task_create_input_fingerprint(
                workspace_id=workspace_id,
                task_id=task_id,
                plan_doc=ready_payload["plan_doc"],
                plan_sha256=ready_payload["plan_sha256"],
                title=ready_payload["title"],
                phase=ready_payload["phase"],
                priority=ready_payload["priority"],
            )
        else:
            expected_input = build_issue_materialize_input_fingerprint(
                workspace_id=workspace_id,
                task_id=task_id,
                source_id=op.source_id or "",
                plan_doc=ready_payload["plan_doc"],
                plan_sha256=ready_payload["plan_sha256"],
                title=ready_payload["title"],
                phase=ready_payload["phase"],
                priority=ready_payload["priority"],
            )
    except Exception as exc:
        errors.append(f"input fingerprint recomputation failed: {exc}")
        return errors

    if expected_input != op.input_fingerprint:
        errors.append(
            f"input_fingerprint: stored={op.input_fingerprint!r} recomputed={expected_input!r}"
        )

    expected_before = compute_task_item_fingerprint(item=None, task_id=task_id)
    if expected_before != op.before_fingerprint:
        errors.append(
            f"before_fingerprint: stored={op.before_fingerprint!r} recomputed={expected_before!r}"
        )

    try:
        historical_item = reconstruct_creation_time_checklist_item(
            task_id=task_id,
            title=ready_payload["title"],
            plan_doc=ready_payload["plan_doc"],
            priority=ready_payload["priority"],
            phase=ready_payload["phase"],
            files_applied_at=envelope["files_applied_at"],
            envelope=envelope,
        )
    except Exception as exc:
        errors.append(f"creation-time checklist projection failed: {exc}")
        return errors

    expected_after = compute_task_item_fingerprint(item=historical_item, task_id=task_id)
    if expected_after != op.after_fingerprint:
        errors.append(
            f"after_fingerprint: stored={op.after_fingerprint!r} recomputed={expected_after!r}"
        )

    return errors


def _immutable_identity_errors(
    item: dict[str, Any],
    envelope: dict[str, Any],
    ready_payload: dict[str, Any],
    task_id: str,
    operation_id: str,
) -> list[str]:
    """Return errors for immutable identity drift and unknown top-level fields."""
    errors: list[str] = []
    if item.get("id") != task_id:
        errors.append(f"item id: {item.get('id')!r} != {task_id!r}")
    if item.get("title") != ready_payload.get("title"):
        errors.append(
            f"title: deployed={item.get('title')!r} recorded={ready_payload.get('title')!r}"
        )
    if item.get("phase") != ready_payload.get("phase"):
        errors.append(
            f"phase: deployed={item.get('phase')!r} recorded={ready_payload.get('phase')!r}"
        )
    if item.get("priority") != ready_payload.get("priority"):
        errors.append(
            f"priority: deployed={item.get('priority')!r} recorded={ready_payload.get('priority')!r}"
        )

    recorded_plan_doc = ready_payload.get("plan_doc")
    plan_path = item.get("plan_path")
    artifacts = item.get("artifacts")
    artifacts_plan = artifacts.get("plan") if isinstance(artifacts, dict) else None

    if not isinstance(plan_path, str) or not plan_path:
        errors.append(f"plan_path: missing or malformed ({plan_path!r})")
    elif plan_path != recorded_plan_doc:
        errors.append(
            f"plan_path: deployed={plan_path!r} recorded={recorded_plan_doc!r}"
        )

    if not isinstance(artifacts, dict):
        errors.append(f"artifacts: missing or malformed ({artifacts!r})")
    elif not isinstance(artifacts_plan, str) or not artifacts_plan:
        errors.append(f"artifacts.plan: missing or malformed ({artifacts_plan!r})")
    elif artifacts_plan != recorded_plan_doc:
        errors.append(
            f"artifacts.plan: deployed={artifacts_plan!r} recorded={recorded_plan_doc!r}"
        )

    if (
        isinstance(plan_path, str)
        and isinstance(artifacts_plan, str)
        and plan_path != artifacts_plan
    ):
        errors.append(
            f"plan_path and artifacts.plan conflict: {plan_path!r} != {artifacts_plan!r}"
        )

    if envelope.get("operation_id") != operation_id:
        errors.append(
            f"envelope operation_id: {envelope.get('operation_id')!r} != {operation_id!r}"
        )

    unknown_keys = set(item.keys()) - RECOGNIZED_CHECKLIST_ITEM_FIELDS
    for key in sorted(unknown_keys):
        errors.append(f"unknown top-level field: {key!r}")

    return errors


def _supersedes_chain_reaches(
    candidate_id: str,
    target_id: str,
    ready_by_id: dict[str, dict[str, Any]],
) -> bool:
    """Follow supersedes links from *candidate_id* and return True if *target_id* is reached.

    The chain is valid only when every link is a known plan.ready event in the
    same workspace/task and contains no cycles.
    """
    visited: set[str] = set()
    current = candidate_id
    while current is not None:
        if current == target_id:
            return True
        if current in visited:
            return False
        visited.add(current)
        info = ready_by_id.get(current)
        if info is None:
            return False
        current = info["payload"].get("supersedes_plan_ready_event_id")
    return False


def _find_approved_plan_supersession(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    base_ready_event_id: str,
    base_ready_rowid: int,
    canonical_plan_doc: str,
    current_plan_sha256: str,
) -> dict[str, Any] | None:
    """Find an approved plan revision that supersedes the base ready event.

    Returns a dict with the superseding ready event id, approved event id, old
    and new SHA, or ``None`` when no exact approved supersession exists.
    """
    rows = conn.execute(
        "SELECT rowid, id, event_type, payload_json FROM events "
        "WHERE workspace_id = ? AND task_id = ? AND event_type IN ('plan.ready', 'plan.approved', 'plan.rejected') "
        "ORDER BY rowid",
        (workspace_id, task_id),
    ).fetchall()

    ready_by_id: dict[str, dict[str, Any]] = {}
    ready_rows: list[tuple[int, str, dict[str, Any]]] = []
    approved: list[tuple[int, str, str]] = []  # rowid, ready_event_id, approved_event_id
    rejected_rowids: list[int] = []

    for row in rows:
        payload = _json_loads(row["payload_json"])
        if row["event_type"] == "plan.ready":
            ready_by_id[row["id"]] = {"rowid": row["rowid"], "payload": payload}
            ready_rows.append((row["rowid"], row["id"], payload))
        elif row["event_type"] == "plan.approved":
            approved.append((row["rowid"], payload.get("plan_ready_event_id"), row["id"]))
        elif row["event_type"] == "plan.rejected":
            rejected_rowids.append(row["rowid"])

    for cand_rowid, cand_id, cand_payload in sorted(ready_rows, key=lambda x: -x[0]):
        if cand_rowid <= base_ready_rowid:
            continue
        if cand_payload.get("plan_doc") != canonical_plan_doc:
            continue
        if cand_payload.get("plan_sha256") != current_plan_sha256:
            continue
        if not _supersedes_chain_reaches(cand_id, base_ready_event_id, ready_by_id):
            continue
        approved_event_id: str | None = None
        for a_rowid, a_ready_id, a_event_id in approved:
            if a_rowid > cand_rowid and a_ready_id == cand_id:
                approved_event_id = a_event_id
                break
        if approved_event_id is None:
            continue
        if any(r_rowid > cand_rowid for r_rowid in rejected_rowids):
            continue
        return {
            "superseding_ready_event_id": cand_id,
            "approved_event_id": approved_event_id,
            "old_plan_sha256": ready_by_id[base_ready_event_id]["payload"].get("plan_sha256"),
            "new_plan_sha256": current_plan_sha256,
        }

    return None


# ---------------------------------------------------------------------------
# Task-mirror findings
# ---------------------------------------------------------------------------


def _diagnose_task_mirrors(
    conn: sqlite3.Connection,
    workspace: Workspace,
    item_lookup: dict[str, dict[str, Any]],
) -> list[Finding]:
    findings: list[Finding] = []
    operations = list_split_operations(conn, workspace_id=workspace.id)
    mirror_rows = list_task_mirrors(conn, workspace_id=workspace.id)
    mirror_lookup = {row["task_id"]: row for row in mirror_rows}

    # Build an event lookup by id including rowid.
    event_rows = list_events(conn, workspace_id=workspace.id)
    event_by_id: dict[str, dict[str, Any]] = {}
    for row in event_rows:
        d = row_to_dict(row)
        event_by_id[d["id"]] = d

    for op in operations:
        task_id = op.target_id
        mirror = mirror_lookup.get(task_id)
        if mirror is None:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_task_mirror_missing", workspace.id, task_id=task_id, operation_id=op.operation_id
                ),
                kind="operation_task_mirror_missing",
                severity=SEVERITY_ERROR,
                scope="task_mirror",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op.operation_id,
                authority="tasks",
                evidence=_evidence(operation_id=op.operation_id),
                repairable=False,
                next_action="Escalate: operation ledger exists but task mirror is missing.",
            ))
            continue

        payload = _json_loads(mirror["payload_json"])
        op_meta = payload.get("split_operation")
        if not isinstance(op_meta, dict):
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_task_mirror_metadata_drift", workspace.id, task_id=task_id, operation_id=op.operation_id
                ),
                kind="operation_task_mirror_metadata_drift",
                severity=SEVERITY_ERROR,
                scope="task_mirror",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op.operation_id,
                authority="tasks",
                evidence=_evidence(reason="mirror_payload_has_no_split_operation_metadata"),
                repairable=False,
                next_action="Escalate: task mirror lacks operation metadata.",
            ))
        else:
            meta_mismatches: list[str] = []
            if op_meta.get("operation_id") != op.operation_id:
                meta_mismatches.append(
                    f"operation_id: mirror={op_meta.get('operation_id')!r} ledger={op.operation_id!r}"
                )
            if op_meta.get("operation_kind") != op.operation_kind:
                meta_mismatches.append(
                    f"operation_kind: mirror={op_meta.get('operation_kind')!r} ledger={op.operation_kind!r}"
                )
            if op_meta.get("contract_version") != op.contract_version:
                meta_mismatches.append(
                    f"contract_version: mirror={op_meta.get('contract_version')!r} ledger={op.contract_version!r}"
                )
            for fp_key in ("input_fingerprint", "before_fingerprint", "after_fingerprint"):
                if op_meta.get(fp_key) != getattr(op, fp_key):
                    meta_mismatches.append(
                        f"{fp_key}: mirror={op_meta.get(fp_key)!r} ledger={getattr(op, fp_key)!r}"
                    )
            if meta_mismatches:
                findings.append(Finding(
                    finding_id=_make_finding_id(
                        "operation_task_mirror_metadata_drift", workspace.id, task_id=task_id, operation_id=op.operation_id
                    ),
                    kind="operation_task_mirror_metadata_drift",
                    severity=SEVERITY_ERROR,
                    scope="task_mirror",
                    workspace_id=workspace.id,
                    task_id=task_id,
                    operation_id=op.operation_id,
                    authority="tasks",
                    evidence=_evidence(mismatches=meta_mismatches),
                    repairable=False,
                    next_action="Escalate: task mirror operation metadata does not match ledger.",
                ))
            # Immutable record payload required by C1/C2.
            record_mismatches: list[str] = []
            if payload.get("task_id") != task_id:
                record_mismatches.append(
                    f"task_id: mirror={payload.get('task_id')!r} ledger={task_id!r}"
                )
            item = item_lookup.get(task_id, {})
            envelope = item.get("split_operation") if isinstance(item.get("split_operation"), dict) else {}
            if payload.get("title") != item.get("title"):
                record_mismatches.append(
                    f"title: mirror={payload.get('title')!r} deployed={item.get('title')!r}"
                )
            if payload.get("phase") != item.get("phase"):
                record_mismatches.append(
                    f"phase: mirror={payload.get('phase')!r} deployed={item.get('phase')!r}"
                )
            plan_doc = _plan_doc_from_envelope_or_item(envelope, item)
            if payload.get("plan_doc") != plan_doc:
                record_mismatches.append(
                    f"plan_doc: mirror={payload.get('plan_doc')!r} deployed={plan_doc!r}"
                )
            if op.operation_kind == OPERATION_KIND_ISSUE_MATERIALIZE:
                if payload.get("triage_event_id") != op.source_id:
                    record_mismatches.append(
                        f"triage_event_id: mirror={payload.get('triage_event_id')!r} ledger={op.source_id!r}"
                    )
            if record_mismatches:
                findings.append(Finding(
                    finding_id=_make_finding_id(
                        "operation_task_mirror_metadata_drift", workspace.id, task_id=task_id, operation_id=op.operation_id
                    ),
                    kind="operation_task_mirror_metadata_drift",
                    severity=SEVERITY_ERROR,
                    scope="task_mirror",
                    workspace_id=workspace.id,
                    task_id=task_id,
                    operation_id=op.operation_id,
                    authority="tasks",
                    evidence=_evidence(record_mismatches=record_mismatches),
                    repairable=False,
                    next_action="Escalate: task mirror immutable record payload does not match ledger/deployed item.",
                ))

        # last_event_id must be absent, point to another workspace/task, or
        # precede the operation record event by rowid -> regression.
        record_event_id = op.record_event_id
        last_event_id = mirror["last_event_id"]
        if not record_event_id:
            continue
        record_event = event_by_id.get(record_event_id)
        if record_event is None:
            continue
        record_rowid = record_event.get("rowid")
        if not last_event_id:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_task_event_regression", workspace.id, task_id=task_id, operation_id=op.operation_id
                ),
                kind="operation_task_event_regression",
                severity=SEVERITY_ERROR,
                scope="task_mirror",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op.operation_id,
                authority="tasks",
                evidence=_evidence(
                    last_event_id=None,
                    record_event_id=record_event_id,
                    record_event_rowid=record_rowid,
                ),
                repairable=False,
                next_action="Escalate: task mirror last_event_id is null after operation record event.",
            ))
            continue

        last_event = event_by_id.get(last_event_id)
        if last_event is None:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_task_event_regression", workspace.id, task_id=task_id, operation_id=op.operation_id
                ),
                kind="operation_task_event_regression",
                severity=SEVERITY_ERROR,
                scope="task_mirror",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op.operation_id,
                authority="tasks",
                evidence=_evidence(
                    last_event_id=last_event_id,
                    record_event_id=record_event_id,
                    reason="last_event_not_found",
                ),
                repairable=False,
                next_action="Escalate: task mirror last_event_id points to a missing event.",
            ))
            continue

        if (
            last_event.get("workspace_id") != workspace.id
            or last_event.get("task_id") != task_id
        ):
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_task_event_regression", workspace.id, task_id=task_id, operation_id=op.operation_id
                ),
                kind="operation_task_event_regression",
                severity=SEVERITY_ERROR,
                scope="task_mirror",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op.operation_id,
                authority="tasks",
                evidence=_evidence(
                    last_event_id=last_event_id,
                    last_event_workspace_id=last_event.get("workspace_id"),
                    last_event_task_id=last_event.get("task_id"),
                    expected_workspace_id=workspace.id,
                    expected_task_id=task_id,
                ),
                repairable=False,
                next_action="Escalate: task mirror last_event_id points to another workspace/task.",
            ))
            continue

        last_rowid = last_event.get("rowid")
        if last_rowid is not None and record_rowid is not None and last_rowid < record_rowid:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "operation_task_event_regression", workspace.id, task_id=task_id, operation_id=op.operation_id
                ),
                kind="operation_task_event_regression",
                severity=SEVERITY_ERROR,
                scope="task_mirror",
                workspace_id=workspace.id,
                task_id=task_id,
                operation_id=op.operation_id,
                authority="tasks",
                evidence=_evidence(
                    last_event_id=last_event_id,
                    last_event_rowid=last_rowid,
                    record_event_id=record_event_id,
                    record_event_rowid=record_rowid,
                ),
                repairable=False,
                next_action="Escalate: task mirror last_event_id precedes the operation record event by rowid.",
            ))

    return findings


# ---------------------------------------------------------------------------
# Receipt findings
# ---------------------------------------------------------------------------


def _diagnose_receipts(
    conn: sqlite3.Connection,
    workspace: Workspace,
    *,
    now: str | None = None,
) -> list[Finding]:
    findings: list[Finding] = []
    rows = list_events(conn, workspace_id=workspace.id)
    events = [row_to_dict(row) for row in rows]

    # Build receipt chains.
    chains: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        if event.get("event_type") not in RECEIPT_EVENT_TYPES:
            continue
        payload = event.get("payload") or {}
        receipt_id = payload.get("receipt_id")
        if not receipt_id:
            continue
        chains.setdefault(receipt_id, []).append(event)

    # Map terminal consumed receipts by workspace/task for supersession checks.
    consumed_receipts: list[tuple[str, str, str, int]] = []
    for receipt_id, chain in chains.items():
        chain.sort(key=lambda e: e.get("rowid", 0))
        consumed = next(
            (e for e in chain if (e.get("payload") or {}).get("status") == STATUS_CONSUMED),
            None,
        )
        if consumed:
            payload = consumed.get("payload") or {}
            ws = payload.get("workspace_id")
            task = payload.get("task_id")
            if ws and task:
                consumed_receipts.append((receipt_id, ws, task, consumed.get("rowid", 0)))

    for receipt_id, chain in chains.items():
        chain.sort(key=lambda e: e.get("rowid", 0))
        findings.extend(_diagnose_one_receipt_chain(
            conn, workspace, receipt_id, chain, consumed_receipts, now=now
        ))

    return findings


def _diagnose_one_receipt_chain(
    conn: sqlite3.Connection,
    workspace: Workspace,
    receipt_id: str,
    chain: list[dict[str, Any]],
    consumed_receipts: list[tuple[str, str, str, int]],
    *,
    now: str | None = None,
) -> list[Finding]:
    findings: list[Finding] = []
    expected_order = RECEIPT_TRANSITION_ORDER
    seen_statuses: list[str] = []
    authorized_rowid: int | None = None
    authorized_payload: dict[str, Any] | None = None
    authorized_harness_fingerprint: str | None = None
    claimed_expected_after: str | None = None
    applied_after: str | None = None
    ws: str | None = None
    task: str | None = None
    actor: str | None = None
    conflicts: list[str] = []
    predecessor_missing: list[str] = []

    def _check_fp(name: str, value: Any, rowid: int | None) -> bool:
        """Return True if the fingerprint value is a canonical SHA-256."""
        if not value:
            return False
        if not _is_canonical_sha256(value):
            conflicts.append(
                f"{name} {value!r} is not a canonical SHA-256 at rowid {rowid}"
            )
            return False
        return True

    for event in chain:
        payload = event.get("payload") or {}
        status = payload.get("status")
        rowid = event.get("rowid")
        if status == STATUS_AUTHORIZED:
            authorized_rowid = rowid
            authorized_payload = payload
            fp = payload.get("harness_fingerprint")
            if not fp:
                predecessor_missing.append(
                    f"authorized event missing harness_fingerprint at rowid {rowid}"
                )
            elif not _is_canonical_sha256(fp):
                conflicts.append(
                    f"authorized harness_fingerprint {fp!r} is not a canonical SHA-256 at rowid {rowid}"
                )
            else:
                authorized_harness_fingerprint = fp
        if status not in expected_order:
            conflicts.append(f"unknown status {status!r} at rowid {rowid}")
            continue
        # Validate monotonic order: each status may appear only after its predecessors.
        idx = expected_order.index(status)
        required = expected_order[:idx]
        missing = [s for s in required if s not in seen_statuses]
        if missing:
            predecessor_missing.append(
                f"{status} at rowid {rowid} missing predecessors {missing}"
            )
        if status in seen_statuses:
            # Duplicate transition, including duplicate consumed, is a conflict.
            conflicts.append(f"duplicate {status} at rowid {rowid}")
            continue
        seen_statuses.append(status)

        # Consistency checks.
        ev_ws = payload.get("workspace_id")
        ev_task = payload.get("task_id")
        ev_actor = payload.get("authorized_actor") or payload.get("actor")
        if ws is None and ev_ws:
            ws = ev_ws
        if task is None and ev_task:
            task = ev_task
        if actor is None and ev_actor:
            actor = ev_actor
        if ev_ws and ev_ws != ws:
            conflicts.append(f"workspace_id mismatch: {ev_ws!r} != {ws!r} at rowid {rowid}")
        if ev_task and ev_task != task:
            conflicts.append(f"task_id mismatch: {ev_task!r} != {task!r} at rowid {rowid}")
        if ev_actor and ev_actor != actor:
            conflicts.append(f"actor mismatch: {ev_actor!r} != {actor!r} at rowid {rowid}")

        # Immutable fingerprint/required-link checks.
        if status == STATUS_CLAIMED:
            before_fingerprint = payload.get("before_fingerprint")
            if not before_fingerprint:
                predecessor_missing.append(
                    f"claimed event missing before_fingerprint at rowid {rowid}"
                )
            elif not _is_canonical_sha256(before_fingerprint):
                conflicts.append(
                    f"claimed before_fingerprint {before_fingerprint!r} is not a canonical SHA-256 at rowid {rowid}"
                )
            elif before_fingerprint != authorized_harness_fingerprint:
                conflicts.append(
                    f"claimed before_fingerprint {before_fingerprint!r} does not match "
                    f"authorized harness_fingerprint {authorized_harness_fingerprint!r} at rowid {rowid}"
                )
            expected_after = payload.get("expected_after_fingerprint")
            if not expected_after:
                predecessor_missing.append(
                    f"claimed event missing expected_after_fingerprint at rowid {rowid}"
                )
            elif not _is_canonical_sha256(expected_after):
                conflicts.append(
                    f"claimed expected_after_fingerprint {expected_after!r} is not a canonical SHA-256 at rowid {rowid}"
                )
            else:
                claimed_expected_after = expected_after
        elif status == STATUS_APPLIED:
            before_fp = payload.get("before_fingerprint")
            if not before_fp:
                predecessor_missing.append(
                    f"applied event missing before_fingerprint at rowid {rowid}"
                )
            elif not _is_canonical_sha256(before_fp):
                conflicts.append(
                    f"applied before_fingerprint {before_fp!r} is not a canonical SHA-256 at rowid {rowid}"
                )
            elif before_fp != authorized_harness_fingerprint:
                # applied.before must equal the claimed before, which already matched authorized.harness.
                conflicts.append(
                    f"applied before_fingerprint {before_fp!r} does not match "
                    f"claimed before_fingerprint {authorized_harness_fingerprint!r} at rowid {rowid}"
                )
            after_fp = payload.get("after_fingerprint")
            if not after_fp:
                predecessor_missing.append(
                    f"applied event missing after_fingerprint at rowid {rowid}"
                )
            elif not _is_canonical_sha256(after_fp):
                conflicts.append(
                    f"applied after_fingerprint {after_fp!r} is not a canonical SHA-256 at rowid {rowid}"
                )
            elif after_fp != claimed_expected_after:
                conflicts.append(
                    f"applied after_fingerprint {after_fp!r} does not match "
                    f"claimed expected_after_fingerprint {claimed_expected_after!r} at rowid {rowid}"
                )
            else:
                applied_after = after_fp
        elif status == STATUS_CONSUMED:
            task_done_event_id = payload.get("task_done_event_id")
            if not task_done_event_id:
                predecessor_missing.append("consumed event missing task_done_event_id")
            elif not applied_after:
                predecessor_missing.append(
                    "consumed event missing applied after_fingerprint predecessor"
                )
            else:
                task_done_row = conn.execute(
                    "SELECT * FROM events WHERE id = ?", (task_done_event_id,)
                ).fetchone()
                if task_done_row is None:
                    predecessor_missing.append(
                        f"consumed event references missing task.done {task_done_event_id}"
                    )
                elif task_done_row["event_type"] != "task.done":
                    predecessor_missing.append(
                        f"consumed event references non-task.done event {task_done_event_id}"
                    )
                elif task_done_row["workspace_id"] != ws or task_done_row["task_id"] != task:
                    conflicts.append(
                        f"consumed event task.done {task_done_event_id} has wrong workspace/task"
                    )
                else:
                    task_done_payload = _json_loads(task_done_row["payload_json"])
                    if task_done_payload.get("receipt_id") != receipt_id:
                        conflicts.append(
                            f"consumed event task.done {task_done_event_id} references another receipt"
                        )
                    else:
                        applied_fingerprint = task_done_payload.get("applied_fingerprint")
                        if not applied_fingerprint:
                            predecessor_missing.append(
                                f"task.done {task_done_event_id} missing applied_fingerprint"
                            )
                        elif not _is_canonical_sha256(applied_fingerprint):
                            conflicts.append(
                                f"task.done applied_fingerprint {applied_fingerprint!r} is not a canonical SHA-256"
                            )
                        elif applied_fingerprint != applied_after:
                            conflicts.append(
                                f"consumed task.done applied_fingerprint {applied_fingerprint!r} does not match "
                                f"applied after_fingerprint {applied_after!r}"
                            )

    if conflicts:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "receipt_chain_conflict", workspace.id, receipt_id=receipt_id
            ),
            kind="receipt_chain_conflict",
            severity=SEVERITY_ERROR,
            scope="receipt",
            workspace_id=workspace.id,
            receipt_id=receipt_id,
            authority="events",
            evidence=_evidence(
                chain_length=len(chain),
                chain_statuses=[(e.get("rowid"), (e.get("payload") or {}).get("status")) for e in chain],
                conflicts=conflicts,
            ),
            repairable=False,
            next_action="Escalate: receipt event chain has conflicting workspace/task/actor/fingerprint linkage.",
        ))

    if predecessor_missing:
        findings.append(Finding(
            finding_id=_make_finding_id(
                "receipt_chain_incomplete", workspace.id, receipt_id=receipt_id
            ),
            kind="receipt_chain_incomplete",
            severity=SEVERITY_ERROR,
            scope="receipt",
            workspace_id=workspace.id,
            receipt_id=receipt_id,
            authority="events",
            evidence=_evidence(
                chain_length=len(chain),
                chain_statuses=[(e.get("rowid"), (e.get("payload") or {}).get("status")) for e in chain],
                missing_predecessors=predecessor_missing,
            ),
            repairable=False,
            next_action="Escalate: receipt chain is missing required predecessor transitions.",
        ))

    # Terminal consumed chain summary.
    if STATUS_CONSUMED in seen_statuses and not conflicts and not predecessor_missing:
        consumed_event = next(
            e for e in chain if (e.get("payload") or {}).get("status") == STATUS_CONSUMED
        )
        findings.append(Finding(
            finding_id=_make_finding_id(
                "receipt_terminal", workspace.id, receipt_id=receipt_id
            ),
            kind="receipt_terminal",
            severity=SEVERITY_INFO,
            scope="receipt",
            workspace_id=workspace.id,
            receipt_id=receipt_id,
            authority="events",
            evidence=_evidence(
                workspace_id=ws,
                task_id=task,
                actor=actor,
                consumed_event_id=consumed_event.get("id"),
                consumed_event_rowid=consumed_event.get("rowid"),
            ),
            repairable=False,
            next_action="Receipt is consumed; no further action.",
        ))

    # Authorized unused warnings.
    if (
        STATUS_AUTHORIZED in seen_statuses
        and STATUS_CLAIMED not in seen_statuses
        and not conflicts
        and not predecessor_missing
    ):
        superseded = False
        expired = False
        if ws and task and authorized_rowid is not None:
            for other_id, other_ws, other_task, other_rowid in consumed_receipts:
                if other_id == receipt_id:
                    continue
                if other_ws == ws and other_task == task and other_rowid > authorized_rowid:
                    superseded = True
                    break
        if not superseded and authorized_payload:
            expires_at = authorized_payload.get("expires_at")
            if expires_at and _utc_now_str(now) > expires_at:
                expired = True
        if superseded or expired:
            findings.append(Finding(
                finding_id=_make_finding_id(
                    "receipt_authorization_unused", workspace.id, receipt_id=receipt_id
                ),
                kind="receipt_authorization_unused",
                severity=SEVERITY_WARNING,
                scope="receipt",
                workspace_id=workspace.id,
                receipt_id=receipt_id,
                authority="events",
                evidence=_evidence(
                    workspace_id=ws,
                    task_id=task,
                    authorized_rowid=authorized_rowid,
                    superseded=superseded,
                    expired=expired,
                ),
                repairable=False,
                next_action="Receipt is unused and superseded/expired; no action required unless it was applied.",
            ))

    return findings
