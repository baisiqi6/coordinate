from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from . import db


_DISCORD_CHANNEL_NAME_MAX_CODE_POINTS = 100
_IDEMPOTENCY_KEY_MAX_CODE_POINTS = 200
REQUEST_EVENT_TYPE = "channel.provision.requested"
PROVISIONED_EVENT_TYPE = "channel.provisioned"
FAILED_EVENT_TYPE = "channel.provision.failed"
_FAILURE_REASON_CODES = frozenset(
    {
        "unknown_workspace",
        "binding_conflict",
        "recovery_marker_conflict",
        "control_channel_unavailable",
        "discord_api_unavailable",
        "internal",
    }
)


@dataclass(frozen=True)
class DiscordProvisioningState:
    workspace: db.Workspace
    channel_id: str | None


def _request_target(workspace_id: str) -> str:
    return f"discord-workspace:{workspace_id}"


def _request_key(workspace_id: str, idempotency_key: str) -> str:
    return f"{workspace_id}:discord-channel-provision-request:{idempotency_key}"


def _terminal_key(request_event_id: str) -> str:
    return f"discord-channel-provision-request:{request_event_id}:terminal"


def _validate_idempotency_key(value: Any) -> str:
    key = str(value) if value is not None else ""
    if not key or key != key.strip():
        raise ValueError("idempotency_key is required without surrounding whitespace")
    if len(key) > _IDEMPOTENCY_KEY_MAX_CODE_POINTS:
        raise ValueError("idempotency_key is too long")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in key):
        raise ValueError("idempotency_key must not contain control characters")
    return key


def _terminal_for_request(conn, request_event_id: str):
    rows = conn.execute(
        """
        SELECT rowid, * FROM events
        WHERE causation_id = ? AND event_type IN (?, ?)
        ORDER BY rowid
        """,
        (request_event_id, PROVISIONED_EVENT_TYPE, FAILED_EVENT_TYPE),
    ).fetchall()
    if len(rows) > 1:
        raise ValueError("channel provisioning request has multiple terminal events")
    return rows[0] if rows else None


def _pending_requests(conn, workspace_id: str | None = None) -> list[Any]:
    params: list[Any] = [REQUEST_EVENT_TYPE, PROVISIONED_EVENT_TYPE, FAILED_EVENT_TYPE]
    workspace_clause = ""
    if workspace_id is not None:
        workspace_clause = "AND request.workspace_id = ?"
        params.append(workspace_id)
    return conn.execute(
        f"""
        SELECT request.rowid, request.*
        FROM events AS request
        WHERE request.event_type = ?
          AND NOT EXISTS (
            SELECT 1 FROM events AS terminal
            WHERE terminal.causation_id = request.id
              AND terminal.event_type IN (?, ?)
          )
          {workspace_clause}
        ORDER BY request.rowid
        """,
        params,
    ).fetchall()


def _assert_request_matches(
    event,
    *,
    actor: str,
    workspace_id: str,
    target: str,
    payload_json: str,
) -> None:
    exact = (
        event["event_type"] == REQUEST_EVENT_TYPE
        and event["actor"] == actor
        and event["workspace_id"] == workspace_id
        and event["target"] == target
        and event["payload_json"] == payload_json
    )
    if not exact:
        raise ValueError("request replay: idempotency key parameters differ")


def _project_request(conn, request_event) -> dict[str, Any]:
    payload = json.loads(request_event["payload_json"] or "{}")
    result: dict[str, Any] = {
        "workspace_id": request_event["workspace_id"],
        "channel_name": payload["channel_name"],
        "request_event_id": request_event["id"],
        "status": "pending",
    }
    terminal = _terminal_for_request(conn, request_event["id"])
    if terminal is None:
        return result
    terminal_payload = json.loads(terminal["payload_json"] or "{}")
    if terminal["event_type"] == PROVISIONED_EVENT_TYPE:
        result.update(status="provisioned", channel_id=terminal_payload["channel_id"])
    else:
        result.update(status="failed", reason_code=terminal_payload["reason_code"])
    result["terminal_event_id"] = terminal["id"]
    return result


def request_discord_channel_provision(
    conn,
    *,
    workspace_id: Any,
    channel_name: Any,
    actor: Any,
    idempotency_key: Any,
) -> dict[str, Any]:
    """Record or replay one durable MCP provisioning request."""
    workspace = str(workspace_id).strip() if workspace_id is not None else ""
    principal = str(actor).strip() if actor is not None else ""
    if not workspace or not principal:
        raise ValueError("workspace_id and actor are required")
    if db.get_workspace(conn, workspace) is None:
        raise ValueError(f"unknown workspace: {workspace}")
    name = normalize_discord_channel_name(channel_name)
    caller_key = _validate_idempotency_key(idempotency_key)
    key = _request_key(workspace, caller_key)
    target = _request_target(workspace)
    payload = {"platform": "discord", "channel_name": name}
    payload_json = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (key,)
    ).fetchone()
    if existing is not None:
        _assert_request_matches(
            existing,
            actor=principal,
            workspace_id=workspace,
            target=target,
            payload_json=payload_json,
        )
        return _project_request(conn, existing)

    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        # Recheck after taking the write lock: two MCP processes must not both
        # introduce a different pending request for one workspace.
        existing = conn.execute(
            "SELECT * FROM events WHERE idempotency_key = ?", (key,)
        ).fetchone()
        if existing is not None:
            _assert_request_matches(
                existing,
                actor=principal,
                workspace_id=workspace,
                target=target,
                payload_json=payload_json,
            )
            if owns_transaction:
                conn.commit()
            return _project_request(conn, existing)
        if _pending_requests(conn, workspace):
            raise ValueError("request replay: channel provisioning already pending")

        event = db.append_event(
            conn,
            event_type=REQUEST_EVENT_TYPE,
            actor=principal,
            workspace_id=workspace,
            target=target,
            idempotency_key=key,
            payload=payload,
            commit=False,
        )
        if not event.created:
            raise ValueError("request replay: concurrent idempotency conflict")
        if owns_transaction:
            conn.commit()
        return _project_request(conn, event.row)
    except Exception:
        if owns_transaction and conn.in_transaction:
            conn.rollback()
        raise


def list_pending_discord_channel_requests(conn) -> list[Any]:
    """Derive durable pending requests from append-only facts, oldest first."""
    return _pending_requests(conn)


def finish_discord_channel_request(
    conn,
    *,
    request_event_id: str,
    channel_id: str | None = None,
    reason_code: str | None = None,
) -> dict[str, Any]:
    """Append one request-scoped terminal event and return its projection."""
    request = db.get_event(conn, request_event_id)
    if request["event_type"] != REQUEST_EVENT_TYPE:
        raise ValueError("channel provisioning terminal causation is not a request")
    prior = _terminal_for_request(conn, request_event_id)
    if prior is not None:
        return _project_request(conn, request)
    if (channel_id is None) == (reason_code is None):
        raise ValueError("exactly one terminal outcome is required")
    if channel_id is not None:
        event_type = PROVISIONED_EVENT_TYPE
        payload = {"platform": "discord", "channel_id": str(channel_id)}
    else:
        if reason_code not in _FAILURE_REASON_CODES:
            raise ValueError("invalid channel provisioning failure reason")
        event_type = FAILED_EVENT_TYPE
        payload = {"platform": "discord", "reason_code": str(reason_code)}
    result = db.append_event(
        conn,
        event_type=event_type,
        actor="coordinator-daemon",
        workspace_id=request["workspace_id"],
        target=request["target"],
        causation_id=request_event_id,
        idempotency_key=_terminal_key(request_event_id),
        payload=payload,
    )
    if not result.created:
        exact = (
            result.row["event_type"] == event_type
            and result.row["actor"] == "coordinator-daemon"
            and result.row["workspace_id"] == request["workspace_id"]
            and result.row["target"] == request["target"]
            and result.row["causation_id"] == request_event_id
            and result.row["payload_json"]
            == json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
        )
        if not exact:
            raise ValueError("channel provisioning terminal idempotency conflict")
    return _project_request(conn, request)


def normalize_discord_channel_name(value: str) -> str:
    """Validate a caller-supplied Discord channel name without normalizing it."""
    name = str(value)
    if not name:
        raise ValueError("channel_name is required")
    if name != name.strip():
        raise ValueError("channel_name must not have leading/trailing whitespace")
    if len(name) > _DISCORD_CHANNEL_NAME_MAX_CODE_POINTS:
        raise ValueError(
            f"channel_name exceeds {_DISCORD_CHANNEL_NAME_MAX_CODE_POINTS} code points"
        )
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in name):
        raise ValueError("channel_name must not contain control characters")
    return name


def workspace_topic_marker(workspace_id: str) -> str:
    """Return the stable recovery marker stored in a managed Discord topic."""
    workspace = str(workspace_id).strip()
    if not workspace:
        raise ValueError("workspace_id is required")
    digest = hashlib.sha256(workspace.encode("utf-8")).hexdigest()
    return f"coordinate-managed-workspace:sha256={digest}"


def inspect_discord_provisioning(
    conn,
    *,
    workspace_id: str,
) -> DiscordProvisioningState:
    workspace = db.get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    bindings = db.list_channel_bindings(
        conn,
        platform="discord",
        workspace_id=workspace_id,
    )
    if len(bindings) > 1:
        raise ValueError(
            f"workspace {workspace_id!r} has multiple Discord channel bindings; "
            "release the obsolete binding before provisioning"
        )
    return DiscordProvisioningState(
        workspace=workspace,
        channel_id=bindings[0].channel_id if bindings else None,
    )


def ensure_discord_delivery_route(
    conn,
    *,
    workspace: db.Workspace,
    channel_id: str,
) -> db.Workspace:
    """Point the existing workspace delivery route at its canonical Discord channel."""
    return db.upsert_workspace(
        conn,
        workspace_id=workspace.id,
        name=workspace.name,
        path=workspace.path,
        harness_root=workspace.harness_root,
        harnessctl_path=workspace.harnessctl_path,
        default_bus="discord_webhook",
        default_destination=str(channel_id),
        base_branch=workspace.base_branch,
        branch_namespace=workspace.branch_namespace,
    )
