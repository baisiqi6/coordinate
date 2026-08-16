"""Bounded agent-facing application facade for the MCP adapter (R1).

This module is the only error-sanitization boundary between MCP tool calls and
the existing Coordinate domain/application core. It deliberately does NOT:

- open a connection on the server startup thread; every call creates and closes
  its own SQLite connection inside the calling (SDK worker) thread;
- re-implement domain validation or transaction boundaries;
- call ``print``/``print_json``, the CLI ``main()``, shell, SSH or subprocesses;
- hold process-level mutable business state.

Every public method returns the stable R1 envelope::

    {"ok": true, "data": {...}, "error": null}
    {"ok": false, "data": null, "error": {"code": "...", "message": "..."}}

``actor`` is fixed from server startup configuration; tool callers can never
inject it.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from .audit import audit_workspace
from .channel_provisioning import request_discord_channel_provision
from .completion import (
    CompletionReceiptError,
    apply_completion_receipt,
    claim_completion_receipt,
    consume_completion_receipt,
    lookup_receipt_for_preflight,
    parse_iso_timestamp,
    preflight_expired,
    prepare_completion_receipt,
)
from .db import get_workspace, row_to_dict
from .onboarding import create_plan_task_record
from .operator import list_pending_actions, pending_snapshot_metadata
from .runtime import list_agents
from .split_operations import SplitOperationError
from .runtime_interface import (
    MESSAGE_HOST_PROFILE_NOT_FOUND,
    MESSAGE_INTERNAL,
    MESSAGE_INVALID_REQUEST,
    MESSAGE_INVALID_ROUTING_REQUEST,
    MESSAGE_JOB_NOT_FOUND,
    MESSAGE_REPLAY_CONFLICT,
    MESSAGE_TASK_MIRROR_NOT_FOUND,
    MESSAGE_UNAVAILABLE,
    MESSAGE_UNKNOWN_AGENT,
    MESSAGE_UNKNOWN_WORKSPACE,
    RuntimeInterface,
    _classify_error,
    error_envelope,
    ok_envelope,
)

# ``_ROUTING_BUILDER_FIELDS`` and the envelope/error helpers are owned by the
# shared runtime interface (R2A); this module re-exports them for callers that
# imported them from the R1 facade.
from .runtime_interface import (  # noqa: E402  (re-export)
    _ROUTING_BUILDER_FIELDS,
)

logger = logging.getLogger("coordinate.agent_interface")

# Static per-reason wire messages for the split-operation and completion
# receipt domains (R5B).  The domain ``reason`` code is preserved as the
# envelope error code (stable, machine-readable); the message is always this
# static text — exception text, paths, values, tokens and SQL never reach the
# wire, and any unknown reason degrades to one static default.
_REASON_MESSAGES: dict[str, str] = {
    # split operations / checklist boundary
    "files_not_deployed": "deployed files are not available",
    "operation_conflict": "operation conflict",
    "fingerprint_drift": "deployed fingerprint drift",
    "lock_timeout": "checklist lock timeout",
    "validation_error": "invalid input",
    "legacy_unbound_item": "legacy unbound item",
    "checklist_missing": "deployed checklist is missing",
    "dual_authority": "dual file authority detected",
    "phase_not_creatable": "phase is not creatable",
    # completion receipts / preflight
    "unknown_workspace": "unknown workspace",
    "gate_not_passed": "completion gate not passed",
    "harness_fingerprint_unavailable": "harness fingerprint unavailable",
    "harness_item_missing": "checklist item missing",
    "forge_gate_failed": "forge gate failed",
    "unknown_receipt": "unknown receipt",
    "workspace_mismatch": "workspace mismatch",
    "task_mismatch": "task mismatch",
    "actor_mismatch": "actor mismatch",
    "malformed_expiry": "receipt expiry malformed",
    "expired": "receipt expired",
    "before_fingerprint_mismatch": "before fingerprint mismatch",
    "fingerprint_mismatch": "fingerprint mismatch",
    "already_consumed": "receipt already consumed",
    "not_claimed": "receipt not claimed",
    "after_fingerprint_mismatch": "after fingerprint mismatch",
    "consumed_without_task_done": "consumed receipt missing task.done",
    "not_applied": "receipt not applied",
    "task_already_done_other_authority": "task already done under another authority",
    "deployed_not_done": "deployed harness not done",
    "deployed_terminal_ownership_unreleased": "deployed terminal ownership unreleased",
    "deployed_harness_unavailable": "deployed harness unavailable",
    "deployed_task_missing": "deployed task missing",
    "receipt_chain_incomplete": "receipt chain incomplete",
    "receipt_chain_conflict": "receipt chain conflict",
    "receipt_chain_broken": "receipt chain broken",
}
_DEFAULT_REASON_MESSAGE = "operation refused"


def _reason_error_envelope(reason: str) -> dict[str, Any]:
    """Map one domain ``reason`` code to a static (code, message) envelope."""
    return error_envelope(
        reason, _REASON_MESSAGES.get(reason, _DEFAULT_REASON_MESSAGE)
    )


@dataclass(frozen=True)
class AgentInterfaceConfig:
    """Startup configuration for the agent-facing facade."""

    db_path: str
    actor: str = "mcp"


class AgentInterface:
    """Bounded agent-facing facade over existing Coordinate domain functions.

    ``runtime_request_submit`` and ``runtime_job_get`` delegate to the shared
    ``RuntimeInterface`` (R2A) so MCP and HTTP share one submit/get core;
    the remaining R1 tools stay local to this facade.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], sqlite3.Connection],
        actor: str = "mcp",
    ) -> None:
        self._connection_factory = connection_factory
        self._actor = actor
        self._runtime = RuntimeInterface(
            connection_factory=connection_factory, actor=actor
        )

    @classmethod
    def from_config(cls, config: AgentInterfaceConfig) -> "AgentInterface":
        from pathlib import Path

        from .db import initialize

        db_path = Path(config.db_path).expanduser()

        def _factory() -> sqlite3.Connection:
            return initialize(db_path)

        return cls(connection_factory=_factory, actor=config.actor)

    # -- tool 1: operator_pending ------------------------------------------

    def operator_pending(self, workspace_id: str | None) -> dict[str, Any]:
        try:
            if workspace_id is None or not str(workspace_id).strip():
                return error_envelope("invalid_request", "workspace_id is required")
            with closing(self._connection_factory()) as conn, conn:
                if get_workspace(conn, workspace_id) is None:
                    return error_envelope(
                        "not_found", MESSAGE_UNKNOWN_WORKSPACE
                    )
                actions = list_pending_actions(conn, workspace_id=workspace_id)
                snapshot = pending_snapshot_metadata(conn, workspace_id=workspace_id)
            return ok_envelope(
                {
                    "pending_actions": [a.to_dict() for a in actions],
                    "snapshot": snapshot,
                }
            )
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    # -- tool 2: workspace_audit --------------------------------------------

    def workspace_audit(self, workspace_id: str | None) -> dict[str, Any]:
        try:
            if workspace_id is None or not str(workspace_id).strip():
                return error_envelope("invalid_request", "workspace_id is required")
            with closing(self._connection_factory()) as conn, conn:
                report = audit_workspace(
                    conn,
                    workspace_id=workspace_id,
                    refresh=False,
                )
            return ok_envelope(report.to_dict())
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    # -- tool 3: runtime_request_submit -------------------------------------
    # Shared with the HTTP adapter via RuntimeInterface; signature keeps the
    # R1 wire shape (no actor parameter; identity is fixed by startup config).

    def runtime_request_submit(
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
        # Actor is fixed by the startup config of the shared RuntimeInterface;
        # the facade never accepts or overrides it.
        return self._runtime.submit_request(
            workspace_id=workspace_id,
            prompt=prompt,
            origin=origin,
            reply=reply,
            task_id=task_id,
            target_agent=target_agent,
            routing_request=routing_request,
            worktree_path=worktree_path,
            idempotency_key=idempotency_key,
        )

    # -- tool 4: runtime_job_get ---------------------------------------------

    def runtime_job_get(self, job_id: str | None) -> dict[str, Any]:
        return self._runtime.get_job(job_id)

    # -- tool 5: runtime_agent_list ------------------------------------------

    def runtime_agent_list(self) -> dict[str, Any]:
        try:
            with closing(self._connection_factory()) as conn, conn:
                agents = list_agents(conn)
            return ok_envelope({"agents": agents})
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    def channel_create(
        self,
        *,
        workspace_id: str | None,
        channel_name: str | None,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        """Record/replay one durable Discord provisioning request."""
        try:
            with closing(self._connection_factory()) as conn, conn:
                result = request_discord_channel_provision(
                    conn,
                    workspace_id=workspace_id,
                    channel_name=channel_name,
                    actor=self._actor,
                    idempotency_key=idempotency_key,
                )
            return ok_envelope(result)
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    # -- tool 6: task_create_record (R5B) ------------------------------------
    # Record half only: the deployed checklist envelope and fingerprints are
    # re-derived from the registered workspace's deployed bytes; the caller
    # supplies scalars only. No payload dict, no arbitrary path, no actor and
    # no full envelope: actor is the fixed request-scoped principal and the
    # payload is always None (the deployed file half is the only authority).

    def task_create_record(
        self,
        *,
        workspace_id: str | None,
        operation_id: str | None,
        input_fingerprint: str | None,
        before_fingerprint: str | None,
        after_fingerprint: str | None,
        task_id: str | None,
        plan_doc: str | None,
        title: str | None = None,
        phase: str | None = None,
        owner: str | None = None,
        branch: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        try:
            with closing(self._connection_factory()) as conn, conn:
                result = create_plan_task_record(
                    conn,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    plan_doc=plan_doc,
                    title=title,
                    phase=phase,
                    owner=owner,
                    branch=branch,
                    actor=self._actor,
                    target="worker",
                    payload=None,
                    idempotency_key=idempotency_key,
                    operation_id=operation_id,
                    input_fingerprint=input_fingerprint,
                    before_fingerprint=before_fingerprint,
                    after_fingerprint=after_fingerprint,
                )
            return ok_envelope(result.to_dict())
        except SplitOperationError as exc:
            return _reason_error_envelope(exc.reason)
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    # -- tools 7-11: completion receipt lifecycle (R5B) ----------------------
    # prepare/preflight/claim/apply/consume.  ``requester`` and
    # ``authorized_actor`` are fixed to the request-scoped principal actor;
    # callers cannot set them (the facade signature has no such parameters).

    def completion_prepare(
        self, *, workspace_id: str | None, task_id: str | None
    ) -> dict[str, Any]:
        try:
            with closing(self._connection_factory()) as conn, conn:
                receipt = prepare_completion_receipt(
                    conn,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    requester=self._actor,
                    authorized_actor=self._actor,
                )
            return ok_envelope(receipt.to_dict())
        except CompletionReceiptError as exc:
            return _reason_error_envelope(exc.reason)
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    def completion_preflight(
        self, *, workspace_id: str | None, receipt_id: str | None
    ) -> dict[str, Any]:
        try:
            if not workspace_id or not receipt_id:
                return error_envelope("invalid_request", MESSAGE_INVALID_REQUEST)
            with closing(self._connection_factory()) as conn, conn:
                state = lookup_receipt_for_preflight(conn, receipt_id)
            if state is None:
                raise CompletionReceiptError(
                    f"unknown receipt: {receipt_id}", reason="unknown_receipt",
                )
            if state.get("broken"):
                raise CompletionReceiptError(
                    state.get("message") or "receipt chain is broken",
                    reason=state.get("reason") or "receipt_chain_broken",
                )
            # The middleware already scoped the caller's workspace_id; the
            # domain state must bind to it.
            if state.get("workspace_id") != workspace_id:
                raise CompletionReceiptError(
                    f"receipt workspace {state.get('workspace_id')!r} does not "
                    f"match {workspace_id!r}",
                    reason="workspace_mismatch",
                )
            if preflight_expired(state):
                raise CompletionReceiptError(
                    "receipt expired", reason="expired",
                )
            return ok_envelope({
                "ok": True,
                "receipt_id": receipt_id,
                "workspace_id": state.get("workspace_id"),
                "task_id": state.get("task_id"),
                "status": state.get("status"),
                "issued_at": state.get("issued_at"),
                "expires_at": state.get("expires_at"),
                "actor": state.get("actor"),
                "terminal_event_id": state.get("terminal_event_id"),
            })
        except CompletionReceiptError as exc:
            return _reason_error_envelope(exc.reason)
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    def completion_claim(
        self,
        *,
        workspace_id: str | None,
        receipt_id: str | None,
        task_id: str | None,
        before_fingerprint: str | None,
        expected_after_fingerprint: str | None,
    ) -> dict[str, Any]:
        try:
            with closing(self._connection_factory()) as conn, conn:
                result = claim_completion_receipt(
                    conn,
                    receipt_id=receipt_id,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    actor=self._actor,
                    before_fingerprint=before_fingerprint,
                    expected_after_fingerprint=expected_after_fingerprint,
                )
            return ok_envelope(result.to_dict())
        except CompletionReceiptError as exc:
            return _reason_error_envelope(exc.reason)
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    def completion_apply(
        self,
        *,
        workspace_id: str | None,
        receipt_id: str | None,
        task_id: str | None,
        after_fingerprint: str | None,
    ) -> dict[str, Any]:
        try:
            with closing(self._connection_factory()) as conn, conn:
                result = apply_completion_receipt(
                    conn,
                    receipt_id=receipt_id,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    actor=self._actor,
                    after_fingerprint=after_fingerprint,
                )
            return ok_envelope(result.to_dict())
        except CompletionReceiptError as exc:
            return _reason_error_envelope(exc.reason)
        except Exception as exc:
            return error_envelope(*_classify_error(exc))

    def completion_consume(
        self,
        *,
        workspace_id: str | None,
        receipt_id: str | None,
        verification: str | None = None,
    ) -> dict[str, Any]:
        try:
            with closing(self._connection_factory()) as conn, conn:
                result = consume_completion_receipt(
                    conn,
                    receipt_id=receipt_id,
                    actor=self._actor,
                    verification=verification,
                    expected_workspace_id=workspace_id,
                )
            return ok_envelope(result.to_dict())
        except CompletionReceiptError as exc:
            return _reason_error_envelope(exc.reason)
        except Exception as exc:
            return error_envelope(*_classify_error(exc))
