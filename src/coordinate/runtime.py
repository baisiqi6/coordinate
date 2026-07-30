from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .agent_report import AgentReport, parse_agent_report
from .db_support import _absolute_path
from .db import (
    append_event,
    create_delivery,
    create_job,
    get_job,
    get_runner_profile,
    get_workspace,
    get_workspace_host_profile,
    row_to_dict,
    upsert_runner_profile,
    utc_now,
)
from .executor_identity import (
    _binding_snapshot_canonical_dict,
    executor_binding_claim_evidence,
    resolve_exact_executor_binding,
)
from .executor_routing import (
    ExecutorRoutingError,
    RoutingRequest,
    _validate_routing_cross_links,
    parse_routing_request,
    resolve_routing_candidates,
    routing_claim_evidence,
    routing_decision_to_dict,
    routing_request_to_dict,
    select_routing_decision,
    validate_routing_decision,
)
from .execution_context import (
    CONTRACT_VERSION,
    ContextError,
    ExecutionContextV1,
    execution_context_dict_matches,
    resolve_execution_context_v1,
    validate_execution_context_snapshot,
)
from .execution_leases import (
    LeaseError as LeasePrimitiveError,
    attempt_has_any_lease,
)
from .runtime_lease import (
    RuntimeLeaseError,
    _validate_claim_reap_policy,
    _validate_id,
    _validate_reason,
    _validate_utc_timestamp,
    append_terminal_events_and_delivery,
    claim_leased_job,
    release_lease_for_terminal_report,
    require_mutation_authority,
)


def _lease_from_result(result: dict[str, Any] | None) -> tuple[str | None, int | None]:
    if not result:
        return None, None
    lease = result.get("execution_lease") if isinstance(result, dict) else None
    if not lease:
        return None, None
    return lease.get("lease_id"), lease.get("attempt_token")


class RuntimeError(ValueError):
    pass


@dataclass(frozen=True)
class RuntimeAgentResult:
    agent: dict[str, Any]
    event: dict[str, Any]
    event_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent,
            "event": self.event,
            "event_created": self.event_created,
        }


@dataclass(frozen=True)
class RuntimeRequestResult:
    event: dict[str, Any]
    event_created: bool
    job: dict[str, Any]
    job_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "event": self.event,
            "event_created": self.event_created,
            "job": self.job,
            "job_created": self.job_created,
        }


@dataclass(frozen=True)
class RuntimeClaimResult:
    job: dict[str, Any] | None
    claimed: bool
    attempt_token: int | None = None
    execution_context: dict[str, Any] | None = None
    execution_lease: dict[str, Any] | None = None
    reason: str | None = None
    oldest_blocked_job_id: str | None = None
    oldest_blocked_resource_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "job": self.job,
            "claimed": self.claimed,
            "attempt_token": self.attempt_token,
            "execution_context": self.execution_context,
        }
        if self.execution_lease is not None:
            result["execution_lease"] = self.execution_lease
        if self.reason is not None:
            result["reason"] = self.reason
        if self.oldest_blocked_job_id is not None:
            result["oldest_blocked_job_id"] = self.oldest_blocked_job_id
        if self.oldest_blocked_resource_key is not None:
            result["oldest_blocked_resource_key"] = self.oldest_blocked_resource_key
        return result


@dataclass(frozen=True)
class RuntimeAgentDeactivateResult:
    agent: dict[str, Any]
    changed: bool
    deactivated: bool
    dry_run: bool
    blocked: bool
    reason: str | None
    blockers: dict[str, Any]
    event: dict[str, Any] | None
    event_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent,
            "changed": self.changed,
            "deactivated": self.deactivated,
            "dry_run": self.dry_run,
            "blocked": self.blocked,
            "reason": self.reason,
            "blockers": self.blockers,
            "event": self.event,
            "event_created": self.event_created,
        }


@dataclass(frozen=True)
class RuntimeReportResult:
    job: dict[str, Any]
    event: dict[str, Any]
    event_created: bool
    delivery: dict[str, Any] | None
    delivery_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": self.job,
            "event": self.event,
            "event_created": self.event_created,
            "delivery": self.delivery,
            "delivery_created": self.delivery_created,
        }


@dataclass(frozen=True)
class RuntimeProgressResult:
    job: dict[str, Any]
    event: dict[str, Any]
    event_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": self.job,
            "event": self.event,
            "event_created": self.event_created,
        }


def register_agent(
    conn: sqlite3.Connection,
    *,
    agent_id: str,
    host_id: str,
    capabilities: dict[str, Any] | None = None,
    client_type: str = "agentd",
    actor: str = "runtime",
) -> RuntimeAgentResult:
    if not agent_id.strip():
        raise RuntimeError("agent_id is required")
    if not host_id.strip():
        raise RuntimeError("host_id is required")
    if client_type not in {"agentd", "bridge"}:
        raise RuntimeError("client_type must be agentd or bridge")

    now = utc_now()
    caps = capabilities or {}
    conn.execute(
        """
        INSERT INTO agents (
          id, name, role, capabilities_json, online_state, current_load,
          host_id, client_type, last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          name = excluded.name,
          role = excluded.role,
          capabilities_json = excluded.capabilities_json,
          online_state = excluded.online_state,
          host_id = excluded.host_id,
          client_type = excluded.client_type,
          last_seen_at = excluded.last_seen_at,
          updated_at = excluded.updated_at
        """,
        (
            agent_id,
            agent_id,
            client_type,
            _json(caps),
            "online",
            0,
            host_id,
            client_type,
            now,
            now,
            now,
        ),
    )
    conn.commit()

    if client_type == "agentd" and get_runner_profile(conn, agent_id) is None:
        upsert_runner_profile(
            conn,
            profile_id=agent_id,
            name=agent_id,
            runner_type="agentd",
            command="",
            working_directory_strategy="current_dir",
            supports_stream_attach=False,
            env={},
        )

    event = append_event(
        conn,
        event_type="agent.registered",
        actor=actor,
        target=agent_id,
        idempotency_key=f"runtime:agent:{agent_id}:registered:{host_id}:{client_type}",
        payload={
            "agent_id": agent_id,
            "host_id": host_id,
            "client_type": client_type,
            "capabilities": caps,
            "last_seen_at": now,
        },
    )
    return RuntimeAgentResult(
        agent=_agent(conn, agent_id),
        event=row_to_dict(event.row),
        event_created=event.created,
    )


def heartbeat_agent(
    conn: sqlite3.Connection,
    *,
    agent_id: str,
    host_id: str,
    actor: str = "runtime",
) -> RuntimeAgentResult:
    row = conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"unknown agent: {agent_id}")
    if row["host_id"] and row["host_id"] != host_id:
        raise RuntimeError(f"agent {agent_id} is registered on host {row['host_id']}")

    now = utc_now()
    conn.execute(
        """
        UPDATE agents
        SET online_state = ?, host_id = ?, last_seen_at = ?, updated_at = ?
        WHERE id = ?
        """,
        ("online", host_id, now, now, agent_id),
    )
    conn.commit()
    event = append_event(
        conn,
        event_type="agent.heartbeat",
        actor=actor,
        target=agent_id,
        idempotency_key=f"runtime:agent:{agent_id}:heartbeat:{now}",
        payload={"agent_id": agent_id, "host_id": host_id, "last_seen_at": now},
    )
    return RuntimeAgentResult(
        agent=_agent(conn, agent_id),
        event=row_to_dict(event.row),
        event_created=event.created,
    )


def _deactivation_blockers(
    conn: sqlite3.Connection, agent_id: str
) -> dict[str, dict[str, Any]]:
    specs = (
        (
            "active_leases",
            "first_lease_id",
            "execution_attempt_leases",
            "lease_id",
            "agent_id = ? AND status = 'active'",
        ),
        (
            "pending_jobs",
            "first_job_id",
            "jobs",
            "id",
            "assigned_agent = ? AND status = 'pending'",
        ),
        (
            "running_jobs",
            "first_job_id",
            "jobs",
            "id",
            "assigned_agent = ? AND status = 'running'",
        ),
        (
            "recoverable_timed_out_jobs",
            "first_job_id",
            "jobs",
            "id",
            "assigned_agent = ? AND status = 'timed_out' AND recoverable = 1",
        ),
    )
    blockers: dict[str, dict[str, Any]] = {}
    for category, first_key, table, id_column, where in specs:
        row = conn.execute(
            f"SELECT COUNT(*) AS count, MIN({id_column}) AS first_id "
            f"FROM {table} WHERE {where}",
            (agent_id,),
        ).fetchone()
        blockers[category] = {
            "count": int(row["count"]),
            first_key: row["first_id"],
        }
    return blockers


def _validated_deactivation_events(
    conn: sqlite3.Connection, *, agent_id: str, host_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM events "
        "WHERE event_type = 'agent.deactivated' AND target = ? "
        "ORDER BY rowid",
        (agent_id,),
    ).fetchall()
    events: list[dict[str, Any]] = []
    previous_event_id: str | None = None
    expected_payload_keys = {
        "agent_id",
        "host_id",
        "actor",
        "reason",
        "previous_online_state",
        "deactivated_at",
        "generation",
        "previous_deactivation_event_id",
    }
    for generation, row in enumerate(rows, start=1):
        event = row_to_dict(row)
        payload = event.get("payload")
        if not isinstance(payload, dict) or set(payload) != expected_payload_keys:
            raise RuntimeError("invalid agent.deactivated audit payload")
        if (
            payload.get("agent_id") != agent_id
            or payload.get("host_id") != host_id
            or payload.get("previous_online_state") != "online"
            or isinstance(payload.get("generation"), bool)
            or payload.get("generation") != generation
            or payload.get("previous_deactivation_event_id") != previous_event_id
            or event.get("actor") != payload.get("actor")
            or event.get("idempotency_key")
            != f"runtime:agent:{agent_id}:deactivated:{generation}"
        ):
            raise RuntimeError("invalid agent.deactivated audit chain")
        try:
            _validate_id(event.get("id"), "stored deactivation event id")
            _validate_id(payload.get("actor"), "stored deactivation actor")
            _validate_reason(payload.get("reason"), "stored deactivation reason")
            _validate_utc_timestamp(
                payload.get("deactivated_at"), "stored deactivated_at"
            )
        except RuntimeLeaseError as exc:
            raise RuntimeError(str(exc)) from exc
        previous_event_id = event["id"]
        events.append(event)
    return events


def deactivate_agent(
    conn: sqlite3.Connection,
    *,
    agent_id: str,
    host_id: str,
    reason: str,
    actor: str = "runtime",
    dry_run: bool = False,
) -> RuntimeAgentDeactivateResult:
    """Deactivate a runtime agent with collision-free generation.

    Validates inputs, then in one BEGIN IMMEDIATE transaction:
    reads agent, checks blockers, optionally CAS to offline, appends audit event.
    """
    try:
        agent_id = _validate_id(agent_id, "agent_id")
        host_id = _validate_id(host_id, "host_id")
        reason = _validate_reason(reason, "reason")
        actor = _validate_id(actor, "actor")
    except RuntimeLeaseError as exc:
        raise RuntimeError(str(exc)) from exc
    if not isinstance(dry_run, bool):
        raise RuntimeError("dry_run must be a boolean")

    conn.execute("BEGIN IMMEDIATE")
    try:
        agent_row = conn.execute(
            "SELECT * FROM agents WHERE id = ?", (agent_id,)
        ).fetchone()
        if agent_row is None:
            raise RuntimeError(f"unknown agent: {agent_id}")
        agent = row_to_dict(agent_row)

        if agent.get("client_type") != "agentd":
            raise RuntimeError(
                f"only agentd clients can be deactivated, got {agent.get('client_type')!r}"
            )

        stored_host_id = agent.get("host_id")
        if not stored_host_id:
            raise RuntimeError(f"agent {agent_id!r} has no host_id")
        if stored_host_id != host_id:
            raise RuntimeError(
                f"host mismatch for agent {agent_id!r}: stored={stored_host_id!r}, "
                f"requested={host_id!r}"
            )

        online_state = agent.get("online_state")
        if online_state not in {"online", "offline"}:
            raise RuntimeError(
                f"unknown online_state {online_state!r} for agent {agent_id!r}"
            )

        blockers = _deactivation_blockers(conn, agent_id)
        if any(entry["count"] for entry in blockers.values()):
            conn.commit()
            return RuntimeAgentDeactivateResult(
                agent=agent,
                changed=False,
                deactivated=False,
                dry_run=dry_run,
                blocked=True,
                reason=reason,
                blockers=blockers,
                event=None,
                event_created=False,
            )

        if dry_run:
            conn.commit()
            return RuntimeAgentDeactivateResult(
                agent=agent,
                changed=False,
                deactivated=False,
                dry_run=True,
                blocked=False,
                reason=reason,
                blockers=blockers,
                event=None,
                event_created=False,
            )

        if online_state == "online":
            prior_events = _validated_deactivation_events(
                conn, agent_id=agent_id, host_id=host_id
            )
            generation = len(prior_events) + 1
            previous_deactivation_event_id = (
                prior_events[-1]["id"] if prior_events else None
            )

            deactivated_at = utc_now()
            cursor = conn.execute(
                "UPDATE agents SET online_state = ?, updated_at = ? "
                "WHERE id = ? AND host_id = ? AND online_state = 'online'",
                ("offline", deactivated_at, agent_id, host_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("agent deactivate compare-and-swap failed")

            event = append_event(
                conn,
                event_type="agent.deactivated",
                actor=actor,
                target=agent_id,
                idempotency_key=f"runtime:agent:{agent_id}:deactivated:{generation}",
                payload={
                    "agent_id": agent_id,
                    "host_id": host_id,
                    "actor": actor,
                    "reason": reason,
                    "previous_online_state": online_state,
                    "deactivated_at": deactivated_at,
                    "generation": generation,
                    "previous_deactivation_event_id": previous_deactivation_event_id,
                },
                commit=False,
            )

            if not event.created:
                raise RuntimeError(
                    f"unexpected existing deactivation event for generation {generation}"
                )

            updated_agent = _agent(conn, agent_id)
            conn.commit()
            return RuntimeAgentDeactivateResult(
                agent=updated_agent,
                changed=True,
                deactivated=True,
                dry_run=False,
                blocked=False,
                reason=reason,
                blockers=blockers,
                event=row_to_dict(event.row),
                event_created=True,
            )

        prior_events = _validated_deactivation_events(
            conn, agent_id=agent_id, host_id=host_id
        )
        if not prior_events:
            raise RuntimeError(
                "offline_without_deactivation_audit: "
                f"agent {agent_id!r} is offline without a valid deactivation audit event"
            )
        latest_event = prior_events[-1]
        payload = latest_event["payload"]

        conn.commit()
        return RuntimeAgentDeactivateResult(
            agent=agent,
            changed=False,
            deactivated=True,
            dry_run=False,
            blocked=False,
            reason=payload["reason"],
            blockers=blockers,
            event=latest_event,
            event_created=False,
        )

    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise


def _agent_host_id(conn: sqlite3.Connection, agent_id: str) -> str | None:
    row = conn.execute("SELECT host_id FROM agents WHERE id = ?", (agent_id,)).fetchone()
    return row["host_id"] if row else None


def _task_mirror(
    conn: sqlite3.Connection, workspace_id: str, task_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    return row_to_dict(row) if row else None


def _resolve_submit_context(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    target_agent: str,
    task_id: str | None,
    origin: dict[str, Any],
    job_id: str,
    job_worktree_path: str | None = None,
) -> ExecutionContextV1:
    workspace = get_workspace(conn, workspace_id)
    host_id = _agent_host_id(conn, target_agent)
    if not host_id:
        raise RuntimeError(f"agent {target_agent} has no host_id")
    profile = get_workspace_host_profile(
        conn, workspace_id=workspace_id, host_id=host_id
    )
    if profile is None:
        raise RuntimeError(
            f"workspace {workspace_id} has no host profile for host {host_id}"
        )
    if task_id is not None:
        task = _task_mirror(conn, workspace_id, task_id)
        if task is None:
            raise RuntimeError(f"task mirror not found: {workspace_id}/{task_id}")
    else:
        task = None
    try:
        return resolve_execution_context_v1(
            job_id=job_id,
            workspace=workspace,
            task=task,
            assigned_agent=target_agent,
            host_id=host_id,
            profile=profile,
            origin=origin,
            job_worktree_path=job_worktree_path,
        )
    except ContextError as exc:
        raise RuntimeError(f"invalid execution context: {exc}") from exc


def submit_request(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    target_agent: str | None = None,
    prompt: str,
    origin: dict[str, Any],
    reply: dict[str, Any],
    actor: str = "bridge",
    task_id: str | None = None,
    idempotency_key: str | None = None,
    routing_request: dict[str, Any] | RoutingRequest | None = None,
    worktree_path: str | None = None,
) -> RuntimeRequestResult:
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise RuntimeError(f"unknown workspace: {workspace_id}")
    if not prompt.strip():
        raise RuntimeError("prompt is required")
    _validate_destination(origin, label="origin")
    _validate_destination(reply, label="reply")

    if target_agent is not None and routing_request is not None:
        raise RuntimeError(
            "exact-plus-routed: target_agent and routing_request are mutually exclusive"
        )
    if target_agent is None and routing_request is None:
        raise RuntimeError("neither-mode: target_agent or routing_request is required")
    if target_agent is None and worktree_path is not None:
        raise RuntimeError("routed request does not support worktree_path")

    if target_agent is not None:
        return _submit_exact_request(
            conn,
            workspace_id=workspace_id,
            target_agent=target_agent,
            prompt=prompt,
            origin=origin,
            reply=reply,
            actor=actor,
            task_id=task_id,
            idempotency_key=idempotency_key,
            worktree_path=worktree_path,
        )
    return _submit_routed_request(
        conn,
        workspace_id=workspace_id,
        routing_request=routing_request,
        prompt=prompt,
        origin=origin,
        reply=reply,
        actor=actor,
        task_id=task_id,
        idempotency_key=idempotency_key,
    )


def _replay_exact_request(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    target_agent: str,
    prompt: str,
    origin: dict[str, Any],
    reply: dict[str, Any],
    task_id: str | None,
    worktree_path: str | None,
    existing_event: sqlite3.Row,
) -> RuntimeRequestResult:
    """Replay a stored exact request event and job without mutation."""
    if existing_event["event_type"] != "request.received":
        raise RuntimeError(
            "request replay: idempotency key conflicts with existing non-request event"
        )
    if existing_event["workspace_id"] != workspace_id:
        raise RuntimeError("request replay: stored event workspace_id conflicts")
    if existing_event["target"] != target_agent:
        raise RuntimeError("request replay: stored event target conflicts")

    event_payload = _job_payload(existing_event)
    if event_payload.get("target_agent") != target_agent:
        raise RuntimeError("request replay: stored event payload target_agent conflicts")
    if event_payload.get("prompt") != prompt:
        raise RuntimeError("request replay: prompt conflicts with stored event")
    if event_payload.get("worktree_path") != worktree_path:
        raise RuntimeError("request replay: worktree_path conflicts with stored event")
    if event_payload.get("routing_request") is not None or event_payload.get("routing_decision") is not None:
        raise RuntimeError("explicit idempotency key conflicts with routed request")

    job_id = f"request:{existing_event['id']}"
    existing = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if existing is None:
        raise RuntimeError("request replay: stored event exists but job is missing")
    if existing["workspace_id"] != workspace_id:
        raise RuntimeError("request replay: stored job workspace_id conflicts")
    if existing["assigned_agent"] != target_agent:
        raise RuntimeError("request replay: stored job assignment conflicts")
    if existing["worktree_path"] != worktree_path:
        raise RuntimeError("request replay: stored job worktree_path conflicts")

    job_payload = _job_payload(existing)
    if job_payload.get("request_event_id") != existing_event["id"]:
        raise RuntimeError("request replay: stored job request_event_id conflicts with event")
    if job_payload.get("prompt") != prompt:
        raise RuntimeError("request replay: prompt conflicts with stored job")
    if job_payload.get("reply") != reply:
        raise RuntimeError("request replay: reply conflicts with stored job")

    stored_ctx = job_payload.get("execution_context")
    if stored_ctx is not None:
        try:
            expected_ctx = _resolve_submit_context(
                conn,
                workspace_id=workspace_id,
                target_agent=target_agent,
                task_id=task_id,
                origin=origin,
                job_id=job_id,
                job_worktree_path=worktree_path,
            )
        except ContextError as exc:
            raise RuntimeError(f"request replay context conflict: {exc}") from exc
        if not execution_context_dict_matches(stored_ctx, expected_ctx.to_dict()):
            raise RuntimeError(
                "request replay: execution_context conflicts with stored snapshot"
            )
        if job_payload.get("origin") != origin:
            raise RuntimeError("request replay: origin conflicts with stored job")
        if event_payload.get("origin") != origin:
            raise RuntimeError("request replay: origin conflicts with stored event")
        if event_payload.get("reply") != reply:
            raise RuntimeError("request replay: reply conflicts with stored event")
        if event_payload.get("task_id") != task_id:
            raise RuntimeError("request replay: task_id conflicts with stored event")
        if existing_event["task_id"] != task_id:
            raise RuntimeError("request replay: event row task_id conflicts with request")
        if existing["task_id"] != task_id:
            raise RuntimeError("request replay: stored job task_id conflicts with request")
        stored_binding = job_payload.get("executor_binding")
        if stored_binding is not None:
            current_binding = resolve_exact_executor_binding(conn, target_agent)
            if current_binding is None:
                raise RuntimeError(
                    "request replay: executor_binding was removed from the catalog"
                )
            if _binding_snapshot_canonical_dict(
                stored_binding
            ) != _binding_snapshot_canonical_dict(current_binding):
                raise RuntimeError(
                    "request replay: executor_binding conflicts with stored snapshot"
                )
    else:
        # Pre-upgrade job without a snapshot: enforce origin/task identity.
        if event_payload.get("origin") != origin:
            raise RuntimeError("request replay: origin conflicts with stored event")
        if event_payload.get("reply") != reply:
            raise RuntimeError("request replay: reply conflicts with stored event")
        if event_payload.get("task_id") != task_id:
            raise RuntimeError("request replay: task_id conflicts with stored event")
        if job_payload.get("origin") != origin:
            raise RuntimeError("request replay: origin conflicts with stored job")
        if existing_event["task_id"] != task_id:
            raise RuntimeError("request replay: event row task_id conflicts with request")
        if existing["task_id"] != task_id:
            raise RuntimeError("request replay: task_id conflicts with stored job")
        if job_payload.get("prompt") != prompt:
            raise RuntimeError("request replay: prompt conflicts with stored job")
        if job_payload.get("reply") != reply:
            raise RuntimeError("request replay: reply conflicts with stored job")

    return RuntimeRequestResult(
        event=row_to_dict(existing_event),
        event_created=False,
        job=row_to_dict(existing),
        job_created=False,
    )


def _submit_exact_request(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    target_agent: str,
    prompt: str,
    origin: dict[str, Any],
    reply: dict[str, Any],
    actor: str,
    task_id: str | None,
    idempotency_key: str | None,
    worktree_path: str | None,
) -> RuntimeRequestResult:
    _require_agent(conn, target_agent)
    event_key = idempotency_key or _request_key(workspace_id, target_agent, origin)
    # Resolve the optional executor binding before any durable write so a typed
    # target failure is a zero-mutation preflight error.
    executor_binding = resolve_exact_executor_binding(conn, target_agent)
    runner_profile_id = (
        executor_binding["runner_profile_id"] if executor_binding else target_agent
    )
    # Resolve and validate all authority inputs before the first durable write.
    # Use a placeholder job_id for preflight validation; the real job_id depends
    # on the request event id, which we only obtain after the validated insert.
    _resolve_submit_context(
        conn,
        workspace_id=workspace_id,
        target_agent=target_agent,
        task_id=task_id,
        origin=origin,
        job_id="request:preflight",
        job_worktree_path=worktree_path,
    )
    if worktree_path is not None:
        worktree_path = _absolute_path(worktree_path)
        # ``create_job`` persists this same control-plane canonical form.  Run
        # the authority check again after resolution so a symlink cannot turn
        # an in-workspace lexical path into an out-of-workspace durable path.
        _resolve_submit_context(
            conn,
            workspace_id=workspace_id,
            target_agent=target_agent,
            task_id=task_id,
            origin=origin,
            job_id="request:preflight",
            job_worktree_path=worktree_path,
        )

    # Replay must detect explicit idempotency-key conflicts before any durable
    # write, including exact/routed mode collisions.
    existing_event = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (event_key,)
    ).fetchone()
    if existing_event is not None:
        return _replay_exact_request(
            conn,
            workspace_id=workspace_id,
            target_agent=target_agent,
            prompt=prompt,
            origin=origin,
            reply=reply,
            task_id=task_id,
            worktree_path=worktree_path,
            existing_event=existing_event,
        )

    try:
        request_payload = {
            "target_agent": target_agent,
            "prompt": prompt,
            "origin": origin,
            "reply": reply,
            "task_id": task_id,
        }
        if worktree_path is not None:
            request_payload["worktree_path"] = worktree_path
        event = append_event(
            conn,
            workspace_id=workspace_id,
            event_type="request.received",
            actor=actor,
            target=target_agent,
            task_id=task_id,
            idempotency_key=event_key,
            payload=request_payload,
            commit=False,
        )
        if not event.created:
            # Lost a concurrent idempotency race: replay the stored event/job.
            conn.rollback()
            return _replay_exact_request(
                conn,
                workspace_id=workspace_id,
                target_agent=target_agent,
                prompt=prompt,
                origin=origin,
                reply=reply,
                task_id=task_id,
                worktree_path=worktree_path,
                existing_event=event.row,
            )
        job_id = f"request:{event.row['id']}"
        existing = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if existing is None:
            ctx = _resolve_submit_context(
                conn,
                workspace_id=workspace_id,
                target_agent=target_agent,
                task_id=task_id,
                origin=origin,
                job_id=job_id,
                job_worktree_path=worktree_path,
            )
            job_payload: dict[str, Any] = {
                "prompt": prompt,
                "origin": origin,
                "reply": reply,
                "request_event_id": event.row["id"],
                "execution_context": ctx.to_dict(),
            }
            if executor_binding is not None:
                job_payload["executor_binding"] = executor_binding
            job = create_job(
                conn,
                workspace_id=workspace_id,
                task_id=task_id,
                runner_profile_id=runner_profile_id,
                assigned_agent=target_agent,
                worktree_path=worktree_path,
                payload=job_payload,
                job_id=job_id,
                commit=False,
            )
            created = True
        else:
            payload = _job_payload(existing)
            stored_ctx = payload.get("execution_context")
            if stored_ctx is not None:
                try:
                    expected_ctx = _resolve_submit_context(
                        conn,
                        workspace_id=workspace_id,
                        target_agent=target_agent,
                        task_id=task_id,
                        origin=origin,
                        job_id=job_id,
                        job_worktree_path=worktree_path,
                    )
                except ContextError as exc:
                    raise RuntimeError(
                        f"request replay context conflict: {exc}"
                    ) from exc
                if not execution_context_dict_matches(stored_ctx, expected_ctx.to_dict()):
                    raise RuntimeError(
                        "request replay: execution_context conflicts with stored snapshot"
                    )
                if payload.get("prompt") != prompt:
                    raise RuntimeError("request replay: prompt conflicts with stored job")
                if payload.get("origin") != origin:
                    raise RuntimeError("request replay: origin conflicts with stored job")
                if payload.get("reply") != reply:
                    raise RuntimeError("request replay: reply conflicts with stored job")
                # Replay must return the original binding snapshot and never
                # silently upgrade to a newer catalog.
                stored_binding = payload.get("executor_binding")
                if stored_binding is not None:
                    if executor_binding is None:
                        raise RuntimeError(
                            "request replay: executor_binding was removed from the catalog"
                        )
                    if _binding_snapshot_canonical_dict(
                        stored_binding
                    ) != _binding_snapshot_canonical_dict(executor_binding):
                        raise RuntimeError(
                            "request replay: executor_binding conflicts with stored snapshot"
                        )
            else:
                # Pre-upgrade job without a snapshot: enforce origin/task identity.
                if payload.get("origin") != origin:
                    raise RuntimeError("request replay: origin conflicts with stored job")
                if existing["task_id"] != task_id:
                    raise RuntimeError("request replay: task_id conflicts with stored job")
                if payload.get("prompt") != prompt:
                    raise RuntimeError("request replay: prompt conflicts with stored job")
                if payload.get("reply") != reply:
                    raise RuntimeError("request replay: reply conflicts with stored job")
            job = existing
            created = False
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return RuntimeRequestResult(
        event=row_to_dict(event.row),
        event_created=event.created,
        job=row_to_dict(job),
        job_created=created,
    )


def _routed_request_key(
    workspace_id: str, routing_request: RoutingRequest, origin: dict[str, Any]
) -> str:
    platform = origin.get("platform")
    destination = origin.get("destination")
    message_id = origin.get("message_id", "")
    return (
        f"runtime:request:{workspace_id}:{platform}:{destination}:{message_id}:"
        f"{routing_request.routing_request_id}"
    )


def _submit_routed_request(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    routing_request: dict[str, Any] | RoutingRequest,
    prompt: str,
    origin: dict[str, Any],
    reply: dict[str, Any],
    actor: str,
    task_id: str | None,
    idempotency_key: str | None,
) -> RuntimeRequestResult:
    if isinstance(routing_request, RoutingRequest):
        routing_request = parse_routing_request(routing_request_to_dict(routing_request))
    elif isinstance(routing_request, dict):
        routing_request = parse_routing_request(routing_request)
    else:
        raise RuntimeError("routing_request must be a dict or RoutingRequest")

    event_key = idempotency_key or _routed_request_key(workspace_id, routing_request, origin)

    # Replay must look up the existing event before reading current candidates
    # or load so the original decision is returned unchanged.
    existing_event = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (event_key,)
    ).fetchone()
    if existing_event is not None:
        return _replay_routed_request(
            conn,
            workspace_id=workspace_id,
            routing_request=routing_request,
            prompt=prompt,
            origin=origin,
            reply=reply,
            task_id=task_id,
            existing_event=existing_event,
        )

    # Resolve all authority before any durable write.
    candidates = resolve_routing_candidates(conn, workspace_id, routing_request)
    try:
        decision = select_routing_decision(routing_request, candidates)
    except ExecutorRoutingError as exc:
        raise RuntimeError(str(exc)) from exc
    selected = next(
        c for c in candidates if c.agent_id == decision.selected_agent_id
    )
    target_agent = selected.agent_id
    executor_binding = selected.binding_snapshot
    runner_profile_id = selected.runner_profile_id

    # Preflight context validation before any durable write.
    _resolve_submit_context(
        conn,
        workspace_id=workspace_id,
        target_agent=target_agent,
        task_id=task_id,
        origin=origin,
        job_id="request:preflight",
    )

    try:
        event = append_event(
            conn,
            workspace_id=workspace_id,
            event_type="request.received",
            actor=actor,
            target=target_agent,
            task_id=task_id,
            idempotency_key=event_key,
            payload={
                "target_agent": target_agent,
                "prompt": prompt,
                "origin": origin,
                "reply": reply,
                "task_id": task_id,
                "routing_request": routing_request_to_dict(routing_request),
                "routing_decision": routing_decision_to_dict(decision),
            },
            commit=False,
        )
        if not event.created:
            # Lost a concurrent idempotency race: replay the stored event/job
            # without consulting current candidate/load state.
            conn.rollback()
            return _replay_routed_request(
                conn,
                workspace_id=workspace_id,
                routing_request=routing_request,
                prompt=prompt,
                origin=origin,
                reply=reply,
                task_id=task_id,
                existing_event=event.row,
            )
        job_id = f"request:{event.row['id']}"
        existing = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if existing is None:
            ctx = _resolve_submit_context(
                conn,
                workspace_id=workspace_id,
                target_agent=target_agent,
                task_id=task_id,
                origin=origin,
                job_id=job_id,
            )
            decision_dict = routing_decision_to_dict(decision)
            job = create_job(
                conn,
                workspace_id=workspace_id,
                task_id=task_id,
                runner_profile_id=runner_profile_id,
                assigned_agent=target_agent,
                payload={
                    "prompt": prompt,
                    "origin": origin,
                    "reply": reply,
                    "request_event_id": event.row["id"],
                    "execution_context": ctx.to_dict(),
                    "executor_binding": executor_binding,
                    "routing_request": routing_request_to_dict(routing_request),
                    "routing_decision": decision_dict,
                },
                job_id=job_id,
                commit=False,
            )
            created = True
        else:
            # The event was not present above, so this branch should not occur
            # under normal operation. Treat it as a conflict and fail closed.
            payload = _job_payload(existing)
            expected_request = routing_request_to_dict(routing_request)
            expected_decision = routing_decision_to_dict(decision)
            if payload.get("routing_request") != expected_request:
                raise RuntimeError(
                    "request replay: routing_request conflicts with stored job"
                )
            if payload.get("routing_decision") != expected_decision:
                raise RuntimeError(
                    "request replay: routing_decision conflicts with stored job"
                )
            job = existing
            created = False
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return RuntimeRequestResult(
        event=row_to_dict(event.row),
        event_created=event.created,
        job=row_to_dict(job),
        job_created=created,
    )


def _replay_routed_request(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    routing_request: RoutingRequest,
    prompt: str,
    origin: dict[str, Any],
    reply: dict[str, Any],
    task_id: str | None,
    existing_event: sqlite3.Row,
) -> RuntimeRequestResult:
    if existing_event["event_type"] != "request.received":
        raise RuntimeError(
            "request replay: idempotency key conflicts with existing non-request event"
        )
    if existing_event["workspace_id"] != workspace_id:
        raise RuntimeError("request replay: stored event workspace_id conflicts")
    if existing_event["task_id"] != task_id:
        raise RuntimeError("request replay: event row task_id conflicts with request")

    event_payload = _job_payload(existing_event)
    if event_payload.get("prompt") != prompt:
        raise RuntimeError("request replay: prompt conflicts with stored event")
    if event_payload.get("origin") != origin:
        raise RuntimeError("request replay: origin conflicts with stored event")
    if event_payload.get("reply") != reply:
        raise RuntimeError("request replay: reply conflicts with stored event")
    if event_payload.get("task_id") != task_id:
        raise RuntimeError("request replay: payload task_id conflicts with stored event")

    stored_request = event_payload.get("routing_request")
    expected_request = routing_request_to_dict(routing_request)
    if stored_request != expected_request:
        raise RuntimeError("request replay: routing_request conflicts with stored event")

    stored_decision = event_payload.get("routing_decision")
    if not isinstance(stored_decision, dict):
        raise RuntimeError("request replay: routing_decision missing in stored event")
    try:
        validate_routing_decision(stored_decision, routing_request=routing_request)
    except ExecutorRoutingError as exc:
        raise RuntimeError(
            f"request replay: invalid stored routing_decision: {exc}"
        ) from exc

    if event_payload.get("target_agent") != stored_decision["selected_agent_id"]:
        raise RuntimeError(
            "request replay: stored event payload target_agent conflicts with stored decision"
        )
    if existing_event["target"] != stored_decision["selected_agent_id"]:
        raise RuntimeError(
            "request replay: stored event target conflicts with stored decision"
        )

    job_id = f"request:{existing_event['id']}"
    existing = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if existing is None:
        raise RuntimeError("request replay: stored event exists but job is missing")
    if existing["workspace_id"] != workspace_id:
        raise RuntimeError("request replay: stored job workspace_id conflicts with event")
    if existing["task_id"] != task_id:
        raise RuntimeError("request replay: stored job task_id conflicts with request")

    job_payload = _job_payload(existing)
    if job_payload.get("prompt") != prompt:
        raise RuntimeError("request replay: prompt conflicts with stored job")
    if job_payload.get("origin") != origin:
        raise RuntimeError("request replay: origin conflicts with stored job")
    if job_payload.get("reply") != reply:
        raise RuntimeError("request replay: reply conflicts with stored job")
    if job_payload.get("request_event_id") != existing_event["id"]:
        raise RuntimeError(
            "request replay: stored job request_event_id conflicts with event"
        )
    if job_payload.get("routing_request") != stored_request:
        raise RuntimeError("request replay: routing_request conflicts with stored job")
    if job_payload.get("routing_decision") != stored_decision:
        raise RuntimeError("request replay: routing_decision conflicts with stored job")

    stored_binding = job_payload.get("executor_binding")
    stored_ctx = job_payload.get("execution_context")
    if stored_binding is None or stored_ctx is None:
        raise RuntimeError(
            "request replay: routed job requires executor_binding and execution_context snapshots"
        )

    try:
        _validate_routing_cross_links(
            routing_request,
            stored_decision,
            job=dict(existing),
            binding_snapshot=stored_binding,
            execution_context=stored_ctx,
        )
    except ExecutorRoutingError as exc:
        raise RuntimeError(f"request replay: invalid routing cross-links: {exc}") from exc

    current_binding = resolve_exact_executor_binding(
        conn, existing["assigned_agent"]
    )
    if current_binding is None:
        raise RuntimeError(
            "request replay: executor_binding was removed from the catalog"
        )
    if _binding_snapshot_canonical_dict(
        stored_binding
    ) != _binding_snapshot_canonical_dict(current_binding):
        raise RuntimeError(
            "request replay: executor_binding conflicts with stored snapshot"
        )

    try:
        expected_ctx = _resolve_submit_context(
            conn,
            workspace_id=workspace_id,
            target_agent=existing["assigned_agent"],
            task_id=task_id,
            origin=origin,
            job_id=job_id,
        )
    except ContextError as exc:
        raise RuntimeError(f"request replay context conflict: {exc}") from exc
    if not execution_context_dict_matches(stored_ctx, expected_ctx.to_dict()):
        raise RuntimeError(
            "request replay: execution_context conflicts with stored snapshot"
        )

    return RuntimeRequestResult(
        event=row_to_dict(existing_event),
        event_created=False,
        job=row_to_dict(existing),
        job_created=False,
    )


def claim_job(
    conn: sqlite3.Connection,
    *,
    agent_id: str,
    recoverable: bool = False,
    ttl_seconds: int = 120,
    recovery_reason: str | None = None,
    prior_process_stopped: bool | None = None,
    reap_mode: str = "global",
    reap_reason: str | None = None,
) -> RuntimeClaimResult:
    try:
        _validate_claim_reap_policy(reap_mode=reap_mode, reap_reason=reap_reason)
        agent_id = _validate_id(agent_id, "agent_id")
    except RuntimeLeaseError as exc:
        raise RuntimeError(str(exc)) from exc
    _require_online_agent(conn, agent_id)
    host_id = _agent_host_id(conn, agent_id)
    if not host_id:
        raise RuntimeError(f"agent {agent_id} has no host_id")

    is_typed_agent = resolve_exact_executor_binding(conn, agent_id) is not None

    if is_typed_agent:
        conn.execute("BEGIN IMMEDIATE")
        try:
            claim_result = claim_leased_job(
                conn,
                agent_id=agent_id,
                host_id=host_id,
                recoverable=recoverable,
                ttl_seconds=ttl_seconds,
                actor=agent_id,
                recovery_reason=recovery_reason,
                prior_process_stopped=prior_process_stopped,
                reap_mode=reap_mode,
                reap_reason=reap_reason,
            )
            conn.commit()
        except RuntimeLeaseError as exc:
            if conn.in_transaction:
                conn.rollback()
            reason = str(exc)
            if reason in {
                "queue_empty",
                "capacity_exhausted",
                "resource_blocked",
                "scan_limit_reached",
            }:
                return RuntimeClaimResult(
                    job=None,
                    claimed=False,
                    reason=reason,
                    oldest_blocked_job_id=exc.oldest_blocked_job_id,
                    oldest_blocked_resource_key=exc.oldest_blocked_resource_key,
                )
            if reason.startswith("resource_blocked"):
                return RuntimeClaimResult(
                    job=None,
                    claimed=False,
                    reason="resource_blocked",
                    oldest_blocked_job_id=exc.oldest_blocked_job_id,
                    oldest_blocked_resource_key=exc.oldest_blocked_resource_key,
                )
            raise RuntimeError(reason) from exc
        except LeasePrimitiveError as exc:
            if conn.in_transaction:
                conn.rollback()
            raise RuntimeError(str(exc)) from exc

        claimed_job = row_to_dict(claim_result.job)
        return RuntimeClaimResult(
            job=claimed_job,
            claimed=True,
            attempt_token=claim_result.attempt_token,
            execution_context=claim_result.execution_context.to_dict(),
            execution_lease=claim_result.execution_lease,
        )

    # Legacy untyped claim: none mode requires typed agent.
    if reap_mode == "none":
        raise RuntimeError(
            f"reap_mode=none requires a typed agent with exact executor binding; "
            f"agent {agent_id!r} is untyped"
        )

    statuses = ("pending", "timed_out") if recoverable else ("pending",)
    placeholders = ",".join("?" for _ in statuses)
    candidate = conn.execute(
        f"""
        SELECT * FROM jobs
        WHERE status IN ({placeholders}) AND assigned_agent = ?
          AND (status = 'pending' OR recoverable = 1)
        ORDER BY created_at, id
        LIMIT 1
        """,
        (*statuses, agent_id),
    ).fetchone()
    if candidate is None:
        return RuntimeClaimResult(job=None, claimed=False)

    workspace = get_workspace(conn, candidate["workspace_id"])
    profile = get_workspace_host_profile(
        conn, workspace_id=candidate["workspace_id"], host_id=host_id
    )
    if profile is None:
        raise RuntimeError(
            f"workspace {candidate['workspace_id']} has no host profile for host {host_id}"
        )

    payload = _job_payload(candidate)
    snapshot = payload.get("execution_context")
    binding_snapshot = payload.get("executor_binding")

    task = _task_mirror(conn, candidate["workspace_id"], candidate["task_id"]) if candidate["task_id"] else None
    if snapshot is None:
        # Backfill pre-upgrade pending jobs once at first claim.
        if candidate["task_id"] and task is None:
            raise RuntimeError(
                f"task mirror not found: {candidate['workspace_id']}/{candidate['task_id']}"
            )
        origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}
        try:
            ctx = resolve_execution_context_v1(
                job_id=candidate["id"],
                workspace=workspace,
                task=task,
                assigned_agent=agent_id,
                host_id=host_id,
                profile=profile,
                origin=origin,
                job_branch=candidate["branch"],
                job_worktree_path=candidate["worktree_path"],
                job_logs_path=candidate["logs_path"],
            )
        except ContextError as exc:
            raise RuntimeError(f"invalid execution context: {exc}") from exc
    else:
        try:
            ctx = validate_execution_context_snapshot(
                snapshot,
                job_id=candidate["id"],
                workspace_id=candidate["workspace_id"],
                task_id=candidate["task_id"],
                assigned_agent=agent_id,
                host_id=host_id,
            )
        except ContextError as exc:
            raise RuntimeError(f"invalid stored execution context: {exc}") from exc

    now = utc_now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        previous_status = candidate["status"]
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = ?, attempt_count = ?, started_at = ?, last_activity_at = ?,
                recoverable = 0, updated_at = ?, payload_json = ?
            WHERE id = ? AND status = ?
            """,
            (
                "running",
                int(candidate["attempt_count"]) + 1,
                now,
                now,
                now,
                _json({**payload, "execution_context": ctx.to_dict()}),
                candidate["id"],
                previous_status,
            ),
        )
        if cursor.rowcount == 0:
            conn.rollback()
            return RuntimeClaimResult(job=None, claimed=False)

        route_evidence = routing_claim_evidence(payload, job=dict(candidate))

        append_event(
            conn,
            workspace_id=candidate["workspace_id"],
            event_type="job.claimed",
            actor=agent_id,
            target=agent_id,
            task_id=candidate["task_id"],
            idempotency_key=f"runtime:job:{candidate['id']}:claimed:{int(candidate['attempt_count']) + 1}",
            payload={
                "job_id": candidate["id"],
                "agent_id": agent_id,
                "previous_status": previous_status,
                "recovered": previous_status == "timed_out",
                "execution_context_id": ctx.context_id,
                "context_version": CONTRACT_VERSION,
                "host_id": ctx.host_id,
                "worktree_path": ctx.worktree_path,
                "branch": ctx.branch,
                "session_scope_id": ctx.session_scope_id,
                **(
                    executor_binding_claim_evidence(binding_snapshot)
                    if binding_snapshot is not None
                    else {}
                ),
                **route_evidence,
            },
            commit=False,
        )
        conn.commit()
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise

    claimed_job = row_to_dict(get_job(conn, candidate["id"]))
    return RuntimeClaimResult(
        job=claimed_job,
        claimed=True,
        attempt_token=claimed_job["attempt_count"],
        execution_context=ctx.to_dict(),
        execution_lease=None,
    )


def report_job_result(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    agent_id: str,
    status: str,
    result: dict[str, Any],
    actor: str | None = None,
    attempt_token: int | None = None,
    lease_id: str | None = None,
) -> RuntimeReportResult:
    _validate_report_status(status)
    job = get_job(conn, job_id)
    if job["assigned_agent"] != agent_id:
        raise RuntimeError(f"job {job_id} is assigned to {job['assigned_agent']}")

    # Terminal already (done/failed): the result is immutable — replay only.
    if job["status"] in {"done", "failed"}:
        return _replay_terminal_result(
            conn, job=job, job_id=job_id, agent_id=agent_id, status=status,
            result=result, actor=actor,
        )
    # 8.4.3 recovery: a late done/failed for a recoverable timed_out job.
    if job["status"] == "timed_out" and status in {"done", "failed"}:
        return _accept_late_result(
            conn, job=job, agent_id=agent_id, status=status, result=result,
            actor=actor, attempt_token=attempt_token, lease_id=lease_id,
        )
    if job["status"] != "running":
        raise RuntimeError(f"job {job_id} is {job['status']}; only running jobs can report result")

    if attempt_token is not None and job["attempt_count"] != attempt_token:
        raise RuntimeError(
            f"job {job_id} report rejected: CAS failed — attempt_token {attempt_token} "
            f"does not match current attempt {job['attempt_count']}"
        )

    # --- running → terminal pipeline ---
    now = utc_now()
    result = _normalize_result(status=status, result=result, job=job, now=now)

    conn.execute("BEGIN IMMEDIATE")
    try:
        current = get_job(conn, job_id)
        if current is None:
            raise RuntimeError(f"job {job_id} disappeared")
        if current["status"] != "running":
            raise RuntimeError(f"job {job_id} is {current['status']}; only running jobs can report result")

        # B5A-core: shared managed-or-legacy mutation authority. Always checks the
        # authoritative current attempt; rejects managed attempts missing token/lease.
        require_mutation_authority(
            conn,
            current_attempt_token=current["attempt_count"],
            supplied_attempt_token=attempt_token,
            job_id=job_id,
            agent_id=agent_id,
            lease_id=lease_id,
        )

        # Use the current attempt token for the CAS if the caller supplied one.
        cas_attempt_token = attempt_token if attempt_token is not None else None
        _apply_terminal_job_update(
            conn, job=job, job_id=job_id, status=status, result=result,
            now=now, attempt_token=cas_attempt_token, commit=False,
        )
        # P9-3B: release the exact lease as part of the terminal transaction.
        if lease_id is not None and attempt_token is not None:
            release_lease_for_terminal_report(
                conn,
                lease_id=lease_id,
                job_id=job_id,
                attempt_token=attempt_token,
                agent_id=agent_id,
                status=status,
            )
        terminal_outcome = append_terminal_events_and_delivery(
            conn,
            job=job,
            job_id=job_id,
            agent_id=agent_id,
            status=status,
            result=result,
            actor=actor,
        )
        conn.commit()
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise

    return RuntimeReportResult(
        job=row_to_dict(get_job(conn, job_id)),
        event=terminal_outcome["event"],
        event_created=terminal_outcome["event_created"],
        delivery=terminal_outcome["delivery"],
        delivery_created=terminal_outcome["delivery_created"],
    )


def _validate_report_status(status: str) -> None:
    if status not in {"done", "failed", "timed_out"}:
        raise RuntimeError("status must be done, failed, or timed_out")


def _terminal_event_type(status: str) -> str:
    if status == "timed_out":
        return "job.timed_out"
    if status == "done":
        return "job.completed"
    return "job.failed"


def _replay_terminal_result(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    job_id: str,
    agent_id: str,
    status: str,
    result: dict[str, Any],
    actor: str | None,
) -> RuntimeReportResult:
    event = append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type="job.result_replayed",
        actor=actor or agent_id,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:result-replayed:{status}:{_stable_result_key(result)}",
        payload={
            "job_id": job_id,
            "agent_id": agent_id,
            "status": job["status"],
            "submitted_status": status,
            "submitted_result": result,
            "applied": False,
            "reason": "terminal_result_immutable",
        },
    )
    return RuntimeReportResult(
        job=row_to_dict(job),
        event=row_to_dict(event.row),
        event_created=event.created,
        delivery=None,
        delivery_created=False,
    )


def _apply_terminal_job_update(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    job_id: str,
    status: str,
    result: dict[str, Any],
    now: str,
    attempt_token: int | None,
    commit: bool = True,
) -> None:
    # 8.4.3 P1 #2: atomic SQL CAS. When attempt_token is supplied, the UPDATE only
    # lands if the job is still running at that attempt_count; rowcount==0 means a
    # concurrent reclaim beat us → reject WITHOUT appending event or delivery.
    recoverable_flag = 1 if status == "timed_out" and result.get("recoverable", True) else 0
    if attempt_token is not None:
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = ?, result_json = ?, completed_at = ?, last_activity_at = ?,
                recoverable = ?, updated_at = ?
            WHERE id = ? AND status = 'running' AND attempt_count = ?
            """,
            (status, _json(result), now, now, recoverable_flag, now, job_id, attempt_token),
        )
        if cursor.rowcount == 0:
            if commit:
                conn.rollback()
            current = get_job(conn, job_id)
            raise RuntimeError(
                f"job {job_id} report rejected: CAS failed — not running as attempt "
                f"{attempt_token} (status={current['status']} attempt_count={current['attempt_count']}; "
                "stale attempt or reclaimed)"
            )
    else:
        conn.execute(
            """
            UPDATE jobs
            SET status = ?, result_json = ?, completed_at = ?, last_activity_at = ?,
                recoverable = ?, updated_at = ?
            WHERE id = ? AND status = 'running'
            """,
            (status, _json(result), now, now, recoverable_flag, now, job_id),
        )
    if commit:
        conn.commit()


def _append_job_terminal_event(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    job_id: str,
    agent_id: str,
    event_type: str,
    status: str,
    result: dict[str, Any],
    actor: str | None,
):
    return append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type=event_type,
        actor=actor or agent_id,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:result:{status}",
        payload={"job_id": job_id, "agent_id": agent_id, "status": status, **result},
    )


@dataclass(frozen=True)
class _ReportOutcome:
    """Parsed ``[agent-report]`` outcome for a terminal job result.

    ``decision`` is normalized to approve/reject or ``None``. ``result_summary``
    is the truncated summary mirrored onto the ``agent.reported`` payload; the
    review event falls back to it when the report carries no ``summary=``.
    """

    parsed: AgentReport | None
    decision: str | None
    result_summary: str | None


def _parse_job_agent_report(
    *,
    result: dict[str, Any],
    job: sqlite3.Row,
) -> _ReportOutcome:
    # Phase 8.8: parse [agent-report] decision= from response_text so reviewer
    # decisions surface as review.completed/rejected (visible) instead of being
    # buried in job result. Unifies the runtime path with the daemon Discord
    # path. fallback_workspace_id/task_id (from job context) keeps a reviewer's
    # decision=approve/reject that omits them from being silently dropped.
    response_text = result.get("response_text") or result.get("text") or ""
    parsed = (
        parse_agent_report(
            response_text,
            fallback_workspace_id=job["workspace_id"],
            fallback_task_id=job["task_id"],
        )
        if isinstance(response_text, str) and response_text
        else None
    )
    decision = parsed.decision if parsed and parsed.decision in {"approve", "reject"} else None
    result_summary_raw = result.get("summary") or result.get("response_text") or result.get("text")
    if isinstance(result_summary_raw, str) and result_summary_raw.strip():
        result_summary = " ".join(result_summary_raw.split())[:500]
    else:
        result_summary = None
    return _ReportOutcome(parsed=parsed, decision=decision, result_summary=result_summary)


def _append_agent_reported_event(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    job_id: str,
    agent_id: str,
    status: str,
    outcome: _ReportOutcome,
    actor: str | None,
) -> None:
    # Phase 8.6: runtime job terminal → agent.reported event.
    payload: dict[str, Any] = {
        "source": "runtime",
        "job_id": job_id,
        "agent_id": agent_id,
        "status": status,
        "action": "done" if status == "done" else "blocker",
    }
    if outcome.decision:
        payload["decision"] = outcome.decision
    if outcome.result_summary:
        payload["result_summary"] = outcome.result_summary
    append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type="agent.reported",
        actor=actor or agent_id,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:agent-reported:{status}:{outcome.decision or 'nodecision'}",
        payload=payload,
    )


def _append_review_decision_event_if_needed(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    job_id: str,
    agent_id: str,
    outcome: _ReportOutcome,
    actor: str | None,
) -> None:
    # Phase 8.8: reviewer decision → review.completed/rejected (auto, visible,
    # operator receives via delivery). Loop-guard: review.completed here does
    # NOT trigger another reviewer handoff (only daemon pump creates handoffs).
    if outcome.decision not in {"approve", "reject"}:
        return
    review_event_type = "review.completed" if outcome.decision == "approve" else "review.rejected"
    parsed = outcome.parsed
    append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type=review_event_type,
        actor=actor or agent_id,
        target=None,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:review-decision:{outcome.decision}",
        payload={
            "reviewer": agent_id,
            "decision": outcome.decision,
            "reason": parsed.reason if parsed else None,
            "summary": (parsed.summary if parsed else None) or outcome.result_summary,
            "source": "runtime",
            "job_id": job_id,
        },
    )


def record_job_progress(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    agent_id: str,
    stage: str | None = None,
    summary: str | None = None,
    session_id: str | None = None,
    actor: str | None = None,
    attempt_token: int | None = None,
    lease_id: str | None = None,
) -> RuntimeProgressResult:
    job = get_job(conn, job_id)
    if job["assigned_agent"] != agent_id:
        raise RuntimeError(f"job {job_id} is assigned to {job['assigned_agent']}")
    if job["status"] != "running":
        raise RuntimeError(f"job {job_id} is {job['status']}; only running jobs can record progress")

    progress = _bounded_progress(stage=stage, summary=summary, session_id=session_id)
    now = utc_now()

    conn.execute("BEGIN IMMEDIATE")
    try:
        current = get_job(conn, job_id)
        if current is None:
            raise RuntimeError(f"job {job_id} disappeared")
        if current["assigned_agent"] != agent_id:
            raise RuntimeError(f"job {job_id} is assigned to {current['assigned_agent']}")
        if current["status"] != "running":
            raise RuntimeError(f"job {job_id} is {current['status']}; only running jobs can record progress")

        # B5A-core: shared managed-or-legacy mutation authority. Always checks the
        # authoritative current attempt; rejects managed attempts missing token/lease.
        require_mutation_authority(
            conn,
            current_attempt_token=current["attempt_count"],
            supplied_attempt_token=attempt_token,
            job_id=job_id,
            agent_id=agent_id,
            lease_id=lease_id,
        )

        # Use the current attempt token for the CAS if the caller supplied one.
        cas_attempt_token = attempt_token if attempt_token is not None else None
        if cas_attempt_token is not None:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET last_activity_at = ?, progress_json = ?, terminal_session_id = COALESCE(?, terminal_session_id),
                    updated_at = ?
                WHERE id = ? AND status = 'running' AND attempt_count = ?
                """,
                (now, _json(progress), session_id or None, now, job_id, cas_attempt_token),
            )
            if cursor.rowcount == 0:
                conn.rollback()
                current = get_job(conn, job_id)
                raise RuntimeError(
                    f"job {job_id} progress rejected: CAS failed — not running as attempt "
                    f"{cas_attempt_token} (status={current['status']} attempt_count={current['attempt_count']}; "
                    "stale attempt or reclaimed)"
                )
        else:
            conn.execute(
                """
                UPDATE jobs
                SET last_activity_at = ?, progress_json = ?, terminal_session_id = COALESCE(?, terminal_session_id),
                    updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (now, _json(progress), session_id or None, now, job_id, "running"),
            )

        event = append_event(
            conn,
            workspace_id=job["workspace_id"],
            event_type="job.progress",
            actor=actor or agent_id,
            target=agent_id,
            task_id=job["task_id"],
            idempotency_key=f"runtime:job:{job_id}:progress:{now}",
            payload={"job_id": job_id, "agent_id": agent_id, "last_activity_at": now, **progress},
            commit=False,
        )
        conn.commit()
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise

    return RuntimeProgressResult(
        job=row_to_dict(get_job(conn, job_id)),
        event=row_to_dict(event.row),
        event_created=event.created,
    )


def _accept_late_result(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    agent_id: str,
    status: str,
    result: dict[str, Any],
    actor: str | None,
    attempt_token: int | None = None,
    lease_id: str | None = None,
) -> RuntimeReportResult:
    now = utc_now()
    normalized = _normalize_result(status=status, result=result, job=job, now=now)

    conn.execute("BEGIN IMMEDIATE")
    try:
        # B5A-core: re-read the timed_out+recoverable row under the write lock.
        current = get_job(conn, job["id"])
        if current is None:
            raise RuntimeError(f"job {job['id']} disappeared")
        if current["status"] != "timed_out" or not current["recoverable"]:
            raise RuntimeError(
                f"job {job['id']} is not timed_out+recoverable (status={current['status']} "
                f"recoverable={current['recoverable']})"
            )

        # B5A-core: late acceptance is only allowed for legacy unleased attempts.
        # If the current attempt has any lease row, fail closed regardless of token
        # or lease identity.
        if attempt_has_any_lease(conn, job["id"], current["attempt_count"]):
            raise RuntimeLeaseError(
                f"job {job['id']} late-result rejected: current attempt is managed"
            )

        # 8.4.3 P1 #2: SQL CAS. When attempt_token is supplied, gate on attempt_count
        # too — a late result from attempt N must not overwrite a job reclaimed as N+1.
        if attempt_token is not None:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, result_json = ?, completed_at = ?, last_activity_at = ?,
                    recoverable = 0, updated_at = ?
                WHERE id = ? AND status = 'timed_out' AND recoverable = 1 AND attempt_count = ?
                """,
                (status, _json(normalized), now, now, now, job["id"], attempt_token),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, result_json = ?, completed_at = ?, last_activity_at = ?,
                    recoverable = 0, updated_at = ?
                WHERE id = ? AND status = 'timed_out' AND recoverable = 1
                """,
                (status, _json(normalized), now, now, now, job["id"]),
            )
        if cursor.rowcount == 0:
            # 8.4.3 P1 #2: token supplied ⇒ stale/reclaimed late result → reject loud,
            # no event, no delivery. token None ⇒ backward-compat result_replayed.
            conn.rollback()
            current = get_job(conn, job["id"])
            if attempt_token is not None:
                raise RuntimeError(
                    f"job {job['id']} late-result rejected: CAS failed — not timed_out+recoverable "
                    f"as attempt {attempt_token} (status={current['status']} "
                    f"attempt_count={current['attempt_count']}; stale attempt or reclaimed)"
                )
            event = append_event(
                conn,
                workspace_id=current["workspace_id"],
                event_type="job.result_replayed",
                actor=actor or agent_id,
                target=agent_id,
                task_id=current["task_id"],
                idempotency_key=f"runtime:job:{job['id']}:result-replayed:{status}:{_stable_result_key(result)}",
                payload={
                    "job_id": job["id"],
                    "agent_id": agent_id,
                    "status": current["status"],
                    "submitted_status": status,
                    "submitted_result": result,
                    "applied": False,
                    "reason": "recoverable_timeout_already_claimed",
                },
            )
            return RuntimeReportResult(
                job=row_to_dict(current),
                event=row_to_dict(event.row),
                event_created=event.created,
                delivery=None,
                delivery_created=False,
            )

        event = append_event(
            conn,
            workspace_id=job["workspace_id"],
            event_type="job.late_result_accepted",
            actor=actor or agent_id,
            target=agent_id,
            task_id=job["task_id"],
            idempotency_key=f"runtime:job:{job['id']}:late-result:{status}",
            payload={
                "job_id": job["id"],
                "agent_id": agent_id,
                "status": status,
                "previous_status": "timed_out",
                **normalized,
            },
            commit=False,
        )
        delivery, delivery_created = _create_response_delivery(
            conn,
            job=get_job(conn, job["id"]),
            event_id=event.row["id"],
            result=normalized,
        )
        conn.commit()
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise

    return RuntimeReportResult(
        job=row_to_dict(get_job(conn, job["id"])),
        event=row_to_dict(event.row),
        event_created=event.created,
        delivery=row_to_dict(delivery) if delivery is not None else None,
        delivery_created=delivery_created,
    )


def _create_response_delivery(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    event_id: str,
    result: dict[str, Any],
) -> tuple[sqlite3.Row | None, bool]:
    payload = _job_payload(job)
    reply = payload.get("reply")
    if not isinstance(reply, dict):
        return None, False
    text = result.get("response_text") or result.get("text") or result.get("summary")
    if not isinstance(text, str) or not text.strip():
        return None, False
    _validate_destination(reply, label="reply")
    workspace = get_workspace(conn, job["workspace_id"])
    platform = _normalize_reply_platform(
        str(reply["platform"]),
        default_bus=workspace.default_bus if workspace else None,
    )
    message_key = f"runtime:job:{job['id']}:response:{event_id}:{platform}:{reply['destination']}"
    delivery, created = create_delivery(
        conn,
        event_id=event_id,
        platform=platform,
        destination=str(reply["destination"]),
        message_key=message_key,
        payload={
            "text": text,
            "workspace_id": job["workspace_id"],
            "task_id": job["task_id"],
            "job_id": job["id"],
            "origin": payload.get("origin"),
        },
    )
    return delivery, created


def _normalize_reply_platform(platform: str, default_bus: str | None = None) -> str:
    # 8.4.3 P1 #4 / A4: resolve per-workspace. Only coerce discord→discord_webhook
    # when the workspace actually pumps webhooks. A DiscordBus workspace
    # (default_bus='discord') must keep platform=discord. Hardcoding the rewrite
    # broke DiscordBus workspaces.
    if platform == "discord" and default_bus != "discord":
        return "discord_webhook"
    return platform


def _normalize_result(
    *,
    status: str,
    result: dict[str, Any],
    job: sqlite3.Row,
    now: str,
) -> dict[str, Any]:
    normalized = dict(result)
    session_id = normalized.get("session_id")
    if isinstance(session_id, str) and session_id.strip():
        normalized["session_id"] = session_id.strip()
    progress = _job_progress(job)
    if status == "timed_out":
        timeout = normalized.get("timeout") if isinstance(normalized.get("timeout"), dict) else {}
        normalized["timeout"] = {
            "kind": timeout.get("kind") or normalized.get("timeout_kind") or "recoverable",
            "configured_budget_seconds": (
                timeout.get("configured_budget_seconds")
                or normalized.get("timeout_seconds")
                or job["timeout_seconds"]
            ),
            "last_activity_at": timeout.get("last_activity_at") or job["last_activity_at"] or now,
            "session_id": timeout.get("session_id") or normalized.get("session_id") or job["terminal_session_id"] or "",
            "progress": timeout.get("progress") or progress,
            "resume_allowed": bool(timeout.get("resume_allowed", True)),
        }
        normalized["recoverable"] = bool(normalized["timeout"]["resume_allowed"])
        normalized.setdefault("response_text", "Agent timed out; progress was saved and recovery is available.")
    return normalized


def _bounded_progress(
    *,
    stage: str | None,
    summary: str | None,
    session_id: str | None,
) -> dict[str, Any]:
    progress: dict[str, Any] = {}
    if stage:
        progress["stage"] = str(stage).strip()[:80]
    if summary:
        progress["summary"] = str(summary).strip()[:1000]
    if session_id:
        progress["session_id"] = str(session_id).strip()[:200]
    return progress


def _job_progress(job: sqlite3.Row) -> dict[str, Any]:
    raw = job["progress_json"] if "progress_json" in job.keys() else None
    if not raw:
        return {}

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _request_key(workspace_id: str, target_agent: str, origin: dict[str, Any]) -> str:
    platform = origin.get("platform")
    destination = origin.get("destination")
    message_id = origin.get("message_id")
    if not message_id:
        raise RuntimeError("origin.message_id is required when idempotency_key is omitted")
    return f"runtime:request:{workspace_id}:{platform}:{destination}:{message_id}:{target_agent}"


def _validate_destination(value: dict[str, Any], *, label: str) -> None:
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be an object")
    if not value.get("platform"):
        raise RuntimeError(f"{label}.platform is required")
    if not value.get("destination"):
        raise RuntimeError(f"{label}.destination is required")


def _require_agent(conn: sqlite3.Connection, agent_id: str) -> None:
    row = conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"unknown agent: {agent_id}")


def _require_online_agent(conn: sqlite3.Connection, agent_id: str) -> None:
    row = conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"unknown agent: {agent_id}")
    if row["online_state"] != "online":
        raise RuntimeError(f"agent {agent_id} is {row['online_state']}")


def _agent(conn: sqlite3.Connection, agent_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"unknown agent: {agent_id}")
    return row_to_dict(row)


def _job_payload(job: sqlite3.Row) -> dict[str, Any]:
    payload = job["payload_json"]
    if not payload:
        return {}

    decoded = json.loads(payload)
    return decoded if isinstance(decoded, dict) else {}


def _json(value: dict[str, Any] | None) -> str:

    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_result_key(value: dict[str, Any]) -> str:
    import hashlib

    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()[:16]
