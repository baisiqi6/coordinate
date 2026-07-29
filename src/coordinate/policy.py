from __future__ import annotations

import json
import shlex
import sqlite3
from dataclasses import dataclass
from typing import Any

from .db import create_delivery, get_agent_discord_id, get_event, list_events, row_to_dict
from .discord_rendering import render_embed

from .event_presentation import (
    _agent_reported_header,
    _agent_reported_text,
    _assignment_accepted_text,
    _assignment_requested_text,
    _base_payload,
    _blocker_raised_text,
    _blocker_resolved_text,
    _ci_failed_text,
    _ci_passed_text,
    _closeout_requested_text,
    _compact_visible,
    _handoff_requested_text,
    _harness_mutation_failed_text,
    _issue_materialized_text,
    _issue_spotted_text,
    _issue_triaged_text,
    _job_completed_text,
    _job_failed_text,
    _links,
    _optional_suffix,
    _plan_approved_text,
    _plan_ready_text,
    _plan_rejected_text,
    _plan_review_requested_text,
    _pr_created_text,
    _pr_linked_text,
    _pr_review_approved_text,
    _pr_review_changes_requested_text,
    _pr_review_required_text,
    _progress_reported_text,
    _publish_blocked_text,
    _push_required_text,
    _reconciliation_text,
    _render_agent_reported_base,
    _render_assignment_accepted_base,
    _render_assignment_requested_base,
    _review_completed_text,
    _review_rejected_text,
    _standard_base_renderer,
    _task_done_text,
    _task_label,
    _task_mirror_text,
    _visible_block,
    _worker_handoff_text,
    _EVENT_BASE_PAYLOAD_RENDERERS,
    EXPLICITLY_UNSTYLED_EVENT_TYPES,
)

SUPPORTED_EVENT_TYPES = {
    "agent.reported",
    "assignment.accepted",
    "assignment.requested",
    "blocker.raised",
    "blocker.resolved",
    "ci.failed",
    "ci.passed",
    "closeout.requested",
    "handoff.requested",
    "harness.mutation_failed",
    "issue.spotted",
    "issue.triaged",
    "issue.materialized",
    "job.completed",
    "job.failed",
    "plan.ready",
    "plan.approved",
    "plan.rejected",
    "plan.review_requested",
    "pr.created",
    "pr.linked",
    "pr_review.approved",
    "pr_review.changes_requested",
    "pr_review.required",
    "progress.reported",
    "publish.blocked",
    "push.required",
    "reconciliation.completed",
    "review.completed",
    "review.rejected",
    "task.done",
    "task_mirror.created",
    "task_mirror.updated",
    "worker.handoff.prepared",
}
SUPPORTED_PLATFORMS = {"discord", "discord_webhook", "kook", "stdout"}


class PolicyError(RuntimeError):
    pass


@dataclass(frozen=True)
class RenderEventResult:
    supported: bool
    event: dict[str, Any]
    payload: dict[str, Any] | None
    message_key: str | None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "supported": self.supported,
            "event": self.event,
            "payload": self.payload,
            "message_key": self.message_key,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PolicyDeliveryResult:
    supported: bool
    created: bool
    skipped: bool
    event: dict[str, Any]
    delivery: dict[str, Any] | None
    payload: dict[str, Any] | None
    message_key: str | None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "supported": self.supported,
            "created": self.created,
            "skipped": self.skipped,
            "event": self.event,
            "delivery": self.delivery,
            "payload": self.payload,
            "message_key": self.message_key,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PumpEventsResult:
    workspace_id: str
    considered: int
    supported: int
    created: int
    existing: int
    skipped: int
    deliveries: list[dict[str, Any]]
    skipped_events: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "considered": self.considered,
            "supported": self.supported,
            "created": self.created,
            "existing": self.existing,
            "skipped": self.skipped,
            "deliveries": self.deliveries,
            "skipped_events": self.skipped_events,
        }


def _is_runtime_dialog_job_event(event: sqlite3.Row, payload: dict[str, Any]) -> bool:
    if event["event_type"] not in {"job.completed", "job.failed"}:
        return False
    job_id = payload.get("job_id")
    if not isinstance(job_id, str) or not job_id.startswith("request:"):
        return False
    return any(key in payload for key in ("response_text", "text", "summary"))


def render_event(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    platform: str,
    destination: str,
) -> RenderEventResult:
    _ensure_supported_platform(platform)
    event = get_event(conn, event_id)
    event_dict = row_to_dict(event)
    if event["event_type"] not in SUPPORTED_EVENT_TYPES:
        return RenderEventResult(
            supported=False,
            event=event_dict,
            payload=None,
            message_key=None,
            reason=f"unsupported event type: {event['event_type']}",
        )

    raw_payload = _event_payload(event)
    if _is_runtime_dialog_job_event(event, raw_payload):
        return RenderEventResult(
            supported=False,
            event=event_dict,
            payload=None,
            message_key=None,
            reason="runtime dialog job result is delivered via bridge reply, not status card",
        )
    if event["event_type"] == "progress.reported" and raw_payload.get("source") == "discord":
        return RenderEventResult(
            supported=False,
            event=event_dict,
            payload=None,
            message_key=None,
            reason="discord agent progress report is already visible in channel",
        )
    if (
        event["event_type"] == "agent.reported"
        and raw_payload.get("source") == "discord"
        and raw_payload.get("action") in {"accept", "progress"}
    ):
        return RenderEventResult(
            supported=False,
            event=event_dict,
            payload=None,
            message_key=None,
            reason="discord agent accept/progress report is already visible in channel",
        )
    if event["event_type"] == "agent.reported" and raw_payload.get("source") == "runtime":
        return RenderEventResult(
            supported=False,
            event=event_dict,
            payload=None,
            message_key=None,
            reason="runtime agent.reported is covered by the bridge job reply delivery; skip status-card render to avoid duplicate delivery",
        )

    payload = render_event_payload(event)
    return RenderEventResult(
        supported=True,
        event=event_dict,
        payload=payload,
        message_key=message_key_for_event(event, platform=platform, destination=destination),
    )


def create_delivery_for_event(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    platform: str,
    destination: str,
    commit: bool = True,
) -> PolicyDeliveryResult:
    rendered = render_event(conn, event_id, platform=platform, destination=destination)
    if not rendered.supported:
        return PolicyDeliveryResult(
            supported=False,
            created=False,
            skipped=True,
            event=rendered.event,
            delivery=None,
            payload=None,
            message_key=None,
            reason=rendered.reason,
        )

    if rendered.payload is None or rendered.message_key is None:
        raise PolicyError(f"event {event_id} did not render a delivery payload")

    delivery, created = create_delivery(
        conn,
        event_id=event_id,
        platform=platform,
        destination=destination,
        message_key=rendered.message_key,
        payload=rendered.payload,
        commit=commit,
    )
    return PolicyDeliveryResult(
        supported=True,
        created=created,
        skipped=False,
        event=rendered.event,
        delivery=row_to_dict(delivery),
        payload=rendered.payload,
        message_key=rendered.message_key,
    )


def render_event_deliveries(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    platform: str,
    destination: str,
) -> list[RenderEventResult]:
    rendered = render_event(conn, event_id, platform=platform, destination=destination)
    if not rendered.supported:
        return [rendered]

    results = [rendered]
    for extra in (
        _agent_handoff_delivery(conn, rendered.event, platform=platform, destination=destination),
        _agent_lifecycle_delivery(conn, rendered.event, platform=platform, destination=destination),
    ):
        if extra is not None:
            results.append(extra)
    return results


def create_deliveries_for_event(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    platform: str,
    destination: str,
) -> list[PolicyDeliveryResult]:
    results: list[PolicyDeliveryResult] = []
    for rendered in render_event_deliveries(conn, event_id, platform=platform, destination=destination):
        if not rendered.supported:
            results.append(PolicyDeliveryResult(
                supported=False, created=False, skipped=True,
                event=rendered.event, delivery=None, payload=None,
                message_key=None, reason=rendered.reason,
            ))
            continue
        if rendered.payload is None or rendered.message_key is None:
            raise PolicyError(f"event {event_id} delivery did not render a payload")
        delivery, created = create_delivery(
            conn,
            event_id=event_id,
            platform=platform,
            destination=destination,
            message_key=rendered.message_key,
            payload=rendered.payload,
        )
        results.append(PolicyDeliveryResult(
            supported=True, created=created, skipped=False,
            event=rendered.event, delivery=row_to_dict(delivery),
            payload=rendered.payload, message_key=rendered.message_key,
        ))
    return results


def _agent_handoff_delivery(
    conn: sqlite3.Connection,
    event: dict[str, Any],
    *,
    platform: str,
    destination: str,
) -> RenderEventResult | None:
    if platform != "discord_webhook":
        return None

    target_agent = event.get("payload", {}).get("target_agent")
    if not target_agent:
        return None

    workspace_id = event.get("workspace_id")
    if not workspace_id:
        return None

    discord_id = get_agent_discord_id(conn, workspace_id, target_agent)
    if not discord_id:
        return None

    task_id = event.get("payload", {}).get("task_id", "")
    bootstrap_path = event.get("payload", {}).get("bootstrap_path", "")

    role = event.get("payload", {}).get("role", "worker")
    if role == "reviewer":
        action_field = "action=review.begin"
    else:
        action_field = "action=assignment.accept"

    # Append v1 execution context fields safely quoted. These are advisory
    # bootstrap-location metadata; adapter cwd/session authority comes from the
    # Coordinate runtime job claim response.
    profile = event.get("payload", {}).get("execution_profile") or {}
    workspace_path = profile.get("workspace_path") or ""
    harness_root = profile.get("harness_root") or ""
    branch = event.get("payload", {}).get("branch") or ""

    # Legacy machine block ends with action=...; v1 fields are appended after it
    # so the prefix remains byte-compatible with pre-P9-1 parsers.
    text = (
        f"[handoff] <@{discord_id}>\n"
        f"workspace_id={workspace_id}\n"
        f"task_id={task_id}\n"
        f"bootstrap={bootstrap_path}\n"
        f"{action_field}\n"
    )
    if workspace_path:
        text += f"context_version=1 workspace_path={shlex.quote(workspace_path)}\n"
    if harness_root:
        text += f"harness_root={shlex.quote(harness_root)}\n"
    if branch:
        text += f"branch={shlex.quote(branch)}\n"

    event_id = event.get("id", "")
    message_key = f"{workspace_id}:{event_id}:agent_handoff:{platform}:{destination}"

    return RenderEventResult(
        supported=True,
        event=event,
        payload={"text": text, "mention_users": [discord_id]},
        message_key=message_key,
    )


def _agent_lifecycle_delivery(
    conn: sqlite3.Connection,
    event: dict[str, Any],
    *,
    platform: str,
    destination: str,
) -> RenderEventResult | None:
    if platform != "discord_webhook":
        return None

    event_type = event.get("event_type")
    if event_type not in {"closeout.requested", "task.done"}:
        return None

    workspace_id = event.get("workspace_id")
    task_id = event.get("task_id") or event.get("payload", {}).get("task_id")
    if not workspace_id or not task_id:
        return None

    owner = _task_owner_for_event(conn, workspace_id, task_id)
    if not owner:
        return None

    discord_id = get_agent_discord_id(conn, workspace_id, owner)
    if not discord_id:
        return None

    action = "assignment.closeout" if event_type == "closeout.requested" else "task.done"
    text = (
        f"[lifecycle] <@{discord_id}>\n"
        f"workspace_id={workspace_id}\n"
        f"task_id={task_id}\n"
        f"action={action}"
    )

    event_id = event.get("id", "")
    message_key = (
        f"{workspace_id}:{event_id}:agent_lifecycle:{platform}:"
        f"{destination}:target_{owner}"
    )

    return RenderEventResult(
        supported=True,
        event=event,
        payload={"text": text, "mention_users": [discord_id]},
        message_key=message_key,
    )


def _task_owner_for_event(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
) -> str | None:
    row = conn.execute(
        "SELECT owner FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if row is not None and row["owner"]:
        return row["owner"]
    rows = conn.execute(
        """
        SELECT payload_json
        FROM events
        WHERE workspace_id = ?
          AND task_id = ?
          AND event_type IN ('assignment.accepted', 'task_mirror.updated')
        ORDER BY created_at DESC, rowid DESC
        LIMIT 20
        """,
        (workspace_id, task_id),
    ).fetchall()
    for event_row in rows:
        if not event_row["payload_json"]:
            continue
        try:
            payload = json.loads(event_row["payload_json"])
        except json.JSONDecodeError:
            continue
        owner = payload.get("owner")
        if owner:
            return owner
    return None


def pump_events(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    platform: str,
    destination: str,
    limit: int = 20,
    task_id: str | None = None,
    event_type: str | None = None,
    allow_backfill: bool = False,
    min_rowid: int | None = None,
    max_rowid: int | None = None,
) -> PumpEventsResult:
    _ensure_supported_platform(platform)
    if limit < 1:
        raise PolicyError("limit must be at least 1")
    live_platforms = {"discord", "discord_webhook", "kook"}
    if platform in live_platforms and not allow_backfill and not (task_id or event_type or (min_rowid is not None and max_rowid is not None)):
        raise PolicyError(
            "refusing broad live event backfill; use policy create-deliveries <event-id>, "
            "add --task-id/--event-type, or pass --allow-backfill intentionally"
        )

    considered = 0
    supported = 0
    created = 0
    existing = 0
    skipped_events: list[dict[str, Any]] = []
    deliveries: list[dict[str, Any]] = []

    for event in list_events(conn, workspace_id):
        if task_id and event["task_id"] != task_id:
            continue
        if event_type and event["event_type"] != event_type:
            continue
        event_rowid = event["rowid"]
        if min_rowid is not None and event_rowid <= min_rowid:
            continue
        if max_rowid is not None and event_rowid > max_rowid:
            continue
        considered += 1
        delivery_results = create_deliveries_for_event(
            conn,
            event["id"],
            platform=platform,
            destination=destination,
        )

        event_had_supported = False
        for dr in delivery_results:
            if not dr.supported:
                skipped_events.append(
                    {
                        "event_id": event["id"],
                        "event_type": event["event_type"],
                        "reason": dr.reason,
                    }
                )
                continue
            event_had_supported = True
            if dr.created:
                created += 1
                if dr.delivery is not None:
                    deliveries.append(dr.delivery)
            else:
                existing += 1

        if event_had_supported:
            supported += 1

        if created >= limit:
            break

    return PumpEventsResult(
        workspace_id=workspace_id,
        considered=considered,
        supported=supported,
        created=created,
        existing=existing,
        skipped=len(skipped_events),
        deliveries=deliveries,
        skipped_events=skipped_events,
    )


def render_event_payload(event: sqlite3.Row) -> dict[str, Any]:
    payload = _event_payload(event)
    event_type = event["event_type"]
    result = _render_event_base_payload(event, event_type, payload)
    _enrich_with_embed(result, event, payload)
    return result


def _render_event_base_payload(
    event: sqlite3.Row,
    event_type: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    # #12.3: registry dispatch replaces the former if/elif chain. Each event
    # type maps to a renderer (event, payload) -> base-payload dict; unknown
    # types raise the same PolicyError as before. See _EVENT_BASE_PAYLOAD_RENDERERS.
    renderer = _EVENT_BASE_PAYLOAD_RENDERERS.get(event_type)
    if renderer is None:
        raise PolicyError(f"unsupported event type: {event_type}")
    return renderer(event, payload)


def _enrich_with_embed(result: dict[str, Any], event: sqlite3.Row, payload: dict[str, Any]) -> None:
    event_type = event["event_type"]
    event_dict = row_to_dict(event)
    embed = render_embed(event_type, event_dict, payload)
    if embed is not None:
        result["embeds"] = [embed]


def _ensure_supported_platform(platform: str) -> None:
    if platform not in SUPPORTED_PLATFORMS:
        supported = ", ".join(sorted(SUPPORTED_PLATFORMS))
        raise PolicyError(f"unsupported policy platform: {platform}; supported: {supported}")


def message_key_for_event(event: sqlite3.Row, *, platform: str, destination: str) -> str:
    workspace_id = event["workspace_id"]
    if not workspace_id:
        raise PolicyError(f"event {event['id']} has no workspace_id")
    return f"{workspace_id}:{event['id']}:{platform}:{destination}"




def _event_payload(event: sqlite3.Row) -> dict[str, Any]:
    value = json.loads(event["payload_json"])
    if not isinstance(value, dict):
        raise PolicyError(f"event {event['id']} payload_json must decode to an object")
    return value






















def _delivery_for_message_key(conn: sqlite3.Connection, message_key: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM deliveries WHERE message_key = ?",
        (message_key,),
    ).fetchone()




























































# ---------------------------------------------------------------------------
# #12.3 — registry dispatch for _render_event_base_payload
# ---------------------------------------------------------------------------
# Each event type maps to a renderer (event, payload) -> base-payload dict.
# Standard events (fixed header + text_fn + _links) use _standard_base_renderer;
# events that deviate (dynamic header, post-processing, custom links) have
# dedicated named renderers. Adding a new event type = one dict entry.
