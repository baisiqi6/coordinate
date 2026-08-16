"""Derived runtime-agent liveness backed only by the Coordinate agent registry."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from .db_support import utc_now


AGENT_ACTIVITY_TOUCH_INTERVAL_SECONDS = 30
AGENT_LIVENESS_STALE_AFTER_SECONDS = 90


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def last_seen_age_seconds(value: object, *, now: str | None = None) -> int | None:
    seen = _parse_utc(value)
    current = _parse_utc(now or utc_now())
    if seen is None or current is None or seen > current:
        return None
    return int((current - seen).total_seconds())


def is_agent_liveness_fresh(value: object, *, now: str | None = None) -> bool:
    age = last_seen_age_seconds(value, now=now)
    return age is not None and age <= AGENT_LIVENESS_STALE_AFTER_SECONDS


def touch_agent_activity(
    conn: sqlite3.Connection,
    *,
    agent_id: str,
    now: str | None = None,
) -> bool:
    """Refresh one online agent at most once per touch interval, without events.

    The caller owns the transaction/commit boundary. This function never turns
    an explicitly offline agent back online.
    """
    row = conn.execute(
        "SELECT online_state, last_seen_at FROM agents WHERE id = ?", (agent_id,)
    ).fetchone()
    if row is None or row["online_state"] != "online":
        return False

    observed_at = now or utc_now()
    age = last_seen_age_seconds(row["last_seen_at"], now=observed_at)
    if age is not None and age < AGENT_ACTIVITY_TOUCH_INTERVAL_SECONDS:
        return False

    cursor = conn.execute(
        """
        UPDATE agents
        SET last_seen_at = ?, updated_at = ?
        WHERE id = ? AND online_state = 'online'
        """,
        (observed_at, observed_at, agent_id),
    )
    return cursor.rowcount == 1


def agent_liveness_projection(
    agent: dict[str, Any], *, now: str | None = None
) -> dict[str, Any]:
    projected = dict(agent)
    age = last_seen_age_seconds(agent.get("last_seen_at"), now=now)
    declared_state = agent.get("online_state")
    if declared_state != "online":
        state = "offline"
    elif age is None:
        state = "unknown"
    elif age > AGENT_LIVENESS_STALE_AFTER_SECONDS:
        state = "stale"
    else:
        state = "online"
    projected.update(
        {
            "liveness_state": state,
            "last_seen_age_seconds": age,
            "liveness_stale_after_seconds": AGENT_LIVENESS_STALE_AFTER_SECONDS,
        }
    )
    return projected
