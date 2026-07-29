"""Parse agent metadata from discord-nexus agents.toml for registry sync."""
from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceInfo:
    source_id: str | None
    source_version: int | None
    source_hash: str | None


@dataclass(frozen=True)
class AgentEntry:
    id: str
    display_name: str
    discord_user_id: str
    agent_type: str  # "managed" or "external"


@dataclass(frozen=True)
class ParseResult:
    agents: list[AgentEntry]
    skipped: list[dict[str, Any]]
    errors: list[str]
    source: SourceInfo | None = None


def _validate_discord_user_id(value: Any) -> str:
    """Return a normalized Discord user id string or raise ValueError."""
    did = str(value).strip()
    if not did or not did.isascii() or not did.isdigit() or int(did) <= 0:
        raise ValueError(f"invalid discord_user_id: {value!r}")
    return did


def _canonical_roster_hash(agents: list[AgentEntry]) -> str:
    """Deterministic SHA-256 of the normalized roster.

    The input is a top-level JSON list sorted by normalized ``id`` ascending.
    Each item is exactly ``id``, ``discord_user_id``, ``display_name``,
    ``agent_type``.  Source metadata, secrets, paths and unknown fields are
    excluded.
    """
    payload = [
        {
            "id": entry.id,
            "discord_user_id": entry.discord_user_id,
            "display_name": entry.display_name,
            "agent_type": entry.agent_type,
        }
        for entry in sorted(agents, key=lambda e: e.id)
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_agents_toml(source: str | Path) -> ParseResult:
    """Parse agents.toml and extract registry-relevant fields.

    Only reads agent metadata. Never reads token_env values, env vars,
    webhook URLs, or secret material.

    Returns ParseResult with agents (those having discord_user_id),
    skipped (entries without discord_user_id), errors, and source identity
    metadata when present.
    """
    path = Path(source).expanduser()
    with open(path, "rb") as f:
        data = tomllib.load(f)

    source = _parse_registry_source(data.get("registry"), path)

    agents: list[AgentEntry] = []
    skipped: list[dict[str, Any]] = []
    errors: list[str] = []
    seen_ids: dict[str, str] = {}
    seen_discord_ids: dict[str, str] = {}

    for raw in data.get("agents", []):
        _process_entry(raw, "managed", agents, skipped, errors, seen_ids, seen_discord_ids)

    for raw in data.get("external_agents", []):
        _process_entry(raw, "external", agents, skipped, errors, seen_ids, seen_discord_ids)

    source_hash: str | None = None
    if source.source_id is not None and not errors:
        source_hash = _canonical_roster_hash(agents)

    return ParseResult(
        agents=agents,
        skipped=skipped,
        errors=errors,
        source=SourceInfo(
            source_id=source.source_id,
            source_version=source.source_version,
            source_hash=source_hash,
        ),
    )


def _parse_registry_source(raw: Any, path: Path) -> SourceInfo:
    """Validate and normalize the [registry] section."""
    if raw is None:
        return SourceInfo(None, None, None)
    if not isinstance(raw, dict):
        return SourceInfo(None, None, None)

    source_id = raw.get("id")
    if source_id is None:
        return SourceInfo(None, None, None)
    source_id = str(source_id).strip()
    if not source_id:
        return SourceInfo(None, None, None)

    version = raw.get("version")
    if version is None:
        return SourceInfo(None, None, None)
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        return SourceInfo(None, None, None)
    source_version = version

    return SourceInfo(source_id, source_version, None)


def _process_entry(
    raw: dict[str, Any],
    agent_type: str,
    agents: list[AgentEntry],
    skipped: list[dict[str, Any]],
    errors: list[str],
    seen_ids: dict[str, str],
    seen_discord_ids: dict[str, str],
) -> None:
    agent_id = raw.get("id")
    if not agent_id:
        errors.append(f"{agent_type} entry missing 'id'")
        return
    agent_id = str(agent_id).strip()
    if not agent_id:
        errors.append(f"{agent_type} entry missing 'id'")
        return

    if agent_id in seen_ids:
        errors.append(f"duplicate agent id '{agent_id}': first in {seen_ids[agent_id]}, duplicate in {agent_type}")
        return
    seen_ids[agent_id] = agent_type

    display_name = str(raw.get("display_name", agent_id)).strip() or agent_id
    discord_user_id = raw.get("discord_user_id")

    if not discord_user_id:
        skipped.append({"id": agent_id, "display_name": display_name, "reason": "missing discord_user_id"})
        return

    try:
        discord_user_id_str = _validate_discord_user_id(discord_user_id)
    except ValueError as exc:
        errors.append(f"{agent_type} entry '{agent_id}': {exc}")
        return

    if discord_user_id_str in seen_discord_ids:
        errors.append(
            f"duplicate discord_user_id '{discord_user_id_str}': "
            f"first in {seen_discord_ids[discord_user_id_str]}, duplicate in {agent_id}"
        )
        return
    seen_discord_ids[discord_user_id_str] = agent_id

    agents.append(AgentEntry(
        id=agent_id,
        display_name=display_name,
        discord_user_id=discord_user_id_str,
        agent_type=agent_type,
    ))
