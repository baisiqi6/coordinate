"""Frozen service-owned channel bindings; no caller-selected transport credentials."""
from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .agent_registry import parse_agents_toml
from .bus import DiscordBus
from .discord_history import DiscordHistory, DiscordHistoryError, _snowflake
from .policy_common import read_strict_policy_file

MESSAGES = {
    "sender_unbound": "Channel sender is not bound to this client.",
    "sender_unavailable": "Channel sender is unavailable; contact the service operator.",
    "sender_identity_mismatch": "Channel sender identity does not match its binding.",
}


def sender_error(code: str) -> DiscordHistoryError:
    return DiscordHistoryError(code, MESSAGES[code])


def valid_agent_id(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,127}", value) is not None


def _absolute(value: Any) -> str:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise ValueError
    return value


def _private_file(path: str) -> str:
    # Reuse policy ownership/regular-file checks, with private secret permissions.
    if stat.S_IMODE(Path(path).stat().st_mode) & 0o077:
        raise ValueError
    return read_strict_policy_file(path)


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError
        value[key] = item
    return value


@dataclass(frozen=True)
class SenderIdentity:
    version: int
    client_id: str
    agent_id: str
    discord_user_id: str
    registry_source_id: str
    registry_source_version: int
    registry_source_hash: str

    def snapshot(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ChannelIdentityContext:
    sender_identity: SenderIdentity
    history: DiscordHistory = field(repr=False)
    bus: DiscordBus = field(repr=False)


@dataclass(frozen=True)
class ChannelIdentityResolver:
    """One config/registry generation. Reload by constructing a new service.

    Token files are opened only for new transport operations, never inspections.
    The token read once here is shared by this operation's history and bus.
    """
    _agents: Mapping = field(repr=False)
    _credentials: Mapping = field(repr=False)
    _source: Any = field(repr=False)

    @classmethod
    def from_env(cls) -> "ChannelIdentityResolver":
        path = os.environ.get("COORDINATE_CHANNEL_IDENTITIES_FILE")
        if path is None:
            return cls(MappingProxyType({}), MappingProxyType({}), None)
        try:
            config = json.loads(_private_file(_absolute(path)), object_pairs_hook=_unique_object)
            if (not isinstance(config, dict) or set(config) != {"version", "registry_path", "credentials"}
                    or type(config["version"]) is not int or config["version"] != 1
                    or not isinstance(config["credentials"], dict)):
                raise ValueError
            parsed = parse_agents_toml(_absolute(config["registry_path"]))
            source = parsed.source
            if parsed.errors or source is None or not source.source_id or source.source_version is None or not source.source_hash:
                raise ValueError
            agents = {agent.id: agent for agent in parsed.agents}
            if len(agents) != len(parsed.agents):
                raise ValueError
            credentials = {}
            for agent_id, credential in config["credentials"].items():
                if (not valid_agent_id(agent_id) or not isinstance(credential, dict)
                        or set(credential) != {"token_file"} or agent_id not in agents):
                    raise ValueError
                _snowflake(agents[agent_id].discord_user_id)
                credentials[agent_id] = _absolute(credential["token_file"])
            return cls(MappingProxyType(agents), MappingProxyType(credentials), source)
        except Exception:
            raise ValueError("Invalid channel identity configuration.") from None

    def identity(self, principal: Any) -> SenderIdentity:
        agent_id = getattr(principal, "channel_agent_id", None)
        if agent_id is None:
            raise sender_error("sender_unbound")
        agent = self._agents.get(agent_id)
        if (agent is None or not agent.enabled or agent_id not in self._credentials
                or not isinstance(getattr(principal, "client_id", None), str) or not principal.client_id):
            raise sender_error("sender_unavailable")
        return SenderIdentity(1, principal.client_id, agent_id, agent.discord_user_id,
                              self._source.source_id, self._source.source_version, self._source.source_hash)

    def resolve(self, principal: Any) -> ChannelIdentityContext:
        identity = self.identity(principal)
        try:
            token = _private_file(self._credentials[identity.agent_id]).strip()
            if not token or not token.isascii() or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in token):
                raise ValueError
        except Exception:
            raise sender_error("sender_unavailable") from None
        return ChannelIdentityContext(identity,
            DiscordHistory(token, expected_bot_id=identity.discord_user_id),
            DiscordBus(token=token, expected_bot_id=identity.discord_user_id))

    def binding_current(self, principal: Any, snapshot: dict | None) -> bool:
        if snapshot is None:
            return False
        try:
            current = self.identity(principal).snapshot()
        except DiscordHistoryError:
            return False
        return all(current[key] == snapshot.get(key) for key in ("client_id", "agent_id", "discord_user_id"))
