"""Narrow Discord REST reader; no platform cache or persistent read position."""
from __future__ import annotations

import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

_API = "https://discord.com/api/v10"
VIEW_CHANNEL = 1 << 10
READ_MESSAGE_HISTORY = 1 << 16
SEND_MESSAGES = 1 << 11
EMBED_LINKS = 1 << 14
_CONTENT_FLAGS = (1 << 18) | (1 << 19)


class DiscordHistoryError(Exception):
    """Public, credential-free failure that the channel service can envelope."""
    def __init__(self, code: str, message: str, *, retry_after_seconds: float | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retry_after_seconds = retry_after_seconds


class DiscordHistoryDeadline(DiscordHistoryError):
    """Local operation budget expired, distinct from an upstream HTTP failure."""
    def __init__(self):
        super().__init__("upstream_unavailable", "Discord operation deadline expired.")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _snowflake(value: Any, *, zero: bool = False) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,20}", value):
        raise DiscordHistoryError("invalid_input", "Invalid Discord identifier.")
    if (int(value) == 0 and not zero) or int(value) > (1 << 64) - 1 or str(int(value)) != value:
        raise DiscordHistoryError("invalid_input", "Invalid Discord identifier.")
    return value


def _unavailable() -> DiscordHistoryError:
    return DiscordHistoryError("upstream_unavailable", "Discord is unavailable.")


class DiscordHistory:
    def __init__(self, token: str, *, request_json: Callable | None = None, clock: Callable = time.monotonic):
        if not isinstance(token, str) or not token:
            raise DiscordHistoryError("upstream_unavailable", "Discord reader is not configured.")
        self._token = token
        self._request_json = request_json or self._http_get
        self._clock = clock

    @classmethod
    def from_env(cls) -> "DiscordHistory":
        return cls(os.environ.get("DISCORD_BOT_TOKEN", ""))

    def _http_get(self, method: str, path: str, *, timeout: float) -> Any:
        request = urllib.request.Request(_API + path, headers={"Authorization": f"Bot {self._token}", "User-Agent": "Coordinate-channel/1"}, method=method)
        try:
            with urllib.request.build_opener(_NoRedirect).open(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                retry = None
                try:
                    candidate = json.loads(exc.read().decode("utf-8")).get("retry_after")
                    if isinstance(candidate, (int, float)) and not isinstance(candidate, bool) and math.isfinite(candidate) and candidate >= 0:
                        retry = candidate
                except (ValueError, UnicodeError, AttributeError):
                    pass
                raise DiscordHistoryError("rate_limited", "Discord rate limited the request.", retry_after_seconds=retry) from None
            if exc.code in (401, 403, 404):
                raise DiscordHistoryError("forbidden", "Forbidden.") from None
            raise _unavailable() from None
        except (OSError, ValueError, UnicodeError):
            raise _unavailable() from None

    def _get(self, path: str, deadline: float, expected: type) -> Any:
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise DiscordHistoryDeadline()
        try:
            value = self._request_json("GET", path, timeout=min(5.0, remaining))
        except DiscordHistoryError:
            raise
        except (OSError, ValueError, TypeError):
            raise _unavailable() from None
        if not isinstance(value, expected):
            raise _unavailable()
        if self._clock() > deadline:
            raise DiscordHistoryDeadline()
        return value

    def _deadline(self, timeout: float) -> float:
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not 0 < timeout <= 30:
            raise ValueError("operation timeout must be positive and at most thirty seconds")
        return self._clock() + timeout

    def _capabilities(self, channel_id: str, deadline: float, required: int) -> dict:
        channel = self._get(f"/channels/{_snowflake(channel_id)}", deadline, dict)
        if channel.get("id") != channel_id:
            raise _unavailable()
        if channel.get("type") != 0 or not channel.get("guild_id"):
            raise DiscordHistoryError("unsupported_channel_type", "Only ordinary guild text channels are supported.")
        try:
            guild_id = _snowflake(channel["guild_id"])
            me = self._get("/users/@me", deadline, dict)
            bot_id = _snowflake(me["id"])
            guild = self._get(f"/guilds/{guild_id}", deadline, dict)
            member = self._get(f"/guilds/{guild_id}/members/{bot_id}", deadline, dict)
            roles = self._get(f"/guilds/{guild_id}/roles", deadline, list)
            permissions = _permissions(guild_id, bot_id, guild, member, roles, channel)
            if permissions & required != required:
                raise DiscordHistoryError("forbidden", "Forbidden.")
            application = self._get("/applications/@me", deadline, dict)
            if int(application.get("flags", 0)) & _CONTENT_FLAGS == 0:
                raise DiscordHistoryError("content_unavailable", "Discord message content is unavailable.")
        except (KeyError, TypeError, ValueError):
            raise _unavailable() from None
        return channel

    def validate_channel(self, channel_id: str, *, timeout: float = 5) -> dict:
        return self._capabilities(channel_id, self._deadline(timeout), VIEW_CHANNEL | READ_MESSAGE_HISTORY)

    def validate_send_channel(self, channel_id: str, *, timeout: float = 5) -> dict:
        return self._capabilities(channel_id, self._deadline(timeout), VIEW_CHANNEL | READ_MESSAGE_HISTORY | SEND_MESSAGES | EMBED_LINKS)

    def get_message(self, channel_id: str, message_id: str, *, timeout: float = 5) -> dict:
        channel_id, message_id = _snowflake(channel_id), _snowflake(message_id)
        try:
            value = self._get(f"/channels/{channel_id}/messages/{message_id}", self._deadline(timeout), dict)
        except DiscordHistoryError as exc:
            # A missing message is distinguishable only on an already authorized channel.
            if exc.code == "forbidden":
                self.validate_channel(channel_id, timeout=timeout)
                raise DiscordHistoryError("invalid_input", "Reply message is inaccessible or does not exist.") from None
            raise
        if value.get("id") != message_id or value.get("channel_id") != channel_id:
            raise DiscordHistoryError("invalid_input", "Reply must reference a message in this channel.")
        return value

    def preflight_content(self, channel_id: str, message_id: str, *, timeout: float = 5) -> dict:
        """Deployment proof must use a known message authored by someone else."""
        deadline = self._deadline(timeout)
        self._capabilities(channel_id, deadline, VIEW_CHANNEL | READ_MESSAGE_HISTORY)
        me = self._get("/users/@me", deadline, dict)
        raw = self._get(f"/channels/{_snowflake(channel_id)}/messages/{_snowflake(message_id)}", deadline, dict)
        author = raw.get("author", {})
        if author.get("id") == me.get("id") or not (raw.get("content") or raw.get("embeds") or raw.get("attachments")):
            raise DiscordHistoryError("content_unavailable", "Known external message content is not visible.")
        return normalize_message(raw, channel_id)

    def read_page(self, channel_id: str, *, after_message_id: str | None = None, limit: int = 20, timeout: float = 30) -> dict:
        deadline = self._deadline(timeout)
        self._capabilities(channel_id, deadline, VIEW_CHANNEL | READ_MESSAGE_HISTORY)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise DiscordHistoryError("invalid_input", "limit must be between 1 and 100.")
        params: dict[str, Any] = {"limit": min(limit + 1, 100) if after_message_id is not None else limit}
        if after_message_id is not None:
            params["after"] = _snowflake(after_message_id, zero=True)
        rows = self._get(f"/channels/{channel_id}/messages?{urllib.parse.urlencode(params)}", deadline, list)
        try:
            rows = sorted(rows, key=lambda row: int(_snowflake(row["id"])))
            if len({row["id"] for row in rows}) != len(rows) or len(rows) > params["limit"]:
                raise _unavailable()
            if after_message_id is not None and any(int(row["id"]) <= int(after_message_id) for row in rows):
                raise _unavailable()
            has_more = after_message_id is not None and len(rows) > limit
            rows = rows[:limit]
            if rows and (after_message_id is None or limit == 100):
                probe = self._get(f"/channels/{channel_id}/messages?after={rows[-1]['id']}&limit=1", deadline, list)
                if len(probe) > 1 or any(int(_snowflake(item["id"])) <= int(rows[-1]["id"]) for item in probe):
                    raise _unavailable()
                has_more = bool(probe)
            messages = [normalize_message(row, channel_id) for row in rows]
        except (KeyError, TypeError, ValueError):
            raise _unavailable() from None
        # Permission revocation during a read must not expose a just-fetched page.
        self._capabilities(channel_id, deadline, VIEW_CHANNEL | READ_MESSAGE_HISTORY)
        return {"messages": messages, "has_more": has_more}


def _permissions(guild_id: str, bot_id: str, guild: dict, member: dict, roles: list, channel: dict) -> int:
    role_ids = set(member["roles"]) | {guild_id}
    by_id = {role["id"]: int(role["permissions"]) for role in roles}
    if guild_id not in by_id or not role_ids <= by_id.keys():
        raise ValueError("incomplete permission data")
    permissions = 0
    for role_id in role_ids:
        permissions |= by_id[role_id]
    if guild.get("owner_id") == bot_id or permissions & (1 << 3):
        return (1 << 64) - 1
    overwrites = channel["permission_overwrites"]
    for overwrite in overwrites:
        if overwrite["id"] == guild_id and overwrite["type"] == 0:
            permissions = (permissions & ~int(overwrite["deny"])) | int(overwrite["allow"])
    allow = deny = 0
    for overwrite in overwrites:
        if overwrite["type"] == 0 and overwrite["id"] in role_ids - {guild_id}:
            allow |= int(overwrite["allow"])
            deny |= int(overwrite["deny"])
    permissions = (permissions & ~deny) | allow
    for overwrite in overwrites:
        if overwrite["type"] == 1 and overwrite["id"] == bot_id:
            permissions = (permissions & ~int(overwrite["deny"])) | int(overwrite["allow"])
    return permissions


def normalize_message(raw: dict, channel_id: str) -> dict:
    try:
        if raw["channel_id"] != channel_id:
            raise _unavailable()
        message_id = _snowflake(raw["id"])
        author = raw["author"]
        embeds, attachments = raw["embeds"], raw["attachments"]
        text = raw["content"]
        if not isinstance(text, str) or not isinstance(embeds, list) or not isinstance(attachments, list):
            raise _unavailable()
        if text == "[multinexus-channel]":
            text = "\n".join(embed["description"] for embed in embeds if isinstance(embed.get("description"), str))
        reference = raw.get("message_reference") or {}
        reply = reference.get("message_id") if reference.get("channel_id", channel_id) == channel_id else None
        return {"message_id": message_id, "author": {"id": _snowflake(author["id"]), "name": author.get("global_name") or author["username"], "is_bot": bool(author.get("bot", False))}, "created_at": raw["timestamp"], "text": text, "embeds": embeds, "attachments": attachments, "reply_to_message_id": reply}
    except (KeyError, TypeError, ValueError):
        raise _unavailable() from None
