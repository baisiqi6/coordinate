"""One-shot channel messages on the existing event and delivery ledger."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Callable

from .bus import ChannelDeliveryRejected, DiscordBus, MessageBus
from .db import (
    ChannelBindingForbidden, ChannelIdempotencyConflict, finish_channel_delivery,
    inspect_channel_delivery, reserve_channel_delivery, resolve_channel_workspace,
)

_FORMAT_VERSION = "channel_io.v1"
_MESSAGES = {
    "invalid_input": "invalid input",
    "forbidden": "forbidden",
    "unsupported_platform": "unsupported platform",
    "unsupported_channel_type": "unsupported channel type",
    "idempotency_conflict": "send_key was already used with different parameters",
    "upstream_unavailable": "channel service is unavailable",
    "content_unavailable": "message content is unavailable",
    "rate_limited": "channel service is rate limited",
    "delivery_rejected": "Discord rejected the channel message; use a new send_key for a new attempt.",
    "delivery_uncertain": "Delivery outcome is unknown; reuse the same send_key to inspect it.",
}


class _InputError(ValueError):
    def __init__(self, code: str):
        self.code = code


def _error(code: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"ok": False, "data": data, "error": {"code": code, "message": _MESSAGES[code]}}


@dataclass(frozen=True)
class _SendInput:
    workspace_id: str
    channel_id: str
    platform: str
    text: str
    send_key: str
    reply_to_message_id: str | None


def _parse_input(value: Any) -> _SendInput:
    allowed = {"workspace_id", "channel_id", "platform", "text", "send_key", "reply_to_message_id"}
    if not isinstance(value, dict) or set(value) - allowed:
        raise _InputError("invalid_input")
    workspace = value.get("workspace_id")
    channel = value.get("channel_id")
    platform = value.get("platform", "discord")
    text = value.get("text")
    key = value.get("send_key")
    reply = value.get("reply_to_message_id")
    if not isinstance(platform, str):
        raise _InputError("invalid_input")
    if platform != "discord":
        raise _InputError("unsupported_platform")
    if (not isinstance(workspace, str) or not workspace or workspace != workspace.strip()
            or not isinstance(channel, str) or not re.fullmatch(r"[0-9]{1,20}", channel)
            or not isinstance(text, str) or not text.strip()
            or not isinstance(key, str) or not 1 <= len(key) <= 128 or not key.isascii()
            or (reply is not None and (not isinstance(reply, str) or not re.fullmatch(r"[0-9]{1,20}", reply)))):
        raise _InputError("invalid_input")
    try:
        if len(text.encode("utf-16-le")) // 2 > 1800:
            raise _InputError("invalid_input")
    except UnicodeEncodeError as exc:
        raise _InputError("invalid_input") from exc
    return _SendInput(workspace, channel, platform, text, key, reply)


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def _authorize(request: _SendInput, principal: Any, connection_factory: Callable) -> None:
    if (principal is None or not isinstance(getattr(principal, "actor", None), str)
            or not principal.actor
            or "coordinate.channel_send" not in getattr(principal, "tools", ())
            or request.workspace_id not in getattr(principal, "workspace_ids", ())
            or request.platform not in getattr(principal, "platforms", ())):
        raise _InputError("forbidden")
    conn = connection_factory()
    try:
        binding = resolve_channel_workspace(conn, platform=request.platform, channel_id=request.channel_id)
        if binding is None or binding.workspace_id != request.workspace_id:
            raise _InputError("forbidden")
    finally:
        conn.close()


def _identity(request: _SendInput, actor: str) -> tuple[str, str]:
    scope = [actor, request.workspace_id, request.platform, request.channel_id]
    return ("channel-io:" + _hash([*scope, request.send_key]),
            _hash([*scope, request.text, request.reply_to_message_id, _FORMAT_VERSION]))


def _receipt(row: Any, *, replayed: bool) -> dict[str, Any]:
    if row["status"] == "sent" and row["platform_message_id"]:
        return {"ok": True, "data": {
            "delivery_id": row["id"], "message_id": row["platform_message_id"].removeprefix("discord:"),
            "status": "sent", "replayed": replayed,
        }, "error": None}
    if row["status"] == "dead":
        return _error("delivery_rejected", {"delivery_id": row["id"], "status": "rejected"})
    return _error("delivery_uncertain", {"delivery_id": row["id"], "status": "unknown"})


class ChannelSender:
    def __init__(self, connection_factory: Callable, history: Any = None, bus: MessageBus | None = None):
        self.connection_factory = connection_factory
        self.history = history
        self.bus = bus

    def send(self, input: dict[str, Any], principal: Any) -> dict[str, Any]:
        """Reserve once, release SQLite, POST once, and durably save the result."""
        if principal is None:
            return _error("forbidden")
        try:
            request = _parse_input(input)
            _authorize(request, principal, self.connection_factory)
            history = self._validate_channel(request)
            replay = self._inspect(request, principal.actor)
            if replay is not None:
                return _receipt(replay, replayed=True)
            self._validate_reply(request, history)
            reservation = self._reserve(request, principal.actor)
        except (_InputError, ChannelBindingForbidden, ChannelIdempotencyConflict) as exc:
            code = getattr(exc, "code", None)
            if isinstance(exc, ChannelBindingForbidden):
                code = "forbidden"
            if isinstance(exc, ChannelIdempotencyConflict):
                code = "idempotency_conflict"
            return _error(code)
        except Exception as exc:
            return _error(getattr(exc, "code", "upstream_unavailable")
                          if getattr(exc, "code", "") in _MESSAGES else "upstream_unavailable")
        if not reservation.claimed:
            return _receipt(reservation.delivery, replayed=True)
        return self._post_reserved(reservation.delivery)

    def _validate_channel(self, request: _SendInput) -> Any:
        history = self.history
        if history is None:
            from .discord_history import DiscordHistory
            history = DiscordHistory.from_env()
        validate = getattr(history, "validate_send_channel", None) or getattr(history, "validate_channel", None)
        if validate is None:
            raise _InputError("upstream_unavailable")
        validate(request.channel_id, timeout=5)
        return history

    def _validate_reply(self, request: _SendInput, history: Any) -> None:
        if request.reply_to_message_id is not None:
            reply = history.get_message(request.channel_id, request.reply_to_message_id, timeout=5)
            if (not isinstance(reply, dict) or reply.get("channel_id") != request.channel_id
                    or reply.get("id") != request.reply_to_message_id):
                raise _InputError("invalid_input")

    def _inspect(self, request: _SendInput, actor: str):
        key, fingerprint = _identity(request, actor)
        conn = self.connection_factory()
        try:
            return inspect_channel_delivery(conn, message_key=key, fingerprint=fingerprint,
                                            workspace_id=request.workspace_id,
                                            platform=request.platform, channel_id=request.channel_id)
        finally:
            conn.close()

    def _reserve(self, request: _SendInput, actor: str):
        key, fingerprint = _identity(request, actor)
        payload = {"text": "[multinexus-channel]", "embeds": [{"description": request.text}],
                   "reply_to_message_id": request.reply_to_message_id}
        conn = self.connection_factory()
        try:
            return reserve_channel_delivery(
                conn, actor=actor, workspace_id=request.workspace_id, platform=request.platform,
                channel_id=request.channel_id, message_key=key,
                fingerprint=fingerprint, payload=payload,
            )
        finally:
            conn.close()

    def _post_reserved(self, row: Any) -> dict[str, Any]:
        rejected = False
        platform_message_id = None
        try:
            adapter = self.bus or DiscordBus.from_env()
            platform_message_id = adapter.send(
                destination=row["destination"], payload=json.loads(row["payload_json"]),
                message_key=row["message_key"],
            )
            if (not isinstance(platform_message_id, str)
                    or not re.fullmatch(r"discord:[0-9]{1,20}", platform_message_id)):
                return _receipt(row, replayed=False)
        except ChannelDeliveryRejected:
            rejected = True
        except Exception:
            return _receipt(row, replayed=False)
        try:
            conn = self.connection_factory()
            try:
                saved = finish_channel_delivery(conn, row["id"],
                                                platform_message_id=platform_message_id, rejected=rejected)
            finally:
                conn.close()
        except Exception:
            return _receipt(row, replayed=False)
        return _receipt(saved, replayed=False)


def send_channel_message(input, principal, connection_factory, history=None, bus=None):
    return ChannelSender(connection_factory, history=history, bus=bus).send(input, principal)
