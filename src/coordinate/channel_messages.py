"""Authenticated, workspace-bound channel reads and bounded waits."""
from __future__ import annotations

import asyncio
import base64
import binascii
import json
import re
import threading
import time
from contextlib import closing
from typing import Any, Callable

from coordinate import db
from coordinate.discord_history import DiscordHistory, DiscordHistoryDeadline, DiscordHistoryError, _snowflake

Envelope = dict[str, Any]
_COMMON = {"workspace_id", "platform", "channel_id"}
_CURSOR_KEYS = {"v", "platform", "workspace_id", "channel_id", "last_message_id"}


def error(code: str, message: str, *, retry_after_seconds=None) -> Envelope:
    details = {"code": code, "message": message}
    if code == "rate_limited" and retry_after_seconds is not None:
        details["retry_after_seconds"] = retry_after_seconds
    return {"ok": False, "data": None, "error": details}


def _failure(exc: DiscordHistoryError) -> Envelope:
    return error(exc.code, exc.message, retry_after_seconds=exc.retry_after_seconds)


def _encode_cursor(request: dict, last: str) -> str:
    value = {"v": 1, "platform": "discord", "workspace_id": request["workspace_id"], "channel_id": request["channel_id"], "last_message_id": last}
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")


def _decode_cursor(value: str, request: dict) -> str:
    try:
        if not isinstance(value, str) or len(value) > 4096 or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
            raise ValueError
        decoded = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if not isinstance(decoded, dict) or set(decoded) != _CURSOR_KEYS or type(decoded["v"]) is not int or decoded["v"] != 1:
            raise ValueError
        last = _snowflake(decoded["last_message_id"], zero=True)
        if any(not isinstance(decoded[key], str) for key in ("platform", "workspace_id", "channel_id")):
            raise ValueError
    except (ValueError, TypeError, UnicodeError, binascii.Error, DiscordHistoryError):
        raise DiscordHistoryError("invalid_cursor", "Invalid channel cursor.") from None
    if any(decoded[key] != request[key] for key in ("platform", "workspace_id", "channel_id")):
        raise DiscordHistoryError("cursor_scope_mismatch", "Cursor scope does not match this request.")
    return last


def _input(value: dict, operation: str) -> dict:
    extras = {"read": {"after_cursor", "limit"}, "wait": {"after_cursor", "limit", "timeout_seconds"}, "send": {"text", "send_key", "reply_to_message_id"}}
    if not isinstance(value, dict) or set(value) - _COMMON - extras[operation]:
        raise DiscordHistoryError("invalid_input", "Invalid channel input.")
    request = {"platform": "discord", **value}
    workspace = request.get("workspace_id")
    if not isinstance(workspace, str) or not workspace or workspace != workspace.strip() or len(workspace) > 256 or any(ord(c) < 32 for c in workspace):
        raise DiscordHistoryError("invalid_input", "workspace_id is required.")
    _snowflake(request.get("channel_id"))
    if request["platform"] != "discord":
        raise DiscordHistoryError("unsupported_platform", "Only Discord is supported.")
    if operation != "send":
        request.setdefault("limit", 20)
        if type(request["limit"]) is not int or not 1 <= request["limit"] <= 100:
            raise DiscordHistoryError("invalid_input", "limit must be between 1 and 100.")
        if "after_cursor" in request:
            if not isinstance(request["after_cursor"], str):
                raise DiscordHistoryError("invalid_input", "after_cursor must be a string.")
            request["after_message_id"] = _decode_cursor(request["after_cursor"], request)
        elif operation == "wait":
            raise DiscordHistoryError("invalid_input", "after_cursor is required for wait.")
    if operation == "wait":
        request.setdefault("timeout_seconds", 20)
        if type(request["timeout_seconds"]) is not int or not 0 <= request["timeout_seconds"] <= 30:
            raise DiscordHistoryError("invalid_input", "timeout_seconds must be between 0 and 30.")
    return request


class ChannelMessageService:
    def __init__(self, connection_factory: Callable, history=None, sender=None, *, clock: Callable = time.monotonic, sleep: Callable = asyncio.sleep):
        self._connection_factory = connection_factory
        self._history_adapter = history
        self._sender = sender
        self._clock, self._sleep = clock, sleep
        self._wait_lock = threading.Lock()
        self._wait_total = 0
        self._wait_principals: dict[str, int] = {}

    def _history(self):
        if self._history_adapter is None:
            self._history_adapter = DiscordHistory.from_env()
        return self._history_adapter

    def _authorize_grants(self, request: dict, principal, operation: str) -> None:
        if principal is None or f"coordinate.channel_{operation}" not in principal.tools or request["workspace_id"] not in principal.workspace_ids or request["platform"] not in principal.platforms:
            raise DiscordHistoryError("forbidden", "Forbidden.")

    def _authorize(self, request: dict, principal, operation: str) -> None:
        self._authorize_grants(request, principal, operation)
        with closing(self._connection_factory()) as conn:
            binding = db.resolve_channel_workspace(conn, platform=request["platform"], channel_id=request["channel_id"])
        if binding is None or binding.workspace_id != request["workspace_id"]:
            raise DiscordHistoryError("forbidden", "Forbidden.")

    def _page(self, request: dict, timeout: float) -> dict:
        return self._history().read_page(request["channel_id"], after_message_id=request.get("after_message_id"), limit=request["limit"], timeout=timeout)

    def _result(self, request: dict, page: dict) -> Envelope:
        messages = page["messages"]
        next_cursor = _encode_cursor(request, messages[-1]["message_id"] if messages else "0") if messages or "after_cursor" not in request else request["after_cursor"]
        return {"ok": True, "data": {"messages": messages, "next_cursor": next_cursor, "has_more": page["has_more"], "source": "discord_rest", "coverage": "after_cursor" if "after_cursor" in request else "recent_window"}, "error": None}

    def read(self, input: dict, principal) -> Envelope:
        try:
            request = _input(input, "read")
            self._authorize(request, principal, "read")
            page = self._page(request, 30)
            self._authorize(request, principal, "read")
            return self._result(request, page)
        except DiscordHistoryError as exc:
            return _failure(exc)

    def send(self, input: dict, principal) -> Envelope:
        try:
            request = _input(input, "send")
            self._authorize(request, principal, "send")
            if self._sender is None:
                from coordinate.channel_delivery import ChannelSender
                self._sender = ChannelSender(self._connection_factory, history=self._history_adapter)
            return self._sender.send(input, principal)
        except DiscordHistoryError as exc:
            return _failure(exc)

    def _claim_wait(self, principal) -> str:
        key = principal.client_id
        with self._wait_lock:
            current = self._wait_principals.get(key, 0)
            if self._wait_total >= 8 or current >= 2:
                raise DiscordHistoryError("busy", "Channel wait capacity is full.")
            self._wait_total += 1
            self._wait_principals[key] = current + 1
        return key

    def _release_wait(self, key: str) -> None:
        with self._wait_lock:
            self._wait_total -= 1
            remaining = self._wait_principals[key] - 1
            if remaining:
                self._wait_principals[key] = remaining
            else:
                del self._wait_principals[key]

    async def _wait_authorize(self, request: dict, principal, deadline: float) -> None:
        budget = deadline - self._clock()
        if budget <= 0:
            raise DiscordHistoryDeadline()
        try:
            await asyncio.wait_for(asyncio.to_thread(self._authorize, request, principal, "wait"), timeout=budget)
        except TimeoutError:
            raise DiscordHistoryDeadline() from None
        if self._clock() > deadline:
            raise DiscordHistoryDeadline()

    async def _wait_loop(self, request: dict, principal, deadline: float) -> Envelope:
        first = True
        result = None
        while True:
            remaining = deadline - self._clock()
            if not first and remaining <= 0:
                result["data"]["status"] = "timeout"
                return result
            try:
                if not first:
                    await self._wait_authorize(request, principal, deadline)
                budget = deadline - self._clock()
                if budget <= 0:
                    raise DiscordHistoryDeadline()
                try:
                    page = await asyncio.wait_for(asyncio.to_thread(self._page, request, budget), timeout=budget)
                except TimeoutError:
                    raise DiscordHistoryDeadline() from None
                await self._wait_authorize(request, principal, deadline)
            except DiscordHistoryDeadline:
                # Only an earlier fully successful empty read proves normal expiry.
                if result is None:
                    raise
                result["data"]["status"] = "timeout"
                return result
            result = self._result(request, page)
            if page["messages"]:
                result["data"]["status"] = "messages"
                return result
            remaining = deadline - self._clock()
            if remaining <= 0 or request["timeout_seconds"] == 0:
                result["data"]["status"] = "timeout"
                return result
            first = False
            await self._sleep(min(2, remaining))

    async def wait(self, input: dict, principal) -> Envelope:
        key = None
        started = self._clock()
        try:
            request = _input(input, "wait")
            # Zero means one immediate page, still bounded as a whole operation.
            deadline = started + (request["timeout_seconds"] or 30)
            self._authorize_grants(request, principal, "wait")
            key = self._claim_wait(principal)
            await self._wait_authorize(request, principal, deadline)
            return await self._wait_loop(request, principal, deadline)
        except DiscordHistoryError as exc:
            return _failure(exc)
        finally:
            if key is not None:
                self._release_wait(key)
