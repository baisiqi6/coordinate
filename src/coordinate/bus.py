from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Protocol, TextIO

from .db import (
    get_delivery,
    list_deliveries,
    mark_delivery_failed,
    mark_delivery_sending,
    mark_delivery_sent,
    recover_sending_deliveries,
    row_to_dict,
)


class BusError(RuntimeError):
    pass


class MessageBus(Protocol):
    def send(self, *, destination: str, payload: dict[str, Any], message_key: str) -> str:
        """Send a message and return the platform message id."""


class StdoutBus:
    def __init__(self, stream: TextIO | None = None) -> None:
        self.stream = stream or sys.stdout

    def send(self, *, destination: str, payload: dict[str, Any], message_key: str) -> str:
        print(
            json.dumps(
                {
                    "platform": "stdout",
                    "destination": destination,
                    "message_key": message_key,
                    "payload": payload,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=self.stream,
        )
        return f"stdout:{uuid.uuid5(uuid.NAMESPACE_URL, message_key)}"


HttpPost = Callable[[str, dict[str, str], dict[str, Any]], dict[str, Any]]


class DiscordBus:
    def __init__(
        self,
        *,
        token: str,
        api_base: str = "https://discord.com/api/v10",
        http_post: HttpPost | None = None,
    ) -> None:
        self.token = token
        self.api_base = api_base.rstrip("/")
        self.http_post = http_post or post_json

    @classmethod
    def from_env(cls) -> "DiscordBus":
        token = os.environ.get("DISCORD_BOT_TOKEN")
        if not token:
            raise BusError("DISCORD_BOT_TOKEN is required for discord delivery")
        return cls(
            token=token,
            api_base=os.environ.get("DISCORD_API_BASE", "https://discord.com/api/v10"),
        )

    def send(self, *, destination: str, payload: dict[str, Any], message_key: str) -> str:
        body: dict[str, Any] = {
            "content": discord_content(payload),
            "allowed_mentions": {"parse": []},
        }
        _add_embeds(body, payload)
        response = self.http_post(
            f"{self.api_base}/channels/{destination}/messages",
            {
                "Authorization": f"Bot {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "multi-agent-coordinator/0.1",
            },
            body,
        )
        message_id = response.get("id")
        if not message_id:
            raise BusError("discord response did not include message id")
        return f"discord:{message_id}"


class KookBus:
    def __init__(
        self,
        *,
        token: str,
        api_base: str = "https://www.kookapp.cn/api/v3",
        http_post: HttpPost | None = None,
    ) -> None:
        self.token = token
        self.api_base = api_base.rstrip("/")
        self.http_post = http_post or post_json

    @classmethod
    def from_env(cls) -> "KookBus":
        token = os.environ.get("KOOK_BOT_TOKEN")
        if not token:
            raise BusError("KOOK_BOT_TOKEN is required for kook delivery")
        return cls(
            token=token,
            api_base=os.environ.get("KOOK_API_BASE", "https://www.kookapp.cn/api/v3"),
        )

    def send(self, *, destination: str, payload: dict[str, Any], message_key: str) -> str:
        response = self.http_post(
            f"{self.api_base}/message/create",
            {
                "Authorization": f"Bot {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "multi-agent-coordinator/0.1",
            },
            {
                "type": 9,
                "target_id": destination,
                "content": message_text(payload),
                "nonce": message_key,
            },
        )
        if response.get("code") != 0:
            raise BusError(f"kook response error: {response.get('message') or response}")
        data = response.get("data")
        if not isinstance(data, dict) or not data.get("msg_id"):
            raise BusError("kook response did not include msg_id")
        return f"kook:{data['msg_id']}"


class WebhookBus:
    def __init__(
        self,
        *,
        webhook_url: str,
        http_post: HttpPost | None = None,
    ) -> None:
        self.webhook_url = webhook_url.rstrip("/")
        self.http_post = http_post or post_json

    @classmethod
    def from_env(cls) -> "WebhookBus":
        url = os.environ.get("DISCORD_WEBHOOK_URL")
        if not url:
            raise BusError("DISCORD_WEBHOOK_URL is required for discord_webhook delivery")
        return cls(webhook_url=url)

    def send(self, *, destination: str, payload: dict[str, Any], message_key: str) -> str:
        mention_users = payload.get("mention_users")
        allowed_mentions = {"users": mention_users} if mention_users else {"parse": []}
        body: dict[str, Any] = {
            "content": discord_content(payload),
            "username": "coordinator",
            "allowed_mentions": allowed_mentions,
        }
        _add_embeds(body, payload)
        response = self.http_post(
            f"{self.webhook_url}?wait=true",
            {
                "Content-Type": "application/json",
                "User-Agent": "multi-agent-coordinator/0.1",
            },
            body,
        )
        message_id = response.get("id")
        if not message_id:
            raise BusError("discord webhook response did not include message id")
        return f"discord_webhook:{message_id}"


@dataclass(frozen=True)
class SendDeliveryResult:
    delivery: dict[str, Any]
    sent: bool

    def to_dict(self) -> dict[str, Any]:
        return {"delivery": self.delivery, "sent": self.sent}


@dataclass(frozen=True)
class PumpResult:
    processed: int
    sent: int
    failed: int
    deliveries: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "processed": self.processed,
            "sent": self.sent,
            "failed": self.failed,
            "deliveries": self.deliveries,
        }


def send_delivery(
    conn: sqlite3.Connection,
    delivery_id: str,
    *,
    bus: MessageBus | None = None,
    output_stream: TextIO | None = None,
    max_attempts: int = 3,
) -> SendDeliveryResult:
    delivery = get_delivery(conn, delivery_id)
    if delivery["status"] not in {"pending", "failed"}:
        raise BusError(
            f"delivery {delivery_id} is {delivery['status']}; only pending/failed deliveries can be sent"
        )

    adapter = bus or bus_for_platform(delivery["platform"], stream=output_stream)
    sending = mark_delivery_sending(conn, delivery_id)
    try:
        payload = json.loads(sending["payload_json"])
        platform_message_id = adapter.send(
            destination=sending["destination"],
            payload=payload,
            message_key=sending["message_key"],
        )
    except Exception as exc:
        dead = int(sending["attempt_count"]) >= max_attempts
        failed = mark_delivery_failed(conn, delivery_id, error=str(exc), dead=dead)
        return SendDeliveryResult(row_to_dict(failed), False)

    sent = mark_delivery_sent(conn, delivery_id, platform_message_id=platform_message_id)
    return SendDeliveryResult(row_to_dict(sent), True)


def pump_deliveries(
    conn: sqlite3.Connection,
    *,
    platform: str | None = None,
    limit: int = 10,
    bus: MessageBus | None = None,
    output_stream: TextIO | None = None,
    recover_sending: bool = False,
) -> PumpResult:
    if recover_sending:
        recover_sending_deliveries(conn, platform=platform)
    pending = list_deliveries(conn, status="pending", platform=platform)
    retryable_failed = list_deliveries(conn, status="failed", platform=platform)
    retryable = (pending + retryable_failed)[:limit]
    sent_count = 0
    failed_count = 0
    results: list[dict[str, Any]] = []
    for delivery in retryable:
        result = send_delivery(conn, delivery["id"], bus=bus, output_stream=output_stream)
        results.append(result.delivery)
        if result.sent:
            sent_count += 1
        else:
            failed_count += 1
    return PumpResult(
        processed=len(retryable),
        sent=sent_count,
        failed=failed_count,
        deliveries=results,
    )


def bus_for_platform(platform: str, *, stream: TextIO | None = None) -> MessageBus:
    if platform == "stdout":
        return StdoutBus(stream)
    if platform == "discord":
        return DiscordBus.from_env()
    if platform == "kook":
        return KookBus.from_env()
    if platform == "discord_webhook":
        return WebhookBus.from_env()
    raise BusError(f"unsupported bus platform: {platform}")


_MACHINE_PROTOCOL_PREFIXES = ("[handoff]", "[lifecycle]", "[agent-report]")


def discord_content(payload: dict[str, Any]) -> str:
    text = message_text(payload)
    if not payload.get("embeds"):
        return text
    if text.lstrip().lower().startswith(_MACHINE_PROTOCOL_PREFIXES):
        return text
    first_newline = text.find("\n")
    if first_newline == -1:
        return text
    return text[:first_newline]


def message_text(payload: dict[str, Any]) -> str:
    text = payload.get("text")
    if text is None:
        text = payload.get("visible_header")
    if text is None:
        raise BusError("delivery payload must include text")
    text = str(text)
    if not text.strip():
        raise BusError("delivery payload text must not be empty")
    return text


def _add_embeds(body: dict[str, Any], payload: dict[str, Any]) -> None:
    embeds = payload.get("embeds")
    if embeds:
        body["embeds"] = embeds


def post_json(url: str, headers: dict[str, str], body: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=encoded, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise BusError(f"http {exc.code}: {raw}") from exc
    except urllib.error.URLError as exc:
        raise BusError(f"http request failed: {exc.reason}") from exc
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BusError(f"http response was not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise BusError("http response must decode to an object")
    return value
