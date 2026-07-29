import io
import json
import os
import unittest

from coordinate.bus import (
    BusError,
    DiscordBus,
    KookBus,
    StdoutBus,
    WebhookBus,
    bus_for_platform,
    discord_content,
    pump_deliveries,
    send_delivery,
)
from coordinate.db import (
    create_delivery,
    initialize,
    list_deliveries,
    mark_delivery_sending,
    row_to_dict,
)


class FailingBus:
    def send(self, *, destination, payload, message_key):
        raise RuntimeError("network down")


class FakeHttpPost:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def __call__(self, url, headers, body):
        self.calls.append({"url": url, "headers": headers, "body": body})
        return self.response


class BusTests(unittest.TestCase):
    def make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        return conn

    def test_stdout_bus_send_marks_delivery_sent(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:1",
            payload={"text": "[ASSIGN] mvp-001"},
        )
        stream = io.StringIO()

        result = send_delivery(conn, row["id"], bus=StdoutBus(stream))

        self.assertTrue(result.sent)
        self.assertEqual(result.delivery["status"], "sent")
        self.assertEqual(result.delivery["attempt_count"], 1)
        self.assertTrue(result.delivery["platform_message_id"].startswith("stdout:"))
        self.assertIn("[ASSIGN] mvp-001", stream.getvalue())

    def test_send_failure_records_error_and_dead_after_max_attempts(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:1",
            payload={"text": "[ASSIGN] mvp-001"},
        )

        first = send_delivery(conn, row["id"], bus=FailingBus(), max_attempts=2)
        second = send_delivery(conn, row["id"], bus=FailingBus(), max_attempts=2)

        self.assertFalse(first.sent)
        self.assertEqual(first.delivery["status"], "failed")
        self.assertFalse(second.sent)
        self.assertEqual(second.delivery["status"], "dead")
        self.assertEqual(second.delivery["attempt_count"], 2)
        self.assertEqual(second.delivery["last_error"], "network down")

    def test_send_rejects_already_sent_delivery(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:1",
            payload={"text": "hello"},
        )
        send_delivery(conn, row["id"], bus=StdoutBus(io.StringIO()))

        with self.assertRaisesRegex(BusError, "only pending/failed deliveries can be sent"):
            send_delivery(conn, row["id"], bus=StdoutBus(io.StringIO()))

    def test_pump_only_processes_pending_limit(self):
        conn = self.make_conn()
        for index in range(3):
            create_delivery(
                conn,
                platform="stdout",
                destination="local",
                message_key=f"demo:message:{index}",
                payload={"text": f"message {index}"},
            )

        result = pump_deliveries(conn, platform="stdout", limit=2, bus=StdoutBus(io.StringIO()))

        deliveries = [row_to_dict(row) for row in list_deliveries(conn)]
        self.assertEqual(result.processed, 2)
        self.assertEqual(result.sent, 2)
        self.assertEqual([delivery["status"] for delivery in deliveries], ["sent", "sent", "pending"])

    def test_pump_retries_failed_deliveries_until_dead(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:retry",
            payload={"text": "[BLOCKER] retry"},
        )

        first = send_delivery(conn, row["id"], bus=FailingBus(), max_attempts=3)
        second = pump_deliveries(conn, platform="stdout", limit=10, bus=FailingBus())
        third = pump_deliveries(conn, platform="stdout", limit=10, bus=FailingBus())

        delivery = row_to_dict(list_deliveries(conn)[0])
        self.assertEqual(first.delivery["status"], "failed")
        self.assertEqual(second.processed, 1)
        self.assertEqual(second.failed, 1)
        self.assertEqual(second.deliveries[0]["status"], "failed")
        self.assertEqual(third.processed, 1)
        self.assertEqual(third.failed, 1)
        self.assertEqual(delivery["status"], "dead")
        self.assertEqual(delivery["attempt_count"], 3)
        self.assertEqual(delivery["last_error"], "network down")

    def test_pump_can_explicitly_recover_sending_deliveries(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:recover",
            payload={"text": "[STATE] recover"},
        )
        mark_delivery_sending(conn, row["id"])

        skipped = pump_deliveries(conn, platform="stdout", limit=10, bus=StdoutBus(io.StringIO()))
        sent = pump_deliveries(
            conn,
            platform="stdout",
            limit=10,
            bus=StdoutBus(io.StringIO()),
            recover_sending=True,
        )

        delivery = row_to_dict(list_deliveries(conn)[0])
        self.assertEqual(skipped.processed, 0)
        self.assertEqual(sent.sent, 1)
        self.assertEqual(delivery["status"], "sent")
        self.assertEqual(delivery["attempt_count"], 2)

    def test_discord_bus_posts_plain_text_message_and_returns_message_id(self):
        http = FakeHttpPost({"id": "discord-message-1"})
        bus = DiscordBus(token="secret", api_base="https://discord.test/api/v10", http_post=http)

        message_id = bus.send(
            destination="channel-1",
            payload={"text": "[RESULT] mvp-001 done"},
            message_key="demo:event:discord:channel-1",
        )

        self.assertEqual(message_id, "discord:discord-message-1")
        self.assertEqual(http.calls[0]["url"], "https://discord.test/api/v10/channels/channel-1/messages")
        self.assertEqual(http.calls[0]["headers"]["Authorization"], "Bot secret")
        self.assertEqual(http.calls[0]["body"]["content"], "[RESULT] mvp-001 done")
        self.assertEqual(http.calls[0]["body"]["allowed_mentions"], {"parse": []})

    def test_kook_bus_posts_kmarkdown_message_and_returns_message_id(self):
        http = FakeHttpPost({"code": 0, "message": "ok", "data": {"msg_id": "kook-message-1"}})
        bus = KookBus(token="secret", api_base="https://kook.test/api/v3", http_post=http)

        message_id = bus.send(
            destination="channel-1",
            payload={"text": "[BLOCKER] mvp-002 failed"},
            message_key="demo:event:kook:channel-1",
        )

        self.assertEqual(message_id, "kook:kook-message-1")
        self.assertEqual(http.calls[0]["url"], "https://kook.test/api/v3/message/create")
        self.assertEqual(http.calls[0]["headers"]["Authorization"], "Bot secret")
        self.assertEqual(http.calls[0]["body"]["type"], 9)
        self.assertEqual(http.calls[0]["body"]["target_id"], "channel-1")
        self.assertEqual(http.calls[0]["body"]["content"], "[BLOCKER] mvp-002 failed")

    def test_discord_delivery_records_platform_message_id_without_real_network(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="discord",
            destination="channel-1",
            message_key="demo:message:1",
            payload={"text": "[RESULT] mvp-001"},
        )
        bus = DiscordBus(
            token="secret",
            http_post=FakeHttpPost({"id": "discord-message-1"}),
        )

        result = send_delivery(conn, row["id"], bus=bus)

        self.assertTrue(result.sent)
        self.assertEqual(result.delivery["status"], "sent")
        self.assertEqual(result.delivery["platform_message_id"], "discord:discord-message-1")

    def test_kook_response_error_is_recorded_without_busy_retry(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="kook",
            destination="channel-1",
            message_key="demo:message:1",
            payload={"text": "[RESULT] mvp-001"},
        )
        bus = KookBus(
            token="secret",
            http_post=FakeHttpPost({"code": 40000, "message": "rate limited", "data": {}}),
        )

        result = send_delivery(conn, row["id"], bus=bus)

        self.assertFalse(result.sent)
        self.assertEqual(result.delivery["status"], "failed")
        self.assertEqual(result.delivery["attempt_count"], 1)
        self.assertIn("rate limited", result.delivery["last_error"])

    def test_real_platform_adapters_require_env_token(self):
        old_discord = os.environ.pop("DISCORD_BOT_TOKEN", None)
        old_kook = os.environ.pop("KOOK_BOT_TOKEN", None)
        self.addCleanup(self._restore_env, "DISCORD_BOT_TOKEN", old_discord)
        self.addCleanup(self._restore_env, "KOOK_BOT_TOKEN", old_kook)

        with self.assertRaisesRegex(BusError, "DISCORD_BOT_TOKEN"):
            bus_for_platform("discord")
        with self.assertRaisesRegex(BusError, "KOOK_BOT_TOKEN"):
            bus_for_platform("kook")

    def _restore_env(self, key, value):
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


class WebhookBusTests(unittest.TestCase):
    def test_webhook_send_posts_to_url_with_wait(self):
        http = FakeHttpPost({"id": "webhook-msg-1"})
        bus = WebhookBus(webhook_url="https://discord.test/api/webhooks/123/token", http_post=http)

        bus.send(
            destination="discord-nexus-status",
            payload={"text": "[ASSIGN] phase-4"},
            message_key="demo:webhook:1",
        )

        self.assertEqual(http.calls[0]["url"], "https://discord.test/api/webhooks/123/token?wait=true")

    def test_webhook_send_returns_platform_message_id(self):
        http = FakeHttpPost({"id": "webhook-msg-42"})
        bus = WebhookBus(webhook_url="https://discord.test/api/webhooks/123/token", http_post=http)

        message_id = bus.send(
            destination="irrelevant",
            payload={"text": "[DONE] phase-4"},
            message_key="demo:webhook:2",
        )

        self.assertEqual(message_id, "discord_webhook:webhook-msg-42")

    def test_webhook_send_uses_coordinator_username(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = WebhookBus(webhook_url="https://discord.test/api/webhooks/123/token", http_post=http)

        bus.send(destination="", payload={"text": "test"}, message_key="k1")

        self.assertEqual(http.calls[0]["body"]["username"], "coordinator")

    def test_webhook_send_allows_no_mentions(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = WebhookBus(webhook_url="https://discord.test/api/webhooks/123/token", http_post=http)

        bus.send(destination="", payload={"text": "test"}, message_key="k1")

        self.assertEqual(http.calls[0]["body"]["allowed_mentions"], {"parse": []})

    def test_webhook_from_env_raises_without_env_var(self):
        old = os.environ.pop("DISCORD_WEBHOOK_URL", None)
        self.addCleanup(self._restore_env, "DISCORD_WEBHOOK_URL", old)

        with self.assertRaisesRegex(BusError, "DISCORD_WEBHOOK_URL"):
            WebhookBus.from_env()

    def test_bus_for_platform_returns_webhook_bus(self):
        os.environ["DISCORD_WEBHOOK_URL"] = "https://discord.test/api/webhooks/1/t"
        self.addCleanup(os.environ.pop, "DISCORD_WEBHOOK_URL", None)

        bus = bus_for_platform("discord_webhook")

        self.assertIsInstance(bus, WebhookBus)

    def test_webhook_ignores_destination(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = WebhookBus(webhook_url="https://discord.test/api/webhooks/123/token", http_post=http)

        bus.send(
            destination="discord-nexus-status",
            payload={"text": "[ASSIGN] test"},
            message_key="k1",
        )

        call = http.calls[0]
        self.assertNotIn("discord-nexus-status", call["url"])
        self.assertNotIn("discord-nexus-status", json.dumps(call["headers"]))
        self.assertNotIn("discord-nexus-status", json.dumps(call["body"]))

    def test_webhook_send_with_mention_users(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = WebhookBus(webhook_url="https://discord.test/api/webhooks/123/token", http_post=http)

        bus.send(
            destination="",
            payload={"text": "ping", "mention_users": ["123"]},
            message_key="k1",
        )

        self.assertEqual(
            http.calls[0]["body"]["allowed_mentions"],
            {"users": ["123"]},
        )

    def test_webhook_send_without_mention_users_keeps_suppress(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = WebhookBus(webhook_url="https://discord.test/api/webhooks/123/token", http_post=http)

        bus.send(
            destination="",
            payload={"text": "no ping"},
            message_key="k1",
        )

        self.assertEqual(
            http.calls[0]["body"]["allowed_mentions"],
            {"parse": []},
        )

    def _restore_env(self, key, value):
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


class DiscordContentDedupTests(unittest.TestCase):
    def test_embed_compresses_to_first_line(self):
        payload = {
            "text": "[REVIEW] phase-5.8-protocol-readability-smoke\n状态：审核完成\n审核人：codex\n结论：approved",
            "embeds": [{"title": "Review"}],
        }
        self.assertEqual(discord_content(payload), "[REVIEW] phase-5.8-protocol-readability-smoke")

    def test_no_embed_keeps_full_text(self):
        payload = {
            "text": "[REVIEW] phase-5.8\n状态：审核完成\n审核人：codex",
        }
        self.assertEqual(discord_content(payload), payload["text"])

    def test_handoff_never_compressed(self):
        payload = {
            "text": "[handoff] <@123>\nworkspace_id=demo\ntask_id=t1\naction=assignment.accept",
            "embeds": [{"title": "Handoff"}],
        }
        self.assertEqual(discord_content(payload), payload["text"])

    def test_lifecycle_never_compressed(self):
        payload = {
            "text": "[lifecycle] <@456>\nworkspace_id=demo\ntask_id=t2\naction=task.done",
            "embeds": [{"title": "Lifecycle"}],
        }
        self.assertEqual(discord_content(payload), payload["text"])

    def test_agent_report_never_compressed(self):
        payload = {
            "text": "[agent-report]\naction=done\nworkspace_id=demo\ntask_id=t3\nsummary=done",
            "embeds": [{"title": "Report"}],
        }
        self.assertEqual(discord_content(payload), payload["text"])

    def test_machine_protocol_detection_tolerates_whitespace_and_case(self):
        payload = {
            "text": "  [Agent-Report]\naction=done\nworkspace_id=demo\ntask_id=t3\nsummary=done",
            "embeds": [{"title": "Report"}],
        }
        self.assertEqual(discord_content(payload), payload["text"])

    def test_single_line_with_embed_stays_as_is(self):
        payload = {
            "text": "[DONE] phase-5.8",
            "embeds": [{"title": "Done"}],
        }
        self.assertEqual(discord_content(payload), "[DONE] phase-5.8")

    def test_discord_bus_uses_compressed_content_with_embeds(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = DiscordBus(token="t", api_base="https://d.test/api/v10", http_post=http)
        bus.send(
            destination="ch-1",
            payload={
                "text": "[REVIEW] task-1\n状态：审核完成\n审核人：codex",
                "embeds": [{"title": "Review"}],
            },
            message_key="k1",
        )
        self.assertEqual(http.calls[0]["body"]["content"], "[REVIEW] task-1")
        self.assertIn("embeds", http.calls[0]["body"])

    def test_webhook_bus_uses_compressed_content_with_embeds(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = WebhookBus(webhook_url="https://d.test/webhooks/1/t", http_post=http)
        bus.send(
            destination="",
            payload={
                "text": "[ACCEPT] task-2\n状态：已接收\n执行者：agent-1",
                "embeds": [{"title": "Accept"}],
            },
            message_key="k2",
        )
        self.assertEqual(http.calls[0]["body"]["content"], "[ACCEPT] task-2")
        self.assertIn("embeds", http.calls[0]["body"])

    def test_discord_bus_sends_full_text_without_embeds(self):
        http = FakeHttpPost({"id": "msg-1"})
        bus = DiscordBus(token="t", api_base="https://d.test/api/v10", http_post=http)
        text = "[REVIEW] task-1\n状态：审核完成\n审核人：codex"
        bus.send(
            destination="ch-1",
            payload={"text": text},
            message_key="k1",
        )
        self.assertEqual(http.calls[0]["body"]["content"], text)


if __name__ == "__main__":
    unittest.main()
