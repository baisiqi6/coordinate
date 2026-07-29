"""Tests for discord_rendering embed generation."""

import json
import unittest

from coordinate.db import (
    append_event,
    initialize,
    set_workspace_agent as _set_workspace_agent,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.discord_rendering import (
    GREEN,
    GREY,
    RED,
    YELLOW,
    BLUE,
    _MAX_FIELD_VALUE,
    _MAX_TOTAL,
    _MAX_TITLE,
    render_embed,
)


def set_workspace_agent(conn, **kwargs):
    """Create an explicit fixture override without leaking setup audit events."""
    result = _set_workspace_agent(
        conn, actor="test-fixture", reason="rendering test fixture", **kwargs
    )
    conn.execute("DELETE FROM events WHERE event_type = 'workspace.agent_override.set'")
    conn.commit()
    return result


def _event(**overrides):
    base = {
        "id": "evt-001",
        "workspace_id": "ws-test",
        "event_type": "assignment.accepted",
        "actor": "operator",
        "target": None,
        "task_id": "phase-1",
        "payload_json": "{}",
        "created_at": "2026-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


def _payload(**overrides):
    base: dict = {}
    base.update(overrides)
    return base


class TestRenderEmbed(unittest.TestCase):
    """Core rendering tests."""

    def test_unknown_event_returns_none(self):
        result = render_embed("unknown.type", _event(), _payload())
        self.assertIsNone(result)

    def test_all_supported_types_produce_embed(self):
        from coordinate.discord_rendering import _STYLING
        for event_type in _STYLING:
            result = render_embed(event_type, _event(event_type=event_type), _payload())
            self.assertIsNotNone(result, f"{event_type} should produce an embed")
            self.assertIn("title", result)
            self.assertIn("color", result)
            self.assertIn("fields", result)

    def test_title_matches_event_type(self):
        embed = render_embed("assignment.accepted", _event(), _payload())
        self.assertEqual(embed["title"], "🚀 任务已接收")

    def test_colour_by_category(self):
        cases = {
            "assignment.accepted": GREEN,
            "task.done": GREEN,
            "blocker.raised": RED,
            "progress.reported": YELLOW,
            "pr.linked": YELLOW,
            "review.completed": BLUE,
            "reconciliation.completed": GREY,
        }
        for event_type, expected_colour in cases.items():
            embed = render_embed(event_type, _event(event_type=event_type), _payload())
            self.assertEqual(embed["color"], expected_colour, f"{event_type} colour")

    def test_fields_populated(self):
        embed = render_embed(
            "assignment.accepted",
            _event(actor="mac-claude"),
            _payload(owner="mac-claude", session="s-123"),
        )
        field_names = [f["name"] for f in embed["fields"]]
        self.assertIn("📌 任务", field_names)
        self.assertIn("🤖 执行者", field_names)
        self.assertIn("🧵 会话", field_names)

    def test_empty_optional_field_excluded(self):
        embed = render_embed(
            "blocker.raised",
            _event(event_type="blocker.raised", actor="mac-claude", task_id="t-1"),
            _payload(),  # no reason
        )
        field_names = [f["name"] for f in embed["fields"]]
        self.assertIn("📌 任务", field_names)
        self.assertNotIn("🧱 原因", field_names)

    def test_progress_reported_fields(self):
        embed = render_embed(
            "progress.reported",
            _event(event_type="progress.reported", actor="mac-claude", task_id="t-1"),
            _payload(owner="mac-claude", summary="Did X, Y, Z"),
        )
        field_names = [f["name"] for f in embed["fields"]]
        self.assertIn("📌 任务", field_names)
        self.assertIn("🤖 执行者", field_names)
        self.assertIn("📝 摘要", field_names)
        summary_field = next(f for f in embed["fields"] if f["name"] == "📝 摘要")
        self.assertEqual(summary_field["value"], "Did X, Y, Z")

    def test_worker_handoff_fields(self):
        embed = render_embed(
            "worker.handoff.prepared",
            _event(event_type="worker.handoff.prepared", target="mac-claude", task_id="phase-5.5"),
            _payload(target_agent="mac-claude", role="worker", bootstrap_path="/tmp/boot.md", branch="br-1"),
        )
        field_names = [f["name"] for f in embed["fields"]]
        self.assertIn("📌 任务", field_names)
        self.assertIn("🎯 目标", field_names)
        self.assertIn("🎭 角色", field_names)
        self.assertIn("🧭 Bootstrap", field_names)
        self.assertIn("🌿 分支", field_names)

    def test_review_completed_fields(self):
        embed = render_embed(
            "review.completed",
            _event(event_type="review.completed", actor="codex", task_id="t-1"),
            _payload(reviewer="codex", decision="approved", summary="Looks good"),
        )
        field_names = [f["name"] for f in embed["fields"]]
        self.assertIn("🔍 审核人", field_names)
        self.assertIn("✅ 结论", field_names)
        self.assertIn("📝 摘要", field_names)

    def test_ci_failed_shows_failed_checks(self):
        checks = [
            {"name": "lint", "status": "passed"},
            {"name": "test", "status": "failed"},
            {"name": "build", "status": "failed"},
        ]
        embed = render_embed(
            "ci.failed",
            _event(event_type="ci.failed", task_id="t-1"),
            _payload(checks=checks, branch="main"),
        )
        failed_field = next(f for f in embed["fields"] if f["name"] == "❌ 失败检查")
        self.assertIn("test", failed_field["value"])
        self.assertIn("build", failed_field["value"])

    def test_footer_contains_actor_and_workspace(self):
        embed = render_embed(
            "task.done",
            _event(event_type="task.done", actor="mac-claude", workspace_id="ws-1"),
            _payload(),
        )
        self.assertIn("footer", embed)
        self.assertIn("mac-claude", embed["footer"]["text"])
        self.assertIn("ws-1", embed["footer"]["text"])

    def test_no_footer_when_no_actor_or_workspace(self):
        embed = render_embed(
            "task.done",
            _event(event_type="task.done", actor=None, workspace_id=None),
            _payload(),
        )
        self.assertNotIn("footer", embed)

    def test_truncation_of_long_value(self):
        long_summary = "x" * 2000
        embed = render_embed(
            "progress.reported",
            _event(event_type="progress.reported", actor="a", task_id="t"),
            _payload(summary=long_summary),
        )
        summary_field = next(f for f in embed["fields"] if f["name"] == "📝 摘要")
        self.assertLessEqual(len(summary_field["value"]), _MAX_FIELD_VALUE)

    def test_total_embed_under_limit(self):
        embed = render_embed(
            "progress.reported",
            _event(event_type="progress.reported", actor="a", task_id="t"),
            _payload(summary="s" * 3000),
        )
        total = sum(len(f["name"]) + len(f["value"]) for f in embed["fields"])
        total += len(embed.get("title", ""))
        total += len(embed.get("footer", {}).get("text", ""))
        self.assertLessEqual(total, _MAX_TOTAL)

    def test_exception_returns_none(self):
        # Force an exception by passing a payload whose __str__ raises
        class BadStr:
            def __str__(self):
                raise RuntimeError("boom")

        result = render_embed(
            "assignment.accepted",
            {"task_id": BadStr(), "actor": "a", "workspace_id": "ws"},
            {"owner": BadStr()},
        )
        self.assertIsNone(result)


class TestEmbedInDeliveryPayload(unittest.TestCase):
    """Verify policy.render_event_payload adds embeds to delivery payloads."""

    def make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(conn, workspace_id="ws-test", name="Test", path=".", harness_root=".")
        return conn

    def _make_event_row(self, event_type, payload_dict=None, **overrides):
        conn = self.make_conn()
        append_event(
            conn,
            workspace_id="ws-test",
            event_type=event_type,
            actor=overrides.pop("actor", "operator"),
            target=overrides.pop("target", None),
            task_id=overrides.pop("task_id", "phase-1"),
            payload=payload_dict or {},
            idempotency_key=f"test:{event_type}",
        )
        return conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT 1").fetchone()

    def test_assignment_accepted_has_embeds(self):
        from coordinate.policy import render_event_payload

        row = self._make_event_row("assignment.accepted", {"owner": "mac-claude", "session": "s-1"})
        payload = render_event_payload(row)
        self.assertIn("embeds", payload)
        self.assertIsInstance(payload["embeds"], list)
        self.assertEqual(len(payload["embeds"]), 1)
        embed = payload["embeds"][0]
        self.assertEqual(embed["title"], "🚀 任务已接收")
        self.assertEqual(embed["color"], GREEN)

    def test_progress_reported_has_embeds(self):
        from coordinate.policy import render_event_payload

        row = self._make_event_row("progress.reported", {"summary": "Did stuff"})
        payload = render_event_payload(row)
        self.assertIn("embeds", payload)
        self.assertEqual(payload["embeds"][0]["color"], YELLOW)

    def test_blocker_has_red_embed(self):
        from coordinate.policy import render_event_payload

        row = self._make_event_row("blocker.raised", {"reason": "stuck"})
        payload = render_event_payload(row)
        self.assertEqual(payload["embeds"][0]["color"], RED)

    def test_text_preserved_alongside_embed(self):
        from coordinate.policy import render_event_payload

        row = self._make_event_row("assignment.accepted", {"owner": "mac-claude"})
        payload = render_event_payload(row)
        self.assertIn("text", payload)
        self.assertIn("embeds", payload)
        # Machine-readable content is still there
        self.assertIn("[ACCEPT]", payload["text"])


class TestBusEmbedSupport(unittest.TestCase):
    """Verify bus implementations pass embeds through."""

    def test_webhook_bus_includes_embeds(self):
        from coordinate.bus import WebhookBus

        captured = {}

        def mock_post(url, headers, body):
            captured["body"] = body
            return {"id": "12345"}

        bus = WebhookBus(webhook_url="https://discord.test/webhook", http_post=mock_post)
        bus.send(
            destination="ch-1",
            payload={
                "text": "hello",
                "embeds": [{"title": "Test", "color": GREEN, "fields": []}],
            },
            message_key="mk-1",
        )
        self.assertIn("embeds", captured["body"])
        self.assertEqual(captured["body"]["embeds"][0]["title"], "Test")

    def test_webhook_bus_no_embeds_backward_compat(self):
        from coordinate.bus import WebhookBus

        captured = {}

        def mock_post(url, headers, body):
            captured["body"] = body
            return {"id": "12345"}

        bus = WebhookBus(webhook_url="https://discord.test/webhook", http_post=mock_post)
        bus.send(
            destination="ch-1",
            payload={"text": "hello"},
            message_key="mk-1",
        )
        self.assertNotIn("embeds", captured["body"])

    def test_discord_bus_includes_embeds(self):
        from coordinate.bus import DiscordBus

        captured = {}

        def mock_post(url, headers, body):
            captured["body"] = body
            return {"id": "12345"}

        bus = DiscordBus(token="fake-token", http_post=mock_post)
        bus.send(
            destination="ch-1",
            payload={
                "text": "hello",
                "embeds": [{"title": "Test", "color": GREEN, "fields": []}],
            },
            message_key="mk-1",
        )
        self.assertIn("embeds", captured["body"])

    def test_discord_bus_no_embeds_backward_compat(self):
        from coordinate.bus import DiscordBus

        captured = {}

        def mock_post(url, headers, body):
            captured["body"] = body
            return {"id": "12345"}

        bus = DiscordBus(token="fake-token", http_post=mock_post)
        bus.send(
            destination="ch-1",
            payload={"text": "hello"},
            message_key="mk-1",
        )
        self.assertNotIn("embeds", captured["body"])


class TestMentionScopePreserved(unittest.TestCase):
    """Handoff and lifecycle deliveries must NOT get embeds — protocol messages stay plain."""

    def make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(conn, workspace_id="ws-test", name="Test", path=".", harness_root=".")
        set_workspace_agent(conn, workspace_id="ws-test", agent_name="mac-claude", discord_user_id="111222333")
        return conn

    def _add_event(self, conn, event_type, payload_dict):
        return append_event(
            conn,
            workspace_id="ws-test",
            event_type=event_type,
            actor="operator",
            task_id="phase-1",
            payload=payload_dict,
            idempotency_key=f"test:{event_type}:{payload_dict.get('target_agent', 'x')}",
        )

    def test_handoff_delivery_no_embeds(self):
        from coordinate.policy import render_event_deliveries

        conn = self.make_conn()
        self._add_event(conn, "worker.handoff.prepared", {
            "target_agent": "mac-claude",
            "task_id": "phase-1",
            "bootstrap_path": "/tmp/boot.md",
        })
        event = conn.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 1").fetchone()
        results = render_event_deliveries(conn, event["id"], platform="discord_webhook", destination="ch-1")

        # Find the handoff delivery (it has mention_users)
        handoff_results = [r for r in results if r.payload and r.payload.get("mention_users")]
        self.assertTrue(len(handoff_results) >= 1, "Expected a handoff delivery")
        for hr in handoff_results:
            self.assertNotIn("embeds", hr.payload, "Handoff delivery should not have embeds")

    def test_lifecycle_delivery_no_embeds(self):
        from coordinate.policy import render_event_deliveries

        conn = self.make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="ws-test",
            task_id="phase-1",
            owner="mac-claude",
            phase="doing",
            branch=None,
            pr=None,
            payload={"status": "doing"},
        )
        self._add_event(conn, "closeout.requested", {
            "task_id": "phase-1",
        })
        event = conn.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 1").fetchone()
        results = render_event_deliveries(conn, event["id"], platform="discord_webhook", destination="ch-1")

        lifecycle_results = [r for r in results if r.payload and r.payload.get("mention_users")]
        self.assertTrue(len(lifecycle_results) >= 1, "Expected a lifecycle delivery")
        for lr in lifecycle_results:
            self.assertNotIn("embeds", lr.payload, "Lifecycle delivery should not have embeds")

    def test_handoff_delivery_mention_scope_narrow(self):
        from coordinate.policy import render_event_deliveries

        conn = self.make_conn()
        self._add_event(conn, "worker.handoff.prepared", {
            "target_agent": "mac-claude",
            "task_id": "phase-1",
            "bootstrap_path": "/tmp/boot.md",
        })
        event = conn.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 1").fetchone()
        results = render_event_deliveries(conn, event["id"], platform="discord_webhook", destination="ch-1")

        handoff_results = [r for r in results if r.payload and r.payload.get("mention_users")]
        for hr in handoff_results:
            mention_users = hr.payload["mention_users"]
            self.assertEqual(len(mention_users), 1, "Mention scope should be exactly the target agent")
            self.assertEqual(mention_users[0], "111222333")

    def test_main_delivery_has_embeds(self):
        from coordinate.policy import render_event_deliveries

        conn = self.make_conn()
        self._add_event(conn, "worker.handoff.prepared", {
            "target_agent": "mac-claude",
            "task_id": "phase-1",
            "bootstrap_path": "/tmp/boot.md",
        })
        event = conn.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 1").fetchone()
        results = render_event_deliveries(conn, event["id"], platform="discord_webhook", destination="ch-1")

        # First result is the main delivery
        main = results[0]
        self.assertIn("embeds", main.payload, "Main delivery should have embeds")


class TestAgentReportedRendering(unittest.TestCase):
    """Tests for agent.reported embed rendering and policy support."""

    def make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(conn, workspace_id="ws-test", name="Test", path=".", harness_root=".")
        return conn

    def _make_event_row(self, event_type, payload_dict=None, **overrides):
        conn = self.make_conn()
        append_event(
            conn,
            workspace_id="ws-test",
            event_type=event_type,
            actor=overrides.pop("actor", "mac-claude"),
            target=overrides.pop("target", None),
            task_id=overrides.pop("task_id", "phase-5.6"),
            payload=payload_dict or {},
            idempotency_key=f"test:{event_type}",
        )
        return conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT 1").fetchone()

    def test_agent_reported_done_embed(self):
        embed = render_embed(
            "agent.reported",
            _event(event_type="agent.reported", actor="mac-claude", task_id="phase-5.6"),
            _payload(action="done", owner="mac-claude", summary="Implemented feature X"),
        )
        self.assertIsNotNone(embed)
        self.assertEqual(embed["title"], "🎯 Agent 完成")
        self.assertEqual(embed["color"], GREEN)
        field_names = [f["name"] for f in embed["fields"]]
        self.assertIn("📌 任务", field_names)
        self.assertIn("🤖 执行者", field_names)
        self.assertIn("📝 摘要", field_names)

    def test_agent_reported_blocker_embed(self):
        embed = render_embed(
            "agent.reported",
            _event(event_type="agent.reported", actor="mac-claude", task_id="phase-5.6"),
            _payload(action="blocker", owner="mac-claude", reason="Need decision on X"),
        )
        self.assertIsNotNone(embed)
        self.assertEqual(embed["title"], "🛑 Agent 阻塞")
        self.assertEqual(embed["color"], RED)
        field_names = [f["name"] for f in embed["fields"]]
        self.assertIn("🧱 原因", field_names)

    def test_agent_reported_progress_embed(self):
        embed = render_embed(
            "agent.reported",
            _event(event_type="agent.reported", actor="mac-claude", task_id="phase-5.6"),
            _payload(action="progress", owner="mac-claude", summary="Working on part A"),
        )
        self.assertIsNotNone(embed)
        self.assertEqual(embed["title"], "🚧 Agent 进度")
        self.assertEqual(embed["color"], YELLOW)

    def test_agent_reported_accept_embed(self):
        embed = render_embed(
            "agent.reported",
            _event(event_type="agent.reported", actor="mac-claude", task_id="phase-5.6"),
            _payload(action="accept", owner="mac-claude"),
        )
        self.assertIsNotNone(embed)
        self.assertEqual(embed["title"], "🚀 Agent 已接收")
        self.assertEqual(embed["color"], BLUE)

    def test_agent_reported_payload_text_done(self):
        from coordinate.policy import render_event_payload

        row = self._make_event_row(
            "agent.reported",
            {"action": "done", "owner": "mac-claude", "summary": "All tests pass"},
        )
        payload = render_event_payload(row)
        self.assertIn("[DONE]", payload["text"])
        self.assertIn("All tests pass", payload["text"])
        self.assertIn("embeds", payload)

    def test_agent_reported_payload_text_blocker(self):
        from coordinate.policy import render_event_payload

        row = self._make_event_row(
            "agent.reported",
            {"action": "blocker", "owner": "mac-claude", "reason": "Cannot proceed"},
        )
        payload = render_event_payload(row)
        self.assertIn("[BLOCKER]", payload["text"])
        self.assertIn("Cannot proceed", payload["text"])

    def test_agent_reported_source_discord_accept_skipped(self):
        from coordinate.policy import render_event

        conn = self.make_conn()
        append_event(
            conn,
            workspace_id="ws-test",
            event_type="agent.reported",
            actor="mac-claude",
            task_id="phase-5.6",
            payload={"action": "accept", "owner": "mac-claude", "source": "discord"},
            idempotency_key="test:agent.reported:discord-src",
        )
        event = conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT 1").fetchone()
        result = render_event(conn, event["id"], platform="discord_webhook", destination="ch-1")
        self.assertFalse(result.supported)
        self.assertIn("accept/progress report is already visible", result.reason)

    def test_agent_reported_source_discord_done_rendered(self):
        from coordinate.policy import render_event

        conn = self.make_conn()
        append_event(
            conn,
            workspace_id="ws-test",
            event_type="agent.reported",
            actor="mac-claude",
            task_id="phase-5.6",
            payload={"action": "done", "owner": "mac-claude", "source": "discord"},
            idempotency_key="test:agent.reported:discord-done",
        )
        event = conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT 1").fetchone()
        result = render_event(conn, event["id"], platform="discord_webhook", destination="ch-1")
        self.assertTrue(result.supported)
        self.assertEqual(result.payload["visible_header"], "[DONE]")

    def test_agent_reported_source_non_discord_rendered(self):
        from coordinate.policy import render_event

        conn = self.make_conn()
        append_event(
            conn,
            workspace_id="ws-test",
            event_type="agent.reported",
            actor="mac-claude",
            task_id="phase-5.6",
            payload={"action": "done", "owner": "mac-claude", "source": "api"},
            idempotency_key="test:agent.reported:api-src",
        )
        event = conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT 1").fetchone()
        result = render_event(conn, event["id"], platform="discord_webhook", destination="ch-1")
        self.assertTrue(result.supported)
        self.assertIsNotNone(result.payload)

    def test_agent_reported_no_source_rendered(self):
        from coordinate.policy import render_event

        conn = self.make_conn()
        append_event(
            conn,
            workspace_id="ws-test",
            event_type="agent.reported",
            actor="mac-claude",
            task_id="phase-5.6",
            payload={"action": "done", "owner": "mac-claude"},
            idempotency_key="test:agent.reported:no-src",
        )
        event = conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT 1").fetchone()
        result = render_event(conn, event["id"], platform="discord_webhook", destination="ch-1")
        self.assertTrue(result.supported)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Phase 8.4 — Discord embed for new publish events
# ---------------------------------------------------------------------------


class Phase84DiscordEmbedTests(unittest.TestCase):
    def _embed(self, event_type, payload):
        from coordinate.discord_rendering import render_embed
        event = {
            "id": "evt-1", "event_type": event_type,
            "actor": "operator", "workspace_id": "demo", "task_id": "phase-8.4-x",
        }
        return render_embed(event_type, event, payload)

    def test_pr_created_embed(self):
        embed = self._embed("pr.created", {
            "task_id": "phase-8.4-x",
            "pr": "https://github.com/acme/repo/pull/10",
            "branch": "agents/mac-claude/x",
            "repo": "acme/repo",
            "head_ref": "acme:agents/mac-claude/x",
            "base": "main",
            "reported_commit": "0123456789abcdef0123456789abcdef01234567",
            "remote_sha": "0123456789abcdef0123456789abcdef01234567",
        })
        self.assertIsNotNone(embed)
        self.assertEqual(embed["color"], 0x57F287)  # GREEN
        names = {f["name"] for f in embed["fields"]}
        self.assertIn("🌿 分支", names)
        self.assertIn("🔗 PR", names)

    def test_push_required_embed(self):
        embed = self._embed("push.required", {
            "task_id": "phase-8.4-x",
            "repo": "acme/repo",
            "branch": "agents/mac-claude/x",
            "reported_commit": "0123456789abcdef0123456789abcdef01234567",
            "remote": "origin",
            "detail": "remote ref not found on GitHub",
        })
        self.assertIsNotNone(embed)
        self.assertEqual(embed["color"], 0xFEE75C)  # YELLOW

    def test_publish_blocked_embed_red(self):
        embed = self._embed("publish.blocked", {
            "task_id": "phase-8.4-x",
            "repo": "acme/repo",
            "branch": "agents/mac-claude/x",
            "reported_commit": "0123456789abcdef0123456789abcdef01234567",
            "remote_sha": "ffffffffffffffffffffffffffffffffffffffff",
            "reason": "sha_mismatch",
        })
        self.assertIsNotNone(embed)
        self.assertEqual(embed["color"], 0xED4245)  # RED
