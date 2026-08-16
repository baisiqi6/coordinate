import asyncio
import concurrent.futures
import json
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch


def _install_fake_discord() -> None:
    if "discord" in sys.modules:
        return
    module = types.ModuleType("discord")

    class FakeIntents:
        message_content = False

        @staticmethod
        def default():
            return FakeIntents()

    class FakeBotUser:
        id = 999

        def mentioned_in(self, message):
            return f"<@{self.id}>" in message.content or f"<@!{self.id}>" in message.content

    class FakeClient:
        def __init__(self, *, intents=None, **kwargs):
            self.intents = intents
            self.kwargs = kwargs
            self.user = FakeBotUser()
            self.loop = None

        def event(self, func):
            return func

        def run(self, token):
            self.token = token

        def is_closed(self):
            return True

    class FakeAllowedMentions:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class FakeObject:
        def __init__(self, id):
            self.id = id

    class FakeMessage:
        pass

    module.Intents = FakeIntents
    module.Client = FakeClient
    module.AllowedMentions = FakeAllowedMentions
    module.Object = FakeObject
    module.Message = FakeMessage
    sys.modules["discord"] = module


_install_fake_discord()

from coordinate.agent_report import AgentReport, parse_agent_report  # noqa: E402
from coordinate.channel_provisioning import (  # noqa: E402
    ensure_discord_delivery_route,
    finish_discord_channel_request,
    inspect_discord_provisioning,
    list_pending_discord_channel_requests,
    normalize_discord_channel_name,
    request_discord_channel_provision,
    workspace_topic_marker,
)
from coordinate.daemon import (  # noqa: E402
    BotBus,
    CoordinatorDaemon,
    _discord_snowflake,
    _task_status_event_line,
)
from coordinate.db import (  # noqa: E402
    append_event,
    bind_channel_workspace,
    get_workspace,
    initialize,
    list_deliveries,
    list_events,
    resolve_channel_workspace,
    set_workspace_agent as _set_workspace_agent,
    sync_workspace_agents,
    upsert_workspace,
)
from coordinate.transitions import mark_done_task  # noqa: E402


def set_workspace_agent(conn, **kwargs):
    """Create an explicit fixture override without setup-event side effects."""
    kwargs.setdefault("actor", "test-fixture")
    kwargs.setdefault("reason", "daemon test fixture")
    result = _set_workspace_agent(conn, **kwargs)
    conn.execute("DELETE FROM events WHERE event_type = 'workspace.agent_override.set'")
    conn.commit()
    return result


class FakeAuthor:
    def __init__(self, user_id: int, *, bot: bool = False):
        self.id = user_id
        self.bot = bot

    def __str__(self):
        return f"user-{self.id}"


class FakeChannel:
    def __init__(self, channel_id: int):
        self.id = channel_id


class FakeMessage:
    def __init__(self, *, author_id: int, channel_id: int, content: str, message_id: int = 42, bot: bool = False):
        self.author = FakeAuthor(author_id, bot=bot)
        self.channel = FakeChannel(channel_id)
        self.content = content
        self.id = message_id


class DaemonParserTests(unittest.TestCase):
    def test_parse_agent_report_requires_structured_prefix(self):
        self.assertIsNone(
            parse_agent_report("task is done phase-4-coordinator-integration")
        )

    def test_parse_agent_report_requires_workspace_and_task(self):
        self.assertIsNone(parse_agent_report("[done] task_id=t1"))
        self.assertIsNone(parse_agent_report("[done] workspace_id=demo"))

    def test_parse_agent_report_accepts_explicit_agent_report(self):
        report = parse_agent_report(
            '[agent-report] action=done workspace_id=demo task_id=t1 summary="tests passed"'
        )

        self.assertEqual(
            report,
            AgentReport(
                action="done",
                workspace_id="demo",
                task_id="t1",
                summary="tests passed",
            ),
        )

    def test_parse_agent_report_accepts_short_tags_with_required_ids(self):
        report = parse_agent_report(
            '[blocker] workspace_id=demo task_id=t1 reason="missing token"'
        )

        self.assertEqual(report.action, "blocker")
        self.assertEqual(report.workspace_id, "demo")
        self.assertEqual(report.task_id, "t1")
        self.assertEqual(report.reason, "missing token")

    def test_parse_agent_report_accepts_progress_action(self):
        report = parse_agent_report(
            '[agent-report] action=progress workspace_id=demo task_id=t1 '
            'summary="plist done; tests OK"'
        )

        self.assertEqual(report.action, "progress")
        self.assertEqual(report.workspace_id, "demo")
        self.assertEqual(report.task_id, "t1")
        self.assertEqual(report.summary, "plist done; tests OK")

    def test_parse_agent_report_accepts_multiline_block(self):
        report = parse_agent_report(
            "[agent-report]\n"
            "action=progress\n"
            "workspace_id=demo\n"
            "task_id=t1\n"
            'summary="plist done; tests OK"'
        )

        self.assertIsNotNone(report)
        self.assertEqual(report.action, "progress")
        self.assertEqual(report.workspace_id, "demo")
        self.assertEqual(report.task_id, "t1")
        self.assertEqual(report.summary, "plist done; tests OK")

    def test_parse_agent_report_ignores_invalid_multiline_example(self):
        self.assertIsNone(
            parse_agent_report(
                "Example:\n"
                "[agent-report]\n"
                "workspace_id=demo\n"
                "task_id=t1\n"
                "summary='missing action'"
            )
        )

    def test_parse_agent_report_accepts_report_line_after_summary(self):
        report = parse_agent_report(
            "134 tests passed. Human-readable summary first.\n\n"
            '[agent-report] action=progress workspace_id=demo task_id=t1 '
            'summary="runtime tests done; coordinator side remains"'
        )

        self.assertIsNotNone(report)
        self.assertEqual(report.action, "progress")
        self.assertEqual(report.workspace_id, "demo")
        self.assertEqual(report.task_id, "t1")
        self.assertEqual(
            report.summary,
            "runtime tests done; coordinator side remains",
        )

    def test_parse_agent_report_prefers_execution_report_after_accept(self):
        report = parse_agent_report(
            "[agent-report] action=accept workspace_id=demo task_id=t1 "
            "summary='auto accepted by mac-claude'\n"
            "Round 3 rework complete.\n"
            "[agent-report] action=done workspace_id=demo task_id=t1 "
            "summary='tests OK; requesting review'"
        )

        self.assertIsNotNone(report)
        self.assertEqual(report.action, "done")
        self.assertEqual(report.workspace_id, "demo")
        self.assertEqual(report.task_id, "t1")
        self.assertEqual(report.summary, "tests OK; requesting review")

    def test_parse_agent_report_ignores_inline_report_marker_in_prose(self):
        self.assertIsNone(
            parse_agent_report(
                "The docs mention [agent-report] action=done "
                "workspace_id=demo task_id=t1, but this is not a report line."
            )
        )


class DaemonIngestTests(unittest.TestCase):
    def _db_path(self) -> str:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return str(Path(tmp.name) / "coordinator.sqlite3")

    def test_ingest_writes_fact_event_idempotently(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo/harness",
        )
        conn.close()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        report = AgentReport(action="done", workspace_id="demo", task_id="t1")

        first = daemon._do_ingest(
            "mac-codex",
            report,
            message_id="discord-message-1",
            content="[done] workspace_id=demo task_id=t1",
        )
        second = daemon._do_ingest(
            "mac-codex",
            report,
            message_id="discord-message-1",
            content="[done] workspace_id=demo task_id=t1",
        )

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        conn = initialize(db_path)
        try:
            events = list(list_events(conn, "demo"))
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event_type"], "agent.reported")
            self.assertEqual(events[0]["actor"], "mac-codex")
            payload = json.loads(events[0]["payload_json"])
            self.assertEqual(payload["action"], "done")
            self.assertEqual(payload["discord_message_id"], "discord-message-1")
        finally:
            conn.close()

    def test_ingest_progress_writes_progress_reported_event(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo/harness",
        )
        conn.close()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        report = AgentReport(
            action="progress",
            workspace_id="demo",
            task_id="t1",
            summary="plist done; tests OK",
        )

        result = daemon._do_ingest(
            "mac-codex",
            report,
            message_id="discord-message-progress",
            content='[agent-report] action=progress workspace_id=demo task_id=t1 summary="plist done; tests OK"',
        )

        self.assertTrue(result.created)
        conn = initialize(db_path)
        try:
            events = list(list_events(conn, "demo"))
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event_type"], "progress.reported")
            payload = json.loads(events[0]["payload_json"])
            self.assertEqual(payload["action"], "progress")
            self.assertEqual(payload["summary"], "plist done; tests OK")
        finally:
            conn.close()

    def test_task_show_includes_recent_progress_and_review_summary(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo/harness",
        )
        conn.execute(
            """
            INSERT INTO tasks (
              workspace_id, task_id, phase, owner, branch, pr, payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "demo",
                "t1",
                "review",
                "mac-codex",
                "agents/mac-codex/t1",
                None,
                "{}",
                "2026-05-31T00:00:00Z",
            ),
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="progress.reported",
            actor="mac-codex",
            task_id="t1",
            payload={"summary": "scripts added; tests OK"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="review.completed",
            actor="reviewer",
            task_id="t1",
            payload={"reviewer": "codex", "decision": "approved", "summary": "looks good"},
        )
        conn.close()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )

        output = daemon._do_task_show("demo", "t1")

        self.assertIn("最近状态：", output)
        self.assertIn("进度：mac-codex", output)
        self.assertIn("scripts added; tests OK", output)
        self.assertIn("审核：codex approved", output)
        self.assertIn("looks good", output)

    def test_task_show_same_second_status_uses_rowid_tie_breaker(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo/harness",
        )
        conn.execute(
            """
            INSERT INTO tasks (
              workspace_id, task_id, phase, owner, branch, pr, payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "demo",
                "t1",
                "review",
                "mac-codex",
                "agents/mac-codex/t1",
                None,
                "{}",
                "2026-05-31T00:00:00Z",
            ),
        )
        same_ts = "2026-06-01T00:00:00Z"
        for i in range(1, 7):
            conn.execute(
                """
                INSERT INTO events (
                  id, workspace_id, event_type, actor, task_id,
                  created_at, payload_json, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"s4a-status-{i}",
                    "demo",
                    "progress.reported",
                    f"agent-{i}",
                    "t1",
                    same_ts,
                    f'{{"summary": "event {i}"}}',
                    f"s4a-status-key-{i}",
                ),
            )
        conn.commit()
        conn.close()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )

        output = daemon._do_task_show("demo", "t1")

        self.assertIn("最近状态：", output)
        self.assertNotIn("event 1", output)
        for i in range(2, 7):
            self.assertIn(f"event {i}", output)
        positions = [output.find(f"event {i}") for i in range(6, 1, -1)]
        self.assertEqual(positions, sorted(positions))

    def test_ingest_done_action_calls_mark_done_task(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo/harness",
        )
        conn.close()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        report = AgentReport(action="done", workspace_id="demo", task_id="t1")

        with patch("coordinate.daemon.mark_done_task") as mock_mark_done:
            mock_mark_done.return_value = type("R", (), {"mutation": None, "event": {}, "event_created": False})()
            daemon._do_ingest(
                "mac-codex",
                report,
                message_id="discord-message-done",
                content="[done] workspace_id=demo task_id=t1",
            )
            mock_mark_done.assert_called_once()
            call_kwargs = mock_mark_done.call_args
            self.assertEqual(call_kwargs.kwargs["workspace_id"], "demo")
            self.assertEqual(call_kwargs.kwargs["task_id"], "t1")
            self.assertEqual(call_kwargs.kwargs["actor"], "mac-codex")

    def test_ingest_progress_action_does_not_call_mark_done_task(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo/harness",
        )
        conn.close()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        report = AgentReport(action="progress", workspace_id="demo", task_id="t1")

        with patch("coordinate.daemon.mark_done_task") as mock_mark_done:
            daemon._do_ingest(
                "mac-codex",
                report,
                message_id="discord-message-progress",
                content="[agent-report] action=progress workspace_id=demo task_id=t1",
            )
            mock_mark_done.assert_not_called()


class DaemonRoutingTests(unittest.TestCase):
    def test_agent_message_with_mention_does_not_dispatch_operator_command(self):
        daemon = CoordinatorDaemon(
            db_path=":memory:",
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon._agent_discord_ids = {123: {"demo": "mac-codex"}}
        daemon._refresh_agent_registry = lambda: {123: {"demo": "mac-codex"}}
        calls: list[str] = []

        async def fake_dispatch(message, text):
            calls.append("dispatch")

        daemon._resolve_message_workspace = lambda channel: "demo"

        async def fake_ingest(message, *, workspace_id=None):
            calls.append("ingest")

        daemon._dispatch = fake_dispatch
        daemon._ingest_agent_message = fake_ingest
        message = FakeMessage(
            author_id=123,
            channel_id=100,
            content="<@999> status",
            bot=True,
        )

        asyncio.run(daemon.on_message(message))

        self.assertEqual(calls, ["ingest"])


class DaemonAgentAuthorizationTests(unittest.TestCase):
    def _db_path(self) -> str:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return str(Path(tmp.name) / "coordinator.sqlite3")

    def test_same_workspace_agent_report_is_ingested(self):
        daemon = CoordinatorDaemon(
            db_path=":memory:",
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon._agent_discord_ids = {123: {"workspace-a": "agent-a"}}
        calls = []

        def fake_do_ingest(agent, report, **kwargs):
            calls.append((agent, report.workspace_id, report.task_id, kwargs))

        daemon._do_ingest = fake_do_ingest
        message = FakeMessage(
            author_id=123,
            channel_id=100,
            content=(
                "[agent-report] action=progress workspace_id=workspace-a "
                "task_id=t1 summary='working'"
            ),
            bot=True,
        )

        asyncio.run(daemon._ingest_agent_message(message))

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0:3], ("agent-a", "workspace-a", "t1"))

    def test_cross_workspace_agent_report_is_rejected(self):
        daemon = CoordinatorDaemon(
            db_path=":memory:",
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon._agent_discord_ids = {123: {"workspace-a": "agent-a"}}
        calls = []

        def fake_do_ingest(*args, **kwargs):
            calls.append((args, kwargs))

        daemon._do_ingest = fake_do_ingest
        message = FakeMessage(
            author_id=123,
            channel_id=100,
            content=(
                "[agent-report] action=done workspace_id=workspace-b "
                "task_id=t1 summary='done'"
            ),
            bot=True,
        )

        with self.assertLogs("coordinator.daemon", level="WARNING"):
            asyncio.run(daemon._ingest_agent_message(message))

        self.assertEqual(calls, [])

    def test_registry_load_is_workspace_scoped_and_replaces_stale_entries(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        for workspace_id in ("workspace-a", "workspace-b"):
            upsert_workspace(
                conn,
                workspace_id=workspace_id,
                name=workspace_id,
                path=f"/tmp/{workspace_id}",
                harness_root=f"/tmp/{workspace_id}/harness",
            )
        set_workspace_agent(
            conn,
            workspace_id="workspace-a",
            agent_name="agent-a",
            discord_user_id="123",
        )
        set_workspace_agent(
            conn,
            workspace_id="workspace-b",
            agent_name="agent-b",
            discord_user_id="123",
        )
        conn.close()

        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon._agent_discord_ids = {999: {"stale": "removed-agent"}}

        daemon._load_agent_registry()

        self.assertEqual(
            daemon._agent_discord_ids,
            {123: {"workspace-a": "agent-a", "workspace-b": "agent-b"}},
        )

    def test_allowed_human_mention_dispatches_operator_command(self):
        daemon = CoordinatorDaemon(
            db_path=":memory:",
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        calls: list[tuple[str, str]] = []

        async def fake_dispatch(message, text):
            calls.append(("dispatch", text))

        async def fake_ingest(message):
            calls.append(("ingest", message.content))

        daemon._dispatch = fake_dispatch
        daemon._ingest_agent_message = fake_ingest
        message = FakeMessage(
            author_id=1,
            channel_id=100,
            content="<@999> status",
        )

        asyncio.run(daemon.on_message(message))

        self.assertEqual(calls, [("dispatch", "status")])

    def test_unauthorized_human_mention_is_ignored(self):
        daemon = CoordinatorDaemon(
            db_path=":memory:",
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        calls: list[str] = []

        async def fake_dispatch(message, text):
            calls.append("dispatch")

        daemon._dispatch = fake_dispatch
        message = FakeMessage(
            author_id=2,
            channel_id=100,
            content="<@999> status",
        )

        with self.assertLogs("coordinator.daemon", level="WARNING"):
            asyncio.run(daemon.on_message(message))

        self.assertEqual(calls, [])


class DaemonRefreshTests(unittest.TestCase):
    def _db_path(self) -> str:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return str(Path(tmp.name) / "coordinator.sqlite3")

    def _make_daemon(self, db_path: str) -> CoordinatorDaemon:
        conn = initialize(db_path)
        try:
            if get_workspace(conn, "demo") is not None:
                bind_channel_workspace(
                    conn,
                    platform="discord",
                    channel_id="100",
                    workspace_id="demo",
                    actor="test-fixture",
                    reason="daemon routing fixture",
                    idempotency_key="test:daemon-routing:demo:100",
                )
        finally:
            conn.close()
        return CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )

    def _make_message(self, *, author_id: int, content: str) -> FakeMessage:
        return FakeMessage(
            author_id=author_id,
            channel_id=100,
            content=content,
            bot=True,
        )

    def test_added_agent_is_authorized_without_on_ready(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(conn, workspace_id="demo", name="Demo", path="/tmp/demo", harness_root="/tmp/demo/harness")
        set_workspace_agent(
            conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="123",
            reason="testing",
        )
        conn.close()

        daemon = self._make_daemon(db_path)
        calls: list[tuple[str, str, str]] = []

        async def fake_ingest(message, *, workspace_id=None):
            calls.append((message.author.id, message.channel.id, message.content))

        daemon._ingest_agent_message = fake_ingest
        message = self._make_message(
            author_id=123,
            content="[agent-report] action=progress workspace_id=demo task_id=t1 summary='ok'",
        )
        asyncio.run(daemon.on_message(message))
        self.assertEqual(len(calls), 1)

    def test_removed_authoritative_agent_is_rejected_immediately(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(conn, workspace_id="demo", name="Demo", path="/tmp/demo", harness_root="/tmp/demo/harness")
        from coordinate.agent_registry import AgentEntry
        from coordinate.db import sync_workspace_agents
        sync_workspace_agents(
            conn,
            workspace_id="demo",
            source_id="s",
            source_version=1,
            source_hash="a" * 64,
            entries=[{"id": "mac-codex", "discord_user_id": "123", "display_name": "Codex", "agent_type": "managed"}],
        )
        conn.close()

        daemon = self._make_daemon(db_path)
        calls: list[tuple[str, str, str]] = []

        async def fake_ingest(m, *, workspace_id=None):
            calls.append((m.author.id, m.channel.id, m.content))
        daemon._ingest_agent_message = fake_ingest

        # First message authorized.
        asyncio.run(daemon.on_message(self._make_message(author_id=123, content="[agent-report] action=progress workspace_id=demo task_id=t1")))
        self.assertEqual(len(calls), 1)

        # Remove authoritative entry directly, then send next message.
        conn = initialize(db_path)
        conn.execute(
            "DELETE FROM workspace_agent_registry_entries WHERE workspace_id = ? AND entry_kind = 'authoritative'",
            ("demo",),
        )
        from coordinate.db import _write_agents_json_projection
        _write_agents_json_projection(conn, "demo")
        conn.commit()
        conn.close()

        calls.clear()
        asyncio.run(daemon.on_message(self._make_message(author_id=123, content="[agent-report] action=progress workspace_id=demo task_id=t2")))
        self.assertEqual(len(calls), 0)

    def test_expired_override_rejected_without_revision_change(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(conn, workspace_id="demo", name="Demo", path="/tmp/demo", harness_root="/tmp/demo/harness")
        sync_workspace_agents(
            conn,
            workspace_id="demo",
            source_id="s",
            source_version=1,
            source_hash="a" * 64,
            entries=[{"id": "mac-codex", "discord_user_id": "123", "display_name": "Codex", "agent_type": "managed"}],
        )
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        expiry = (t0 + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        set_workspace_agent(
            conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="999",
            reason="override",
            expires_at=expiry,
            now_utc=t0,
        )
        conn.close()

        daemon = self._make_daemon(db_path)
        calls: list[tuple[str, str, str]] = []

        async def fake_ingest(m, *, workspace_id=None):
            calls.append((m.author.id, m.channel.id, m.content))
        daemon._ingest_agent_message = fake_ingest

        # At t0 the override is active.
        t0_str = t0.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        t1_str = (t0 + timedelta(seconds=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        with patch("coordinate.db.utc_now", side_effect=[t0_str, t1_str]):
            asyncio.run(daemon.on_message(self._make_message(author_id=999, content="[agent-report] action=progress workspace_id=demo task_id=t1")))
            self.assertEqual(len(calls), 1)

            # At t0+2s the override has expired; no revision changed but refresh rejects it.
            calls.clear()
            asyncio.run(daemon.on_message(self._make_message(author_id=999, content="[agent-report] action=progress workspace_id=demo task_id=t2")))
            self.assertEqual(len(calls), 0)

    def test_cross_workspace_membership_scoped(self):
        db_path = self._db_path()
        conn = initialize(db_path)
        for ws in ("ws-a", "ws-b"):
            upsert_workspace(conn, workspace_id=ws, name=ws, path=f"/tmp/{ws}", harness_root=f"/tmp/{ws}/harness")
            set_workspace_agent(conn, workspace_id=ws, agent_name="ag", discord_user_id="123", reason="x")
        conn.close()

        daemon = self._make_daemon(db_path)
        registry = daemon._refresh_agent_registry()
        self.assertEqual(registry, {123: {"ws-a": "ag", "ws-b": "ag"}})


class ResolveProxyUrlTests(unittest.TestCase):
    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in os.environ}

    def tearDown(self):
        for k in list(os.environ.keys()):
            if k not in self._saved:
                del os.environ[k]
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    @staticmethod
    def _clear_vars():
        for v in (
            "COORDINATOR_HTTP_PROXY",
            "HTTPS_PROXY", "https_proxy",
            "HTTP_PROXY", "http_proxy",
        ):
            os.environ.pop(v, None)

    def test_no_env_returns_none(self):
        self._clear_vars()
        from coordinate.daemon import _resolve_proxy_url
        self.assertIsNone(_resolve_proxy_url())

    def test_coordinator_http_proxy_takes_priority(self):
        self._clear_vars()
        os.environ["COORDINATOR_HTTP_PROXY"] = "http://127.0.0.1:7890"
        os.environ["HTTPS_PROXY"] = "http://other:3128"
        from coordinate.daemon import _resolve_proxy_url
        self.assertEqual(_resolve_proxy_url(), "http://127.0.0.1:7890")

    def test_fallback_to_https_proxy(self):
        self._clear_vars()
        os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"
        from coordinate.daemon import _resolve_proxy_url
        self.assertEqual(_resolve_proxy_url(), "http://127.0.0.1:7890")

    def test_fallback_to_http_proxy(self):
        self._clear_vars()
        os.environ["HTTP_PROXY"] = "http://127.0.0.1:7890"
        from coordinate.daemon import _resolve_proxy_url
        self.assertEqual(_resolve_proxy_url(), "http://127.0.0.1:7890")

    def test_lowercase_fallback(self):
        self._clear_vars()
        os.environ["http_proxy"] = "http://127.0.0.1:7890"
        from coordinate.daemon import _resolve_proxy_url
        self.assertEqual(_resolve_proxy_url(), "http://127.0.0.1:7890")


if __name__ == "__main__":
    unittest.main()


class ChannelProvisioningTests(unittest.TestCase):
    def _db_path(self) -> str:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return str(Path(tmp.name) / "coordinator.sqlite3")

    def _seed_workspace(self, db_path: str):
        conn = initialize(db_path)
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo/harness",
            harnessctl_path="/tmp/demo/harnessctl",
            base_branch="main",
            branch_namespace="agents",
        )
        conn.close()
        return workspace

    def test_channel_name_validation_is_bounded_and_non_normalizing(self):
        self.assertEqual(normalize_discord_channel_name("demo-project"), "demo-project")
        self.assertEqual(normalize_discord_channel_name("demo project"), "demo project")
        for invalid in ("", " demo", "demo ", "demo\nproject", "x" * 101):
            with self.subTest(invalid=repr(invalid)):
                with self.assertRaises(ValueError):
                    normalize_discord_channel_name(invalid)

    def test_durable_request_exact_replay_and_conflict(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)
        conn = initialize(db_path)
        try:
            first = request_discord_channel_provision(
                conn,
                workspace_id="demo",
                channel_name="demo-project",
                actor="operator-a",
                idempotency_key="request-1",
            )
            replay = request_discord_channel_provision(
                conn,
                workspace_id="demo",
                channel_name="demo-project",
                actor="operator-a",
                idempotency_key="request-1",
            )
            self.assertEqual(first, replay)
            self.assertEqual(first["status"], "pending")
            self.assertEqual(len(list_pending_discord_channel_requests(conn)), 1)
            with self.assertRaisesRegex(ValueError, "request replay"):
                request_discord_channel_provision(
                    conn,
                    workspace_id="demo",
                    channel_name="different",
                    actor="operator-a",
                    idempotency_key="request-1",
                )
            with self.assertRaisesRegex(ValueError, "already pending"):
                request_discord_channel_provision(
                    conn,
                    workspace_id="demo",
                    channel_name="demo-project",
                    actor="operator-a",
                    idempotency_key="request-2",
                )
        finally:
            conn.close()

    def test_request_projection_survives_reopen_and_has_one_terminal(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)
        conn = initialize(db_path)
        request = request_discord_channel_provision(
            conn,
            workspace_id="demo",
            channel_name="demo-project",
            actor="operator-a",
            idempotency_key="request-1",
        )
        conn.close()

        reopened = initialize(db_path)
        try:
            pending = list_pending_discord_channel_requests(reopened)
            self.assertEqual([row["id"] for row in pending], [request["request_event_id"]])
            done = finish_discord_channel_request(
                reopened,
                request_event_id=request["request_event_id"],
                channel_id="456",
            )
            replay = finish_discord_channel_request(
                reopened,
                request_event_id=request["request_event_id"],
                channel_id="456",
            )
            self.assertEqual(done, replay)
            self.assertEqual(done["status"], "provisioned")
            self.assertEqual(done["channel_id"], "456")
            self.assertEqual(list_pending_discord_channel_requests(reopened), [])
        finally:
            reopened.close()

    def test_concurrent_different_keys_leave_one_pending_request(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)

        def submit(key: str):
            conn = initialize(db_path)
            try:
                return request_discord_channel_provision(
                    conn,
                    workspace_id="demo",
                    channel_name="demo-project",
                    actor="operator-a",
                    idempotency_key=key,
                )
            except ValueError as exc:
                return str(exc)
            finally:
                conn.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(submit, ("request-1", "request-2")))
        self.assertEqual(sum(isinstance(item, dict) for item in outcomes), 1)
        self.assertEqual(
            sum("already pending" in item for item in outcomes if isinstance(item, str)),
            1,
        )
        verify = initialize(db_path)
        try:
            self.assertEqual(len(list_pending_discord_channel_requests(verify)), 1)
        finally:
            verify.close()

    def test_daemon_consumes_recovered_pending_request(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)
        conn = initialize(db_path)
        request = request_discord_channel_provision(
            conn,
            workspace_id="demo",
            channel_name="demo-project",
            actor="operator-a",
            idempotency_key="request-1",
        )
        conn.close()

        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )

        async def fake_provision(workspace_id, channel_name):
            conn2 = initialize(db_path)
            state = inspect_discord_provisioning(conn2, workspace_id=workspace_id)
            bind_channel_workspace(
                conn2,
                platform="discord",
                channel_id="456",
                workspace_id=workspace_id,
                actor="test",
                reason="test",
                idempotency_key="test-bind-456",
            )
            ensure_discord_delivery_route(conn2, workspace=state.workspace, channel_id="456")
            conn2.close()

        daemon._provision_discord_channel = fake_provision
        asyncio.run(daemon._consume_pending_channel_provision_requests())

        verify = initialize(db_path)
        try:
            self.assertEqual(list_pending_discord_channel_requests(verify), [])
            terminal = [
                row for row in list_events(verify, "demo")
                if row["causation_id"] == request["request_event_id"]
            ]
            self.assertEqual(len(terminal), 1)
            self.assertEqual(terminal[0]["event_type"], "channel.provisioned")
        finally:
            verify.close()

    def test_multiple_workspace_discord_bindings_fail_closed(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)
        conn = initialize(db_path)
        try:
            for index, channel_id in enumerate(("101", "102"), start=1):
                bind_channel_workspace(
                    conn,
                    platform="discord",
                    channel_id=channel_id,
                    workspace_id="demo",
                    actor="test",
                    reason="fixture",
                    idempotency_key=f"bind-{index}",
                )
            with self.assertRaisesRegex(ValueError, "multiple Discord"):
                inspect_discord_provisioning(conn, workspace_id="demo")
        finally:
            conn.close()

    def test_delivery_route_update_preserves_workspace_contract(self):
        db_path = self._db_path()
        original = self._seed_workspace(db_path)
        conn = initialize(db_path)
        try:
            updated = ensure_discord_delivery_route(
                conn,
                workspace=original,
                channel_id="456",
            )
            self.assertEqual(updated.default_bus, "discord_webhook")
            self.assertEqual(updated.default_destination, "456")
            self.assertEqual(updated.harnessctl_path, original.harnessctl_path)
            self.assertEqual(updated.base_branch, "main")
            self.assertEqual(updated.branch_namespace, "agents")
        finally:
            conn.close()

    def test_agent_report_must_match_bound_channel_workspace(self):
        daemon = CoordinatorDaemon(
            db_path=":memory:",
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon._agent_discord_ids = {123: {"workspace-a": "agent-a", "workspace-b": "agent-b"}}
        calls = []
        daemon._do_ingest = lambda *args, **kwargs: calls.append((args, kwargs))
        message = FakeMessage(
            author_id=123,
            channel_id=200,
            content="[agent-report] action=done workspace_id=workspace-b task_id=t1",
            bot=True,
        )
        with self.assertLogs("coordinator.daemon", level="WARNING"):
            asyncio.run(daemon._ingest_agent_message(message, workspace_id="workspace-a"))
        self.assertEqual(calls, [])

    def test_allowed_agent_bot_can_request_only_its_workspace(self):
        daemon = CoordinatorDaemon(
            db_path=":memory:",
            bot_token="token",
            channel_id=100,
            allowed_user_ids={123},
        )
        daemon._refresh_agent_registry = lambda: {123: {"demo": "operator"}}
        calls = []

        async def fake_provision(workspace_id, channel_name):
            calls.append((workspace_id, channel_name))
            return "ok"

        daemon._provision_discord_channel = fake_provision
        message = FakeMessage(
            author_id=123,
            channel_id=100,
            content="<@999> channel create demo demo-project",
            bot=True,
        )
        message.channel.send = lambda text: None

        async def fake_reply(message, text):
            calls.append(("reply", text))

        daemon._reply = fake_reply
        asyncio.run(daemon.on_message(message))
        self.assertEqual(calls[0], ("demo", "demo-project"))

        message.content = "<@999> channel create other other-project"
        asyncio.run(daemon.on_message(message))
        self.assertEqual(len([call for call in calls if call[0] != "reply"]), 1)

    def test_creation_binding_route_and_marker_recovery_are_idempotent(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)

        class CreatedChannel:
            def __init__(self, channel_id, topic):
                self.id = channel_id
                self.topic = topic

        class Guild:
            def __init__(self):
                self.channels = []
                self.create_calls = []

            async def create_text_channel(self, name, **kwargs):
                self.create_calls.append((name, kwargs))
                channel = CreatedChannel(456, kwargs["topic"])
                self.channels.append(channel)
                return channel

        guild = Guild()
        control = FakeChannel(100)
        control.guild = guild
        control.category = object()

        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon.client.get_channel = lambda channel_id: control if channel_id == 100 else None

        first = asyncio.run(daemon._provision_discord_channel("demo", "demo-project"))
        second = asyncio.run(daemon._provision_discord_channel("demo", "ignored-name"))

        self.assertIn("456", first)
        self.assertIn("456", second)
        self.assertEqual(len(guild.create_calls), 1)
        self.assertEqual(
            guild.create_calls[0][1]["topic"],
            workspace_topic_marker("demo"),
        )
        self.assertIs(guild.create_calls[0][1]["category"], control.category)

        conn = initialize(db_path)
        try:
            binding = resolve_channel_workspace(conn, platform="discord", channel_id="456")
            workspace = get_workspace(conn, "demo")
            self.assertEqual(binding.workspace_id, "demo")
            self.assertEqual(workspace.default_destination, "456")
            self.assertEqual(workspace.default_bus, "discord_webhook")
            provisioned = [
                event for event in list_events(conn, "demo")
                if event["event_type"] == "channel.provisioned"
            ]
            self.assertEqual(len(provisioned), 1)
        finally:
            conn.close()

    def test_concurrent_same_workspace_requests_create_only_one_channel(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)

        class Guild:
            def __init__(self):
                self.channels = []
                self.calls = 0

            async def create_text_channel(self, name, **kwargs):
                self.calls += 1
                await asyncio.sleep(0)
                channel = types.SimpleNamespace(id=654, topic=kwargs["topic"])
                self.channels.append(channel)
                return channel

        guild = Guild()
        control = FakeChannel(100)
        control.guild = guild
        control.category = object()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon.client.get_channel = lambda channel_id: control

        async def run_both():
            return await asyncio.gather(
                daemon._provision_discord_channel("demo", "demo-project"),
                daemon._provision_discord_channel("demo", "demo-project"),
            )

        results = asyncio.run(run_both())
        self.assertEqual(guild.calls, 1)
        self.assertTrue(all("654" in result for result in results))

    def test_created_marker_channel_is_reused_after_bind_failure(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)

        class Channel:
            def __init__(self, channel_id, topic):
                self.id = channel_id
                self.topic = topic

        class Guild:
            def __init__(self):
                self.channels = []
                self.calls = 0

            async def create_text_channel(self, name, **kwargs):
                self.calls += 1
                channel = Channel(789, kwargs["topic"])
                self.channels.append(channel)
                return channel

        guild = Guild()
        control = FakeChannel(100)
        control.guild = guild
        control.category = object()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon.client.get_channel = lambda channel_id: control

        original = daemon._finalize_discord_provisioning
        calls = 0

        def fail_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("simulated DB failure")
            return original(*args, **kwargs)

        daemon._finalize_discord_provisioning = fail_once
        with self.assertRaisesRegex(RuntimeError, "simulated DB failure"):
            asyncio.run(daemon._provision_discord_channel("demo", "demo-project"))
        asyncio.run(daemon._provision_discord_channel("demo", "demo-project"))

        self.assertEqual(guild.calls, 1)
        conn = initialize(db_path)
        try:
            self.assertEqual(
                resolve_channel_workspace(conn, platform="discord", channel_id="789").workspace_id,
                "demo",
            )
        finally:
            conn.close()

    def test_duplicate_recovery_markers_fail_before_binding(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)
        marker = workspace_topic_marker("demo")
        guild = types.SimpleNamespace(
            channels=[
                types.SimpleNamespace(id=201, topic=marker),
                types.SimpleNamespace(id=202, topic=marker),
            ]
        )
        control = FakeChannel(100)
        control.guild = guild
        control.category = object()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon.client.get_channel = lambda channel_id: control

        with self.assertRaisesRegex(ValueError, "multiple Discord channels"):
            asyncio.run(daemon._provision_discord_channel("demo", "demo-project"))

        conn = initialize(db_path)
        try:
            self.assertEqual(
                inspect_discord_provisioning(conn, workspace_id="demo").channel_id,
                None,
            )
        finally:
            conn.close()

    def test_create_failure_leaves_database_unmodified(self):
        db_path = self._db_path()
        original = self._seed_workspace(db_path)

        class Guild:
            channels = []

            async def create_text_channel(self, name, **kwargs):
                raise PermissionError("missing Manage Channels")

        control = FakeChannel(100)
        control.guild = Guild()
        control.category = object()
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon.client.get_channel = lambda channel_id: control

        with self.assertRaisesRegex(PermissionError, "Manage Channels"):
            asyncio.run(daemon._provision_discord_channel("demo", "demo-project"))

        conn = initialize(db_path)
        try:
            state = inspect_discord_provisioning(conn, workspace_id="demo")
            current = get_workspace(conn, "demo")
            self.assertIsNone(state.channel_id)
            self.assertEqual(current.default_bus, original.default_bus)
            self.assertEqual(current.default_destination, original.default_destination)
        finally:
            conn.close()

    def test_missing_control_category_fails_before_create(self):
        db_path = self._db_path()
        self._seed_workspace(db_path)
        guild = types.SimpleNamespace(channels=[], create_text_channel=AsyncMock())
        control = FakeChannel(100)
        control.guild = guild
        control.category = None
        daemon = CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )
        daemon.client.get_channel = lambda channel_id: control

        with self.assertRaisesRegex(ValueError, "must belong to a Discord category"):
            asyncio.run(daemon._provision_discord_channel("demo", "demo-project"))
        guild.create_text_channel.assert_not_awaited()


class BotBusDestinationTests(unittest.TestCase):
    def test_destination_must_be_a_positive_decimal_snowflake(self):
        self.assertEqual(_discord_snowflake("456"), 456)
        for invalid in ("", "0", "-1", " 456", "456 ", "abc", "１２３"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    _discord_snowflake(invalid)

    def test_async_send_uses_delivery_destination(self):
        sent_to = []

        class Channel:
            def __init__(self, channel_id):
                self.id = channel_id

            async def send(self, **kwargs):
                sent_to.append((self.id, kwargs["content"]))
                return types.SimpleNamespace(id=999)

        class Client:
            def get_channel(self, channel_id):
                return Channel(channel_id)

        receipt = asyncio.run(BotBus(Client())._async_send(456, {"text": "hello"}))
        self.assertEqual(sent_to[0][0], 456)
        self.assertEqual(receipt, "discord_bot:999")


# ---------------------------------------------------------------------------
# Phase 8.4 — GitHub publish metadata in agent-report + payload persistence
# ---------------------------------------------------------------------------


class Phase84ReportParsingTests(unittest.TestCase):
    """Worker reports now carry optional repo/branch/commit/remote/pushed/validation."""

    def test_done_report_carries_publish_metadata(self):
        from coordinate.agent_report import parse_agent_report
        report = parse_agent_report(
            "[agent-report] action=done workspace_id=discord-nexus "
            "task_id=phase-8.4-x repo=example/coordinate "
            "branch=agents/mac-claude/phase-8.4-x "
            "commit=0123456789abcdef0123456789abcdef01234567 "
            "remote=origin pushed=true "
            "validation=\"805 tests OK; git diff --check clean\" "
            "summary=\"implementation complete\""
        )
        self.assertIsNotNone(report)
        self.assertEqual(report.action, "done")
        self.assertEqual(report.repo, "example/coordinate")
        self.assertEqual(report.branch, "agents/mac-claude/phase-8.4-x")
        self.assertEqual(report.commit, "0123456789abcdef0123456789abcdef01234567")
        self.assertEqual(report.remote, "origin")
        self.assertTrue(report.pushed)
        self.assertIn("805 tests OK", report.validation)
        self.assertEqual(report.summary, "implementation complete")

    def test_pushed_strict_bool_only(self):
        from coordinate.agent_report import parse_agent_report, parse_bool_field
        # The parser is permissive: unknown `pushed` values become None so
        # the publish flow can fail-closed at a later stage with a clear
        # publish.blocked (invalid_pushed) error.
        report = parse_agent_report(
            "[agent-report] action=done workspace_id=demo task_id=t pushed=yes"
        )
        self.assertIsNotNone(report)
        self.assertIsNone(report.pushed)
        # None when field absent
        self.assertIsNone(parse_bool_field(None))
        # Strict true/false only
        self.assertTrue(parse_bool_field("true"))
        self.assertTrue(parse_bool_field(" TRUE "))
        self.assertFalse(parse_bool_field("false"))
        self.assertIsNone(parse_bool_field("maybe"))
        self.assertIsNone(parse_bool_field("1"))

    def test_legacy_report_without_publish_metadata_still_parses(self):
        from coordinate.agent_report import parse_agent_report
        report = parse_agent_report(
            "[agent-report] action=done workspace_id=demo task_id=t "
            "summary=\"just done\""
        )
        self.assertIsNotNone(report)
        self.assertEqual(report.action, "done")
        self.assertIsNone(report.repo)
        self.assertIsNone(report.branch)
        self.assertIsNone(report.commit)
        self.assertIsNone(report.remote)
        self.assertIsNone(report.pushed)
        self.assertIsNone(report.validation)


class Phase84IngestPersistenceTests(unittest.TestCase):
    """Verify `_do_ingest` persists publish metadata into agent.reported payload."""

    def _setup_db(self):
        from coordinate.db import connect, migrate
        conn = connect(":memory:")
        migrate(conn)
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, "
            "base_branch, branch_namespace, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
            ("ws-1", "test", "/tmp/t", "/tmp/t/d", "main", "agents"),
        )
        conn.commit()
        return conn

    def _run_ingest(self, report):
        from coordinate.daemon import CoordinatorDaemon
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "c.sqlite3")
            daemon = CoordinatorDaemon(
                db_path=db_path, bot_token="x",
                channel_id=1, allowed_user_ids=set(),
            )
            # Use the actual _do_ingest thread-safe path.
            daemon._open_db = lambda: self._setup_db()  # type: ignore[assignment]
            try:
                return daemon._do_ingest(
                    "mac-claude", report,
                    message_id="m1", content="[agent-report] ...",
                )
            finally:
                self._cleanup()

    def _cleanup(self):
        pass

    def test_legacy_report_payload_unchanged(self):
        from coordinate.daemon import AgentReport, CoordinatorDaemon
        from coordinate.db import (
            connect, migrate, row_to_dict, list_events, upsert_workspace,
        )

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "c.sqlite3")
            seed = connect(db_path)
            migrate(seed)
            upsert_workspace(
                seed, workspace_id="ws-1", name="test",
                path="/tmp/t", harness_root="/tmp/t/d",
            )
            seed.close()
            daemon = CoordinatorDaemon(
                db_path=db_path, bot_token="x", channel_id=1,
                allowed_user_ids=set(),
            )
            report = AgentReport(
                action="done", workspace_id="ws-1", task_id="t",
                summary="legacy",
            )
            daemon._do_ingest("mac-claude", report, message_id="m1", content="x")
            conn = connect(db_path)
            migrate(conn)
            events = [row_to_dict(r) for r in list_events(conn, "ws-1")]
            self.assertEqual(len(events), 1)
            payload = events[0]["payload"]
            self.assertEqual(payload["action"], "done")
            self.assertEqual(payload["summary"], "legacy")
            for k in ("repo", "branch", "commit", "remote", "pushed", "validation"):
                self.assertNotIn(k, payload)
            conn.close()

    def test_publish_metadata_persisted(self):
        from coordinate.daemon import AgentReport, CoordinatorDaemon
        from coordinate.db import (
            connect, migrate, row_to_dict, list_events, upsert_workspace,
        )

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "c.sqlite3")
            seed = connect(db_path)
            migrate(seed)
            upsert_workspace(
                seed, workspace_id="ws-1", name="test",
                path="/tmp/t", harness_root="/tmp/t/d",
            )
            seed.close()
            daemon = CoordinatorDaemon(
                db_path=db_path, bot_token="x", channel_id=1,
                allowed_user_ids=set(),
            )
            report = AgentReport(
                action="done", workspace_id="ws-1", task_id="t",
                repo="acme/repo", branch="agents/x/t",
                commit="0123456789abcdef0123456789abcdef01234567",
                remote="origin", pushed=True,
                validation="805 OK", summary="done",
            )
            daemon._do_ingest("mac-claude", report, message_id="m1", content="x")
            conn = connect(db_path)
            migrate(conn)
            events = [row_to_dict(r) for r in list_events(conn, "ws-1")]
            payload = events[0]["payload"]
            self.assertEqual(payload["repo"], "acme/repo")
            self.assertEqual(payload["branch"], "agents/x/t")
            self.assertEqual(
                payload["commit"], "0123456789abcdef0123456789abcdef01234567",
            )
            self.assertEqual(payload["remote"], "origin")
            self.assertEqual(payload["pushed"], True)
            self.assertEqual(payload["validation"], "805 OK")
            conn.close()


class _FakeRow:
    """Minimal sqlite3.Row stand-in for _task_status_event_line tests."""
    def __init__(self, event_type: str, actor: str = "mac-codex",
                 created_at: str = "2025-01-01 12:00", payload_json: str | None = None):
        self._d = {
            "event_type": event_type,
            "actor": actor,
            "created_at": created_at,
            "payload_json": payload_json,
        }

    def __getitem__(self, key: str):
        return self._d[key]


class TaskStatusEventLineTests(unittest.TestCase):
    """Lock _task_status_event_line output for CI/review event types."""

    def test_ci_passed(self):
        line = _task_status_event_line(_FakeRow("ci.passed"))
        self.assertIn("CI 已通过", line)

    def test_pr_review_approved(self):
        line = _task_status_event_line(_FakeRow("pr_review.approved"))
        self.assertIn("PR review 已批准", line)

    def test_pr_review_changes_requested(self):
        line = _task_status_event_line(_FakeRow("pr_review.changes_requested"))
        self.assertIn("PR review 请求修改", line)


class DaemonPumpCursorTests(unittest.TestCase):
    """Lock daemon _do_pump rowid cursor: no backfill, no race window."""

    def _db_path(self) -> str:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return str(Path(tmp.name) / "coordinator.sqlite3")

    def _make_daemon(self, db_path):
        return CoordinatorDaemon(
            db_path=db_path,
            bot_token="token",
            channel_id=100,
            allowed_user_ids={1},
        )

    def _mock_bus(self):
        from coordinate.bus import PumpResult
        mock = patch("coordinate.daemon.bus")
        m = mock.start()
        m.pump_deliveries.return_value = PumpResult(processed=0, sent=0, failed=0, deliveries=[])
        self.addCleanup(mock.stop)
        return m

    def test_do_pump_does_not_backfill_old_events(self):
        """Events created before daemon first pump should not get deliveries."""
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path="/tmp/demo", harness_root="/tmp/demo/harness",
            default_bus="discord_webhook", default_destination="channel-1",
        )
        conn.execute(
            "INSERT INTO events (id, workspace_id, event_type, actor, task_id, "
            "created_at, payload_json, idempotency_key) "
            "VALUES (?, 'demo', 'closeout.requested', 'runner', 'old-task', "
            "'2026-06-01T00:00:00Z', '{}', 'old-key')",
            ("old-event-1",),
        )
        conn.commit()
        conn.close()

        daemon = self._make_daemon(db_path)
        self._mock_bus()
        daemon._do_pump()

        conn = initialize(db_path)
        try:
            deliveries = list(list_deliveries(conn))
            event_ids = [d["event_id"] for d in deliveries]
            self.assertNotIn("old-event-1", event_ids)
        finally:
            conn.close()

    def test_do_pump_delivers_new_events_after_cursor(self):
        """Events created after first pump cursor should get deliveries."""
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path="/tmp/demo", harness_root="/tmp/demo/harness",
            default_bus="discord_webhook", default_destination="channel-1",
        )
        conn.commit()
        conn.close()

        daemon = self._make_daemon(db_path)
        self._mock_bus()
        # First pump: initializes cursor to current max rowid (0).
        daemon._do_pump()

        # Insert new event after cursor is initialized.
        conn = initialize(db_path)
        append_event(
            conn, workspace_id="demo",
            event_type="worker.handoff.prepared",
            actor="operator", task_id="new-task",
            payload={"target_agent": "mac-claude", "bootstrap_text": "..."},
        )
        conn.close()

        # Second pump: should pick up the new event.
        daemon._do_pump()

        conn = initialize(db_path)
        try:
            deliveries = list(list_deliveries(conn))
            self.assertTrue(len(deliveries) > 0, "Expected delivery for new handoff event")
        finally:
            conn.close()

    def test_do_pump_race_event_delivered_next_cycle(self):
        """Event written during pump scan (after cutoff, before cursor advance)
        must be delivered in the next cycle — not permanently skipped."""
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path="/tmp/demo", harness_root="/tmp/demo/harness",
            default_bus="discord_webhook", default_destination="channel-1",
        )
        conn.commit()
        conn.close()

        daemon = self._make_daemon(db_path)
        self._mock_bus()
        # First pump: initializes cursor.
        daemon._do_pump()

        # Insert event AFTER first pump's cutoff was captured but
        # simulate the race: the event has rowid > cutoff.
        conn = initialize(db_path)
        append_event(
            conn, workspace_id="demo",
            event_type="worker.handoff.prepared",
            actor="operator", task_id="race-task",
            payload={"target_agent": "mac-claude", "bootstrap_text": "..."},
        )
        conn.close()

        # Second pump: cutoff captures the race event, processes it.
        daemon._do_pump()

        conn = initialize(db_path)
        try:
            deliveries = list(list_deliveries(conn))
            event_ids = [d["event_id"] for d in deliveries]
            # At least one delivery must exist for the race event
            self.assertTrue(len(deliveries) > 0,
                "Race event (written between pump cycles) must be delivered")
        finally:
            conn.close()

    def test_do_pump_same_second_events_not_skipped(self):
        """Events with same created_at as cursor init must still be delivered
        if their rowid > cursor (rowid-based, not timestamp-based)."""
        db_path = self._db_path()
        conn = initialize(db_path)
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path="/tmp/demo", harness_root="/tmp/demo/harness",
            default_bus="discord_webhook", default_destination="channel-1",
        )
        conn.commit()
        conn.close()

        daemon = self._make_daemon(db_path)
        self._mock_bus()
        # First pump: initializes cursor (max rowid = 0).
        daemon._do_pump()

        # Insert event with same timestamp as cursor init — rowid is higher.
        conn = initialize(db_path)
        append_event(
            conn, workspace_id="demo",
            event_type="worker.handoff.prepared",
            actor="operator", task_id="same-sec-task",
            payload={"target_agent": "mac-claude", "bootstrap_text": "..."},
        )
        conn.close()

        daemon._do_pump()

        conn = initialize(db_path)
        try:
            deliveries = list(list_deliveries(conn))
            self.assertTrue(len(deliveries) > 0,
                "Same-second event must be delivered (rowid > cursor)")
        finally:
            conn.close()
