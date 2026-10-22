import asyncio
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from coordinate.db import (
    bind_channel_workspace, connect, initialize, list_deliveries,
    list_events, upsert_workspace,
)


class History:
    def __init__(self):
        self.replies = []

    def validate_send_channel(self, channel_id, *, timeout=5):
        return {"id": channel_id, "type": 0, "guild_id": "500"}

    def get_message(self, channel_id, message_id, *, timeout=5):
        self.replies.append((channel_id, message_id))
        return {"id": message_id, "channel_id": channel_id}


class RecordingBus:
    def __init__(self, action=None):
        self.posts = []
        self.action = action

    def send(self, **kwargs):
        self.posts.append(kwargs)
        if self.action:
            return self.action(kwargs)
        return "discord:999"


@pytest.fixture
def setup(tmp_path):
    db_path = tmp_path / "channel.sqlite3"
    conn = initialize(db_path)
    for ws in ("alpha", "beta"):
        upsert_workspace(conn, workspace_id=ws, name=ws, path=tmp_path, harness_root=tmp_path)
    for channel, ws in (("123", "alpha"), ("124", "alpha"), ("125", "beta")):
        bind_channel_workspace(conn, platform="discord", channel_id=channel,
                               workspace_id=ws, actor="fixture", reason="test",
                               idempotency_key="bind-" + channel)
    conn.close()
    return db_path


def principal(actor="mcp-remote:agent-a"):
    return SimpleNamespace(actor=actor, workspace_ids=frozenset({"alpha", "beta"}),
                           platforms=frozenset({"discord"}),
                           tools=frozenset({"coordinate.channel_send"}))


def input_data(**changes):
    return {"workspace_id": "alpha", "channel_id": "123", "text": " hello 😀 ",
            "send_key": "batch-1", **changes}


def make_sender(db_path, *, bus=None, history=None):
    from coordinate.channel_delivery import ChannelSender
    return ChannelSender(lambda: connect(db_path), history=history or History(),
                         bus=bus or RecordingBus())


def rows(db_path):
    conn = connect(db_path)
    try:
        return [dict(row) for row in list_deliveries(conn)]
    finally:
        conn.close()


def test_send_records_receipt_event_and_raw_body_then_replays(setup):
    bus = RecordingBus()
    sender = make_sender(setup, bus=bus)
    first = sender.send(input_data(), principal())
    second = sender.send(input_data(), principal())
    assert first["ok"] is True
    assert first["data"]["message_id"] == "999"
    assert first["data"]["status"] == "sent"
    assert first["data"]["replayed"] is False
    assert second["data"] == {**first["data"], "replayed": True}
    assert len(bus.posts) == 1
    assert bus.posts[0]["payload"]["text"] == "[multinexus-channel]"
    assert bus.posts[0]["payload"]["embeds"] == [{"description": " hello 😀 "}]
    recorded = rows(setup)
    assert len(recorded) == 1 and recorded[0]["attempt_count"] == 1
    conn = connect(setup)
    try:
        audit = [dict(row) for row in list_events(conn) if row["event_type"].startswith("channel.message.")]
        assert [row["event_type"] for row in audit] == ["channel.message.requested", "channel.message.sent"]
        assert all(row["actor"] == "mcp-remote:agent-a" for row in audit)
    finally:
        conn.close()


@pytest.mark.parametrize("change", [{"text": "hello 😀"}, {"reply_to_message_id": "456"}])
def test_same_key_different_raw_text_or_reply_conflicts(setup, change):
    bus = RecordingBus()
    sender = make_sender(setup, bus=bus)
    assert sender.send(input_data(), principal())["ok"]
    result = sender.send(input_data(**change), principal())
    assert result["error"]["code"] == "idempotency_conflict"
    assert len(bus.posts) == 1


def test_key_scope_isolated_by_actor_and_channel(setup):
    sender = make_sender(setup)
    results = [sender.send(input_data(), principal()),
               sender.send(input_data(), principal("mcp-remote:agent-b")),
               sender.send(input_data(channel_id="124"), principal())]
    assert all(result["ok"] for result in results)
    assert len({result["data"]["delivery_id"] for result in results}) == 3


def test_concurrent_same_key_reserves_before_post_and_releases_transaction(setup):
    entered = threading.Event()
    finish = threading.Event()

    def post(_):
        # A second independent write succeeds while the first POST is blocked.
        probe = connect(setup)
        try:
            probe.execute("BEGIN IMMEDIATE")
            probe.rollback()
        finally:
            probe.close()
        entered.set()
        assert finish.wait(5)
        return "discord:999"

    bus = RecordingBus(post)
    sender = make_sender(setup, bus=bus)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(sender.send, input_data(), principal())
        assert entered.wait(5)
        second = pool.submit(sender.send, input_data(), principal()).result(timeout=5)
        finish.set()
        first = first.result(timeout=5)
    assert first["ok"] is True
    assert second["error"]["code"] == "delivery_uncertain"
    assert first["data"]["delivery_id"] == second["data"]["delivery_id"]
    assert len(bus.posts) == 1
    assert len(rows(setup)) == 1


@pytest.mark.parametrize("problem", [TimeoutError("secret response"), OSError("secret connection"), RuntimeError("broken success")])
def test_ambiguous_platform_failure_is_unknown_and_never_reposted(setup, problem):
    def fail(_):
        raise problem
    bus = RecordingBus(fail)
    sender = make_sender(setup, bus=bus)
    first = sender.send(input_data(), principal())
    second = sender.send(input_data(), principal())
    assert first["error"]["code"] == second["error"]["code"] == "delivery_uncertain"
    assert first["data"]["status"] == second["data"]["status"] == "unknown"
    assert "secret" not in json.dumps(first)
    assert len(bus.posts) == 1
    assert rows(setup)[0]["status"] == "sending"


def test_local_receipt_failure_after_accept_is_unknown_and_no_retry(setup):
    sender = make_sender(setup)
    with patch("coordinate.channel_delivery.finish_channel_delivery", side_effect=sqlite3.OperationalError("disk full")):
        result = sender.send(input_data(), principal())
    replay = sender.send(input_data(), principal())
    assert result["error"]["code"] == replay["error"]["code"] == "delivery_uncertain"
    assert rows(setup)[0]["status"] == "sending"


def test_definitive_rejection_is_terminal_and_key_can_only_replay_it(setup):
    from coordinate.bus import ChannelDeliveryRejected
    def fail(_):
        raise ChannelDeliveryRejected("definitely not accepted")
    bus = RecordingBus(fail)
    sender = make_sender(setup, bus=bus)
    first = sender.send(input_data(), principal())
    replay = sender.send(input_data(), principal())
    assert first["error"]["code"] == replay["error"]["code"] == "delivery_rejected"
    assert rows(setup)[0]["status"] == "dead"
    assert len(bus.posts) == 1


@pytest.mark.parametrize("changes", [
    {"text": "  \n\t"}, {"text": "😀" * 901}, {"text": "a" * 1801},
    {"send_key": ""}, {"send_key": "汉字"}, {"send_key": "a" * 129},
    {"actor": "override"}, {"channel_id": 123}, {"reply_to_message_id": "bad/id"},
])
def test_invalid_input_never_creates_delivery(setup, changes):
    result = make_sender(setup).send(input_data(**changes), principal())
    assert result["error"]["code"] == "invalid_input"
    assert rows(setup) == []


@pytest.mark.parametrize("channel_input,subject", [
    (input_data(), None), (input_data(channel_id="777"), principal()),
    (input_data(channel_id="125"), principal()),
    (input_data(), SimpleNamespace(actor="x", workspace_ids={"alpha"}, platforms={"discord"}, tools=set())),
])
def test_missing_principal_grant_and_inexact_binding_are_forbidden(setup, channel_input, subject):
    result = make_sender(setup).send(channel_input, subject)
    assert result == {"ok": False, "data": None, "error": {"code": "forbidden", "message": "forbidden"}}
    assert rows(setup) == []


def test_reply_checks_same_channel_and_utf16_boundary_accepts(setup):
    history = History()
    sender = make_sender(setup, history=history)
    result = sender.send(input_data(text="😀" * 900, reply_to_message_id="456"), principal())
    assert result["ok"] is True
    assert history.replies == [("123", "456")]


def test_cross_channel_reply_is_rejected_before_reserve(setup):
    history = History()
    history.get_message = lambda *_args, **_kwargs: {"id": "456", "channel_id": "125"}
    result = make_sender(setup, history=history).send(input_data(reply_to_message_id="456"), principal())
    assert result["error"]["code"] == "invalid_input"
    assert rows(setup) == []


def test_replay_does_not_require_original_reply_still_to_exist(setup):
    history = History()
    sender = make_sender(setup, history=history)
    first = sender.send(input_data(reply_to_message_id="456"), principal())
    def deleted(*_args, **_kwargs):
        raise ValueError("message deleted")
    history.get_message = deleted
    replay = sender.send(input_data(reply_to_message_id="456"), principal())
    assert replay["ok"] is True
    assert replay["data"] == {**first["data"], "replayed": True}


def test_same_key_changed_missing_reply_is_a_conflict(setup):
    history = History()
    sender = make_sender(setup, history=history)
    assert sender.send(input_data(), principal())["ok"]
    history.get_message = lambda *_args, **_kwargs: None
    result = sender.send(input_data(reply_to_message_id="456"), principal())
    assert result["error"]["code"] == "idempotency_conflict"


def test_actual_receipt_write_failure_rolls_back_result_event(setup):
    conn = connect(setup)
    conn.execute("CREATE TRIGGER fail_receipt BEFORE UPDATE OF status ON deliveries "
                 "WHEN NEW.status='sent' BEGIN SELECT RAISE(ABORT, 'simulated disk failure'); END")
    conn.commit()
    conn.close()
    bus = RecordingBus()
    sender = make_sender(setup, bus=bus)
    first = sender.send(input_data(), principal())
    replay = sender.send(input_data(), principal())
    assert first["error"]["code"] == replay["error"]["code"] == "delivery_uncertain"
    assert len(bus.posts) == 1
    assert rows(setup)[0]["status"] == "sending"
    conn = connect(setup)
    try:
        assert [row["event_type"] for row in list_events(conn)
                if row["event_type"].startswith("channel.message.")] == ["channel.message.requested"]
    finally:
        conn.close()


def test_cancellation_leaves_durable_unknown_without_retry(setup):
    def cancel(_):
        raise asyncio.CancelledError()
    bus = RecordingBus(cancel)
    sender = make_sender(setup, bus=bus)
    with pytest.raises(asyncio.CancelledError):
        sender.send(input_data(), principal())
    replay = sender.send(input_data(), principal())
    assert replay["error"]["code"] == "delivery_uncertain"
    assert len(bus.posts) == 1
    assert rows(setup)[0]["status"] == "sending"


def test_reservation_failure_rolls_back_audit_and_never_posts(setup):
    bus = RecordingBus()
    sender = make_sender(setup, bus=bus)
    with patch("coordinate.db.create_delivery", side_effect=sqlite3.OperationalError("insert failed")):
        result = sender.send(input_data(), principal())
    assert result["error"]["code"] == "upstream_unavailable"
    assert rows(setup) == []
    assert bus.posts == []
    conn = connect(setup)
    try:
        assert not [row for row in list_events(conn) if row["event_type"].startswith("channel.message.")]
    finally:
        conn.close()


def test_replay_rechecks_binding_after_platform_permission_validation(setup):
    history = History()
    sender = make_sender(setup, history=history)
    assert sender.send(input_data(), principal())["ok"]
    def change_binding(channel_id, *, timeout=5):
        conn = connect(setup)
        try:
            conn.execute("UPDATE channel_bindings SET workspace_id='beta' WHERE channel_id=?", (channel_id,))
            conn.commit()
        finally:
            conn.close()
        return {"id": channel_id, "type": 0, "guild_id": "500"}
    history.validate_send_channel = change_binding
    replay = sender.send(input_data(), principal())
    assert replay["error"]["code"] == "forbidden"


def test_simultaneous_same_key_calls_cross_reservation_race_with_one_post(setup):
    barrier = threading.Barrier(2)
    history = History()
    def both_validate(channel_id, *, timeout=5):
        barrier.wait(timeout=5)
        return {"id": channel_id, "type": 0, "guild_id": "500"}
    history.validate_send_channel = both_validate
    bus = RecordingBus()
    sender = make_sender(setup, bus=bus, history=history)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(sender.send, input_data(), principal()) for _ in range(2)]
        results = [future.result(timeout=5) for future in futures]
    assert len(bus.posts) == 1
    assert len(rows(setup)) == 1
    assert len({result["data"]["delivery_id"] for result in results}) == 1


def test_rebound_workspace_key_scope_is_independent(setup):
    sender = make_sender(setup)
    first = sender.send(input_data(), principal())
    conn = connect(setup)
    try:
        conn.execute("UPDATE channel_bindings SET workspace_id='beta' WHERE channel_id='123'")
        conn.commit()
    finally:
        conn.close()
    second = sender.send(input_data(workspace_id="beta"), principal())
    assert first["ok"] and second["ok"]
    assert first["data"]["delivery_id"] != second["data"]["delivery_id"]


def test_changed_format_version_conflicts_instead_of_resending(setup):
    bus = RecordingBus()
    sender = make_sender(setup, bus=bus)
    assert sender.send(input_data(), principal())["ok"]
    with patch("coordinate.channel_delivery._FORMAT_VERSION", "channel_io.v2"):
        result = sender.send(input_data(), principal())
    assert result["error"]["code"] == "idempotency_conflict"
    assert len(bus.posts) == 1


def test_orphaned_audit_key_is_not_silently_reused(setup):
    bus = RecordingBus()
    sender = make_sender(setup, bus=bus)
    assert sender.send(input_data(), principal())["ok"]
    conn = connect(setup)
    try:
        # Simulate a damaged ledger: an audit remains without its delivery.
        conn.execute("DELETE FROM deliveries")
        conn.commit()
    finally:
        conn.close()
    result = sender.send(input_data(), principal())
    assert result["error"]["code"] == "idempotency_conflict"
    assert len(bus.posts) == 1
    assert rows(setup) == []
