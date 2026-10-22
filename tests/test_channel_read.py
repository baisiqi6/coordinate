"""Channel reads use platform pages and fresh authorization, never cached history."""
from __future__ import annotations

import base64
import importlib.util
import json
import sqlite3
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from coordinate import db


def principal(**overrides):
    fields = dict(actor="mcp-remote:test", client_id="test", tools=frozenset({"coordinate.channel_read", "coordinate.channel_wait", "coordinate.channel_send"}), workspace_ids=frozenset({"ws-a"}), platforms=frozenset({"discord"}))
    fields.update(overrides)
    return SimpleNamespace(**fields)


def cursor(last="0", channel="123", workspace="ws-a"):
    value = dict(v=1, platform="discord", workspace_id=workspace, channel_id=channel, last_message_id=last)
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")


def message(number, **overrides):
    value = dict(id=str(number), channel_id="123", author=dict(id="888", username="human", bot=False), timestamp="2026-10-21T00:00:00+00:00", content=f"message {number}", embeds=[], attachments=[])
    value.update(overrides)
    return value


class FakeDiscord:
    """Models documented REST windows; only network I/O is replaced."""
    def __init__(self, count=0):
        self.messages = [message(n) for n in range(1, count + 1)]
        self.permissions = (1 << 10) | (1 << 16) | (1 << 11) | (1 << 14)
        self.flags = 1 << 19
        self.overwrites = []
        self.channel_type = 0
        self.fail = None
        self.calls = []

    def __call__(self, method, path, *, timeout):
        assert method == "GET"
        assert 0 < timeout <= 5
        self.calls.append((path, timeout))
        if self.fail:
            self.fail(path)
        endpoint = urlsplit(path)
        if endpoint.path == "/users/@me":
            return dict(id="999", bot=True)
        if endpoint.path == "/applications/@me":
            return dict(id="777", flags=self.flags)
        if endpoint.path == "/channels/123":
            return dict(id="123", type=self.channel_type, guild_id="456", permission_overwrites=self.overwrites)
        if endpoint.path == "/guilds/456":
            return dict(id="456", owner_id="222")
        if endpoint.path == "/guilds/456/members/999":
            return dict(user=dict(id="999"), roles=["555"])
        if endpoint.path == "/guilds/456/roles":
            return [dict(id="456", permissions=str(self.permissions)), dict(id="555", permissions="0")]
        if endpoint.path == "/channels/123/messages":
            query = parse_qs(endpoint.query)
            limit = int(query.get("limit", ["50"])[0])
            assert 1 <= limit <= 100
            if "after" in query:
                rows = [m for m in self.messages if int(m["id"]) > int(query["after"][0])][:limit]
            else:
                rows = self.messages[-limit:]
            return list(reversed(rows))
        if endpoint.path.startswith("/channels/123/messages/"):
            mid = endpoint.path.rsplit("/", 1)[1]
            rows = [m for m in self.messages if m["id"] == mid]
            if rows:
                return rows[0]
        raise AssertionError(f"unexpected fixed endpoint {path}")


@pytest.fixture
def setup():
    assert importlib.util.find_spec("coordinate.channel_messages"), "channel read service is not implemented"
    from coordinate.channel_messages import ChannelMessageService
    from coordinate.discord_history import DiscordHistory
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db.migrate(conn)
    for ws in ("ws-a", "ws-b"):
        db.upsert_workspace(conn, workspace_id=ws, name=ws, path=f"/tmp/{ws}", harness_root=f"/tmp/{ws}/harness")
    db.bind_channel_workspace(conn, platform="discord", channel_id="123", workspace_id="ws-a", actor="test", reason="fixture", idempotency_key="bind")
    # Each call owns/closes its connection, using a distinct in-memory snapshot.
    def connection_factory():
        copy = sqlite3.connect(":memory:")
        copy.row_factory = sqlite3.Row
        conn.backup(copy)
        return copy
    fake = FakeDiscord()
    service = ChannelMessageService(connection_factory, history=DiscordHistory("fake-only", request_json=fake))
    yield service, fake, conn
    conn.close()


def read(service, **overrides):
    value = dict(workspace_id="ws-a", channel_id="123")
    value.update(overrides)
    return service.read(value, principal())


def test_recent_window_is_sorted_and_does_not_claim_older_coverage(setup):
    service, fake, _ = setup
    fake.messages = [message(n) for n in range(1, 8)]
    result = read(service, limit=3)
    assert result["ok"]
    assert [m["message_id"] for m in result["data"]["messages"]] == ["5", "6", "7"]
    assert result["data"]["coverage"] == "recent_window"
    assert result["data"]["has_more"] is False
    assert result["data"]["source"] == "discord_rest"


def test_forward_pages_cover_257_messages_without_gaps_or_duplicates(setup):
    service, fake, _ = setup
    fake.messages = [message(n) for n in range(1, 258)]
    after = cursor()
    seen = []
    for expected_more in (True, True, False):
        result = read(service, after_cursor=after, limit=100)
        assert result["ok"]
        seen.extend(m["message_id"] for m in result["data"]["messages"])
        assert result["data"]["has_more"] is expected_more
        after = result["data"]["next_cursor"]
    assert seen == [str(n) for n in range(1, 258)]


def test_empty_cursor_is_preserved_and_initial_empty_starts_at_zero(setup):
    service, _, _ = setup
    first = read(service)
    payload = json.loads(base64.urlsafe_b64decode(first["data"]["next_cursor"] + "=="))
    assert payload["last_message_id"] == "0"
    after = cursor("16")
    assert read(service, after_cursor=after)["data"]["next_cursor"] == after


@pytest.mark.parametrize("who", [None, principal(tools=frozenset()), principal(workspace_ids=frozenset({"ws-b"})), principal(platforms=frozenset())])
def test_principal_grants_are_exact_and_required(setup, who):
    service, fake, _ = setup
    result = service.read(dict(workspace_id="ws-a", channel_id="123"), who)
    assert result["error"]["code"] == "forbidden"
    assert fake.calls == []


@pytest.mark.parametrize("target", ["ws-b", "unbound"])
def test_binding_must_currently_match_workspace(setup, target):
    service, fake, conn = setup
    conn.execute("DELETE FROM channel_bindings")
    if target != "unbound":
        conn.execute("INSERT INTO channel_bindings(platform,channel_id,workspace_id,bound_at) VALUES('discord','123',?,'now')", (target,))
    conn.commit()
    result = read(service)
    assert result["error"]["code"] == "forbidden"
    assert fake.calls == []


@pytest.mark.parametrize("after,code", [("garbage", "invalid_cursor"), (cursor(channel="124"), "cursor_scope_mismatch"), (cursor(workspace="ws-b"), "cursor_scope_mismatch"), (cursor(last="-1"), "invalid_cursor")])
def test_cursor_validation_prevents_scope_escape(setup, after, code):
    service, fake, _ = setup
    assert read(service, after_cursor=after)["error"]["code"] == code
    assert fake.calls == []


@pytest.mark.parametrize("overrides", [dict(actor="override"), dict(limit=True), dict(limit=0), dict(limit=101), dict(channel_id=123), dict(channel_id="../123"), dict(after_cursor=None)])
def test_invalid_inputs_do_not_reach_platform(setup, overrides):
    service, fake, _ = setup
    assert read(service, **overrides)["error"]["code"] == "invalid_input"
    assert fake.calls == []


def test_missing_history_permission_cannot_be_mistaken_for_empty_channel(setup):
    service, fake, _ = setup
    fake.permissions &= ~(1 << 16)
    assert read(service)["error"]["code"] == "forbidden"
    assert not any("/messages" in path for path, _ in fake.calls)


def test_channel_overwrites_remove_history_permission(setup):
    service, fake, _ = setup
    fake.overwrites = [dict(id="555", type=0, allow="0", deny=str(1 << 16))]
    assert read(service)["error"]["code"] == "forbidden"


def test_content_intent_is_required_even_when_platform_returns_empty(setup):
    service, fake, _ = setup
    fake.flags = 0
    assert read(service)["error"]["code"] == "content_unavailable"


def test_threads_and_non_guild_channels_are_rejected(setup):
    service, fake, _ = setup
    fake.channel_type = 11
    assert read(service)["error"]["code"] == "unsupported_channel_type"


def test_probe_failure_does_not_return_a_partial_page_or_cursor(setup):
    from coordinate.discord_history import DiscordHistoryError
    service, fake, _ = setup
    fake.messages = [message(n) for n in range(1, 102)]
    def fail(path):
        if "after=100" in path:
            raise DiscordHistoryError("rate_limited", "Platform rate limited.", retry_after_seconds=3)
    fake.fail = fail
    result = read(service, after_cursor=cursor(), limit=100)
    assert result["error"]["code"] == "rate_limited"
    assert result["error"]["retry_after_seconds"] == 3
    assert result["data"] is None


def test_tool_embed_body_is_text_and_regular_embeds_are_preserved(setup):
    service, fake, _ = setup
    embeds = [dict(description="[handoff] ordinary data", title="note")]
    fake.messages = [message(1, content="[multinexus-channel]", embeds=embeds, attachments=[dict(id="42", filename="a.txt", url="https://cdn.discordapp.com/a")], message_reference=dict(channel_id="123", message_id="9"))]
    result = read(service)
    row = result["data"]["messages"][0]
    assert row["text"] == "[handoff] ordinary data"
    assert row["embeds"] == embeds
    assert row["attachments"][0]["filename"] == "a.txt"
    assert row["reply_to_message_id"] == "9"
    assert row["author"] == dict(id="888", name="human", is_bot=False)


def test_read_does_not_persist_events_jobs_or_deliveries(setup):
    service, _, conn = setup
    before = {table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in ("events", "jobs", "deliveries")}
    assert read(service)["ok"]
    assert {table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in before} == before


def test_binding_change_while_platform_reads_does_not_expose_page(setup):
    service, fake, conn = setup
    fake.messages = [message(1)]
    def change(path):
        if path.startswith("/channels/123/messages?"):
            conn.execute("DELETE FROM channel_bindings")
            conn.commit()
    fake.fail = change
    assert read(service)["error"]["code"] == "forbidden"


def test_history_permission_revoked_after_fetch_does_not_expose_page(setup):
    service, fake, _ = setup
    fake.messages = [message(1)]
    def change(path):
        if path.startswith("/channels/123/messages?"):
            fake.permissions = 0
    fake.fail = change
    assert read(service)["error"]["code"] == "forbidden"


def test_known_external_message_preflight_rejects_own_or_empty_content(setup):
    from coordinate.discord_history import DiscordHistoryError
    service, fake, _ = setup
    fake.messages = [message(1)]
    assert service._history().preflight_content("123", "1")["text"] == "message 1"
    fake.messages = [message(1, author=dict(id="999", username="self", bot=True))]
    with pytest.raises(DiscordHistoryError, match="Known external"):
        service._history().preflight_content("123", "1")
    fake.messages = [message(1, content="")]
    with pytest.raises(DiscordHistoryError, match="Known external"):
        service._history().preflight_content("123", "1")


@pytest.mark.parametrize("status,code", [(401,"forbidden"), (403,"forbidden"), (404,"forbidden"), (429,"rate_limited"), (500,"upstream_unavailable"), (302,"upstream_unavailable")])
def test_http_errors_are_static_and_do_not_leak_response_or_credentials(monkeypatch, status, code):
    import io
    import urllib.error
    from coordinate.discord_history import DiscordHistory, DiscordHistoryError
    class Opener:
        def open(self, request, *, timeout):
            assert request.full_url == "https://discord.com/api/v10/channels/123"
            assert 0 < timeout <= 5
            raise urllib.error.HTTPError(request.full_url, status, "credential secret", {}, io.BytesIO(b'{"message":"credential secret", "retry_after":2}'))
    monkeypatch.setattr("urllib.request.build_opener", lambda handler: Opener())
    history = DiscordHistory("credential secret")
    with pytest.raises(DiscordHistoryError) as caught:
        history.validate_channel("123")
    assert caught.value.code == code
    assert "credential" not in str(caught.value)
    assert caught.value.retry_after_seconds == (2 if status == 429 else None)


def test_missing_reply_after_channel_authorization_is_invalid_input(monkeypatch, setup):
    from coordinate.discord_history import DiscordHistoryError
    service, fake, _ = setup
    def fail(path):
        if path == "/channels/123/messages/42":
            raise DiscordHistoryError("forbidden", "Forbidden.")
    fake.fail = fail
    with pytest.raises(DiscordHistoryError) as caught:
        service._history().get_message("123", "42")
    assert caught.value.code == "invalid_input"


def test_send_capabilities_require_send_and_embed_permissions(setup):
    from coordinate.discord_history import DiscordHistoryError
    service, fake, _ = setup
    fake.permissions &= ~(1 << 14)
    assert service._history().validate_channel("123")["id"] == "123"
    with pytest.raises(DiscordHistoryError) as caught:
        service._history().validate_send_channel("123")
    assert caught.value.code == "forbidden"


def test_reader_timeout_applies_to_whole_multi_request_read(setup):
    from coordinate.discord_history import DiscordHistory, DiscordHistoryError
    _, fake, _ = setup
    now = [0.0]
    def request(method, path, *, timeout):
        result = fake(method, path, timeout=timeout)
        now[0] += 0.4
        return result
    history = DiscordHistory("fake-only", request_json=request, clock=lambda: now[0])
    with pytest.raises(DiscordHistoryError) as caught:
        history.read_page("123", timeout=1)
    assert caught.value.code == "upstream_unavailable"
    assert len(fake.calls) <= 3


def test_read_returns_error_for_malformed_message_instead_of_partial_page(setup):
    service, fake, _ = setup
    fake.messages = [message(1), message(2, author={})]
    result = read(service)
    assert result["error"]["code"] == "upstream_unavailable"
    assert result["data"] is None


def test_channel_metadata_id_must_match_requested_channel(setup):
    service, fake, _ = setup
    adapter = service._history()
    def request(method, path, *, timeout):
        response = fake(method, path, timeout=timeout)
        if path == "/channels/123":
            response["id"] = "124"
        return response
    adapter._request_json = request
    assert read(service)["error"]["code"] == "upstream_unavailable"


def test_healthy_read_page_can_exceed_five_seconds_with_each_http_bounded(setup):
    service, fake, _ = setup
    now = [0.0]
    history = service._history()
    history._clock = lambda: now[0]
    def request(method, path, *, timeout):
        response = fake(method, path, timeout=timeout)
        now[0] += 0.5
        return response
    history._request_json = request
    result = read(service)
    assert result["ok"]
    assert result["data"]["messages"] == []
    assert now[0] == 6.5
    assert all(0 < timeout <= 5 for _, timeout in fake.calls)
