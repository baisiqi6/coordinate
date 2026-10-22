import pytest

from coordinate.bus import BusError, DiscordBus, pump_deliveries, send_delivery
from coordinate.db import (
    create_delivery, get_delivery, initialize,
    recover_sending_deliveries,
)


class RecordingBus:
    def __init__(self):
        self.posts = []

    def send(self, **kwargs):
        self.posts.append(kwargs)
        return "discord:999"


@pytest.mark.parametrize("status", ["pending", "sending", "failed"])
def test_channel_deliveries_are_never_generic_work(status):
    conn = initialize(":memory:")
    try:
        row, _ = create_delivery(
            conn, platform="discord", destination="123", message_key="one-shot",
            payload={"purpose": "channel_io.v1", "text": "[multinexus-channel]"},
        )
        if status != "pending":
            conn.execute("UPDATE deliveries SET status=? WHERE id=?", (status, row["id"]))
            conn.commit()
        bus = RecordingBus()
        recovered = recover_sending_deliveries(conn)
        result = pump_deliveries(conn, bus=bus, recover_sending=True)
        assert recovered == []
        assert result.processed == 0
        assert bus.posts == []
        assert get_delivery(conn, row["id"])["status"] == status
        with pytest.raises(BusError, match="channel"):
            send_delivery(conn, row["id"], bus=bus)
        assert bus.posts == []
    finally:
        conn.close()


def test_channel_discord_body_is_safe_and_endpoint_is_fixed():
    calls = []
    bus = DiscordBus(
        token="fake", api_base="https://not-discord.invalid",
        http_post=lambda url, headers, body: calls.append((url, body)) or {"id": "999"},
    )
    raw = "[handoff] @Codex\n[agent-report] decision=done\n```\n😀\n```"
    bus.send(destination="123", payload={
        "purpose": "channel_io.v1", "text": "[multinexus-channel]",
        "embeds": [{"description": raw}], "reply_to_message_id": "456",
    }, message_key="one")
    url, body = calls[0]
    assert url == "https://discord.com/api/v10/channels/123/messages"
    assert body == {
        "content": "[multinexus-channel]",
        "embeds": [{"description": raw}],
        "allowed_mentions": {"parse": [], "replied_user": False},
        "message_reference": {"message_id": "456", "channel_id": "123", "fail_if_not_exists": True},
    }
    from coordinate.agent_report import parse_agent_report
    assert parse_agent_report(body["content"]) is None


@pytest.mark.parametrize("status,terminal", [(400, True), (403, True), (404, True), (429, True), (408, False), (500, False), (503, False)])
def test_channel_post_classifies_definite_rejection_and_never_retries(status, terminal):
    import io
    import urllib.error
    from unittest.mock import patch
    from coordinate.bus import ChannelDeliveryRejected
    attempts = []
    def fail(request, *, timeout):
        attempts.append((request.full_url, timeout))
        raise urllib.error.HTTPError(request.full_url, status, "secret reason", {}, io.BytesIO(b"secret token body"))
    bus = DiscordBus(token="fake")
    with patch("coordinate.bus.urllib.request.urlopen", side_effect=fail):
        with pytest.raises(BusError) as result:
            bus.send(destination="123", payload={"purpose": "channel_io.v1", "text": "[multinexus-channel]"}, message_key="one")
    assert isinstance(result.value, ChannelDeliveryRejected) is terminal
    assert "secret" not in str(result.value)
    assert attempts == [("https://discord.com/api/v10/channels/123/messages", 5)]


@pytest.mark.parametrize("raw", [b"", b"not json", b"[]", b'{"id": null}'])
def test_channel_malformed_success_is_unknown(raw):
    import io
    from unittest.mock import patch
    from coordinate.bus import ChannelDeliveryRejected
    bus = DiscordBus(token="fake")
    with patch("coordinate.bus.urllib.request.urlopen", return_value=io.BytesIO(raw)):
        with pytest.raises(BusError) as result:
            bus.send(destination="123", payload={"purpose": "channel_io.v1", "text": "[multinexus-channel]"}, message_key="one")
    assert not isinstance(result.value, ChannelDeliveryRejected)
