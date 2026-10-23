"""Bound production channel paths: only HTTP is fake; config and ledger are real."""
import asyncio
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from coordinate import db
from coordinate.channel_messages import ChannelMessageService, _encode_cursor
from coordinate.mcp_remote import McpPrincipal, load_mcp_auth_policy
from coordinate.policy_common import AuthPolicyError
from test_channel_send import setup, input_data, rows, make_sender, principal as legacy_principal

CODEX = "111111111111111111"
DSH = "222222222222222222"
TOKENS = {"codex-fixture": CODEX, "dsh-fixture": DSH, "rotated-fixture": CODEX}


def principal(agent="agent-a", client="codex"):
    return McpPrincipal(client, "0" * 64, frozenset({"alpha"}), frozenset({"discord"}),
                        frozenset({"coordinate.channel_read", "coordinate.channel_wait", "coordinate.channel_send"}),
                        channel_agent_id=agent)


def write_private(path, value):
    path.write_text(value)
    path.chmod(0o600)
    return str(path)


@pytest.fixture
def configured(tmp_path, monkeypatch, setup):
    registry = tmp_path / "agents.toml"
    registry.write_text(f'''[registry]\nid = "multinexus"\nversion = 8\n[[agents]]\nid = "agent-a"\ndiscord_user_id = "{CODEX}"\n[[agents]]\nid = "agent-b"\ndiscord_user_id = "{DSH}"\n''')
    credentials = {agent: {"token_file": write_private(tmp_path / agent, token)}
                   for agent, token in (("agent-a", "codex-fixture"), ("agent-b", "dsh-fixture"))}
    config = tmp_path / "identities.json"
    write_private(config, json.dumps(dict(version=1, registry_path=str(registry), credentials=credentials)))
    monkeypatch.setenv("COORDINATE_CHANNEL_IDENTITIES_FILE", str(config))
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "never-use-global")
    calls, posts = [], []
    state = {"self": None, "author": "correct", "post_hook": None}

    def get(history, method, path, *, timeout):
        token = history._token
        calls.append((token, path))
        bot = TOKENS[token]
        if path == "/users/@me":
            value = state["self"]
            if isinstance(value, Exception):
                raise value
            return value if value is not None else {"id": bot, "bot": True}
        if path == "/channels/123":
            return dict(id="123", type=0, guild_id="456", permission_overwrites=[])
        if path == "/guilds/456":
            return dict(id="456", owner_id=bot)
        if path == f"/guilds/456/members/{bot}":
            return dict(roles=[])
        if path == "/guilds/456/roles":
            return [dict(id="456", permissions="0")]
        if path == "/applications/@me":
            return dict(flags=1 << 19)
        if path.startswith("/channels/123/messages?"):
            return []
        if path == "/channels/123/messages/999":
            return dict(id="999", channel_id="123")
        raise AssertionError(path)

    def post(url, headers, body):
        posts.append((url, headers, body))
        if state["post_hook"]:
            state["post_hook"]()
        token = headers["Authorization"].removeprefix("Bot ")
        response = {"id": str(999 + len(posts))}
        if state["author"] == "correct":
            response["author"] = {"id": TOKENS[token], "bot": True}
        elif state["author"] is not None:
            response["author"] = state["author"]
        return response

    monkeypatch.setattr("coordinate.discord_history.DiscordHistory._http_get", get)
    monkeypatch.setattr("coordinate.bus.post_channel_json", post)
    return dict(db=setup, registry=registry, config=config, credentials=credentials,
                calls=calls, posts=posts, state=state)


def service(f):
    return ChannelMessageService(lambda: db.connect(f["db"]))


def read_input():
    return dict(workspace_id="alpha", channel_id="123")


def test_optional_policy_binding_is_strict_and_legacy_tools_survive(tmp_path):
    entry = dict(client_id="client", token_sha256="0" * 64, workspace_ids=["alpha"],
                 platforms=["discord"], tools=["coordinate.channel_send"], channel_agent_id="agent-a")
    path = tmp_path / "policy"
    write_private(path, json.dumps(dict(version=1, clients=[entry])))
    policy = load_mcp_auth_policy(str(path))
    assert policy._by_digest["0" * 64].channel_agent_id == "agent-a"
    for invalid in ["", " agent-a", "agent-a ", 42, True, "mac\ncodex", "x/y"]:
        write_private(path, json.dumps(dict(version=1, clients=[{**entry, "channel_agent_id": invalid}])))
        with pytest.raises(AuthPolicyError):
            load_mcp_auth_policy(str(path))
    entry.pop("channel_agent_id")
    write_private(path, json.dumps(dict(version=1, clients=[entry])))
    assert load_mcp_auth_policy(str(path))._by_digest["0" * 64].channel_agent_id is None


def test_two_principals_use_own_token_and_verified_receipts(configured):
    f = configured
    svc = service(f)
    for who, expected in [(principal(), CODEX), (principal("agent-b", "dsh"), DSH)]:
        result = svc.send(input_data(), who)
        assert result["ok"], result
        receipt = result["data"]
        assert receipt["actual_author_bot_id"] == expected
        assert receipt["identity_status"] == "verified" and receipt["binding_current"] is True
        assert receipt["sender_identity"] == dict(version=1, client_id=who.client_id, agent_id=who.channel_agent_id,
            discord_user_id=expected, registry_source_id="multinexus", registry_source_version=8,
            registry_source_hash=receipt["sender_identity"]["registry_source_hash"])
    assert [p[1]["Authorization"] for p in f["posts"]] == ["Bot codex-fixture", "Bot dsh-fixture"]
    assert all("sender_identity" not in json.dumps(p[2]) for p in f["posts"])
    assert all(p[0] == "https://discord.com/api/v10/channels/123/messages" for p in f["posts"])
    assert len(rows(f["db"])) == 2


@pytest.mark.parametrize("self_value,code", [({"id": DSH, "bot": True}, "sender_identity_mismatch"),
    ({"id": CODEX, "bot": False}, "sender_identity_mismatch"), ({"id": CODEX}, "sender_identity_mismatch"),
    ({}, "sender_identity_mismatch"), (OSError("do not expose token or path"), "sender_unavailable")])
def test_self_identity_rejects_before_other_network_or_reservation(configured, self_value, code):
    f = configured
    f["state"]["self"] = self_value
    result = service(f).send(input_data(), principal())
    assert result["error"]["code"] == code
    assert [p for _, p in f["calls"]] == ["/users/@me"]
    assert not f["posts"] and not rows(f["db"])
    assert "expose" not in json.dumps(result)


@pytest.mark.parametrize("author", [None, {"id": DSH, "bot": True}, {"id": CODEX, "bot": False}, {"id": CODEX}])
def test_post_author_unknown_is_durable_and_never_reposts(configured, author):
    f = configured
    f["state"]["author"] = author
    first = service(f).send(input_data(), principal())
    assert first["error"]["code"] == "delivery_uncertain"
    assert first["data"]["actual_author_bot_id"] is None
    assert first["data"]["identity_status"] == "unconfirmed"
    f["calls"].clear()
    for path in f["credentials"].values():
        from pathlib import Path
        Path(path["token_file"]).unlink()
    second = service(f).send(input_data(), principal())
    assert second["error"]["code"] == "delivery_uncertain"
    assert second["data"]["delivery_id"] == first["data"]["delivery_id"]
    assert not f["calls"] and len(f["posts"]) == 1


def test_replay_drift_returns_original_receipt_without_network(configured):
    f = configured
    svc = service(f)
    first = svc.send(input_data(), principal())
    before = rows(f["db"])
    f["calls"].clear()
    for who in [principal("agent-b"), principal(None)]:
        result = svc.send(input_data(), who)
        assert result["error"]["code"] == "idempotency_conflict"
        assert result["data"]["delivery_id"] == first["data"]["delivery_id"]
        assert result["data"]["sender_identity"] == first["data"]["sender_identity"]
        assert result["data"]["actual_author_bot_id"] == CODEX
        assert result["data"]["binding_current"] is False
    assert rows(f["db"]) == before and not f["calls"] and len(f["posts"]) == 1


def test_rotation_and_unrelated_roster_change_do_not_change_replay(configured):
    f = configured
    first = service(f).send(input_data(), principal())
    before = rows(f["db"])
    from pathlib import Path
    Path(f["credentials"]["agent-a"]["token_file"]).write_text("rotated-fixture")
    f["registry"].write_text(f["registry"].read_text().replace("version = 8", "version = 9") + '\n[[agents]]\nid="other"\ndiscord_user_id="555"\n')
    f["calls"].clear()
    result = service(f).send(input_data(), principal())
    assert result["data"] == {**first["data"], "replayed": True}
    assert rows(f["db"]) == before and not f["calls"] and len(f["posts"]) == 1


def test_legacy_replay_is_unattributed_and_unchanged(configured):
    f = configured
    make_sender(f["db"]).send(input_data(), legacy_principal("mcp-remote:codex"))
    before = rows(f["db"])
    result = service(f).send(input_data(), principal())
    assert result["ok"] and result["data"]["replayed"]
    assert result["data"]["sender_identity"] is None
    assert result["data"]["identity_status"] == "legacy_unattributed"
    assert result["data"]["actual_author_bot_id"] is None and result["data"]["binding_current"] is False
    assert rows(f["db"]) == before and not f["calls"] and not f["posts"]


@pytest.mark.parametrize("operation", ["send", "read", "wait"])
def test_unbound_default_never_uses_global_token(configured, operation):
    f = configured
    svc = service(f)
    if operation == "send":
        result = svc.send(input_data(), principal(None))
    elif operation == "read":
        result = svc.read(read_input(), principal(None))
    else:
        result = asyncio.run(svc.wait({**read_input(), "after_cursor": _encode_cursor(read_input(), "0"), "timeout_seconds": 0}, principal(None)))
    assert result["error"]["code"] == "sender_unbound"
    assert not f["calls"] and not f["posts"]


@pytest.mark.parametrize("override", ["bot_id", "token", "token_file", "channel_agent_id", "client_id", "sender_identity"])
def test_callers_cannot_override_sender(configured, override):
    f = configured
    assert service(f).send(input_data(**{override: "override"}), principal())["error"]["code"] == "invalid_input"
    assert not f["calls"] and not f["posts"]


def test_missing_credential_and_disabled_or_unknown_agent_fail_closed(configured):
    f = configured
    svc = service(f)
    for who in [principal("unknown"), principal("agent-a-desktop")]:
        assert svc.send(input_data(), who)["error"]["code"] == "sender_unavailable"
    f["registry"].write_text(f["registry"].read_text().replace('id = "agent-a"', 'id = "agent-a"\nenabled = false'))
    assert service(f).send(input_data(), principal())["error"]["code"] == "sender_unavailable"
    assert not f["posts"] and not f["calls"]


def test_concurrent_same_key_and_different_principals_isolate_contexts(configured):
    f = configured
    entered, finish = threading.Event(), threading.Event()
    def hold():
        entered.set()
        assert finish.wait(5)
    f["state"]["post_hook"] = hold
    svc = service(f)
    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(svc.send, input_data(), principal())
        assert entered.wait(5)
        duplicate = pool.submit(svc.send, input_data(), principal()).result(5)
        peer = pool.submit(svc.send, input_data(), principal("agent-b", "dsh"))
        finish.set()
        results = [first.result(5), peer.result(5)]
    assert duplicate["error"]["code"] == "delivery_uncertain"
    assert [r["data"]["actual_author_bot_id"] for r in results] == [CODEX, DSH]
    assert len(f["posts"]) == 2 and len(rows(f["db"])) == 2


def test_read_and_wait_use_bound_context_once_per_operation(configured, monkeypatch):
    from coordinate.channel_identity import ChannelIdentityResolver
    f = configured
    resolved = []
    original = ChannelIdentityResolver.resolve
    def resolve(self, who):
        resolved.append(who.client_id)
        return original(self, who)
    monkeypatch.setattr(ChannelIdentityResolver, "resolve", resolve)
    svc = service(f)
    assert svc.read(read_input(), principal())["ok"]
    from test_channel_wait import Clock
    clock = Clock()
    svc._clock, svc._sleep = clock, clock.sleep
    result = asyncio.run(svc.wait({**read_input(), "after_cursor": _encode_cursor(read_input(), "0"), "timeout_seconds": 4}, principal("agent-b", "dsh")))
    assert result["data"]["status"] == "timeout"
    assert resolved == ["codex", "dsh"]
    assert set(t for t, _ in f["calls"]) == {"codex-fixture", "dsh-fixture"}


@pytest.mark.parametrize("change", ["unknown", "duplicate", "relative", "inline", "version", "public"])
def test_identity_config_strict_eager_validation_is_sanitized(configured, change):
    f = configured
    data = json.loads(f["config"].read_text())
    if change == "unknown": data["extra"] = "not allowed"
    if change == "relative": data["registry_path"] = "agents.toml"
    if change == "inline": data["credentials"]["agent-a"]["token"] = "secret"
    if change == "version": data["version"] = True
    f["config"].write_text(json.dumps(data))
    if change == "duplicate": f["config"].write_text('{"version":1,"version":1}')
    if change == "public": f["config"].chmod(0o644)
    with pytest.raises(ValueError) as exc:
        service(f)
    assert str(f["config"]) not in str(exc.value) and "secret" not in str(exc.value)


@pytest.mark.parametrize("problem", ["missing", "public", "empty", "control"])
def test_bad_token_file_fails_without_leaking_reference(configured, problem):
    from pathlib import Path
    f = configured
    token_file = Path(f["credentials"]["agent-a"]["token_file"])
    if problem == "missing": token_file.unlink()
    if problem == "public": token_file.chmod(0o644)
    if problem == "empty": token_file.write_text("")
    if problem == "control": token_file.write_text("fixture\r\ninjected")
    result = service(f).send(input_data(), principal())
    assert result["error"]["code"] == "sender_unavailable"
    assert str(token_file) not in json.dumps(result)
    assert not f["posts"] and not f["calls"] and not rows(f["db"])


@pytest.mark.parametrize("registry", [
    '[registry]\nid="test"\nversion=1\n[[agents]]\nid="agent-a"\ndiscord_user_id="1"\n[[agents]]\nid="agent-a"\ndiscord_user_id="2"',
    '[registry]\nid="test"\nversion=1\n[[agents]]\nid="agent-a"\ndiscord_user_id="1"\n[[agents]]\nid="other"\ndiscord_user_id="1"',
    '[[agents]]\nid="agent-a"\ndiscord_user_id="1"',
    '[registry]\nid="test"\nversion=1\n[[agents]]\nid="agent-a"',
    '[registry]\nid="test"\nversion=1\n[[agents]]\nid="agent-a"\ndiscord_user_id="0"',
])
def test_bad_registry_uses_existing_parser_and_rejects_eagerly(configured, registry):
    f = configured
    f["registry"].write_text(registry)
    with pytest.raises(ValueError, match="Invalid channel identity configuration"):
        service(f)
    assert not f["posts"] and not f["calls"]


def test_missing_config_disables_only_new_operations_and_preserves_replay(configured, monkeypatch):
    f = configured
    first = service(f).send(input_data(), principal())
    monkeypatch.delenv("COORDINATE_CHANNEL_IDENTITIES_FILE")
    f["calls"].clear()
    svc = service(f)
    result = svc.send(input_data(), principal())
    assert result["error"]["code"] == "idempotency_conflict"
    assert result["data"]["delivery_id"] == first["data"]["delivery_id"]
    assert result["data"]["binding_current"] is False
    assert svc.send(input_data(send_key="new"), principal())["error"]["code"] == "sender_unavailable"
    assert not f["calls"] and len(f["posts"]) == 1


def test_bound_loss_of_completion_write_is_unknown_after_recreation(configured, monkeypatch):
    f = configured
    def fail(*args, **kwargs):
        raise sqlite3.OperationalError("secret database path")
    monkeypatch.setattr("coordinate.channel_delivery.finish_channel_delivery", fail)
    first = service(f).send(input_data(), principal())
    before = rows(f["db"])
    f["calls"].clear()
    replay = service(f).send(input_data(), principal())
    assert first["error"]["code"] == replay["error"]["code"] == "delivery_uncertain"
    assert replay["data"]["identity_status"] == "unconfirmed"
    assert replay["data"]["actual_author_bot_id"] is None
    assert before == rows(f["db"]) and not f["calls"] and len(f["posts"]) == 1


def test_bound_rows_cannot_enter_generic_replay(configured):
    from coordinate.bus import BusError, pump_deliveries, send_delivery
    f = configured
    f["state"]["author"] = None
    first = service(f).send(input_data(), principal())
    with db.connect(f["db"]) as conn:
        assert db.recover_sending_deliveries(conn) == []
        assert pump_deliveries(conn, recover_sending=True).processed == 0
        with pytest.raises(BusError):
            send_delivery(conn, first["data"]["delivery_id"])
    assert len(f["posts"]) == 1


@pytest.mark.parametrize("operation", ["read", "wait"])
def test_read_wait_identity_mismatch_never_fetches_messages(configured, operation):
    f = configured
    f["state"]["self"] = dict(id=DSH, bot=True)
    svc = service(f)
    if operation == "read":
        result = svc.read(read_input(), principal())
    else:
        result = asyncio.run(svc.wait({**read_input(), "after_cursor": _encode_cursor(read_input(), "0"), "timeout_seconds": 0}, principal()))
    assert result["error"]["code"] == "sender_identity_mismatch"
    assert [path for _, path in f["calls"]] == ["/users/@me"]


def test_wait_retains_token_and_config_for_entire_bounded_lifetime(configured):
    from pathlib import Path
    from test_channel_wait import Clock
    f = configured
    svc = service(f)
    clock = Clock()
    svc._clock, svc._sleep = clock, clock.sleep
    def change_files():
        Path(f["credentials"]["agent-a"]["token_file"]).write_text("dsh-fixture")
        f["config"].write_text("invalid after startup")
    clock.hook = change_files
    result = asyncio.run(svc.wait({**read_input(), "after_cursor": _encode_cursor(read_input(), "0"), "timeout_seconds": 4}, principal()))
    assert result["data"]["status"] == "timeout"
    assert {token for token, _ in f["calls"]} == {"codex-fixture"}
    assert clock.now == 4


def test_principal_grants_are_checked_before_token_file_access(configured, monkeypatch):
    from dataclasses import replace
    f = configured
    def no_access(_):
        pytest.fail("token lookup before grant validation")
    svc = service(f)
    monkeypatch.setattr("coordinate.channel_identity._private_file", no_access)
    who = replace(principal(), tools=frozenset())
    assert svc.send(input_data(), who)["error"]["code"] == "forbidden"
    assert svc.read(read_input(), who)["error"]["code"] == "forbidden"
    assert not f["calls"] and not f["posts"]


def test_bound_injection_cannot_bypass_identity_validation(configured):
    from coordinate.channel_delivery import ChannelSender
    from test_channel_send import History, RecordingBus
    f = configured
    f["state"]["self"] = dict(id=DSH, bot=True)
    sender = ChannelSender(lambda: db.connect(f["db"]), history=History(), bus=RecordingBus())
    assert sender.send(input_data(), principal())["error"]["code"] == "sender_identity_mismatch"
    assert not f["posts"] and not rows(f["db"])


def test_snapshot_contains_no_credential_material(configured):
    f = configured
    result = service(f).send(input_data(), principal())
    serialized = json.dumps([result, rows(f["db"])])
    for secret in ["codex-fixture", "dsh-fixture", str(f["config"]), "token_file", "token_sha256"]:
        assert secret not in serialized


def test_bound_race_before_reservation_posts_once(configured, monkeypatch):
    from coordinate.discord_history import DiscordHistory
    f = configured
    barrier = threading.Barrier(2)
    original = DiscordHistory._http_get
    def get(history, method, path, *, timeout):
        if path == "/users/@me":
            barrier.wait(timeout=5)
        return original(history, method, path, timeout=timeout)
    monkeypatch.setattr(DiscordHistory, "_http_get", get)
    svc = service(f)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(svc.send, input_data(), principal()) for _ in range(2)]
        results = [future.result(5) for future in futures]
    assert len(f["posts"]) == 1 and len(rows(f["db"])) == 1
    assert len({result["data"]["delivery_id"] for result in results}) == 1


def test_same_agent_bot_mapping_drift_cannot_repost(configured):
    f = configured
    first = service(f).send(input_data(), principal())
    before = rows(f["db"])
    f["registry"].write_text(f["registry"].read_text().replace(CODEX, "555"))
    f["calls"].clear()
    result = service(f).send(input_data(), principal())
    assert result["error"]["code"] == "idempotency_conflict"
    assert result["data"]["actual_author_bot_id"] == CODEX
    assert result["data"]["delivery_id"] == first["data"]["delivery_id"]
    assert not result["data"]["binding_current"]
    assert rows(f["db"]) == before and not f["calls"] and len(f["posts"]) == 1


def test_missing_credential_mapping_disables_selected_agent(configured):
    f = configured
    config = json.loads(f["config"].read_text())
    del config["credentials"]["agent-a"]
    f["config"].write_text(json.dumps(config))
    result = service(f).send(input_data(), principal())
    assert result["error"]["code"] == "sender_unavailable"
    assert not f["calls"] and not f["posts"]


def test_new_rejected_reservation_replays_without_credentials(configured, monkeypatch):
    from coordinate.bus import ChannelDeliveryRejected
    f = configured
    def reject():
        raise ChannelDeliveryRejected("rejected")
    f["state"]["post_hook"] = reject
    first = service(f).send(input_data(), principal())
    before = rows(f["db"])
    svc = service(f)
    def no_access(_):
        pytest.fail("replay read token")
    monkeypatch.setattr("coordinate.channel_identity._private_file", no_access)
    replay = svc.send(input_data(), principal())
    assert first["error"]["code"] == replay["error"]["code"] == "delivery_rejected"
    assert replay["data"]["identity_status"] == "unconfirmed"
    assert replay["data"]["actual_author_bot_id"] is None
    assert rows(f["db"]) == before and len(f["posts"]) == 1


def test_legacy_unknown_replay_never_gains_identity(configured):
    from test_channel_send import RecordingBus
    f = configured
    def lose(_):
        raise TimeoutError()
    make_sender(f["db"], bus=RecordingBus(lose)).send(input_data(), legacy_principal("mcp-remote:codex"))
    before = rows(f["db"])
    result = service(f).send(input_data(), principal())
    assert result["error"]["code"] == "delivery_uncertain"
    assert result["data"]["identity_status"] == "legacy_unattributed"
    assert result["data"]["sender_identity"] is None
    assert rows(f["db"]) == before and not f["calls"] and not f["posts"]


def test_wait_self_lookup_deadline_does_not_report_normal_timeout(configured, monkeypatch):
    from coordinate.channel_identity import ChannelIdentityResolver
    from test_channel_wait import Clock
    f = configured
    clock = Clock()
    original_resolve = ChannelIdentityResolver.resolve
    def resolve(resolver, who):
        context = original_resolve(resolver, who)
        context.history._clock = clock
        original_get = context.history._request_json
        def get(method, path, *, timeout):
            result = original_get(method, path, timeout=timeout)
            clock.now += 2
            return result
        context.history._request_json = get
        return context
    monkeypatch.setattr(ChannelIdentityResolver, "resolve", resolve)
    svc = service(f)
    svc._clock, svc._sleep = clock, clock.sleep
    result = asyncio.run(svc.wait({**read_input(), "after_cursor": _encode_cursor(read_input(), "0"), "timeout_seconds": 1}, principal()))
    assert result["error"]["code"] == "upstream_unavailable"
    assert result["data"] is None
    assert [path for _, path in f["calls"]] == ["/users/@me"]
