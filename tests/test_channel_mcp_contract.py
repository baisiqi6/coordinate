"""Channel tools must preserve authentication and the shared MCP contract."""
import asyncio
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from coordinate.agent_interface import AgentInterface
from coordinate.db import initialize, upsert_workspace, bind_channel_workspace
from coordinate import db
from coordinate.mcp_server import build_mcp_server, _typed_shape_middleware
from coordinate.mcp_remote import (
    McpPrincipal, McpAuthPolicy, build_remote_mcp_app,
    make_interface_provider, _request_principal,
)

TOOLS = {f"coordinate.channel_{kind}" for kind in ("read", "send", "wait")}
ARGS = {"workspace_id": "test", "channel_id": "123"}


class StubService:
    def __init__(self):
        self.calls = []

    def read(self, value, principal):
        self.calls.append(("read", value, principal))
        return {"ok": True, "data": {"actor": principal.actor}, "error": None}

    send = read

    async def wait(self, value, principal):
        return self.read(value, principal)


class ChannelMcpContractTests(unittest.TestCase):
    def test_real_remote_wait_lock_obeys_deadline_and_busy_limit(self):
        from coordinate.channel_messages import ChannelMessageService
        from tests.test_mcp_r3_remote import _rpc, _tool_call, _envelope, _lifespan_start, _lifespan_stop
        from coordinate.policy_common import digest_token
        from test_channel_read import cursor
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "locked.db"
            conn = initialize(path)
            upsert_workspace(conn, workspace_id="test", name="Test", path=temp, harness_root=temp)
            bind_channel_workspace(conn, platform="discord", channel_id="123", workspace_id="test", actor="test", reason="test", idempotency_key="test")
            conn.close()
            factory = lambda: db.connect(path)
            principal = McpPrincipal("one", digest_token("test-only"), frozenset({"test"}), frozenset({"discord"}), frozenset(TOOLS))
            service = ChannelMessageService(factory)
            server = build_mcp_server(interface_provider=make_interface_provider(factory, channel_service=service))
            app = build_remote_mcp_app(server, policy=McpAuthPolicy({principal.token_sha256: principal}), connection_factory=factory, allowed_hosts=["127.0.0.1:8766"], allowed_origins=[])
            locked, release = threading.Event(), threading.Event()
            def hold_lock():
                with factory() as writer:
                    writer.execute("BEGIN EXCLUSIVE")
                    locked.set()
                    release.wait(4)
                    writer.rollback()
            thread = threading.Thread(target=hold_lock)
            loop = asyncio.new_event_loop()
            lifespan = loop.run_until_complete(_lifespan_start(app))
            thread.start()
            self.assertTrue(locked.wait(1))
            value = {**ARGS, "after_cursor": cursor(workspace="test"), "timeout_seconds": 1}
            slots = [service._claim_wait(principal), service._claim_wait(principal)]
            try:
                started = time.monotonic()
                status, _, response = _rpc(app, token="test-only", method="tools/call", params=_tool_call("coordinate.channel_wait", {"input": value}), _loop=loop)
                self.assertEqual(status, 200)
                self.assertEqual(_envelope(response["result"])["error"]["code"], "busy")
                self.assertLess(time.monotonic() - started, .75)
                for key in slots:
                    service._release_wait(key)
                slots.clear()
                started = time.monotonic()
                _, _, response = _rpc(app, token="test-only", method="tools/call", params=_tool_call("coordinate.channel_wait", {"input": value}), _loop=loop)
                self.assertEqual(_envelope(response["result"])["error"]["code"], "upstream_unavailable")
                self.assertLess(time.monotonic() - started, 1.75)
            finally:
                for key in slots:
                    service._release_wait(key)
                release.set()
                thread.join(2)
                loop.run_until_complete(_lifespan_stop(lifespan))
                loop.run_until_complete(loop.shutdown_default_executor())
                loop.close()

    def test_mcp_omits_absent_optional_cursor_and_reply(self):
        service = StubService()
        principal = McpPrincipal("one", "0" * 64)
        server = build_mcp_server(AgentInterface(connection_factory=lambda: None, channel_service=service, principal_provider=lambda: principal))
        asyncio.run(server.call_tool("coordinate.channel_read", {"input": ARGS}))
        self.assertNotIn("after_cursor", service.calls[-1][1])
        asyncio.run(server.call_tool("coordinate.channel_send", {"input": {**ARGS, "text": "test", "send_key": "test"}}))
        self.assertNotIn("reply_to_message_id", service.calls[-1][1])

    def test_channel_input_validation_never_echoes_forged_fields(self):
        server = build_mcp_server(AgentInterface(connection_factory=lambda: None))
        ctx = SimpleNamespace(method="tools/call", params={"name": "coordinate.channel_read", "arguments": {"input": {**ARGS, "token": "private-sentinel"}}})
        async def next_call(ctx):
            self.fail("invalid input must be rejected before SDK validation")
        result = asyncio.run(_typed_shape_middleware(ctx, next_call))
        self.assertEqual(result.structured_content["error"]["code"], "invalid_input")
        self.assertNotIn("private-sentinel", result.content[0].text)

    def test_typed_tools_advertise_only_channel_fields(self):
        server = build_mcp_server(AgentInterface(connection_factory=lambda: None))
        tools = {t.name: t for t in asyncio.run(server.list_tools())}
        self.assertTrue(TOOLS <= tools.keys())
        for name in TOOLS:
            schema = tools[name].input_schema
            self.assertEqual(schema["required"], ["input"])
            ref = schema["properties"]["input"]["$ref"].split("/")[-1]
            model = schema["$defs"][ref]
            self.assertFalse(model["additionalProperties"])
            self.assertTrue({"workspace_id", "channel_id"} <= set(model["required"]))
            self.assertFalse({"actor", "session_id", "token", "provider", "model"} & model["properties"].keys())
            self.assertTrue(tools[name].annotations.open_world_hint)

    def test_stdio_cannot_use_channel_credentials_or_database(self):
        def forbidden_factory():
            self.fail("stdio channel calls must not access the database")
        server = build_mcp_server(AgentInterface(connection_factory=forbidden_factory))
        for name in TOOLS:
            value = dict(ARGS)
            if name.endswith("send"):
                value.update(text="test", send_key="test")
            if name.endswith("wait"):
                value.update(after_cursor="opaque")
            result = asyncio.run(server.call_tool(name, {"input": value}))
            self.assertTrue(result.is_error)
            self.assertEqual(result.structured_content["error"]["code"], "forbidden")
            self.assertEqual(json.loads(result.content[0].text), result.structured_content)

    def test_provider_passes_current_principal_and_reuses_wait_limiter(self):
        service = StubService()
        provider = make_interface_provider(lambda: None, channel_service=service)
        for client_id in ("one", "two"):
            principal = McpPrincipal(client_id, "0" * 64)
            token = _request_principal.set(principal)
            try:
                facade = provider()
                self.assertEqual(facade.channel_read(ARGS)["data"]["actor"], principal.actor)
                self.assertTrue(asyncio.run(facade.channel_wait(ARGS))["ok"])
            finally:
                _request_principal.reset(token)
        self.assertEqual([call[2].client_id for call in service.calls], ["one", "one", "two", "two"])

    def test_remote_static_scope_denies_before_service_invocation(self):
        with tempfile.TemporaryDirectory() as temp:
            db_path = Path(temp) / "test.db"
            def factory():
                return initialize(db_path)
            conn = factory()
            upsert_workspace(conn, workspace_id="test", name="Test", path=temp, harness_root=temp)
            bind_channel_workspace(conn, platform="discord", channel_id="123", workspace_id="test", actor="test", reason="isolated test", idempotency_key="bind-test")
            conn.commit()
            conn.close()
            server = build_mcp_server(interface_provider=lambda: self.fail("domain called"))
            build_remote_mcp_app(server, policy=McpAuthPolicy({}), connection_factory=factory,
                                 allowed_hosts=["localhost"], allowed_origins=[])
            middleware = server.middleware[0]
            principal = McpPrincipal("one", "0" * 64, frozenset({"test"}), frozenset({"discord"}), frozenset(TOOLS))
            token = _request_principal.set(principal)
            async def next_call(ctx):
                return "allowed"
            try:
                for overrides in ({"workspace_id": "other"}, {"platform": "kook"}):
                    ctx = SimpleNamespace(method="tools/call", params={"name": "coordinate.channel_read", "arguments": {"input": {**ARGS, **overrides}}})
                    result = asyncio.run(middleware(ctx, next_call))
                    self.assertEqual(result.structured_content["error"]["code"], "forbidden")
                ctx = SimpleNamespace(method="tools/call", params={"name": "coordinate.channel_read", "arguments": {"input": ARGS}})
                self.assertEqual(asyncio.run(middleware(ctx, next_call)), "allowed")
            finally:
                _request_principal.reset(token)
