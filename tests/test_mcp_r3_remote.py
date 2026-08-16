"""R3-I1 Remote MCP streamable-HTTP vertical slice tests.

Covers the loopback-only, stateless, private-bearer-auth streamable HTTP
transport and its request-scoped authorization boundary:

- CLI fail-closed validation (stdio default preserved; remote flags required;
  loopback-only bind; exact host/origin allowlists; missing DB never created);
- server-local policy loading (strict file, digest-only, unknown field,
  duplicate client/digest, illegal scope all fail closed; timing-safe auth);
- ASGI middleware order auth -> bounded concurrency -> bounded access log,
  request-scoped principal ContextVar set/reset and leak-free logs;
- SDK-level authorization: tools/list deletion filtering and tools/call
  re-authorization for tool/workspace/platform/channel/job scope, identical
  cross-workspace vs missing job responses, and the global agent-list grant;
- Host/Origin/body-size policy and concurrency exhaustion fail closed;
- the five tool schemas staying identical to the stdio registration.

The MCP SDK remains an optional extra: policy and CLI validation tests run on
a base install; tests that need the real SDK lifecycle are skipped when
``mcp`` is not importable.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from types import SimpleNamespace

try:
    import mcp  # noqa: F401

    _MCP_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised on base installs
    _MCP_AVAILABLE = False

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
REPO_ROOT = Path(__file__).resolve().parents[1]

from coordinate.cli import build_parser  # noqa: E402
from coordinate.db import (  # noqa: E402
    bind_channel_workspace,
    initialize,
    list_events,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.mcp_cli import handle_mcp_serve  # noqa: E402
from coordinate.mcp_remote import (  # noqa: E402
    DEFAULT_MAX_CONCURRENT,
    McpPrincipal,
    RemoteMCPApp,
    build_remote_mcp_app,
    get_request_principal,
    load_mcp_auth_policy,
    make_interface_provider,
)
from coordinate.mcp_server import MCP_TOOL_NAMES, build_mcp_server  # noqa: E402
from coordinate.runtime import register_agent, submit_request  # noqa: E402
from coordinate.runtime_http import AuthPolicyError, LOOPBACK_HOSTS  # noqa: E402
from coordinate.runtime_interface import (  # noqa: E402
    RuntimeInterface,
    RuntimeInterfaceConfig,
)
from tests.test_runtime_interface import _sync_catalog  # noqa: E402

TOKEN_ALPHA = "alpha-secret-token"
TOKEN_BETA = "beta-secret-token"
TOKEN_GAMMA = "gamma-secret-token"

ALL_TOOLS = sorted(MCP_TOOL_NAMES)
WORKSPACE_SCOPED = (
    "coordinate.operator_pending",
    "coordinate.workspace_audit",
    "coordinate.runtime_request_submit",
    "coordinate.task_create_record",
    "coordinate.completion_prepare",
    "coordinate.completion_preflight",
    "coordinate.completion_claim",
    "coordinate.completion_apply",
    "coordinate.completion_consume",
    "coordinate.channel_create",
)


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _modern_meta() -> dict:
    """The 2026-07-28 per-request envelope used by the raw stdio tests."""
    return {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {
            "name": "r3-raw-modern",
            "version": "0",
        },
    }


def _client(client_id: str, token: str, **scopes) -> dict:
    entry = {
        "client_id": client_id,
        "token_sha256": _digest(token),
        "workspace_ids": ["demo"],
        "platforms": ["discord"],
        "tools": list(ALL_TOOLS),
    }
    entry.update(scopes)
    return entry


class PolicyFileTestBase(unittest.TestCase):
    """Shared policy fixture helpers (no MCP SDK required)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "clients.json")

    def _write(self, clients, mode: int = 0o600, path: str | None = None) -> str:
        target = path or self.path
        with open(target, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "clients": clients}, f)
        os.chmod(target, mode)
        return target


class McpPolicyTests(PolicyFileTestBase):
    """Digest-only policy loading; every invalid shape fails closed."""

    def _valid_clients(self):
        return [
            _client("alpha", TOKEN_ALPHA),
            _client(
                "beta",
                TOKEN_BETA,
                workspace_ids=["other"],
                platforms=["slack"],
                tools=["coordinate.runtime_job_get"],
            ),
        ]

    def test_valid_policy_loads_and_authenticates(self):
        policy = load_mcp_auth_policy(self._write(self._valid_clients()))
        principal = policy.authenticate(TOKEN_ALPHA)
        self.assertIsInstance(principal, McpPrincipal)
        self.assertEqual(principal.client_id, "alpha")
        self.assertEqual(principal.token_sha256, _digest(TOKEN_ALPHA))
        self.assertEqual(principal.workspace_ids, frozenset({"demo"}))
        self.assertEqual(principal.platforms, frozenset({"discord"}))
        self.assertEqual(principal.tools, frozenset(ALL_TOOLS))
        self.assertEqual(principal.actor, "mcp-remote:alpha")

    def test_authenticate_bad_unknown_empty_all_none(self):
        policy = load_mcp_auth_policy(self._write(self._valid_clients()))
        self.assertIsNone(policy.authenticate("wrong-token"))
        self.assertIsNone(policy.authenticate(""))
        self.assertIsNone(policy.authenticate(None))
        self.assertIsNotNone(policy.authenticate(TOKEN_BETA))
        self.assertIsNone(policy.authenticate(TOKEN_ALPHA + "x"))

    def _assert_policy_error(self, clients, fragment):
        with self.assertRaises(AuthPolicyError) as ctx:
            load_mcp_auth_policy(self._write(clients))
        self.assertIn(fragment, str(ctx.exception))

    def test_unknown_root_field_fails_closed(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "clients": [], "extra": 1}, f)
        os.chmod(self.path, 0o600)
        with self.assertRaises(AuthPolicyError):
            load_mcp_auth_policy(self.path)

    def test_unknown_entry_field_fails_closed(self):
        self._assert_policy_error(
            [_client("alpha", TOKEN_ALPHA, role="bridge")], "unknown"
        )

    def test_duplicate_client_id_fails_closed(self):
        self._assert_policy_error(
            [
                _client("alpha", TOKEN_ALPHA),
                _client("alpha", TOKEN_BETA),
            ],
            "duplicate client",
        )

    def test_duplicate_token_digest_fails_closed(self):
        self._assert_policy_error(
            [
                _client("alpha", TOKEN_ALPHA),
                _client("beta", TOKEN_ALPHA, workspace_ids=["other"]),
            ],
            "duplicate token",
        )

    def test_plaintext_token_rejected_as_bad_digest(self):
        self._assert_policy_error(
            [
                {
                    "client_id": "alpha",
                    "token_sha256": "alpha-secret-token",
                    "workspace_ids": ["demo"],
                    "platforms": ["discord"],
                    "tools": list(ALL_TOOLS),
                }
            ],
            "token_sha256",
        )

    def test_unknown_tool_name_fails_closed(self):
        self._assert_policy_error(
            [_client("alpha", TOKEN_ALPHA, tools=["coordinate.nope"])],
            "unknown tool",
        )

    def test_non_list_scope_fails_closed(self):
        self._assert_policy_error(
            [_client("alpha", TOKEN_ALPHA, workspace_ids="demo")], "workspace_ids"
        )
        self._assert_policy_error(
            [_client("alpha", TOKEN_ALPHA, platforms="discord")], "platforms"
        )
        self._assert_policy_error(
            [_client("alpha", TOKEN_ALPHA, tools="coordinate.operator_pending")],
            "tools",
        )

    def test_missing_required_fields_fails_closed(self):
        self._assert_policy_error(
            [
                {
                    "client_id": "alpha",
                    "token_sha256": _digest(TOKEN_ALPHA),
                    "workspace_ids": ["demo"],
                    "platforms": ["discord"],
                }
            ],
            "tools",
        )
        self._assert_policy_error(
            [
                {
                    "token_sha256": _digest(TOKEN_ALPHA),
                    "workspace_ids": ["demo"],
                    "platforms": ["discord"],
                    "tools": list(ALL_TOOLS),
                }
            ],
            "client_id",
        )

    def test_empty_workspace_and_platform_lists_are_valid(self):
        policy = load_mcp_auth_policy(
            self._write(
                [_client("alpha", TOKEN_ALPHA, workspace_ids=[], platforms=[])]
            )
        )
        principal = policy.authenticate(TOKEN_ALPHA)
        self.assertEqual(principal.workspace_ids, frozenset())
        self.assertEqual(principal.platforms, frozenset())

    def test_empty_tools_list_fails_closed(self):
        self._assert_policy_error(
            [_client("alpha", TOKEN_ALPHA, tools=[])], "tools"
        )

    def test_version_mismatch_fails_closed(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"version": 2, "clients": []}, f)
        os.chmod(self.path, 0o600)
        with self.assertRaises(AuthPolicyError):
            load_mcp_auth_policy(self.path)

    def test_empty_clients_fails_closed(self):
        with self.assertRaises(AuthPolicyError):
            load_mcp_auth_policy(self._write([]))

    def test_bad_json_fails_closed(self):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("{not json")
        os.chmod(self.path, 0o600)
        with self.assertRaises(AuthPolicyError):
            load_mcp_auth_policy(self.path)

    def test_group_world_writable_mode_fails_closed(self):
        for mode in (0o620, 0o602, 0o666):
            with self.assertRaises(AuthPolicyError) as ctx:
                load_mcp_auth_policy(
                    self._write(self._valid_clients(), mode=mode)
                )
            self.assertIn("writable", str(ctx.exception))

    def test_non_regular_file_fails_closed(self):
        fifo = os.path.join(self.tmp.name, "clients.fifo")
        os.mkfifo(fifo)
        os.chmod(fifo, 0o600)
        with self.assertRaises(AuthPolicyError):
            load_mcp_auth_policy(fifo)

    def test_missing_file_fails_closed(self):
        with self.assertRaises(AuthPolicyError):
            load_mcp_auth_policy(os.path.join(self.tmp.name, "absent.json"))

    def test_foreign_owner_fails_closed(self):
        if os.geteuid() == 0:  # pragma: no cover - root cannot construct this case
            self.skipTest("running as root; foreign-owner case cannot be constructed")
        with unittest.mock.patch(
            "coordinate.policy_common.os.geteuid",
            return_value=os.geteuid() + 1,
        ):
            with self.assertRaises(AuthPolicyError) as ctx:
                load_mcp_auth_policy(self._write(self._valid_clients()))
        self.assertIn("owned", str(ctx.exception))

    def test_root_owned_policy_allowed(self):
        with unittest.mock.patch(
            "coordinate.policy_common.Path.stat",
            return_value=SimpleNamespace(st_mode=0o100600, st_uid=0),
        ):
            policy = load_mcp_auth_policy(self._write(self._valid_clients()))
        self.assertIsNotNone(policy.authenticate(TOKEN_ALPHA))

    def test_current_user_owned_policy_allowed(self):
        # Real file owned by the effective uid must load without owner errors.
        policy = load_mcp_auth_policy(self._write(self._valid_clients()))
        self.assertIsNotNone(policy.authenticate(TOKEN_ALPHA))


def _remote_args(**overrides) -> argparse.Namespace:
    args = argparse.Namespace(
        db=":memory:",
        actor="mcp",
        transport="streamable-http",
        host="127.0.0.1",
        port=8766,
        path="/mcp",
        auth_file="/nonexistent/clients.json",
        allowed_host=["127.0.0.1:8766"],
        allowed_origin=[],
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _serve(args) -> tuple[int, str]:
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        rc = handle_mcp_serve(args)
    return rc, stderr.getvalue()


class McpCliFailClosedTests(unittest.TestCase):
    """CLI surface: stdio stays default; remote flags fail before binding."""

    def test_parser_default_transport_still_stdio(self):
        parser = build_parser()
        mcp = next(
            a
            for a in parser._actions
            if getattr(a, "dest", None) == "command"
        ).choices["mcp"]
        serve = next(
            a
            for a in mcp._actions
            if getattr(a, "dest", None) == "mcp_command"
        ).choices["serve"]
        self.assertEqual(serve.get_default("transport"), "stdio")
        self.assertEqual(serve.get_default("host"), "127.0.0.1")
        self.assertEqual(serve.get_default("port"), 8766)
        self.assertEqual(serve.get_default("path"), "/mcp")

    def test_unknown_transport_rejected_with_stdio_hint(self):
        rc, stderr = _serve(
            argparse.Namespace(db=":memory:", actor="mcp", transport="sse")
        )
        self.assertEqual(rc, 1)
        self.assertIn("stdio", stderr)

    def test_streamable_http_requires_auth_file(self):
        args = _remote_args(auth_file=None)
        rc, stderr = _serve(args)
        self.assertEqual(rc, 1)
        self.assertIn("--auth-file", stderr)

    def test_streamable_http_requires_allowed_host(self):
        args = _remote_args(allowed_host=[])
        rc, stderr = _serve(args)
        self.assertEqual(rc, 1)
        self.assertIn("--allowed-host", stderr)

    def test_non_loopback_host_rejected(self):
        args = _remote_args(host="0.0.0.0")
        rc, stderr = _serve(args)
        self.assertEqual(rc, 1)
        self.assertIn("loopback", stderr)

    def test_wildcard_host_and_origin_rejected(self):
        rc, stderr = _serve(_remote_args(allowed_host=["*"]))
        self.assertEqual(rc, 1)
        self.assertIn("wildcard", stderr)
        rc, stderr = _serve(_remote_args(allowed_origin=["*"]))
        self.assertEqual(rc, 1)
        self.assertIn("wildcard", stderr)

    def test_invalid_path_rejected(self):
        rc, stderr = _serve(_remote_args(path="mcp"))
        self.assertEqual(rc, 1)
        self.assertIn("--path", stderr)
        rc, stderr = _serve(_remote_args(path="/mcp?x=1"))
        self.assertEqual(rc, 1)
        self.assertIn("--path", stderr)

    def test_invalid_port_rejected(self):
        rc, stderr = _serve(_remote_args(port=0))
        self.assertEqual(rc, 1)
        rc, stderr = _serve(_remote_args(port=70000))
        self.assertEqual(rc, 1)
        self.assertIn("--port", stderr)

    def test_invalid_policy_fails_before_import_probe(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "clients.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "clients": [{"client_id": "a"}]}, f)
        os.chmod(path, 0o600)
        rc, stderr = _serve(_remote_args(auth_file=path))
        self.assertEqual(rc, 1)
        self.assertIn("auth policy", stderr)


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class RemoteDbFailClosedTests(unittest.TestCase):
    """Missing DB must fail startup and never create the file."""

    def test_missing_db_fails_without_creating_file(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        policy_path = os.path.join(tmp.name, "clients.json")
        with open(policy_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "version": 1,
                    "clients": [
                        _client("alpha", TOKEN_ALPHA),
                    ],
                },
                f,
            )
        os.chmod(policy_path, 0o600)
        missing_db = os.path.join(tmp.name, "absent.sqlite3")
        args = _remote_args(db=missing_db, auth_file=policy_path)
        rc, stderr = _serve(args)
        self.assertEqual(rc, 1)
        self.assertFalse(os.path.exists(missing_db))
        self.assertIn("database", stderr)

    def test_empty_sqlite_file_fails_startup(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        policy_path = os.path.join(tmp.name, "clients.json")
        with open(policy_path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "clients": [_client("alpha", TOKEN_ALPHA)]}, f)
        os.chmod(policy_path, 0o600)
        empty_db = os.path.join(tmp.name, "empty.sqlite3")
        with open(empty_db, "wb") as f:
            f.write(b"")
        args = _remote_args(db=empty_db, auth_file=policy_path)
        rc, stderr = _serve(args)
        self.assertEqual(rc, 1)
        self.assertIn("database", stderr)


def _asgi_call(app, *, method="POST", path="/mcp", headers=None, body=b""):
    """Synchronous wrapper driving one request through a plain ASGI app."""
    return asyncio.run(_asgi_call_async(app, method=method, path=path, headers=headers, body=body))


async def _asgi_call_async(app, *, method="POST", path="/mcp", headers=None, body=b""):
    """Drive one request through a plain ASGI app in-process (no sockets)."""
    import anyio

    request_tx, request_rx = anyio.create_memory_object_stream(8)
    response_tx, response_rx = anyio.create_memory_object_stream(8)

    normalized = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
    normalized.setdefault("host", "127.0.0.1:8766")
    wire_headers = [
        (key.encode("latin-1"), value.encode("latin-1"))
        for key, value in normalized.items()
    ]

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "root_path": "",
        "headers": wire_headers,
        "client": ("127.0.0.1", 45678),
        "server": ("127.0.0.1", 8766),
    }

    async def _runner():
        await app(scope, request_rx.receive, response_tx.send)
        await response_tx.send(None)

    status = None
    resp_headers: dict[str, str] = {}
    chunks: list[bytes] = []

    async with anyio.create_task_group() as tg:
        tg.start_soon(_runner)
        await request_tx.send(
            {"type": "http.request", "body": body, "more_body": False}
        )
        while True:
            msg = await response_rx.receive()
            if msg is None:
                break
            if msg["type"] == "http.response.start":
                status = msg["status"]
                for key, value in msg.get("headers", []):
                    resp_headers[key.decode()] = value.decode()
            elif msg["type"] == "http.response.body":
                chunks.append(msg.get("body") or b"")
                if not msg.get("more_body", False):
                    # The response is complete on the wire; keep draining so
                    # the app's own shutdown (transport terminate) can finish
                    # instead of leaking background tasks.
                    continue

    return status, resp_headers, b"".join(chunks)


def _sse_events(body: bytes) -> list[dict]:
    """Parse the SSE frames of a stateless streamable HTTP response."""
    events: list[dict] = []
    for block in body.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
        data = [
            line[len("data:"):].strip()
            for line in block.splitlines()
            if line.startswith("data:")
        ]
        if data:
            events.append(json.loads("".join(data)))
    return events


def _rpc(
    app,
    *,
    token,
    method,
    params=None,
    extra_headers=None,
    body=None,
    raw: bool = False,
    _loop=None,
):
    """One JSON-RPC POST; returns (status, headers, response dict or None)."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2026-07-28",
        "Authorization": f"Bearer {token}",
    }
    if extra_headers:
        headers.update(extra_headers)
    if body is None:
        if params is None:
            params = {"_meta": _modern_meta()}
        message = {"jsonrpc": "2.0", "id": 1, "method": method}
        if params is not None:
            message["params"] = params
        body = json.dumps(message).encode("utf-8")
        # 2026-07-28 transport contract: routing headers must match the body.
        headers["MCP-Method"] = method
        if (
            method == "tools/call"
            and isinstance(params, dict)
            and isinstance(params.get("name"), str)
        ):
            headers["MCP-Name"] = params["name"]
    coro = _asgi_call_async(app, headers=headers, body=body)
    if _loop is not None:
        status, resp_headers, raw_body = _loop.run_until_complete(coro)
    else:
        status, resp_headers, raw_body = asyncio.run(coro)
    if raw:
        return status, resp_headers, raw_body
    if raw_body.lstrip().startswith(b"{"):
        # The modern path answers with plain JSON when the handler completes
        # without notifications; SSE framing is used otherwise.
        try:
            return status, resp_headers, json.loads(raw_body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return status, resp_headers, None
    events = _sse_events(raw_body)
    return status, resp_headers, events[0] if events else None


async def _lifespan_start(app):
    """Start a Starlette app's lifespan; return (task, send_tx, receive_rx)."""
    import anyio

    send_tx, send_rx = anyio.create_memory_object_stream(8)
    recv_tx, recv_rx = anyio.create_memory_object_stream(8)
    scope = {
        "type": "lifespan",
        "asgi": {"version": "3.0", "spec_version": "2.0"},
        "state": {},
    }

    async def _runner():
        await app(scope, send_rx.receive, recv_tx.send)

    task = asyncio.create_task(_runner())
    await send_tx.send({"type": "lifespan.startup"})
    message = await recv_rx.receive()
    if message["type"] != "lifespan.startup.complete":
        raise RuntimeError(f"lifespan startup failed: {message}")
    return task, send_tx, recv_rx


async def _lifespan_stop(handle):
    task, send_tx, recv_rx = handle
    await send_tx.send({"type": "lifespan.shutdown"})
    message = await recv_rx.receive()
    if message["type"] != "lifespan.shutdown.complete":
        raise RuntimeError(f"lifespan shutdown failed: {message}")
    await task


def _result_of(response: dict | None) -> dict | None:
    if response is None:
        return None
    return response.get("result")


def _envelope(result: dict | None) -> dict | None:
    if result is None:
        return None
    return result.get("structuredContent")


def _tool_call(name: str, arguments: dict | None) -> dict:
    return {
        "name": name,
        "arguments": arguments or {},
        "_meta": _modern_meta(),
    }


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class RemoteMCPAppIntegrationTests(unittest.TestCase):
    """Real SDK app + auth middleware + real DB, driven in-process."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "r3.sqlite3")
        self.conn = initialize(self.db_path)
        self.addCleanup(self.conn.close)
        self._seed_db()
        self.policy_path = os.path.join(self.tmp.name, "clients.json")
        self._write_policy(self._clients())
        self.app = self._build_app()
        # One loop per test: the SDK session manager binds its task group to
        # the loop that ran its lifespan, so every request must share it.
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self._close_loop)
        self._lifespan = self.loop.run_until_complete(
            _lifespan_start(self.app)
        )

    def _close_loop(self):
        if getattr(self, "_lifespan", None) is not None:
            try:
                self.loop.run_until_complete(_lifespan_stop(self._lifespan))
            except Exception:
                pass
            self._lifespan = None
        if getattr(self, "loop", None) is not None:
            try:
                self.loop.close()
            except Exception:
                pass

    def _rpc(self, app=None, **kwargs):
        """One JSON-RPC POST on the shared loop; returns (status, headers, dict|None)."""
        target = app if app is not None else self.app
        return _rpc(target, **kwargs, _loop=self.loop)

    def _request(self, app=None, **kwargs):
        target = app if app is not None else self.app
        return self.loop.run_until_complete(_asgi_call_async(target, **kwargs))

    def _seed_db(self):
        for workspace_id in ("demo", "other"):
            upsert_workspace(
                self.conn,
                workspace_id=workspace_id,
                name=workspace_id.title(),
                path=self.tmp.name,
                harness_root=self.tmp.name,
            )
            upsert_workspace_host_profile(
                self.conn,
                workspace_id=workspace_id,
                host_id="mac",
                workspace_path=self.tmp.name,
                harness_root=self.tmp.name,
            )
        register_agent(
            self.conn, agent_id="mac-codex", host_id="mac", capabilities={}
        )
        self.conn.commit()

    def _clients(self):
        return [
            _client("alpha", TOKEN_ALPHA),
            _client(
                "beta",
                TOKEN_BETA,
                workspace_ids=["other"],
                platforms=["slack"],
                tools=[
                    "coordinate.operator_pending",
                    "coordinate.runtime_job_get",
                    "coordinate.runtime_request_submit",
                    "coordinate.task_create_record",
                    "coordinate.completion_prepare",
                    "coordinate.completion_preflight",
                    "coordinate.completion_claim",
                    "coordinate.completion_apply",
                    "coordinate.completion_consume",
                ],
            ),
            _client(
                "gamma",
                TOKEN_GAMMA,
                workspace_ids=[],
                platforms=[],
                tools=["coordinate.runtime_agent_list"],
            ),
        ]

    def _write_policy(self, clients):
        with open(self.policy_path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "clients": clients}, f)
        os.chmod(self.policy_path, 0o600)

    def _conn_factory(self):
        runtime = RuntimeInterface.from_config(
            RuntimeInterfaceConfig(db_path=self.db_path, actor="mcp-remote")
        )
        return runtime.connection_factory

    def _policy(self):
        return load_mcp_auth_policy(self.policy_path)

    def _build_app(self, **overrides):
        server = build_mcp_server(
            interface_provider=make_interface_provider(self._conn_factory())
        )
        kwargs = {
            "allowed_hosts": ["127.0.0.1:8766"],
            "allowed_origins": [],
            "max_concurrent": 16,
            "max_request_body_size": 4 * 1024 * 1024,
        }
        kwargs.update(overrides)
        return build_remote_mcp_app(
            server,
            policy=self._policy(),
            connection_factory=self._conn_factory(),
            **kwargs,
        )

    # -- transport: auth ----------------------------------------------------

    def test_missing_bad_unknown_token_all_identical_401(self):
        no_token = self._request(
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            body=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}).encode(),
        )
        bad = self._rpc(token="wrong-token", method="ping", raw=True)
        unknown = self._rpc(token=TOKEN_ALPHA + "-nope", method="ping", raw=True)
        self.assertEqual(no_token[0], 401)
        self.assertEqual(bad[0], 401)
        self.assertEqual(unknown[0], 401)
        self.assertEqual(no_token[2], bad[2])
        self.assertEqual(bad[2], unknown[2])
        for _, headers, body in (no_token, bad, unknown):
            self.assertNotIn(b"wrong-token", body)
            self.assertNotIn(TOKEN_ALPHA.encode(), body)
            self.assertIn("x-coordinate-request-id", headers)
        # The 401 body is the static JSON envelope, never the token.
        self.assertEqual(
            json.loads(no_token[2])["error"]["code"], "unauthorized"
        )

    def test_good_token_ping_and_discover(self):
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="server/discover",
            params={"_meta": _modern_meta()},
        )
        self.assertEqual(status, 200)
        self.assertEqual(response["id"], 1)
        self.assertIn("result", response)
        versions = response["result"]["supportedVersions"]
        self.assertTrue(any("2026-07-28" in v for v in versions))
        self.assertEqual(response["result"]["resultType"], "complete")

    # -- tools/list scope ---------------------------------------------------

    def test_tools_list_full_for_alpha(self):
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/list",
            params={"_meta": _modern_meta()},
        )
        self.assertEqual(status, 200)
        names = [t["name"] for t in response["result"]["tools"]]
        self.assertEqual(sorted(names), ALL_TOOLS)

    def test_tools_list_filtered_for_restricted_principal(self):
        status, _, response = self._rpc(
            token=TOKEN_GAMMA,
            method="tools/list",
            params={"_meta": _modern_meta()},
        )
        self.assertEqual(status, 200)
        names = [t["name"] for t in response["result"]["tools"]]
        self.assertEqual(names, ["coordinate.runtime_agent_list"])

    def test_eleven_schemas_identical_to_stdio_registration(self):
        server_remote = build_mcp_server(
            interface_provider=make_interface_provider(self._conn_factory())
        )
        from coordinate.agent_interface import AgentInterface

        server_stdio = build_mcp_server(
            AgentInterface(
                connection_factory=self._conn_factory(), actor="mcp"
            )
        )

        async def _dump(server):
            tools = await server.list_tools()
            return [
                {
                    "name": t.name,
                    "input_schema": t.input_schema,
                    "output_schema": t.output_schema,
                    "annotations": (
                        t.annotations.model_dump() if t.annotations else None
                    ),
                }
                for t in tools
            ]

        remote_tools = asyncio.run(_dump(server_remote))
        stdio_tools = asyncio.run(_dump(server_stdio))
        self.assertEqual(len(remote_tools), 12)
        self.assertEqual(
            sorted(t["name"] for t in remote_tools),
            sorted(t["name"] for t in stdio_tools),
        )
        for remote, stdio in zip(
            sorted(remote_tools, key=lambda t: t["name"]),
            sorted(stdio_tools, key=lambda t: t["name"]),
        ):
            self.assertEqual(remote, stdio)

    # -- tools/call scope ---------------------------------------------------

    def test_workspace_scoped_call_allowed_and_forbidden(self):
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.operator_pending", {"workspace_id": "demo"}),
        )
        self.assertEqual(status, 200)
        self.assertFalse(response["result"]["isError"])
        self.assertTrue(_envelope(response["result"])["ok"])

        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.operator_pending", {"workspace_id": "other"}),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertTrue(response["result"]["isError"])
        self.assertEqual(envelope["error"]["code"], "forbidden")
        self.assertIsNone(envelope["data"])
        # The forbidden body never echoes the requested workspace.
        self.assertNotIn(b"other", self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.operator_pending", {"workspace_id": "other"}),
            raw=True,
        )[2])

    def test_channel_create_requires_discord_platform_and_returns_pending(self):
        args = {
            "input": {
                "workspace_id": "demo",
                "channel_name": "demo-project",
                "idempotency_key": "remote-request-1",
            }
        }
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.channel_create", args),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(envelope["data"]["status"], "pending")

        clients = self._clients()
        clients[0]["platforms"] = []
        self._write_policy(clients)
        denied_app = self._build_app()
        denied_lifespan = self.loop.run_until_complete(_lifespan_start(denied_app))
        self.addCleanup(
            lambda: self.loop.run_until_complete(_lifespan_stop(denied_lifespan))
        )
        status, _, response = self._rpc(
            app=denied_app,
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.channel_create", {
                "input": {
                    **args["input"],
                    "idempotency_key": "remote-request-2",
                }
            }),
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            _envelope(response["result"])["error"]["code"], "forbidden"
        )

    def test_tool_not_granted_is_forbidden(self):
        status, _, response = self._rpc(
            token=TOKEN_GAMMA,
            method="tools/call",
            params=_tool_call("coordinate.operator_pending", {"workspace_id": "demo"}),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")

    def test_unknown_tool_is_forbidden(self):
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.nope", {}),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")

    # -- R5B: six typed tools -----------------------------------------------

    R5B_TOOLS = [
        "coordinate.task_create_record",
        "coordinate.completion_prepare",
        "coordinate.completion_preflight",
        "coordinate.completion_claim",
        "coordinate.completion_apply",
        "coordinate.completion_consume",
    ]

    def _r5b_args(self, name: str, workspace_id: str, **fields) -> dict:
        base = {
            "coordinate.task_create_record": {
                "operation_id": "00000000-0000-0000-0000-000000000000",
                "input_fingerprint": "a" * 64,
                "before_fingerprint": "b" * 64,
                "after_fingerprint": "c" * 64,
                "task_id": "t1",
                "plan_doc": "plans/p.md",
            },
            "coordinate.completion_prepare": {"task_id": "t1"},
            "coordinate.completion_preflight": {"receipt_id": "r1"},
            "coordinate.completion_claim": {
                "receipt_id": "r1",
                "task_id": "t1",
                "before_fingerprint": "b" * 64,
                "expected_after_fingerprint": "c" * 64,
            },
            "coordinate.completion_apply": {
                "receipt_id": "r1",
                "task_id": "t1",
                "after_fingerprint": "c" * 64,
            },
            "coordinate.completion_consume": {"receipt_id": "r1"},
        }[name]
        return {"input": {"workspace_id": workspace_id, **base, **fields}}

    def test_r5b_six_tools_all_workspace_scoped(self):
        """Every one of the six tools is workspace-scoped: a workspace outside
        the principal's set is a static forbidden before the body runs."""
        for name in self.R5B_TOOLS:
            with self.subTest(tool=name):
                status, _, response = self._rpc(
                    token=TOKEN_ALPHA,
                    method="tools/call",
                    params=_tool_call(name, self._r5b_args(name, "other")),
                )
                self.assertEqual(status, 200)
                envelope = _envelope(response["result"])
                self.assertTrue(response["result"]["isError"])
                self.assertEqual(envelope["error"]["code"], "forbidden")
                self.assertIsNone(envelope["data"])
                # The forbidden body never echoes the requested workspace.
                raw = self._rpc(
                    token=TOKEN_ALPHA,
                    method="tools/call",
                    params=_tool_call(name, self._r5b_args(name, "other")),
                    raw=True,
                )
                self.assertNotIn(b"other", raw[2])

    def test_r5b_scoped_workspace_reaches_domain_not_forbidden(self):
        """Within the principal's workspace the middleware passes and the
        domain answers with its own error (proving the body ran)."""
        for name, expected in [
            ("coordinate.task_create_record", "files_not_deployed"),
            ("coordinate.completion_prepare", "gate_not_passed"),
            ("coordinate.completion_preflight", "unknown_receipt"),
            ("coordinate.completion_claim", "unknown_receipt"),
            ("coordinate.completion_apply", "unknown_receipt"),
            ("coordinate.completion_consume", "unknown_receipt"),
        ]:
            with self.subTest(tool=name):
                status, _, response = self._rpc(
                    token=TOKEN_ALPHA,
                    method="tools/call",
                    params=_tool_call(name, self._r5b_args(name, "demo")),
                )
                self.assertEqual(status, 200)
                envelope = _envelope(response["result"])
                self.assertTrue(response["result"]["isError"])
                self.assertEqual(envelope["error"]["code"], expected)

    def test_r5b_typed_input_shape_is_exact(self):
        """The six tools take exactly one ``input`` object; a malformed wire
        shape is forbidden by the middleware before the body runs."""
        malformed = [
            {"input": 5},
            {"input": None},
            {"input": {"workspace_id": "demo"}, "extra": 1},
            {"workspace_id": "demo"},
            {},
        ]
        for name in self.R5B_TOOLS:
            for arguments in malformed:
                with self.subTest(tool=name, arguments=arguments):
                    status, _, response = self._rpc(
                        token=TOKEN_ALPHA,
                        method="tools/call",
                        params=_tool_call(name, arguments),
                    )
                    self.assertEqual(status, 200)
                    envelope = _envelope(response["result"])
                    self.assertEqual(
                        envelope["error"]["code"], "forbidden",
                        f"{name} {arguments}",
                    )

    def test_r5b_sdk_rejects_incomplete_input(self):
        """An unwrapped-but-incomplete input reaches the SDK argument model
        and fails closed there (bounded is_error, no traceback)."""
        incomplete = [
            {"input": {}},
            {"input": {"workspace_id": "demo"}},
        ]
        for name in self.R5B_TOOLS:
            for arguments in incomplete:
                with self.subTest(tool=name, arguments=arguments):
                    status, _, response = self._rpc(
                        token=TOKEN_ALPHA,
                        method="tools/call",
                        params=_tool_call(name, arguments),
                    )
                    self.assertEqual(status, 200)
                    result = response["result"]
                    self.assertTrue(result["isError"], f"{name} {arguments}")
                    text = result["content"][0]["text"]
                    self.assertNotIn("Traceback", text)

    def _seed_receipt(self, conn, *, workspace_id="demo", task_id="t1",
                      actor="mcp-remote:alpha"):
        """Write a real completion.authorized receipt directly (fake gate
        adapter); the middleware/domain binding tests operate on it."""
        from coordinate.completion import prepare_completion_receipt
        from coordinate.db import get_workspace

        workspace = get_workspace(conn, workspace_id)

        class _FakeAdapter:
            def __init__(self, workspace):
                self.workspace = workspace

            def refresh_state(self):
                return {"current_item": {
                    "id": task_id,
                    "workflow": {"status": "review_approved", "branch": "feat-x"},
                    "status": "doing",
                }}

            def read_state(self):
                return self.refresh_state()

            def read_checklist(self):
                from coordinate.completion import compute_item_fingerprint

                item = {
                    "id": task_id,
                    "title": "Task",
                    "status": "doing",
                    "priority": "p1",
                    "workflow": {"status": "review_approved", "branch": "feat-x"},
                    "verification": "",
                    "owner": None,
                    "selected_in_session": None,
                }
                return {"items": [item]}

        return prepare_completion_receipt(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            requester=actor,
            authorized_actor=actor,
            adapter=_FakeAdapter(workspace),
        )

    def test_r5b_consume_workspace_scope_then_domain_binding(self):
        """completion_consume: middleware scopes the caller's workspace_id
        first; the domain then binds it against the receipt payload."""
        receipt = self._seed_receipt(self.conn)

        # Alpha (workspace demo) with the matching workspace: middleware
        # passes, domain runs (receipt never applied -> not_applied).
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_consume",
                {"input": {
                    "workspace_id": "demo",
                    "receipt_id": receipt.receipt_id,
                }},
            ),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "not_applied")

        # Alpha with a workspace outside its scope: static forbidden.
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_consume",
                {"input": {
                    "workspace_id": "other",
                    "receipt_id": receipt.receipt_id,
                }},
            ),
        )
        self.assertEqual(_envelope(response["result"])["error"]["code"], "forbidden")

        # Beta (scoped to other) with the demo receipt: middleware passes,
        # domain binding rejects with workspace_mismatch.
        status, _, response = self._rpc(
            token=TOKEN_BETA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_consume",
                {"input": {
                    "workspace_id": "other",
                    "receipt_id": receipt.receipt_id,
                }},
            ),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "workspace_mismatch")

    def test_r5b_preflight_workspace_scope_then_domain_binding(self):
        receipt = self._seed_receipt(self.conn)

        # Alpha scoped to demo: preflight resolves the receipt.
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_preflight",
                {"input": {
                    "workspace_id": "demo",
                    "receipt_id": receipt.receipt_id,
                }},
            ),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"])
        self.assertTrue(envelope["data"]["ok"])
        self.assertEqual(envelope["data"]["workspace_id"], "demo")
        self.assertEqual(envelope["data"]["status"], "authorized")

        # Alpha with workspace outside scope: static forbidden.
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_preflight",
                {"input": {
                    "workspace_id": "other",
                    "receipt_id": receipt.receipt_id,
                }},
            ),
        )
        self.assertEqual(_envelope(response["result"])["error"]["code"], "forbidden")

        # Beta scoped to other: middleware passes, domain binding rejects.
        status, _, response = self._rpc(
            token=TOKEN_BETA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_preflight",
                {"input": {
                    "workspace_id": "other",
                    "receipt_id": receipt.receipt_id,
                }},
            ),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "workspace_mismatch")

    def test_r5b_claim_uses_principal_actor(self):
        """The receipt's authorized_actor is the derived principal actor; a
        claim by the same principal succeeds, and no caller field can change
        the actor."""
        receipt = self._seed_receipt(self.conn)
        fps = {
            "before_fingerprint": receipt.harness_fingerprint,
            "expected_after_fingerprint": "c" * 64,
        }
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_claim",
                {"input": {
                    "workspace_id": "demo",
                    "receipt_id": receipt.receipt_id,
                    "task_id": "t1",
                    **fps,
                }},
            ),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(envelope["data"]["authorized_actor"], "mcp-remote:alpha")

        # A forged actor field is rejected by the SDK argument model before
        # the body runs (bounded is_error, never honored).
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call(
                "coordinate.completion_claim",
                {"input": {
                    "workspace_id": "demo",
                    "receipt_id": receipt.receipt_id,
                    "task_id": "t1",
                    **fps,
                    "actor": "intruder",
                }},
            ),
        )
        self.assertEqual(status, 200)
        self.assertTrue(response["result"]["isError"])
        text = response["result"]["content"][0]["text"]
        self.assertNotIn("Traceback", text)
        self.assertNotIn("completion.claimed", text)

    def test_r5b_forbidden_never_invokes_domain(self):
        """A provider that raises on invocation proves forbidden responses
        are produced before the body can run."""
        calls: list[str] = []

        def _exploding_provider():
            calls.append("provider-called")
            raise AssertionError("domain must not be invoked")

        from coordinate.mcp_server import build_mcp_server as _build

        server = _build(interface_provider=_exploding_provider)
        app = build_remote_mcp_app(
            server,
            policy=self._policy(),
            connection_factory=self._conn_factory(),
            allowed_hosts=["127.0.0.1:8766"],
            allowed_origins=[],
        )
        extra_lifespan = self.loop.run_until_complete(_lifespan_start(app))
        self.addCleanup(
            lambda: self.loop.run_until_complete(_lifespan_stop(extra_lifespan))
        )
        for name in self.R5B_TOOLS:
            with self.subTest(tool=name):
                status, _, response = self._rpc(
                    app=app,
                    token=TOKEN_ALPHA,
                    method="tools/call",
                    params=_tool_call(name, self._r5b_args(name, "other")),
                )
                self.assertEqual(status, 200)
                self.assertEqual(
                    _envelope(response["result"])["error"]["code"], "forbidden",
                )
        # Malformed input shapes are refused by the auth/scope middleware
        # (or the shared shape gate) before the provider can run.
        for name in self.R5B_TOOLS:
            for arguments in ({"input": 5}, {"input": {}}, {"workspace_id": "demo"}):
                with self.subTest(tool=name, arguments=arguments):
                    status, _, response = self._rpc(
                        app=app,
                        token=TOKEN_ALPHA,
                        method="tools/call",
                        params=_tool_call(name, arguments),
                    )
                    self.assertEqual(status, 200)
                    envelope = _envelope(response["result"])
                    self.assertIn(
                        envelope["error"]["code"], ("forbidden", "invalid_request"),
                    )
        self.assertEqual(calls, [])

    def test_r5b_auth_runs_before_common_shape_gate(self):
        """C2: an unauthorized caller probing a malformed shape gets the
        static forbidden — the per-principal auth/scope middleware runs
        before the shared shape gate, so tool existence/shape cannot be
        probed."""
        for name in self.R5B_TOOLS:
            with self.subTest(tool=name):
                status, _, response = self._rpc(
                    token=TOKEN_GAMMA,
                    method="tools/call",
                    params=_tool_call(name, {"input": 5}),
                )
                self.assertEqual(status, 200)
                envelope = _envelope(response["result"])
                self.assertEqual(envelope["error"]["code"], "forbidden")
                self.assertIsNone(envelope["data"])

    def test_agent_list_global_grant_only_with_explicit_tool(self):
        status, _, response = self._rpc(
            token=TOKEN_GAMMA,
            method="tools/call",
            params=_tool_call("coordinate.runtime_agent_list", None),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["data"]["agents"][0]["id"], "mac-codex")

        # beta lacks the grant even though it is a workspace-scoped client.
        status, _, response = self._rpc(
            token=TOKEN_BETA,
            method="tools/call",
            params=_tool_call("coordinate.runtime_agent_list", None),
        )
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")

    def test_job_get_missing_job_is_not_found(self):
        status, _, response = self._rpc(
            token=TOKEN_BETA,
            method="tools/call",
            params=_tool_call("coordinate.runtime_job_get", {"job_id": "request:missing"}),
        )
        self.assertEqual(status, 200)
        envelope = _envelope(response["result"])
        self.assertTrue(response["result"]["isError"])
        self.assertEqual(envelope["error"]["code"], "not_found")

    def _create_job(self, workspace_id: str, key: str) -> str:
        _sync_catalog(self.conn, ["mac-codex"])
        result = submit_request(
            self.conn,
            workspace_id=workspace_id,
            target_agent="mac-codex",
            prompt="r3 job",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": f"m-{key}",
                "session_scope_id": "discord:ch",
            },
            reply={"platform": "none", "destination": "ch"},
            actor="test-seed",
            idempotency_key=f"key-{key}",
        )
        self.conn.commit()
        return result.job["id"]

    def test_job_get_own_workspace_ok(self):
        job_id = self._create_job("other", "own")
        status, _, response = self._rpc(
            token=TOKEN_BETA,
            method="tools/call",
            params=_tool_call("coordinate.runtime_job_get", {"job_id": job_id}),
        )
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["data"]["workspace_id"], "other")

    def test_job_get_cross_workspace_same_shape_as_missing(self):
        job_id = self._create_job("other", "cross")
        missing = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.runtime_job_get", {"job_id": "request:missing"}),
        )
        cross = self._rpc(
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call("coordinate.runtime_job_get", {"job_id": job_id}),
        )
        self.assertEqual(missing[0], cross[0])
        self.assertEqual(_envelope(missing[2]["result"]), _envelope(cross[2]["result"]))
        envelope = _envelope(cross[2]["result"])
        self.assertEqual(envelope["error"]["code"], "not_found")
        self.assertIsNone(envelope["data"])

    # -- runtime_request_submit scope --------------------------------------

    def _submit_params(self, **overrides) -> dict:
        params = _tool_call(
            "coordinate.runtime_request_submit",
            {
                "workspace_id": "demo",
                "prompt": "r3 submit",
                "origin": {
                    "platform": "discord",
                    "destination": "ch-alpha",
                    "message_id": "m-s1",
                    "session_scope_id": "discord:ch-alpha",
                },
                "reply": {"platform": "none", "destination": "ch-alpha"},
                "target_agent": "mac-codex",
                "idempotency_key": "s1",
            },
        )
        params["arguments"].update(overrides)
        return params

    def test_submit_origin_platform_out_of_scope_forbidden(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["origin"] = {
            "platform": "slack",
            "destination": "ch",
            "message_id": "m-s2",
        }
        status, _, response = self._rpc(
            token=TOKEN_ALPHA, method="tools/call", params=params
        )
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")

    def test_submit_reply_cross_platform_forbidden(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["reply"] = {
            "platform": "kook",
            "destination": "ch",
        }
        status, _, response = self._rpc(
            token=TOKEN_ALPHA, method="tools/call", params=params
        )
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")

    def test_submit_reply_none_sentinel_allowed(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["reply"] = {"platform": "none", "destination": "ch"}
        status, _, response = self._rpc(
            token=TOKEN_ALPHA, method="tools/call", params=params
        )
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"])
        # Mutation landed with the derived principal actor.
        events = list_events(self.conn)
        self.assertTrue(
            any(
                ev["actor"] == "mcp-remote:alpha"
                for ev in events
                if ev["event_type"].startswith("request.")
            )
        )

    def test_submit_channel_binding_to_other_workspace_forbidden(self):
        _sync_catalog(self.conn, ["mac-codex"])
        bind_channel_workspace(
            self.conn,
            platform="discord",
            channel_id="ch-other",
            workspace_id="other",
            actor="test",
            reason="test",
            idempotency_key="bind-other",
        )
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["origin"] = {
            "platform": "discord",
            "destination": "ch-other",
            "message_id": "m-s3",
        }
        status, _, response = self._rpc(
            token=TOKEN_ALPHA, method="tools/call", params=params
        )
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")

    def test_submit_channel_binding_to_same_workspace_allowed(self):
        _sync_catalog(self.conn, ["mac-codex"])
        bind_channel_workspace(
            self.conn,
            platform="discord",
            channel_id="ch-alpha",
            workspace_id="demo",
            actor="test",
            reason="test",
            idempotency_key="bind-alpha",
        )
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["origin"]["destination"] = "ch-alpha"
        status, _, response = self._rpc(
            token=TOKEN_ALPHA, method="tools/call", params=params
        )
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"])

    def test_submit_reply_channel_binding_to_other_workspace_forbidden(self):
        _sync_catalog(self.conn, ["mac-codex"])
        bind_channel_workspace(
            self.conn,
            platform="discord",
            channel_id="ch-reply-other",
            workspace_id="other",
            actor="test",
            reason="test",
            idempotency_key="bind-reply-other",
        )
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["reply"] = {
            "platform": "discord",
            "destination": "ch-reply-other",
        }
        status, _, response = self._rpc(
            token=TOKEN_ALPHA, method="tools/call", params=params
        )
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")

    def test_submit_unbound_destination_passes_auth(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["origin"]["destination"] = "ch-nobinding"
        status, _, response = self._rpc(
            token=TOKEN_ALPHA, method="tools/call", params=params
        )
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"])

    def test_channel_lookup_error_fails_closed(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["origin"]["destination"] = "ch-x"
        with unittest.mock.patch.object(
            RuntimeInterface,
            "resolve_channel_workspace",
            return_value={
                "ok": False,
                "data": None,
                "error": {"code": "internal", "message": "internal error"},
            },
        ):
            status, _, response = self._rpc(
                token=TOKEN_ALPHA, method="tools/call", params=params
            )
        envelope = _envelope(response["result"])
        self.assertEqual(envelope["error"]["code"], "forbidden")
        self.assertIsNone(envelope["data"])

    def test_channel_lookup_malformed_results_fail_closed(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        malformed = [
            {"ok": True, "data": {"bound": True, "binding": None}},
            {"ok": True, "data": "junk"},
            {"ok": True, "data": None},
            {"ok": True},
            {"ok": True, "data": {"bound": None}},
            "not-a-dict",
        ]
        for case in malformed:
            with self.subTest(case=case):
                params = self._submit_params()
                params["arguments"]["origin"]["destination"] = "ch-x"
                with unittest.mock.patch.object(
                    RuntimeInterface,
                    "resolve_channel_workspace",
                    return_value=case,
                ):
                    status, _, response = self._rpc(
                        token=TOKEN_ALPHA, method="tools/call", params=params
                    )
                envelope = _envelope(response["result"])
                self.assertEqual(envelope["error"]["code"], "forbidden")

    def test_channel_lookup_explicit_unbound_allowed(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        params = self._submit_params()
        params["arguments"]["origin"]["destination"] = "ch-x"
        with unittest.mock.patch.object(
            RuntimeInterface,
            "resolve_channel_workspace",
            return_value={"ok": True, "data": {"bound": False, "binding": None}},
        ):
            status, _, response = self._rpc(
                token=TOKEN_ALPHA, method="tools/call", params=params
            )
        envelope = _envelope(response["result"])
        self.assertTrue(envelope["ok"])

    # -- transport policy: host/origin/body/concurrency ---------------------

    def test_wrong_host_rejected(self):
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="server/discover",
            params={"_meta": _modern_meta()},
            extra_headers={"Host": "evil.example"},
            raw=True,
        )
        self.assertEqual(status, 421)

    def test_origin_policy_exact_match(self):
        app = self._build_app(allowed_origins=["https://allowed.example"])
        handle = self.loop.run_until_complete(_lifespan_start(app))
        self.addCleanup(
            lambda: self.loop.run_until_complete(_lifespan_stop(handle))
        )
        status, _, _ = self._rpc(
            app,
            token=TOKEN_ALPHA,
            method="server/discover",
            params={"_meta": _modern_meta()},
            extra_headers={"Origin": "https://allowed.example"},
        )
        self.assertEqual(status, 200)
        status, _, _ = self._rpc(
            app,
            token=TOKEN_ALPHA,
            method="server/discover",
            params={"_meta": _modern_meta()},
            extra_headers={"Origin": "https://evil.example"},
        )
        self.assertEqual(status, 403)

    def test_missing_origin_allowed(self):
        status, _, response = self._rpc(
            token=TOKEN_ALPHA,
            method="server/discover",
            params={"_meta": _modern_meta()},
        )
        self.assertEqual(status, 200)

    def test_body_over_limit_rejected(self):
        app = self._build_app(max_request_body_size=2048)
        handle = self.loop.run_until_complete(_lifespan_start(app))
        self.addCleanup(
            lambda: self.loop.run_until_complete(_lifespan_stop(handle))
        )
        status, _, _ = self._rpc(
            app,
            token=TOKEN_ALPHA,
            method="tools/call",
            params=_tool_call(
                "coordinate.operator_pending",
                {"workspace_id": "demo", "pad": "x" * 4096},
            ),
            raw=True,
        )
        self.assertEqual(status, 413)

    # -- context isolation --------------------------------------------------

    def test_principal_context_reset_after_request(self):
        class _Probe:
            def __init__(self, inner):
                self.inner = inner

            async def __call__(self, scope, receive, send):
                self.before = get_request_principal()
                await self.inner(scope, receive, send)
                self.after = get_request_principal()

        probe = _Probe(self.app)
        status, _, _ = self._rpc(
            probe,
            token=TOKEN_ALPHA,
            method="server/discover",
            params={"_meta": _modern_meta()},
        )
        self.assertEqual(status, 200)
        self.assertIsNone(probe.before)
        self.assertIsNone(probe.after)
        self.assertIsNone(get_request_principal())

    def test_concurrent_requests_see_own_principal(self):
        async def _run():
            def one(token, workspace_id):
                return _asgi_call_async(
                    self.app,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                        "Authorization": f"Bearer {token}",
                    },
                    body=json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "tools/call",
                            "params": _tool_call(
                                "coordinate.operator_pending",
                                {"workspace_id": workspace_id},
                            ),
                        }
                    ).encode(),
                )

            results = await asyncio.gather(
                one(TOKEN_ALPHA, "demo"), one(TOKEN_BETA, "other")
            )
            return results

        (alpha_status, _, alpha_body), (beta_status, _, beta_body) = (
            self.loop.run_until_complete(_run())
        )
        self.assertEqual(alpha_status, 200)
        self.assertEqual(beta_status, 200)
        alpha_event = _sse_events(alpha_body)[0]
        beta_event = _sse_events(beta_body)[0]
        self.assertTrue(_envelope(alpha_event["result"])["ok"])
        self.assertTrue(_envelope(beta_event["result"])["ok"])
        self.assertIsNone(get_request_principal())

    def test_access_log_redacts_token_prompt_and_db_path(self):
        with self.assertLogs("coordinate.mcp_remote", level="INFO") as logs:
            self._rpc(
                token="totally-wrong",
                method="server/discover",
                params={"_meta": _modern_meta()},
            )
            self._rpc(
                token=TOKEN_ALPHA,
                method="tools/call",
                params=_tool_call(
                    "coordinate.operator_pending", {"workspace_id": "other"}
                ),
            )
            self._rpc(
                token=TOKEN_ALPHA,
                method="tools/call",
                params=_tool_call(
                    "coordinate.runtime_request_submit",
                    self._submit_params()["arguments"],
                ),
            )
        rendered = "\n".join(logs.output)
        self.assertNotIn("totally-wrong", rendered)
        self.assertNotIn(TOKEN_ALPHA, rendered)
        self.assertNotIn("r3 submit", rendered)
        self.assertNotIn(self.db_path, rendered)
        self.assertIn("client=alpha", rendered)
        self.assertIn("client=-", rendered)
        self.assertIn("tool=coordinate.operator_pending", rendered)

    def test_access_log_single_line_with_injected_tool_name(self):
        with self.assertLogs("coordinate.mcp_remote", level="INFO") as logs:
            status, _, response = self._rpc(
                token=TOKEN_ALPHA,
                method="tools/call",
                params=_tool_call(
                    "coordinate.operator_pending\nEVIL", {"workspace_id": "demo"}
                ),
            )
            self.assertEqual(status, 200)
            envelope = _envelope(response["result"])
            self.assertEqual(envelope["error"]["code"], "forbidden")
            # Unknown method with an injected name is refused, not served.
            status2, _, _ = self._rpc(
                token=TOKEN_ALPHA,
                method="tools/list\nEVIL",
                params={"_meta": _modern_meta()},
                raw=True,
            )
            self.assertNotEqual(status2, 200)
        rendered = "\n".join(logs.output)
        self.assertNotIn("\nEVIL", rendered)
        for line in logs.output:
            self.assertEqual(line.count("\n"), 0)
            self.assertLess(len(line), 250)


class RemoteMCPAppUnitTests(unittest.TestCase):
    """Middleware-only tests with a stub SDK app (no MCP SDK needed)."""

    def _stub_policy(self):
        class StubPolicy:
            def authenticate(self, token):
                if token == "ok":
                    return McpPrincipal(
                        client_id="c1",
                        token_sha256="0" * 64,
                        workspace_ids=frozenset(),
                        platforms=frozenset(),
                        tools=frozenset(),
                    )
                return None

        return StubPolicy()

    def test_unauthenticated_requests_never_reach_app(self):
        calls: list = []

        class StubApp:
            async def __call__(self, scope, receive, send):
                calls.append(scope)
                await send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": [(b"content-type", b"application/json")],
                    }
                )
                await send(
                    {"type": "http.response.body", "body": b"{}"}
                )

        app = RemoteMCPApp(
            StubApp(), policy=self._stub_policy(), max_concurrent=1
        )
        status, _, _ = _asgi_call(
            app,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            body=b"{}",
        )
        self.assertEqual(status, 401)
        self.assertEqual(calls, [])

    def test_concurrency_exhaustion_fails_closed_with_503(self):
        gate = None
        entered = 0

        class StubApp:
            async def __call__(self, scope, receive, send):
                nonlocal entered
                entered += 1
                await gate.wait()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": [(b"content-type", b"application/json")],
                    }
                )
                await send({"type": "http.response.body", "body": b"{}"})

        app = RemoteMCPApp(
            StubApp(), policy=self._stub_policy(), max_concurrent=1
        )

        async def _run():
            nonlocal gate
            gate = asyncio.Event()
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "Authorization": "Bearer ok",
            }
            first = asyncio.create_task(
                _asgi_call_async(app, headers=headers, body=b"{}")
            )
            for _ in range(200):
                if entered:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(entered, 1)
            second = await _asgi_call_async(app, headers=headers, body=b"{}")
            gate.set()
            first_result = await first
            return first_result, second

        (first_status, _, _), (second_status, _, second_body) = asyncio.run(_run())
        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 503)
        self.assertEqual(
            json.loads(second_body)["error"]["code"], "unavailable"
        )
        self.assertEqual(entered, 1)

    def test_request_id_header_present_on_every_response(self):
        class StubApp:
            async def __call__(self, scope, receive, send):
                await send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": [(b"content-type", b"application/json")],
                    }
                )
                await send({"type": "http.response.body", "body": b"{}"})

        app = RemoteMCPApp(
            StubApp(), policy=self._stub_policy(), max_concurrent=1
        )
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": "Bearer ok",
        }
        _, resp_headers, _ = _asgi_call(app, headers=headers, body=b"{}")
        self.assertIn("x-coordinate-request-id", resp_headers)
        self.assertTrue(resp_headers["x-coordinate-request-id"])


class RemoteShapeFailClosedTests(unittest.TestCase):
    """Fail-closed shape handling of middleware results (no SDK required)."""

    def test_list_tools_malformed_result_returns_empty(self):
        from coordinate.mcp_remote import _filter_list_tools_wire_result

        allowed = frozenset({"coordinate.operator_pending"})
        self.assertEqual(_filter_list_tools_wire_result("junk", allowed), {"tools": []})
        self.assertEqual(_filter_list_tools_wire_result(None, allowed), {"tools": []})
        self.assertEqual(
            _filter_list_tools_wire_result({"tools": None}, allowed), {"tools": []}
        )
        self.assertEqual(
            _filter_list_tools_wire_result({"junk": 1}, allowed), {"tools": []}
        )

    def test_list_tools_filters_and_keeps_other_keys(self):
        from coordinate.mcp_remote import _filter_list_tools_wire_result

        allowed = frozenset({"coordinate.operator_pending"})
        result = _filter_list_tools_wire_result(
            {
                "resultType": "complete",
                "tools": [
                    {"name": "coordinate.operator_pending"},
                    {"name": "coordinate.runtime_agent_list"},
                    {"name": 5},
                    "junk",
                ],
            },
            allowed,
        )
        self.assertEqual(
            result["tools"], [{"name": "coordinate.operator_pending"}]
        )
        self.assertEqual(result["resultType"], "complete")

    def test_job_get_malformed_result_returns_not_found_shape(self):
        from coordinate.mcp_remote import _checked_job_get_wire_result

        not_found = {
            "structuredContent": {
                "ok": False,
                "data": None,
                "error": {"code": "not_found", "message": "job not found"},
            }
        }
        allowed = frozenset({"demo"})
        for malformed in (
            "junk",
            None,
            {"structuredContent": None},
            {},
            {"structuredContent": "junk"},
            {"structuredContent": {"ok": True, "data": "junk"}},
            {"structuredContent": {"ok": True, "data": None}},
            {"structuredContent": {"ok": True, "data": {}}},
            {"structuredContent": {"ok": True, "data": {"workspace_id": "other"}}},
        ):
            with self.subTest(case=malformed):
                self.assertIs(
                    _checked_job_get_wire_result(malformed, allowed, not_found),
                    not_found,
                )

    def test_job_get_allowed_and_domain_error_pass_through(self):
        from coordinate.mcp_remote import _checked_job_get_wire_result

        allowed = frozenset({"demo"})
        ok_env = {
            "structuredContent": {"ok": True, "data": {"workspace_id": "demo"}}
        }
        self.assertIs(
            _checked_job_get_wire_result(ok_env, allowed, "sentinel"), ok_env
        )
        error_env = {
            "structuredContent": {
                "ok": False,
                "data": None,
                "error": {"code": "internal", "message": "internal error"},
            }
        }
        self.assertIs(
            _checked_job_get_wire_result(error_env, allowed, "sentinel"),
            error_env,
        )


class RemoteAccessLogSanitizationTests(unittest.TestCase):
    """Access-log fields must stay single-line, bounded and printable."""

    def test_safe_log_field_rendering(self):
        from coordinate.mcp_remote import _LOG_FIELD_LIMIT, _safe_log_field

        self.assertEqual(_safe_log_field("tools/call"), "tools/call")
        self.assertEqual(_safe_log_field(None), "-")
        self.assertEqual(_safe_log_field(""), "-")
        self.assertEqual(_safe_log_field("   "), "-")
        self.assertEqual(_safe_log_field("bad\ntool\r\nEVIL\t"), "bad?tool??EVIL?")
        self.assertNotIn("\n", _safe_log_field("a\nb"))
        self.assertNotIn("\r", _safe_log_field("a\rb"))
        self.assertNotIn("\t", _safe_log_field("a\tb"))
        long_value = "x" * 1000
        self.assertEqual(len(_safe_log_field(long_value)), _LOG_FIELD_LIMIT)

    def test_log_access_renders_single_line_with_hostile_fields(self):
        from coordinate.mcp_remote import RemoteMCPApp

        principal = McpPrincipal(
            client_id="evil\nclient\rname",
            token_sha256="0" * 64,
            workspace_ids=frozenset(),
            platforms=frozenset(),
            tools=frozenset(),
        )
        with self.assertLogs("coordinate.mcp_remote", level="INFO") as logs:
            RemoteMCPApp._log_access(
                "req-123", principal, "tools/call\nEVIL", "bad\ttool", 403, 0.0
            )
        rendered = "\n".join(logs.output)
        self.assertNotIn("\nclient", rendered)
        self.assertNotIn("\nEVIL", rendered)
        self.assertNotIn("\t", rendered)
        for line in logs.output:
            self.assertEqual(line.count("\n"), 0)
            self.assertLess(len(line), 250)


class McpRemotePublicApiTests(unittest.TestCase):
    """Module-level contract checks that need no SDK and no server."""

    def test_principal_requires_all_five_fields(self):
        principal = McpPrincipal(
            client_id="a",
            token_sha256="1" * 64,
            workspace_ids=frozenset({"w"}),
            platforms=frozenset({"discord"}),
            tools=frozenset({"coordinate.operator_pending"}),
        )
        self.assertEqual(principal.actor, "mcp-remote:a")

    def test_default_max_concurrent_matches_r2a_derived_profile(self):
        self.assertEqual(DEFAULT_MAX_CONCURRENT, 16)

    def test_loopback_host_set_reused_from_r2a(self):
        self.assertEqual(LOOPBACK_HOSTS, frozenset({"127.0.0.1", "::1", "localhost"}))

    def test_mcp_http_systemd_unit_contract(self):
        """The R3 deploy unit stays fail-safe: dual condition files, coord
        user, loopback-only bind, env-injected exact allowed host, hardening
        no weaker than the runtime-http unit, no plaintext token material."""
        unit_path = REPO_ROOT / "deploy" / "systemd" / "coordinate-mcp-http.service"
        text = unit_path.read_text(encoding="utf-8")
        sections: dict[str, list[str]] = {}
        current = None
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                current = stripped[1:-1]
                sections.setdefault(current, [])
            elif current and stripped and not stripped.startswith("#"):
                sections[current].append(stripped)
        unit_keys = {line.split("=", 1)[0] for line in sections["Unit"]}
        service_keys = {line.split("=", 1)[0] for line in sections["Service"]}

        # Dual fail-safe conditions: digest policy AND env file must both
        # exist before the unit may start; StartLimit stays in [Unit].
        self.assertIn("ConditionPathExists", unit_keys)
        conditions = {
            line.split("=", 1)[1] for line in sections["Unit"]
            if line.startswith("ConditionPathExists=")
        }
        self.assertEqual(
            conditions,
            {
                "/etc/coordinate/mcp-http-clients.json",
                "/etc/coordinate/mcp-http.env",
            },
        )
        self.assertIn("StartLimitIntervalSec", unit_keys)
        self.assertIn("StartLimitBurst", unit_keys)
        self.assertNotIn("StartLimitIntervalSec", service_keys)
        self.assertNotIn("StartLimitBurst", service_keys)
        self.assertIn("After=network-online.target", sections["Unit"])

        # User/hardening must be at least as strong as the runtime-http unit.
        self.assertIn("User=coord", sections["Service"])
        self.assertIn("Group=coord", sections["Service"])
        for key in (
            "NoNewPrivileges=true",
            "PrivateTmp=true",
            "ProtectSystem=strict",
            "ProtectHome=true",
            "ReadWritePaths=/var/lib/coordinate",
        ):
            self.assertIn(key, sections["Service"])

        # Loopback-only bind, exact CLI surface, env-injected allowed host:
        # never hardcoded, never a wildcard.
        exec_start = next(
            line.split("=", 1)[1] for line in sections["Service"]
            if line.startswith("ExecStart=")
        )
        self.assertIn("--host 127.0.0.1", exec_start)
        self.assertIn("--port 8766", exec_start)
        self.assertIn("--path /mcp", exec_start)
        self.assertIn(
            "--auth-file /etc/coordinate/mcp-http-clients.json", exec_start
        )
        self.assertIn(
            "--allowed-host ${MCP_ALLOWED_HOST}", exec_start
        )
        self.assertNotIn("*", exec_start)
        self.assertIn(
            "EnvironmentFile=/etc/coordinate/mcp-http.env", sections["Service"]
        )

        # No plaintext token material anywhere in the unit: no digest key,
        # no authorization header, no 64-hex digest value.
        self.assertNotIn("token_sha256", text)
        self.assertNotIn("Authorization", text)
        self.assertNotRegex(text, r"[0-9a-f]{64}")


if __name__ == "__main__":
    unittest.main()
