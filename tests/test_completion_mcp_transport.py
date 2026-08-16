"""R5B completion-only Remote MCP transport tests.

The transport is the narrow client ``mark-done-files`` uses instead of the
SSH event CLI: it calls exactly three fixed tools (completion_preflight /
completion_claim / completion_apply) over the official MCP streamable HTTP
client against a real in-process server, with the bearer token read only
from a caller-named environment variable.

Covered here:

- real end-to-end preflight/claim/apply against the R3 remote app;
- domain reasons survive the wire (unknown_receipt, before_fingerprint_mismatch);
- fail-closed transport errors: missing token, bad credentials, unreachable
  endpoint, malformed responses;
- the transport exposes no generic call surface;
- the token literal never appears in argv, results, or exception messages.

The MCP SDK remains an optional extra: these tests skip on a base install.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
import unittest
from pathlib import Path

try:
    import mcp  # noqa: F401

    _MCP_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised on base installs
    _MCP_AVAILABLE = False

SRC_PATH = Path(__file__).resolve().parents[1] / "src"

from coordinate.completion import (  # noqa: E402
    CompletionReceiptError,
    prepare_completion_receipt,
)
from coordinate.completion_mcp_transport import (  # noqa: E402
    CompletionMCPTransport,
    _translate_result,
)
from coordinate.db import (  # noqa: E402
    initialize,
    upsert_workspace,
)
from coordinate.mcp_remote import (  # noqa: E402
    build_remote_mcp_app,
    load_mcp_auth_policy,
    make_interface_provider,
)
from coordinate.mcp_server import build_mcp_server  # noqa: E402

TOKEN = "r5b-transport-super-secret-token"
TOKEN_ENV = "R5B_TRANSPORT_TEST_TOKEN"

COMPLETION_TOOLS = [
    "coordinate.completion_prepare",
    "coordinate.completion_preflight",
    "coordinate.completion_claim",
    "coordinate.completion_apply",
    "coordinate.completion_consume",
    "coordinate.task_create_record",
]


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _make_workspace_conn(db_path: str):
    conn = initialize(db_path)
    upsert_workspace(
        conn, workspace_id="demo", name="Demo", path=".", harness_root=".",
    )
    return conn


def _seed_receipt(conn, *, task_id="task-1", actor="mcp-remote:alpha"):
    """Write a real completion.authorized receipt (fake gate adapter)."""
    from coordinate.db import get_workspace

    workspace = get_workspace(conn, "demo")

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
            return {"items": [{
                "id": task_id,
                "title": "Task",
                "status": "doing",
                "priority": "p1",
                "workflow": {"status": "review_approved", "branch": "feat-x"},
                "verification": "",
                "owner": None,
                "selected_in_session": None,
            }]}

    return prepare_completion_receipt(
        conn,
        workspace_id="demo",
        task_id=task_id,
        requester=actor,
        authorized_actor=actor,
        adapter=_FakeAdapter(workspace),
    )


class _ThreadedServer:
    """uvicorn on an ephemeral loopback port in a daemon thread."""

    def __init__(self, app):
        import uvicorn

        self._uvicorn = uvicorn
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        self.config = uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="warning",
            access_log=False,
        )
        self.server = uvicorn.Server(self.config)
        self.thread = threading.Thread(
            target=lambda: self.server.run(sockets=[sock]), daemon=True,
        )

    def start(self) -> str:
        self.thread.start()
        deadline = time.monotonic() + 15
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("uvicorn did not start")
            time.sleep(0.01)
        return f"http://127.0.0.1:{self.port}/mcp"

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=15)


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class CompletionMCPTransportTests(unittest.TestCase):
    """Real streamable-HTTP end-to-end against the R3 remote app."""

    def setUp(self):
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "transport.sqlite3")
        self.conn = _make_workspace_conn(self.db_path)
        self.addCleanup(self.conn.close)
        self.receipt = _seed_receipt(self.conn)
        self.conn.commit()

        policy_path = os.path.join(self.tmp.name, "clients.json")
        with open(policy_path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "clients": [{
                "client_id": "alpha",
                "token_sha256": _digest(TOKEN),
                "workspace_ids": ["demo"],
                "platforms": [],
                "tools": COMPLETION_TOOLS,
            }]}, f)
        os.chmod(policy_path, 0o600)

        server = build_mcp_server(
            interface_provider=make_interface_provider(
                self._conn_factory()
            )
        )
        app = build_remote_mcp_app(
            server,
            policy=load_mcp_auth_policy(policy_path),
            connection_factory=self._conn_factory(),
            allowed_hosts=["127.0.0.1:*"],
            allowed_origins=[],
        )
        self.server_thread = _ThreadedServer(app)
        self.url = self.server_thread.start()
        self.addCleanup(self.server_thread.stop)

        self._old_token = os.environ.get(TOKEN_ENV)
        os.environ[TOKEN_ENV] = TOKEN
        self.addCleanup(self._restore_token)

    def _restore_token(self):
        if self._old_token is None:
            os.environ.pop(TOKEN_ENV, None)
        else:
            os.environ[TOKEN_ENV] = self._old_token

    def _conn_factory(self):
        return lambda: initialize(self.db_path)

    def _transport(self, token_env: str = TOKEN_ENV) -> CompletionMCPTransport:
        return CompletionMCPTransport(
            url=self.url, token_env=token_env,
        )

    # -- end-to-end happy path ---------------------------------------------

    def test_preflight_claim_apply_end_to_end(self):
        transport = self._transport()
        pre = transport.preflight(
            receipt_id=self.receipt.receipt_id, workspace_id="demo",
        )
        self.assertTrue(pre["ok"])
        self.assertEqual(pre["workspace_id"], "demo")
        self.assertEqual(pre["task_id"], "task-1")
        self.assertEqual(pre["status"], "authorized")

        fps = {
            "before_fingerprint": self.receipt.harness_fingerprint,
            "expected_after_fingerprint": "c" * 64,
        }
        claim = transport.claim(
            receipt_id=self.receipt.receipt_id,
            workspace_id="demo",
            task_id="task-1",
            **fps,
        )
        self.assertEqual(claim["status"], "claimed")
        self.assertEqual(claim["authorized_actor"], "mcp-remote:alpha")
        self.assertEqual(
            claim["before_fingerprint"], self.receipt.harness_fingerprint,
        )

        applied = transport.apply(
            receipt_id=self.receipt.receipt_id,
            workspace_id="demo",
            task_id="task-1",
            after_fingerprint="c" * 64,
        )
        self.assertEqual(applied["status"], "applied")

        # Idempotent replay over the wire converges.
        claim2 = transport.claim(
            receipt_id=self.receipt.receipt_id,
            workspace_id="demo",
            task_id="task-1",
            **fps,
        )
        self.assertTrue(claim2["idempotent"])

    def test_domain_reason_survives_the_wire(self):
        transport = self._transport()
        with self.assertRaises(CompletionReceiptError) as ctx:
            transport.preflight(
                receipt_id="missing-receipt", workspace_id="demo",
            )
        self.assertEqual(ctx.exception.reason, "unknown_receipt")

        with self.assertRaises(CompletionReceiptError) as ctx:
            transport.claim(
                receipt_id=self.receipt.receipt_id,
                workspace_id="demo",
                task_id="task-1",
                before_fingerprint="0" * 64,
                expected_after_fingerprint="c" * 64,
            )
        self.assertEqual(ctx.exception.reason, "before_fingerprint_mismatch")

    # -- fail-closed transport errors ---------------------------------------

    def test_missing_token_fails_closed(self):
        os.environ.pop(TOKEN_ENV, None)
        transport = self._transport()
        with self.assertRaises(CompletionReceiptError) as ctx:
            transport.preflight(
                receipt_id=self.receipt.receipt_id, workspace_id="demo",
            )
        self.assertEqual(ctx.exception.reason, "token_missing")
        self.assertNotIn(TOKEN, str(ctx.exception))

    def test_bad_credentials_fail_closed(self):
        transport = CompletionMCPTransport(
            url=self.url, token_env="R5B_TRANSPORT_WRONG_TOKEN",
        )
        os.environ["R5B_TRANSPORT_WRONG_TOKEN"] = "wrong-token"
        self.addCleanup(os.environ.pop, "R5B_TRANSPORT_WRONG_TOKEN", None)
        with self.assertRaises(CompletionReceiptError) as ctx:
            transport.preflight(
                receipt_id=self.receipt.receipt_id, workspace_id="demo",
            )
        self.assertEqual(ctx.exception.reason, "transport_failed")
        self.assertNotIn("wrong-token", str(ctx.exception))

    def test_unreachable_endpoint_fails_closed(self):
        transport = CompletionMCPTransport(
            url="http://127.0.0.1:1/mcp", token_env=TOKEN_ENV,
        )
        with self.assertRaises(CompletionReceiptError) as ctx:
            transport.preflight(
                receipt_id=self.receipt.receipt_id, workspace_id="demo",
            )
        self.assertEqual(ctx.exception.reason, "transport_failed")

    # -- narrow surface -----------------------------------------------------

    def test_transport_exposes_only_three_fixed_calls(self):
        transport = self._transport()
        public = {
            name for name in dir(transport)
            if not name.startswith("_")
            and callable(getattr(transport, name))
        }
        self.assertEqual(public, {"preflight", "claim", "apply"})
        self.assertEqual(
            CompletionMCPTransport.FIXED_TOOLS,
            (
                "coordinate.completion_preflight",
                "coordinate.completion_claim",
                "coordinate.completion_apply",
            ),
        )

    def test_token_never_in_results_or_exceptions(self):
        transport = self._transport()
        pre = transport.preflight(
            receipt_id=self.receipt.receipt_id, workspace_id="demo",
        )
        serialized = json.dumps(pre)
        self.assertNotIn(TOKEN, serialized)
        self.assertNotIn(TOKEN_ENV + "=" + TOKEN, serialized)
        try:
            transport.preflight(
                receipt_id="missing-receipt", workspace_id="demo",
            )
        except CompletionReceiptError as exc:
            self.assertNotIn(TOKEN, str(exc))


# --------------------------------------------------------------------------
# C1: endpoint URL validation at the transport boundary
# --------------------------------------------------------------------------


class EndpointURLValidationTests(unittest.TestCase):
    """The transport boundary parses the endpoint URL before any token read
    or network connection: https anywhere, http only for loopback hosts, and
    a static rejection for everything else."""

    def _transport(self, url: str) -> CompletionMCPTransport:
        return CompletionMCPTransport(url=url, token_env=TOKEN_ENV)

    def _assert_rejected(self, url: str) -> None:
        with self.assertRaises(CompletionReceiptError) as ctx:
            self._transport(url)
        self.assertEqual(ctx.exception.reason, "invalid_url")
        # Static message: never echoes the URL or any URL-derived content.
        self.assertEqual(
            str(ctx.exception), "invalid Remote MCP endpoint URL",
        )

    def test_https_any_host_accepted(self):
        transport = self._transport("https://coord.example.com/mcp")
        self.assertEqual(transport._url, "https://coord.example.com/mcp")
        transport = self._transport("https://127.0.0.1:8766/mcp")
        self.assertIsNotNone(transport)

    def test_loopback_http_accepted(self):
        for url in (
            "http://localhost:8766/mcp",
            "http://127.0.0.1:8766/mcp",
            "http://127.255.255.1:8766/mcp",
            "http://[::1]:8766/mcp",
        ):
            with self.subTest(url=url):
                transport = self._transport(url)
                self.assertIsNotNone(transport)

    def test_non_loopback_http_rejected(self):
        for url in (
            "http://example.com/mcp",
            "http://example.com:8080/mcp",
            "http://10.0.0.1/mcp",
            "http://[::2]:8766/mcp",
            "http://127.0.0.1.evil.com/mcp",
            "http://localhost.evil.com/mcp",
        ):
            with self.subTest(url=url):
                self._assert_rejected(url)

    def test_userinfo_rejected(self):
        for url in (
            "https://user:pass@coord.example.com/mcp",
            "https://user@coord.example.com/mcp",
            "http://user:pass@localhost:8766/mcp",
        ):
            with self.subTest(url=url):
                self._assert_rejected(url)

    def test_non_http_scheme_rejected(self):
        for url in (
            "ftp://coord.example.com/mcp",
            "ws://coord.example.com/mcp",
            "file:///tmp/mcp",
            "mcp://coord.example.com/mcp",
        ):
            with self.subTest(url=url):
                self._assert_rejected(url)

    def test_missing_host_or_garbage_rejected(self):
        for url in (
            "https:///mcp",
            "https://",
            "http://",
            "",
            "not-a-url",
            "coord.example.com/mcp",
        ):
            with self.subTest(url=url):
                self._assert_rejected(url)

    def test_rejection_happens_before_token_read(self):
        """A rejected URL must fail before the token environment variable is
        ever consulted."""
        calls: list[str] = []

        class _RecordingEnv(dict):
            def get(self, key, *args, **kwargs):
                calls.append(key)
                return super().get(key, *args, **kwargs)

        from unittest import mock

        recording = _RecordingEnv()
        recording[TOKEN_ENV] = TOKEN
        with mock.patch.object(os, "environ", recording):
            with self.assertRaises(CompletionReceiptError) as ctx:
                self._transport("http://example.com/mcp")
            self.assertEqual(ctx.exception.reason, "invalid_url")
        self.assertEqual(calls, [], "token env must not be read for a bad URL")

    @unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
    def test_https_endpoint_reaches_token_check_without_network(self):
        """A valid https URL is accepted; with the token unset the transport
        fails at the token check (token_missing), proving URL validation ran
        first and no network connection was attempted."""
        os.environ.pop(TOKEN_ENV, None)
        try:
            transport = self._transport("https://127.0.0.1:1/mcp")
            with self.assertRaises(CompletionReceiptError) as ctx:
                transport.preflight(
                    receipt_id="r1", workspace_id="demo",
                )
            self.assertEqual(ctx.exception.reason, "token_missing")
        finally:
            if self._old_token is not None:
                os.environ[TOKEN_ENV] = self._old_token

    def setUp(self):
        self._old_token = os.environ.get(TOKEN_ENV)

    def tearDown(self):
        if self._old_token is None:
            os.environ.pop(TOKEN_ENV, None)
        else:
            os.environ[TOKEN_ENV] = self._old_token


# --------------------------------------------------------------------------
# C3: the client never adopts server-provided error text
# --------------------------------------------------------------------------


class TranslateResultStaticMessageTests(unittest.TestCase):
    """Even a misconfigured or hostile endpoint cannot inject its error
    message text into the CLI error output: only a bounded reason code is
    kept, and the exception message is local static text."""

    class _FakeResult:
        def __init__(self, envelope, content=None):
            self.structured_content = envelope
            self.content = content if content is not None else []

    def _assert_raises(self, envelope, *, reason, message_absent):
        result = self._FakeResult(envelope)
        with self.assertRaises(CompletionReceiptError) as ctx:
            _translate_result("coordinate.completion_preflight", result)
        self.assertEqual(ctx.exception.reason, reason)
        for fragment in message_absent:
            self.assertNotIn(fragment, str(ctx.exception))

    def test_domain_reason_kept_message_static(self):
        self._assert_raises(
            {"ok": False, "data": None, "error": {
                "code": "unknown_receipt",
                "message": "SECRET-LEAK-PROBE unknown receipt details",
            }},
            reason="unknown_receipt",
            message_absent=["SECRET-LEAK-PROBE", "unknown receipt details"],
        )

    def test_forbidden_and_unauthorized_messages_static(self):
        self._assert_raises(
            {"ok": False, "data": None, "error": {
                "code": "forbidden",
                "message": "echo: r5b-transport-super-secret-token",
            }},
            reason="forbidden",
            message_absent=["echo:", "r5b-transport-super-secret-token"],
        )

    def test_hostile_code_falls_back_to_transport_refused(self):
        # A hostile endpoint can put anything in the code field; only a
        # bounded printable token is accepted as the reason.
        for code in (
            "SOME\nEVIL CODE",
            "a" * 200,
            "with spaces",
            "UPPER",
            "",
            None,
            42,
            {"x": 1},
        ):
            with self.subTest(code=code):
                self._assert_raises(
                    {"ok": False, "data": None, "error": {
                        "code": code, "message": "EVIL TEXT",
                    }},
                    reason="transport_refused",
                    message_absent=["EVIL TEXT"],
                )

    def test_static_message_used_for_known_reason(self):
        result = self._FakeResult({"ok": False, "data": None, "error": {
            "code": "expired", "message": "EVIL TEXT",
        }})
        with self.assertRaises(CompletionReceiptError) as ctx:
            _translate_result("coordinate.completion_preflight", result)
        self.assertEqual(ctx.exception.reason, "expired")
        self.assertEqual(str(ctx.exception), "receipt expired")
        self.assertNotIn("EVIL TEXT", str(ctx.exception))

    def test_default_static_message_for_unknown_reason(self):
        self._assert_raises(
            {"ok": False, "data": None, "error": {
                "code": "wibble_xyz", "message": "EVIL TEXT",
            }},
            reason="wibble_xyz",
            message_absent=["EVIL TEXT"],
        )

    def test_success_data_passthrough_unchanged(self):
        result = self._FakeResult({
            "ok": True, "data": {"ok": True, "receipt_id": "r1"}, "error": None,
        })
        data = _translate_result("coordinate.completion_preflight", result)
        self.assertEqual(data["receipt_id"], "r1")


if __name__ == "__main__":
    unittest.main()
