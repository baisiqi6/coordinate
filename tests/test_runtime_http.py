"""R2A focused tests: auth policy parser, loopback HTTP server, E2E.

Covers plan §10.2/§10.3: strict policy schema, static 401/403, fail-closed
transport handling, redaction, bounded concurrency, disconnect honesty,
SIGTERM drain and a fresh-temp-DB E2E whose acceptance is the final DB
job/event/lease/delivery semantics (never HTTP 200 alone).
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
from pathlib import Path

import aiohttp

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
REPO_ROOT = Path(__file__).resolve().parents[1]

from coordinate.bus import pump_deliveries  # noqa: E402
from coordinate.db import (  # noqa: E402
    bind_channel_workspace,
    create_delivery,
    get_job,
    initialize,
    list_deliveries,
    list_events,
    release_channel_workspace,
    row_to_dict,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.runtime import register_agent  # noqa: E402
from coordinate.runtime_http import (  # noqa: E402
    AuthPolicyError,
    RuntimeHttpServer,
    load_auth_policy,
    serve_forever,
)
from coordinate.runtime_interface import (  # noqa: E402
    MESSAGE_CONFLICT,
    RuntimeInterface,
    RuntimeInterfaceConfig,
)
from tests.test_runtime_interface import _sync_catalog  # noqa: E402


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


BRIDGE_TOKEN = "bridge-secret-token-1"
AGENTD_TOKEN = "agentd-secret-token-1"


class AuthPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "clients.json")

    def _write(self, entries, *, version=1, mode=0o600):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"version": version, "clients": entries}, f)
        os.chmod(self.path, mode)
        return self.path

    def _bridge(self, **overrides):
        entry = {
            "client_id": "discord-bridge",
            "role": "bridge",
            "token_sha256": _digest(BRIDGE_TOKEN),
            "platforms": ["discord"],
            "workspace_ids": ["demo"],
        }
        entry.update(overrides)
        return entry

    def _agentd(self, **overrides):
        entry = {
            "client_id": "mac-qoder",
            "role": "agentd",
            "token_sha256": _digest(AGENTD_TOKEN),
            "agent_id": "mac-codex",
        }
        entry.update(overrides)
        return entry

    def test_valid_bridge_and_agentd_load(self):
        policy = load_auth_policy(
            self._write([self._bridge(), self._agentd()])
        )
        principal = policy.authenticate("discord-bridge", BRIDGE_TOKEN)
        self.assertIsNotNone(principal)
        self.assertEqual(principal.role, "bridge")
        self.assertEqual(principal.actor, "runtime-http:discord-bridge")
        self.assertIn("demo", principal.workspace_ids)
        agentd = policy.authenticate("mac-qoder", AGENTD_TOKEN)
        self.assertEqual(agentd.agent_id, "mac-codex")

    def test_bad_token_unknown_client_and_missing_all_return_none(self):
        policy = load_auth_policy(self._write([self._bridge()]))
        self.assertIsNone(policy.authenticate("discord-bridge", "wrong-token"))
        self.assertIsNone(policy.authenticate("unknown-client", BRIDGE_TOKEN))
        self.assertIsNone(policy.authenticate("", ""))
        self.assertIsNone(policy.authenticate(None, None))

    def _assert_policy_error(self, entries, fragment):
        with self.assertRaises(AuthPolicyError) as ctx:
            load_auth_policy(self._write(entries))
        self.assertIn(fragment, str(ctx.exception))

    def test_unknown_root_field_fails_closed(self):
        with self.assertRaises(AuthPolicyError):
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"version": 1, "clients": [], "extra": 1}, f)
            os.chmod(self.path, 0o600)
            load_auth_policy(self.path)

    def test_unknown_entry_field_fails_closed(self):
        self._assert_policy_error(
            [self._bridge(extra_field="x")], "unknown clients[0] field"
        )

    def test_wrong_version_fails_closed(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"version": 2, "clients": [self._bridge()]}, f)
        os.chmod(self.path, 0o600)
        with self.assertRaises(AuthPolicyError):
            load_auth_policy(self.path)

    def test_duplicate_client_id_fails_closed(self):
        self._assert_policy_error(
            [self._bridge(), self._bridge()], "duplicate client_id"
        )

    def test_bad_digest_fails_closed(self):
        self._assert_policy_error(
            [self._bridge(token_sha256="not-hex")], "token_sha256"
        )
        self._assert_policy_error(
            [self._bridge(token_sha256="A" * 64)], "token_sha256"
        )

    def test_bridge_missing_scope_fails_closed(self):
        for entry in (
            self._bridge(platforms=[]),
            self._bridge(workspace_ids=[]),
        ):
            with self.assertRaises(AuthPolicyError):
                load_auth_policy(self._write([entry]))

    def test_bridge_with_agent_id_fails_closed(self):
        self._assert_policy_error(
            [self._bridge(agent_id="x")], "must not set agent_id"
        )

    def test_agentd_missing_agent_id_fails_closed(self):
        entry = self._agentd()
        del entry["agent_id"]
        self._assert_policy_error([entry], "agent_id")

    def test_agentd_with_platform_scope_fails_closed(self):
        self._assert_policy_error(
            [self._agentd(platforms=["discord"])], "must not set platforms"
        )

    def test_agentd_duplicate_agent_id_fails_closed(self):
        self._assert_policy_error(
            [
                self._agentd(),
                self._agentd(client_id="mac-qoder-2"),
            ],
            "claimed by more than one",
        )

    def test_invalid_role_fails_closed(self):
        self._assert_policy_error(
            [self._bridge(role="executor")], "role"
        )

    def test_group_world_writable_fails_closed(self):
        for mode in (0o620, 0o602, 0o666):
            with self.assertRaises(AuthPolicyError) as ctx:
                load_auth_policy(self._write([self._bridge()], mode=mode))
            self.assertIn("writable", str(ctx.exception))

    def test_missing_file_fails_closed(self):
        with self.assertRaises(AuthPolicyError):
            load_auth_policy(os.path.join(self.tmp.name, "absent.json"))

    def test_atomic_replace_is_picked_up_by_fresh_load(self):
        """R1c: policy replacement via same-dir write + ``os.replace`` is
        atomic; a fresh ``load_auth_policy`` reads exactly the replacement and
        nothing else — no reload endpoint, watcher or dual-token protocol."""
        old_token = BRIDGE_TOKEN
        new_token = "bridge-secret-token-2"
        policy_path = self._write([self._bridge()])
        self.assertIsNotNone(
            load_auth_policy(policy_path).authenticate("discord-bridge", old_token)
        )

        candidate = os.path.join(self.tmp.name, "clients.json.new")
        with open(candidate, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "version": 1,
                    "clients": [self._bridge(token_sha256=_digest(new_token))],
                },
                f,
            )
        os.chmod(candidate, 0o600)
        os.replace(candidate, policy_path)

        policy = load_auth_policy(policy_path)
        self.assertIsNotNone(policy.authenticate("discord-bridge", new_token))
        self.assertIsNone(policy.authenticate("discord-bridge", old_token))
        self.assertIsNone(policy.authenticate("discord-bridge", ""))

    def test_invalid_json_fails_closed(self):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("{not json")
        os.chmod(self.path, 0o600)
        with self.assertRaises(AuthPolicyError):
            load_auth_policy(self.path)


class HttpServerTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "http.sqlite3")
        self.conn = initialize(self.db_path)
        self.addCleanup(self.conn.close)
        self._seed()
        self.port = self._free_port()
        self.policy = load_auth_policy(self._write_policy())
        # One persistent loop for the whole test: the listener and every
        # request must share it (asyncio.run would close the loop under a
        # live listener and stall all subsequent connects).
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.addCleanup(self._close_loop)

    def _close_loop(self):
        if hasattr(self, "site"):
            self.loop.run_until_complete(self._stop())
        self.loop.close()
        asyncio.set_event_loop(None)

    def _seed(self):
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        register_agent(
            self.conn,
            agent_id="mac-codex",
            host_id="mac",
            capabilities={"models": ["codex"]},
        )
        bind_channel_workspace(
            self.conn,
            platform="discord",
            channel_id="ch",
            workspace_id="demo",
            actor="test",
            reason="default bridge channel",
            idempotency_key="bind-default-ch",
        )
        self.conn.commit()

    def _seed_dynamic_workspace(self, *, channel_id: str = "ch-dynamic") -> None:
        upsert_workspace(
            self.conn,
            workspace_id="dynamic-ws",
            name="Dynamic",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="dynamic-ws",
            host_id="mac",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        bind_channel_workspace(
            self.conn,
            platform="discord",
            channel_id=channel_id,
            workspace_id="dynamic-ws",
            actor="test",
            reason="dynamic channel test",
            idempotency_key=f"bind-{channel_id}",
        )
        self.conn.commit()

    def _write_policy(self, *, entries=None, mode=0o600):
        entries = entries or [
            {
                "client_id": "discord-bridge",
                "role": "bridge",
                "token_sha256": _digest(BRIDGE_TOKEN),
                "platforms": ["discord"],
                "workspace_ids": ["demo"],
            },
            {
                "client_id": "mac-qoder",
                "role": "agentd",
                "token_sha256": _digest(AGENTD_TOKEN),
                "agent_id": "mac-codex",
            },
        ]
        path = os.path.join(self.tmp.name, "clients.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "clients": entries}, f)
        os.chmod(path, mode)
        return path

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _interface(self, created: list | None = None):
        def factory():
            conn = initialize(self.db_path)
            if created is not None:
                created.append(conn)
            return conn

        return RuntimeInterface(connection_factory=factory)

    def _request_with_server(self, server, method, path, **kwargs):
        async def _run():
            port = self._free_port()
            runner, site = await server.start("127.0.0.1", port)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, f"http://127.0.0.1:{port}{path}", **kwargs) as resp:
                        return resp.status, await resp.text(), resp.headers
            finally:
                await site.stop()
                await runner.cleanup()

        return self.loop.run_until_complete(_run())

    async def _start(self, server: RuntimeHttpServer):
        self.runner, self.site = await server.start("127.0.0.1", self.port)

    async def _stop(self):
        if hasattr(self, "site"):
            await self.site.stop()
            await self.runner.cleanup()
            del self.site
            del self.runner

    def _headers(self, client_id: str, token: str) -> dict:
        return {
            "X-Coordinate-Client-ID": client_id,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }


class TransportBehaviorTests(HttpServerTestBase):
    def setUp(self):
        super().setUp()
        self.created: list = []
        self.interface = self._interface(self.created)
        self.server = RuntimeHttpServer(interface=self.interface, policy=self.policy)
        self.loop.run_until_complete(self._start(self.server))

    def tearDown(self):
        super().tearDown()

    def _request(self, method: str, path: str, **kwargs):
        async def _run():
            async with aiohttp.ClientSession() as session:
                async with session.request(method, f"http://127.0.0.1:{self.port}{path}", **kwargs) as resp:
                    return resp.status, await resp.text(), resp.headers

        return self.loop.run_until_complete(_run())

    def test_healthz_public_no_db(self):
        status, body, _ = self._request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["data"]["status"], "ok")
        self.assertEqual(len(self.created), 0)

    def test_readyz_ok(self):
        status, body, _ = self._request("GET", "/readyz")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["data"]["status"], "ready")
        self.assertEqual(len(self.created), 1)  # one short-lived probe connection

    def test_readyz_unavailable_when_db_missing(self):
        broken = RuntimeHttpServer(
            interface=RuntimeInterface(
                connection_factory=lambda: (_ for _ in ()).throw(OSError("no db"))
            ),
            policy=self.policy,
        )
        status, body, _ = self._request_with_server(broken, "GET", "/readyz")
        self.assertEqual(status, 503)
        self.assertNotIn("no db", body)

    def _request_with_server(self, server, method, path, **kwargs):
        async def _run():
            port = self._free_port()
            runner, site = await server.start("127.0.0.1", port)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, f"http://127.0.0.1:{port}{path}", **kwargs) as resp:
                        return resp.status, await resp.text(), resp.headers
            finally:
                await site.stop()
                await runner.cleanup()

        return self.loop.run_until_complete(_run())

    def test_missing_auth_is_static_401_and_no_domain_connection(self):
        status, body, _ = self._request("POST", "/v1/requests", data="{}")
        self.assertEqual(status, 401)
        self.assertEqual(
            json.loads(body),
            {"ok": False, "data": None, "error": {"code": "unauthorized", "message": "invalid credentials"}},
        )
        self.assertEqual(len(self.created), 0)

    def test_unknown_client_bad_token_and_missing_token_identical_body(self):
        bodies = []
        for headers in (
            {"X-Coordinate-Client-ID": "discord-bridge", "Authorization": "Bearer wrong"},
            {"X-Coordinate-Client-ID": "nobody", "Authorization": f"Bearer {BRIDGE_TOKEN}"},
            {"X-Coordinate-Client-ID": "discord-bridge"},
            {},
        ):
            status, body, _ = self._request(
                "GET", "/v1/workspaces/demo/jobs/x", headers=headers
            )
            self.assertEqual(status, 401)
            bodies.append(body)
        self.assertEqual(len(set(bodies)), 1)

    def test_cross_role_403(self):
        status, body, _ = self._request(
            "POST", "/v1/jobs/claim", headers=self._headers("discord-bridge", BRIDGE_TOKEN), data="{}"
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body)["error"]["code"], "forbidden")
        status, body, _ = self._request(
            "GET", "/v1/workspaces/demo/jobs/x", headers=self._headers("mac-qoder", AGENTD_TOKEN)
        )
        self.assertEqual(status, 403)

    def test_platform_scope_403(self):
        status, body, _ = self._request(
            "GET",
            "/v1/channel-bindings/kook/ch-1",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
        )
        self.assertEqual(status, 403)

    def test_workspace_scope_403(self):
        status, body, _ = self._request(
            "GET",
            "/v1/workspaces/other-ws/jobs/x",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
        )
        self.assertEqual(status, 403)

    def test_binding_outside_static_scope_is_dynamic_authority(self):
        self._seed_dynamic_workspace()
        status, body, _ = self._request(
            "GET",
            "/v1/channel-bindings/discord/ch-dynamic",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
        )
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertTrue(payload["data"]["bound"])
        self.assertEqual(payload["data"]["binding"]["workspace_id"], "dynamic-ws")

    def test_wrong_content_type_400(self):
        status, body, _ = self._request(
            "POST", "/v1/jobs/claim",
            headers={
                "X-Coordinate-Client-ID": "mac-qoder",
                "Authorization": f"Bearer {AGENTD_TOKEN}",
                "Content-Type": "text/plain",
            },
            data="{}",
        )
        self.assertEqual(status, 400)

    def test_malformed_json_400(self):
        status, body, _ = self._request(
            "POST", "/v1/jobs/claim",
            headers=self._headers("mac-qoder", AGENTD_TOKEN),
            data="{not json",
        )
        self.assertEqual(status, 400)

    def test_non_object_body_400(self):
        status, _, _ = self._request(
            "POST", "/v1/jobs/claim",
            headers=self._headers("mac-qoder", AGENTD_TOKEN),
            data="[1, 2]",
        )
        self.assertEqual(status, 400)

    def test_unknown_field_400(self):
        status, _, _ = self._request(
            "POST", "/v1/jobs/claim",
            headers=self._headers("mac-qoder", AGENTD_TOKEN),
            data=json.dumps({"recoverable": True, "recovery_reason": "x", "prior_process_stopped": True}),
        )
        self.assertEqual(status, 400)

    def test_oversized_body_413(self):
        status, body, _ = self._request(
            "POST", "/v1/jobs/claim",
            headers=self._headers("mac-qoder", AGENTD_TOKEN),
            data="x" * (1024 * 1024 + 1),
        )
        self.assertEqual(status, 413)
        self.assertIn("payload_too_large", body)

    def test_wrong_method_405(self):
        # Auth runs first (fail closed), then method mismatch is a structured
        # 405 envelope (plan code invalid_request) with the request-id header.
        status, body, headers = self._request(
            "GET", "/v1/requests",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
        )
        self.assertEqual(status, 405)
        payload = json.loads(body)
        self.assertEqual(set(payload), {"ok", "data", "error"})
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "invalid_request")
        self.assertEqual(payload["error"]["message"], "invalid request")
        self.assertNotIn("/v1/requests", payload["error"]["message"])
        self.assertIn("X-Coordinate-Request-Id", headers)

    def test_unknown_route_is_structured_404(self):
        status, body, headers = self._request(
            "GET", "/v1/nonexistent-route",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
        )
        self.assertEqual(status, 404)
        payload = json.loads(body)
        self.assertEqual(set(payload), {"ok", "data", "error"})
        self.assertEqual(payload["error"]["code"], "not_found")
        self.assertNotIn("nonexistent-route", payload["error"]["message"])
        self.assertIn("X-Coordinate-Request-Id", headers)

    def test_request_id_header_present(self):
        _, _, headers = self._request("GET", "/healthz")
        self.assertIn("X-Coordinate-Request-Id", headers)

    def test_access_log_redacts_token_prompt_and_db_path(self):
        records: list[str] = []

        class _Handler(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        handler = _Handler()
        logger = logging.getLogger("coordinate.runtime_http")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            self._request(
                "POST", "/v1/requests",
                headers=self._headers("discord-bridge", BRIDGE_TOKEN),
                data=json.dumps(
                    {
                        "workspace_id": "demo",
                        "prompt": "secret-prompt-text",
                        "origin": {"platform": "discord", "destination": "ch", "message_id": "m1"},
                        "reply": {"platform": "discord", "destination": "ch"},
                        "target_agent": "mac-codex",
                        "idempotency_key": "log-k",
                    }
                ),
            )
            self._request(
                "POST", "/v1/jobs/claim",
                headers=self._headers("mac-qoder", "wrong-token-secret"),
                data="{}",
            )
        finally:
            logger.removeHandler(handler)
        joined = "\n".join(records)
        self.assertIn("route=/v1/requests", joined)
        self.assertNotIn(BRIDGE_TOKEN, joined)
        self.assertNotIn("wrong-token-secret", joined)
        self.assertNotIn("secret-prompt-text", joined)
        self.assertNotIn(self.db_path, joined)
        self.assertNotIn("request:log-k", joined)

    def test_bounded_concurrency_serializes_domain_calls(self):
        import threading

        state = {"active": 0, "peak": 0}
        lock = threading.Lock()

        def factory():
            with lock:
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
            try:
                time.sleep(0.2)
                return initialize(self.db_path)
            finally:
                with lock:
                    state["active"] -= 1

        interface = RuntimeInterface(connection_factory=factory)
        server = RuntimeHttpServer(interface=interface, policy=self.policy, max_concurrent=1)

        async def _run():
            port = self._free_port()
            runner, site = await server.start("127.0.0.1", port)
            try:
                async with aiohttp.ClientSession() as session:
                    async def _one():
                        async with session.post(
                            f"http://127.0.0.1:{port}/v1/jobs/claim",
                            headers=self._headers("mac-qoder", AGENTD_TOKEN),
                            data="{}",
                        ) as resp:
                            return resp.status

                    return await asyncio.gather(_one(), _one())
            finally:
                await site.stop()
                await runner.cleanup()

        statuses = self.loop.run_until_complete(_run())
        self.assertEqual(statuses, [200, 200])
        self.assertEqual(state["peak"], 1)
        self.assertEqual(state["active"], 0)


class EndToEndTests(HttpServerTestBase):
    """Fresh temp DB + real listener: resolve -> submit -> claim -> progress
    -> renew -> report -> exact get. Acceptance is final DB semantics."""

    def setUp(self):
        super().setUp()
        self.created: list = []
        self.interface = self._interface(self.created)
        self.server = RuntimeHttpServer(interface=self.interface, policy=self.policy)
        self.runner = None

    def tearDown(self):
        super().tearDown()

    def test_full_lifecycle_db_semantics(self):
        bind_channel_workspace(
            self.conn, platform="discord", channel_id="ch-1", workspace_id="demo",
            actor="test", reason="test", idempotency_key="bind-1",
        )
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()

        async def _run():
            self.runner, self.site = await self.server.start("127.0.0.1", self.port)
            bridge = self._headers("discord-bridge", BRIDGE_TOKEN)
            agentd = self._headers("mac-qoder", AGENTD_TOKEN)
            async with aiohttp.ClientSession() as session:
                # 1. resolve
                async with session.get(
                    f"http://127.0.0.1:{self.port}/v1/channel-bindings/discord/ch-1",
                    headers=bridge,
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    resolved = await resp.json()
                self.assertTrue(resolved["data"]["bound"])
                self.assertEqual(resolved["data"]["binding"]["workspace_id"], "demo")

                # 2. submit
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/requests",
                    headers=bridge,
                    json={
                        "workspace_id": "demo",
                        "prompt": "e2e prompt",
                        "origin": {
                            "platform": "discord",
                            "destination": "ch",
                            "message_id": "m-e2e",
                            "session_scope_id": "discord:ch",
                        },
                        "reply": {"platform": "discord", "destination": "ch"},
                        "target_agent": "mac-codex",
                        "idempotency_key": "e2e-key-1",
                    },
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    submitted = await resp.json()
                self.job_id = submitted["data"]["job"]["id"]
                job_id = self.job_id

                # 3. claim (typed agentd principal)
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/jobs/claim",
                    headers=agentd,
                    json={},
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    claimed = await resp.json()
                self.assertTrue(claimed["data"]["claimed"])
                attempt_token = claimed["data"]["attempt_token"]
                lease_id = claimed["data"]["execution_lease"]["lease_id"]
                self.attempt_token = attempt_token
                self.lease_id = lease_id

                # 4. progress with attempt/lease identity
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/jobs/{job_id}/progress",
                    headers=agentd,
                    json={
                        "stage": "working",
                        "summary": "e2e progress",
                        "attempt_token": attempt_token,
                        "lease_id": lease_id,
                    },
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    progressed = await resp.json()
                self.assertTrue(progressed["ok"])

                # 5. renew (advance the stored expiry first, same-second rule)
                current = claimed["data"]["execution_lease"]["expires_at"]
                from datetime import datetime, timedelta, timezone

                shrunk = (
                    datetime.fromisoformat(current.replace("Z", "+00:00"))
                    - timedelta(seconds=1)
                ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                self.conn.execute(
                    "UPDATE execution_attempt_leases SET expires_at = ? WHERE lease_id = ?",
                    (shrunk, lease_id),
                )
                self.conn.commit()
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/jobs/{job_id}/lease/renew",
                    headers=agentd,
                    json={"lease_id": lease_id, "attempt_token": attempt_token},
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    renewed = await resp.json()
                self.assertTrue(renewed["ok"])
                self.assertEqual(renewed["data"]["status"], "active")

                # 6. report done with the same attempt/lease identity
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/jobs/{job_id}/report",
                    headers=agentd,
                    json={
                        "status": "done",
                        "result": {"response_text": "e2e finished"},
                        "attempt_token": attempt_token,
                        "lease_id": lease_id,
                    },
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    reported = await resp.json()
                self.assertTrue(reported["ok"])
                self.assertTrue(reported["data"]["delivery_created"])

                # 7. exact workspace-bound job get
                async with session.get(
                    f"http://127.0.0.1:{self.port}/v1/workspaces/demo/jobs/{job_id}",
                    headers=bridge,
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    got = await resp.json()
                self.assertEqual(got["data"]["status"], "done")

        self.loop.run_until_complete(_run())

        # DB semantics, not HTTP 200: job done, events, lease released,
        # delivery row present, and every short-lived domain connection closed.
        job = row_to_dict(
            self.conn.execute("SELECT * FROM jobs WHERE id = ?", (self.job_id,)).fetchone()
        )
        self.assertEqual(job["status"], "done")
        event_types = [e["event_type"] for e in list_events(self.conn, "demo")]
        for expected in (
            "request.received",
            "job.claimed",
            "job.progress",
            "job.completed",
            "agent.reported",
        ):
            self.assertIn(expected, event_types)
        lease = self.conn.execute(
            "SELECT * FROM execution_attempt_leases WHERE lease_id = ?", (self.lease_id,)
        ).fetchone()
        self.assertEqual(lease["status"], "released")
        deliveries = self.conn.execute("SELECT COUNT(*) AS n FROM deliveries").fetchone()
        self.assertEqual(deliveries["n"], 1)
        self.assertTrue(self.created)
        for connection in self.created:
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")

    def test_disconnect_does_not_rewrite_commit_semantics(self):
        """A client that disconnects mid-response never cancels the domain
        call: the mutation commits at its existing transaction boundary.

        Barrier-controlled (R1c): the test waits until the domain call has
        entered the worker thread, disconnects the client, lets the server
        observe the disconnect, then releases the gate — the commit must land
        afterwards. No random-sleep concurrency guessing.
        """
        entered = threading.Event()
        release = threading.Event()
        real_submit = RuntimeInterface.submit_request

        def _gated_submit(interface, **kwargs):
            entered.set()
            if not release.wait(30):
                raise TimeoutError("submit gate was never released")
            return real_submit(interface, **kwargs)

        async def _run():
            self.runner, self.site = await self.server.start("127.0.0.1", self.port)
            body = json.dumps(
                {
                    "workspace_id": "demo",
                    "prompt": "disconnect prompt",
                    "origin": {
                        "platform": "discord",
                        "destination": "ch",
                        "message_id": "m-dc",
                        "session_scope_id": "discord:ch",
                    },
                    "reply": {"platform": "discord", "destination": "ch"},
                    "target_agent": "mac-codex",
                    "idempotency_key": "dc-key-1",
                }
            ).encode("utf-8")
            request = (
                "POST /v1/requests HTTP/1.1\r\n"
                f"Host: 127.0.0.1:{self.port}\r\n"
                "X-Coordinate-Client-ID: discord-bridge\r\n"
                f"Authorization: Bearer {BRIDGE_TOKEN}\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("utf-8") + body
            with socket.create_connection(("127.0.0.1", self.port), timeout=5) as sock:
                sock.sendall(request)
                # Wait (yielding to the loop so the server can dispatch) until
                # the domain call is inside the worker thread.
                deadline = time.monotonic() + 10
                while not entered.is_set():
                    if time.monotonic() > deadline:
                        self.fail("domain call never entered the worker thread")
                    await asyncio.sleep(0.02)
                # Disconnect without reading the response.
            # The client socket is closed before the domain call may commit.
            release.set()

        with unittest.mock.patch.object(
            RuntimeInterface, "submit_request", _gated_submit
        ):
            self.loop.run_until_complete(_run())
        deadline = time.monotonic() + 10
        jobs = []
        while time.monotonic() < deadline:
            jobs = self.conn.execute("SELECT id, status FROM jobs").fetchall()
            if jobs:
                break
            time.sleep(0.05)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["status"], "pending")


class ServeForeverSubprocessTests(unittest.TestCase):
    """Real CLI process: non-loopback rejection and SIGTERM drain."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "cli.sqlite3")
        conn = initialize(self.db_path)
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path=self.tmp.name, harness_root=self.tmp.name,
        )
        conn.close()
        policy_path = os.path.join(self.tmp.name, "clients.json")
        with open(policy_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "version": 1,
                    "clients": [
                        {
                            "client_id": "discord-bridge",
                            "role": "bridge",
                            "token_sha256": _digest(BRIDGE_TOKEN),
                            "platforms": ["discord"],
                            "workspace_ids": ["demo"],
                        }
                    ],
                },
                f,
            )
        os.chmod(policy_path, 0o600)
        self.policy_path = policy_path
        self.port = self._free_port()

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _env(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(SRC_PATH)
        return env

    def test_non_loopback_host_rejected_before_listen(self):
        result = subprocess.run(
            [
                sys.executable, "-m", "coordinate", "--db", self.db_path,
                "runtime-http", "serve", "--host", "0.0.0.0",
                "--port", str(self.port), "--auth-file", self.policy_path,
            ],
            capture_output=True, text=True, env=self._env(),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("loopback", result.stderr)
        with socket.socket() as s:
            self.assertNotEqual(s.connect_ex(("127.0.0.1", self.port)), 0)

    def test_sigterm_drains_and_exits_zero(self):
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "coordinate", "--db", self.db_path,
                "runtime-http", "serve", "--host", "127.0.0.1",
                "--port", str(self.port), "--auth-file", self.policy_path,
            ],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=self._env(),
        )
        self.addCleanup(self._cleanup_proc, proc)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                async def _probe():
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"http://127.0.0.1:{self.port}/healthz", timeout=aiohttp.ClientTimeout(total=1)
                        ) as resp:
                            return resp.status

                status = asyncio.run(_probe())
                if status == 200:
                    break
            except Exception:
                pass
            time.sleep(0.1)
        else:
            proc.kill()
            self.fail("server did not become ready")

        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=20)
        self.assertEqual(proc.returncode, 0, stderr)
        self.assertIn("runtime-http stopped", stderr)

    @staticmethod
    def _cleanup_proc(proc):
        if proc.poll() is None:
            proc.kill()
            proc.wait()


class CorrectionTransportTests(HttpServerTestBase):
    """R2A correction C1/C3/C4/C5/C6 focused behavior."""

    def setUp(self):
        super().setUp()
        self.created: list = []
        self.interface = self._interface(self.created)
        self.server = RuntimeHttpServer(interface=self.interface, policy=self.policy)
        self.loop.run_until_complete(self._start(self.server))

    def _request(self, method: str, path: str, **kwargs):
        async def _run():
            async with aiohttp.ClientSession() as session:
                async with session.request(method, f"http://127.0.0.1:{self.port}{path}", **kwargs) as resp:
                    return resp.status, await resp.text(), resp.headers

        return self.loop.run_until_complete(_run())

    @staticmethod
    def _attach(records: list[str], *names: str):
        class _Handler(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        handler = _Handler()
        for name in names:
            logger = logging.getLogger(name)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return handler

    # -- C1: no raw aiohttp access log; bounded middleware 500 ---------------

    def test_default_aiohttp_access_log_never_records_sentinels(self):
        records: list[str] = []
        handler = self._attach(records, "aiohttp.access", "")
        sentinel_channel = "SENTINEL-CHANNEL-9f3a"
        sentinel_workspace = "sentinel-ws-7c1e"
        try:
            # 401 (bad auth), 200 (public), 403 (scope), 200 (agentd).
            self._request(
                "GET", f"/v1/channel-bindings/discord/{sentinel_channel}"
            )
            self._request("GET", "/healthz")
            self._request(
                "GET",
                f"/v1/workspaces/{sentinel_workspace}/jobs/x",
                headers=self._headers("discord-bridge", BRIDGE_TOKEN),
            )
            self._request(
                "POST",
                "/v1/jobs/claim",
                headers=self._headers("mac-qoder", AGENTD_TOKEN),
                data="{}",
            )
            # Structured 405 with a sentinel in the dynamic path must stay
            # sentinel-free in every logger (route template, not raw path).
            sentinel_job = "request:SENTINEL-JOB-7c1e"
            self._request(
                "GET",
                f"/v1/jobs/{sentinel_job}/progress",
                headers=self._headers("mac-qoder", AGENTD_TOKEN),
            )
        finally:
            for name in ("aiohttp.access", ""):
                logging.getLogger(name).removeHandler(handler)
        joined = "\n".join(records)
        self.assertNotIn("SENTINEL", joined)
        self.assertNotIn(sentinel_workspace, joined)
        self.assertNotIn("request:SENTINEL-JOB-7c1e", joined)
        # The bounded route-template log is the only request log.
        self.assertIn("route=/v1/jobs/claim", joined)

    def test_middleware_unexpected_error_is_static_500_with_bounded_log(self):
        class _BoomServer(RuntimeHttpServer):
            async def _call(self, principal, fn):
                raise RuntimeError("secret-detail-boom /tmp/private/db.sqlite3")

        server = _BoomServer(interface=self._interface(), policy=self.policy)
        records: list[str] = []
        handler = self._attach(records, "coordinate.runtime_http")
        try:
            status, body, _ = self._request_with_server(
                server,
                "POST",
                "/v1/jobs/claim",
                headers=self._headers("mac-qoder", AGENTD_TOKEN),
                data="{}",
            )
        finally:
            logging.getLogger("coordinate.runtime_http").removeHandler(handler)
        self.assertEqual(status, 500)
        self.assertEqual(
            json.loads(body),
            {"ok": False, "data": None, "error": {"code": "internal", "message": "internal error"}},
        )
        joined = "\n".join(records)
        self.assertIn("unhandled request error type=RuntimeError", joined)
        self.assertIn("request_id=", joined)
        self.assertNotIn("secret-detail-boom", joined)
        self.assertNotIn("/tmp/private", joined)
        self.assertNotIn("Traceback", joined)

    # -- C3: honest 409 for stale/late/expired/mismatch authority ------------

    def _seed_running_job(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()

        async def _run():
            bridge = self._headers("discord-bridge", BRIDGE_TOKEN)
            agentd = self._headers("mac-qoder", AGENTD_TOKEN)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/requests",
                    headers=bridge,
                    json={
                        "workspace_id": "demo",
                        "prompt": "correction c3",
                        "origin": {
                            "platform": "discord",
                            "destination": "ch",
                            "message_id": "m-c3",
                            "session_scope_id": "discord:ch",
                        },
                        "reply": {"platform": "discord", "destination": "ch"},
                        "target_agent": "mac-codex",
                        "idempotency_key": "c3-key",
                    },
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    submitted = await resp.json()
                job_id = submitted["data"]["job"]["id"]
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/jobs/claim",
                    headers=agentd,
                    json={},
                ) as resp:
                    self.assertEqual(resp.status, 200)
                    claimed = await resp.json()
                return (
                    job_id,
                    claimed["data"]["attempt_token"],
                    claimed["data"]["execution_lease"]["lease_id"],
                )

        return self.loop.run_until_complete(_run())

    def test_http_submit_event_actor_is_principal_derived(self):
        """C2: HTTP actor comes from the principal, never the body."""
        self._seed_running_job()
        events = list_events(self.conn, "demo")
        actors = {e["actor"] for e in events}
        self.assertIn("runtime-http:discord-bridge", actors)
        self.assertNotIn("mcp", actors)

    def _agentd_call(self, method: str, path: str, payload: dict) -> tuple[int, dict]:
        async def _run():
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method,
                    f"http://127.0.0.1:{self.port}{path}",
                    headers=self._headers("mac-qoder", AGENTD_TOKEN),
                    json=payload,
                ) as resp:
                    return resp.status, await resp.json()

        return self.loop.run_until_complete(_run())

    def test_http_stale_progress_is_409(self):
        job_id, _, _ = self._seed_running_job()
        status, body = self._agentd_call(
            "POST",
            f"/v1/jobs/{job_id}/progress",
            {"stage": "x", "attempt_token": 999},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")

    def test_http_late_report_is_409(self):
        job_id, _, _ = self._seed_running_job()
        status, body = self._agentd_call(
            "POST",
            f"/v1/jobs/{job_id}/report",
            {"status": "done", "result": {}, "attempt_token": 999},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")

    def test_http_late_result_managed_attempt_is_409(self):
        """R1c: the real ``late-result rejected: current attempt is managed``
        domain error reaches the wire as an honest 409 conflict (not 400)."""
        job_id, attempt_token, lease_id = self._seed_running_job()
        # Timed-out terminal report with the exact tuple: the attempt becomes
        # managed (released lease row) and the job timed_out+recoverable.
        status, body = self._agentd_call(
            "POST",
            f"/v1/jobs/{job_id}/report",
            {
                "status": "timed_out",
                "result": {"response_text": "recoverable timeout"},
                "attempt_token": attempt_token,
                "lease_id": lease_id,
            },
        )
        self.assertEqual(status, 200, body)
        self.assertTrue(body["ok"], body)
        # The late terminal result for the managed attempt must fail closed
        # with the bounded conflict envelope and no second terminal outcome.
        status, body = self._agentd_call(
            "POST",
            f"/v1/jobs/{job_id}/report",
            {
                "status": "done",
                "result": {"response_text": "late done"},
                "attempt_token": attempt_token,
                "lease_id": lease_id,
            },
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")
        self.assertEqual(body["error"]["message"], MESSAGE_CONFLICT)
        job = row_to_dict(
            self.conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        )
        self.assertEqual(job["status"], "timed_out")
        self.assertTrue(job["recoverable"])
        terminal = [
            e["event_type"]
            for e in list_events(self.conn, "demo")
            if e["event_type"].startswith("job.")
        ]
        self.assertEqual(terminal.count("job.completed"), 0)
        self.assertEqual(terminal.count("job.late_result_accepted"), 0)

    def test_http_timed_out_exact_replay_is_200_idempotent(self):
        """R2B P1: a timed_out terminal report on an already timed_out job
        (same body) is a 200 immutable replay — never a 500, never a state
        change, never a second terminal event. Same-body retry is idempotent.
        """
        job_id, attempt_token, lease_id = self._seed_running_job()
        body = {
            "status": "timed_out",
            "result": {"response_text": "recoverable timeout"},
            "attempt_token": attempt_token,
            "lease_id": lease_id,
        }
        status, first = self._agentd_call(
            "POST", f"/v1/jobs/{job_id}/report", body
        )
        self.assertEqual(status, 200)
        self.assertEqual(first["data"]["job"]["status"], "timed_out")
        self.assertTrue(first["data"]["job"]["recoverable"])

        # Exact same body retry (lost-response replay) is 200, not 409/500.
        status, replay = self._agentd_call(
            "POST", f"/v1/jobs/{job_id}/report", body
        )
        self.assertEqual(status, 200)
        self.assertEqual(replay["data"]["event"]["event_type"], "job.result_replayed")
        self.assertFalse(replay["data"]["event"]["payload"]["applied"])
        self.assertEqual(
            replay["data"]["event"]["payload"]["submitted_status"], "timed_out"
        )
        self.assertEqual(replay["data"]["job"]["status"], "timed_out")
        self.assertFalse(replay["data"]["delivery_created"])

        # A third same-body retry is still 200 (idempotent replay event).
        status, replay2 = self._agentd_call(
            "POST", f"/v1/jobs/{job_id}/report", body
        )
        self.assertEqual(status, 200)
        self.assertEqual(replay2["data"]["event"]["event_type"], "job.result_replayed")

        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        terminal = [e for e in events if e["event_type"] == "job.timed_out"]
        self.assertEqual(len(terminal), 1)
        job = row_to_dict(get_job(self.conn, job_id))
        self.assertEqual(job["status"], "timed_out")

    def test_http_timed_out_replay_does_not_swallow_stale_attempt_conflict(self):
        """R2B P1: the exact timed_out replay must never turn a real stale
        attempt/lease conflict into success — a mismatched tuple on a still
        running job stays a 409.
        """
        job_id, attempt_token, lease_id = self._seed_running_job()
        status, body = self._agentd_call(
            "POST",
            f"/v1/jobs/{job_id}/report",
            {
                "status": "timed_out",
                "result": {"response_text": "x"},
                "attempt_token": attempt_token + 1,
                "lease_id": lease_id,
            },
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")
        job = row_to_dict(get_job(self.conn, job_id))
        self.assertEqual(job["status"], "running")

    def test_http_expired_renew_is_409(self):
        job_id, attempt_token, lease_id = self._seed_running_job()
        self.conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', "
            "    renewed_at = '2020-01-01T00:00:00Z', "
            "    expires_at = '2020-01-01T00:00:01Z' "
            "WHERE lease_id = ?",
            (lease_id,),
        )
        self.conn.commit()
        status, body = self._agentd_call(
            "POST",
            f"/v1/jobs/{job_id}/lease/renew",
            {"lease_id": lease_id, "attempt_token": attempt_token},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")

    def test_http_mismatched_renew_is_409(self):
        job_id, attempt_token, lease_id = self._seed_running_job()
        status, body = self._agentd_call(
            "POST",
            f"/v1/jobs/{job_id}/lease/renew",
            {"lease_id": "lease:other", "attempt_token": attempt_token},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "conflict")

    def test_http_missing_job_get_is_404(self):
        async def _run():
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://127.0.0.1:{self.port}/v1/workspaces/demo/jobs/request:missing",
                    headers=self._headers("discord-bridge", BRIDGE_TOKEN),
                ) as resp:
                    return resp.status, await resp.json()

        status, body = self.loop.run_until_complete(_run())
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")

    # -- C4: bridge origin/reply scope ---------------------------------------

    def _submit_body(self, **overrides) -> dict:
        body = {
            "workspace_id": "demo",
            "prompt": "c4",
            "origin": {
                "platform": "discord",
                "destination": "ch",
                "message_id": "m-c4",
                "session_scope_id": "discord:ch",
            },
            "reply": {"platform": "discord", "destination": "ch"},
            "target_agent": "mac-codex",
            "idempotency_key": "c4-key",
        }
        body.update(overrides)
        return body

    def _submit_status(self, body: dict) -> int:
        async def _run():
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://127.0.0.1:{self.port}/v1/requests",
                    headers=self._headers("discord-bridge", BRIDGE_TOKEN),
                    json=body,
                ) as resp:
                    return resp.status, await resp.json()

        status, resp = self.loop.run_until_complete(_run())
        return status

    def test_submit_reply_same_platform_allowed(self):
        self.assertEqual(self._submit_status(self._submit_body()), 200)

    def test_submit_reply_none_sentinel_allowed(self):
        body = self._submit_body(reply={"platform": "none", "destination": "ch"})
        self.assertEqual(self._submit_status(body), 200)

    def test_submit_static_workspace_rejects_unbound_or_cross_workspace_origin(self):
        self._seed_dynamic_workspace()
        before = self.conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE workspace_id = 'demo'"
        ).fetchone()["n"]
        for destination in ("unbound", "ch-dynamic"):
            body = self._submit_body(
                origin={
                    "platform": "discord",
                    "destination": destination,
                    "message_id": f"m-static-{destination}",
                },
                reply={"platform": "none", "destination": destination},
                idempotency_key=f"static-reject-{destination}",
            )
            self.assertEqual(self._submit_status(body), 403)

        after = self.conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE workspace_id = 'demo'"
        ).fetchone()["n"]
        self.assertEqual(after, before)

    def test_submit_static_workspace_rejects_reply_bound_to_other_workspace(self):
        self._seed_dynamic_workspace()
        body = self._submit_body(
            reply={"platform": "discord", "destination": "ch-dynamic"},
            idempotency_key="static-cross-reply",
        )
        self.assertEqual(self._submit_status(body), 403)

    def test_submit_reply_cross_platform_403(self):
        body = self._submit_body(reply={"platform": "kook", "destination": "ch"})
        self.assertEqual(self._submit_status(body), 403)

    def test_submit_workspace_missing_400(self):
        body = self._submit_body()
        del body["workspace_id"]
        self.assertEqual(self._submit_status(body), 400)

    def test_submit_workspace_wrong_type_400(self):
        self.assertEqual(self._submit_status(self._submit_body(workspace_id=123)), 400)

    def test_submit_workspace_outside_scope_403(self):
        self.assertEqual(
            self._submit_status(self._submit_body(workspace_id="other-ws")), 403
        )

    def test_submit_dynamic_workspace_allowed_by_exact_origin_binding(self):
        self._seed_dynamic_workspace()
        body = self._submit_body(
            workspace_id="dynamic-ws",
            origin={
                "platform": "discord",
                "destination": "ch-dynamic",
                "message_id": "m-dynamic",
                "session_scope_id": "discord:ch-dynamic",
            },
            reply={"platform": "none", "destination": "ch-dynamic"},
            idempotency_key="dynamic-submit",
        )
        self.assertEqual(self._submit_status(body), 200)
        row = self.conn.execute(
            "SELECT workspace_id, payload_json FROM jobs "
            "WHERE workspace_id = 'dynamic-ws'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["workspace_id"], "dynamic-ws")
        self.assertEqual(
            json.loads(row["payload_json"])["origin"]["destination"],
            "ch-dynamic",
        )

    def test_submit_dynamic_workspace_rejects_unbound_or_mismatched_origin(self):
        self._seed_dynamic_workspace()
        for destination in ("unbound", "ch"):
            body = self._submit_body(
                workspace_id="dynamic-ws",
                origin={
                    "platform": "discord",
                    "destination": destination,
                    "message_id": f"m-{destination}",
                },
                reply={"platform": "none", "destination": destination},
                idempotency_key=f"dynamic-reject-{destination}",
            )
            self.assertEqual(self._submit_status(body), 403)
        count = self.conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE workspace_id = 'dynamic-ws'"
        ).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_submit_dynamic_workspace_rejects_reply_to_other_binding(self):
        self._seed_dynamic_workspace()
        body = self._submit_body(
            workspace_id="dynamic-ws",
            origin={
                "platform": "discord",
                "destination": "ch-dynamic",
                "message_id": "m-cross-reply",
            },
            reply={"platform": "discord", "destination": "ch"},
            idempotency_key="dynamic-cross-reply",
        )
        self.assertEqual(self._submit_status(body), 403)

    def test_job_get_dynamic_workspace_requires_stored_origin_binding(self):
        self._seed_dynamic_workspace()
        body = self._submit_body(
            workspace_id="dynamic-ws",
            origin={
                "platform": "discord",
                "destination": "ch-dynamic",
                "message_id": "m-dynamic-get",
                "session_scope_id": "discord:ch-dynamic",
            },
            reply={"platform": "none", "destination": "ch-dynamic"},
            idempotency_key="dynamic-get",
        )
        self.assertEqual(self._submit_status(body), 200)
        job_id = self.conn.execute(
            "SELECT id FROM jobs WHERE workspace_id = 'dynamic-ws'"
        ).fetchone()["id"]

        status, response, _ = self._request(
            "GET",
            f"/v1/workspaces/dynamic-ws/jobs/{job_id}",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(response)["data"]["id"], job_id)

        release_channel_workspace(
            self.conn,
            platform="discord",
            channel_id="ch-dynamic",
            expected_workspace_id="dynamic-ws",
            actor="test",
            reason="verify dynamic scope revocation",
            idempotency_key="release-ch-dynamic",
        )
        self.conn.commit()
        status, response, _ = self._request(
            "GET",
            f"/v1/workspaces/dynamic-ws/jobs/{job_id}",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(response)["error"]["code"], "forbidden")

    def test_submit_origin_cross_platform_403(self):
        body = self._submit_body(
            origin={"platform": "kook", "destination": "ch", "message_id": "m"}
        )
        self.assertEqual(self._submit_status(body), 403)

    # -- C5: never create/migrate the DB; strict readiness ---------------------

    def test_db_connect_must_exist_is_atomic(self):
        from coordinate.db import connect

        missing = os.path.join(self.tmp.name, "atomic-missing.sqlite3")
        with self.assertRaises(sqlite3.OperationalError):
            connect(missing, must_exist=True)
        self.assertFalse(
            os.path.exists(missing), "must_exist open must never create the file"
        )
        # The legacy default keeps create-if-absent semantics.
        legacy = os.path.join(self.tmp.name, "legacy-created.sqlite3")
        conn = connect(legacy)
        conn.close()
        self.assertTrue(os.path.exists(legacy))

    def test_db_connect_must_exist_relative_path(self):
        from coordinate.db import connect

        previous = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            rel_existing = "rel-existing.sqlite3"
            seed = sqlite3.connect(rel_existing)
            seed.close()
            opened = connect(rel_existing, must_exist=True)
            opened.close()
            rel_missing = "rel-missing.sqlite3"
            with self.assertRaises(sqlite3.OperationalError):
                connect(rel_missing, must_exist=True)
            self.assertFalse(
                os.path.exists(rel_missing),
                "relative must_exist open must never create the file",
            )
        finally:
            os.chdir(previous)

    def test_readyz_missing_db_503_and_file_not_created(self):
        missing = os.path.join(self.tmp.name, "absent.sqlite3")
        interface = RuntimeInterface.from_config(
            dataclasses.replace(
                RuntimeInterfaceConfig(db_path=missing), actor="runtime-http"
            )
        )
        server = RuntimeHttpServer(interface=interface, policy=self.policy)
        status, body, _ = self._request_with_server(server, "GET", "/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body)["error"]["code"], "unavailable")
        self.assertFalse(os.path.exists(missing), "readyz must not create the DB file")

    def test_readyz_schema_less_sqlite_file_is_503(self):
        empty = os.path.join(self.tmp.name, "empty.sqlite3")
        conn = sqlite3.connect(empty)
        conn.close()
        interface = RuntimeInterface.from_config(
            dataclasses.replace(
                RuntimeInterfaceConfig(db_path=empty), actor="runtime-http"
            )
        )
        server = RuntimeHttpServer(interface=interface, policy=self.policy)
        status, body, _ = self._request_with_server(server, "GET", "/readyz")
        self.assertEqual(status, 503)

    def test_readyz_partial_schema_is_503(self):
        partial = os.path.join(self.tmp.name, "partial.sqlite3")
        conn = sqlite3.connect(partial)
        conn.execute("CREATE TABLE events (id TEXT PRIMARY KEY)")
        conn.commit()
        conn.close()
        interface = RuntimeInterface.from_config(
            dataclasses.replace(
                RuntimeInterfaceConfig(db_path=partial), actor="runtime-http"
            )
        )
        server = RuntimeHttpServer(interface=interface, policy=self.policy)
        status, body, _ = self._request_with_server(server, "GET", "/readyz")
        self.assertEqual(status, 503)

    def test_submit_missing_db_503_and_file_not_created(self):
        missing = os.path.join(self.tmp.name, "absent2.sqlite3")
        interface = RuntimeInterface.from_config(
            dataclasses.replace(
                RuntimeInterfaceConfig(db_path=missing), actor="runtime-http"
            )
        )
        server = RuntimeHttpServer(interface=interface, policy=self.policy)
        status, body, _ = self._request_with_server(
            server,
            "POST",
            "/v1/requests",
            headers=self._headers("discord-bridge", BRIDGE_TOKEN),
            json=self._submit_body(),
        )
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body)["error"]["code"], "unavailable")
        self.assertFalse(os.path.exists(missing))

    # -- C6: systemd unit sections --------------------------------------------

    def test_systemd_unit_start_limit_lives_in_unit_section(self):
        unit_path = REPO_ROOT / "deploy" / "systemd" / "coordinate-runtime-http.service"
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
        self.assertIn("StartLimitIntervalSec", unit_keys)
        self.assertIn("StartLimitBurst", unit_keys)
        self.assertNotIn("StartLimitIntervalSec", service_keys)
        self.assertNotIn("StartLimitBurst", service_keys)
        self.assertIn("Restart", service_keys)
        self.assertIn("RestartSec", service_keys)
        self.assertIn("ConditionPathExists", unit_keys)


class _BlockingBus:
    """Test seam: a delivery bus whose ``send`` blocks until released, proving
    the delivery pump is genuinely in-flight while the HTTP path writes."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls: list[tuple] = []

    def send(self, *, destination, payload, message_key) -> str:
        self.calls.append((destination, payload, message_key))
        self.entered.set()
        if not self.release.wait(30):
            raise TimeoutError("pump bus was never released")
        return f"fake-msg-{len(self.calls)}"


class RollbackJournalConcurrencyTests(HttpServerTestBase):
    """R1c: a daemon-style delivery pump and the HTTP data plane share one
    rollback-journal DB without corruption or duplicated terminal outcomes.
    Both paths really overlap: the pump is blocked mid-delivery on a test seam
    while the HTTP claim -> progress -> report commits on another connection.
    """

    def setUp(self):
        super().setUp()
        self.server = RuntimeHttpServer(
            interface=self._interface(), policy=self.policy
        )

    def test_pump_and_http_overlap_on_rollback_journal(self):
        mode = self.conn.execute("PRAGMA journal_mode").fetchone()[0]
        self.assertNotEqual(mode.lower(), "wal", "must be rollback journal, not WAL")

        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        preset, created = create_delivery(
            self.conn,
            platform="stdout",
            destination="local",
            message_key="preset-1",
            payload={"text": "preset delivery"},
        )
        self.assertTrue(created)
        self.conn.commit()
        preset_id = preset["id"]

        bus = _BlockingBus()
        pump_results: list = []
        pump_errors: list = []

        def _pump():
            conn = initialize(self.db_path)
            try:
                pump_results.append(
                    pump_deliveries(conn, platform="stdout", bus=bus)
                )
            except Exception as exc:  # pragma: no cover - failure path
                pump_errors.append(exc)
            finally:
                conn.close()

        thread = threading.Thread(target=_pump, daemon=True)
        thread.start()
        try:
            self.assertTrue(
                bus.entered.wait(10), "pump never entered the blocking bus"
            )

            # The pump holds the preset delivery in ``sending`` mid-flight.
            sending = list_deliveries(self.conn, status="sending", platform="stdout")
            self.assertEqual([d["id"] for d in sending], [preset_id])

            # While the pump is blocked, run the full HTTP lifecycle.
            async def _lifecycle():
                bridge = self._headers("discord-bridge", BRIDGE_TOKEN)
                agentd = self._headers("mac-qoder", AGENTD_TOKEN)
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"http://127.0.0.1:{self.port}/v1/requests",
                        headers=bridge,
                        json={
                            "workspace_id": "demo",
                            "prompt": "concurrent prompt",
                            "origin": {
                                "platform": "discord",
                                "destination": "ch",
                                "message_id": "m-conc",
                                "session_scope_id": "discord:ch",
                            },
                            "reply": {"platform": "discord", "destination": "ch"},
                            "target_agent": "mac-codex",
                            "idempotency_key": "conc-key-1",
                        },
                    ) as resp:
                        self.assertEqual(resp.status, 200)
                        submitted = await resp.json()
                    job_id = submitted["data"]["job"]["id"]
                    async with session.post(
                        f"http://127.0.0.1:{self.port}/v1/jobs/claim",
                        headers=agentd,
                        json={},
                    ) as resp:
                        self.assertEqual(resp.status, 200)
                        claimed = await resp.json()
                    self.assertTrue(claimed["data"]["claimed"], claimed)
                    attempt_token = claimed["data"]["attempt_token"]
                    lease_id = claimed["data"]["execution_lease"]["lease_id"]
                    async with session.post(
                        f"http://127.0.0.1:{self.port}/v1/jobs/{job_id}/progress",
                        headers=agentd,
                        json={
                            "stage": "working",
                            "summary": "concurrent progress",
                            "attempt_token": attempt_token,
                            "lease_id": lease_id,
                        },
                    ) as resp:
                        self.assertEqual(resp.status, 200)
                    async with session.post(
                        f"http://127.0.0.1:{self.port}/v1/jobs/{job_id}/report",
                        headers=agentd,
                        json={
                            "status": "done",
                            "result": {"response_text": "concurrent done"},
                            "attempt_token": attempt_token,
                            "lease_id": lease_id,
                        },
                    ) as resp:
                        self.assertEqual(resp.status, 200, await resp.text())
                        reported = await resp.json()
                    self.assertTrue(reported["ok"], reported)
                    self.assertTrue(reported["data"]["delivery_created"])
                    return job_id, attempt_token, lease_id

            self.loop.run_until_complete(self._start(self.server))
            try:
                job_id, attempt_token, lease_id = self.loop.run_until_complete(
                    _lifecycle()
                )
            finally:
                self.loop.run_until_complete(self._stop())

            # While the pump is still blocked: exactly one terminal outcome,
            # released lease, one terminal delivery, preset still ``sending``.
            job = row_to_dict(
                self.conn.execute(
                    "SELECT * FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()
            )
            self.assertEqual(job["status"], "done")
            event_types = [e["event_type"] for e in list_events(self.conn, "demo")]
            self.assertEqual(event_types.count("job.completed"), 1)
            self.assertEqual(event_types.count("agent.reported"), 1)
            lease = self.conn.execute(
                "SELECT status FROM execution_attempt_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            self.assertEqual(lease["status"], "released")
            terminal = list_deliveries(self.conn, platform="discord_webhook")
            self.assertEqual(len(terminal), 1)
            self.assertEqual(terminal[0]["status"], "pending")
            self.assertIsNone(terminal[0]["platform_message_id"])
            still_sending = list_deliveries(self.conn, status="sending", platform="stdout")
            self.assertEqual([d["id"] for d in still_sending], [preset_id])
            self.assertFalse(bus.release.is_set())
        finally:
            bus.release.set()
            thread.join(timeout=15)

        self.assertFalse(thread.is_alive())
        self.assertEqual(pump_errors, [], f"pump failed: {pump_errors}")
        self.assertEqual(len(pump_results), 1)
        result = pump_results[0]
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.sent, 1)
        self.assertEqual(result.failed, 0)
        # The preset delivery was sent exactly once and the terminal delivery
        # was never touched by the pump (platform-scoped).
        sent = list_deliveries(self.conn, status="sent", platform="stdout")
        self.assertEqual([d["id"] for d in sent], [preset_id])
        self.assertEqual(sent[0]["attempt_count"], 1)
        self.assertEqual(len(list_deliveries(self.conn, platform="discord_webhook")), 1)
        self.assertEqual(
            self.conn.execute("PRAGMA integrity_check").fetchone()[0], "ok"
        )


class GracefulDrainLifecycleTests(unittest.TestCase):
    """R1c: after SIGTERM, ``serve_forever`` stops accepting requests but does
    not claim ``stopped`` / return until in-flight cleanup completes; releasing
    the cleanup gate lets it return normally (exit code 0)."""

    def test_serve_forever_blocks_until_cleanup_completes_after_sigterm(self):
        records: list[str] = []
        handler = self._attach(records, "coordinate.runtime_http")

        async def _scenario():
            started = asyncio.Event()
            cleanup_blocked = asyncio.Event()
            release_cleanup = asyncio.Event()
            handlers: dict[int, object] = {}

            class _FakeSite:
                async def stop(self):
                    return None

            class _FakeRunner:
                async def cleanup(self):
                    cleanup_blocked.set()
                    await release_cleanup.wait()

            class _StubServer:
                async def start(self, host, port):
                    started.set()
                    return _FakeRunner(), _FakeSite()

            loop = asyncio.get_running_loop()
            with unittest.mock.patch.object(
                loop,
                "add_signal_handler",
                side_effect=lambda sig, callback: handlers.setdefault(sig, callback),
            ):
                task = asyncio.create_task(
                    serve_forever(_StubServer(), "127.0.0.1", 0, drain_timeout=5)
                )
                await asyncio.wait_for(started.wait(), timeout=2)
                handlers[signal.SIGTERM]()
                await asyncio.wait_for(cleanup_blocked.wait(), timeout=2)
                self.assertFalse(task.done())
                self.assertNotIn("runtime-http stopped", records)
                release_cleanup.set()
                self.assertEqual(await asyncio.wait_for(task, timeout=2), 0)

        try:
            asyncio.run(_scenario())
            self.assertIn("runtime-http stopped", records)
        finally:
            logging.getLogger("coordinate.runtime_http").removeHandler(handler)

    @staticmethod
    def _attach(records: list[str], name: str):
        class _Handler(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        handler = _Handler()
        logger = logging.getLogger(name)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return handler


if __name__ == "__main__":
    unittest.main()
