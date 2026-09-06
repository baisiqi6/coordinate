from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

import aiohttp
from aiohttp import web

from coordinate.execution_cli import handle_runtime_agent_reconcile
from coordinate.runtime_http import RuntimeHttpServer, load_auth_policy
from coordinate.runtime_interface import RuntimeInterface


class ReconcileFacadeTests(unittest.TestCase):
    def test_minimal_projection_and_redaction(self) -> None:
        conn = Mock()
        conn.execute.return_value.fetchall.return_value = []
        leases = [
            {
                "job_id": "job-1",
                "lease_id": "lease-1",
                "attempt_token": 3,
                "status": "active",
                "expires_at": "2030-01-01T00:01:00Z",
                "resource_key": "secret-resource",
                "normalized_path": "/private/worktree",
            }
        ]
        with patch("coordinate.runtime_interface.list_active_leases_for_agent", return_value=leases), patch(
            "coordinate.runtime_interface.utc_now", return_value="2030-01-01T00:00:00Z"
        ):
            result = RuntimeInterface(connection_factory=lambda: conn).reconcile_agent(
                agent_id="agent-1"
            )
        self.assertTrue(result["ok"])
        self.assertEqual(
            result["data"],
            {
                "active_leases": [
                    {
                        "job_id": "job-1",
                        "lease_id": "lease-1",
                        "attempt_token": 3,
                        "status": "active",
                        "expires_at": "2030-01-01T00:01:00Z",
                        "server_now": "2030-01-01T00:00:00Z",
                    }
                ]
            },
        )
        self.assertNotIn("resource_key", json.dumps(result))
        self.assertNotIn("normalized_path", json.dumps(result))

    def test_empty_snapshot_is_authoritative(self) -> None:
        conn = Mock()
        conn.execute.return_value.fetchall.return_value = []
        with patch("coordinate.runtime_interface.list_active_leases_for_agent", return_value=[]):
            result = RuntimeInterface(connection_factory=lambda: conn).reconcile_agent(
                agent_id="agent-1"
            )
        self.assertEqual(result["data"], {"active_leases": []})

    def test_untyped_running_job_is_not_reported_as_clear(self) -> None:
        conn = Mock()
        conn.execute.return_value.fetchall.return_value = [
            {"id": "legacy-job", "attempt_count": 2, "status": "running"}
        ]
        with patch("coordinate.runtime_interface.list_active_leases_for_agent", return_value=[]):
            result = RuntimeInterface(connection_factory=lambda: conn).reconcile_agent(
                agent_id="agent-1"
            )
        self.assertEqual(result["data"]["active_leases"][0]["job_id"], "legacy-job")
        self.assertIsNone(result["data"]["active_leases"][0]["lease_id"])


class ReconcileCliTests(unittest.TestCase):
    def test_cli_contract_and_minimal_output(self) -> None:
        conn = Mock()
        conn.__enter__ = Mock(return_value=conn)
        conn.__exit__ = Mock(return_value=None)
        conn.execute.return_value.fetchall.return_value = []
        leases = [
            {
                "job_id": "job-1",
                "lease_id": "lease-1",
                "attempt_token": 1,
                "status": "active",
                "expires_at": "2030-01-01T00:01:00Z",
                "normalized_path": "/secret",
            }
        ]
        args = type("Args", (), {"agent_id": "agent-1", "db": ":memory:"})()
        with patch("coordinate.execution_cli._conn", return_value=conn), patch(
            "coordinate.execution_cli.list_active_leases_for_agent", return_value=leases
        ), patch("coordinate.execution_cli.utc_now", return_value="2030-01-01T00:00:00Z"):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(handle_runtime_agent_reconcile(args), 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(set(payload), {"result"})
        self.assertEqual(set(payload["result"]["active_leases"][0]), {
            "job_id", "lease_id", "attempt_token", "status", "expires_at", "server_now"
        })
        self.assertNotIn("/secret", out.getvalue())


class ReconcileHttpTests(unittest.TestCase):
    def test_agentd_only_and_agent_scoped(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                policy_path = Path(tmp) / "policy.json"
                agent_token = "agent-token"
                bridge_token = "bridge-token"
                policy_path.write_text(
                    json.dumps(
                        {
                            "version": 1,
                            "clients": [
                                {
                                    "client_id": "agent-client",
                                    "role": "agentd",
                                    "token_sha256": hashlib.sha256(agent_token.encode()).hexdigest(),
                                    "agent_id": "agent-1",
                                },
                                {
                                    "client_id": "bridge-client",
                                    "role": "bridge",
                                    "token_sha256": hashlib.sha256(bridge_token.encode()).hexdigest(),
                                    "platforms": ["discord"],
                                    "workspace_ids": ["ws"],
                                },
                            ],
                        }
                    )
                )
                policy_path.chmod(0o600)

                class Server(RuntimeHttpServer):
                    async def _call(self, principal, fn):
                        class Interface:
                            def reconcile_agent(self, *, agent_id):
                                return {"ok": True, "data": {"active_leases": []}, "error": None}

                        return fn(Interface())

                server = Server(interface=Mock(), policy=load_auth_policy(policy_path))
                runner = web.AppRunner(server.build_app(), access_log=None)
                await runner.setup()
                site = web.TCPSite(runner, "127.0.0.1", 0)
                await site.start()
                port = site._server.sockets[0].getsockname()[1]
                try:
                    async with aiohttp.ClientSession() as session:
                        agent_headers = {
                            "X-Coordinate-Client-ID": "agent-client",
                            "Authorization": f"Bearer {agent_token}",
                        }
                        async with session.get(
                            f"http://127.0.0.1:{port}/v1/agents/agent-1/reconcile",
                            headers=agent_headers,
                        ) as response:
                            self.assertEqual(response.status, 200)
                            self.assertEqual((await response.json())["data"], {"active_leases": []})
                        async with session.get(
                            f"http://127.0.0.1:{port}/v1/agents/other/reconcile",
                            headers=agent_headers,
                        ) as response:
                            self.assertEqual(response.status, 403)
                        bridge_headers = {
                            "X-Coordinate-Client-ID": "bridge-client",
                            "Authorization": f"Bearer {bridge_token}",
                        }
                        async with session.get(
                            f"http://127.0.0.1:{port}/v1/agents/agent-1/reconcile",
                            headers=bridge_headers,
                        ) as response:
                            self.assertEqual(response.status, 403)
                finally:
                    await runner.cleanup()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
