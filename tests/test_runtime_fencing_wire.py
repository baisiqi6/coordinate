"""Exercise advertised runtime capabilities through real HTTP and CLI boundaries.

Every job and credential is a local test fixture. Replies use platform=none.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from unittest.mock import patch

import aiohttp

from coordinate.runtime import submit_request
from tests.test_runtime_http import (
    AGENTD_TOKEN, BRIDGE_TOKEN, HttpServerTestBase, RuntimeHttpServer,
)
from tests.test_runtime_lease import _sync_catalog


class RuntimeFencingWireTests(HttpServerTestBase):
    def setUp(self):
        super().setUp()
        self.server = RuntimeHttpServer(interface=self._interface(), policy=self.policy)

    def _submit(self, *, managed=True):
        if managed:
            _sync_catalog(self.conn, ["mac-codex"])
            self.conn.commit()
        return submit_request(
            self.conn, workspace_id="demo", target_agent="mac-codex",
            prompt="local no-send contract probe",
            origin={"platform": "discord", "destination": "ch", "message_id": "probe-1",
                    "session_scope_id": "discord:ch"},
            reply={"platform": "none", "destination": "ch"},
        ).job["id"]

    def _wire(self, method, path, payload=None, *, bridge=False):
        client, token = ("discord-bridge", BRIDGE_TOKEN) if bridge else ("mac-qoder", AGENTD_TOKEN)
        kwargs = {"headers": self._headers(client, token)}
        if payload is not None:
            kwargs["json"] = payload
        status, body, _ = self._request_with_server(self.server, method, path, **kwargs)
        return status, json.loads(body)

    def _cli(self, *args):
        # The fresh-install gate sets this to the installed console script.
        command = [os.environ["COORDINATE_TEST_CLI"]] if "COORDINATE_TEST_CLI" in os.environ else [sys.executable, "-m", "coordinate"]
        result = subprocess.run(
            [*command, "--db", self.db_path, *args], capture_output=True, text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        return json.loads(result.stdout)

    def _database_snapshot(self):
        return list(self.conn.iterdump())

    def test_advertised_empty_claim_and_reconcile_are_callable(self):
        status, contract = self._wire("GET", "/v1/runtime/contract")
        self.assertEqual(status, 200)
        self.assertTrue(contract["data"]["capabilities"]["claim_fencing"])
        self.assertTrue(contract["data"]["capabilities"]["agent_reconcile"])
        status, claim = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "empty"})
        self.assertEqual(status, 200)
        self.assertFalse(claim["data"]["claimed"])
        status, reconcile = self._wire("GET", "/v1/agents/mac-codex/reconcile")
        self.assertEqual((status, reconcile["data"]), (200, {"active_leases": []}))
        self.assertEqual(self.conn.execute("SELECT count(*) FROM events WHERE event_type='job.claim_request'").fetchone()[0], 0)
        # Empty polling must not permanently consume the operation key.
        job_id = self._submit()
        status, claim = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "empty"})
        self.assertEqual(status, 200)
        self.assertEqual(claim["data"]["job"]["id"], job_id)

    def test_concurrent_same_key_replays_one_managed_attempt(self):
        job_id = self._submit()

        async def run():
            runner, site = await self.server.start("127.0.0.1", self.port)
            try:
                async with aiohttp.ClientSession() as session:
                    async def claim():
                        async with session.post(
                            f"http://127.0.0.1:{self.port}/v1/jobs/claim",
                            headers=self._headers("mac-qoder", AGENTD_TOKEN),
                            json={"claim_request_id": "lost-response"},
                        ) as response:
                            return response.status, await response.json()
                    return await asyncio.gather(claim(), claim())
            finally:
                await site.stop()
                await runner.cleanup()

        results = self.loop.run_until_complete(run())
        self.assertEqual([status for status, _ in results], [200, 200])
        claims = [body["data"] for _, body in results]
        self.assertEqual({c["job"]["id"] for c in claims}, {job_id})
        self.assertEqual({c["attempt_token"] for c in claims}, {1})
        self.assertEqual(len({c["execution_lease"]["lease_id"] for c in claims}), 1)
        self.assertEqual(sum(c.get("replayed", False) for c in claims), 1)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM events WHERE event_type='job.claim_request'").fetchone()[0], 1)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM execution_attempt_leases").fetchone()[0], 1)
        before = self._database_snapshot()
        status, replay = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "lost-response"})
        self.assertEqual(status, 200)
        self.assertTrue(replay["data"]["replayed"])
        self.assertEqual(self._database_snapshot(), before)

    def test_cli_and_http_share_claim_and_reconcile_authority(self):
        job_id = self._submit()
        first = self._cli("runtime", "job", "claim", "--agent-id", "mac-codex", "--claim-request-id", "cli-http")["result"]
        replay = self._cli("runtime", "job", "claim", "--agent-id", "mac-codex", "--claim-request-id", "cli-http")["result"]
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["execution_lease"]["lease_id"], first["execution_lease"]["lease_id"])
        self.assertEqual(first["job"]["id"], job_id)
        cli = self._cli("runtime", "agent", "reconcile", "--agent-id", "mac-codex")["result"]
        status, http = self._wire("GET", "/v1/agents/mac-codex/reconcile")
        self.assertEqual(status, 200)
        for snapshot in (cli, http["data"]):
            for item in snapshot["active_leases"]:
                item.pop("server_now")
        self.assertEqual(cli, http["data"])
        self.assertEqual(len(cli["active_leases"]), 1)

    def test_legacy_running_claim_is_visible_to_both_transports(self):
        job_id = self._submit(managed=False)
        status, first = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "legacy"})
        self.assertEqual(status, 200)
        self.assertNotIn("execution_lease", first["data"])
        status, body = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "legacy"})
        self.assertEqual(status, 200)
        replay = body["data"]
        self.assertTrue(replay["replayed"])
        for snapshot in (
            self._cli("runtime", "agent", "reconcile", "--agent-id", "mac-codex")["result"],
            self._wire("GET", "/v1/agents/mac-codex/reconcile")[1]["data"],
        ):
            self.assertEqual(len(snapshot["active_leases"]), 1)
            self.assertEqual(snapshot["active_leases"][0]["job_id"], job_id)
            self.assertIsNone(snapshot["active_leases"][0]["lease_id"])

    def test_conflicting_and_terminal_replays_do_not_mutate_database(self):
        job_id = self._submit()
        status, first = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "terminal"})
        self.assertEqual(status, 200)
        before = self._database_snapshot()
        status, conflict = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "terminal", "ttl_seconds": 180})
        self.assertEqual((status, conflict["error"]["code"]), (409, "conflict"))
        self.assertEqual(self._database_snapshot(), before)
        claim = first["data"]
        status, report = self._wire("POST", f"/v1/jobs/{job_id}/report", {
            "status": "done", "result": {"response_text": "local result"},
            "attempt_token": claim["attempt_token"],
            "lease_id": claim["execution_lease"]["lease_id"],
        })
        self.assertEqual(status, 200)
        self.assertFalse(report["data"]["delivery_created"])
        before = self._database_snapshot()
        status, conflict = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "terminal"})
        self.assertEqual((status, conflict["error"]["code"]), (409, "conflict"))
        self.assertEqual(self._database_snapshot(), before)
        self.assertEqual(self._wire("GET", "/v1/agents/mac-codex/reconcile")[1]["data"], {"active_leases": []})

    def test_reconcile_cannot_substitute_principal_or_role(self):
        self._submit()
        self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "scope"})
        before = self._database_snapshot()
        self.assertEqual(self._wire("GET", "/v1/agents/other-agent/reconcile")[0], 403)
        self.assertEqual(self._wire("GET", "/v1/agents/mac-codex/reconcile", bridge=True)[0], 403)
        self.assertEqual(self._database_snapshot(), before)

    def test_failed_marker_write_rolls_back_claim_and_lease(self):
        job_id = self._submit()
        with patch("coordinate.runtime_lease._append_claim_request_marker", side_effect=RuntimeError("test marker failure")):
            status, _ = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "atomic"})
        self.assertEqual(status, 500)
        job = self.conn.execute("SELECT status, attempt_count FROM jobs WHERE id=?", (job_id,)).fetchone()
        self.assertEqual(tuple(job), ("pending", 0))
        self.assertEqual(self.conn.execute("SELECT count(*) FROM execution_attempt_leases").fetchone()[0], 0)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM events WHERE event_type='job.claim_request'").fetchone()[0], 0)
        status, claim = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "atomic"})
        self.assertEqual(status, 200)
        self.assertEqual(claim["data"]["attempt_token"], 1)

    def test_corrupt_marker_authority_fails_closed(self):
        self._submit()
        self.assertEqual(self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "corrupt"})[0], 200)
        marker = self.conn.execute("SELECT * FROM events WHERE event_type='job.claim_request'").fetchone()
        original = json.loads(marker["payload_json"])
        for field, value in (("attempt_token", True), ("ttl", False), ("lease_id", "missing"), ("job_id", "missing"), ("agent_id", "other")):
            with self.subTest(field=field):
                self.conn.execute("UPDATE events SET payload_json=? WHERE id=?", (json.dumps({**original, field: value}), marker["id"]))
                self.conn.commit()
                before = self._database_snapshot()
                status, body = self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "corrupt"})
                self.assertEqual((status, body["error"]["code"]), (409, "conflict"))
                self.assertEqual(self._database_snapshot(), before)
        self.conn.execute("UPDATE events SET payload_json=?, actor=? WHERE id=?", (marker["payload_json"], "other-agent", marker["id"]))
        self.conn.commit()
        before = self._database_snapshot()
        self.assertEqual(self._wire("POST", "/v1/jobs/claim", {"claim_request_id": "corrupt"})[0], 409)
        self.assertEqual(self._database_snapshot(), before)
