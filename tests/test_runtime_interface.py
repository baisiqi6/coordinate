"""R2A focused tests: bounded RuntimeInterface (7 use cases) vs domain oracle.

The facade is the shared submit/get core for MCP (R1) and HTTP (R2A). These
tests prove authority parity with direct domain calls on the same SQLite
fixture, per-call connection closure and fixed actor semantics.
"""

from __future__ import annotations

import dataclasses
import os
import sqlite3
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from coordinate.db import (
    bind_channel_workspace,
    initialize,
    list_events,
    row_to_dict,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.executor_capacity import (
    CapacityCatalog,
    CapacityPolicy,
    compute_capacity_catalog_hash,
    sync_capacity_catalog,
)
from coordinate.executor_identity import (
    ExecutorCatalog,
    ExecutorDefinition,
    ExecutorInstanceBinding,
    compute_executor_catalog_hash,
    sync_executor_catalog,
)
from coordinate.job_repository import get_job as db_get_job, list_jobs
from coordinate.runtime import (
    claim_job as domain_claim_job,
    register_agent,
    report_job_result as domain_report_job_result,
    submit_request as domain_submit_request,
)
from coordinate.runtime_interface import (
    MESSAGE_CONFLICT,
    MESSAGE_JOB_NOT_FOUND,
    RuntimeInterface,
    RuntimeInterfaceConfig,
    classify_error,
)
from coordinate.runtime_lease import RuntimeLeaseError


def _sync_catalog(conn: sqlite3.Connection, agent_ids: list[str]):
    definitions = (
        ExecutorDefinition(
            id="coder",
            provider="kimi-code",
            adapter="omp",
            capabilities=("coding",),
        ),
    )
    bindings = tuple(
        ExecutorInstanceBinding(
            agent_id=aid,
            executor_definition_id="coder",
            runner_profile_id=aid,
            enabled=True,
        )
        for aid in agent_ids
    )
    catalog = ExecutorCatalog(
        source_id="multinexus.discord",
        source_version=2,
        catalog_hash="",
        source_path="/dev/null",
        definitions=definitions,
        bindings=bindings,
    )
    catalog = dataclasses.replace(catalog, catalog_hash=compute_executor_catalog_hash(catalog))
    sync_executor_catalog(conn, catalog)

    policies = tuple(CapacityPolicy(agent_id=aid, max_concurrent_jobs=2) for aid in agent_ids)
    capacity = CapacityCatalog(
        source_id="multinexus.discord.capacity",
        source_version=1,
        catalog_hash="",
        source_path="/dev/null",
        policies=policies,
    )
    capacity = dataclasses.replace(capacity, catalog_hash=compute_capacity_catalog_hash(capacity))
    sync_capacity_catalog(conn, capacity)


class FacadeTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "r2a.sqlite3")
        self.conn = initialize(self.db_path)
        self.addCleanup(self.conn.close)
        self._seed()

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
        self.conn.commit()

    def _interface(self, actor: str = "runtime-http") -> RuntimeInterface:
        return RuntimeInterface.from_config(
            dataclasses.replace(
                RuntimeInterfaceConfig(db_path=self.db_path, actor=actor)
            )
        )

    def _origin(self, message_id: str = "m1") -> dict:
        return {
            "platform": "discord",
            "destination": "ch",
            "message_id": message_id,
            "session_scope_id": "discord:ch",
        }

    def _reply(self) -> dict:
        return {"platform": "discord", "destination": "ch"}

    def _submit(self) -> str:
        envelope = self._interface().submit_request(**self._submit_args())
        self.assertTrue(envelope["ok"], envelope)
        return envelope["data"]["job"]["id"]

    def _submit_args(self, **overrides) -> dict:
        args = {
            "workspace_id": "demo",
            "prompt": "hello",
            "origin": self._origin(),
            "reply": self._reply(),
            "target_agent": "mac-codex",
            "idempotency_key": "k1",
        }
        args.update(overrides)
        return args


class ResolveTests(FacadeTestBase):
    def test_unbound_channel_returns_bound_false(self):
        envelope = self._interface().resolve_channel_workspace(
            platform="discord", channel_id="ch-absent"
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(envelope["data"], {"bound": False, "binding": None})

    def test_bound_channel_matches_domain_oracle(self):
        bind_channel_workspace(
            self.conn,
            platform="discord",
            channel_id="ch-1",
            workspace_id="demo",
            actor="test",
            reason="test",
            idempotency_key="bind-1",
        )
        self.conn.commit()
        envelope = self._interface().resolve_channel_workspace(
            platform="discord", channel_id="ch-1"
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertTrue(envelope["data"]["bound"])
        self.assertEqual(
            envelope["data"]["binding"]["workspace_id"], "demo"
        )

    def test_missing_keys_invalid(self):
        interface = self._interface()
        for kwargs in (
            {"platform": None, "channel_id": "c"},
            {"platform": "discord", "channel_id": None},
            {"platform": "", "channel_id": "c"},
        ):
            envelope = interface.resolve_channel_workspace(**kwargs)
            self.assertFalse(envelope["ok"])
            self.assertEqual(envelope["error"]["code"], "invalid_request")


class SubmitTests(FacadeTestBase):
    def test_submit_exact_matches_domain_oracle(self):
        envelope = self._interface().submit_request(**self._submit_args())
        self.assertTrue(envelope["ok"], envelope)
        expected = domain_submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin=self._origin(),
            reply=self._reply(),
            actor="runtime-http",
            idempotency_key="k1",
        ).to_dict()
        self.assertEqual(envelope["data"]["job"], expected["job"])
        self.assertEqual(envelope["data"]["event"]["event_type"], "request.received")

    def test_submit_actor_fixed_from_config(self):
        envelope = self._interface(actor="bridge-42").submit_request(
            **self._submit_args()
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(envelope["data"]["event"]["actor"], "bridge-42")

    def test_submit_has_no_actor_parameter(self):
        import inspect

        params = inspect.signature(RuntimeInterface.submit_request).parameters
        self.assertNotIn("actor", params)

    def test_submit_replay_no_duplicate_event_or_job(self):
        interface = self._interface()
        first = interface.submit_request(**self._submit_args())
        second = interface.submit_request(**self._submit_args())
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertFalse(second["data"]["event_created"])
        self.assertFalse(second["data"]["job_created"])
        self.assertEqual(
            len(list_events(self.conn, "demo")), 1
        )

    def test_submit_conflict_fails_closed(self):
        interface = self._interface()
        first = interface.submit_request(**self._submit_args(prompt="hello"))
        second = interface.submit_request(**self._submit_args(prompt="DIFFERENT"))
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["error"]["code"], "conflict")
        self.assertEqual(second["error"]["message"], "request replay conflict")

    def test_submit_validation_shapes(self):
        interface = self._interface()
        bad_cases = [
            {"idempotency_key": ""},
            {"workspace_id": None},
            {"prompt": "   "},
            {"origin": "not-an-object"},
            {"reply": None},
            {"target_agent": None, "routing_request": None},
            {"target_agent": "a", "routing_request": {"required_capabilities": ["x"]}},
            {"routing_request": {"required_capabilities": ["x"], "routing_request_id": "forged"}},
        ]
        for overrides in bad_cases:
            envelope = interface.submit_request(**self._submit_args(**overrides))
            self.assertFalse(envelope["ok"], overrides)
            self.assertEqual(envelope["error"]["code"], "invalid_request", overrides)


class JobGetTests(FacadeTestBase):
    def test_job_id_only_matches_domain_oracle(self):
        job_id = self._submit()
        envelope = self._interface().get_job(job_id)
        self.assertTrue(envelope["ok"], envelope)
        expected = row_to_dict(db_get_job(self.conn, job_id))
        self.assertEqual(envelope["data"], expected)

    def test_workspace_bound_get_matches(self):
        job_id = self._submit()
        envelope = self._interface().get_job(job_id, workspace_id="demo")
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(envelope["data"]["workspace_id"], "demo")

    def test_workspace_mismatch_is_not_found(self):
        job_id = self._submit()
        envelope = self._interface().get_job(job_id, workspace_id="other-ws")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")
        self.assertEqual(envelope["error"]["message"], MESSAGE_JOB_NOT_FOUND)

    def test_missing_job_not_found(self):
        envelope = self._interface().get_job("request:missing")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")

    def test_missing_id_invalid(self):
        envelope = self._interface().get_job(None)
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")


class ClaimTests(FacadeTestBase):
    def test_claim_happy_path(self):
        job_id = self._submit()
        envelope = self._interface().claim_job(agent_id="mac-codex")
        self.assertTrue(envelope["ok"], envelope)
        self.assertTrue(envelope["data"]["claimed"])
        self.assertEqual(envelope["data"]["job"]["id"], job_id)
        self.assertEqual(envelope["data"]["job"]["status"], "running")
        self.assertIsNotNone(envelope["data"]["attempt_token"])

    def test_claim_queue_empty_claimed_false(self):
        self._submit()
        interface = self._interface()
        first = interface.claim_job(agent_id="mac-codex")
        self.assertTrue(first["ok"])
        second = interface.claim_job(agent_id="mac-codex")
        self.assertTrue(second["ok"])
        self.assertFalse(second["data"]["claimed"])
        self.assertIsNone(second["data"]["job"])

    def test_claim_reap_mode_validation(self):
        interface = self._interface()
        # global works for untyped agents (ordinary poll).
        envelope = interface.claim_job(agent_id="mac-codex", reap_mode="global")
        self.assertTrue(envelope["ok"], envelope)
        # none requires a typed agent: untyped fails closed.
        envelope = interface.claim_job(agent_id="mac-codex", reap_mode="none")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")
        # unknown mode fails closed.
        envelope = interface.claim_job(agent_id="mac-codex", reap_mode="exact")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_claim_reap_mode_none_typed_happy(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        self._submit()
        envelope = self._interface().claim_job(
            agent_id="mac-codex", reap_mode="none", reap_reason="bounded test reason"
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertTrue(envelope["data"]["claimed"])

    def test_claim_ttl_validation(self):
        envelope = self._interface().claim_job(agent_id="mac-codex", ttl_seconds=0)
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_claim_rejects_recoverable_shape(self):
        # The facade signature is the authority surface: no recoverable/
        # recovery_reason/prior_process_stopped parameters exist at all.
        import inspect

        params = inspect.signature(RuntimeInterface.claim_job).parameters
        for forbidden in (
            "recoverable",
            "recovery_reason",
            "prior_process_stopped",
            "actor",
        ):
            self.assertNotIn(forbidden, params)


class ProgressReportRenewTests(FacadeTestBase):
    def _claim(self) -> tuple[str, int | None]:
        job_id = self._submit()
        envelope = self._interface().claim_job(agent_id="mac-codex")
        self.assertTrue(envelope["ok"], envelope)
        return job_id, envelope["data"]["attempt_token"]

    def test_progress_happy_path(self):
        job_id, attempt_token = self._claim()
        envelope = self._interface().record_job_progress(
            job_id=job_id,
            agent_id="mac-codex",
            stage="working",
            summary="committed two commits",
            attempt_token=attempt_token,
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(envelope["data"]["job"]["status"], "running")

    def test_progress_stale_attempt_fails_closed(self):
        job_id, _ = self._claim()
        envelope = self._interface().record_job_progress(
            job_id=job_id,
            agent_id="mac-codex",
            stage="working",
            attempt_token=999,
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "conflict")

    def test_progress_identity_mismatch_fails_closed(self):
        job_id, attempt_token = self._claim()
        envelope = self._interface().record_job_progress(
            job_id=job_id,
            agent_id="other-agent",
            attempt_token=attempt_token,
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "conflict")

    def test_report_done_creates_events_and_delivery(self):
        job_id, attempt_token = self._claim()
        envelope = self._interface().report_job_result(
            job_id=job_id,
            agent_id="mac-codex",
            status="done",
            result={"response_text": "finished"},
            attempt_token=attempt_token,
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertTrue(envelope["data"]["delivery_created"])
        job = row_to_dict(db_get_job(self.conn, job_id))
        self.assertEqual(job["status"], "done")
        event_types = [e["event_type"] for e in list_events(self.conn, "demo")]
        self.assertIn("job.completed", event_types)
        self.assertIn("agent.reported", event_types)

    def test_report_status_validation(self):
        job_id, attempt_token = self._claim()
        envelope = self._interface().report_job_result(
            job_id=job_id,
            agent_id="mac-codex",
            status="bogus",
            result={},
            attempt_token=attempt_token,
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_report_identity_mismatch_fails_closed(self):
        job_id, attempt_token = self._claim()
        envelope = self._interface().report_job_result(
            job_id=job_id,
            agent_id="other-agent",
            status="done",
            result={},
            attempt_token=attempt_token,
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "conflict")

    def test_report_late_attempt_fails_closed(self):
        job_id, _ = self._claim()
        envelope = self._interface().report_job_result(
            job_id=job_id,
            agent_id="mac-codex",
            status="done",
            result={},
            attempt_token=999,
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "conflict")

    def test_renew_managed_lease_happy_path(self):
        # Typed agent: claim returns a real execution lease that can renew.
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        job_id = self._submit()
        claim = self._interface().claim_job(agent_id="mac-codex")
        self.assertTrue(claim["ok"], claim)
        self.assertIsNotNone(claim["data"]["execution_lease"])
        lease_id = claim["data"]["execution_lease"]["lease_id"]
        attempt_token = claim["data"]["attempt_token"]

        # Domain refuses renewals that do not advance expires_at; shrink the
        # stored expiry by one second (still active) so the renewal must
        # advance it (now+ttl > expires_at).
        from datetime import datetime, timedelta, timezone

        current_expiry = claim["data"]["execution_lease"]["expires_at"]
        shrunk = (
            datetime.fromisoformat(current_expiry.replace("Z", "+00:00"))
            - timedelta(seconds=1)
        ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.conn.execute(
            "UPDATE execution_attempt_leases SET expires_at = ? WHERE lease_id = ?",
            (shrunk, lease_id),
        )
        self.conn.commit()

        envelope = self._interface().renew_managed_lease(
            lease_id=lease_id,
            job_id=job_id,
            attempt_token=attempt_token,
            agent_id="mac-codex",
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(envelope["data"]["status"], "active")

        # The stored lease really advanced (DB semantics, not just HTTP 200).
        row = self.conn.execute(
            "SELECT expires_at FROM execution_attempt_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        self.assertEqual(row["expires_at"], envelope["data"]["expires_at"])
        self.assertGreater(envelope["data"]["expires_at"], "2020-01-01T00:00:01Z")

    def test_renew_unknown_lease_fails_closed(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        job_id = self._submit()
        claim = self._interface().claim_job(agent_id="mac-codex")
        attempt_token = claim["data"]["attempt_token"]
        envelope = self._interface().renew_managed_lease(
            lease_id="lease:missing",
            job_id=job_id,
            attempt_token=attempt_token,
            agent_id="mac-codex",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "conflict")

    def test_renew_expired_lease_fails_closed(self):
        _sync_catalog(self.conn, ["mac-codex"])
        self.conn.commit()
        job_id = self._submit()
        claim = self._interface().claim_job(agent_id="mac-codex")
        lease_id = claim["data"]["execution_lease"]["lease_id"]
        attempt_token = claim["data"]["attempt_token"]
        self.conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', "
            "    renewed_at = '2020-01-01T00:00:00Z', "
            "    expires_at = '2020-01-01T00:00:01Z' "
            "WHERE lease_id = ?",
            (lease_id,),
        )
        self.conn.commit()
        envelope = self._interface().renew_managed_lease(
            lease_id=lease_id,
            job_id=job_id,
            attempt_token=attempt_token,
            agent_id="mac-codex",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "conflict")

    def test_renew_validation_shapes(self):
        interface = self._interface()
        for kwargs in (
            {"lease_id": None, "job_id": "j", "attempt_token": 1, "agent_id": "a"},
            {"lease_id": "l", "job_id": None, "attempt_token": 1, "agent_id": "a"},
            {"lease_id": "l", "job_id": "j", "attempt_token": None, "agent_id": "a"},
            {"lease_id": "l", "job_id": "j", "attempt_token": "1", "agent_id": "a"},
            {"lease_id": "l", "job_id": "j", "attempt_token": 1, "agent_id": ""},
            {"lease_id": "l", "job_id": "j", "attempt_token": 1, "agent_id": "a", "ttl_seconds": 0},
        ):
            envelope = interface.renew_managed_lease(**kwargs)
            self.assertFalse(envelope["ok"], kwargs)
            self.assertEqual(envelope["error"]["code"], "invalid_request", kwargs)


class ConnectionClosureTests(FacadeTestBase):
    def _tracked_interface(self, created):
        def factory():
            conn = initialize(self.db_path)
            created.append(conn)
            return conn

        return RuntimeInterface(connection_factory=factory), created

    @staticmethod
    def _assert_closed(conn):
        with unittest.TestCase().assertRaises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")

    def test_every_use_case_closes_its_connection(self):
        # resolve (unbound) + submit + get + claim + progress + report +
        # renew-validation: each call must create and close exactly one
        # connection, including failure paths.
        created = []
        interface, created = self._tracked_interface(created)

        envelope = interface.resolve_channel_workspace(
            platform="discord", channel_id="nope"
        )
        self.assertTrue(envelope["ok"])
        self.assertEqual(len(created), 1)
        self._assert_closed(created[0])

        envelope = interface.submit_request(**self._submit_args())
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(len(created), 2)
        self._assert_closed(created[-1])

        job_id = envelope["data"]["job"]["id"]
        envelope = interface.get_job(job_id, workspace_id="demo")
        self.assertTrue(envelope["ok"])
        self.assertEqual(len(created), 3)
        self._assert_closed(created[-1])

        envelope = interface.claim_job(agent_id="mac-codex")
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(len(created), 4)
        self._assert_closed(created[-1])

        attempt_token = envelope["data"]["attempt_token"]
        envelope = interface.record_job_progress(
            job_id=job_id, agent_id="mac-codex", attempt_token=attempt_token
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(len(created), 5)
        self._assert_closed(created[-1])

        envelope = interface.report_job_result(
            job_id=job_id,
            agent_id="mac-codex",
            status="done",
            result={},
            attempt_token=attempt_token,
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(len(created), 6)
        self._assert_closed(created[-1])

        envelope = interface.claim_job(agent_id="mac-codex", reap_mode="bogus")
        self.assertFalse(envelope["ok"])
        # Facade validation fails before any connection is created.
        self.assertEqual(len(created), 6)

    def test_unexpected_failure_closes_connection(self):
        created = []
        interface, created = self._tracked_interface(created)
        with unittest.mock.patch(
            "coordinate.runtime_interface.resolve_channel_workspace",
            side_effect=TypeError("boom"),
        ):
            envelope = interface.resolve_channel_workspace(
                platform="discord", channel_id="c"
            )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "internal")
        self.assertEqual(len(created), 1)
        self._assert_closed(created[0])


class ClassifierBoundaryTests(unittest.TestCase):
    """R1a C3: the conflict classifier uses bounded markers; integrity and
    shape failures are never swallowed into 409."""

    def test_late_result_managed_attempt_is_conflict(self):
        """R1c: a late result for a managed attempt is a state/authority
        conflict (409), never a 400 shape error or a 500."""
        code, message = classify_error(
            RuntimeLeaseError(
                "job r-1 late-result rejected: current attempt is managed"
            )
        )
        self.assertEqual(code, "conflict")
        self.assertEqual(message, MESSAGE_CONFLICT)

    def test_lease_integrity_failure_is_not_conflict(self):
        code, _ = classify_error(
            RuntimeError("stored lease resource snapshot is tampered: boom")
        )
        self.assertNotEqual(code, "conflict")

    def test_lease_row_messages_are_conflict(self):
        for message in (
            "lease 'l1' not found",
            "lease 'l1' has expired",
            "lease 'l1' is released",
            "lease 'l1' is expired",
        ):
            with self.subTest(message=message):
                code, _ = classify_error(RuntimeError(message))
                self.assertEqual(code, "conflict", message)

    def test_shape_and_config_failures_stay_invalid(self):
        # Domain validation failures are ValueError subclasses; the classifier
        # must keep them on 400 unless a conflict marker matches.
        for message in (
            "TTL must be between 30 and 600 seconds",
            "release reason must not be empty",
            "attempt_token must be an integer",
            "reap_mode must be 'global' or 'none'",
        ):
            with self.subTest(message=message):
                code, _ = classify_error(ValueError(message))
                self.assertEqual(code, "invalid_request", message)


class EnvelopeShapeTests(FacadeTestBase):
    def test_envelope_shape_stable(self):
        interface = self._interface()
        envelope = interface.claim_job(agent_id="mac-codex")
        self.assertEqual(set(envelope), {"ok", "data", "error"})
        self.assertIsNone(envelope["error"])
        missing = interface.get_job("request:missing")
        self.assertEqual(set(missing), {"ok", "data", "error"})
        self.assertIsNone(missing["data"])
        self.assertEqual(set(missing["error"]), {"code", "message"})
        self.assertLessEqual(len(missing["error"]["message"]), 64)


if __name__ == "__main__":
    unittest.main()
