"""P9-3B runtime lease orchestration focused tests.

These tests cover the runtime-level atomic claim, reap, renewal, managed-mutation
rejection, CLI delegation, queue order, capacity gating, and resource skip
visibility that live in ``runtime_lease.py`` and ``runtime.py``.
"""
from __future__ import annotations

import dataclasses
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from typing import Any
from types import SimpleNamespace
from unittest.mock import Mock, patch

from coordinate.db import get_job, initialize, list_events, row_to_dict, set_workspace_agent, upsert_workspace, upsert_workspace_host_profile
from coordinate.execution_leases import LEASE_DEFAULT_TTL_SECONDS, get_attempt_lease
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
from coordinate.execution_cli import (
    handle_runtime_job_lease_reap,
    handle_runtime_job_lease_renew,
    handle_runtime_job_progress,
    handle_runtime_job_report,
)
from coordinate.executor_routing import build_routing_request
from coordinate.runtime import (
    RuntimeError as CoordinateRuntimeError,
    claim_job,
    deactivate_agent,
    record_job_progress,
    register_agent,
    report_job_result,
    submit_request,
)
from coordinate.runtime_lease import (
    RuntimeLeaseError,
    _validate_claim_reap_policy,
    reap_due_leases,
    reap_exact_lease,
    renew_managed_lease,
    select_claim_candidate,
)


def _sync_catalog(conn: sqlite3.Connection, agent_ids: list[str], max_jobs: int = 2):
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

    policies = tuple(CapacityPolicy(agent_id=aid, max_concurrent_jobs=max_jobs) for aid in agent_ids)
    capacity = CapacityCatalog(
        source_id="multinexus.discord.capacity",
        source_version=1,
        catalog_hash="",
        source_path="/dev/null",
        policies=policies,
    )
    capacity = dataclasses.replace(capacity, catalog_hash=compute_capacity_catalog_hash(capacity))
    sync_capacity_catalog(conn, capacity)


def _resync_capacity(conn: sqlite3.Connection, agent_ids: list[str], max_jobs: int):
    """Re-sync capacity with a bumped version so tests can change limits."""
    policies = tuple(CapacityPolicy(agent_id=aid, max_concurrent_jobs=max_jobs) for aid in agent_ids)
    capacity = CapacityCatalog(
        source_id="multinexus.discord.capacity",
        source_version=2,
        catalog_hash="",
        source_path="/dev/null",
        policies=policies,
    )
    capacity = dataclasses.replace(capacity, catalog_hash=compute_capacity_catalog_hash(capacity))
    sync_capacity_catalog(conn, capacity)


class RuntimeLeaseClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        register_agent(self.conn, agent_id="mac-codex", host_id="mac", capabilities={})
        _sync_catalog(self.conn, ["mac-omp"])

    def _origin(self):
        return {
            "platform": "discord",
            "destination": "ch",
            "message_id": "m1",
            "session_scope_id": "discord:ch",
        }

    def _reply(self):
        return {"platform": "discord", "destination": "ch"}

    def _submit_exact(self, target_agent="mac-omp", **overrides):
        params = {
            "workspace_id": "demo",
            "target_agent": target_agent,
            "prompt": "hello",
            "origin": self._origin(),
            "reply": self._reply(),
        }
        params.update(overrides)
        return submit_request(self.conn, **params)


    def test_typed_claim_returns_execution_lease(self):
        request = self._submit_exact()
        result = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(result.claimed)
        self.assertEqual(result.job["status"], "running")
        self.assertEqual(result.attempt_token, 1)
        self.assertIsNotNone(result.execution_lease)
        lease = result.execution_lease
        self.assertEqual(lease["contract_version"], 1)
        self.assertEqual(lease["job_id"], request.job["id"])
        self.assertEqual(lease["agent_id"], "mac-omp")
        self.assertEqual(lease["ttl_seconds"], LEASE_DEFAULT_TTL_SECONDS)
        self.assertIn("renew_interval_seconds", lease)
        self.assertTrue(lease["resource_key"].startswith("sha256:"))
        self.assertTrue(lease["capacity_policy_id"].startswith("sha256:"))

    def test_legacy_untyped_claim_has_no_lease(self):
        _request = self._submit_exact(target_agent="mac-codex")
        result = claim_job(self.conn, agent_id="mac-codex")
        self.assertTrue(result.claimed)
        self.assertEqual(result.job["status"], "running")
        self.assertIsNone(result.execution_lease)

    def test_claim_stores_lease_row(self):
        request = self._submit_exact()
        result = claim_job(self.conn, agent_id="mac-omp")
        lease_id = result.execution_lease["lease_id"]
        row = get_attempt_lease(self.conn, lease_id)
        self.assertIsNotNone(row)
        self.assertEqual(row["job_id"], request.job["id"])
        self.assertEqual(row["attempt_token"], 1)
        self.assertEqual(row["agent_id"], "mac-omp")
        self.assertEqual(row["status"], "active")

    def test_second_claim_for_same_agent_returns_empty(self):
        self._submit_exact()
        claim_job(self.conn, agent_id="mac-omp")
        second = claim_job(self.conn, agent_id="mac-omp")
        self.assertFalse(second.claimed)
        self.assertIsNone(second.job)

    def test_claim_respects_capacity_limit(self):
        _resync_capacity(self.conn, ["mac-omp"], max_jobs=1)
        # Create a second workspace with a different path so the second job targets
        # a different resource for the same typed agent.
        ws2 = f"demo-{uuid.uuid4().hex[:8]}"
        path2 = f"{self.tmp.name}/other"
        os.makedirs(path2, exist_ok=True)
        upsert_workspace(
            self.conn,
            workspace_id=ws2,
            name="Demo2",
            path=path2,
            harness_root=path2,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id=ws2,
            host_id="mac",
            workspace_path=path2,
            harness_root=path2,
        )
        # Submit both jobs, then inspect which sorts first and assert that one is claimed.
        self._submit_exact(workspace_id=ws2)
        self._submit_exact()
        claim1 = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim1.claimed)
        claimed_id = claim1.job["id"]
        self.assertEqual(get_job(self.conn, claimed_id)["status"], "running")
        claim2 = claim_job(self.conn, agent_id="mac-omp")
        self.assertFalse(claim2.claimed)
        # The claimed job is still running; capacity is saturated.


class RuntimeLeaseTerminalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        register_agent(
            self.conn, agent_id="mac-sentinel", host_id="mac", capabilities={}
        )
        _sync_catalog(self.conn, ["mac-omp", "mac-sentinel"])
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        self.job_id = request.job["id"]
        claim = claim_job(self.conn, agent_id="mac-omp")
        self.lease_id = claim.execution_lease["lease_id"]
        self.attempt_token = claim.attempt_token

    def test_report_with_lease_releases_and_completes(self):
        result = report_job_result(
            self.conn,
            job_id=self.job_id,
            agent_id="mac-omp",
            status="done",
            result={"response_text": "ok"},
            attempt_token=self.attempt_token,
            lease_id=self.lease_id,
        )
        self.assertEqual(result.job["status"], "done")
        lease = get_attempt_lease(self.conn, self.lease_id)
        self.assertEqual(lease["status"], "released")
        self.assertEqual(lease["release_reason"], "job_done")

    def test_report_without_lease_id_rejects_managed_attempt(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "requires lease_id"):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "ok"},
                attempt_token=self.attempt_token,
            )

    def test_report_wrong_lease_id_rejects(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "lease"):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "ok"},
                attempt_token=self.attempt_token,
                lease_id="00000000-0000-0000-0000-000000000000",
            )

    def test_progress_with_lease_writes(self):
        result = record_job_progress(
            self.conn,
            job_id=self.job_id,
            agent_id="mac-omp",
            stage="editing",
            summary="ok",
            attempt_token=self.attempt_token,
            lease_id=self.lease_id,
        )
        self.assertEqual(result.job["progress"]["stage"], "editing")

    def test_progress_without_lease_id_rejects_managed_attempt(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "requires lease_id"):
            record_job_progress(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                stage="editing",
                summary="ok",
                attempt_token=self.attempt_token,
            )

    def test_progress_wrong_lease_id_rejects(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "lease"):
            record_job_progress(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                stage="editing",
                summary="ok",
                attempt_token=self.attempt_token,
                lease_id="00000000-0000-0000-0000-000000000000",
            )


    def test_progress_rejects_due_active_lease_zero_writes(self):
        """A due-but-active lease must fail progress and leave job/lease/event untouched."""
        # Make the lease due while the job is still running.
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.lease_id),
        )
        self.conn.commit()
        before_job = get_job(self.conn, self.job_id)
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(RuntimeLeaseError, "expired"):
            record_job_progress(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                stage="editing",
                summary="too late",
                attempt_token=self.attempt_token,
                lease_id=self.lease_id,
            )

        job = get_job(self.conn, self.job_id)
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["attempt_count"], before_job["attempt_count"])
        self.assertEqual(job["progress_json"], before_job["progress_json"])
        lease = get_attempt_lease(self.conn, self.lease_id)
        self.assertEqual(lease["status"], "active")
        self.assertEqual(lease["expires_at"], "2020-01-01T00:00:01Z")
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(self.conn.in_transaction)

    def test_terminal_rejects_due_active_lease_zero_writes(self):
        """A due-but-active lease must fail terminal report and leave job/lease/event untouched."""
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.lease_id),
        )
        self.conn.commit()
        before_job = get_job(self.conn, self.job_id)
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(RuntimeLeaseError, "expired"):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "too late"},
                attempt_token=self.attempt_token,
                lease_id=self.lease_id,
            )

        job = get_job(self.conn, self.job_id)
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["attempt_count"], before_job["attempt_count"])
        lease = get_attempt_lease(self.conn, self.lease_id)
        self.assertEqual(lease["status"], "active")
        self.assertEqual(lease["expires_at"], "2020-01-01T00:00:01Z")
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(self.conn.in_transaction)

    def test_report_managed_omits_attempt_token_and_lease_id_rejects_zero_writes(self):
        before_job = row_to_dict(get_job(self.conn, self.job_id))
        before_lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(RuntimeLeaseError, "requires attempt_token"):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "ok"},
            )

        job = row_to_dict(get_job(self.conn, self.job_id))
        lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        self.assertEqual(job, before_job)
        self.assertEqual(lease, before_lease)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(self.conn.in_transaction)

    def test_progress_managed_omits_attempt_token_and_lease_id_rejects_zero_writes(self):
        before_job = row_to_dict(get_job(self.conn, self.job_id))
        before_lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(RuntimeLeaseError, "requires attempt_token"):
            record_job_progress(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                stage="editing",
                summary="ok",
            )

        job = row_to_dict(get_job(self.conn, self.job_id))
        lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        self.assertEqual(job, before_job)
        self.assertEqual(lease, before_lease)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(self.conn.in_transaction)


class RuntimeLeaseReapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(self.conn, ["mac-omp"])
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        self.job_id = request.job["id"]
        claim = claim_job(self.conn, agent_id="mac-omp")
        self.lease_id = claim.execution_lease["lease_id"]
        # Make the lease due immediately (keep status active so reap can expire it).
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.lease_id),
        )
        self.conn.commit()

    def test_reap_makes_job_recoverable_and_lease_expired(self):
        summary = reap_due_leases(self.conn, actor="runtime")
        self.assertEqual(summary["reaped_count"], 1)
        job = get_job(self.conn, self.job_id)
        self.assertEqual(job["status"], "timed_out")
        self.assertTrue(job["recoverable"])
        lease = get_attempt_lease(self.conn, self.lease_id)
        self.assertEqual(lease["status"], "expired")

    def test_reap_is_idempotent(self):
        reap_due_leases(self.conn)
        self.conn.commit()
        summary = reap_due_leases(self.conn)
        self.assertEqual(summary["due_found"], 0)
        self.assertEqual(summary["reaped_count"], 0)

    def test_reclaim_after_reap_needs_recoverable_flag(self):
        reap_due_leases(self.conn)
        self.conn.commit()
        ordinary = claim_job(self.conn, agent_id="mac-omp")
        self.assertFalse(ordinary.claimed)
        recoverable = claim_job(
            self.conn,
            agent_id="mac-omp",
            recoverable=True,
            recovery_reason="operator confirmed prior process stopped via tooling",
            prior_process_stopped=True,
        )
        self.assertTrue(recoverable.claimed)
        self.assertEqual(recoverable.job["attempt_count"], 2)

    def test_recovery_rejects_missing_recovery_reason_zero_writes(self):
        reap_due_leases(self.conn)
        self.conn.commit()
        before_events = len(list_events(self.conn, "demo"))
        before_attempt_count = get_job(self.conn, self.job_id)["attempt_count"]
        before_leases = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ? AND status = 'active'",
            (self.job_id,),
        ).fetchone()["n"]
        with self.assertRaisesRegex(CoordinateRuntimeError, "recovery_reason"):
            claim_job(
                self.conn,
                agent_id="mac-omp",
                recoverable=True,
                prior_process_stopped=True,
            )
        job = get_job(self.conn, self.job_id)
        self.assertEqual(job["status"], "timed_out")
        self.assertTrue(job["recoverable"])
        self.assertEqual(job["attempt_count"], before_attempt_count)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        after_leases = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ? AND status = 'active'",
            (self.job_id,),
        ).fetchone()["n"]
        self.assertEqual(after_leases, before_leases)

    def test_recovery_rejects_missing_prior_process_stopped_zero_writes(self):
        reap_due_leases(self.conn)
        self.conn.commit()
        before_events = len(list_events(self.conn, "demo"))
        before_attempt_count = get_job(self.conn, self.job_id)["attempt_count"]
        before_leases = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ? AND status = 'active'",
            (self.job_id,),
        ).fetchone()["n"]
        with self.assertRaisesRegex(CoordinateRuntimeError, "prior_process_stopped"):
            claim_job(
                self.conn,
                agent_id="mac-omp",
                recoverable=True,
                recovery_reason="operator confirmed prior process stopped via tooling",
            )
        job = get_job(self.conn, self.job_id)
        self.assertEqual(job["status"], "timed_out")
        self.assertTrue(job["recoverable"])
        self.assertEqual(job["attempt_count"], before_attempt_count)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        after_leases = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ? AND status = 'active'",
            (self.job_id,),
        ).fetchone()["n"]
        self.assertEqual(after_leases, before_leases)

    def test_recovery_rejects_prior_process_stopped_false_zero_writes(self):
        """Only explicit True is accepted; False must fail with no durable writes."""
        reap_due_leases(self.conn)
        self.conn.commit()
        before_events = len(list_events(self.conn, "demo"))
        before_attempt_count = get_job(self.conn, self.job_id)["attempt_count"]
        before_leases = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ? AND status = 'active'",
            (self.job_id,),
        ).fetchone()["n"]
        with self.assertRaisesRegex(CoordinateRuntimeError, "prior_process_stopped=True"):
            claim_job(
                self.conn,
                agent_id="mac-omp",
                recoverable=True,
                recovery_reason="operator confirmed prior process stopped via tooling",
                prior_process_stopped=False,
            )
        job = get_job(self.conn, self.job_id)
        self.assertEqual(job["status"], "timed_out")
        self.assertTrue(job["recoverable"])
        self.assertEqual(job["attempt_count"], before_attempt_count)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        after_leases = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ? AND status = 'active'",
            (self.job_id,),
        ).fetchone()["n"]
        self.assertEqual(after_leases, before_leases)

    def test_recovery_success_records_reason_and_confirmation(self):
        """A valid recovery claim records the bounded reason and true confirmation."""
        reap_due_leases(self.conn)
        self.conn.commit()
        reason = "operator confirmed prior process stopped via tooling"
        result = claim_job(
            self.conn,
            agent_id="mac-omp",
            recoverable=True,
            recovery_reason=reason,
            prior_process_stopped=True,
        )
        self.assertTrue(result.claimed)
        self.assertEqual(result.job["attempt_count"], 2)
        events = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        claimed = [e for e in events if e["event_type"] == "job.claimed"]
        self.assertEqual(len(claimed), 2)
        recovery_event = claimed[-1]
        self.assertTrue(recovery_event["payload"]["recovered"])
        self.assertEqual(recovery_event["payload"]["recovery_reason"], reason)
        self.assertIs(recovery_event["payload"]["prior_process_stopped"], True)




class RuntimeLeaseServerNowTests(unittest.TestCase):
    """server_now ordering must survive a second-boundary cross."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(self.conn, ["mac-omp"])
        # A brand-new pending typed job is required; RuntimeLeaseReapTests has none.
        submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )

    def test_claim_server_now_not_before_acquired_at(self):
        """server_now must be sampled after the lease row is written, so it is
        always >= acquired_at even when the system clock crosses a second boundary.
        """
        from unittest.mock import patch
        from coordinate import runtime_lease
        from coordinate import execution_leases

        # Emulate a second boundary crossing: reserve sees 00:00:01, response sees 00:00:02.
        with patch.object(runtime_lease, "utc_now", side_effect=[
            "2020-01-01T00:00:01Z",  # reap now / claim body now
            "2020-01-01T00:00:02Z",  # server_now after reading lease
        ]):
            with patch.object(execution_leases, "_utc_now", return_value="2020-01-01T00:00:01Z"):
                result = claim_job(self.conn, agent_id="mac-omp")

        self.assertTrue(result.claimed)
        lease = result.execution_lease
        self.assertEqual(lease["acquired_at"], "2020-01-01T00:00:01Z")
        self.assertEqual(lease["server_now"], "2020-01-01T00:00:02Z")
        self.assertGreaterEqual(lease["server_now"], lease["acquired_at"])
        # The envelope must be parseable as a strict v1 lease.
        from coordinate.lease_envelope import parse_execution_lease
        parsed = parse_execution_lease(lease)
        self.assertEqual(parsed.contract_version, 1)
        self.assertEqual(parsed.server_now, "2020-01-01T00:00:02Z")
        self.assertTrue(parsed.resource_key.startswith("sha256:"))


class RuntimeLeaseRenewalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(self.conn, ["mac-omp"])
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        self.job_id = request.job["id"]
        claim = claim_job(self.conn, agent_id="mac-omp")
        self.lease_id = claim.execution_lease["lease_id"]
        self.attempt_token = claim.attempt_token

    def test_renew_advances_expires(self):
        old = get_attempt_lease(self.conn, self.lease_id)
        time.sleep(1.1)
        result = renew_managed_lease(
            self.conn,
            lease_id=self.lease_id,
            job_id=self.job_id,
            attempt_token=self.attempt_token,
            agent_id="mac-omp",
        )
        self.assertGreater(result["expires_at"], old["expires_at"])
        self.assertIn("server_now", result)
        self.assertEqual(result["server_now"], result["renewed_at"])

    def test_renew_rejects_when_job_not_running(self):
        report_job_result(
            self.conn,
            job_id=self.job_id,
            agent_id="mac-omp",
            status="done",
            result={"response_text": "ok"},
            attempt_token=self.attempt_token,
            lease_id=self.lease_id,
        )
        with self.assertRaisesRegex(RuntimeLeaseError, "not running"):
            renew_managed_lease(
                self.conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                attempt_token=self.attempt_token,
                agent_id="mac-omp",
            )

    def test_renew_response_server_now_matches_authoritative_time(self):
        """Successful renewal response contains Coordinate server_now and expires_at."""
        from unittest.mock import patch

        # Put the lease in a known state: unexpired at patched now, but will advance.
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2030-01-01T00:00:00Z", "2030-01-01T00:00:00Z", "2030-01-01T00:01:00Z", self.lease_id),
        )
        self.conn.commit()

        with patch("coordinate.execution_leases._utc_now", return_value="2030-01-01T00:00:00Z"):
            with patch("coordinate.runtime_lease.utc_now", return_value="2030-01-01T00:01:30Z"):
                result = renew_managed_lease(
                    self.conn,
                    lease_id=self.lease_id,
                    job_id=self.job_id,
                    attempt_token=self.attempt_token,
                    agent_id="mac-omp",
                )

        self.assertEqual(result["renewed_at"], "2030-01-01T00:00:00Z")
        self.assertEqual(result["expires_at"], "2030-01-01T00:02:00Z")
        self.assertEqual(result["server_now"], "2030-01-01T00:01:30Z")
        # server_now is sampled after the primitive update: renewed_at <= server_now < expires_at.
        self.assertGreaterEqual(result["server_now"], result["renewed_at"])
        self.assertLess(result["server_now"], result["expires_at"])
        lease = get_attempt_lease(self.conn, self.lease_id)
        self.assertEqual(result["expires_at"], lease["expires_at"])
        self.assertEqual(result["renewed_at"], lease["renewed_at"])

    def test_renew_rolls_back_if_response_server_now_fails(self):
        """If response construction fails after the lease update, rollback leaves state unchanged."""
        from unittest.mock import patch

        # Put the lease in a known state so the primitive update is allowed to proceed.
        self.conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2030-01-01T00:00:00Z", "2030-01-01T00:00:00Z", "2030-01-01T00:01:00Z", self.lease_id),
        )
        self.conn.commit()
        before_lease = dict(get_attempt_lease(self.conn, self.lease_id))

        with patch("coordinate.execution_leases._utc_now", return_value="2030-01-01T00:00:00Z"):
            with patch("coordinate.runtime_lease.utc_now", side_effect=RuntimeError("clock broken")):
                with self.assertRaises(RuntimeError):
                    renew_managed_lease(
                        self.conn,
                        lease_id=self.lease_id,
                        job_id=self.job_id,
                        attempt_token=self.attempt_token,
                        agent_id="mac-omp",
                    )

        after_lease = get_attempt_lease(self.conn, self.lease_id)
        self.assertEqual(after_lease["renewed_at"], before_lease["renewed_at"])
        self.assertEqual(after_lease["expires_at"], before_lease["expires_at"])
        self.assertEqual(after_lease["status"], before_lease["status"])
        self.assertEqual(after_lease["acquired_at"], before_lease["acquired_at"])
        self.assertEqual(after_lease["released_at"], before_lease["released_at"])
        self.assertEqual(after_lease["release_reason"], before_lease["release_reason"])
        self.assertFalse(self.conn.in_transaction)


class RuntimeLeaseSelectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(self.conn, ["mac-omp"], max_jobs=2)

    def test_queue_order_created_at_then_id(self):
        ids = []
        for i in range(3):
            request = submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="mac-omp",
                prompt=f"job-{i}",
                origin={"platform": "discord", "destination": "ch", "message_id": f"m{i}", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
            ids.append(request.job["id"])
        # Artificially stagger created_at to verify ordering.
        for i, jid in enumerate(ids):
            self.conn.execute(
                "UPDATE jobs SET created_at = ? WHERE id = ?",
                (f"2020-01-0{i+1}T00:00:00Z", jid),
            )
        self.conn.commit()
        result = select_claim_candidate(self.conn, agent_id="mac-omp", host_id="mac", recoverable=False)
        self.assertIsNotNone(result.job)
        self.assertEqual(result.job["id"], ids[0])

    def test_resource_blocked_candidate_is_skipped_and_reported(self):
        # Two pending jobs for the same worktree path (same resource).
        for i in range(2):
            submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="mac-omp",
                prompt=f"job-{i}",
                origin={"platform": "discord", "destination": "ch", "message_id": f"m{i}", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        claim1 = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim1.claimed)
        claim2 = claim_job(self.conn, agent_id="mac-omp")
        self.assertFalse(claim2.claimed)
        # The second job is still pending; claim did not error out.
        events = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        self.assertTrue(any(e["event_type"] == "job.claimed" for e in events))

    def test_capacity_stops_selection(self):
        _resync_capacity(self.conn, ["mac-omp"], max_jobs=1)
        for i in range(2):
            ws_id = "demo" if i == 0 else f"demo-{uuid.uuid4().hex[:8]}"
            path = self.tmp.name if i == 0 else f"{self.tmp.name}/ws{i}"
            if i != 0:
                os.makedirs(path, exist_ok=True)
                upsert_workspace(
                    self.conn,
                    workspace_id=ws_id,
                    name=f"Demo{i}",
                    path=path,
                    harness_root=path,
                )
                upsert_workspace_host_profile(
                    self.conn,
                    workspace_id=ws_id,
                    host_id="mac",
                    workspace_path=path,
                    harness_root=path,
                )
            submit_request(
                self.conn,
                workspace_id=ws_id,
                target_agent="mac-omp",
                prompt=f"job-{i}",
                origin={"platform": "discord", "destination": "ch", "message_id": f"m{i}", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        claim1 = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim1.claimed)
        claim2 = claim_job(self.conn, agent_id="mac-omp")
        self.assertFalse(claim2.claimed)

    def test_resource_blocked_skip_selects_other_resource(self):
        """A2 on the same resource is skipped; B on a different resource is selected."""
        # Job A: claim it so resource A is actively leased.
        request_a = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="job-A",
            origin={"platform": "discord", "destination": "ch", "message_id": "mA", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        claim_a = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim_a.claimed)
        self.assertEqual(claim_a.job["id"], request_a.job["id"])

        # Candidate A2: same workspace/resource, must stay pending.
        request_a2 = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="job-A2",
            origin={"platform": "discord", "destination": "ch", "message_id": "mA2", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        a2_job_id = request_a2.job["id"]
        a2_payload_json_before = self.conn.execute(
            "SELECT payload_json FROM jobs WHERE id = ?", (a2_job_id,)
        ).fetchone()["payload_json"]

        # Candidate B: different workspace/resource.
        ws_b = "demo-other"
        path_b = f"{self.tmp.name}/other"
        os.makedirs(path_b, exist_ok=True)
        upsert_workspace(
            self.conn,
            workspace_id=ws_b,
            name="DemoOther",
            path=path_b,
            harness_root=path_b,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id=ws_b,
            host_id="mac",
            workspace_path=path_b,
            harness_root=path_b,
        )
        request_b = submit_request(
            self.conn,
            workspace_id=ws_b,
            target_agent="mac-omp",
            prompt="job-B",
            origin={"platform": "discord", "destination": "ch", "message_id": "mB", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )

        # Capacity allows one more concurrent job; selection must skip A2 and pick B.
        result = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(result.claimed)
        self.assertEqual(result.job["id"], request_b.job["id"])
        self.assertEqual(result.execution_context["workspace_id"], ws_b)
        self.assertEqual(result.execution_lease["job_id"], request_b.job["id"])

        # A2 remains pending and its payload is untouched.
        a2 = row_to_dict(get_job(self.conn, request_a2.job["id"]))
        self.assertEqual(a2["status"], "pending")
        self.assertEqual(a2["payload"]["prompt"], "job-A2")
        a2_payload_json_after = self.conn.execute(
            "SELECT payload_json FROM jobs WHERE id = ?", (a2_job_id,)
        ).fetchone()["payload_json"]
        self.assertEqual(a2_payload_json_after, a2_payload_json_before)
        # No lease was created for the skipped A2.
        a2_leases = self.conn.execute(
            "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ?",
            (a2_job_id,),
        ).fetchone()["n"]
        self.assertEqual(a2_leases, 0)

    def test_claim_to_dict_preserves_resource_blocked_diagnostics(self):
        """resource_blocked surfaces the oldest blocked job and resource key."""
        for i in range(2):
            submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="mac-omp",
                prompt=f"job-{i}",
                origin={"platform": "discord", "destination": "ch", "message_id": f"m{i}", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        claim1 = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim1.claimed)
        blocked_resource_key = claim1.execution_lease["resource_key"]
        pending_id = self.conn.execute(
            "SELECT id FROM jobs WHERE status = 'pending' AND assigned_agent = ?",
            ("mac-omp",),
        ).fetchone()["id"]

        claim2 = claim_job(self.conn, agent_id="mac-omp")
        self.assertFalse(claim2.claimed)
        self.assertEqual(claim2.reason, "resource_blocked")
        self.assertEqual(claim2.oldest_blocked_job_id, pending_id)
        self.assertEqual(claim2.oldest_blocked_resource_key, blocked_resource_key)

        as_dict = claim2.to_dict()
        self.assertEqual(as_dict["reason"], "resource_blocked")
        self.assertEqual(as_dict["oldest_blocked_job_id"], pending_id)
        self.assertEqual(as_dict["oldest_blocked_resource_key"], blocked_resource_key)

    def test_claim_to_dict_preserves_capacity_exhausted(self):
        """capacity_exhausted is a bounded reason with no blocked candidate."""
        _resync_capacity(self.conn, ["mac-omp"], max_jobs=1)
        for i in range(2):
            submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="mac-omp",
                prompt=f"job-{i}",
                origin={"platform": "discord", "destination": "ch", "message_id": f"m{i}", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        claim1 = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim1.claimed)

        result = claim_job(self.conn, agent_id="mac-omp")
        self.assertFalse(result.claimed)
        self.assertEqual(result.reason, "capacity_exhausted")
        self.assertIsNone(result.oldest_blocked_job_id)
        self.assertIsNone(result.oldest_blocked_resource_key)
        as_dict = result.to_dict()
        self.assertEqual(as_dict["reason"], "capacity_exhausted")
        self.assertNotIn("oldest_blocked_job_id", as_dict)
        self.assertNotIn("oldest_blocked_resource_key", as_dict)

    def test_claim_fails_closed_when_routing_request_has_no_decision(self):
        """B4: typed pending job with valid routing_request but no routing_decision.

        claim_job must fail closed and leave the job/lease/event state completely
        untouched.
        """
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-omp",
            discord_user_id="12345",
            actor="test",
            reason="test",
        )
        routing = build_routing_request(required_capabilities=["coding"])
        request = submit_request(
            self.conn,
            workspace_id="demo",
            routing_request=routing,
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        job_id = request.job["id"]
        payload = dict(request.job["payload"])
        del payload["routing_decision"]
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), job_id),
        )
        self.conn.commit()

        before = self.conn.execute(
            "SELECT status, attempt_count, payload_json FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        before_payload_json = before["payload_json"]
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(CoordinateRuntimeError, "invalid routing claim evidence|routing_request"):
            claim_job(self.conn, agent_id="mac-omp")

        job = get_job(self.conn, job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempt_count"], 0)
        after_payload_json = self.conn.execute(
            "SELECT payload_json FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()["payload_json"]
        self.assertEqual(after_payload_json, before_payload_json)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(
            any(e["event_type"] == "job.claimed" for e in [row_to_dict(ev) for ev in list_events(self.conn, "demo")])
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ?",
                (job_id,),
            ).fetchone()["n"],
            0,
        )
        self.assertFalse(self.conn.in_transaction)

    def test_authority_error_before_resource_skip_does_not_fall_through_to_b(self):
        """B5B: A1 holds resource A; older A2 same resource has forged binding;
        B on a different resource is available.

        The candidate loop must validate A2's authority before checking the
        active resource and must hard-fail. It must not silently skip A2 and
        claim B, and must leave A2/B jobs, leases, and events untouched.
        """
        # Job A1: claim it so resource A is actively leased.
        request_a1 = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="job-A1",
            origin={"platform": "discord", "destination": "ch", "message_id": "mA1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        claim_a1 = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim_a1.claimed)
        self.assertEqual(claim_a1.job["id"], request_a1.job["id"])

        # Candidate A2: same workspace/resource, created slightly earlier via DB update.
        request_a2 = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="job-A2",
            origin={"platform": "discord", "destination": "ch", "message_id": "mA2", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        a2_job_id = request_a2.job["id"]
        self.conn.execute(
            "UPDATE jobs SET created_at = ? WHERE id = ?",
            ("2019-12-31T23:59:00Z", a2_job_id),
        )
        a2_payload_before = dict(request_a2.job["payload"])
        a2_payload_before["executor_binding"]["binding_id"] = "sha256:" + "0" * 64
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(a2_payload_before), a2_job_id),
        )
        self.conn.commit()

        # Candidate B: different workspace/resource.
        ws_b = "demo-other"
        path_b = f"{self.tmp.name}/other"
        os.makedirs(path_b, exist_ok=True)
        upsert_workspace(
            self.conn,
            workspace_id=ws_b,
            name="DemoOther",
            path=path_b,
            harness_root=path_b,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id=ws_b,
            host_id="mac",
            workspace_path=path_b,
            harness_root=path_b,
        )
        request_b = submit_request(
            self.conn,
            workspace_id=ws_b,
            target_agent="mac-omp",
            prompt="job-B",
            origin={"platform": "discord", "destination": "ch", "message_id": "mB", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        b_job_id = request_b.job["id"]

        # Capture pre-claim durable state for zero-write assertions.
        a2_payload_json_before = self.conn.execute(
            "SELECT payload_json FROM jobs WHERE id = ?", (a2_job_id,)
        ).fetchone()["payload_json"]
        b_payload_json_before = self.conn.execute(
            "SELECT payload_json FROM jobs WHERE id = ?", (b_job_id,)
        ).fetchone()["payload_json"]
        leases_before = [
            dict(r) for r in self.conn.execute(
                "SELECT * FROM execution_attempt_leases ORDER BY lease_id"
            ).fetchall()
        ]
        events_before = [row_to_dict(e) for e in list_events(self.conn, "demo")]

        # The forged binding must fail the claim before any resource skip or B selection.
        with self.assertRaisesRegex(CoordinateRuntimeError, "invalid executor binding|executor_binding_mismatch"):
            claim_job(self.conn, agent_id="mac-omp")

        # A2 and B remain pending and untouched.
        a2 = row_to_dict(get_job(self.conn, a2_job_id))
        b = row_to_dict(get_job(self.conn, b_job_id))
        self.assertEqual(a2["status"], "pending")
        self.assertEqual(b["status"], "pending")
        self.assertEqual(
            self.conn.execute("SELECT payload_json FROM jobs WHERE id = ?", (a2_job_id,)).fetchone()["payload_json"],
            a2_payload_json_before,
        )
        self.assertEqual(
            self.conn.execute("SELECT payload_json FROM jobs WHERE id = ?", (b_job_id,)).fetchone()["payload_json"],
            b_payload_json_before,
        )

        # No new lease rows; existing leases unchanged.
        leases_after = [
            dict(r) for r in self.conn.execute(
                "SELECT * FROM execution_attempt_leases ORDER BY lease_id"
            ).fetchall()
        ]
        self.assertEqual(leases_after, leases_before)

        # No new events.
        events_after = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        self.assertEqual(events_after, events_before)
        self.assertFalse(self.conn.in_transaction)

    def test_pre_upgrade_typed_pending_backfills_context_and_claims(self):
        """Pre-upgrade typed pending job lacks payload.execution_context but has
        valid runner_profile, executor_binding, and routing snapshots.

        claim_job must backfill the context atomically, CAS the job to running,
        reserve a lease, and append a job.claimed event. Unselected candidates
        must not be mutated.
        """
        # Create a normal typed job, then strip execution_context from its payload.
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="pre-upgrade",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        job_id = request.job["id"]
        payload = dict(request.job["payload"])
        self.assertIn("execution_context", payload)
        del payload["execution_context"]
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), job_id),
        )
        self.conn.commit()

        before_events = len(list_events(self.conn, "demo"))

        result = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(result.claimed)
        self.assertEqual(result.job["id"], job_id)
        self.assertEqual(result.job["status"], "running")
        self.assertEqual(result.attempt_token, 1)
        self.assertIsNotNone(result.execution_lease)
        self.assertEqual(result.execution_lease["job_id"], job_id)

        # Payload was backfilled with execution_context in the same transaction.
        job = row_to_dict(get_job(self.conn, job_id))
        self.assertIn("execution_context", job["payload"])
        ctx = job["payload"]["execution_context"]
        self.assertEqual(ctx["job_id"], job_id)
        self.assertEqual(ctx["workspace_id"], "demo")
        self.assertEqual(ctx["assigned_agent"], "mac-omp")
        self.assertEqual(ctx["host_id"], "mac")

        # Lease reserved.
        lease_row = get_attempt_lease(self.conn, result.execution_lease["lease_id"])
        self.assertIsNotNone(lease_row)
        self.assertEqual(lease_row["status"], "active")
        self.assertEqual(lease_row["job_id"], job_id)
        self.assertEqual(lease_row["attempt_token"], 1)

        # job.claimed event appended.
        events = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        self.assertEqual(len(events), before_events + 1)
        claimed_event = events[-1]
        self.assertEqual(claimed_event["event_type"], "job.claimed")
        self.assertEqual(claimed_event["payload"]["job_id"], job_id)
        self.assertEqual(claimed_event["payload"]["execution_context_id"], ctx["context_id"])

    def test_invalid_payload_json_fails_closed_zero_writes(self):
        """Typed job with invalid JSON payload must fail closed and write nothing."""
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="bad-json",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        job_id = request.job["id"]
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            ("not valid json", job_id),
        )
        self.conn.commit()

        before_payload_json = "not valid json"
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(CoordinateRuntimeError, "invalid payload_json|payload_json"):
            claim_job(self.conn, agent_id="mac-omp")

        job = get_job(self.conn, job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempt_count"], 0)
        self.assertEqual(
            self.conn.execute("SELECT payload_json FROM jobs WHERE id = ?", (job_id,)).fetchone()["payload_json"],
            before_payload_json,
        )
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(
            any(e["event_type"] == "job.claimed" for e in [row_to_dict(ev) for ev in list_events(self.conn, "demo")])
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ?", (job_id,)
            ).fetchone()["n"],
            0,
        )
        self.assertFalse(self.conn.in_transaction)

    def test_non_object_payload_json_fails_closed_zero_writes(self):
        """Typed job whose payload decodes to a top-level list must fail closed."""
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="list-payload",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        job_id = request.job["id"]
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(["not", "an", "object"]), job_id),
        )
        self.conn.commit()

        before_payload_json = json.dumps(["not", "an", "object"])
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(CoordinateRuntimeError, "payload_json is not an object|payload_json"):
            claim_job(self.conn, agent_id="mac-omp")

        job = get_job(self.conn, job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempt_count"], 0)
        self.assertEqual(
            self.conn.execute("SELECT payload_json FROM jobs WHERE id = ?", (job_id,)).fetchone()["payload_json"],
            before_payload_json,
        )
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(
            any(e["event_type"] == "job.claimed" for e in [row_to_dict(ev) for ev in list_events(self.conn, "demo")])
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM execution_attempt_leases WHERE job_id = ?", (job_id,)
            ).fetchone()["n"],
            0,
        )
        self.assertFalse(self.conn.in_transaction)


class RuntimeLeaseRaceTests(unittest.TestCase):
    def test_two_connections_claim_same_job_race(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = Path(tmp.name) / "race.sqlite3"
        conn = initialize(str(db_path))
        upsert_workspace(conn, workspace_id="demo", name="Demo", path=tmp.name, harness_root=tmp.name)
        upsert_workspace_host_profile(conn, workspace_id="demo", host_id="mac", workspace_path=tmp.name, harness_root=tmp.name)
        register_agent(conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(conn, ["mac-omp"])
        request = submit_request(
            conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        _job_id = request.job["id"]
        conn.close()

        results: list[dict] = []
        errors: list[Exception] = []

        def worker():
            c = sqlite3.connect(str(db_path))
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON")
            c.execute("PRAGMA busy_timeout = 30000")
            try:
                result = claim_job(c, agent_id="mac-omp")
                c.commit()
                results.append(result.to_dict())
            except Exception as exc:
                errors.append(exc)
            finally:
                c.close()

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        claimed = [r for r in results if r["claimed"]]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(len(errors), 0)


class RuntimeLeaseReaperSnapshotRaceTests(unittest.TestCase):
    """Stale snapshot race on a file-backed DB with two independent connections."""

    def _open(self, db_path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _setup_typed_job(self, conn: sqlite3.Connection):
        upsert_workspace(conn, workspace_id="demo", name="Demo", path=self.tmp, harness_root=self.tmp)
        upsert_workspace_host_profile(conn, workspace_id="demo", host_id="mac", workspace_path=self.tmp, harness_root=self.tmp)
        register_agent(conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(conn, ["mac-omp"])
        request = submit_request(
            conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        claim = claim_job(conn, agent_id="mac-omp")
        return request.job["id"], claim.execution_lease["lease_id"], claim.attempt_token

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        self.tmp = self.tmp_dir.name
        self.db_path = Path(self.tmp) / "race.sqlite3"
        conn = initialize(str(self.db_path))
        self.job_id, self.lease_id, self.attempt_token = self._setup_typed_job(conn)
        # Make the lease due in the past so _find_due_active_leases returns it.
        conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.lease_id),
        )
        conn.commit()
        conn.close()

    def test_reaper_skips_lease_renewed_after_snapshot(self):
        """Stale snapshot race: reaper sees due candidate, another conn renews it.

        The reaper must re-check the current expires_at inside its write lock and
        skip the lease without expiring it or mutating the job/lease/event state.
        """
        from coordinate import runtime_lease

        conn_reaper = self._open(self.db_path)
        try:
            original_find = runtime_lease._find_due_active_leases

            def _find_then_renew(conn, now, limit):
                leases = original_find(conn, now, limit)
                if leases:
                    # Renew the lease on a separate connection before reaper acquires lock.
                    renew_conn = self._open(self.db_path)
                    try:
                        renew_conn.execute("BEGIN IMMEDIATE")
                        renew_conn.execute(
                            """
                            UPDATE execution_attempt_leases
                            SET renewed_at = ?, expires_at = ?
                            WHERE lease_id = ? AND status = 'active'
                            """,
                            ("2030-01-01T00:00:00Z", "2030-01-01T00:01:00Z", self.lease_id),
                        )
                        renew_conn.commit()
                    finally:
                        renew_conn.close()
                return leases

            with patch.object(runtime_lease, "_find_due_active_leases", side_effect=_find_then_renew):
                summary = reap_due_leases(conn_reaper, actor="runtime", now="2020-01-02T00:00:00Z")

            self.assertEqual(summary["due_found"], 1)
            self.assertEqual(summary["reaped_count"], 0)
            self.assertFalse(conn_reaper.in_transaction)
        finally:
            conn_reaper.close()

        # Verify from a fresh connection that nothing was mutated.
        conn_check = self._open(self.db_path)
        try:
            job = get_job(conn_check, self.job_id)
            self.assertEqual(job["status"], "running")
            self.assertFalse(job["recoverable"])
            lease = get_attempt_lease(conn_check, self.lease_id)
            self.assertEqual(lease["status"], "active")
            self.assertEqual(lease["expires_at"], "2030-01-01T00:01:00Z")
            events = [row_to_dict(e) for e in list_events(conn_check, "demo")]
            self.assertFalse(any(e["event_type"] == "execution_lease.expired" for e in events))
            self.assertFalse(any(e["event_type"] == "job.timed_out" for e in events))
            self.assertFalse(conn_check.in_transaction)
        finally:
            conn_check.close()


class RuntimeLeaseTerminalReaperRaceTests(unittest.TestCase):
    """Deterministic terminal-report vs lease-reaper races on a file-backed DB.

    Uses two independent SQLite connections to model exactly the interleavings
    we care about: A) the terminal report commits first, then the reaper runs
    against the already-terminal job; B) the reaper commits first, then a late
    terminal report for the exact stale tuple is fail-closed.
    """

    def _open(self, db_path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _setup_typed_job(self, conn: sqlite3.Connection):
        upsert_workspace(conn, workspace_id="demo", name="Demo", path=self.tmp, harness_root=self.tmp)
        upsert_workspace_host_profile(conn, workspace_id="demo", host_id="mac", workspace_path=self.tmp, harness_root=self.tmp)
        register_agent(conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(conn, ["mac-omp"])
        request = submit_request(
            conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        claim = claim_job(conn, agent_id="mac-omp")
        return request.job["id"], claim.execution_lease["lease_id"], claim.attempt_token

    def _events_for_job(self, conn: sqlite3.Connection, job_id: str) -> list[dict[str, Any]]:
        rows = [row_to_dict(e) for e in list_events(conn, "demo")]
        return [e for e in rows if e.get("payload", {}).get("job_id") == job_id]

    def _assert_no_terminal_mix(self, conn: sqlite3.Connection, job_id: str, allowed_terminal: str):
        job = get_job(conn, job_id)
        self.assertEqual(job["status"], allowed_terminal)
        terminal_types = {"job.completed", "job.failed", "job.timed_out"}
        terminal_events = [e for e in self._events_for_job(conn, job_id) if e["event_type"] in terminal_types]
        self.assertEqual(
            len(terminal_events), 1,
            f"expected exactly one terminal event for {job_id}, got {terminal_events}",
        )
        expected_event_type = "job.completed" if allowed_terminal == "done" else f"job.{allowed_terminal}"
        self.assertEqual(terminal_events[0]["event_type"], expected_event_type)
        lease = get_attempt_lease(conn, self.lease_id)
        self.assertIn(lease["status"], {"released", "expired"})
        if allowed_terminal == "done":
            self.assertEqual(lease["status"], "released")
            self.assertEqual(lease["release_reason"], "job_done")
        else:
            self.assertEqual(lease["status"], "expired")

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        self.tmp = self.tmp_dir.name
        self.db_path = Path(self.tmp) / "race.sqlite3"
        conn = initialize(str(self.db_path))
        self.job_id, self.lease_id, self.attempt_token = self._setup_typed_job(conn)
        conn.close()

    def test_terminal_wins_then_reaper_is_idempotent(self):
        """A: terminal report commits while lease is still active, then reaper runs.

        The reaper must observe a released lease + terminal job and skip without
        creating a contradictory timeout event or mutating the lease.
        """
        conn_report = self._open(self.db_path)
        conn_reaper = self._open(self.db_path)

        try:
            # Terminal report wins while the lease is still unexpired.
            result = report_job_result(
                conn_report,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "ok"},
                attempt_token=self.attempt_token,
                lease_id=self.lease_id,
            )
            self.assertEqual(result.job["status"], "done")

            # Reaper runs with an explicit future clock; it must skip the already-released lease.
            summary = reap_due_leases(conn_reaper, actor="runtime", now="2030-01-02T00:00:00Z")
            self.assertEqual(summary["due_found"], 0)
            self.assertEqual(summary["reaped_count"], 0)

            # Verify atomic consistency from a fresh connection.
            conn_check = self._open(self.db_path)
            try:
                self._assert_no_terminal_mix(conn_check, self.job_id, "done")
                job = get_job(conn_check, self.job_id)
                self.assertFalse(job["recoverable"])
                lease = get_attempt_lease(conn_check, self.lease_id)
                self.assertEqual(lease["status"], "released")
                self.assertEqual(lease["release_reason"], "job_done")
                events = self._events_for_job(conn_check, self.job_id)
                terminal_events = [e for e in events if e["event_type"] in {"job.completed", "job.failed", "job.timed_out"}]
                self.assertEqual(len(terminal_events), 1)
                self.assertEqual(terminal_events[0]["event_type"], "job.completed")
                self.assertFalse(any(e["event_type"] == "job.timed_out" for e in events))
                self.assertFalse(any(e["event_type"] == "execution_lease.expired" for e in events))
            finally:
                conn_check.close()
        finally:
            conn_report.close()
            conn_reaper.close()

    def test_reaper_wins_then_terminal_report_fails_closed(self):
        """B: reaper expires the due lease and makes the job timed_out+recoverable.

        A subsequent terminal report with the exact stale (lease_id, attempt_token,
        agent_id) tuple must fail closed and must not append a done/failed event or
        mutate the lease away from the reaper outcome.
        """
        conn_report = self._open(self.db_path)
        conn_reaper = self._open(self.db_path)

        try:
            # Make the lease due.
            conn_reaper.execute(
                "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
                ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.lease_id),
            )
            conn_reaper.commit()

            # Reaper wins on conn_reaper.
            summary = reap_due_leases(conn_reaper, actor="runtime", now="2020-01-02T00:00:00Z")
            self.assertEqual(summary["reaped_count"], 1)

            # Terminal report now arrives with the stale exact tuple.
            # Because the reaper already expired the lease, this is not a valid
            # late result — it must fail closed and leave the reaper outcome intact.
            with self.assertRaisesRegex(RuntimeLeaseError, "managed"):
                report_job_result(
                    conn_report,
                    job_id=self.job_id,
                    agent_id="mac-omp",
                    status="done",
                    result={"response_text": "too late"},
                    attempt_token=self.attempt_token,
                    lease_id=self.lease_id,
                )

            # Verify atomic consistency from a fresh connection.
            conn_check = self._open(self.db_path)
            try:
                self._assert_no_terminal_mix(conn_check, self.job_id, "timed_out")
                job = get_job(conn_check, self.job_id)
                self.assertTrue(job["recoverable"])
                self.assertEqual(job["attempt_count"], self.attempt_token)
                lease = get_attempt_lease(conn_check, self.lease_id)
                self.assertEqual(lease["status"], "expired")
                events = self._events_for_job(conn_check, self.job_id)
                terminal_events = [e for e in events if e["event_type"] in {"job.completed", "job.failed", "job.timed_out"}]
                self.assertEqual(len(terminal_events), 1)
                self.assertEqual(terminal_events[0]["event_type"], "job.timed_out")
                self.assertTrue(any(e["event_type"] == "execution_lease.expired" for e in events))
                self.assertFalse(any(e["event_type"] == "job.completed" for e in events))
                self.assertFalse(any(e["event_type"] == "agent.reported" for e in events if e.get("payload", {}).get("status") == "done"))
            finally:
                conn_check.close()
        finally:
            conn_report.close()
            conn_reaper.close()


class RuntimeLeaseReaperIntegrityTests(unittest.TestCase):
    """Stale-snapshot integrity failures must surface as errors, not silent skips."""

    def _open(self, db_path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _setup_typed_job(self, conn: sqlite3.Connection):
        upsert_workspace(conn, workspace_id="demo", name="Demo", path=self.tmp, harness_root=self.tmp)
        upsert_workspace_host_profile(conn, workspace_id="demo", host_id="mac", workspace_path=self.tmp, harness_root=self.tmp)
        register_agent(conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(conn, ["mac-omp"])
        request = submit_request(
            conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        claim = claim_job(conn, agent_id="mac-omp")
        return request.job["id"], claim.execution_lease["lease_id"], claim.attempt_token

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        self.tmp = self.tmp_dir.name
        self.db_path = Path(self.tmp) / "integrity.sqlite3"
        conn = initialize(str(self.db_path))
        self.job_id, self.lease_id, self.attempt_token = self._setup_typed_job(conn)
        conn.execute(
            "UPDATE execution_attempt_leases SET acquired_at = ?, renewed_at = ?, expires_at = ? WHERE lease_id = ?",
            ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:01Z", self.lease_id),
        )
        conn.commit()
        conn.close()

    def _capture_before(self, conn: sqlite3.Connection) -> tuple[dict[str, Any], int]:
        job = row_to_dict(get_job(conn, self.job_id))
        events = len(list_events(conn, "demo"))
        return job, events

    def _assert_job_and_events_unchanged(self, conn: sqlite3.Connection, before: tuple[dict[str, Any], int]):
        before_job, before_events = before
        job = row_to_dict(get_job(conn, self.job_id))
        self.assertEqual(job, before_job)
        self.assertEqual(len(list_events(conn, "demo")), before_events)

    def test_reaper_errors_on_missing_lease_row(self):
        """A stale snapshot pointing to a deleted lease row must be reported."""
        from coordinate import runtime_lease
        from coordinate import execution_leases

        conn_before = self._open(self.db_path)
        try:
            before = self._capture_before(conn_before)
        finally:
            conn_before.close()

        conn_reaper = self._open(self.db_path)
        try:
            original_find = execution_leases._find_due_active_leases

            def _find_then_delete(conn, now, limit):
                leases = original_find(conn, now, limit)
                del_conn = self._open(self.db_path)
                try:
                    del_conn.execute("BEGIN IMMEDIATE")
                    del_conn.execute(
                        "DELETE FROM execution_attempt_leases WHERE lease_id = ?",
                        (self.lease_id,),
                    )
                    del_conn.commit()
                finally:
                    del_conn.close()
                return leases

            with patch.object(runtime_lease, "_find_due_active_leases", side_effect=_find_then_delete):
                summary = reap_due_leases(conn_reaper, actor="runtime", now="2020-01-02T00:00:00Z")

            self.assertEqual(summary["due_found"], 1)
            self.assertEqual(summary["reaped_count"], 0)
            self.assertEqual(len(summary["errors"]), 1)
            self.assertEqual(summary["errors"][0]["lease_id"], self.lease_id)
            self.assertEqual(summary["errors"][0]["job_id"], self.job_id)
            self.assertIn("not found", summary["errors"][0]["error"])
        finally:
            conn_reaper.close()

        conn_check = self._open(self.db_path)
        try:
            self._assert_job_and_events_unchanged(conn_check, before)
            self.assertIsNone(get_attempt_lease(conn_check, self.lease_id))
        finally:
            conn_check.close()

    def test_reaper_errors_on_corrupt_resource_snapshot(self):
        """A stale snapshot with a tampered resource_key must be reported."""
        from coordinate import runtime_lease
        from coordinate import execution_leases

        conn_before = self._open(self.db_path)
        try:
            before = self._capture_before(conn_before)
        finally:
            conn_before.close()

        conn_reaper = self._open(self.db_path)
        try:
            original_find = execution_leases._find_due_active_leases

            def _find_then_corrupt(conn, now, limit):
                leases = original_find(conn, now, limit)
                corrupt_conn = self._open(self.db_path)
                try:
                    corrupt_conn.execute("BEGIN IMMEDIATE")
                    corrupt_conn.execute(
                        "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
                        ("sha256:" + "0" * 64, self.lease_id),
                    )
                    corrupt_conn.commit()
                finally:
                    corrupt_conn.close()
                return leases

            with patch.object(runtime_lease, "_find_due_active_leases", side_effect=_find_then_corrupt):
                summary = reap_due_leases(conn_reaper, actor="runtime", now="2020-01-02T00:00:00Z")

            self.assertEqual(summary["due_found"], 1)
            self.assertEqual(summary["reaped_count"], 0)
            self.assertEqual(len(summary["errors"]), 1)
            self.assertEqual(summary["errors"][0]["lease_id"], self.lease_id)
            self.assertEqual(summary["errors"][0]["job_id"], self.job_id)
            self.assertIn("resource snapshot invalid", summary["errors"][0]["error"])
        finally:
            conn_reaper.close()

        conn_check = self._open(self.db_path)
        try:
            self._assert_job_and_events_unchanged(conn_check, before)
            lease_row = conn_check.execute(
                "SELECT * FROM execution_attempt_leases WHERE lease_id = ?", (self.lease_id,)
            ).fetchone()
            self.assertEqual(lease_row["resource_key"], "sha256:" + "0" * 64)
        finally:
            conn_check.close()

    def test_reaper_errors_on_tuple_mismatch(self):
        """A stale snapshot whose tuple no longer matches the stored row must be reported."""
        from coordinate import runtime_lease
        from coordinate import execution_leases

        conn_before = self._open(self.db_path)
        try:
            before = self._capture_before(conn_before)
        finally:
            conn_before.close()

        conn_reaper = self._open(self.db_path)
        try:
            original_find = execution_leases._find_due_active_leases

            def _find_then_mutate(conn, now, limit):
                leases = original_find(conn, now, limit)
                mutate_conn = self._open(self.db_path)
                try:
                    mutate_conn.execute("BEGIN IMMEDIATE")
                    mutate_conn.execute(
                        "UPDATE execution_attempt_leases SET attempt_token = ? WHERE lease_id = ?",
                        (999, self.lease_id),
                    )
                    mutate_conn.commit()
                finally:
                    mutate_conn.close()
                return leases

            with patch.object(runtime_lease, "_find_due_active_leases", side_effect=_find_then_mutate):
                summary = reap_due_leases(conn_reaper, actor="runtime", now="2020-01-02T00:00:00Z")

            self.assertEqual(summary["due_found"], 1)
            self.assertEqual(summary["reaped_count"], 0)
            self.assertEqual(len(summary["errors"]), 1)
            self.assertEqual(summary["errors"][0]["lease_id"], self.lease_id)
            self.assertEqual(summary["errors"][0]["job_id"], self.job_id)
            self.assertIn("attempt_token mismatch", summary["errors"][0]["error"])
        finally:
            conn_reaper.close()

        conn_check = self._open(self.db_path)
        try:
            self._assert_job_and_events_unchanged(conn_check, before)
            lease = get_attempt_lease(conn_check, self.lease_id)
            self.assertEqual(lease["attempt_token"], 999)
        finally:
            conn_check.close()


class RuntimeLeaseLateResultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(self.conn, ["mac-omp"])
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        self.job_id = request.job["id"]
        claim = claim_job(self.conn, agent_id="mac-omp")
        self.lease_id = claim.execution_lease["lease_id"]
        self.attempt_token = claim.attempt_token
        report_job_result(
            self.conn,
            job_id=self.job_id,
            agent_id="mac-omp",
            status="timed_out",
            result={"response_text": "recoverable timeout"},
            attempt_token=self.attempt_token,
            lease_id=self.lease_id,
        )

    def test_late_result_with_released_lease_rejects_and_zero_writes(self):
        """An exact released lease on a timed_out+recoverable job means the attempt was
        managed. Late results for managed attempts are fail-closed regardless of tuple.
        """
        before_job = row_to_dict(get_job(self.conn, self.job_id))
        before_lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(RuntimeLeaseError, "managed"):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "late ok"},
                attempt_token=self.attempt_token,
                lease_id=self.lease_id,
            )

        job = row_to_dict(get_job(self.conn, self.job_id))
        lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        self.assertEqual(job, before_job)
        self.assertEqual(lease, before_lease)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(self.conn.in_transaction)

    def test_late_result_omits_attempt_token_and_lease_id_rejects_zero_writes(self):
        """A managed timed_out job requires both attempt_token and lease_id."""
        before_job = row_to_dict(get_job(self.conn, self.job_id))
        before_lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        before_events = len(list_events(self.conn, "demo"))

        with self.assertRaisesRegex(RuntimeLeaseError, "managed"):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "late ok"},
            )

        job = row_to_dict(get_job(self.conn, self.job_id))
        lease = row_to_dict(get_attempt_lease(self.conn, self.lease_id))
        self.assertEqual(job, before_job)
        self.assertEqual(lease, before_lease)
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertFalse(self.conn.in_transaction)

    def test_late_result_after_reclaim_rejected(self):
        claim2 = claim_job(
            self.conn,
            agent_id="mac-omp",
            recoverable=True,
            recovery_reason="operator confirmed prior process stopped via tooling",
            prior_process_stopped=True,
        )
        self.assertTrue(claim2.claimed)
        with self.assertRaises(CoordinateRuntimeError):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "stale"},
                attempt_token=self.attempt_token,
                lease_id=self.lease_id,
            )

    def test_late_result_wrong_lease_id_rejects_managed_attempt(self):
        # A timed_out job whose current attempt has a lease row is managed. Any
        # late result without the exact active lease tuple must fail closed.
        with self.assertRaisesRegex(RuntimeLeaseError, "managed"):
            report_job_result(
                self.conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "wrong lease"},
                attempt_token=self.attempt_token,
                lease_id="not-the-real-lease-id",
            )


class RuntimeLeaseCLIDelegationTests(unittest.TestCase):
    def _args(self, **kwargs):
        return SimpleNamespace(**kwargs)

    def _capture_json(self, func, args):
        import io
        import contextlib
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = func(args)
        return code, json.loads(stdout.getvalue())

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.report_job_result")
    def test_report_delegates_lease_id(self, mock_report, mock_conn):
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_report.return_value = result
        args = self._args(
            job_id="j1",
            agent_id="mac-omp",
            status="done",
            result_json='{"ok": true}',
            actor="runtime",
            attempt_token=3,
            lease_id="lease-1",
            db=":memory:",
        )
        code, _ = self._capture_json(handle_runtime_job_report, args)
        self.assertEqual(code, 0)
        mock_report.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="mac-omp",
            status="done",
            result={"ok": True},
            actor="runtime",
            attempt_token=3,
            lease_id="lease-1",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.record_job_progress")
    def test_progress_delegates_lease_id(self, mock_record, mock_conn):
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_record.return_value = result
        args = self._args(
            job_id="j1",
            agent_id="mac-omp",
            stage="run",
            summary="ok",
            session_id="s1",
            actor="runtime",
            attempt_token=3,
            lease_id="lease-1",
            db=":memory:",
        )
        code, _ = self._capture_json(handle_runtime_job_progress, args)
        self.assertEqual(code, 0)
        mock_record.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="mac-omp",
            stage="run",
            summary="ok",
            session_id="s1",
            actor="runtime",
            attempt_token=3,
            lease_id="lease-1",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.renew_managed_lease")
    def test_lease_renew_delegation(self, mock_renew, mock_conn):
        mock_renew.return_value = {"lease_id": "lease-1"}
        args = self._args(
            job_id="j1",
            agent_id="mac-omp",
            attempt_token=3,
            lease_id="lease-1",
            actor="runtime",
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_lease_renew, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"lease_id": "lease-1"}})
        mock_renew.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            lease_id="lease-1",
            job_id="j1",
            attempt_token=3,
            agent_id="mac-omp",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.reap_due_leases")
    def test_lease_reap_delegation(self, mock_reap, mock_conn):
        mock_reap.return_value = {"reaped_count": 2}
        args = self._args(actor="runtime", batch_size=50, db=":memory:")
        code, payload = self._capture_json(handle_runtime_job_lease_reap, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"reaped_count": 2}})
        mock_reap.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            actor="runtime",
            batch_size=50,
        )




class RuntimeLeaseExactReapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        sentinel_path = os.path.join(self.tmp.name, "sentinel")
        os.makedirs(sentinel_path)
        upsert_workspace(
            self.conn,
            workspace_id="sentinel",
            name="Sentinel",
            path=sentinel_path,
            harness_root=sentinel_path,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="sentinel",
            host_id="mac",
            workspace_path=sentinel_path,
            harness_root=sentinel_path,
        )
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        register_agent(
            self.conn, agent_id="mac-sentinel", host_id="mac", capabilities={}
        )
        _sync_catalog(self.conn, ["mac-omp", "mac-sentinel"])
        request = submit_request(
            self.conn,
            workspace_id="sentinel",
            target_agent="mac-omp",
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        self.job_id = request.job["id"]
        claim = claim_job(self.conn, agent_id="mac-omp")
        self.lease_id = claim.execution_lease["lease_id"]
        self.attempt_token = claim.attempt_token
        # Make lease due — update all timestamps to satisfy CHECK constraint.
        self.conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', renewed_at = '2020-01-01T00:00:00Z', "
            "expires_at = '2020-01-01T00:00:01Z' WHERE lease_id = ?",
            (self.lease_id,),
        )
        self.conn.commit()
        self.sentinel_lease_id, self.sentinel_job_id = self._sentinel()
        self.sentinel_before = self._sentinel_snapshot()
        self.expect_sentinel_untouched = True

    def tearDown(self):
        if self.expect_sentinel_untouched:
            self.assertEqual(self._sentinel_snapshot(), self.sentinel_before)

    def _sentinel(self):
        """Create a second due lease for an unrelated agent that must stay untouched."""
        req = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-sentinel",
            prompt="sentinel",
            origin={"platform": "discord", "destination": "ch", "message_id": "m2", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        cl = claim_job(
            self.conn,
            agent_id="mac-sentinel",
            reap_mode="none",
            reap_reason="preserve exact-reap target",
        )
        sid = cl.execution_lease["lease_id"]
        self.conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', renewed_at = '2020-01-01T00:00:00Z', "
            "expires_at = '2020-01-01T00:00:01Z' WHERE lease_id = ?",
            (sid,),
        )
        self.conn.commit()
        return sid, req.job["id"]

    def _durable_snapshot(self):
        return {
            "job": row_to_dict(get_job(self.conn, self.job_id)),
            "lease": row_to_dict(
                self.conn.execute(
                    "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
                    (self.lease_id,),
                ).fetchone()
            ),
            "events": [
                row_to_dict(row)
                for row in self.conn.execute(
                    "SELECT * FROM events ORDER BY rowid"
                ).fetchall()
            ],
        }

    def _sentinel_snapshot(self):
        return {
            "job": row_to_dict(get_job(self.conn, self.sentinel_job_id)),
            "lease": row_to_dict(
                self.conn.execute(
                    "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
                    (self.sentinel_lease_id,),
                ).fetchone()
            ),
            "events": [
                row_to_dict(row)
                for row in self.conn.execute(
                    "SELECT * FROM events WHERE workspace_id = 'demo' ORDER BY rowid"
                ).fetchall()
            ],
        }

    def test_exact_reap_success_expires_only_target(self):
        events_before = len(list(self.conn.execute("SELECT * FROM events").fetchall()))

        result = reap_exact_lease(
            self.conn, lease_id=self.lease_id, job_id=self.job_id, now="2020-01-02T00:00:00Z"
        )
        self.assertEqual(result["mode"], "exact")
        self.assertEqual(result["reaped_count"], 1)
        self.assertEqual(result["lease_id"], self.lease_id)

        # Target lease is expired, sentinel is untouched.
        target = self.conn.execute(
            "SELECT status FROM execution_attempt_leases WHERE lease_id = ?", (self.lease_id,)
        ).fetchone()
        self.assertEqual(target["status"], "expired")
        sentinel = self.conn.execute(
            "SELECT status FROM execution_attempt_leases WHERE lease_id = ?",
            (self.sentinel_lease_id,),
        ).fetchone()
        self.assertEqual(sentinel["status"], "active")

        # Target job is timed_out, sentinel is running.
        target_job = get_job(self.conn, self.job_id)
        self.assertEqual(target_job["status"], "timed_out")
        self.assertEqual(target_job["recoverable"], 1)
        sentinel_job = get_job(self.conn, self.sentinel_job_id)
        self.assertEqual(sentinel_job["status"], "running")

        # Events were appended.
        events_after = len(list(self.conn.execute("SELECT * FROM events").fetchall()))
        self.assertGreater(events_after, events_before)

    def test_exact_reap_not_found(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "not found"):
            reap_exact_lease(self.conn, lease_id="nonexistent", job_id=self.job_id)
        self.assertFalse(self.conn.in_transaction)

    def test_exact_reap_job_id_mismatch(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "not requested job"):
            reap_exact_lease(self.conn, lease_id=self.lease_id, job_id="wrong-job-id")
        self.assertFalse(self.conn.in_transaction)

    def test_exact_reap_not_active(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET status = 'released', released_at = '2020-01-01T00:00:00Z', release_reason = 'test' WHERE lease_id = ?",
            (self.lease_id,),
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeLeaseError, "is released"):
            reap_exact_lease(self.conn, lease_id=self.lease_id, job_id=self.job_id,
                             now="2020-01-02T00:00:00Z")
        self.assertFalse(self.conn.in_transaction)

    def test_exact_reap_not_due(self):
        self.conn.execute(
            "UPDATE execution_attempt_leases SET expires_at = '2099-01-01T00:00:00Z' WHERE lease_id = ?",
            (self.lease_id,),
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeLeaseError, "no longer due"):
            reap_exact_lease(self.conn, lease_id=self.lease_id, job_id=self.job_id,
                             now="2020-01-02T00:00:00Z")
        self.assertFalse(self.conn.in_transaction)

    def test_exact_reap_retry_after_success_is_error(self):
        reap_exact_lease(self.conn, lease_id=self.lease_id, job_id=self.job_id,
                         now="2020-01-02T00:00:00Z")
        with self.assertRaisesRegex(RuntimeLeaseError, "is expired"):
            reap_exact_lease(self.conn, lease_id=self.lease_id, job_id=self.job_id,
                             now="2020-01-02T00:00:00Z")

    def test_exact_reap_invalid_ids(self):
        with self.assertRaises(RuntimeLeaseError):
            reap_exact_lease(self.conn, lease_id="", job_id=self.job_id)
        with self.assertRaises(RuntimeLeaseError):
            reap_exact_lease(self.conn, lease_id=self.lease_id, job_id="")
        with self.assertRaises(RuntimeLeaseError):
            reap_exact_lease(
                self.conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                actor="",
            )
        self.assertFalse(self.conn.in_transaction)

    def test_exact_reap_invalid_now_fails_before_transaction(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "YYYY-MM-DD"):
            reap_exact_lease(
                self.conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                now="not-a-time",
            )
        self.assertFalse(self.conn.in_transaction)

    def test_exact_reap_resource_and_job_drift_are_zero_additional_mutation(self):
        original = self._durable_snapshot()
        self.conn.execute(
            "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
            ("sha256:" + "0" * 64, self.lease_id),
        )
        self.conn.commit()
        before = self._durable_snapshot()
        with self.assertRaisesRegex(RuntimeLeaseError, "resource snapshot invalid"):
            reap_exact_lease(
                self.conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                now="2020-01-02T00:00:00Z",
            )
        self.assertEqual(self._durable_snapshot(), before)

        self.conn.execute(
            "UPDATE execution_attempt_leases SET resource_key = ? WHERE lease_id = ?",
            (original["lease"]["resource_key"], self.lease_id),
        )
        self.conn.execute(
            "UPDATE jobs SET status = 'pending' WHERE id = ?", (self.job_id,)
        )
        self.conn.commit()
        before = self._durable_snapshot()
        with self.assertRaisesRegex(RuntimeLeaseError, "lease/job inconsistency"):
            reap_exact_lease(
                self.conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                now="2020-01-02T00:00:00Z",
            )
        self.assertEqual(self._durable_snapshot(), before)

    def test_exact_reap_cas_and_event_failures_roll_back(self):
        before = self._durable_snapshot()
        self.conn.execute(
            "CREATE TRIGGER p1_ignore_timeout BEFORE UPDATE OF status ON jobs "
            "WHEN NEW.status = 'timed_out' BEGIN SELECT RAISE(IGNORE); END"
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeLeaseError, "CAS failed"):
            reap_exact_lease(
                self.conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                now="2020-01-02T00:00:00Z",
            )
        self.assertEqual(self._durable_snapshot(), before)
        self.conn.execute("DROP TRIGGER p1_ignore_timeout")
        self.conn.commit()

        with patch(
            "coordinate.runtime_lease.append_event",
            side_effect=RuntimeLeaseError("simulated event failure"),
        ):
            with self.assertRaisesRegex(RuntimeLeaseError, "simulated event failure"):
                reap_exact_lease(
                    self.conn,
                    lease_id=self.lease_id,
                    job_id=self.job_id,
                    now="2020-01-02T00:00:00Z",
                )
        self.assertEqual(self._durable_snapshot(), before)

    def test_exact_reap_global_default_batch_unchanged(self):
        self.expect_sentinel_untouched = False
        result = reap_due_leases(self.conn, now="2020-01-02T00:00:00Z", batch_size=100)
        # Both leases are due, so both should be reaped.
        self.assertGreaterEqual(result["reaped_count"], 2)


class RuntimeLeaseClaimPolicyTests(unittest.TestCase):
    def test_validate_policy_global_no_reason(self):
        mode, reason = _validate_claim_reap_policy(reap_mode="global", reap_reason=None)
        self.assertEqual(mode, "global")
        self.assertIsNone(reason)

    def test_validate_policy_global_with_reason_fails(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "reap_reason must not be set"):
            _validate_claim_reap_policy(reap_mode="global", reap_reason="x")

    def test_validate_policy_none_with_reason(self):
        mode, reason = _validate_claim_reap_policy(reap_mode="none", reap_reason="scoped-deploy")
        self.assertEqual(mode, "none")
        self.assertEqual(reason, "scoped-deploy")

    def test_validate_policy_none_missing_reason(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "reap_reason is required"):
            _validate_claim_reap_policy(reap_mode="none", reap_reason=None)

    def test_validate_policy_none_empty_reason(self):
        with self.assertRaises(RuntimeLeaseError):
            _validate_claim_reap_policy(reap_mode="none", reap_reason="")

    def test_validate_policy_unknown_mode(self):
        with self.assertRaisesRegex(RuntimeLeaseError, "reap_mode"):
            _validate_claim_reap_policy(reap_mode="invalid", reap_reason=None)

    def test_validate_policy_none_reason_control_chars(self):
        with self.assertRaises(RuntimeLeaseError):
            _validate_claim_reap_policy(reap_mode="none", reap_reason="bad\x00char")


class RuntimeLeaseTypedClaimPolicyIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
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
        sentinel_path = os.path.join(self.tmp.name, "sentinel")
        os.makedirs(sentinel_path)
        upsert_workspace(
            self.conn,
            workspace_id="sentinel",
            name="Sentinel",
            path=sentinel_path,
            harness_root=sentinel_path,
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="sentinel",
            host_id="mac",
            workspace_path=sentinel_path,
            harness_root=sentinel_path,
        )
        register_agent(self.conn, agent_id="mac-omp", host_id="mac", capabilities={})
        register_agent(self.conn, agent_id="mac-codex", host_id="mac", capabilities={})
        register_agent(
            self.conn, agent_id="mac-sentinel", host_id="mac", capabilities={}
        )
        _sync_catalog(self.conn, ["mac-omp", "mac-sentinel"])

    def _submit(self, agent="mac-omp"):
        return submit_request(
            self.conn,
            workspace_id="sentinel",
            target_agent=agent,
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )

    def _create_due_sentinel(self):
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-sentinel",
            prompt="sentinel",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "sentinel",
                "session_scope_id": "discord:ch",
            },
            reply={"platform": "discord", "destination": "ch"},
        )
        claim = claim_job(self.conn, agent_id="mac-sentinel")
        lease_id = claim.execution_lease["lease_id"]
        self.conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', "
            "renewed_at = '2020-01-01T00:00:00Z', "
            "expires_at = '2020-01-01T00:00:01Z' WHERE lease_id = ?",
            (lease_id,),
        )
        self.conn.commit()
        return request.job["id"], lease_id

    def test_none_untyped_agent_rejected(self):
        self._submit(agent="mac-codex")
        with self.assertRaisesRegex(CoordinateRuntimeError, "untyped"):
            claim_job(self.conn, agent_id="mac-codex", reap_mode="none", reap_reason="test")

    def test_typed_none_claim_succeeds_preserves_unrelated_due_sentinel(self):
        _, sentinel_lease_id = self._create_due_sentinel()

        # Submit and claim with none mode.
        self._submit()
        result = claim_job(self.conn, agent_id="mac-omp", reap_mode="none", reap_reason="scoped-test")
        self.assertTrue(result.claimed)

        # Sentinel lease is still active (global reap was skipped).
        sentinel = self.conn.execute(
            "SELECT status FROM execution_attempt_leases WHERE lease_id = ?", (sentinel_lease_id,)
        ).fetchone()
        self.assertEqual(sentinel["status"], "active")

        # Claim event has reap evidence.
        events = [row_to_dict(r) for r in list_events(self.conn, "sentinel")]
        claimed_events = [e for e in events if e["event_type"] == "job.claimed"]
        self.assertTrue(len(claimed_events) >= 1)
        last_claim = claimed_events[-1]
        self.assertEqual(last_claim["payload"].get("reap_mode"), "none")
        self.assertEqual(last_claim["payload"].get("reap_reason"), "scoped-test")

    def test_recoverable_none_claim_preserves_unrelated_due_sentinel(self):
        _, sentinel_lease_id = self._create_due_sentinel()
        target = self._submit()
        self.conn.execute(
            "UPDATE jobs SET status = 'timed_out', recoverable = 1 WHERE id = ?",
            (target.job["id"],),
        )
        self.conn.commit()

        result = claim_job(
            self.conn,
            agent_id="mac-omp",
            recoverable=True,
            recovery_reason="operator recovery",
            prior_process_stopped=True,
            reap_mode="none",
            reap_reason="scoped recovery",
        )
        self.assertTrue(result.claimed)
        self.assertEqual(
            get_attempt_lease(self.conn, sentinel_lease_id)["status"], "active"
        )
        events = [row_to_dict(row) for row in list_events(self.conn, "sentinel")]
        claimed = [event for event in events if event["event_type"] == "job.claimed"]
        self.assertEqual(claimed[-1]["payload"]["reap_mode"], "none")
        self.assertEqual(claimed[-1]["payload"]["reap_reason"], "scoped recovery")
        self.assertEqual(
            claimed[-1]["payload"]["recovery_reason"], "operator recovery"
        )
        self.assertTrue(claimed[-1]["payload"]["prior_process_stopped"])

    def test_default_global_claim_reaps_as_before(self):
        self._submit()
        result = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(result.claimed)
        events = [row_to_dict(r) for r in list_events(self.conn, "sentinel")]
        claimed_events = [e for e in events if e["event_type"] == "job.claimed"]
        self.assertTrue(len(claimed_events) >= 1)
        last_claim = claimed_events[-1]
        self.assertEqual(last_claim["payload"].get("reap_mode"), "global")
        self.assertIsNone(last_claim["payload"].get("reap_reason"))

    def test_explicit_global_claim_reaps_due_sentinel(self):
        sentinel_job_id, sentinel_lease_id = self._create_due_sentinel()
        self._submit()
        result = claim_job(
            self.conn,
            agent_id="mac-omp",
            reap_mode="global",
            reap_reason=None,
        )
        self.assertTrue(result.claimed)
        self.assertEqual(
            get_attempt_lease(self.conn, sentinel_lease_id)["status"], "expired"
        )
        sentinel_job = get_job(self.conn, sentinel_job_id)
        self.assertEqual(sentinel_job["status"], "timed_out")
        self.assertTrue(sentinel_job["recoverable"])

    def test_none_reason_validation_before_transaction(self):
        with self.assertRaisesRegex(CoordinateRuntimeError, "reap_mode"):
            claim_job(self.conn, agent_id="mac-omp", reap_mode="bad", reap_reason=None)

    def test_policy_validation_before_connection(self):
        with self.assertRaises(CoordinateRuntimeError):
            claim_job(self.conn, agent_id="mac-omp", reap_mode="none", reap_reason="")


class RuntimeExactReapDualConnectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        self.db_path = Path(self.tmp_dir.name) / "exact-reap.sqlite3"
        conn = initialize(str(self.db_path))
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp_dir.name,
            harness_root=self.tmp_dir.name,
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path=self.tmp_dir.name,
            harness_root=self.tmp_dir.name,
        )
        register_agent(conn, agent_id="mac-omp", host_id="mac", capabilities={})
        _sync_catalog(conn, ["mac-omp"])
        request = submit_request(
            conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="exact race",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "exact-race",
                "session_scope_id": "discord:ch",
            },
            reply={"platform": "discord", "destination": "ch"},
        )
        claim = claim_job(conn, agent_id="mac-omp")
        self.job_id = request.job["id"]
        self.lease_id = claim.execution_lease["lease_id"]
        self.attempt_token = claim.attempt_token
        conn.close()

    def _open(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _make_due(self, conn):
        conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', "
            "renewed_at = '2020-01-01T00:00:00Z', "
            "expires_at = '2020-01-01T00:00:01Z' WHERE lease_id = ?",
            (self.lease_id,),
        )
        conn.commit()

    def test_renew_commits_first_exact_reap_rechecks_due_state(self):
        renew_conn = self._open()
        reap_conn = self._open()
        try:
            renew_conn.execute(
                "UPDATE execution_attempt_leases "
                "SET acquired_at = '2029-01-01T00:00:00Z', "
                "renewed_at = '2029-01-01T00:00:00Z', "
                "expires_at = '2029-01-01T00:01:00Z' WHERE lease_id = ?",
                (self.lease_id,),
            )
            renew_conn.commit()
            with patch(
                "coordinate.execution_leases._utc_now",
                return_value="2029-01-01T00:00:30Z",
            ):
                renew_managed_lease(
                    renew_conn,
                    lease_id=self.lease_id,
                    job_id=self.job_id,
                    attempt_token=self.attempt_token,
                    agent_id="mac-omp",
                )
            with self.assertRaisesRegex(RuntimeLeaseError, "no longer due"):
                reap_exact_lease(
                    reap_conn,
                    lease_id=self.lease_id,
                    job_id=self.job_id,
                    now="2029-01-01T00:01:00Z",
                )
            self.assertEqual(get_job(reap_conn, self.job_id)["status"], "running")
            self.assertEqual(
                get_attempt_lease(reap_conn, self.lease_id)["status"], "active"
            )
        finally:
            renew_conn.close()
            reap_conn.close()

    def test_exact_reap_commits_first_stale_renew_fails(self):
        reap_conn = self._open()
        renew_conn = self._open()
        try:
            self._make_due(reap_conn)
            reap_exact_lease(
                reap_conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                now="2020-01-02T00:00:00Z",
            )
            with self.assertRaisesRegex(RuntimeLeaseError, "not running"):
                renew_managed_lease(
                    renew_conn,
                    lease_id=self.lease_id,
                    job_id=self.job_id,
                    attempt_token=self.attempt_token,
                    agent_id="mac-omp",
                )
            self.assertEqual(get_job(renew_conn, self.job_id)["status"], "timed_out")
        finally:
            reap_conn.close()
            renew_conn.close()

    def test_terminal_commits_first_exact_reap_adds_no_timeout(self):
        report_conn = self._open()
        reap_conn = self._open()
        try:
            report_job_result(
                report_conn,
                job_id=self.job_id,
                agent_id="mac-omp",
                status="done",
                result={"response_text": "ok"},
                attempt_token=self.attempt_token,
                lease_id=self.lease_id,
            )
            with self.assertRaisesRegex(RuntimeLeaseError, "is released"):
                reap_exact_lease(
                    reap_conn,
                    lease_id=self.lease_id,
                    job_id=self.job_id,
                    now="2099-01-01T00:00:00Z",
                )
            events = [row_to_dict(row) for row in list_events(reap_conn, "demo")]
            self.assertFalse(any(e["event_type"] == "job.timed_out" for e in events))
        finally:
            report_conn.close()
            reap_conn.close()

    def test_exact_reap_commits_first_stale_terminal_fails(self):
        reap_conn = self._open()
        report_conn = self._open()
        try:
            self._make_due(reap_conn)
            reap_exact_lease(
                reap_conn,
                lease_id=self.lease_id,
                job_id=self.job_id,
                now="2020-01-02T00:00:00Z",
            )
            with self.assertRaises(RuntimeLeaseError):
                report_job_result(
                    report_conn,
                    job_id=self.job_id,
                    agent_id="mac-omp",
                    status="done",
                    result={"response_text": "late"},
                    attempt_token=self.attempt_token,
                    lease_id=self.lease_id,
                )
            events = [row_to_dict(row) for row in list_events(report_conn, "demo")]
            terminal = [
                e
                for e in events
                if e["event_type"] in {"job.completed", "job.failed", "job.timed_out"}
            ]
            self.assertEqual([e["event_type"] for e in terminal], ["job.timed_out"])
        finally:
            reap_conn.close()
            report_conn.close()


class RuntimeClaimDeactivateRaceTests(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        self.db_path = Path(self.tmp_dir.name) / "claim-deactivate.sqlite3"
        sentinel_path = os.path.join(self.tmp_dir.name, "sentinel")
        os.makedirs(sentinel_path)
        conn = initialize(str(self.db_path))
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp_dir.name,
            harness_root=self.tmp_dir.name,
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path=self.tmp_dir.name,
            harness_root=self.tmp_dir.name,
        )
        upsert_workspace(
            conn,
            workspace_id="sentinel",
            name="Sentinel",
            path=sentinel_path,
            harness_root=sentinel_path,
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="sentinel",
            host_id="mac",
            workspace_path=sentinel_path,
            harness_root=sentinel_path,
        )
        register_agent(conn, agent_id="mac-omp", host_id="mac", capabilities={})
        register_agent(
            conn, agent_id="mac-sentinel", host_id="mac", capabilities={}
        )
        _sync_catalog(conn, ["mac-omp", "mac-sentinel"])
        sentinel = submit_request(
            conn,
            workspace_id="sentinel",
            target_agent="mac-sentinel",
            prompt="sentinel",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "sentinel",
                "session_scope_id": "discord:ch",
            },
            reply={"platform": "discord", "destination": "ch"},
        )
        sentinel_claim = claim_job(conn, agent_id="mac-sentinel")
        self.sentinel_job_id = sentinel.job["id"]
        self.sentinel_lease_id = sentinel_claim.execution_lease["lease_id"]
        conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', "
            "renewed_at = '2020-01-01T00:00:00Z', "
            "expires_at = '2020-01-01T00:00:01Z' WHERE lease_id = ?",
            (self.sentinel_lease_id,),
        )
        conn.commit()
        conn.close()

    def _open(self, *, begin_event: threading.Event | None = None):
        if begin_event is None:
            conn = sqlite3.connect(str(self.db_path))
        else:

            class BeginSignalingConnection(sqlite3.Connection):
                def execute(self, sql, parameters=()):
                    if sql.strip().upper() == "BEGIN IMMEDIATE":
                        begin_event.set()
                    return super().execute(sql, parameters)

            conn = sqlite3.connect(
                str(self.db_path), factory=BeginSignalingConnection
            )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _submit_target(self, conn, message_id="target"):
        return submit_request(
            conn,
            workspace_id="demo",
            target_agent="mac-omp",
            prompt="target",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": message_id,
                "session_scope_id": "discord:ch",
            },
            reply={"platform": "discord", "destination": "ch"},
        )

    def test_claim_transaction_wins_then_deactivate_observes_blockers(self):
        setup = self._open()
        target = self._submit_target(setup)
        setup.close()
        claim_locked = threading.Event()
        release_claim = threading.Event()
        claim_results: list[Any] = []
        deactivate_results: list[Any] = []
        errors: list[BaseException] = []
        deactivate_begin = threading.Event()
        from coordinate import runtime as runtime_module

        original_claim_leased_job = runtime_module.claim_leased_job

        def paused_claim(conn, **kwargs):
            claim_locked.set()
            if not release_claim.wait(10):
                raise AssertionError("claim barrier timed out")
            return original_claim_leased_job(conn, **kwargs)

        def run_claim():
            conn = self._open()
            try:
                claim_results.append(
                    claim_job(
                        conn,
                        agent_id="mac-omp",
                        reap_mode="none",
                        reap_reason="claim/deactivate race",
                    )
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                conn.close()

        def run_deactivate():
            conn = self._open(begin_event=deactivate_begin)
            try:
                deactivate_results.append(
                    deactivate_agent(
                        conn,
                        agent_id="mac-omp",
                        host_id="mac",
                        reason="race",
                        actor="operator",
                    )
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                conn.close()

        with patch.object(runtime_module, "claim_leased_job", side_effect=paused_claim):
            claim_thread = threading.Thread(target=run_claim, name="claim-thread")
            claim_thread.start()
            self.assertTrue(claim_locked.wait(10))
            deactivate_thread = threading.Thread(
                target=run_deactivate, name="deactivate-thread"
            )
            deactivate_thread.start()
            self.assertTrue(deactivate_begin.wait(10))
            self.assertTrue(deactivate_thread.is_alive())
            release_claim.set()
            claim_thread.join(10)
            deactivate_thread.join(10)
        self.assertFalse(claim_thread.is_alive())
        self.assertFalse(deactivate_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(claim_results[0].claimed)
        self.assertTrue(deactivate_results[0].blocked)
        check = self._open()
        try:
            self.assertEqual(get_job(check, target.job["id"])["status"], "running")
            self.assertEqual(
                check.execute(
                    "SELECT online_state FROM agents WHERE id = 'mac-omp'"
                ).fetchone()["online_state"],
                "online",
            )
        finally:
            check.close()

    def test_deactivate_wins_stale_precheck_then_claim_stops_before_reap(self):
        prechecked = threading.Event()
        release_claim = threading.Event()
        claim_errors: list[BaseException] = []
        deactivate_results: list[Any] = []
        from coordinate import runtime as runtime_module

        original_resolve = runtime_module.resolve_exact_executor_binding

        def paused_resolve(conn, agent_id):
            result = original_resolve(conn, agent_id)
            if threading.current_thread().name == "claim-thread":
                prechecked.set()
                if not release_claim.wait(10):
                    raise AssertionError("precheck barrier timed out")
            return result

        def run_claim():
            conn = self._open()
            try:
                claim_job(conn, agent_id="mac-omp")
            except BaseException as exc:
                claim_errors.append(exc)
            finally:
                conn.close()

        with patch.object(
            runtime_module, "resolve_exact_executor_binding", side_effect=paused_resolve
        ):
            claim_thread = threading.Thread(target=run_claim, name="claim-thread")
            claim_thread.start()
            self.assertTrue(prechecked.wait(10))
            deactivate_conn = self._open()
            try:
                deactivate_results.append(
                    deactivate_agent(
                        deactivate_conn,
                        agent_id="mac-omp",
                        host_id="mac",
                        reason="deactivate wins",
                        actor="operator",
                    )
                )
            finally:
                deactivate_conn.close()
            submit_conn = self._open()
            try:
                target = self._submit_target(submit_conn, message_id="after-offline")
            finally:
                submit_conn.close()
            release_claim.set()
            claim_thread.join(10)
        self.assertFalse(claim_thread.is_alive())
        self.assertTrue(deactivate_results[0].deactivated)
        self.assertEqual(len(claim_errors), 1)
        self.assertIn("not online", str(claim_errors[0]))
        check = self._open()
        try:
            self.assertEqual(get_job(check, target.job["id"])["status"], "pending")
            sentinel_lease = get_attempt_lease(check, self.sentinel_lease_id)
            self.assertEqual(sentinel_lease["status"], "active")
            self.assertEqual(get_job(check, self.sentinel_job_id)["status"], "running")
            self.assertEqual(
                check.execute(
                    "SELECT COUNT(*) AS n FROM execution_attempt_leases "
                    "WHERE job_id = ?",
                    (target.job["id"],),
                ).fetchone()["n"],
                0,
            )
        finally:
            check.close()

    def test_offline_agent_with_later_pending_job_cannot_claim(self):
        conn = self._open()
        try:
            result = deactivate_agent(
                conn,
                agent_id="mac-omp",
                host_id="mac",
                reason="offline",
                actor="operator",
            )
            self.assertTrue(result.deactivated)
            target = self._submit_target(conn, message_id="offline-pending")
            before_events = len(list_events(conn, "demo"))
            with self.assertRaisesRegex(CoordinateRuntimeError, "offline"):
                claim_job(conn, agent_id="mac-omp")
            self.assertEqual(get_job(conn, target.job["id"])["status"], "pending")
            self.assertEqual(len(list_events(conn, "demo")), before_events)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
