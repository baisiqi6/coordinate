import dataclasses
import json
import os
import tempfile
import unittest
from typing import Any
from unittest.mock import patch

from coordinate.db import (
    get_job,
    initialize,
    list_deliveries,
    list_events,
    list_task_mirrors,
    row_to_dict,
    set_workspace_agent,
    upsert_workspace,
    upsert_workspace_host_profile,
    upsert_runner_profile,
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
from coordinate.executor_routing import (
    _compute_routing_decision_id,
    build_routing_request,
)
from coordinate.runtime import (
    RuntimeError,
    claim_job,
    deactivate_agent,
    heartbeat_agent,
    register_agent,
    record_job_progress,
    report_job_result,
    submit_request,
)


class RuntimeServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
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

    def register_codex(self):
        return register_agent(
            self.conn,
            agent_id="mac-codex",
            host_id="mac",
            capabilities={"models": ["codex"]},
        )

    def test_register_agent_creates_agent_and_runner_profile(self):
        result = self.register_codex()

        self.assertTrue(result.event_created)
        self.assertEqual(result.agent["id"], "mac-codex")
        self.assertEqual(result.agent["host_id"], "mac")
        self.assertEqual(result.agent["client_type"], "agentd")
        self.assertEqual(result.agent["capabilities"], {"models": ["codex"]})
        runner = self.conn.execute(
            "SELECT * FROM runner_profiles WHERE id = ?", ("mac-codex",)
        ).fetchone()
        self.assertIsNotNone(runner)
        self.assertEqual(runner["runner_type"], "agentd")

    def test_register_agent_does_not_overwrite_existing_runner_profile(self):
        upsert_runner_profile(
            self.conn,
            profile_id="mac-codex",
            name="Mac Codex",
            runner_type="generic_subprocess",
            command="/tmp/existing-runner.sh {prompt_path} {result_path}",
            env={"KEEP": "1"},
        )

        self.register_codex()

        runner = self.conn.execute(
            "SELECT * FROM runner_profiles WHERE id = ?", ("mac-codex",)
        ).fetchone()
        self.assertEqual(runner["runner_type"], "generic_subprocess")
        self.assertEqual(
            runner["command"],
            "/tmp/existing-runner.sh {prompt_path} {result_path}",
        )

    def test_heartbeat_updates_last_seen_and_rejects_host_mismatch(self):
        self.register_codex()

        result = heartbeat_agent(self.conn, agent_id="mac-codex", host_id="mac")

        self.assertEqual(result.agent["online_state"], "online")
        self.assertTrue(result.agent["last_seen_at"])
        with self.assertRaisesRegex(RuntimeError, "registered on host mac"):
            heartbeat_agent(self.conn, agent_id="mac-codex", host_id="windows")

    def test_submit_request_creates_idempotent_event_and_job(self):
        self.register_codex()
        origin = {"platform": "discord", "destination": "channel-1", "message_id": "m1","session_scope_id":"discord:test"}
        reply = {"platform": "discord", "destination": "channel-1"}

        first = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin=origin,
            reply=reply,
            actor="discord-bridge",
        )
        second = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin=origin,
            reply=reply,
            actor="discord-bridge",
        )

        self.assertTrue(first.event_created)
        self.assertTrue(first.job_created)
        self.assertFalse(second.event_created)
        self.assertFalse(second.job_created)
        self.assertEqual(first.job["id"], second.job["id"])
        self.assertEqual(first.job["assigned_agent"], "mac-codex")
        self.assertEqual(first.job["payload"]["prompt"], "hello")

    def test_claim_job_moves_next_pending_job_to_running_once(self):
        self.register_codex()
        submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m1","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
        )

        first = claim_job(self.conn, agent_id="mac-codex")
        second = claim_job(self.conn, agent_id="mac-codex")

        self.assertTrue(first.claimed)
        self.assertEqual(first.job["status"], "running")
        self.assertEqual(first.job["attempt_count"], 1)
        self.assertNotIn("execution_lease", first.to_dict())
        self.assertFalse(second.claimed)
        self.assertIsNone(second.job)
        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        self.assertIn("job.claimed", [event["event_type"] for event in events])

    def test_report_result_completes_job_and_creates_response_delivery(self):
        self.register_codex()
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m1","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")

        result = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "hi back"},
        )
        replay = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "hi back"},
        )

        job = row_to_dict(get_job(self.conn, request.job["id"]))
        deliveries = [row_to_dict(row) for row in list_deliveries(self.conn)]
        self.assertEqual(job["status"], "done")
        self.assertEqual(job["result"]["response_text"], "hi back")
        self.assertTrue(result.event_created)
        self.assertTrue(result.delivery_created)
        self.assertFalse(replay.delivery_created)
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["platform"], "discord_webhook")
        self.assertEqual(deliveries[0]["payload"]["text"], "hi back")

    def test_progress_checkpoint_and_recoverable_timeout_can_be_reclaimed(self):
        self.register_codex()
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="long job",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m1","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")

        progress = record_job_progress(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            stage="editing",
            summary="created two commits",
            session_id="sess-123",
        )
        timeout = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={
                "response_text": "recoverable timeout",
                "timeout": {"kind": "activity", "configured_budget_seconds": 1800},
            },
        )
        reclaimed = claim_job(self.conn, agent_id="mac-codex", recoverable=True)

        self.assertEqual(progress.job["progress"]["summary"], "created two commits")
        self.assertEqual(timeout.job["status"], "timed_out")
        self.assertEqual(timeout.job["result"]["timeout"]["session_id"], "sess-123")
        self.assertTrue(timeout.job["recoverable"])
        self.assertTrue(reclaimed.claimed)
        self.assertEqual(reclaimed.job["status"], "running")
        self.assertEqual(reclaimed.job["attempt_count"], 2)
        self.assertEqual(reclaimed.job["progress"]["stage"], "editing")

    def test_late_result_after_recoverable_timeout_is_accepted_once(self):
        self.register_codex()
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="long job",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m1","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "recoverable timeout"},
        )

        accepted = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "final answer", "session_id": "sess-late"},
        )
        replay = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "duplicate final", "session_id": "sess-late"},
        )

        deliveries = [row_to_dict(row) for row in list_deliveries(self.conn)]
        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        self.assertEqual(accepted.job["status"], "done")
        self.assertEqual(accepted.event["event_type"], "job.late_result_accepted")
        self.assertEqual(deliveries[-1]["payload"]["text"], "final answer")
        self.assertEqual(replay.event["event_type"], "job.result_replayed")
        self.assertEqual(replay.event["payload"]["submitted_result"]["response_text"], "duplicate final")
        self.assertIn("job.result_replayed", [event["event_type"] for event in events])

    def test_claim_rejects_offline_agent(self):
        self.register_codex()
        self.conn.execute(
            "UPDATE agents SET online_state = ? WHERE id = ?",
            ("offline", "mac-codex"),
        )
        self.conn.commit()

        with self.assertRaisesRegex(RuntimeError, "offline"):
            claim_job(self.conn, agent_id="mac-codex")

    # -- Runtime completion records events without overwriting harness phase --

    def _setup_task_mirror(self, task_id: str = "phase-8.6", phase: str = "running"):
        """Create a task mirror so upsert_task_mirror can update phase."""
        from coordinate.db import upsert_task_mirror as utm
        utm(
            self.conn,
            workspace_id="demo",
            task_id=task_id,
            phase=phase,
            owner="mac-omp",
            branch=None,
            pr=None,
            payload={},
        )

    def test_report_result_done_creates_agent_reported_event(self):
        """Runtime job done records agent.reported and preserves the harness phase."""
        self.register_codex()
        self._setup_task_mirror("phase-8.6", "running")
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="do phase 8.6",
            task_id="phase-8.6",
            origin={"platform": "discord", "destination": "ch-1", "message_id": "m2","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")

        _result = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"summary": "Implemented Phase 8.6", "response_text": "Done!"},
        )

        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        event_types = [e["event_type"] for e in events]
        self.assertIn("agent.reported", event_types)

        agent_event = next(e for e in events if e["event_type"] == "agent.reported")
        payload = agent_event["payload"]
        self.assertEqual(payload["source"], "runtime")
        self.assertEqual(payload["action"], "done")
        self.assertEqual(payload["status"], "done")
        self.assertEqual(payload["agent_id"], "mac-codex")
        self.assertIn("job_id", payload)
        self.assertIn("result_summary", payload)

        # Runtime attention is event-derived; task phase remains the harness projection.
        tasks = list_task_mirrors(self.conn, workspace_id="demo")
        phase_task = next(t for t in tasks if t["task_id"] == "phase-8.6")
        self.assertEqual(phase_task["phase"], "running")

        # Verify replay is idempotent
        # Verify replay doesn't create duplicate agent.reported events
        _replay = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"summary": "Implemented Phase 8.6"},
        )
        # replay goes through job.result_replayed path (different key),
        # but agent.reported must not duplicate
        events_after = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        agent_events_after = [e for e in events_after if e["event_type"] == "agent.reported"]
        self.assertEqual(len(agent_events_after), 1, "agent.reported must not duplicate on replay")
    def test_report_result_failed_creates_agent_reported_blocker(self):
        """Runtime job failed → agent.reported with action=blocker."""
        self.register_codex()
        self._setup_task_mirror("phase-8.6", "running")
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="do phase 8.6",
            task_id="phase-8.6",
            origin={"platform": "discord", "destination": "ch-1", "message_id": "m3","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")

        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="failed",
            result={"summary": "Build failed", "error": "compile error"},
        )

        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        agent_events = [e for e in events if e["event_type"] == "agent.reported"]
        self.assertEqual(len(agent_events), 1)
        payload = agent_events[0]["payload"]
        self.assertEqual(payload["source"], "runtime")
        self.assertEqual(payload["action"], "blocker")
        # Failed jobs also preserve the harness phase.
        tasks = list_task_mirrors(self.conn, workspace_id="demo")
        phase_task = next(t for t in tasks if t["task_id"] == "phase-8.6")
        self.assertEqual(phase_task["phase"], "running")

    # -- Phase 8.8: runtime [agent-report] decision= → review.completed/rejected --

    def test_report_result_review_decision_approve_creates_review_completed(self):
        """Phase 8.8: response_text with [agent-report] decision=approve → review.completed + agent.reported carries decision."""
        self.register_codex()
        self._setup_task_mirror("phase-8.8", "running")
        request = submit_request(
            self.conn, workspace_id="demo", target_agent="mac-codex",
            prompt="review phase 8.8", task_id="phase-8.8",
            origin={"platform": "discord", "destination": "ch-1", "message_id": "m4","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn, job_id=request.job["id"], agent_id="mac-codex", status="done",
            result={"response_text": "Approved.\n[agent-report]\ndecision=approve\nworkspace_id=demo\ntask_id=phase-8.8\nsummary=\"OK\""},
        )
        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        types = [e["event_type"] for e in events]
        self.assertIn("review.completed", types)
        rc = next(e for e in events if e["event_type"] == "review.completed")
        self.assertEqual(rc["payload"]["decision"], "approve")
        self.assertEqual(rc["payload"]["reviewer"], "mac-codex")
        self.assertEqual(rc["payload"]["source"], "runtime")
        ar = next(e for e in events if e["event_type"] == "agent.reported")
        self.assertEqual(ar["payload"]["decision"], "approve")

    def test_report_result_review_decision_reject_creates_review_rejected(self):
        """Phase 8.8: decision=reject → review.rejected."""
        self.register_codex()
        self._setup_task_mirror("phase-8.8", "running")
        request = submit_request(
            self.conn, workspace_id="demo", target_agent="mac-codex",
            prompt="review phase 8.8", task_id="phase-8.8",
            origin={"platform": "discord", "destination": "ch-1", "message_id": "m5","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn, job_id=request.job["id"], agent_id="mac-codex", status="done",
            result={"response_text": "[agent-report]\ndecision=reject\nworkspace_id=demo\ntask_id=phase-8.8\nreason=\"bugs found\""},
        )
        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        types = [e["event_type"] for e in events]
        self.assertIn("review.rejected", types)
        rr = next(e for e in events if e["event_type"] == "review.rejected")
        self.assertEqual(rr["payload"]["decision"], "reject")
        # Layer B (backlog #8): reviewer reason must land in the payload so the
        # delivery renderer can show it on Discord (not just the one-line summary).
        self.assertEqual(rr["payload"]["reason"], "bugs found")

    def test_report_result_no_decision_no_review_event(self):
        """Phase 8.8: response_text without decision= → no review.completed/rejected (fallback to phase-8.6 agent.reported only)."""
        self.register_codex()
        self._setup_task_mirror("phase-8.8", "running")
        request = submit_request(
            self.conn, workspace_id="demo", target_agent="mac-codex",
            prompt="do phase 8.8", task_id="phase-8.8",
            origin={"platform": "discord", "destination": "ch-1", "message_id": "m6","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn, job_id=request.job["id"], agent_id="mac-codex", status="done",
            result={"response_text": "Done, no decision."},
        )
        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        types = [e["event_type"] for e in events]
        self.assertNotIn("review.completed", types)
        self.assertNotIn("review.rejected", types)
        self.assertIn("agent.reported", types)

    def test_report_review_decision_without_ws_task_uses_job_fallback(self):
        """#12.2 invariant: a reviewer decision=approve block that omits
        workspace_id/task_id must still create review.completed — runtime
        passes the job's workspace_id/task_id as parse fallback so the signal
        is not silently dropped."""
        self.register_codex()
        self._setup_task_mirror("phase-fb", "running")
        request = submit_request(
            self.conn, workspace_id="demo", target_agent="mac-codex",
            prompt="review fallback", task_id="phase-fb",
            origin={"platform": "discord", "destination": "ch-1", "message_id": "m-fb","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn, job_id=request.job["id"], agent_id="mac-codex", status="done",
            # decision present, but workspace_id/task_id deliberately omitted
            result={"response_text": "Approved.\n[agent-report]\ndecision=approve\nreason=\"lgtm\""},
        )
        events = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        types = [e["event_type"] for e in events]
        self.assertIn("review.completed", types)
        rc = next(e for e in events if e["event_type"] == "review.completed")
        self.assertEqual(rc["payload"]["decision"], "approve")
        self.assertEqual(rc["payload"]["reason"], "lgtm")
        self.assertEqual(rc["payload"]["source"], "runtime")
        # agent.reported also carries the decision recovered via fallback
        ar = next(e for e in events if e["event_type"] == "agent.reported")
        self.assertEqual(ar["payload"]["decision"], "approve")

    def test_report_result_no_task_mirror_still_creates_event(self):
        """agent.reported event created even when no task mirror exists (no phase update)."""
        self.register_codex()
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="test",
            origin={"platform": "discord", "destination": "ch-1", "message_id": "m4","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")

        result = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"text": "ok"},
        )
        self.assertTrue(result.event_created)

        events = [row_to_dict(row) for row in list_events(self.conn, "demo")]
        self.assertIn("agent.reported", [e["event_type"] for e in events])

    # -- 8.4.3 P1 #1: ordinary claim must not auto-reclaim recoverable timed_out --

    def _seed_timed_out_recoverable(self, agent_id="mac-codex"):
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent=agent_id,
            prompt="long job",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m-rec","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
        )
        claim_job(self.conn, agent_id=agent_id, recoverable=True)  # pending → running
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id=agent_id,
            status="timed_out",
            result={"response_text": "recoverable timeout"},
        )
        return request.job["id"]

    def test_claim_default_does_not_pick_up_recoverable_timed_out(self):
        """P1 #1: ordinary claim (default recoverable=False) must NOT reclaim timed_out+recoverable."""
        self.register_codex()
        self._seed_timed_out_recoverable()
        result = claim_job(self.conn, agent_id="mac-codex")  # default
        self.assertFalse(result.claimed, "ordinary poll must not reclaim recoverable timed_out")
        self.assertIsNone(result.job)

    def test_claim_recoverable_true_reclaims_timed_out(self):
        """Explicit recovery claim still reclaims timed_out+recoverable."""
        self.register_codex()
        self._seed_timed_out_recoverable()
        result = claim_job(self.conn, agent_id="mac-codex", recoverable=True)
        self.assertTrue(result.claimed)
        self.assertEqual(result.job["status"], "running")
        self.assertEqual(result.job["attempt_count"], 2)

    # -- 8.4.3 P1 #2: attempt token CAS on report/progress --

    def test_report_with_stale_attempt_token_is_rejected(self):
        """P1 #2: a late result from attempt 1 must not overwrite a job reclaimed as attempt 2."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()  # attempt 1 timed_out (recoverable)
        second = claim_job(self.conn, agent_id="mac-codex", recoverable=True)  # attempt 2
        self.assertEqual(second.attempt_token, 2)
        with self.assertRaisesRegex(RuntimeError, "attempt"):
            report_job_result(
                self.conn,
                job_id=jid,
                agent_id="mac-codex",
                status="done",
                result={"response_text": "late from attempt 1"},
                attempt_token=1,
            )
        # job still running (attempt 2), not overwritten by stale attempt 1
        self.assertEqual(row_to_dict(get_job(self.conn, jid))["status"], "running")

    def test_report_with_stale_attempt_token_writes_no_events(self):
        """#12.2 invariant: a stale-attempt CAS rejection must not append any
        job terminal / agent.reported / review event — the report is rejected
        before the event-issuing stages run (so a reordered refactor can't
        leak a decision past a failed CAS)."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()  # attempt 1 timed_out (recoverable)
        second = claim_job(self.conn, agent_id="mac-codex", recoverable=True)  # attempt 2
        self.assertEqual(second.attempt_token, 2)
        events_before = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        # decision=approve (no ws/task_id) so a reordering bug would leak BOTH
        # agent.reported and review.completed — making this guard sensitive.
        with self.assertRaisesRegex(RuntimeError, "attempt"):
            report_job_result(
                self.conn, job_id=jid, agent_id="mac-codex", status="done",
                result={"response_text": "Approved.\n[agent-report]\ndecision=approve\nsummary=\"ok\""},
                attempt_token=1,
            )
        events_after = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        new_types = [e["event_type"] for e in events_after[len(events_before):]]
        self.assertNotIn("job.completed", new_types)
        self.assertNotIn("agent.reported", new_types)
        self.assertNotIn("review.completed", new_types)
        self.assertNotIn("review.rejected", new_types)
        # job untouched: still running at attempt 2
        job = row_to_dict(get_job(self.conn, jid))
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["attempt_count"], 2)

    def test_report_with_correct_attempt_token_applies(self):
        """P1 #2: a result with the current attempt_token applies normally."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()
        second = claim_job(self.conn, agent_id="mac-codex", recoverable=True)
        report_job_result(
            self.conn,
            job_id=jid,
            agent_id="mac-codex",
            status="done",
            result={"response_text": "ok"},
            attempt_token=second.attempt_token,
        )
        self.assertEqual(row_to_dict(get_job(self.conn, jid))["status"], "done")

    def test_report_without_attempt_token_applies_backward_compatible(self):
        """P1 #2: omitting attempt_token (None) does NOT CAS — backward compatible for operator/CLI explicit reports."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()
        claim_job(self.conn, agent_id="mac-codex", recoverable=True)  # attempt 2
        report_job_result(
            self.conn,
            job_id=jid,
            agent_id="mac-codex",
            status="done",
            result={"response_text": "explicit"},
            # no attempt_token
        )
        self.assertEqual(row_to_dict(get_job(self.conn, jid))["status"], "done")

    def test_progress_with_stale_attempt_token_is_rejected(self):
        """P1 #2: progress from a stale attempt is rejected (no job.progress event)."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()  # attempt 1 timed_out
        claim_job(self.conn, agent_id="mac-codex", recoverable=True)  # reclaimed → attempt 2
        events_before = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        with self.assertRaisesRegex(RuntimeError, "attempt"):
            record_job_progress(
                self.conn,
                job_id=jid,
                agent_id="mac-codex",
                stage="editing",
                summary="stale",
                attempt_token=1,
            )
        # no job.progress event appended for the rejected CAS
        events_after = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        self.assertEqual(len(events_after), len(events_before))

    def test_progress_with_correct_attempt_token_writes(self):
        """P1 #2: progress with current attempt_token records the checkpoint."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()
        second = claim_job(self.conn, agent_id="mac-codex", recoverable=True)
        record_job_progress(
            self.conn,
            job_id=jid,
            agent_id="mac-codex",
            stage="editing",
            summary="current",
            attempt_token=second.attempt_token,
        )
        job = row_to_dict(get_job(self.conn, jid))
        self.assertEqual(job["progress"]["stage"], "editing")
        self.assertEqual(job["progress"]["summary"], "current")

    def test_late_result_with_stale_attempt_token_is_rejected(self):
        """P1 #2: a late done from attempt 1 must NOT overwrite a job reclaimed+timed_out as attempt 2."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()  # attempt 1 timed_out
        # reclaim → attempt 2, then attempt 2 also times out (still timed_out+recoverable)
        claim_job(self.conn, agent_id="mac-codex", recoverable=True)
        report_job_result(
            self.conn, job_id=jid, agent_id="mac-codex", status="timed_out",
            result={"response_text": "attempt 2 timeout", "recoverable": True},
            attempt_token=2,
        )
        self.assertEqual(row_to_dict(get_job(self.conn, jid))["attempt_count"], 2)
        events_before = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        # attempt 1's late done arrives with stale token 1 → must reject
        with self.assertRaisesRegex(RuntimeError, "attempt"):
            report_job_result(
                self.conn, job_id=jid, agent_id="mac-codex", status="done",
                result={"response_text": "attempt 1 late done"}, attempt_token=1,
            )
        # no job.late_result_accepted for the rejected stale attempt
        events_after = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        self.assertFalse(
            any(e["event_type"] == "job.late_result_accepted" for e in events_after[len(events_before):]),
            "rejected stale late result must not produce job.late_result_accepted",
        )
        # job untouched: still timed_out attempt 2
        job = row_to_dict(get_job(self.conn, jid))
        self.assertEqual(job["status"], "timed_out")
        self.assertEqual(job["attempt_count"], 2)

    def test_late_result_with_current_attempt_token_is_accepted(self):
        """P1 #2: a late done with the current attempt_token (attempt 2) accepts normally."""
        self.register_codex()
        jid = self._seed_timed_out_recoverable()  # attempt 1 timed_out
        claim_job(self.conn, agent_id="mac-codex", recoverable=True)  # attempt 2
        report_job_result(
            self.conn, job_id=jid, agent_id="mac-codex", status="timed_out",
            result={"response_text": "attempt 2 timeout", "recoverable": True},
            attempt_token=2,
        )
        # attempt 2's own late done with token 2 → accepted
        report_job_result(
            self.conn, job_id=jid, agent_id="mac-codex", status="done",
            result={"response_text": "attempt 2 late done"}, attempt_token=2,
        )
        job = row_to_dict(get_job(self.conn, jid))
        self.assertEqual(job["status"], "done")
        events = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        self.assertIn("job.late_result_accepted", [e["event_type"] for e in events])

    def test_reply_platform_discordbus_workspace_keeps_discord(self):
        """P1 #4: a DiscordBus workspace (default_bus=discord) keeps reply platform=discord, not forced to discord_webhook."""
        upsert_workspace(
            self.conn,
            workspace_id="bus-ws",
            name="Bus",
            path=self.tmp.name,
            harness_root=self.tmp.name,
            default_bus="discord",
        )
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="bus-ws",
            host_id="mac",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        self.register_codex()
        request = submit_request(
            self.conn,
            workspace_id="bus-ws",
            target_agent="mac-codex",
            prompt="x",
            origin={"platform": "discord", "destination": "ch", "message_id": "m-bus","session_scope_id":"discord:test"},
            reply={"platform": "discord", "destination": "ch"},
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "hi"},
            attempt_token=1,
        )
        deliveries = [row_to_dict(r) for r in list_deliveries(self.conn)]
        self.assertTrue(
            any(d["platform"] == "discord" for d in deliveries),
            "DiscordBus workspace must keep platform=discord, not forced to discord_webhook",
        )

    def test_reply_platform_none_keeps_completion_evidence_without_delivery(self):
        self.register_codex()
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="audit-only reply",
            origin={
                "platform": "discord",
                "destination": "channel-1",
                "message_id": "m-none",
                "session_scope_id": "discord:test",
            },
            reply={"platform": "none", "destination": "audit"},
        )
        claim_job(self.conn, agent_id="mac-codex")

        result = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "already delivered by bridge"},
        )

        event_types = [
            row_to_dict(row)["event_type"] for row in list_events(self.conn, "demo")
        ]
        self.assertEqual(result.job["status"], "done")
        self.assertIn("job.completed", event_types)
        self.assertIsNone(result.delivery)
        self.assertFalse(result.delivery_created)
        self.assertEqual(list_deliveries(self.conn), [])


class PreWriteRejectionTests(unittest.TestCase):
    """R1-1: authority inputs must resolve before any event/job write."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = initialize(":memory:")
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
            capabilities={},
        )

    def _submit(self, **overrides):
        params = {
            "workspace_id": "demo",
            "target_agent": "mac-codex",
            "prompt": "hello",
            "origin": {
                "platform": "discord",
                "destination": "ch",
                "message_id": "m1",
                "session_scope_id": "discord:ch",
            },
            "reply": {"platform": "discord", "destination": "ch"},
        }
        params.update(overrides)
        return submit_request(self.conn, **params)

    def test_missing_host_profile_rejects_before_event_write(self):
        register_agent(self.conn, agent_id="other", host_id="other-host", capabilities={})
        before = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "no host profile"):
            submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="other",
                prompt="hello",
                origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        after = len(list_events(self.conn))
        self.assertEqual(after, before)
        jobs = self.conn.execute("SELECT * FROM jobs WHERE assigned_agent = ?", ("other",)).fetchall()
        self.assertEqual(len(jobs), 0)

    def test_missing_task_mirror_rejects_before_event_write(self):
        before = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "task mirror not found"):
            self._submit(task_id="does-not-exist")
        after = len(list_events(self.conn))
        self.assertEqual(after, before)
        jobs = self.conn.execute("SELECT * FROM jobs WHERE task_id = ?", ("does-not-exist",)).fetchall()
        self.assertEqual(len(jobs), 0)

    def test_unsafe_host_profile_path_rejects_before_event_write(self):
        # R2-1: a host profile with a relative workspace_path must fail before
        # any event or job is created.
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="demo",
            host_id="unsafe",
            workspace_path="relative/ws",
            harness_root="relative/h",
        )
        register_agent(self.conn, agent_id="unsafe-agent", host_id="unsafe", capabilities={})
        before = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "invalid execution context"):
            submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="unsafe-agent",
                prompt="hello",
                origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        after = len(list_events(self.conn))
        self.assertEqual(after, before)
        jobs = self.conn.execute("SELECT * FROM jobs WHERE assigned_agent = ?", ("unsafe-agent",)).fetchall()
        self.assertEqual(len(jobs), 0)

    def test_backfill_rejects_missing_task_mirror(self):
        # R2-1: a pre-upgrade pending job with a non-null task id and no mirror
        # must be rejected before the CAS mutation.
        from coordinate.db import create_job, row_to_dict

        job = create_job(
            self.conn,
            workspace_id="demo",
            task_id="ghost",
            runner_profile_id="mac-codex",
            assigned_agent="mac-codex",
            payload={
                "prompt": "legacy",
                "origin": {"session_scope_id": "discord:legacy", "legacy_scope_ids": []},
                "reply": {"platform": "discord", "destination": "ch"},
            },
        )
        before = row_to_dict(self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job["id"],)).fetchone())
        with self.assertRaisesRegex(RuntimeError, "task mirror not found"):
            claim_job(self.conn, agent_id="mac-codex")
        after = row_to_dict(self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job["id"],)).fetchone())
        self.assertEqual(after["status"], before["status"])
        self.assertEqual(after["attempt_count"], before["attempt_count"])

    def test_invalid_session_scope_rejects_before_event_write(self):
        before = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "invalid execution context"):
            self._submit(origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": ""})
        after = len(list_events(self.conn))
        self.assertEqual(after, before)

    def test_replayed_request_with_different_prompt_rejected(self):
        self._submit(prompt="first")
        with self.assertRaisesRegex(RuntimeError, "prompt conflicts"):
            self._submit(prompt="second")

    def test_replayed_request_with_different_reply_rejected(self):
        self._submit()
        with self.assertRaisesRegex(RuntimeError, "reply conflicts"):
            self._submit(reply={"platform": "discord", "destination": "other"})

    def test_replayed_request_with_different_task_rejected(self):
        from coordinate.db import upsert_task_mirror
        upsert_task_mirror(self.conn, workspace_id="demo", task_id="t1", phase="open", owner="o", branch=None, pr=None, payload={})
        self._submit(task_id="t1")
        with self.assertRaisesRegex(RuntimeError, "execution_context conflicts"):
            self._submit(task_id=None)

    def test_replayed_request_with_different_session_scope_rejected(self):
        self._submit(origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"})
        with self.assertRaisesRegex(RuntimeError, "execution_context conflicts"):
            self._submit(origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:other"})


class ReplayRejectionTests(unittest.TestCase):
    """R1-2: semantic request authority must match stored snapshot."""

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
        register_agent(self.conn, agent_id="mac-codex", host_id="mac", capabilities={})

    def tearDown(self):
        pass

    def _submit(self, **overrides):
        params = {
            "workspace_id": "demo",
            "target_agent": "mac-codex",
            "prompt": "hello",
            "origin": {
                "platform": "discord",
                "destination": "ch",
                "message_id": "m1",
                "session_scope_id": "discord:ch",
            },
            "reply": {"platform": "discord", "destination": "ch"},
        }
        params.update(overrides)
        return submit_request(self.conn, **params)

    def test_pre_upgrade_identical_replay_accepted(self):
        """Pre-v1 jobs accept exact replay."""
        first = self._submit(task_id=None)
        second = self._submit(task_id=None)
        self.assertEqual(first.job["id"], second.job["id"])

    def test_pre_upgrade_different_prompt_rejected(self):
        """Pre-v1 jobs still reject semantic prompt changes."""
        self._submit(task_id=None, prompt="first")
        with self.assertRaisesRegex(RuntimeError, "prompt conflicts"):
            self._submit(task_id=None, prompt="second")

    def test_pre_upgrade_different_session_scope_rejected(self):
        self._submit(task_id=None)
        with self.assertRaisesRegex(RuntimeError, "execution_context conflicts"):
            self._submit(task_id=None, origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:other"})

    def test_v1_different_session_scope_rejected(self):
        from coordinate.db import upsert_task_mirror
        upsert_task_mirror(self.conn, workspace_id="demo", task_id="t1", phase="open", owner="o", branch=None, pr=None, payload={})
        self._submit(task_id="t1")
        with self.assertRaisesRegex(RuntimeError, "origin conflicts"):
            self._submit(task_id="t1", origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:other"})


class RoutedRuntimeTests(unittest.TestCase):
    """P9-2B: explicit deterministic routing mode."""

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
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="demo",
            host_id="pc",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )

    def _register(self, agent_id: str, host_id: str):
        register_agent(
            self.conn,
            agent_id=agent_id,
            host_id=host_id,
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id=agent_id, host_id=host_id)

    def _authorize(self, agent_name: str, discord_id: str):
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name=agent_name,
            discord_user_id=discord_id,
            actor="test",
            reason="test",
        )

    def _sync_capacity(self, agent_ids: list[str]):
        policies = tuple(
            CapacityPolicy(agent_id=aid, max_concurrent_jobs=2) for aid in agent_ids
        )
        catalog = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path="/dev/null",
            policies=policies,
        )
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_capacity_catalog_hash(catalog)
        )
        sync_capacity_catalog(self.conn, catalog)

    def _sync_catalog(self, agent_ids: list[str]):
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
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_executor_catalog_hash(catalog)
        )
        sync_executor_catalog(self.conn, catalog)
        self._sync_capacity(agent_ids)

    def _routed_request(self, **overrides):
        params = {
            "workspace_id": "demo",
            "prompt": "hello",
            "origin": {
                "platform": "discord",
                "destination": "ch",
                "message_id": "m1",
                "session_scope_id": "discord:ch",
            },
            "reply": {"platform": "discord", "destination": "ch"},
        }
        routing = build_routing_request(required_capabilities=["coding"])
        params["routing_request"] = routing
        params.update(overrides)
        return submit_request(self.conn, **params)

    def test_exact_plus_routed_rejected(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        routing = build_routing_request(required_capabilities=["coding"])
        with self.assertRaisesRegex(RuntimeError, "exact-plus-routed"):
            submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="mac-omp",
                routing_request=routing,
                prompt="hello",
                origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )

    def test_neither_mode_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "neither-mode"):
            submit_request(
                self.conn,
                workspace_id="demo",
                prompt="hello",
                origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )

    def test_no_candidate_zero_write(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        before_events = len(list_events(self.conn))
        before_jobs = self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        routing = build_routing_request(required_capabilities=["review"])
        with self.assertRaisesRegex(RuntimeError, "executor_route_no_candidate"):
            submit_request(
                self.conn,
                workspace_id="demo",
                routing_request=routing,
                prompt="hello",
                origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        self.assertEqual(len(list_events(self.conn)), before_events)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], before_jobs
        )

    def test_override_ineligible_zero_write(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        before_events = len(list_events(self.conn))
        before_jobs = self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        routing = build_routing_request(
            required_capabilities=["coding"],
            operator_override_agent_id="mac-codex",
            operator_override_reason="not eligible",
        )
        with self.assertRaisesRegex(RuntimeError, "executor_route_override_ineligible"):
            submit_request(
                self.conn,
                workspace_id="demo",
                routing_request=routing,
                prompt="hello",
                origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
                reply={"platform": "discord", "destination": "ch"},
            )
        self.assertEqual(len(list_events(self.conn)), before_events)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], before_jobs
        )

    def test_routed_submit_creates_event_and_job(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        self.assertTrue(first.event_created)
        self.assertTrue(first.job_created)
        self.assertEqual(first.job["assigned_agent"], "mac-omp")
        self.assertEqual(first.job["runner_profile_id"], "mac-omp")
        self.assertEqual(first.job["payload"]["routing_request"]["routing_request_id"], first.event["payload"]["routing_request"]["routing_request_id"])
        self.assertEqual(first.job["payload"]["routing_decision"]["routing_decision_id"], first.event["payload"]["routing_decision"]["routing_decision_id"])
        self.assertEqual(first.event["payload"]["target_agent"], "mac-omp")
        self.assertEqual(first.event["target"], "mac-omp")

    def test_routed_replay_ignores_current_load_and_host(self):
        self._register("mac-omp", "mac")
        self._register("mac-codex", "mac")
        self._authorize("mac-omp", "12345")
        self._authorize("mac-codex", "12346")
        self._sync_catalog(["mac-omp", "mac-codex"])
        first = self._routed_request()
        original_job_id = first.job["id"]
        original_decision_id = first.job["payload"]["routing_decision"]["routing_decision_id"]
        original_agent = first.job["assigned_agent"]

        # Mutate current state: add a pending job to the originally selected agent
        # and change the preferred host; replay must still return the original decision.
        from coordinate.job_repository import create_job
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id=original_agent,
            assigned_agent=original_agent,
            payload={},
        )
        second = self._routed_request()
        self.assertFalse(second.event_created)
        self.assertFalse(second.job_created)
        self.assertEqual(second.job["id"], original_job_id)
        self.assertEqual(second.job["payload"]["routing_decision"]["routing_decision_id"], original_decision_id)
        self.assertEqual(second.job["assigned_agent"], original_agent)

        # Changing the preferred host id in the request makes a different routing_request id,
        # so it is a new routed request, not a replay. It should create a new event/job.
        routing = build_routing_request(
            required_capabilities=["coding"], preferred_host_id="pc"
        )
        second_new = submit_request(
            self.conn,
            workspace_id="demo",
            routing_request=routing,
            prompt="hello",
            origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:ch"},
            reply={"platform": "discord", "destination": "ch"},
        )
        self.assertTrue(second_new.event_created)
        self.assertTrue(second_new.job_created)
        self.assertNotEqual(second_new.job["id"], original_job_id)

    def test_routed_replay_rejects_content_changes(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        self._routed_request()
        with self.assertRaisesRegex(RuntimeError, "prompt conflicts"):
            self._routed_request(prompt="changed")
        with self.assertRaisesRegex(RuntimeError, "origin conflicts"):
            self._routed_request(origin={"platform": "discord", "destination": "ch", "message_id": "m1", "session_scope_id": "discord:other"})
        with self.assertRaisesRegex(RuntimeError, "reply conflicts"):
            self._routed_request(reply={"platform": "discord", "destination": "other"})
        with self.assertRaisesRegex(RuntimeError, "task_id conflicts"):
            self._routed_request(task_id="t1")

    def test_claim_adds_route_evidence(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        self._routed_request()
        claim = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim.claimed)
        self.assertIsInstance(claim.to_dict()["execution_lease"], dict)
        events = [row_to_dict(e) for e in list_events(self.conn, "demo")]
        claimed = [e for e in events if e["event_type"] == "job.claimed"]
        self.assertEqual(len(claimed), 1)
        payload = claimed[0]["payload"]
        self.assertIn("routing_request_id", payload)
        self.assertIn("routing_decision_id", payload)
        self.assertIn("selection_kind", payload)

    def test_retry_routed_job_rejected(self):
        from coordinate.jobs import JobError, retry_job

        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        request = self._routed_request()
        # Force the job to a failed status so retry_job would otherwise accept it.
        self.conn.execute("UPDATE jobs SET status = 'failed' WHERE id = ?", (request.job["id"],))
        self.conn.commit()
        with self.assertRaisesRegex(JobError, "routed_runtime_retry_requires_explicit_resubmission"):
            retry_job(self.conn, request.job["id"])

    def test_routed_job_idempotency_key_is_target_independent(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        # The default idempotency key is stable across different current candidates.
        # We can observe this by asserting a second identical request replays.
        second = self._routed_request()
        self.assertEqual(first.job["id"], second.job["id"])
        self.assertFalse(second.event_created)


class RoutedRuntimeCorrectionTests(unittest.TestCase):
    """R1-4/R1-6 adversarial and atomic tests."""

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
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="demo",
            host_id="pc",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )

    def _register(self, agent_id: str, host_id: str):
        register_agent(
            self.conn,
            agent_id=agent_id,
            host_id=host_id,
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id=agent_id, host_id=host_id)

    def _authorize(self, agent_name: str, discord_id: str):
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name=agent_name,
            discord_user_id=discord_id,
            actor="test",
            reason="test",
        )

    def _sync_capacity(self, agent_ids: list[str]):
        policies = tuple(
            CapacityPolicy(agent_id=aid, max_concurrent_jobs=2) for aid in agent_ids
        )
        catalog = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path="/dev/null",
            policies=policies,
        )
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_capacity_catalog_hash(catalog)
        )
        sync_capacity_catalog(self.conn, catalog)

    def _sync_catalog(self, agent_ids: list[str]):
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
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_executor_catalog_hash(catalog)
        )
        sync_executor_catalog(self.conn, catalog)
        self._sync_capacity(agent_ids)

    def _origin(self):
        return {
            "platform": "discord",
            "destination": "ch",
            "message_id": "m1",
            "session_scope_id": "discord:ch",
        }

    def _reply(self):
        return {"platform": "discord", "destination": "ch"}

    def _routed_request(self, **overrides):
        params = {
            "workspace_id": "demo",
            "prompt": "hello",
            "origin": self._origin(),
            "reply": self._reply(),
        }
        routing = build_routing_request(required_capabilities=["coding"])
        params["routing_request"] = routing
        params.update(overrides)
        return submit_request(self.conn, **params)

    def _exact_request(self, target_agent="mac-omp", **overrides):
        params = {
            "workspace_id": "demo",
            "target_agent": target_agent,
            "prompt": "hello",
            "origin": self._origin(),
            "reply": self._reply(),
        }
        params.update(overrides)
        return submit_request(self.conn, **params)

    def test_explicit_idempotency_key_exact_then_routed_rejects(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._exact_request(idempotency_key="shared-key")
        self.assertTrue(first.event_created)
        routing = build_routing_request(required_capabilities=["coding"])
        before_events = len(list_events(self.conn))
        before_jobs = self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        with self.assertRaisesRegex(RuntimeError, "routing_request conflicts with stored event"):
            submit_request(
                self.conn,
                workspace_id="demo",
                routing_request=routing,
                prompt="hello",
                origin=self._origin(),
                reply=self._reply(),
                idempotency_key="shared-key",
            )
        self.assertEqual(len(list_events(self.conn)), before_events)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], before_jobs)

    def test_explicit_idempotency_key_routed_then_exact_rejects(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request(idempotency_key="shared-key")
        self.assertTrue(first.event_created)
        before_events = len(list_events(self.conn))
        before_jobs = self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        with self.assertRaisesRegex(RuntimeError, "explicit idempotency key conflicts with routed request"):
            submit_request(
                self.conn,
                workspace_id="demo",
                target_agent="mac-omp",
                prompt="hello",
                origin=self._origin(),
                reply=self._reply(),
                idempotency_key="shared-key",
            )
        self.assertEqual(len(list_events(self.conn)), before_events)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], before_jobs)

    def test_replay_rejects_forged_event_target(self):
        self._register("mac-omp", "mac")
        self._register("mac-codex", "mac")
        self._authorize("mac-omp", "12345")
        self._authorize("mac-codex", "12346")
        self._sync_catalog(["mac-omp", "mac-codex"])
        first = self._routed_request()
        selected_agent = first.job["payload"]["routing_decision"]["selected_agent_id"]
        # Mutate the stored event target to a different agent.
        other_agent = "mac-omp" if selected_agent == "mac-codex" else "mac-codex"
        self.conn.execute(
            "UPDATE events SET target = ? WHERE id = ?",
            (other_agent, first.event["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "stored event target conflicts with stored decision"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_replay_rejects_forged_payload_target_agent(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        payload = dict(first.event["payload"])
        payload["target_agent"] = "other"
        self.conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), first.event["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "stored event payload target_agent conflicts with stored decision"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_replay_rejects_forged_job_request_event_id(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        payload = dict(first.job["payload"])
        payload["request_event_id"] = "forged"
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "request_event_id conflicts with event"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_replay_rejects_forged_job_assignment(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        self.conn.execute(
            "UPDATE jobs SET assigned_agent = ? WHERE id = ?",
            ("other", first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "does not match job assignment"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_replay_rejects_forged_job_runner_profile_id(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        self.conn.execute(
            "UPDATE jobs SET runner_profile_id = ? WHERE id = ?",
            ("other", first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "does not match job runner_profile_id"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_replay_rejects_forged_job_workspace(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        # Create a different workspace so the foreign key constraint is satisfied.
        upsert_workspace(
            self.conn,
            workspace_id="other",
            name="Other",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        self.conn.execute(
            "UPDATE jobs SET workspace_id = ? WHERE id = ?",
            ("other", first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "stored job workspace_id conflicts with event"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_concurrent_loser_replays_stored_event(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        # Simulate a concurrent race: the initial idempotency lookup misses,
        # but append_event loses the INSERT race and returns the stored row.
        from coordinate import runtime as runtime_module

        calls = {"execute": 0, "append_event": False}

        original_append_event = runtime_module.append_event
        original_execute = self.conn.execute

        def losing_execute(sql, *args, **kwargs):
            if "SELECT * FROM events WHERE idempotency_key" in (sql or ""):
                calls["execute"] += 1

                class Cursor:
                    def fetchone(self):
                        return None

                return Cursor()
            return original_execute(sql, *args, **kwargs)

        def losing_append_event(*args, **kwargs):
            calls["append_event"] = True
            row = list_events(self.conn, "demo")[-1]
            return type("AppendResult", (), {"row": row, "created": False})()

        self.conn.execute = losing_execute
        runtime_module.append_event = losing_append_event
        try:
            second = self._routed_request()
            self.assertFalse(second.event_created)
            self.assertEqual(second.job["id"], first.job["id"])
        finally:
            self.conn.execute = original_execute
            runtime_module.append_event = original_append_event

        self.assertEqual(calls["execute"], 1)
        self.assertTrue(calls["append_event"])

    def test_event_job_creation_rollback(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        from coordinate import runtime as runtime_module

        def failing_create_job(*args, **kwargs):
            raise RuntimeError("boom")

        original_create_job = runtime_module.create_job
        runtime_module.create_job = failing_create_job
        try:
            before_events = len(list_events(self.conn))
            before_jobs = self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            with self.assertRaisesRegex(RuntimeError, "boom"):
                self._routed_request()
            self.assertEqual(len(list_events(self.conn)), before_events)
            self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], before_jobs)
        finally:
            runtime_module.create_job = original_create_job

    def test_exact_event_job_creation_rollback(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        from coordinate import runtime as runtime_module

        def failing_create_job(*args, **kwargs):
            raise RuntimeError("boom")

        original_create_job = runtime_module.create_job
        runtime_module.create_job = failing_create_job
        try:
            before_events = len(list_events(self.conn))
            before_jobs = self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            with self.assertRaisesRegex(RuntimeError, "boom"):
                self._exact_request()
            self.assertEqual(len(list_events(self.conn)), before_events)
            self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], before_jobs)
        finally:
            runtime_module.create_job = original_create_job

    def test_timed_out_recovery_retains_routing_decision(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        original_decision_id = first.job["payload"]["routing_decision"]["routing_decision_id"]
        original_binding = first.job["payload"]["executor_binding"]
        original_context = first.job["payload"]["execution_context"]
        # Move the job to timed_out/recoverable.
        self.conn.execute(
            "UPDATE jobs SET status = 'timed_out', recoverable = 1 WHERE id = ?",
            (first.job["id"],),
        )
        self.conn.commit()
        claim = claim_job(
            self.conn,
            agent_id="mac-omp",
            recoverable=True,
            recovery_reason="operator confirmed prior process stopped via tooling",
            prior_process_stopped=True,
        )
        self.assertTrue(claim.claimed)
        job = get_job(self.conn, first.job["id"])
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["attempt_count"], 1)
        self.assertEqual(job["assigned_agent"], "mac-omp")
        payload = json.loads(job["payload_json"])
        self.assertEqual(
            payload["routing_decision"]["routing_decision_id"],
            original_decision_id,
        )
        self.assertEqual(payload["executor_binding"], original_binding)
        self.assertEqual(payload["execution_context"], original_context)
        # A second recoverable claim should fail because recoverable is now 0.
        second = claim_job(
            self.conn,
            agent_id="mac-omp",
            recoverable=True,
            recovery_reason="operator confirmed prior process stopped via tooling",
            prior_process_stopped=True,
        )
        self.assertFalse(second.claimed)

    def test_generic_retry_rejects_routed_job_before_writes(self):
        from coordinate.jobs import JobError, retry_job

        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        request = self._routed_request()
        self.conn.execute("UPDATE jobs SET status = 'failed' WHERE id = ?", (request.job["id"],))
        self.conn.commit()
        before_events = len(list_events(self.conn))
        before_jobs = self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        with self.assertRaisesRegex(JobError, "routed_runtime_retry_requires_explicit_resubmission"):
            retry_job(self.conn, request.job["id"])
        self.assertEqual(len(list_events(self.conn)), before_events)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], before_jobs)

    def test_forged_claim_evidence_zero_mutation(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        job_id = first.job["id"]
        # Forge the execution context host in the stored job payload.
        payload = dict(first.job["payload"])
        payload["execution_context"] = dict(payload["execution_context"])
        payload["execution_context"]["host_id"] = "pc"
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), job_id),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "invalid routing claim evidence|invalid stored execution context"):
            claim_job(self.conn, agent_id="mac-omp")
        job = get_job(self.conn, job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempt_count"], 0)
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_replay_rejects_forged_event_row_task_id(self):
        from coordinate.db import upsert_task_mirror
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        upsert_task_mirror(self.conn, workspace_id="demo", task_id="t1", phase="open", owner="o", branch=None, pr=None, payload={})
        first = self._routed_request(task_id="t1")
        self.conn.execute(
            "UPDATE events SET task_id = ? WHERE id = ?",
            ("other", first.event["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "event row task_id conflicts"):
            self._routed_request(task_id="t1")
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_exact_replay_rejects_forged_job_prompt(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._exact_request()
        payload = dict(first.job["payload"])
        payload["prompt"] = "forged"
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "prompt conflicts with stored job"):
            self._exact_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_routed_replay_rejects_forged_job_prompt(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        payload = dict(first.job["payload"])
        payload["prompt"] = "forged"
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "prompt conflicts with stored job"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def _recompute_decision_id(self, decision: dict[str, Any]) -> dict[str, Any]:
        body = {k: v for k, v in decision.items() if k != "routing_decision_id"}
        decision["routing_decision_id"] = _compute_routing_decision_id(body)
        return decision

    def test_routed_replay_rejects_forged_decision_binding_link(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        forged_binding_id = "sha256:" + "0" * 64
        event_payload = dict(first.event["payload"])
        job_payload = dict(first.job["payload"])
        for payload in (event_payload, job_payload):
            decision = dict(payload["routing_decision"])
            decision["selected_binding_id"] = forged_binding_id
            decision["eligible_candidates"] = [
                dict(c) for c in decision["eligible_candidates"]
            ]
            for c in decision["eligible_candidates"]:
                if c["agent_id"] == decision["selected_agent_id"]:
                    c["binding_id"] = forged_binding_id
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
        self.conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps(event_payload), first.event["id"]),
        )
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(job_payload), first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "selected_binding_id does not match stored binding"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_routed_replay_rejects_forged_decision_context_link(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        job_payload = dict(first.job["payload"])
        job_payload["execution_context"] = dict(job_payload["execution_context"])
        job_payload["execution_context"]["assigned_agent"] = "other"
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(job_payload), first.job["id"]),
        )
        self.conn.commit()
        before_events = len(list_events(self.conn))
        with self.assertRaisesRegex(RuntimeError, "assigned_agent does not match decision"):
            self._routed_request()
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_routed_replay_rejects_forged_decision_capabilities(self):
        """R4-1: event and job carrying the same forged selected capabilities must be rejected."""
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        event_payload = dict(first.event["payload"])
        job_payload = dict(first.job["payload"])
        for payload in (event_payload, job_payload):
            decision = dict(payload["routing_decision"])
            decision["eligible_candidates"] = [
                dict(c) for c in decision["eligible_candidates"]
            ]
            for c in decision["eligible_candidates"]:
                if c["agent_id"] == decision["selected_agent_id"]:
                    c["capabilities"] = sorted(["coding", "review"])
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
        self.conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps(event_payload), first.event["id"]),
        )
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(job_payload), first.job["id"]),
        )
        self.conn.commit()

        # Snapshot the forged state that must remain unchanged after rejection.
        before_event_count = len(list_events(self.conn))
        before_event_payload = json.loads(
            self.conn.execute(
                "SELECT payload_json FROM events WHERE id = ?", (first.event["id"],)
            ).fetchone()[0]
        )
        before_job_row = get_job(self.conn, first.job["id"])
        before_job_payload = json.loads(before_job_row["payload_json"])
        before_job_status = before_job_row["status"]
        before_job_attempts = before_job_row["attempt_count"]

        with self.assertRaisesRegex(RuntimeError, "candidate capabilities do not match stored binding"):
            self._routed_request()

        self.assertEqual(len(list_events(self.conn)), before_event_count)
        current_event_payload = json.loads(
            self.conn.execute(
                "SELECT payload_json FROM events WHERE id = ?", (first.event["id"],)
            ).fetchone()[0]
        )
        self.assertEqual(current_event_payload, before_event_payload)
        current_job_row = get_job(self.conn, first.job["id"])
        self.assertEqual(current_job_row["status"], before_job_status)
        self.assertEqual(current_job_row["attempt_count"], before_job_attempts)
        self.assertEqual(json.loads(current_job_row["payload_json"]), before_job_payload)

    def test_routed_replay_rejects_forged_overlong_candidate_capability(self):
        """R5-1: event and job carrying the same forged 65-character capability must be rejected."""
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        overlong = "a" * 65
        event_payload = dict(first.event["payload"])
        job_payload = dict(first.job["payload"])
        for payload in (event_payload, job_payload):
            decision = dict(payload["routing_decision"])
            decision["eligible_candidates"] = [
                dict(c) for c in decision["eligible_candidates"]
            ]
            for c in decision["eligible_candidates"]:
                if c["agent_id"] == decision["selected_agent_id"]:
                    # Keep required "coding" so the only illegal point is item length.
                    c["capabilities"] = [overlong, "coding"]
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
        self.conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps(event_payload), first.event["id"]),
        )
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(job_payload), first.job["id"]),
        )
        self.conn.commit()

        before_event_count = len(list_events(self.conn))
        before_event_payload = json.loads(
            self.conn.execute(
                "SELECT payload_json FROM events WHERE id = ?", (first.event["id"],)
            ).fetchone()[0]
        )
        before_job_row = get_job(self.conn, first.job["id"])
        before_job_payload = json.loads(before_job_row["payload_json"])
        before_job_status = before_job_row["status"]
        before_job_attempts = before_job_row["attempt_count"]

        with self.assertRaisesRegex(RuntimeError, "exceeds maximum item length: 64"):
            self._routed_request()

        self.assertEqual(len(list_events(self.conn)), before_event_count)
        current_event_payload = json.loads(
            self.conn.execute(
                "SELECT payload_json FROM events WHERE id = ?", (first.event["id"],)
            ).fetchone()[0]
        )
        self.assertEqual(current_event_payload, before_event_payload)
        current_job_row = get_job(self.conn, first.job["id"])
        self.assertEqual(current_job_row["status"], before_job_status)
        self.assertEqual(current_job_row["attempt_count"], before_job_attempts)
        self.assertEqual(json.loads(current_job_row["payload_json"]), before_job_payload)

    def _assert_claim_zero_mutation(self, job_id: str, original_job: dict[str, Any], original_payload: dict[str, Any], original_events: int, mutate_fn) -> None:
        mutate_fn()
        self.conn.commit()
        expected_payload = json.loads(get_job(self.conn, job_id)["payload_json"])
        with self.assertRaisesRegex(RuntimeError, "invalid routing claim evidence|executor_binding_mismatch|invalid stored routing|invalid stored execution context"):
            claim_job(self.conn, agent_id="mac-omp")
        job = get_job(self.conn, job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempt_count"], 0)
        self.assertEqual(json.loads(job["payload_json"]), expected_payload)
        self.assertEqual(len(list_events(self.conn)), original_events)
        # Restore original job state.
        self.conn.execute(
            "UPDATE jobs SET payload_json = ?, assigned_agent = ?, runner_profile_id = ?, status = ?, attempt_count = ? WHERE id = ?",
            (json.dumps(original_payload), original_job["assigned_agent"], original_job["runner_profile_id"], original_job["status"], original_job["attempt_count"], job_id),
        )
        self.conn.commit()

    def test_claim_zero_mutation_full_matrix(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        job_id = first.job["id"]
        original_job = get_job(self.conn, job_id)
        original_payload = json.loads(original_job["payload_json"])
        original_events = len(list_events(self.conn))

        def mutate_binding(key, value):
            payload = json.loads(original_job["payload_json"])
            payload["executor_binding"] = dict(payload["executor_binding"])
            payload["executor_binding"][key] = value
            self.conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), job_id))

        def mutate_context(key, value):
            payload = json.loads(original_job["payload_json"])
            payload["execution_context"] = dict(payload["execution_context"])
            payload["execution_context"][key] = value
            self.conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), job_id))

        def mutate_decision_selected_agent(value):
            payload = json.loads(original_job["payload_json"])
            decision = dict(payload["routing_decision"])
            decision["selected_agent_id"] = value
            decision["eligible_candidates"] = [dict(c) for c in decision["eligible_candidates"]]
            for c in decision["eligible_candidates"]:
                if c["agent_id"] == "mac-omp":
                    c["agent_id"] = value
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
            self.conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), job_id))

        def mutate_decision_selected_host(value):
            payload = json.loads(original_job["payload_json"])
            decision = dict(payload["routing_decision"])
            decision["selected_host_id"] = value
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
            self.conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), job_id))

        def mutate_job_assignment(value):
            self.conn.execute("UPDATE jobs SET assigned_agent = ? WHERE id = ?", (value, job_id))

        def mutate_decision_selected_binding_id(value):
            payload = json.loads(original_job["payload_json"])
            decision = dict(payload["routing_decision"])
            decision["selected_binding_id"] = value
            decision["eligible_candidates"] = [dict(c) for c in decision["eligible_candidates"]]
            for c in decision["eligible_candidates"]:
                if c["agent_id"] == decision["selected_agent_id"]:
                    c["binding_id"] = value
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
            self.conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), job_id))

        def mutate_decision_selected_runner_profile_id(value):
            payload = json.loads(original_job["payload_json"])
            decision = dict(payload["routing_decision"])
            decision["selected_runner_profile_id"] = value
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
            self.conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), job_id))

        def mutate_decision_selected_capabilities():
            payload = json.loads(original_job["payload_json"])
            decision = dict(payload["routing_decision"])
            decision["eligible_candidates"] = [dict(c) for c in decision["eligible_candidates"]]
            for c in decision["eligible_candidates"]:
                if c["agent_id"] == decision["selected_agent_id"]:
                    c["capabilities"] = sorted(["coding", "review"])
            decision = self._recompute_decision_id(decision)
            payload["routing_decision"] = decision
            self.conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), job_id))

        mutations = [
            ("executor_binding binding_id", lambda: mutate_binding("binding_id", "sha256:" + "0" * 64)),
            ("executor_binding executor_instance_id", lambda: mutate_binding("executor_instance_id", "other")),
            ("executor_binding runner_profile_id", lambda: mutate_binding("runner_profile_id", "other")),
            ("executor_binding executor_definition_id", lambda: mutate_binding("executor_definition_id", "other")),
            ("executor_binding source_id", lambda: mutate_binding("source_id", "other")),
            ("executor_binding source_version", lambda: mutate_binding("source_version", 99)),
            ("executor_binding catalog_hash", lambda: mutate_binding("catalog_hash", "0" * 64)),
            ("execution_context workspace_id", lambda: mutate_context("workspace_id", "other")),
            ("execution_context task_id", lambda: mutate_context("task_id", "t1")),
            ("execution_context job_id", lambda: mutate_context("job_id", "other")),
            ("execution_context assigned_agent", lambda: mutate_context("assigned_agent", "other")),
            ("execution_context host_id", lambda: mutate_context("host_id", "pc")),
            ("routing_decision selected_agent_id", lambda: mutate_decision_selected_agent("other")),
            ("routing_decision selected_host_id", lambda: mutate_decision_selected_host("pc")),
            ("routing_decision selected_binding_id", lambda: mutate_decision_selected_binding_id("sha256:" + "0" * 64)),
            ("routing_decision selected_runner_profile_id", lambda: mutate_decision_selected_runner_profile_id("other")),
            ("routing_decision selected_capabilities", lambda: mutate_decision_selected_capabilities()),
        ]

        for label, mutate_fn in mutations:
            with self.subTest(label=label):
                self._assert_claim_zero_mutation(job_id, original_job, original_payload, original_events, mutate_fn)

    def test_claim_rejects_forged_overlong_selected_capability(self):
        """R5-1: claim must reject a forged 65-character selected capability before CAS."""
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._routed_request()
        job_id = first.job["id"]
        overlong = "a" * 65
        payload = dict(first.job["payload"])
        decision = dict(payload["routing_decision"])
        decision["eligible_candidates"] = [dict(c) for c in decision["eligible_candidates"]]
        for c in decision["eligible_candidates"]:
            if c["agent_id"] == decision["selected_agent_id"]:
                # Keep required "coding" so the only illegal point is item length.
                c["capabilities"] = [overlong, "coding"]
        decision = self._recompute_decision_id(decision)
        payload["routing_decision"] = decision
        self.conn.execute(
            "UPDATE jobs SET payload_json = ? WHERE id = ?",
            (json.dumps(payload), job_id),
        )
        self.conn.commit()

        before_events = len(list_events(self.conn))
        before_job = get_job(self.conn, job_id)
        before_payload = json.loads(before_job["payload_json"])
        with self.assertRaisesRegex(RuntimeError, "exceeds maximum item length: 64"):
            claim_job(self.conn, agent_id="mac-omp")
        job = get_job(self.conn, job_id)
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempt_count"], 0)
        self.assertEqual(json.loads(job["payload_json"]), before_payload)
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_exact_replay_rejects_forged_event_payload_matrix(self):
        """Forged stored-event payload origin/reply/task_id must be rejected
        with zero mutation to event count, job status, attempt count, and
        stored payloads in both typed and legacy exact branches."""
        from coordinate.db import upsert_task_mirror

        cases = [
            ("typed", "origin"),
            ("typed", "reply"),
            ("typed", "task_id"),
            ("legacy", "origin"),
            ("legacy", "reply"),
            ("legacy", "task_id"),
        ]

        for branch, field in cases:
            with self.subTest(branch=branch, field=field):
                self._register("mac-omp", "mac")
                self._authorize("mac-omp", "12345")
                if branch == "typed":
                    self._sync_catalog(["mac-omp"])
                    target_agent = "mac-omp"
                else:
                    self._register("mac-codex", "mac")
                    self._authorize("mac-codex", "12346")
                    target_agent = "mac-codex"

                idempotency_key = f"exact-{branch}-{field}"
                submit_kwargs = {
                    "target_agent": target_agent,
                    "idempotency_key": idempotency_key,
                }
                if field == "task_id":
                    upsert_task_mirror(
                        self.conn,
                        workspace_id="demo",
                        task_id="t1",
                        phase="open",
                        owner="o",
                        branch=None,
                        pr=None,
                        payload={},
                    )
                    submit_kwargs["task_id"] = "t1"

                first = self._exact_request(**submit_kwargs)
                self.assertTrue(first.event_created)
                self.assertTrue(first.job_created)
                event_id = first.event["id"]
                job_id = first.job["id"]

                original_event_payload = json.loads(
                    self.conn.execute(
                        "SELECT payload_json FROM events WHERE id = ?", (event_id,)
                    ).fetchone()[0]
                )
                original_job_row = get_job(self.conn, job_id)
                original_job_payload = json.loads(original_job_row["payload_json"])
                original_event_count = len(list_events(self.conn))

                if branch == "legacy":
                    # Strip the execution_context (and any binding) so the
                    # replay enters the no-context branch, then persist it as
                    # the pre-replay expected authority.
                    no_context_job_payload = dict(original_job_payload)
                    no_context_job_payload.pop("execution_context", None)
                    no_context_job_payload.pop("executor_binding", None)
                    self.conn.execute(
                        "UPDATE jobs SET payload_json = ? WHERE id = ?",
                        (json.dumps(no_context_job_payload), job_id),
                    )
                    self.conn.commit()
                    stored_job = get_job(self.conn, job_id)
                    stored_job_payload = json.loads(stored_job["payload_json"])
                    self.assertNotIn("execution_context", stored_job_payload)
                    expected_job_payload = no_context_job_payload
                else:
                    expected_job_payload = original_job_payload

                forged_event_payload = dict(original_event_payload)
                if field == "origin":
                    forged_event_payload["origin"] = {
                        **forged_event_payload["origin"],
                        "message_id": "forged",
                    }
                    expected_message = "origin conflicts with stored event"
                elif field == "reply":
                    forged_event_payload["reply"] = {
                        **forged_event_payload["reply"],
                        "destination": "forged",
                    }
                    expected_message = "reply conflicts with stored event"
                else:  # task_id
                    forged_event_payload["task_id"] = "forged"
                    expected_message = "task_id conflicts with stored event"

                self.conn.execute(
                    "UPDATE events SET payload_json = ? WHERE id = ?",
                    (json.dumps(forged_event_payload), event_id),
                )
                self.conn.commit()

                with self.assertRaisesRegex(RuntimeError, expected_message):
                    self._exact_request(**submit_kwargs)

                self.assertEqual(len(list_events(self.conn)), original_event_count)
                job = get_job(self.conn, job_id)
                self.assertEqual(job["status"], original_job_row["status"])
                self.assertEqual(job["attempt_count"], original_job_row["attempt_count"])
                self.assertEqual(
                    json.loads(job["payload_json"]),
                    expected_job_payload,
                )
                # The replay must not restore or otherwise mutate the forged
                # stored-event payload; it must still be the forged value.
                current_event_payload = json.loads(
                    self.conn.execute(
                        "SELECT payload_json FROM events WHERE id = ?", (event_id,)
                    ).fetchone()[0]
                )
                self.assertEqual(current_event_payload, forged_event_payload)

    def test_exact_typed_replay_context_conflict_precedes_event_payload_checks(self):
        """Changing the current request origin must still surface the accepted
        execution_context conflict before the new stored-event payload checks."""
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        first = self._exact_request()
        event_id = first.event["id"]
        # Forge the stored event payload origin to a different value.
        forged_event_payload = dict(first.event["payload"])
        forged_event_payload["origin"] = {
            **forged_event_payload["origin"],
            "session_scope_id": "discord:forged",
        }
        self.conn.execute(
            "UPDATE events SET payload_json = ? WHERE id = ?",
            (json.dumps(forged_event_payload), event_id),
        )
        self.conn.commit()
        # Replay with a different origin that also changes the expected context.
        changed_origin = dict(self._origin())
        changed_origin["session_scope_id"] = "discord:other"
        with self.assertRaisesRegex(RuntimeError, "execution_context conflicts"):
            self._exact_request(origin=changed_origin)

    def test_exact_typed_legacy_compatibility(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])
        # Exact typed request uses binding snapshot.
        typed = self._exact_request()
        self.assertIn("execution_context", typed.job["payload"])
        self.assertIn("executor_binding", typed.job["payload"])
        # Exact legacy request without a typed binding should still work.
        self._register("mac-codex", "mac")
        self._authorize("mac-codex", "12346")
        # No catalog binding for mac-codex; legacy exact path.
        legacy = self._exact_request(target_agent="mac-codex")
        self.assertTrue(legacy.job_created)
        self.assertEqual(legacy.job["assigned_agent"], "mac-codex")


class LeasedProgressFailureTests(unittest.TestCase):
    """P9-3B B1: progress event append failure must roll back all writes and preserve the lease."""

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

    def _register(self, agent_id: str, host_id: str):
        register_agent(
            self.conn,
            agent_id=agent_id,
            host_id=host_id,
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id=agent_id, host_id=host_id)

    def _authorize(self, agent_name: str, discord_id: str):
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name=agent_name,
            discord_user_id=discord_id,
            actor="test",
            reason="test",
        )

    def _sync_capacity(self, agent_ids: list[str]):
        from coordinate.executor_capacity import (
            CapacityCatalog,
            CapacityPolicy,
            compute_capacity_catalog_hash,
        )

        policies = tuple(
            CapacityPolicy(agent_id=aid, max_concurrent_jobs=2) for aid in agent_ids
        )
        catalog = CapacityCatalog(
            source_id="multinexus.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path="/dev/null",
            policies=policies,
        )
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_capacity_catalog_hash(catalog)
        )
        sync_capacity_catalog(self.conn, catalog)

    def _sync_catalog(self, agent_ids: list[str]):
        from coordinate.executor_identity import (
            ExecutorCatalog,
            ExecutorDefinition,
            ExecutorInstanceBinding,
            compute_executor_catalog_hash,
        )

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
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_executor_catalog_hash(catalog)
        )
        sync_executor_catalog(self.conn, catalog)
        self._sync_capacity(agent_ids)

    def test_progress_event_append_failure_zero_write_preserves_lease(self):
        """If append_event fails inside record_job_progress, the job UPDATE and any partial state must roll back and the active lease must remain active."""
        self._register("mac-omp", "mac")
        self._authorize("mac-omp", "12345")
        self._sync_catalog(["mac-omp"])

        origin = {
            "platform": "discord",
            "destination": "ch",
            "message_id": "m1",
            "session_scope_id": "discord:ch",
        }
        reply = {"platform": "discord", "destination": "ch"}
        request = submit_request(
            self.conn,
            workspace_id="demo",
            prompt="hello",
            origin=origin,
            reply=reply,
            routing_request=build_routing_request(required_capabilities=["coding"]),
        )
        self.assertTrue(request.job_created)

        claim = claim_job(self.conn, agent_id="mac-omp")
        self.assertTrue(claim.claimed)
        job_id = claim.job["id"]
        attempt_token = claim.attempt_token
        lease_id = claim.execution_lease["lease_id"]
        self.assertEqual(claim.execution_lease["attempt_token"], attempt_token)

        before_job = row_to_dict(get_job(self.conn, job_id))
        before_event_count = len(list(list_events(self.conn, "demo")))
        before_lease = row_to_dict(
            self.conn.execute(
                "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
        )
        self.assertEqual(before_lease["status"], "active")

        from coordinate import runtime as runtime_module

        original_append_event = runtime_module.append_event

        def failing_append_event(*args, **kwargs):
            raise RuntimeError("simulated event append failure")

        runtime_module.append_event = failing_append_event
        try:
            with self.assertRaisesRegex(RuntimeError, "simulated event append failure"):
                record_job_progress(
                    self.conn,
                    job_id=job_id,
                    agent_id="mac-omp",
                    stage="editing",
                    summary="must not persist",
                    session_id="sess-boom",
                    attempt_token=attempt_token,
                    lease_id=lease_id,
                )
        finally:
            runtime_module.append_event = original_append_event

        after_job = row_to_dict(get_job(self.conn, job_id))
        after_event_count = len(list(list_events(self.conn, "demo")))
        after_lease = row_to_dict(
            self.conn.execute(
                "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
        )

        # Full persistent job snapshot must be unchanged (transaction rolled back).
        self.assertEqual(after_job, before_job)
        # Explicit guard on fields a successful progress write would mutate.
        self.assertEqual(after_job["progress_json"], before_job["progress_json"])
        self.assertEqual(
            after_job["terminal_session_id"], before_job["terminal_session_id"]
        )
        self.assertEqual(after_job["updated_at"], before_job["updated_at"])
        self.assertEqual(after_job["last_activity_at"], before_job["last_activity_at"])
        self.assertEqual(after_event_count, before_event_count)
        self.assertEqual(after_lease["status"], "active")


class RuntimeAgentDeactivateTests(unittest.TestCase):
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
        register_agent(
            self.conn, agent_id="mac-agent", host_id="mac", capabilities={}
        )

    def _request(self):
        return submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-agent",
            prompt="work",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "deactivate",
                "session_scope_id": "discord:ch",
            },
            reply={"platform": "discord", "destination": "ch"},
        )

    def _deactivate(self, **overrides):
        params = {
            "agent_id": "mac-agent",
            "host_id": "mac",
            "reason": "planned maintenance",
            "actor": "operator",
        }
        params.update(overrides)
        return deactivate_agent(self.conn, **params)

    def test_success_retry_and_audit_chain(self):
        result = self._deactivate()
        self.assertTrue(result.changed)
        self.assertTrue(result.deactivated)
        self.assertEqual(result.reason, "planned maintenance")
        self.assertEqual(result.agent["online_state"], "offline")
        self.assertEqual(result.event["payload"]["generation"], 1)
        self.assertIsNone(
            result.event["payload"]["previous_deactivation_event_id"]
        )
        self.assertTrue(result.event_created)

        retry = self._deactivate(reason="retry reason is not a new audit")
        self.assertFalse(retry.changed)
        self.assertTrue(retry.deactivated)
        self.assertEqual(retry.reason, "planned maintenance")
        self.assertEqual(retry.event["id"], result.event["id"])
        self.assertFalse(retry.event_created)
        count = self.conn.execute(
            "SELECT COUNT(*) AS n FROM events "
            "WHERE event_type = 'agent.deactivated' AND target = 'mac-agent'"
        ).fetchone()["n"]
        self.assertEqual(count, 1)

    def test_dry_run_is_zero_mutation(self):
        before_agent = row_to_dict(
            self.conn.execute(
                "SELECT * FROM agents WHERE id = 'mac-agent'"
            ).fetchone()
        )
        before_events = len(list_events(self.conn))
        result = self._deactivate(dry_run=True)
        self.assertFalse(result.changed)
        self.assertFalse(result.deactivated)
        self.assertTrue(result.dry_run)
        self.assertFalse(result.blocked)
        self.assertEqual(result.reason, "planned maintenance")
        self.assertEqual(
            row_to_dict(
                self.conn.execute(
                    "SELECT * FROM agents WHERE id = 'mac-agent'"
                ).fetchone()
            ),
            before_agent,
        )
        self.assertEqual(len(list_events(self.conn)), before_events)

    def test_pending_running_and_recoverable_blockers_are_bounded(self):
        request = self._request()
        pending = self._deactivate()
        self.assertTrue(pending.blocked)
        self.assertEqual(pending.blockers["pending_jobs"]["count"], 1)
        self.assertEqual(
            pending.blockers["pending_jobs"]["first_job_id"], request.job["id"]
        )
        self.assertEqual(pending.agent["online_state"], "online")

        claim_job(self.conn, agent_id="mac-agent")
        running = self._deactivate()
        self.assertTrue(running.blocked)
        self.assertEqual(running.blockers["running_jobs"]["count"], 1)

        self.conn.execute(
            "UPDATE jobs SET status = 'timed_out', recoverable = 1 "
            "WHERE id = ?",
            (request.job["id"],),
        )
        self.conn.commit()
        recoverable = self._deactivate()
        self.assertTrue(recoverable.blocked)
        self.assertEqual(
            recoverable.blockers["recoverable_timed_out_jobs"]["count"], 1
        )
        self.assertEqual(
            set(recoverable.blockers),
            {
                "active_leases",
                "pending_jobs",
                "running_jobs",
                "recoverable_timed_out_jobs",
            },
        )

    def test_active_lease_is_a_blocker_even_when_due(self):
        definitions = (
            ExecutorDefinition(
                id="coder",
                provider="test",
                adapter="omp",
                capabilities=("coding",),
            ),
        )
        bindings = (
            ExecutorInstanceBinding(
                agent_id="mac-agent",
                executor_definition_id="coder",
                runner_profile_id="mac-agent",
                enabled=True,
            ),
        )
        catalog = ExecutorCatalog(
            source_id="test",
            source_version=1,
            catalog_hash="",
            source_path="/dev/null",
            definitions=definitions,
            bindings=bindings,
        )
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_executor_catalog_hash(catalog)
        )
        sync_executor_catalog(self.conn, catalog)
        capacity = CapacityCatalog(
            source_id="test.capacity",
            source_version=1,
            catalog_hash="",
            source_path="/dev/null",
            policies=(CapacityPolicy(agent_id="mac-agent", max_concurrent_jobs=1),),
        )
        capacity = dataclasses.replace(
            capacity, catalog_hash=compute_capacity_catalog_hash(capacity)
        )
        sync_capacity_catalog(self.conn, capacity)
        request = self._request()
        claim = claim_job(self.conn, agent_id="mac-agent")
        lease_id = claim.execution_lease["lease_id"]
        self.conn.execute(
            "UPDATE execution_attempt_leases "
            "SET acquired_at = '2020-01-01T00:00:00Z', "
            "renewed_at = '2020-01-01T00:00:00Z', "
            "expires_at = '2020-01-01T00:00:01Z' WHERE lease_id = ?",
            (lease_id,),
        )
        self.conn.commit()
        result = self._deactivate()
        self.assertTrue(result.blocked)
        self.assertEqual(result.blockers["active_leases"]["count"], 1)
        self.assertEqual(result.blockers["active_leases"]["first_lease_id"], lease_id)
        self.assertEqual(result.blockers["running_jobs"]["first_job_id"], request.job["id"])

    def test_identity_state_and_input_failures_are_zero_mutation(self):
        before = row_to_dict(
            self.conn.execute(
                "SELECT * FROM agents WHERE id = 'mac-agent'"
            ).fetchone()
        )
        cases = (
            {"agent_id": "missing"},
            {"host_id": "other"},
            {"reason": " "},
            {"actor": "bad\x00actor"},
            {"dry_run": "yes"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self._deactivate(**overrides)
            self.assertFalse(self.conn.in_transaction)
            self.assertEqual(
                row_to_dict(
                    self.conn.execute(
                        "SELECT * FROM agents WHERE id = 'mac-agent'"
                    ).fetchone()
                ),
                before,
            )

        self.conn.execute(
            "UPDATE agents SET client_type = 'bridge' WHERE id = 'mac-agent'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeError, "agentd"):
            self._deactivate()
        self.conn.execute(
            "UPDATE agents SET client_type = 'agentd', online_state = 'unknown' "
            "WHERE id = 'mac-agent'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeError, "unknown online_state"):
            self._deactivate()

    def test_event_failure_rolls_back_agent(self):
        with patch(
            "coordinate.runtime.append_event",
            side_effect=RuntimeError("simulated append failure"),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated append failure"):
                self._deactivate()
        agent = self.conn.execute(
            "SELECT online_state FROM agents WHERE id = 'mac-agent'"
        ).fetchone()
        self.assertEqual(agent["online_state"], "online")
        self.assertFalse(self.conn.in_transaction)

    def test_offline_without_valid_audit_fails_closed(self):
        self.conn.execute(
            "UPDATE agents SET online_state = 'offline' WHERE id = 'mac-agent'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(RuntimeError, "offline_without_deactivation_audit"):
            self._deactivate()
        self.assertFalse(self.conn.in_transaction)

    def test_offline_retry_rejects_corrupt_audit_evidence(self):
        first = self._deactivate()
        event_id = first.event["id"]
        row = self.conn.execute(
            "SELECT actor, idempotency_key, payload_json FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        original_payload = json.loads(row["payload_json"])
        cases = (
            ("actor", "UPDATE events SET actor = 'forged' WHERE id = ?", None),
            (
                "idempotency_key",
                "UPDATE events SET idempotency_key = 'forged' WHERE id = ?",
                None,
            ),
            ("previous_state", None, {"previous_online_state": "offline"}),
            ("generation", None, {"generation": 2}),
            (
                "previous_event_id",
                None,
                {"previous_deactivation_event_id": "forged"},
            ),
            ("timestamp", None, {"deactivated_at": "not-a-time"}),
            ("extra_payload_key", None, {"unexpected": True}),
        )
        for label, statement, payload_delta in cases:
            with self.subTest(label=label):
                self.conn.execute(
                    "UPDATE events SET actor = ?, idempotency_key = ?, payload_json = ? "
                    "WHERE id = ?",
                    (
                        row["actor"],
                        row["idempotency_key"],
                        json.dumps(original_payload, sort_keys=True),
                        event_id,
                    ),
                )
                if statement is not None:
                    self.conn.execute(statement, (event_id,))
                else:
                    forged_payload = {**original_payload, **payload_delta}
                    self.conn.execute(
                        "UPDATE events SET payload_json = ? WHERE id = ?",
                        (json.dumps(forged_payload, sort_keys=True), event_id),
                    )
                self.conn.commit()
                with self.assertRaisesRegex(RuntimeError, "invalid|stored"):
                    self._deactivate()
                self.assertFalse(self.conn.in_transaction)

        self.conn.execute(
            "UPDATE events SET actor = ?, idempotency_key = ?, payload_json = ? "
            "WHERE id = ?",
            (
                row["actor"],
                row["idempotency_key"],
                json.dumps(original_payload, sort_keys=True),
                event_id,
            ),
        )
        self.conn.commit()
        retry = self._deactivate()
        self.assertFalse(retry.changed)
        self.assertEqual(retry.event["id"], event_id)

    def test_same_second_reactivation_creates_next_generation(self):
        fixed = "2026-07-16T00:00:00Z"
        with patch("coordinate.runtime.utc_now", return_value=fixed):
            first = self._deactivate()
            heartbeat_agent(
                self.conn,
                agent_id="mac-agent",
                host_id="mac",
                actor="runtime",
            )
            second = self._deactivate(reason="second cycle")
        self.assertEqual(first.event["payload"]["generation"], 1)
        self.assertEqual(second.event["payload"]["generation"], 2)
        self.assertEqual(
            second.event["payload"]["previous_deactivation_event_id"],
            first.event["id"],
        )
        self.assertNotEqual(first.event["idempotency_key"], second.event["idempotency_key"])


if __name__ == "__main__":
    unittest.main()
