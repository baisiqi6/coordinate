"""Runtime integration tests: usage evidence ledger + warning on terminal reports (issue #12)."""
from __future__ import annotations

import json
import tempfile
import unittest

from coordinate.db import (
    get_job,
    initialize,
    list_events,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.runtime import (
    RuntimeError,
    claim_job,
    register_agent,
    report_job_result,
    submit_request,
)
from coordinate.usage_evidence import UsageEvidenceError, parse_usage_evidence
from coordinate.usage_policy import (
    UsagePolicyError,
    build_usage_status,
    set_task_usage_warning_policy,
)


def evidence_block(**overrides):
    value = {
        "contract_version": 1,
        "records": [
            {
                "provider": "qoder",
                "model": "lite",
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_tokens": 10,
                "cache_write_tokens": 0,
                "provider_cost_microusd": 1234,
                "source": "provider_reported",
                "completeness": "complete",
            }
        ],
    }
    value.update(overrides)
    return value


class UsageRuntimeTests(unittest.TestCase):
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
        upsert_task_mirror(
            self.conn,
            workspace_id="demo",
            task_id="task-1",
            phase="active",
            owner="mac-codex",
            branch=None,
            pr=None,
            payload={},
        )

    def register_codex(self):
        return register_agent(
            self.conn,
            agent_id="mac-codex",
            host_id="mac",
            capabilities={"models": ["codex"]},
        )

    def submit(self, task_id="task-1"):
        return submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m1", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
            task_id=task_id,
        )

    def _usage_rows(self, job_id=None):
        if job_id is None:
            return self.conn.execute("SELECT * FROM job_attempt_usage ORDER BY job_id, attempt_token").fetchall()
        return self.conn.execute(
            "SELECT * FROM job_attempt_usage WHERE job_id = ? ORDER BY attempt_token", (job_id,)
        ).fetchall()

    def _warning_events(self):
        return [
            row_to_dict(row)
            for row in self.conn.execute(
                "SELECT * FROM events WHERE event_type = 'usage.warning' ORDER BY rowid"
            ).fetchall()
        ]

    def _report_done(self, job_id, result, **kwargs):
        return report_job_result(
            self.conn,
            job_id=job_id,
            agent_id="mac-codex",
            status="done",
            result=result,
            **kwargs,
        )

    # -- accepted terminal writes exactly one usage row --

    def test_accepted_terminal_writes_usage_row_and_bounded_summary(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")

        outcome = self._report_done(
            request.job["id"], {"response_text": "ok", "usage_evidence": evidence_block()}
        )
        self.assertTrue(outcome.event_created)

        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["job_id"], request.job["id"])
        self.assertEqual(row["attempt_token"], 1)
        self.assertEqual(row["workspace_id"], "demo")
        self.assertEqual(row["task_id"], "task-1")
        self.assertEqual(row["observed_tokens"], 160)
        self.assertEqual(row["provider_cost_microusd"], 1234)
        self.assertEqual(row["completeness"], "complete")
        self.assertIsNotNone(row["terminal_event_id"])
        self.assertEqual(row["event_created"], 1)

        canonical = json.loads(row["evidence_json"])
        self.assertEqual(parse_usage_evidence(canonical).digest, row["evidence_digest"])
        self.assertEqual(row["evidence_digest"], outcome.job["result"]["usage_evidence"]["digest"])

        # result_json and the terminal event carry only the bounded summary.
        summary = outcome.job["result"]["usage_evidence"]
        self.assertNotIn("input_tokens", summary)
        self.assertNotIn("records", [summary])
        event = outcome.event
        self.assertIn("usage_evidence", event["payload"])
        self.assertNotIn("input_tokens", event["payload"]["usage_evidence"])

    def test_exact_replay_does_not_duplicate_usage(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        body = {"response_text": "ok", "usage_evidence": evidence_block()}
        self._report_done(request.job["id"], body)
        replay = self._report_done(request.job["id"], body)

        self.assertEqual(replay.event["event_type"], "job.result_replayed")
        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(self._warning_events()), 0)

    def test_conflicting_replay_does_not_change_usage(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        self._report_done(
            request.job["id"], {"response_text": "ok", "usage_evidence": evidence_block()}
        )
        replay = self._report_done(
            request.job["id"],
            {
                "response_text": "different body",
                "usage_evidence": evidence_block(
                    records=[
                        {
                            "provider": "grok",
                            "model": None,
                            "input_tokens": 999,
                            "output_tokens": 999,
                            "cache_read_tokens": 999,
                            "cache_write_tokens": 999,
                            "provider_cost_microusd": 777,
                            "source": "provider_reported",
                            "completeness": "complete",
                        }
                    ]
                ),
            },
        )
        self.assertEqual(replay.event["event_type"], "job.result_replayed")
        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["observed_tokens"], 160)
        self.assertEqual(rows[0]["provider_cost_microusd"], 1234)

    # -- invalid evidence fails closed --

    def test_invalid_evidence_fails_closed_atomically(self):
        self.register_codex()
        request = self.submit()
        claimed = claim_job(self.conn, agent_id="mac-codex")

        before_events = len(list_events(self.conn, "demo"))
        with self.assertRaises(UsageEvidenceError):
            self._report_done(
                request.job["id"],
                {"response_text": "bad", "usage_evidence": {"contract_version": 1, "records": [{"provider": "qoder", "model": "lite"}]}},
            )
        # Job still running, no events, no usage row, no lease release.
        job = get_job(self.conn, request.job["id"])
        self.assertEqual(job["status"], "running")
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertEqual(len(self._usage_rows(request.job["id"])), 0)
        lease = self.conn.execute(
            "SELECT * FROM execution_attempt_leases WHERE job_id = ?", (request.job["id"],)
        ).fetchone()
        self.assertIsNone(lease)
        self.assertEqual(claimed.job["status"], "running")

    def test_invalid_evidence_on_replay_fails_closed(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        self._report_done(request.job["id"], {"response_text": "ok"})
        before_events = len(list_events(self.conn, "demo"))
        with self.assertRaises(UsageEvidenceError):
            self._report_done(
                request.job["id"],
                {"response_text": "ok", "usage_evidence": {"contract_version": 2, "records": [{}]}},
            )
        self.assertEqual(len(list_events(self.conn, "demo")), before_events)
        self.assertEqual(len(self._usage_rows(request.job["id"])), 0)

    # -- reclaim: one row per attempt --

    def test_timed_out_then_reclaim_then_done_two_attempt_rows(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "t", "usage_evidence": evidence_block()},
        )
        reclaimed = claim_job(self.conn, agent_id="mac-codex", recoverable=True)
        self.assertEqual(reclaimed.attempt_token, 2)
        self._report_done(
            request.job["id"],
            {"response_text": "done", "usage_evidence": evidence_block(records=[{
                "provider": "grok",
                "model": None,
                "input_tokens": 1,
                "output_tokens": 1,
                "cache_read_tokens": 1,
                "cache_write_tokens": 1,
                "provider_cost_microusd": 5,
                "source": "provider_reported",
                "completeness": "complete",
            }])},
            attempt_token=2,
        )
        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 2)
        self.assertEqual([r["attempt_token"] for r in rows], [1, 2])
        self.assertEqual(rows[0]["observed_tokens"], 160)
        self.assertEqual(rows[1]["observed_tokens"], 4)

    def test_two_consecutive_timed_out_attempts_have_independent_rows(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "t1", "usage_evidence": evidence_block()},
        )
        claim_job(self.conn, agent_id="mac-codex", recoverable=True)
        second = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "t2", "usage_evidence": evidence_block(records=[{
                "provider": "zcode",
                "model": None,
                "input_tokens": 2,
                "output_tokens": 2,
                "cache_read_tokens": 2,
                "cache_write_tokens": 2,
                "provider_cost_microusd": 3,
                "source": "provider_reported",
                "completeness": "complete",
            }])},
        )
        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 2)
        # Both attempts share the same job.timed_out idempotency key: the
        # second terminal event is a replay of the first, so its locator is the
        # OLD event and event_created=0 — rows must stay independent anyway.
        self.assertEqual(rows[0]["terminal_event_id"], rows[1]["terminal_event_id"])
        self.assertEqual(rows[0]["event_created"], 1)
        self.assertEqual(rows[1]["event_created"], 0)
        self.assertEqual(rows[0]["observed_tokens"], 160)
        self.assertEqual(rows[1]["observed_tokens"], 8)

    # -- late result --

    def test_late_result_does_not_duplicate_usage_row(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "t", "usage_evidence": evidence_block()},
        )
        late = report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "late", "usage_evidence": evidence_block(records=[{
                "provider": "zcode",
                "model": None,
                "input_tokens": 5,
                "output_tokens": 5,
                "cache_read_tokens": 5,
                "cache_write_tokens": 5,
                "provider_cost_microusd": 3,
                "source": "provider_reported",
                "completeness": "complete",
            }])},
        )
        self.assertEqual(late.event["event_type"], "job.late_result_accepted")
        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 1)
        # First accepted evidence wins (the timed_out report's).
        self.assertEqual(rows[0]["observed_tokens"], 160)
        self.assertEqual(
            late.job["result"]["usage_evidence"]["digest"],
            rows[0]["evidence_digest"],
        )
        self.assertEqual(
            late.event["payload"]["usage_evidence"]["digest"],
            rows[0]["evidence_digest"],
        )

    def test_late_result_fills_row_when_timeout_had_no_evidence(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "t"},
        )
        late = self._report_done(
            request.job["id"], {"response_text": "late", "usage_evidence": evidence_block()}
        )
        self.assertEqual(late.event["event_type"], "job.late_result_accepted")
        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["observed_tokens"], 160)

    # -- task_id null --

    def test_task_id_null_evidence_persisted_but_excluded_from_policy(self):
        self.register_codex()
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m-null", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
        )
        claim_job(self.conn, agent_id="mac-codex")
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1", revision=1,
            observed_tokens_threshold=1,
        )
        self._report_done(
            request.job["id"], {"response_text": "ok", "usage_evidence": evidence_block()}
        )
        rows = self._usage_rows(request.job["id"])
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["task_id"])
        self.assertEqual(len(self._warning_events()), 0)
        status = build_usage_status(self.conn, workspace_id="demo", task_id="task-1")
        self.assertEqual(status["aggregate"]["attempt_count"], 0)

    # -- cost aggregation --

    def test_aggregate_cost_null_when_any_record_cost_null(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        outcome = self._report_done(
            request.job["id"],
            {
                "response_text": "ok",
                "usage_evidence": evidence_block(
                    records=[
                        {
                            "provider": "qoder",
                            "model": "lite",
                            "input_tokens": 100,
                            "output_tokens": 50,
                            "cache_read_tokens": 10,
                            "cache_write_tokens": 0,
                            "provider_cost_microusd": 1234,
                            "source": "provider_reported",
                            "completeness": "complete",
                        },
                        {
                            "provider": "grok",
                            "model": None,
                            "input_tokens": 1,
                            "output_tokens": 1,
                            "cache_read_tokens": 1,
                            "cache_write_tokens": 1,
                            "provider_cost_microusd": None,
                            "source": "provider_reported",
                            "completeness": "partial",
                        },
                    ]
                ),
            },
        )
        self.assertIsNone(outcome.job["result"]["usage_evidence"]["provider_cost_microusd"])
        self.assertEqual(outcome.job["result"]["usage_evidence"]["observed_tokens"], 164)
        self.assertEqual(outcome.job["result"]["usage_evidence"]["completeness"], "partial")

    # -- missing evidence keeps old behavior --

    def test_missing_evidence_writes_no_usage_row(self):
        self.register_codex()
        request = self.submit()
        claim_job(self.conn, agent_id="mac-codex")
        outcome = self._report_done(request.job["id"], {"response_text": "ok"})
        self.assertTrue(outcome.event_created)
        self.assertEqual(len(self._usage_rows(request.job["id"])), 0)
        self.assertNotIn("usage_evidence", outcome.job["result"])


if __name__ == "__main__":
    unittest.main()
