"""Policy + warning tests: revision CAS, thresholds, causation, status (issue #12)."""
from __future__ import annotations

import tempfile
import unittest

from coordinate.db import (
    initialize,
    list_events,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.runtime import claim_job, register_agent, report_job_result, submit_request
from coordinate.usage_policy import (
    UsagePolicyError,
    build_usage_status,
    evaluate_task_usage_warning,
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


class PolicySetTests(unittest.TestCase):
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

    def test_initial_set_creates_policy(self):
        policy = set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=1000,
        )
        self.assertEqual(policy["revision"], 1)
        self.assertEqual(policy["observed_tokens_threshold"], 1000)
        self.assertTrue(policy["enabled"])
        self.assertEqual(policy["created_at"], policy["updated_at"])

    def test_revision_must_increase(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=2, observed_tokens_threshold=100,
        )
        with self.assertRaisesRegex(UsagePolicyError, "strictly increase"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="demo", task_id="task-1",
                revision=1, observed_tokens_threshold=100,
            )
        with self.assertRaisesRegex(UsagePolicyError, "positive integer"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="demo", task_id="task-1",
                revision=0, observed_tokens_threshold=100,
            )

    def test_same_revision_same_body_idempotent(self):
        first = set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        second = set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        self.assertEqual(first["revision"], second["revision"])
        self.assertEqual(first["updated_at"], second["updated_at"])

    def test_same_revision_conflicting_body_fails_closed(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        with self.assertRaisesRegex(UsagePolicyError, "conflicting body"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="demo", task_id="task-1",
                revision=1, observed_tokens_threshold=200,
            )
        with self.assertRaisesRegex(UsagePolicyError, "conflicting body"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="demo", task_id="task-1",
                revision=1, observed_tokens_threshold=100, enabled=False,
            )

    def test_disable_and_reenable(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        disabled = set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=2, observed_tokens_threshold=100, enabled=False,
        )
        self.assertFalse(disabled["enabled"])
        reenabled = set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=3, observed_tokens_threshold=100, enabled=True,
        )
        self.assertTrue(reenabled["enabled"])

    def test_unknown_workspace_fails_closed(self):
        with self.assertRaisesRegex(UsagePolicyError, "unknown workspace"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="nope", task_id="task-1",
                revision=1, observed_tokens_threshold=100,
            )

    def test_invalid_inputs(self):
        with self.assertRaisesRegex(UsagePolicyError, "positive integer"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="demo", task_id="task-1",
                revision=0, observed_tokens_threshold=100,
            )
        with self.assertRaisesRegex(UsagePolicyError, "positive integer"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="demo", task_id="task-1",
                revision=1, observed_tokens_threshold=0,
            )
        with self.assertRaisesRegex(UsagePolicyError, "task_id"):
            set_task_usage_warning_policy(
                self.conn, workspace_id="demo", task_id="",
                revision=1, observed_tokens_threshold=100,
            )

    def test_task_scope_isolation(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        other = set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-2",
            revision=1, observed_tokens_threshold=50,
        )
        self.assertEqual(other["observed_tokens_threshold"], 50)


class WarningEvaluationTests(unittest.TestCase):
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
        register_agent(self.conn, agent_id="mac-codex", host_id="mac")

    def _run_attempt(self, tokens=160, message_id=None):
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin={"platform": "discord", "destination": "channel-1", "message_id": message_id or f"m-{tokens}", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
            task_id="task-1",
        )
        claim_job(self.conn, agent_id="mac-codex")
        return report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={
                "response_text": "ok",
                "usage_evidence": evidence_block(
                    records=[
                        {
                            "provider": "qoder",
                            "model": "lite",
                            "input_tokens": tokens,
                            "output_tokens": 0,
                            "cache_read_tokens": 0,
                            "cache_write_tokens": 0,
                            "provider_cost_microusd": 1,
                            "source": "provider_reported",
                            "completeness": "complete",
                        }
                    ]
                ),
            },
        )

    def _warnings(self):
        return [
            row_to_dict(row)
            for row in self.conn.execute(
                "SELECT * FROM events WHERE event_type = 'usage.warning' ORDER BY rowid"
            ).fetchall()
        ]

    def test_below_threshold_no_warning(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=200,
        )
        self._run_attempt(tokens=160)
        self.assertEqual(len(self._warnings()), 0)

    def test_equal_threshold_warns_once(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=160,
        )
        self._run_attempt(tokens=160)
        warnings = self._warnings()
        self.assertEqual(len(warnings), 1)
        payload = warnings[0]["payload"]
        self.assertEqual(payload["revision"], 1)
        self.assertEqual(payload["observed_tokens_threshold"], 160)
        self.assertEqual(payload["observed_tokens"], 160)
        self.assertEqual(payload["completeness"], "complete")
        self.assertEqual(payload["scope"], {"workspace_id": "demo", "task_id": "task-1"})
        self.assertEqual(payload["attempt_token"], 1)
        # Causation points at the real terminal event.
        terminal = [
            row_to_dict(row)
            for row in self.conn.execute(
                "SELECT * FROM events WHERE event_type = 'job.completed'"
            ).fetchall()
        ]
        self.assertEqual(len(terminal), 1)
        self.assertEqual(warnings[0]["causation_id"], terminal[0]["id"])

    def test_above_threshold_does_not_repeat_warning(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        self._run_attempt(tokens=160)
        self._run_attempt(tokens=300)
        self.assertEqual(len(self._warnings()), 1)

    def test_new_revision_warns_again(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        self._run_attempt(tokens=160, message_id="w1")
        self.assertEqual(len(self._warnings()), 1)
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=2, observed_tokens_threshold=100,
        )
        self._run_attempt(tokens=160, message_id="w2")
        warnings = self._warnings()
        self.assertEqual(len(warnings), 2)
        self.assertEqual([w["payload"]["revision"] for w in warnings], [1, 2])

    def test_scope_delimiters_cannot_collide_warning_keys(self):
        upsert_workspace(
            self.conn,
            workspace_id="demo:child",
            name="Demo Child",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        set_task_usage_warning_policy(
            self.conn,
            workspace_id="demo",
            task_id="child:task-1",
            revision=1,
            observed_tokens_threshold=1,
        )
        set_task_usage_warning_policy(
            self.conn,
            workspace_id="demo:child",
            task_id="task-1",
            revision=1,
            observed_tokens_threshold=1,
        )
        first = evaluate_task_usage_warning(
            self.conn,
            workspace_id="demo",
            task_id="child:task-1",
            job_id="job-1",
            attempt_token=1,
            observed_tokens=1,
            completeness="complete",
            terminal_event_id=None,
            terminal_event_created=False,
            commit=True,
        )
        second = evaluate_task_usage_warning(
            self.conn,
            workspace_id="demo:child",
            task_id="task-1",
            job_id="job-2",
            attempt_token=1,
            observed_tokens=1,
            completeness="complete",
            terminal_event_id=None,
            terminal_event_created=False,
            commit=True,
        )
        self.assertNotEqual(first["id"], second["id"])

    def test_disabled_policy_stops_new_warnings_but_keeps_history(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        self._run_attempt(tokens=160)
        self.assertEqual(len(self._warnings()), 1)
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=2, observed_tokens_threshold=100, enabled=False,
        )
        self._run_attempt(tokens=999)
        # No new warning, historical event retained.
        self.assertEqual(len(self._warnings()), 1)

    def test_repeat_timed_out_causation_is_null(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="long",
            origin={"platform": "discord", "destination": "channel-1", "message_id": "m2", "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
            task_id="task-1",
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "t1", "usage_evidence": evidence_block(records=[{
                "provider": "qoder", "model": "lite",
                "input_tokens": 500, "output_tokens": 0, "cache_read_tokens": 0,
                "cache_write_tokens": 0, "provider_cost_microusd": 1,
                "source": "provider_reported", "completeness": "complete",
            }])},
        )
        warnings = self._warnings()
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["payload"]["attempt_token"], 1)
        self.assertIsNotNone(warnings[0]["causation_id"])

        # Attempt 2 timed_out reuses the old job.timed_out event → causation null.
        claim_job(self.conn, agent_id="mac-codex", recoverable=True)
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="timed_out",
            result={"response_text": "t2", "usage_evidence": evidence_block(records=[{
                "provider": "grok", "model": None,
                "input_tokens": 600, "output_tokens": 0, "cache_read_tokens": 0,
                "cache_write_tokens": 0, "provider_cost_microusd": 1,
                "source": "provider_reported", "completeness": "complete",
            }])},
        )
        # Same revision: still exactly one warning, causation unchanged.
        warnings = self._warnings()
        self.assertEqual(len(warnings), 1)

    def test_warning_policy_set_itself_never_evaluates(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=1,
        )
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=2, observed_tokens_threshold=1,
        )
        self.assertEqual(len(self._warnings()), 0)


class UsageStatusTests(unittest.TestCase):
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
        register_agent(self.conn, agent_id="mac-codex", host_id="mac")

    def _run(self, records, message_id):
        request = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin={"platform": "discord", "destination": "channel-1", "message_id": message_id, "session_scope_id": "discord:test"},
            reply={"platform": "discord", "destination": "channel-1"},
            task_id="task-1",
        )
        claim_job(self.conn, agent_id="mac-codex")
        report_job_result(
            self.conn,
            job_id=request.job["id"],
            agent_id="mac-codex",
            status="done",
            result={"response_text": "ok", "usage_evidence": {"contract_version": 1, "records": records}},
        )

    def test_status_aggregate_and_ledger(self):
        set_task_usage_warning_policy(
            self.conn, workspace_id="demo", task_id="task-1",
            revision=1, observed_tokens_threshold=100,
        )
        self._run(
            [{
                "provider": "qoder", "model": "lite",
                "input_tokens": 100, "output_tokens": 50, "cache_read_tokens": 10,
                "cache_write_tokens": 0, "provider_cost_microusd": 100,
                "source": "provider_reported", "completeness": "complete",
            }],
            "m1",
        )
        self._run(
            [{
                "provider": "grok", "model": None,
                "input_tokens": 1, "output_tokens": 1, "cache_read_tokens": 1,
                "cache_write_tokens": 1, "provider_cost_microusd": 5,
                "source": "provider_reported", "completeness": "complete",
            }],
            "m2",
        )
        status = build_usage_status(self.conn, workspace_id="demo", task_id="task-1")
        self.assertEqual(status["policy"]["revision"], 1)
        self.assertEqual(status["aggregate"]["attempt_count"], 2)
        self.assertEqual(status["aggregate"]["observed_tokens"], 164)
        self.assertEqual(status["aggregate"]["provider_cost_microusd"], 105)
        self.assertEqual(len(status["attempts"]), 2)
        self.assertEqual([a["attempt_token"] for a in status["attempts"]], [1, 1])
        self.assertEqual(len(status["warnings"]), 1)
        self.assertEqual(status["warnings"][0]["observed_tokens"], 160)

    def test_status_cost_aggregate_null_when_any_attempt_cost_null(self):
        self._run(
            [{
                "provider": "qoder", "model": "lite",
                "input_tokens": 100, "output_tokens": 50, "cache_read_tokens": 10,
                "cache_write_tokens": 0, "provider_cost_microusd": None,
                "source": "provider_reported", "completeness": "partial",
            }],
            "m1",
        )
        status = build_usage_status(self.conn, workspace_id="demo", task_id="task-1")
        self.assertIsNone(status["aggregate"]["provider_cost_microusd"])
        self.assertEqual(status["aggregate"]["observed_tokens"], 160)

    def test_status_all_unknown_attempts_do_not_become_zero(self):
        self._run(
            [{
                "provider": "zcode", "model": None,
                "input_tokens": None, "output_tokens": None,
                "cache_read_tokens": None, "cache_write_tokens": None,
                "provider_cost_microusd": None,
                "source": "unknown", "completeness": "unknown",
            }],
            "unknown-1",
        )
        status = build_usage_status(self.conn, workspace_id="demo", task_id="task-1")
        self.assertIsNone(status["aggregate"]["observed_tokens"])

    def test_status_unknown_workspace_fails_closed(self):
        with self.assertRaisesRegex(UsagePolicyError, "unknown workspace"):
            build_usage_status(self.conn, workspace_id="nope", task_id="task-1")

    def test_status_no_policy_no_attempts(self):
        status = build_usage_status(self.conn, workspace_id="demo", task_id="task-1")
        self.assertIsNone(status["policy"])
        self.assertEqual(status["aggregate"]["attempt_count"], 0)
        self.assertEqual(status["aggregate"]["observed_tokens"], 0)
        self.assertEqual(status["attempts"], [])
        self.assertEqual(status["warnings"], [])


if __name__ == "__main__":
    unittest.main()
