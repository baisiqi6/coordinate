import tempfile
import unittest
from pathlib import Path

from coordinate.db import (
    create_job,
    get_job,
    initialize,
    list_events,
    list_jobs,
    row_to_dict,
    upsert_runner_profile,
    upsert_workspace,
)
from coordinate.jobs import JobError, cancel_job, pump_jobs, retry_job, run_job


class JobRunnerTests(unittest.TestCase):
    def create_workspace_and_runner(self, tmp, command, *, strategy="current_dir", runner_type="generic_subprocess"):
        conn = initialize(":memory:")
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=tmp,
            harness_root=tmp,
        )
        runner = upsert_runner_profile(
            conn,
            profile_id="subprocess",
            name="Subprocess",
            runner_type=runner_type,
            command=command,
            working_directory_strategy=strategy,
        )
        return conn, workspace, runner

    def test_run_job_success_updates_status_and_writes_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "printf 'hello {task_id}'",
            )
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )

            result = run_job(conn, job["id"])

            updated = row_to_dict(get_job(conn, job["id"]))
            log_path = Path(result.log_path)
            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            self.assertEqual(updated["status"], "done")
            self.assertEqual(updated["attempt_count"], 1)
            self.assertEqual(updated["result"]["exit_code"], 0)
            self.assertTrue(log_path.exists())
            self.assertIn("hello mvp-001", log_path.read_text(encoding="utf-8"))
            self.assertEqual(
                [event["event_type"] for event in events],
                ["job.started", "job.completed"],
            )

    def test_run_job_failure_updates_status_and_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "python3 -c 'import sys; print(\"bad\"); sys.exit(3)'",
            )
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )

            result = run_job(conn, job["id"])

            updated = row_to_dict(get_job(conn, job["id"]))
            self.assertEqual(updated["status"], "failed")
            self.assertEqual(updated["result"]["exit_code"], 3)
            self.assertIn("bad", Path(result.log_path).read_text(encoding="utf-8"))

    def test_run_completed_job_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(tmp, "true")
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )
            run_job(conn, job["id"])

            with self.assertRaisesRegex(JobError, "only pending jobs can be run"):
                run_job(conn, job["id"])

    def test_git_worktree_strategy_requires_worktree_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "true",
                strategy="git_worktree",
            )
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )

            with self.assertRaisesRegex(JobError, "no worktree_path"):
                run_job(conn, job["id"])

    def test_only_generic_subprocess_runner_is_supported(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "true",
                runner_type="codex_cli",
            )
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )

            with self.assertRaisesRegex(JobError, "only generic_subprocess"):
                run_job(conn, job["id"])

    def test_pump_jobs_runs_pending_jobs_with_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "printf 'pump {task_id}'",
            )
            for index in range(3):
                create_job(
                    conn,
                    workspace_id="demo",
                    task_id=f"mvp-00{index}",
                    runner_profile_id="subprocess",
                )

            result = pump_jobs(conn, workspace_id="demo", limit=2)

            jobs = [row_to_dict(row) for row in list_jobs(conn, workspace_id="demo")]
            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            self.assertEqual(result.processed, 2)
            self.assertEqual(result.done, 2)
            self.assertEqual(result.failed, 0)
            self.assertEqual(result.errors, 0)
            self.assertEqual([job["status"] for job in jobs], ["done", "done", "pending"])
            self.assertEqual([event["event_type"] for event in events].count("job.completed"), 2)

    def test_pump_jobs_does_not_repeat_finished_jobs_on_second_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(tmp, "true")
            create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )

            first = pump_jobs(conn, workspace_id="demo", limit=10)
            second = pump_jobs(conn, workspace_id="demo", limit=10)

            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            self.assertEqual(first.done, 1)
            self.assertEqual(second.processed, 0)
            self.assertEqual([event["event_type"] for event in events].count("job.started"), 1)

    def test_cancelled_job_is_not_pumped(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "printf 'run {task_id}'",
            )
            first = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-cancelled",
                runner_profile_id="subprocess",
            )
            create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-active",
                runner_profile_id="subprocess",
            )

            cancel_result = cancel_job(conn, first["id"], reason="duplicate request")
            pump_result = pump_jobs(conn, workspace_id="demo", limit=10)

            jobs = {
                row["task_id"]: row_to_dict(row)
                for row in list_jobs(conn, workspace_id="demo")
            }
            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            self.assertEqual(cancel_result.job["status"], "cancelled")
            self.assertEqual(cancel_result.job["result"]["cancel_reason"], "duplicate request")
            self.assertEqual(pump_result.processed, 1)
            self.assertEqual(jobs["mvp-cancelled"]["status"], "cancelled")
            self.assertEqual(jobs["mvp-cancelled"]["attempt_count"], 0)
            self.assertEqual(jobs["mvp-active"]["status"], "done")
            self.assertIn("job.cancelled", [event["event_type"] for event in events])

    def test_retry_failed_job_creates_new_pending_job_and_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "python3 -c 'import sys; sys.exit(3)'",
            )
            source = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
                payload={"result_path": "shared-result.json", "purpose": "initial"},
            )
            run_job(conn, source["id"])

            retry_result = retry_job(conn, source["id"], reason="fix prompt")

            source_after = row_to_dict(get_job(conn, source["id"]))
            retry_after = row_to_dict(get_job(conn, retry_result.retry_job["id"]))
            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            self.assertEqual(source_after["status"], "failed")
            self.assertEqual(source_after["result"]["exit_code"], 3)
            self.assertEqual(retry_after["status"], "pending")
            self.assertEqual(retry_after["attempt_count"], 0)
            self.assertEqual(retry_after["payload"]["retry_of_job_id"], source["id"])
            self.assertEqual(retry_after["payload"]["retry_reason"], "fix prompt")
            self.assertEqual(retry_after["payload"]["purpose"], "initial")
            self.assertNotIn("result_path", retry_after["payload"])
            self.assertEqual(events[-1]["event_type"], "job.retry_requested")
            self.assertEqual(events[-1]["payload"]["source_job_id"], source["id"])
            self.assertEqual(events[-1]["payload"]["retry_job_id"], retry_after["id"])

    def test_pump_jobs_reports_pending_job_errors_without_stopping_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(tmp, "true")
            upsert_runner_profile(
                conn,
                profile_id="codex",
                name="Codex",
                runner_type="codex_cli",
                command="codex",
            )
            create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-unsupported",
                runner_profile_id="codex",
            )
            create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-supported",
                runner_profile_id="subprocess",
            )

            result = pump_jobs(conn, workspace_id="demo", limit=10)

            jobs = {
                row["task_id"]: row_to_dict(row)
                for row in list_jobs(conn, workspace_id="demo")
            }
            self.assertEqual(result.processed, 2)
            self.assertEqual(result.done, 1)
            self.assertEqual(result.errors, 1)
            self.assertIn("only generic_subprocess", result.error_details[0]["error"])
            self.assertEqual(jobs["mvp-unsupported"]["status"], "pending")
            self.assertEqual(jobs["mvp-supported"]["status"], "done")

    def test_pump_jobs_validates_limit(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)

        with self.assertRaisesRegex(JobError, "limit"):
            pump_jobs(conn, limit=0)

    def test_run_job_reads_structured_agent_response_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            response = {
                "status": "done",
                "summary": "Implemented the slice.",
                "artifact_paths": ["docs/result.md"],
                "branch": "agent/codex/mvp-001",
                "commit": "abc123",
                "pr": "https://github.example/pr/1",
            }
            command = (
                "python3 -c "
                + repr(
                    "import json, os; "
                    "json.dump("
                    f"dict(status={response['status']!r}, "
                    f"summary={response['summary']!r}, "
                    f"artifact_paths={response['artifact_paths']!r}, "
                    f"branch={response['branch']!r}, "
                    f"commit={response['commit']!r}, "
                    f"pr={response['pr']!r}), "
                    "open(os.environ['COORDINATOR_RESULT_PATH'], 'w'))"
                )
            )
            conn, _, _ = self.create_workspace_and_runner(tmp, command)
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
                payload={"result_path": "agent-response.json"},
            )

            result = run_job(conn, job["id"])

            updated = row_to_dict(get_job(conn, job["id"]))
            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            self.assertEqual(updated["status"], "done")
            self.assertEqual(updated["result"]["agent_status"], "done")
            self.assertEqual(updated["result"]["summary"], "Implemented the slice.")
            self.assertEqual(updated["result"]["artifact_paths"], ["docs/result.md"])
            self.assertEqual(updated["result"]["branch"], "agent/codex/mvp-001")
            self.assertEqual(updated["result"]["commit"], "abc123")
            self.assertEqual(updated["result"]["pr"], "https://github.example/pr/1")
            self.assertEqual(updated["result"]["logs_path"], result.log_path)
            self.assertTrue(str(updated["result"]["result_path"]).endswith("agent-response.json"))
            self.assertEqual(events[-1]["payload"]["summary"], "Implemented the slice.")

    def test_agent_response_blocked_status_marks_job_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            command = (
                "python3 -c "
                + repr(
                    "import json, os; "
                    "json.dump(dict(status='blocked', summary='Needs human decision.'), "
                    "open(os.environ['COORDINATOR_RESULT_PATH'], 'w'))"
                )
            )
            conn, _, _ = self.create_workspace_and_runner(tmp, command)
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )

            run_job(conn, job["id"])

            updated = row_to_dict(get_job(conn, job["id"]))
            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            self.assertEqual(updated["status"], "failed")
            self.assertEqual(updated["result"]["agent_status"], "blocked")
            self.assertEqual(events[-1]["event_type"], "job.failed")

    def test_invalid_agent_response_file_marks_job_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            command = "printf '{{bad json' > {result_path}"
            conn, _, _ = self.create_workspace_and_runner(tmp, command)
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
            )

            run_job(conn, job["id"])

            updated = row_to_dict(get_job(conn, job["id"]))
            self.assertEqual(updated["status"], "failed")
            self.assertIn("invalid AgentResponse JSON", updated["result"]["agent_response_error"])

    def test_run_job_removes_stale_result_path_before_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            result_path = Path(tmp) / "shared-result.json"
            result_path.write_text(
                '{"status":"blocked","summary":"stale blocker"}',
                encoding="utf-8",
            )
            conn, _, _ = self.create_workspace_and_runner(tmp, "true")
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
                payload={"result_path": str(result_path)},
            )

            run_job(conn, job["id"])

            updated = row_to_dict(get_job(conn, job["id"]))
            self.assertEqual(updated["status"], "done")
            self.assertNotIn("agent_status", updated["result"])
            self.assertFalse(result_path.exists())

    def test_run_job_rejects_directory_result_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            result_path = Path(tmp) / "result-dir"
            result_path.mkdir()
            conn, _, _ = self.create_workspace_and_runner(tmp, "true")
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-001",
                runner_profile_id="subprocess",
                payload={"result_path": str(result_path)},
            )

            with self.assertRaisesRegex(JobError, "points to a directory"):
                run_job(conn, job["id"])

    def test_timeout_failure_records_logs_and_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn, _, _ = self.create_workspace_and_runner(
                tmp,
                "python3 -c 'import time; print(\"before\", flush=True); time.sleep(2)'",
            )
            job = create_job(
                conn,
                workspace_id="demo",
                task_id="mvp-timeout",
                runner_profile_id="subprocess",
                timeout_seconds=1,
            )

            result = run_job(conn, job["id"])

            updated = row_to_dict(get_job(conn, job["id"]))
            events = [row_to_dict(row) for row in list_events(conn, "demo")]
            log_path = Path(result.log_path)
            self.assertEqual(updated["status"], "failed")
            self.assertTrue(updated["result"]["timeout"])
            self.assertEqual(updated["result"]["failure_reason"], "timeout")
            self.assertTrue(log_path.exists())
            self.assertIn("timed_out: True", log_path.read_text(encoding="utf-8"))
            self.assertEqual(events[-1]["event_type"], "job.failed")
            self.assertTrue(events[-1]["payload"]["timeout"])
            self.assertEqual(events[-1]["payload"]["logs_path"], str(log_path))


if __name__ == "__main__":
    unittest.main()
