"""Issue #11 trace projection R1: read-only builder and CLI contract tests.

Covers the canonical plan's 11-item matrix: forward/reverse relations,
current-job selection and ambiguity, typed-vs-legacy evidence, the six
closed evidence states with fixed per-field classification, the three
provable stale rules, delivery locators, read-only guarantees, next gate
reuse, and CLI error handling.
"""
from __future__ import annotations
import json
import sqlite3
import tempfile
import typing
import unittest
from pathlib import Path

from coordinate.cli import main as cli_main
from coordinate.db import (
    append_event,
    create_delivery,
    initialize,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.job_repository import create_job
from coordinate.trace_projection import (
    DEFAULT_HISTORY_LIMIT,
    MAX_HISTORY_LIMIT,
    TRACE_CONTRACT_VERSION,
    TraceProjectionV1,
    TraceQueryError,
    build_job_trace,
    build_task_trace,
)

WS = "ws"
TASK = "t1"
AGENT = "agent-1"
PLAN_DOC = "docs/tasks/t1/plan.md"
T0 = "2026-01-01T00:00:00Z"
T1 = "2026-01-01T00:01:00Z"
T2 = "2026-01-01T00:02:00Z"
T3 = "2026-01-01T00:03:00Z"
NOW = "2026-01-01T00:10:00Z"


def _sha(char: str) -> str:
    return f"sha256:{char * 64}"


def _typed_payload(job_id: str, *, task_id: str | None = TASK) -> dict:
    return {
        "execution_context": {
            "contract_version": 1,
            "context_id": _sha("c"),
            "job_id": job_id,
            "workspace_id": WS,
            "task_id": task_id,
            "assigned_agent": AGENT,
            "host_id": "mac",
            "workspace_path": "/hosts/mac/ws",
            "worktree_path": "/hosts/mac/wt",
            "harness_root": "/hosts/mac/hr",
            "branch": "issue/11",
            "session_scope_id": "discord:ch1",
            "legacy_scope_ids": [],
            "log_handle": {"kind": "file", "job_id": job_id, "logs_path": "/hosts/mac/logs"},
        },
        "executor_binding": {
            "contract_version": 1,
            "source_id": "multinexus.discord",
            "source_version": 3,
            "catalog_hash": _sha("d"),
            "executor_definition_id": "coder",
            "executor_instance_id": AGENT,
            "runner_profile_id": AGENT,
            "provider": "kimi-code",
            "adapter": "omp",
            "capabilities": ["coding"],
            "binding_id": _sha("e"),
        },
        "prompt": "SECRET-PROMPT-TEXT",
    }


def _make_job(
    conn,
    job_id: str,
    *,
    status: str = "pending",
    task_id: str | None = TASK,
    attempt_count: int = 0,
    typed: bool = True,
    created_at: str = T0,
    session_id: str | None = None,
    progress: dict | None = None,
) -> None:
    payload = _typed_payload(job_id, task_id=task_id) if typed else {"prompt": "SECRET-PROMPT-TEXT"}
    create_job(
        conn,
        workspace_id=WS,
        task_id=task_id,
        runner_profile_id=AGENT,
        assigned_agent=AGENT,
        payload=payload,
        job_id=job_id,
    )
    conn.execute(
        """
        UPDATE jobs
        SET status = ?, attempt_count = ?, created_at = ?, updated_at = ?,
            terminal_session_id = ?, progress_json = ?, last_activity_at = ?,
            started_at = ?, completed_at = ?
        WHERE id = ?
        """,
        (
            status,
            attempt_count,
            created_at,
            created_at,
            session_id,
            json.dumps(progress) if progress is not None else None,
            (progress or {}).get("last_activity_at"),
            T1 if status in {"running", "done", "failed", "timed_out"} else None,
            T3 if status in {"done", "failed"} else None,
            job_id,
        ),
    )
    conn.commit()


def _claim(conn, job_id: str, attempt: int, created_at: str = T1) -> None:
    append_event(
        conn,
        workspace_id=WS,
        event_type="job.claimed",
        actor=AGENT,
        target=AGENT,
        task_id=TASK,
        idempotency_key=f"runtime:job:{job_id}:claimed:{attempt}",
        payload={"job_id": job_id, "agent_id": AGENT, "host_id": "mac"},
        commit=False,
    )
    conn.execute(
        "UPDATE events SET created_at = ? WHERE idempotency_key = ?",
        (created_at, f"runtime:job:{job_id}:claimed:{attempt}"),
    )
    conn.commit()


def _progress_event(
    conn,
    job_id: str,
    *,
    created_at: str = T2,
    stage: str = "coding",
    session_id: str | None = "sess-a",
) -> None:
    event = append_event(
        conn,
        workspace_id=WS,
        event_type="job.progress",
        actor=AGENT,
        target=AGENT,
        task_id=TASK,
        payload={
            "job_id": job_id,
            "agent_id": AGENT,
            "last_activity_at": created_at,
            "stage": stage,
            "summary": "SECRET-PROGRESS-SUMMARY",
            **({"session_id": session_id} if session_id else {}),
        },
        commit=False,
    )
    conn.execute(
        "UPDATE events SET created_at = ? WHERE id = ?",
        (created_at, event.row["id"]),
    )
    conn.commit()


def _lease(
    conn,
    job_id: str,
    *,
    attempt_token: int = 1,
    status: str = "active",
    expires_at: str = "2026-01-01T00:20:00Z",
    acquired_at: str = T1,
    released_at: str | None = None,
    release_reason: str | None = None,
) -> str:
    lease_id = f"lease-{job_id}-{attempt_token}"
    if conn.execute("SELECT 1 FROM agents WHERE id = ?", (AGENT,)).fetchone() is None:
        conn.execute(
            """
            INSERT INTO agents (
              id, name, role, capabilities_json, online_state,
              current_load, created_at, updated_at
            ) VALUES (?, 'agent one', 'worker', '{}', 'online', 0, ?, ?)
            """,
            (AGENT, T0, T0),
        )
    conn.execute(
        """
        INSERT INTO execution_attempt_leases (
          lease_id, job_id, attempt_token, agent_id, runner_profile_id, host_id,
          resource_kind, resource_key, normalized_path, capacity_policy_id,
          max_concurrent_jobs, status, acquired_at, renewed_at, expires_at,
          released_at, release_reason
        ) VALUES (?, ?, ?, ?, ?, ?, 'worktree', ?, ?, ?, 2, ?, ?, ?, ?, ?, ?)
        """,
        (
            lease_id,
            job_id,
            attempt_token,
            AGENT,
            AGENT,
            "mac",
            _sha("a"),
            "/hosts/mac/wt",
            _sha("b"),
            status,
            acquired_at,
            acquired_at,
            expires_at,
            released_at,
            release_reason,
        ),
    )
    conn.commit()
    return lease_id


def _mirror(
    conn,
    *,
    pr: str | None = "https://github.com/o/r/pull/1",
    phase: str = "implementing",
    payload: dict | None = None,
) -> None:
    if payload is None:
        # Real coordinator record-half shape (apply_task_create_record /
        # apply_issue_materialize_record): plan_doc is the primary locator,
        # absolute_plan_doc sits alongside.
        payload = {
            "plan_doc": PLAN_DOC,
            "absolute_plan_doc": f"/hosts/mac/ws/{PLAN_DOC}",
        }
    upsert_task_mirror(
        conn,
        workspace_id=WS,
        task_id=TASK,
        phase=phase,
        owner=AGENT,
        branch="issue/11",
        pr=pr,
        payload=payload,
    )


class TraceFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "trace.sqlite3")
        self.conn = initialize(self.db_path)
        self.addCleanup(self.conn.close)
        upsert_workspace(
            self.conn,
            workspace_id=WS,
            name="Demo",
            path="/hosts/mac/ws",
            harness_root="/hosts/mac/hr",
        )
        self.conn.execute(
            """
            INSERT INTO runner_profiles (
              id, name, runner_type, command, working_directory_strategy,
              supports_stream_attach, env_json, created_at, updated_at
            ) VALUES (?, 'omp', 'subprocess', 'true', 'workspace', 0, '{}', ?, ?)
            """,
            (AGENT, T0, T0),
        )
        self.conn.commit()
        _mirror(self.conn)


class RelationTests(TraceFixture):
    """Matrix 1-2: forward/reverse relations, selection, history bounds."""

    def test_task_query_projects_forward_relation_with_current_job(self) -> None:
        _make_job(self.conn, "job-live", status="running", attempt_count=1)
        _make_job(self.conn, "job-old", status="done", attempt_count=1, created_at="2025-12-01T00:00:00Z")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["contract_version"], TRACE_CONTRACT_VERSION)
        self.assertEqual(
            trace["query"],
            {"kind": "task", "workspace_id": WS, "task_id": TASK, "history_limit": DEFAULT_HISTORY_LIMIT},
        )
        self.assertEqual(trace["task"]["state"], "present")
        self.assertEqual(trace["task"]["phase"], "implementing")
        self.assertEqual(trace["task"]["pr"], "https://github.com/o/r/pull/1")
        self.assertEqual(trace["jobs"]["total"], 2)
        self.assertFalse(trace["jobs"]["history_truncated"])
        self.assertEqual(len(trace["jobs"]["summaries"]), 2)
        summary_ids = [s["job_id"] for s in trace["jobs"]["summaries"]]
        self.assertEqual(summary_ids[0], "job-live")  # newest first
        self.assertEqual(trace["execution"]["selection"]["kind"], "current")
        self.assertEqual(trace["execution"]["selection"]["job_id"], "job-live")
        self.assertEqual(trace["execution"]["job"]["job_id"], "job-live")

    def test_task_query_zero_jobs(self) -> None:
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["jobs"]["total"], 0)
        self.assertEqual(trace["jobs"]["summaries"], [])
        self.assertFalse(trace["jobs"]["history_truncated"])
        self.assertEqual(trace["execution"]["selection"]["kind"], "none")
        self.assertNotIn("job", trace["execution"])
        self.assertNotIn("executor", trace["execution"])

    def test_task_query_terminal_history_selects_latest_not_current(self) -> None:
        _make_job(self.conn, "job-a", status="done", created_at="2025-12-01T00:00:00Z")
        _make_job(self.conn, "job-b", status="failed", created_at="2025-12-02T00:00:00Z")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["execution"]["selection"]["kind"], "latest")
        self.assertEqual(trace["execution"]["selection"]["job_id"], "job-b")
        self.assertNotIn("current", json.dumps(trace["execution"]["selection"]))

    def test_task_query_multiple_live_jobs_is_ambiguous(self) -> None:
        _make_job(self.conn, "job-x", status="running", attempt_count=1)
        _make_job(self.conn, "job-y", status="pending")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        selection = trace["execution"]["selection"]
        self.assertEqual(selection["kind"], "ambiguous")
        self.assertEqual(sorted(selection["candidate_job_ids"]), ["job-x", "job-y"])
        self.assertNotIn("job_id", selection)
        self.assertNotIn("job", trace["execution"])
        self.assertNotIn("executor", trace["execution"])
        classifications = [d["classification"] for d in trace["diagnostics"]]
        self.assertIn("ambiguous_current_job", classifications)

    def test_recoverable_timed_out_counts_as_live(self) -> None:
        _make_job(self.conn, "job-t", status="timed_out", attempt_count=1)
        self.conn.execute("UPDATE jobs SET recoverable = 1 WHERE id = 'job-t'")
        self.conn.commit()
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["execution"]["selection"]["kind"], "current")

    def test_history_limit_and_truncation(self) -> None:
        for i in range(3):
            _make_job(self.conn, f"job-{i}", status="done", created_at=f"2025-12-0{i + 1}T00:00:00Z")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, history_limit=2, now=NOW)
        self.assertEqual(trace["jobs"]["total"], 3)
        self.assertEqual(len(trace["jobs"]["summaries"]), 2)
        self.assertTrue(trace["jobs"]["history_truncated"])
        self.assertEqual(trace["query"]["history_limit"], 2)

    def test_history_limit_bounds_enforced(self) -> None:
        for bad in (0, -1, MAX_HISTORY_LIMIT + 1):
            with self.subTest(limit=bad):
                with self.assertRaises(TraceQueryError):
                    build_task_trace(self.conn, workspace_id=WS, task_id=TASK, history_limit=bad)
        build_task_trace(self.conn, workspace_id=WS, task_id=TASK, history_limit=MAX_HISTORY_LIMIT, now=NOW)

    def test_unknown_workspace_and_task_fail_closed(self) -> None:
        with self.assertRaises(TraceQueryError):
            build_task_trace(self.conn, workspace_id="nope", task_id=TASK, now=NOW)
        with self.assertRaises(TraceQueryError):
            build_task_trace(self.conn, workspace_id=WS, task_id="nope", now=NOW)

    def test_job_query_reverse_relation_returns_task_and_plan_locator(self) -> None:
        _make_job(self.conn, "job-r", status="running", attempt_count=1)
        trace = build_job_trace(self.conn, job_id="job-r", now=NOW)
        self.assertEqual(trace["query"]["kind"], "job")
        self.assertEqual(trace["query"]["job_id"], "job-r")
        self.assertEqual(trace["workspace"]["state"], "present")
        self.assertEqual(trace["task"]["state"], "present")
        self.assertEqual(trace["task"]["task_id"], TASK)
        locator = trace["task"]["plan_locator"]
        self.assertEqual(locator["state"], "present")
        self.assertEqual(locator["plan_path"], "docs/tasks/t1/plan.md")
        self.assertEqual(trace["execution"]["selection"]["kind"], "exact")
        self.assertEqual(trace["execution"]["selection"]["job_id"], "job-r")

    def test_job_query_missing_task_mirror(self) -> None:
        _make_job(self.conn, "job-m", status="running", attempt_count=1)
        self.conn.execute("DELETE FROM tasks WHERE workspace_id = ? AND task_id = ?", (WS, TASK))
        self.conn.commit()
        trace = build_job_trace(self.conn, job_id="job-m", now=NOW)
        self.assertEqual(trace["task"]["state"], "missing")

    def test_job_query_job_without_task_id(self) -> None:
        _make_job(self.conn, "job-nt", status="running", attempt_count=1, task_id=None)
        trace = build_job_trace(self.conn, job_id="job-nt", now=NOW)
        self.assertEqual(trace["task"]["state"], "unavailable")
        self.assertEqual(trace["forge"]["state"], "unavailable")

    def test_unknown_job_fails_closed(self) -> None:
        with self.assertRaises(TraceQueryError):
            build_job_trace(self.conn, job_id="nope", now=NOW)

    def test_job_query_workspace_guard(self) -> None:
        _make_job(self.conn, "job-g", status="running", attempt_count=1)
        trace = build_job_trace(self.conn, job_id="job-g", workspace_id=WS, now=NOW)
        self.assertEqual(trace["workspace"]["workspace_id"], WS)
        with self.assertRaises(TraceQueryError):
            build_job_trace(self.conn, job_id="job-g", workspace_id="other", now=NOW)

class PlanLocatorTests(TraceFixture):
    """Operator follow-up #1: plan locator must use the real write-path field.

    The tasks mirror stores the locator under ``plan_doc`` on the coordinator
    record paths (task create/create-record, issue materialize) and under
    ``plan_path``/``artifacts.plan`` on the harness reconcile path (the
    checklist item payload is stored verbatim). Both shapes are production
    reality; the resolution order is fixed and provenance-tagged.
    """

    def test_production_combined_record_payload_plan_locator_present(self) -> None:
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        locator = trace["task"]["plan_locator"]
        self.assertEqual(locator["state"], "present")
        self.assertEqual(locator["plan_path"], PLAN_DOC)
        self.assertEqual(locator["source"], "plan_doc")

    def test_reconcile_checklist_item_payload_falls_back_to_plan_path(self) -> None:
        # Real reconcile mirror shape: the checklist item payload verbatim
        # (no plan_doc key; artifacts.plan is a strict alias of plan_path).
        _mirror(
            self.conn,
            payload={
                "id": TASK,
                "plan_path": PLAN_DOC,
                "artifact_path": PLAN_DOC,
                "artifacts": {"plan": PLAN_DOC},
            },
        )
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        locator = trace["task"]["plan_locator"]
        self.assertEqual(locator["state"], "present")
        self.assertEqual(locator["plan_path"], PLAN_DOC)
        self.assertEqual(locator["source"], "plan_path")

    def test_plan_doc_takes_precedence_over_reconcile_keys(self) -> None:
        # Merged mirror: reconcile updates the item payload into an existing
        # coordinator mirror, so both locator shapes coexist.
        _mirror(
            self.conn,
            payload={
                "plan_doc": PLAN_DOC,
                "plan_path": "docs/other/legacy-plan.md",
                "artifacts": {"plan": "docs/other/legacy-plan.md"},
            },
        )
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        locator = trace["task"]["plan_locator"]
        self.assertEqual(locator["state"], "present")
        self.assertEqual(locator["plan_path"], PLAN_DOC)
        self.assertEqual(locator["source"], "plan_doc")

    def test_mirror_without_any_plan_field_is_missing(self) -> None:
        _mirror(self.conn, payload={"title": TASK})
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["task"]["plan_locator"]["state"], "missing")


class TypedEvidenceTests(TraceFixture):
    """Matrix 3-4: typed vs legacy evidence and per-field states."""

    def test_execution_job_outputs_agent_and_runner_locators(self) -> None:
        """Operator follow-up #2: jobs.assigned_agent/runner_profile_id are
        durable job-row authority (C2 requires agent/runner) and must be
        projected for typed and legacy payloads alike."""
        _make_job(self.conn, "job-ar", status="running", attempt_count=1)
        _make_job(self.conn, "job-ar-legacy", status="done", attempt_count=1, typed=False, created_at="2025-12-01T00:00:00Z")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        job = trace["execution"]["job"]
        self.assertEqual(job["job_id"], "job-ar")
        self.assertEqual(job["assigned_agent"], AGENT)
        self.assertEqual(job["runner_profile_id"], AGENT)
        exact = build_job_trace(self.conn, job_id="job-ar-legacy", now=NOW)
        legacy_job = exact["execution"]["job"]
        self.assertEqual(legacy_job["assigned_agent"], AGENT)
        self.assertEqual(legacy_job["runner_profile_id"], AGENT)

    def test_typed_execution_evidence_present(self) -> None:
        _make_job(
            self.conn,
            "job-typed",
            status="running",
            attempt_count=1,
            session_id="sess-a",
            progress={"stage": "coding", "session_id": "sess-a", "summary": "SECRET-PROGRESS-SUMMARY"},
        )
        _claim(self.conn, "job-typed", 1)
        _progress_event(self.conn, "job-typed", created_at=T2, session_id="sess-a")
        lease_id = _lease(self.conn, "job-typed")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        execution = trace["execution"]
        self.assertEqual(execution["job"]["state"], "present")
        self.assertEqual(execution["job"]["status"], "running")
        self.assertEqual(execution["job"]["attempt_count"], 1)
        self.assertEqual(execution["outcome"]["state"], "present")
        executor = execution["executor"]
        self.assertEqual(executor["state"], "present")
        self.assertEqual(executor["source"], "stored_snapshot")
        self.assertEqual(executor["provider"], "kimi-code")
        self.assertEqual(executor["adapter"], "omp")
        self.assertEqual(executor["executor_definition_id"], "coder")
        ctx = execution["execution_context"]
        self.assertEqual(ctx["state"], "present")
        self.assertEqual(ctx["host_id"], "mac")
        self.assertEqual(ctx["worktree_path"], "/hosts/mac/wt")
        self.assertEqual(ctx["branch"], "issue/11")
        session = execution["provider_session"]
        self.assertEqual(session["state"], "present")
        self.assertEqual(session["session_id"], "sess-a")
        progress = execution["last_progress"]
        self.assertEqual(progress["state"], "present")
        self.assertEqual(progress["stage"], "coding")
        self.assertEqual(progress["session_id"], "sess-a")
        self.assertEqual(progress["last_activity_at"], T2)
        self.assertNotIn("summary", progress)
        leases = execution["attempt_leases"]
        self.assertEqual(leases["state"], "present")
        self.assertEqual(leases["current_lease_id"], lease_id)
        self.assertEqual(leases["leases"][0]["attempt_token"], 1)
        self.assertEqual(leases["leases"][0]["host_id"], "mac")
        self.assertEqual(leases["leases"][0]["resource_kind"], "worktree")

    def test_trace_json_never_leaks_private_payload(self) -> None:
        _make_job(
            self.conn,
            "job-priv",
            status="running",
            attempt_count=1,
            session_id="sess-a",
            progress={"stage": "coding", "summary": "SECRET-PROGRESS-SUMMARY"},
        )
        _claim(self.conn, "job-priv", 1)
        _progress_event(self.conn, "job-priv")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        raw = json.dumps(trace)
        self.assertNotIn("SECRET-PROMPT-TEXT", raw)
        self.assertNotIn("SECRET-PROGRESS-SUMMARY", raw)
        self.assertNotIn("prompt", raw)

    def test_legacy_job_marks_snapshots_unknown(self) -> None:
        _make_job(self.conn, "job-legacy", status="running", attempt_count=1, typed=False)
        _claim(self.conn, "job-legacy", 1)
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        execution = trace["execution"]
        self.assertEqual(execution["executor"]["state"], "unknown")
        self.assertEqual(execution["execution_context"]["state"], "unknown")
        self.assertEqual(execution["provider_session"]["state"], "unknown")
        self.assertEqual(execution["last_progress"]["state"], "unavailable")
        self.assertEqual(execution["attempt_leases"]["state"], "unavailable")

    def test_current_attempt_missing_lease(self) -> None:
        _make_job(self.conn, "job-nl", status="running", attempt_count=1)
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["execution"]["attempt_leases"]["state"], "missing")

    def test_expired_active_lease_is_stale(self) -> None:
        _make_job(self.conn, "job-exp", status="running", attempt_count=1)
        _lease(self.conn, "job-exp", expires_at=NOW)  # expires_at <= generated_at
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        leases = trace["execution"]["attempt_leases"]
        self.assertEqual(leases["state"], "present")
        self.assertIsNone(leases["current_lease_id"])
        self.assertEqual(leases["leases"][0]["state"], "stale")

    def test_progress_from_previous_attempt_is_stale(self) -> None:
        _make_job(
            self.conn,
            "job-st",
            status="running",
            attempt_count=2,
            progress={"stage": "coding"},
        )
        _claim(self.conn, "job-st", 1, created_at=T0)
        _claim(self.conn, "job-st", 2, created_at=T2)
        _progress_event(self.conn, "job-st", created_at=T1)  # before attempt-2 claim
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["execution"]["last_progress"]["state"], "stale")

    def test_progress_after_current_claim_is_present(self) -> None:
        _make_job(self.conn, "job-ok", status="running", attempt_count=1, progress={"stage": "coding"})
        _claim(self.conn, "job-ok", 1, created_at=T1)
        _progress_event(self.conn, "job-ok", created_at=T2)
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.assertEqual(trace["execution"]["last_progress"]["state"], "present")

    def test_conflicting_session_sources_unknown(self) -> None:
        _make_job(
            self.conn,
            "job-cf",
            status="running",
            attempt_count=1,
            session_id="sess-terminal",
        )
        _claim(self.conn, "job-cf", 1)
        _progress_event(self.conn, "job-cf", session_id="sess-event")
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        session = trace["execution"]["provider_session"]
        self.assertEqual(session["state"], "unknown")
        self.assertNotIn("session_id", session)
        self.assertEqual(len(session["sources"]), 2)

    def test_orphan_job_workspace_unknown_with_diagnostic(self) -> None:
        _make_job(self.conn, "job-orphan", status="running", attempt_count=1)
        self.conn.execute("UPDATE jobs SET workspace_id = NULL WHERE id = 'job-orphan'")
        self.conn.commit()
        trace = build_job_trace(self.conn, job_id="job-orphan", now=NOW)
        self.assertEqual(trace["workspace"]["state"], "unknown")
        self.assertNotIn("workspace_id", trace["workspace"])
        diagnostics = [d["classification"] for d in trace["diagnostics"]]
        self.assertIn("orphan_workspace", diagnostics)
        orphan = next(d for d in trace["diagnostics"] if d["classification"] == "orphan_workspace")
        self.assertEqual(orphan["component"], "workspace")
        self.assertEqual(orphan["locator"], {"job_id": "job-orphan"})


class ForgeEvidenceTests(TraceFixture):
    """Matrix 5: forge gate states and the same-PR stale rule."""

    PR = "https://github.com/o/r/pull/1"
    PR2 = "https://github.com/o/r/pull/2"

    def _ci(self, event_type: str, *, pr: str, head: str, created_at: str = T1) -> None:
        event = append_event(
            self.conn,
            workspace_id=WS,
            event_type=event_type,
            actor="runner",
            task_id=TASK,
            payload={"task_id": TASK, "pr": pr, "pr_url": pr, "head_sha": head, "status": event_type.split(".")[1]},
            commit=False,
        )
        self.conn.execute("UPDATE events SET created_at = ? WHERE id = ?", (created_at, event.row["id"]))
        self.conn.commit()

    def _review(self, decision: str, *, pr: str, head: str, created_at: str = T2) -> None:
        event = append_event(
            self.conn,
            workspace_id=WS,
            event_type=f"pr_review.{decision}",
            actor="reviewer",
            task_id=TASK,
            payload={"task_id": TASK, "pr": pr, "pr_url": pr, "head_sha": head, "review_decision": decision},
            commit=False,
        )
        self.conn.execute("UPDATE events SET created_at = ? WHERE id = ?", (created_at, event.row["id"]))
        self.conn.commit()

    def _result_review(self, decision: str = "approve", created_at: str = T3) -> None:
        event = append_event(
            self.conn,
            workspace_id=WS,
            event_type="review.completed",
            actor="reviewer",
            task_id=TASK,
            payload={"task_id": TASK, "decision": decision, "reviewer": "reviewer"},
            commit=False,
        )
        self.conn.execute("UPDATE events SET created_at = ? WHERE id = ?", (created_at, event.row["id"]))
        self.conn.commit()

    def _rejected_result_review(self, decision: str = "request_changes", created_at: str = T3) -> None:
        event = append_event(
            self.conn,
            workspace_id=WS,
            event_type="review.rejected",
            actor="reviewer",
            task_id=TASK,
            payload={"task_id": TASK, "decision": decision, "reviewer": "reviewer"},
            commit=False,
        )
        self.conn.execute("UPDATE events SET created_at = ? WHERE id = ?", (created_at, event.row["id"]))
        self.conn.commit()

    def _forge(self) -> dict:
        return build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)["forge"]

    def test_forge_unconfigured_without_pr(self) -> None:
        _mirror(self.conn, pr=None)
        forge = self._forge()
        self.assertEqual(forge["state"], "unavailable")

    def test_ci_failed_outcome_is_failed_but_identity_present(self) -> None:
        _make_job(self.conn, "job-f", status="running", attempt_count=1)
        self._ci("ci.failed", pr=self.PR, head="h1")
        forge = self._forge()
        ci = forge["ci"]
        self.assertEqual(ci["state"], "failed")
        self.assertEqual(ci["status"], "failed")
        self.assertEqual(ci["pr"], self.PR)
        self.assertEqual(ci["head_sha"], "h1")

    def test_same_pr_head_mismatch_marks_review_stale(self) -> None:
        self._ci("ci.passed", pr=self.PR, head="h1")
        self._review("approved", pr=self.PR, head="h2")
        forge = self._forge()
        self.assertEqual(forge["ci"]["state"], "present")
        self.assertEqual(forge["pr_review"]["state"], "stale")

    def test_pr_switch_makes_old_review_unavailable_not_stale(self) -> None:
        _mirror(self.conn, pr=self.PR2)
        self._ci("ci.passed", pr=self.PR2, head="h3")
        self._review("approved", pr=self.PR, head="h1")
        forge = self._forge()
        self.assertEqual(forge["ci"]["state"], "present")
        self.assertEqual(forge["ci"]["pr"], self.PR2)
        self.assertEqual(forge["pr_review"]["state"], "unavailable")
        self.assertNotIn("stale", json.dumps(forge["pr_review"]))

    def test_pr_switch_with_only_old_pr_ci_is_unavailable_not_present(self) -> None:
        """Operator follow-up #3: after a PR switch with no CI on the new PR
        yet, the old PR's CI event must not be presented as evidence for the
        current PR (CI events record the exact task-mirror PR at write time)."""
        _mirror(self.conn, pr=self.PR2)
        self._ci("ci.passed", pr=self.PR, head="h1")
        forge = self._forge()
        self.assertEqual(forge["pr"], self.PR2)
        self.assertEqual(forge["ci"]["state"], "unavailable")
        self.assertNotIn("stale", json.dumps(forge["ci"]))

    def test_consistent_latest_evidence_present(self) -> None:
        self._ci("ci.passed", pr=self.PR, head="h1")
        self._review("approved", pr=self.PR, head="h1")
        self._result_review()
        forge = self._forge()
        self.assertEqual(forge["state"], "present")
        self.assertEqual(forge["pr"], self.PR)
        self.assertEqual(forge["ci"]["state"], "present")
        self.assertEqual(forge["pr_review"]["state"], "present")
        self.assertEqual(forge["pr_review"]["review_decision"], "approved")
        self.assertEqual(forge["result_review"]["state"], "present")
        self.assertEqual(forge["result_review"]["decision"], "approve")

    def test_no_forge_events_recorded(self) -> None:
        forge = self._forge()
        self.assertEqual(forge["state"], "present")
        self.assertEqual(forge["ci"]["state"], "unavailable")
        self.assertEqual(forge["pr_review"]["state"], "unavailable")
        self.assertEqual(forge["result_review"]["state"], "unavailable")

    def test_rejected_result_review_is_present_not_unavailable(self) -> None:
        self._rejected_result_review()
        result_review = self._forge()["result_review"]
        self.assertEqual(result_review["state"], "present")
        self.assertEqual(result_review["event_type"], "review.rejected")
        self.assertEqual(result_review["decision"], "request_changes")

    def test_head_sha_not_required_for_review_state(self) -> None:
        # Same PR but CI event without head_sha: mismatch is not provable.
        event = append_event(
            self.conn,
            workspace_id=WS,
            event_type="ci.passed",
            actor="runner",
            task_id=TASK,
            payload={"task_id": TASK, "pr": self.PR, "status": "passed"},
        )
        self.conn.execute("UPDATE events SET created_at = ? WHERE id = ?", (T1, event.row["id"]))
        self.conn.commit()
        self._review("approved", pr=self.PR, head="h2")
        forge = self._forge()
        self.assertEqual(forge["pr_review"]["state"], "present")


class DeliveryEvidenceTests(TraceFixture):
    """Matrix 6: delivery locators and the platform=none audit sink."""

    def _terminal_event(self, job_id: str) -> str:
        event = append_event(
            self.conn,
            workspace_id=WS,
            event_type="job.completed",
            actor=AGENT,
            task_id=TASK,
            payload={"job_id": job_id, "status": "done", "response_text": "SECRET-RESPONSE"},
        )
        return event.row["id"]

    def test_sent_delivery_locator_present(self) -> None:
        _make_job(self.conn, "job-d1", status="done", attempt_count=1)
        event_id = self._terminal_event("job-d1")
        delivery, _ = create_delivery(
            self.conn,
            event_id=event_id,
            platform="discord",
            destination="ch1",
            message_key="mk-1",
            payload={"secret": "SECRET-DELIVERY-BODY"},
        )
        self.conn.execute(
            "UPDATE deliveries SET status = 'sent', platform_message_id = 'pm-1' WHERE id = ?",
            (delivery["id"],),
        )
        self.conn.commit()
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        delivery_section = trace["execution"]["delivery"]
        self.assertEqual(delivery_section["state"], "present")
        row = delivery_section["deliveries"][0]
        self.assertEqual(row["state"], "present")
        self.assertEqual(row["platform"], "discord")
        self.assertEqual(row["destination"], "ch1")
        self.assertEqual(row["status"], "sent")
        self.assertEqual(row["platform_message_id"], "pm-1")
        self.assertNotIn("SECRET-DELIVERY-BODY", json.dumps(trace))
        self.assertNotIn("SECRET-RESPONSE", json.dumps(trace))

    def test_failed_delivery_outcome_failed(self) -> None:
        _make_job(self.conn, "job-d2", status="done", attempt_count=1)
        event_id = self._terminal_event("job-d2")
        delivery, _ = create_delivery(
            self.conn,
            event_id=event_id,
            platform="discord",
            destination="ch1",
            message_key="mk-2",
            payload={},
        )
        self.conn.execute("UPDATE deliveries SET status = 'failed' WHERE id = ?", (delivery["id"],))
        self.conn.commit()
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        row = trace["execution"]["delivery"]["deliveries"][0]
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["status"], "failed")

    def test_platform_none_audit_sink_has_no_delivery_locator(self) -> None:
        _make_job(self.conn, "job-d3", status="done", attempt_count=1)
        self._terminal_event("job-d3")  # platform=none sink creates no delivery row
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        delivery_section = trace["execution"]["delivery"]
        self.assertEqual(delivery_section["state"], "unavailable")
        self.assertEqual(delivery_section["deliveries"], [])


class FailureSeparationTests(TraceFixture):
    """Matrix 7: domain failure vs missing/unavailable/stale; DB errors fail."""

    def test_domain_failed_job_separated_from_other_states(self) -> None:
        _make_job(self.conn, "job-df", status="failed", attempt_count=1)
        trace = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        job = trace["execution"]["job"]
        self.assertEqual(job["state"], "present")
        self.assertEqual(job["status"], "failed")
        self.assertEqual(trace["execution"]["outcome"]["state"], "failed")

    def test_database_error_is_not_masked_as_unavailable(self) -> None:
        _make_job(self.conn, "job-db", status="running", attempt_count=1)
        self.conn.execute("DROP TABLE execution_attempt_leases")
        with self.assertRaises(sqlite3.OperationalError):
            build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)


class ReadOnlyTests(TraceFixture):
    """Matrix 8: zero business mutation and reopen equivalence."""

    _TABLES = (
        "workspaces", "events", "jobs", "deliveries", "agents", "runner_profiles",
        "tasks", "task_groups", "task_group_items", "decision_requests",
        "workspace_host_profiles", "split_operations", "executor_catalog_sources",
        "executor_definitions", "executor_instance_bindings",
        "executor_capacity_sources", "executor_capacity_policies",
        "execution_attempt_leases", "channel_bindings",
    )

    @staticmethod
    def _snapshot(conn) -> dict:
        return {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()
            for table in ReadOnlyTests._TABLES
        }

    def test_builder_mutates_nothing(self) -> None:
        _make_job(
            self.conn,
            "job-ro",
            status="running",
            attempt_count=1,
            session_id="sess-a",
            progress={"stage": "coding"},
        )
        _claim(self.conn, "job-ro", 1)
        _progress_event(self.conn, "job-ro")
        _lease(self.conn, "job-ro")
        before_changes = self.conn.total_changes
        before = self._snapshot(self.conn)
        build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        build_job_trace(self.conn, job_id="job-ro", now=NOW)
        self.assertEqual(self.conn.total_changes, before_changes)
        self.assertEqual(
            [tuple(row) for row in self._snapshot(self.conn)["jobs"]],
            [tuple(row) for row in before["jobs"]],
        )
        after = self._snapshot(self.conn)
        for table in self._TABLES:
            self.assertEqual(
                [tuple(r) for r in after[table]], [tuple(r) for r in before[table]], table
            )

    def test_reopen_produces_equivalent_projection(self) -> None:
        _make_job(self.conn, "job-re", status="running", attempt_count=1, session_id="sess-a")
        _claim(self.conn, "job-re", 1)
        _lease(self.conn, "job-re")
        first = build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)
        self.conn.close()
        reopened = initialize(self.db_path)
        try:
            second = build_task_trace(reopened, workspace_id=WS, task_id=TASK, now=NOW)
        finally:
            reopened.close()
        first.pop("generated_at")
        second.pop("generated_at")
        self.assertEqual(first, second)
        self.conn = initialize(self.db_path)  # restore for addCleanup close


class NextGateTests(TraceFixture):
    """Matrix 10: next gate reuses operator.pending then job lifecycle."""

    def _gate(self) -> dict:
        return build_task_trace(self.conn, workspace_id=WS, task_id=TASK, now=NOW)["next_gate"]

    def test_pending_action_gate_wins(self) -> None:
        _mirror(self.conn, phase="planned")
        append_event(
            self.conn,
            workspace_id=WS,
            event_type="plan.review_requested",
            actor="operator",
            task_id=TASK,
            payload={"task_id": TASK},
        )
        gate = self._gate()
        self.assertEqual(gate["state"], "present")
        self.assertEqual(gate["source"], "operator.pending")
        self.assertEqual(gate["action"], "approve_plan")

    def test_pending_job_falls_back_to_claim(self) -> None:
        _make_job(self.conn, "job-p", status="pending")
        gate = self._gate()
        self.assertEqual(gate["state"], "present")
        self.assertEqual(gate["source"], "job_lifecycle")
        self.assertEqual(gate["action"], "claim")

    def test_lifecycle_gates_by_status(self) -> None:
        _make_job(self.conn, "job-run", status="running", attempt_count=1)
        self.assertEqual(self._gate()["action"], "progress-or-result")
        self.conn.execute("DELETE FROM jobs")
        _make_job(self.conn, "job-to", status="timed_out", attempt_count=1)
        self.conn.execute("UPDATE jobs SET recoverable = 1 WHERE id = 'job-to'")
        self.conn.commit()
        self.assertEqual(self._gate()["action"], "recover")
        self.conn.execute("DELETE FROM jobs")
        _make_job(self.conn, "job-done", status="done", attempt_count=1)
        self.assertEqual(self._gate()["action"], "operator-review")

    def test_no_authority_is_unknown(self) -> None:
        _make_job(self.conn, "job-x", status="failed", attempt_count=1, task_id=None)
        self.conn.execute("DELETE FROM tasks")
        self.conn.commit()
        trace = build_job_trace(self.conn, job_id="job-x", now=NOW)
        self.assertEqual(trace["next_gate"]["state"], "unknown")


class TraceCLITests(unittest.TestCase):
    """Matrix 9: CLI JSON contract, unknown ids, workspace guard, limits."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "cli.sqlite3")
        conn = initialize(self.db_path)
        upsert_workspace(conn, workspace_id=WS, name="Demo", path="/w", harness_root="/h")
        upsert_task_mirror(
            conn,
            workspace_id=WS,
            task_id=TASK,
            phase="implementing",
            owner=AGENT,
            branch="issue/11",
            pr=None,
            payload={},
        )
        conn.execute(
            """
            INSERT INTO runner_profiles (
              id, name, runner_type, command, working_directory_strategy,
              supports_stream_attach, env_json, created_at, updated_at
            ) VALUES (?, 'omp', 'subprocess', 'true', 'workspace', 0, '{}', ?, ?)
            """,
            (AGENT, T0, T0),
        )
        create_job(
            conn,
            workspace_id=WS,
            task_id=TASK,
            runner_profile_id=AGENT,
            assigned_agent=AGENT,
            payload=_typed_payload("job-cli"),
            job_id="job-cli",
        )
        conn.execute("UPDATE jobs SET status = 'running', attempt_count = 1 WHERE id = 'job-cli'")
        conn.commit()
        conn.close()

    def _run(self, *argv: str) -> tuple[int, str, str]:
        import contextlib
        import io

        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = cli_main(["--db", self.db_path, *argv])
        return code, stdout.getvalue(), stderr.getvalue()

    def test_trace_task_prints_only_trace_json(self) -> None:
        code, out, err = self._run("trace", "task", WS, TASK)
        self.assertEqual(code, 0, err)
        document = json.loads(out)
        self.assertEqual(list(document), ["trace"])
        self.assertEqual(document["trace"]["contract_version"], 1)
        self.assertEqual(document["trace"]["query"]["kind"], "task")
        self.assertEqual(document["trace"]["execution"]["selection"]["job_id"], "job-cli")

    def test_trace_job_prints_only_trace_json(self) -> None:
        code, out, err = self._run("trace", "job", "job-cli")
        self.assertEqual(code, 0, err)
        document = json.loads(out)
        self.assertEqual(list(document), ["trace"])
        self.assertEqual(document["trace"]["query"]["kind"], "job")
        self.assertEqual(document["trace"]["execution"]["selection"]["kind"], "exact")
        self.assertEqual(document["trace"]["execution"]["selection"]["job_id"], "job-cli")

    def test_unknown_ids_exit_nonzero_with_error(self) -> None:
        for argv in (
            ("trace", "task", "nope", TASK),
            ("trace", "task", WS, "nope"),
            ("trace", "job", "nope"),
        ):
            with self.subTest(argv=argv):
                code, out, err = self._run(*argv)
                self.assertEqual(code, 1)
                self.assertTrue(err.startswith("error:"), err)
                self.assertEqual(out, "")

    def test_workspace_guard_mismatch_fails(self) -> None:
        code, out, err = self._run("trace", "job", "job-cli", "--workspace-id", "other")
        self.assertEqual(code, 1)
        self.assertTrue(err.startswith("error:"), err)

    def test_invalid_history_limit_fails(self) -> None:
        for bad in ("0", "101", "-3"):
            with self.subTest(limit=bad):
                code, out, err = self._run("trace", "task", WS, TASK, "--history-limit", bad)
                self.assertEqual(code, 1)
                self.assertTrue(err.startswith("error:"), err)

    def test_valid_history_limit_accepted(self) -> None:
        code, out, err = self._run("trace", "task", WS, TASK, "--history-limit", "5")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["trace"]["query"]["history_limit"], 5)


class BoundedHistoryTests(TraceFixture):
    """Operator correction R1 #1: task history must come from bounded SQL."""

    def test_task_history_uses_bounded_sql(self) -> None:
        _make_job(self.conn, "job-b1", status="running", attempt_count=1)
        _make_job(self.conn, "job-b2", status="done", created_at="2025-12-01T00:00:00Z")
        statements: list[str] = []
        self.conn.set_trace_callback(statements.append)
        try:
            trace = build_task_trace(
                self.conn, workspace_id=WS, task_id=TASK, history_limit=5, now=NOW
            )
        finally:
            self.conn.set_trace_callback(None)
        jobs_selects = [s for s in statements if "FROM jobs" in s]
        # jobs.total comes from an exact COUNT(*), not from a fetched row list.
        self.assertTrue(any("COUNT(*)" in s for s in jobs_selects))
        # Every other jobs SELECT must carry an explicit LIMIT: no unbounded
        # history load is allowed to reach SQLite.
        bounded = [s for s in jobs_selects if "COUNT(*)" not in s]
        self.assertTrue(bounded)
        for statement in bounded:
            self.assertIn("LIMIT", statement, statement)
        self.assertTrue(any("LIMIT 5" in s for s in bounded))
        self.assertEqual(trace["jobs"]["total"], 2)
        self.assertEqual(len(trace["jobs"]["summaries"]), 2)

    def test_live_jobs_beyond_history_limit_stay_ambiguous(self) -> None:
        for i in range(3):
            _make_job(self.conn, f"live-{i}", status="pending")
        trace = build_task_trace(
            self.conn, workspace_id=WS, task_id=TASK, history_limit=2, now=NOW
        )
        selection = trace["execution"]["selection"]
        self.assertEqual(selection["kind"], "ambiguous")
        self.assertEqual(len(selection["candidate_job_ids"]), 3)
        self.assertEqual(trace["jobs"]["total"], 3)
        self.assertEqual(len(trace["jobs"]["summaries"]), 2)
        self.assertTrue(trace["jobs"]["history_truncated"])
        self.assertNotIn("job", trace["execution"])


class TraceContractTypeTests(unittest.TestCase):
    """Operator correction R1 #3: TraceProjectionV1 must be a real typed contract."""

    def test_trace_projection_v1_is_importable_typed_dict(self) -> None:
        self.assertTrue(issubclass(TraceProjectionV1, dict))
        for key in (
            "contract_version", "query", "generated_at", "workspace", "task",
            "jobs", "execution", "forge", "next_gate", "diagnostics",
        ):
            with self.subTest(key=key):
                self.assertIn(key, TraceProjectionV1.__annotations__)

    def test_public_builders_return_trace_projection_v1(self) -> None:
        for builder in (build_task_trace, build_job_trace):
            with self.subTest(builder=builder.__name__):
                hints = typing.get_type_hints(builder)
                self.assertIs(hints["return"], TraceProjectionV1)

if __name__ == "__main__":
    unittest.main()
