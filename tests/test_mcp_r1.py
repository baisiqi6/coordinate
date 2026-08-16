"""R1 MCP stdio vertical-slice tests.

The MCP SDK is an optional extra; tests that need the real SDK lifecycle are
skipped when ``mcp`` is not importable, and the full suite is expected to run
both on a base install (facade/CLI/AST/contract coverage) and inside a fresh
venv with ``mcp==2.0.0`` (SDK discovery, call, envelope and stdio coverage).

Protocol coverage is split by era:

- legacy lifecycle (``initialize`` handshake, protocol <= 2025-11-25): the
  SDK-client subprocess tests and the raw framing test below;
- modern stateless era (2026-07-28 per-request ``_meta`` envelope, no
  ``initialize``, ``server/discover``, ``resultType``): the raw
  ``test_modern_*`` subprocess tests, which exchange plain JSON-RPC lines so
  no SDK client shape can hide a wire regression.
"""

from __future__ import annotations

import argparse
import asyncio
import builtins
import contextlib
import dataclasses
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
import uuid
from pathlib import Path
from types import SimpleNamespace

try:
    import mcp  # noqa: F401

    _MCP_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised on base installs
    _MCP_AVAILABLE = False

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
REPO_ROOT = Path(__file__).resolve().parents[1]

from coordinate.agent_interface import AgentInterface  # noqa: E402
from coordinate.audit import audit_workspace  # noqa: E402
from coordinate.completion import (  # noqa: E402
    ReceiptEvidence,
    compute_mark_done_fingerprints,
)
from coordinate.cli import build_parser  # noqa: E402
from coordinate.split_operations import apply_task_create_files  # noqa: E402
from coordinate.transitions import mark_done_files  # noqa: E402
from coordinate.db import (  # noqa: E402
    get_job,
    initialize,
    list_events,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.executor_capacity import (  # noqa: E402
    CapacityCatalog,
    CapacityPolicy,
    compute_capacity_catalog_hash,
    sync_capacity_catalog,
)
from coordinate.executor_identity import (  # noqa: E402
    ExecutorCatalog,
    ExecutorDefinition,
    ExecutorInstanceBinding,
    compute_executor_catalog_hash,
    sync_executor_catalog,
)
from coordinate.executor_routing import build_routing_request  # noqa: E402
from coordinate.job_repository import list_jobs  # noqa: E402
from coordinate.mcp_cli import INSTALL_HINT, handle_mcp_serve, register_mcp_command  # noqa: E402
from coordinate.operator import list_pending_actions, pending_snapshot_metadata  # noqa: E402
from coordinate.runtime import (  # noqa: E402
    RuntimeError,
    heartbeat_agent,
    list_agents,
    register_agent,
    submit_request,
)


def _make_interface(conn_factory, actor: str = "mcp") -> AgentInterface:
    return AgentInterface(connection_factory=conn_factory, actor=actor)


class FacadeTestBase(unittest.TestCase):
    """Shared disposable workspace: temp dir DB + one host + one agent."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "r1.sqlite3")
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

    def _interface(self, actor: str = "mcp") -> AgentInterface:
        return AgentInterface.from_config(
            dataclasses.replace(
                self._config(actor=actor), db_path=self.db_path
            )
        )

    def _config(self, actor: str = "mcp"):
        from coordinate.agent_interface import AgentInterfaceConfig

        return AgentInterfaceConfig(db_path=self.db_path, actor=actor)

    def _origin(self, message_id: str = "m1") -> dict:
        return {
            "platform": "discord",
            "destination": "ch",
            "message_id": message_id,
            "session_scope_id": "discord:ch",
        }

    def _reply(self) -> dict:
        return {"platform": "discord", "destination": "ch"}


class AgentInterfaceTests(FacadeTestBase):
    """Tool facade behavior vs the existing domain/CLI oracle."""

    # -- operator_pending -------------------------------------------------

    def test_operator_pending_matches_domain_oracle(self):
        upsert_task_mirror(
            self.conn,
            workspace_id="demo",
            task_id="t1",
            phase="ready",
            owner=None,
            branch=None,
            pr=None,
            payload={},
        )
        from coordinate.db import append_event

        append_event(
            self.conn,
            workspace_id="demo",
            event_type="plan.review_requested",
            actor="reviewer",
            task_id="t1",
            idempotency_key="test:plan.review_requested:t1",
            payload={"summary": "please review"},
        )
        expected_actions = [
            a.to_dict() for a in list_pending_actions(self.conn, "demo")
        ]
        expected_snapshot = pending_snapshot_metadata(self.conn, "demo")

        envelope = self._interface().operator_pending("demo")

        self.assertTrue(envelope["ok"])
        self.assertEqual(
            envelope["data"]["pending_actions"], expected_actions
        )
        self.assertEqual(envelope["data"]["snapshot"], expected_snapshot)

    def test_operator_pending_unknown_workspace_not_found(self):
        envelope = self._interface().operator_pending("nope")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")
        self.assertIn("unknown workspace", envelope["error"]["message"])

    def test_operator_pending_missing_workspace_id_invalid(self):
        envelope = self._interface().operator_pending(None)
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    # -- workspace_audit --------------------------------------------------

    def test_workspace_audit_matches_domain_oracle_refresh_false(self):
        expected = audit_workspace(
            self.conn, "demo", refresh=False
        ).to_dict()
        envelope = self._interface().workspace_audit("demo")
        self.assertTrue(envelope["ok"])
        # No harness files in the disposable dir: unavailable harness state is
        # audit data, not a transport failure, and freshness info is carried.
        self.assertEqual(envelope["data"], expected)
        self.assertFalse(envelope["data"]["harness_available"])
        self.assertIsNotNone(envelope["data"]["harness_error"])

    def test_workspace_audit_unknown_workspace_not_found(self):
        envelope = self._interface().workspace_audit("nope")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")

    def test_workspace_audit_missing_workspace_id_invalid(self):
        envelope = self._interface().workspace_audit(None)
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    # -- runtime_request_submit (exact) -----------------------------------

    def _strip_created_flags(self, data: dict) -> dict:
        data = dict(data)
        data.pop("event_created", None)
        data.pop("job_created", None)
        return data

    def test_submit_exact_matches_domain_oracle(self):
        args = {
            "workspace_id": "demo",
            "prompt": "hello",
            "origin": self._origin("m1"),
            "reply": self._reply(),
            "target_agent": "mac-codex",
            "idempotency_key": "k1",
        }
        # Domain/CLI oracle first; the facade replay of the same key must return
        # the identical stored event/job projection (creation flags aside).
        expected = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin=self._origin("m1"),
            reply=self._reply(),
            actor="mcp",
            idempotency_key="k1",
        )
        envelope = self._interface().runtime_request_submit(**args)
        self.assertTrue(envelope["ok"], envelope)
        self.assertFalse(envelope["data"]["event_created"])
        self.assertFalse(envelope["data"]["job_created"])
        self.assertEqual(
            self._strip_created_flags(envelope["data"]),
            self._strip_created_flags(expected.to_dict()),
        )

    def test_submit_actor_fixed_from_config(self):
        interface = self._interface(actor="bridge-42")
        envelope = interface.runtime_request_submit(
            workspace_id="demo",
            prompt="hello",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="mac-codex",
            idempotency_key="actor-k",
        )
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["data"]["event"]["actor"], "bridge-42")

    def test_submit_replay_no_duplicate_event_or_job(self):
        interface = self._interface()
        args = {
            "workspace_id": "demo",
            "prompt": "hello",
            "origin": self._origin("m1"),
            "reply": self._reply(),
            "target_agent": "mac-codex",
            "idempotency_key": "replay-k",
        }
        first = interface.runtime_request_submit(**args)
        second = interface.runtime_request_submit(**args)
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertTrue(first["data"]["event_created"])
        self.assertTrue(first["data"]["job_created"])
        self.assertFalse(second["data"]["event_created"])
        self.assertFalse(second["data"]["job_created"])
        self.assertEqual(
            second["data"]["event"]["id"], first["data"]["event"]["id"]
        )
        self.assertEqual(
            second["data"]["job"]["id"], first["data"]["job"]["id"]
        )
        with initialize(self.db_path) as conn:
            self.assertEqual(
                len(
                    [
                        e
                        for e in list_events(conn)
                        if e["event_type"] == "request.received"
                    ]
                ),
                1,
            )
            self.assertEqual(len(list_jobs(conn)), 1)

    def test_submit_replay_conflict_fails_closed(self):
        interface = self._interface()
        common = {
            "workspace_id": "demo",
            "origin": self._origin("m1"),
            "reply": self._reply(),
            "target_agent": "mac-codex",
            "idempotency_key": "conflict-k",
        }
        first = interface.runtime_request_submit(prompt="hello", **common)
        second = interface.runtime_request_submit(prompt="DIFFERENT", **common)
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["error"]["code"], "conflict")
        # The wire message is the static conflict text, never the domain text.
        self.assertEqual(second["error"]["message"], "request replay conflict")
        with initialize(self.db_path) as conn:
            self.assertEqual(len(list_jobs(conn)), 1)

    # -- fail-closed submit cases ------------------------------------------

    def test_submit_exact_plus_routed_invalid(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="mac-codex",
            routing_request={"required_capabilities": ["coding"]},
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_submit_neither_mode_invalid(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_submit_unknown_workspace_not_found(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="nope",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="mac-codex",
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")

    def test_submit_unknown_agent_not_found(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="ghost",
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")

    def test_submit_missing_host_profile_not_found(self):
        register_agent(
            self.conn,
            agent_id="orphan-agent",
            host_id="orphan-host",
            capabilities={},
        )
        self.conn.commit()
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="orphan-agent",
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")
        self.assertIn("host profile", envelope["error"]["message"])

    def test_submit_unknown_task_not_found(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="mac-codex",
            task_id="no-such-task",
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")
        self.assertIn("task mirror", envelope["error"]["message"])

    def test_submit_empty_prompt_invalid(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="   ",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="mac-codex",
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_submit_empty_idempotency_key_invalid(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            target_agent="mac-codex",
            idempotency_key="",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")
        self.assertIn("idempotency_key", envelope["error"]["message"])

    def test_submit_invalid_origin_type_invalid(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin="not-an-object",
            reply=self._reply(),
            target_agent="mac-codex",
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_submit_forged_routing_request_id_invalid(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            routing_request={
                "required_capabilities": ["coding"],
                "routing_request_id": "forged",
            },
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")
        # The forged field name must never be echoed back onto the wire.
        self.assertNotIn("routing_request_id", envelope["error"]["message"])
        self.assertEqual(
            envelope["error"]["message"], "unknown routing_request field"
        )

    def test_submit_override_pair_required_together(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            routing_request={
                "required_capabilities": ["coding"],
                "operator_override_agent_id": "mac-codex",
            },
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_submit_bad_capabilities_type_invalid(self):
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            routing_request={"required_capabilities": "coding"},
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    def test_submit_routing_missing_required_capabilities_invalid(self):
        # Direct facade call (no SDK layer): a routing_request without the
        # required raw builder field must fail closed, never silently route
        # with an empty capability set.
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m1"),
            reply=self._reply(),
            routing_request={"executor_definition_id": "coder"},
            idempotency_key="k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")
        self.assertIn("required_capabilities", envelope["error"]["message"])

    # -- routed happy path -------------------------------------------------

    def _sync_routing_catalog(self):
        heartbeat_agent(
            self.conn, agent_id="mac-codex", host_id="mac"
        )
        from coordinate.db import set_workspace_agent

        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="12345",
            actor="test",
            reason="test",
        )
        definitions = (
            ExecutorDefinition(
                id="coder",
                provider="kimi-code",
                adapter="omp",
                capabilities=("coding",),
            ),
        )
        bindings = (
            ExecutorInstanceBinding(
                agent_id="mac-codex",
                executor_definition_id="coder",
                runner_profile_id="mac-codex",
                enabled=True,
            ),
        )
        catalog = ExecutorCatalog(
            source_id="test.discord",
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
        policies = (
            CapacityPolicy(agent_id="mac-codex", max_concurrent_jobs=2),
        )
        cap = CapacityCatalog(
            source_id="test.discord.capacity",
            source_version=1,
            catalog_hash="",
            source_path="/dev/null",
            policies=policies,
        )
        cap = dataclasses.replace(
            cap, catalog_hash=compute_capacity_catalog_hash(cap)
        )
        sync_capacity_catalog(self.conn, cap)
        self.conn.commit()

    def test_submit_routed_happy_path_matches_oracle(self):
        self._sync_routing_catalog()
        routing = build_routing_request(required_capabilities=["coding"])
        interface = self._interface()
        # Domain oracle first; the facade replay must return the same decision.
        expected = submit_request(
            self.conn,
            workspace_id="demo",
            prompt="routed hello",
            origin=self._origin("m2"),
            reply=self._reply(),
            actor="mcp",
            routing_request=routing,
            idempotency_key="routed-k",
        )
        self.assertTrue(expected.event_created)
        envelope = interface.runtime_request_submit(
            workspace_id="demo",
            prompt="routed hello",
            origin=self._origin("m2"),
            reply=self._reply(),
            routing_request={
                "required_capabilities": ["coding"],
            },
            idempotency_key="routed-k",
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertFalse(envelope["data"]["event_created"])
        self.assertEqual(
            self._strip_created_flags(envelope["data"]),
            self._strip_created_flags(expected.to_dict()),
        )
        self.assertEqual(
            envelope["data"]["event"]["target"], "mac-codex"
        )

    def test_submit_routed_replay_returns_stored_decision(self):
        self._sync_routing_catalog()
        interface = self._interface()
        args = {
            "workspace_id": "demo",
            "prompt": "routed hello",
            "origin": self._origin("m3"),
            "reply": self._reply(),
            "routing_request": {"required_capabilities": ["coding"]},
            "idempotency_key": "routed-replay-k",
        }
        first = interface.runtime_request_submit(**args)
        second = interface.runtime_request_submit(**args)
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertFalse(second["data"]["event_created"])
        self.assertFalse(second["data"]["job_created"])
        self.assertEqual(
            second["data"]["event"]["id"], first["data"]["event"]["id"]
        )

    def test_submit_routed_conflict_payload_fails_closed(self):
        self._sync_routing_catalog()
        interface = self._interface()
        common = {
            "workspace_id": "demo",
            "origin": self._origin("m4"),
            "reply": self._reply(),
            "routing_request": {"required_capabilities": ["coding"]},
            "idempotency_key": "routed-conflict-k",
        }
        first = interface.runtime_request_submit(prompt="a", **common)
        second = interface.runtime_request_submit(prompt="b", **common)
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["error"]["code"], "conflict")

    def test_submit_routed_no_candidate_invalid(self):
        # No catalog synced: routing must fail closed before any write.
        envelope = self._interface().runtime_request_submit(
            workspace_id="demo",
            prompt="x",
            origin=self._origin("m5"),
            reply=self._reply(),
            routing_request={"required_capabilities": ["coding"]},
            idempotency_key="routed-nc-k",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    # -- runtime_job_get ---------------------------------------------------

    def test_job_get_matches_domain_oracle(self):
        result = submit_request(
            self.conn,
            workspace_id="demo",
            target_agent="mac-codex",
            prompt="hello",
            origin=self._origin("m1"),
            reply=self._reply(),
            actor="bridge",
            idempotency_key="jobget-k",
        )
        job_id = result.job["id"]
        expected = row_to_dict(get_job(self.conn, job_id))
        envelope = self._interface().runtime_job_get(job_id)
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["data"], expected)

    def test_job_get_missing_not_found(self):
        envelope = self._interface().runtime_job_get("request:missing")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")

    def test_job_get_missing_id_invalid(self):
        envelope = self._interface().runtime_job_get(None)
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "invalid_request")

    # -- runtime_agent_list ------------------------------------------------

    def test_agent_list_matches_domain_oracle(self):
        register_agent(
            self.conn,
            agent_id="second-agent",
            host_id="mac",
            capabilities={"models": ["claude"]},
        )
        self.conn.commit()
        expected = list_agents(self.conn)
        envelope = self._interface().runtime_agent_list()
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["data"]["agents"], expected)
        self.assertEqual(len(envelope["data"]["agents"]), 2)

    def test_agent_list_empty_db(self):
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        db_path = os.path.join(other.name, "empty.sqlite3")
        interface = _make_interface(lambda: initialize(db_path))
        envelope = interface.runtime_agent_list()
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["data"]["agents"], [])


class R5BFacadeLifecycleTests(unittest.TestCase):
    """R5B: six new typed tools' facade behavior over the real domain.

    Covers task-create record half (deployed envelope re-derivation, zero
    mutation on drift) and the completion prepare/preflight/claim/apply/
    consume lifecycle with the request-scoped actor fixed by the facade.
    """

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))
        self.workspace_path = self.tmp / "workspace"
        self.workspace_path.mkdir()
        self.harness_root = self.tmp / "docs"
        self.harness_root.mkdir()
        self.checklist_path = self.harness_root / "mvp-checklist.json"
        self.checklist_path.write_text(json.dumps({
            "project": "demo",
            "harness_root": ".",
            "version": 1,
            "updated_at": "2026-08-11",
            "items": [],
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self.plan = self.workspace_path / "plans" / "foo.md"
        self.plan.parent.mkdir(parents=True)
        self.plan.write_text("# plan\n", encoding="utf-8")

        self.db_path = os.path.join(self.tmp.name, "r5b.sqlite3")
        self.conn = initialize(self.db_path)
        self.addCleanup(self.conn.close)
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.workspace_path),
            harness_root=str(self.harness_root),
        )
        self.conn.commit()
        self.operation_id = str(uuid.uuid4())
        self.files_result = apply_task_create_files(
            workspace_path=str(self.workspace_path),
            harness_root=str(self.harness_root),
            task_id="task-1",
            plan_doc="plans/foo.md",
            title="Task Title",
            phase="ready",
            priority="p1",
            operation_id=self.operation_id,
            workspace_id="demo",
        ).to_dict()

    def _factory(self):
        from coordinate.db import initialize as _init

        return lambda: _init(self.db_path)

    def _interface(self, actor: str = "mcp-r5b") -> AgentInterface:
        return AgentInterface(connection_factory=self._factory(), actor=actor)

    # -- task_create_record ------------------------------------------------

    def _record_args(self):
        return dict(
            workspace_id="demo",
            operation_id=self.operation_id,
            input_fingerprint=self.files_result["input_fingerprint"],
            before_fingerprint=self.files_result["before_fingerprint"],
            after_fingerprint=self.files_result["after_fingerprint"],
            task_id="task-1",
            plan_doc="plans/foo.md",
        )

    def test_task_create_record_happy_path_and_idempotent_replay(self):
        interface = self._interface()
        envelope = interface.task_create_record(**self._record_args())
        self.assertTrue(envelope["ok"], envelope)
        data = envelope["data"]
        self.assertTrue(data["event_created"])
        self.assertEqual(data["event"]["actor"], "mcp-r5b")

        replay = interface.task_create_record(**self._record_args())
        self.assertTrue(replay["ok"])
        self.assertFalse(replay["data"]["event_created"])
        self.assertEqual(replay["data"]["event"]["id"], data["event"]["id"])

    def test_task_create_record_actor_and_payload_not_expressible(self):
        interface = self._interface()
        with self.assertRaises(TypeError):
            interface.task_create_record(**self._record_args(), actor="intruder")
        with self.assertRaises(TypeError):
            interface.task_create_record(**self._record_args(), payload={"forged": True})
        with self.assertRaises(TypeError):
            interface.task_create_record(**self._record_args(), requester="intruder")

    def test_task_create_record_missing_deployed_zero_mutation(self):
        (self.harness_root / "mvp-checklist.json").unlink()
        interface = self._interface()
        envelope = interface.task_create_record(**self._record_args())
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "files_not_deployed")
        # Static message: never echoes the temp path.
        self.assertNotIn(self.tmp.name, envelope["error"]["message"])
        self.assertIsNone(envelope["data"])

    def test_task_create_record_drift_zero_mutation(self):
        data = json.loads(self.checklist_path.read_text(encoding="utf-8"))
        data["items"][0]["title"] = "Drifted Title"
        self.checklist_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
        )
        interface = self._interface()
        envelope = interface.task_create_record(**self._record_args())
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "fingerprint_drift")
        self.assertNotIn(self.tmp.name, envelope["error"]["message"])
        # Zero DB mutation: no task mirror, no plan.ready event.
        conn = self._factory()()
        try:
            mirror = conn.execute(
                "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                ("demo", "task-1"),
            ).fetchone()
            ready = conn.execute(
                "SELECT * FROM events WHERE event_type = 'plan.ready'"
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNone(mirror)
        self.assertIsNone(ready)

    def test_task_create_record_invalid_operation_id_preserves_reason(self):
        interface = self._interface()
        args = self._record_args()
        args["operation_id"] = "not-a-uuid"
        envelope = interface.task_create_record(**args)
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "validation_error")

    # -- completion lifecycle ----------------------------------------------

    def _gate_adapter_factory(self, workflow_status="review_approved"):
        """Fake HarnessAdapter: gate via current_item, checklist from file."""
        from coordinate.harness import HarnessError

        def _make(workspace):
            class _Fake:
                def __init__(self, workspace):
                    self.workspace = workspace

                def refresh_state(self):
                    return {"current_item": {
                        "id": "task-1",
                        "workflow": {"status": workflow_status, "branch": "feat-x"},
                        "status": "doing",
                    }}

                def read_state(self):
                    return self.refresh_state()

                def read_checklist(self):
                    try:
                        return json.loads(Path(self.workspace.harness_root).joinpath(
                            "mvp-checklist.json",
                        ).read_text(encoding="utf-8"))
                    except OSError as exc:
                        raise HarnessError(f"checklist gone: {exc}") from exc

            return _Fake(workspace)

        return _make

    def test_completion_prepare_actor_fixed_and_gate_fail(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            envelope = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        self.assertTrue(envelope["ok"], envelope)
        receipt = envelope["data"]
        self.assertEqual(receipt["requester"], "mcp-r5b")
        self.assertEqual(receipt["authorized_actor"], "mcp-r5b")
        # Caller override is not expressible in the facade signature.
        with self.assertRaises(TypeError):
            interface.completion_prepare(
                workspace_id="demo", task_id="task-1", requester="intruder",
            )
        with self.assertRaises(TypeError):
            interface.completion_prepare(
                workspace_id="demo", task_id="task-1", authorized_actor="intruder",
            )

    def test_completion_prepare_gate_not_passed(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(workflow_status="doing"),
        ):
            envelope = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "gate_not_passed")

    def test_completion_preflight_unknown_receipt(self):
        interface = self._interface()
        envelope = interface.completion_preflight(
            workspace_id="demo", receipt_id="missing-receipt",
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "unknown_receipt")

    def test_completion_preflight_workspace_mismatch(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            prepared = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        receipt = prepared["data"]
        envelope = interface.completion_preflight(
            workspace_id="other", receipt_id=receipt["receipt_id"],
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "workspace_mismatch")

    def test_completion_preflight_expired_authorized_receipt(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            prepared = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        receipt = prepared["data"]
        conn = self._factory()()
        try:
            conn.execute(
                "UPDATE events SET payload_json = json_set("
                "payload_json, '$.expires_at', ?) WHERE id = ?",
                ("2020-01-01T00:00:00Z", receipt["event"]["id"]),
            )
            conn.commit()
        finally:
            conn.close()
        envelope = interface.completion_preflight(
            workspace_id="demo", receipt_id=receipt["receipt_id"],
        )
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "expired")

    def test_completion_lifecycle_end_to_end(self):
        """prepare -> preflight -> claim -> local mark-done -> apply -> consume
        through the facade with the principal actor."""
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            envelope = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        self.assertTrue(envelope["ok"], envelope)
        receipt = envelope["data"]
        self.assertEqual(receipt["authorized_actor"], "mcp-r5b")

        pre = interface.completion_preflight(
            workspace_id="demo", receipt_id=receipt["receipt_id"],
        )
        self.assertTrue(pre["ok"], pre)
        self.assertTrue(pre["data"]["ok"])
        self.assertEqual(pre["data"]["workspace_id"], "demo")
        self.assertEqual(pre["data"]["task_id"], "task-1")
        self.assertEqual(pre["data"]["status"], "authorized")

        fps = compute_mark_done_fingerprints(
            harness_root=str(self.harness_root), task_id="task-1",
        )
        claim = interface.completion_claim(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            task_id="task-1",
            before_fingerprint=receipt["harness_fingerprint"],
            expected_after_fingerprint=fps.after_fingerprint,
        )
        self.assertTrue(claim["ok"], claim)
        self.assertEqual(claim["data"]["authorized_actor"], "mcp-r5b")

        claim2 = interface.completion_claim(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            task_id="task-1",
            before_fingerprint=receipt["harness_fingerprint"],
            expected_after_fingerprint=fps.after_fingerprint,
        )
        self.assertTrue(claim2["ok"])
        self.assertTrue(claim2["data"]["idempotent"])

        evidence = ReceiptEvidence(
            receipt_id=claim["data"]["receipt_id"],
            before_fingerprint=claim["data"]["before_fingerprint"],
            after_fingerprint=claim["data"]["expected_after_fingerprint"],
        )
        written = mark_done_files(
            workspace_path=str(self.workspace_path),
            harness_root=str(self.harness_root),
            task_id="task-1",
            actor="mcp-r5b",
            verification="verified by facade test",
            receipt=evidence,
        )
        self.assertTrue(written.checklist_changed)
        self.assertEqual(written.after_fingerprint, fps.after_fingerprint)

        applied = interface.completion_apply(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            task_id="task-1",
            after_fingerprint=written.after_fingerprint,
        )
        self.assertTrue(applied["ok"], applied)

        consumed = interface.completion_consume(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            verification="verified by facade test",
        )
        self.assertTrue(consumed["ok"], consumed)
        self.assertTrue(consumed["data"]["event_created"])
        self.assertEqual(consumed["data"]["event"]["event_type"], "task.done")
        self.assertEqual(consumed["data"]["event"]["actor"], "mcp-r5b")

        replay = interface.completion_consume(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            verification="verified by facade test",
        )
        self.assertTrue(replay["ok"])
        self.assertFalse(replay["data"]["event_created"])

    def test_completion_claim_fingerprint_mismatch(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            prepared = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        receipt = prepared["data"]
        fps = compute_mark_done_fingerprints(
            harness_root=str(self.harness_root), task_id="task-1",
        )
        claim = interface.completion_claim(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            task_id="task-1",
            before_fingerprint="0" * 64,
            expected_after_fingerprint=fps.after_fingerprint,
        )
        self.assertFalse(claim["ok"])
        self.assertEqual(claim["error"]["code"], "before_fingerprint_mismatch")

    def test_completion_apply_requires_claim(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            prepared = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        receipt = prepared["data"]
        applied = interface.completion_apply(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            task_id="task-1",
            after_fingerprint="a" * 64,
        )
        self.assertFalse(applied["ok"])
        self.assertEqual(applied["error"]["code"], "not_claimed")

    def test_completion_consume_workspace_binding(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            prepared = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        receipt = prepared["data"]
        consumed = interface.completion_consume(
            workspace_id="other",
            receipt_id=receipt["receipt_id"],
            verification="v",
        )
        self.assertFalse(consumed["ok"])
        self.assertEqual(consumed["error"]["code"], "workspace_mismatch")
        conn = self._factory()()
        try:
            done = conn.execute(
                "SELECT * FROM events WHERE event_type = 'task.done'"
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNone(done)

    def test_completion_consume_deployed_not_done(self):
        interface = self._interface()
        with unittest.mock.patch(
            "coordinate.completion.HarnessAdapter",
            self._gate_adapter_factory(),
        ):
            prepared = interface.completion_prepare(
                workspace_id="demo", task_id="task-1",
            )
        receipt = prepared["data"]
        fps = compute_mark_done_fingerprints(
            harness_root=str(self.harness_root), task_id="task-1",
        )
        claim = interface.completion_claim(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            task_id="task-1",
            before_fingerprint=receipt["harness_fingerprint"],
            expected_after_fingerprint=fps.after_fingerprint,
        )
        self.assertTrue(claim["ok"])
        applied = interface.completion_apply(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            task_id="task-1",
            after_fingerprint=fps.after_fingerprint,
        )
        self.assertTrue(applied["ok"])
        # No local mark-done: deployed harness is still doing.
        consumed = interface.completion_consume(
            workspace_id="demo",
            receipt_id=receipt["receipt_id"],
            verification="v",
        )
        self.assertFalse(consumed["ok"])
        self.assertEqual(consumed["error"]["code"], "deployed_not_done")


class ConnectionClosureTests(unittest.TestCase):
    """P1-1: every tool call must actually close its own SQLite connection.

    ``sqlite3.Connection.__exit__`` only commits/rolls back; the facade wraps
    the factory in ``contextlib.closing`` so success, domain-error and
    unexpected-failure paths all close the connection.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "closure.sqlite3")
        conn = initialize(self.db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        conn.close()

    def _tracked_interface(self, created):
        def factory():
            conn = initialize(self.db_path)
            created.append(conn)
            return conn

        return AgentInterface(connection_factory=factory, actor="mcp"), created

    @staticmethod
    def _assert_closed(conn):
        with unittest.TestCase().assertRaises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")

    def test_success_path_closes_connection(self):
        interface, created = self._tracked_interface([])
        envelope = interface.operator_pending("demo")
        self.assertTrue(envelope["ok"])
        self.assertEqual(len(created), 1)
        self._assert_closed(created[0])

    def test_not_found_path_closes_connection(self):
        interface, created = self._tracked_interface([])
        envelope = interface.operator_pending("nope")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "not_found")
        self.assertEqual(len(created), 1)
        self._assert_closed(created[0])

    def test_unexpected_failure_path_closes_connection(self):
        interface, created = self._tracked_interface([])
        with unittest.mock.patch(
            "coordinate.agent_interface.list_pending_actions",
            side_effect=TypeError("boom"),
        ):
            envelope = interface.operator_pending("demo")
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error"]["code"], "internal")
        self.assertEqual(len(created), 1)
        self._assert_closed(created[0])

    def test_submit_path_closes_connection(self):
        conn = initialize(self.db_path)
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        register_agent(
            conn, agent_id="mac-codex", host_id="mac", capabilities={}
        )
        conn.close()
        interface, created = self._tracked_interface([])
        envelope = interface.runtime_request_submit(
            workspace_id="demo",
            prompt="hello",
            origin={
                "platform": "discord",
                "destination": "ch",
                "message_id": "m1",
                "session_scope_id": "discord:ch",
            },
            reply={"platform": "discord", "destination": "ch"},
            target_agent="mac-codex",
            idempotency_key="closure-k",
        )
        self.assertTrue(envelope["ok"], envelope)
        self.assertEqual(len(created), 1)
        self._assert_closed(created[0])
        # The transaction context commits before close: reopening the DB must
        # show the job persisted, never rolled back by connection teardown.
        from coordinate.job_repository import list_jobs

        with initialize(self.db_path) as conn:
            self.assertEqual(len(list_jobs(conn)), 1)


class ErrorClassificationTests(unittest.TestCase):
    """R2: replay-conflict signatures classify as conflict; wire messages are
    static, short and never echo domain text, paths or long inputs."""

    MAX_MESSAGE_LEN = 64

    def _classify(self, exc):
        from coordinate.agent_interface import _classify_error

        code, message = _classify_error(exc)
        self.assertLessEqual(len(message), self.MAX_MESSAGE_LEN, message)
        return code, message

    def test_plain_replay_conflict_signature(self):
        code, message = self._classify(
            RuntimeError(
                "request replay: prompt conflicts with stored event"
            )
        )
        self.assertEqual(code, "conflict")
        self.assertEqual(message, "request replay conflict")

    def test_context_replay_conflict_signature(self):
        code, message = self._classify(
            RuntimeError(
                "request replay context conflict: invalid execution context: "
                "path is outside control workspace '/private/workspace'"
            )
        )
        self.assertEqual(code, "conflict")
        self.assertEqual(message, "request replay conflict")
        self.assertNotIn("/private/workspace", message)

    def test_plain_validation_stays_invalid_request(self):
        code, message = self._classify(ValueError("prompt is required"))
        self.assertEqual(code, "invalid_request")
        self.assertEqual(message, "invalid request")

    def test_routing_error_static_message(self):
        from coordinate.executor_routing import ExecutorRoutingError

        code, message = self._classify(
            ExecutorRoutingError("executor_route_no_candidate")
        )
        self.assertEqual(code, "invalid_request")
        self.assertEqual(message, "invalid routing request")

    def test_long_domain_value_never_reaches_wire(self):
        long_input = "x" * 600
        code, message = self._classify(
            RuntimeError(
                "invalid execution context: path is outside control workspace "
                f"'/private/workspace': '/tmp/{long_input}'"
            )
        )
        self.assertEqual(code, "invalid_request")
        self.assertEqual(message, "invalid request")
        self.assertNotIn(long_input, message)
        self.assertNotIn("/private/workspace", message)

    def test_unknown_field_name_never_reaches_wire(self):
        code, message = self._classify(
            ValueError("unknown routing_request field: routing_request_id")
        )
        self.assertEqual(code, "invalid_request")
        self.assertEqual(message, "invalid request")
        self.assertNotIn("routing_request_id", message)

    def test_not_found_markers_map_to_static_messages(self):
        from coordinate.agent_interface import (
            MESSAGE_HOST_PROFILE_NOT_FOUND,
            MESSAGE_TASK_MIRROR_NOT_FOUND,
            MESSAGE_UNKNOWN_AGENT,
            MESSAGE_UNKNOWN_WORKSPACE,
        )

        cases = [
            ("unknown workspace: demo", MESSAGE_UNKNOWN_WORKSPACE),
            ("unknown agent: mac-codex", MESSAGE_UNKNOWN_AGENT),
            ("task mirror not found: ws/t", MESSAGE_TASK_MIRROR_NOT_FOUND),
            (
                "agent mac-codex has no host_id",
                MESSAGE_HOST_PROFILE_NOT_FOUND,
            ),
            (
                "workspace demo has no host profile for host mac",
                MESSAGE_HOST_PROFILE_NOT_FOUND,
            ),
        ]
        for text, expected in cases:
            with self.subTest(text=text):
                code, message = self._classify(ValueError(text))
                self.assertEqual(code, "not_found")
                self.assertEqual(message, expected)

    def test_key_error_maps_to_job_not_found(self):
        code, message = self._classify(KeyError("request:missing"))
        self.assertEqual(code, "not_found")
        self.assertEqual(message, "job not found")
        self.assertNotIn("request:missing", message)

    def test_storage_error_static_unavailable(self):
        code, message = self._classify(
            sqlite3.OperationalError("cannot open database file /private/db")
        )
        self.assertEqual(code, "unavailable")
        self.assertEqual(message, "coordinate storage or filesystem is unavailable")
        self.assertNotIn("/private/db", message)

    def test_unknown_exception_static_internal_with_stderr_log(self):
        with self.assertLogs("coordinate.agent_interface", level="ERROR"):
            code, message = self._classify(TypeError("secret-detail boom"))
        self.assertEqual(code, "internal")
        self.assertEqual(message, "internal error")
        self.assertNotIn("secret-detail", message)

    def test_all_static_messages_are_bounded(self):
        from coordinate.agent_interface import (
            MESSAGE_HOST_PROFILE_NOT_FOUND,
            MESSAGE_INTERNAL,
            MESSAGE_INVALID_REQUEST,
            MESSAGE_INVALID_ROUTING_REQUEST,
            MESSAGE_JOB_NOT_FOUND,
            MESSAGE_REPLAY_CONFLICT,
            MESSAGE_TASK_MIRROR_NOT_FOUND,
            MESSAGE_UNAVAILABLE,
            MESSAGE_UNKNOWN_AGENT,
            MESSAGE_UNKNOWN_WORKSPACE,
        )

        messages = [
            MESSAGE_HOST_PROFILE_NOT_FOUND,
            MESSAGE_INTERNAL,
            MESSAGE_INVALID_REQUEST,
            MESSAGE_INVALID_ROUTING_REQUEST,
            MESSAGE_JOB_NOT_FOUND,
            MESSAGE_REPLAY_CONFLICT,
            MESSAGE_TASK_MIRROR_NOT_FOUND,
            MESSAGE_UNAVAILABLE,
            MESSAGE_UNKNOWN_AGENT,
            MESSAGE_UNKNOWN_WORKSPACE,
        ]
        for message in messages:
            self.assertLessEqual(len(message), self.MAX_MESSAGE_LEN, message)

    def test_wire_conflict_message_is_static_via_facade(self):
        # End-to-end: a real context-conflict classification surfaces the static
        # message on the envelope, with the detail only in stderr.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "r2.sqlite3")
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=tmp.name,
            harness_root=tmp.name,
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path=tmp.name,
            harness_root=tmp.name,
        )
        register_agent(
            conn, agent_id="mac-codex", host_id="mac", capabilities={}
        )
        conn.close()

        from coordinate.agent_interface import AgentInterfaceConfig

        interface = AgentInterface.from_config(
            AgentInterfaceConfig(db_path=db_path, actor="mcp")
        )
        common = {
            "workspace_id": "demo",
            "origin": {
                "platform": "discord",
                "destination": "ch",
                "message_id": "m1",
                "session_scope_id": "discord:ch",
            },
            "reply": {"platform": "discord", "destination": "ch"},
            "target_agent": "mac-codex",
            "idempotency_key": "r2-conflict-k",
        }
        first = interface.runtime_request_submit(prompt="a", **common)
        second = interface.runtime_request_submit(prompt="b", **common)
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["error"]["code"], "conflict")
        self.assertEqual(second["error"]["message"], "request replay conflict")


class InternalErrorBoundaryTests(unittest.TestCase):
    """Unknown exceptions must stay bounded on the wire; stderr carries detail."""

    def _exploding_interface(self):
        class _Exploding(AgentInterface):
            def operator_pending(self, workspace_id):
                raise TypeError("secret-detail boom")

        return _Exploding(
            connection_factory=lambda: None, actor="test"
        )

    @unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
    def test_unknown_exception_bounded_internal(self):
        from coordinate.mcp_server import build_mcp_server

        server = build_mcp_server(self._exploding_interface())
        result = asyncio.run(
            server.call_tool(
                "coordinate.operator_pending", {"workspace_id": "demo"}
            )
        )
        self.assertTrue(result.is_error)
        self.assertEqual(
            result.structured_content["error"]["code"], "internal"
        )
        self.assertEqual(
            result.structured_content["error"]["message"], "internal error"
        )
        # The wire text is the bounded envelope JSON, never the raw exception.
        text = result.content[0].text
        self.assertIn('"internal error"', text)
        self.assertNotIn("secret-detail boom", text)
        self.assertNotIn("Traceback", text)

    @unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
    def test_stderr_receives_diagnostic(self):
        from coordinate.mcp_server import build_mcp_server

        server = build_mcp_server(self._exploding_interface())
        with self.assertLogs("coordinate.mcp_server", level="ERROR"):
            asyncio.run(
                server.call_tool(
                    "coordinate.operator_pending", {"workspace_id": "demo"}
                )
            )

    @unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
    def test_storage_failure_maps_to_unavailable(self):
        from coordinate.mcp_server import build_mcp_server
        from coordinate.agent_interface import AgentInterface

        import sqlite3 as _sqlite3

        def _broken_factory():
            raise _sqlite3.OperationalError("cannot open database file")

        server = build_mcp_server(
            AgentInterface(connection_factory=_broken_factory, actor="test")
        )
        result = asyncio.run(
            server.call_tool(
                "coordinate.operator_pending", {"workspace_id": "demo"}
            )
        )
        self.assertTrue(result.is_error)
        self.assertEqual(
            result.structured_content["error"]["code"], "unavailable"
        )
        self.assertNotIn("cannot open database file", result.content[0].text)


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class MCPServerToolContractTests(unittest.TestCase):
    """Tool list, input schemas, annotations and envelope shape."""

    EXPECTED_TOOLS = [
        "coordinate.operator_pending",
        "coordinate.workspace_audit",
        "coordinate.runtime_request_submit",
        "coordinate.runtime_job_get",
        "coordinate.runtime_agent_list",
        "coordinate.task_create_record",
        "coordinate.completion_prepare",
        "coordinate.completion_preflight",
        "coordinate.completion_claim",
        "coordinate.completion_apply",
        "coordinate.completion_consume",
        "coordinate.channel_create",
    ]

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        db_path = os.path.join(self.tmp.name, "r1.sqlite3")
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        register_agent(
            conn, agent_id="mac-codex", host_id="mac", capabilities={}
        )
        conn.close()
        from coordinate.agent_interface import AgentInterfaceConfig

        from coordinate.mcp_server import build_mcp_server

        interface = AgentInterface.from_config(
            AgentInterfaceConfig(db_path=db_path, actor="mcp")
        )
        self.server = build_mcp_server(interface)

    def test_exactly_twelve_tools(self):
        tools = asyncio.run(self.server.list_tools())
        names = [t.name for t in tools]
        self.assertEqual(sorted(names), sorted(self.EXPECTED_TOOLS))
        self.assertEqual(len(names), 12)

    def test_annotations_per_tool(self):
        tools = asyncio.run(self.server.list_tools())
        by_name = {t.name: t.annotations for t in tools}
        for name in (
            "coordinate.operator_pending",
            "coordinate.workspace_audit",
            "coordinate.runtime_job_get",
            "coordinate.runtime_agent_list",
        ):
            ann = by_name[name]
            self.assertIsNotNone(ann)
            self.assertIs(ann.read_only_hint, True)
            self.assertIs(ann.open_world_hint, False)
        submit_ann = by_name["coordinate.runtime_request_submit"]
        self.assertIs(submit_ann.read_only_hint, False)
        self.assertIs(submit_ann.destructive_hint, False)
        self.assertIs(submit_ann.idempotent_hint, True)
        self.assertIs(submit_ann.open_world_hint, False)

    def test_required_fields_per_tool(self):
        tools = asyncio.run(self.server.list_tools())
        by_name = {t.name: t for t in tools}
        self.assertEqual(
            by_name["coordinate.operator_pending"].input_schema.get("required"),
            ["workspace_id"],
        )
        self.assertEqual(
            by_name["coordinate.workspace_audit"].input_schema.get("required"),
            ["workspace_id"],
        )
        self.assertEqual(
            by_name["coordinate.runtime_job_get"].input_schema.get("required"),
            ["job_id"],
        )
        # No required fields: an absent ``required`` key is the empty set.
        self.assertFalse(
            by_name["coordinate.runtime_agent_list"].input_schema.get("required")
        )
        submit = by_name["coordinate.runtime_request_submit"].input_schema
        self.assertEqual(
            submit.get("required"),
            ["workspace_id", "prompt", "origin", "reply", "idempotency_key"],
        )
        # Required fields must not carry a null default in the schema.
        for name in (
            "coordinate.operator_pending",
            "coordinate.workspace_audit",
            "coordinate.runtime_job_get",
        ):
            tool = by_name[name]
            field = next(iter(tool.input_schema["properties"].values()))
            self.assertNotIn("default", field)
            self.assertEqual(field.get("type"), "string")

    def test_submit_input_schema_fields(self):
        tools = asyncio.run(self.server.list_tools())
        submit = next(
            t for t in tools if t.name == "coordinate.runtime_request_submit"
        )
        props = set(submit.input_schema.get("properties", {}))
        self.assertEqual(
            props,
            {
                "workspace_id",
                "prompt",
                "origin",
                "reply",
                "task_id",
                "target_agent",
                "routing_request",
                "worktree_path",
                "idempotency_key",
            },
        )

    def test_routing_input_schema_exact(self):
        tools = asyncio.run(self.server.list_tools())
        submit = next(
            t for t in tools if t.name == "coordinate.runtime_request_submit"
        )
        routing = submit.input_schema["properties"]["routing_request"]
        # The typed model is referenced from $defs; the property itself allows null.
        refs = [
            item["$ref"]
            for item in routing.get("anyOf", [])
            if isinstance(item, dict) and "$ref" in item
        ]
        self.assertEqual(refs, ["#/$defs/RoutingRequestInput"])
        definitions = submit.input_schema.get("$defs", {})
        self.assertIn("RoutingRequestInput", definitions)
        model = definitions["RoutingRequestInput"]
        self.assertEqual(
            set(model.get("properties", {})),
            {
                "required_capabilities",
                "executor_definition_id",
                "preferred_host_id",
                "operator_override_agent_id",
                "operator_override_reason",
            },
        )
        self.assertEqual(model.get("required"), ["required_capabilities"])
        self.assertIs(model.get("additionalProperties"), False)

    def test_r5b_input_schemas_are_single_typed_forbid_models(self):
        """Each R5B tool takes exactly one ``input`` argument whose model is
        the flat scalar contract with ``additionalProperties: false``."""
        tools = asyncio.run(self.server.list_tools())
        by_name = {t.name: t for t in tools}
        expected = {
            "coordinate.task_create_record": {
                "TaskCreateRecordInput": [
                    "workspace_id", "operation_id", "input_fingerprint",
                    "before_fingerprint", "after_fingerprint", "task_id",
                    "plan_doc",
                ],
                "optional": ["title", "phase", "owner", "branch", "idempotency_key"],
            },
            "coordinate.completion_prepare": {
                "CompletionPrepareInput": ["workspace_id", "task_id"],
                "optional": [],
            },
            "coordinate.completion_preflight": {
                "CompletionPreflightInput": ["workspace_id", "receipt_id"],
                "optional": [],
            },
            "coordinate.completion_claim": {
                "CompletionClaimInput": [
                    "workspace_id", "receipt_id", "task_id",
                    "before_fingerprint", "expected_after_fingerprint",
                ],
                "optional": [],
            },
            "coordinate.completion_apply": {
                "CompletionApplyInput": [
                    "workspace_id", "receipt_id", "task_id", "after_fingerprint",
                ],
                "optional": [],
            },
            "coordinate.completion_consume": {
                "CompletionConsumeInput": ["workspace_id", "receipt_id"],
                "optional": ["verification"],
            },
            "coordinate.channel_create": {
                "ChannelCreateInput": [
                    "workspace_id", "channel_name", "idempotency_key",
                ],
                "optional": [],
            },
        }
        for name, spec in expected.items():
            with self.subTest(tool=name):
                model_name = next(k for k in spec if k != "optional")
                required = spec[model_name]
                optional = spec["optional"]
                tool = by_name[name]
                schema = tool.input_schema
                self.assertEqual(schema.get("required"), ["input"], name)
                props = schema.get("properties", {})
                input_prop = props.get("input", {})
                candidates = [input_prop]
                candidates += input_prop.get("anyOf", [])
                refs = [
                    item["$ref"]
                    for item in candidates
                    if isinstance(item, dict) and "$ref" in item
                ]
                self.assertEqual(refs, [f"#/$defs/{model_name}"], name)
                model = schema.get("$defs", {})[model_name]
                self.assertIs(model.get("additionalProperties"), False, name)
                self.assertEqual(
                    model.get("required"), required, name,
                )
                self.assertEqual(
                    set(model.get("properties", {})),
                    set(required) | set(optional),
                    name,
                )
                # No payload/actor/requester/authorized_actor anywhere.
                self.assertNotIn("payload", model.get("properties", {}))
                self.assertNotIn("requester", model.get("properties", {}))
                self.assertNotIn("authorized_actor", model.get("properties", {}))
                self.assertNotIn("actor", model.get("properties", {}))

    def test_r5b_annotations_per_tool(self):
        tools = asyncio.run(self.server.list_tools())
        by_name = {t.name: t.annotations for t in tools}
        for name in (
            "coordinate.completion_prepare",
            "coordinate.completion_preflight",
        ):
            ann = by_name[name]
            self.assertIsNotNone(ann, name)
            self.assertIs(ann.read_only_hint, name == "coordinate.completion_preflight")
            self.assertIs(ann.open_world_hint, False)
        for name in (
            "coordinate.task_create_record",
            "coordinate.completion_claim",
            "coordinate.completion_apply",
            "coordinate.completion_consume",
        ):
            ann = by_name[name]
            self.assertIsNotNone(ann, name)
            self.assertIs(ann.read_only_hint, False)
            self.assertIs(ann.destructive_hint, False)
            self.assertIs(ann.idempotent_hint, True)
            self.assertIs(ann.open_world_hint, False)

    def test_r5b_sdk_rejects_forged_fields_before_body(self):
        """extra='forbid' at the SDK layer: payload/authorized_actor/requester
        and unknown keys are rejected before the tool body runs."""
        from mcp.server.mcpserver.exceptions import ToolError

        forged_calls = [
            (
                "coordinate.task_create_record",
                {"input": {
                    "workspace_id": "demo",
                    "operation_id": "00000000-0000-0000-0000-000000000000",
                    "input_fingerprint": "a" * 64,
                    "before_fingerprint": "b" * 64,
                    "after_fingerprint": "c" * 64,
                    "task_id": "t1",
                    "plan_doc": "plans/p.md",
                    "payload": {"forged": True},
                }},
            ),
            (
                "coordinate.completion_prepare",
                {"input": {
                    "workspace_id": "demo", "task_id": "t1",
                    "requester": "intruder",
                }},
            ),
            (
                "coordinate.completion_prepare",
                {"input": {
                    "workspace_id": "demo", "task_id": "t1",
                    "authorized_actor": "intruder",
                }},
            ),
            (
                "coordinate.completion_prepare",
                {"input": {
                    "workspace_id": "demo", "task_id": "t1",
                    "actor": "intruder",
                }},
            ),
            (
                "coordinate.completion_consume",
                {"input": {
                    "workspace_id": "demo", "receipt_id": "r1",
                    "workspace": "other",
                }},
            ),
        ]
        for name, arguments in forged_calls:
            with self.subTest(tool=name, args=arguments):
                with self.assertRaises(ToolError) as ctx:
                    asyncio.run(self.server.call_tool(name, arguments))
                self.assertNotIn("Traceback", str(ctx.exception))

    def test_r5b_missing_required_fields_rejected(self):
        from mcp.server.mcpserver.exceptions import ToolError

        for name in (
            "coordinate.task_create_record",
            "coordinate.completion_prepare",
            "coordinate.completion_preflight",
            "coordinate.completion_claim",
            "coordinate.completion_apply",
            "coordinate.completion_consume",
        ):
            with self.subTest(tool=name):
                with self.assertRaises(ToolError):
                    asyncio.run(self.server.call_tool(name, {"input": {}}))

    def test_output_schema_envelope_per_tool(self):
        tools = asyncio.run(self.server.list_tools())
        for tool in tools:
            schema = tool.output_schema
            self.assertIsNotNone(schema, f"{tool.name} must publish an outputSchema")
            self.assertEqual(
                schema.get("required"), ["ok", "data", "error"], tool.name
            )
            self.assertEqual(
                set(schema.get("properties", {})), {"ok", "data", "error"}
            )

    def test_success_envelope_dual_content(self):
        result = asyncio.run(
            self.server.call_tool(
                "coordinate.operator_pending", {"workspace_id": "demo"}
            )
        )
        self.assertFalse(result.is_error)
        self.assertTrue(result.structured_content["ok"])
        text = result.content[0].text
        self.assertEqual(json.loads(text), result.structured_content)

    def test_failure_envelope_dual_content_is_error(self):
        result = asyncio.run(
            self.server.call_tool(
                "coordinate.runtime_job_get", {"job_id": "request:missing"}
            )
        )
        self.assertTrue(result.is_error)
        self.assertFalse(result.structured_content["ok"])
        self.assertIsNone(result.structured_content["data"])
        self.assertEqual(
            result.structured_content["error"]["code"], "not_found"
        )
        self.assertEqual(
            json.loads(result.content[0].text),
            result.structured_content,
        )

    def test_unknown_tool_returns_protocol_error_not_envelope(self):
        with self.assertRaises(Exception):
            asyncio.run(
                self.server.call_tool("coordinate.claim", {})
            )


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class R5BShapeMiddlewareTests(unittest.TestCase):
    """C2: the shared stdio/remote R5B call-shape gate.

    The SDK silently drops unknown top-level fields for scalar signatures,
    so the gate requires ``arguments`` to be exactly ``{"input": object}``
    for the six typed tools and refuses before the tool body can run."""

    @staticmethod
    def _ctx(name: str, arguments: object) -> object:
        return SimpleNamespace(
            method="tools/call",
            params={"name": name, "arguments": arguments},
        )

    def test_valid_shape_reaches_call_next(self):
        from coordinate.mcp_server import _typed_shape_middleware

        called: list[object] = []

        async def fake_next(ctx):
            called.append(ctx)
            return {"ok": "next"}

        result = asyncio.run(_typed_shape_middleware(
            self._ctx("coordinate.completion_claim", {"input": {"workspace_id": "demo"}}),
            fake_next,
        ))
        self.assertEqual(result, {"ok": "next"})
        self.assertEqual(len(called), 1)

    def test_malformed_shape_refused_without_call_next(self):
        from coordinate.mcp_server import _typed_shape_middleware

        def boom(_ctx):
            raise AssertionError("call_next must not run for a malformed shape")

        malformed = [
            {"input": 5},
            {"input": None},
            {"input": "text"},
            {},
            {"workspace_id": "demo"},
            {"input": {"workspace_id": "demo"}, "evil": 1},
            {"input": {"workspace_id": "demo"}, "payload": {"x": 1}},
        ]
        for name in (
            "coordinate.task_create_record",
            "coordinate.completion_prepare",
            "coordinate.completion_preflight",
            "coordinate.completion_claim",
            "coordinate.completion_apply",
            "coordinate.completion_consume",
        ):
            for arguments in malformed:
                with self.subTest(tool=name, arguments=arguments):
                    result = asyncio.run(_typed_shape_middleware(
                        self._ctx(name, arguments), boom,
                    ))
                    self.assertTrue(result.is_error)
                    self.assertEqual(
                        result.structured_content["error"]["code"],
                        "invalid_request",
                    )
                    text = result.content[0].text
                    self.assertNotIn("Traceback", text)
                    self.assertNotIn("evil", text)

    def test_non_r5b_tools_pass_through_unchanged(self):
        """The legacy five tools keep their exact wire behavior: an extra
        top-level field is neither rejected nor unwrapped by the gate."""
        from coordinate.mcp_server import _typed_shape_middleware

        called: list[object] = []

        async def fake_next(ctx):
            called.append(ctx)
            return {"ok": "next"}

        for name in (
            "coordinate.operator_pending",
            "coordinate.workspace_audit",
            "coordinate.runtime_request_submit",
            "coordinate.runtime_job_get",
            "coordinate.runtime_agent_list",
        ):
            with self.subTest(tool=name):
                result = asyncio.run(_typed_shape_middleware(
                    self._ctx(name, {"workspace_id": "demo", "extra": 1}),
                    fake_next,
                ))
                self.assertEqual(result, {"ok": "next"})
        self.assertEqual(len(called), 5)

    def test_non_call_methods_pass_through(self):
        from coordinate.mcp_server import _typed_shape_middleware

        called: list[object] = []

        async def fake_next(ctx):
            called.append(ctx)
            return {"ok": "next"}

        for method in ("tools/list", "ping", "server/discover"):
            with self.subTest(method=method):
                result = asyncio.run(_typed_shape_middleware(
                    SimpleNamespace(method=method, params={}), fake_next,
                ))
                self.assertEqual(result, {"ok": "next"})
        self.assertEqual(len(called), 3)

    def test_r5b_tool_names_single_definition(self):
        """Registration, the shared gate and the remote unwrap read one set:
        the six typed tool names are exactly the R5B set (no second
        handwritten copy), and the transport's three fixed calls are drawn
        from the same constants."""
        from coordinate.mcp_server import TYPED_INPUT_TOOLS
        from coordinate.mcp_remote import _typed_input_args

        self.assertEqual(
            TYPED_INPUT_TOOLS,
            frozenset({
                "coordinate.task_create_record",
                "coordinate.completion_prepare",
                "coordinate.completion_preflight",
                "coordinate.completion_claim",
                "coordinate.completion_apply",
                "coordinate.completion_consume",
                "coordinate.channel_create",
            }),
        )
        # The remote unwrap recognizes exactly the shared set.
        for name in TYPED_INPUT_TOOLS:
            self.assertEqual(
                _typed_input_args(name, {"input": {"workspace_id": "demo"}}),
                {"workspace_id": "demo"},
            )
        self.assertIsNone(
            _typed_input_args("coordinate.operator_pending", {"input": {"x": 1}}),
        )
        from coordinate.completion_mcp_transport import CompletionMCPTransport

        self.assertEqual(
            set(CompletionMCPTransport.FIXED_TOOLS),
            {
                "coordinate.completion_preflight",
                "coordinate.completion_claim",
                "coordinate.completion_apply",
            },
        )


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class StdioSubprocessTests(unittest.TestCase):
    """Real stdio subprocess lifecycle: legacy SDK client + modern raw JSON-RPC.

    The ``test_modern_*`` tests exchange raw JSON-RPC lines (no SDK client) to
    pin the 2026-07-28 stateless wire contract; the remaining tests keep the
    official SDK client lifecycle regression.
    """

    def _spawn(self, db_path):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        params = StdioServerParameters(
            command=sys.executable,
            args=[
                "-m",
                "coordinate",
                "--db",
                db_path,
                "mcp",
                "serve",
            ],
            env={**os.environ, "PYTHONPATH": str(SRC_PATH)},
            cwd=str(REPO_ROOT),
        )
        errlog = tempfile.NamedTemporaryFile("w", suffix=".log")
        # delete=True: the file is removed when errlog is closed in cleanup.
        self.addCleanup(errlog.close)
        return stdio_client(params, errlog=errlog), ClientSession, errlog.name

    def _seed_db(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        db_path = os.path.join(self.tmp.name, "r1.sqlite3")
        conn = initialize(db_path)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="mac",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )
        register_agent(
            conn, agent_id="mac-codex", host_id="mac", capabilities={}
        )
        conn.close()
        return db_path

    def test_discovery_and_call_over_stdio(self):
        # Legacy lifecycle: ``session.initialize()`` runs the <=2025-11-25
        # handshake, which mcp 2.0.0 keeps for backward compatibility. The
        # modern 2026-07-28 era needs no initialize at all; that path is
        # pinned by the raw test_modern_* tests below.
        db_path = self._seed_db()

        async def _run():
            client_ctx, session_cls, _errlog_path = self._spawn(db_path)
            async with client_ctx as (read_stream, write_stream):
                async with session_cls(read_stream, write_stream) as session:
                    await session.initialize()
                    tools = (await session.list_tools()).tools
                    return [t.name for t in tools]

        names = asyncio.run(_run())
        self.assertEqual(
            sorted(names), sorted(MCPServerToolContractTests.EXPECTED_TOOLS)
        )

    def test_submit_and_get_over_stdio(self):
        # Legacy lifecycle (initialize handshake; see test_discovery_and_call_over_stdio).
        db_path = self._seed_db()

        async def _run():
            client_ctx, session_cls, _errlog_path = self._spawn(db_path)
            async with client_ctx as (read_stream, write_stream):
                async with session_cls(read_stream, write_stream) as session:
                    await session.initialize()
                    result = await session.call_tool(
                        "coordinate.runtime_request_submit",
                        {
                            "workspace_id": "demo",
                            "prompt": "hello stdio",
                            "origin": {
                                "platform": "discord",
                                "destination": "ch",
                                "message_id": "m1",
                                "session_scope_id": "discord:ch",
                            },
                            "reply": {
                                "platform": "discord",
                                "destination": "ch",
                            },
                            "target_agent": "mac-codex",
                            "idempotency_key": "stdio-k1",
                        },
                        read_timeout_seconds=20,
                    )
                    self.assertFalse(result.is_error)
                    job_id = result.structured_content["data"]["job"]["id"]
                    got = await session.call_tool(
                        "coordinate.runtime_job_get",
                        {"job_id": job_id},
                        read_timeout_seconds=20,
                    )
                    self.assertFalse(got.is_error)
                    self.assertEqual(
                        got.structured_content["data"]["id"], job_id
                    )
                    missing = await session.call_tool(
                        "coordinate.runtime_job_get",
                        {"job_id": "request:missing"},
                        read_timeout_seconds=20,
                    )
                    self.assertTrue(missing.is_error)
                    return (
                        missing.structured_content["error"]["code"],
                        result.structured_content["data"]["event"]["actor"],
                    )

        code, actor = asyncio.run(_run())
        self.assertEqual(code, "not_found")
        self.assertEqual(actor, "mcp")

    def test_schema_layer_rejects_missing_and_forged_arguments(self):
        """P1-2/P1-3: SDK-layer argument validation fails closed on the wire.

        Missing required fields and extra routing fields are rejected by the
        typed argument model before the tool body runs. The wire result is a
        bounded ``is_error`` result (no traceback, no secret), which is the
        SDK's protocol-level rendering for malformed tool arguments.
        """
        # Legacy lifecycle (initialize handshake; see test_discovery_and_call_over_stdio).
        db_path = self._seed_db()

        async def _run():
            client_ctx, session_cls, _errlog_path = self._spawn(db_path)
            async with client_ctx as (read_stream, write_stream):
                async with session_cls(read_stream, write_stream) as session:
                    await session.initialize()
                    missing = await session.call_tool(
                        "coordinate.operator_pending", {},
                        read_timeout_seconds=20,
                    )
                    forged = await session.call_tool(
                        "coordinate.runtime_request_submit",
                        {
                            "workspace_id": "demo",
                            "prompt": "x",
                            "origin": {
                                "platform": "discord",
                                "destination": "ch",
                                "message_id": "m1",
                                "session_scope_id": "discord:ch",
                            },
                            "reply": {
                                "platform": "discord",
                                "destination": "ch",
                            },
                            "routing_request": {
                                "required_capabilities": ["coding"],
                                "routing_request_id": "forged",
                            },
                            "idempotency_key": "schema-k",
                        },
                        read_timeout_seconds=20,
                    )
                    return missing, forged

        missing, forged = asyncio.run(_run())
        self.assertTrue(missing.is_error)
        missing_text = missing.content[0].text
        self.assertNotIn("Traceback", missing_text)
        self.assertNotIn("secret", missing_text)
        self.assertTrue(forged.is_error)
        forged_text = forged.content[0].text
        self.assertNotIn("Traceback", forged_text)

    @staticmethod
    def _readline_bounded(stream, timeout: float = 15.0):
        """Read one line with a hard deadline; returns None on timeout.

        MCP stdio is client-first, so a server that never answers must not
        block the test forever: every blocking read is bounded and the child
        is reaped by the cleanup hook.
        """
        import select
        import time

        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            readable, _, _ = select.select([stream], [], [], min(remaining, 0.5))
            if not readable:
                continue
            line = stream.readline()
            if not line:
                return None
            return line

    @classmethod
    def _reap(cls, proc) -> None:
        """Force-kill, wait and close pipes so no child survives a failing test."""
        if proc.poll() is None:
            proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            pass
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:  # pragma: no cover - defensive
                    pass

    def _spawn_raw(self, db_path):
        """Plain stdio subprocess for raw JSON-RPC exchange (no SDK client)."""
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "coordinate",
                "--db",
                db_path,
                "mcp",
                "serve",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "PYTHONPATH": str(SRC_PATH)},
            cwd=str(REPO_ROOT),
            text=True,
        )
        self.addCleanup(self._reap, proc)
        return proc

    def _raw_request(self, proc, message: dict) -> dict | None:
        """Write one JSON-RPC request line; return the parsed response line.

        None on timeout/EOF: the caller's assertions fail the test and the
        child is reaped by the cleanup hook.
        """
        assert proc.stdin is not None and proc.stdout is not None
        proc.stdin.write(json.dumps(message) + "\n")
        proc.stdin.flush()
        line = self._readline_bounded(proc.stdout)
        return json.loads(line) if line is not None else None

    @staticmethod
    def _modern_meta(protocol_version: str = "2026-07-28") -> dict:
        """The 2026-07-28 per-request envelope: version + client identity.

        Every request carries protocolVersion/clientCapabilities/clientInfo in
        ``params._meta``; there is no protocol-level session or initialize.
        """
        return {
            "io.modelcontextprotocol/protocolVersion": protocol_version,
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {
                "name": "r1-raw-modern",
                "version": "0",
            },
        }

    def _assert_clean_stderr(self, proc) -> None:
        """Kill the child and assert its stderr carried no diagnostics."""
        if proc.poll() is None:
            proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            pass
        if proc.stderr is not None:
            stderr = proc.stderr.read()
            self.assertNotIn("Traceback", stderr)
            self.assertNotIn("Error executing tool", stderr)

    def test_stdout_framing_is_clean_protocol_only(self):
        db_path = self._seed_db()
        proc = self._spawn_raw(db_path)
        assert proc.stdout is not None and proc.stdin is not None
        try:
            # MCP stdio is client-first: the server emits nothing before the
            # client's first request. This test drives the legacy handshake
            # era (initialize at 2025-11-25); the modern 2026-07-28 era, which
            # needs no initialize, is covered by the test_modern_* tests.
            request = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "r1-test", "version": "0"},
                    },
                }
            )
            proc.stdin.write(request + "\n")
            proc.stdin.flush()
            line = self._readline_bounded(proc.stdout)
            self.assertIsNotNone(line, "server never answered initialize")
            # Every stdout line must be JSON-RPC framing, never a banner/log.
            response = json.loads(line)
            self.assertEqual(response.get("jsonrpc"), "2.0")
            self.assertEqual(response.get("id"), 1)
            self.assertIn("result", response)
            self.assertNotIn("banner", response["result"].get("serverInfo", {}))

            # Round-trip tools/list and verify framing stays clean.
            proc.stdin.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/list",
                        "params": {},
                    }
                )
                + "\n"
            )
            proc.stdin.flush()
            tools_line = self._readline_bounded(proc.stdout)
            self.assertIsNotNone(tools_line, "server never answered tools/list")
            tools_message = json.loads(tools_line)
            self.assertEqual(tools_message.get("id"), 2)
            names = [t["name"] for t in tools_message["result"]["tools"]]
            self.assertEqual(len(names), 12)
            self.assertEqual(sorted(names), sorted(MCPServerToolContractTests.EXPECTED_TOOLS))
        finally:
            self._assert_clean_stderr(proc)
            self._reap(proc)

    def test_modern_discover_list_call_without_initialize(self):
        """2026-07-28 stateless era: discover + list + call, no initialize.

        Every request carries its own ``_meta`` envelope; the connection has
        no protocol session, so the same server process serves requests with
        nothing but per-request metadata. Responses carry ``resultType`` and
        the ``serverInfo`` ``_meta`` stamp.
        """
        db_path = self._seed_db()
        proc = self._spawn_raw(db_path)

        discover = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {"_meta": self._modern_meta()},
            },
        )
        self.assertIsNotNone(discover, "server never answered server/discover")
        self.assertEqual(discover["jsonrpc"], "2.0")
        self.assertEqual(discover["id"], 1)
        self.assertIn("result", discover)
        self.assertIn(
            "2026-07-28", discover["result"]["supportedVersions"]
        )
        self.assertEqual(discover["result"]["resultType"], "complete")
        self.assertIsNotNone(discover["result"]["capabilities"].get("tools"))
        self.assertEqual(
            discover["result"]["_meta"][
                "io.modelcontextprotocol/serverInfo"
            ]["name"],
            "coordinate",
        )

        tools = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {"_meta": self._modern_meta()},
            },
        )
        self.assertIsNotNone(tools, "server never answered tools/list")
        self.assertEqual(tools["id"], 2)
        self.assertEqual(tools["result"]["resultType"], "complete")
        names = [t["name"] for t in tools["result"]["tools"]]
        self.assertEqual(
            sorted(names), sorted(MCPServerToolContractTests.EXPECTED_TOOLS)
        )

        ok = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "coordinate.operator_pending",
                    "arguments": {"workspace_id": "demo"},
                    "_meta": self._modern_meta(),
                },
            },
        )
        self.assertIsNotNone(ok, "server never answered tools/call")
        self.assertEqual(ok["id"], 3)
        self.assertEqual(ok["result"]["resultType"], "complete")
        self.assertFalse(ok["result"]["isError"])
        self.assertTrue(ok["result"]["structuredContent"]["ok"])
        self.assertIn(
            "pending_actions", ok["result"]["structuredContent"]["data"]
        )

        missing = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "coordinate.runtime_job_get",
                    "arguments": {"job_id": "request:missing"},
                    "_meta": self._modern_meta(),
                },
            },
        )
        self.assertIsNotNone(missing, "server never answered failing call")
        self.assertEqual(missing["id"], 4)
        self.assertTrue(missing["result"]["isError"])
        self.assertEqual(
            missing["result"]["structuredContent"]["error"]["code"],
            "not_found",
        )
        self._assert_clean_stderr(proc)

    def test_r5b_stdio_shape_gate_rejects_top_level_extras(self):
        """C2: stdio fails closed on a top-level extra field next to the typed
        ``input`` object (the SDK would silently drop it). The tool body never
        runs: a valid input whose body would answer with a domain error
        (unknown_receipt) is answered by the shape gate with invalid_request.
        The legacy five tools keep their exact behavior with extra fields."""
        db_path = self._seed_db()
        proc = self._spawn_raw(db_path)

        forged = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "coordinate.completion_claim",
                    "arguments": {
                        "input": {
                            "workspace_id": "demo",
                            "receipt_id": "r1",
                            "task_id": "t1",
                            "before_fingerprint": "b" * 64,
                            "expected_after_fingerprint": "c" * 64,
                        },
                        "evil": 1,
                    },
                    "_meta": self._modern_meta(),
                },
            },
        )
        self.assertIsNotNone(forged, "server never answered forged call")
        self.assertEqual(forged["id"], 1)
        self.assertTrue(forged["result"]["isError"])
        self.assertEqual(
            forged["result"]["structuredContent"]["error"]["code"],
            "invalid_request",
        )
        # The body must not have run: a domain answer would be
        # unknown_receipt, and no traceback or echoed value may appear.
        text = json.dumps(forged, ensure_ascii=False)
        self.assertNotIn("unknown_receipt", text)
        self.assertNotIn("Traceback", text)
        self.assertNotIn("evil", text)

        # A missing/empty input object is refused the same way.
        empty = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "coordinate.completion_preflight",
                    "arguments": {},
                    "_meta": self._modern_meta(),
                },
            },
        )
        self.assertIsNotNone(empty, "server never answered empty call")
        self.assertTrue(empty["result"]["isError"])
        self.assertEqual(
            empty["result"]["structuredContent"]["error"]["code"],
            "invalid_request",
        )

        # Legacy five tools are untouched: an extra top-level field still
        # executes (SDK drop semantics preserved).
        ok = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "coordinate.operator_pending",
                    "arguments": {"workspace_id": "demo", "extra": 1},
                    "_meta": self._modern_meta(),
                },
            },
        )
        self.assertIsNotNone(ok, "server never answered legacy call")
        self.assertFalse(ok["result"]["isError"])
        self.assertTrue(ok["result"]["structuredContent"]["ok"])
        self._assert_clean_stderr(proc)

    def test_modern_unsupported_protocol_version_is_32022(self):
        """A version outside the served modern set is refused with -32022.

        The legacy handshake version 2025-11-25 is deliberately not in
        ``supportedVersions``: on the 2026-07-28 envelope it must be refused
        as a JSON-RPC error naming the supported set, never silently served.
        """
        db_path = self._seed_db()
        proc = self._spawn_raw(db_path)
        response = self._raw_request(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "server/discover",
                "params": {"_meta": self._modern_meta("2025-11-25")},
            },
        )
        self.assertIsNotNone(response, "server never answered")
        self.assertEqual(response["id"], 7)
        self.assertNotIn("result", response)
        self.assertEqual(response["error"]["code"], -32022)
        self.assertEqual(response["error"]["data"]["supported"], ["2026-07-28"])
        self.assertEqual(response["error"]["data"]["requested"], "2025-11-25")
        self._assert_clean_stderr(proc)


class LazyImportCliTests(unittest.TestCase):
    """Base install without the mcp extra must keep every CLI path working."""

    @staticmethod
    def _source(name):
        with open(SRC_PATH / "coordinate" / name, encoding="utf-8") as f:
            return f.read()

    def test_import_coordinate_without_mcp(self):
        # Guard: coordinate's own modules must not import mcp at module level.
        import ast

        for module in ("agent_interface", "mcp_server", "mcp_cli"):
            tree = ast.parse(
                self._source(f"{module}.py"), filename=f"{module}.py"
            )
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Import, ast.ImportFrom)):
                    continue
                # Only module-level imports matter; lazy imports inside
                # functions (including inside try/except) are the deliberate
                # isolation mechanism.
                parent = next(
                    (
                        p
                        for p in ast.walk(tree)
                        if any(child is node for child in ast.iter_child_nodes(p))
                    ),
                    None,
                )
                while parent is not None and not isinstance(
                    parent, (ast.FunctionDef, ast.AsyncFunctionDef)
                ):
                    parent = next(
                        (
                            p
                            for p in ast.walk(tree)
                            if any(
                                child is parent
                                for child in ast.iter_child_nodes(p)
                            )
                        ),
                        None,
                    )
                if isinstance(
                    parent, (ast.FunctionDef, ast.AsyncFunctionDef)
                ):
                    continue
                names = (
                    [a.name for a in node.names]
                    if isinstance(node, ast.Import)
                    else [node.module or ""]
                )
                for imported in names:
                    self.assertFalse(
                        imported == "mcp" or imported.startswith("mcp."),
                        f"{module}.py imports {imported} at module level",
                    )

    def test_parser_includes_mcp_serve(self):
        parser = build_parser()
        mcp = next(
            a
            for a in parser._actions
            if getattr(a, "dest", None) == "command"
        ).choices["mcp"]
        self.assertIsNotNone(mcp)
        serve = next(
            a
            for a in mcp._actions
            if getattr(a, "dest", None) == "mcp_command"
        ).choices["serve"]
        self.assertEqual(
            serve.get_default("handler").__module__,
            "coordinate.mcp_cli",
        )
        self.assertEqual(serve.get_default("transport"), "stdio")

    def test_mcp_serve_returns_install_hint_without_extra(self):
        @contextlib.contextmanager
        def _block_mcp():
            real_import = builtins.__import__

            def fake_import(name, *args, **kwargs):
                if name == "mcp" or name.startswith("mcp."):
                    raise ImportError(f"blocked: {name}")
                return real_import(name, *args, **kwargs)

            builtins.__import__ = fake_import
            try:
                yield
            finally:
                builtins.__import__ = real_import

        args = argparse.Namespace(
            db=":memory:", actor="mcp", transport="stdio"
        )
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), _block_mcp():
            rc = handle_mcp_serve(args)
        self.assertEqual(rc, 1)
        self.assertIn("coordinate[mcp]", stderr.getvalue())
        self.assertIn(INSTALL_HINT, stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_mcp_serve_rejects_non_stdio_transport(self):
        args = argparse.Namespace(
            db=":memory:", actor="mcp", transport="sse"
        )
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = handle_mcp_serve(args)
        self.assertEqual(rc, 1)
        self.assertIn("stdio", stderr.getvalue())


class NoSubprocessNoPrinterTests(unittest.TestCase):
    """AST proof that the adapter/facade never shells out or prints to stdout."""

    MODULES = ("agent_interface.py", "mcp_server.py", "mcp_cli.py")

    def _source(self, name):
        with open(SRC_PATH / "coordinate" / name, encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _call_names(source: str) -> set[str]:
        import ast

        tree = ast.parse(source)
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    names.add(func.id)
                elif isinstance(func, ast.Attribute):
                    names.add(func.attr)
        return names

    @staticmethod
    def _imported_names(source: str) -> set[str]:
        import ast

        tree = ast.parse(source)
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    names.add(node.module.split(".")[0])
                names.update(alias.name for alias in node.names)
        return names

    def test_no_subprocess_shell_ssh(self):
        forbidden_imports = {"subprocess", "os", "shlex", "ssh"}
        forbidden_calls = {"system", "popen", "ssh"}
        for name in self.MODULES:
            source = self._source(name)
            imports = self._imported_names(source)
            self.assertTrue(
                imports.isdisjoint(forbidden_imports),
                f"{name} imports {imports & forbidden_imports}",
            )
            calls = self._call_names(source)
            self.assertTrue(
                calls.isdisjoint(forbidden_calls),
                f"{name} calls {calls & forbidden_calls}",
            )

    def test_no_stdout_printers(self):
        import ast

        for name in self.MODULES:
            source = self._source(name)
            calls = self._call_names(source)
            self.assertNotIn("print_json", calls, f"{name} calls print_json()")
            if "print" in calls:
                # Only stderr diagnostics are allowed; stdout print is forbidden.
                tree = ast.parse(source)
                for node in ast.walk(tree):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == "print"
                    ):
                        file_kw = next(
                            (
                                kw.value
                                for kw in node.keywords
                                if kw.arg == "file"
                            ),
                            None,
                        )
                        is_stderr = (
                            isinstance(file_kw, ast.Attribute)
                            and isinstance(file_kw.value, ast.Name)
                            and file_kw.value.id == "sys"
                            and file_kw.attr == "stderr"
                        )
                        self.assertTrue(
                            is_stderr,
                            f"{name} prints to stdout",
                        )

    def test_no_cli_main_and_no_refresh_state(self):
        for name in self.MODULES:
            calls = self._call_names(self._source(name))
            self.assertNotIn("main", calls, f"{name} calls main()")
            self.assertNotIn("refresh_state", calls, f"{name} calls refresh_state()")
            self.assertNotIn("HarnessAdapter", calls, f"{name} instantiates HarnessAdapter")

    def test_audit_fixed_refresh_false(self):
        source = self._source("agent_interface.py")
        self.assertIn("refresh=False", source)
        # The only audit call site must use refresh=False.
        self.assertNotIn("refresh=True", source)

    def test_facade_creates_connection_per_call(self):
        source = self._source("agent_interface.py")
        # Connection lifecycle is owned by the facade, not by the adapter.
        self.assertIn("self._connection_factory()", source)

    def test_submit_actor_not_callable_parameter(self):
        # Facade signature must not accept an actor parameter from callers.
        import inspect

        from coordinate.agent_interface import AgentInterface

        params = inspect.signature(
            AgentInterface.runtime_request_submit
        ).parameters
        self.assertNotIn("actor", params)

    def test_no_runtime_refresh_in_mcp_server(self):
        # The adapter has no refresh surface at all.
        calls = self._call_names(self._source("mcp_server.py"))
        self.assertNotIn("refresh", calls)
        self.assertNotIn("refresh_state", calls)


if __name__ == "__main__":
    unittest.main()
