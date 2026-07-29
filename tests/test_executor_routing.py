"""Tests for P9-2B executor routing contract and candidate selection."""
from __future__ import annotations

import dataclasses
import tempfile
import unittest

from coordinate.db import (
    initialize,
    set_workspace_agent,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from coordinate.executor_identity import (
    ExecutorCatalog,
    ExecutorDefinition,
    ExecutorInstanceBinding,
    compute_executor_catalog_hash,
    sync_executor_catalog,
)
from coordinate.executor_routing import (
    Candidate,
    ExecutorRoutingError,
    RoutingRequest,
    _compute_routing_decision_id,
    build_routing_request,
    compute_routing_load,
    parse_routing_request,
    resolve_routing_candidates,
    routing_claim_evidence,
    routing_decision_to_dict,
    routing_request_to_dict,
    select_routing_decision,
    validate_routing_decision,
)
from coordinate.runtime import heartbeat_agent, register_agent
from coordinate.job_repository import create_job


class RoutingRequestContractTests(unittest.TestCase):
    def test_build_and_round_trip(self):
        req = build_routing_request(
            required_capabilities=["coding"],
            executor_definition_id="coder",
            preferred_host_id="mac",
            operator_override_agent_id="mac-omp",
            operator_override_reason="operator override",
        )
        self.assertEqual(req.required_capabilities, ("coding",))
        self.assertEqual(req.executor_definition_id, "coder")
        self.assertEqual(req.preferred_host_id, "mac")
        self.assertEqual(req.operator_override_agent_id, "mac-omp")
        self.assertEqual(req.operator_override_reason, "operator override")
        self.assertTrue(req.routing_request_id.startswith("sha256:"))

        as_dict = routing_request_to_dict(req)
        parsed = parse_routing_request(as_dict)
        self.assertEqual(parsed.routing_request_id, req.routing_request_id)

    def test_digest_is_canonical(self):
        req = build_routing_request(required_capabilities=["coding"])
        # The id must be stable and equal to a canonical SHA-256 over the body.
        body = {
            "contract_version": 1,
            "mode": "deterministic",
            "required_capabilities": ["coding"],
            "executor_definition_id": None,
            "preferred_host_id": None,
            "operator_override_agent_id": None,
            "operator_override_reason": None,
            "policy_version": 1,
        }
        import hashlib, json

        canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        expected = f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"
        self.assertEqual(req.routing_request_id, expected)

    def test_capabilities_sorted_after_normalization(self):
        req = build_routing_request(required_capabilities=["coding", "review"])
        self.assertEqual(req.required_capabilities, ("coding", "review"))

    def test_unsorted_capabilities_normalized(self):
        req = build_routing_request(required_capabilities=["review", "coding"])
        self.assertEqual(req.required_capabilities, ("coding", "review"))

    def test_duplicate_capabilities_deduplicated(self):
        req = build_routing_request(required_capabilities=["coding", "coding", "review"])
        self.assertEqual(req.required_capabilities, ("coding", "review"))

    def test_empty_capabilities_rejected(self):
        with self.assertRaisesRegex(ExecutorRoutingError, "at least one capability"):
            build_routing_request(required_capabilities=[])

    def test_build_accepts_64_char_capability(self):
        cap = "a" * 64
        req = build_routing_request(required_capabilities=[cap])
        self.assertEqual(req.required_capabilities, (cap,))

    def test_build_rejects_65_char_capability(self):
        cap = "a" * 65
        with self.assertRaisesRegex(ExecutorRoutingError, "exceeds 64 characters"):
            build_routing_request(required_capabilities=[cap])

    def test_build_accepts_32_capabilities(self):
        caps = [f"cap{i:02d}" for i in range(32)]
        req = build_routing_request(required_capabilities=caps)
        self.assertEqual(req.required_capabilities, tuple(sorted(caps)))

    def test_build_rejects_33_capabilities(self):
        caps = [f"cap{i:02d}" for i in range(33)]
        with self.assertRaisesRegex(ExecutorRoutingError, "exceeds maximum cardinality"):
            build_routing_request(required_capabilities=caps)

    def test_override_requires_both_fields(self):
        with self.assertRaisesRegex(ExecutorRoutingError, "both be supplied or both absent"):
            build_routing_request(
                required_capabilities=["coding"],
                operator_override_agent_id="mac-omp",
            )

    def test_override_reason_too_long_rejected(self):
        with self.assertRaisesRegex(ExecutorRoutingError, "exceeds 512"):
            build_routing_request(
                required_capabilities=["coding"],
                operator_override_agent_id="mac-omp",
                operator_override_reason="x" * 513,
            )

    def test_override_reason_control_char_rejected(self):
        with self.assertRaisesRegex(ExecutorRoutingError, "control characters"):
            build_routing_request(
                required_capabilities=["coding"],
                operator_override_agent_id="mac-omp",
                operator_override_reason="bad\x00reason",
            )

    def test_parse_rejects_unknown_keys(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = routing_request_to_dict(req)
        dct["extra"] = "value"
        with self.assertRaisesRegex(ExecutorRoutingError, "incorrect keys"):
            parse_routing_request(dct)

    def test_parse_rejects_digest_mismatch(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = routing_request_to_dict(req)
        dct["routing_request_id"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ExecutorRoutingError, "digest mismatch"):
            parse_routing_request(dct)

    def test_parse_rejects_unsupported_version(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = routing_request_to_dict(req)
        dct["contract_version"] = 2
        with self.assertRaisesRegex(ExecutorRoutingError, "contract_version must be 1"):
            parse_routing_request(dct)


class RoutingCandidateTests(unittest.TestCase):
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
        upsert_workspace_host_profile(
            self.conn,
            workspace_id="demo",
            host_id="pc",
            workspace_path=self.tmp.name,
            harness_root=self.tmp.name,
        )

    def _authorize(self, agent_name: str, discord_id: str = "12345"):
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name=agent_name,
            discord_user_id=discord_id,
            actor="test",
            reason="test",
        )

    def _register(self, agent_id: str, host_id: str):
        register_agent(
            self.conn,
            agent_id=agent_id,
            host_id=host_id,
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id=agent_id, host_id=host_id)

    def _sync_catalog(self, agent_ids: list[str], capabilities: list[str] = None):
        capabilities = capabilities or ["coding"]
        definitions = (
            ExecutorDefinition(
                id="coder",
                provider="kimi-code",
                adapter="omp",
                capabilities=tuple(capabilities),
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
        sync_executor_catalog(self.conn, catalog)

    def test_unknown_workspace_raises(self):
        req = build_routing_request(required_capabilities=["coding"])
        with self.assertRaisesRegex(ExecutorRoutingError, "unknown workspace"):
            resolve_routing_candidates(self.conn, "missing", req)

    def test_no_candidates_when_no_binding(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        req = build_routing_request(required_capabilities=["coding"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_disabled_binding_not_eligible(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        self.conn.execute(
            "UPDATE executor_instance_bindings SET enabled = 0 WHERE agent_id = ?",
            ("mac-omp",),
        )
        self.conn.commit()
        req = build_routing_request(required_capabilities=["coding"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_offline_agent_not_eligible(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        self.conn.execute(
            "UPDATE agents SET online_state = 'offline' WHERE id = ?", ("mac-omp",)
        )
        self.conn.commit()
        req = build_routing_request(required_capabilities=["coding"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_bridge_client_type_not_eligible(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        self.conn.execute(
            "UPDATE agents SET client_type = 'bridge' WHERE id = ?", ("mac-omp",)
        )
        self.conn.commit()
        req = build_routing_request(required_capabilities=["coding"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_missing_host_profile_not_eligible(self):
        self._register("mac-omp", "unknown-host")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        req = build_routing_request(required_capabilities=["coding"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_non_agentd_runner_profile_not_eligible(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        self.conn.execute(
            "UPDATE runner_profiles SET runner_type = 'generic_subprocess' WHERE id = ?",
            ("mac-omp",),
        )
        self.conn.commit()
        req = build_routing_request(required_capabilities=["coding"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_capability_missing_not_eligible(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"], capabilities=["coding"])
        req = build_routing_request(required_capabilities=["coding", "review"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_definition_filter(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        req = build_routing_request(
            required_capabilities=["coding"], executor_definition_id="other"
        )
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)
        req = build_routing_request(
            required_capabilities=["coding"], executor_definition_id="coder"
        )
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 1)

    def test_workspace_unauthorized_not_eligible(self):
        self._register("mac-omp", "mac")
        # No workspace authorization.
        self._sync_catalog(["mac-omp"])
        req = build_routing_request(required_capabilities=["coding"])
        self.assertEqual(len(resolve_routing_candidates(self.conn, "demo", req)), 0)

    def test_last_seen_recorded_not_filter(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        # Set last_seen_at to a distant past value; P9-2B must not invent a cutoff.
        self.conn.execute(
            "UPDATE agents SET last_seen_at = '2020-01-01T00:00:00Z' WHERE id = ?",
            ("mac-omp",),
        )
        self.conn.commit()
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].last_seen_at, "2020-01-01T00:00:00Z")

    def test_routing_load_counts(self):
        self._register("mac-omp", "mac")
        self._register("mac-codex", "mac")
        self._authorize("mac-omp", "12345")
        self._authorize("mac-codex", "12346")
        self._sync_catalog(["mac-omp", "mac-codex"])
        # Create a pending job for mac-omp.
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="mac-omp",
            assigned_agent="mac-omp",
            payload={},
        )
        # Create a running job for mac-codex.
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="mac-codex",
            assigned_agent="mac-codex",
            payload={},
        )
        self.conn.execute(
            "UPDATE jobs SET status = 'running' WHERE assigned_agent = ?", ("mac-codex",)
        )
        self.conn.commit()
        # Create a recoverable timed_out job for mac-omp.
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="mac-omp",
            assigned_agent="mac-omp",
            payload={},
        )
        self.conn.execute(
            "UPDATE jobs SET status = 'timed_out', recoverable = 1 WHERE rowid = (SELECT MAX(rowid) FROM jobs)"
        )
        self.conn.commit()
        # Create a non-recoverable timed_out job for mac-codex.
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="mac-codex",
            assigned_agent="mac-codex",
            payload={},
        )
        self.conn.execute(
            "UPDATE jobs SET status = 'timed_out', recoverable = 0 WHERE rowid = (SELECT MAX(rowid) FROM jobs)"
        )
        self.conn.commit()
        # Create a done job for mac-omp; should not count.
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="mac-omp",
            assigned_agent="mac-omp",
            payload={},
        )
        self.conn.execute(
            "UPDATE jobs SET status = 'done' WHERE rowid = (SELECT MAX(rowid) FROM jobs)"
        )
        self.conn.commit()

        self.assertEqual(compute_routing_load(self.conn, "mac-omp"), 2)
        self.assertEqual(compute_routing_load(self.conn, "mac-codex"), 1)

    def test_ordering_by_host_load_definition_agent(self):
        self._register("mac-omp", "mac")
        self._register("mac-codex", "mac")
        self._authorize("mac-omp", "12345")
        self._authorize("mac-codex", "12346")
        self._sync_catalog(["mac-omp", "mac-codex"])
        # Make mac-omp have load 1 and mac-codex load 0.
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="mac-omp",
            assigned_agent="mac-omp",
            payload={},
        )
        self.conn.commit()
        req = build_routing_request(
            required_capabilities=["coding"], preferred_host_id="mac"
        )
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        # Both on mac; load ties host rank; mac-codex has lower load, then id order.
        self.assertEqual([c.agent_id for c in candidates], ["mac-codex", "mac-omp"])

    def test_preferred_host_rank(self):
        self._register("mac-omp", "mac")
        self._register("pc-omp", "pc")
        self._authorize("mac-omp", "12345")
        self._authorize("pc-omp", "12346")
        self._sync_catalog(["mac-omp", "pc-omp"])
        # Equal load; preferred host should win.
        req = build_routing_request(
            required_capabilities=["coding"], preferred_host_id="pc"
        )
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        self.assertEqual(candidates[0].agent_id, "pc-omp")

    def test_no_candidates_raises(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"], capabilities=["review"])
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        with self.assertRaisesRegex(ExecutorRoutingError, "executor_route_no_candidate"):
            select_routing_decision(req, candidates)

    def test_override_eligible(self):
        self._register("mac-omp", "mac")
        self._register("mac-codex", "mac")
        self._authorize("mac-omp", "12345")
        self._authorize("mac-codex", "12346")
        self._sync_catalog(["mac-omp", "mac-codex"])
        # Automatic selection would pick mac-codex (lower load/id). Override picks mac-omp.
        create_job(
            self.conn,
            workspace_id="demo",
            task_id=None,
            runner_profile_id="mac-codex",
            assigned_agent="mac-codex",
            payload={},
        )
        self.conn.commit()
        req = build_routing_request(
            required_capabilities=["coding"],
            operator_override_agent_id="mac-omp",
            operator_override_reason="need omp",
        )
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        decision = select_routing_decision(req, candidates)
        self.assertEqual(decision.selection_kind, "operator_override")
        self.assertEqual(decision.selected_agent_id, "mac-omp")

    def test_override_ineligible_raises(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        req = build_routing_request(
            required_capabilities=["coding"],
            operator_override_agent_id="mac-codex",
            operator_override_reason="not eligible",
        )
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        with self.assertRaisesRegex(ExecutorRoutingError, "executor_route_override_ineligible"):
            select_routing_decision(req, candidates)

    def test_candidate_cap_raises(self):
        # Fabricate many candidates by directly inserting bindings/agents.
        for i in range(257):
            agent_id = f"agent-{i:03d}"
            self._register(agent_id, "mac")
            self._authorize(agent_id, str(10000 + i))
        self._sync_catalog([f"agent-{i:03d}" for i in range(257)])
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        with self.assertRaisesRegex(ExecutorRoutingError, "executor_route_candidate_cap_exceeded"):
            select_routing_decision(req, candidates)

    def test_decision_digest_and_validate(self):
        self._register("mac-omp", "mac")
        self._authorize("mac-omp")
        self._sync_catalog(["mac-omp"])
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        decision = select_routing_decision(req, candidates)
        dct = routing_decision_to_dict(decision)
        validated = validate_routing_decision(dct, routing_request=req)
        self.assertEqual(validated["routing_decision_id"], decision.routing_decision_id)
        self.assertEqual(validated["selected_agent_id"], "mac-omp")


class RoutingClaimEvidenceTests(unittest.TestCase):
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
            agent_id="mac-omp",
            host_id="mac",
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id="mac-omp", host_id="mac")
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-omp",
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
                agent_id="mac-omp",
                executor_definition_id="coder",
                runner_profile_id="mac-omp",
                enabled=True,
            ),
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
        sync_executor_catalog(self.conn, catalog)

    def test_valid_claim_evidence(self):
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        decision = select_routing_decision(req, candidates)
        payload = {
            "routing_request": routing_request_to_dict(req),
            "routing_decision": routing_decision_to_dict(decision),
            "executor_binding": candidates[0].binding_snapshot,
            "execution_context": {
                "job_id": "job-1",
                "workspace_id": "demo",
                "task_id": None,
                "assigned_agent": "mac-omp",
                "host_id": "mac",
            },
        }
        job = {"id": "job-1", "assigned_agent": "mac-omp", "runner_profile_id": "mac-omp", "workspace_id": "demo", "task_id": None}
        evidence = routing_claim_evidence(payload, job=job)
        self.assertEqual(evidence["routing_request_id"], req.routing_request_id)
        self.assertEqual(evidence["routing_decision_id"], decision.routing_decision_id)
        self.assertEqual(evidence["selection_kind"], "automatic")

    def test_mismatched_assignment_rejected(self):
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        decision = select_routing_decision(req, candidates)
        payload = {
            "routing_request": routing_request_to_dict(req),
            "routing_decision": routing_decision_to_dict(decision),
            "executor_binding": candidates[0].binding_snapshot,
            "execution_context": {
                "job_id": "job-1",
                "workspace_id": "demo",
                "task_id": None,
                "assigned_agent": "mac-omp",
                "host_id": "mac",
            },
        }
        job = {"id": "job-1", "assigned_agent": "other", "runner_profile_id": "mac-omp", "workspace_id": "demo", "task_id": None}
        with self.assertRaisesRegex(ExecutorRoutingError, "does not match job assignment"):
            routing_claim_evidence(payload, job=job)

    def test_missing_decision_rejected(self):
        req = build_routing_request(required_capabilities=["coding"])
        payload = {"routing_request": routing_request_to_dict(req)}
        job = {"assigned_agent": "mac-omp", "runner_profile_id": "mac-omp"}
        with self.assertRaisesRegex(ExecutorRoutingError, "both be present or both absent"):
            routing_claim_evidence(payload, job=job)


class RoutingRequestStrictParseTests(unittest.TestCase):
    """R1-1: stored request parsing must be strict and fail closed."""

    def _canonical_dict(self) -> dict[str, Any]:
        req = build_routing_request(required_capabilities=["coding"])
        return routing_request_to_dict(req)

    def test_parse_rejects_bool_contract_version(self):
        dct = self._canonical_dict()
        dct["contract_version"] = True
        with self.assertRaisesRegex(ExecutorRoutingError, "contract_version must be an integer"):
            parse_routing_request(dct)

    def test_parse_rejects_bool_policy_version(self):
        dct = self._canonical_dict()
        dct["policy_version"] = True
        with self.assertRaisesRegex(ExecutorRoutingError, "policy_version must be an integer"):
            parse_routing_request(dct)

    def test_parse_rejects_unsorted_capabilities(self):
        dct = self._canonical_dict()
        dct["required_capabilities"] = ["review", "coding"]
        with self.assertRaisesRegex(ExecutorRoutingError, "not in canonical sorted order"):
            parse_routing_request(dct)

    def test_parse_rejects_duplicate_capabilities(self):
        dct = self._canonical_dict()
        dct["required_capabilities"] = ["coding", "coding"]
        with self.assertRaisesRegex(ExecutorRoutingError, "contains duplicate values"):
            parse_routing_request(dct)

    def test_parse_rejects_bool_capability_item(self):
        dct = self._canonical_dict()
        dct["required_capabilities"] = [True]
        with self.assertRaisesRegex(ExecutorRoutingError, "item must be a string"):
            parse_routing_request(dct)

    def test_parse_rejects_noncanonical_reason(self):
        req = build_routing_request(
            required_capabilities=["coding"],
            operator_override_agent_id="mac-omp",
            operator_override_reason="operator override",
        )
        dct = routing_request_to_dict(req)
        dct["operator_override_reason"] = "  operator override  "
        with self.assertRaisesRegex(ExecutorRoutingError, "must be canonical stripped text"):
            parse_routing_request(dct)

    def test_caller_input_normalization_separate_from_strict_parse(self):
        # Builder accepts unsorted/duplicate input and returns a canonical object.
        req = build_routing_request(required_capabilities=["review", "coding", "review"])
        self.assertEqual(req.required_capabilities, ("coding", "review"))
        # Strict parser rejects the same unsorted input.
        dct = routing_request_to_dict(req)
        dct["required_capabilities"] = ["review", "coding"]
        with self.assertRaisesRegex(ExecutorRoutingError, "not in canonical sorted order"):
            parse_routing_request(dct)
        # Strict parser rejects duplicates even if sorted.
        dct["required_capabilities"] = ["coding", "coding"]
        with self.assertRaisesRegex(ExecutorRoutingError, "contains duplicate values"):
            parse_routing_request(dct)

    def test_manually_constructed_routing_request_cannot_bypass_parse(self):
        # A dataclass constructed by hand must still round-trip through the strict parser.
        bad = RoutingRequest(
            required_capabilities=("review", "coding"),  # unsorted
            executor_definition_id="coder",
            preferred_host_id="mac",
            operator_override_agent_id=None,
            operator_override_reason=None,
            routing_request_id="sha256:" + "0" * 64,
        )
        with self.assertRaisesRegex(ExecutorRoutingError, "not in canonical sorted order"):
            parse_routing_request(routing_request_to_dict(bad))

    def test_parse_rejects_missing_key(self):
        dct = self._canonical_dict()
        for key in list(dct.keys()):
            mutated = {k: v for k, v in dct.items() if k != key}
            with self.subTest(missing=key):
                with self.assertRaisesRegex(ExecutorRoutingError, "incorrect keys"):
                    parse_routing_request(mutated)

    def test_parse_rejects_extra_key(self):
        dct = self._canonical_dict()
        dct["extra"] = "value"
        with self.assertRaisesRegex(ExecutorRoutingError, "incorrect keys"):
            parse_routing_request(dct)

    def test_parse_rejects_invalid_mode(self):
        dct = self._canonical_dict()
        dct["mode"] = "legacy"
        with self.assertRaisesRegex(ExecutorRoutingError, "mode unsupported"):
            parse_routing_request(dct)

    def test_parse_rejects_bool_override_id(self):
        dct = self._canonical_dict()
        dct["operator_override_agent_id"] = True
        dct["operator_override_reason"] = "reason"
        with self.assertRaisesRegex(ExecutorRoutingError, "operator_override_agent_id must be a string"):
            parse_routing_request(dct)

    def test_parse_rejects_bool_preferred_host_id(self):
        dct = self._canonical_dict()
        dct["preferred_host_id"] = True
        with self.assertRaisesRegex(ExecutorRoutingError, "preferred_host_id must be a string"):
            parse_routing_request(dct)

    def test_parse_rejects_unsafe_label(self):
        dct = self._canonical_dict()
        dct["executor_definition_id"] = "bad/def"
        with self.assertRaisesRegex(ExecutorRoutingError, "unsafe characters"):
            parse_routing_request(dct)

    def test_parse_rejects_override_reason_only(self):
        dct = self._canonical_dict()
        dct["operator_override_reason"] = "reason"
        with self.assertRaisesRegex(ExecutorRoutingError, "both be supplied or both absent"):
            parse_routing_request(dct)

    def test_parse_rejects_override_id_only(self):
        dct = self._canonical_dict()
        dct["operator_override_agent_id"] = "mac-omp"
        with self.assertRaisesRegex(ExecutorRoutingError, "both be supplied or both absent"):
            parse_routing_request(dct)

    def test_parse_rejects_blank_override_reason(self):
        dct = self._canonical_dict()
        dct["operator_override_agent_id"] = "mac-omp"
        dct["operator_override_reason"] = "   "
        with self.assertRaisesRegex(ExecutorRoutingError, "must be canonical stripped text"):
            parse_routing_request(dct)

    def test_parse_rejects_whitespace_only_override_reason(self):
        dct = self._canonical_dict()
        dct["operator_override_agent_id"] = "mac-omp"
        dct["operator_override_reason"] = ""
        with self.assertRaisesRegex(ExecutorRoutingError, "is required when override is supplied"):
            parse_routing_request(dct)

    def test_parse_rejects_invalid_routing_request_id_format(self):
        dct = self._canonical_dict()
        dct["routing_request_id"] = "md5:deadbeef"
        with self.assertRaisesRegex(ExecutorRoutingError, "sha256:<64-lowercase-hex>"):
            parse_routing_request(dct)

    def test_parse_accepts_32_capabilities(self):
        caps = [f"cap{i:02d}" for i in range(32)]
        req = build_routing_request(required_capabilities=caps)
        dct = routing_request_to_dict(req)
        parsed = parse_routing_request(dct)
        self.assertEqual(parsed.required_capabilities, tuple(sorted(caps)))

    def test_parse_rejects_33_capabilities(self):
        caps = [f"cap{i:02d}" for i in range(33)]
        req = build_routing_request(required_capabilities=caps[:32])
        dct = routing_request_to_dict(req)
        dct["required_capabilities"] = sorted(caps)
        import hashlib, json

        body = {k: v for k, v in dct.items() if k != "routing_request_id"}
        canonical = json.dumps(
            body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        dct["routing_request_id"] = f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"
        with self.assertRaisesRegex(ExecutorRoutingError, "exceeds maximum cardinality"):
            parse_routing_request(dct)

    def test_parse_accepts_64_char_capability(self):
        cap = "a" * 64
        req = build_routing_request(required_capabilities=[cap])
        dct = routing_request_to_dict(req)
        parsed = parse_routing_request(dct)
        self.assertEqual(parsed.required_capabilities, (cap,))

    def test_parse_rejects_65_char_capability(self):
        cap = "a" * 65
        short = cap[:64]
        req = build_routing_request(required_capabilities=[short])
        dct = routing_request_to_dict(req)
        dct["required_capabilities"] = [cap]
        import hashlib, json

        body = {k: v for k, v in dct.items() if k != "routing_request_id"}
        canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        dct["routing_request_id"] = f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"
        with self.assertRaisesRegex(ExecutorRoutingError, "exceeds maximum item length: 64"):
            parse_routing_request(dct)


class RoutingDecisionValidationTests(unittest.TestCase):
    """R1-2: stored decision validation must enforce exact shape and links."""

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
            agent_id="mac-omp",
            host_id="mac",
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id="mac-omp", host_id="mac")
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-omp",
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
                agent_id="mac-omp",
                executor_definition_id="coder",
                runner_profile_id="mac-omp",
                enabled=True,
            ),
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
        sync_executor_catalog(self.conn, catalog)

    def _build_decision(self, req: RoutingRequest) -> dict[str, Any]:
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        decision = select_routing_decision(req, candidates)
        return routing_decision_to_dict(decision)

    def test_validate_decision_rejects_unknown_candidate_key(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["extra"] = "value"
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate has incorrect keys"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_boolean_routing_load(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["routing_load"] = True
        with self.assertRaisesRegex(ExecutorRoutingError, "routing_load must be an integer"):
            validate_routing_decision(dct, routing_request=req)

    def _recompute_decision_id(self, dct: dict[str, Any]) -> dict[str, Any]:
        body = {k: v for k, v in dct.items() if k != "routing_decision_id"}
        dct["routing_decision_id"] = _compute_routing_decision_id(body)
        return dct

    def test_validate_decision_rejects_offline_candidate(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._recompute_decision_id(self._build_decision(req))
        dct["eligible_candidates"][0]["online_state"] = "offline"
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate online_state must be 'online'"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_candidate_missing_required_capability(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._recompute_decision_id(self._build_decision(req))
        dct["eligible_candidates"][0]["capabilities"] = ["review"]
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate capabilities .* do not satisfy required"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_candidate_definition_mismatch(self):
        req = build_routing_request(
            required_capabilities=["coding"], executor_definition_id="coder"
        )
        dct = self._recompute_decision_id(self._build_decision(req))
        dct["eligible_candidates"][0]["executor_definition_id"] = "other"
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "executor_definition_id does not match routing_request filter"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_candidate_unsafe_label(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._recompute_decision_id(self._build_decision(req))
        dct["eligible_candidates"][0]["agent_id"] = "bad/agent"
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate agent_id contains unsafe characters"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_non_positive_source_version(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._recompute_decision_id(self._build_decision(req))
        dct["eligible_candidates"][0]["source_version"] = 0
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate source_version must be positive"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_ineligible_candidate_adversarial(self):
        """R2-1: an offline candidate with wrong capabilities/definition recomputed digest still rejected."""
        req = build_routing_request(
            required_capabilities=["coding"], executor_definition_id="coder"
        )
        dct = self._recompute_decision_id(self._build_decision(req))
        candidate = dct["eligible_candidates"][0]
        candidate["online_state"] = "offline"
        candidate["capabilities"] = ["review"]
        candidate["executor_definition_id"] = "other"
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate online_state must be 'online'"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_boolean_source_version(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._recompute_decision_id(self._build_decision(req))
        dct["eligible_candidates"][0]["source_version"] = True
        with self.assertRaisesRegex(ExecutorRoutingError, "source_version must be an integer"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_selection_kind_request_mismatch(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["selection_kind"] = "operator_override"
        with self.assertRaisesRegex(ExecutorRoutingError, "selection_kind must be automatic"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_unsorted_candidates(self):
        req = build_routing_request(required_capabilities=["coding"])
        # Add a second candidate by registering another agent.
        register_agent(
            self.conn,
            agent_id="mac-codex",
            host_id="mac",
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id="mac-codex", host_id="mac")
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="12346",
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
                agent_id="mac-omp",
                executor_definition_id="coder",
                runner_profile_id="mac-omp",
                enabled=True,
            ),
            ExecutorInstanceBinding(
                agent_id="mac-codex",
                executor_definition_id="coder",
                runner_profile_id="mac-codex",
                enabled=True,
            ),
        )
        catalog = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=3,
            catalog_hash="",
            source_path="/dev/null",
            definitions=definitions,
            bindings=bindings,
        )
        catalog = dataclasses.replace(catalog, catalog_hash=compute_executor_catalog_hash(catalog))
        sync_executor_catalog(self.conn, catalog)
        dct = self._build_decision(req)
        # Swap the two candidates; policy order should be mac-codex then mac-omp.
        dct["eligible_candidates"][0], dct["eligible_candidates"][1] = (
            dct["eligible_candidates"][1],
            dct["eligible_candidates"][0],
        )
        with self.assertRaisesRegex(ExecutorRoutingError, "not in policy order"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_duplicate_candidates(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"] = dct["eligible_candidates"] + dct["eligible_candidates"]
        with self.assertRaisesRegex(ExecutorRoutingError, "contains duplicate agent_id"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_selected_mismatch(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["selected_binding_id"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ExecutorRoutingError, "selected candidate not in eligible_candidates"):
            validate_routing_decision(dct, routing_request=req)

    def test_no_preference_host_rank_is_zero(self):
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        # When no preferred host is supplied, every candidate receives host rank
        # zero and preferred_host is false.
        for c in candidates:
            self.assertFalse(c.preferred_host)
            self.assertEqual(c.sort_key()[0], 0)
            self.assertEqual(c.host_rank, 0)

    def test_validate_decision_rejects_preferred_host_true_without_request(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["preferred_host"] = True
        with self.assertRaisesRegex(ExecutorRoutingError, "preferred_host does not match routing_request"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_preferred_host_false_with_request(self):
        req = build_routing_request(
            required_capabilities=["coding"], preferred_host_id="mac"
        )
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["preferred_host"] = False
        with self.assertRaisesRegex(ExecutorRoutingError, "preferred_host does not match routing_request"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_boolean_preferred_host(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["preferred_host"] = 1
        with self.assertRaisesRegex(ExecutorRoutingError, "preferred_host must be a boolean"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_non_string_candidate_field(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["agent_id"] = 123
        with self.assertRaisesRegex(ExecutorRoutingError, "agent_id must be a string"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_unsorted_candidate_capabilities(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["capabilities"] = ["review", "coding"]
        with self.assertRaisesRegex(ExecutorRoutingError, "not in canonical sorted order"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_duplicate_candidate_capabilities(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"][0]["capabilities"] = ["coding", "coding"]
        with self.assertRaisesRegex(ExecutorRoutingError, "contains duplicate values"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_accepts_32_candidate_capabilities(self):
        caps = [f"cap{i:02d}" for i in range(32)]
        req = build_routing_request(required_capabilities=["cap00"])
        definitions = (
            ExecutorDefinition(
                id="coder",
                provider="kimi-code",
                adapter="omp",
                capabilities=tuple(caps),
            ),
        )
        bindings = (
            ExecutorInstanceBinding(
                agent_id="mac-omp",
                executor_definition_id="coder",
                runner_profile_id="mac-omp",
                enabled=True,
            ),
        )
        catalog = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=3,
            catalog_hash="",
            source_path="/dev/null",
            definitions=definitions,
            bindings=bindings,
        )
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_executor_catalog_hash(catalog)
        )
        sync_executor_catalog(self.conn, catalog)
        dct = self._recompute_decision_id(self._build_decision(req))
        validated = validate_routing_decision(dct, routing_request=req)
        self.assertEqual(validated["eligible_candidates"][0]["capabilities"], sorted(caps))

    def test_validate_decision_rejects_33_candidate_capabilities(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._recompute_decision_id(self._build_decision(req))
        caps = [f"cap{i:02d}" for i in range(33)]
        dct["eligible_candidates"][0]["capabilities"] = sorted(caps)
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "exceeds maximum cardinality"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_accepts_64_char_candidate_capability(self):
        cap = "a" * 64
        req = build_routing_request(required_capabilities=[cap])
        definitions = (
            ExecutorDefinition(
                id="coder",
                provider="kimi-code",
                adapter="omp",
                capabilities=(cap,),
            ),
        )
        bindings = (
            ExecutorInstanceBinding(
                agent_id="mac-omp",
                executor_definition_id="coder",
                runner_profile_id="mac-omp",
                enabled=True,
            ),
        )
        catalog = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=3,
            catalog_hash="",
            source_path="/dev/null",
            definitions=definitions,
            bindings=bindings,
        )
        catalog = dataclasses.replace(
            catalog, catalog_hash=compute_executor_catalog_hash(catalog)
        )
        sync_executor_catalog(self.conn, catalog)
        dct = self._recompute_decision_id(self._build_decision(req))
        validated = validate_routing_decision(dct, routing_request=req)
        self.assertEqual(validated["eligible_candidates"][0]["capabilities"], [cap])

    def test_validate_decision_rejects_65_char_candidate_capability(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._recompute_decision_id(self._build_decision(req))
        cap = "a" * 65
        # Keep the required capability in the list so the only illegal point is length.
        dct["eligible_candidates"][0]["capabilities"] = [cap, "coding"]
        dct = self._recompute_decision_id(dct)
        with self.assertRaisesRegex(ExecutorRoutingError, "exceeds maximum item length: 64"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_selected_agent_mismatch(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["selected_agent_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "selected candidate not in eligible_candidates"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_selected_host_mismatch(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["selected_host_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "selected candidate not in eligible_candidates"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_override_selection_kind_mismatch(self):
        req = build_routing_request(
            required_capabilities=["coding"],
            operator_override_agent_id="mac-omp",
            operator_override_reason="need omp",
        )
        dct = self._build_decision(req)
        dct["selection_kind"] = "automatic"
        with self.assertRaisesRegex(ExecutorRoutingError, "selection_kind must be operator_override"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_override_selected_agent_mismatch(self):
        req = build_routing_request(
            required_capabilities=["coding"],
            operator_override_agent_id="mac-omp",
            operator_override_reason="need omp",
        )
        dct = self._build_decision(req)
        # mac-omp is the only candidate, so change selected_agent_id to trigger mismatch.
        dct["selected_agent_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "selected_agent_id does not match operator override"):
            validate_routing_decision(dct, routing_request=req)

    def test_validate_decision_rejects_candidate_cap_exceeded(self):
        req = build_routing_request(required_capabilities=["coding"])
        dct = self._build_decision(req)
        dct["eligible_candidates"] = [dct["eligible_candidates"][0]] * 257
        # Make agent_ids unique to avoid duplicate error first.
        for i, c in enumerate(dct["eligible_candidates"]):
            c["agent_id"] = f"agent-{i:03d}"
        with self.assertRaisesRegex(ExecutorRoutingError, "eligible_candidates exceed cap"):
            validate_routing_decision(dct, routing_request=req)

    def test_policy_order_tie_breaks_definition_and_agent(self):
        req = build_routing_request(required_capabilities=["coding"])
        # Add a second candidate on mac with same load and definition; tie breaks by agent_id.
        register_agent(
            self.conn,
            agent_id="mac-codex",
            host_id="mac",
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id="mac-codex", host_id="mac")
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-codex",
            discord_user_id="12346",
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
            ExecutorDefinition(
                id="coder2",
                provider="kimi-code",
                adapter="omp",
                capabilities=("coding",),
            ),
        )
        bindings = (
            ExecutorInstanceBinding(
                agent_id="mac-omp",
                executor_definition_id="coder2",
                runner_profile_id="mac-omp",
                enabled=True,
            ),
            ExecutorInstanceBinding(
                agent_id="mac-codex",
                executor_definition_id="coder",
                runner_profile_id="mac-codex",
                enabled=True,
            ),
        )
        catalog = ExecutorCatalog(
            source_id="multinexus.discord",
            source_version=3,
            catalog_hash="",
            source_path="/dev/null",
            definitions=definitions,
            bindings=bindings,
        )
        catalog = dataclasses.replace(catalog, catalog_hash=compute_executor_catalog_hash(catalog))
        sync_executor_catalog(self.conn, catalog)
        dct = self._build_decision(req)
        # Order: coder (definition id < coder2), then mac-codex < mac-omp by agent id.
        self.assertEqual(dct["eligible_candidates"][0]["agent_id"], "mac-codex")
        self.assertEqual(dct["eligible_candidates"][1]["agent_id"], "mac-omp")


class RoutingClaimEvidenceLinkTests(unittest.TestCase):
    """R1-3: claim evidence must cross-bind routing decision to binding/context."""

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
            agent_id="mac-omp",
            host_id="mac",
            capabilities={"models": ["test"]},
        )
        heartbeat_agent(self.conn, agent_id="mac-omp", host_id="mac")
        set_workspace_agent(
            self.conn,
            workspace_id="demo",
            agent_name="mac-omp",
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
                agent_id="mac-omp",
                executor_definition_id="coder",
                runner_profile_id="mac-omp",
                enabled=True,
            ),
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
        sync_executor_catalog(self.conn, catalog)

    def _make_payload(self) -> tuple[dict[str, Any], dict[str, Any]]:
        req = build_routing_request(required_capabilities=["coding"])
        candidates = resolve_routing_candidates(self.conn, "demo", req)
        decision = select_routing_decision(req, candidates)
        payload = {
            "routing_request": routing_request_to_dict(req),
            "routing_decision": routing_decision_to_dict(decision),
            "executor_binding": candidates[0].binding_snapshot,
            "execution_context": {
                "job_id": "job-1",
                "workspace_id": "demo",
                "task_id": None,
                "assigned_agent": "mac-omp",
                "host_id": "mac",
            },
        }
        job = {
            "id": "job-1",
            "assigned_agent": "mac-omp",
            "runner_profile_id": "mac-omp",
            "workspace_id": "demo",
            "task_id": None,
        }
        return payload, job

    def test_valid_claim_evidence_with_links(self):
        payload, job = self._make_payload()
        evidence = routing_claim_evidence(payload, job=job)
        self.assertIn("routing_request_id", evidence)
        self.assertIn("routing_decision_id", evidence)
        self.assertIn("selection_kind", evidence)

    def test_forged_binding_rejected(self):
        payload, job = self._make_payload()
        payload["executor_binding"]["binding_id"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ExecutorRoutingError, "selected_binding_id does not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_host_rejected(self):
        payload, job = self._make_payload()
        payload["execution_context"]["host_id"] = "pc"
        with self.assertRaisesRegex(ExecutorRoutingError, "host_id does not match decision"):
            routing_claim_evidence(payload, job=job)

    def test_missing_binding_rejected(self):
        payload, job = self._make_payload()
        del payload["executor_binding"]
        with self.assertRaisesRegex(ExecutorRoutingError, "requires executor_binding and execution_context snapshots"):
            routing_claim_evidence(payload, job=job)

    def test_forged_binding_definition_id_rejected(self):
        payload, job = self._make_payload()
        payload["executor_binding"]["executor_definition_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "selected_executor_definition_id does not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_binding_instance_id_rejected(self):
        payload, job = self._make_payload()
        payload["executor_binding"]["executor_instance_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "selected_agent_id does not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_binding_runner_profile_id_rejected(self):
        payload, job = self._make_payload()
        payload["executor_binding"]["runner_profile_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "selected_runner_profile_id does not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_binding_source_id_rejected(self):
        payload, job = self._make_payload()
        payload["executor_binding"]["source_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate source_id does not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_binding_source_version_rejected(self):
        payload, job = self._make_payload()
        payload["executor_binding"]["source_version"] = 99
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate source_version does not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_binding_catalog_hash_rejected(self):
        payload, job = self._make_payload()
        payload["executor_binding"]["catalog_hash"] = "0" * 64
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate catalog_hash does not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_selected_capabilities_rejected(self):
        """R4-1: selected candidate capabilities must exactly match the stored binding."""
        payload, job = self._make_payload()
        decision = dict(payload["routing_decision"])
        decision["eligible_candidates"] = [dict(c) for c in decision["eligible_candidates"]]
        for c in decision["eligible_candidates"]:
            if c["agent_id"] == decision["selected_agent_id"]:
                c["capabilities"] = sorted(["coding", "review"])
        body = {k: v for k, v in decision.items() if k != "routing_decision_id"}
        decision["routing_decision_id"] = _compute_routing_decision_id(body)
        payload["routing_decision"] = decision
        with self.assertRaisesRegex(ExecutorRoutingError, "candidate capabilities do not match stored binding"):
            routing_claim_evidence(payload, job=job)

    def test_forged_context_workspace_id_rejected(self):
        payload, job = self._make_payload()
        payload["execution_context"]["workspace_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "workspace_id does not match job"):
            routing_claim_evidence(payload, job=job)

    def test_forged_context_task_id_rejected(self):
        payload, job = self._make_payload()
        payload["execution_context"]["task_id"] = "t1"
        with self.assertRaisesRegex(ExecutorRoutingError, "task_id does not match job"):
            routing_claim_evidence(payload, job=job)

    def test_forged_context_job_id_rejected(self):
        payload, job = self._make_payload()
        payload["execution_context"]["job_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "job_id does not match job"):
            routing_claim_evidence(payload, job=job)

    def test_forged_context_assigned_agent_rejected(self):
        payload, job = self._make_payload()
        payload["execution_context"]["assigned_agent"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "assigned_agent does not match decision"):
            routing_claim_evidence(payload, job=job)

    def test_forged_job_assignment_rejected(self):
        payload, job = self._make_payload()
        job["assigned_agent"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "does not match job assignment"):
            routing_claim_evidence(payload, job=job)

    def test_forged_job_runner_profile_id_rejected(self):
        payload, job = self._make_payload()
        job["runner_profile_id"] = "other"
        with self.assertRaisesRegex(ExecutorRoutingError, "does not match job runner_profile_id"):
            routing_claim_evidence(payload, job=job)


if __name__ == "__main__":
    unittest.main()
