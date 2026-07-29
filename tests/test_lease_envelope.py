"""Canonical negative fixture tests for the v1 execution_lease envelope.

These tests are Coordinate-owned and do not import MultiNexus code. They verify
that ``parse_execution_lease`` rejects malformed structural envelopes and that
``validate_execution_lease`` rejects semantic cross-link mismatches.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any

from coordinate.lease_envelope import (
    LeaseEnvelopeError,
    parse_execution_lease,
    validate_execution_lease,
)


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


class LeaseEnvelopePositiveTests(unittest.TestCase):
    def test_positive_fixture_parses(self):
        data = _load_fixture("execution_lease_v1_positive.json")
        lease = parse_execution_lease(data)
        self.assertEqual(lease.contract_version, 1)
        self.assertEqual(lease.lease_id, data["lease_id"])
        self.assertEqual(
            lease.resource_key,
            "sha256:c963d5aa94bb80886234867484e9fb8e47d7f6a75da6fc2ed4b2f47679040ffe",
        )

    def test_positive_fixture_validates(self):
        data = _load_fixture("execution_lease_v1_positive.json")
        lease = validate_execution_lease(data)
        self.assertEqual(lease.job_id, data["job_id"])
        self.assertEqual(lease.normalized_path, "/home/synthetic-user/projects/multinexus")


class LeaseEnvelopeNegativeFixtureTests(unittest.TestCase):
    def test_missing_keys_rejected(self):
        data = _load_fixture("execution_lease_v1_missing_keys.json")
        with self.assertRaisesRegex(LeaseEnvelopeError, "incorrect keys.*missing"):
            parse_execution_lease(data)

    def test_bad_identity_rejected(self):
        data = _load_fixture("execution_lease_v1_bad_identity.json")
        with self.assertRaisesRegex(LeaseEnvelopeError, "agent_id mismatch"):
            validate_execution_lease(
                data,
                expected_agent_id="mac-omp",
                expected_job_id=data["job_id"],
                expected_attempt_token=data["attempt_token"],
            )

    def test_context_mismatch_rejected(self):
        data = _load_fixture("execution_lease_v1_context_mismatch.json")
        with self.assertRaisesRegex(LeaseEnvelopeError, "normalized_path does not match"):
            validate_execution_lease(
                data,
                expected_agent_id=data["agent_id"],
                execution_context={
                    "job_id": data["job_id"],
                    "assigned_agent": data["agent_id"],
                    "host_id": data["host_id"],
                    "worktree_path": "/home/synthetic-user/projects/multinexus",
                },
            )

    def test_resource_mismatch_rejected(self):
        data = _load_fixture("execution_lease_v1_resource_mismatch.json")
        with self.assertRaisesRegex(
            LeaseEnvelopeError, "resource_key does not match normalized_path"
        ):
            validate_execution_lease(data)

    def test_bad_digest_rejected(self):
        data = _load_fixture("execution_lease_v1_bad_digest.json")
        with self.assertRaisesRegex(LeaseEnvelopeError, "resource_key must be sha256"):
            parse_execution_lease(data)

    def test_bad_timestamps_rejected(self):
        data = _load_fixture("execution_lease_v1_bad_timestamps.json")
        with self.assertRaisesRegex(
            LeaseEnvelopeError, "ttl_seconds .* does not match acquired_at/expires_at interval"
        ):
            parse_execution_lease(data)

    def test_invalid_ttl_interval_rejected(self):
        data = _load_fixture("execution_lease_v1_invalid_ttl_interval.json")
        with self.assertRaisesRegex(
            LeaseEnvelopeError, "renew_interval_seconds .* must be < ttl_seconds"
        ):
            parse_execution_lease(data)

    def test_stale_token_rejected(self):
        data = _load_fixture("execution_lease_v1_stale_token.json")
        with self.assertRaisesRegex(LeaseEnvelopeError, "attempt_token must be positive"):
            parse_execution_lease(data)

    def test_extra_keys_rejected(self):
        data = _load_fixture("execution_lease_v1_extra_keys.json")
        with self.assertRaisesRegex(LeaseEnvelopeError, "incorrect keys.*unexpected"):
            parse_execution_lease(data)


class LeaseEnvelopeStructuralTests(unittest.TestCase):
    def _positive(self) -> dict[str, Any]:
        return dict(_load_fixture("execution_lease_v1_positive.json"))

    def test_resource_kind_must_be_worktree(self):
        data = self._positive()
        data["resource_kind"] = "other"
        with self.assertRaisesRegex(LeaseEnvelopeError, "resource_kind must be 'worktree'"):
            parse_execution_lease(data)

    def test_negative_fixtures_are_single_fault_against_positive(self):
        positive = self._positive()
        cases = {
            "missing_keys": ({"lease_id"}, set(), set()),
            "bad_identity": (set(), set(), {"agent_id"}),
            "bad_timestamps": (set(), set(), {"expires_at"}),
            "context_mismatch": (
                set(),
                set(),
                {"normalized_path", "resource_key"},
            ),
            "extra_keys": (set(), {"extra_key"}, set()),
            "invalid_ttl_interval": (set(), set(), {"renew_interval_seconds"}),
            "resource_mismatch": (set(), set(), {"resource_key"}),
            "stale_token": (set(), set(), {"attempt_token"}),
            "bad_digest": (set(), set(), {"resource_key"}),
        }
        for name, (expected_missing, expected_extra, expected_changed) in cases.items():
            data = _load_fixture(f"execution_lease_v1_{name}.json")
            missing = set(positive) - set(data)
            extra = set(data) - set(positive)
            changed = {
                key
                for key in set(positive) & set(data)
                if data[key] != positive[key]
            }
            self.assertEqual(missing, expected_missing, f"{name}: missing keys")
            self.assertEqual(extra, expected_extra, f"{name}: extra keys")
            self.assertEqual(changed, expected_changed, f"{name}: changed values")

    def test_noncanonical_path_rejected_in_validate(self):
        data = self._positive()
        data["normalized_path"] = "/home/synthetic-user/projects/multinexus/"
        with self.assertRaisesRegex(LeaseEnvelopeError, "normalized_path is not canonical"):
            validate_execution_lease(data)

    def test_noncanonical_path_rejected_in_parse(self):
        data = self._positive()
        data["normalized_path"] = "/home/synthetic-user/projects/multinexus/../multinexus"
        with self.assertRaisesRegex(LeaseEnvelopeError, "contains traversal"):
            parse_execution_lease(data)

    def test_server_now_at_expiry_rejected(self):
        data = self._positive()
        data["server_now"] = data["expires_at"]
        with self.assertRaisesRegex(LeaseEnvelopeError, "server_now must be before expires_at"):
            parse_execution_lease(data)

    def test_server_now_after_expiry_rejected(self):
        data = self._positive()
        data["server_now"] = "2026-07-14T12:02:01Z"
        with self.assertRaisesRegex(LeaseEnvelopeError, "server_now must be before expires_at"):
            parse_execution_lease(data)


class LeaseEnvelopeCrossLinkTests(unittest.TestCase):
    def _valid_envelope(self) -> dict[str, Any]:
        return dict(_load_fixture("execution_lease_v1_positive.json"))

    def test_expected_job_id_mismatch(self):
        data = self._valid_envelope()
        with self.assertRaisesRegex(LeaseEnvelopeError, "job_id mismatch"):
            validate_execution_lease(data, expected_job_id="other")

    def test_expected_attempt_token_mismatch(self):
        data = self._valid_envelope()
        with self.assertRaisesRegex(LeaseEnvelopeError, "attempt_token mismatch"):
            validate_execution_lease(data, expected_attempt_token=99)

    def test_context_worktree_mismatch(self):
        data = self._valid_envelope()
        with self.assertRaisesRegex(LeaseEnvelopeError, "normalized_path does not match"):
            validate_execution_lease(
                data,
                execution_context={
                    "job_id": data["job_id"],
                    "assigned_agent": data["agent_id"],
                    "host_id": data["host_id"],
                    "worktree_path": "/other/path",
                },
            )

    def test_executor_binding_runner_profile_mismatch(self):
        data = self._valid_envelope()
        with self.assertRaisesRegex(LeaseEnvelopeError, "runner_profile_id does not match"):
            validate_execution_lease(
                data,
                executor_binding={
                    "executor_instance_id": data["agent_id"],
                    "runner_profile_id": "other-runner",
                },
            )


if __name__ == "__main__":
    unittest.main()
