"""Focused tests for the checklist I/O boundary and contract parity.

Covers: the old/new resolver matrix (new-only/old-only/none/both fail closed),
raw read + full validation, the callback mutation pipeline, the unique-temp
atomic writer, the initial phase projection, plan-locator runtime problems,
the validator distribution parity (``scripts/harness/validate_checklist.py``
is byte-identical to ``src/coordinate/checklist_contract.py``), and the
U1/U2 semantic parity fixture (errors/warnings captured from the approved U1
implementation).
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinate.checklist_contract import validate_checklist as contract_validate
from coordinate.checklist_io import (
    CHECKLIST_LEGACY_NAME,
    CHECKLIST_NEW_NAME,
    REASON_CHECKLIST_MISSING,
    REASON_DUAL_AUTHORITY,
    REASON_LOCK_TIMEOUT,
    REASON_PHASE_NOT_CREATABLE,
    REASON_VALIDATION_ERROR,
    ChecklistError,
    atomic_write_json,
    checklist_candidates,
    checklist_runtime_problems,
    create_empty_checklist,
    initial_projection,
    load_checklist,
    mutate_checklist,
    reconstruct_projection,
    resolve_checklist,
    sha256_bytes,
)

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts" / "harness"
PARITY_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures" / "checklist_parity" / "expected_errors_warnings.json"
)


def _empty_checklist(name: str = CHECKLIST_NEW_NAME, **root) -> Path:
    """Write a validator-passing empty checklist and return its path."""
    path = Path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "project": "demo",
        "harness_root": ".",
        "updated_at": "2026-07-13",
        "items": [],
    }
    data.update(root)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


class ResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))

    def test_none_fails_closed_with_checklist_missing(self) -> None:
        with self.assertRaises(ChecklistError) as ctx:
            resolve_checklist(self.tmp, purpose="read")
        self.assertEqual(ctx.exception.reason, REASON_CHECKLIST_MISSING)

    def test_new_only_resolves_new(self) -> None:
        path = _empty_checklist(self.tmp / CHECKLIST_NEW_NAME)
        resolved = resolve_checklist(self.tmp, purpose="read")
        self.assertEqual(resolved.path, path)
        self.assertEqual(resolved.kind, "new")

    def test_legacy_only_resolves_legacy(self) -> None:
        path = _empty_checklist(self.tmp / CHECKLIST_LEGACY_NAME)
        resolved = resolve_checklist(self.tmp, purpose="read")
        self.assertEqual(resolved.path, path)
        self.assertEqual(resolved.kind, "legacy")

    def test_both_fail_closed(self) -> None:
        _empty_checklist(self.tmp / CHECKLIST_NEW_NAME)
        _empty_checklist(self.tmp / CHECKLIST_LEGACY_NAME)
        with self.assertRaises(ChecklistError) as ctx:
            resolve_checklist(self.tmp, purpose="read")
        self.assertEqual(ctx.exception.reason, REASON_DUAL_AUTHORITY)

    def test_migrate_purpose_requires_legacy_only(self) -> None:
        with self.assertRaises(ChecklistError):
            resolve_checklist(self.tmp, purpose="migrate")
        _empty_checklist(self.tmp / CHECKLIST_NEW_NAME)
        with self.assertRaises(ChecklistError):
            resolve_checklist(self.tmp, purpose="migrate")
        (self.tmp / CHECKLIST_NEW_NAME).unlink()
        _empty_checklist(self.tmp / CHECKLIST_LEGACY_NAME)
        resolved = resolve_checklist(self.tmp, purpose="migrate")
        self.assertEqual(resolved.kind, "legacy")

    def test_unknown_purpose_rejected(self) -> None:
        with self.assertRaises(ValueError):
            resolve_checklist(self.tmp, purpose="nope")  # type: ignore[arg-type]


class LoadChecklistTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))

    def test_load_validates_current_fails_closed(self) -> None:
        path = self.tmp / CHECKLIST_NEW_NAME
        path.write_text(json.dumps({"items": []}), encoding="utf-8")  # missing root fields
        with self.assertRaises(ChecklistError) as ctx:
            load_checklist(self.tmp, purpose="read")
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)

    def test_load_unparseable_fails_closed(self) -> None:
        path = self.tmp / CHECKLIST_NEW_NAME
        path.write_text("not json{{{{", encoding="utf-8")
        with self.assertRaises(ChecklistError) as ctx:
            load_checklist(self.tmp, purpose="read")
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)

    def test_load_returns_parsed_dict(self) -> None:
        _empty_checklist(self.tmp / CHECKLIST_NEW_NAME)
        data, resolved = load_checklist(self.tmp, purpose="read")
        self.assertEqual(data["items"], [])
        self.assertEqual(resolved.kind, "new")


class MutationPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))
        _empty_checklist(self.tmp / CHECKLIST_NEW_NAME)

    def test_callback_true_writes_and_updates_timestamp(self) -> None:
        def callback(candidate):
            candidate["items"].append(_valid_item())
            return True

        candidate, changed = mutate_checklist(self.tmp, callback)
        self.assertTrue(changed)
        self.assertEqual(len(candidate["items"]), 1)
        self.assertIn("updated_at", candidate)

    def test_callback_false_does_not_rewrite(self) -> None:
        path = self.tmp / CHECKLIST_NEW_NAME
        before = path.read_bytes()
        candidate, changed = mutate_checklist(self.tmp, lambda _c: False)
        self.assertFalse(changed)
        self.assertEqual(path.read_bytes(), before)

    def test_candidate_invalid_rejected_nothing_written(self) -> None:
        path = self.tmp / CHECKLIST_NEW_NAME
        before = path.read_bytes()

        def bad_callback(candidate):
            candidate["items"] = "not a list"
            return True

        with self.assertRaises(ChecklistError) as ctx:
            mutate_checklist(self.tmp, bad_callback)
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)
        self.assertEqual(path.read_bytes(), before)

    def test_runtime_problem_rejected_nothing_written(self) -> None:
        path = self.tmp / CHECKLIST_NEW_NAME
        before = path.read_bytes()

        def bad_callback(candidate):
            candidate["items"].append({"id": "../evil", "note": "x"})
            return True

        with self.assertRaises(ChecklistError):
            mutate_checklist(self.tmp, bad_callback)
        self.assertEqual(path.read_bytes(), before)

    def test_lock_contention_times_out(self) -> None:
        lock_path = (self.tmp / CHECKLIST_NEW_NAME).with_suffix(".json.lock")
        lock_path.write_text(json.dumps({"owner_pid": 1, "created_at": "x"}))
        with self.assertRaises(ChecklistError) as ctx:
            mutate_checklist(self.tmp, lambda _c: True, lock_timeout=0.05)
        self.assertEqual(ctx.exception.reason, REASON_LOCK_TIMEOUT)


class AtomicWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))
        self.target = self.tmp / CHECKLIST_NEW_NAME
        self.target.write_text("ORIGINAL\n")

    def test_writes_json_and_preserves_mode(self) -> None:
        os.chmod(self.target, 0o640)
        atomic_write_json(self.target, {"items": []})
        self.assertEqual(stat.S_IMODE(self.target.stat().st_mode), 0o640)
        self.assertEqual(json.loads(self.target.read_text(encoding="utf-8")), {"items": []})

    def test_unique_temp_cleaned_on_replace_failure(self) -> None:
        with patch("os.replace", side_effect=OSError("replace refused")):
            with self.assertRaises(OSError):
                atomic_write_json(self.target, {"items": []})
        self.assertEqual(self.target.read_text(), "ORIGINAL\n")
        self.assertEqual(list(self.tmp.glob(f".{CHECKLIST_NEW_NAME}.*.tmp")), [])

    def test_unique_temp_cleaned_on_fsync_failure(self) -> None:
        with patch("os.fsync", side_effect=OSError("fsync refused")):
            with self.assertRaises(OSError):
                atomic_write_json(self.target, {"items": []})
        self.assertEqual(self.target.read_text(), "ORIGINAL\n")
        self.assertEqual(list(self.tmp.glob(f".{CHECKLIST_NEW_NAME}.*.tmp")), [])


class CreateEmptyChecklistTests(unittest.TestCase):
    """The init-only empty-checklist creation must reuse the unified
    validator + atomic writer; a failpoint must leave no authority file.
    """

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))

    def test_creates_valid_new_checklist_only(self) -> None:
        path = create_empty_checklist(self.tmp, project="demo", harness_root_rel=".")
        self.assertEqual(path, self.tmp / CHECKLIST_NEW_NAME)
        self.assertFalse((self.tmp / CHECKLIST_LEGACY_NAME).exists())
        data = json.loads(path.read_text(encoding="utf-8"))
        errors, _ = contract_validate(data)
        self.assertEqual(errors, [])
        self.assertEqual(data["project"], "demo")
        self.assertEqual(data["harness_root"], ".")
        self.assertEqual(data["items"], [])
        self.assertIn("updated_at", data)

    def test_refuses_when_new_checklist_already_exists(self) -> None:
        existing = self.tmp / CHECKLIST_NEW_NAME
        existing.write_text("ORIGINAL\n", encoding="utf-8")
        with self.assertRaises(ChecklistError) as ctx:
            create_empty_checklist(self.tmp, project="demo", harness_root_rel=".")
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)
        self.assertEqual(existing.read_text(encoding="utf-8"), "ORIGINAL\n")

    def test_refuses_legacy_only_without_creating_dual_authority(self) -> None:
        # P1-B: the init-only helper must not shadow a legacy authority with a
        # new checklist — legacy-only is reused by init, never duplicated.
        legacy = self.tmp / CHECKLIST_LEGACY_NAME
        legacy.write_text("LEGACY\n", encoding="utf-8")
        with self.assertRaises(ChecklistError) as ctx:
            create_empty_checklist(self.tmp, project="demo", harness_root_rel=".")
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)
        self.assertEqual(legacy.read_text(encoding="utf-8"), "LEGACY\n")
        self.assertFalse((self.tmp / CHECKLIST_NEW_NAME).exists())

    def test_refuses_both_with_dual_authority_reason(self) -> None:
        # P1-B: both candidates present must fail closed with the stable
        # dual_authority reason and leave both files byte-identical.
        legacy = self.tmp / CHECKLIST_LEGACY_NAME
        new = self.tmp / CHECKLIST_NEW_NAME
        legacy.write_text("LEGACY\n", encoding="utf-8")
        new.write_text("NEW\n", encoding="utf-8")
        with self.assertRaises(ChecklistError) as ctx:
            create_empty_checklist(self.tmp, project="demo", harness_root_rel=".")
        self.assertEqual(ctx.exception.reason, REASON_DUAL_AUTHORITY)
        self.assertEqual(legacy.read_text(encoding="utf-8"), "LEGACY\n")
        self.assertEqual(new.read_text(encoding="utf-8"), "NEW\n")

    def test_atomic_writer_failure_leaves_no_authority_file(self) -> None:
        with patch(
            "coordinate.checklist_io.atomic_write_bytes",
            side_effect=OSError("write refused"),
        ):
            with self.assertRaises(OSError):
                create_empty_checklist(self.tmp, project="demo", harness_root_rel=".")
        self.assertFalse((self.tmp / CHECKLIST_NEW_NAME).exists())
        self.assertEqual(list(self.tmp.glob(f".{CHECKLIST_NEW_NAME}.*.tmp")), [])


class ProjectionTests(unittest.TestCase):
    def test_safe_planning_phases_map_todo_todo(self) -> None:
        for phase in ("ready", "planned", "todo"):
            with self.subTest(phase=phase):
                self.assertEqual(initial_projection(phase), ("todo", "todo"))

    def test_arbitrary_project_label_maps_todo_todo(self) -> None:
        self.assertEqual(initial_projection("phase-8"), ("todo", "todo"))
        self.assertEqual(initial_projection("milestone-3"), ("todo", "todo"))

    def test_lifecycle_phases_fail_closed(self) -> None:
        for phase in (
            "assigned", "accepted", "awaiting_operator", "running",
            "handoff_requested", "review_requested", "ready_for_review",
            "closeout_requested", "review_approved", "changes_requested",
            "unblocked",
        ):
            with self.subTest(phase=phase):
                with self.assertRaises(ChecklistError) as ctx:
                    initial_projection(phase)
                self.assertEqual(ctx.exception.reason, REASON_PHASE_NOT_CREATABLE)

    def test_blocked_fails_closed(self) -> None:
        with self.assertRaises(ChecklistError) as ctx:
            initial_projection("blocked")
        self.assertEqual(ctx.exception.reason, REASON_PHASE_NOT_CREATABLE)

    def test_terminal_phases_fail_closed(self) -> None:
        for phase in ("released", "closed", "done"):
            with self.subTest(phase=phase):
                with self.assertRaises(ChecklistError) as ctx:
                    initial_projection(phase)
                self.assertEqual(ctx.exception.reason, REASON_PHASE_NOT_CREATABLE)

    def test_reconstruct_preserves_historical_mapping(self) -> None:
        # Read-only proof verification must reproduce pre-U2 creation items.
        self.assertEqual(reconstruct_projection("ready"), ("todo", "todo"))
        self.assertEqual(reconstruct_projection("awaiting_operator"), ("doing", "awaiting_operator"))
        self.assertEqual(reconstruct_projection("blocked"), ("blocked", "blocked"))
        self.assertEqual(reconstruct_projection("done"), ("done", "closed"))


class RuntimeProblemsTests(unittest.TestCase):
    def test_dual_locator_conflict_detected(self) -> None:
        checklist = {
            "items": [
                {
                    "id": "task-1",
                    "plan_path": "docs/plan.md",
                    "artifacts": {"plan": "docs/other.md"},
                }
            ]
        }
        problems = checklist_runtime_problems(checklist)
        self.assertEqual(len(problems), 1)
        self.assertIn("conflicting plan locators", problems[0])

    def test_lexical_dotdot_detected(self) -> None:
        checklist = {"items": [{"id": "task-1", "plan_path": "docs/../escape.md"}]}
        problems = checklist_runtime_problems(checklist)
        self.assertEqual(len(problems), 1)
        self.assertIn("'..'", problems[0])

    def test_safe_id_and_matching_locators_clean(self) -> None:
        checklist = {
            "items": [
                {
                    "id": "task-1",
                    "plan_path": "docs/plan.md",
                    "artifacts": {"plan": "docs/plan.md"},
                }
            ]
        }
        self.assertEqual(checklist_runtime_problems(checklist), [])

    def test_unsafe_item_id_detected(self) -> None:
        checklist = {"items": [{"id": "a/b", "plan_path": "docs/plan.md"}]}
        problems = checklist_runtime_problems(checklist)
        self.assertEqual(len(problems), 1)
        self.assertIn("path separator", problems[0])


class ValidatorDistributionParityTests(unittest.TestCase):
    def test_distribution_copy_is_byte_identical_to_source(self) -> None:
        source = (SRC_DIR / "coordinate" / "checklist_contract.py").read_bytes()
        distribution = (SCRIPTS_DIR / "validate_checklist.py").read_bytes()
        self.assertEqual(source, distribution)

    def test_semantic_parity_with_u1_approved_fixture(self) -> None:
        """Every case's (errors, warnings) must match the expectations captured
        from the approved U1 implementation (the joint release gate re-runs the
        same matrix against EXharness directly)."""
        cases = {
            "empty_valid": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": []},
            "item_with_split_shape": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                {
                    "id": "task-1", "title": "Task", "status": "todo", "priority": "p1",
                    "owner": None, "selected_in_session": None, "verification": "",
                    "updated_at": "2026-01-01T00:00:00Z", "dependencies": [],
                    "blocked_by": [], "blocked_reason": "", "acceptance": "Accept",
                    "handoff": {"from": None, "to": None, "reason": None},
                    "workflow": {"status": "todo", "branch": None, "updated_at": "2026-01-01T00:00:00Z"},
                    "phase": "ready", "human_gate_required": True,
                    "artifacts": {"plan": "docs/plan.md"}, "review": {},
                    "split_operation": {"contract_version": 1, "operation_id": "x"},
                }
            ]},
            "invalid_priority": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(priority="high")
            ]},
            "invalid_status": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(status="pending")
            ]},
            "done_without_verification": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(status="done", workflow_status="closed", verification="")
            ]},
            "done_with_verification": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(status="done", workflow_status="closed", verification="verified")
            ]},
            "missing_dependency": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(dependencies=["missing-1"])
            ]},
            "unknown_fields": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(future_field={"nested": [1, 2]})
            ], "version": 2},
            "blocked_warnings": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(status="blocked", workflow_status="blocked")
            ]},
            "done_workflow_todo": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(status="done", workflow_status="todo", verification="v")
            ]},
            "doing_without_owner": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(status="doing", workflow_status="running", owner=None)
            ]},
            "missing_item_field": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                {k: v for k, v in _item_with().items() if k != "acceptance"}
            ]},
            "non_object_root": [1, 2, 3],
            "items_not_list": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": "nope"},
            "duplicate_ids": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(), _item_with(title="Other")
            ]},
            "empty_acceptance": {"project": "p", "harness_root": ".", "updated_at": "2026-01-01", "items": [
                _item_with(acceptance="")
            ]},
        }
        expected = json.loads(PARITY_FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(set(cases), set(expected))
        for name, data in cases.items():
            with self.subTest(case=name):
                errors, warnings = contract_validate(data)
                self.assertEqual(errors, expected[name]["errors"], name)
                self.assertEqual(warnings, expected[name]["warnings"], name)


def _valid_item():
    return {
        "id": "task-1", "title": "Task", "status": "todo", "priority": "p1",
        "owner": None, "selected_in_session": None, "verification": "",
        "updated_at": "2026-01-01T00:00:00Z", "dependencies": [],
        "blocked_by": [], "blocked_reason": "", "acceptance": "Accept",
        "handoff": {"from": None, "to": None, "reason": None},
        "workflow": {"status": "todo", "branch": None, "updated_at": "2026-01-01T00:00:00Z"},
    }


def _item_with(**overrides):
    item = {
        "id": "task-1", "title": "Task", "status": "todo", "priority": "p1",
        "owner": None, "selected_in_session": None, "verification": "",
        "updated_at": "2026-01-01T00:00:00Z", "dependencies": [],
        "blocked_by": [], "blocked_reason": "", "acceptance": "Accept",
        "handoff": {"from": None, "to": None, "reason": None},
        "workflow": {"status": "todo", "branch": None, "updated_at": "2026-01-01T00:00:00Z"},
    }
    workflow_status = overrides.pop("workflow_status", None)
    if workflow_status is not None:
        item["workflow"]["status"] = workflow_status
    item.update(overrides)
    return item


class FreshnessDigestTests(unittest.TestCase):
    def test_sha256_bytes_is_byte_exact(self) -> None:
        payload = b"bytes"
        self.assertEqual(
            sha256_bytes(payload),
            hashlib.sha256(payload).hexdigest(),
        )

    def test_candidates_lists_both_names(self) -> None:
        candidates = checklist_candidates(".")
        self.assertEqual(candidates["new"].name, CHECKLIST_NEW_NAME)
        self.assertEqual(candidates["legacy"].name, CHECKLIST_LEGACY_NAME)


if __name__ == "__main__":
    unittest.main()
