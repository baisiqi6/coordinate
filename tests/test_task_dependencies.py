"""Focused tests for the Coordinate-managed dependency update entry points.

Covers the bootstrap verification matrix for
``coordinate task update-dependencies`` (combined same-host) and
``coordinate task update-dependencies-files`` (coding-host file-only):

- new-only / legacy-only checklists; dual authority / missing / invalid
  current fail closed with zero mutation;
- target / dependency / self / add-remove overlap rejections;
- multiple add/remove, input dedupe, field + order preservation, exact
  replay / no-op idempotency;
- pre-commit atomic writer failure leaves the original bytes untouched;
  a post-commit failure is reported honestly as unknown commit status and
  re-running the same desired-state command converges;
- file-only never opens the Coordinate DB and is not saddled with a
  harnessctl preflight;
- combined preflight fails before any mutation when harnessctl is missing;
- combined updates only the target task mirror; refresh/reconcile failure
  returns structured recovery and re-running converges;
- /opt runtime-copy guard stays fail closed with the explicit repair
  override unchanged in scope;
- CLI help / argument / JSON output contract.
"""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from coordinate.checklist_io import (
    CHECKLIST_LEGACY_NAME,
    CHECKLIST_NEW_NAME,
    REASON_DUAL_AUTHORITY,
    REASON_VALIDATION_ERROR,
    ChecklistError,
    ChecklistLock,
)
from coordinate.cli import build_parser, main as cli_main
from coordinate.db import (
    get_workspace,
    initialize,
    list_events,
    list_task_mirrors,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.harness import HarnessError
from coordinate.planning_cli import (
    handle_task_update_dependencies,
    handle_task_update_dependencies_files,
)
from coordinate.reconcile import ReconcileConflictError
from coordinate.split_operations import compute_task_item_fingerprint
from coordinate.task_dependencies import (
    REASON_DEPENDENCY_NOT_FOUND,
    REASON_HARNESSCTL_UNAVAILABLE,
    REASON_INVALID_INPUT,
    REASON_IO_ERROR,
    REASON_OVERLAP,
    REASON_RELATIVE_PATH,
    REASON_RUNTIME_COPY,
    REASON_SELF_DEPENDENCY,
    REASON_TARGET_NOT_FOUND,
    DependencyUpdateError,
    TaskDependenciesRecoveryError,
    apply_dependency_mutation,
    update_task_dependencies,
)


def _item(item_id: str, dependencies=(), **extra) -> dict:
    item = {
        "id": item_id,
        "title": f"Task {item_id}",
        "status": "todo",
        "priority": "p1",
        "owner": None,
        "selected_in_session": None,
        "verification": "",
        "updated_at": "2026-01-01T00:00:00Z",
        "dependencies": list(dependencies),
        "blocked_by": [],
        "blocked_reason": None,
        "acceptance": "Accept",
        "handoff": "",
    }
    item.update(extra)
    return item


def _write_checklist(path: Path, items, *, project: str = "demo") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "project": project,
        "harness_root": ".",
        "version": 1,
        "updated_at": "2026-07-13",
        "items": items,
    }
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def _checklist_paths(tmp: Path) -> tuple[Path, Path]:
    return tmp / CHECKLIST_NEW_NAME, tmp / CHECKLIST_LEGACY_NAME


class FakeHarnessAdapter:
    """HarnessAdapter stand-in: dynamic checklist reads from the real file,
    controllable harnessctl availability and refresh failure."""

    def __init__(self, harness_root, state, *, harnessctl_ok=True, refresh_error=None):
        self.harness_root = Path(harness_root)
        self.state = state
        self.harnessctl_ok = harnessctl_ok
        self.refresh_error = refresh_error
        self.refresh_calls = 0

    def harnessctl_available(self) -> bool:
        return self.harnessctl_ok

    def refresh_state(self) -> dict:
        self.refresh_calls += 1
        if self.refresh_error is not None:
            raise self.refresh_error
        return self.state

    def read_state(self) -> dict:
        return self.state

    def read_checklist(self) -> dict:
        from coordinate.checklist_io import load_checklist

        data, _ = load_checklist(self.harness_root, purpose="read")
        return data


class DependencyMutationFileTests(unittest.TestCase):
    """File-half semantics shared by both commands (apply_dependency_mutation)."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))
        self.new_path, self.legacy_path = _checklist_paths(self.tmp)

    def _harness(self, name: str, items) -> Path:
        path = self.new_path if name == "new" else self.legacy_path
        _write_checklist(path, items)
        return self.tmp

    def _legacy_harness(self, items) -> Path:
        _write_checklist(self.legacy_path, items)
        return self.tmp

    def _result(self, harness: Path, task_id="t1", add=(), remove=()):
        return apply_dependency_mutation(
            workspace_path=str(self.tmp),
            harness_root=str(harness),
            workspace_id="demo",
            task_id=task_id,
            add=add,
            remove=remove,
        )

    def _deps_of(self, harness: Path, task_id: str) -> list:
        path = (
            harness / CHECKLIST_NEW_NAME
            if (harness / CHECKLIST_NEW_NAME).exists()
            else harness / CHECKLIST_LEGACY_NAME
        )
        data = json.loads(path.read_text(encoding="utf-8"))
        for item in data["items"]:
            if item["id"] == task_id:
                return item["dependencies"]
        raise AssertionError(f"task {task_id} missing")

    def test_new_only_add_and_remove(self) -> None:
        harness = self._harness("new", [_item("t1"), _item("t2"), _item("t3")])
        result = self._result(harness, add=["t2", "t3"])
        self.assertTrue(result.changed)
        self.assertEqual(self._deps_of(harness, "t1"), ["t2", "t3"])
        self.assertEqual(
            [o.to_dict() for o in result.outcomes],
            [
                {"dependency": "t2", "action": "add", "outcome": "applied"},
                {"dependency": "t3", "action": "add", "outcome": "applied"},
            ],
        )
        self.assertEqual(list(result.dependencies), ["t2", "t3"])

        result = self._result(harness, remove=["t2"])
        self.assertEqual(self._deps_of(harness, "t1"), ["t3"])
        self.assertEqual(
            [o.outcome for o in result.outcomes], ["applied"]
        )

    def test_legacy_only_add_and_remove(self) -> None:
        harness = self._legacy_harness([_item("t1"), _item("t2")])
        result = self._result(harness, add=["t2"])
        self.assertTrue(result.changed)
        # The legacy file itself was modified and no new-filename authority was
        # created next to it.
        self.assertEqual(self._deps_of(harness, "t1"), ["t2"])
        self.assertFalse(self.new_path.exists())

        result = self._result(harness, remove=["t2"])
        self.assertEqual(self._deps_of(harness, "t1"), [])

    def test_dual_authority_fails_closed_zero_mutation(self) -> None:
        _write_checklist(self.new_path, [_item("t1")])
        _write_checklist(self.legacy_path, [_item("t1")])
        before_new = self.new_path.read_bytes()
        before_legacy = self.legacy_path.read_bytes()
        with self.assertRaises(ChecklistError) as ctx:
            self._result(self.tmp, add=["t1"])
        self.assertEqual(ctx.exception.reason, REASON_DUAL_AUTHORITY)
        self.assertEqual(self.new_path.read_bytes(), before_new)
        self.assertEqual(self.legacy_path.read_bytes(), before_legacy)

    def test_missing_checklist_fails_closed(self) -> None:
        missing = self.tmp / "no-such-root"
        with self.assertRaises(ChecklistError) as ctx:
            self._result(missing, add=["t1"])
        self.assertEqual(ctx.exception.reason, "checklist_missing")

    def test_invalid_current_fails_closed_zero_mutation(self) -> None:
        self.new_path.write_text(json.dumps({"items": []}), encoding="utf-8")
        before = self.new_path.read_bytes()
        with self.assertRaises(ChecklistError) as ctx:
            self._result(self.tmp, add=["t1"])
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)
        self.assertEqual(self.new_path.read_bytes(), before)

    def test_unparseable_current_fails_closed(self) -> None:
        self.new_path.write_text("not json{{{{", encoding="utf-8")
        with self.assertRaises(ChecklistError) as ctx:
            self._result(self.tmp, add=["t1"])
        self.assertEqual(ctx.exception.reason, REASON_VALIDATION_ERROR)

    def test_target_not_found_fails_closed(self) -> None:
        harness = self._harness("new", [_item("t2")])
        before = self.new_path.read_bytes()
        with self.assertRaises(DependencyUpdateError) as ctx:
            self._result(harness, task_id="ghost", add=["t2"])
        self.assertEqual(ctx.exception.reason, REASON_TARGET_NOT_FOUND)
        self.assertEqual(self.new_path.read_bytes(), before)

    def test_dependency_not_found_fails_closed(self) -> None:
        harness = self._harness("new", [_item("t1")])
        before = self.new_path.read_bytes()
        with self.assertRaises(DependencyUpdateError) as ctx:
            self._result(harness, add=["ghost"])
        self.assertEqual(ctx.exception.reason, REASON_DEPENDENCY_NOT_FOUND)
        self.assertEqual(self.new_path.read_bytes(), before)

    def test_self_dependency_fails_closed(self) -> None:
        harness = self._harness("new", [_item("t1")])
        before = self.new_path.read_bytes()
        with self.assertRaises(DependencyUpdateError) as ctx:
            self._result(harness, add=["t1"])
        self.assertEqual(ctx.exception.reason, REASON_SELF_DEPENDENCY)
        self.assertEqual(self.new_path.read_bytes(), before)

    def test_overlap_fails_closed(self) -> None:
        harness = self._harness("new", [_item("t1"), _item("t2")])
        before = self.new_path.read_bytes()
        with self.assertRaises(DependencyUpdateError) as ctx:
            self._result(harness, add=["t2"], remove=["t2"])
        self.assertEqual(ctx.exception.reason, REASON_OVERLAP)
        self.assertEqual(self.new_path.read_bytes(), before)

    def test_no_add_or_remove_fails_closed(self) -> None:
        harness = self._harness("new", [_item("t1")])
        with self.assertRaises(DependencyUpdateError) as ctx:
            self._result(harness)
        self.assertEqual(ctx.exception.reason, REASON_INVALID_INPUT)

    def test_input_dedupe_preserves_order_and_untouched_fields(self) -> None:
        harness = self._harness(
            "new",
            [
                _item("t1", ["t2", "t4"], artifacts={"branch": "b1"}, plan_path="p.md"),
                _item("t2"),
                _item("t3"),
                _item("t4"),
            ],
        )
        result = self._result(harness, add=["t3", "t2", "t3"], remove=["t4"])
        self.assertEqual(
            [o.to_dict() for o in result.outcomes],
            [
                {"dependency": "t4", "action": "remove", "outcome": "applied"},
                {"dependency": "t3", "action": "add", "outcome": "applied"},
                {"dependency": "t2", "action": "add", "outcome": "already_satisfied"},
            ],
        )
        data = json.loads(self.new_path.read_text(encoding="utf-8"))
        target = next(item for item in data["items"] if item["id"] == "t1")
        # Order: untouched deps first (deduped), adds appended in caller order.
        self.assertEqual(target["dependencies"], ["t2", "t3"])
        # Unknown compatible fields and other items are untouched.
        self.assertEqual(target["artifacts"], {"branch": "b1"})
        self.assertEqual(target["plan_path"], "p.md")
        self.assertEqual(len(data["items"]), 4)

    def test_exact_replay_is_noop_preserving_bytes(self) -> None:
        harness = self._harness("new", [_item("t1"), _item("t2")])
        first = self._result(harness, add=["t2"])
        self.assertTrue(first.changed)
        self.assertEqual(
            [o.outcome for o in first.outcomes], ["applied"]
        )
        bytes_after_first = self.new_path.read_bytes()

        second = self._result(harness, add=["t2"])
        self.assertFalse(second.changed)
        self.assertEqual(
            [o.to_dict() for o in second.outcomes],
            [{"dependency": "t2", "action": "add", "outcome": "already_satisfied"}],
        )
        self.assertEqual(self.new_path.read_bytes(), bytes_after_first)
        self.assertEqual(second.before_fingerprint, second.after_fingerprint)

    def test_remove_absent_is_noop(self) -> None:
        harness = self._harness("new", [_item("t1")])
        result = self._result(harness, remove=["t2"])
        self.assertFalse(result.changed)
        self.assertEqual(
            [o.to_dict() for o in result.outcomes],
            [{"dependency": "t2", "action": "remove", "outcome": "already_satisfied"}],
        )
        self.assertEqual(result.before_fingerprint, result.after_fingerprint)

    def test_mixed_applied_and_already_satisfied(self) -> None:
        harness = self._harness(
            "new", [_item("t1", ["t2", "t3"]), _item("t2"), _item("t3"), _item("t4")]
        )
        result = self._result(harness, add=["t2", "t4"], remove=["t3"])
        self.assertTrue(result.changed)
        self.assertEqual(
            [o.outcome for o in result.outcomes],
            ["applied", "already_satisfied", "applied"],
        )
        self.assertEqual(self._deps_of(harness, "t1"), ["t2", "t4"])

    def test_writer_precommit_failure_leaves_original_bytes(self) -> None:
        # Pre-commit (before os.replace) failure: original bytes untouched and
        # the error is surfaced as a stable io_error, not a traceback.
        harness = self._harness("new", [_item("t1"), _item("t2")])
        before = self.new_path.read_bytes()
        with patch(
            "coordinate.checklist_io.atomic_write_bytes",
            side_effect=OSError("mocked write failure"),
        ):
            with self.assertRaises(DependencyUpdateError) as ctx:
                self._result(harness, add=["t2"])
        self.assertEqual(ctx.exception.reason, REASON_IO_ERROR)
        self.assertIn("unknown commit status", str(ctx.exception))
        self.assertEqual(self.new_path.read_bytes(), before)

    def test_writer_postcommit_failure_is_honest_and_rerun_converges(self) -> None:
        # Real atomic writer completes (os.replace + fsync) and THEN raises:
        # the file is committed, the error still says commit status is unknown,
        # the DB half never ran, and the same desired-state command converges
        # without creating a second authority.
        from coordinate.checklist_io import atomic_write_bytes as real_atomic

        harness = self._harness("new", [_item("t1"), _item("t2")])

        def fail_after_commit(path, data, mode=None):
            real_atomic(path, data, mode=mode)
            raise OSError("mocked post-commit fsync failure")

        with patch(
            "coordinate.checklist_io.atomic_write_bytes",
            side_effect=fail_after_commit,
        ):
            with self.assertRaises(DependencyUpdateError) as ctx:
                self._result(harness, add=["t2"])
        self.assertEqual(ctx.exception.reason, REASON_IO_ERROR)
        self.assertIn("unknown commit status", str(ctx.exception))
        # The file actually committed; only one checklist authority exists.
        self.assertEqual(self._deps_of(harness, "t1"), ["t2"])
        self.assertEqual(
            [p.name for p in self.tmp.iterdir() if p.suffix == ".json"],
            [CHECKLIST_NEW_NAME],
        )
        # Re-run converges: no further mutation, already satisfied.
        result = self._result(harness, add=["t2"])
        self.assertFalse(result.changed)
        self.assertEqual(
            [o.outcome for o in result.outcomes], ["already_satisfied"]
        )
        self.assertEqual(result.before_fingerprint, result.after_fingerprint)
        self.assertEqual(self._deps_of(harness, "t1"), ["t2"])

    def test_remove_reorders_nothing_and_removes_all_occurrences(self) -> None:
        harness = self._harness("new", [_item("t1", ["t2", "t3"]), _item("t2"), _item("t3")])
        result = self._result(harness, remove=["t3"])
        self.assertEqual(self._deps_of(harness, "t1"), ["t2"])
        self.assertEqual([o.outcome for o in result.outcomes], ["applied"])

    def test_fingerprints_bind_locked_item_not_pre_call_snapshot(self) -> None:
        # An external writer committing between the command start and the
        # mutation's locked read must be reflected in the receipt: the
        # before/after fingerprints come from the item the callback actually
        # sees under the lock, never from a pre-lock byte snapshot.
        harness = self._harness("new", [_item("t1"), _item("t2")])

        def external_write():
            _write_checklist(self.new_path, [_item("t1", ["t2"]), _item("t2")])

        class RewriteBeforeCallbackLock(ChecklistLock):
            def acquire(self):
                external_write()
                super().acquire()

        result = apply_dependency_mutation(
            workspace_path=str(self.tmp),
            harness_root=str(harness),
            workspace_id="demo",
            task_id="t1",
            add=["t2"],
            _lock=RewriteBeforeCallbackLock(self.new_path),
        )
        self.assertFalse(result.changed)
        expected = compute_task_item_fingerprint(
            item=_item("t1", ["t2"]), task_id="t1"
        )
        self.assertEqual(result.before_fingerprint, expected)
        self.assertEqual(result.after_fingerprint, expected)

    def test_relative_paths_fail_closed_zero_mutation(self) -> None:
        _write_checklist(self.new_path, [_item("t1"), _item("t2")])
        before = self.new_path.read_bytes()
        with self.assertRaises(DependencyUpdateError) as ctx:
            apply_dependency_mutation(
                workspace_path=str(self.tmp),
                harness_root="docs",  # relative: never resolved vs cwd
                workspace_id="demo",
                task_id="t1",
                add=["t2"],
            )
        self.assertEqual(ctx.exception.reason, REASON_RELATIVE_PATH)
        with self.assertRaises(DependencyUpdateError) as ctx:
            apply_dependency_mutation(
                workspace_path="repo",  # relative workspace path
                harness_root=str(self.tmp),
                workspace_id="demo",
                task_id="t1",
                add=["t2"],
            )
        self.assertEqual(ctx.exception.reason, REASON_RELATIVE_PATH)
        self.assertEqual(self.new_path.read_bytes(), before)

    def test_file_only_never_touches_db(self) -> None:
        # A file-only run must not require or open any Coordinate DB: prove it
        # with spies, not by inspecting the temp dir for sqlite artifacts.
        harness = self._harness("new", [_item("t1"), _item("t2")])
        with patch(
            "coordinate.task_dependencies.get_workspace",
        ) as get_ws, patch("coordinate.db.initialize") as db_init:
            result = apply_dependency_mutation(
                workspace_path=str(self.tmp),
                harness_root=str(harness),
                workspace_id="demo",
                task_id="t1",
                add=["t2"],
            )
            get_ws.assert_not_called()
            db_init.assert_not_called()
        self.assertTrue(result.changed)


class CombinedCommandTests(unittest.TestCase):
    """Same-host combined semantics: preflight, file-first, targeted mirror."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))
        self.conn = initialize(":memory:")
        self.addCleanup(self.conn.close)
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1"), _item("t2"), _item("t3")])
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.tmp),
            harness_root=str(self.tmp),
        )
        # Seed a non-target mirror so "only the target updates" is observable.
        upsert_task_mirror(
            self.conn,
            workspace_id="demo",
            task_id="other",
            phase="todo",
            owner=None,
            branch=None,
            pr=None,
            payload={"task_id": "other", "dependencies": []},
        )

    def _adapter(self, **kwargs):
        return FakeHarnessAdapter(
            self.tmp, {"project": "demo", "generated_at": "2026-05-17T00:00:00Z"}, **kwargs
        )

    def _combined(self, task_id="t1", add=(), remove=(), **kwargs):
        return update_task_dependencies(
            self.conn,
            workspace_id="demo",
            task_id=task_id,
            add=add,
            remove=remove,
            **kwargs,
        )

    def _checklist_deps(self, task_id: str) -> list:
        path = self.tmp / CHECKLIST_NEW_NAME
        data = json.loads(path.read_text(encoding="utf-8"))
        return next(item for item in data["items"] if item["id"] == task_id)["dependencies"]

    def _mirror_payload(self, task_id: str) -> dict:
        rows = list_task_mirrors(self.conn, "demo")
        for row in rows:
            mirror = row_to_dict(row)
            if mirror["task_id"] == task_id:
                return mirror["payload"]
        raise AssertionError(f"mirror {task_id} missing")

    def test_combined_success_updates_checklist_and_target_mirror_only(self) -> None:
        result = self._combined(
            task_id="t1", add=["t2", "t3"], _adapter=self._adapter()
        )
        self.assertEqual(self._checklist_deps("t1"), ["t2", "t3"])
        self.assertEqual(result["dependencies"], ["t2", "t3"])
        self.assertEqual(
            [r["outcome"] for r in result["requests"]], ["applied", "applied"]
        )
        # Flat result: file-half facts appear exactly once (no nested "files"
        # copy), reconciliation is additive.
        self.assertNotIn("files", result)
        self.assertIn("reconciliation", result)
        self.assertEqual(
            result["reconciliation"]["scope"],
            {"kind": "task", "task_id": "t1"},
        )
        # Target mirror follows the authority.
        self.assertEqual(self._mirror_payload("t1")["dependencies"], ["t2", "t3"])
        # Non-target mirror untouched.
        self.assertEqual(self._mirror_payload("other")["dependencies"], [])
        # reconciliation.completed + task_mirror.updated events exist, and no
        # new event type was invented.
        event_types = {
            row_to_dict(e)["event_type"]
            for e in list_events(self.conn, "demo")
        }
        self.assertTrue(
            {"task_mirror.created", "task_mirror.updated"} & event_types,
            "mirror mutation event missing",
        )
        self.assertIn("reconciliation.completed", event_types)
        self.assertFalse(
            any("dependency" in event_type for event_type in event_types)
        )

    def test_combined_concurrent_update_mirror_follows_latest_authority(self) -> None:
        # A legal concurrent update landing after this mutation's locked
        # snapshot but before the targeted reconcile: the result still
        # describes the snapshot it modified, while the reconcile re-reads
        # the then-current canonical checklist and the mirror follows the
        # latest authority without ever overwriting it with the stale
        # snapshot.
        tmp = self.tmp

        class ConcurrentWriterAdapter(FakeHarnessAdapter):
            def refresh_state(self):
                _write_checklist(
                    tmp / CHECKLIST_NEW_NAME,
                    [
                        _item("t1", ["t2", "t4"]),
                        _item("t2"),
                        _item("t3"),
                        _item("t4"),
                    ],
                )
                return super().refresh_state()

        result = self._combined(
            task_id="t1",
            add=["t2"],
            _adapter=ConcurrentWriterAdapter(
                self.tmp,
                {"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            ),
        )
        # This result honestly describes the locked snapshot it modified.
        self.assertEqual(result["dependencies"], ["t2"])
        self.assertEqual(
            [r["outcome"] for r in result["requests"]], ["applied"]
        )
        # The targeted reconcile read the latest canonical checklist; the
        # mirror follows it and no stale overwrite happened.
        self.assertEqual(self._checklist_deps("t1"), ["t2", "t4"])
        self.assertEqual(self._mirror_payload("t1")["dependencies"], ["t2", "t4"])
        self.assertEqual(
            self._mirror_payload("t1")["dependencies"],
            self._checklist_deps("t1"),
        )

    def test_combined_missing_harnessctl_fails_before_any_mutation(self) -> None:
        path = self.tmp / CHECKLIST_NEW_NAME
        before = path.read_bytes()
        with self.assertRaises(DependencyUpdateError) as ctx:
            self._combined(
                task_id="t1", add=["t2"], _adapter=self._adapter(harnessctl_ok=False)
            )
        self.assertEqual(ctx.exception.reason, REASON_HARNESSCTL_UNAVAILABLE)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(
            [row_to_dict(r)["task_id"] for r in list_task_mirrors(self.conn, "demo")],
            ["other"],
        )
        self.assertEqual(list_events(self.conn, "demo"), [])

    def test_combined_unknown_workspace_fails_closed(self) -> None:
        with self.assertRaises(DependencyUpdateError) as ctx:
            update_task_dependencies(
                self.conn,
                workspace_id="ghost",
                task_id="t1",
                add=["t2"],
            )
        self.assertEqual(ctx.exception.reason, REASON_INVALID_INPUT)

    def test_combined_runtime_copy_guard(self) -> None:
        upsert_workspace(
            self.conn,
            workspace_id="opt-ws",
            name="Opt",
            path="/opt/coordinate",
            harness_root="/opt/coordinate/docs",
        )
        with self.assertRaises(DependencyUpdateError) as ctx:
            update_task_dependencies(
                self.conn, workspace_id="opt-ws", task_id="t1", add=["t2"]
            )
        self.assertEqual(ctx.exception.reason, REASON_RUNTIME_COPY)
        self.assertNotIn("task create", str(ctx.exception))

    def test_combined_relative_paths_fail_closed_zero_mutation(self) -> None:
        # Registered paths are normalized to absolute by upsert_workspace, but
        # the service must fail closed on relative paths in stored data (e.g.
        # legacy/manual DB rows) before any mutation.
        from coordinate.db import Workspace as DBWorkspace

        with patch(
            "coordinate.task_dependencies.get_workspace",
            return_value=DBWorkspace(
                id="rel-ws", name="Rel", path="repo", harness_root="docs"
            ),
        ):
            with self.assertRaises(DependencyUpdateError) as ctx:
                update_task_dependencies(
                    self.conn, workspace_id="rel-ws", task_id="t1", add=["t2"]
                )
        self.assertEqual(ctx.exception.reason, REASON_RELATIVE_PATH)
        # Zero mutation: checklist untouched, no events.
        self.assertEqual(self._checklist_deps("t1"), [])
        self.assertEqual(list_events(self.conn, "demo"), [])

    def test_combined_refresh_failure_returns_recovery_and_rerun_converges(self) -> None:
        path = self.tmp / CHECKLIST_NEW_NAME
        failing = self._adapter(
            refresh_error=HarnessError("harnessctl state failed")
        )
        with self.assertRaises(TaskDependenciesRecoveryError) as ctx:
            self._combined(task_id="t1", add=["t2"], _adapter=failing)
        recovery = ctx.exception.recovery
        self.assertTrue(recovery.to_dict()["checklist_committed"])
        # Checklist is committed and authoritative.
        self.assertEqual(self._checklist_deps("t1"), ["t2"])
        # Both recovery commands are present and copyable.
        self.assertEqual(
            recovery.retry_argv()[:4],
            ["coordinate", "task", "update-dependencies", "demo"],
        )
        self.assertEqual(
            recovery.reconcile_argv(),
            ["coordinate", "reconcile", "demo", "--task-id", "t1"],
        )
        self.assertIn("--add", recovery.retry_argv())
        self.assertIn("t2", recovery.retry_argv())

        # Re-running the same command with a healthy adapter converges:
        # checklist already satisfied, mirror follows, no new mutation.
        path_before_rerun = path.read_bytes()
        result = self._combined(task_id="t1", add=["t2"], _adapter=self._adapter())
        self.assertEqual(
            [r["outcome"] for r in result["requests"]], ["already_satisfied"]
        )
        self.assertEqual(path.read_bytes(), path_before_rerun)
        self.assertEqual(self._mirror_payload("t1")["dependencies"], ["t2"])

    def test_combined_reconcile_failure_returns_recovery(self) -> None:
        with patch(
            "coordinate.task_dependencies.reconcile_workspace",
            side_effect=ReconcileConflictError("coordinator conflict"),
        ):
            with self.assertRaises(TaskDependenciesRecoveryError) as ctx:
                self._combined(task_id="t1", add=["t2"], _adapter=self._adapter())
        self.assertTrue(ctx.exception.recovery.to_dict()["checklist_committed"])
        self.assertEqual(self._checklist_deps("t1"), ["t2"])
        self.assertIn("reconcile", ctx.exception.recovery.to_dict()["reconcile_command"])

    def test_combined_rerun_after_recovery_via_targeted_reconcile(self) -> None:
        # Simulate the operator choosing the targeted-reconcile recovery path
        # after a DB-half failure: file committed, then reconcile --task-id.
        path = self.tmp / CHECKLIST_NEW_NAME
        failing = self._adapter(refresh_error=HarnessError("boom"))
        with self.assertRaises(TaskDependenciesRecoveryError):
            self._combined(task_id="t1", add=["t2"], _adapter=failing)
        self.assertEqual(self._checklist_deps("t1"), ["t2"])

        from coordinate.reconcile import reconcile_workspace

        result = reconcile_workspace(
            self.conn,
            get_workspace(self.conn, "demo"),
            refresh=False,
            adapter=self._adapter(),
            task_id="t1",
        )
        self.assertEqual(result.created + result.updated, 1)
        self.assertEqual(self._mirror_payload("t1")["dependencies"], ["t2"])
        self.assertEqual(self._mirror_payload("other")["dependencies"], [])


class FileOnlyPreflightTests(unittest.TestCase):
    """file-only must NOT be saddled with the harnessctl preflight."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1"), _item("t2")])

    def test_file_only_works_without_any_harness_runtime(self) -> None:
        # No harnessctl anywhere: the file-only entry must still succeed.
        result = apply_dependency_mutation(
            workspace_path=str(self.tmp),
            harness_root=str(self.tmp),
            workspace_id="demo",
            task_id="t1",
            add=["t2"],
        )
        self.assertTrue(result.changed)
        data = json.loads((self.tmp / CHECKLIST_NEW_NAME).read_text(encoding="utf-8"))
        self.assertEqual(
            next(i for i in data["items"] if i["id"] == "t1")["dependencies"], ["t2"]
        )


class CliContractTests(unittest.TestCase):
    """CLI help / argument / JSON output contract for the two new commands."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))

    def test_parser_exposes_both_leaves_with_expected_flags(self) -> None:
        parser = build_parser()
        combined = parser.parse_args(
            ["task", "update-dependencies", "demo", "--task-id", "t1", "--add", "t2"]
        )
        self.assertEqual(combined.workspace_id, "demo")
        self.assertEqual(combined.task_id, "t1")
        self.assertEqual(combined.add, ["t2"])
        self.assertEqual(combined.remove, [])
        self.assertFalse(hasattr(combined, "actor"), "--actor must not exist on combined")
        self.assertFalse(combined.allow_runtime_copy)
        self.assertTrue(callable(combined.handler))

        files = parser.parse_args(
            [
                "task", "update-dependencies-files",
                "--workspace-path", "/ws",
                "--harness-root", "/ws/docs",
                "--workspace-id", "demo",
                "--task-id", "t1",
                "--add", "t2",
                "--remove", "t3",
            ]
        )
        self.assertEqual(files.workspace_path, "/ws")
        self.assertEqual(files.harness_root, "/ws/docs")
        self.assertEqual(files.add, ["t2"])
        self.assertEqual(files.remove, ["t3"])
        self.assertTrue(callable(files.handler))

    def test_combined_handler_json_output_and_exit_codes(self) -> None:
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1"), _item("t2")])
        db_path = self.tmp / "coord.sqlite3"
        conn = initialize(str(db_path))
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.tmp),
            harness_root=str(self.tmp),
        )
        conn.close()
        args = SimpleNamespace(
            db=str(db_path),
            workspace_id="demo",
            task_id="t1",
            add=["t2"],
            remove=[],
            allow_runtime_copy=False,
        )
        with patch(
            "coordinate.task_dependencies.HarnessAdapter",
        ) as adapter_cls:
            adapter_cls.return_value = FakeHarnessAdapter(
                self.tmp,
                {"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            )
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = handle_task_update_dependencies(args)
        self.assertEqual(code, 0)
        payload = json.loads(buffer.getvalue())
        self.assertEqual(payload["result"]["task_id"], "t1")
        self.assertEqual(payload["result"]["dependencies"], ["t2"])
        self.assertEqual(
            payload["result"]["requests"],
            [{"dependency": "t2", "action": "add", "outcome": "applied"}],
        )

    def test_combined_handler_json_error_on_rejection(self) -> None:
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1")])
        db_path = self.tmp / "coord.sqlite3"
        conn = initialize(str(db_path))
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.tmp),
            harness_root=str(self.tmp),
        )
        conn.close()
        args = SimpleNamespace(
            db=str(db_path),
            workspace_id="demo",
            task_id="t1",
            add=["ghost"],
            remove=[],
            allow_runtime_copy=False,
        )
        with patch(
            "coordinate.task_dependencies.HarnessAdapter",
        ) as adapter_cls:
            adapter_cls.return_value = FakeHarnessAdapter(
                self.tmp,
                {"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            )
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = handle_task_update_dependencies(args)
        self.assertEqual(code, 1)
        payload = json.loads(buffer.getvalue())
        self.assertEqual(payload["error"]["reason"], REASON_DEPENDENCY_NOT_FOUND)

    def test_combined_handler_recovery_error_shape(self) -> None:
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1"), _item("t2")])
        db_path = self.tmp / "coord.sqlite3"
        conn = initialize(str(db_path))
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.tmp),
            harness_root=str(self.tmp),
        )
        conn.close()
        args = SimpleNamespace(
            db=str(db_path),
            workspace_id="demo",
            task_id="t1",
            add=["t2"],
            remove=[],
            allow_runtime_copy=False,
        )
        with patch(
            "coordinate.task_dependencies.HarnessAdapter",
        ) as adapter_cls, patch(
            "coordinate.task_dependencies.reconcile_workspace",
            side_effect=ReconcileConflictError("boom"),
        ):
            adapter_cls.return_value = FakeHarnessAdapter(
                self.tmp,
                {"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            )
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = handle_task_update_dependencies(args)
        self.assertEqual(code, 1)
        payload = json.loads(buffer.getvalue())
        self.assertTrue(payload["error"]["checklist_committed"])
        self.assertIn("update-dependencies", payload["error"]["retry_command"])
        self.assertIn("reconcile", payload["error"]["reconcile_command"])

    def test_files_handler_requires_no_db_and_outputs_result(self) -> None:
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1"), _item("t2")])
        args = SimpleNamespace(
            workspace_path=str(self.tmp),
            harness_root=str(self.tmp),
            workspace_id="demo",
            task_id="t1",
            add=["t2"],
            remove=[],
            allow_runtime_copy=False,
        )
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = handle_task_update_dependencies_files(args)
        self.assertEqual(code, 0)
        payload = json.loads(buffer.getvalue())
        self.assertEqual(payload["result"]["task_id"], "t1")
        self.assertEqual(payload["result"]["dependencies"], ["t2"])
        self.assertTrue(payload["result"]["changed"])

    def test_files_handler_refuses_runtime_copy(self) -> None:
        args = SimpleNamespace(
            workspace_path="/opt/coordinate",
            harness_root="/opt/coordinate/docs",
            workspace_id="demo",
            task_id="t1",
            add=["t2"],
            remove=[],
            allow_runtime_copy=False,
        )
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = handle_task_update_dependencies_files(args)
        self.assertEqual(code, 1)
        payload = json.loads(buffer.getvalue())
        self.assertEqual(payload["error"]["reason"], REASON_RUNTIME_COPY)

    def test_files_handler_allow_runtime_copy_repair_override(self) -> None:
        # Explicit repair override must keep working (scope unchanged), but the
        # underlying mutation still validates the checklist authority.
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1"), _item("t2")])
        args = SimpleNamespace(
            workspace_path=str(self.tmp),
            harness_root=str(self.tmp),
            workspace_id="demo",
            task_id="t1",
            add=["t2"],
            remove=[],
            allow_runtime_copy=True,
        )
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = handle_task_update_dependencies_files(args)
        self.assertEqual(code, 0)

    def test_main_combined_end_to_end_json(self) -> None:
        _write_checklist(self.tmp / CHECKLIST_NEW_NAME, [_item("t1"), _item("t2")])
        db_path = self.tmp / "coord.sqlite3"
        conn = initialize(str(db_path))
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(self.tmp),
            harness_root=str(self.tmp),
        )
        conn.close()
        with patch(
            "coordinate.task_dependencies.HarnessAdapter",
        ) as adapter_cls:
            fake = FakeHarnessAdapter(
                self.tmp, {"project": "demo", "generated_at": "2026-05-17T00:00:00Z"}
            )
            adapter_cls.return_value = fake
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = cli_main(
                    [
                        "--db", str(db_path),
                        "task", "update-dependencies", "demo",
                        "--task-id", "t1", "--add", "t2",
                    ]
                )
        self.assertEqual(code, 0)
        payload = json.loads(buffer.getvalue())
        self.assertEqual(payload["result"]["dependencies"], ["t2"])
        self.assertEqual(payload["result"]["requests"][0]["outcome"], "applied")


if __name__ == "__main__":
    unittest.main()
