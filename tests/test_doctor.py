import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from coordinate.db import (
    Workspace,
    initialize,
    list_events,
    list_split_operations,
    list_task_mirrors,
    upsert_workspace,
)
from coordinate.doctor import diagnose_workspace
from coordinate.onboarding import (
    REASON_RUNTIME_ROOT_INCOMPATIBLE,
    REASON_RUNTIME_SOURCE_INCOMPLETE,
    RuntimeSourceError,
    init_full_harness,
)
from tests.fixtures.runtime_template import (
    coordinate_runtime_dir,
    make_template_source,
)


class _FakeRunner:
    """Fake subprocess runner for doctor tests."""

    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls = []

    def __call__(self, cmd, **kwargs):
        self.calls.append(cmd)
        return subprocess.CompletedProcess(cmd, self.returncode, self.stdout, self.stderr)


# ── Doctor Tests ──


class DoctorMissingWorkspacePathTest(unittest.TestCase):
    def test_reports_none_when_path_missing(self):
        ws = Workspace(
            id="ghost",
            name="Ghost",
            path="/nonexistent/path/that/does/not/exist",
            harness_root="/nonexistent/path/that/does/not/exist/harness",
        )
        report = diagnose_workspace(ws)
        self.assertFalse(report.workspace_path_exists)
        self.assertEqual(report.harness_mode, "none")
        self.assertIn("does not exist", report.summary)


class DoctorMissingHarnessRootTest(unittest.TestCase):
    def test_reports_none_when_harness_root_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            ws = Workspace(
                id="demo",
                name="Demo",
                path=tmp,
                harness_root=os.path.join(tmp, "missing-harness"),
            )
            report = diagnose_workspace(ws)
            self.assertTrue(report.workspace_path_exists)
            self.assertFalse(report.harness_root_exists)
            self.assertEqual(report.harness_mode, "none")


class DoctorMissingHarnessctlTest(unittest.TestCase):
    def test_reports_minimal_when_no_harnessctl(self):
        with tempfile.TemporaryDirectory() as tmp:
            hr = Path(tmp) / "harness"
            hr.mkdir()
            for fname in ["harness-config.json", "mvp-checklist.json",
                          "events.jsonl", "harness-state.json", "progress.md"]:
                (hr / fname).write_text("{}", encoding="utf-8")
            ws = Workspace(
                id="demo",
                name="Demo",
                path=tmp,
                harness_root=str(hr),
            )
            report = diagnose_workspace(ws)
            self.assertEqual(report.harness_mode, "minimal_file_backed")
            self.assertFalse(report.harnessctl_exists)


class DoctorHarnessctlNotExecutableTest(unittest.TestCase):
    def test_reports_minimal_with_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hr = root / "harness"
            hr.mkdir()
            # Create harnessctl but not executable
            harnessctl = root / "scripts" / "harness" / "harnessctl"
            harnessctl.parent.mkdir(parents=True)
            harnessctl.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            for fname in ["harness-config.json", "mvp-checklist.json",
                          "events.jsonl", "harness-state.json", "progress.md"]:
                (hr / fname).write_text("{}", encoding="utf-8")
            ws = Workspace(
                id="demo",
                name="Demo",
                path=tmp,
                harness_root=str(hr),
                harnessctl_path=str(harnessctl),
            )
            report = diagnose_workspace(ws)
            self.assertTrue(report.harnessctl_exists)
            self.assertFalse(report.harnessctl_executable)
            self.assertEqual(report.harness_mode, "minimal_file_backed")
            self.assertTrue(any("not executable" in w for w in report.warnings))


class DoctorHealthyFullHarnessTest(unittest.TestCase):
    def test_reports_full_harness_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hr = root / "harness"
            hr.mkdir()
            harnessctl = root / "scripts" / "harness" / "harnessctl"
            harnessctl.parent.mkdir(parents=True)
            harnessctl.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            harnessctl.chmod(0o755)
            for fname in ["harness-config.json", "mvp-checklist.json",
                          "events.jsonl", "harness-state.json", "progress.md"]:
                (hr / fname).write_text("{}", encoding="utf-8")
            # Write valid checklist
            (hr / "mvp-checklist.json").write_text(
                json.dumps({"items": []}), encoding="utf-8"
            )
            ws = Workspace(
                id="demo",
                name="Demo",
                path=tmp,
                harness_root=str(hr),
                harnessctl_path=str(harnessctl),
            )
            runner = _FakeRunner(returncode=0)
            report = diagnose_workspace(ws, runner=runner)
            self.assertEqual(report.harness_mode, "full_harness_runtime")
            self.assertTrue(report.harnessctl_exists)
            self.assertTrue(report.harnessctl_executable)
            self.assertTrue(report.checklist_valid)
            self.assertTrue(report.harnessctl_version_ok)
            self.assertTrue(report.harnessctl_doctor_ok)
            self.assertEqual(len(report.warnings), 0)


class DoctorHarnessctlValidationFailureTest(unittest.TestCase):
    def test_cli_returns_nonzero_when_harnessctl_validate_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            root = Path(tmp)
            hr = root / "harness"
            hr.mkdir()
            harnessctl = root / "scripts" / "harness" / "harnessctl"
            harnessctl.parent.mkdir(parents=True)
            harnessctl.write_text(
                "#!/bin/bash\n"
                "if [ \"$1\" = validate ]; then echo bad >&2; exit 2; fi\n"
                "exit 0\n",
                encoding="utf-8",
            )
            harnessctl.chmod(0o755)
            for fname in ["harness-config.json", "events.jsonl",
                          "harness-state.json", "progress.md"]:
                (hr / fname).write_text("{}", encoding="utf-8")
            (hr / "mvp-checklist.json").write_text(
                json.dumps({"items": []}), encoding="utf-8"
            )

            rc, _, _ = CLIDoctorTest()._run_cli([
                "--db", db, "workspace", "add", "ws1",
                "--path", tmp,
                "--harness-root", str(hr),
                "--harnessctl-path", str(harnessctl),
            ])
            self.assertEqual(rc, 0)
            rc, out, _ = CLIDoctorTest()._run_cli([
                "--db", db, "workspace", "doctor", "ws1",
            ])
            self.assertEqual(rc, 1)
            self.assertIn('"harness_mode": "full_harness_runtime"', out)
            self.assertIn('"harnessctl_version_ok": false', out)
            self.assertIn("harnessctl validate failed", out)


class DoctorChecklistInvalidTest(unittest.TestCase):
    def test_reports_invalid_checklist(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hr = root / "harness"
            hr.mkdir()
            harnessctl = root / "scripts" / "harness" / "harnessctl"
            harnessctl.parent.mkdir(parents=True)
            harnessctl.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            harnessctl.chmod(0o755)
            for fname in ["harness-config.json", "events.jsonl",
                          "harness-state.json", "progress.md"]:
                (hr / fname).write_text("{}", encoding="utf-8")
            (hr / "mvp-checklist.json").write_text("not json{{{{", encoding="utf-8")
            ws = Workspace(
                id="demo",
                name="Demo",
                path=tmp,
                harness_root=str(hr),
            )
            runner = _FakeRunner(returncode=0)
            report = diagnose_workspace(ws, runner=runner)
            self.assertFalse(report.checklist_valid)
            self.assertTrue(any("invalid" in w for w in report.warnings))


class DoctorBusNoteTest(unittest.TestCase):
    def test_bus_configured(self):
        with tempfile.TemporaryDirectory() as tmp:
            hr = Path(tmp) / "harness"
            hr.mkdir()
            ws = Workspace(
                id="demo",
                name="Demo",
                path=tmp,
                harness_root=str(hr),
                default_bus="discord",
                default_destination="channel-123",
            )
            report = diagnose_workspace(ws)
            self.assertIn("discord", report.bus_note)

    def test_no_bus_configured(self):
        with tempfile.TemporaryDirectory() as tmp:
            hr = Path(tmp) / "harness"
            hr.mkdir()
            ws = Workspace(
                id="demo",
                name="Demo",
                path=tmp,
                harness_root=str(hr),
            )
            report = diagnose_workspace(ws)
            self.assertIn("no default_bus", report.bus_note)


class DoctorToDictTest(unittest.TestCase):
    def test_to_dict_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            hr = Path(tmp) / "harness"
            hr.mkdir()
            ws = Workspace(id="d", name="D", path=tmp, harness_root=str(hr))
            d = diagnose_workspace(ws).to_dict()
            expected_keys = {
                "workspace_id", "workspace_path", "workspace_path_exists",
                "harness_root", "harness_root_exists",
                "harnessctl_path", "harnessctl_exists", "harnessctl_executable",
                "files", "harness_mode", "harnessctl_version_ok",
                "checklist_valid", "harnessctl_doctor_ok",
                "default_bus", "default_destination", "bus_note",
                "warnings", "summary",
                "projection_report", "projection_ok",
            }
            self.assertEqual(set(d.keys()), expected_keys)


# ── Full Init Tests ──


class _InitTestBase(unittest.TestCase):
    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        return conn

    def _make_workspace(self, conn, tmp, harness_root=None):
        hr = harness_root or os.path.join(tmp, "docs", "project-harness")
        upsert_workspace(
            conn,
            workspace_id="test-ws",
            name="Test",
            path=tmp,
            harness_root=hr,
        )

    def _make_source(self, tmp):
        """Build a U1-style template source from Coordinate's vendored runtime."""
        return str(make_template_source(Path(tmp)))

    def _snapshot(self, tmp, conn):
        """Full file-tree digests plus DB task/event/operation counts."""
        ws = Path(tmp)
        files = {}
        for p in sorted(ws.rglob("*")):
            rel = str(p.relative_to(ws))
            if p.is_dir():
                files[rel + "/"] = None
            else:
                files[rel] = hashlib.sha256(p.read_bytes()).hexdigest()
        return {
            "files": files,
            "events": len(list(list_events(conn, "test-ws"))),
            "mirrors": len(list(list_task_mirrors(conn, "test-ws"))),
            "ops": len(list_split_operations(conn, workspace_id="test-ws")),
        }


class FullInitDryRunTest(_InitTestBase):
    def test_dry_run_does_not_write_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)
            source = self._make_source(tmp)
            result = init_full_harness(
                conn,
                workspace_id="test-ws",
                source=source,
                dry_run=True,
            )
            # Scripts should NOT exist on disk
            harnessctl = Path(tmp) / "scripts" / "harness" / "harnessctl"
            self.assertFalse(harnessctl.exists())
            # But result should list them as "copied"
            self.assertTrue(len(result.scripts_copied) > 0)
            # No harnessctl_path update in dry-run DB
            from coordinate.db import get_workspace
            ws = get_workspace(conn, "test-ws")
            self.assertIsNone(ws.harnessctl_path)


class FullInitCreatesFilesTest(_InitTestBase):
    def test_creates_missing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)
            source = self._make_source(tmp)
            init_full_harness(
                conn,
                workspace_id="test-ws",
                source=source,
            )
            # Check scripts were copied
            harnessctl = Path(tmp) / "scripts" / "harness" / "harnessctl"
            self.assertTrue(harnessctl.exists())
            self.assertTrue(os.access(harnessctl, os.X_OK))

            # Check protocol files created
            hr = Path(tmp) / "docs" / "project-harness"
            for fname in ["scope.md", "architecture.md", "domain-model.md", "runbook.md"]:
                self.assertTrue((hr / fname).exists(), f"{fname} should exist")

            # Check minimal files created
            for fname in ["harness-config.json", "harness-checklist.json",
                          "events.jsonl", "progress.md", "harness-state.json"]:
                self.assertTrue((hr / fname).exists(), f"{fname} should exist")


class FullInitNoOverwriteTest(_InitTestBase):
    def test_provably_compatible_existing_runtime_is_kept(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)
            source = self._make_source(tmp)

            # Pre-create protocol files with known content
            hr = Path(tmp) / "docs" / "project-harness"
            hr.mkdir(parents=True)
            (hr / "scope.md").write_text("ORIGINAL SCOPE", encoding="utf-8")
            (hr / "harness-config.json").write_text('{"existing": true}', encoding="utf-8")

            result = init_full_harness(
                conn,
                workspace_id="test-ws",
                source=source,
            )

            # Existing files should be preserved
            self.assertEqual((hr / "scope.md").read_text(), "ORIGINAL SCOPE")
            self.assertEqual((hr / "harness-config.json").read_text(), '{"existing": true}')

            # They should appear in existing lists, not created (compare resolved paths)
            scope_resolved = str((hr / "scope.md").resolve())
            existing_resolved = [str(Path(p).resolve()) for p in result.files_existing]
            self.assertIn(scope_resolved, existing_resolved)

            # A second full init over the compatible rendered runtime keeps it
            # (existing, not re-copied, not overwritten).
            second = init_full_harness(conn, workspace_id="test-ws", source=source)
            self.assertEqual(second.scripts_copied, [])
            self.assertIn("scripts/harness/harnessctl", second.scripts_existing)

    def test_incompatible_existing_runtime_fails_closed_zero_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)  # harness_root=docs/project-harness
            source = self._make_source(tmp)

            # Pre-existing runtime rendered for the repo's own root (docs):
            # the P1-3 root-mismatch destination case.
            scripts_dir = Path(tmp) / "scripts" / "harness"
            scripts_dir.mkdir(parents=True)
            runtime = coordinate_runtime_dir()
            shutil.copy2(runtime / "harnessctl", scripts_dir / "harnessctl")
            shutil.copy2(runtime / "harness_common.py", scripts_dir / "harness_common.py")
            hr = Path(tmp) / "docs" / "project-harness"

            before = self._snapshot(tmp, conn)
            with self.assertRaises(RuntimeSourceError) as ctx:
                init_full_harness(conn, workspace_id="test-ws", source=source)
            self.assertEqual(ctx.exception.reason, REASON_RUNTIME_ROOT_INCOMPATIBLE)
            self.assertEqual(self._snapshot(tmp, conn), before)
            self.assertFalse((hr / "scope.md").exists())
            self.assertFalse((hr / "harness-config.json").exists())
            self.assertFalse((hr / "harness-checklist.json").exists())
            self.assertEqual(
                (scripts_dir / "harnessctl").read_bytes(),
                (runtime / "harnessctl").read_bytes(),
            )


class FullInitUpdatesHarnessctlPathTest(_InitTestBase):
    def test_updates_harnessctl_path_in_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)
            source = self._make_source(tmp)
            result = init_full_harness(
                conn,
                workspace_id="test-ws",
                source=source,
            )
            self.assertTrue(result.harnessctl_path_updated)
            from coordinate.db import get_workspace
            ws = get_workspace(conn, "test-ws")
            expected = str((Path(tmp) / "scripts" / "harness" / "harnessctl").resolve())
            self.assertEqual(ws.harnessctl_path, expected)


class FullInitSourceMissingTest(_InitTestBase):
    def test_raises_on_missing_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)
            with self.assertRaises(ValueError):
                init_full_harness(
                    conn,
                    workspace_id="test-ws",
                    source="/nonexistent/source",
                )


class FullInitHarnessRootOutsideWorkspaceTest(_InitTestBase):
    def test_raises_on_harness_root_outside_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            # harness_root outside workspace path
            upsert_workspace(
                conn,
                workspace_id="test-ws",
                name="Test",
                path=tmp,
                harness_root="/tmp/outside/workspace",
            )
            source = self._make_source(tmp)
            with self.assertRaises(ValueError):
                init_full_harness(
                    conn,
                    workspace_id="test-ws",
                    source=source,
                )


class FullInitUnknownWorkspaceTest(_InitTestBase):
    def test_raises_on_unknown_workspace(self):
        conn = self._make_conn()
        with self.assertRaises(ValueError):
            init_full_harness(
                conn,
                workspace_id="nonexistent",
                source="/some/path",
            )


class FullInitEmptySourceTest(_InitTestBase):
    def test_empty_source_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)
            empty_source = Path(tmp) / "empty-source"
            empty_source.mkdir()
            before = self._snapshot(tmp, conn)
            with self.assertRaises(RuntimeSourceError) as ctx:
                init_full_harness(
                    conn,
                    workspace_id="test-ws",
                    source=str(empty_source),
                )
            self.assertEqual(ctx.exception.reason, REASON_RUNTIME_SOURCE_INCOMPLETE)
            self.assertEqual(self._snapshot(tmp, conn), before)


class FullInitToDictTest(_InitTestBase):
    def test_to_dict_structure(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = self._make_conn()
            self._make_workspace(conn, tmp)
            source = self._make_source(tmp)
            result = init_full_harness(
                conn,
                workspace_id="test-ws",
                source=source,
            )
            d = result.to_dict()
            expected_keys = {
                "workspace", "harness_root",
                "scripts_copied", "scripts_existing",
                "files_created", "files_existing",
                "warnings", "harnessctl_path_updated",
            }
            self.assertEqual(set(d.keys()), expected_keys)


# ── CLI Integration Tests ──


class CLIDoctorTest(unittest.TestCase):
    def _run_cli(self, args):
        from io import StringIO
        from coordinate.cli import main
        import sys

        buf = StringIO()
        err_buf = StringIO()
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = buf, err_buf
        try:
            rc = main(args)
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr
        return rc, buf.getvalue(), err_buf.getvalue()

    def test_doctor_unknown_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            rc, out, err = self._run_cli(["--db", db, "workspace", "doctor", "nope"])
            self.assertEqual(rc, 1)
            self.assertIn("unknown workspace", err)

    def test_doctor_reports_minimal(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            hr = os.path.join(tmp, "harness")
            os.makedirs(hr)
            rc, _, _ = self._run_cli([
                "--db", db, "workspace", "add", "ws1",
                "--path", tmp, "--harness-root", hr,
            ])
            self.assertEqual(rc, 0)
            rc, out, _ = self._run_cli(["--db", db, "workspace", "doctor", "ws1"])
            self.assertEqual(rc, 1)
            self.assertIn("minimal_file_backed", out)

    def test_init_harness_full_requires_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            rc, _, err = self._run_cli([
                "--db", db, "workspace", "init-harness", "ws1",
                "--mode", "full",
            ])
            self.assertEqual(rc, 1)
            self.assertIn("--source is required", err)

    def test_init_harness_minimal_requires_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            rc, _, err = self._run_cli([
                "--db", db, "workspace", "init-harness", "ws1",
                "--mode", "minimal",
            ])
            self.assertEqual(rc, 1)
            self.assertIn("--root is required", err)

    def test_doctor_no_projections_flag_is_visible_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            hr = os.path.join(tmp, "harness")
            os.makedirs(hr)
            rc, _, _ = self._run_cli([
                "--db", db, "workspace", "add", "ws1",
                "--path", tmp, "--harness-root", hr,
            ])
            self.assertEqual(rc, 0)
            rc, out, _ = self._run_cli([
                "--db", db, "workspace", "doctor", "ws1", "--no-projections",
            ])
            self.assertEqual(rc, 1)
            data = json.loads(out)
            self.assertIsNone(data["projection_report"])
            self.assertIsNone(data["projection_ok"])
            self.assertTrue(
                any("--no-projections" in w for w in data["warnings"]),
                data["warnings"],
            )


if __name__ == "__main__":
    unittest.main()
