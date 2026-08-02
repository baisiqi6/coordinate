import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from coordinate.db import Workspace
from coordinate.harness import HarnessAdapter, HarnessError, HarnessMutationResult


class HarnessAdapterTests(unittest.TestCase):
    def test_refresh_state_runs_harnessctl_and_reads_state_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            harness_root = root / "harness"
            harness_root.mkdir()
            harnessctl = root / "harnessctl"
            harnessctl.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            harnessctl.chmod(0o755)
            state = {"project": "demo", "current_item": None}
            checklist_bytes = json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [],
            }).encode("utf-8")
            (harness_root / "mvp-checklist.json").write_bytes(checklist_bytes)
            state["source"] = {
                "checklist_path": "harness/mvp-checklist.json",
                "checklist_sha256": __import__("hashlib").sha256(checklist_bytes).hexdigest(),
            }
            (harness_root / "harness-state.json").write_text(
                json.dumps(state), encoding="utf-8"
            )
            workspace = Workspace(
                id="demo",
                name="Demo",
                path=str(root),
                harness_root=str(harness_root),
                harnessctl_path=str(harnessctl),
            )
            calls = []

            def runner(*args, **kwargs):
                calls.append((args, kwargs))
                return subprocess.CompletedProcess(args[0], 0, "", "")

            adapter = HarnessAdapter(workspace, runner=runner)

            self.assertEqual(adapter.refresh_state(), state)
            self.assertEqual(calls[0][0][0], [str(harnessctl), "state"])
            self.assertEqual(calls[0][1]["cwd"], str(root))

    def test_read_checklist_rejects_contract_invalid_checklist(self):
        """P1-4 direct contract: HarnessAdapter.read_checklist enforces full
        checklist validation — a parseable checklist missing a required field
        raises HarnessError instead of returning the invalid authority."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "mvp-checklist.json").write_text(
                json.dumps({
                    "project": "demo",
                    "harness_root": ".",
                    "updated_at": "2026-07-13",
                    # "items" deliberately missing
                }),
                encoding="utf-8",
            )
            workspace = Workspace(
                id="demo",
                name="Demo",
                path=str(root),
                harness_root=str(root),
            )
            adapter = HarnessAdapter(workspace)

            with self.assertRaises(HarnessError) as ctx:
                adapter.read_checklist()
            self.assertIn("items", str(ctx.exception))

    def test_refresh_state_reports_harnessctl_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            harness_root = root / "harness"
            harness_root.mkdir()
            harnessctl = root / "harnessctl"
            harnessctl.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
            harnessctl.chmod(0o755)
            workspace = Workspace(
                id="demo",
                name="Demo",
                path=str(root),
                harness_root=str(harness_root),
                harnessctl_path=str(harnessctl),
            )

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(args[0], 2, "", "boom")

            with self.assertRaisesRegex(HarnessError, "boom"):
                HarnessAdapter(workspace, runner=runner).refresh_state()

    def test_refresh_state_runs_non_executable_harnessctl_through_bash(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            harness_root = root / "harness"
            harness_root.mkdir()
            harnessctl = root / "harnessctl"
            harnessctl.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            checklist_bytes = json.dumps({
                "project": "demo",
                "harness_root": ".",
                "updated_at": "2026-01-01",
                "items": [],
            }).encode("utf-8")
            (harness_root / "mvp-checklist.json").write_bytes(checklist_bytes)
            (harness_root / "harness-state.json").write_text(
                json.dumps({
                    "project": "demo",
                    "source": {
                        "checklist_path": "harness/mvp-checklist.json",
                        "checklist_sha256": __import__("hashlib").sha256(checklist_bytes).hexdigest(),
                    },
                }), encoding="utf-8"
            )
            workspace = Workspace(
                id="demo",
                name="Demo",
                path=str(root),
                harness_root=str(harness_root),
                harnessctl_path=str(harnessctl),
            )
            calls = []

            def runner(*args, **kwargs):
                calls.append((args, kwargs))
                return subprocess.CompletedProcess(args[0], 0, "", "")

            HarnessAdapter(workspace, runner=runner).refresh_state()

            self.assertEqual(calls[0][0][0], ["bash", str(harnessctl), "state"])

    # --- run_mutation tests ---

    def _make_workspace(self, tmp, *, executable=True):
        root = Path(tmp)
        harness_root = root / "harness"
        harness_root.mkdir()
        harnessctl = root / "harnessctl"
        harnessctl.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        if executable:
            harnessctl.chmod(0o755)
        return Workspace(
            id="demo",
            name="Demo",
            path=str(root),
            harness_root=str(harness_root),
            harnessctl_path=str(harnessctl),
        )

    def test_run_mutation_success_returns_structured_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._make_workspace(tmp)
            calls = []

            def runner(*args, **kwargs):
                calls.append((args, kwargs))
                return subprocess.CompletedProcess(
                    args[0], 0, "assigned ok", ""
                )

            result = HarnessAdapter(workspace, runner=runner).run_mutation(
                operation="assign",
                task_id="mvp-001",
                actor="codex",
                args=["--owner", "codex"],
            )

            self.assertIsInstance(result, HarnessMutationResult)
            self.assertTrue(result.success)
            self.assertEqual(result.operation, "assign")
            self.assertEqual(result.task_id, "mvp-001")
            self.assertEqual(result.actor, "codex")
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, "assigned ok")
            self.assertEqual(result.stderr, "")
            self.assertEqual(
                result.command,
                [str(Path(tmp) / "harnessctl"), "assign", "mvp-001", "--owner", "codex"],
            )
            self.assertTrue(result.started_at)
            self.assertTrue(result.completed_at)
            self.assertNotEqual(result.started_at, result.completed_at)
            self.assertEqual(calls[0][1]["cwd"], str(Path(tmp)))
            self.assertTrue(calls[0][1]["text"])
            self.assertTrue(calls[0][1]["capture_output"])
            self.assertFalse(calls[0][1]["check"])
            # Check started_at is valid ISO
            from datetime import datetime
            datetime.fromisoformat(result.started_at)
            self.assertEqual(result.to_dict()["operation"], "assign")
            self.assertEqual(result.to_dict()["command"], result.command)

    def test_run_mutation_failure_returns_result_not_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._make_workspace(tmp)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(
                    args[0], 1, "", "item not found"
                )

            result = HarnessAdapter(workspace, runner=runner).run_mutation(
                operation="assign",
                task_id="mvp-999",
                actor="codex",
            )

            self.assertFalse(result.success)
            self.assertEqual(result.exit_code, 1)
            self.assertEqual(result.stderr, "item not found")
            self.assertEqual(result.operation, "assign")
            self.assertEqual(result.task_id, "mvp-999")

    def test_run_mutation_non_executable_uses_bash(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._make_workspace(tmp, executable=False)
            harnessctl_path = str(Path(tmp) / "harnessctl")

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(args[0], 0, "ok", "")

            result = HarnessAdapter(workspace, runner=runner).run_mutation(
                operation="blocker",
                task_id="mvp-003",
                actor="codex",
            )

            self.assertTrue(result.success)
            self.assertEqual(
                result.command,
                ["bash", harnessctl_path, "blocker", "mvp-003"],
            )

    def test_run_mutation_default_idempotency_hint_stable(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._make_workspace(tmp)

            def runner(*args, **kwargs):
                return subprocess.CompletedProcess(args[0], 0, "", "")

            adapter = HarnessAdapter(workspace, runner=runner)
            r1 = adapter.run_mutation(
                operation="assign", task_id="mvp-001", actor="codex"
            )
            r2 = adapter.run_mutation(
                operation="assign", task_id="mvp-001", actor="codex"
            )

            self.assertEqual(r1.idempotency_hint, "demo:assign:mvp-001:codex")
            self.assertEqual(r1.idempotency_hint, r2.idempotency_hint)

    def test_run_mutation_invalid_input_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._make_workspace(tmp)
            adapter = HarnessAdapter(workspace, runner=lambda *a, **kw: None)

            with self.assertRaises(HarnessError):
                adapter.run_mutation(operation="", task_id="mvp-001", actor="codex")

            with self.assertRaises(HarnessError):
                adapter.run_mutation(operation="assign", task_id="", actor="codex")


if __name__ == "__main__":
    unittest.main()
