"""Portable-path contract tests for the coordinate-operator wrappers."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SKILL_ROOT = REPO_ROOT / "skills" / "coordinate-operator"
SCRIPTS = SKILL_ROOT / "scripts"
MAC_SH = SCRIPTS / "mac.sh"
INSPECT_SH = SCRIPTS / "inspect.sh"
PUMP_SH = SCRIPTS / "pump-visible-once.sh"


class CoordinateOperatorSkillTests(unittest.TestCase):
    def _fake_python(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "print(json.dumps({'cwd': os.getcwd(), "
            "'argv': sys.argv, 'executable': sys.argv[0]}))\n",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    def _run(self, script: Path, *args: str, **overrides: str):
        env = dict(os.environ)
        for key in (
            "MAC_REPO",
            "COORDINATE_REPO",
            "COORDINATOR_PYTHON_BIN",
            "MULTI_AGENT_COORDINATOR_DB",
            "COORDINATE_DB",
        ):
            env.pop(key, None)
        env.update(overrides)
        result = subprocess.run(
            [str(script), *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return next(
            json.loads(line)
            for line in result.stdout.splitlines()
            if line.startswith("{")
        )

    def _path_env(self, fake_python: Path) -> str:
        return f"{fake_python.parent}{os.pathsep}{os.environ.get('PATH', '')}"

    def test_wrappers_have_valid_bash_syntax(self):
        for script in (MAC_SH, INSPECT_SH, PUMP_SH):
            with self.subTest(script=script.name):
                subprocess.run(
                    ["bash", "-n", str(script)],
                    check=True,
                    timeout=10,
                )

    def test_mac_repo_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            default_repo = home / "projects" / "coordinate"
            coordinate_repo = root / "coordinate-override"
            mac_repo = root / "mac-override"
            for repo in (default_repo, coordinate_repo, mac_repo):
                repo.mkdir(parents=True)
            fake = self._fake_python(home / "bin" / "python3")
            common = {"HOME": str(home), "PATH": self._path_env(fake)}

            cases = (
                ({}, default_repo),
                ({"COORDINATE_REPO": str(coordinate_repo)}, coordinate_repo),
                (
                    {
                        "COORDINATE_REPO": str(coordinate_repo),
                        "MAC_REPO": str(mac_repo),
                    },
                    mac_repo,
                ),
            )
            for overrides, expected in cases:
                with self.subTest(overrides=overrides):
                    call = self._run(
                        MAC_SH,
                        "workspace",
                        "list",
                        **common,
                        **overrides,
                    )
                    self.assertEqual(
                        Path(call["cwd"]).resolve(),
                        expected.resolve(),
                    )

    def test_mac_python_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            (home / "projects" / "coordinate").mkdir(parents=True)
            path_python = self._fake_python(home / "bin" / "python3")
            common = {"HOME": str(home), "PATH": self._path_env(path_python)}

            fallback = self._run(MAC_SH, "workspace", "list", **common)
            self.assertEqual(Path(fallback["executable"]), path_python)

            venv_python = self._fake_python(
                home / "projects" / "coordinate" / ".venv" / "bin" / "python"
            )
            venv = self._run(MAC_SH, "workspace", "list", **common)
            self.assertEqual(Path(venv["executable"]), venv_python)

            explicit_python = self._fake_python(home / "explicit" / "python3")
            explicit = self._run(
                MAC_SH,
                "workspace",
                "list",
                **common,
                COORDINATOR_PYTHON_BIN=str(explicit_python),
            )
            self.assertEqual(Path(explicit["executable"]), explicit_python)

    def test_forwarding_wrappers_use_portable_default_repo(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            repo = home / "projects" / "coordinate"
            target_mac = repo / "skills" / "coordinate-operator" / "scripts" / "mac.sh"
            target_mac.parent.mkdir(parents=True)
            shutil.copy2(MAC_SH, target_mac)
            fake = self._fake_python(home / "bin" / "python3")
            common = {"HOME": str(home), "PATH": self._path_env(fake)}

            for script in (INSPECT_SH, PUMP_SH):
                with self.subTest(script=script.name):
                    call = self._run(
                        script,
                        "--workspace",
                        "demo",
                        **common,
                    )
                    self.assertEqual(
                        Path(call["cwd"]).resolve(),
                        repo.resolve(),
                    )

    def test_operator_skill_has_no_machine_specific_python_or_home_path(self):
        text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in SKILL_ROOT.rglob("*")
            if path.is_file()
        )
        self.assertNotIn("/Users/yinxin", text)
        self.assertNotIn(".pyenv/versions/3.12.13", text)

    def test_command_reference_tracks_current_runtime_cli_shape(self):
        reference = (
            SKILL_ROOT / "references" / "command-reference.md"
        ).read_text(encoding="utf-8")
        required = (
            "$MAC operator pending WORKSPACE",
            "$MAC runtime executor sync --source /path/to/agent-registry.toml",
            "$MAC runtime capacity sync --source /path/to/agent-registry.toml",
            "$MAC runtime request submit WORKSPACE",
            "--target-agent AGENT_ID",
            "--origin-json",
            "--reply-json",
            "$MAC runtime job lease renew JOB_ID",
            "--attempt-token ATTEMPT",
        )
        for snippet in required:
            with self.subTest(snippet=snippet):
                self.assertIn(snippet, reference)


if __name__ == "__main__":
    unittest.main()
