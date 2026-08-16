"""Portable-path contract tests for the coordinate-operator wrappers."""

import io
import json
import os
import runpy
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parent.parent
SKILL_ROOT = REPO_ROOT / "skills" / "coordinate-operator"
SCRIPTS = SKILL_ROOT / "scripts"
WINDOWS_SUBSKILL = SKILL_ROOT / "subskills" / "windows-multi-cli"
MAC_SH = SCRIPTS / "mac.sh"
INSPECT_SH = SCRIPTS / "inspect.sh"
PUMP_SH = SCRIPTS / "pump-visible-once.sh"
MCP_PREFLIGHT = WINDOWS_SUBSKILL / "scripts" / "mcp-preflight.py"


class CoordinateOperatorSkillTests(unittest.TestCase):
    def _mcp_server(self, expected_token: str, denied_status: int = 401):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format, *_args):
                return

            def do_POST(self):
                if self.headers.get("Authorization") != f"Bearer {expected_token}":
                    self.send_response(denied_status)
                    self.end_headers()
                    return
                length = int(self.headers.get("Content-Length", "0"))
                request = json.loads(self.rfile.read(length))
                if self.headers.get("MCP-Method") != request.get("method"):
                    self.send_response(400)
                    self.end_headers()
                    return
                if request["method"] == "server/discover":
                    result = {
                        "supportedVersions": ["2026-07-28"],
                        "capabilities": {"tools": {}},
                        "resultType": "complete",
                    }
                elif request["method"] == "tools/list":
                    result = {
                        "tools": [
                            {"name": "coordinate.operator_pending"},
                            {"name": "coordinate.channel_create"},
                        ],
                        "resultType": "complete",
                    }
                else:
                    result = {}
                body = json.dumps(
                    {"jsonrpc": "2.0", "id": request["id"], "result": result}
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}/mcp"

    def _run_mcp_preflight(self, url: str, token: str, *extra: str):
        env = dict(os.environ)
        env["COORDINATE_TEST_TOKEN"] = token
        return subprocess.run(
            [
                sys.executable,
                str(MCP_PREFLIGHT),
                "--url",
                url,
                "--token-env",
                "COORDINATE_TEST_TOKEN",
                *extra,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def _fake_python(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Resolve the interpreter before embedding it in the shebang. Release
        # candidates intentionally live under ``my release``; an unresolved
        # venv path there contains a space and is not a valid POSIX shebang.
        interpreter = Path(sys.executable).resolve()
        path.write_text(
            f"#!{interpreter}\n"
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

    def test_parent_skill_is_a_thin_progressive_disclosure_router(self):
        parent_path = SKILL_ROOT / "SKILL.md"
        parent = parent_path.read_text(encoding="utf-8")
        self.assertLessEqual(len(parent.splitlines()), 150)
        subskills = (
            "workspace-task-lifecycle",
            "remote-mcp-control",
            "runtime-execution",
            "messaging-delivery",
            "github-collaboration",
            "worker-supervision",
            "production-recovery",
            "windows-multi-cli",
        )
        for name in subskills:
            with self.subTest(subskill=name):
                self.assertIn(f"subskills/{name}/SKILL.md", parent)
                skill = SKILL_ROOT / "subskills" / name / "SKILL.md"
                self.assertTrue(skill.is_file())
                self.assertTrue(skill.read_text(encoding="utf-8").startswith("---\nname:"))

        for detail_that_must_stay_lazy in (
            "DPAPI",
            "issue scan WORKSPACE",
            "delivery recover-sending",
            "runtime job lease renew JOB_ID",
            "workspace init-harness --mode",
        ):
            with self.subTest(detail=detail_that_must_stay_lazy):
                self.assertNotIn(detail_that_must_stay_lazy, parent)

    def test_legacy_reference_paths_are_thin_routing_indexes(self):
        for name in ("command-reference.md", "workflows.md", "troubleshooting.md"):
            with self.subTest(reference=name):
                text = (SKILL_ROOT / "references" / name).read_text(encoding="utf-8")
                self.assertLessEqual(len(text.splitlines()), 30)
                self.assertIn("subskills/", text)

    def test_domain_contracts_live_in_their_own_subskills(self):
        cases = {
            "workspace-task-lifecycle": (
                "task create",
                "update-dependencies",
                "completion_prepare",
                "harness-checklist.json",
            ),
            "remote-mcp-control": (
                "tools/list",
                "workspace",
                "platform",
                "strict typed",
            ),
            "runtime-execution": (
                "runtime executor sync",
                "runtime request submit",
                "attempt_token",
                "generic_subprocess",
            ),
            "messaging-delivery": (
                "workspace channel bind",
                "recover-sending",
                "discord",
            ),
            "github-collaboration": (
                "issue scan",
                "pr publish",
                "merge gate",
            ),
            "worker-supervision": (
                "JSONL",
                "idle",
                "局部 Operator",
            ),
            "production-recovery": (
                "coord-ssh",
                "/opt/coordinate",
                "rollback",
            ),
        }
        for name, required in cases.items():
            root = SKILL_ROOT / "subskills" / name
            text = "\n".join(
                path.read_text(encoding="utf-8")
                for path in root.rglob("*.md")
            )
            for snippet in required:
                with self.subTest(subskill=name, snippet=snippet):
                    self.assertIn(snippet, text)

    def test_mcp_preflight_rejects_remote_plaintext_before_token_read(self):
        env = dict(os.environ)
        env.pop("UNSET_COORDINATE_TEST_TOKEN", None)
        result = subprocess.run(
            [
                sys.executable,
                str(MCP_PREFLIGHT),
                "--url",
                "http://coordinate.example/mcp",
                "--token-env",
                "UNSET_COORDINATE_TEST_TOKEN",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["classification"], "config")
        self.assertIn("HTTPS", payload["message"])

    def test_windows_subskill_is_progressively_disclosed_and_separates_data_planes(self):
        skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        subskill = (WINDOWS_SUBSKILL / "SKILL.md").read_text(encoding="utf-8")
        references = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((WINDOWS_SUBSKILL / "references").glob("*.md"))
        )
        self.assertIn("subskills/windows-multi-cli/SKILL.md", skill)
        self.assertNotIn("DPAPI", skill)
        for reference_name in (
            "remote-mcp.md",
            "runtime-http-agentd.md",
            "credential-rotation.md",
        ):
            self.assertIn(reference_name, subskill)
        for snippet in (
            "mcp-preflight.py",
            "Remote MCP",
            "Runtime HTTP",
            "每个 Operator client 一个 principal 和 token",
            "worker profile 不带 Operator authority",
            "prepare new digest",
            "retire old digest",
        ):
            with self.subTest(snippet=snippet):
                self.assertIn(snippet, subskill + references)

    def test_mcp_preflight_classifies_ready_and_tool_scope_without_token_echo(self):
        url = self._mcp_server("test-only-secret")
        ready = self._run_mcp_preflight(
            url,
            "test-only-secret",
            "--expected-tool",
            "coordinate.operator_pending",
        )
        self.assertEqual(ready.returncode, 0, ready.stderr)
        payload = json.loads(ready.stdout)
        self.assertEqual(payload["classification"], "ready")
        self.assertNotIn("test-only-secret", ready.stdout + ready.stderr)

        missing = self._run_mcp_preflight(
            url,
            "test-only-secret",
            "--expected-tool",
            "coordinate.not_granted",
        )
        self.assertEqual(missing.returncode, 14, missing.stderr)
        payload = json.loads(missing.stdout)
        self.assertEqual(payload["classification"], "tool_scope")

    def test_mcp_preflight_classifies_401_without_response_or_token_echo(self):
        url = self._mcp_server("expected-secret")
        result = self._run_mcp_preflight(url, "wrong-secret")
        self.assertEqual(result.returncode, 11, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["classification"], "authentication")
        self.assertEqual(payload["http_status"], 401)
        self.assertNotIn("wrong-secret", result.stdout + result.stderr)

    def test_mcp_preflight_classifies_403_and_network(self):
        forbidden = self._run_mcp_preflight(
            self._mcp_server("expected-secret", denied_status=403),
            "wrong-secret",
        )
        self.assertEqual(forbidden.returncode, 12)
        self.assertEqual(json.loads(forbidden.stdout)["classification"], "forbidden")

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        network = self._run_mcp_preflight(
            f"http://127.0.0.1:{port}/mcp",
            "test-only-secret",
            "--timeout",
            "1",
        )
        self.assertEqual(network.returncode, 10)
        self.assertEqual(json.loads(network.stdout)["classification"], "network")

    @unittest.skipIf(os.name == "nt", "POSIX mode contract")
    def test_mcp_preflight_rejects_group_readable_token_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "coordinate.token"
            token.write_text("test-only-secret", encoding="utf-8")
            token.chmod(0o640)
            result = subprocess.run(
                [
                    sys.executable,
                    str(MCP_PREFLIGHT),
                    "--url",
                    "http://127.0.0.1:9/mcp",
                    "--token-file",
                    str(token),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout)["classification"], "config")

    def test_mcp_preflight_windows_token_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "coordinate.token"
            token.write_text("test-only-secret", encoding="utf-8")
            namespace = runpy.run_path(str(MCP_PREFLIGHT))
            main = namespace["main"]
            stdout = io.StringIO()
            with patch.object(main.__globals__["os"], "name", "nt"), redirect_stdout(stdout):
                rc = main(
                    [
                        "--url",
                        "http://127.0.0.1:9/mcp",
                        "--token-file",
                        str(token),
                    ]
                )
        self.assertEqual(rc, 2)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["classification"], "config")
        self.assertIn("Windows", payload["message"])


if __name__ == "__main__":
    unittest.main()
