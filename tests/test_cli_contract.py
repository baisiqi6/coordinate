"""Deterministic CLI contract snapshot and support-seam boundary tests for P9-0A1."""
from __future__ import annotations

import argparse
import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import coordinate.cli
import coordinate.cli_support
from coordinate.cli import build_parser, DEFAULT_DB_PATH
from coordinate.cli_support import open_connection, print_json


FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "cli_contract.json"
SRC_PATH = Path(__file__).resolve().parents[1] / "src"
CONTRACT_GENERATION_SCRIPT = Path(__file__).resolve()
HOME_TOKEN = "<HOME>"

# P9-0A2a migrated exactly these 11 leaves from coordinate.cli to coordinate.workspace_cli.

# P9-0A2b migrated exactly these 10 leaves from coordinate.cli to coordinate.planning_cli.

# P9-0A2c migrated exactly these 5 leaves from coordinate.cli to coordinate.issue_cli.

# P9-0A3a migrated exactly these 16 leaves from coordinate.cli to coordinate.execution_cli.

# P9-3A added these capacity leaves under runtime capacity.
P9_3A_CAPACITY_LEAVES = {
    "runtime capacity sync",
    "runtime capacity list",
    "runtime capacity show",
}

# P9-3B added these lease leaves under runtime job.
P9_3B_LEASE_LEAVES = {
    "runtime job lease renew": "handle_runtime_job_lease_renew",
    "runtime job lease reap": "handle_runtime_job_lease_reap",
}

# Issue #12 added these usage leaves under runtime usage.
P9_ISSUE12_USAGE_LEAVES = {
    "runtime usage policy-set": "handle_runtime_usage_policy_set",
    "runtime usage status": "handle_runtime_usage_status",
}

_P9_3C1_P1_BASE_FIXTURE_SHA256 = (
    "869084cdc985a0efb9921266af98f5813d0d6efca03b90aeebf5c7916f2b5746"
)
_P9_3C1_P1_AGENT_HELP = (
    "usage: coordinate runtime agent [-h] {register,heartbeat} ...\n\n"
    "positional arguments:\n"
    "  {register,heartbeat}\n"
    "    register            Upsert an agentd or bridge record in the runtime agent registry\n"
    "    heartbeat           Mark an already-registered runtime client as online and refresh last-seen\n\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
)
_P9_3C1_P1_CLAIM_HELP = (
    "usage: coordinate runtime job claim [-h] --agent-id AGENT_ID [--recoverable]\n"
    "                                    [--recovery-reason RECOVERY_REASON] [--prior-process-stopped]\n\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
    "  --agent-id AGENT_ID\n"
    "  --recoverable         Also claim recoverable timed_out jobs (explicit recovery path). Default:\n"
    "                        only pending.\n"
    "  --recovery-reason RECOVERY_REASON\n"
    "                        Audited Operator reason for recovery; required with --recoverable\n"
    "  --prior-process-stopped\n"
    "                        Operator confirmation that the prior provider process/session has stopped\n"
)
_P9_3C1_P1_REAP_HELP = (
    "usage: coordinate runtime job lease reap [-h] [--actor ACTOR] [--batch-size BATCH_SIZE]\n\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
    "  --actor ACTOR\n"
    "  --batch-size BATCH_SIZE\n"
)

# P9-0A3b migrated exactly these 10 leaves from coordinate.cli to coordinate.delivery_cli.

# P9-0A4a migrated exactly these 6 leaves from coordinate.cli to coordinate.completion_cli.

# P9-0A4b migrated exactly these 12 leaves from coordinate.cli to coordinate.workflow_cli.


@contextmanager
def _sanitized_environ():
    """Remove MULTI_AGENT_COORDINATOR_DB and pin COLUMNS for deterministic parser builds."""
    removed: list[tuple[str, str]] = []
    for key in ("MULTI_AGENT_COORDINATOR_DB",):
        if key in os.environ:
            removed.append((key, os.environ.pop(key)))
    old_columns = os.environ.get("COLUMNS")
    os.environ["COLUMNS"] = "100"
    try:
        yield
    finally:
        if old_columns is None:
            os.environ.pop("COLUMNS", None)
        else:
            os.environ["COLUMNS"] = old_columns
        for key, value in removed:
            os.environ[key] = value


def _allowed_env() -> dict[str, str]:
    """Explicit environment allowlist for clean contract-generation subprocesses.

    HOME is intentionally omitted so contract bytes never depend on the caller's
    home directory. The help normalizer recognizes only the portable ``~/``
    prefix preserved in ``DEFAULT_DB_PATH``.
    """
    return {
        "PATH": os.environ.get("PATH", ""),
        "LANG": "C",
        "LC_ALL": "C",
        "COLUMNS": "100",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(SRC_PATH),
    }


def _normalize_home(value: str, default_db_path: str) -> str:
    """Replace a portable ``~/`` prefix with a deterministic semantic token."""
    if not default_db_path.startswith("~/"):
        return value
    return value.replace("~/", f"{HOME_TOKEN}/")


def _normalize_value(value: object, default_db_path: str) -> object:
    """Return a JSON-safe, deterministic representation of an action attribute."""
    if value is None:
        return None
    if callable(value):
        return f"{value.__module__}.{value.__qualname__}"
    if isinstance(value, str):
        if value == default_db_path:
            return "<DEFAULT_DB_PATH>"
        return _normalize_home(value, default_db_path)
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_normalize_value(v, default_db_path) for v in value]
    raise TypeError(f"Unsupported contract value: {value!r}")


def _normalize_action(action: argparse.Action, default_db_path: str) -> dict[str, object]:
    """Serialize one parser action deterministically."""
    choices: list[str] | None = None
    if action.choices is not None:
        choices = list(action.choices)

    type_identity: str | None = None
    if action.type is not None:
        type_identity = f"{action.type.__module__}.{action.type.__qualname__}"

    help_text: str | None = action.help
    if isinstance(help_text, str):
        help_text = _normalize_home(help_text, default_db_path)

    return {
        "option_strings": list(action.option_strings),
        "dest": action.dest,
        "action_class": type(action).__name__,
        "nargs": action.nargs,
        "required": action.required,
        "choices": choices,
        "default": _normalize_value(action.default, default_db_path),
        "const": _normalize_value(action.const, default_db_path),
        "metavar": action.metavar,
        "type": type_identity,
        "help": help_text,
    }


def _build_contract() -> dict[str, object]:
    """Build the normalized contract dictionary from the current parser tree."""
    from coordinate.cli import build_parser
    from coordinate.cli_support import DEFAULT_DB_PATH

    with _sanitized_environ():
        parser = build_parser()

        nodes: list[dict[str, object]] = []
        leaf_paths: list[str] = []
        top_level_commands: list[str] = []

        def traverse(p: argparse.ArgumentParser, path: list[str]) -> None:
            subparser_actions = [
                a for a in p._actions if isinstance(a, argparse._SubParsersAction)
            ]
            assert len(subparser_actions) <= 1, f"Parser {' '.join(path) or 'root'} has multiple subparser actions"
            subparser_action = subparser_actions[0] if subparser_actions else None

            nodes.append(
                {
                    "path": path,
                    "prog": p.prog,
                    "help": _normalize_home(p.format_help(), DEFAULT_DB_PATH),
                    "actions": [_normalize_action(a, DEFAULT_DB_PATH) for a in p._actions],
                    "defaults": {
                        k: _normalize_value(v, DEFAULT_DB_PATH)
                        for k, v in getattr(p, "_defaults", {}).items()
                    },
                }
            )

            if subparser_action is None:
                if path:
                    leaf_paths.append(" ".join(path))
                return

            children = list(subparser_action.choices.items())
            if not path:
                top_level_commands[:] = [name for name, _ in children]
            for name, child in children:
                traverse(child, path + [name])

        traverse(parser, [])

    return {
        "metadata": {
            "prog": parser.prog,
            "top_level_commands": top_level_commands,
            "leaf_count": len(leaf_paths),
            "node_count": len(nodes),
            "default_db_path_sha256": hashlib.sha256(str(DEFAULT_DB_PATH).encode("utf-8")).hexdigest(),
        },
        "leaf_paths": leaf_paths,
        "nodes": nodes,
    }


def _validate_raw_leaf_handlers(parser: argparse.ArgumentParser) -> None:
    """Recursively assert every leaf has exactly one callable handler default."""
    subparser_actions = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    if subparser_actions:
        assert len(subparser_actions) == 1
        for child in subparser_actions[0].choices.values():
            _validate_raw_leaf_handlers(child)
        return

    defaults = getattr(parser, "_defaults", {})
    assert set(defaults.keys()) == {"handler"}, (
        f"Leaf {parser.prog!r} must have exactly one default named 'handler', got {set(defaults.keys())}"
    )
    assert callable(defaults["handler"]), (
        f"Leaf {parser.prog!r} handler must be callable, got {defaults['handler']!r}"
    )


def _generate_contract_bytes() -> bytes:
    """Generate the canonical contract JSON bytes."""
    contract = _build_contract()
    return json.dumps(contract, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8") + b"\n"


def _run_generation_subprocess(flag: str = "--dump") -> bytes:
    """Run a clean subprocess that generates the requested dump and return stdout bytes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        result = subprocess.run(
            [sys.executable, str(CONTRACT_GENERATION_SCRIPT), flag],
            cwd=tmpdir,
            env=_allowed_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    return result.stdout


# Marker recorded in the semantic dump metadata so receipts can prove which
# projection produced the bytes being byte-compared.
_SEMANTIC_PROJECTION_MARKER = "semantic-help-whitespace-v1"


def _project_semantic_help(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy with only the node formatted help whitespace layout normalized.

    Each node's ``format_help()`` text is folded to its equivalent token
    sequence (single-space separated), so argparse layout differences across
    Python versions -- usage wrapping, description column alignment, blank
    lines -- no longer affect comparison. Every other contract field is
    preserved byte-for-byte: path, prog, actions (option strings, dest, action
    class, nargs, required, choices, default, const, metavar, type), help text
    tokens in order, defaults, leaf order and counts.
    """
    projected = copy.deepcopy(contract)
    for node in projected["nodes"]:
        help_text = node["help"]
        if not isinstance(help_text, str):
            raise TypeError(
                f"node {' '.join(node['path']) or '<root>'!r} help must be str, got {type(help_text).__name__}"
            )
        node["help"] = " ".join(help_text.split())
    projected["metadata"]["projection"] = _SEMANTIC_PROJECTION_MARKER
    return projected


def _generate_semantic_contract_bytes() -> bytes:
    """Generate the semantic projection bytes for cross-version byte comparison.

    Machine-callable via ``tests/test_cli_contract.py --dump-semantic``; the
    operator byte-compares the stdout of the Python 3.12 and 3.14 runs.
    """
    return _serialize_contract(_project_semantic_help(_build_contract()))


# Historical help strings captured from the post-C1 / pre-C2 fixture.
_OLD_ISSUE_MATERIALIZE_FILES_HELP = (
    "usage: coordinate issue materialize-files [-h] --workspace-path WORKSPACE_PATH\n"
    "                                          --harness-root HARNESS_ROOT --task-id TASK_ID\n"
    "                                          --plan-doc PLAN_DOC [--title TITLE] [--phase PHASE]\n"
    "                                          [--priority PRIORITY] [--allow-runtime-copy]\n"
    "\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
    "  --workspace-path WORKSPACE_PATH\n"
    "  --harness-root HARNESS_ROOT\n"
    "  --task-id TASK_ID\n"
    "  --plan-doc PLAN_DOC\n"
    "  --title TITLE\n"
    "  --phase PHASE\n"
    "  --priority PRIORITY\n"
    "  --allow-runtime-copy  Override the /opt runtime-copy guard\n"
)

_OLD_ISSUE_MATERIALIZE_RECORD_HELP = (
    "usage: coordinate issue materialize-record [-h] --event-id EVENT_ID --plan-doc PLAN_DOC\n"
    "                                           [--task-id TASK_ID] [--title TITLE] [--owner OWNER]\n"
    "                                           [--branch BRANCH] [--phase PHASE] [--actor ACTOR]\n"
    "                                           [--platform PLATFORM] [--destination DESTINATION]\n"
    "                                           workspace_id\n"
    "\n"
    "positional arguments:\n"
    "  workspace_id\n"
    "\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
    "  --event-id EVENT_ID\n"
    "  --plan-doc PLAN_DOC\n"
    "  --task-id TASK_ID\n"
    "  --title TITLE\n"
    "  --owner OWNER\n"
    "  --branch BRANCH\n"
    "  --phase PHASE\n"
    "  --actor ACTOR\n"
    "  --platform PLATFORM\n"
    "  --destination DESTINATION\n"
)

# SHA-256 of the post-C1 / pre-C2 issue materialize-files node canonical bytes.
_S4C2_ISSUE_MATERIALIZE_FILES_NODE_SHA256 = (
    "c794be65c5efc3fdc804112695571695997c5bc9a736f7087c1bc510a53fba94"
)

# SHA-256 of the post-C1 / pre-C2 issue materialize-record node canonical bytes.
_S4C2_ISSUE_MATERIALIZE_RECORD_NODE_SHA256 = (
    "ae7bf36031316db4bcccae756f7693e2f1205cfbc15fa25e8990a7b4871c5422"
)

# S4-D projection-doctor CLI deltas.
_S4D_BASELINE_FIXTURE_SHA256 = (
    "779c146bf1b861d51455dc3ba5d21a436f1327b5b39e6cfad828a309c251146f"
)
_S4D_WORKSPACE_DOCTOR_NODE_SHA256 = (
    "d51fc123073a032bec4bd82fc0c34dd190503c89a9f6656c9fe80d8e6279e0ec"
)
_S4C2_WORKSPACE_DOCTOR_NODE_SHA256 = (
    "6b09b80735b37735b17d37c6c2176f76c5efa1a0c748a46e1acb5f51498f6622"
)

# Historical help strings captured from the S4-B1 baseline fixture.


# Pre-targeted-reconcile ``reconcile`` help, extracted from the baseline
# commit 1aeadbaa43405208b76f3b24f2f848dc4219f059 before ``reconcile`` gained
# ``--task-id``. Restoring it keeps historical rewind SHA proofs free of the
# later targeted-reconcile CLI addition.
_PRE_TARGETED_RECONCILE_HELP = (
    "usage: coordinate reconcile [-h] [--no-refresh] workspace_id\n"
    "\n"
    "positional arguments:\n"
    "  workspace_id\n"
    "\n"
    "options:\n"
    "  -h, --help    show this help message and exit\n"
    "  --no-refresh  Read state without running harnessctl state\n"
)

# SHA-256 of the canonical baseline fixture (commit
# 1aeadbaa43405208b76f3b24f2f848dc4219f059) before ``reconcile --task-id``.
_PRE_TARGETED_BASELINE_FIXTURE_SHA256 = (
    "4393fc12facaa3bb6dd9bf6116cb74ee22c8a4ce3c25b627e317d4e29698a0e3"
)


def _restore_pre_r1_root_help(help_text: str) -> str:
    """Rebuild the pre-R1 root help without the ``mcp`` subcommand.

    The root choices line drops ``mcp`` and the subcommand description line is
    removed; all other lines keep their exact text (``mcp`` is short enough
    that no column alignment changes).
    """
    result: list[str] = []
    for line in help_text.splitlines():
        if ",mcp" in line:
            result.append(line.replace(",mcp", ""))
            continue
        if line.startswith("    mcp ") and "MCP agent interface" in line:
            continue
        result.append(line)
    return "\n".join(result) + "\n"


# Pre-update-dependencies ``task`` parent help, extracted from the committed
# fixture that predates the two dependency-update leaves.  Restoring it keeps
# historical rewind SHA proofs free of the later CLI addition.
_PRE_UPDATE_DEPENDENCIES_TASK_HELP = (
    "usage: coordinate task [-h] {create,create-files,create-record,handoff} ...\n\n"
    "positional arguments:\n"
    "  {create,create-files,create-record,handoff}\n"
    "    create              Combined managed create: checklist file half first, DB record half second\n"
    "                        (idempotent; --operation-id to pin)\n"
    "    create-files        Coding-host half of host-aware task create: checklist file half only (no\n"
    "                        DB write)\n"
    "    create-record       Server half of host-aware task create: write DB task mirror + plan.ready\n"
    "                        only (no checklist write)\n"
    "    handoff             Generate a structured worker handoff\n\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
)


def _restore_pre_r2a_root_help(help_text: str) -> str:
    """Rebuild the pre-R2A root help without the ``runtime-http`` subcommand.

    The root choices line drops ``runtime-http`` and the subcommand description
    line is removed. ``runtime-http`` is the longest subcommand name, so the
    description column of every remaining subcommand must also be re-aligned
    to the next-longest name (``assignment``).
    """
    longest = len("assignment")
    result: list[str] = []
    for line in help_text.splitlines():
        if ",runtime-http," in line or ",runtime-http}" in line:
            result.append(line.replace(",runtime-http,", ",").replace(",runtime-http}", "}"))
            continue
        if line.startswith("    runtime-http ") and "runtime HTTP" in line:
            continue
        result.append(line)
    return "\n".join(result) + "\n"

def _restore_pre_trace_root_help(help_text: str) -> str:
    """Rebuild the pre-trace root help without the ``trace`` subcommand.

    Issue #11 adds ``trace`` between ``runtime-http`` and ``serve`` in the
    root choices plus one description line; removing both restores the exact
    pre-trace bytes. Description-column alignment is unchanged because
    ``runtime-http`` remains the longest subcommand name.
    """
    result: list[str] = []
    for line in help_text.splitlines():
        if ",trace," in line:
            result.append(line.replace(",trace,", ","))
            continue
        if line.startswith("    trace ") and "trace projection (Issue #11)" in line:
            continue
        result.append(line)
    return "\n".join(result) + "\n"


def _remove_trace_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the Issue #11 ``trace`` delta removed.

    Issue #11 adds exactly one top-level command (``trace``), two leaves
    (``trace task`` / ``trace job``) and three nodes. Historical rewind
    proofs strip this post-baseline delta first so their pinned baseline
    SHA-256 proofs keep verifying the pre-trace bytes. No-op when absent.
    """
    historical = _remove_issue12_usage_leaves(contract)
    has_trace = any(node["path"][:1] == ["trace"] for node in historical["nodes"])
    if not has_trace:
        return historical

    historical["nodes"] = [
        node for node in historical["nodes"] if node["path"][:1] != ["trace"]
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"] if not path.startswith("trace ")
    ]
    historical["metadata"]["leaf_count"] = int(
        historical["metadata"]["leaf_count"]
    ) - 2
    historical["metadata"]["node_count"] = int(
        historical["metadata"]["node_count"]
    ) - 3
    historical["metadata"]["top_level_commands"] = [
        name
        for name in historical["metadata"]["top_level_commands"]
        if name != "trace"
    ]

    found = set()
    for node in historical["nodes"]:
        if not node["path"]:
            subparsers = [
                action
                for action in node["actions"]
                if action["action_class"] == "_SubParsersAction"
            ]
            if len(subparsers) != 1 or "trace" not in subparsers[0]["choices"]:
                raise AssertionError("unexpected root subparser delta for trace")
            subparsers[0]["choices"] = [
                choice for choice in subparsers[0]["choices"] if choice != "trace"
            ]
            node["help"] = _restore_pre_trace_root_help(node["help"])
            found.add("root")
    if found != {"root"}:
        raise AssertionError(f"incomplete trace CLI delta: {sorted(found)}")
    return historical


def _remove_runtime_http_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the R2A ``runtime-http`` delta removed.

    R2A adds exactly one top-level command (``runtime-http``), one leaf
    (``runtime-http serve``) and two nodes. Rewinding restores the pre-R2A
    tree (22 commands / 93 leaves / 122 nodes). No-op when already absent.
    """
    return _strip_runtime_http_nodes(copy.deepcopy(contract))

def _strip_runtime_http_nodes(historical: dict[str, object]) -> dict[str, object]:
    """Strip the two ``runtime-http`` nodes from a copy of *historical*."""
    historical = _remove_trace_delta(historical)
    has_rt = any(node["path"] == ["runtime-http"] for node in historical["nodes"])
    if not has_rt:
        return historical

    historical["nodes"] = [
        node
        for node in historical["nodes"]
        if node["path"] not in (["runtime-http"], ["runtime-http", "serve"])
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"] if path != "runtime-http serve"
    ]
    historical["metadata"]["leaf_count"] = int(
        historical["metadata"]["leaf_count"]
    ) - 1
    historical["metadata"]["node_count"] = int(
        historical["metadata"]["node_count"]
    ) - 2
    historical["metadata"]["top_level_commands"] = [
        name
        for name in historical["metadata"]["top_level_commands"]
        if name != "runtime-http"
    ]

    found = set()
    for node in historical["nodes"]:
        if not node["path"]:
            subparsers = [
                action
                for action in node["actions"]
                if action["action_class"] == "_SubParsersAction"
            ]
            if len(subparsers) != 1 or "runtime-http" not in subparsers[0]["choices"]:
                raise AssertionError("unexpected root subparser delta")
            subparsers[0]["choices"] = [
                choice for choice in subparsers[0]["choices"] if choice != "runtime-http"
            ]
            node["help"] = _restore_pre_r2a_root_help(node["help"])
            found.add("root")
    if found != {"root"}:
        raise AssertionError(f"incomplete runtime-http CLI delta: {sorted(found)}")
    return historical


def _remove_update_dependencies_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the dependency-update parser delta removed.

    Restores the pre-delta ``task`` subparser choices/help and the leaf/node
    counts so historical baseline rewinds are not polluted by the two new
    leaves (``task update-dependencies`` / ``task update-dependencies-files``).
    No-op when the delta is already absent.
    """
    historical = copy.deepcopy(contract)
    delta_paths = {
        ("task", "update-dependencies"): "task update-dependencies",
        ("task", "update-dependencies-files"): "task update-dependencies-files",
    }
    has_delta = any(tuple(node["path"]) in delta_paths for node in historical["nodes"])
    if not has_delta:
        return historical

    historical["nodes"] = [
        node for node in historical["nodes"] if tuple(node["path"]) not in delta_paths
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"] if path not in delta_paths.values()
    ]
    historical["metadata"]["leaf_count"] = int(
        historical["metadata"]["leaf_count"]
    ) - 2
    historical["metadata"]["node_count"] = int(
        historical["metadata"]["node_count"]
    ) - 2

    found = set()
    for node in historical["nodes"]:
        if node["path"] == ["task"]:
            subparsers = [
                action
                for action in node["actions"]
                if action["action_class"] == "_SubParsersAction"
            ]
            if len(subparsers) != 1:
                raise AssertionError("unexpected task subparser delta")
            choices = subparsers[0]["choices"]
            new_choices = [path.split(" ", 1)[1] for path in delta_paths.values()]
            if not set(new_choices).issubset(choices):
                raise AssertionError("missing update-dependencies subparser choices")
            subparsers[0]["choices"] = [
                choice for choice in choices if choice not in new_choices
            ]
            node["help"] = _PRE_UPDATE_DEPENDENCIES_TASK_HELP
            found.add("task")
    if found != {"task"}:
        raise AssertionError(f"incomplete update-dependencies CLI delta: {sorted(found)}")
    return historical


# Pre-R3 ``mcp`` help texts (extracted from the pre-R3 committed fixture).
_PRE_R3_ROOT_HELP = "usage: coordinate [-h] [--db DB] [--version]\n                  {workspace,state,event,task,plan,runner,reconcile,branch,pr,ci,review,merge,issue,job,delivery,policy,worker,runtime,assignment,operator,mcp,runtime-http,serve}\n                  ...\n\npositional arguments:\n  {workspace,state,event,task,plan,runner,reconcile,branch,pr,ci,review,merge,issue,job,delivery,policy,worker,runtime,assignment,operator,mcp,runtime-http,serve}\n    workspace           Manage harness workspaces\n    state               Refresh and print harness state for a workspace\n    event               Append or inspect normalized events\n    task                Create and inspect coordinator task mirrors\n    plan                Plan review and approval gate\n    runner              Manage runner profiles\n    reconcile           Sync coordinator task mirror from harness state\n    branch              Manage branch allocations\n    pr                  Manage PR links\n    ci                  Check CI status\n    review              Check PR review status\n    merge               Check merge readiness\n    issue               Scan and triage GitHub issues\n    job                 Create, run, and list jobs\n    delivery            Create, send, and list bus deliveries\n    policy              Render workflow events into visible deliveries\n    worker              Run coordinator worker loops\n    runtime             Bridge and agentd runtime operations\n    assignment          Manage task assignments\n    operator            Operator-facing pending-action queries\n    mcp                 MCP agent interface (stdio)\n    runtime-http        Loopback runtime HTTP data plane (R2A)\n    serve               Run coordinator daemon with Discord bot\n\noptions:\n  -h, --help            show this help message and exit\n  --db DB               SQLite database path\n  --version             show program's version number and exit\n"
_PRE_R3_MCP_HELP = "usage: coordinate mcp [-h] {serve} ...\n\npositional arguments:\n  {serve}\n    serve     Serve the MCP agent interface over stdio (R1)\n\noptions:\n  -h, --help  show this help message and exit\n"
_PRE_R3_MCP_SERVE_HELP = "usage: coordinate mcp serve [-h] [--transport {stdio}] [--actor ACTOR]\n\noptions:\n  -h, --help           show this help message and exit\n  --transport {stdio}  Transport to serve (R1: stdio only)\n  --actor ACTOR        Fixed actor identity recorded for tool mutations; callers cannot override\n"
_PRE_R3_MCP_SERVE_TRANSPORT_HELP = "Transport to serve (R1: stdio only)"
_PRE_R3_MCP_SERVE_ACTOR_HELP = "Fixed actor identity recorded for tool mutations; callers cannot override"

# Pre-R5B ``mark-done-files`` help/action texts (extracted from the committed
# fixture before the Remote MCP transport options landed).
_PRE_R5B_ASSIGNMENT_HELP = "usage: coordinate assignment [-h]\n                             {request,accept,handoff,blocker,unblock,closeout,review-result,mark-done,mark-done-prepare,mark-done-preflight,mark-done-claim,mark-done-apply,mark-done-files,mark-done-record}\n                             ...\n\npositional arguments:\n  {request,accept,handoff,blocker,unblock,closeout,review-result,mark-done,mark-done-prepare,mark-done-preflight,mark-done-claim,mark-done-apply,mark-done-files,mark-done-record}\n    request             Request a task assignment\n    accept              Accept a task assignment\n    handoff             Hand off a task to another agent\n    blocker             Raise a blocker on a task\n    unblock             Resolve a blocker on a task\n    closeout            Request closeout review for a task\n    review-result       Submit a review result for a task\n    mark-done           Mark a task as done\n    mark-done-prepare   Validate the closeout/review gate on the control plane and issue a one-\n                        time completion.authorized receipt binding the host-aware mark-done files\n                        + record pair.\n    mark-done-preflight\n                        Read-only: re-query a receipt from the control-plane DB and return its\n                        authoritative workspace/task/status/expiry. The coding host calls this\n                        through coord-ssh before mutating the canonical checklist so it never\n                        trusts its own claims.\n    mark-done-claim     Atomically reserve a receipt authorized -> claimed on the control plane,\n                        recording before/expected-after fingerprints. Server-side sink invoked by\n                        the coding host through coord-ssh BEFORE the checklist mutation (two-phase\n                        reserve step).\n    mark-done-apply     Acknowledge a claimed receipt -> applied on the control plane, recording\n                        the actual after-fingerprint. Server-side sink invoked by the coding host\n                        through coord-ssh AFTER the canonical checklist write lands (two-phase\n                        apply step).\n    mark-done-files     Coding-host half of host-aware mark-done. Writes local checklist file half\n                        only. Normal path requires --receipt and a remote coord CLI (--event-cli-\n                        path) to verify/claim the receipt online before any file mutation.\n                        --repair-reason selects the explicit repair-only path.\n    mark-done-record    Server half of host-aware mark-done. Writes the control-plane task.done\n                        event after re-verifying the receipt and the deployed harness. Normal path\n                        requires --receipt; --repair-reason selects the explicit repair-only path.\n\noptions:\n  -h, --help            show this help message and exit\n"
_PRE_R5B_MARK_DONE_FILES_HELP = "usage: coordinate assignment mark-done-files [-h] --workspace-path WORKSPACE_PATH --harness-root\n                                             HARNESS_ROOT --task-id TASK_ID\n                                             [--workspace-id WORKSPACE_ID] [--actor ACTOR]\n                                             [--verification VERIFICATION] [--receipt RECEIPT]\n                                             [--event-cli-path EVENT_CLI_PATH]\n                                             [--repair-reason REPAIR_REASON]\n                                             [--allow-runtime-copy]\n\noptions:\n  -h, --help            show this help message and exit\n  --workspace-path WORKSPACE_PATH\n  --harness-root HARNESS_ROOT\n  --task-id TASK_ID\n  --workspace-id WORKSPACE_ID\n  --actor ACTOR\n  --verification VERIFICATION\n  --receipt RECEIPT\n  --event-cli-path EVENT_CLI_PATH\n                        Path to a coord CLI that runs mark-done-preflight / mark-done-claim\n                        against the control-plane DB (e.g. <HOME>/.local/bin/coord-ssh). Required\n                        for the normal receipt path so the host verifies the receipt online before\n                        mutating files.\n  --repair-reason REPAIR_REASON\n  --allow-runtime-copy  Allow mutation of /opt deployment copy\n"
_PRE_R5B_EVENT_CLI_PATH_HELP = "Path to a coord CLI that runs mark-done-preflight / mark-done-claim against the control-plane DB (e.g. <HOME>/.local/bin/coord-ssh). Required for the normal receipt path so the host verifies the receipt online before mutating files."

# Pre-Issue-#18 ``workspace host-profile set`` help, extracted from the
# committed fixture before the sibling worktree-root flags landed. Restoring it
# keeps historical rewind SHA proofs free of the later CLI addition.
_PRE_ISSUE18_HOST_PROFILE_SET_HELP = (
    "usage: coordinate workspace host-profile set [-h] --host-id HOST_ID --workspace-path\n"
    "                                             WORKSPACE_PATH [--harness-root HARNESS_ROOT]\n"
    "                                             [--harnessctl-path HARNESSCTL_PATH]\n"
    "                                             [--coordinator-cli-path COORDINATOR_CLI_PATH]\n"
    "                                             [--coordinator-db-path COORDINATOR_DB_PATH]\n"
    "                                             [--shell SHELL] [--metadata-json METADATA_JSON]\n"
    "                                             workspace_id\n"
    "\n"
    "positional arguments:\n"
    "  workspace_id\n"
    "\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
    "  --host-id HOST_ID\n"
    "  --workspace-path WORKSPACE_PATH\n"
    "  --harness-root HARNESS_ROOT\n"
    "  --harnessctl-path HARNESSCTL_PATH\n"
    "  --coordinator-cli-path COORDINATOR_CLI_PATH\n"
    "  --coordinator-db-path COORDINATOR_DB_PATH\n"
    "  --shell SHELL\n"
    "  --metadata-json METADATA_JSON\n"
)


def _remove_issue18_worktree_roots_delta(
    contract: dict[str, object],
) -> dict[str, object]:
    """Return a copy of *contract* with the Issue #18 sibling worktree-root
    parser delta removed from ``workspace host-profile set``.

    Issue #18 added the mutually exclusive ``--worktree-root`` (append) and
    ``--clear-worktree-roots`` actions and reworded the leaf help. Every
    historical rewind must strip this delta first so older baseline SHA proofs
    stay meaningful. No-op when the delta is already absent; fails closed on
    structural surprises.
    """
    historical = copy.deepcopy(contract)
    node = next(
        (
            candidate
            for candidate in historical["nodes"]
            if candidate["path"] == ["workspace", "host-profile", "set"]
        ),
        None,
    )
    if node is None:
        return historical
    delta_dests = {"worktree_roots", "clear_worktree_roots"}
    if not any(action.get("dest") in delta_dests for action in node["actions"]):
        return historical
    removed = [
        action
        for action in node["actions"]
        if action.get("dest") in delta_dests
    ]
    if sorted(action["dest"] for action in removed) != [
        "clear_worktree_roots",
        "worktree_roots",
    ]:
        raise AssertionError("unexpected Issue #18 worktree-roots parser delta")
    node["actions"] = [
        action
        for action in node["actions"]
        if action.get("dest") not in delta_dests
    ]
    node["help"] = _PRE_ISSUE18_HOST_PROFILE_SET_HELP
    return historical


def _remove_r3_mcp_serve_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the R3 streamable-HTTP serve delta
    removed, restoring the pre-R3 ``mcp``/``mcp serve``/root nodes.

    R3 added ``--host/--port/--path/--auth-file/--allowed-host/
    --allowed-origin`` to ``mcp serve``, widened the ``--transport`` choices
    and rewrote the serve/mcp/root help texts. Every historical rewind must
    strip this delta (it postdates the last committed fixture regeneration)
    before older baseline SHA proofs stay meaningful. No-op when the delta
    is already absent; fails closed on structural surprises.
    """
    historical = _remove_issue18_worktree_roots_delta(contract)
    serve_node = next(
        (node for node in historical["nodes"] if node["path"] == ["mcp", "serve"]),
        None,
    )
    if serve_node is None:
        # Pre-R1 tree (no mcp command at all): nothing to strip.
        return historical
    r3_dests = {
        "host", "port", "path", "auth_file", "allowed_host", "allowed_origin",
    }
    if not any(action.get("dest") in r3_dests for action in serve_node["actions"]):
        return historical

    found: set[str] = set()
    for node in historical["nodes"]:
        if node["path"] == ["mcp", "serve"]:
            for action in node["actions"]:
                if action.get("dest") in r3_dests:
                    found.add(action["dest"])
            node["actions"] = [
                action
                for action in node["actions"]
                if action.get("dest") not in r3_dests
            ]
            for action in node["actions"]:
                if action.get("dest") == "transport":
                    if action.get("choices") != ["stdio", "streamable-http"]:
                        raise AssertionError("unexpected R3 transport choices delta")
                    action["choices"] = ["stdio"]
                    action["help"] = _PRE_R3_MCP_SERVE_TRANSPORT_HELP
                elif action.get("dest") == "actor":
                    action["help"] = _PRE_R3_MCP_SERVE_ACTOR_HELP
            node["help"] = _PRE_R3_MCP_SERVE_HELP
            found.add("serve-node")
        elif node["path"] == ["mcp"]:
            node["help"] = _PRE_R3_MCP_HELP
            found.add("mcp-node")
        elif node["path"] == []:
            node["help"] = _PRE_R3_ROOT_HELP
            found.add("root-node")
    if found != {
        "serve-node", "mcp-node", "root-node",
        "host", "port", "path", "auth_file", "allowed_host", "allowed_origin",
    }:
        raise AssertionError(f"incomplete R3 mcp-serve CLI delta: {sorted(found)}")
    return historical


def _remove_r5b_mark_done_mcp_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the R5B Remote MCP transport delta
    removed from ``assignment mark-done-files``.

    R5B added ``--event-mcp-url``/``--event-mcp-token-env`` and reworded the
    ``--event-cli-path`` help. Every historical rewind must strip this delta
    first so pre-R5B baseline SHA proofs stay meaningful. No-op when the
    delta is already absent; fails closed on structural surprises.
    """
    historical = _remove_r3_mcp_serve_delta(contract)
    node = next(
        (
            candidate
            for candidate in historical["nodes"]
            if candidate["path"] == ["assignment", "mark-done-files"]
        ),
        None,
    )
    if node is None:
        return historical
    r5b_dests = {"event_mcp_url", "event_mcp_token_env"}
    if not any(action.get("dest") in r5b_dests for action in node["actions"]):
        return historical
    removed = [action for action in node["actions"] if action.get("dest") in r5b_dests]
    if sorted(action["dest"] for action in removed) != ["event_mcp_token_env", "event_mcp_url"]:
        raise AssertionError("unexpected R5B mark-done-files parser delta")
    for action in removed:
        expected = (
            ["--event-mcp-url"] if action["dest"] == "event_mcp_url"
            else ["--event-mcp-token-env"]
        )
        if action.get("option_strings") != expected:
            raise AssertionError(f"unexpected {action['dest']} option strings")
    node["actions"] = [
        action for action in node["actions"] if action.get("dest") not in r5b_dests
    ]
    for action in node["actions"]:
        if action.get("dest") == "event_cli_path":
            action["help"] = _PRE_R5B_EVENT_CLI_PATH_HELP
    node["help"] = _PRE_R5B_MARK_DONE_FILES_HELP
    # The parent ``assignment`` node help carries the mark-done-files
    # description line; restore the pre-R5B text.
    for parent in historical["nodes"]:
        if parent["path"] == ["assignment"]:
            parent["help"] = _PRE_R5B_ASSIGNMENT_HELP
    return historical


# Pre-Issue-#12 help for the ``runtime`` node, captured from the committed
# fixture before the usage leaves were added. Every historical rewind restores
# this exact string after stripping the ``runtime usage`` subtree.
_OLD_RUNTIME_USAGE_HELP = (
    "usage: coordinate runtime [-h] {agent,request,job,executor,capacity} ...\n"
    "\n"
    "positional arguments:\n"
    "  {agent,request,job,executor,capacity}\n"
    "    agent               Register or heartbeat a runtime client\n"
    "    request             Submit a bridge request and create a pending agent job\n"
    "    job                 Claim or report runtime jobs\n"
    "    executor            Sync and inspect the executor identity catalog\n"
    "    capacity            Sync and inspect the capacity catalog\n"
    "\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
)


# Pre-adoption ``task`` node help (extracted from the committed fixture before
# the ``task adopt`` / ``adopt-files`` / ``adopt-record`` leaves landed).
_PRE_ADOPT_TASK_HELP = (
    "usage: coordinate task [-h]\n"
    "                       {create,create-files,create-record,update-dependencies,update-dependencies-files,handoff}\n"
    "                       ...\n"
    "\n"
    "positional arguments:\n"
    "  {create,create-files,create-record,update-dependencies,update-dependencies-files,handoff}\n"
    "    create              Combined managed create: checklist file half first, DB record half second\n"
    "                        (idempotent; --operation-id to pin)\n"
    "    create-files        Coding-host half of host-aware task create: checklist file half only (no\n"
    "                        DB write)\n"
    "    create-record       Server half of host-aware task create: write DB task mirror + plan.ready\n"
    "                        only (no checklist write)\n"
    "    update-dependencies\n"
    "                        Combined managed dependency update: checklist file mutation first, state\n"
    "                        refresh + targeted reconcile second (idempotent)\n"
    "    update-dependencies-files\n"
    "                        Coding-host half of dependency update: canonical checklist file only (no\n"
    "                        DB write, no harnessctl preflight)\n"
    "    handoff             Generate a structured worker handoff\n"
    "\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
)

TASK_ADOPT_LEAVES = {
    "task adopt": "handle_task_adopt",
    "task adopt-files": "handle_task_adopt_files",
    "task adopt-record": "handle_task_adopt_record",
}


def _remove_task_adopt_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the legacy-adoption ``task`` delta removed.

    The adoption entry adds exactly three leaves (``task adopt``,
    ``task adopt-files``, ``task adopt-record``) and the matching choices on
    the ``task`` subparser. Every historical baseline rewind must strip this
    delta first so older baseline SHA proofs stay meaningful. No-op when the
    delta is already absent; fails closed on structural surprises.
    """
    historical = copy.deepcopy(contract)
    leaf_paths_to_remove = set(TASK_ADOPT_LEAVES)
    has_adopt = any(
        path in historical["leaf_paths"] for path in leaf_paths_to_remove
    )
    if not has_adopt:
        return historical

    historical["nodes"] = [
        node
        for node in historical["nodes"]
        if " ".join(node["path"]) not in leaf_paths_to_remove
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"] if path not in leaf_paths_to_remove
    ]
    historical["metadata"]["leaf_count"] = (
        int(historical["metadata"]["leaf_count"]) - len(leaf_paths_to_remove)
    )
    historical["metadata"]["node_count"] = (
        int(historical["metadata"]["node_count"]) - len(leaf_paths_to_remove)
    )

    found = set()
    for node in historical["nodes"]:
        if node["path"] != ["task"]:
            continue
        subparsers = [
            action
            for action in node["actions"]
            if action["action_class"] == "_SubParsersAction"
        ]
        if len(subparsers) != 1 or not set(
            path.split(" ", 1)[1] for path in leaf_paths_to_remove
        ).issubset(subparsers[0]["choices"]):
            raise AssertionError("unexpected task adopt CLI delta")
        subparsers[0]["choices"] = [
            choice
            for choice in subparsers[0]["choices"]
            if f"task {choice}" not in leaf_paths_to_remove
        ]
        node["help"] = _PRE_ADOPT_TASK_HELP
        found.add("task")
    if found != {"task"}:
        raise AssertionError(f"incomplete task adopt CLI delta: {sorted(found)}")
    return historical


def _remove_issue12_usage_leaves(
    contract: dict[str, object],
) -> dict[str, object]:
    """Return a copy of *contract* with the Issue #12 ``runtime usage`` subtree
    removed, restoring the pre-Issue-#12 ``runtime`` node.

    Issue #12 adds exactly one node (``runtime usage``), two leaves
    (``policy-set``, ``status``) and the ``usage`` choice on the runtime
    subparser. Every historical baseline rewind must strip this delta first so
    older baseline SHA proofs stay meaningful. No-op when the delta is already
    absent; fails closed on structural surprises. The later ``task adopt``
    delta is stripped first for the same reason.
    """
    historical = _remove_task_adopt_delta(copy.deepcopy(contract))
    leaf_paths_to_remove = set(P9_ISSUE12_USAGE_LEAVES)
    node_paths_to_remove = {tuple(p.split()) for p in leaf_paths_to_remove} | {
        ("runtime", "usage")
    }
    has_usage = any(
        tuple(node["path"]) == ("runtime", "usage") for node in historical["nodes"]
    )
    if not has_usage:
        return historical

    historical["nodes"] = [
        node
        for node in historical["nodes"]
        if tuple(node["path"]) not in node_paths_to_remove
    ]
    historical["leaf_paths"] = [
        path
        for path in historical["leaf_paths"]
        if path not in leaf_paths_to_remove
    ]
    historical["metadata"]["leaf_count"] = (
        int(historical["metadata"]["leaf_count"]) - len(leaf_paths_to_remove)
    )
    historical["metadata"]["node_count"] = (
        int(historical["metadata"]["node_count"]) - len(node_paths_to_remove)
    )

    found = set()
    for node in historical["nodes"]:
        if node["path"] != ["runtime"]:
            continue
        subparsers = [
            action
            for action in node["actions"]
            if action["action_class"] == "_SubParsersAction"
        ]
        if len(subparsers) != 1 or "usage" not in subparsers[0]["choices"]:
            raise AssertionError("unexpected runtime usage CLI delta")
        subparsers[0]["choices"] = [
            choice for choice in subparsers[0]["choices"] if choice != "usage"
        ]
        node["help"] = _OLD_RUNTIME_USAGE_HELP
        found.add("runtime")
    if found != {"runtime"}:
        raise AssertionError(f"incomplete issue-12 usage CLI delta: {sorted(found)}")
    return historical


def _remove_mcp_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the R1 ``mcp`` command delta removed.

    R1 adds exactly one top-level command (``mcp``), one leaf (``mcp serve``)
    and two nodes (``mcp``, ``mcp serve``). Every historical baseline rewind
    must strip this delta first so pre-R1 SHA proofs stay meaningful. No-op
    when the delta is already absent. The post-R1 R5B and R3 CLI deltas are
    stripped first so pre-R1 rewinds stay free of them too.
    """
    historical = _remove_issue12_usage_leaves(contract)
    historical = _remove_r5b_mark_done_mcp_delta(historical)
    historical = _remove_update_dependencies_delta(historical)
    historical = _strip_runtime_http_nodes(historical)
    has_mcp = any(node["path"] == ["mcp"] for node in historical["nodes"])
    if not has_mcp:
        return historical

    historical["nodes"] = [
        node
        for node in historical["nodes"]
        if node["path"] not in (["mcp"], ["mcp", "serve"])
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"] if path != "mcp serve"
    ]
    historical["metadata"]["leaf_count"] = int(
        historical["metadata"]["leaf_count"]
    ) - 1
    historical["metadata"]["node_count"] = int(
        historical["metadata"]["node_count"]
    ) - 2
    historical["metadata"]["top_level_commands"] = [
        name
        for name in historical["metadata"]["top_level_commands"]
        if name != "mcp"
    ]

    found = set()
    for node in historical["nodes"]:
        if not node["path"]:
            subparsers = [
                action
                for action in node["actions"]
                if action["action_class"] == "_SubParsersAction"
            ]
            if len(subparsers) != 1 or "mcp" not in subparsers[0]["choices"]:
                raise AssertionError("unexpected root subparser delta")
            subparsers[0]["choices"] = [
                choice for choice in subparsers[0]["choices"] if choice != "mcp"
            ]
            node["help"] = _restore_pre_r1_root_help(node["help"])
            found.add("root")
    if found != {"root"}:
        raise AssertionError(f"incomplete mcp CLI delta: {sorted(found)}")
    return historical


def _remove_targeted_reconcile_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the targeted-reconcile ``--task-id``
    parser delta removed, restoring the pre-targeted baseline help.

    No-op only when the reconcile node exists without the ``task_id`` action;
    fails closed on structural surprises (missing or multiple reconcile
    nodes, unexpected task_id action).
    """
    historical = _remove_mcp_delta(contract)
    matches = [node for node in historical["nodes"] if node["path"] == ["reconcile"]]
    if not matches:
        raise AssertionError("missing reconcile parser node; cannot strip targeted delta")
    if len(matches) != 1:
        raise AssertionError("unexpected reconcile parser node cardinality")
    node = matches[0]
    removed = [action for action in node["actions"] if action.get("dest") == "task_id"]
    if not removed:
        return historical
    if len(removed) != 1 or removed[0].get("option_strings") != ["--task-id"]:
        raise AssertionError("unexpected targeted reconcile parser delta")
    node["actions"] = [
        action for action in node["actions"] if action.get("dest") != "task_id"
    ]
    node["help"] = _PRE_TARGETED_RECONCILE_HELP
    return historical


# Pre-plan-revise ``plan`` parent help, extracted from the committed fixture
# that predates the ``plan revise`` leaf.  Restoring it keeps historical
# rewind SHA proofs free of the later CLI addition.
_PRE_PLAN_REVISE_PLAN_HELP = (
    "usage: coordinate plan [-h] {review-request,approve,reject} ...\n\n"
    "positional arguments:\n"
    "  {review-request,approve,reject}\n"
    "    review-request      Request plan review\n"
    "    approve             Approve a plan\n"
    "    reject              Reject a plan\n\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
)


def _remove_plan_revise_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the ``plan revise`` parser delta removed.

    Restores the pre-revision ``plan`` subparser choices/help and the leaf/node
    counts so historical baseline rewinds are not polluted by the later CLI
    addition.  No-op when the delta is already absent.
    """
    historical = _remove_mcp_delta(contract)
    revise_path = ["plan", "revise"]
    has_revise = any(node["path"] == revise_path for node in historical["nodes"])
    if not has_revise:
        return historical

    historical["nodes"] = [
        node for node in historical["nodes"] if node["path"] != revise_path
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"] if path != "plan revise"
    ]
    historical["metadata"]["leaf_count"] = int(
        historical["metadata"]["leaf_count"]
    ) - 1
    historical["metadata"]["node_count"] = int(
        historical["metadata"]["node_count"]
    ) - 1

    found = set()
    for node in historical["nodes"]:
        if node["path"] == ["plan"]:
            subparsers = [
                action
                for action in node["actions"]
                if action["action_class"] == "_SubParsersAction"
            ]
            if len(subparsers) != 1 or "revise" not in subparsers[0]["choices"]:
                raise AssertionError("unexpected plan subparser delta")
            subparsers[0]["choices"] = [
                choice for choice in subparsers[0]["choices"] if choice != "revise"
            ]
            node["help"] = _PRE_PLAN_REVISE_PLAN_HELP
            found.add("plan")
    if found != {"plan"}:
        raise AssertionError(f"incomplete plan revise CLI delta: {sorted(found)}")
    return historical


def _serialize_contract(contract: dict[str, object]) -> bytes:
    """Serialize a normalized contract dict using the canonical fixture format."""
    return json.dumps(contract, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8") + b"\n"


def _rewrite_contract_to_p9_3c1_p1_baseline(
    contract: dict[str, object],
) -> dict[str, object]:
    """Remove only the P9-3C1 P1 parser delta from a generated contract.

    Any later delta (targeted-reconcile ``--task-id``, ``plan revise``, R1
    ``mcp``) is stripped first so historical baseline rewinds stay free of
    post-P9-3C1 CLI additions. This function is the shared entry of every
    cumulative rewind chain; ``_remove_targeted_reconcile_delta`` already
    strips the R1 ``mcp`` delta, so it is not repeated here.
    """
    historical = _remove_targeted_reconcile_delta(contract)
    historical = _remove_plan_revise_delta(historical)
    deactivate_path = ["runtime", "agent", "deactivate"]
    has_deactivate = any(node["path"] == deactivate_path for node in historical["nodes"])
    if not has_deactivate:
        return historical

    historical["nodes"] = [
        node for node in historical["nodes"] if node["path"] != deactivate_path
    ]
    historical["leaf_paths"] = [
        path
        for path in historical["leaf_paths"]
        if path != "runtime agent deactivate"
    ]
    historical["metadata"]["leaf_count"] = int(
        historical["metadata"]["leaf_count"]
    ) - 1
    historical["metadata"]["node_count"] = int(
        historical["metadata"]["node_count"]
    ) - 1

    found = set()
    for node in historical["nodes"]:
        path = node["path"]
        if path == ["runtime", "agent"]:
            subparsers = [
                action
                for action in node["actions"]
                if action["action_class"] == "_SubParsersAction"
            ]
            if len(subparsers) != 1 or subparsers[0]["choices"] != [
                "register",
                "heartbeat",
                "deactivate",
            ]:
                raise AssertionError("unexpected runtime agent P1 parser delta")
            subparsers[0]["choices"] = ["register", "heartbeat"]
            node["help"] = _P9_3C1_P1_AGENT_HELP
            found.add("agent")
        elif path == ["runtime", "job", "claim"]:
            p1_actions = [
                action
                for action in node["actions"]
                if action.get("dest") in {"reap_mode", "reap_reason"}
            ]
            if {action["dest"] for action in p1_actions} != {
                "reap_mode",
                "reap_reason",
            }:
                raise AssertionError("unexpected runtime claim P1 parser delta")
            node["actions"] = [
                action
                for action in node["actions"]
                if action.get("dest") not in {"reap_mode", "reap_reason"}
            ]
            node["help"] = _P9_3C1_P1_CLAIM_HELP
            found.add("claim")
        elif path == ["runtime", "job", "lease", "reap"]:
            actions = {action.get("dest"): action for action in node["actions"]}
            if set(actions) < {"help", "actor", "batch_size", "lease_id", "job_id"}:
                raise AssertionError("unexpected runtime lease reap P1 parser delta")
            actions["batch_size"]["default"] = 100
            node["actions"] = [
                action
                for action in node["actions"]
                if action.get("dest") not in {"lease_id", "job_id"}
            ]
            node["help"] = _P9_3C1_P1_REAP_HELP
            found.add("reap")
    if found != {"agent", "claim", "reap"}:
        raise AssertionError(f"incomplete P1 CLI delta: {sorted(found)}")
    return historical


# SHA-256 of the P9-2B pre-routing fixture (before runtime request submit gained routed flags).
_P9_2B_BASELINE_FIXTURE_SHA256 = (
    "4b11a5c25f1ac30d395cc5777f6a766ae0f5b16369676420181515f612dddc62"
)

# SHA-256 of the reviewed pre-P9-3C0 fixture after masking the request-submit
# help text.  Masking isolates the new action structure without coupling the
# proof to argparse line wrapping.
_P9_3C0_WORKTREE_PATH_BASELINE_FIXTURE_SHA256 = (
    "1f6a8784fcea3baf9749c856ad40eff2ad183bc6b092db30646c05d1542577fc"
)

# SHA-256 of the committed pre-Issue-#18 ``workspace host-profile set`` node
# (the reviewed baseline before the sibling worktree-root flags landed).
_ISSUE18_PRE_WORKTREE_ROOTS_SET_NODE_SHA256 = (
    "e73603750a508975d368e9b275e96dc0925775bc8f9a28f9a2e900eb4cc3b3e6"
)

# P9-2A added exactly these 3 executor leaves under ``runtime executor``.
P9_2A_EXECUTOR_LEAVES = {
    "runtime executor sync": "handle_runtime_executor_sync",
    "runtime executor list": "handle_runtime_executor_list",
    "runtime executor show": "handle_runtime_executor_show",
}


_OLD_RUNTIME_REQUEST_SUBMIT_HELP = (
    "usage: coordinate runtime request submit [-h] --target-agent TARGET_AGENT --prompt PROMPT\n"
    "                                         --origin-json ORIGIN_JSON --reply-json REPLY_JSON\n"
    "                                         [--task-id TASK_ID] [--actor ACTOR]\n"
    "                                         [--idempotency-key IDEMPOTENCY_KEY]\n"
    "                                         workspace_id\n"
    "\n"
    "positional arguments:\n"
    "  workspace_id\n"
    "\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
    "  --target-agent TARGET_AGENT\n"
    "  --prompt PROMPT\n"
    "  --origin-json ORIGIN_JSON\n"
    "  --reply-json REPLY_JSON\n"
    "  --task-id TASK_ID\n"
    "  --actor ACTOR\n"
    "  --idempotency-key IDEMPOTENCY_KEY\n"
)


def _rewrite_contract_to_p9_2b_baseline(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the P9-2B routed flags removed.

    Restores the pre-P9-2B ``runtime request submit`` help and removes every
    later action on that leaf (the five routed-mode actions plus the P9-3C
    exact-request ``worktree_path`` authority input). This is the first step in
    any cumulative rewind that goes earlier than P9-2B.
    """
    historical = _rewrite_contract_to_p9_3c1_p1_baseline(contract)
    historical = _remove_p9_3b_lease_leaves(
        _remove_p9_3a_capacity_leaves(historical)
    )
    post_p9_2b_dests = {
        "route_capabilities",
        "route_definition",
        "preferred_host",
        "override_agent",
        "override_reason",
        "worktree_path",
    }

    for node in historical["nodes"]:
        if node["path"] == ["runtime", "request", "submit"]:
            node["actions"] = [
                action
                for action in node["actions"]
                if action.get("dest") not in post_p9_2b_dests
            ]
            node["help"] = _OLD_RUNTIME_REQUEST_SUBMIT_HELP
            for action in node["actions"]:
                if action.get("dest") == "target_agent":
                    action["required"] = True
            break

    return historical


def _mask_p9_3c0_worktree_path_delta(contract: dict[str, object]) -> dict[str, object]:
    """Remove the P9-3C0 action and mask only its argparse help reflow."""
    historical = _rewrite_contract_to_p9_3c1_p1_baseline(contract)
    for node in historical["nodes"]:
        if node["path"] == ["runtime", "request", "submit"]:
            node["actions"] = [
                action
                for action in node["actions"]
                if action.get("dest") != "worktree_path"
            ]
            node["help"] = "<P9-3C0-RUNTIME-REQUEST-SUBMIT-HELP>"
            break
    return historical


def _remove_p9_2a_executor_leaves(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the P9-2A ``runtime executor`` subtree removed.

    Restores the pre-P9-2A runtime help string so historical baseline proofs
    keep their meaning.
    """
    historical = _remove_mcp_delta(contract)
    leaf_paths_to_remove = set(P9_2A_EXECUTOR_LEAVES.keys())
    node_paths_to_remove = {tuple(p.split()) for p in leaf_paths_to_remove} | {("runtime", "executor")}

    historical["nodes"] = [
        node for node in historical["nodes"]
        if tuple(node["path"]) not in node_paths_to_remove
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"]
        if path not in leaf_paths_to_remove
    ]
    historical["metadata"]["leaf_count"] = int(historical["metadata"]["leaf_count"]) - len(leaf_paths_to_remove)
    historical["metadata"]["node_count"] = int(historical["metadata"]["node_count"]) - len(node_paths_to_remove)

    for node in historical["nodes"]:
        if node["path"] == ["runtime"]:
            for action in node["actions"]:
                if action["action_class"] == "_SubParsersAction":
                    action["choices"] = [c for c in action["choices"] if c != "executor"]
            node["help"] = _restore_pre_p9_2a_runtime_help(node["help"])

    return historical


def _restore_pre_p9_2a_runtime_help(help_text: str) -> str:
    """Rebuild the pre-P9-2A runtime help without the executor subcommand.

    argparse aligns description columns to the longest subcommand name, so
    simply editing the multi-subcommand help string leaves one extra space of
    indentation.  We parse the descriptions from the current help and reformat
    them with the pre-P9-2A column width (description starts at column 23).
    """
    import re

    # Extract descriptions from the current four-subcommand help.
    descriptions: dict[str, str] = {}
    for line in help_text.splitlines():
        m = re.match(r"^    (agent|request|job|executor) +(\S.*)$", line)
        if m:
            descriptions[m.group(1)] = m.group(2)

    lines = help_text.splitlines()
    result: list[str] = []
    for line in lines:
        if "{agent,request,job,executor}" in line:
            result.append(line.replace("{agent,request,job,executor}", "{agent,request,job}"))
            continue
        if re.match(r"^    executor +\S", line):
            continue
        result.append(line)

    # Reformat description lines to the pre-P9-2A column width.
    formatted: list[str] = []
    for line in result:
        m = re.match(r"^(    (agent|request|job))  +(\S.*)$", line)
        if m:
            prefix = m.group(1)
            desc = m.group(3)
            padding = " " * (23 - len(prefix))
            formatted.append(f"{prefix}{padding}{desc}")
            continue
        m = re.match(r"^(  -h, --help)  +(\S.*)$", line)
        if m:
            prefix = m.group(1)
            desc = m.group(2)
            padding = " " * (23 - len(prefix))
            formatted.append(f"{prefix}{padding}{desc}")
            continue
        formatted.append(line)

    return "\n".join(formatted) + "\n"


def _restore_pre_p9_3a_runtime_help(help_text: str) -> str:
    """Rebuild the pre-P9-3A runtime help without the capacity subcommand.

    argparse aligns description columns to the longest subcommand name, so
    we parse the descriptions from the current help and reformat them to the
    column width implied by the remaining subcommands (executor present -> 24,
    otherwise -> 23).
    """
    import re

    # Extract descriptions for all non-capacity subcommands.
    descriptions: dict[str, str] = {}
    for line in help_text.splitlines():
        m = re.match(r"^    (agent|request|job|executor|capacity) +(\S.*)$", line)
        if m and m.group(1) != "capacity":
            descriptions[m.group(1)] = m.group(2)

    if "capacity" not in help_text:
        return help_text

    remaining = sorted(descriptions.keys())
    max_len = max(len(s) for s in remaining) if remaining else 0
    start_col = 24 if max_len >= 8 else 23

    lines = help_text.splitlines()
    result: list[str] = []
    for line in lines:
        if "{agent,request,job,executor,capacity}" in line:
            result.append(line.replace("{agent,request,job,executor,capacity}", "{agent,request,job,executor}"))
            continue
        if re.match(r"^    capacity +\S", line):
            continue
        result.append(line)

    subcmd_re = "|".join(re.escape(s) for s in remaining)
    formatted: list[str] = []
    for line in result:
        m = re.match(rf"^(    ({subcmd_re}))  +(\S.*)$", line)
        if m:
            prefix = m.group(1)
            desc = m.group(3)
            padding = " " * (start_col - len(prefix))
            formatted.append(f"{prefix}{padding}{desc}")
            continue
        m = re.match(r"^(  -h, --help)  +(\S.*)$", line)
        if m:
            prefix = m.group(1)
            desc = m.group(2)
            padding = " " * (start_col - len(prefix))
            formatted.append(f"{prefix}{padding}{desc}")
            continue
        formatted.append(line)

    return "\n".join(formatted) + "\n"


def _remove_p9_3a_capacity_leaves(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the P9-3A ``runtime capacity`` subtree removed."""
    historical = _remove_mcp_delta(contract)
    leaf_paths_to_remove = set(P9_3A_CAPACITY_LEAVES)
    node_paths_to_remove = {tuple(p.split()) for p in leaf_paths_to_remove} | {("runtime", "capacity")}

    historical["nodes"] = [
        node for node in historical["nodes"]
        if tuple(node["path"]) not in node_paths_to_remove
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"]
        if path not in leaf_paths_to_remove
    ]
    historical["metadata"]["leaf_count"] = int(historical["metadata"]["leaf_count"]) - len(leaf_paths_to_remove)
    historical["metadata"]["node_count"] = int(historical["metadata"]["node_count"]) - len(node_paths_to_remove)

    for node in historical["nodes"]:
        if node["path"] == ["runtime"]:
            for action in node["actions"]:
                if action["action_class"] == "_SubParsersAction":
                    action["choices"] = [c for c in action["choices"] if c != "capacity"]
            node["help"] = _restore_pre_p9_3a_runtime_help(node["help"])

    return historical


# Pre-P9-3B help strings for the runtime job subtree, extracted from the HEAD
# fixture before P9-3B lease leaves were added. Using the historical strings
# lets the rewind helper restore the exact pre-P9-3B contract bytes.
_OLD_RUNTIME_JOB_HELP = (
    """\
usage: coordinate runtime job [-h] {claim,report,progress} ...

positional arguments:
  {claim,report,progress}
    claim               Claim the next pending job for an agent; returns claimed=false when the
                        queue is empty
    report              Report a terminal or recoverable timeout job status with a structured
                        result payload
    progress            Record a bounded progress checkpoint for a running runtime job

options:
  -h, --help            show this help message and exit
"""
)

_OLD_RUNTIME_JOB_REPORT_HELP = (
    """\
usage: coordinate runtime job report [-h] --agent-id AGENT_ID --status {done,failed,timed_out}
                                     --result-json RESULT_JSON [--actor ACTOR]
                                     [--attempt-token ATTEMPT_TOKEN]
                                     job_id

positional arguments:
  job_id

options:
  -h, --help            show this help message and exit
  --agent-id AGENT_ID
  --status {done,failed,timed_out}
  --result-json RESULT_JSON
  --actor ACTOR
  --attempt-token ATTEMPT_TOKEN
                        Current attempt_count from claim; rejects stale attempts (8.4.3 P1 #2)
"""
)

_OLD_RUNTIME_JOB_PROGRESS_HELP = (
    """\
usage: coordinate runtime job progress [-h] --agent-id AGENT_ID [--stage STAGE]
                                       [--summary SUMMARY] [--session-id SESSION_ID]
                                       [--actor ACTOR] [--attempt-token ATTEMPT_TOKEN]
                                       job_id

positional arguments:
  job_id

options:
  -h, --help            show this help message and exit
  --agent-id AGENT_ID
  --stage STAGE
  --summary SUMMARY
  --session-id SESSION_ID
  --actor ACTOR
  --attempt-token ATTEMPT_TOKEN
                        Current attempt_count from claim; rejects stale attempts (8.4.3 P1 #2)
"""
)

_OLD_RUNTIME_JOB_CLAIM_HELP = (
    """\
usage: coordinate runtime job claim [-h] --agent-id AGENT_ID [--recoverable]

options:
  -h, --help           show this help message and exit
  --agent-id AGENT_ID
  --recoverable        Also claim recoverable timed_out jobs (explicit recovery path). Default:
                       only pending.
"""
)


def _restore_pre_p9_3b_runtime_job_help(help_text: str) -> str:
    """Rebuild the pre-P9-3B ``runtime job`` help without the lease subcommand.

    The runtime job descriptions are aligned to the longest subcommand name, so
    simply dropping the lease line leaves one extra space of indentation. We
    return the captured pre-P9-3B help string directly because it is already
    normalized for the historical formatter output.
    """
    return _OLD_RUNTIME_JOB_HELP


def _remove_p9_3b_lease_leaves(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the P9-3B ``runtime job lease`` subtree removed.

    Removes the two new lease leaves, the ``lease`` subparser node, the
    ``--lease-id`` action on ``runtime job report``/``progress``, the
    ``--recovery-reason``/``--prior-process-stopped`` actions on
    ``runtime job claim`` (added by the P9-3B recovery-evidence correction),
    and restores the captured pre-P9-3B help strings so historical baseline
    proofs keep their meaning.
    """
    historical = _remove_mcp_delta(contract)
    leaf_paths_to_remove = set(P9_3B_LEASE_LEAVES.keys())
    node_paths_to_remove = {tuple(p.split()) for p in leaf_paths_to_remove} | {("runtime", "job", "lease")}

    historical["nodes"] = [
        node for node in historical["nodes"]
        if tuple(node["path"]) not in node_paths_to_remove
    ]
    historical["leaf_paths"] = [
        path for path in historical["leaf_paths"]
        if path not in leaf_paths_to_remove
    ]
    historical["metadata"]["leaf_count"] = int(historical["metadata"]["leaf_count"]) - len(leaf_paths_to_remove)
    historical["metadata"]["node_count"] = int(historical["metadata"]["node_count"]) - len(node_paths_to_remove)

    for node in historical["nodes"]:
        path = node["path"]
        if path == ["runtime", "job"]:
            for action in node["actions"]:
                if action["action_class"] == "_SubParsersAction":
                    action["choices"] = [c for c in action["choices"] if c != "lease"]
            node["help"] = _restore_pre_p9_3b_runtime_job_help(node["help"])
        elif path == ["runtime", "job", "report"]:
            node["actions"] = [
                action for action in node["actions"]
                if action.get("dest") != "lease_id"
            ]
            node["help"] = _OLD_RUNTIME_JOB_REPORT_HELP
        elif path == ["runtime", "job", "progress"]:
            node["actions"] = [
                action for action in node["actions"]
                if action.get("dest") != "lease_id"
            ]
            node["help"] = _OLD_RUNTIME_JOB_PROGRESS_HELP
        elif path == ["runtime", "job", "claim"]:
            node["actions"] = [
                action for action in node["actions"]
                if action.get("dest") not in ("recovery_reason", "prior_process_stopped")
            ]
            node["help"] = _OLD_RUNTIME_JOB_CLAIM_HELP

    return historical


def _rewrite_contract_to_s4c2_baseline(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with only S4-C2 parser changes rewound.

    Strips the new ``issue materialize-files`` options ``--workspace-id``,
    ``--operation-id`` and ``--event-id`` and the new ``issue materialize-record``
    options ``--operation-id``, ``--input-fingerprint``, ``--before-fingerprint``
    and ``--after-fingerprint``. Restores the captured post-C1 help strings.
    """
    historical = _remove_p9_2a_executor_leaves(_rewrite_contract_to_p9_2b_baseline(contract))

    for node in historical["nodes"]:
        path = node["path"]
        if path == ["issue", "materialize-files"]:
            node["actions"] = [
                action for action in node["actions"]
                if action.get("dest") not in {"workspace_id", "operation_id", "event_id"}
            ]
            node["help"] = _OLD_ISSUE_MATERIALIZE_FILES_HELP
        elif path == ["issue", "materialize-record"]:
            node["actions"] = [
                action for action in node["actions"]
                if action.get("dest") not in {
                    "operation_id",
                    "input_fingerprint",
                    "before_fingerprint",
                    "after_fingerprint",
                }
            ]
            node["help"] = _OLD_ISSUE_MATERIALIZE_RECORD_HELP

    return historical


def _rewrite_contract_to_s4d_baseline(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with only the S4-D parser change rewound.

    Strips the ``--no-projections`` flag from ``workspace doctor`` so the
    C2-to-D delta proof is independent of Git topology and fixture generation.
    """
    historical = _remove_mcp_delta(contract)

    for node in historical["nodes"]:
        if node["path"] == ["workspace", "doctor"]:
            node["actions"] = [
                action for action in node["actions"]
                if action.get("dest") != "no_projections"
            ]

    return historical


class CLIContractTests(unittest.TestCase):
    """Tests for the deterministic CLI contract snapshot."""

    def test_fixture_exists(self) -> None:
        self.assertTrue(FIXTURE_PATH.exists(), "Committed fixture is missing")

    def test_contract_counts_match_plan(self) -> None:
        contract = _build_contract()
        metadata = contract["metadata"]
        self.assertEqual(len(metadata["top_level_commands"]), 24)
        self.assertEqual(metadata["leaf_count"], 101)
        self.assertEqual(metadata["node_count"], 133)
        self.assertEqual(len(contract["leaf_paths"]), 101)
        self.assertEqual(len(contract["nodes"]), 133)

    def test_task_adopt_delta_rewind_is_idempotent(self) -> None:
        """Stripping the adoption delta twice is a no-op the second time."""
        contract = _build_contract()
        once = _remove_task_adopt_delta(contract)
        twice = _remove_task_adopt_delta(once)
        self.assertEqual(twice["metadata"], once["metadata"])
        self.assertEqual(twice["leaf_paths"], once["leaf_paths"])
        self.assertEqual(
            [n["path"] for n in twice["nodes"]], [n["path"] for n in once["nodes"]]
        )

    def test_task_adopt_delta_rewind_fails_closed_on_malformed_structure(self) -> None:
        """A task leaf without the matching subparser choice fails closed."""
        contract = _build_contract()
        task_node = next(n for n in contract["nodes"] if n["path"] == ["task"])
        subparsers = [
            a for a in task_node["actions"] if a.get("action_class") == "_SubParsersAction"
        ]
        # Simulate structural corruption: the leaf exists but the subparser
        # choices were tampered with.
        subparsers[0]["choices"] = ["create"]
        with self.assertRaises(AssertionError):
            _remove_task_adopt_delta(contract)

    def test_task_adopt_delta_exactly(self) -> None:
        contract = _build_contract()
        for path, handler in TASK_ADOPT_LEAVES.items():
            self.assertEqual(contract["leaf_paths"].count(path), 1)
            node = next(
                node for node in contract["nodes"] if " ".join(node["path"]) == path
            )
            self.assertEqual(
                node["defaults"]["handler"], f"coordinate.planning_cli.{handler}"
            )
        # Rewinding the delta restores the pre-adoption task tree exactly.
        historical = _remove_task_adopt_delta(contract)
        self.assertEqual(
            historical["metadata"]["leaf_count"],
            int(contract["metadata"]["leaf_count"]) - 3,
        )
        for path in TASK_ADOPT_LEAVES:
            self.assertNotIn(path, historical["leaf_paths"])
        task_node = next(
            node for node in historical["nodes"] if node["path"] == ["task"]
        )
        self.assertEqual(task_node["help"], _PRE_ADOPT_TASK_HELP)
        subparsers = [
            action
            for action in task_node["actions"]
            if action["action_class"] == "_SubParsersAction"
        ]
        self.assertEqual(
            subparsers[0]["choices"],
            [
                "create",
                "create-files",
                "create-record",
                "update-dependencies",
                "update-dependencies-files",
                "handoff",
            ],
        )

    def test_contract_issue12_usage_delta_matches_baseline(self) -> None:
        """Issue #12 adds exactly ``runtime usage policy-set`` and
        ``runtime usage status`` under a new ``runtime usage`` node."""
        contract = _build_contract()
        for path, handler in P9_ISSUE12_USAGE_LEAVES.items():
            self.assertEqual(contract["leaf_paths"].count(path), 1)
            node = next(
                node for node in contract["nodes"] if " ".join(node["path"]) == path
            )
            self.assertEqual(
                node["defaults"]["handler"], f"coordinate.usage_cli.{handler}"
            )
        usage_nodes = [
            node for node in contract["nodes"] if node["path"][:2] == ["runtime", "usage"]
        ]
        self.assertEqual(
            [node["path"] for node in usage_nodes],
            [
                ["runtime", "usage"],
                ["runtime", "usage", "policy-set"],
                ["runtime", "usage", "status"],
            ],
        )
        # Rewinding the delta restores the pre-Issue-#12 tree exactly.
        historical = _remove_issue12_usage_leaves(contract)
        self.assertEqual(historical["metadata"]["leaf_count"], 96)
        self.assertEqual(historical["metadata"]["node_count"], 127)
        self.assertNotIn("runtime usage", historical["leaf_paths"])
        self.assertNotIn("runtime usage policy-set", historical["leaf_paths"])
        self.assertNotIn("runtime usage status", historical["leaf_paths"])
        runtime = next(
            node for node in historical["nodes"] if node["path"] == ["runtime"]
        )
        self.assertEqual(runtime["help"], _OLD_RUNTIME_USAGE_HELP)

    def test_r2a_runtime_http_serve_delta_exactly(self) -> None:
        """R2A adds exactly ``runtime-http serve`` and nothing else."""
        contract = _build_contract()
        self.assertEqual(contract["leaf_paths"].count("runtime-http serve"), 1)
        self.assertNotIn("runtime-http", contract["leaf_paths"])
        rt_nodes = [
            node for node in contract["nodes"] if node["path"][:1] == ["runtime-http"]
        ]
        self.assertEqual(
            [node["path"] for node in rt_nodes],
            [["runtime-http"], ["runtime-http", "serve"]],
        )
        serve = next(
            node for node in rt_nodes if node["path"] == ["runtime-http", "serve"]
        )
        self.assertEqual(
            serve["defaults"]["handler"],
            "coordinate.runtime_http_cli.handle_runtime_http_serve",
        )
        host_action = next(
            action
            for action in serve["actions"]
            if action.get("dest") == "host"
        )
        self.assertEqual(host_action["default"], "127.0.0.1")
        auth_action = next(
            action for action in serve["actions"] if action.get("dest") == "auth_file"
        )
        self.assertTrue(auth_action["required"])
        rt_parent = next(node for node in rt_nodes if node["path"] == ["runtime-http"])
        subparsers = [
            action
            for action in rt_parent["actions"]
            if action["action_class"] == "_SubParsersAction"
        ]
        self.assertEqual(len(subparsers), 1)
        self.assertEqual(subparsers[0]["choices"], ["serve"])
        self.assertIn("runtime-http", contract["metadata"]["top_level_commands"])
        # Rewinding the R2A delta must restore the pre-R2A tree exactly.
        historical = _remove_runtime_http_delta(contract)
        self.assertEqual(
            len(historical["metadata"]["top_level_commands"]), 22
        )
        self.assertEqual(historical["metadata"]["leaf_count"], 93)
        self.assertEqual(historical["metadata"]["node_count"], 122)
        self.assertNotIn("runtime-http", historical["metadata"]["top_level_commands"])
        self.assertNotIn("runtime-http serve", historical["leaf_paths"])

    def test_r1_mcp_serve_delta_exactly(self) -> None:
        """R1 adds exactly ``mcp serve`` and nothing else to the CLI tree."""
        contract = _build_contract()
        self.assertEqual(contract["leaf_paths"].count("mcp serve"), 1)
        self.assertNotIn("mcp", contract["leaf_paths"])
        mcp_nodes = [
            node for node in contract["nodes"] if node["path"][:1] == ["mcp"]
        ]
        self.assertEqual(
            [node["path"] for node in mcp_nodes], [["mcp"], ["mcp", "serve"]]
        )
        serve = next(
            node for node in mcp_nodes if node["path"] == ["mcp", "serve"]
        )
        self.assertEqual(
            serve["defaults"]["handler"],
            "coordinate.mcp_cli.handle_mcp_serve",
        )
        transport_action = next(
            action
            for action in serve["actions"]
            if action.get("dest") == "transport"
        )
        self.assertEqual(transport_action["default"], "stdio")
        # R3 extends the same R1 leaf with the loopback streamable-http
        # profile; stdio remains the default and the remote flags stay on the
        # same leaf (no new nodes/leaves).
        self.assertEqual(
            transport_action["choices"], ["stdio", "streamable-http"]
        )
        for flag in ("--host", "--port", "--path", "--auth-file",
                     "--allowed-host", "--allowed-origin"):
            self.assertIn(flag, [action.get("option_strings", [None])[0] for action in serve["actions"]])
        mcp_parent = next(node for node in mcp_nodes if node["path"] == ["mcp"])
        subparsers = [
            action
            for action in mcp_parent["actions"]
            if action["action_class"] == "_SubParsersAction"
        ]
        self.assertEqual(len(subparsers), 1)
        self.assertEqual(subparsers[0]["choices"], ["serve"])
        self.assertIn("mcp", contract["metadata"]["top_level_commands"])
        # Rewinding the R1 delta must restore the pre-R1 tree exactly.
        historical = _remove_mcp_delta(contract)
        self.assertEqual(
            len(historical["metadata"]["top_level_commands"]), 21
        )
        self.assertEqual(historical["metadata"]["leaf_count"], 90)
        self.assertEqual(historical["metadata"]["node_count"], 118)
        self.assertNotIn("mcp", historical["metadata"]["top_level_commands"])
        self.assertNotIn("mcp serve", historical["leaf_paths"])

    def test_plan_revise_present_exactly_once(self) -> None:
        contract = _build_contract()
        revise_nodes = [
            node for node in contract["nodes"] if node["path"] == ["plan", "revise"]
        ]
        self.assertEqual(len(revise_nodes), 1)
        self.assertEqual(
            revise_nodes[0]["defaults"]["handler"],
            "coordinate.planning_cli.handle_plan_revise",
        )
        self.assertEqual(contract["leaf_paths"].count("plan revise"), 1)

    def test_no_duplicate_leaf_paths(self) -> None:
        contract = _build_contract()
        self.assertEqual(len(contract["leaf_paths"]), len(set(contract["leaf_paths"])))

    def test_at_most_one_subparser_action_per_node(self) -> None:
        contract = _build_contract()
        for node in contract["nodes"]:
            subparser_count = sum(
                1 for action in node["actions"] if action["action_class"] == "_SubParsersAction"
            )
            self.assertLessEqual(subparser_count, 1, f"Node {' '.join(node['path']) or 'root'} has multiple subparser actions")

    def test_every_leaf_has_exactly_one_handler(self) -> None:
        contract = _build_contract()
        for node in contract["nodes"]:
            if not node["path"]:
                continue
            leaf = not any(
                action["action_class"] == "_SubParsersAction" for action in node["actions"]
            )
            if not leaf:
                continue
            handler_default = node["defaults"].get("handler")
            self.assertIsNotNone(
                handler_default,
                f"Leaf {' '.join(node['path'])!r} must have a handler default",
            )
            self.assertIsInstance(handler_default, str)

    def test_raw_leaf_handlers_are_callable_and_unique(self) -> None:
        with _sanitized_environ():
            parser = build_parser()
        _validate_raw_leaf_handlers(parser)

    def test_non_callable_leaf_handler_is_rejected(self) -> None:
        with _sanitized_environ():
            parser = build_parser()

        def first_leaf(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
            for action in p._actions:
                if isinstance(action, argparse._SubParsersAction):
                    return first_leaf(next(iter(action.choices.values())))
            return p

        leaf = first_leaf(parser)
        leaf.set_defaults(handler="not.callable")
        with self.assertRaises(AssertionError):
            _validate_raw_leaf_handlers(parser)

    def test_db_default_semantic_token(self) -> None:
        with _sanitized_environ():
            parser = build_parser()
        self.assertEqual(parser.get_default("db"), str(DEFAULT_DB_PATH))
        expected_sha256 = hashlib.sha256(str(DEFAULT_DB_PATH).encode("utf-8")).hexdigest()
        with FIXTURE_PATH.open(encoding="utf-8") as f:
            metadata = json.load(f)["metadata"]
        self.assertEqual(
            metadata["default_db_path_sha256"],
            expected_sha256,
            "Fixture must record the exact DEFAULT_DB_PATH bytes without exposing them",
        )

    def test_fixture_has_only_token_no_local_path(self) -> None:
        raw = FIXTURE_PATH.read_bytes()
        self.assertNotIn(str(DEFAULT_DB_PATH).encode("utf-8"), raw)
        self.assertNotIn(b"/Users/", raw)
        caller_home = os.environ.get("HOME")
        if caller_home:
            self.assertNotIn(caller_home.encode("utf-8"), raw)
        self.assertIn(b"<DEFAULT_DB_PATH>", raw)
        self.assertIn(HOME_TOKEN.encode("utf-8"), raw)

    def test_contract_generation_is_deterministic(self) -> None:
        first = _run_generation_subprocess()
        second = _run_generation_subprocess()
        self.assertEqual(first, second)
        self.assertEqual(
            hashlib.sha256(first).hexdigest(),
            hashlib.sha256(second).hexdigest(),
        )

    def test_fixture_matches_generated_contract(self) -> None:
        generated = _run_generation_subprocess()
        fixture = FIXTURE_PATH.read_bytes()
        self.assertEqual(
            generated,
            fixture,
            "Generated contract differs from committed fixture; update intentionally only through review",
        )

    def test_contract_p9_3c1_p1_delta_matches_base_fixture(self) -> None:
        contract = _build_contract()
        deactivate = next(
            node
            for node in contract["nodes"]
            if node["path"] == ["runtime", "agent", "deactivate"]
        )
        self.assertEqual(
            deactivate["defaults"]["handler"],
            "coordinate.execution_cli.handle_runtime_agent_deactivate",
        )

        claim = next(
            node
            for node in contract["nodes"]
            if node["path"] == ["runtime", "job", "claim"]
        )
        claim_actions = {action.get("dest"): action for action in claim["actions"]}
        self.assertEqual(claim_actions["reap_mode"]["default"], "global")
        self.assertEqual(claim_actions["reap_mode"]["choices"], ["global", "none"])
        self.assertIsNone(claim_actions["reap_reason"]["default"])

        reap = next(
            node
            for node in contract["nodes"]
            if node["path"] == ["runtime", "job", "lease", "reap"]
        )
        reap_actions = {action.get("dest"): action for action in reap["actions"]}
        self.assertIsNone(reap_actions["batch_size"]["default"])
        self.assertFalse(reap_actions["lease_id"]["required"])
        self.assertFalse(reap_actions["job_id"]["required"])

        historical = _rewrite_contract_to_p9_3c1_p1_baseline(contract)
        self.assertEqual(
            hashlib.sha256(_serialize_contract(historical)).hexdigest(),
            _P9_3C1_P1_BASE_FIXTURE_SHA256,
            "P9-3C1 P1 CLI delta must rewind exactly to the approved package base fixture",
        )

    def test_contract_p9_3c0_worktree_path_delta_matches_baseline(self) -> None:
        contract = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        submit_node = next(
            node
            for node in contract["nodes"]
            if node["path"] == ["runtime", "request", "submit"]
        )
        actions = [
            action
            for action in submit_node["actions"]
            if action.get("dest") == "worktree_path"
        ]
        self.assertEqual(len(actions), 1)
        self.assertFalse(actions[0]["required"])
        self.assertIn("--worktree-path WORKTREE_PATH", submit_node["help"])

        historical = _mask_p9_3c0_worktree_path_delta(contract)
        self.assertEqual(
            hashlib.sha256(_serialize_contract(historical)).hexdigest(),
            _P9_3C0_WORKTREE_PATH_BASELINE_FIXTURE_SHA256,
            "P9-3C0 CLI delta must be limited to the optional worktree_path action and its help reflow",
        )

    def test_contract_issue18_worktree_roots_delta_matches_baseline(self) -> None:
        """Issue #18 delta proof: the ``workspace host-profile set`` leaf carries
        exactly the two approved sibling worktree-root actions, and stripping
        them restores the pre-delta node bytes byte-for-byte."""
        contract = _build_contract()
        set_node = next(
            node
            for node in contract["nodes"]
            if node["path"] == ["workspace", "host-profile", "set"]
        )
        actions = {action.get("dest"): action for action in set_node["actions"]}
        self.assertEqual(actions["worktree_roots"]["option_strings"], ["--worktree-root"])
        self.assertEqual(actions["worktree_roots"]["action_class"], "_AppendAction")
        self.assertFalse(actions["worktree_roots"]["required"])
        self.assertEqual(
            actions["clear_worktree_roots"]["option_strings"],
            ["--clear-worktree-roots"],
        )
        self.assertEqual(
            actions["clear_worktree_roots"]["action_class"],
            "_StoreTrueAction",
        )
        self.assertIn("--worktree-root", set_node["help"])
        self.assertIn("--clear-worktree-roots", set_node["help"])

        historical = _remove_issue18_worktree_roots_delta(contract)
        hist_set = next(
            node
            for node in historical["nodes"]
            if node["path"] == ["workspace", "host-profile", "set"]
        )
        self.assertNotIn("worktree_roots", {a.get("dest") for a in hist_set["actions"]})
        self.assertNotIn(
            "clear_worktree_roots", {a.get("dest") for a in hist_set["actions"]}
        )
        self.assertEqual(
            hashlib.sha256(_serialize_contract(hist_set)).hexdigest(),
            _ISSUE18_PRE_WORKTREE_ROOTS_SET_NODE_SHA256,
            "Issue #18 rewind must restore the pre-delta host-profile set node exactly",
        )

        stripped = _remove_issue18_worktree_roots_delta(historical)
        self.assertEqual(
            _serialize_contract(stripped),
            _serialize_contract(historical),
            "Stripping an already-stripped contract must no-op",
        )

    def test_contract_p9_2b_delta_matches_baseline(self) -> None:
        """P9-2B delta proof: removing the routed flags restores the pre-P9-2B fixture."""
        contract = _build_contract()
        historical = _rewrite_contract_to_p9_2b_baseline(contract)
        historical_bytes = _serialize_contract(historical)
        self.assertEqual(
            hashlib.sha256(historical_bytes).hexdigest(),
            _P9_2B_BASELINE_FIXTURE_SHA256,
            "Fixture with P9-2B routed flags removed must match the pre-P9-2B baseline",
        )

    def test_contract_s4d_delta_matches_baseline(self) -> None:
        """S4-D delta proof: removing P9-2B flags and P9-2A executor leaves restores the reviewed S4-D baseline."""
        contract = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        historical = _remove_p9_2a_executor_leaves(_rewrite_contract_to_p9_2b_baseline(contract))
        historical_bytes = _serialize_contract(historical)
        self.assertEqual(
            hashlib.sha256(historical_bytes).hexdigest(),
            _S4D_BASELINE_FIXTURE_SHA256,
            "Fixture with P9-2B flags and P9-2A executor leaves removed must match the reviewed S4-D baseline SHA-256",
        )

    def test_contract_s4d_workspace_doctor_delta(self) -> None:
        """S4-D C2-to-D delta proof: the only change to the workspace doctor node
        is the approved ``--no-projections`` compatibility flag."""
        contract = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        doctor_node = next(n for n in contract["nodes"] if n["path"] == ["workspace", "doctor"])
        doctor_bytes = json.dumps(
            doctor_node, ensure_ascii=False, sort_keys=True, indent=2,
        ).encode("utf-8") + b"\n"
        self.assertEqual(
            hashlib.sha256(doctor_bytes).hexdigest(),
            _S4D_WORKSPACE_DOCTOR_NODE_SHA256,
            "Current workspace doctor node must match the S4-D baseline node",
        )

        historical = _rewrite_contract_to_s4d_baseline(contract)
        hist_doctor = next(n for n in historical["nodes"] if n["path"] == ["workspace", "doctor"])
        hist_bytes = json.dumps(
            hist_doctor, ensure_ascii=False, sort_keys=True, indent=2,
        ).encode("utf-8") + b"\n"
        self.assertEqual(
            hashlib.sha256(hist_bytes).hexdigest(),
            _S4C2_WORKSPACE_DOCTOR_NODE_SHA256,
            "Workspace doctor node with --no-projections removed must match the pre-D baseline node",
        )

    def test_contract_targeted_reconcile_delta_matches_baseline(self) -> None:
        """Targeted-reconcile delta proof: the current contract carries the
        optional ``--task-id`` action, and stripping it restores the canonical
        pre-targeted baseline fixture bytes (baseline commit 1aeadbaa)."""
        contract = _build_contract()
        reconcile_node = next(
            node for node in contract["nodes"] if node["path"] == ["reconcile"]
        )
        task_id_action = next(
            action
            for action in reconcile_node["actions"]
            if action.get("dest") == "task_id"
        )
        self.assertEqual(task_id_action["option_strings"], ["--task-id"])
        self.assertFalse(task_id_action["required"])

        historical = _remove_targeted_reconcile_delta(contract)
        self.assertEqual(
            hashlib.sha256(_serialize_contract(historical)).hexdigest(),
            _PRE_TARGETED_BASELINE_FIXTURE_SHA256,
            "Fixture with the targeted-reconcile delta removed must match the "
            "pre-targeted baseline commit fixture SHA-256",
        )

    def test_remove_targeted_reconcile_delta_structure_contract(self) -> None:
        """Delta removal fails closed on a missing or duplicate reconcile node
        and no-ops when the node exists without the ``task_id`` action."""
        base = _build_contract()
        reconcile_node = next(n for n in base["nodes"] if n["path"] == ["reconcile"])

        missing = copy.deepcopy(base)
        missing["nodes"] = [
            node for node in missing["nodes"] if node["path"] != ["reconcile"]
        ]
        with self.assertRaises(AssertionError):
            _remove_targeted_reconcile_delta(missing)

        duplicate = copy.deepcopy(base)
        duplicate["nodes"].append(copy.deepcopy(reconcile_node))
        with self.assertRaises(AssertionError):
            _remove_targeted_reconcile_delta(duplicate)

        stripped = _remove_targeted_reconcile_delta(base)
        again = _remove_targeted_reconcile_delta(stripped)
        self.assertEqual(
            _serialize_contract(again),
            _serialize_contract(stripped),
            "Stripping an already-stripped contract must no-op",
        )

    def test_contract_s4c2_rewind_matches_baseline(self) -> None:
        """S4-C2 delta proof: removing only the approved issue.materialize C2 args
        from the committed post-C2 fixture restores the exact post-C1 issue
        materialize nodes.

        The proof isolates the two C2 leaves rather than comparing whole bytes,
        because unrelated pre-existing Python 3.12 argparse/CLI drift keeps the
        historical cumulative rewind tests red. The issue.materialize nodes
        themselves must rewind to the post-C1 fixture byte-for-byte.

        The witness SHA constants are pinned to the reviewed post-C1 fixture
        node bytes so the proof does not depend on HEAD topology or git show.
        """
        contract = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        historical = _rewrite_contract_to_s4c2_baseline(contract)

        expected = {
            ("issue", "materialize-files"): _S4C2_ISSUE_MATERIALIZE_FILES_NODE_SHA256,
            ("issue", "materialize-record"): _S4C2_ISSUE_MATERIALIZE_RECORD_NODE_SHA256,
        }
        for path, expected_sha in expected.items():
            hist_node = next(n for n in historical["nodes"] if n["path"] == list(path))
            hist_sha = hashlib.sha256(_serialize_contract(hist_node)).hexdigest()
            self.assertEqual(
                hist_sha,
                expected_sha,
                f"C2 rewind of {' '.join(path)} must restore the post-C1 node exactly",
            )

        # Sanity check: C2 introduced exactly the expected new argument dests.
        for path, new_dests in (
            (["issue", "materialize-files"], {"workspace_id", "operation_id", "event_id"}),
            (
                ["issue", "materialize-record"],
                {"operation_id", "input_fingerprint", "before_fingerprint", "after_fingerprint"},
            ),
        ):
            post_node = next(n for n in contract["nodes"] if n["path"] == path)
            post_dests = {a.get("dest") for a in post_node["actions"] if a.get("dest")}
            self.assertTrue(
                new_dests.issubset(post_dests),
                f"Post-C2 {' '.join(path)} must expose {new_dests}",
            )
            hist_node = next(n for n in historical["nodes"] if n["path"] == path)
            hist_dests = {a.get("dest") for a in hist_node["actions"] if a.get("dest")}
            self.assertFalse(
                new_dests & hist_dests,
                f"Rewound {' '.join(path)} must not expose C2-only dests",
            )

    def test_semantic_projection_is_deterministic(self) -> None:
        first = _run_generation_subprocess("--dump-semantic")
        second = _run_generation_subprocess("--dump-semantic")
        self.assertEqual(first, second)
        self.assertEqual(
            hashlib.sha256(first).hexdigest(),
            hashlib.sha256(second).hexdigest(),
        )

    def test_semantic_projection_preserves_non_layout_fields(self) -> None:
        """The projection must keep every non-layout field and every help token."""
        contract = _build_contract()
        projected = _project_semantic_help(contract)

        self.assertEqual(
            projected["metadata"]["projection"], _SEMANTIC_PROJECTION_MARKER
        )
        raw_metadata = dict(contract["metadata"])
        projected["metadata"].pop("projection")
        self.assertEqual(projected["metadata"], raw_metadata)
        self.assertEqual(projected["leaf_paths"], contract["leaf_paths"])

        for raw, sem in zip(contract["nodes"], projected["nodes"]):
            with self.subTest(path=" ".join(raw["path"]) or "<root>"):
                self.assertEqual(sem["path"], raw["path"])
                self.assertEqual(sem["prog"], raw["prog"])
                self.assertEqual(sem["actions"], raw["actions"])
                self.assertEqual(sem["defaults"], raw["defaults"])
                self.assertEqual(sem["help"], " ".join(raw["help"].split()))
                self.assertEqual(sem["help"].split(), raw["help"].split())


class CLISupportSeamTests(unittest.TestCase):
    """Tests for the extracted cli_support seam and facade compatibility."""

    def test_default_db_path_alias(self) -> None:
        self.assertEqual(coordinate.cli.DEFAULT_DB_PATH, coordinate.cli_support.DEFAULT_DB_PATH)

    def test_connection_alias_points_to_support(self) -> None:
        self.assertIs(coordinate.cli._conn, coordinate.cli_support.open_connection)

    def test_print_json_alias_points_to_support(self) -> None:
        self.assertIs(coordinate.cli._print_json, coordinate.cli_support.print_json)

    def test_open_connection_yields_and_closes_on_success(self) -> None:
        conn = Mock()
        args = SimpleNamespace(db=":memory:")
        with unittest.mock.patch("coordinate.cli_support.initialize", return_value=conn):
            with open_connection(args) as yielded:
                self.assertIs(yielded, conn)
                self.assertFalse(conn.close.called)
        conn.close.assert_called_once_with()

    def test_open_connection_closes_on_exception(self) -> None:
        conn = Mock()
        args = SimpleNamespace(db=":memory:")
        with unittest.mock.patch("coordinate.cli_support.initialize", return_value=conn):
            with self.assertRaises(RuntimeError):
                with open_connection(args):
                    raise RuntimeError("boom")
        conn.close.assert_called_once_with()

    def test_print_json_unicode_and_sorting(self) -> None:
        stream = io.StringIO()
        with unittest.mock.patch("sys.stdout", stream):
            print_json({"emoji": "🎉", "nested": {"z": 1, "a": 2}})
        expected = json.dumps(
            {"emoji": "🎉", "nested": {"z": 1, "a": 2}},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        self.assertEqual(stream.getvalue(), expected + "\n")
        self.assertIn("🎉", stream.getvalue())

    def test_cli_support_does_not_import_cli(self) -> None:
        script = """
import sys
import coordinate.cli_support
if 'coordinate.cli' in sys.modules:
    raise SystemExit('cli_support imported coordinate.cli')
print('ok')
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=tmpdir,
                env={"PYTHONPATH": str(SRC_PATH), "PATH": os.environ.get("PATH", "")},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
        self.assertIn("ok", result.stdout)

    def test_completion_cli_does_not_import_cli_or_workflow_registrars(self) -> None:
        script = """
import sys
import coordinate.completion_cli
forbidden = set()
for name in sys.modules:
    if name == 'coordinate.cli' or name.startswith('coordinate.workflow_cli'):
        forbidden.add(name)
if forbidden:
    raise SystemExit(f'completion_cli imported forbidden modules: {sorted(forbidden)}')
print('ok')
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=tmpdir,
                env={"PYTHONPATH": str(SRC_PATH), "PATH": os.environ.get("PATH", "")},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
        self.assertIn("ok", result.stdout)

    def test_import_orders_succeed(self) -> None:
        orders = [
            ["coordinate.cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.completion_cli"],
            ["coordinate.cli_support", "coordinate.cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.completion_cli"],
            ["coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.cli", "coordinate.cli_support", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.completion_cli"],
            ["coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.completion_cli", "coordinate.cli", "coordinate.cli_support", "coordinate.workspace_cli"],
            ["coordinate.cli_support", "coordinate.pr_cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.completion_cli", "coordinate.cli"],
            ["coordinate.issue_cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.completion_cli", "coordinate.cli"],
            ["coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.completion_cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.cli"],
            ["coordinate.delivery_cli", "coordinate.completion_cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.cli"],
            ["coordinate.completion_cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.cli"],
        ]
        for order in orders:
            script = "; ".join(f"import {name}" for name in order) + "; print('ok')"
            with self.subTest(order=order):
                with tempfile.TemporaryDirectory() as tmpdir:
                    result = subprocess.run(
                        [sys.executable, "-c", script],
                        cwd=tmpdir,
                        env={
                            "PYTHONPATH": str(SRC_PATH),
                            "PATH": os.environ.get("PATH", ""),
                        },
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        check=True,
                    )
                self.assertIn("ok", result.stdout)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--dump":
        sys.stdout.buffer.write(_generate_contract_bytes())
        sys.exit(0)
    if len(sys.argv) > 1 and sys.argv[1] == "--dump-semantic":
        sys.stdout.buffer.write(_generate_semantic_contract_bytes())
        sys.exit(0)
    unittest.main()
