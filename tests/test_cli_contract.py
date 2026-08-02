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


def _remove_targeted_reconcile_delta(contract: dict[str, object]) -> dict[str, object]:
    """Return a copy of *contract* with the targeted-reconcile ``--task-id``
    parser delta removed, restoring the pre-targeted baseline help.

    No-op only when the reconcile node exists without the ``task_id`` action;
    fails closed on structural surprises (missing or multiple reconcile
    nodes, unexpected task_id action).
    """
    historical = copy.deepcopy(contract)
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
    historical = copy.deepcopy(contract)
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

    Any later delta (targeted-reconcile ``--task-id``, ``plan revise``) is
    stripped first so historical baseline rewinds stay free of post-P9-3C1
    CLI additions. This function is the shared entry of every cumulative
    rewind chain.
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
    historical = copy.deepcopy(contract)
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
    historical = copy.deepcopy(contract)
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
    historical = copy.deepcopy(contract)
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
    historical = copy.deepcopy(contract)

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
        self.assertEqual(len(metadata["top_level_commands"]), 21)
        self.assertEqual(metadata["leaf_count"], 90)
        self.assertEqual(metadata["node_count"], 118)
        self.assertEqual(len(contract["leaf_paths"]), 90)
        self.assertEqual(len(contract["nodes"]), 118)

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
