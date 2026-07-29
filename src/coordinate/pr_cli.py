"""PR command registration, handlers, and host/server forwarding."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from coordinate.db import initialize
from coordinate.prs import (
    GitHubCommandError,
    PublishError,
    RecordPublishError,
    link_pr,
    publish_pr,
    publish_pr_existing,
    record_publish_preflight,
    record_publish_result,
)


@contextmanager
def _conn(args: argparse.Namespace):
    conn = initialize(Path(args.db).expanduser())
    try:
        yield conn
    finally:
        conn.close()


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def register_pr_commands(subcommands) -> None:

    pr = subcommands.add_parser("pr", help="Manage PR links")
    pr_subcommands = pr.add_subparsers(dest="pr_command")

    pr_link = pr_subcommands.add_parser("link", help="Link a PR to a task")
    pr_link.add_argument("workspace_id")
    pr_link.add_argument("--task-id", required=True)
    pr_link.add_argument("--pr-url")
    pr_link.add_argument("--branch")
    pr_link.add_argument("--actor", default="operator")
    pr_link.set_defaults(handler=handle_pr_link)

    pr_publish = pr_subcommands.add_parser(
        "publish",
        help="Verify worker branch on GitHub and create or link a PR (Phase 8.4).",
    )
    pr_publish.add_argument("workspace_id")
    pr_publish.add_argument("--task-id", required=True)
    pr_publish.add_argument("--repo", required=True, help="GitHub repo in owner/name form")
    pr_publish.add_argument("--branch", required=True, help="Worker branch name")
    pr_publish.add_argument(
        "--head-owner",
        required=True,
        help="GitHub owner of the head branch (often the same as the repo owner)",
    )
    pr_publish.add_argument(
        "--base",
        required=True,
        help="Target base branch on GitHub (e.g. main). Must be explicit to avoid cross-repo confusion.",
    )
    pr_publish.add_argument("--title", required=True)
    pr_publish.add_argument(
        "--body",
        default="",
        help="PR body; empty allowed. Will not be treated as trusted input.",
    )
    pr_publish.add_argument("--commit", required=True, help="Full 40-char worker commit SHA")
    pr_publish.add_argument(
        "--pushed",
        required=True,
        help="Strict true|false; reported by worker host about its own push.",
    )
    pr_publish.add_argument("--actor", default="operator")
    pr_publish.add_argument("--remote", help="Audit-only remote name, e.g. origin")
    pr_publish.add_argument("--validation", help="Short validation summary from worker")
    pr_publish.add_argument(
        "--event-cli-path",
        help=(
            "Path to a coord CLI that runs `pr publish-record` against a "
            "remote DB (e.g. <HOME>/.local/bin/coord-ssh). The host "
            "forwards its PublishResult JSON to the remote CLI; the remote "
            "re-validates the mirror and (on success) upserts the remote "
            "task mirror. The remote CLI never invokes `gh`. .py paths "
            "are auto-prepended with sys.executable for Windows hosts. "
            "When this flag is set, the host ALSO runs a remote "
            "`pr publish-preflight` using the same path BEFORE any `gh` "
            "call, unless overridden by --preflight-event-cli-path."
        ),
    )
    pr_publish.add_argument(
        "--preflight-event-cli-path",
        help=(
            "Optional override for the preflight CLI path. When set, the "
            "host runs a remote `pr publish-preflight` first; if it fails "
            "(mirror_conflict or unknown_workspace), the host DOES NOT "
            "invoke `gh pr create` and returns a `publish.blocked` "
            "envelope with code 1. This is how the host guarantees no "
            "GitHub write happens before the remote state has been "
            "re-validated. If omitted but --event-cli-path is set, the "
            "same path is used automatically."
        ),
    )
    pr_publish.set_defaults(handler=handle_pr_publish)

    pr_publish_record = pr_subcommands.add_parser(
        "publish-record",
        help=(
            "Record a host-side publish result into the local DB "
            "(record-only, never invokes gh). Phase 8.4 remote sink."
        ),
    )
    pr_publish_record.add_argument("workspace_id")
    pr_publish_record.add_argument(
        "--result-json",
        required=True,
        help=(
            "JSON envelope of a host-side PublishResult.to_dict() output. "
            "The remote CLI never calls `gh`; it only re-validates the "
            "mirror, appends the event with the host-supplied idempotency_key, "
            "and (on success) upserts the task mirror with the resolved PR URL."
        ),
    )
    pr_publish_record.add_argument("--actor", default="operator")
    pr_publish_record.set_defaults(handler=handle_pr_publish_record)

    pr_publish_preflight = pr_subcommands.add_parser(
        "publish-preflight",
        help=(
            "Read-only check against the local DB: would "
            "`pr publish-record` accept the host's claim? "
            "Used by the host CLI *before* `gh pr create` to catch "
            "mirror_conflict / unknown_workspace without writing anything."
        ),
    )
    pr_publish_preflight.add_argument("workspace_id")
    pr_publish_preflight.add_argument("--repo", required=True)
    pr_publish_preflight.add_argument("--branch", required=True)
    pr_publish_preflight.add_argument(
        "--commit", required=True, dest="reported_commit",
        help="Full 40-char worker commit SHA",
    )
    pr_publish_preflight.add_argument("--task-id", required=True)
    pr_publish_preflight.set_defaults(handler=handle_pr_publish_preflight)


def handle_pr_link(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        try:
            result = link_pr(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                pr_url=args.pr_url,
                branch=args.branch,
                actor=args.actor,
            )
        except ValueError as exc:
            _print_json({"error": {"message": str(exc)}})
            return 1
    _print_json({
        "workspace_id": result.workspace_id,
        "task_id": result.task_id,
        "pr_url": result.pr_url,
        "branch": result.branch,
        "event_created": result.event_created,
        "existing": result.existing,
    })
    return 0


def handle_pr_publish(args: argparse.Namespace) -> int:
    """Phase 8.4 publish entry point.

    Host/server split:

    - Default mode: runs `publish_pr` against the local DB. This is the
      only mode that may invoke `gh` — it runs on the GitHub-capable
      coding host (Mac/Windows) where the operator holds the GitHub
      token.

    - `--preflight-event-cli-path` (optional): before any `gh` call,
      forward a `pr publish-preflight` to the remote coord CLI. If the
      remote returns `ok=false` (mirror_conflict / unknown_workspace /
      invalid_result), the host DOES NOT invoke `gh pr create` and
      returns a `publish.blocked` envelope with code 1. This is the
      "no GitHub write before remote check" guarantee.

    - `--event-cli-path`: after `publish_pr` succeeds, forward the
      `PublishResult.to_dict()` envelope to a remote `pr publish-record`
      invocation. The remote CLI is record-only — never `gh`. The
      record-only sink recomputes event_type / payload /
      idempotency_key from the host's facts and applies a single
      SQLite transaction so a half-completion can be repaired on
      replay.

    Exit codes:

    - 0: result.action in {created, linked}.
    - 1: preflight failed, OR result.action in {push_required,
      blocked}, OR local publish_pr raised, OR remote record-only
      sink rejected.
    - 2: argparse / validation failure.
    """
    # Resolve through the root CLI facade so established test/operator
    # injection hooks keep working after this module extraction.
    from coordinate import cli as cli_facade

    # Step 1: optional remote preflight — gate `gh` writes behind the
    # remote mirror check so a host never creates a PR whose record
    # the remote will refuse.
    #
    # If --event-cli-path is set, preflight is mandatory. Use
    # --preflight-event-cli-path when provided; otherwise default to the
    # same path as --event-cli-path. This prevents the unsafe "record but
    # no preflight" bypass.
    preflight_path = args.preflight_event_cli_path or args.event_cli_path
    if preflight_path:
        try:
            pre = cli_facade._forward_publish_preflight(
                preflight_path,
                workspace_id=args.workspace_id,
                repo=args.repo,
                branch=args.branch,
                reported_commit=args.commit,
                task_id=args.task_id,
            )
        except (PublishError, ValueError) as exc:
            reason = getattr(exc, "reason", "preflight_failed")
            _print_json({
                "error": {
                    "message": f"remote preflight failed: {exc}",
                    "reason": reason,
                },
            })
            return 1
        if pre.get("ok") is not True:
            reason = pre.get("reason") or "preflight_failed"
            _print_json({
                "error": {
                    "message": (
                        f"remote preflight rejected host's claim: "
                        f"{pre.get('message') or reason}"
                    ),
                    "reason": reason,
                },
                "preflight": pre,
            })
            return 1
        preflight_mode = pre.get("mode")
        if preflight_mode not in {None, "link_existing"}:
            _print_json({
                "error": {
                    "message": f"remote preflight returned unsupported mode: {preflight_mode!r}",
                    "reason": "invalid_preflight",
                },
                "preflight": pre,
            })
            return 1
        # Preflight says the mirror already has a PR for this task. The host
        # must not call `gh pr create`; it performs a read-only discovery and
        # links the existing PR if URL/SHA/base all match.
        if preflight_mode == "link_existing":
            expected_pr_url = pre.get("expected_pr_url")
            if not isinstance(expected_pr_url, str) or not expected_pr_url:
                _print_json({
                    "error": {
                        "message": "remote link_existing preflight omitted expected_pr_url",
                        "reason": "invalid_preflight",
                    },
                    "preflight": pre,
                })
                return 1
            with _conn(args) as conn:
                try:
                    result = cli_facade.publish_pr_existing(
                        conn,
                        workspace_id=args.workspace_id,
                        task_id=args.task_id,
                        repo=args.repo,
                        branch=args.branch,
                        head_owner=args.head_owner,
                        base=args.base,
                        commit=args.commit,
                        expected_pr_url=expected_pr_url,
                        actor=args.actor,
                        remote=args.remote,
                        validation=args.validation,
                    )
                except (PublishError, GitHubCommandError, ValueError) as exc:
                    reason = getattr(exc, "reason", "validation_failed")
                    _print_json({"error": {"message": str(exc), "reason": reason}})
                    return 1
            # Forward the linked result to the remote record-only sink if
            # requested, same as the normal publish path.
            if args.event_cli_path:
                try:
                    forward = cli_facade._forward_publish_result(
                        args.event_cli_path,
                        workspace_id=args.workspace_id,
                        result=result.to_dict(),
                        actor=args.actor,
                    )
                except (PublishError, GitHubCommandError, ValueError) as exc:
                    reason = getattr(exc, "reason", "validation_failed")
                    _print_json({
                        "error": {
                            "message": (
                                f"local publish succeeded but remote record-only "
                                f"sink rejected the result: {exc}"
                            ),
                            "reason": reason,
                        },
                        "result": result.to_dict(),
                    })
                    return 1
                _print_json({"result": result.to_dict(), "remote": forward})
            else:
                _print_json({"result": result.to_dict()})
            # linked is a success exit; blocked is a failure exit.
            return 0 if result.action == "linked" else 1

    with _conn(args) as conn:
        try:
            result = cli_facade.publish_pr(
                conn,
                workspace_id=args.workspace_id,
                task_id=args.task_id,
                repo=args.repo,
                branch=args.branch,
                head_owner=args.head_owner,
                base=args.base,
                title=args.title,
                body=args.body,
                commit=args.commit,
                pushed=args.pushed,
                actor=args.actor,
                remote=args.remote,
                validation=args.validation,
            )
        except (PublishError, GitHubCommandError, ValueError) as exc:
            reason = getattr(exc, "reason", "validation_failed")
            _print_json({"error": {"message": str(exc), "reason": reason}})
            return 1

    if args.event_cli_path:
        try:
            forward = cli_facade._forward_publish_result(
                args.event_cli_path,
                workspace_id=args.workspace_id,
                result=result.to_dict(),
                actor=args.actor,
            )
        except (PublishError, GitHubCommandError, ValueError) as exc:
            reason = getattr(exc, "reason", "validation_failed")
            _print_json({
                "error": {
                    "message": (
                        f"local publish succeeded but remote record-only "
                        f"sink rejected the result: {exc}"
                    ),
                    "reason": reason,
                },
                "result": result.to_dict(),
            })
            return 1
        _print_json({"result": result.to_dict(), "remote": forward})
    else:
        _print_json({"result": result.to_dict()})

    # Exit code: fail-closed on push_required / blocked.
    if result.action in {"push_required", "blocked"}:
        return 1
    return 0


def handle_pr_publish_record(args: argparse.Namespace) -> int:
    """Record-only sink for a host's publish result.

    Runs `record_publish_result` against the local DB. Never invokes
    `gh`. The merge gate / cross-host reconciliation reads from this DB,
    so the success path must reliably upsert the task mirror's PR
    column when the host reports action=created or action=linked.
    """
    try:
        result = json.loads(args.result_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --result-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(result, dict):
        print("error: --result-json must decode to an object", file=sys.stderr)
        return 1
    with _conn(args) as conn:
        try:
            recorded = record_publish_result(
                conn,
                workspace_id=args.workspace_id,
                result=result,
                actor=args.actor,
            )
        except RecordPublishError as exc:
            _print_json({
                "error": {"message": str(exc), "reason": exc.reason},
            })
            return 1
    _print_json({"result": recorded.to_dict()})
    return 0


def handle_pr_publish_preflight(args: argparse.Namespace) -> int:
    """Read-only preflight: would the host's claim be accepted?

    Used on the host *before* `gh pr create` so mirror conflicts are
    caught before any GitHub write happens. Exits 0 on `ok=true`,
    1 on `ok=false` (so CI / the host CLI can short-circuit).
    """
    with _conn(args) as conn:
        result = record_publish_preflight(
            conn,
            workspace_id=args.workspace_id,
            repo=args.repo,
            branch=args.branch,
            reported_commit=args.reported_commit,
            task_id=args.task_id,
        )
    _print_json({"result": result})
    return 0 if result.get("ok") else 1


def _forward_publish_preflight(
    event_cli_path: str,
    *,
    workspace_id: str,
    repo: str,
    branch: str,
    reported_commit: str,
    task_id: str,
) -> dict[str, Any]:
    """Forward a preflight check to a remote coord CLI.

    The remote CLI invokes `pr publish-preflight` (read-only) against
    the remote DB. We never call `gh` and never write anything; this
    is the "no GitHub write before remote check" gate.
    """
    argv = _build_event_cli_argv(event_cli_path, [
        "pr", "publish-preflight", workspace_id,
        "--repo", repo,
        "--branch", branch,
        "--commit", reported_commit,
        "--task-id", task_id,
    ])
    completed = subprocess.run(  # noqa: S603 - controlled argv list
        argv,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        reason = "preflight_failed"
        message = stderr or completed.returncode
        if stdout:
            try:
                parsed = json.loads(stdout)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict) and isinstance(parsed.get("error"), dict):
                err = parsed["error"]
                reason = err.get("reason") or reason
                if err.get("message"):
                    message = err["message"]
            elif isinstance(parsed, dict) and isinstance(parsed.get("result"), dict):
                # Non-zero exit but preflight payload returned —
                # surface its `ok=false` reason.
                payload = parsed["result"]
                reason = payload.get("reason") or reason
                if payload.get("message"):
                    message = payload["message"]
        raise PublishError(
            f"remote preflight failed: {message}",
            reason=reason,
        )
    stdout = (completed.stdout or "").strip()
    if not stdout:
        raise PublishError(
            "remote preflight returned empty stdout",
            reason="event_cli_invalid_json",
        )
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise PublishError(
            f"remote preflight returned invalid JSON: {exc}",
            reason="event_cli_invalid_json",
        ) from exc
    if not isinstance(parsed, dict) or "result" not in parsed:
        raise PublishError(
            "remote preflight JSON missing 'result' field",
            reason="event_cli_invalid_json",
        )
    result = parsed["result"]
    if not isinstance(result, dict):
        raise PublishError(
            "remote preflight JSON result not an object",
            reason="event_cli_invalid_json",
        )
    return result


def _forward_publish_result(
    event_cli_path: str,
    *,
    workspace_id: str,
    result: dict[str, Any],
    actor: str,
) -> dict[str, Any]:
    """Forward a host-side PublishResult to a remote `pr publish-record`.

    Builds the argv list `<event_cli_path> pr publish-record <workspace>
    --result-json <json> --actor <actor>` and runs it. The remote CLI
    is record-only — it never calls `gh`. On success we return the
    remote sink's response (which includes `mirror_updated`).
    """
    result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    argv = _build_event_cli_argv(event_cli_path, [
        "pr", "publish-record", workspace_id,
        "--result-json", result_json,
        "--actor", actor,
    ])
    completed = subprocess.run(  # noqa: S603 - controlled argv list
        argv,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        # The remote CLI prints {"error": {"message": ..., "reason": ...}}
        # on stdout too — try to surface a structured reason if present.
        stdout = (completed.stdout or "").strip()
        reason = "event_cli_failed"
        message = stderr or completed.returncode
        if stdout:
            try:
                parsed = json.loads(stdout)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict) and isinstance(parsed.get("error"), dict):
                err = parsed["error"]
                reason = err.get("reason") or reason
                if err.get("message"):
                    message = err["message"]
        raise PublishError(
            f"remote publish-record failed: {message}",
            reason=reason,
        )
    stdout = (completed.stdout or "").strip()
    if not stdout:
        raise PublishError(
            "remote publish-record returned empty stdout",
            reason="event_cli_invalid_json",
        )
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise PublishError(
            f"remote publish-record returned invalid JSON: {exc}",
            reason="event_cli_invalid_json",
        ) from exc
    if not isinstance(parsed, dict) or "result" not in parsed:
        raise PublishError(
            "remote publish-record JSON missing 'result' field",
            reason="event_cli_invalid_json",
        )
    if not isinstance(parsed["result"], dict):
        raise PublishError(
            "remote publish-record JSON result not an object",
            reason="event_cli_invalid_json",
        )
    return parsed["result"]


def _build_event_cli_argv(event_cli_path: str, args: list[str]) -> list[str]:
    """Build a subprocess argv for a coordinate coord CLI.

    On Windows, a coord CLI that is shipped as a `.py` script must be
    invoked via `python` (the Windows coding host lacks an executable
    bit / shebang resolution for `.py` files in some flows). We prepend
    `sys.executable` only when the path ends with `.py`, leaving native
    binaries (Mac's coord-ssh, Linux wrappers) untouched.
    """
    if event_cli_path.lower().endswith(".py"):
        return [sys.executable, event_cli_path, *args]
    return [event_cli_path, *args]  # type: ignore[list-item]  # noqa: PERF401
