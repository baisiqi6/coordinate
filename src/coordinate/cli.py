from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

# Optional `python-dotenv` dependency. If it is not installed in the
# current interpreter, use a small KEY=VALUE fallback parser so service
# callers can still load simple .env files. The fallback prevents a
# missing transitive dependency from breaking every subprocess-style
# caller (notably `multinexus.agentd` -> `mac.sh` -> `python -m coordinate ...`).
try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:
    def find_dotenv(*_args, **_kwargs):  # type: ignore[no-redef]
        current = Path.cwd()
        for path in (current, *current.parents):
            candidate = path / ".env"
            if candidate.exists():
                return str(candidate)
        return ""

    def load_dotenv(*_args, dotenv_path=None, **_kwargs):  # type: ignore[no-redef]
        dotenv_path = dotenv_path or find_dotenv(usecwd=True)
        if not dotenv_path:
            return False
        path = Path(dotenv_path)
        if not path.exists():
            return False
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)
        return True

from . import __version__
from .assignments import request_assignment
from .branches import allocate_branch
from .ci import check_ci
from .reviews import check_merge_gate, check_pr_review
from .prs import (
    GitHubCommandError,
    PublishError,
    RecordPublishError,
    link_pr,
    publish_pr,
    publish_pr_existing,
    record_publish_preflight,
    record_publish_result,
)
from .completion import (
    CompletionClaimResult,
    CompletionConsumeResult,
    CompletionReceipt,
    CompletionReceiptError,
    ReceiptEvidence,
    apply_completion_receipt,
    claim_completion_receipt,
    compute_mark_done_fingerprints,
    consume_completion_receipt,
    parse_iso_timestamp,
    prepare_completion_receipt,
)
from .transitions import (
    AcceptTaskResult,
    BlockerTaskResult,
    CloseoutTaskResult,
    HandoffTaskResult,
    MarkDoneFilesResult,
    MarkDoneGateResult,
    MarkDoneRecordResult,
    MarkDoneTaskResult,
    ReviewResultTaskResult,
    UnblockTaskResult,
    accept_task,
    blocker_task,
    closeout_task,
    handoff_task,
    mark_done_files,
    mark_done_record,
    mark_done_task,
    review_result_task,
    unblock_task,
)
from .bus import BusError
from .db import (
    get_workspace,
    row_to_dict,
)
from .harness import HarnessError
from .jobs import JobError
from .handoff import latest_prepared_handoff_bootstrap
from .policy import PolicyError
from .cli_support import DEFAULT_DB_PATH, open_connection, print_json
from .workspace_cli import (
    handle_reconcile,
    handle_state,
    handle_workspace_add,
    handle_workspace_agent_add,
    handle_workspace_agent_sync,
    handle_workspace_audit,
    handle_workspace_doctor,
    handle_workspace_host_profile_list,
    handle_workspace_host_profile_set,
    handle_workspace_init_harness,
    handle_workspace_list,
    register_reconcile_command,
    register_workspace_commands,
)
from .planning_cli import (
    handle_event_append,
    handle_event_list,
    handle_operator_pending,
    handle_plan_approve,
    handle_plan_reject,
    handle_plan_review_request,
    handle_task_create,
    handle_task_create_files,
    handle_task_create_record,
    handle_task_handoff,
    register_operator_command,
    register_planning_commands,
)
from .execution_cli import (
    handle_job_cancel,
    handle_job_create,
    handle_job_list,
    handle_job_pump,
    handle_job_retry,
    handle_job_run,
    handle_runner_add,
    handle_runner_example,
    handle_runner_examples,
    handle_runner_list,
    handle_runtime_agent_heartbeat,
    handle_runtime_agent_register,
    handle_runtime_executor_list,
    handle_runtime_executor_show,
    handle_runtime_executor_sync,
    handle_runtime_job_claim,
    handle_runtime_job_progress,
    handle_runtime_job_report,
    handle_runtime_request_submit,
    register_job_commands,
    register_runner_commands,
    register_runtime_commands,
)
from .delivery_cli import (
    handle_delivery_create,
    handle_delivery_list,
    handle_delivery_pump,
    handle_delivery_recover_sending,
    handle_delivery_send,
    handle_policy_create_deliveries,
    handle_policy_create_delivery,
    handle_policy_pump_events,
    handle_policy_render_event,
    handle_worker_delivery,
    register_delivery_commands,
)


# Compatibility aliases for existing handlers and tests that patch these names.
_conn = open_connection
_print_json = print_json


def handle_serve(args: argparse.Namespace) -> int:
    import logging

    bot_token = os.environ.get("COORDINATOR_BOT_TOKEN")
    if not bot_token:
        print("error: COORDINATOR_BOT_TOKEN environment variable is required", file=sys.stderr)
        return 1

    channel_id_str = os.environ.get("COORDINATOR_CHANNEL_ID")
    if not channel_id_str:
        print("error: COORDINATOR_CHANNEL_ID environment variable is required", file=sys.stderr)
        return 1

    allowed_raw = os.environ.get("COORDINATOR_ALLOWED_USER_IDS", "")
    allowed_user_ids: set[int] = set()
    if allowed_raw:
        for part in allowed_raw.split(","):
            part = part.strip()
            if part:
                allowed_user_ids.add(int(part))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    from .daemon import CoordinatorDaemon

    logger = logging.getLogger("coordinator.daemon")
    logger.info(
        "Starting coordinator daemon git_sha=%s db=%s channel=%s pump_interval=%s",
        os.environ.get("COORDINATOR_GIT_SHA") or "unknown",
        args.db,
        channel_id_str,
        args.pump_interval,
    )

    daemon = CoordinatorDaemon(
        db_path=args.db,
        bot_token=bot_token,
        channel_id=int(channel_id_str),
        allowed_user_ids=allowed_user_ids,
        pump_interval=args.pump_interval,
    )
    try:
        daemon.run()
    except Exception:
        logger.exception("Coordinator daemon crashed")
        raise
    finally:
        logger.info("Coordinator daemon stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    # Load .env from the current working directory (or any ancestor) so
    # `coordinate serve` and other subcommands pick up
    # COORDINATOR_BOT_TOKEN / COORDINATOR_CHANNEL_ID /
    # COORDINATOR_ALLOWED_USER_IDS when invoked from a launchd-managed
    # process whose WorkingDirectory already exists. Existing process
    # env wins over .env (override=False), so launchd-supplied values
    # still take precedence.
    dotenv_path = find_dotenv(usecwd=True)
    load_dotenv(dotenv_path=dotenv_path or None)

    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 2
    try:
        return args.handler(args)
    except (HarnessError, JobError, BusError, PolicyError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="coordinate")
    parser.add_argument(
        "--db",
        default=os.environ.get("MULTI_AGENT_COORDINATOR_DB", DEFAULT_DB_PATH),
        help="SQLite database path",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subcommands = parser.add_subparsers(dest="command")

    register_workspace_commands(subcommands)

    register_planning_commands(subcommands)

    register_runner_commands(subcommands)

    register_reconcile_command(subcommands)

    register_branch_command(subcommands)

    register_pr_commands(subcommands)

    register_forge_commands(subcommands)

    register_issue_commands(subcommands)

    register_job_commands(subcommands)

    register_delivery_commands(subcommands)

    register_runtime_commands(subcommands)

    register_assignment_commands(subcommands)

    register_operator_command(subcommands)

    serve = subcommands.add_parser("serve", help="Run coordinator daemon with Discord bot")
    serve.add_argument("--pump-interval", type=int, default=30, help="Pump interval in seconds")
    serve.set_defaults(handler=handle_serve)

    return parser


# Compatibility exports for existing callers and test injection hooks.
from coordinate.pr_cli import (  # noqa: E402
    _build_event_cli_argv,
    _forward_publish_preflight,
    _forward_publish_result,
    handle_pr_link,
    handle_pr_publish,
    handle_pr_publish_preflight,
    handle_pr_publish_record,
    register_pr_commands,
)

# Compatibility exports for existing callers and test injection hooks.
from coordinate.issue_cli import (  # noqa: E402
    handle_issue_materialize,
    handle_issue_materialize_files,
    handle_issue_materialize_record,
    handle_issue_scan,
    handle_issue_triage,
    register_issue_commands,
)

# Compatibility exports for existing callers and test injection hooks.
from coordinate.completion_cli import (  # noqa: E402
    _build_mark_done_event_cli_argv,
    _forward_mark_done_apply,
    _forward_mark_done_claim,
    _forward_mark_done_preflight,
    _lookup_receipt_for_preflight,
    _run_mark_done_files_receipt,
    _run_remote_cli_json,
    _stamp_repair_verification,
    handle_assignment_mark_done_apply,
    handle_assignment_mark_done_claim,
    handle_assignment_mark_done_files,
    handle_assignment_mark_done_prepare,
    handle_assignment_mark_done_preflight,
    handle_assignment_mark_done_record,
    register_completion_commands,
)

# Compatibility exports for existing callers and test injection hooks.
from coordinate.workflow_cli import (  # noqa: E402
    handle_branch_allocate,
    handle_ci_check,
    handle_merge_gate,
    handle_review_check,
    handle_assignment_accept,
    handle_assignment_blocker,
    handle_assignment_closeout,
    handle_assignment_handoff,
    handle_assignment_mark_done,
    handle_assignment_request,
    handle_assignment_review_result,
    handle_assignment_unblock,
    register_assignment_commands,
    register_branch_command,
    register_forge_commands,
)
