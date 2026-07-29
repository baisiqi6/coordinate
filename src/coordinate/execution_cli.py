"""Runner, job, and runtime CLI registration and handlers."""
from __future__ import annotations

import argparse
import json
import sys

from .cli_support import open_connection, print_json
from .db import (
    create_job,
    list_jobs,
    list_runner_profiles,
    row_to_dict,
    upsert_runner_profile,
)
from .executor_capacity import (
    CapacityError,
    get_capacity_policy,
    list_capacity_policies,
    list_capacity_sources,
    parse_capacity_catalog,
    sync_capacity_catalog,
)
from .executor_identity import (
    ExecutorIdentityError,
    get_executor_instance_binding,
    list_executor_catalog_sources,
    list_executor_definitions,
    list_executor_instance_bindings,
    parse_executor_catalog,
    resolve_exact_executor_binding,
    sync_executor_catalog,
)
from .executor_routing import ExecutorRoutingError, build_routing_request
from .jobs import cancel_job, pump_jobs, retry_job, run_job
from .runner_examples import get_runner_profile_example, list_runner_profile_examples
from .runtime import (
    claim_job as runtime_claim_job,
    deactivate_agent,
    heartbeat_agent,
    register_agent,
    record_job_progress,
    report_job_result,
    submit_request,
)
from .runtime_lease import (
    _validate_id,
    _validate_reason,
    reap_due_leases,
    reap_exact_lease,
    renew_managed_lease,
)


def handle_runtime_capacity_sync(args: argparse.Namespace) -> int:
    """CLI handler for ``runtime capacity sync``."""
    try:
        catalog = parse_capacity_catalog(args.source)
    except (CapacityError, FileNotFoundError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    with _conn(args) as conn:
        try:
            result = sync_capacity_catalog(
                conn,
                catalog,
                source_path=args.source,
                synced_by=getattr(args, "synced_by", "operator"),
            )
        except CapacityError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        result["synced_by"] = getattr(args, "synced_by", "operator")
        _print_json(result)
    return 0


def handle_runtime_capacity_list(args: argparse.Namespace) -> int:
    """CLI handler for ``runtime capacity list``."""
    with _conn(args) as conn:
        sources = [
            {
                "source_id": row["source_id"],
                "source_version": row["source_version"],
                "catalog_hash": row["catalog_hash"],
                "updated_at": row["updated_at"],
            }
            for row in list_capacity_sources(conn)
        ]
        policies = [
            {
                "agent_id": row["agent_id"],
                "source_id": row["source_id"],
                "capacity_policy_id": row["capacity_policy_id"],
                "max_concurrent_jobs": row["max_concurrent_jobs"],
            }
            for row in list_capacity_policies(conn)
        ]
    _print_json({"sources": sources, "policies": policies})
    return 0


def handle_runtime_capacity_show(args: argparse.Namespace) -> int:
    """CLI handler for ``runtime capacity show``."""
    with _conn(args) as conn:
        policy = get_capacity_policy(conn, args.agent_id)
        if policy is None:
            print(f"error: unknown capacity agent: {args.agent_id}", file=sys.stderr)
            return 1
    _print_json({"agent_id": args.agent_id, "policy": policy})
    return 0


# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json


def register_runner_commands(subcommands) -> None:
    runner = subcommands.add_parser("runner", help="Manage runner profiles")
    runner_subcommands = runner.add_subparsers(dest="runner_command")

    runner_add = runner_subcommands.add_parser("add", help="Register or update a runner profile")
    runner_add.add_argument("id")
    runner_add.add_argument("--name")
    runner_add.add_argument("--runner-type", required=True)
    runner_add.add_argument("--command", required=True)
    runner_add.add_argument("--working-directory-strategy", default="current_dir")
    runner_add.add_argument("--supports-stream-attach", action="store_true")
    runner_add.add_argument("--env-json", default="{}")
    runner_add.set_defaults(handler=handle_runner_add)

    runner_list = runner_subcommands.add_parser("list", help="List runner profiles")
    runner_list.set_defaults(handler=handle_runner_list)

    runner_examples = runner_subcommands.add_parser("examples", help="List built-in runner profile examples")
    runner_examples.set_defaults(handler=handle_runner_examples)

    runner_example = runner_subcommands.add_parser("example", help="Show one built-in runner profile example")
    runner_example.add_argument("id")
    runner_example.set_defaults(handler=handle_runner_example)


def register_job_commands(subcommands) -> None:
    job = subcommands.add_parser("job", help="Create, run, and list jobs")
    job_subcommands = job.add_subparsers(dest="job_command")

    job_create = job_subcommands.add_parser("create", help="Create a pending runner job")
    job_create.add_argument("workspace_id")
    job_create.add_argument("--runner-profile-id", required=True)
    job_create.add_argument("--task-id")
    job_create.add_argument("--prompt-path")
    job_create.add_argument("--branch")
    job_create.add_argument("--worktree-path")
    job_create.add_argument("--terminal-session-id")
    job_create.add_argument("--logs-path")
    job_create.add_argument("--result-path")
    job_create.add_argument("--timeout-seconds", type=int)
    job_create.add_argument("--payload-json", default="{}")
    job_create.set_defaults(handler=handle_job_create)

    job_list = job_subcommands.add_parser("list", help="List jobs")
    job_list.add_argument("--workspace-id")
    job_list.add_argument("--status")
    job_list.set_defaults(handler=handle_job_list)

    job_run = job_subcommands.add_parser("run", help="Run one pending generic_subprocess job")
    job_run.add_argument("job_id")
    job_run.set_defaults(handler=handle_job_run)

    job_cancel = job_subcommands.add_parser("cancel", help="Cancel a pending/running job")
    job_cancel.add_argument("job_id")
    job_cancel.add_argument("--reason")
    job_cancel.set_defaults(handler=handle_job_cancel)

    job_retry = job_subcommands.add_parser("retry", help="Create a new pending job from a failed/cancelled job")
    job_retry.add_argument("job_id")
    job_retry.add_argument("--reason")
    job_retry.set_defaults(handler=handle_job_retry)

    job_pump = job_subcommands.add_parser("pump", help="Run pending generic_subprocess jobs")
    job_pump.add_argument("--workspace-id")
    job_pump.add_argument("--limit", type=int, default=10)
    job_pump.set_defaults(handler=handle_job_pump)


def register_runtime_commands(subcommands) -> None:
    runtime = subcommands.add_parser("runtime", help="Bridge and agentd runtime operations")
    runtime_subcommands = runtime.add_subparsers(dest="runtime_command")

    runtime_agent = runtime_subcommands.add_parser("agent", help="Register or heartbeat a runtime client")
    runtime_agent_sub = runtime_agent.add_subparsers(dest="runtime_agent_command")

    runtime_agent_register = runtime_agent_sub.add_parser(
        "register", help="Upsert an agentd or bridge record in the runtime agent registry"
    )
    runtime_agent_register.add_argument("--agent-id", required=True)
    runtime_agent_register.add_argument("--host-id", required=True)
    runtime_agent_register.add_argument("--client-type", choices=["agentd", "bridge"], default="agentd")
    runtime_agent_register.add_argument("--capabilities-json", default="")
    runtime_agent_register.add_argument("--actor", default="runtime")
    runtime_agent_register.set_defaults(handler=handle_runtime_agent_register)

    runtime_agent_heartbeat = runtime_agent_sub.add_parser(
        "heartbeat", help="Mark an already-registered runtime client as online and refresh last-seen"
    )
    runtime_agent_heartbeat.add_argument("--agent-id", required=True)
    runtime_agent_heartbeat.add_argument("--host-id", required=True)
    runtime_agent_heartbeat.add_argument("--actor", default="runtime")
    runtime_agent_heartbeat.set_defaults(handler=handle_runtime_agent_heartbeat)

    runtime_agent_deactivate = runtime_agent_sub.add_parser(
        "deactivate", help="Deactivate a runtime agent and block it from claiming work"
    )
    runtime_agent_deactivate.add_argument("--agent-id", required=True)
    runtime_agent_deactivate.add_argument("--host-id", required=True)
    runtime_agent_deactivate.add_argument("--reason", required=True)
    runtime_agent_deactivate.add_argument("--actor", default="runtime")
    runtime_agent_deactivate.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Check blockers without deactivating",
    )
    runtime_agent_deactivate.set_defaults(handler=handle_runtime_agent_deactivate)

    runtime_request = runtime_subcommands.add_parser(
        "request", help="Submit a bridge request and create a pending agent job"
    )
    runtime_request_sub = runtime_request.add_subparsers(dest="runtime_request_command")
    runtime_request_submit = runtime_request_sub.add_parser(
        "submit", help="Submit a bridge request: persists request.received event and creates a pending job"
    )
    runtime_request_submit.add_argument("workspace_id")
    route_group = runtime_request_submit.add_mutually_exclusive_group(required=True)
    route_group.add_argument("--target-agent", dest="target_agent")
    route_group.add_argument(
        "--route-capability",
        dest="route_capabilities",
        action="append",
        help="Required executor capability (repeatable); enables routed mode",
    )
    runtime_request_submit.add_argument("--route-definition", dest="route_definition")
    runtime_request_submit.add_argument("--preferred-host", dest="preferred_host")
    runtime_request_submit.add_argument("--override-agent", dest="override_agent")
    runtime_request_submit.add_argument("--override-reason", dest="override_reason")
    runtime_request_submit.add_argument("--worktree-path")
    runtime_request_submit.add_argument("--prompt", required=True)
    runtime_request_submit.add_argument("--origin-json", required=True)
    runtime_request_submit.add_argument("--reply-json", required=True)
    runtime_request_submit.add_argument("--task-id")
    runtime_request_submit.add_argument("--actor", default="bridge")
    runtime_request_submit.add_argument("--idempotency-key")
    runtime_request_submit.set_defaults(handler=handle_runtime_request_submit)

    runtime_job = runtime_subcommands.add_parser("job", help="Claim or report runtime jobs")
    runtime_job_sub = runtime_job.add_subparsers(dest="runtime_job_command")

    runtime_job_claim = runtime_job_sub.add_parser(
        "claim", help="Claim the next pending job for an agent; returns claimed=false when the queue is empty"
    )
    runtime_job_claim.add_argument("--agent-id", required=True)
    runtime_job_claim.add_argument(
        "--recoverable",
        action="store_true",
        default=False,
        help="Also claim recoverable timed_out jobs (explicit recovery path). Default: only pending.",
    )
    runtime_job_claim.add_argument(
        "--recovery-reason",
        help="Audited Operator reason for recovery; required with --recoverable",
    )
    runtime_job_claim.add_argument(
        "--prior-process-stopped",
        action="store_true",
        default=False,
        help="Operator confirmation that the prior provider process/session has stopped",
    )
    runtime_job_claim.add_argument(
        "--reap-mode",
        choices=["global", "none"],
        default="global",
        help="Reap mode: global (default) or none (scoped no-reap)",
    )
    runtime_job_claim.add_argument(
        "--reap-reason",
        help="Required reason when reap-mode=none",
    )
    runtime_job_claim.set_defaults(handler=handle_runtime_job_claim)

    runtime_job_report = runtime_job_sub.add_parser(
        "report", help="Report a terminal or recoverable timeout job status with a structured result payload"
    )
    runtime_job_report.add_argument("job_id")
    runtime_job_report.add_argument("--agent-id", required=True)
    runtime_job_report.add_argument("--status", choices=["done", "failed", "timed_out"], required=True)
    runtime_job_report.add_argument("--result-json", required=True)
    runtime_job_report.add_argument("--actor")
    runtime_job_report.add_argument("--attempt-token", type=int, help="Current attempt_count from claim; rejects stale attempts (8.4.3 P1 #2)")
    runtime_job_report.add_argument("--lease-id", help="P9-3B managed-attempt lease id; required for leased attempts")
    runtime_job_report.set_defaults(handler=handle_runtime_job_report)

    runtime_job_progress = runtime_job_sub.add_parser(
        "progress", help="Record a bounded progress checkpoint for a running runtime job"
    )
    runtime_job_progress.add_argument("job_id")
    runtime_job_progress.add_argument("--agent-id", required=True)
    runtime_job_progress.add_argument("--stage")
    runtime_job_progress.add_argument("--summary")
    runtime_job_progress.add_argument("--session-id")
    runtime_job_progress.add_argument("--actor")
    runtime_job_progress.add_argument("--attempt-token", type=int, help="Current attempt_count from claim; rejects stale attempts (8.4.3 P1 #2)")
    runtime_job_progress.add_argument("--lease-id", help="P9-3B managed-attempt lease id; required for leased attempts")
    runtime_job_progress.set_defaults(handler=handle_runtime_job_progress)

    runtime_job_lease = runtime_job_sub.add_parser(
        "lease", help="P9-3B managed attempt lease operations"
    )
    runtime_job_lease_sub = runtime_job_lease.add_subparsers(dest="runtime_job_lease_command")

    runtime_job_lease_renew = runtime_job_lease_sub.add_parser(
        "renew", help="Renew an active managed lease"
    )
    runtime_job_lease_renew.add_argument("job_id")
    runtime_job_lease_renew.add_argument("--agent-id", required=True)
    runtime_job_lease_renew.add_argument("--attempt-token", type=int, required=True)
    runtime_job_lease_renew.add_argument("--lease-id", required=True)
    runtime_job_lease_renew.add_argument("--actor")
    runtime_job_lease_renew.set_defaults(handler=handle_runtime_job_lease_renew)

    runtime_job_lease_reap = runtime_job_lease_sub.add_parser(
        "reap", help="Expire due active leases and make their jobs recoverable"
    )
    runtime_job_lease_reap.add_argument("--actor", default="runtime")
    reap_selector = runtime_job_lease_reap.add_mutually_exclusive_group()
    reap_selector.add_argument("--batch-size", type=int, default=None)
    reap_selector.add_argument("--lease-id")
    runtime_job_lease_reap.add_argument("--job-id")
    runtime_job_lease_reap.set_defaults(handler=handle_runtime_job_lease_reap)

    runtime_executor = runtime_subcommands.add_parser(
        "executor", help="Sync and inspect the executor identity catalog"
    )
    runtime_executor_sub = runtime_executor.add_subparsers(dest="runtime_executor_command")

    runtime_executor_sync = runtime_executor_sub.add_parser(
        "sync", help="Atomically sync the executor catalog from an agent-registry.toml authority"
    )
    runtime_executor_sync.add_argument("--source", required=True, help="Path to agent-registry.toml")
    runtime_executor_sync.set_defaults(handler=handle_runtime_executor_sync)

    runtime_executor_list = runtime_executor_sub.add_parser(
        "list", help="List executor catalog sources, definitions and instance bindings"
    )
    runtime_executor_list.set_defaults(handler=handle_runtime_executor_list)

    runtime_executor_show = runtime_executor_sub.add_parser(
        "show", help="Show the executor binding for one instance id"
    )
    runtime_executor_show.add_argument("instance_id")
    runtime_executor_show.set_defaults(handler=handle_runtime_executor_show)

    runtime_capacity = runtime_subcommands.add_parser(
        "capacity", help="Sync and inspect the capacity catalog"
    )
    runtime_capacity_sub = runtime_capacity.add_subparsers(dest="runtime_capacity_command")

    runtime_capacity_sync = runtime_capacity_sub.add_parser(
        "sync", help="Atomically sync the capacity catalog from an agent-registry.toml authority"
    )
    runtime_capacity_sync.add_argument("--source", required=True, help="Path to agent-registry.toml")
    runtime_capacity_sync.set_defaults(handler=handle_runtime_capacity_sync)

    runtime_capacity_list = runtime_capacity_sub.add_parser(
        "list", help="List capacity sources and policies"
    )
    runtime_capacity_list.set_defaults(handler=handle_runtime_capacity_list)

    runtime_capacity_show = runtime_capacity_sub.add_parser(
        "show", help="Show the capacity policy for one agent id"
    )
    runtime_capacity_show.add_argument("agent_id")
    runtime_capacity_show.set_defaults(handler=handle_runtime_capacity_show)


def handle_runner_add(args: argparse.Namespace) -> int:
    try:
        env = json.loads(args.env_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --env-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(env, dict):
        print("error: --env-json must decode to an object", file=sys.stderr)
        return 1
    with _conn(args) as conn:
        profile = upsert_runner_profile(
            conn,
            profile_id=args.id,
            name=args.name or args.id,
            runner_type=args.runner_type,
            command=args.command,
            working_directory_strategy=args.working_directory_strategy,
            supports_stream_attach=args.supports_stream_attach,
            env=env,
        )
    _print_json({"runner_profile": profile.to_dict()})
    return 0


def handle_runner_list(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        profiles = [profile.to_dict() for profile in list_runner_profiles(conn)]
    _print_json({"runner_profiles": profiles})
    return 0


def handle_runner_examples(args: argparse.Namespace) -> int:
    _print_json({"runner_profile_examples": list_runner_profile_examples()})
    return 0


def handle_runner_example(args: argparse.Namespace) -> int:
    _print_json({"runner_profile_example": get_runner_profile_example(args.id)})
    return 0


def handle_job_create(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --payload-json: {exc}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict):
        print("error: --payload-json must decode to an object", file=sys.stderr)
        return 1
    if args.result_path:
        payload["result_path"] = args.result_path
    with _conn(args) as conn:
        job = create_job(
            conn,
            workspace_id=args.workspace_id,
            task_id=args.task_id,
            runner_profile_id=args.runner_profile_id,
            prompt_path=args.prompt_path,
            branch=args.branch,
            worktree_path=args.worktree_path,
            terminal_session_id=args.terminal_session_id,
            logs_path=args.logs_path,
            timeout_seconds=args.timeout_seconds,
            payload=payload,
        )
        row = row_to_dict(job)
    _print_json({"job": row})
    return 0


def handle_job_list(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        jobs = [
            row_to_dict(row)
            for row in list_jobs(conn, workspace_id=args.workspace_id, status=args.status)
        ]
    _print_json({"jobs": jobs})
    return 0


def handle_job_run(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = run_job(conn, args.job_id)
    _print_json({"result": result.to_dict()})
    return 0


def handle_job_cancel(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = cancel_job(conn, args.job_id, reason=args.reason)
    _print_json({"result": result.to_dict()})
    return 0


def handle_job_retry(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = retry_job(conn, args.job_id, reason=args.reason)
    _print_json({"result": result.to_dict()})
    return 0


def handle_job_pump(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = pump_jobs(conn, workspace_id=args.workspace_id, limit=args.limit)
    _print_json({"result": result.to_dict()})
    return 0


def handle_runtime_agent_register(args: argparse.Namespace) -> int:
    capabilities = json.loads(args.capabilities_json) if args.capabilities_json else None
    with _conn(args) as conn:
        result = register_agent(
            conn,
            agent_id=args.agent_id,
            host_id=args.host_id,
            capabilities=capabilities,
            client_type=args.client_type,
            actor=args.actor,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_runtime_agent_heartbeat(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = heartbeat_agent(
            conn,
            agent_id=args.agent_id,
            host_id=args.host_id,
            actor=args.actor,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_runtime_agent_deactivate(args: argparse.Namespace) -> int:
    agent_id = _validate_id(getattr(args, "agent_id", None), "agent_id")
    host_id = _validate_id(getattr(args, "host_id", None), "host_id")
    reason = _validate_reason(getattr(args, "reason", None), "reason")
    actor = _validate_id(getattr(args, "actor", "runtime"), "actor")
    with _conn(args) as conn:
        result = deactivate_agent(
            conn,
            agent_id=agent_id,
            host_id=host_id,
            reason=reason,
            actor=actor,
            dry_run=getattr(args, "dry_run", False),
        )
    d = result.to_dict()
    _print_json({"result": d})
    if d.get("blocked"):
        return 1
    return 0


def handle_runtime_request_submit(args: argparse.Namespace) -> int:
    worktree_path = getattr(args, "worktree_path", None)
    route_only_flags = {
        "route-capability": args.route_capabilities,
        "route-definition": args.route_definition,
        "preferred-host": args.preferred_host,
        "override-agent": args.override_agent,
        "override-reason": args.override_reason,
    }
    if args.target_agent:
        for name, value in route_only_flags.items():
            if value is not None and value != []:
                print(f"error: --{name} is not allowed with --target-agent", file=sys.stderr)
                return 1
        routing_request = None
    else:
        if worktree_path is not None:
            print("error: --worktree-path requires --target-agent", file=sys.stderr)
            return 1
        if not args.route_capabilities:
            print("error: --route-capability is required", file=sys.stderr)
            return 1
        if (args.override_agent is None) != (args.override_reason is None):
            print(
                "error: --override-agent and --override-reason must be supplied together",
                file=sys.stderr,
            )
            return 1
        if args.override_reason is not None and not args.override_reason.strip():
            print("error: --override-reason must not be blank", file=sys.stderr)
            return 1
        try:
            routing_request = build_routing_request(
                required_capabilities=args.route_capabilities,
                executor_definition_id=args.route_definition,
                preferred_host_id=args.preferred_host,
                operator_override_agent_id=args.override_agent,
                operator_override_reason=args.override_reason,
            )
        except ExecutorRoutingError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    with _conn(args) as conn:
        result = submit_request(
            conn,
            workspace_id=args.workspace_id,
            target_agent=args.target_agent or None,
            prompt=args.prompt,
            origin=json.loads(args.origin_json),
            reply=json.loads(args.reply_json),
            actor=args.actor,
            task_id=args.task_id,
            idempotency_key=args.idempotency_key,
            routing_request=routing_request,
            worktree_path=worktree_path,
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_runtime_job_claim(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = runtime_claim_job(
            conn,
            agent_id=args.agent_id,
            recoverable=args.recoverable,
            recovery_reason=args.recovery_reason,
            prior_process_stopped=args.prior_process_stopped,
            reap_mode=getattr(args, "reap_mode", "global"),
            reap_reason=getattr(args, "reap_reason", None),
        )
    _print_json({"result": result.to_dict()})
    return 0


def handle_runtime_job_report(args: argparse.Namespace) -> int:
    kwargs = {
        "job_id": args.job_id,
        "agent_id": args.agent_id,
        "status": args.status,
        "result": json.loads(args.result_json),
        "actor": args.actor,
        "attempt_token": args.attempt_token,
    }
    lease_id = getattr(args, "lease_id", None)
    if lease_id is not None:
        kwargs["lease_id"] = lease_id
    with _conn(args) as conn:
        result = report_job_result(conn, **kwargs)
    _print_json({"result": result.to_dict()})
    return 0


def handle_runtime_job_progress(args: argparse.Namespace) -> int:
    kwargs = {
        "job_id": args.job_id,
        "agent_id": args.agent_id,
        "stage": args.stage,
        "summary": args.summary,
        "session_id": args.session_id,
        "actor": args.actor,
        "attempt_token": args.attempt_token,
    }
    lease_id = getattr(args, "lease_id", None)
    if lease_id is not None:
        kwargs["lease_id"] = lease_id
    with _conn(args) as conn:
        result = record_job_progress(conn, **kwargs)
    _print_json({"result": result.to_dict()})
    return 0


def handle_runtime_job_lease_renew(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        result = renew_managed_lease(
            conn,
            lease_id=args.lease_id,
            job_id=args.job_id,
            attempt_token=args.attempt_token,
            agent_id=args.agent_id,
        )
    _print_json({"result": result})
    return 0


def handle_runtime_job_lease_reap(args: argparse.Namespace) -> int:
    lease_id = getattr(args, "lease_id", None)
    job_id = getattr(args, "job_id", None)
    batch_size = getattr(args, "batch_size", None)

    # Validate mixed/invalid combinations before opening connection.
    has_exact = lease_id is not None or job_id is not None
    if has_exact and batch_size is not None:
        print("error: --batch-size is mutually exclusive with --lease-id", file=sys.stderr)
        return 1
    if lease_id is not None and job_id is None:
        print("error: --job-id is required with --lease-id", file=sys.stderr)
        return 1
    if lease_id is None and job_id is not None:
        print("error: --lease-id is required with --job-id", file=sys.stderr)
        return 1

    if lease_id is not None and job_id is not None:
        # Exact reap.
        with _conn(args) as conn:
            result = reap_exact_lease(
                conn,
                lease_id=lease_id,
                job_id=job_id,
                actor=getattr(args, "actor", "runtime"),
            )
        _print_json({"result": result})
        return 0

    # Global reap.
    effective_batch = batch_size if batch_size is not None else 100
    with _conn(args) as conn:
        result = reap_due_leases(
            conn, actor=args.actor or "runtime", batch_size=effective_batch
        )
    _print_json({"result": result})
    return 0


def handle_runtime_executor_sync(args: argparse.Namespace) -> int:
    """CLI handler for ``runtime executor sync``."""
    try:
        catalog = parse_executor_catalog(args.source)
    except ExecutorIdentityError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    with _conn(args) as conn:
        try:
            result = sync_executor_catalog(
                conn,
                catalog,
                source_path=args.source,
                synced_by=getattr(args, "synced_by", "operator"),
            )
        except ExecutorIdentityError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        _print_json(result)
    return 0


def handle_runtime_executor_list(args: argparse.Namespace) -> int:
    """CLI handler for ``runtime executor list``."""
    with _conn(args) as conn:
        sources = [
            {
                "source_id": row["source_id"],
                "source_version": row["source_version"],
                "catalog_hash": row["catalog_hash"],
                "updated_at": row["updated_at"],
            }
            for row in list_executor_catalog_sources(conn)
        ]
        definitions = [
            {
                "id": row["id"],
                "source_id": row["source_id"],
                "provider": row["provider"],
                "adapter": row["adapter"],
                "capabilities": json.loads(row["capabilities_json"]),
            }
            for row in list_executor_definitions(conn)
        ]
        bindings = [
            {
                "agent_id": row["agent_id"],
                "source_id": row["source_id"],
                "executor_definition_id": row["executor_definition_id"],
                "runner_profile_id": row["runner_profile_id"],
                "enabled": row["enabled"],
            }
            for row in list_executor_instance_bindings(conn)
        ]
    _print_json(
        {
            "sources": sources,
            "definitions": definitions,
            "bindings": bindings,
        }
    )
    return 0


def handle_runtime_executor_show(args: argparse.Namespace) -> int:
    """CLI handler for ``runtime executor show``."""
    with _conn(args) as conn:
        binding = get_executor_instance_binding(conn, args.instance_id)
        if binding is None:
            print(f"error: unknown executor instance: {args.instance_id}", file=sys.stderr)
            return 1
        snapshot = resolve_exact_executor_binding(conn, args.instance_id)
    _print_json(
        {
            "instance_id": args.instance_id,
            "binding": binding,
            "snapshot": snapshot,
        }
    )
    return 0
