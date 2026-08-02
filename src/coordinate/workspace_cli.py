from __future__ import annotations

import argparse
import json
import sys

from .agent_registry import parse_agents_toml
from .audit import audit_workspace
from .channel_binding_cli import register_workspace_channel_commands
from .cli_support import open_connection, print_json
from .db import (
    get_workspace,
    list_workspace_host_profiles,
    list_workspaces,
    remove_workspace_agent_override,
    set_workspace_agent,
    sync_workspace_agents,
    upsert_workspace,
    upsert_workspace_host_profile,
)
from .doctor import diagnose_workspace
from .harness import HarnessAdapter, HarnessError
from .onboarding import init_file_harness, init_full_harness
from .reconcile import (
    ReconcileConflictError,
    ReconcileTaskNotFoundError,
    reconcile_workspace,
)


# Compatibility aliases so handlers read like the originals.
_conn = open_connection
_print_json = print_json


def handle_workspace_add(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        workspace = upsert_workspace(
            conn,
            workspace_id=args.id,
            name=args.name or args.id,
            path=args.path,
            harness_root=args.harness_root,
            harnessctl_path=args.harnessctl_path,
            default_bus=args.default_bus,
            default_destination=args.default_destination,
            base_branch=args.base_branch,
            branch_namespace=args.branch_namespace,
        )
    _print_json({"workspace": workspace.to_dict()})
    return 0


def handle_workspace_list(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        workspaces = [workspace.to_dict() for workspace in list_workspaces(conn)]
    _print_json({"workspaces": workspaces})
    return 0


def handle_workspace_audit(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        report = audit_workspace(
            conn,
            workspace_id=args.workspace_id,
            refresh=not args.no_refresh,
        )
    _print_json(report.to_dict())
    return 1 if report.drifts or report.mutation_failures else 0


def handle_workspace_doctor(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        workspace = get_workspace(conn, args.workspace_id)
        if workspace is None:
            print(f"error: unknown workspace: {args.workspace_id}", file=sys.stderr)
            return 1
        report = diagnose_workspace(workspace, conn=conn, no_projections=args.no_projections)
    _print_json(report.to_dict())
    healthy = (
        report.harness_mode == "full_harness_runtime"
        and report.checklist_valid is not False
        and report.harnessctl_version_ok is not False
        and report.harnessctl_doctor_ok is not False
        and report.projection_ok is not False
    )
    return 0 if healthy else 1


def handle_workspace_init_harness(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        if args.mode == "full":
            if not args.source:
                print("error: --source is required for full mode", file=sys.stderr)
                return 1
            result = init_full_harness(
                conn,
                workspace_id=args.workspace_id,
                source=args.source,
                dry_run=args.dry_run,
                actor=args.actor,
            )
            _print_json({"result": result.to_dict()})
        else:
            if not args.root:
                print("error: --root is required for minimal mode", file=sys.stderr)
                return 1
            if not args.task_id:
                print("error: --task-id is required for minimal mode", file=sys.stderr)
                return 1
            if not args.plan_doc:
                print("error: --plan-doc is required for minimal mode", file=sys.stderr)
                return 1
            result = init_file_harness(
                conn,
                workspace_id=args.workspace_id,
                root=args.root,
                task_id=args.task_id,
                plan_doc=args.plan_doc,
                title=args.title,
                owner=args.owner,
                status=args.status,
                actor=args.actor,
            )
            _print_json({"result": result.to_dict()})
    return 0


def handle_workspace_agent_add(args: argparse.Namespace) -> int:
    if not args.reason or not args.reason.strip():
        print("error: --reason is required", file=sys.stderr)
        return 1

    with _conn(args) as conn:
        set_workspace_agent(
            conn,
            workspace_id=args.workspace_id,
            agent_name=args.name,
            discord_user_id=args.discord_user_id,
            actor=args.actor,
            reason=args.reason,
            expires_at=args.expires_at,
        )
    _print_json({
        "workspace_id": args.workspace_id,
        "agent_name": args.name,
        "discord_user_id": args.discord_user_id,
        "actor": args.actor,
        "reason": args.reason,
        "expires_at": args.expires_at,
        "status": "registered",
    })
    return 0


def handle_workspace_agent_remove_override(args: argparse.Namespace) -> int:
    if not args.reason or not args.reason.strip():
        print("error: --reason is required", file=sys.stderr)
        return 1

    with _conn(args) as conn:
        try:
            result = remove_workspace_agent_override(
                conn,
                workspace_id=args.workspace_id,
                agent_name=args.name,
                actor=args.actor,
                reason=args.reason,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    _print_json({
        "workspace_id": args.workspace_id,
        "agent_name": result["agent_name"],
        "discord_user_id": result["discord_user_id"],
        "status": "removed",
    })
    return 0


def handle_workspace_agent_sync(args: argparse.Namespace) -> int:
    parsed = parse_agents_toml(args.source)
    if parsed.errors:
        for error in parsed.errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    if parsed.source is None or parsed.source.source_id is None:
        print("error: [registry] id and version are required for authoritative sync", file=sys.stderr)
        return 1

    if not args.replace:
        print("error: --replace is required for authoritative sync", file=sys.stderr)
        return 1

    entries = [
        {
            "id": a.id,
            "display_name": a.display_name,
            "discord_user_id": a.discord_user_id,
            "agent_type": a.agent_type,
        }
        for a in parsed.agents
    ]

    with _conn(args) as conn:
        try:
            result = sync_workspace_agents(
                conn,
                workspace_id=args.workspace_id,
                source_id=parsed.source.source_id,
                source_version=parsed.source.source_version,
                source_hash=parsed.source.source_hash,
                source_path=args.source,
                entries=entries,
                replace=True,
                synced_by="operator",
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    result["skipped"] = parsed.skipped
    result["workspace_id"] = args.workspace_id
    _print_json(result)
    return 0


def handle_workspace_host_profile_set(args: argparse.Namespace) -> int:
    metadata = json.loads(args.metadata_json) if args.metadata_json else {}
    with _conn(args) as conn:
        profile = upsert_workspace_host_profile(
            conn,
            workspace_id=args.workspace_id,
            host_id=args.host_id,
            workspace_path=args.workspace_path,
            harness_root=args.harness_root,
            harnessctl_path=args.harnessctl_path,
            coordinator_cli_path=args.coordinator_cli_path,
            coordinator_db_path=args.coordinator_db_path,
            shell=args.shell,
            metadata=metadata,
        )
    _print_json({"result": profile.to_dict()})
    return 0


def handle_workspace_host_profile_list(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        profiles = list_workspace_host_profiles(conn, workspace_id=args.workspace_id)
    _print_json({"profiles": [profile.to_dict() for profile in profiles]})
    return 0


def handle_state(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        workspace = get_workspace(conn, args.workspace_id)
    if workspace is None:
        print(f"error: unknown workspace: {args.workspace_id}", file=sys.stderr)
        return 1
    adapter = HarnessAdapter(workspace)
    if args.no_refresh:
        # Diagnostic only: never authoritative; stale is a nonzero report.
        state, fresh, reasons = adapter.read_state_diagnostic()
        output = {
            "workspace": workspace.to_dict(),
            "state": state,
            "authoritative": fresh,
            "stale_reasons": reasons,
        }
        _print_json(output)
        return 0 if fresh else 1
    try:
        state = adapter.refresh_state()
    except HarnessError as exc:
        _print_json({
            "error": {"message": str(exc)},
            "workspace": workspace.to_dict(),
        })
        return 1
    _print_json({"workspace": workspace.to_dict(), "state": state})
    return 0


def handle_reconcile(args: argparse.Namespace) -> int:
    with _conn(args) as conn:
        workspace = get_workspace(conn, args.workspace_id)
        if workspace is None:
            print(f"error: unknown workspace: {args.workspace_id}", file=sys.stderr)
            return 1
        try:
            result = reconcile_workspace(
                conn,
                workspace,
                refresh=not args.no_refresh,
                task_id=args.task_id,
            )
        except (ReconcileTaskNotFoundError, ReconcileConflictError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    _print_json({"reconciliation": result.to_dict()})
    return 0


def register_workspace_commands(subcommands) -> None:
    """Register the workspace and state parsers in their canonical positions."""
    workspace = subcommands.add_parser("workspace", help="Manage harness workspaces")
    workspace_subcommands = workspace.add_subparsers(dest="workspace_command")

    workspace_add = workspace_subcommands.add_parser("add", help="Register or update a harness workspace")
    workspace_add.add_argument("id")
    workspace_add.add_argument("--name")
    workspace_add.add_argument("--path", required=True)
    workspace_add.add_argument("--harness-root", required=True)
    workspace_add.add_argument("--harnessctl-path")
    workspace_add.add_argument("--default-bus")
    workspace_add.add_argument("--default-destination")
    workspace_add.add_argument("--base-branch")
    workspace_add.add_argument("--branch-namespace")
    workspace_add.set_defaults(handler=handle_workspace_add)

    workspace_list = workspace_subcommands.add_parser("list", help="List registered workspaces")
    workspace_list.set_defaults(handler=handle_workspace_list)

    workspace_audit = workspace_subcommands.add_parser("audit", help="Audit workspace for drift between coordinator and harness state")
    workspace_audit.add_argument("workspace_id")
    workspace_audit.add_argument("--no-refresh", action="store_true", help="Read existing harness files without running harnessctl state")
    workspace_audit.set_defaults(handler=handle_workspace_audit)

    workspace_doctor = workspace_subcommands.add_parser("doctor", help="Diagnose workspace harness health and capability level")
    workspace_doctor.add_argument("workspace_id")
    workspace_doctor.add_argument(
        "--no-projections",
        action="store_true",
        help="Skip S4-D projection diagnostics (compatibility escape hatch; not for gates).",
    )
    workspace_doctor.set_defaults(handler=handle_workspace_doctor)

    workspace_init_harness = workspace_subcommands.add_parser("init-harness", help="Initialize harness for a workspace")
    workspace_init_harness.add_argument("workspace_id")
    workspace_init_harness.add_argument("--root", help="Harness root path (required for minimal mode, relative to workspace path unless absolute)")
    workspace_init_harness.add_argument("--task-id", help="Task ID (required for minimal mode)")
    workspace_init_harness.add_argument("--plan-doc", help="Plan document path (required for minimal mode, relative to workspace path unless absolute)")
    workspace_init_harness.add_argument("--title")
    workspace_init_harness.add_argument("--owner", default="worker")
    workspace_init_harness.add_argument("--status", default="ready")
    workspace_init_harness.add_argument("--actor", default="operator")
    workspace_init_harness.add_argument("--mode", choices=["minimal", "full"], default="minimal", help="Init mode: minimal (file-backed) or full (copies harness runtime from --source)")
    workspace_init_harness.add_argument("--source", help="Source directory for harness runtime scripts (required for full mode)")
    workspace_init_harness.add_argument("--dry-run", action="store_true", help="Show what would be created without writing files")
    workspace_init_harness.set_defaults(handler=handle_workspace_init_harness)

    workspace_agent = workspace_subcommands.add_parser("agent", help="Manage agent registry for a workspace")
    workspace_agent_sub = workspace_agent.add_subparsers(dest="workspace_agent_command")
    workspace_agent_add = workspace_agent_sub.add_parser("add", help="Register or update an agent override")
    workspace_agent_add.add_argument("workspace_id")
    workspace_agent_add.add_argument("--name", required=True, help="Agent name (e.g. mac-codex)")
    workspace_agent_add.add_argument("--discord-user-id", required=True, help="Discord user ID for this agent")
    workspace_agent_add.add_argument("--reason", required=True, help="Non-empty reason for the override")
    workspace_agent_add.add_argument("--actor", default="operator", help="Actor performing the override")
    workspace_agent_add.add_argument("--expires-at", help="Optional UTC expiry (YYYY-MM-DDTHH:MM:SSZ)")
    workspace_agent_add.set_defaults(handler=handle_workspace_agent_add)

    workspace_agent_remove = workspace_agent_sub.add_parser("remove-override", help="Remove an agent override")
    workspace_agent_remove.add_argument("workspace_id")
    workspace_agent_remove.add_argument("--name", required=True, help="Agent name")
    workspace_agent_remove.add_argument("--reason", required=True, help="Non-empty reason for the removal")
    workspace_agent_remove.add_argument("--actor", default="operator", help="Actor performing the removal")
    workspace_agent_remove.set_defaults(handler=handle_workspace_agent_remove_override)

    workspace_agent_sync = workspace_agent_sub.add_parser("sync", help="Sync agent registry from agents.toml")
    workspace_agent_sync.add_argument("workspace_id")
    workspace_agent_sync.add_argument("--source", required=True, help="Path to agents.toml")
    workspace_agent_sync.add_argument("--replace", action="store_true", help="Replace entire registry instead of merging")
    workspace_agent_sync.set_defaults(handler=handle_workspace_agent_sync)

    workspace_host = workspace_subcommands.add_parser("host-profile", help="Manage per-host workspace execution paths")
    workspace_host_sub = workspace_host.add_subparsers(dest="workspace_host_command")

    workspace_host_set = workspace_host_sub.add_parser("set", help="Register or update a host execution profile")
    workspace_host_set.add_argument("workspace_id")
    workspace_host_set.add_argument("--host-id", required=True)
    workspace_host_set.add_argument("--workspace-path", required=True)
    workspace_host_set.add_argument("--harness-root")
    workspace_host_set.add_argument("--harnessctl-path")
    workspace_host_set.add_argument("--coordinator-cli-path")
    workspace_host_set.add_argument("--coordinator-db-path")
    workspace_host_set.add_argument("--shell")
    workspace_host_set.add_argument("--metadata-json", default="{}")
    workspace_host_set.set_defaults(handler=handle_workspace_host_profile_set)

    workspace_host_list = workspace_host_sub.add_parser("list", help="List host execution profiles for a workspace")
    workspace_host_list.add_argument("workspace_id")
    workspace_host_list.set_defaults(handler=handle_workspace_host_profile_list)

    register_workspace_channel_commands(workspace_subcommands)

    state = subcommands.add_parser("state", help="Refresh and print harness state for a workspace")
    state.add_argument("workspace_id")
    state.add_argument("--no-refresh", action="store_true", help="Read harness-state.json without running harnessctl state")
    state.set_defaults(handler=handle_state)


def register_reconcile_command(subcommands) -> None:
    """Register the reconcile parser in its canonical position."""
    reconcile = subcommands.add_parser("reconcile", help="Sync coordinator task mirror from harness state")
    reconcile.add_argument("workspace_id")
    reconcile.add_argument("--no-refresh", action="store_true", help="Read state without running harnessctl state")
    reconcile.add_argument("--task-id", help="Reconcile only this task mirror (targeted); full reconcile when omitted")
    reconcile.set_defaults(handler=handle_reconcile)
