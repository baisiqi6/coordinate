from __future__ import annotations

import shlex
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import (
    Workspace,
    WorkspaceHostProfile,
    append_event,
    get_agent_discord_id,
    get_workspace_host_profile,
    get_workspace,
    row_to_dict,
)
from .harness import HarnessAdapter, HarnessError


@dataclass(frozen=True)
class HandoffPreparedResult:
    workspace: Workspace
    task: dict[str, Any]
    handoff_text: str
    bootstrap_text: str
    bootstrap_recommended_path: str
    event: dict[str, Any]
    event_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace.to_dict(),
            "task": self.task,
            "handoff_text": self.handoff_text,
            "bootstrap_text": self.bootstrap_text,
            "bootstrap_recommended_path": self.bootstrap_recommended_path,
            "event": self.event,
            "event_created": self.event_created,
        }


def _require_workspace(conn: sqlite3.Connection, workspace_id: str) -> Workspace:
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    return workspace


def _require_task(conn: sqlite3.Connection, workspace_id: str, task_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if row is None:
        raise ValueError(f"task mirror not found: {workspace_id}/{task_id}")
    return row_to_dict(row)


def _contains_task_id(value: Any, task_id: str) -> bool:
    if isinstance(value, dict):
        if value.get("id") == task_id or value.get("task_id") == task_id:
            return True
        return any(_contains_task_id(child, task_id) for child in value.values())
    if isinstance(value, list):
        return any(_contains_task_id(child, task_id) for child in value)
    return False


def _require_harness_task(workspace: Workspace, task_id: str) -> None:
    adapter = HarnessAdapter(workspace)
    try:
        # The state file is a runtime summary and may not enumerate every
        # checklist item. Read it to ensure the harness is usable, but use the
        # canonical checklist for task existence.
        adapter.read_state()
        checklist = adapter.read_checklist()
    except (HarnessError, OSError, ValueError) as exc:
        raise ValueError(
            f"workspace harness preflight failed for {workspace.id}/{task_id}: {exc}"
        ) from exc

    if not _contains_task_id(checklist, task_id):
        raise ValueError(
            f"workspace harness checklist does not contain task '{task_id}' "
            f"for {workspace.id}; refusing to generate worker handoff"
        )


def _require_latest_gate_approved(conn: sqlite3.Connection, workspace_id: str, task_id: str, required_scope: str, role: str = "worker") -> dict[str, Any]:
    # Reviewer handoffs skip the plan gate: for code review the plan is
    # already approved; for plan review the reviewer IS the gate.
    if role == "reviewer":
        return {"event_type": "plan.approved", "scope": required_scope}

    rows = conn.execute(
        "SELECT * FROM events WHERE workspace_id = ? AND task_id = ? AND event_type IN ('plan.approved', 'plan.rejected') AND json_extract(payload_json, '$.scope') = ? ORDER BY rowid DESC LIMIT 1",
        (workspace_id, task_id, required_scope),
    ).fetchall()
    if not rows:
        raise ValueError(f"no plan gate event with scope '{required_scope}' found for {workspace_id}/{task_id}; approve the plan with this scope before generating worker handoff")
    gate = row_to_dict(rows[0])
    if gate["event_type"] != "plan.approved":
        raise ValueError(f"latest gate decision for {workspace_id}/{task_id} scope '{required_scope}' is {gate['event_type']}, not plan.approved; approve the plan before generating worker handoff")
    return gate


def _latest_plan_ready(conn: sqlite3.Connection, workspace_id: str, task_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM events WHERE workspace_id = ? AND task_id = ? AND event_type = 'plan.ready' ORDER BY rowid DESC LIMIT 1",
        (workspace_id, task_id),
    ).fetchone()
    return row_to_dict(row) if row else None


def _plan_ready_by_id(conn: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM events WHERE id = ?",
        (event_id,),
    ).fetchone()
    return row_to_dict(row) if row else None


def _build_handoff_text(
    *,
    workspace: Workspace,
    task: dict[str, Any],
    plan_payload: dict[str, Any],
    role: str,
    execution_profile: WorkspaceHostProfile | None = None,
) -> str:
    task_id = task.get("task_id", "unknown")
    title = plan_payload.get("title") or task_id
    source_plan = plan_payload.get("plan_doc") or plan_payload.get("absolute_plan_doc") or "No source plan recorded"
    branch = task.get("branch") or workspace.base_branch or "unknown"
    phase = task.get("phase") or "unknown"
    owner = task.get("owner") or "unassigned"
    non_goals = plan_payload.get("non_goals") or "No non-goals specified. See source plan for scope boundaries."
    test_baseline = plan_payload.get("test_baseline") or "No validation baseline recorded. See source plan for acceptance criteria."

    harness_rel = _harness_root_relative(workspace)
    checklist_rel = _resolved_checklist_relative(workspace)
    profile = _materialize_handoff_profile(workspace, execution_profile)
    workspace_path = profile["workspace_path"]
    execution_harness = profile["harness_root"]
    execution_source_plan = _path_for_execution_host(
        source_plan,
        workspace=workspace,
        workspace_path=workspace_path,
    )

    context_lines = [
        "",
        "### Execution Context",
        "context_version=1",
        f"workspace_path={shlex.quote(workspace_path)}",
        f"harness_root={shlex.quote(execution_harness)}",
    ]
    if branch and branch != "unknown":
        context_lines.append(f"branch={shlex.quote(branch)}")
    context_section = "\n".join(context_lines) + "\n"

    return (
        f"## Worker Handoff: {task_id}\n\n"
        f"### Context Recovery\n"
        f"- Workspace: {workspace_path}\n"
        f"- Branch: {branch}\n"
        f"- Source Plan: {execution_source_plan}\n"
        f"- Harness State: phase={phase}, owner={owner}\n\n"
        f"Recovery commands:\n"
        f"```bash\n"
        f"cd {workspace_path}\n"
        f"git status --short\n"
        f"git branch --show-current\n"
        f"git log --oneline -8\n"
        f"cat {harness_rel}/harness-state.json\n"
        f"cat {shlex.quote(checklist_rel)}\n"
        f"cat {execution_source_plan}\n"
        f"```\n\n"
        f"### Implementation Scope\n"
        f"{title}\n\n"
        f"### Non-Goals\n"
        f"{non_goals}\n\n"
        f"### Validation Commands\n"
        f"{test_baseline}\n\n"
        f"### Return Format\n"
        f"Report: changes made, test results, remaining risks, and files modified.\n"
        f"If blocked, describe the blocker and what you need to proceed.\n\n"
        f"### Constraints\n"
        f"- Human gate required: no merge without explicit approval\n"
        f"- No deploy without explicit approval\n"
        f"- Do not modify files outside scope without asking first\n"
        + context_section
    )


def _harness_root_relative(workspace: Workspace) -> str:
    try:
        return str(Path(workspace.harness_root).relative_to(Path(workspace.path)))
    except ValueError:
        return str(workspace.harness_root)


def _resolved_checklist_relative(workspace: Workspace) -> str:
    """Workspace-relative path of the single resolver-selected checklist.

    Preflight has already passed, so the resolver picks exactly one authority
    (new-only/legacy-only); never render a compat candidate that does not exist.
    """
    from .checklist_io import ChecklistError, resolve_checklist

    try:
        resolved = resolve_checklist(workspace.harness_root, purpose="read")
    except ChecklistError as exc:
        # Unreachable in the normal path (preflight already resolved); keep the
        # renderer total instead of crashing on a mid-flight authority change.
        raise ValueError(
            f"cannot resolve the checklist for handoff recovery commands: {exc}"
        ) from exc
    try:
        return str(resolved.path.relative_to(Path(workspace.path)))
    except ValueError:
        return str(resolved.path)


def _materialize_handoff_profile(
    workspace: Workspace,
    execution_profile: WorkspaceHostProfile | None,
) -> dict[str, str]:
    """Return the canonical host workspace_path and harness_root for handoff rendering.

    When the host profile does not specify a harness_root, fall back to the
    control-plane harness root mapped under the host workspace path. The result
    is always a complete, host-native absolute pair.
    """
    harness_rel = _harness_root_relative(workspace)
    workspace_path = execution_profile.workspace_path if execution_profile else workspace.path
    execution_harness = (
        execution_profile.harness_root
        if execution_profile and execution_profile.harness_root
        else _join_foreign_path(workspace_path, harness_rel)
    )
    return {"workspace_path": workspace_path, "harness_root": execution_harness}


def _join_foreign_path(root: str, relative_path: str) -> str:
    normalized_rel = relative_path.replace("\\", "/").lstrip("/")
    if not normalized_rel:
        return root
    separator = "\\" if ("\\" in root or (len(root) >= 2 and root[1] == ":")) else "/"
    return root.rstrip("\\/") + separator + normalized_rel.replace("/", separator)


def _path_for_execution_host(path_text: str, *, workspace: Workspace, workspace_path: str) -> str:
    """Map a control-plane absolute workspace path into a target host path.

    Relative paths are already portable inside the repo and are left unchanged.
    """
    if not path_text:
        return path_text
    try:
        source = Path(path_text)
        control_root = Path(workspace.path)
        if not source.is_absolute():
            return path_text
        relative = source.resolve().relative_to(control_root.resolve())
    except (OSError, ValueError):
        return path_text
    return _join_foreign_path(workspace_path, str(relative))


def _agent_host_id(conn: sqlite3.Connection, agent_id: str) -> str | None:
    row = conn.execute(
        "SELECT host_id FROM agents WHERE id = ?",
        (agent_id,),
    ).fetchone()
    if row is None:
        return None
    return row["host_id"]


def _execution_profile_for_target(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    target_agent: str | None,
) -> WorkspaceHostProfile | None:
    if not target_agent:
        return None
    host_id = _agent_host_id(conn, target_agent)
    if not host_id:
        return None
    profile = get_workspace_host_profile(conn, workspace_id=workspace_id, host_id=host_id)
    if profile is None:
        raise ValueError(
            f"target agent '{target_agent}' is registered on host '{host_id}', "
            f"but workspace '{workspace_id}' has no execution profile for that host; "
            "run `workspace host-profile set` before generating handoff"
        )
    return profile


def _coordinator_cli_block(
    *,
    workspace_id: str,
    profile: WorkspaceHostProfile | None,
    db_path: str | None,
    coordinator_path: str | None,
) -> str:
    if profile and profile.coordinator_cli_path:
        lines = [f"{profile.coordinator_cli_path} <command> {workspace_id} [options]"]
        if profile.coordinator_db_path:
            lines.insert(0, f"# coordinator DB is configured by host profile: {profile.coordinator_db_path}")
        return "\n".join(lines)

    resolved_db = db_path or "<db-path>"
    resolved_coord = coordinator_path or "<coordinator-path>"
    return (
        f"cd {resolved_coord}\n"
        f"PYTHONPATH=src python3 -m coordinate --db {resolved_db} <command> {workspace_id} [options]"
    )


def _build_worker_bootstrap(
    *,
    workspace: Workspace,
    task: dict[str, Any],
    plan_payload: dict[str, Any],
    db_path: str | None,
    coordinator_path: str | None,
    execution_profile: WorkspaceHostProfile | None = None,
) -> str:
    # #12.4: split into an ordered section pipeline. Each section is rendered
    # by a dedicated _render_worker_* helper off a resolved context, so the
    # main function reads as a list of sections rather than one ~155-line
    # f-string. Output is byte-for-byte identical to the former monolith.
    ctx = _resolve_worker_bootstrap_context(
        workspace=workspace,
        task=task,
        plan_payload=plan_payload,
        db_path=db_path,
        coordinator_path=coordinator_path,
        execution_profile=execution_profile,
    )
    return (
        f"# Worker Bootstrap: {ctx.task_id}\n\n"
        + _render_worker_session_startup(ctx)
        + _render_worker_assignment(ctx)
        + _render_worker_coordinator_cli(ctx)
        + _render_worker_implementation_protocol()
        + _render_worker_visible_discord(ctx)
        + _render_worker_self_test(ctx)
        + _render_worker_session_end(ctx)
        + _render_worker_constraints()
    )


@dataclass(frozen=True)
class _WorkerBootstrapContext:
    """Resolved scalars used by the worker-bootstrap section renderers.

    All paths are already mapped to the execution host (workspace_path /
    harness_root / source_plan) by ``_resolve_worker_bootstrap_context``.
    """

    task_id: str
    title: str
    branch: str
    ws_id: str
    execution_workspace_path: str
    execution_harness: str
    execution_source_plan: str
    coordinator_cli: str


def _resolve_worker_bootstrap_context(
    *,
    workspace: Workspace,
    task: dict[str, Any],
    plan_payload: dict[str, Any],
    db_path: str | None,
    coordinator_path: str | None,
    execution_profile: WorkspaceHostProfile | None = None,
) -> _WorkerBootstrapContext:
    source_plan = plan_payload.get("plan_doc") or plan_payload.get("absolute_plan_doc") or "No source plan recorded"
    harness_rel = _harness_root_relative(workspace)
    execution_workspace_path = execution_profile.workspace_path if execution_profile else workspace.path
    execution_harness = (
        execution_profile.harness_root
        if execution_profile and execution_profile.harness_root
        else harness_rel
    )
    execution_source_plan = _path_for_execution_host(
        source_plan,
        workspace=workspace,
        workspace_path=execution_workspace_path,
    )
    return _WorkerBootstrapContext(
        task_id=task.get("task_id", "unknown"),
        title=plan_payload.get("title") or task.get("task_id", "unknown"),
        branch=task.get("branch") or workspace.base_branch or "unknown",
        ws_id=workspace.id,
        execution_workspace_path=execution_workspace_path,
        execution_harness=execution_harness,
        execution_source_plan=execution_source_plan,
        coordinator_cli=_coordinator_cli_block(
            workspace_id=workspace.id,
            profile=execution_profile,
            db_path=db_path,
            coordinator_path=coordinator_path,
        ),
    )


def _render_worker_session_startup(ctx: _WorkerBootstrapContext) -> str:
    return (
        f"## Session Startup\n\n"
        f"### Step 1: Confirm working directory\n\n"
        f"```bash\npwd\n```\n\n"
        f"You should be at `{ctx.execution_workspace_path}`. If not, `cd {ctx.execution_workspace_path}`.\n\n"
        f"### Step 2: Check workspace state (read-only)\n\n"
        f"```bash\n"
        f"git status --short\n"
        f"git branch --show-current\n"
        f"git log --oneline -10\n"
        f"```\n\n"
        f"Rule: do not overwrite/revert changes that are not yours. "
        f"If you find unrelated dirty files, log them but do not clean up.\n\n"
        f"Shared-worktree guard: this checkout may be used by other agents. "
        f"If `pwd` is not `{ctx.execution_workspace_path}` or `git branch --show-current` is not `{ctx.branch}`, "
        f"stop and report a blocker instead of switching branches.\n"
        f"Never run `git reset`, `git rebase`, `git checkout`, `git switch`, `git cherry-pick`, "
        f"or `git push --force` to repair this workspace unless the operator explicitly asks you to.\n\n"
        f"### Step 3: Read project state\n\n"
        f"Read these files:\n"
        f"- `{ctx.execution_harness}/harness-state.json` — current_item, checklist_summary, recent_events\n"
        f"- `{ctx.execution_harness}/progress.md` — recent session logs\n\n"
        f"### Step 4: Read project boundaries\n\n"
        f"- `{ctx.execution_harness}/scope.md` — goals, non-goals, constraints\n"
        f"- `{ctx.execution_harness}/architecture.md` — module boundaries\n"
        f"- `{ctx.execution_harness}/domain-model.md` — core entities\n\n"
        f"### Step 5: Read assigned task plan\n\n"
        f"```bash\ncat {ctx.execution_source_plan}\n```\n\n"
        f"Follow this plan step by step.\n\n"
    )


def _render_worker_assignment(ctx: _WorkerBootstrapContext) -> str:
    return (
        f"## Your Assignment\n\n"
        f"- **Task**: {ctx.task_id}\n"
        f"- **Title**: {ctx.title}\n"
        f"- **Branch**: {ctx.branch}\n"
        f"- **Plan**: {ctx.execution_source_plan}\n"
        f"- **Phase**: approved\n\n"
    )


def _render_worker_coordinator_cli(ctx: _WorkerBootstrapContext) -> str:
    return (
        f"## Coordinator CLI\n\n"
        f"All state changes MUST go through coordinator CLI.\n"
        f"Do NOT call harnessctl directly.\n"
        f"Do NOT modify harness JSON files directly.\n"
        f"harnessctl is only for operator/harness repair.\n\n"
        f"```bash\n"
        f"{ctx.coordinator_cli}\n"
        f"```\n\n"
        f"Commands:\n"
        f"- `assignment accept {ctx.ws_id} --task-id <id> --owner <agent> --session <sid>`\n"
        f"- `branch allocate {ctx.ws_id} --task-id <id> --owner <agent>`\n"
        f"- `pr link {ctx.ws_id} --task-id <id> --pr-url <url>`\n"
        f"- `ci check {ctx.ws_id} --task-id <id>`\n"
        f"- `merge gate {ctx.ws_id} --task-id <id>`\n"
        f"- `assignment closeout {ctx.ws_id} --task-id <id> --reviewer <name>`\n"
        f"- `assignment mark-done {ctx.ws_id} --task-id <id>`\n\n"
    )


def _render_worker_implementation_protocol() -> str:
    return (
        f"## Implementation Protocol\n\n"
        f"- Work on ONE feature at a time\n"
        f"- Commit with descriptive messages after each logical change\n"
        f"- Run tests after every change\n"
        f"- Update progress.md with what you did\n\n"
    )


def _render_worker_visible_discord(ctx: _WorkerBootstrapContext) -> str:
    return (
        f"## Visible Discord Updates\n\n"
        f"You, the worker agent, own execution updates in Discord. The coordinator should stay as the control plane; "
        f"do not rely on coordinator event echoes as the human-readable collaboration thread.\n\n"
        f"Send concise human-readable updates in the channel at these points:\n"
        f"- **Start**: say you accepted `{ctx.task_id}` and list the 2-3 concrete steps you will do first.\n"
        f"- **Milestone**: when a meaningful sub-step is complete, summarize what changed, tests run, and next step.\n"
        f"- **Blocker**: if you need operator/reviewer input, mention `@Coordinator`, `@Codex`, or the assigned reviewer/operator if visible in the channel.\n"
        f"- **Done / review needed**: mention `@Coordinator` and `@Codex` (or the assigned reviewer) with changed files, tests, risks, and review request.\n\n"
        f"Keep each visible update short. Do not stream private reasoning or every command.\n"
        f"Each progress/blocker/done update should end with one machine-readable block so coordinator can ingest it:\n\n"
        f"```text\n"
        f"[agent-report]\n"
        f"action=progress\n"
        f"workspace_id={ctx.ws_id}\n"
        f"task_id={ctx.task_id}\n"
        f"summary=\"Completed <milestone>; tests: <result>; next: <next step>\"\n\n"
        f"[agent-report]\n"
        f"action=blocker\n"
        f"workspace_id={ctx.ws_id}\n"
        f"task_id={ctx.task_id}\n"
        f"reason=\"Need <decision/input>\"\n\n"
        f"[agent-report]\n"
        f"action=done\n"
        f"workspace_id={ctx.ws_id}\n"
        f"task_id={ctx.task_id}\n"
        f"summary=\"Implemented <scope>; tests: <result>; risks: <risk-or-none>\"\n"
        f"```\n\n"
        f"Use exactly one report block per visible update. The `[agent-report]` marker must start at the beginning of its own line.\n\n"
    )


def _render_worker_self_test(ctx: _WorkerBootstrapContext) -> str:
    return (
        f"## Self-Test Before Closeout\n\n"
        f"**Rule**: before requesting closeout, you MUST self-test your changes. When server, "
        f"daemon, or bridge code changes and the task carries explicit deployment authority, "
        f"follow the project's reviewed deployment runbook and run an authorized environment "
        f"smoke. Without deployment authority, record deployment as pending instead of mutating "
        f"an environment. Closeout without self-test evidence hides "
        f"integration bugs — unit tests alone cannot catch daemon/bridge long-process defects "
        f"(phase-8.5 KeyError / phase-8.6 dedup precedent).\n\n"
        f"### When deploy is required\n\n"
        f"- **Server / daemon / bridge code changes with explicit deployment authority** → "
        f"project deployment runbook + authorized environment smoke through the new code path\n"
        f"- **The task has no deployment authority** → do not deploy; record the pending "
        f"deployment/smoke boundary in self-test evidence\n"
        f"- **Pure doc / test / config changes** → skip deploy; self-test = run the full test suite\n\n"
        f"### Deploy + environment smoke\n\n"
        f"Use the deployment command and target named by the project's reviewed runbook and "
        f"current task authority. Never infer production-write authority from this bootstrap.\n\n"
        f"### Self-test evidence\n\n"
        f"When calling `assignment closeout`, fill `--self-test-evidence` with what you did:\n\n"
        f"```bash\n"
        f"assignment closeout {ctx.ws_id} --task-id {ctx.task_id} --reviewer <reviewer> \\\n"
        f"  --self-test-evidence \"Deploy SHA: <sha>; E2E: <result>; Bugs found: <list or none>\"\n"
        f"```\n\n"
        f"**Empty `--self-test-evidence` → reviewer will see a warning. Do not skip this step.**\n\n"
        f"### Cross-repo coordination\n\n"
        f"If this task spans multiple repositories, verify the correct branch in each:\n"
        f"- Primary: `{ctx.execution_workspace_path}` branch `{ctx.branch}`\n"
        f"- Coordinate (if applicable): `<coordinate-checkout>` — confirm which branch "
        f"the coordinator handoff/bootstrap code lives on before modifying it\n\n"
    )


def _render_worker_session_end(ctx: _WorkerBootstrapContext) -> str:
    return (
        f"## Session End Protocol\n\n"
        f"1. Run tests to verify clean state\n"
        f"2. Update progress.md with session summary\n"
        f"3. For implementation tasks, run `assignment closeout {ctx.ws_id} --task-id {ctx.task_id} --reviewer <reviewer>`; "
        f"do not mark your own implementation done\n"
        f"4. Commit only task-relevant changes — do not commit secrets, local config, or generated noise\n"
        f"5. Your final visible Discord message MUST include exactly one parseable `[agent-report]` block with `action=done`; "
        f"natural-language completion alone is not enough for the operator\n"
        f"6. Report: what changed, test results, remaining risks, files modified\n\n"
    )


def _render_worker_constraints() -> str:
    return (
        f"## Constraints\n\n"
        f"- Human gate: no merge without explicit approval\n"
        f"- No deploy without approval\n"
        f"- No out-of-scope changes without asking\n"
        f"- If stuck 3+ attempts on the same issue: stop and report blocker via coordinator CLI\n"
    )


@dataclass(frozen=True)
class _ReviewerBootstrapContext:
    """Resolved scalars for reviewer-bootstrap section renderers.

    For plan review, paths are resolved against the control-plane workspace
    (where the plan lives), not the host execution_profile worktree (which may
    be a stale per-task worktree that doesn't contain this task's plan).
    """

    task_id: str
    title: str
    branch: str
    ws_id: str
    is_plan_review: bool
    execution_workspace_path: str
    execution_harness: str
    execution_source_plan: str
    harness_rel: str
    acceptance_criteria: str


def _resolve_reviewer_bootstrap_context(
    *,
    workspace: Workspace,
    task: dict[str, Any],
    plan_payload: dict[str, Any],
    execution_profile: WorkspaceHostProfile | None = None,
    review_type: str = "code",
) -> _ReviewerBootstrapContext:
    source_plan = plan_payload.get("plan_doc") or plan_payload.get("absolute_plan_doc") or "No source plan recorded"
    harness_rel = _harness_root_relative(workspace)
    is_plan_review = review_type == "plan"

    if is_plan_review:
        # Plan review is read-only and evaluates the plan document itself.
        # Resolve doc paths against the control-plane workspace (where the plan
        # lives), NOT the host execution_profile worktree (which may be a stale
        # per-task worktree that doesn't contain this task's plan).
        execution_workspace_path = workspace.path
        execution_harness = harness_rel
    else:
        execution_workspace_path = execution_profile.workspace_path if execution_profile else workspace.path
        execution_harness = (
            execution_profile.harness_root
            if execution_profile and execution_profile.harness_root
            else harness_rel
        )

    execution_source_plan = _path_for_execution_host(
        source_plan,
        workspace=workspace,
        workspace_path=execution_workspace_path,
    )
    return _ReviewerBootstrapContext(
        task_id=task.get("task_id", "unknown"),
        title=plan_payload.get("title") or task.get("task_id", "unknown"),
        branch=task.get("branch") or workspace.base_branch or "unknown",
        ws_id=workspace.id,
        is_plan_review=is_plan_review,
        execution_workspace_path=execution_workspace_path,
        execution_harness=execution_harness,
        execution_source_plan=execution_source_plan,
        harness_rel=harness_rel,
        acceptance_criteria=plan_payload.get("test_baseline") or "No acceptance criteria recorded. See source plan.",
    )


def _build_reviewer_bootstrap(
    *,
    workspace: Workspace,
    task: dict[str, Any],
    plan_payload: dict[str, Any],
    execution_profile: WorkspaceHostProfile | None = None,
    review_type: str = "code",
) -> str:
    # #12.5: split into context resolution + plan/code section renderers.
    # Each section is rendered by a dedicated helper; the main function reads
    # as a list of sections. Output is byte-for-byte identical to the former
    # monolith.
    ctx = _resolve_reviewer_bootstrap_context(
        workspace=workspace,
        task=task,
        plan_payload=plan_payload,
        execution_profile=execution_profile,
        review_type=review_type,
    )
    return (
        f"# Reviewer Bootstrap: {ctx.task_id}\n\n"
        f"## Session Startup\n\n"
        f"{_render_reviewer_session_startup(ctx)}"
        f"{_render_reviewer_assignment(ctx)}"
        f"## Acceptance Criteria\n\n"
        f"{ctx.acceptance_criteria}\n\n"
        f"{_render_reviewer_self_test(ctx)}"
        f"{_render_reviewer_focus(ctx)}"
        f"{_render_reviewer_output_format(ctx)}"
        f"{_render_reviewer_constraints_block()}"
    )


def _render_reviewer_session_startup(ctx: _ReviewerBootstrapContext) -> str:
    """Session startup section — plan review is read-only (no worktree guard),
    code review pins to the execution worktree with branch guard."""
    if ctx.is_plan_review:
        return (
            f"### Step 1: Locate the plan document (read-only, in your local repo)\n\n"
            f"This is a **plan review** — you evaluate the plan/spec, not code. "
            f"There is no implementation yet, so no task worktree or branch to guard. "
            f"In your local checkout of the workspace repo, read:\n\n"
            f"```bash\n"
            f"# primary entry (relative — works on any host's checkout):\n"
            f"cat openspec/changes/{ctx.task_id}/proposal.md\n"
            f"# also review: design.md, specs/*/spec.md, tasks.md in that dir,\n"
            f"# and any docs/superpowers/plans/*-{ctx.task_id}*.md implementation plan\n"
            f"```\n\n"
            f"Control-plane recorded the plan at `{ctx.execution_source_plan}` (server path, "
            f"for reference only — read your LOCAL repo, not the server path). "
            f"Do NOT switch branches, create worktrees, or modify any file — plan review is strictly read-only.\n\n"
            f"### Step 2: Read project boundaries (read-only, relative paths)\n\n"
            f"- `{ctx.harness_rel}/scope.md` — goals, non-goals, constraints\n"
            f"- `{ctx.harness_rel}/architecture.md` — module boundaries\n"
            f"- `{ctx.harness_rel}/domain-model.md` — core entities\n\n"
        )
    return (
        f"### Step 1: Confirm working directory\n\n"
        f"```bash\npwd\n```\n\n"
        f"You should be at `{ctx.execution_workspace_path}`. If not, `cd {ctx.execution_workspace_path}`.\n\n"
        f"### Step 2: Check workspace state (read-only)\n\n"
        f"```bash\n"
        f"git status --short\n"
        f"git branch --show-current\n"
        f"git log --oneline -10\n"
        f"```\n\n"
        f"Rule: do not overwrite/revert changes that are not yours. "
        f"If you find unrelated dirty files, log them but do not clean up.\n\n"
        f"Shared-worktree guard: this checkout may be used by other agents. "
        f"If `pwd` is not `{ctx.execution_workspace_path}` or `git branch --show-current` is not `{ctx.branch}`, "
        f"stop and report a blocker instead of switching branches.\n"
        f"Never run `git reset`, `git rebase`, `git checkout`, `git switch`, `git cherry-pick`, "
        f"or `git push --force` to repair this workspace unless the operator explicitly asks you to.\n\n"
        f"### Step 3: Read project boundaries\n\n"
        f"- `{ctx.execution_harness}/scope.md` — goals, non-goals, constraints\n"
        f"- `{ctx.execution_harness}/architecture.md` — module boundaries\n"
        f"- `{ctx.execution_harness}/domain-model.md` — core entities\n\n"
        f"### Step 4: Read the source plan\n\n"
        f"```bash\ncat {ctx.execution_source_plan}\n```\n\n"
    )


def _render_reviewer_assignment(ctx: _ReviewerBootstrapContext) -> str:
    """Review assignment block — plan review omits Branch, uses openspec relative path."""
    if ctx.is_plan_review:
        role_line = "- **Role**: reviewer (plan review — read-only, you do NOT own this task, do NOT mutate code or branches)"
        source_plan_line = f"- **Source Plan**: openspec/changes/{ctx.task_id}/proposal.md (relative; server copy: {ctx.execution_source_plan})"
    else:
        role_line = "- **Role**: reviewer (review only — you do NOT own this task, do NOT mutate code)"
        source_plan_line = f"- **Source Plan**: {ctx.execution_source_plan}"
    return (
        f"## Review Assignment\n\n"
        f"- **Task**: {ctx.task_id}\n"
        f"- **Title**: {ctx.title}\n"
        f"{source_plan_line}\n"
        + ("" if ctx.is_plan_review else f"- **Branch**: {ctx.branch}\n")
        + f"{role_line}\n\n"
    )


def _render_reviewer_self_test(ctx: _ReviewerBootstrapContext) -> str:
    """Self-test evidence verification — code review only.

    Plan review returns empty string: there is no implementation or closeout
    packet to verify at the plan review stage.
    """
    if ctx.is_plan_review:
        return ""
    return (
        f"## Verify Worker Self-Test Evidence\n\n"
        f"The closeout packet should include a `self_test_evidence` field. "
        f"Verify it is present and credible:\n\n"
        f"- **Non-empty**: the worker filled in deploy SHA, e2e smoke result, and bugs found\n"
        f"- **Credible**: the deploy SHA corresponds to an actual deploy; "
        f"the e2e result matches the task scope\n"
        f"- **Task-appropriate**: server/daemon/bridge tasks require deploy + e2e; "
        f"pure doc/test tasks may skip deploy (self-test = test suite)\n\n"
        f"**Reject** the closeout if `self_test_evidence` is missing, empty, or clearly fabricated. "
        f"This is the mechanism that prevents phase-8.5/8.6-style hidden bugs.\n\n"
    )


def _render_reviewer_focus(ctx: _ReviewerBootstrapContext) -> str:
    """Review focus bullets — shared criteria with plan/code intro line."""
    if ctx.is_plan_review:
        intro = "## Review Focus (plan review)\n\nEvaluate the plan document itself:"
    else:
        intro = "## Review Focus\n\n"
    return (
        f"{intro}\n"
        f"- Plan completeness: are edge cases and error paths covered?\n"
        f"- Architecture alignment: does the plan respect scope/architecture boundaries?\n"
        f"- Test baseline: are acceptance criteria testable?\n"
        f"- Non-goals: does the plan avoid out-of-scope creep?\n"
        f"- Risk assessment: are there unaddressed failure modes?\n\n"
    )


def _render_reviewer_output_format(ctx: _ReviewerBootstrapContext) -> str:
    """Machine-readable [agent-report] decision block — shared."""
    return (
        f"## Review Output Format\n\n"
        f"Your response MUST include exactly one machine-readable block:\n\n"
        f"```text\n"
        f"[agent-report]\n"
        f"decision=approve\n"
        f"workspace_id={ctx.ws_id}\n"
        f"task_id={ctx.task_id}\n"
        f"summary=\"Approved. <optional notes>\"\n"
        f"```\n\n"
        f"OR\n\n"
        f"```text\n"
        f"[agent-report]\n"
        f"decision=reject\n"
        f"workspace_id={ctx.ws_id}\n"
        f"task_id={ctx.task_id}\n"
        f"reason=\"<specific issue requiring revision>\"\n"
        f"summary=\"Rejected. <brief explanation>\"\n"
        f"```\n\n"
        f"The `[agent-report]` marker must start at the beginning of its own line.\n\n"
    )


def _render_reviewer_constraints_block() -> str:
    """Reviewer constraints — shared."""
    return (
        f"## Constraints\n\n"
        f"- Review only — do NOT modify code, commit, or push\n"
        f"- Do NOT run `assignment accept` — you do not own this task\n"
        f"- No merge or deploy without explicit operator approval\n"
        f"- Your review is the gate: approve only when requirements are met\n"
    )


def prepare_handoff(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    role: str,
    required_scope: str = "implementation plan",
    actor: str = "operator",
    idempotency_key: str | None = None,
    db_path: str | None = None,
    coordinator_path: str | None = None,
    target_agent: str | None = None,
    review_type: str = "code",
) -> HandoffPreparedResult:
    workspace = _require_workspace(conn, workspace_id)
    task = _require_task(conn, workspace_id, task_id)
    approved_gate = _require_latest_gate_approved(conn, workspace_id, task_id, required_scope, role=role)

    if target_agent is not None:
        discord_id = get_agent_discord_id(conn, workspace_id, target_agent)
        if not discord_id:
            raise ValueError(
                f"target agent '{target_agent}' not registered in workspace '{workspace_id}'; "
                f"run: workspace agent add {workspace_id} --name {target_agent} --discord-user-id <id>"
            )
    execution_profile = _execution_profile_for_target(
        conn,
        workspace_id=workspace_id,
        target_agent=target_agent,
    )

    if role == "reviewer":
        # Reviewer reviews the plan itself — use latest plan.ready directly,
        # skip the approved-gate payload check (reviewer IS the plan gate).
        plan_event = _latest_plan_ready(conn, workspace_id, task_id)
        if not plan_event:
            raise ValueError(
                f"no plan.ready found for {workspace_id}/{task_id}; "
                f"create the task (task create) before reviewer handoff"
            )
    else:
        approved_plan_ready_id = approved_gate["payload"].get("plan_ready_event_id")
        if not approved_plan_ready_id:
            raise ValueError(
                f"plan.approved event {approved_gate['id']} for {workspace_id}/{task_id} "
                f"lacks plan_ready_event_id (legacy approval). "
                f"Re-approve with: plan approve --scope '{required_scope}'"
            )

        latest_plan = _latest_plan_ready(conn, workspace_id, task_id)

        if latest_plan and latest_plan["id"] != approved_plan_ready_id:
            raise ValueError(
                f"plan.ready was updated after approval for {workspace_id}/{task_id}; "
                f"approved plan_ready_event_id={approved_plan_ready_id}, "
                f"current latest={latest_plan['id']}. "
                f"Re-review and re-approve the updated plan before generating handoff."
            )

        plan_event = _plan_ready_by_id(conn, approved_plan_ready_id)

    _require_harness_task(workspace, task_id)
    plan_payload = plan_event.get("payload", {}) if plan_event else {}

    handoff_text = _build_handoff_text(
        workspace=workspace,
        task=task,
        plan_payload=plan_payload,
        role=role,
        execution_profile=execution_profile,
    )

    harness_rel = _harness_root_relative(workspace)
    bootstrap_filename = "reviewer-bootstrap.md" if role == "reviewer" else "worker-bootstrap.md"
    bootstrap_recommended_path = f"{harness_rel}/tasks/{task_id}/{bootstrap_filename}"

    if role == "reviewer":
        bootstrap_text = _build_reviewer_bootstrap(
            workspace=workspace,
            task=task,
            plan_payload=plan_payload,
            execution_profile=execution_profile,
            review_type=review_type,
        )
    else:
        bootstrap_text = _build_worker_bootstrap(
            workspace=workspace,
            task=task,
            plan_payload=plan_payload,
            db_path=db_path,
            coordinator_path=coordinator_path,
            execution_profile=execution_profile,
        )

    payload = {
        "task_id": task_id,
        "workspace_id": workspace_id,
        "role": role,
        "handoff_text": handoff_text,
        "source_plan": plan_payload.get("plan_doc") or plan_payload.get("absolute_plan_doc", ""),
        "branch": task.get("branch") or workspace.base_branch,
        "workspace_path": workspace.path,
        "control_workspace_path": workspace.path,
        "execution_profile": (
            {**execution_profile.to_dict(), **_materialize_handoff_profile(workspace, execution_profile)}
            if execution_profile
            else None
        ),
        "bootstrap_text": bootstrap_text,
        "approved_gate_event_id": (plan_event["id"] if role == "reviewer" else approved_gate["id"]),
        "constraints": {
            "human_gate_required": True,
            "no_merge_without_approval": True,
            "no_deploy_without_approval": True,
        },
        "target_agent": target_agent,
        "bootstrap_path": bootstrap_recommended_path,
    }

    target_suffix = f":target_{target_agent}" if target_agent else ""
    if role == "reviewer":
        # Include review_type so plan-review and code-review handoffs don't collide
        # on the same idempotency key (same plan_event + target). See backlog
        # 2026-06-23 (plan/code review same key) + progress-archiving dogfood.
        gate_segment = f"reviewer_{plan_event['id']}_{review_type}"
    else:
        gate_segment = f"gate_{approved_gate['id']}"
    resolved_key = idempotency_key or f"{workspace_id}:{task_id}:worker.handoff.prepared.v2:{gate_segment}{target_suffix}"
    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="worker.handoff.prepared",
        actor=actor,
        target=role,
        task_id=task_id,
        idempotency_key=resolved_key,
        payload=payload,
    )

    return HandoffPreparedResult(
        workspace=workspace,
        task=task,
        handoff_text=handoff_text,
        bootstrap_text=bootstrap_text,
        bootstrap_recommended_path=bootstrap_recommended_path,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def latest_prepared_handoff_bootstrap(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    target_agent: str,
) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT * FROM events
        WHERE workspace_id = ?
          AND task_id = ?
          AND event_type = 'worker.handoff.prepared'
        ORDER BY created_at DESC, rowid DESC
        """,
        (workspace_id, task_id),
    ).fetchall()
    for row in rows:
        event = row_to_dict(row)
        payload = event.get("payload", {})
        if payload.get("target_agent") != target_agent:
            continue
        return {
            "bootstrap_text": payload.get("bootstrap_text"),
            "bootstrap_path": payload.get("bootstrap_path"),
            "execution_profile": payload.get("execution_profile"),
            "event_id": event.get("id"),
        }
    return None
