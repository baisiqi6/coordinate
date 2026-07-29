"""Discord embed rendering for coordinator events.

Generates structured Discord embeds from event data. Embeds supplement the
plain-text content — they are never the sole protocol carrier.  The bot-to-bot
trigger chain (handoff, lifecycle, agent-report) lives in ``content`` only.
"""

from __future__ import annotations

from typing import Any

# Discord embed field limits
_MAX_TITLE = 256
_MAX_FIELD_NAME = 256
_MAX_FIELD_VALUE = 1024
_MAX_TOTAL = 6000

# Status colours
GREEN = 0x57F287
YELLOW = 0xFEE75C
RED = 0xED4245
BLUE = 0x5865F2
GREY = 0x99AAB5

# event_type → (embed title, colour)
_STYLING: dict[str, tuple[str, int]] = {
    "agent.reported":                 ("🚧 Agent 汇报", YELLOW),
    "assignment.accepted":            ("🚀 任务已接收", GREEN),
    "assignment.requested":           ("📌 任务已分配", BLUE),
    "blocker.raised":                 ("🛑 任务阻塞", RED),
    "blocker.resolved":               ("✅ 阻塞已解除", GREEN),
    "closeout.requested":             ("🔍 请求 Closeout", BLUE),
    "handoff.requested":              ("🤝 请求交接", BLUE),
    "harness.mutation_failed":        ("⚠️ Harness 操作失败", RED),
    "issue.spotted":                  ("🧭 GitHub Issue 候选", BLUE),
    "job.completed":                  ("✅ Job 完成", GREEN),
    "job.failed":                     ("❌ Job 失败", RED),
    "plan.ready":                     ("📋 计划就绪", YELLOW),
    "plan.approved":                  ("✅ 计划已批准", GREEN),
    "plan.rejected":                  ("❌ 计划被拒绝", RED),
    "plan.review_requested":          ("🔍 请求计划审核", YELLOW),
    "worker.handoff.prepared":        ("🤝 Worker 交接", BLUE),
    "task_mirror.created":            ("🪞 任务已同步", YELLOW),
    "task_mirror.updated":            ("🚧 任务状态更新", YELLOW),
    "reconciliation.completed":       ("🔄 状态同步", GREY),
    "task.done":                      ("🏁 任务完成", GREEN),
    "review.completed":               ("🔍 审核完成", BLUE),
    "pr.linked":                      ("🔗 PR 已关联", YELLOW),
    "pr.created":                     ("✨ PR 已创建", GREEN),
    "push.required":                  ("📤 需要 Push", YELLOW),
    "publish.blocked":                ("🛑 Publish 被拦截", RED),
    "ci.failed":                      ("❌ CI 失败", RED),
    "ci.passed":                      ("✅ CI 通过", GREEN),
    "pr_review.approved":             ("✅ PR 审核通过", GREEN),
    "pr_review.changes_requested":    ("📝 PR 需要修改", RED),
    "pr_review.required":             ("🔍 需要 PR 审核", BLUE),
    "progress.reported":              ("🚧 任务进度", YELLOW),
}

_FIELD_LABELS: dict[str, str] = {
    "Action": "⚙️ 动作",
    "Actor": "👤 操作者",
    "Agent": "🤖 执行者",
    "Assigned By": "📨 分配人",
    "Bootstrap": "🧭 Bootstrap",
    "Branch": "🌿 分支",
    "Created": "➕ 新增",
    "Decision": "✅ 结论",
    "Exit Code": "🚪 退出码",
    "Failed Checks": "❌ 失败检查",
    "From": "📤 来源",
    "Job": "🧰 Job",
    "Labels": "🏷️ 标签",
    "Logs": "📜 日志",
    "Operation": "🛠️ 操作",
    "Owner": "👤 负责人",
    "PR": "🔗 PR",
    "Phase": "🚦 阶段",
    "Plan": "📄 计划文档",
    "Project": "📁 项目",
    "Reason": "🧱 原因",
    "Reviewer": "🔍 审核人",
    "Role": "🎭 角色",
    "Session": "🧵 会话",
    "Summary": "📝 摘要",
    "Trust": "🔒 信任边界",
    "Target": "🎯 目标",
    "Task": "📌 任务",
    "Title": "🏷️ 标题",
    "Unchanged": "➖ 未变",
    "Updated": "🔄 更新",
}

_VALUE_LABELS: dict[str, str] = {
    "accept": "已接收",
    "approved": "通过",
    "blocked": "阻塞",
    "blocker": "阻塞",
    "changes_requested": "需要修改",
    "closed": "已关闭",
    "closeout_requested": "等待 closeout",
    "done": "完成",
    "failed": "失败",
    "pass": "通过",
    "passed": "通过",
    "pending": "等待中",
    "progress": "进度",
    "ready": "就绪",
    "ready_for_worker": "可交给 worker",
    "rejected": "拒绝",
    "review_approved": "审核通过",
    "running": "运行中",
    "worker": "worker",
}


def render_embed(
    event_type: str,
    event: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    """Return a Discord embed dict for the event, or ``None`` on failure."""
    try:
        styling = _STYLING.get(event_type)
        if styling is None:
            return None
        title, color = styling

        # Dynamic color for agent.reported based on action
        if event_type == "agent.reported":
            action = payload.get("action", "")
            if action == "done":
                title, color = "🎯 Agent 完成", GREEN
            elif action == "blocker":
                title, color = "🛑 Agent 阻塞", RED
            elif action == "accept":
                title, color = "🚀 Agent 已接收", BLUE
            elif action == "progress":
                title, color = "🚧 Agent 进度", YELLOW

        fields = _build_fields(event_type, event, payload)
        footer = _build_footer(event)

        embed: dict[str, Any] = {
            "title": _truncate(title, _MAX_TITLE),
            "color": color,
            "fields": fields,
        }
        if footer:
            embed["footer"] = footer

        # Trim if over Discord total limit
        while _embed_total(embed) > _MAX_TOTAL and len(embed["fields"]) > 1:
            embed["fields"].pop()

        return embed
    except Exception:
        return None


# ── helpers ──────────────────────────────────────────────────────────

def _truncate(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _field(name: str, value: Any, inline: bool = True) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    original_name = str(name)
    return {
        "name": _truncate(_FIELD_LABELS.get(original_name, original_name), _MAX_FIELD_NAME),
        "value": _truncate(_display_value(original_name, value), _MAX_FIELD_VALUE),
        "inline": inline,
    }


def _fields(*candidates: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [f for f in candidates if f is not None]


def _embed_total(embed: dict[str, Any]) -> int:
    total = len(embed.get("title", ""))
    total += len(embed.get("description", ""))
    total += len(embed.get("footer", {}).get("text", ""))
    for f in embed.get("fields", []):
        total += len(f.get("name", ""))
        total += len(f.get("value", ""))
    return total


def _display_value(name: str, value: Any) -> str:
    text = str(value)
    if name in {"Action", "Decision", "Phase"}:
        return _VALUE_LABELS.get(text, text)
    return text


def _actor(event: dict[str, Any], payload: dict[str, Any]) -> str:
    return (
        payload.get("owner")
        or payload.get("target")
        or event.get("target")
        or event.get("actor")
        or "operator"
    )


def _build_footer(event: dict[str, Any]) -> dict[str, Any] | None:
    parts = [p for p in [event.get("actor"), event.get("workspace_id")] if p]
    if not parts:
        return None
    return {"text": " · ".join(parts)}


def _build_fields(
    event_type: str,
    event: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    task_id = event.get("task_id") or payload.get("task_id") or ""
    actor = event.get("actor") or "operator"

    if event_type == "agent.reported":
        owner = payload.get("owner") or actor
        action = payload.get("action", "unknown")
        if action == "done":
            return _fields(
                _field("Task", task_id),
                _field("Agent", owner),
                _field("Summary", payload.get("summary"), inline=False),
            )
        if action == "blocker":
            return _fields(
                _field("Task", task_id),
                _field("Agent", owner),
                _field("Reason", payload.get("reason") or payload.get("summary"), inline=False),
            )
        if action == "progress":
            return _fields(
                _field("Task", task_id),
                _field("Agent", owner),
                _field("Summary", payload.get("summary"), inline=False),
            )
        return _fields(
            _field("Task", task_id),
            _field("Agent", owner),
            _field("Action", action),
        )

    if event_type == "assignment.accepted":
        return _fields(
            _field("Task", task_id),
            _field("Agent", _actor(event, payload)),
            _field("Session", payload.get("session")),
        )

    if event_type == "assignment.requested":
        return _fields(
            _field("Task", task_id),
            _field("Owner", _actor(event, payload)),
            _field("Assigned By", actor),
        )

    if event_type == "blocker.raised":
        return _fields(
            _field("Task", task_id),
            _field("Agent", actor),
            _field("Reason", payload.get("reason"), inline=False),
        )

    if event_type == "blocker.resolved":
        return _fields(
            _field("Task", task_id),
            _field("Agent", actor),
            _field("Decision", payload.get("decision"), inline=False),
        )

    if event_type == "progress.reported":
        owner = payload.get("owner") or actor
        summary = payload.get("summary") or payload.get("content_summary") or ""
        return _fields(
            _field("Task", task_id),
            _field("Agent", owner),
            _field("Summary", summary, inline=False),
        )

    if event_type == "review.completed":
        return _fields(
            _field("Task", task_id),
            _field("Reviewer", payload.get("reviewer")),
            _field("Decision", payload.get("decision")),
            _field("Summary", payload.get("summary"), inline=False),
        )

    if event_type == "task.done":
        return _fields(
            _field("Task", task_id),
            _field("Agent", actor),
        )

    if event_type == "closeout.requested":
        return _fields(
            _field("Task", task_id),
            _field("Agent", actor),
            _field("Reviewer", payload.get("reviewer")),
        )

    if event_type == "worker.handoff.prepared":
        return _fields(
            _field("Task", task_id),
            _field("Target", payload.get("target_agent") or event.get("target")),
            _field("Role", payload.get("role")),
            _field("Bootstrap", payload.get("bootstrap_path"), inline=False),
            _field("Branch", payload.get("branch")),
        )

    if event_type == "handoff.requested":
        return _fields(
            _field("Task", task_id),
            _field("Target", payload.get("target") or event.get("target")),
            _field("From", actor),
            _field("Reason", payload.get("reason"), inline=False),
        )

    if event_type == "plan.ready":
        return _fields(
            _field("Task", task_id),
            _field("Target", payload.get("target") or event.get("target")),
            _field("Title", payload.get("title"), inline=False),
            _field("Plan", payload.get("plan_doc") or payload.get("source_plan"), inline=False),
        )

    if event_type == "plan.approved":
        return _fields(
            _field("Task", task_id),
            _field("Reviewer", payload.get("reviewer") or actor),
            _field("Plan", payload.get("source_plan"), inline=False),
        )

    if event_type == "plan.rejected":
        return _fields(
            _field("Task", task_id),
            _field("Reviewer", payload.get("reviewer") or actor),
            _field("Reason", payload.get("reason"), inline=False),
        )

    if event_type == "pr.linked":
        return _fields(
            _field("Task", task_id),
            _field("PR", payload.get("pr") or payload.get("pr_url"), inline=False),
            _field("Branch", payload.get("branch")),
        )

    if event_type == "pr.created":
        return _fields(
            _field("Task", task_id),
            _field("PR", payload.get("pr") or payload.get("pr_url"), inline=False),
            _field("Repo", payload.get("repo")),
            _field("Branch", payload.get("branch")),
            _field("Head", payload.get("head_ref")),
            _field("Base", payload.get("base")),
            _field("Reported", payload.get("reported_commit")),
            _field("Remote SHA", payload.get("remote_sha")),
        )

    if event_type == "push.required":
        return _fields(
            _field("Task", task_id),
            _field("Repo", payload.get("repo")),
            _field("Branch", payload.get("branch")),
            _field("Commit", payload.get("reported_commit")),
            _field("Remote", payload.get("remote") or "origin"),
            _field("Reason", payload.get("detail") or "branch not pushed", inline=False),
        )

    if event_type == "publish.blocked":
        return _fields(
            _field("Task", task_id),
            _field("Reason", payload.get("reason"), inline=False),
            _field("Repo", payload.get("repo")),
            _field("Branch", payload.get("branch")),
            _field("Head", payload.get("head_ref")),
            _field("Base", payload.get("base")),
            _field("Reported", payload.get("reported_commit")),
            _field("Remote SHA", payload.get("remote_sha")),
        )

    if event_type == "ci.passed":
        return _fields(
            _field("Task", task_id),
            _field("Branch", payload.get("branch")),
        )

    if event_type == "ci.failed":
        checks = payload.get("checks") or []
        failed = ", ".join(
            c["name"] for c in checks
            if isinstance(c, dict) and c.get("status") == "failed"
        )
        return _fields(
            _field("Task", task_id),
            _field("Branch", payload.get("branch")),
            _field("Failed Checks", failed or None, inline=False),
        )

    if event_type in ("pr_review.approved", "pr_review.changes_requested", "pr_review.required"):
        return _fields(
            _field("Task", task_id),
            _field("Branch", payload.get("branch")),
        )

    if event_type == "job.completed":
        return _fields(
            _field("Task", task_id),
            _field("Job", payload.get("job_id")),
            _field("Logs", payload.get("logs_path"), inline=False),
        )

    if event_type == "job.failed":
        reason = ""
        if payload.get("timeout"):
            reason = f"timeout {payload.get('timeout_seconds')}s"
        elif payload.get("exit_code") is not None:
            reason = f"exit_code={payload.get('exit_code')}"
        else:
            reason = "runner failed"
        return _fields(
            _field("Task", task_id),
            _field("Job", payload.get("job_id")),
            _field("Reason", reason),
            _field("Logs", payload.get("logs_path"), inline=False),
        )

    if event_type == "harness.mutation_failed":
        exit_code = payload.get("exit_code")
        if exit_code is None:
            exit_code = (payload.get("mutation") or {}).get("exit_code", "unknown")
        return _fields(
            _field("Task", task_id),
            _field("Operation", payload.get("operation")),
            _field("Owner", _actor(event, payload)),
            _field("Exit Code", str(exit_code)),
        )

    if event_type == "issue.spotted":
        labels = payload.get("labels") or []
        label_text = ", ".join(str(label) for label in labels) if isinstance(labels, list) else str(labels)
        return _fields(
            _field("Project", payload.get("repo") or event.get("target")),
            _field("Task", f"#{payload.get('number')}"),
            _field("Title", payload.get("title"), inline=False),
            _field("Owner", payload.get("author")),
            _field("Labels", label_text),
            _field("Trust", "untrusted", inline=True),
            _field("Summary", payload.get("body_excerpt"), inline=False),
        )

    if event_type in ("task_mirror.created", "task_mirror.updated"):
        return _fields(
            _field("Task", task_id),
            _field("Phase", payload.get("phase")),
            _field("Owner", payload.get("owner")),
            _field("Branch", payload.get("branch")),
            _field("PR", payload.get("pr"), inline=False),
        )

    if event_type == "reconciliation.completed":
        project = payload.get("project") or event.get("workspace_id") or ""
        return _fields(
            _field("Project", project),
            _field("Created", str(payload.get("created", 0))),
            _field("Updated", str(payload.get("updated", 0))),
            _field("Unchanged", str(payload.get("unchanged", 0))),
        )

    if event_type == "plan.review_requested":
        return _fields(
            _field("Task", task_id),
            _field("Title", payload.get("title"), inline=False),
        )

    # Generic fallback for any styled event without specific handling
    return _fields(
        _field("Task", task_id),
        _field("Actor", actor),
    )
