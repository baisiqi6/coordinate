from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any

import discord

from . import bus, db, policy
from .agent_report import AgentReport, parse_agent_report
from .bus import discord_content, message_text
from .db import AppendEventResult, append_event
from .handoff import prepare_handoff
from .transitions import mark_done_task

logger = logging.getLogger("coordinator.daemon")


def _resolve_proxy_url() -> str | None:
    return (
        os.environ.get("COORDINATOR_HTTP_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
        or None
    )


class BotBus:
    """Send deliveries via the Discord bot API (transport for discord_webhook platform)."""

    def __init__(self, client: discord.Client, channel_id: int):
        self._client = client
        self._channel_id = channel_id

    def send(self, *, destination: str, payload: dict[str, Any], message_key: str) -> str:
        loop = self._client.loop
        future = asyncio.run_coroutine_threadsafe(self._async_send(payload), loop)
        return future.result(timeout=30)

    async def _async_send(self, payload: dict[str, Any]) -> str:
        channel = self._client.get_channel(self._channel_id)
        if channel is None:
            channel = await self._client.fetch_channel(self._channel_id)

        content = discord_content(payload)
        mention_users = payload.get("mention_users")
        if mention_users:
            allowed = discord.AllowedMentions(
                everyone=False,
                users=[discord.Object(id=int(uid)) for uid in mention_users],
                roles=False,
            )
        else:
            allowed = discord.AllowedMentions(everyone=False, users=False, roles=False)

        kwargs: dict[str, Any] = {"content": content, "allowed_mentions": allowed}
        embeds_payload = payload.get("embeds")
        if embeds_payload:
            kwargs["embeds"] = [discord.Embed.from_dict(e) for e in embeds_payload]

        msg = await channel.send(**kwargs)
        return f"discord_bot:{msg.id}"


class CoordinatorDaemon:
    def __init__(
        self,
        *,
        db_path: str,
        bot_token: str,
        channel_id: int,
        allowed_user_ids: set[int],
        pump_interval: int = 30,
    ):
        self.db_path = db_path
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.allowed_user_ids = allowed_user_ids
        self.pump_interval = pump_interval
        self.coordinator_path = str(Path(__file__).resolve().parents[2])

        self._bot_bus: BotBus | None = None
        self._agent_discord_ids: dict[int, dict[str, str]] = {}
        self._pump_cursor_rowid: int | None = None

        intents = discord.Intents.default()
        intents.message_content = True
        proxy_url = _resolve_proxy_url()
        self.client = discord.Client(intents=intents, proxy=proxy_url)

        self.client.event(self.on_ready)
        self.client.event(self.on_message)

    def run(self) -> None:
        self.client.run(self.bot_token)

    # --- Discord events ---

    async def on_ready(self) -> None:
        logger.info("Coordinator bot online: %s (id=%s)", self.client.user, self.client.user.id)
        self._bot_bus = BotBus(self.client, self.channel_id)
        await asyncio.to_thread(self._load_agent_registry)
        self.client.loop.create_task(self._pump_loop())

    def _refresh_agent_registry(self) -> dict[int, dict[str, str]]:
        """Open/migrate the DB and rebuild the effective registry map.

        Resolution uses the current UTC clock. Failures propagate so callers
        can treat the author as unauthorized rather than reusing stale cache.
        """
        conn = self._open_db()
        try:
            return db.build_agent_registry_map(conn)
        finally:
            conn.close()

    def _load_agent_registry(self) -> None:
        self._agent_discord_ids = self._refresh_agent_registry()
        logger.info("Loaded agent registry: %s", self._agent_discord_ids)

    async def on_message(self, message: discord.Message) -> None:
        if message.author == self.client.user:
            return

        if message.channel.id != self.channel_id:
            return

        try:
            registry = await asyncio.to_thread(self._refresh_agent_registry)
        except Exception:
            logger.exception("Agent registry refresh failed; treating message as unauthorized")
            registry = {}
        self._agent_discord_ids = registry

        author_id = message.author.id
        is_agent = author_id in self._agent_discord_ids

        if is_agent:
            await self._ingest_agent_message(message)
            return

        if self.client.user.mentioned_in(message):
            if author_id in self.allowed_user_ids:
                await self._dispatch(message, self._strip_mention(message))
            else:
                logger.warning("Unauthorized command from %s (%s)", message.author, author_id)
            return

    def _strip_mention(self, message: discord.Message) -> str:
        content = message.content
        for form in (f"<@{self.client.user.id}>", f"<@!{self.client.user.id}>"):
            content = content.replace(form, "")
        return content.strip()

    # --- Inbound ingest ---

    async def _ingest_agent_message(self, message: discord.Message) -> None:
        text = message.content

        match = parse_agent_report(text)
        if match is None:
            return

        workspace_memberships = self._agent_discord_ids.get(message.author.id, {})
        agent_name = workspace_memberships.get(match.workspace_id)
        if agent_name is None:
            logger.warning(
                "Rejected cross-workspace agent report: author_id=%s workspace=%s task=%s",
                message.author.id,
                match.workspace_id,
                match.task_id,
            )
            return

        logger.info(
            "Ingest: agent=%s action=%s task=%s/%s",
            agent_name,
            match.action,
            match.workspace_id,
            match.task_id,
        )

        try:
            await asyncio.to_thread(
                self._do_ingest,
                agent_name,
                match,
                message_id=str(message.id),
                content=text,
            )
        except Exception:
            logger.exception("Ingest error for %s/%s", match.action, match.task_id)

    def _do_ingest(
        self,
        agent: str,
        report: AgentReport,
        *,
        message_id: str,
        content: str,
    ) -> AppendEventResult:
        conn = self._open_db()
        try:
            payload: dict[str, Any] = {
                "action": report.action,
                "owner": agent,
                "discord_message_id": message_id,
                "source": "discord",
                "content_summary": " ".join(content.split())[:500],
            }
            if report.reason:
                payload["reason"] = report.reason
            if report.summary:
                payload["summary"] = report.summary
            # Phase 8.4 publish metadata. Only persist when provided so
            # legacy reports (no GitHub fields) continue to work.
            if report.repo:
                payload["repo"] = report.repo
            if report.branch:
                payload["branch"] = report.branch
            if report.commit:
                payload["commit"] = report.commit
            if report.remote:
                payload["remote"] = report.remote
            if report.pushed is not None:
                payload["pushed"] = bool(report.pushed)
            if report.validation:
                payload["validation"] = report.validation
            event_type = "progress.reported" if report.action == "progress" else "agent.reported"
            result = append_event(
                conn,
                event_type=event_type,
                actor=agent,
                target=agent,
                workspace_id=report.workspace_id,
                task_id=report.task_id,
                idempotency_key=(
                    f"{report.workspace_id}:discord-agent-report:"
                    f"{message_id}:{report.action}"
                ),
                payload=payload,
            )
            if report.action == "review_decision":
                decision_event = "review.completed" if report.decision == "approve" else "review.rejected"
                append_event(
                    conn,
                    event_type=decision_event,
                    actor=agent,
                    target=report.workspace_id,
                    workspace_id=report.workspace_id,
                    task_id=report.task_id,
                    idempotency_key=(
                        f"{report.workspace_id}:discord-review-decision:"
                        f"{message_id}:{report.decision}"
                    ),
                    payload={**payload, "decision": report.decision},
                )
                # Reviewer decision does NOT mark-done — the reviewer doesn't own the task
                return result
            if report.action == "done":
                mark_done_task(
                    conn,
                    workspace_id=report.workspace_id,
                    task_id=report.task_id,
                    actor=agent,
                    idempotency_hint=(
                        f"{report.workspace_id}:auto-mark-done:"
                        f"{report.task_id}:{message_id}"
                    ),
                )
            return result
        finally:
            conn.close()

    # --- Command dispatch ---

    async def _dispatch(self, message: discord.Message, text: str) -> None:
        parts = text.split()
        if not parts:
            await self._reply(message, "Commands: status | task list | handoff | pump | help")
            return

        cmd = parts[0].lower()
        try:
            if cmd == "help":
                await self._cmd_help(message)
            elif cmd == "status":
                await self._cmd_status(message)
            elif cmd == "task":
                await self._cmd_task(message, parts[1:])
            elif cmd == "handoff":
                await self._cmd_handoff(message, parts[1:])
            elif cmd == "pump":
                await self._cmd_pump(message)
            else:
                await self._reply(message, f"未知命令：`{cmd}`。可用：help, status, task, handoff, pump")
        except Exception as exc:
            logger.exception("Command error: %s", cmd)
            await self._reply(message, f"执行失败：{exc}")

    # --- Commands ---

    async def _cmd_help(self, message: discord.Message) -> None:
        await self._reply(message, (
            "**Coordinator 命令**\n"
            "`status` — 查看 workspace\n"
            "`task list [workspace]` — 列出任务\n"
            "`task show <ws> <task-id>` — 查看任务详情\n"
            "`handoff <ws> <task-id> <agent>` — 交接任务给 agent\n"
            "`pump` — 触发一次事件/消息投递"
        ))

    async def _cmd_status(self, message: discord.Message) -> None:
        text = await asyncio.to_thread(self._do_status)
        await self._reply(message, text)

    async def _cmd_task(self, message: discord.Message, args: list[str]) -> None:
        if not args or args[0] == "list":
            ws_id = args[1] if len(args) > 1 else None
            text = await asyncio.to_thread(self._do_task_list, ws_id)
        elif args[0] == "show" and len(args) >= 3:
            text = await asyncio.to_thread(self._do_task_show, args[1], args[2])
        else:
            text = "用法：task list [workspace] | task show <ws> <task-id>"
        await self._reply(message, text)

    async def _cmd_handoff(self, message: discord.Message, args: list[str]) -> None:
        if len(args) < 3:
            await self._reply(message, "用法：handoff <workspace> <task-id> <agent> [--role worker|reviewer]")
            return
        ws_id, task_id, agent = args[0], args[1], args[2]
        role = "worker"
        remaining = args[3:]
        if len(remaining) >= 2 and remaining[0] == "--role":
            role = remaining[1]
            if role not in ("worker", "reviewer"):
                await self._reply(message, f"无效 role：{role}（支持 worker, reviewer）")
                return
        text = await asyncio.to_thread(self._do_handoff, ws_id, task_id, agent, role)
        await self._reply(message, text)

    async def _cmd_pump(self, message: discord.Message) -> None:
        text = await asyncio.to_thread(self._do_pump)
        await self._reply(message, text)

    # --- DB operations (run in thread) ---

    def _open_db(self) -> sqlite3.Connection:
        conn = db.connect(self.db_path)
        db.migrate(conn)
        conn.commit()
        return conn

    def _do_status(self) -> str:
        conn = self._open_db()
        try:
            workspaces = db.list_workspaces(conn)
            if not workspaces:
                return "还没有注册 workspace。"
            git_sha = os.environ.get("COORDINATOR_GIT_SHA") or "unknown"
            lines = [
                "**Coordinator daemon：**",
                f"- git_sha=`{git_sha}` db=`{self.db_path}` channel=`{self.channel_id}`",
                "",
                "**Workspaces：**",
            ]
            for ws in workspaces:
                bus_label = ws.default_bus or "none"
                lines.append(f"- `{ws.id}` bus={bus_label}")
            return "\n".join(lines)
        finally:
            conn.close()

    def _do_task_list(self, workspace_id: str | None) -> str:
        conn = self._open_db()
        try:
            workspaces = db.list_workspaces(conn)
            if workspace_id:
                workspaces = [ws for ws in workspaces if ws.id == workspace_id]
            if not workspaces:
                return f"找不到 workspace：{workspace_id or '(none)'}"
            lines: list[str] = []
            for ws in workspaces:
                rows = conn.execute(
                    "SELECT task_id, phase, owner FROM tasks WHERE workspace_id = ? ORDER BY updated_at DESC",
                    (ws.id,),
                ).fetchall()
                lines.append(f"**{ws.id}**（{len(rows)} 个任务）")
                for r in rows:
                    lines.append(f"  `{r['task_id']}` phase={r['phase']} owner={r['owner'] or 'none'}")
            return "\n".join(lines) if lines else "暂无任务。"
        finally:
            conn.close()

    def _do_task_show(self, workspace_id: str, task_id: str) -> str:
        conn = self._open_db()
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
                (workspace_id, task_id),
            ).fetchone()
            if not row:
                return f"找不到任务：{workspace_id}/{task_id}"
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
            lines = [
                f"**{task_id}**",
                f"phase={row['phase']} owner={row['owner'] or 'none'} branch={row['branch'] or 'none'}",
            ]
            status = payload.get("status")
            if status:
                lines.append(f"status={status}")
            lease = payload.get("lease")
            if lease:
                lines.append(f"lease: owner={lease.get('owner')} expires={lease.get('expires_at')}")
            recent = conn.execute(
                """
                SELECT event_type, actor, payload_json, created_at
                FROM events
                WHERE workspace_id = ? AND task_id = ?
                  AND event_type IN (
                    'progress.reported',
                    'review.completed',
                    'blocker.raised',
                    'blocker.resolved',
                    'task.done',
                    'ci.failed',
                    'ci.passed',
                    'pr_review.approved',
                    'pr_review.changes_requested',
                    'pr_review.required'
                  )
                ORDER BY created_at DESC, rowid DESC
                LIMIT 5
                """,
                (workspace_id, task_id),
            ).fetchall()
            if recent:
                lines.append("最近状态：")
                for event in recent:
                    lines.append(f"  - {_task_status_event_line(event)}")
            return "\n".join(lines)
        finally:
            conn.close()

    def _do_handoff(self, workspace_id: str, task_id: str, agent: str, role: str = "worker") -> str:
        conn = self._open_db()
        try:
            result = prepare_handoff(
                conn,
                workspace_id=workspace_id,
                task_id=task_id,
                role=role,
                actor="coordinator-daemon",
                db_path=self.db_path,
                coordinator_path=self.coordinator_path,
                target_agent=agent,
            )
        finally:
            conn.close()

        if result.bootstrap_text:
            ws_path = result.workspace.path
            bootstrap_abs = os.path.join(ws_path, result.bootstrap_recommended_path)
            os.makedirs(os.path.dirname(bootstrap_abs), exist_ok=True)
            with open(bootstrap_abs, "w") as f:
                f.write(result.bootstrap_text)

        pump_summary = self._do_pump()

        eid = result.event.get("id", "?")[:8]
        return f"已交接：`{task_id}` -> {agent}（event={eid}...）\n{pump_summary}"

    def _do_pump(self) -> str:
        conn = self._open_db()
        try:
            # Initialize cursor on first pump: skip all historical events.
            if self._pump_cursor_rowid is None:
                row = conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM events").fetchone()
                self._pump_cursor_rowid = row[0]

            # Snapshot current max rowid as cutoff for this cycle.
            # Events with rowid > cutoff (written during this cycle's scan)
            # are left for the next cycle — no race window.
            cutoff_row = conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM events").fetchone()
            cutoff = cutoff_row[0]

            workspaces = db.list_workspaces(conn)
            total_created = 0
            for ws in workspaces:
                if ws.default_bus == "discord_webhook":
                    r = policy.pump_events(
                        conn,
                        workspace_id=ws.id,
                        platform="discord_webhook",
                        destination=ws.default_destination or "",
                        min_rowid=self._pump_cursor_rowid,
                        max_rowid=cutoff,
                    )
                    total_created += r.created

            pump = bus.pump_deliveries(conn, platform="discord_webhook", bus=self._bot_bus)
            # Advance cursor to cutoff. Events with rowid > cutoff
            # will be processed in the next cycle.
            self._pump_cursor_rowid = cutoff
            return f"Pump 完成：创建 {total_created} 条，发送 {pump.sent} 条，失败 {pump.failed} 条"
        finally:
            conn.close()

    # --- Pump loop ---

    async def _pump_loop(self) -> None:
        while not self.client.is_closed():
            await asyncio.sleep(self.pump_interval)
            try:
                await asyncio.to_thread(self._do_pump)
            except Exception:
                logger.exception("Pump loop error")

    # --- Helpers ---

    async def _reply(self, message: discord.Message, text: str) -> None:
        for chunk in _chunks(text, 1900):
            await message.channel.send(chunk)


def _task_status_event_line(event: sqlite3.Row) -> str:
    payload = json.loads(event["payload_json"]) if event["payload_json"] else {}
    event_type = event["event_type"]
    actor = event["actor"] or "unknown"
    created_at = event["created_at"]

    if event_type == "progress.reported":
        summary = _compact(payload.get("summary") or payload.get("content_summary") or "无摘要")
        return f"{created_at} 进度：{actor} — {summary}"
    if event_type == "agent.reported":
        action = payload.get("action", "unknown")
        owner = payload.get("owner") or actor
        summary = _compact(payload.get("summary") or payload.get("reason") or "")
        suffix = f" — {summary}" if summary else ""
        return f"{created_at} agent {action}：{owner}{suffix}"
    if event_type == "review.completed":
        decision = payload.get("decision") or "unknown"
        reviewer = payload.get("reviewer") or actor
        summary = _compact(payload.get("summary") or "")
        suffix = f" — {summary}" if summary else ""
        return f"{created_at} 审核：{reviewer} {decision}{suffix}"
    if event_type == "blocker.raised":
        reason = _compact(payload.get("reason") or "未说明原因")
        return f"{created_at} 阻塞：{actor} — {reason}"
    if event_type == "blocker.resolved":
        decision = _compact(payload.get("decision") or "resolved")
        return f"{created_at} 解除阻塞：{actor} — {decision}"
    if event_type == "task.done":
        return f"{created_at} 完成：{actor}"
    if event_type == "ci.failed":
        return f"{created_at} CI 未通过"
    if event_type == "ci.passed":
        return f"{created_at} CI 已通过"
    if event_type == "pr_review.approved":
        return f"{created_at} PR review 已批准"
    if event_type == "pr_review.changes_requested":
        return f"{created_at} PR review 请求修改"
    if event_type == "pr_review.required":
        return f"{created_at} 需要 PR review"
    return f"{created_at} {event_type}"


def _compact(value: str, *, limit: int = 120) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _chunks(text: str, size: int) -> list[str]:
    if len(text) <= size:
        return [text]
    parts: list[str] = []
    for i in range(0, len(text), size):
        parts.append(text[i : i + size])
    return parts
