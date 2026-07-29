"""Shared ``[agent-report]`` parsing — daemon + runtime paths.

Phase 8.8 extracted these from ``daemon.py`` so the Discord message path
(daemon) and the runtime job-result path (``report_job_result``) parse
``[agent-report] decision=`` / ``action=`` identically. Avoids the drift where
runtime created ``agent.reported`` with action based only on job status, missing
the ``decision=`` reviewers put in their output.
"""
from __future__ import annotations

import logging
import re
import shlex
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentReport:
    action: str
    workspace_id: str
    task_id: str
    reason: str | None = None
    summary: str | None = None
    repo: str | None = None
    branch: str | None = None
    commit: str | None = None
    remote: str | None = None
    pushed: bool | None = None
    validation: str | None = None
    decision: str | None = None


_TAG_RE = re.compile(
    r"^\[(agent-report|accept|accepted|handoff-received|done|blocker|progress)\](?=\s|$)",
    re.IGNORECASE,
)


def parse_agent_report(
    text: str,
    *,
    fallback_workspace_id: str | None = None,
    fallback_task_id: str | None = None,
) -> AgentReport | None:
    """Parse the first ``[agent-report]`` block in ``text``.

    Returns None if no structured report block is found. Supports the
    reviewer ``decision=`` field (phase 8.5): when present and
    approve/reject, ``action`` becomes ``review_decision``.

    If the block carries a decision/action signal but omits workspace_id /
    task_id, the fallback values are used and a warning is logged — so a
    reviewer's approve/reject is never silently dropped (backlog #10). Without
    fallback, returns None + warns (signal lost, but loud).
    """
    if not text:
        return None
    report_block = extract_agent_report_block(text)
    if report_block is None:
        return None

    match = re.match(
        r"^\[(agent-report|accept|accepted|handoff-received|done|blocker|progress)\]\s*(.*)$",
        report_block,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return None

    tag = match.group(1).lower()
    fields = parse_key_values(match.group(2))
    if tag == "agent-report":
        action = (fields.get("action") or "").lower()
    elif tag in {"accept", "accepted", "handoff-received"}:
        action = "accept"
    else:
        action = tag

    decision = (fields.get("decision") or "").lower()
    if decision in {"approve", "reject"}:
        action = "review_decision"

    if action not in {"accept", "done", "blocker", "progress", "review_decision"}:
        return None

    workspace_id = fields.get("workspace_id")
    task_id = fields.get("task_id")
    if not workspace_id or not task_id:
        # Has a decision/action signal but missing workspace_id/task_id.
        # Don't silently drop (backlog #10): use fallback if caller has it
        # (runtime job context), else warn loudly so the lost signal is seen.
        if fallback_workspace_id and fallback_task_id:
            logger.warning(
                "agent-report has action=%r decision=%r but missing workspace_id/task_id "
                "(ws=%r task=%r); using fallback (ws=%s task=%s). Agent should set "
                "workspace_id/task_id in [agent-report].",
                action, decision, workspace_id, task_id,
                fallback_workspace_id, fallback_task_id,
            )
            workspace_id = workspace_id or fallback_workspace_id
            task_id = task_id or fallback_task_id
        else:
            logger.warning(
                "agent-report has action=%r decision=%r but missing workspace_id/task_id "
                "and no fallback; signal ignored. Agent should set workspace_id/task_id "
                "in [agent-report].",
                action, decision,
            )
            return None

    return AgentReport(
        action=action,
        workspace_id=workspace_id,
        task_id=task_id,
        reason=fields.get("reason"),
        summary=fields.get("summary"),
        repo=fields.get("repo"),
        branch=fields.get("branch"),
        commit=fields.get("commit"),
        remote=fields.get("remote"),
        pushed=parse_bool_field(fields.get("pushed")),
        validation=fields.get("validation"),
        decision=decision if decision in {"approve", "reject"} else None,
    )


def parse_bool_field(value: str | None) -> bool | None:
    """Parse a key=value field as a strict boolean.

    True only for literal ``true``, False only for literal ``false``;
    None otherwise. The publish flow treats ``pushed`` as a security gate,
    so we never invent True/False from arbitrary truthy strings.
    """
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return None


def extract_agent_report_block(text: str) -> str | None:
    """Return the first structured agent-report block from a message.

    The report must start at a line boundary, so prose like "mention
    [agent-report] in docs" is not treated as a lifecycle signal. Accepts
    both the single-line and readable multiline forms.
    """
    lines = text.strip().splitlines()
    accept_candidate: str | None = None
    for index, line in enumerate(lines):
        stripped = line.strip()
        match = _TAG_RE.match(stripped)
        if not match:
            continue

        block_lines = [stripped]
        for continuation in lines[index + 1:]:
            continuation = continuation.strip()
            if not continuation:
                break
            if _TAG_RE.match(continuation):
                break
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_-]*=", continuation):
                break
            block_lines.append(continuation)

        candidate = "\n".join(block_lines)
        tag = match.group(1).lower()
        fields_match = re.match(
            r"^\[(agent-report|accept|accepted|handoff-received|done|blocker|progress)\]\s*(.*)$",
            candidate,
            re.IGNORECASE | re.DOTALL,
        )
        fields = parse_key_values(fields_match.group(2) if fields_match else "")
        if tag == "agent-report":
            action = (fields.get("action") or "").lower()
        elif tag in {"accept", "accepted", "handoff-received"}:
            action = "accept"
        else:
            action = tag
        if action in {"accept", "done", "blocker", "progress"} or fields.get("decision") in {"approve", "reject"}:
            # Signal present. ws/task is validated in parse_agent_report (with
            # fallback for incomplete reports — backlog #10); don't drop here,
            # or a reviewer's decision=approve without workspace_id/task_id
            # gets silently lost before fallback can recover it.
            if action != "accept":
                return candidate
            if accept_candidate is None:
                accept_candidate = candidate
    return accept_candidate


def parse_key_values(text: str) -> dict[str, str]:
    """Parse ``key=value`` pairs (shlex-split), normalizing keys to
    lowercase with underscores. Returns {} on unparseable input."""
    fields: dict[str, str] = {}
    try:
        parts = shlex.split(text)
    except ValueError:
        return fields
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        normalized = key.strip().lower().replace("-", "_")
        if normalized:
            fields[normalized] = value.strip()
    return fields
