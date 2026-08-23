"""Pure checklist-item projection shared by reconcile and task adoption."""
from __future__ import annotations

from typing import Any


def task_mirror_from_item(item: dict[str, Any]) -> dict[str, Any]:
    """Project file-backed task fields into the Coordinate mirror shape."""
    workflow = item.get("workflow") if isinstance(item.get("workflow"), dict) else {}
    artifacts = item.get("artifacts") if isinstance(item.get("artifacts"), dict) else {}
    return {
        "task_id": item["id"],
        "phase": workflow.get("status") or item.get("status"),
        "owner": item.get("owner"),
        "branch": workflow.get("branch") or artifacts.get("branch"),
        "pr": artifacts.get("pr") or artifacts.get("pull_request"),
        "payload": item,
    }
