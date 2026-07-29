from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .db import Workspace


class HarnessError(RuntimeError):
    pass


@dataclass(frozen=True)
class HarnessMutationResult:
    operation: str
    task_id: str
    actor: str
    idempotency_hint: str
    started_at: str
    completed_at: str
    command: list[str] = field(default_factory=list)
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    success: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "task_id": self.task_id,
            "actor": self.actor,
            "idempotency_hint": self.idempotency_hint,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "command": list(self.command),
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "success": self.success,
        }


class HarnessAdapter:
    def __init__(
        self,
        workspace: Workspace,
        *,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.workspace = workspace
        self.runner = runner

    def run_mutation(
        self,
        operation: str,
        task_id: str,
        actor: str,
        args: list[str] | None = None,
        idempotency_hint: str | None = None,
    ) -> HarnessMutationResult:
        if not operation or not task_id:
            raise HarnessError("operation and task_id must be non-empty strings")

        harnessctl = self._resolve_harnessctl()
        extra = args or []

        if os.access(harnessctl, os.X_OK):
            command = [str(harnessctl), operation, task_id, *extra]
        else:
            command = ["bash", str(harnessctl), operation, task_id, *extra]

        hint = idempotency_hint or f"{self.workspace.id}:{operation}:{task_id}:{actor}"

        started_at = datetime.now(timezone.utc).isoformat()
        completed = self.runner(
            command,
            cwd=self.workspace.path,
            text=True,
            capture_output=True,
            check=False,
        )
        completed_at = datetime.now(timezone.utc).isoformat()

        return HarnessMutationResult(
            operation=operation,
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            started_at=started_at,
            completed_at=completed_at,
            command=command,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            success=completed.returncode == 0,
        )

    def refresh_state(self) -> dict[str, Any]:
        harnessctl = self._resolve_harnessctl()
        command = [str(harnessctl), "state"]
        if not os.access(harnessctl, os.X_OK):
            command = ["bash", str(harnessctl), "state"]
        completed = self.runner(
            command,
            cwd=self.workspace.path,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            raise HarnessError(
                f"harnessctl state failed with exit code {completed.returncode}: "
                f"{completed.stderr.strip() or completed.stdout.strip()}"
            )
        return self.read_state()

    def read_state(self) -> dict[str, Any]:
        state_path = Path(self.workspace.harness_root) / "harness-state.json"
        if not state_path.exists():
            raise HarnessError(f"harness-state.json not found: {state_path}")
        return json.loads(state_path.read_text(encoding="utf-8"))

    def read_checklist(self) -> dict[str, Any]:
        checklist_path = Path(self.workspace.harness_root) / "mvp-checklist.json"
        if not checklist_path.exists():
            raise HarnessError(f"mvp-checklist.json not found: {checklist_path}")
        return json.loads(checklist_path.read_text(encoding="utf-8"))

    def harnessctl_available(self) -> bool:
        try:
            self._resolve_harnessctl()
            return True
        except HarnessError:
            return False

    def _resolve_harnessctl(self) -> Path:
        if self.workspace.harnessctl_path:
            path = Path(self.workspace.harnessctl_path)
            if path.exists():
                return path
            raise HarnessError(f"configured harnessctl_path not found: {path}")

        workspace_path = Path(self.workspace.path)
        harness_root = Path(self.workspace.harness_root)
        candidates = [
            harness_root / "references" / "scripts" / "harnessctl",
            harness_root / "scripts" / "harnessctl",
            workspace_path / "references" / "scripts" / "harnessctl",
            workspace_path / "scripts" / "harnessctl",
            workspace_path / "harnessctl",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise HarnessError(
            "harnessctl not found; register workspace with --harnessctl-path "
            "or place harnessctl under the workspace/harness scripts directory"
        )
