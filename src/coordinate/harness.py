from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .checklist_io import (
    ChecklistError,
    load_checklist,
    resolve_checklist,
    sha256_bytes,
)
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
        """Strict state read: the cache must prove it derives from the current
        checklist (same resolved path, same bytes SHA-256). Any gap raises
        ``HarnessError`` so no caller treats a stale cache as authoritative."""
        state, fresh, reasons = self.read_state_diagnostic()
        if not fresh:
            raise HarnessError(
                "harness state is not authoritative for the current checklist: "
                + "; ".join(reasons)
            )
        return state

    def read_state_diagnostic(self) -> tuple[dict[str, Any], bool, list[str]]:
        """Read-only diagnostic surface: returns ``(raw_state, fresh, reasons)``
        without raising on stale state. Never authoritative for gates/reconcile.

        Fresh means the state carries a non-empty ``source.checklist_path`` and
        ``source.checklist_sha256``, the source path resolves to the same file
        the resolver selects, and the digest matches the current checklist
        bytes. Missing/unparseable state, a missing or dual-authority checklist,
        a path mismatch, or a digest mismatch all make the state stale.
        """
        state_path = Path(self.workspace.harness_root) / "harness-state.json"
        if not state_path.exists():
            return {}, False, ["harness-state.json not found"]
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {}, False, [f"harness-state.json cannot be read: {exc}"]
        if not isinstance(state, dict):
            return {}, False, ["harness-state.json root must be a JSON object"]

        source = state.get("source")
        if not isinstance(source, dict):
            return state, False, [
                "state has no checklist source (source.checklist_path / source.checklist_sha256)"
            ]
        source_path = source.get("checklist_path")
        source_digest = source.get("checklist_sha256")
        if not isinstance(source_path, str) or not source_path:
            return state, False, ["state source.checklist_path is missing"]
        if not isinstance(source_digest, str) or not source_digest:
            return state, False, ["state source.checklist_sha256 is missing"]

        try:
            resolved = resolve_checklist(self.workspace.harness_root, purpose="read")
        except ChecklistError as exc:
            return state, False, [str(exc)]

        candidate = Path(source_path)
        if not candidate.is_absolute():
            candidate = Path(self.workspace.path) / candidate
        try:
            expected = candidate.resolve()
            actual = resolved.path.resolve()
        except OSError:
            expected, actual = candidate, resolved.path
        if expected != actual:
            return state, False, [
                f"state checklist path {source_path!r} does not match the "
                f"resolved checklist {resolved.path}"
            ]
        try:
            current_digest = sha256_bytes(resolved.path.read_bytes())
        except OSError as exc:
            return state, False, [f"current checklist cannot be read: {exc}"]
        if current_digest != source_digest:
            return state, False, [
                "state source.checklist_sha256 does not match the current checklist bytes"
            ]
        return state, True, []

    def read_checklist(self) -> dict[str, Any]:
        """Read the resolved checklist (new/legacy; none/both fail closed).

        Full contract validation is enforced here (plan §4.1/§4.3): a checklist
        that parses as JSON but is semantically invalid is surfaced as
        ``HarnessError`` so normal decision callers (reconcile, handoff, audit,
        completion gates) never consume an invalid authority.
        """
        try:
            data, _ = load_checklist(self.workspace.harness_root, purpose="read")
        except ChecklistError as exc:
            raise HarnessError(str(exc)) from exc
        return data

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
