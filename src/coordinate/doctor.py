"""Workspace harness health diagnostics.

Produces a structured report on a workspace's harness capability level,
distinguishing between minimal_file_backed and full_harness_runtime.
"""
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import Workspace
from .harness import HarnessAdapter
from .projection_doctor import diagnose_projections, ProjectionReport


@dataclass(frozen=True)
class FileCheck:
    path: str
    exists: bool


@dataclass(frozen=True)
class DoctorReport:
    workspace_id: str
    workspace_path: str
    workspace_path_exists: bool
    harness_root: str
    harness_root_exists: bool
    harnessctl_path: str | None
    harnessctl_exists: bool
    harnessctl_executable: bool
    files: list[FileCheck]
    harness_mode: str  # "none", "minimal_file_backed", "full_harness_runtime"
    harnessctl_version_ok: bool | None  # None if harnessctl not available
    checklist_valid: bool | None  # None if checklist not found
    harnessctl_doctor_ok: bool | None  # None if harnessctl not available
    default_bus: str | None
    default_destination: str | None
    bus_note: str
    warnings: list[str]
    summary: str
    projection_report: ProjectionReport | None = None
    projection_ok: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "workspace_path": self.workspace_path,
            "workspace_path_exists": self.workspace_path_exists,
            "harness_root": self.harness_root,
            "harness_root_exists": self.harness_root_exists,
            "harnessctl_path": self.harnessctl_path,
            "harnessctl_exists": self.harnessctl_exists,
            "harnessctl_executable": self.harnessctl_executable,
            "files": [{"path": f.path, "exists": f.exists} for f in self.files],
            "harness_mode": self.harness_mode,
            "harnessctl_version_ok": self.harnessctl_version_ok,
            "checklist_valid": self.checklist_valid,
            "harnessctl_doctor_ok": self.harnessctl_doctor_ok,
            "default_bus": self.default_bus,
            "default_destination": self.default_destination,
            "bus_note": self.bus_note,
            "warnings": self.warnings,
            "summary": self.summary,
            "projection_report": self.projection_report.to_dict() if self.projection_report else None,
            "projection_ok": self.projection_ok,
        }


REQUIRED_HARNESS_FILES = [
    "harness-config.json",
    "mvp-checklist.json",
    "events.jsonl",
    "harness-state.json",
    "progress.md",
]

OPTIONAL_HARNESS_FILES = [
    "scope.md",
    "architecture.md",
    "domain-model.md",
    "runbook.md",
]


def diagnose_workspace(
    workspace: Workspace,
    *,
    runner: Any = subprocess.run,
    conn: Any = None,
    no_projections: bool = False,
) -> DoctorReport:
    warnings: list[str] = []

    ws_path = Path(workspace.path)
    ws_exists = ws_path.is_dir()
    if not ws_exists:
        return DoctorReport(
            workspace_id=workspace.id,
            workspace_path=workspace.path,
            workspace_path_exists=False,
            harness_root=workspace.harness_root,
            harness_root_exists=False,
            harnessctl_path=None,
            harnessctl_exists=False,
            harnessctl_executable=False,
            files=[],
            harness_mode="none",
            harnessctl_version_ok=None,
            checklist_valid=None,
            harnessctl_doctor_ok=None,
            default_bus=workspace.default_bus,
            default_destination=workspace.default_destination,
            bus_note=_bus_note(workspace),
            warnings=["Workspace path does not exist."],
            summary="workspace path does not exist; harness cannot be diagnosed",
            projection_report=None,
            projection_ok=None,
        )

    hr_path = Path(workspace.harness_root)
    hr_exists = hr_path.is_dir()

    harnessctl_resolved: Path | None = None
    harnessctl_exists = False
    harnessctl_executable = False
    if hr_exists:
        try:
            adapter = HarnessAdapter(workspace, runner=runner)
            harnessctl_resolved = adapter._resolve_harnessctl()
            harnessctl_exists = True
            harnessctl_executable = os.access(harnessctl_resolved, os.X_OK)
        except Exception:
            pass

    file_checks: list[FileCheck] = []
    for fname in REQUIRED_HARNESS_FILES:
        fpath = hr_path / fname
        file_checks.append(FileCheck(path=str(fpath), exists=fpath.is_file()))
    for fname in OPTIONAL_HARNESS_FILES:
        fpath = hr_path / fname
        file_checks.append(FileCheck(path=str(fpath), exists=fpath.is_file()))

    all_required = all(f.exists for f in file_checks[:len(REQUIRED_HARNESS_FILES)])

    if not hr_exists or not all_required:
        mode = "minimal_file_backed" if hr_exists else "none"
    elif harnessctl_exists and harnessctl_executable:
        mode = "full_harness_runtime"
    else:
        mode = "minimal_file_backed"
        if harnessctl_exists and not harnessctl_executable:
            warnings.append(
                "harnessctl exists but is not executable; "
                "run chmod +x or rely on bash fallback"
            )

    # Checklist validation
    checklist_valid: bool | None = None
    checklist_path = hr_path / "mvp-checklist.json"
    if checklist_path.is_file():
        try:
            raw = checklist_path.read_text(encoding="utf-8")
            data = json.loads(raw)
            items = data.get("items", [])
            checklist_valid = isinstance(items, list)
            if not checklist_valid:
                warnings.append("mvp-checklist.json has invalid structure: items is not a list")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            checklist_valid = False
            warnings.append(f"mvp-checklist.json is invalid: {exc}")

    # harnessctl health checks
    harnessctl_version_ok: bool | None = None
    harnessctl_doctor_ok: bool | None = None
    if harnessctl_exists and harnessctl_resolved is not None:
        try:
            cmd = [str(harnessctl_resolved), "validate"]
            if not harnessctl_executable:
                cmd = ["bash", str(harnessctl_resolved), "validate"]
            result = runner(
                cmd,
                cwd=workspace.path,
                text=True,
                capture_output=True,
                check=False,
            )
            harnessctl_version_ok = result.returncode == 0
            if result.returncode != 0:
                warnings.append(
                    f"harnessctl validate failed (exit {result.returncode}): "
                    f"{(result.stderr or result.stdout).strip()[:200]}"
                )
        except Exception as exc:
            harnessctl_version_ok = False
            warnings.append(f"harnessctl validate error: {exc}")

        try:
            cmd = [str(harnessctl_resolved), "doctor"]
            if not harnessctl_executable:
                cmd = ["bash", str(harnessctl_resolved), "doctor"]
            result = runner(
                cmd,
                cwd=workspace.path,
                text=True,
                capture_output=True,
                check=False,
            )
            harnessctl_doctor_ok = result.returncode == 0
            if result.returncode != 0:
                warnings.append(
                    f"harnessctl doctor failed (exit {result.returncode}): "
                    f"{(result.stderr or result.stdout).strip()[:200]}"
                )
        except Exception as exc:
            harnessctl_doctor_ok = False
            warnings.append(f"harnessctl doctor error: {exc}")

    bus_note = _bus_note(workspace)
    summary = _build_summary(mode, hr_exists, all_required, harnessctl_exists, harnessctl_executable)

    projection_report: ProjectionReport | None = None
    projection_ok: bool | None = None
    if no_projections:
        warnings.append(
            "Projections skipped by --no-projections; this flag is for compatibility only "
            "and must not be used in acceptance, deployment smoke, dogfood, or release gates."
        )
    elif conn is not None:
        try:
            projection_report = diagnose_projections(conn, workspace)
            projection_ok = projection_report.ok
        except Exception as exc:
            warnings.append(f"projection diagnostic failed: {exc}")
            projection_ok = False

    return DoctorReport(
        workspace_id=workspace.id,
        workspace_path=workspace.path,
        workspace_path_exists=ws_exists,
        harness_root=workspace.harness_root,
        harness_root_exists=hr_exists,
        harnessctl_path=str(harnessctl_resolved) if harnessctl_resolved else None,
        harnessctl_exists=harnessctl_exists,
        harnessctl_executable=harnessctl_executable,
        files=file_checks,
        harness_mode=mode,
        harnessctl_version_ok=harnessctl_version_ok,
        checklist_valid=checklist_valid,
        harnessctl_doctor_ok=harnessctl_doctor_ok,
        default_bus=workspace.default_bus,
        default_destination=workspace.default_destination,
        bus_note=bus_note,
        warnings=warnings,
        summary=summary,
        projection_report=projection_report,
        projection_ok=projection_ok,
    )


def _bus_note(workspace: Workspace) -> str:
    if workspace.default_bus:
        return f"visible delivery via {workspace.default_bus}"
    return (
        "no default_bus configured; this only affects visible delivery, "
        "not file-backed state or mutation lifecycle"
    )


def _build_summary(
    mode: str,
    hr_exists: bool,
    all_required: bool,
    harnessctl_exists: bool,
    harnessctl_executable: bool,
) -> str:
    if mode == "none":
        if not hr_exists:
            return "harness root does not exist; run init-harness to create minimal structure"
        return "harness root exists but required files are missing"

    if mode == "full_harness_runtime":
        return "full harness runtime: harnessctl available, required files present, mutation lifecycle supported"

    # minimal_file_backed
    parts = ["minimal file-backed harness"]
    if not harnessctl_exists:
        parts.append("harnessctl not found")
    elif not harnessctl_executable:
        parts.append("harnessctl not executable")
    else:
        parts.append("harnessctl present but not all required files present")
    return "; ".join(parts)
