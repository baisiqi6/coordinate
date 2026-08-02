"""Test helper: build a U1-style template source from Coordinate's own
vendored runtime (``scripts/harness``) — no second copy of the U1 fixture.

Coordinate's ``scripts/harness`` is the U1 long-running-project-harness
template rendered with the repo's own layout (``{{HARNESS_ROOT}}`` = the repo's
harness root, ``{{PROJECT_ROOT_DEPTH}}`` = 2, ``{{SCRIPTS_DIR}}`` =
``scripts/harness``, ``{{PROJECT_NAME}}`` = the repo workspace id).
Reverse-substituting exactly those four rendered values reproduces the
template; the helper self-checks that re-rendering with the extracted values
reproduces the vendored bytes, so a drift in the vendored managed runtime
fails the tests loudly instead of silently building a fake template.
"""
from __future__ import annotations

import re
from pathlib import Path

_COORDINATE_RUNTIME = Path(__file__).resolve().parents[2] / "scripts" / "harness"

PLACEHOLDERS = (
    "{{HARNESS_ROOT}}",
    "{{PROJECT_ROOT_DEPTH}}",
    "{{SCRIPTS_DIR}}",
    "{{PROJECT_NAME}}",
)


def coordinate_runtime_dir() -> Path:
    """The repo's vendored, already-rendered runtime (a rendered source)."""
    return _COORDINATE_RUNTIME


def _one(pattern: str, text: str, label: str) -> str:
    matches = list(re.finditer(pattern, text))
    if len(matches) != 1:
        raise AssertionError(
            f"expected exactly one {pattern!r} in {label}, found {len(matches)}"
        )
    return matches[0].group(1)


def rendered_runtime_values() -> dict[str, str]:
    """The four values Coordinate's vendored runtime was rendered with."""
    common = (_COORDINATE_RUNTIME / "harness_common.py").read_text(encoding="utf-8")
    ctl = (_COORDINATE_RUNTIME / "harnessctl").read_text(encoding="utf-8")
    session = (_COORDINATE_RUNTIME / "session_init.py").read_text(encoding="utf-8")
    return {
        "{{HARNESS_ROOT}}": _one(
            r'return project_root\(\) / "([^"]+)"', common, "harness_common.py"
        ),
        "{{PROJECT_ROOT_DEPTH}}": _one(
            r"parents\[(\d+)\]", common, "harness_common.py"
        ),
        "{{SCRIPTS_DIR}}": _one(
            r"Usage: ([^ <]+)/harnessctl <command>", ctl, "harnessctl"
        ),
        "{{PROJECT_NAME}}": _one(
            r"Deterministic session bootstrap for ([^.]+)\.", session, "session_init.py"
        ),
    }


def make_template_source(dest_dir: Path) -> Path:
    """Reverse-render the vendored runtime into a U1 template source.

    Returns the created template source directory under *dest_dir*.
    """
    values = rendered_runtime_values()
    src = dest_dir / "template-src"
    src.mkdir(parents=True, exist_ok=True)
    for entry in sorted(_COORDINATE_RUNTIME.iterdir()):
        if not entry.is_file():
            continue
        body = entry.read_text(encoding="utf-8")
        # Depth first: the rendered value is a bare digit, so only the exact
        # ``parents[N]`` shape is reversed (a blanket digit replace would
        # corrupt unrelated numbers).
        template = body.replace(
            f"parents[{values['{{PROJECT_ROOT_DEPTH}}']}]",
            "parents[{{PROJECT_ROOT_DEPTH}}]",
        )
        template = template.replace(values["{{HARNESS_ROOT}}"], "{{HARNESS_ROOT}}")
        scripts_dir = values["{{SCRIPTS_DIR}}"]
        template = (
            template.replace(f"{scripts_dir}/harnessctl", "{{SCRIPTS_DIR}}/harnessctl")
            .replace(
                f"{scripts_dir}/validate_checklist.py",
                "{{SCRIPTS_DIR}}/validate_checklist.py",
            )
        )
        template = template.replace(values["{{PROJECT_NAME}}"], "{{PROJECT_NAME}}")
        (src / entry.name).write_text(template, encoding="utf-8")

        # Self-checks: only the four approved placeholders may remain, and
        # re-rendering with the extracted values must reproduce the vendored
        # bytes byte-for-byte.
        found = set(re.findall(r"\{\{[A-Z_]+\}\}", template))
        unexpected = found - set(PLACEHOLDERS)
        assert not unexpected, (
            f"template helper produced unexpected placeholders in {entry.name}: "
            f"{sorted(unexpected)}"
        )
        re_rendered = template
        for placeholder, value in values.items():
            re_rendered = re_rendered.replace(placeholder, value)
        assert re_rendered == body, (
            f"template helper drifted from vendored runtime: {entry.name}"
        )
    return src
