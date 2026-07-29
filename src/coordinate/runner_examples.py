from __future__ import annotations

from copy import deepcopy
from typing import Any


RUNNER_PROFILE_EXAMPLES: dict[str, dict[str, Any]] = {
    "codex-wrapper": {
        "id": "codex-wrapper",
        "name": "Codex wrapper via generic_subprocess",
        "runner_profile": {
            "id": "codex",
            "name": "Codex CLI Wrapper",
            "runner_type": "generic_subprocess",
            "command": "bash scripts/runners/codex-agent-response.sh {prompt_path} {result_path}",
            "working_directory_strategy": "git_worktree",
            "supports_stream_attach": False,
            "env": {"COORDINATOR_AGENT_RUNTIME": "codex"},
        },
        "notes": [
            "The wrapper script lives in the target workspace, not in coordinator.",
            "The wrapper must read the prompt path argument and write AgentResponse JSON to the result path argument.",
            "Do not put provider tokens in the runner profile; keep them in the host environment or local secret store.",
        ],
        "agent_response_fields": [
            "status",
            "summary",
            "artifact_paths",
            "branch",
            "commit",
            "pr",
        ],
    },
    "claude-wrapper": {
        "id": "claude-wrapper",
        "name": "Claude Code wrapper via generic_subprocess",
        "runner_profile": {
            "id": "claude",
            "name": "Claude Code Wrapper",
            "runner_type": "generic_subprocess",
            "command": "bash scripts/runners/claude-agent-response.sh {prompt_path} {result_path}",
            "working_directory_strategy": "git_worktree",
            "supports_stream_attach": False,
            "env": {"COORDINATOR_AGENT_RUNTIME": "claude"},
        },
        "notes": [
            "The wrapper script lives in the target workspace, not in coordinator.",
            "The wrapper must read the prompt path argument and write AgentResponse JSON to the result path argument.",
            "Do not put provider tokens in the runner profile; keep them in the host environment or local secret store.",
        ],
        "agent_response_fields": [
            "status",
            "summary",
            "artifact_paths",
            "branch",
            "commit",
            "pr",
        ],
    },
}


def list_runner_profile_examples() -> list[dict[str, Any]]:
    return [
        {
            "id": example["id"],
            "name": example["name"],
            "runner_type": example["runner_profile"]["runner_type"],
            "working_directory_strategy": example["runner_profile"]["working_directory_strategy"],
        }
        for example in RUNNER_PROFILE_EXAMPLES.values()
    ]


def get_runner_profile_example(example_id: str) -> dict[str, Any]:
    try:
        return deepcopy(RUNNER_PROFILE_EXAMPLES[example_id])
    except KeyError as exc:
        raise KeyError(f"unknown runner profile example: {example_id}") from exc
