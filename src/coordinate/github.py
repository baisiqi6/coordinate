from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable
from typing import Any


RunGh = Callable[[list[str]], subprocess.CompletedProcess[str]]


# 40-char lowercase hex (full SHA-1). Workers must report the full SHA.
COMMIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")

# owner/name: lowercase letters/digits/._- with at most one slash separating
# owner and name. Used to validate user-supplied repo before passing to gh.
REPO_PATTERN = re.compile(r"^[a-z0-9._-]+/[a-z0-9._-]+$")

# Branch names may contain slashes, dots, dashes, underscores. Reject anything
# that would break argv (NUL, control chars, spaces at boundaries, leading -).
BRANCH_PATTERN = re.compile(r"^[A-Za-z0-9._/\-+]+$")


class GitHubCommandError(ValueError):
    """Raised when a gh command fails, returns invalid JSON, or is missing.

    Subclasses ValueError so existing callers that catch ValueError keep
    working. Use `reason` to programmatically branch on the failure mode.
    """

    def __init__(self, message: str, *, reason: str, stderr: str = ""):
        super().__init__(message)
        self.reason = reason
        self.stderr = stderr


def query_pr_head_sha(
    workspace_path: str,
    pr_url: str,
    *,
    run: object = subprocess.run,
) -> str:
    try:
        proc = run(
            ["gh", "pr", "view", pr_url, "--json", "headRefOid"],
            timeout=30,
            check=False,
            capture_output=True,
            text=True,
            cwd=workspace_path,
        )
    except FileNotFoundError:
        raise GitHubCommandError("gh CLI not available", reason="gh_missing")

    if proc.returncode != 0:
        raise GitHubCommandError(
            f"gh pr view failed: {proc.stderr}",
            reason="gh_failed",
            stderr=str(proc.stderr or ""),
        )

    try:
        raw = json.loads(proc.stdout)
    except (json.JSONDecodeError, TypeError):
        raise GitHubCommandError("gh pr view returned invalid JSON", reason="invalid_json")

    if not isinstance(raw, dict):
        raise GitHubCommandError("gh pr view returned invalid JSON", reason="invalid_json")

    head_sha = raw.get("headRefOid")
    if not isinstance(head_sha, str) or not head_sha:
        raise GitHubCommandError("gh pr view returned no headRefOid", reason="missing_field")
    return head_sha


# ---------- Phase 8.4 helpers ----------


def validate_repo(repo: str) -> str:
    """Return the repo string if it matches REPO_PATTERN, else raise."""
    valid = isinstance(repo, str) and bool(REPO_PATTERN.fullmatch(repo or ""))
    if valid:
        owner, name = repo.split("/", 1)
        valid = owner not in {".", ".."} and name not in {".", ".."}
    if not valid:
        raise GitHubCommandError(
            f"invalid repo (expected owner/name): {repo!r}",
            reason="invalid_repo",
        )
    return repo


def validate_branch(branch: str) -> str:
    valid = (
        isinstance(branch, str)
        and bool(BRANCH_PATTERN.fullmatch(branch or ""))
        and not branch.startswith("-")
        and not branch.startswith("/")
        and not branch.endswith(("/", "."))
        and "//" not in branch
        and ".." not in branch
        and branch != "@"
    )
    if valid:
        components = branch.split("/")
        valid = all(
            component
            and not component.startswith(".")
            and not component.endswith(".lock")
            for component in components
        )
    if not valid:
        raise GitHubCommandError(
            f"invalid branch name: {branch!r}",
            reason="invalid_branch",
        )
    return branch


def validate_commit(commit: str) -> str:
    if not isinstance(commit, str) or not COMMIT_SHA_PATTERN.match(commit or ""):
        raise GitHubCommandError(
            f"invalid commit SHA (expected 40 lowercase hex): {commit!r}",
            reason="invalid_commit",
        )
    return commit


def validate_pr_url(pr_url: str, repo: str) -> str:
    """Validate a canonical GitHub pull-request URL for ``repo``."""
    validate_repo(repo)
    if not isinstance(pr_url, str):
        raise GitHubCommandError(
            f"invalid PR URL: {pr_url!r}",
            reason="invalid_pr_url",
        )
    try:
        pr_url.encode("ascii")
    except UnicodeEncodeError as exc:
        raise GitHubCommandError(
            f"invalid PR URL for {repo}: {pr_url!r}",
            reason="invalid_pr_url",
        ) from exc
    if any(
        character.isspace()
        or ord(character) < 0x20
        or ord(character) == 0x7F
        for character in pr_url
    ):
        raise GitHubCommandError(
            f"invalid PR URL for {repo}: {pr_url!r}",
            reason="invalid_pr_url",
        )
    repo_owner, repo_name = repo.split("/", 1)
    match = re.fullmatch(
        r"https://github\.com/([A-Za-z0-9._-]+)/"
        r"([A-Za-z0-9._-]+)/pull/([1-9][0-9]*)",
        pr_url,
    )
    valid = bool(
        match
        and match.group(1).casefold() == repo_owner.casefold()
        and match.group(2).casefold() == repo_name.casefold()
    )
    if not valid:
        raise GitHubCommandError(
            f"invalid PR URL for {repo}: {pr_url!r}",
            reason="invalid_pr_url",
        )
    return pr_url


def parse_pushed(value: Any) -> bool:
    """Strict boolean parsing for the worker `pushed` field.

    Accepts only the literal strings `true` and `false` (case-insensitive,
    with surrounding whitespace allowed) or Python booleans. ``None`` is
    treated as missing — callers should default to ``False`` if the field
    is required. Anything else raises — workers must report a clear
    yes/no, not arbitrary truthy strings.
    """
    if value is None:
        raise GitHubCommandError(
            "pushed is required (use true or false)",
            reason="invalid_pushed",
        )
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    raise GitHubCommandError(
        f"invalid pushed value (expected true|false): {value!r}",
        reason="invalid_pushed",
    )


def fetch_remote_ref(
    repo: str,
    branch: str,
    *,
    run: RunGh | None = None,
) -> str | None:
    """Return the SHA at the head of `branch` on `repo`, or None if missing.

    Uses `gh api repos/<owner>/<repo>/git/ref/heads/<branch>` so the
    executing host does not need a local checkout of the target repo.
    Raises GitHubCommandError on:
      - invalid repo / branch shape
      - gh CLI missing
      - non-zero exit + a body (treat as missing only when the response
        is GitHub's 404; otherwise raise)
      - invalid JSON / unexpected shape

    The 404 case is the only "missing ref" signal — anything else surfaces
    as an error so callers can fail closed.
    """
    validate_repo(repo)
    validate_branch(branch)
    runner = run or _run_gh
    cmd = ["gh", "api", f"repos/{repo}/git/ref/heads/{branch}"]
    try:
        proc = runner(cmd)
    except FileNotFoundError:
        raise GitHubCommandError("gh CLI not available", reason="gh_missing")
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        # GitHub returns 404 with a JSON message; the gh CLI surfaces it
        # on stderr. Treat "Not Found" as a clean missing-ref signal so the
        # caller can emit push.required; everything else is an error.
        if "Not Found" in stderr or "not found" in stderr.lower():
            return None
        raise GitHubCommandError(
            f"gh api ref lookup failed: {stderr or proc.returncode}",
            reason="gh_failed",
            stderr=stderr,
        )
    try:
        raw = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise GitHubCommandError(
            f"gh api ref returned invalid JSON: {exc}",
            reason="invalid_json",
        ) from exc
    if not isinstance(raw, dict):
        raise GitHubCommandError(
            "gh api ref returned invalid JSON (not an object)",
            reason="invalid_json",
        )
    obj = raw.get("object")
    if not isinstance(obj, dict):
        raise GitHubCommandError(
            "gh api ref JSON missing object field",
            reason="missing_field",
        )
    sha = obj.get("sha")
    if not isinstance(sha, str) or not COMMIT_SHA_PATTERN.match(sha):
        raise GitHubCommandError(
            "gh api ref JSON missing valid sha",
            reason="missing_field",
        )
    return sha


def discover_open_pr_for_head(
    repo: str,
    head_ref: str,
    *,
    expected_head_sha: str | None = None,
    expected_base: str | None = None,
    run: RunGh | None = None,
) -> dict[str, Any] | None:
    """Return an open PR dict for `<owner>:<branch>` on `repo`, or None.

    `head_ref` must be in `owner:branch` form. Phase 8.4 only supports
    same-owner branches; `gh pr list --head` expects the bare branch for this
    case, even though `gh pr create --head` accepts `owner:branch`.

    When `expected_head_sha` is supplied, the returned PR's `headRefOid`
    must match it exactly. When `expected_base` is supplied, the returned
    PR's `baseRefName` must match it exactly. Mismatches raise
    `GitHubCommandError(reason="discovery_mismatch")` so the caller can
    refuse to bind a PR pointing at a different head or base.
    """
    validate_repo(repo)
    if not isinstance(head_ref, str) or ":" not in head_ref:
        raise GitHubCommandError(
            f"head_ref must be in owner:branch form: {head_ref!r}",
            reason="invalid_head",
        )
    head_owner, head_branch = head_ref.split(":", 1)
    repo_owner = repo.split("/", 1)[0]
    if head_owner.casefold() != repo_owner.casefold():
        raise GitHubCommandError(
            f"head owner {head_owner!r} != repo owner {repo_owner!r}",
            reason="invalid_head",
        )
    validate_branch(head_branch)
    if expected_head_sha is not None:
        validate_commit(expected_head_sha)
    if expected_base is not None:
        validate_branch(expected_base)
    runner = run or _run_gh
    cmd = [
        "gh", "pr", "list",
        "--repo", repo,
        "--head", head_branch,
        "--state", "open",
        "--json", (
            "number,url,headRefName,headRefOid,headRepository,"
            "headRepositoryOwner,isCrossRepository,baseRefName,title,state"
        ),
        "--limit", "100",
    ]
    try:
        proc = runner(cmd)
    except FileNotFoundError:
        raise GitHubCommandError("gh CLI not available", reason="gh_missing")
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise GitHubCommandError(
            f"gh pr list failed: {stderr or proc.returncode}",
            reason="gh_failed",
            stderr=stderr,
        )
    try:
        raw = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise GitHubCommandError(
            f"gh pr list returned invalid JSON: {exc}",
            reason="invalid_json",
        ) from exc
    if not isinstance(raw, list):
        raise GitHubCommandError(
            "gh pr list returned invalid JSON (not an array)",
            reason="invalid_json",
        )
    if not raw:
        return None
    same_repo: list[dict[str, Any]] = []
    for candidate in raw:
        if not isinstance(candidate, dict):
            raise GitHubCommandError(
                "gh pr list returned invalid PR entry",
                reason="invalid_json",
            )
        owner = candidate.get("headRepositoryOwner")
        repository = candidate.get("headRepository")
        candidate_url = candidate.get("url")
        try:
            validate_pr_url(candidate_url, repo)
        except GitHubCommandError:
            canonical_url = False
        else:
            canonical_url = True
        if (
            candidate.get("headRefName") == head_branch
            and isinstance(owner, dict)
            and isinstance(owner.get("login"), str)
            and owner["login"].casefold() == head_owner.casefold()
            and isinstance(repository, dict)
            and isinstance(repository.get("nameWithOwner"), str)
            and repository["nameWithOwner"].casefold() == repo.casefold()
            and candidate.get("isCrossRepository") is False
            and canonical_url
        ):
            same_repo.append(candidate)
    if not same_repo:
        raise GitHubCommandError(
            f"open PR candidates for branch {head_branch!r} did not belong "
            f"to same repository {repo!r}",
            reason="discovery_mismatch",
        )

    matches = same_repo
    if expected_head_sha is not None:
        matches = [
            candidate for candidate in matches
            if isinstance(candidate.get("headRefOid"), str)
            and candidate["headRefOid"].lower() == expected_head_sha.lower()
        ]
        if not matches:
            observed = [candidate.get("headRefOid") for candidate in same_repo]
            raise GitHubCommandError(
                f"discovered same-repo PR heads {observed!r} did not include "
                f"expected commit {expected_head_sha!r}",
                reason="discovery_mismatch",
            )
    if expected_base is not None:
        base_matches = [
            candidate for candidate in matches
            if candidate.get("baseRefName") == expected_base
        ]
        if not base_matches:
            observed = [candidate.get("baseRefName") for candidate in matches]
            raise GitHubCommandError(
                f"discovered same-repo PR bases {observed!r} did not include "
                f"expected base {expected_base!r}",
                reason="discovery_mismatch",
            )
        matches = base_matches
    return matches[0]


def create_pr(
    repo: str,
    head_ref: str,
    base: str,
    *,
    title: str,
    body: str,
    run: RunGh | None = None,
) -> str:
    """Create a PR via `gh pr create` and return the new PR URL.

    Validates repo / head_ref / base / title before invoking gh. Title and
    body are passed as separate argv entries (no shell interpolation).
    Returns the printed PR URL.
    """
    validate_repo(repo)
    if not isinstance(head_ref, str) or ":" not in head_ref:
        raise GitHubCommandError(
            f"head_ref must be in owner:branch form: {head_ref!r}",
            reason="invalid_head",
        )
    validate_branch(base)
    if not isinstance(title, str) or not title.strip():
        raise GitHubCommandError(
            "title is required for gh pr create",
            reason="invalid_title",
        )
    if not isinstance(body, str):
        raise GitHubCommandError(
            "body must be a string (empty allowed)",
            reason="invalid_body",
        )
    runner = run or _run_gh
    cmd = [
        "gh", "pr", "create",
        "--repo", repo,
        "--head", head_ref,
        "--base", base,
        "--title", title,
        "--body", body,
    ]
    try:
        proc = runner(cmd)
    except FileNotFoundError:
        raise GitHubCommandError("gh CLI not available", reason="gh_missing")
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise GitHubCommandError(
            f"gh pr create failed: {stderr or proc.returncode}",
            reason="gh_failed",
            stderr=stderr,
        )
    stdout = (proc.stdout or "").strip()
    if not stdout:
        raise GitHubCommandError(
            "gh pr create returned empty stdout",
            reason="missing_field",
        )
    return stdout.splitlines()[-1].strip()


def _run_gh(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
