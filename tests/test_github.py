"""Tests for the Phase 8.4 GitHub helper layer.

Covers:
- strict format validators (repo/branch/commit/pushed)
- fetch_remote_ref happy path / 404 / gh failure / invalid JSON / missing field
- discover_open_pr_for_head happy path / no PR / gh failure / invalid JSON
- create_pr happy path / gh failure / missing url / validation

All subprocess calls go through injected runners so no real `gh` is invoked.
"""

from __future__ import annotations

import json
import subprocess
import unittest

from coordinate.github import (
    BRANCH_PATTERN,
    COMMIT_SHA_PATTERN,
    REPO_PATTERN,
    GitHubCommandError,
    create_pr,
    discover_open_pr_for_head,
    fetch_remote_ref,
    parse_pushed,
    validate_branch,
    validate_commit,
    validate_pr_url,
    validate_repo,
)


SHA = "0123456789abcdef0123456789abcdef01234567"


def _proc(stdout="", returncode=0, stderr=""):
    return subprocess.CompletedProcess(
        args=["gh"], returncode=returncode, stdout=stdout, stderr=stderr,
    )


class TestValidators(unittest.TestCase):
    def test_validate_repo_accepts_owner_name(self):
        self.assertEqual(validate_repo("acme/repo"), "acme/repo")

    def test_validate_repo_rejects_invalid_shapes(self):
        for bad in [
            "", "no-slash", "OWNER/repo", "acme//repo", "acme/", "/acme",
            "ac me/repo", "acme/.", "acme/..", "./repo", "../repo",
        ]:
            with self.assertRaises(GitHubCommandError) as ctx:
                validate_repo(bad)
            self.assertEqual(ctx.exception.reason, "invalid_repo")

    def test_validate_branch_accepts_slash(self):
        self.assertEqual(
            validate_branch("agents/mac-claude/phase-8.4"),
            "agents/mac-claude/phase-8.4",
        )

    def test_validate_branch_rejects_unsafe(self):
        for bad in [
            "", "-leading-option", "with space", "with\ttab", "with\nnewline",
            "with;semi", "../escape", "agents//task", ".hidden/task",
            "agents/a..b", "agents/task.lock", "agents/task/", "agents/task.",
            "@",
        ]:
            with self.assertRaises(GitHubCommandError) as ctx:
                validate_branch(bad)
            self.assertEqual(ctx.exception.reason, "invalid_branch")

    def test_validate_commit_accepts_40_hex(self):
        self.assertEqual(validate_commit(SHA), SHA)
        self.assertEqual(validate_commit("0" * 40), "0" * 40)

    def test_validate_commit_rejects_bad(self):
        for bad in ["", SHA.upper(), SHA[:39], SHA + "extra", "not-a-sha"]:
            with self.assertRaises(GitHubCommandError) as ctx:
                validate_commit(bad)
            self.assertEqual(ctx.exception.reason, "invalid_commit")

    def test_validate_pr_url_accepts_canonical_case_insensitive_repo_path(self):
        url = "https://github.com/BurntSushi/ripgrep/pull/123"
        self.assertEqual(validate_pr_url(url, "burntsushi/ripgrep"), url)

    def test_validate_pr_url_rejects_wrong_repo_or_noncanonical_parts(self):
        for bad in [
            123,
            "http://github.com/acme/repo/pull/1",
            "https://evil.invalid/acme/repo/pull/1",
            "https://github.com/acme/other/pull/1",
            "https://github.com/acme/repo/issues/1",
            "https://github.com/acme/repo/pull/not-a-number",
            "https://github.com/acme/repo/pull/1?diff=split",
            "https://github.com/acme/repo/pull/١",
            "https://github.com/acme/repo/pull/²",
            "https://github.com/acme/repo/pull/1?",
            "https://github.com/acme/repo/pull/1#",
            "https://[github.com/acme/repo/pull/1",
            " https://github.com/acme/repo/pull/1",
            "\x00https://github.com/acme/repo/pull/1",
            "https://git\thub.com/acme/repo/pull/1",
            "https://github.com/ac\nme/repo/pull/1",
            "https://github.com/acme/repo/pull/1\n",
            "https://github.com/acme/repo/pull/0",
            "https://github.com/acme/repo/pull/01",
            "https://github.com/acme/repo/pull/1;",
        ]:
            with self.subTest(url=bad):
                with self.assertRaises(GitHubCommandError) as ctx:
                    validate_pr_url(bad, "acme/repo")
                self.assertEqual(ctx.exception.reason, "invalid_pr_url")

    def test_parse_pushed_strict(self):
        self.assertTrue(parse_pushed(True))
        self.assertTrue(parse_pushed("true"))
        self.assertTrue(parse_pushed("TRUE"))
        self.assertFalse(parse_pushed(False))
        self.assertFalse(parse_pushed("false"))
        for bad in ["1", "0", "yes", "no", "", " True ", "True "]:
            if bad.strip().lower() in {"true", "false"}:
                continue
            with self.assertRaises(GitHubCommandError) as ctx:
                parse_pushed(bad)
            self.assertEqual(ctx.exception.reason, "invalid_pushed")
        # None is treated as "missing required field" — daemon must default
        # before calling parse_pushed.
        with self.assertRaises(GitHubCommandError) as ctx:
            parse_pushed(None)
        self.assertEqual(ctx.exception.reason, "invalid_pushed")

    def test_patterns_match_practical_inputs(self):
        self.assertTrue(REPO_PATTERN.match("a/b"))
        self.assertTrue(COMMIT_SHA_PATTERN.match("f" * 40))
        self.assertTrue(BRANCH_PATTERN.match("release/v1.2.3-rc4"))


class TestFetchRemoteRef(unittest.TestCase):
    def test_returns_sha_on_match(self):
        def fake(_cmd):
            return _proc(json.dumps({"object": {"sha": SHA}}))
        self.assertEqual(fetch_remote_ref("acme/repo", "main", run=fake), SHA)

    def test_returns_none_when_404(self):
        def fake(_cmd):
            return _proc(returncode=1, stderr="gh: Not Found (HTTP 404)")
        self.assertIsNone(fetch_remote_ref("acme/repo", "missing", run=fake))

    def test_raises_on_gh_failure(self):
        def fake(_cmd):
            return _proc(returncode=1, stderr="authentication required")
        with self.assertRaises(GitHubCommandError) as ctx:
            fetch_remote_ref("acme/repo", "main", run=fake)
        self.assertEqual(ctx.exception.reason, "gh_failed")

    def test_raises_on_invalid_json(self):
        def fake(_cmd):
            return _proc(stdout="not json")
        with self.assertRaises(GitHubCommandError) as ctx:
            fetch_remote_ref("acme/repo", "main", run=fake)
        self.assertEqual(ctx.exception.reason, "invalid_json")

    def test_raises_on_missing_sha(self):
        def fake(_cmd):
            return _proc(json.dumps({"object": {}}))
        with self.assertRaises(GitHubCommandError) as ctx:
            fetch_remote_ref("acme/repo", "main", run=fake)
        self.assertEqual(ctx.exception.reason, "missing_field")

    def test_raises_on_missing_object(self):
        def fake(_cmd):
            return _proc(json.dumps({}))
        with self.assertRaises(GitHubCommandError) as ctx:
            fetch_remote_ref("acme/repo", "main", run=fake)
        self.assertEqual(ctx.exception.reason, "missing_field")

    def test_invalid_repo_raises(self):
        with self.assertRaises(GitHubCommandError):
            fetch_remote_ref("BAD/NAME", "main", run=lambda _c: _proc("{}"))

    def test_gh_missing_raises(self):
        def fake(_cmd):
            raise FileNotFoundError("gh not found")
        with self.assertRaises(GitHubCommandError) as ctx:
            fetch_remote_ref("acme/repo", "main", run=fake)
        self.assertEqual(ctx.exception.reason, "gh_missing")


class TestDiscoverOpenPr(unittest.TestCase):
    @staticmethod
    def _same_repo_pr(**overrides):
        result = {
            "number": 3,
            "url": "https://github.com/acme/repo/pull/3",
            "headRefName": "branch",
            "headRefOid": SHA,
            "headRepository": {"nameWithOwner": "acme/repo"},
            "headRepositoryOwner": {"login": "acme"},
            "isCrossRepository": False,
            "baseRefName": "main",
            "title": "fix",
            "state": "OPEN",
        }
        result.update(overrides)
        return result

    def test_same_repo_owner_head_is_normalized_to_branch_for_gh_cli(self):
        captured = []

        def fake(cmd):
            captured.append(cmd)
            return _proc("[]")

        discover_open_pr_for_head("acme/repo", "acme:branch", run=fake)

        command = captured[0]
        self.assertEqual(command[command.index("--head") + 1], "branch")

    def test_returns_first_pr(self):
        def fake(_cmd):
            return _proc(json.dumps([self._same_repo_pr()]))
        result = discover_open_pr_for_head("acme/repo", "acme:branch", run=fake)
        self.assertIsNotNone(result)
        self.assertEqual(result["url"], "https://github.com/acme/repo/pull/3")

    def test_skips_same_branch_fork_and_selects_exact_same_repo_pr(self):
        fork = self._same_repo_pr(
            number=7,
            url="https://github.com/acme/repo/pull/7",
            headRepository={"nameWithOwner": "attacker/repo"},
            headRepositoryOwner={"login": "attacker"},
            isCrossRepository=True,
        )
        exact = self._same_repo_pr()

        result = discover_open_pr_for_head(
            "acme/repo",
            "acme:branch",
            expected_head_sha=SHA,
            expected_base="main",
            run=lambda _cmd: _proc(json.dumps([fork, exact])),
        )

        self.assertEqual(result["url"], exact["url"])

    def test_rejects_when_only_same_branch_fork_exists(self):
        fork = self._same_repo_pr(
            headRepository={"nameWithOwner": "attacker/repo"},
            headRepositoryOwner={"login": "attacker"},
            isCrossRepository=True,
        )

        with self.assertRaises(GitHubCommandError) as ctx:
            discover_open_pr_for_head(
                "acme/repo",
                "acme:branch",
                expected_head_sha=SHA,
                expected_base="main",
                run=lambda _cmd: _proc(json.dumps([fork])),
            )

        self.assertEqual(ctx.exception.reason, "discovery_mismatch")

    def test_rejects_same_repo_metadata_with_noncanonical_pr_url(self):
        for bad_url in [123, "https://evil.invalid/acme/repo/pull/3"]:
            with self.subTest(url=bad_url):
                candidate = self._same_repo_pr(url=bad_url)
                with self.assertRaises(GitHubCommandError) as ctx:
                    discover_open_pr_for_head(
                        "acme/repo",
                        "acme:branch",
                        run=lambda _cmd: _proc(json.dumps([candidate])),
                    )
                self.assertEqual(ctx.exception.reason, "discovery_mismatch")

    def test_accepts_canonical_mixed_case_repository_metadata(self):
        candidate = self._same_repo_pr(
            url="https://github.com/Acme/Repo/pull/3",
            headRepository={"nameWithOwner": "Acme/Repo"},
            headRepositoryOwner={"login": "Acme"},
        )

        result = discover_open_pr_for_head(
            "acme/repo",
            "acme:branch",
            run=lambda _cmd: _proc(json.dumps([candidate])),
        )

        self.assertEqual(result["url"], candidate["url"])

    def test_returns_none_when_empty(self):
        def fake(_cmd):
            return _proc(json.dumps([]))
        self.assertIsNone(discover_open_pr_for_head("acme/repo", "acme:branch", run=fake))

    def test_invalid_head_raises(self):
        with self.assertRaises(GitHubCommandError) as ctx:
            discover_open_pr_for_head("acme/repo", "no-colon", run=lambda _c: _proc("[]"))
        self.assertEqual(ctx.exception.reason, "invalid_head")

    def test_gh_failure_raises(self):
        def fake(_cmd):
            return _proc(returncode=1, stderr="auth failed")
        with self.assertRaises(GitHubCommandError) as ctx:
            discover_open_pr_for_head("acme/repo", "acme:branch", run=fake)
        self.assertEqual(ctx.exception.reason, "gh_failed")

    def test_invalid_json_raises(self):
        def fake(_cmd):
            return _proc(stdout="not json")
        with self.assertRaises(GitHubCommandError) as ctx:
            discover_open_pr_for_head("acme/repo", "acme:branch", run=fake)
        self.assertEqual(ctx.exception.reason, "invalid_json")

    def test_non_list_json_raises(self):
        def fake(_cmd):
            return _proc(json.dumps({"url": "x"}))
        with self.assertRaises(GitHubCommandError) as ctx:
            discover_open_pr_for_head("acme/repo", "acme:branch", run=fake)
        self.assertEqual(ctx.exception.reason, "invalid_json")


class TestCreatePr(unittest.TestCase):
    def test_returns_last_line_of_stdout(self):
        def fake(_cmd):
            return _proc(stdout="Creating PR...\nhttps://github.com/acme/repo/pull/42\n")
        url = create_pr(
            "acme/repo", "acme:branch", "main",
            title="t", body="b", run=fake,
        )
        self.assertEqual(url, "https://github.com/acme/repo/pull/42")

    def test_raises_on_gh_failure(self):
        def fake(_cmd):
            return _proc(returncode=1, stderr="title required")
        with self.assertRaises(GitHubCommandError) as ctx:
            create_pr("acme/repo", "acme:branch", "main", title="t", body="b", run=fake)
        self.assertEqual(ctx.exception.reason, "gh_failed")

    def test_raises_on_empty_stdout(self):
        def fake(_cmd):
            return _proc(stdout="")
        with self.assertRaises(GitHubCommandError) as ctx:
            create_pr("acme/repo", "acme:branch", "main", title="t", body="b", run=fake)
        self.assertEqual(ctx.exception.reason, "missing_field")

    def test_invalid_inputs_raise(self):
        cases = [
            dict(repo="BAD/REPO", head_ref="x:b", base="main", title="t", body=""),
            dict(repo="a/b", head_ref="no-colon", base="main", title="t", body=""),
            dict(repo="a/b", head_ref="x:b", base="", title="t", body=""),
            dict(repo="a/b", head_ref="x:b", base="main", title="", body=""),
            dict(repo="a/b", head_ref="x:b", base="main", title="t", body=12345),
        ]
        for kw in cases:
            with self.assertRaises(GitHubCommandError):
                create_pr(**kw, run=lambda _c: _proc("https://x"))

    def test_argv_shape(self):
        captured = []

        def fake(cmd):
            captured.append(list(cmd))
            return _proc(stdout="https://github.com/a/b/pull/1\n")

        create_pr("a/b", "owner:feat/x", "main", title="t", body="b", run=fake)
        self.assertEqual(captured[0][:5], ["gh", "pr", "create", "--repo", "a/b"])
        self.assertIn("--head", captured[0])
        self.assertIn("--base", captured[0])
        self.assertIn("main", captured[0])
        # title/body are separate argv entries, not shell strings.
        self.assertIn("--title", captured[0])
        self.assertIn("t", captured[0])
        self.assertIn("--body", captured[0])
        self.assertIn("b", captured[0])
