"""Tests for host-scoped normalized worktree resource identity."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from coordinate.execution_resources import (
    RESOURCE_CONTRACT_VERSION,
    ResourceIdentityError,
    build_worktree_resource,
    compute_resource_key,
    normalize_worktree_path,
    validate_resource_key_matches,
)


class PosixPathTests(unittest.TestCase):
    def test_dot_segments_collapsed(self):
        self.assertEqual(
            normalize_worktree_path("/a/b/../c"),
            "/a/c",
        )

    def test_trailing_slash_removed(self):
        self.assertEqual(
            normalize_worktree_path("/a/b/"),
            "/a/b",
        )

    def test_root_preserved(self):
        self.assertEqual(normalize_worktree_path("/"), "/")

    def test_duplicate_separators_collapsed(self):
        self.assertEqual(
            normalize_worktree_path("/a//b///c"),
            "/a/b/c",
        )

    def test_nfc_normalized(self):
        # e + combining acute should become é (NFC)
        import unicodedata

        e_combining = "e\u0301"
        self.assertNotEqual(e_combining, unicodedata.normalize("NFC", e_combining))
        self.assertEqual(
            normalize_worktree_path("/tmp/" + e_combining),
            "/tmp/" + unicodedata.normalize("NFC", e_combining),
        )

    def test_case_preserved(self):
        self.assertEqual(
            normalize_worktree_path("/Users/Alice"),
            "/Users/Alice",
        )

    def test_relative_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "must be absolute"):
            normalize_worktree_path("relative/path")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "path is required"):
            normalize_worktree_path("")

    def test_control_character_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "control characters"):
            normalize_worktree_path("/tmp/bad\x01")

    def test_overlong_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "exceeds 4096"):
            normalize_worktree_path("/" + "a" * 5000)


class WindowsPathTests(unittest.TestCase):
    def test_drive_casefold(self):
        self.assertEqual(
            normalize_worktree_path("C:\\Users\\Alice"),
            "c:\\users\\alice",
        )

    def test_forward_slash_canonicalized(self):
        self.assertEqual(
            normalize_worktree_path("C:/Users/Alice"),
            "c:\\users\\alice",
        )

    def test_unc_root(self):
        self.assertEqual(
            normalize_worktree_path("\\\\server\\share"),
            "\\\\server\\share",
        )

    def test_unc_casefold(self):
        self.assertEqual(
            normalize_worktree_path("\\\\SERVER\\Share"),
            "\\\\server\\share",
        )

    def test_dot_segments_collapsed(self):
        self.assertEqual(
            normalize_worktree_path("C:\\a\\b\\..\\c"),
            "c:\\a\\c",
        )

    def test_relative_drive_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "must be absolute"):
            normalize_worktree_path("a:relative")

    def test_forward_slash_unc_canonicalized(self):
        self.assertEqual(
            normalize_worktree_path("//SERVER/Share/path"),
            "\\\\server\\share\\path",
        )

    def test_non_absolute_unc_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "must be absolute"):
            normalize_worktree_path("\\server\\share")

    def test_bare_drive_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "must be absolute"):
            normalize_worktree_path("C:")

    def test_mixed_separator_casefold(self):
        self.assertEqual(
            normalize_worktree_path("C:/Users\\Alice"),
            "c:\\users\\alice",
        )

    def test_host_id_whitespace_rejected(self):
        with self.assertRaisesRegex(ResourceIdentityError, "surrounding whitespace"):
            build_worktree_resource(" host1", "/tmp/ws")
        with self.assertRaisesRegex(ResourceIdentityError, "surrounding whitespace"):
            build_worktree_resource("host1 ", "/tmp/ws")



class ResourceKeyTests(unittest.TestCase):
    def test_same_host_same_path_same_key(self):
        r1 = build_worktree_resource("host1", "/tmp/ws")
        r2 = build_worktree_resource("host1", "/tmp/ws/")
        self.assertEqual(compute_resource_key(r1), compute_resource_key(r2))

    def test_different_host_different_key(self):
        r1 = build_worktree_resource("host1", "/tmp/ws")
        r2 = build_worktree_resource("host2", "/tmp/ws")
        self.assertNotEqual(compute_resource_key(r1), compute_resource_key(r2))

    def test_equivalent_posix_same_key(self):
        r1 = build_worktree_resource("host1", "/tmp/a/../ws")
        r2 = build_worktree_resource("host1", "/tmp/ws")
        self.assertEqual(compute_resource_key(r1), compute_resource_key(r2))

    def test_equivalent_windows_same_key(self):
        r1 = build_worktree_resource("host1", "C:\\Users\\WS")
        r2 = build_worktree_resource("host1", "c:/users/ws")
        self.assertEqual(compute_resource_key(r1), compute_resource_key(r2))

    def test_host_id_validation(self):
        with self.assertRaisesRegex(ResourceIdentityError, "host_id is required"):
            build_worktree_resource("", "/tmp/ws")
        with self.assertRaisesRegex(ResourceIdentityError, "exceeds 64 characters"):
            build_worktree_resource("h" * 65, "/tmp/ws")


class StoredResourceValidationTests(unittest.TestCase):
    """Direct validator and read-path tamper tests for stored resource snapshots."""

    def _resource(self, host_id: str = "host1", normalized_path: str = "/tmp/ws") -> dict[str, object]:
        return {
            "contract_version": RESOURCE_CONTRACT_VERSION,
            "resource_kind": "worktree",
            "host_id": host_id,
            "normalized_path": normalized_path,
        }

    def _tampered_key(self, resource: dict[str, object]) -> str:
        """Compute the key an attacker would need for a tampered resource object."""
        canonical = json.dumps(
            {
                "contract_version": resource["contract_version"],
                "host_id": resource["host_id"],
                "normalized_path": resource["normalized_path"],
                "resource_kind": resource["resource_kind"],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def test_valid_stored_resource_passes(self):
        resource = self._resource()
        key = self._tampered_key(resource)
        canonical = validate_resource_key_matches(resource, key)
        self.assertEqual(canonical["host_id"], "host1")
        self.assertEqual(canonical["normalized_path"], "/tmp/ws")

    def test_rejects_whitespace_host_id(self):
        resource = self._resource(host_id=" host1")
        key = self._tampered_key(resource)
        with self.assertRaisesRegex(ResourceIdentityError, "surrounding whitespace"):
            validate_resource_key_matches(resource, key)

    def test_rejects_empty_host_id(self):
        resource = self._resource(host_id="")
        key = self._tampered_key(resource)
        with self.assertRaisesRegex(ResourceIdentityError, "host_id is required"):
            validate_resource_key_matches(resource, key)

    def test_rejects_non_canonical_trailing_slash(self):
        resource = self._resource(normalized_path="/tmp/ws/")
        key = self._tampered_key(resource)
        with self.assertRaisesRegex(ResourceIdentityError, "not canonical"):
            validate_resource_key_matches(resource, key)

    def test_rejects_non_canonical_dot_segments(self):
        resource = self._resource(normalized_path="/tmp/a/../ws")
        key = self._tampered_key(resource)
        with self.assertRaisesRegex(ResourceIdentityError, "not canonical"):
            validate_resource_key_matches(resource, key)

    def test_rejects_relative_path(self):
        resource = self._resource(normalized_path="relative/path")
        key = self._tampered_key(resource)
        with self.assertRaisesRegex(ResourceIdentityError, "must be absolute"):
            validate_resource_key_matches(resource, key)

    def test_rejects_control_character_path(self):
        resource = self._resource(normalized_path="/tmp/bad\x01")
        key = self._tampered_key(resource)
        with self.assertRaisesRegex(ResourceIdentityError, "control characters"):
            validate_resource_key_matches(resource, key)

    def test_rejects_overlong_path(self):
        resource = self._resource(normalized_path="/" + "a" * 5000)
        key = self._tampered_key(resource)
        with self.assertRaisesRegex(ResourceIdentityError, "exceeds 4096"):
            validate_resource_key_matches(resource, key)

    def test_rejects_uppercase_digest(self):
        resource = self._resource()
        key = "sha256:" + "A" * 64
        with self.assertRaisesRegex(ResourceIdentityError, "sha256:<64 lowercase hex>"):
            validate_resource_key_matches(resource, key)

    def test_rejects_truncated_digest(self):
        resource = self._resource()
        key = "sha256:" + "a" * 63
        with self.assertRaisesRegex(ResourceIdentityError, "sha256:<64 lowercase hex>"):
            validate_resource_key_matches(resource, key)

    def test_rejects_nonhex_digest(self):
        resource = self._resource()
        key = "sha256:" + "g" * 64
        with self.assertRaisesRegex(ResourceIdentityError, "sha256:<64 lowercase hex>"):
            validate_resource_key_matches(resource, key)

    def test_rejects_non_string_resource_key(self):
        resource = self._resource()
        with self.assertRaisesRegex(ResourceIdentityError, "resource_key must be a string"):
            validate_resource_key_matches(resource, None)  # type: ignore[arg-type]

    def test_rejects_digest_tamper(self):
        resource = self._resource()
        with self.assertRaisesRegex(ResourceIdentityError, "digest mismatch"):
            validate_resource_key_matches(resource, "sha256:" + "0" * 64)


if __name__ == "__main__":
    unittest.main()
