"""Boundary tests for the event presentation registry extraction."""

from __future__ import annotations

import ast
import hashlib
import json
import sqlite3
import subprocess
import sys
import unittest
from pathlib import Path

# Portable AST projection hashes generated from reviewed start 882c2a1
# using ast.iter_fields recursion, dropping None/empty list/tuple,
# sorted-key compact JSON, and SHA-256 of UTF-8 bytes.
EXPECTED_FUNCTION_HASHES = {
    "_base_payload": "9f31f41d754fed8aaba976ff909e1b23068c6b01ff7edbf19c58cd12e820997d",
    "_job_completed_text": "53bcfd5bd602f1c977b5f31f18cfbea01d63cf67a081c7e9ac89cb6064ffa4d5",
    "_job_failed_text": "544944a4049e5283e26ebfca1458812a29d697e5de2362fcb001877b8104c627",
    "_plan_ready_text": "ca20f509ab9b9e1ff9c140726930a5031b6bea71b030ffdf243a8993a08c6e4f",
    "_plan_review_requested_text": "715be4fb740370d474d9ed84a5481b6533ee8997a6772d947a729900533d07cc",
    "_plan_approved_text": "0c8d55e4f5491f943c75afa866a1d6f6dd740ff7605b424f0c051cc483ced4b8",
    "_plan_rejected_text": "edacb6f594e83f471164b1539373ccc238e15cfe9ef4eb33375858b261afe766",
    "_worker_handoff_text": "ce6f5aee0e74e506c6c45ae96f199cf64c5a6a4a6fc4358b67a9545e55dc928d",
    "_task_mirror_text": "c400df97a5c8cee3f21175d673c0b584f0a08a46918816b75d19f9a678deb04b",
    "_reconciliation_text": "31916c412e542b2dc1b38d04b9f8830c53440fb2fca592ab6ce3dfcceaee48d4",
    "_links": "2656d21de602fbbdd88ec187a8f66816290b50bba8c975622f947aac4d440aea",
    "_harness_mutation_failed_text": "e525cb7592a7bad99e3e862378bb6b9ae9d8c8fbe5915ff188d336131d9b381f",
    "_issue_spotted_text": "bff7439ff793333916363395b0bd974416ddf74af988a651570d7c196c2b12ef",
    "_issue_triaged_text": "d04a85259d22a8b5fc273eabdd9a151990b7637a31c6630f6e3700571c432b4b",
    "_issue_materialized_text": "6297033b2b76984822d0e53e00eb9966a178b9d142bb6d64dd44b65cea1e642e",
    "_assignment_requested_text": "9975514d13810d8d01f9ae8041d1ef81e9be377c71692856c422f327964bd7c7",
    "_blocker_raised_text": "be6baee93a989d107cb114e152802569e325bb06696947b3eab895a56b0293d8",
    "_blocker_resolved_text": "faff61a24bc6d91231cc163c18540a7038e2f58f59f300ed6ff0cf77a5296016",
    "_closeout_requested_text": "0fdb82456d13cf36d77ee3899000913368baf9b3ac8bcdae28d89dec1541569a",
    "_review_completed_text": "9628aa843052ea9dff3663d377cb82effee09a9850bbae7e0d4014f46d3bb04e",
    "_review_rejected_text": "5e18f9e41f54aef2a73ca5cda474251d7d760f5749c730b97f989e1d69f09010",
    "_progress_reported_text": "35eac7dcad8be1452c8c4805432dd4f9559cb6c5e7657e10baee05b3d4f8d18c",
    "_task_done_text": "9ff0f7f2854e06a32c34ad9f48f8d594df4e499d640ef6d7bae65f701bd6dd70",
    "_pr_linked_text": "69f667e44564804bf1df5c3026947283c5917f5103d56cc230bf57ba4c5dc597",
    "_pr_created_text": "7010c1594de5bbb1fb5f7dd57fdada4aa664d6f0a8dc68ecd551d11d1a126da4",
    "_push_required_text": "701226f251a995c6b96239330f4b0bfe9d3d3ac6b5d97a3c2b06cdd63b301f49",
    "_publish_blocked_text": "fb8e1976310f4e5d257f8d7b99cd1a6c0b215ffa154fb59da8e4273cdd7c622a",
    "_ci_failed_text": "cc693be3a3d40275cf3875196a50011c7333f4182ccf064de2e89d579088dde8",
    "_ci_passed_text": "cd1c13b960e48d196cd34955c0ad8367f05055e4d21241d60ad37d89ac55691a",
    "_handoff_requested_text": "fabfc45df74c547b85b119f8a2b66bc79cf8d1fad4d07f6c62e74cedbabcdc64",
    "_assignment_accepted_text": "f9d5f303313a7ab248207755ba64b2499444764b728c267969133e47f5fea43d",
    "_task_label": "0682bb67b1b437c8257780249de11af92249e82b3340894c05d80c09117f6a88",
    "_visible_block": "f1f8b23e5687a6db236b4e05ef87534800ff665d35a13f05ce3a128719a2d5dd",
    "_compact_visible": "0b525d99469bc732eb09ba8a1697ceea44d5824a3ac1909d3b043a4c74d8f292",
    "_optional_suffix": "ddf26592676dc304f759935bf50916e939273a04e71094e8faac8c857ac6392a",
    "_pr_review_approved_text": "b45b8293231a0e58747fe5f8a52056e62d06ebbacd795866464d2b0b15b2152f",
    "_pr_review_changes_requested_text": "d8cad72a19cd9e18124b23ffb3a37e722a3cf645af467bb7d6ee3eca53f654ec",
    "_pr_review_required_text": "3b282e05f5357442735607c558e2fa48f15726a1f5be3758f8ad16b8a132c900",
    "_agent_reported_header": "afc524bab56f3a5ec8eb785e3059f45bac103688505237d792a4a07d68406c8a",
    "_agent_reported_text": "ec45bd30723e681893940f574838dc357738070b4744716cd7d4aecc247be9ab",
    "_standard_base_renderer": "a00154e7135cb308ffc887dd4b92f3825b137f6f41a5715dd2c88b53b8393913",
    "_render_agent_reported_base": "aaf18efe7e0a9322f8805ec7c9983d2c327f216f105ff0b7c01203695adf1221",
    "_render_assignment_accepted_base": "1391e5c7d013f49075f51ab25421530937f7106bd09759db2d711ed014873245",
    "_render_assignment_requested_base": "d8492d1674c616c1c9530e937e669d27b6d6a78828cb1c60f95b549698c0dd19",
}

REGISTRY_ASSIGNMENT_HASH = "82f7ca56d06084cf53f35a70c7724e393f26c6ed0b28e7598c10a006cc8d4301"

EXPECTED_UNSTYLED = frozenset({
    "issue.materialized",
    "issue.triaged",
    "review.rejected",
})

MOVED_NAMES = list(EXPECTED_FUNCTION_HASHES)


SRC = Path(__file__).parent.parent / "src"


def _project(node: ast.AST | list | tuple | None | object) -> object:
    """Portable canonical projection using ast.iter_fields."""
    if isinstance(node, ast.AST):
        result: dict[str, object] = {"_type": node.__class__.__name__}
        for field, value in ast.iter_fields(node):
            projected = _project(value)
            if projected is not None:
                result[field] = projected
        return result
    elif isinstance(node, (list, tuple)):
        projected = [_project(item) for item in node]
        projected = [item for item in projected if item is not None]
        if not projected:
            return None
        return projected
    elif node is None:
        return None
    else:
        return node


def _portable_hash(node: ast.AST) -> str:
    proj = _project(node)
    text = json.dumps(proj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _function_defs(path: Path) -> dict[str, ast.FunctionDef]:
    tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
    return {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }


class ObjectIdentityTests(unittest.TestCase):
    def test_moved_functions_object_identical(self) -> None:
        from coordinate import event_presentation, policy
        for name in MOVED_NAMES:
            with self.subTest(name=name):
                self.assertIs(
                    getattr(policy, name),
                    getattr(event_presentation, name),
                    f"policy.{name} must re-export event_presentation.{name}",
                )

    def test_registry_object_identical(self) -> None:
        from coordinate import event_presentation, policy
        self.assertIs(
            policy._EVENT_BASE_PAYLOAD_RENDERERS,
            event_presentation._EVENT_BASE_PAYLOAD_RENDERERS,
        )

    def test_unstyled_set_object_identical(self) -> None:
        from coordinate import event_presentation, policy
        self.assertIs(
            policy.EXPLICITLY_UNSTYLED_EVENT_TYPES,
            event_presentation.EXPLICITLY_UNSTYLED_EVENT_TYPES,
        )


class OwnershipTests(unittest.TestCase):
    def test_policy_has_no_moved_function_definitions(self) -> None:
        funcs = _function_defs(SRC / "coordinate" / "policy.py")
        for name in MOVED_NAMES:
            self.assertNotIn(name, funcs, f"policy.py must not define {name}")

    def test_presentation_has_all_moved_function_definitions(self) -> None:
        funcs = _function_defs(SRC / "coordinate" / "event_presentation.py")
        for name in MOVED_NAMES:
            self.assertIn(name, funcs, f"event_presentation.py must define {name}")

    def test_policy_keeps_facade_authorities(self) -> None:
        funcs = _function_defs(SRC / "coordinate" / "policy.py")
        required = {
            "_render_event_base_payload",
            "_event_payload",
            "_enrich_with_embed",
            "_delivery_for_message_key",
        }
        for name in required:
            self.assertIn(name, funcs, f"policy.py must retain {name}")

    def test_presentation_imports_only_stdlib(self) -> None:
        tree = ast.parse(
            (SRC / "coordinate" / "event_presentation.py").read_text(encoding="utf-8"),
            "event_presentation.py",
        )
        for node in tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertNotIn(
                        ".", alias.name,
                        "event_presentation must not use dotted imports",
                    )
                    self.assertTrue(
                        _is_stdlib_module(alias.name),
                        f"{alias.name} is not an allowed stdlib import",
                    )
            elif isinstance(node, ast.ImportFrom):
                self.assertEqual(
                    node.level, 0,
                    "event_presentation must not use relative imports",
                )
                self.assertTrue(
                    _is_stdlib_module(node.module),
                    f"{node.module} is not an allowed stdlib import",
                )


class PortableWitnessTests(unittest.TestCase):
    def test_function_hashes_match_reviewed_start(self) -> None:
        funcs = _function_defs(SRC / "coordinate" / "event_presentation.py")
        for name, expected in EXPECTED_FUNCTION_HASHES.items():
            with self.subTest(name=name):
                self.assertIn(name, funcs)
                actual = _portable_hash(funcs[name])
                self.assertEqual(
                    actual,
                    expected,
                    f"AST projection for {name} differs from reviewed start",
                )

    def test_registry_hash_matches_reviewed_start(self) -> None:
        tree = ast.parse(
            (SRC / "coordinate" / "event_presentation.py").read_text(encoding="utf-8"),
            "event_presentation.py",
        )
        registry_node = None
        for child in tree.body:
            if isinstance(child, ast.AnnAssign):
                target_id = getattr(child.target, "id", None)
                if target_id == "_EVENT_BASE_PAYLOAD_RENDERERS":
                    registry_node = child
                    break
        self.assertIsNotNone(registry_node)
        self.assertEqual(_portable_hash(registry_node), REGISTRY_ASSIGNMENT_HASH)


class KeySetRelationshipTests(unittest.TestCase):
    def test_supported_equals_renderer_keys(self) -> None:
        from coordinate import event_presentation, policy
        self.assertEqual(
            set(policy.SUPPORTED_EVENT_TYPES),
            set(event_presentation._EVENT_BASE_PAYLOAD_RENDERERS),
        )
        self.assertEqual(len(policy.SUPPORTED_EVENT_TYPES), 34)

    def test_styled_and_unstyled_partition_supported(self) -> None:
        from coordinate import discord_rendering, event_presentation, policy
        styled = set(discord_rendering._STYLING)
        unstyled = set(event_presentation.EXPLICITLY_UNSTYLED_EVENT_TYPES)
        supported = set(policy.SUPPORTED_EVENT_TYPES)
        rendered = set(event_presentation._EVENT_BASE_PAYLOAD_RENDERERS)
        self.assertTrue(styled.isdisjoint(unstyled))
        self.assertEqual(styled | unstyled, supported)
        self.assertEqual(styled | unstyled, rendered)
        self.assertEqual(len(styled), 31)
        self.assertEqual(len(unstyled), 3)

    def test_explicitly_unstyled_exact(self) -> None:
        from coordinate import event_presentation
        self.assertEqual(
            event_presentation.EXPLICITLY_UNSTYLED_EVENT_TYPES,
            EXPECTED_UNSTYLED,
        )

    def test_no_key_set_drift(self) -> None:
        from coordinate import discord_rendering, event_presentation, policy
        supported = set(policy.SUPPORTED_EVENT_TYPES)
        rendered = set(event_presentation._EVENT_BASE_PAYLOAD_RENDERERS)
        styled = set(discord_rendering._STYLING)
        unstyled = set(event_presentation.EXPLICITLY_UNSTYLED_EVENT_TYPES)
        self.assertEqual(supported, rendered)
        self.assertEqual(styled | unstyled, supported)
        self.assertEqual(len(supported), 34)
        self.assertEqual(len(rendered), 34)
        self.assertEqual(len(styled), 31)
        self.assertEqual(len(unstyled), 3)


class ErrorBehaviorTests(unittest.TestCase):
    def test_unknown_event_raises_policy_error(self) -> None:
        from coordinate import policy
        fake_event: sqlite3.Row = _fake_row(event_type="unknown.event", id="e1")
        with self.assertRaises(policy.PolicyError) as ctx:
            policy._render_event_base_payload(fake_event, "unknown.event", {})
        self.assertEqual(str(ctx.exception), "unsupported event type: unknown.event")


class ImportOrderTests(unittest.TestCase):
    def _run_isolated(self, code: str) -> None:
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=SRC.parent,
            env={
                **__import__("os").environ,
                "PYTHONPATH": str(SRC),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            self.fail(f"Import order failed:\nstdout={result.stdout}\nstderr={result.stderr}")

    def test_import_presentation_then_policy(self) -> None:
        self._run_isolated(
            "import coordinate.event_presentation; import coordinate.policy; print('ok')"
        )

    def test_import_policy_then_presentation(self) -> None:
        self._run_isolated(
            "import coordinate.policy; import coordinate.event_presentation; print('ok')"
        )

    def test_import_discord_then_presentation_then_policy(self) -> None:
        self._run_isolated(
            "import coordinate.discord_rendering; import coordinate.event_presentation; import coordinate.policy; print('ok')"
        )


def _fake_row(**kwargs: object) -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    columns = ",".join(kwargs)
    conn.execute(f"CREATE TABLE t ({columns})")
    conn.execute(
        f"INSERT INTO t ({columns}) VALUES ({','.join('?' for _ in kwargs)})",
        tuple(kwargs.values()),
    )
    row = conn.execute("SELECT * FROM t").fetchone()
    assert row is not None
    conn.close()
    return row


def _is_stdlib_module(name: str | None) -> bool:
    if name is None:
        return False
    stdlib = {"__future__", "sqlite3", "typing"}
    return name in stdlib


if __name__ == "__main__":
    unittest.main()
