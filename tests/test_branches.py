import json
import unittest

from coordinate.branches import (
    BranchAllocationResult,
    _sanitize_component,
    allocate_branch,
    generate_branch_name,
)
from coordinate.db import (
    connect,
    migrate,
    row_to_dict,
    upsert_task_mirror,
)


def _setup_db():
    conn = connect(":memory:")
    migrate(conn)
    conn.execute(
        "INSERT INTO workspaces (id, name, path, harness_root, base_branch, branch_namespace, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
        ("ws-1", "test-ws", "/tmp/test", "/tmp/test/docs", "main", "agents"),
    )
    conn.commit()
    return conn


def _setup_db_no_namespace():
    conn = connect(":memory:")
    migrate(conn)
    conn.execute(
        "INSERT INTO workspaces (id, name, path, harness_root, base_branch, branch_namespace, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
        ("ws-2", "test-ws-ns", "/tmp/test2", "/tmp/test2/docs", "main", None),
    )
    conn.commit()
    return conn


class BranchAllocationTests(unittest.TestCase):
    # --- generate_branch_name pure function tests ---

    def test_generate_branch_name_pure(self):
        """Unit tests for the pure generate_branch_name function."""
        from coordinate.db import Workspace

        # With namespace
        ws = Workspace(
            id="ws-1", name="test", path="/tmp", harness_root="/tmp/docs",
            base_branch="main", branch_namespace="agents",
        )
        self.assertEqual(generate_branch_name(ws, "mvp-001", "codex"), "agents/codex/mvp-001")

        # Without namespace
        ws_no_ns = Workspace(
            id="ws-2", name="test", path="/tmp", harness_root="/tmp/docs",
            base_branch="main", branch_namespace=None,
        )
        self.assertEqual(generate_branch_name(ws_no_ns, "mvp-001", "codex"), "codex/mvp-001")

        # Empty namespace string
        ws_empty_ns = Workspace(
            id="ws-3", name="test", path="/tmp", harness_root="/tmp/docs",
            base_branch="main", branch_namespace="",
        )
        self.assertEqual(generate_branch_name(ws_empty_ns, "mvp-001", "codex"), "codex/mvp-001")

        # Namespace with slashes that get stripped
        ws_slash_ns = Workspace(
            id="ws-4", name="test", path="/tmp", harness_root="/tmp/docs",
            base_branch="main", branch_namespace="/agents/",
        )
        self.assertEqual(generate_branch_name(ws_slash_ns, "mvp-001", "codex"), "agents/codex/mvp-001")

        # All-non-alphanumeric owner falls back to "agent"
        self.assertEqual(generate_branch_name(ws, "task-1", "@@@"), "agents/agent/task-1")

        # All-non-alphanumeric task_id falls back to "task"
        self.assertEqual(generate_branch_name(ws, "!!!@#$", "codex"), "agents/codex/task")

        # Special chars in owner and task_id
        self.assertEqual(
            generate_branch_name(ws, "MVP_001", "Claude/Session"),
            "agents/claude-session/mvp-001",
        )

    # --- allocate_branch integration tests ---

    def test_allocate_with_namespace_and_owner(self):
        """Correct branch name, event written, mirror updated, existing=False."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-1", "mvp-001", owner="codex")

        self.assertIsInstance(result, BranchAllocationResult)
        self.assertEqual(result.workspace_id, "ws-1")
        self.assertEqual(result.task_id, "mvp-001")
        self.assertEqual(result.branch, "agents/codex/mvp-001")
        self.assertEqual(result.owner, "codex")
        self.assertFalse(result.existing)
        self.assertTrue(result.event_created)
        self.assertIsNotNone(result.event)

        # Verify event was written
        event_row = conn.execute(
            "SELECT * FROM events WHERE event_type = 'branch.allocated' AND workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        ).fetchone()
        self.assertIsNotNone(event_row)
        payload = json.loads(event_row["payload_json"])
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["owner"], "codex")
        self.assertEqual(payload["branch"], "agents/codex/mvp-001")
        self.assertEqual(payload["base_branch"], "main")
        self.assertEqual(payload["branch_namespace"], "agents")

        # Verify mirror was updated
        mirror_row = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        ).fetchone()
        self.assertIsNotNone(mirror_row)
        self.assertEqual(mirror_row["branch"], "agents/codex/mvp-001")
        self.assertEqual(mirror_row["owner"], "codex")
        self.assertEqual(mirror_row["last_event_id"], event_row["id"])

    def test_allocate_without_namespace(self):
        """Workspace with no namespace produces owner/task pattern."""
        conn = _setup_db_no_namespace()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-2", "mvp-001", owner="codex")

        self.assertEqual(result.branch, "codex/mvp-001")

    def test_owner_fallback_explicit(self):
        """Explicit owner arg is used."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-1", "mvp-001", owner="explicit-owner")

        self.assertEqual(result.owner, "explicit-owner")
        self.assertEqual(result.branch, "agents/explicit-owner/mvp-001")

    def test_owner_fallback_from_mirror(self):
        """No owner arg, mirror has owner -> uses mirror owner."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        # Create mirror with an owner
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="mvp-001",
            phase="running",
            owner="mirror-owner",
            branch=None,
            pr=None,
            payload=None,
        )

        result = allocate_branch(conn, "ws-1", "mvp-001")

        self.assertEqual(result.owner, "mirror-owner")
        self.assertEqual(result.branch, "agents/mirror-owner/mvp-001")

    def test_owner_fallback_default(self):
        """No owner arg, no mirror -> defaults to 'agent'."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-1", "mvp-001")

        self.assertEqual(result.owner, "agent")
        self.assertEqual(result.branch, "agents/agent/mvp-001")

    def test_sanitize_owner_and_task_id(self):
        """'Claude/Session' -> 'claude-session', 'MVP_001' -> 'mvp-001'."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-1", "MVP_001", owner="Claude/Session")

        self.assertEqual(result.branch, "agents/claude-session/mvp-001")

    def test_sanitize_all_non_alphanumeric_fallback(self):
        """Owner '@@@' -> 'agent', task_id '!!!@#$' -> 'task'."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-1", "!!!@#$", owner="@@@")

        self.assertEqual(result.owner, "agent")
        self.assertEqual(result.branch, "agents/agent/task")

    def test_sanitize_consecutive_dashes_collapsed(self):
        """'a---b' -> 'a-b'."""
        self.assertEqual(_sanitize_component("a---b"), "a-b")
        self.assertEqual(_sanitize_component("--a--b--"), "a-b")
        self.assertEqual(_sanitize_component("Hello___World"), "hello-world")
        self.assertEqual(_sanitize_component("a..b"), "a..b")

    def test_branch_namespace_stripped(self):
        """Namespace '/agents/' -> 'agents/owner/task' (slashes stripped)."""
        conn = connect(":memory:")
        migrate(conn)
        conn.execute(
            "INSERT INTO workspaces (id, name, path, harness_root, base_branch, branch_namespace, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
            ("ws-slash", "test-ws", "/tmp/test", "/tmp/test/docs", "main", "/agents/"),
        )
        conn.commit()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-slash", "mvp-001", owner="codex")

        self.assertEqual(result.branch, "agents/codex/mvp-001")

    def test_idempotent_same_branch(self):
        """Allocate twice -> second returns existing=True, event populated, mirror intact."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        first = allocate_branch(conn, "ws-1", "mvp-001", owner="codex")
        self.assertFalse(first.existing)
        self.assertTrue(first.event_created)

        second = allocate_branch(conn, "ws-1", "mvp-001", owner="codex")
        self.assertTrue(second.existing)
        self.assertFalse(second.event_created)
        self.assertIsNotNone(second.event)
        self.assertEqual(second.branch, "agents/codex/mvp-001")
        self.assertEqual(second.owner, "codex")

        # Only one event with this idempotency key
        events = conn.execute(
            "SELECT * FROM events WHERE event_type = 'branch.allocated' AND workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        ).fetchall()
        self.assertEqual(len(events), 1)

        # Mirror is intact
        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        ).fetchone()
        self.assertIsNotNone(mirror)
        self.assertEqual(mirror["branch"], "agents/codex/mvp-001")

    def test_idempotent_recovers_mirror(self):
        """Allocate, then clear mirror's branch, allocate again -> mirror gets fixed."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        first = allocate_branch(conn, "ws-1", "mvp-001", owner="codex")
        self.assertFalse(first.existing)

        # Simulate crash: clear the mirror's branch (event was written but mirror wasn't updated)
        conn.execute(
            "UPDATE tasks SET branch = NULL WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        )
        conn.commit()

        # Re-allocate: since mirror branch is now NULL, it's a fresh allocation path,
        # not the idempotent path. Mirror gets recovered.
        second = allocate_branch(conn, "ws-1", "mvp-001", owner="codex")
        self.assertFalse(second.existing)  # not idempotent because mirror branch was cleared
        # event_created may be False because idempotency_key matches the original event
        self.assertIsNotNone(second.event)  # event is still populated

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        ).fetchone()
        self.assertEqual(mirror["branch"], "agents/codex/mvp-001")

    def test_existing_different_branch_raises(self):
        """Mirror has a different branch -> ValueError."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        # First allocation
        allocate_branch(conn, "ws-1", "mvp-001", owner="codex")

        # Change the mirror branch to something different manually
        conn.execute(
            "UPDATE tasks SET branch = 'agents/old-owner/mvp-001' WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        )
        conn.commit()

        # Allocating with a different desired branch (via different owner) raises
        with self.assertRaises(ValueError) as ctx:
            allocate_branch(conn, "ws-1", "mvp-001", owner="new-owner")
        self.assertIn("already has branch", str(ctx.exception))
        self.assertIn("cannot reallocate", str(ctx.exception))

    def test_conflict_another_task_same_branch(self):
        """Allocate for task-1, then task-2 would get same branch -> ValueError."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        # Insert mirror for task-a claiming a branch
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-a",
            phase=None,
            owner="codex",
            branch="agents/codex/mvp-002",
            pr=None,
            payload=None,
        )

        # task-b (mvp-002) would also want agents/codex/mvp-002 but it's taken by task-a
        with self.assertRaises(ValueError) as ctx:
            allocate_branch(conn, "ws-1", "mvp-002", owner="codex")
        self.assertIn("already allocated to task", str(ctx.exception))
        self.assertIn("task-a", str(ctx.exception))

    def test_unknown_workspace_raises(self):
        """ValueError for non-existent workspace."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        with self.assertRaises(ValueError) as ctx:
            allocate_branch(conn, "ws-nonexistent", "mvp-001")
        self.assertIn("unknown workspace", str(ctx.exception))

    def test_task_mirror_not_yet_exists(self):
        """First allocation creates mirror with branch, phase/pr/payload default."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        result = allocate_branch(conn, "ws-1", "mvp-new", owner="codex")
        self.assertFalse(result.existing)

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-new"),
        ).fetchone()
        self.assertIsNotNone(mirror)
        self.assertEqual(mirror["branch"], "agents/codex/mvp-new")
        self.assertEqual(mirror["owner"], "codex")
        self.assertIsNone(mirror["phase"])
        self.assertIsNone(mirror["pr"])
        self.assertEqual(json.loads(mirror["payload_json"]), {})
        self.assertEqual(mirror["last_event_id"], result.event["id"])

    def test_preserves_existing_mirror_fields(self):
        """When re-allocating with same branch, existing phase/pr/payload preserved."""
        conn = _setup_db()
        self.addCleanup(conn.close)

        # Create mirror with phase and payload
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="mvp-001",
            phase="running",
            owner="codex",
            branch="agents/codex/mvp-001",
            pr="https://github.example/pr/42",
            payload={"key": "value"},
        )

        # Idempotent allocation preserves phase, pr, payload
        result = allocate_branch(conn, "ws-1", "mvp-001", owner="codex")
        self.assertTrue(result.existing)

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "mvp-001"),
        ).fetchone()
        self.assertEqual(mirror["phase"], "running")
        self.assertEqual(mirror["pr"], "https://github.example/pr/42")
        self.assertEqual(json.loads(mirror["payload_json"]), {"key": "value"})


if __name__ == "__main__":
    unittest.main()
