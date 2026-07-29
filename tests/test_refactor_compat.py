import unittest
from unittest import mock

from coordinate import db, prs, schema
from coordinate.db import connect, migrate, upsert_workspace


class RefactorCompatibilityTests(unittest.TestCase):
    def test_db_keeps_schema_helper_exports(self):
        self.assertIs(db._table_columns, schema._table_columns)
        self.assertIs(db._add_column_if_missing, schema._add_column_if_missing)
        self.assertIs(db.migrate, schema.migrate)

    def test_prs_keeps_phase84_import_surface(self):
        expected = (
            "mirror_branch_update",
            "_idempotency_key",
            "_read_task_mirror",
            "_mirror_publish_identity",
            "_mirror_conflict_check",
            "_emit_publish_event",
            "_record_event_payload",
            "_record_upsert_mirror",
            "_ACTION_TO_EVENT_TYPE",
            "_RECORDABLE_ACTIONS",
        )
        for name in expected:
            with self.subTest(name=name):
                self.assertTrue(hasattr(prs, name))

    def test_record_preflight_uses_mirror_conflict_check(self):
        conn = connect(":memory:")
        migrate(conn)
        upsert_workspace(
            conn,
            workspace_id="ws",
            name="ws",
            path="/tmp/ws",
            harness_root="/tmp/ws/docs",
        )
        import coordinate.pr_recording as recording_module
        with mock.patch.object(
            recording_module, "check_mirror_conflict", return_value="injected conflict"
        ) as patched:
            result = prs.record_publish_preflight(
                conn,
                workspace_id="ws",
                repo="owner/repo",
                branch="agents/test/task",
                reported_commit="a" * 40,
                task_id="task",
            )
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "mirror_conflict")
        patched.assert_called_once()


if __name__ == "__main__":
    unittest.main()
