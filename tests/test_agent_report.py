"""Tests for ``coordinate.agent_report`` — esp. backlog #10 (no silent drop)."""
import unittest

from coordinate.agent_report import parse_agent_report


class ParseAgentReportTests(unittest.TestCase):
    def test_full_block_parses(self):
        text = "[agent-report]\ndecision=approve\nworkspace_id=ws1\ntask_id=t1\nsummary=\"ok\""
        r = parse_agent_report(text)
        self.assertIsNotNone(r)
        self.assertEqual(r.decision, "approve")
        self.assertEqual(r.workspace_id, "ws1")
        self.assertEqual(r.task_id, "t1")

    def test_missing_ws_task_uses_fallback_and_warns(self):
        # mac-codex fix-job style: decision present, workspace_id/task_id absent.
        # Must NOT silently drop (backlog #10) — use job fallback + warn.
        text = "[agent-report]\ndecision=approve"
        with self.assertLogs("coordinate.agent_report", level="WARNING") as cm:
            r = parse_agent_report(text, fallback_workspace_id="ws-job", fallback_task_id="t-job")
        self.assertIsNotNone(r)
        self.assertEqual(r.decision, "approve")
        self.assertEqual(r.workspace_id, "ws-job")
        self.assertEqual(r.task_id, "t-job")
        self.assertTrue(any("missing workspace_id/task_id" in m for m in cm.output))

    def test_missing_ws_task_no_fallback_returns_none_and_warns(self):
        text = "[agent-report]\ndecision=reject\nreason=\"bugs\""
        with self.assertLogs("coordinate.agent_report", level="WARNING") as cm:
            r = parse_agent_report(text)
        self.assertIsNone(r)
        self.assertTrue(any("signal ignored" in m for m in cm.output))

    def test_no_block_returns_none_silently(self):
        # Plain reply with no [agent-report] → None, no warn (no signal to lose).
        r = parse_agent_report("just a normal reply, no report block here")
        self.assertIsNone(r)

    def test_partial_ws_task_uses_fallback_for_missing_one(self):
        # task_id present, workspace_id missing → fallback fills workspace_id only.
        text = "[agent-report]\ndecision=approve\ntask_id=t1"
        with self.assertLogs("coordinate.agent_report", level="WARNING"):
            r = parse_agent_report(text, fallback_workspace_id="ws-job", fallback_task_id="t-fb")
        self.assertIsNotNone(r)
        self.assertEqual(r.workspace_id, "ws-job")  # filled from fallback
        self.assertEqual(r.task_id, "t1")  # kept original


if __name__ == "__main__":
    unittest.main()
