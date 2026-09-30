"""Real EXharness scripts + Coordinate: no fake subprocess result on this path."""
from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

from coordinate.cli import main
from coordinate.db import initialize, upsert_workspace, get_workspace, list_events, row_to_dict
from coordinate.harness import HarnessAdapter
from coordinate.assignments import request_assignment
from coordinate.transitions import accept_task, closeout_task, review_result_task, mark_done_files
from coordinate.completion import (
    prepare_completion_receipt, compute_mark_done_fingerprints, claim_completion_receipt,
    apply_completion_receipt, consume_completion_receipt,
)

FIXTURE = Path(__file__).parent / 'fixtures/canonical_harness'


@unittest.skipUnless(shutil.which('bash'), 'canonical harness requires bash')
class CanonicalCloseoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.h = self.root / 'docs/project-harness'
        self.s = self.root / 'scripts/harness'
        self.s.mkdir(parents=True)
        (self.h / 'current').mkdir(parents=True)
        (self.h / 'tasks/task-1').mkdir(parents=True)
        manifest = json.loads((FIXTURE / 'source-manifest.json').read_text())
        for name, digest in manifest['sha256'].items():
            raw = (FIXTURE / name).read_bytes()
            self.assertEqual(hashlib.sha256(raw).hexdigest(), digest)
            if source := os.environ.get("EXHARNESS_SOURCE"):
                self.assertEqual(raw, (Path(source) / name).read_bytes())
            text = (raw.decode().replace('{{PROJECT_ROOT_DEPTH}}', '2')
                    .replace('{{HARNESS_ROOT}}', 'docs/project-harness')
                    .replace('{{SCRIPTS_DIR}}', 'scripts/harness'))
            (self.s / name).write_text(text)
        (self.s / 'harnessctl').chmod(0o755)
        self.checklist = self.h / 'harness-checklist.json'
        self.plan = self.h / 'tasks/task-1/plan.md'
        self.packet = self.h / 'current/closeout-packet.md'
        self.plan.write_text('# Plan\n\nAcceptance: fixture result.\n')
        (self.h / 'progress.md').write_text('# Progress\n')
        (self.h / 'harness-config.json').write_text('{}')
        item = dict(id='task-1', title='Fixture', status='todo', priority='p1', owner=None,
                    selected_in_session=None, updated_at='2026-09-29', dependencies=[],
                    blocked_by=[], blocked_reason=None, acceptance='fixture acceptance',
                    verification='fixture checks passed', handoff='fixture only',
                    plan_path='docs/project-harness/tasks/task-1/plan.md', workflow={'mode': 'ordinary'})
        self.checklist.write_text(json.dumps(dict(project='fixture', harness_root='docs/project-harness',
                                                updated_at='2026-09-29', items=[item])))
        self.db = self.root / 'fixture.sqlite3'
        self.conn = initialize(self.db)
        self.addCleanup(self.conn.close)
        upsert_workspace(self.conn, workspace_id='fixture', name='fixture', path=str(self.root),
                         harness_root=str(self.h), harnessctl_path=str(self.s / 'harnessctl'))
        for result in (request_assignment(self.conn, 'fixture', 'task-1', 'worker', 'fixture-session'),
                       accept_task(self.conn, 'fixture', 'task-1', 'worker', 'fixture-session')):
            self.assertTrue(result.mutation.success, result.mutation.stderr)

    def cli(self, *args):
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            code = main(['--db', str(self.db), 'assignment', *args])
        return code, json.loads(out.getvalue())

    def closeout(self, evidence='tests: 3 passed\n中文 evidence', hint=None):
        result = closeout_task(self.conn, 'fixture', 'task-1', 'reviewer',
                              self_test_evidence=evidence, idempotency_hint=hint)
        self.assertTrue(result.mutation.success, result.mutation.stderr)
        self.assertIn(evidence, self.packet.read_text())
        return hashlib.sha256(self.packet.read_bytes()).hexdigest()

    def verdict(self, digest, **kw):
        return review_result_task(self.conn, 'fixture', 'task-1', 'reviewer', 'approved',
                                  reviewed_packet_sha256=digest, **kw)

    def assert_denied_without_review(self, result, before):
        self.assertFalse(result.mutation.success)
        self.assertEqual(result.event['event_type'], 'harness.mutation_failed')
        self.assertEqual(self.checklist.read_bytes(), before)
        self.assertFalse(any(row_to_dict(e)['event_type'] == 'review.completed'
                             for e in list_events(self.conn, 'fixture')))

    def test_cli_closeout_review_and_receipt_completion(self):
        evidence = 'pytest 3 passed; build succeeded\nlatest run: fixture'
        code, closeout = self.cli('closeout', 'fixture', '--task-id', 'task-1', '--reviewer', 'reviewer',
                                 '--self-test-evidence', evidence)
        self.assertEqual(code, 0, closeout)
        self.assertIn(evidence, self.packet.read_text())
        digest = hashlib.sha256(self.packet.read_bytes()).hexdigest()
        code, reviewed = self.cli('review-result', 'fixture', '--task-id', 'task-1', '--reviewer', 'reviewer',
                                 '--decision', 'approved', '--reviewed-packet-sha256', digest)
        self.assertEqual(code, 0, reviewed)
        self.assertEqual(reviewed['result']['event']['payload']['reviewed_packet_sha256'], digest)
        item = json.loads(self.checklist.read_bytes())['items'][0]
        self.assertEqual(item['review']['reviewed_packet_sha256'], digest)
        adapter = HarnessAdapter(get_workspace(self.conn, 'fixture'))
        receipt = prepare_completion_receipt(self.conn, workspace_id='fixture', task_id='task-1',
                                             requester='operator', adapter=adapter)
        self.assertEqual(receipt.review_evidence['reviewed_packet_sha256'], digest)
        fps = compute_mark_done_fingerprints(harness_root=str(self.h), task_id="task-1")
        claim = claim_completion_receipt(self.conn, receipt_id=receipt.receipt_id, workspace_id='fixture',
                                         task_id='task-1', actor='operator', before_fingerprint=fps.before_fingerprint,
                                         expected_after_fingerprint=fps.after_fingerprint)
        mark_done_files(workspace_path=str(self.root), harness_root=str(self.h), task_id='task-1',
                        actor='operator', verification='fixture completion passed', receipt=claim.as_evidence())
        apply_completion_receipt(self.conn, receipt_id=receipt.receipt_id, workspace_id='fixture',
                                 task_id='task-1', actor='operator', after_fingerprint=fps.after_fingerprint)
        consume_completion_receipt(self.conn, receipt_id=receipt.receipt_id, actor='operator',
                                   deployed_adapter=adapter, verification='fixture completion passed')
        self.assertEqual(json.loads(self.checklist.read_bytes())['items'][0]['workflow']['status'], 'closed')
        types = [row_to_dict(e)['event_type'] for e in list_events(self.conn, 'fixture')]
        self.assertEqual(types.count('review.completed'), 1)
        self.assertEqual(types.count('task.done'), 1)
        self.assertIn('completion.consumed', types)

    def test_wrong_hash_then_corrected_hash_same_task(self):
        digest = self.closeout()
        before = self.checklist.read_bytes()
        failed = self.verdict('0' * 64)
        self.assert_denied_without_review(failed, before)
        replay = self.verdict('0' * 64)
        self.assertFalse(replay.event_created)
        self.assertIsNone(replay.mutation)
        success = self.verdict(digest)
        self.assertTrue(success.mutation.success, success.mutation.stderr)
        replay = self.verdict(digest)
        self.assertFalse(replay.event_created)
        self.assertEqual(replay.event['id'], success.event['id'])

    def test_packet_tampering_rejects_review(self):
        digest = self.closeout()
        self.packet.write_text(self.packet.read_text() + '\nTampered\n')
        before = self.checklist.read_bytes()
        self.assert_denied_without_review(self.verdict(digest), before)

    def test_plan_drift_rejects_review_then_regenerated_packet_succeeds(self):
        digest = self.closeout()
        self.plan.write_text('# Changed canonical plan\n')
        before = self.checklist.read_bytes()
        self.assert_denied_without_review(self.verdict(digest), before)
        new_digest = self.closeout(hint='closeout-round-2')
        self.assertNotEqual(digest, new_digest)
        success = self.verdict(new_digest)
        self.assertTrue(success.mutation.success, success.mutation.stderr)

    def test_old_packet_after_regeneration_rejected(self):
        old_digest = self.closeout()
        new_digest = self.closeout(evidence='new self-test run', hint='new-round')
        self.assertNotEqual(old_digest, new_digest)
        before = self.checklist.read_bytes()
        self.assert_denied_without_review(self.verdict(old_digest), before)
        self.assertTrue(self.verdict(new_digest).mutation.success)

    def test_explicit_hint_cannot_be_reused_for_different_review_input(self):
        digest = self.closeout()
        self.verdict('0' * 64, idempotency_hint='same-request')
        with self.assertRaisesRegex(ValueError, 'different or legacy'):
            self.verdict(digest, idempotency_hint='same-request')
        self.assertTrue(self.verdict(digest, idempotency_hint='corrected-request').mutation.success)

    def test_closeout_idempotence_and_changed_evidence(self):
        digest = self.closeout(evidence='first', hint='first-request')
        before = self.checklist.read_bytes()
        replay = closeout_task(self.conn, 'fixture', 'task-1', 'reviewer',
                              self_test_evidence='first', idempotency_hint='first-request')
        self.assertFalse(replay.event_created)
        self.assertEqual(self.checklist.read_bytes(), before)
        self.assertEqual(hashlib.sha256(self.packet.read_bytes()).hexdigest(), digest)
        with self.assertRaisesRegex(ValueError, 'different or legacy'):
            closeout_task(self.conn, 'fixture', 'task-1', 'reviewer',
                          self_test_evidence='second', idempotency_hint='first-request')
        new_digest = self.closeout(evidence='second')
        self.assertNotEqual(digest, new_digest)

    def test_legacy_wrapper_refused_before_mutation_and_recoverable_after_upgrade(self):
        wrapper = self.s / 'harnessctl'
        original = wrapper.read_bytes()
        # Legacy wrappers may return success and silently discard extra args.
        wrapper.write_text('#!/bin/bash\necho legacy\nexit 0\n')
        before = self.checklist.read_bytes()
        failed = closeout_task(self.conn, 'fixture', 'task-1', 'reviewer', self_test_evidence='evidence')
        self.assert_denied_without_review(failed, before)
        self.assertIn('workflow-contract v1', failed.mutation.stderr)
        self.assertFalse(self.packet.exists())
        wrapper.write_bytes(original)
        self.closeout(evidence='evidence', hint='after-upgrade')

    def test_cli_requires_reviewer_hash_before_opening_database(self):
        missing_db = self.root / "must-not-be-created.sqlite3"
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            main(["--db", str(missing_db), "assignment", "review-result", "fixture",
                  "--task-id", "task-1", "--reviewer", "reviewer", "--decision", "approved"])
        self.assertEqual(error.exception.code, 2)
        self.assertFalse(missing_db.exists())

    def test_malformed_review_hash_refused_before_event_or_file_mutation(self):
        self.closeout()
        before = self.checklist.read_bytes()
        count = len(list_events(self.conn, 'fixture'))
        for digest in ('', 'fake', 'A' * 64, None):
            with self.assertRaises(ValueError):
                self.verdict(digest)
        self.assertEqual(self.checklist.read_bytes(), before)
        self.assertEqual(len(list_events(self.conn, 'fixture')), count)
