"""User-facing blocker ownership and coding-host path regressions (#13/#15)."""
from __future__ import annotations

from contextlib import chdir, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import unittest

from coordinate.completion import prepare_completion_receipt
from coordinate.cli import main
from coordinate.harness import HarnessAdapter
from coordinate.db import get_workspace, list_events
from coordinate.transitions import blocker_task, mark_done_files
import test_canonical_closeout as canonical


@unittest.skipUnless(shutil.which('bash'), 'canonical harness requires bash')
class OperatorFeedbackFixTests(unittest.TestCase):
    # Reuse the real installed EXharness consumer setup, not a fake adapter.
    setUp = canonical.CanonicalCloseoutTests.setUp
    cli = canonical.CanonicalCloseoutTests.cli
    closeout = canonical.CanonicalCloseoutTests.closeout
    verdict = canonical.CanonicalCloseoutTests.verdict

    def item(self):
        return json.loads(self.checklist.read_bytes())['items'][0]

    def test_named_unblock_owner_is_enforced_by_real_harness(self):
        code, result = self.cli('blocker', 'fixture', '--task-id', 'task-1',
                                '--actor', 'worker', '--reason', 'need a decision',
                                '--unblock-owner', 'architect')
        self.assertEqual(code, 0, result)
        self.assertEqual(result['result']['event']['payload']['unblock_owner'], 'architect')
        self.assertEqual(self.item()['workflow']['unblock_owner'], 'architect')
        self.assertIsNone(self.item()['owner'])
        before = self.checklist.read_bytes()
        code, denied = self.cli('unblock', 'fixture', '--task-id', 'task-1',
                                '--actor', 'worker', '--decision', 'proceed')
        self.assertEqual(code, 1, denied)
        self.assertEqual(self.checklist.read_bytes(), before)
        code, allowed = self.cli('unblock', 'fixture', '--task-id', 'task-1',
                                 '--actor', 'architect', '--decision', 'proceed')
        self.assertEqual(code, 0, allowed)
        self.assertEqual(self.item()['workflow']['unblocked_by'], 'architect')

    def test_omitted_owner_remains_human_and_replay_returns_original_event(self):
        first = blocker_task(self.conn, 'fixture', 'task-1', actor='worker',
                             reason='need human', idempotency_hint='same-blocker')
        self.assertTrue(first.mutation.success, first.mutation.stderr)
        self.assertEqual(first.event['payload']['unblock_owner'], 'human')
        before = self.checklist.read_bytes()
        replay = blocker_task(self.conn, 'fixture', 'task-1', actor='worker',
                              reason='changed input', unblock_owner='architect',
                              idempotency_hint='same-blocker')
        self.assertFalse(replay.event_created)
        self.assertEqual(replay.event, first.event)
        self.assertEqual(self.checklist.read_bytes(), before)

    def test_failed_blocker_records_effective_owner_and_replays(self):
        self.plan.unlink()
        failed = blocker_task(self.conn, 'fixture', 'task-1', actor='worker',
                              unblock_owner='architect', idempotency_hint='failed-blocker')
        self.assertFalse(failed.mutation.success)
        self.assertEqual(failed.event['payload']['unblock_owner'], 'architect')
        replay = blocker_task(self.conn, 'fixture', 'task-1', actor='worker',
                              unblock_owner='human', idempotency_hint='failed-blocker')
        self.assertFalse(replay.event_created)
        self.assertEqual(replay.event, failed.event)
        blank_replay = blocker_task(self.conn, 'fixture', 'task-1', actor='worker',
                                    unblock_owner='', idempotency_hint='failed-blocker')
        self.assertEqual(blank_replay.event, failed.event)
        self.assertIsNone(blank_replay.mutation)

    def test_blank_unblock_owner_is_rejected_before_any_mutation_or_event(self):
        for owner in ('', ' \t\n '):
            with self.subTest(owner=repr(owner)):
                before = self.checklist.read_bytes()
                event_count = len(list_events(self.conn, 'fixture'))
                out, err = io.StringIO(), io.StringIO()
                with redirect_stdout(out), redirect_stderr(err):
                    code = main(['--db', str(self.db), 'assignment', 'blocker', 'fixture',
                                 '--task-id', 'task-1', '--actor', 'worker', '--unblock-owner', owner,
                                 '--idempotency-hint', f'blank-{owner!r}'])
                self.assertEqual(code, 1, out.getvalue())
                self.assertRegex(err.getvalue(), 'unblock_owner.*non-empty')
                self.assertEqual(self.checklist.read_bytes(), before)
                self.assertEqual(len(list_events(self.conn, 'fixture')), event_count)

    def test_existing_blocker_replay_keeps_original_event_with_blank_owner(self):
        first = blocker_task(self.conn, 'fixture', 'task-1', actor='worker',
                             unblock_owner='architect', idempotency_hint='replay-blank')
        self.assertTrue(first.mutation.success, first.mutation.stderr)
        before = self.checklist.read_bytes()
        event_count = len(list_events(self.conn, 'fixture'))
        replay = blocker_task(self.conn, 'fixture', 'task-1', actor='worker',
                              unblock_owner='', idempotency_hint='replay-blank')
        self.assertFalse(replay.event_created)
        self.assertIsNone(replay.mutation)
        self.assertEqual(replay.event, first.event)
        self.assertEqual(self.checklist.read_bytes(), before)
        self.assertEqual(len(list_events(self.conn, 'fixture')), event_count)

    def test_repair_service_relative_root_uses_workspace_not_foreign_cwd(self):
        foreign = self.root / 'foreign'
        decoy = foreign / 'docs/project-harness'
        decoy.mkdir(parents=True)
        decoy_checklist = decoy / 'harness-checklist.json'
        decoy_checklist.write_bytes(self.checklist.read_bytes())
        before = decoy_checklist.read_bytes()
        with chdir(foreign):
            result = mark_done_files(workspace_path=str(self.root),
                                     harness_root='docs/project-harness', task_id='task-1',
                                     repair_reason='local repair', verification='checked')
        self.assertTrue(result.checklist_changed)
        self.assertEqual(self.item()['status'], 'done')
        self.assertEqual(decoy_checklist.read_bytes(), before)

    def test_absolute_sidecar_root_is_supported(self):
        sidecar = self.root / 'sidecar'
        sidecar.mkdir()
        sidecar_checklist = sidecar / 'harness-checklist.json'
        sidecar_checklist.write_bytes(self.checklist.read_bytes())
        before = self.checklist.read_bytes()
        result = mark_done_files(workspace_path=str(self.root / 'other-workspace'),
                                 harness_root=str(sidecar), task_id='task-1',
                                 repair_reason='sidecar repair', verification='checked')
        self.assertTrue(result.checklist_changed)
        self.assertEqual(json.loads(sidecar_checklist.read_bytes())['items'][0]['status'], 'done')
        self.assertEqual(self.checklist.read_bytes(), before)

    def test_relative_symlink_to_runtime_is_guarded_after_workspace_resolution(self):
        (self.root / 'runtime-link').symlink_to('/opt', target_is_directory=True)
        foreign = self.root / 'foreign'
        foreign.mkdir()
        with chdir(foreign), self.assertRaisesRegex(ValueError, 'runtime deployment copy'):
            mark_done_files(workspace_path=str(self.root), harness_root='runtime-link/app',
                            task_id='task-1', repair_reason='repair')

    def test_rejected_preflight_leaves_workspace_and_foreign_decoy_unchanged(self):
        foreign = self.root / 'foreign'
        foreign.mkdir()
        before = self.checklist.read_bytes()
        transport = self.root / 'reject-preflight'
        transport.write_text('#!/bin/sh\nprintf \'%s\\n\' \'{"error":{"reason":"receipt_not_found","message":"unknown"}}\'\nexit 1\n')
        transport.chmod(0o755)
        with chdir(foreign):
            code, result = self.cli('mark-done-files', '--workspace-path', str(self.root),
                                    '--harness-root', 'docs/project-harness', '--task-id', 'task-1',
                                    '--workspace-id', 'fixture', '--receipt', 'missing',
                                    '--event-cli-path', str(transport))
        self.assertEqual(code, 1, result)
        self.assertEqual(self.checklist.read_bytes(), before)
        self.assertFalse((foreign / 'docs').exists())

    def test_receipt_cli_relative_root_and_retry_use_workspace_from_foreign_cwd(self):
        digest = self.closeout()
        self.assertTrue(self.verdict(digest).mutation.success)
        receipt = prepare_completion_receipt(self.conn, workspace_id='fixture', task_id='task-1',
                                             requester='operator',
                                             adapter=HarnessAdapter(get_workspace(self.conn, 'fixture')))
        # Execute the real local CLI as the remote ledger transport; no live service.
        transport = self.root / 'coord-local'
        import sys
        import shlex
        transport.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -m coordinate --db '
                             f'{shlex.quote(str(self.db))} "$@"\n')
        transport.chmod(0o755)
        foreign = self.root / 'foreign'
        foreign.mkdir()
        absolute_src = str(Path(__file__).resolve().parents[1] / 'src')
        from unittest.mock import patch
        with chdir(foreign), patch.dict(os.environ, {'PYTHONPATH': absolute_src}):
            command = ('mark-done-files', '--workspace-path', str(self.root),
                       '--harness-root', 'docs/project-harness', '--task-id', 'task-1',
                       '--workspace-id', 'fixture', '--actor', 'operator',
                       '--receipt', receipt.receipt_id, '--event-cli-path', str(transport),
                       '--verification', 'checked')
            code, result = self.cli(*command)
            self.assertEqual(code, 0, result)
            first = self.checklist.read_bytes()
            code, retry = self.cli(*command)
            self.assertEqual(code, 0, retry)
        self.assertEqual(self.item()['status'], 'done')
        self.assertEqual(first, self.checklist.read_bytes())
        self.assertEqual(self.item()['completion_receipt']['receipt_id'], receipt.receipt_id)

    def test_receipt_fingerprint_and_write_keep_same_root_across_claim(self):
        digest = self.closeout()
        self.assertTrue(self.verdict(digest).mutation.success)
        receipt = prepare_completion_receipt(self.conn, workspace_id='fixture', task_id='task-1',
                                             requester='operator',
                                             adapter=HarnessAdapter(get_workspace(self.conn, 'fixture')))
        link = self.root / 'harness-link'
        link.symlink_to(self.h, target_is_directory=True)
        decoy = self.root / 'decoy'
        decoy.mkdir()
        decoy_checklist = decoy / 'harness-checklist.json'
        decoy_checklist.write_bytes(self.checklist.read_bytes())
        before = decoy_checklist.read_bytes()
        import sys
        import shlex
        transport = self.root / 'coord-local-swaps-link'
        transport.write_text(f'#!/bin/sh\n{shlex.quote(sys.executable)} -m coordinate --db '
                             f'{shlex.quote(str(self.db))} "$@"\nstatus=$?\n'
                             'if [ "$2" = "mark-done-claim" ] && [ "$status" = 0 ]; then\n'
                             f'  ln -sfn {shlex.quote(str(decoy))} {shlex.quote(str(link))}\nfi\n'
                             'exit "$status"\n')
        transport.chmod(0o755)
        absolute_src = str(Path(__file__).resolve().parents[1] / 'src')
        from unittest.mock import patch
        with chdir(self.root), patch.dict(os.environ, {'PYTHONPATH': absolute_src}):
            code, result = self.cli('mark-done-files', '--workspace-path', str(self.root),
                                    '--harness-root', 'harness-link', '--task-id', 'task-1',
                                    '--workspace-id', 'fixture', '--receipt', receipt.receipt_id,
                                    '--event-cli-path', str(transport), '--verification', 'checked')
        self.assertEqual(code, 0, result)
        self.assertEqual(self.item()['status'], 'done')
        self.assertEqual(decoy_checklist.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
