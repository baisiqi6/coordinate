"""Focused tests for the channel binding authority DB API.

Covers the plan's Coordinate-focused matrix: canonical key validation,
bind/no-op/conflict/release/rebind, idempotency exact replay and
cross-operation/cross-payload conflict, event-first single-transaction
semantics, and release replay never deleting a later rebind.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

import coordinate.db as db_module
from coordinate.db import (
    append_event,
    bind_channel_workspace,
    connect,
    get_event,
    initialize,
    list_channel_bindings,
    list_events,
    release_channel_workspace,
    resolve_channel_workspace,
    upsert_workspace,
)


def _make_conn() -> sqlite3.Connection:
    conn = initialize(":memory:")
    for ws in ("ws-a", "ws-b"):
        upsert_workspace(
            conn,
            workspace_id=ws,
            name=ws,
            path=f"/tmp/{ws}",
            harness_root=f"/tmp/{ws}/harness",
        )
    return conn


def _bind(conn, **overrides):
    kwargs = {
        "platform": "discord",
        "channel_id": "123",
        "workspace_id": "ws-a",
        "actor": "op",
        "reason": "r",
        "idempotency_key": "k1",
    }
    kwargs.update(overrides)
    return bind_channel_workspace(conn, **kwargs)


def _release(conn, **overrides):
    kwargs = {
        "platform": "discord",
        "channel_id": "123",
        "expected_workspace_id": "ws-a",
        "actor": "op",
        "reason": "r",
        "idempotency_key": "k2",
    }
    kwargs.update(overrides)
    return release_channel_workspace(conn, **kwargs)


class CanonicalKeyTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()

    def test_platform_is_normalized(self):
        result = _bind(self.conn, platform="  DISCORD ")
        self.assertEqual(result["platform"], "discord")
        self.assertEqual(result["status"], "bound")

    def test_invalid_platform_fails_loud(self):
        for bad in ("slack", "", "telegram", "discord2"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    _bind(self.conn, platform=bad, idempotency_key=f"k-{bad}")
                with self.assertRaises(ValueError):
                    resolve_channel_workspace(self.conn, platform=bad, channel_id="1")
                with self.assertRaises(ValueError):
                    list_channel_bindings(self.conn, platform=bad)

    def test_kook_platform_accepted(self):
        result = _bind(self.conn, platform="kook", channel_id="opaque-kook-id")
        self.assertEqual(result["status"], "bound")
        binding = resolve_channel_workspace(self.conn, platform="kook", channel_id="opaque-kook-id")
        self.assertEqual(binding.workspace_id, "ws-a")

    def test_opaque_channel_id_preserved(self):
        # No narrower per-platform regex is invented; opaque ids are stored as-is.
        opaque = "CH_abc-DEF.0123:~"
        _bind(self.conn, channel_id=opaque)
        binding = resolve_channel_workspace(self.conn, platform="discord", channel_id=opaque)
        self.assertEqual(binding.channel_id, opaque)

    def test_empty_channel_id_rejected(self):
        with self.assertRaises(ValueError):
            _bind(self.conn, channel_id="")

    def test_whitespace_channel_id_rejected(self):
        for bad in (" 123", "123 ", " 123 ", "\t123"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    _bind(self.conn, channel_id=bad, idempotency_key=f"k-{bad!r}")

    def test_control_character_channel_id_rejected(self):
        for bad in ("12\n3", "12\x003", "12\x7f3"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    _bind(self.conn, channel_id=bad, idempotency_key=f"k-{bad!r}")

    def test_overlong_channel_id_rejected(self):
        with self.assertRaises(ValueError):
            _bind(self.conn, channel_id="x" * 129)
        # Boundary: exactly 128 code points is accepted.
        ok = "x" * 128
        result = _bind(self.conn, channel_id=ok)
        self.assertEqual(result["status"], "bound")

    def test_resolve_invalid_key_is_not_unbound(self):
        # Invalid input must raise, never be downgraded to a normal unbound result.
        with self.assertRaises(ValueError):
            resolve_channel_workspace(self.conn, platform="discord", channel_id=" 1")


class BindTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()

    def test_bind_creates_active_row_and_event(self):
        result = _bind(self.conn)
        self.assertEqual(result["status"], "bound")
        self.assertEqual(result["target"], "discord:123")
        binding = resolve_channel_workspace(self.conn, platform="discord", channel_id="123")
        self.assertEqual(binding.workspace_id, "ws-a")

        event = get_event(self.conn, result["event_id"])
        self.assertEqual(event["event_type"], "channel.binding.bound")
        self.assertEqual(event["workspace_id"], "ws-a")
        self.assertEqual(event["target"], "discord:123")
        self.assertEqual(event["idempotency_key"], "k1")

    def test_bind_requires_actor_reason_idempotency_key(self):
        for field in ("actor", "reason", "idempotency_key"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    _bind(self.conn, **{field: ""})
                with self.assertRaises(ValueError):
                    _bind(self.conn, **{field: "   "})

    def test_bind_unknown_workspace_fails(self):
        with self.assertRaises(ValueError):
            _bind(self.conn, workspace_id="nope")

    def test_same_workspace_rebind_is_noop(self):
        first = _bind(self.conn)
        events_before = len(list(list_events(self.conn)))
        second = _bind(self.conn, idempotency_key="k-other")
        self.assertEqual(second["status"], "already_bound")
        self.assertIsNone(second["event_id"])
        self.assertEqual(len(list(list_events(self.conn))), events_before)
        self.assertNotEqual(first["event_id"], second["event_id"])

    def test_bind_other_workspace_conflicts(self):
        _bind(self.conn)
        with self.assertRaises(ValueError):
            _bind(self.conn, workspace_id="ws-b", idempotency_key="k3")

    def test_idempotency_exact_replay_returns_receipt(self):
        first = _bind(self.conn)
        events_before = len(list(list_events(self.conn)))
        replay = _bind(self.conn)  # identical args + same key
        self.assertEqual(replay["status"], "replayed")
        self.assertEqual(replay["event_id"], first["event_id"])
        # No new event, no second mutation.
        self.assertEqual(len(list(list_events(self.conn))), events_before)
        bindings = list_channel_bindings(self.conn)
        self.assertEqual(len(bindings), 1)

    def test_idempotency_cross_payload_conflicts(self):
        _bind(self.conn)
        # Same key, different reason -> conflict.
        with self.assertRaises(ValueError):
            _bind(self.conn, reason="different")
        # Same key, different workspace -> conflict.
        with self.assertRaises(ValueError):
            _bind(self.conn, workspace_id="ws-b")

    def test_idempotency_cross_operation_conflicts(self):
        _bind(self.conn, idempotency_key="shared")
        # Reusing the bind key for a release must fail closed.
        with self.assertRaises(ValueError):
            _release(self.conn, idempotency_key="shared")


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()

    def test_release_requires_expected_workspace_and_fields(self):
        _bind(self.conn)
        for field in ("expected_workspace_id", "actor", "reason", "idempotency_key"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    _release(self.conn, **{field: ""})

    def test_release_unknown_expected_workspace_fails(self):
        _bind(self.conn)
        with self.assertRaises(ValueError):
            _release(self.conn, expected_workspace_id="nope")

    def test_release_removes_active_row_and_writes_event(self):
        _bind(self.conn)
        result = _release(self.conn)
        self.assertEqual(result["status"], "released")
        self.assertIsNone(
            resolve_channel_workspace(self.conn, platform="discord", channel_id="123")
        )
        event = get_event(self.conn, result["event_id"])
        self.assertEqual(event["event_type"], "channel.binding.released")
        self.assertEqual(event["workspace_id"], "ws-a")
        self.assertEqual(event["target"], "discord:123")

    def test_release_expected_workspace_mismatch_conflicts(self):
        _bind(self.conn)  # bound to ws-a
        with self.assertRaises(ValueError):
            _release(self.conn, expected_workspace_id="ws-b")

    def test_release_unbound_is_noop(self):
        events_before = len(list(list_events(self.conn)))
        result = _release(self.conn)
        self.assertEqual(result["status"], "already_unbound")
        self.assertIsNone(result["event_id"])
        self.assertEqual(len(list(list_events(self.conn))), events_before)

    def test_release_exact_replay_returns_receipt(self):
        _bind(self.conn)
        first = _release(self.conn)
        events_before = len(list(list_events(self.conn)))
        replay = _release(self.conn)
        self.assertEqual(replay["status"], "replayed")
        self.assertEqual(replay["event_id"], first["event_id"])
        self.assertEqual(len(list(list_events(self.conn))), events_before)

    def test_release_replay_does_not_delete_later_rebind(self):
        _bind(self.conn, idempotency_key="k-bind")  # bind ws-a
        _release(self.conn, idempotency_key="k-rel")  # release
        _bind(self.conn, workspace_id="ws-b", idempotency_key="k-rebind")  # rebind ws-b
        # Replay the historical release; must only return the receipt.
        replay = _release(self.conn, idempotency_key="k-rel")
        self.assertEqual(replay["status"], "replayed")
        binding = resolve_channel_workspace(self.conn, platform="discord", channel_id="123")
        self.assertIsNotNone(binding)
        self.assertEqual(binding.workspace_id, "ws-b")


class EventFirstAtomicityTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()

    def test_failed_row_insert_rolls_back_event(self):
        # Force the active-row INSERT to fail by pre-creating a conflicting row
        # outside the API, then attempting a bind whose idempotency key is fresh
        # but whose channel is already taken by a *different* workspace through a
        # path that bypasses the pre-check. We simulate the half-state by
        # monkeypatching the INSERT via a dropped column.
        conn = self.conn
        # Drop the bound_at column so the INSERT fails after the event append.
        conn.execute("SAVEPOINT outer")
        conn.execute(
            "CREATE TABLE channel_bindings_backup AS SELECT * FROM channel_bindings"
        )
        conn.execute("DROP TABLE channel_bindings")
        conn.execute(
            "CREATE TABLE channel_bindings (platform TEXT, channel_id TEXT, workspace_id TEXT, PRIMARY KEY(platform, channel_id))"
        )
        conn.execute("RELEASE outer")
        conn.commit()

        events_before = len(list(list_events(conn)))
        with self.assertRaises(sqlite3.Error):
            bind_channel_workspace(
                conn,
                platform="discord",
                channel_id="999",
                workspace_id="ws-a",
                actor="op",
                reason="r",
                idempotency_key="k-atomic",
            )
        # The event appended before the failed INSERT must have been rolled back.
        self.assertEqual(len(list(list_events(conn))), events_before)
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM events WHERE idempotency_key = 'k-atomic'"
            ).fetchone()[0],
            0,
        )


class ListTests(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()

    def test_list_all_and_filters(self):
        _bind(self.conn, channel_id="c1", workspace_id="ws-a", idempotency_key="k1")
        _bind(self.conn, platform="kook", channel_id="c2", workspace_id="ws-b", idempotency_key="k2")
        _bind(self.conn, channel_id="c3", workspace_id="ws-b", idempotency_key="k3")

        all_bindings = list_channel_bindings(self.conn)
        self.assertEqual(len(all_bindings), 3)

        discord_only = list_channel_bindings(self.conn, platform="discord")
        self.assertEqual({b.channel_id for b in discord_only}, {"c1", "c3"})

        ws_b = list_channel_bindings(self.conn, workspace_id="ws-b")
        self.assertEqual({b.channel_id for b in ws_b}, {"c2", "c3"})

        both = list_channel_bindings(self.conn, platform="kook", workspace_id="ws-b")
        self.assertEqual({b.channel_id for b in both}, {"c2"})

    def test_list_ordering_is_deterministic(self):
        _bind(self.conn, channel_id="c2", idempotency_key="k2")
        _bind(self.conn, channel_id="c1", idempotency_key="k1")
        ids = [b.channel_id for b in list_channel_bindings(self.conn)]
        self.assertEqual(ids, sorted(ids))

    def test_list_invalid_platform_filter_fails_loud(self):
        with self.assertRaises(ValueError):
            list_channel_bindings(self.conn, platform="slack")

    def test_list_empty_workspace_filter_fails_loud(self):
        with self.assertRaises(ValueError):
            list_channel_bindings(self.conn, workspace_id="   ")


class ConcurrentTwoConnectionTests(unittest.TestCase):
    """Deterministic cross-connection TOCTOU regression tests (correction round 1).

    Uses a real file-backed SQLite DB and two connections. A second connection is
    driven between the primary connection's precheck and its mutation via a
    controlled hook on ``append_event``; each test proves a fail-closed invariant:

    - concurrent release/rebind: an old release never deletes a new workspace row;
    - concurrent exact replay: never re-mutates;
    - concurrent cross-payload/cross-operation key reuse: fails closed;
    - failure paths leave no half-state event for this round.
    """

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "coordinator.sqlite3")
        # conn1 is the connection under test; conn2 is the concurrent actor.
        self.conn1 = initialize(self.db_path)
        for ws in ("ws-a", "ws-b"):
            upsert_workspace(
                self.conn1,
                workspace_id=ws,
                name=ws,
                path=f"/tmp/{ws}",
                harness_root=f"/tmp/{ws}/harness",
            )
        self.conn2 = connect(self.db_path)

    def tearDown(self):
        self.conn1.close()
        self.conn2.close()
        self._tmpdir.cleanup()

    def _resolve(self, conn, channel_id="123"):
        return resolve_channel_workspace(conn, platform="discord", channel_id=channel_id)

    def _event_count(self, conn, key):
        return conn.execute(
            "SELECT COUNT(*) FROM events WHERE idempotency_key = ?", (key,)
        ).fetchone()[0]

    # -- release: compare-and-delete ----------------------------------------

    def test_concurrent_rebind_blocks_stale_release(self):
        # conn1 binds ch->ws-a. A concurrent conn2 releases ws-a and rebinds ch->ws-b.
        # conn1's stale release(expected ws-a) must fail closed and must NOT delete
        # the ws-b row, and must not leave its own release event behind.
        bind_channel_workspace(
            self.conn1, platform="discord", channel_id="123", workspace_id="ws-a",
            actor="op", reason="r", idempotency_key="bind-1",
        )

        real_append = db_module.append_event
        state = {"fired": False}

        def racing_append(*args, **kwargs):
            # Just before conn1 appends its release event, conn2 wins the race:
            # release ws-a and rebind to ws-b on its own connection.
            if not state["fired"]:
                state["fired"] = True
                release_channel_workspace(
                    self.conn2, platform="discord", channel_id="123",
                    expected_workspace_id="ws-a", actor="op2", reason="r2",
                    idempotency_key="rel-2",
                )
                bind_channel_workspace(
                    self.conn2, platform="discord", channel_id="123", workspace_id="ws-b",
                    actor="op2", reason="r2", idempotency_key="bind-2",
                )
            return real_append(*args, **kwargs)

        db_module.append_event = racing_append
        try:
            with self.assertRaises(ValueError):
                release_channel_workspace(
                    self.conn1, platform="discord", channel_id="123",
                    expected_workspace_id="ws-a", actor="op1", reason="r1",
                    idempotency_key="rel-1",
                )
        finally:
            db_module.append_event = real_append

        # The ws-b rebind survived; conn1's stale release left no event.
        self.assertEqual(self._resolve(self.conn2).workspace_id, "ws-b")
        self.assertEqual(self._event_count(self.conn2, "rel-1"), 0)

    def test_concurrent_release_of_same_row_blocks_second_release(self):
        # conn1 and conn2 both try to release ch->ws-a. conn2 commits first; conn1's
        # compare-and-delete then matches zero rows and fails closed.
        bind_channel_workspace(
            self.conn1, platform="discord", channel_id="123", workspace_id="ws-a",
            actor="op", reason="r", idempotency_key="bind-1",
        )

        real_append = db_module.append_event
        state = {"fired": False}

        def racing_append(*args, **kwargs):
            if not state["fired"]:
                state["fired"] = True
                release_channel_workspace(
                    self.conn2, platform="discord", channel_id="123",
                    expected_workspace_id="ws-a", actor="op2", reason="r2",
                    idempotency_key="rel-2",
                )
            return real_append(*args, **kwargs)

        db_module.append_event = racing_append
        try:
            with self.assertRaises(ValueError):
                release_channel_workspace(
                    self.conn1, platform="discord", channel_id="123",
                    expected_workspace_id="ws-a", actor="op1", reason="r1",
                    idempotency_key="rel-1",
                )
        finally:
            db_module.append_event = real_append

        # Row is unbound (conn2 won); conn1 left no event.
        self.assertIsNone(self._resolve(self.conn2))
        self.assertEqual(self._event_count(self.conn2, "rel-1"), 0)

    # -- append_event(created=False) replay / conflict -----------------------

    def test_concurrent_release_exact_replay_does_not_delete_rebind(self):
        # Historical release used key K. After a later rebind to ws-b, a concurrent
        # exact replay with the same key K lands via append_event(created=False):
        # it must return the receipt only and must NOT delete the ws-b row.
        bind_channel_workspace(
            self.conn1, platform="discord", channel_id="123", workspace_id="ws-a",
            actor="op", reason="r", idempotency_key="bind-1",
        )
        first = release_channel_workspace(
            self.conn1, platform="discord", channel_id="123",
            expected_workspace_id="ws-a", actor="op", reason="r",
            idempotency_key="K",
        )
        self.assertEqual(first["status"], "released")
        bind_channel_workspace(
            self.conn1, platform="discord", channel_id="123", workspace_id="ws-b",
            actor="op", reason="r", idempotency_key="bind-2",
        )

        # Drive the created=False branch deterministically. Hide the historical
        # event from the idempotency precheck, and present the stale active-row
        # snapshot (still bound to expected ws-a) so the call reaches append_event.
        # append_event then finds key K already taken (a concurrent replay won the
        # race) and the code must re-validate and return the receipt, not DELETE.
        real_check = db_module._check_channel_binding_idempotency
        real_get_row = db_module._get_channel_binding_row

        def stale_row(conn, *, platform, channel_id):
            if platform == "discord" and channel_id == "123":
                return {"platform": "discord", "channel_id": "123",
                        "workspace_id": "ws-a", "bound_at": "2026-01-01T00:00:00Z"}
            return real_get_row(conn, platform=platform, channel_id=channel_id)

        db_module._check_channel_binding_idempotency = lambda *a, **k: None
        db_module._get_channel_binding_row = stale_row
        try:
            replay = release_channel_workspace(
                self.conn1, platform="discord", channel_id="123",
                expected_workspace_id="ws-a", actor="op", reason="r",
                idempotency_key="K",
            )
        finally:
            db_module._check_channel_binding_idempotency = real_check
            db_module._get_channel_binding_row = real_get_row

        self.assertEqual(replay["status"], "replayed")
        self.assertEqual(replay["event_id"], first["event_id"])
        # The later rebind to ws-b was NOT deleted by the replayed release.
        self.assertEqual(self._resolve(self.conn2).workspace_id, "ws-b")
        # No duplicate event for key K.
        self.assertEqual(self._event_count(self.conn2, "K"), 1)

    def test_concurrent_bind_exact_replay_does_not_reinsert(self):
        # Bind ch->ws-a with key KB. A concurrent exact replay (created=False) must
        # return the receipt and must not attempt a second INSERT (which would
        # violate the composite PK / produce already_bound noise).
        first = bind_channel_workspace(
            self.conn1, platform="discord", channel_id="123", workspace_id="ws-a",
            actor="op", reason="r", idempotency_key="KB",
        )
        self.assertEqual(first["status"], "bound")

        # Bypass the idempotency precheck AND the active-row guard so the call
        # reaches append_event, which returns created=False for key KB and must
        # re-validate rather than INSERT again.
        real_check = db_module._check_channel_binding_idempotency
        real_get_row = db_module._get_channel_binding_row
        db_module._check_channel_binding_idempotency = lambda *a, **k: None
        db_module._get_channel_binding_row = lambda *a, **k: None
        try:
            replay = bind_channel_workspace(
                self.conn1, platform="discord", channel_id="123", workspace_id="ws-a",
                actor="op", reason="r", idempotency_key="KB",
            )
        finally:
            db_module._check_channel_binding_idempotency = real_check
            db_module._get_channel_binding_row = real_get_row

        self.assertEqual(replay["status"], "replayed")
        self.assertEqual(replay["event_id"], first["event_id"])
        self.assertEqual(len(list_channel_bindings(self.conn2)), 1)
        self.assertEqual(self._event_count(self.conn2, "KB"), 1)

    def test_concurrent_bind_cross_payload_key_reuse_fails_closed(self):
        # conn1 binds ch->ws-a with key KC. A concurrent mutation reuses KC with a
        # different payload (different workspace). Via created=False the mismatch is
        # detected and fails closed rather than inserting.
        bind_channel_workspace(
            self.conn1, platform="discord", channel_id="123", workspace_id="ws-a",
            actor="op", reason="r", idempotency_key="KC",
        )

        real_check = db_module._check_channel_binding_idempotency
        db_module._check_channel_binding_idempotency = lambda *a, **k: None
        try:
            with self.assertRaises(ValueError):
                bind_channel_workspace(
                    self.conn1, platform="discord", channel_id="123", workspace_id="ws-b",
                    actor="op", reason="r", idempotency_key="KC",
                )
        finally:
            db_module._check_channel_binding_idempotency = real_check

        # Still bound to ws-a only; no second binding; single KC event.
        self.assertEqual(self._resolve(self.conn2).workspace_id, "ws-a")
        self.assertEqual(len(list_channel_bindings(self.conn2)), 1)
        self.assertEqual(self._event_count(self.conn2, "KC"), 1)

    def test_concurrent_release_cross_operation_key_reuse_fails_closed(self):
        # Key KO was used by a bind. A release reusing KO (different event_type /
        # payload) via created=False must fail closed, not delete the row.
        bind_channel_workspace(
            self.conn1, platform="discord", channel_id="123", workspace_id="ws-a",
            actor="op", reason="r", idempotency_key="KO",
        )

        real_check = db_module._check_channel_binding_idempotency
        db_module._check_channel_binding_idempotency = lambda *a, **k: None
        try:
            with self.assertRaises(ValueError):
                release_channel_workspace(
                    self.conn1, platform="discord", channel_id="123",
                    expected_workspace_id="ws-a", actor="op", reason="r",
                    idempotency_key="KO",
                )
        finally:
            db_module._check_channel_binding_idempotency = real_check

        # Row untouched; still bound to ws-a; only the original bind event exists.
        self.assertEqual(self._resolve(self.conn2).workspace_id, "ws-a")
        self.assertEqual(self._event_count(self.conn2, "KO"), 1)


if __name__ == "__main__":
    unittest.main()
