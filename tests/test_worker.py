import io
import unittest

from coordinate.bus import StdoutBus
from coordinate.db import (
    create_delivery,
    initialize,
    list_deliveries,
    mark_delivery_sending,
    row_to_dict,
)
from coordinate.worker import run_delivery_worker


class FlakyBus:
    def __init__(self):
        self.calls = 0

    def send(self, *, destination, payload, message_key):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary network failure")
        return "stdout:retry-ok"


class DeliveryWorkerTests(unittest.TestCase):
    def make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        return conn

    def test_worker_pumps_pending_deliveries_and_does_not_repeat_sent(self):
        conn = self.make_conn()
        create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:1",
            payload={"text": "[RESULT] mvp-001"},
        )
        sleeps = []
        stream = io.StringIO()

        result = run_delivery_worker(
            conn,
            platform="stdout",
            limit=10,
            interval=0.1,
            max_iterations=2,
            bus=StdoutBus(stream),
            sleep=sleeps.append,
        )

        deliveries = [row_to_dict(row) for row in list_deliveries(conn)]
        self.assertEqual(result.iterations, 2)
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.sent, 1)
        self.assertEqual(result.failed, 0)
        self.assertFalse(result.interrupted)
        self.assertEqual(sleeps, [0.1])
        self.assertEqual(deliveries[0]["status"], "sent")
        self.assertEqual(stream.getvalue().count("[RESULT] mvp-001"), 1)

    def test_worker_returns_summary_on_keyboard_interrupt(self):
        conn = self.make_conn()
        create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:1",
            payload={"text": "[STATE] reconciled"},
        )

        def interrupt(_interval):
            raise KeyboardInterrupt

        result = run_delivery_worker(
            conn,
            platform="stdout",
            limit=10,
            interval=0.1,
            bus=StdoutBus(io.StringIO()),
            sleep=interrupt,
        )

        self.assertEqual(result.iterations, 1)
        self.assertEqual(result.sent, 1)
        self.assertTrue(result.interrupted)

    def test_worker_can_recover_sending_on_first_iteration(self):
        conn = self.make_conn()
        row, _ = create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:recover",
            payload={"text": "[STATE] recover"},
        )
        mark_delivery_sending(conn, row["id"])

        result = run_delivery_worker(
            conn,
            platform="stdout",
            limit=10,
            interval=0,
            max_iterations=1,
            bus=StdoutBus(io.StringIO()),
            recover_sending=True,
        )

        delivery = row_to_dict(list_deliveries(conn)[0])
        self.assertEqual(result.sent, 1)
        self.assertEqual(delivery["status"], "sent")

    def test_worker_retries_failed_delivery_on_later_iteration(self):
        conn = self.make_conn()
        create_delivery(
            conn,
            platform="stdout",
            destination="local",
            message_key="demo:message:retry",
            payload={"text": "[BLOCKER] retry"},
        )
        bus = FlakyBus()

        result = run_delivery_worker(
            conn,
            platform="stdout",
            limit=10,
            interval=0,
            max_iterations=2,
            bus=bus,
        )

        delivery = row_to_dict(list_deliveries(conn)[0])
        self.assertEqual(result.processed, 2)
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.sent, 1)
        self.assertEqual(bus.calls, 2)
        self.assertEqual(delivery["status"], "sent")
        self.assertEqual(delivery["attempt_count"], 2)

    def test_worker_validates_loop_parameters(self):
        conn = self.make_conn()

        with self.assertRaisesRegex(ValueError, "limit"):
            run_delivery_worker(conn, limit=0, max_iterations=1)
        with self.assertRaisesRegex(ValueError, "interval"):
            run_delivery_worker(conn, interval=-1, max_iterations=1)
        with self.assertRaisesRegex(ValueError, "max_iterations"):
            run_delivery_worker(conn, max_iterations=0)


if __name__ == "__main__":
    unittest.main()
