from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Callable, TextIO

from .bus import MessageBus, pump_deliveries


Sleep = Callable[[float], None]


@dataclass(frozen=True)
class DeliveryWorkerResult:
    iterations: int
    processed: int
    sent: int
    failed: int
    interrupted: bool
    results: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "iterations": self.iterations,
            "processed": self.processed,
            "sent": self.sent,
            "failed": self.failed,
            "interrupted": self.interrupted,
            "results": self.results,
        }


def run_delivery_worker(
    conn: sqlite3.Connection,
    *,
    platform: str | None = None,
    limit: int = 10,
    interval: float = 5.0,
    max_iterations: int | None = None,
    bus: MessageBus | None = None,
    output_stream: TextIO | None = None,
    recover_sending: bool = False,
    sleep: Sleep = time.sleep,
) -> DeliveryWorkerResult:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if interval < 0:
        raise ValueError("interval must be non-negative")
    if max_iterations is not None and max_iterations < 1:
        raise ValueError("max_iterations must be at least 1")

    iterations = 0
    processed = 0
    sent = 0
    failed = 0
    results: list[dict[str, Any]] = []
    interrupted = False

    try:
        while True:
            pump_result = pump_deliveries(
                conn,
                platform=platform,
                limit=limit,
                bus=bus,
                output_stream=output_stream,
                recover_sending=recover_sending and iterations == 0,
            )
            iterations += 1
            processed += pump_result.processed
            sent += pump_result.sent
            failed += pump_result.failed
            results.append(pump_result.to_dict())

            if max_iterations is not None and iterations >= max_iterations:
                break
            sleep(interval)
    except KeyboardInterrupt:
        interrupted = True

    return DeliveryWorkerResult(
        iterations=iterations,
        processed=processed,
        sent=sent,
        failed=failed,
        interrupted=interrupted,
        results=results,
    )
