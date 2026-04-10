# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.
"""Thread-safe per-operation latency histogram and error tracking."""

import threading
import time


class Stats:
    """Thread-safe per-operation latency and error tracking.

    Uses a sorted-list percentile calculation to avoid native dependencies.
    A background reporter drains accumulated data every reporting interval.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._latencies: dict[str, list[float]] = {}
        self._error_counts: dict[str, int] = {}
        self._errors: list[dict] = []

    def record(self, operation: str, duration_ms: float):
        """Record a successful operation with its duration in milliseconds."""
        with self._lock:
            if operation not in self._latencies:
                self._latencies[operation] = []
                self._error_counts[operation] = 0
            self._latencies[operation].append(duration_ms)

    def record_error(self, operation: str, error_msg: str, traceback_str: str,
                     status_code: int = None, sub_status_code: int = None):
        """Record a failed operation with error details."""
        with self._lock:
            if operation not in self._error_counts:
                self._error_counts[operation] = 0
                self._latencies[operation] = []
            self._error_counts[operation] += 1
            self._errors.append({
                "operation": operation,
                "error_message": error_msg,
                "source_message": traceback_str,
                "error_status_code": status_code,
                "error_sub_status_code": sub_status_code,
                "timestamp": time.time(),
            })

    def drain_all(self) -> tuple[list[dict], list[dict]]:
        """Atomically drain both summaries and error details under one lock.

        Returns (summaries, errors) where summaries is a list of dicts with:
        operation, count, errors, min_ms, max_ms, mean_ms, p50_ms, p90_ms, p99_ms
        and errors is a list of dicts with: operation, error_message, source_message,
        error_status_code, error_sub_status_code, timestamp.
        """
        with self._lock:
            summaries = []
            all_ops = set(self._latencies.keys()) | set(self._error_counts.keys())
            for op in sorted(all_ops):
                latencies = self._latencies.get(op, [])
                errors = self._error_counts.get(op, 0)
                count = len(latencies)
                if count == 0 and errors == 0:
                    continue
                if count > 0:
                    latencies.sort()
                    total = sum(latencies)
                    summaries.append({
                        "operation": op,
                        "count": count,
                        "errors": errors,
                        "min_ms": latencies[0],
                        "max_ms": latencies[-1],
                        "mean_ms": total / count,
                        "p50_ms": _percentile(latencies, 50.0),
                        "p90_ms": _percentile(latencies, 90.0),
                        "p99_ms": _percentile(latencies, 99.0),
                    })
                else:
                    summaries.append({
                        "operation": op,
                        "count": 0,
                        "errors": errors,
                        "min_ms": 0.0,
                        "max_ms": 0.0,
                        "mean_ms": 0.0,
                        "p50_ms": 0.0,
                        "p90_ms": 0.0,
                        "p99_ms": 0.0,
                    })
            self._latencies.clear()
            self._error_counts.clear()
            error_details = self._errors
            self._errors = []
            return summaries, error_details

    def drain_summaries(self) -> list[dict]:
        """Drain accumulated stats and return per-operation summaries."""
        summaries, _ = self.drain_all()
        return summaries

    def drain_errors(self) -> list[dict]:
        """Drain accumulated error details."""
        _, errors = self.drain_all()
        return errors


def _percentile(sorted_data: list[float], pct: float) -> float:
    """Calculate percentile from pre-sorted data using nearest-rank method."""
    n = len(sorted_data)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_data[0]
    rank = (pct / 100.0) * (n - 1)
    lower = int(rank)
    upper = lower + 1
    if upper >= n:
        return sorted_data[-1]
    fraction = rank - lower
    return sorted_data[lower] + fraction * (sorted_data[upper] - sorted_data[lower])
