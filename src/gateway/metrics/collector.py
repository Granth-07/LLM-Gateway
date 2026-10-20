"""
Metrics collector for LLM Gateway.

Rules (from CLAUDE.md & requirements):
- Counters at attempt level AND final level for:
    success, timeout, 429, 5xx, connection, auth/4xx
- Separately:
    overflow rejects (queue_full, queue_timeout) and client-cancelled
- Per-request records in a BOUNDED deque (configurable max) plus running counters.
- Percentiles over successful requests (p50/p95/p99) using nearest-rank.
- Failed-request latency reported separately.
- Per-bucket p50/p95/p99 for:
    queue_wait, provider_time, connect_time, response_processing, overhead.
- Invariant: total = queue_wait + client_setup + provider_time + backoff_sleep + response_processing + overhead.
"""
from __future__ import annotations

import collections
import time
from dataclasses import dataclass
from typing import Any, Optional

from gateway.core.limiter import Limiter
from gateway.core.timing import get_clamped_overhead_count
from gateway.metrics.stats import (
    compute_error_rate,
    compute_percentiles,
    compute_throughput,
)
from gateway.schemas import RequestTimings


def normalize_outcome_kind(kind: str) -> str:
    """Normalize diverse error kinds into the standard set of 6 counter buckets."""
    k = str(kind).lower().strip()
    if k in ("success", "ok", "200"):
        return "success"
    if k in ("timeout", "upstream_timeout", "504"):
        return "timeout"
    if k in ("rate_limit", "429", "rate_limit_exceeded"):
        return "429"
    if k in ("server", "5xx", "500", "502", "503", "server_error", "bad_gateway"):
        return "5xx"
    if k in ("connection", "connection_error", "connect_error"):
        return "connection"
    if k in ("auth", "client", "auth/4xx", "auth_4xx", "401", "400", "422", "unknown_provider"):
        return "auth_4xx"
    return "5xx"


@dataclass
class RequestRecord:
    """Structured record of a completed or rejected request."""

    request_id: str
    success: bool
    final_outcome: str
    total_duration: float
    start_time: float
    end_time: float
    attempts: int
    timings: Optional[RequestTimings] = None


class MetricsCollector:
    """
    Central in-memory collector for attempt and final outcome counters,
    bounded per-request latency records, and percentile latency breakdowns.
    """

    def __init__(self, max_records: int = 10_000) -> None:
        self.max_records = max_records
        self.records: collections.deque[RequestRecord] = collections.deque(maxlen=max_records)

        # Running attempt and final outcome counters
        self.attempt_counters: dict[str, int] = {
            "success": 0,
            "timeout": 0,
            "429": 0,
            "5xx": 0,
            "connection": 0,
            "auth_4xx": 0,
        }
        self.final_counters: dict[str, int] = {
            "success": 0,
            "timeout": 0,
            "429": 0,
            "5xx": 0,
            "connection": 0,
            "auth_4xx": 0,
        }

        # Overflow rejects (separate from provider failures)
        self.reject_counters: dict[str, int] = {
            "queue_full": 0,
            "queue_timeout": 0,
        }

        # Client cancellation counter
        self.client_cancelled_count: int = 0

        # Throughput time window tracking (perf_counter)
        self.first_start: Optional[float] = None
        self.last_end: Optional[float] = None

    def record_attempt(self, outcome_kind: str) -> None:
        """Record a single attempt outcome."""
        norm = normalize_outcome_kind(outcome_kind)
        self.attempt_counters[norm] = self.attempt_counters.get(norm, 0) + 1

    def record_request(
        self,
        *,
        request_id: str,
        success: bool,
        final_outcome: str,
        total_duration: float,
        start_time: float,
        end_time: float,
        attempts: int,
        timings: Optional[RequestTimings] = None,
    ) -> None:
        """Record a completed request (success or provider failure)."""
        norm = normalize_outcome_kind(final_outcome)
        self.final_counters[norm] = self.final_counters.get(norm, 0) + 1

        if self.first_start is None or start_time < self.first_start:
            self.first_start = start_time
        if self.last_end is None or end_time > self.last_end:
            self.last_end = end_time

        self.records.append(
            RequestRecord(
                request_id=request_id,
                success=success,
                final_outcome=norm,
                total_duration=max(0.0, total_duration),
                start_time=start_time,
                end_time=end_time,
                attempts=attempts,
                timings=timings,
            )
        )

    def record_reject(
        self,
        reason: str,
        *,
        request_id: str = "",
        duration: float = 0.0,
        start_time: float = 0.0,
        end_time: float = 0.0,
    ) -> None:
        """Record an overflow rejection before slot acquisition (queue_full / queue_timeout)."""
        clean_reason = "queue_timeout" if "timeout" in reason else "queue_full"
        self.reject_counters[clean_reason] = self.reject_counters.get(clean_reason, 0) + 1

        now = end_time or time.perf_counter()
        st = start_time or (now - duration)
        if self.first_start is None or st < self.first_start:
            self.first_start = st
        if self.last_end is None or now > self.last_end:
            self.last_end = now

        self.records.append(
            RequestRecord(
                request_id=request_id,
                success=False,
                final_outcome=clean_reason,
                total_duration=max(0.0, duration),
                start_time=st,
                end_time=now,
                attempts=0,
                timings=None,
            )
        )

    def record_cancelled(
        self,
        *,
        request_id: str = "",
        duration: float = 0.0,
        start_time: float = 0.0,
        end_time: float = 0.0,
        attempts: int = 0,
    ) -> None:
        """Record a request cancelled by client disconnect."""
        self.client_cancelled_count += 1

        now = end_time or time.perf_counter()
        st = start_time or (now - duration)
        if self.first_start is None or st < self.first_start:
            self.first_start = st
        if self.last_end is None or now > self.last_end:
            self.last_end = now

        self.records.append(
            RequestRecord(
                request_id=request_id,
                success=False,
                final_outcome="client_cancelled",
                total_duration=max(0.0, duration),
                start_time=st,
                end_time=now,
                attempts=attempts,
                timings=None,
            )
        )

    def reset(self) -> None:
        """Reset all counters, records, and throughput windows."""
        self.records.clear()
        for k in self.attempt_counters:
            self.attempt_counters[k] = 0
        for k in self.final_counters:
            self.final_counters[k] = 0
        for k in self.reject_counters:
            self.reject_counters[k] = 0
        self.client_cancelled_count = 0
        self.first_start = None
        self.last_end = None

    def get_metrics(self, limiter: Optional[Limiter] = None) -> dict[str, Any]:
        """
        Aggregate and return comprehensive metrics report including percentiles,
        throughput, rates, counters, and limiter gauges.
        """
        # Collect latencies from successful records
        success_totals: list[float] = []
        queue_waits: list[float] = []
        client_setups: list[float] = []
        provider_times: list[float] = []
        connect_times: list[float] = []
        backoff_sleeps: list[float] = []
        response_processings: list[float] = []
        overheads: list[float] = []

        # Residual verification: mean(total - sum of parts)
        residual_deltas: list[float] = []

        # Collect failed latencies separately
        failed_totals: list[float] = []

        for rec in self.records:
            if rec.success and rec.timings is not None:
                t = rec.timings
                success_totals.append(t.total)
                queue_waits.append(t.queue_wait)
                client_setups.append(t.client_setup)
                provider_times.append(t.provider_time)
                connect_times.append(t.connect_time)
                backoff_sleeps.append(t.backoff_sleep)
                response_processings.append(t.response_processing)
                overheads.append(t.overhead)

                # sum of non-connect parts:
                sum_parts = (
                    t.queue_wait
                    + t.client_setup
                    + t.provider_time
                    + t.backoff_sleep
                    + t.response_processing
                    + t.overhead
                )
                residual_deltas.append(abs(t.total - sum_parts))
            elif not rec.success:
                failed_totals.append(rec.total_duration)

        total_attempts = sum(self.attempt_counters.values())
        failed_attempts = total_attempts - self.attempt_counters["success"]
        attempt_error_rate = compute_error_rate(failed_attempts, total_attempts)

        total_finals = sum(self.final_counters.values())
        failed_finals = total_finals - self.final_counters["success"]
        final_error_rate = compute_error_rate(failed_finals, total_finals)

        total_rejects = sum(self.reject_counters.values())
        total_requests = total_finals + total_rejects + self.client_cancelled_count

        throughput = 0.0
        if self.first_start is not None and self.last_end is not None:
            throughput = compute_throughput(
                self.final_counters["success"],
                self.first_start,
                self.last_end,
            )

        mean_residual_delta = (
            round(sum(residual_deltas) / len(residual_deltas), 8) if residual_deltas else 0.0
        )

        limiter_metrics = {}
        if limiter is not None:
            limiter_metrics = {
                "waiting": limiter.waiting,
                "slots_held": limiter.slots_held,
                "in_flight": limiter.in_flight,
                "peak_waiting": limiter.peak_waiting,
                "peak_slots_held": limiter.peak_slots_held,
                "peak_in_flight": limiter.peak_in_flight,
            }

        return {
            "requests": {
                "total": total_requests,
                "successful": self.final_counters["success"],
                "failed": failed_finals,
                "rejected": total_rejects,
                "cancelled": self.client_cancelled_count,
            },
            "counters": {
                "attempts": dict(self.attempt_counters),
                "finals": dict(self.final_counters),
                "rejects": dict(self.reject_counters),
                "client_cancelled": self.client_cancelled_count,
            },
            "rates": {
                "attempt_error_rate": round(attempt_error_rate, 4),
                "final_error_rate": round(final_error_rate, 4),
                "throughput_rps": round(throughput, 2),
            },
            "latency": {
                "successful": {
                    "total": compute_percentiles(success_totals),
                    "queue_wait": compute_percentiles(queue_waits),
                    "client_setup": compute_percentiles(client_setups),
                    "provider_time": compute_percentiles(provider_times),
                    "connect_time": compute_percentiles(connect_times),
                    "backoff_sleep": compute_percentiles(backoff_sleeps),
                    "response_processing": compute_percentiles(response_processings),
                    "overhead": compute_percentiles(overheads),
                },
                "failed": {
                    "count": len(failed_totals),
                    "total": compute_percentiles(failed_totals),
                },
            },
            "limiter": limiter_metrics,
            "timing_integrity": {
                "clamped_overhead_count": get_clamped_overhead_count(),
                "mean_residual_delta": mean_residual_delta,
            },
            "records_retained": len(self.records),
            "max_records": self.max_records,
        }
