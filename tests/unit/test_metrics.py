"""
Comprehensive unit tests for Gateway Metrics (collector, stats, routes, timings).
All tests use fake clocks or injected dependencies; no real sleeps.
"""
from __future__ import annotations

import asyncio
from typing import Optional
import httpx
import pytest

from gateway import errors as errs
from gateway.app import create_app
from gateway.config import Settings
from gateway.core.client_pool import ClientPool
from gateway.core.limiter import Limiter, QueueFullError, QueueTimeoutError
from gateway.core.pipeline import Pipeline
from gateway.core.retry import RetryPolicy
from gateway.core.timing import (
    compute_timings,
    get_clamped_overhead_count,
    reset_clamped_overhead_count,
)
from gateway.metrics.collector import MetricsCollector, normalize_outcome_kind
from gateway.metrics.stats import (
    compute_error_rate,
    compute_percentiles,
    compute_throughput,
    nearest_rank_percentile,
)
from gateway.providers.base import Provider, ProviderResult
from gateway.schemas import ChatRequest, TokenUsage


class FakeClock:
    def __init__(self, start: float = 100.0) -> None:
        self.current = start

    def __call__(self) -> float:
        return self.current

    def advance(self, delta: float) -> None:
        self.current += delta


class ScriptedProvider(Provider):
    """Provider that yields scripted outcomes and advances fake clock."""

    def __init__(
        self,
        clock: FakeClock,
        outcomes: list[Exception | str],
        call_duration: float = 0.05,
        connect_time: float = 0.01,
    ) -> None:
        self.clock = clock
        self.outcomes = list(outcomes)
        self.call_duration = call_duration
        self.connect_time = connect_time
        self.call_count = 0

    @property
    def name(self) -> str:
        return "scripted"

    async def complete(self, request: ChatRequest, *, timeout: Optional[float] = None) -> ProviderResult:
        self.call_count += 1
        self.clock.advance(self.call_duration)
        if not self.outcomes:
            raise errs.ServerError("No scripted outcomes left")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return ProviderResult(
            text=outcome,
            provider="scripted",
            model="test-model",
            usage=TokenUsage(prompt_tokens=5, completion_tokens=5, total_tokens=10),
            http_status=200,
            provider_duration=self.call_duration,
            connect_time=self.connect_time,
        )


# ---------------------------------------------------------------------------
# 1. Nearest-rank known vectors
# ---------------------------------------------------------------------------

def test_nearest_rank_known_vectors() -> None:
    # Empty
    assert nearest_rank_percentile([], 50.0) == 0.0

    # Single value
    assert nearest_rank_percentile([42.0], 50.0) == 42.0
    assert nearest_rank_percentile([42.0], 95.0) == 42.0
    assert nearest_rank_percentile([42.0], 99.0) == 42.0

    # 1 to 10
    v10 = [float(x) for x in range(1, 11)]
    # rank = ceil(p/100 * 10)
    # p50 -> rank 5 -> index 4 -> 5.0
    assert nearest_rank_percentile(v10, 50.0) == 5.0
    # p25 -> rank 3 -> index 2 -> 3.0
    assert nearest_rank_percentile(v10, 25.0) == 3.0
    # p75 -> rank 8 -> index 7 -> 8.0
    assert nearest_rank_percentile(v10, 75.0) == 8.0
    # p95 -> rank 10 -> index 9 -> 10.0
    assert nearest_rank_percentile(v10, 95.0) == 10.0
    # p99 -> rank 10 -> index 9 -> 10.0
    assert nearest_rank_percentile(v10, 99.0) == 10.0

    # NIST example: [15, 20, 35, 40, 50]
    nist = [15.0, 20.0, 35.0, 40.0, 50.0]
    # p50 -> ceil(0.5 * 5) = 3 -> 35.0
    assert nearest_rank_percentile(nist, 50.0) == 35.0
    # p40 -> ceil(0.4 * 5) = 2 -> 20.0
    assert nearest_rank_percentile(nist, 40.0) == 20.0
    # p95 -> ceil(0.95 * 5) = 5 -> 50.0
    assert nearest_rank_percentile(nist, 95.0) == 50.0

    # 1 to 100
    v100 = [float(x) for x in range(1, 101)]
    res = compute_percentiles(v100)
    assert res["p50"] == 50.0
    assert res["p95"] == 95.0
    assert res["p99"] == 99.0


# ---------------------------------------------------------------------------
# 2. Counters for a scripted failure sequence (attempt vs final)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_counters_scripted_failure_sequence() -> None:
    clock = FakeClock(10.0)

    async def fake_sleep(d: float) -> None:
        clock.advance(d)

    # Sequence of 3 requests:
    # Req 1: attempt 1 -> 429, attempt 2 -> 500, attempt 3 -> success
    # Req 2: attempt 1 -> timeout, attempt 2 -> connection, attempt 3 -> connection (fails permanently)
    # Req 3: attempt 1 -> auth (fails permanently, non-retryable)
    outcomes: list[Exception | str] = [
        errs.RateLimitError("429", retry_after=0.01),
        errs.ServerError("500"),
        "Req1 Success",
        errs.TimeoutError("timeout"),
        errs.ConnectionError("conn reset"),
        errs.ConnectionError("conn refused"),
        errs.AuthError("invalid key"),
    ]

    provider = ScriptedProvider(clock, outcomes, call_duration=0.01)
    collector = MetricsCollector()
    settings = Settings(max_attempts=3)
    pool = ClientPool(settings)

    pipeline = Pipeline(
        settings,
        pool,
        retry_policy=RetryPolicy(max_attempts=3, sleep=fake_sleep, clock=clock),
        provider_factory=lambda settings, client=None: provider,
        clock=clock,
        collector=collector,
    )

    # Req 1 succeeds on 3rd attempt
    r1 = await pipeline.handle(ChatRequest(prompt="1"), request_id="req-1")
    assert r1.text == "Req1 Success"

    # Req 2 exhausts 3 attempts and raises ConnectionError
    with pytest.raises(errs.ConnectionError):
        await pipeline.handle(ChatRequest(prompt="2"), request_id="req-2")

    # Req 3 fails immediately on auth error without retries
    with pytest.raises(errs.AuthError):
        await pipeline.handle(ChatRequest(prompt="3"), request_id="req-3")

    metrics = collector.get_metrics()
    attempts = metrics["counters"]["attempts"]
    finals = metrics["counters"]["finals"]

    # Attempt-level counters:
    # 1 success + 1 (429) + 1 (5xx) + 1 (timeout) + 2 (connection) + 1 (auth_4xx) = 7 attempts
    assert attempts["success"] == 1
    assert attempts["429"] == 1
    assert attempts["5xx"] == 1
    assert attempts["timeout"] == 1
    assert attempts["connection"] == 2
    assert attempts["auth_4xx"] == 1

    # Final-level counters:
    # Req 1 was success, Req 2 was connection, Req 3 was auth_4xx
    assert finals["success"] == 1
    assert finals["connection"] == 1
    assert finals["auth_4xx"] == 1
    assert finals["429"] == 0
    assert finals["5xx"] == 0
    assert finals["timeout"] == 0

    assert metrics["requests"]["total"] == 3
    assert metrics["requests"]["successful"] == 1
    assert metrics["requests"]["failed"] == 2


# ---------------------------------------------------------------------------
# 3. Throughput calculation
# ---------------------------------------------------------------------------

def test_throughput_calculation() -> None:
    # Direct function test
    # 20 successes over 4.0 seconds => 5.0 rps
    assert compute_throughput(20, 100.0, 104.0) == 5.0
    # 0 successes => 0.0
    assert compute_throughput(0, 100.0, 105.0) == 0.0
    # <= 0 window => 0.0
    assert compute_throughput(10, 100.0, 100.0) == 0.0

    # Collector integration test
    collector = MetricsCollector()
    collector.record_request(
        request_id="r1",
        success=True,
        final_outcome="success",
        total_duration=1.0,
        start_time=10.0,
        end_time=11.0,
        attempts=1,
    )
    collector.record_request(
        request_id="r2",
        success=True,
        final_outcome="success",
        total_duration=1.5,
        start_time=10.5,
        end_time=12.5,
        attempts=1,
    )
    # 2 successes over (12.5 - 10.0) = 2.5s -> 0.8 rps
    m = collector.get_metrics()
    assert m["rates"]["throughput_rps"] == 0.8


# ---------------------------------------------------------------------------
# 4. Buckets sum to total (residual >= 0, clamp count 0)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_buckets_sum_to_total_and_clamp_count_zero() -> None:
    reset_clamped_overhead_count()
    clock = FakeClock(50.0)

    async def fake_sleep(d: float) -> None:
        clock.advance(d)

    provider = ScriptedProvider(
        clock,
        [errs.RateLimitError("429", retry_after=0.05), "Finished"],
        call_duration=0.04,
        connect_time=0.01,
    )
    settings = Settings(max_attempts=3)
    pool = ClientPool(settings)
    collector = MetricsCollector()

    pipeline = Pipeline(
        settings,
        pool,
        retry_policy=RetryPolicy(max_attempts=3, sleep=fake_sleep, clock=clock),
        provider_factory=lambda settings, client=None: provider,
        clock=clock,
        collector=collector,
    )

    resp = await pipeline.handle(ChatRequest(prompt="hello"), request_id="sum-check")
    t = resp.timings

    # Total must equal sum of non-connect parts + overhead
    sum_parts = (
        t.queue_wait
        + t.client_setup
        + t.provider_time
        + t.backoff_sleep
        + t.response_processing
        + t.overhead
    )
    assert pytest.approx(t.total, abs=1e-6) == sum_parts
    assert t.overhead >= 0.0
    assert t.connect_time > 0.0
    assert get_clamped_overhead_count() == 0

    m = collector.get_metrics()
    assert m["timing_integrity"]["clamped_overhead_count"] == 0
    assert m["timing_integrity"]["mean_residual_delta"] < 1e-6


# ---------------------------------------------------------------------------
# 5. Bounded records in deque
# ---------------------------------------------------------------------------

def test_bounded_records() -> None:
    max_records = 5
    collector = MetricsCollector(max_records=max_records)

    for i in range(15):
        collector.record_request(
            request_id=f"req-{i}",
            success=True,
            final_outcome="success",
            total_duration=0.1,
            start_time=float(i),
            end_time=float(i) + 0.1,
            attempts=1,
        )

    # Deque should only retain the last 5 records
    assert len(collector.records) == max_records
    assert [r.request_id for r in collector.records] == [f"req-{i}" for i in range(10, 15)]

    # Running counters still reflect all 15 requests
    m = collector.get_metrics()
    assert m["requests"]["total"] == 15
    assert m["requests"]["successful"] == 15
    assert m["records_retained"] == 5
    assert m["max_records"] == 5


# ---------------------------------------------------------------------------
# 6. Reset (clears collector and peak gauges)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reset_collector_and_limiter_peaks() -> None:
    reset_clamped_overhead_count()
    limiter = Limiter(limit=5)
    collector = MetricsCollector()

    # Create artificial limiter activity
    async with limiter.slot():
        async with limiter.calling_provider():
            pass

    assert limiter.peak_slots_held >= 1
    assert limiter.peak_in_flight >= 1

    collector.record_attempt("429")
    collector.record_request(
        request_id="r1",
        success=True,
        final_outcome="success",
        total_duration=0.05,
        start_time=1.0,
        end_time=1.05,
        attempts=1,
    )

    assert len(collector.records) == 1
    assert collector.final_counters["success"] == 1

    # Perform reset
    collector.reset()
    limiter.reset_peaks()
    reset_clamped_overhead_count()

    assert len(collector.records) == 0
    assert collector.final_counters["success"] == 0
    assert collector.attempt_counters["429"] == 0
    assert limiter.peak_slots_held == 0
    assert limiter.peak_in_flight == 0
    assert limiter.peak_waiting == 0
    assert get_clamped_overhead_count() == 0


# ---------------------------------------------------------------------------
# 7. /metrics and /metrics/reset route shape
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_metrics_route_shape_and_reset() -> None:
    app = create_app()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # Initial /metrics shape
        r = await client.get("/metrics")
        assert r.status_code == 200
        data = r.json()

        assert "requests" in data
        assert "counters" in data
        assert "attempts" in data["counters"]
        assert "finals" in data["counters"]
        assert "rejects" in data["counters"]
        assert "client_cancelled" in data["counters"]
        assert "rates" in data
        assert "throughput_rps" in data["rates"]
        assert "latency" in data
        assert "successful" in data["latency"]
        succ = data["latency"]["successful"]
        assert "client_setup" in succ and "p50" in succ["client_setup"] and "p95" in succ["client_setup"] and "p99" in succ["client_setup"]
        assert "backoff_sleep" in succ and "p50" in succ["backoff_sleep"] and "p95" in succ["backoff_sleep"] and "p99" in succ["backoff_sleep"]
        assert "failed" in data["latency"]
        assert "limiter" in data
        assert "peak_in_flight" in data["limiter"]
        assert "timing_integrity" in data

        # POST /metrics/reset
        r_reset = await client.post("/metrics/reset")
        assert r_reset.status_code == 200
        assert r_reset.json() == {"status": "reset"}


# ---------------------------------------------------------------------------
# 8. Rejects and cancels counted exactly once
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rejects_and_cancels_counted_exactly_once() -> None:
    clock = FakeClock()
    collector = MetricsCollector()
    limiter = Limiter(limit=1, queue_size=1, queue_wait_timeout=0.05)
    settings = Settings(concurrency_limit=1, queue_size=1)
    pool = ClientPool(settings)

    hold_gate = asyncio.Event()

    class GatedProvider(Provider):
        name = "gate"
        async def complete(self, req: ChatRequest, *, timeout: Optional[float] = None) -> ProviderResult:
            await hold_gate.wait()
            return ProviderResult(
                text="ok", provider="gate", model="model",
                usage=TokenUsage(), http_status=200, provider_duration=0.01,
            )

    pipeline = Pipeline(
        settings,
        pool,
        limiter=limiter,
        provider_factory=lambda settings, client=None: GatedProvider(),
        clock=clock,
        collector=collector,
    )

    # 1. Start slot holder
    t1 = asyncio.create_task(pipeline.handle(ChatRequest(prompt="1"), request_id="t1"))
    await asyncio.sleep(0.01)

    # 2. Fill the single queue slot
    t2 = asyncio.create_task(pipeline.handle(ChatRequest(prompt="2"), request_id="t2"))
    await asyncio.sleep(0.01)

    # 3. Third request causes immediate QueueFullError
    with pytest.raises(QueueFullError):
        await pipeline.handle(ChatRequest(prompt="3"), request_id="t3")

    # Queue full reject must be counted exactly once
    assert collector.reject_counters["queue_full"] == 1

    # 4. t2 will time out in queue after 0.05s
    with pytest.raises(QueueTimeoutError):
        await t2

    # Queue timeout reject must be counted exactly once
    assert collector.reject_counters["queue_timeout"] == 1

    # 5. Cancel t1
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1

    # Cancelled must be counted exactly once
    assert collector.client_cancelled_count == 1

    # Verify total rejected and cancelled in collector
    m = collector.get_metrics()
    assert m["requests"]["rejected"] == 2
    assert m["requests"]["cancelled"] == 1
    assert m["requests"]["successful"] == 0
