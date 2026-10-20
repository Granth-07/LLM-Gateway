"""
Unit tests for Pipeline execution, timings invariant, attempt tracking, and error handling.
Deterministic: injected fake clock and fake sleep.
No network access required.
"""
from __future__ import annotations

import asyncio
from typing import Optional
import pytest

from gateway import errors as errs
from gateway.config import Settings
from gateway.core.client_pool import ClientPool
from gateway.core.pipeline import Pipeline
from gateway.core.retry import RetryBudget, RetryPolicy
from gateway.providers.base import Provider, ProviderResult
from gateway.schemas import ChatRequest, TokenUsage


class FakeClock:
    def __init__(self, start: float = 100.0) -> None:
        self.current = start

    def __call__(self) -> float:
        return self.current

    def advance(self, delta: float) -> None:
        self.current += delta


class MockProvider(Provider):
    def __init__(self, clock: FakeClock, outcomes: list[Exception | str], duration_per_call: float = 0.05) -> None:
        self.clock = clock
        self.outcomes = list(outcomes)
        self.call_count = 0
        self.duration = duration_per_call

    @property
    def name(self) -> str:
        return "mock"

    async def complete(self, request: ChatRequest, *, timeout: Optional[float] = None) -> ProviderResult:
        self.call_count += 1
        self.clock.advance(self.duration)
        if not self.outcomes:
            raise errs.ServerError("No outcomes left")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return ProviderResult(
            text=outcome,
            provider="mock",
            model="mock-model",
            usage=TokenUsage(prompt_tokens=5, completion_tokens=5, total_tokens=10),
            http_status=200,
            provider_duration=self.duration,
            connect_time=0.01,
            connection_reused=True,
            finish_reason="stop",
        )


@pytest.mark.asyncio
async def test_attempts_bounded_by_max() -> None:
    clock = FakeClock()
    slept = []

    async def fake_sleep(d: float) -> None:
        slept.append(d)
        clock.advance(d)

    # Always failing with 500
    mock_p = MockProvider(clock, [errs.ServerError("500") for _ in range(10)], duration_per_call=0.02)
    settings = Settings(max_attempts=3)
    pool = ClientPool(settings)

    policy = RetryPolicy(
        max_attempts=3,
        initial_backoff=0.1,
        sleep=fake_sleep,
        clock=clock,
    )
    pipeline = Pipeline(
        settings,
        pool,
        retry_policy=policy,
        provider_factory=lambda settings, client=None: mock_p,
        clock=clock,
    )

    with pytest.raises(errs.ServerError):
        await pipeline.handle(ChatRequest(prompt="hi"), request_id="req-1")

    # Bounded by max_attempts = 3
    assert mock_p.call_count == 3
    assert len(slept) == 2  # slept between attempt 1->2 and 2->3


@pytest.mark.asyncio
async def test_no_retry_on_auth_or_4xx() -> None:
    clock = FakeClock()
    slept = []

    async def fake_sleep(d: float) -> None:
        slept.append(d)

    # 1st call fails with 401 AuthError
    mock_p = MockProvider(clock, [errs.AuthError("invalid key")], duration_per_call=0.01)
    settings = Settings(max_attempts=5)
    pool = ClientPool(settings)

    policy = RetryPolicy(max_attempts=5, sleep=fake_sleep, clock=clock)
    pipeline = Pipeline(
        settings,
        pool,
        retry_policy=policy,
        provider_factory=lambda settings, client=None: mock_p,
        clock=clock,
    )

    with pytest.raises(errs.AuthError):
        await pipeline.handle(ChatRequest(prompt="hi"), request_id="req-auth")

    # Never retried auth
    assert mock_p.call_count == 1
    assert len(slept) == 0


@pytest.mark.asyncio
async def test_budget_exhaustion_stops_retries() -> None:
    clock = FakeClock()
    slept = []

    async def fake_sleep(d: float) -> None:
        slept.append(d)
        clock.advance(d)

    budget = RetryBudget(ratio=0.0, floor=1)  # only 1 retry total allowed
    settings = Settings(max_attempts=3)
    pool = ClientPool(settings)

    # Request 1: fails once, retries and fails -> uses 1 retry budget
    mock_p1 = MockProvider(clock, [errs.ServerError("500"), errs.ServerError("500")])
    pipeline = Pipeline(
        settings,
        pool,
        retry_policy=RetryPolicy(max_attempts=3, budget=budget, sleep=fake_sleep, clock=clock),
        provider_factory=lambda settings, client=None: mock_p1,
        clock=clock,
    )

    with pytest.raises(errs.ServerError):
        await pipeline.handle(ChatRequest(prompt="1"), request_id="req-1")
    assert mock_p1.call_count == 2  # attempt 1 + attempt 2 (retry)

    # Request 2: fails once. Budget is exhausted, so NO retry allowed (call_count == 1)
    mock_p2 = MockProvider(clock, [errs.ServerError("500"), errs.ServerError("500")])
    pipeline._provider_factory = lambda settings, client=None: mock_p2

    with pytest.raises(errs.ServerError):
        await pipeline.handle(ChatRequest(prompt="2"), request_id="req-2")
    assert mock_p2.call_count == 1  # 0 retries made!


@pytest.mark.asyncio
async def test_per_attempt_vs_final_outcome() -> None:
    clock = FakeClock()

    # Request 1: fails with 429 on attempt 1, succeeds on attempt 2
    mock_p = MockProvider(
        clock,
        [errs.RateLimitError("429", retry_after=0.1), "Success response"],
        duration_per_call=0.02,
    )
    settings = Settings(max_attempts=3)
    pool = ClientPool(settings)

    async def instant_sleep(d: float) -> None:
        clock.advance(d)

    pipeline = Pipeline(
        settings,
        pool,
        retry_policy=RetryPolicy(max_attempts=3, sleep=instant_sleep, clock=clock),
        provider_factory=lambda settings, client=None: mock_p,
        clock=clock,
    )

    resp = await pipeline.handle(ChatRequest(prompt="hi"), request_id="req-combo")
    assert resp.text == "Success response"

    # Per-attempt counts: 1 rate_limit, 1 success
    assert pipeline.stats.attempt_counts["rate_limit"] == 1
    assert pipeline.stats.attempt_counts["success"] == 1

    # Final counts: 1 success, 0 failures!
    assert pipeline.stats.final_counts["success"] == 1
    assert pipeline.stats.final_counts.get("rate_limit", 0) == 0


@pytest.mark.asyncio
async def test_timings_sum_invariant_and_clamped_overhead() -> None:
    clock = FakeClock(10.0)

    async def fake_sleep(d: float) -> None:
        clock.advance(d)

    # 1 retry (0.03s), 1 success (0.03s), backoff sleep 0.1s
    mock_p = MockProvider(
        clock,
        [errs.RateLimitError("429", retry_after=0.1), "Done!"],
        duration_per_call=0.03,
    )
    settings = Settings(max_attempts=3)
    pool = ClientPool(settings)

    pipeline = Pipeline(
        settings,
        pool,
        retry_policy=RetryPolicy(max_attempts=3, sleep=fake_sleep, clock=clock),
        provider_factory=lambda settings, client=None: mock_p,
        clock=clock,
    )

    resp = await pipeline.handle(ChatRequest(prompt="hi"), request_id="req-inv")
    t = resp.timings

    # Total must equal the sum of the components + overhead
    expected_sum = (
        t.queue_wait
        + t.client_setup
        + t.provider_time
        + t.backoff_sleep
        + t.response_processing
        + t.overhead
    )
    assert pytest.approx(t.total, abs=1e-6) == expected_sum

    # Overhead must be >= 0.0
    assert t.overhead >= 0.0

    # Clamped count must be 0
    assert pipeline.stats.clamped_overhead_count == 0

    # connect_time is informational subset of provider_time and NOT summed
    assert t.connect_time > 0.0
    assert t.connect_time <= t.provider_time
