"""
Unit tests for RetryPolicy and RetryBudget (core/retry.py).
Deterministic: injected sleep, rng, and clock.
No network access required.
"""
from __future__ import annotations

import random
import pytest

from gateway import errors as errs
from gateway.core.retry import RetryBudget, RetryPolicy


def test_retryability() -> None:
    policy = RetryPolicy()

    # Retryable: timeout, connection, 429, 5xx
    assert policy.is_retryable(errs.TimeoutError("timeout")) is True
    assert policy.is_retryable(errs.ConnectionError("conn")) is True
    assert policy.is_retryable(errs.RateLimitError("429")) is True
    assert policy.is_retryable(errs.ServerError("500")) is True

    # NEVER retryable: auth (401), client (4xx)
    assert policy.is_retryable(errs.AuthError("401")) is False
    assert policy.is_retryable(errs.ClientError("400")) is False
    assert policy.is_retryable(ValueError("arbitrary error")) is False


def test_backoff_growth_and_jitter_bounds_with_seeded_rng() -> None:
    # Deterministic seeded rng
    rng = random.Random(42)
    policy = RetryPolicy(
        initial_backoff=0.5,
        max_backoff=4.0,
        backoff_factor=2.0,
        rng=rng,
    )

    # For attempt 1: upper bound = min(4.0, 0.5 * 2^0) = 0.5
    for _ in range(20):
        d1 = policy.calculate_backoff(attempt_number=1)
        assert 0.0 <= d1 <= 0.5

    # For attempt 2: upper bound = min(4.0, 0.5 * 2^1) = 1.0
    for _ in range(20):
        d2 = policy.calculate_backoff(attempt_number=2)
        assert 0.0 <= d2 <= 1.0

    # For attempt 3: upper bound = min(4.0, 0.5 * 2^2) = 2.0
    for _ in range(20):
        d3 = policy.calculate_backoff(attempt_number=3)
        assert 0.0 <= d3 <= 2.0

    # For attempt 4: upper bound = min(4.0, 0.5 * 2^3) = 4.0 (capped at max_backoff)
    for _ in range(20):
        d4 = policy.calculate_backoff(attempt_number=4)
        assert 0.0 <= d4 <= 4.0

    # For attempt 10: still capped at 4.0
    for _ in range(20):
        d10 = policy.calculate_backoff(attempt_number=10)
        assert 0.0 <= d10 <= 4.0


def test_retry_after_honored_and_capped() -> None:
    policy = RetryPolicy(
        initial_backoff=0.5,
        retry_after_cap=10.0,
    )

    # Retry-After under cap: honored exactly
    err_5s = errs.RateLimitError("429", retry_after=5.0)
    assert policy.calculate_backoff(1, err_5s) == 5.0

    # Retry-After over cap: capped at retry_after_cap
    err_60s = errs.RateLimitError("429", retry_after=60.0)
    assert policy.calculate_backoff(1, err_60s) == 10.0


def test_budget_exhaustion_stops_retries() -> None:
    # Budget: ratio=0.1, floor=2
    # At 0 requests: max allowed retries = int(0 * 0.1) + 2 = 2
    budget = RetryBudget(ratio=0.1, floor=2)
    policy = RetryPolicy(budget=budget)

    # 1st retry allowed
    assert budget.can_retry() is True
    # 2nd retry allowed
    assert budget.can_retry() is True
    # 3rd retry disallowed (budget exhausted!)
    assert budget.can_retry() is False

    # New top-level requests increment total_requests
    for _ in range(10):
        budget.record_request()
    # Now total_requests = 10 -> max allowed = int(10 * 0.1) + 2 = 3
    # We used 2 retries, so 1 more is allowed:
    assert budget.can_retry() is True
    # And then exhausted again:
    assert budget.can_retry() is False


def test_would_exceed_deadline() -> None:
    policy = RetryPolicy(total_deadline=10.0)

    # elapsed 5.0 + delay 4.0 = 9.0 < 10.0 -> OK
    assert policy.would_exceed_deadline(elapsed=5.0, delay=4.0) is False

    # elapsed 8.0 + delay 3.0 = 11.0 >= 10.0 -> Exceeds!
    assert policy.would_exceed_deadline(elapsed=8.0, delay=3.0) is True

    # Per-request deadline override
    assert policy.would_exceed_deadline(elapsed=2.0, delay=2.0, deadline_override=3.0) is True
