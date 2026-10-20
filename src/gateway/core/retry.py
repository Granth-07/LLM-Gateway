"""
Retry policy and global retry budget management.

Rules (from CLAUDE.md & requirements):
- Retry only timeout/connection/429/5xx per error.retryable; never auth/4xx.
- max_attempts, exponential backoff with FULL jitter (cap max_backoff).
- Honor Retry-After capped by retry_after_cap.
- Skip retry if it would exceed the request's total deadline (total_deadline setting).
- Global retry budget: retries <= ratio * requests + small floor, shared across requests.
- Inject sleep, rng, clock so tests are deterministic.
- Slot is HELD during backoff (document why: throttles retry traffic).
"""
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Awaitable, Callable, Optional

from gateway import errors as errs


class RetryBudget:
    """
    Global retry budget shared across requests.

    Prevents retry amplification storms under heavy load.
    Enforces invariant: retries <= (ratio * requests) + floor.
    """

    def __init__(self, ratio: float = 0.2, floor: int = 10) -> None:
        self.ratio = ratio
        self.floor = floor
        self.total_requests = 0
        self.total_retries = 0

    def record_request(self) -> None:
        """Record the arrival of a new top-level request."""
        self.total_requests += 1

    def can_retry(self) -> bool:
        """
        Check if global budget permits an additional retry attempt.
        Consumes one retry unit if permitted.
        """
        max_allowed = int(self.total_requests * self.ratio) + self.floor
        if self.total_retries < max_allowed:
            self.total_retries += 1
            return True
        return False

    def reset(self) -> None:
        """Reset budget counters."""
        self.total_requests = 0
        self.total_retries = 0


class RetryPolicy:
    """
    Encapsulates retry decision logic, exponential backoff with full jitter,
    deadline checking, and injected dependencies for deterministic testing.

    Note on concurrency slots:
    The concurrency slot is intentionally HELD during backoff sleeps.
    Why: Holding the slot during backoff naturally throttles inbound traffic and
    prevents overloading the upstream provider further when it is already degraded or
    rate-limited. Releasing the slot during backoff would allow new requests to enter,
    multiplying traffic rather than backing off.
    """

    def __init__(
        self,
        *,
        max_attempts: int = 3,
        initial_backoff: float = 0.5,
        max_backoff: float = 10.0,
        backoff_factor: float = 2.0,
        retry_after_cap: float = 30.0,
        total_deadline: float = 60.0,
        budget: Optional[RetryBudget] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        rng: Optional[random.Random] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.max_attempts = max_attempts
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor
        self.retry_after_cap = retry_after_cap
        self.total_deadline = total_deadline
        self.budget = budget if budget is not None else RetryBudget()
        self._sleep = sleep or asyncio.sleep
        self.rng = rng if rng is not None else random.Random()
        self.clock = clock or time.perf_counter

    def is_retryable(self, exc: Exception) -> bool:
        """
        Check if an exception is eligible for retry.
        Retry ONLY timeout/connection/429/5xx per error.retryable.
        NEVER retry auth (401) or 4xx client errors.
        """
        if not isinstance(exc, errs.ProviderError):
            return False
        if exc.kind in ("auth", "client"):
            return False
        return bool(exc.retryable)

    def calculate_backoff(self, attempt_number: int, exc: Optional[Exception] = None) -> float:
        """
        Calculate backoff delay in seconds.

        If the error specifies Retry-After, that value is honored, capped by retry_after_cap.
        Otherwise, exponential backoff with FULL jitter is applied:
        delay = uniform(0, min(max_backoff, initial_backoff * (backoff_factor ** (attempt - 1)))).
        """
        if isinstance(exc, errs.ProviderError) and exc.retry_after is not None:
            return min(float(exc.retry_after), self.retry_after_cap)

        exp_bound = self.initial_backoff * (self.backoff_factor ** max(0, attempt_number - 1))
        upper = min(self.max_backoff, exp_bound)
        return self.rng.uniform(0.0, upper)

    def would_exceed_deadline(
        self,
        elapsed: float,
        delay: float,
        deadline_override: Optional[float] = None,
    ) -> bool:
        """Check if backoff delay would push total elapsed time past the deadline."""
        effective_deadline = (
            min(self.total_deadline, deadline_override)
            if deadline_override is not None
            else self.total_deadline
        )
        return (elapsed + delay) >= effective_deadline

    async def sleep(self, delay: float) -> None:
        """Sleep for backoff delay using the injected sleep function."""
        if delay > 0:
            await self._sleep(delay)
