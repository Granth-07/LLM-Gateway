"""
Concurrency Limiter with bounded wait-queue and slot tracking.

Rules (from CLAUDE.md & requirements):
- asyncio.Semaphore(limit) + bounded waiter count (queue_size).
- Waiters >= queue_size => immediate reject (503 + Retry-After, reason="queue_full").
- Wait > queue_wait_timeout => reject (503 + Retry-After, reason="queue_timeout").
- Track separately: waiting, slots_held, in_flight.
  (in_flight = only while a provider call is active, not during backoff).
- Release in finally; cancellation while queued frees its queue position;
  never swallow CancelledError.
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional


class LimiterError(Exception):
    """Base error for limiter rejections (mapped to HTTP 503)."""

    status_code: int = 503
    retryable: bool = True
    reason: str = "limiter_error"

    def __init__(
        self,
        message: str,
        *,
        retry_after: float = 1.0,
        reason: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.retry_after = retry_after
        if reason:
            self.reason = reason


class QueueFullError(LimiterError):
    """Waiters >= queue_size; request rejected immediately."""

    reason = "queue_full"


class QueueTimeoutError(LimiterError):
    """Wait time in queue exceeded queue_wait_timeout."""

    reason = "queue_timeout"


class Limiter:
    """
    Slot manager enforcing concurrency limits and queue bounds.
    """

    def __init__(
        self,
        limit: int,
        queue_size: int = 200,
        queue_wait_timeout: float = 10.0,
        retry_after: float = 1.0,
    ) -> None:
        if limit <= 0:
            raise ValueError(f"Limit must be > 0, got {limit}")
        self.limit = limit
        self.queue_size = queue_size
        self.queue_wait_timeout = queue_wait_timeout
        self.retry_after = retry_after

        self._semaphore = asyncio.Semaphore(limit)
        self._waiting = 0
        self._slots_held = 0
        self._in_flight = 0
        self._peak_waiting = 0
        self._peak_slots_held = 0
        self._peak_in_flight = 0

    @property
    def waiting(self) -> int:
        """Tasks currently waiting in the queue to acquire a slot."""
        return self._waiting

    @property
    def slots_held(self) -> int:
        """Concurrency slots currently acquired and held."""
        return self._slots_held

    @property
    def in_flight(self) -> int:
        """Requests currently making active upstream calls (not sleeping in backoff)."""
        return self._in_flight

    @property
    def peak_waiting(self) -> int:
        """Peak number of tasks waiting in queue."""
        return self._peak_waiting

    @property
    def peak_slots_held(self) -> int:
        """Peak number of concurrency slots held simultaneously."""
        return self._peak_slots_held

    @property
    def peak_in_flight(self) -> int:
        """Peak number of concurrent upstream provider calls."""
        return self._peak_in_flight

    def reset_peaks(self) -> None:
        """Reset peak gauges to current values."""
        self._peak_waiting = self._waiting
        self._peak_slots_held = self._slots_held
        self._peak_in_flight = self._in_flight

    def _inc_waiting(self) -> None:
        self._waiting += 1
        if self._waiting > self._peak_waiting:
            self._peak_waiting = self._waiting

    def _inc_slots_held(self) -> None:
        self._slots_held += 1
        if self._slots_held > self._peak_slots_held:
            self._peak_slots_held = self._slots_held

    def _inc_in_flight(self) -> None:
        self._in_flight += 1
        if self._in_flight > self._peak_in_flight:
            self._peak_in_flight = self._in_flight

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[float]:
        """
        Acquire a concurrency slot, bounded by limit and queue_size.
        Yields the seconds spent waiting in queue (0.0 if acquired immediately).

        Raises:
            QueueFullError: When waiting count >= queue_size.
            QueueTimeoutError: When queued wait exceeds queue_wait_timeout.
            asyncio.CancelledError: If cancelled while waiting or holding slot.
        """
        wait_time = 0.0
        if self._semaphore._value > 0 and self._waiting == 0:
            await self._semaphore.acquire()
            self._inc_slots_held()
        else:
            if self._waiting >= self.queue_size:
                raise QueueFullError(
                    f"Concurrency queue full ({self._waiting}/{self.queue_size})",
                    retry_after=self.retry_after,
                    reason="queue_full",
                )

            self._inc_waiting()
            t_wait_start = time.perf_counter()
            try:
                if self.queue_wait_timeout is not None and self.queue_wait_timeout >= 0:
                    try:
                        await asyncio.wait_for(
                            self._semaphore.acquire(),
                            timeout=self.queue_wait_timeout,
                        )
                    except asyncio.TimeoutError:
                        raise QueueTimeoutError(
                            f"Queue wait timeout exceeded ({self.queue_wait_timeout}s)",
                            retry_after=self.retry_after,
                            reason="queue_timeout",
                        )
                else:
                    await self._semaphore.acquire()
                wait_time = time.perf_counter() - t_wait_start
                self._inc_slots_held()
            finally:
                self._waiting -= 1

        try:
            yield wait_time
        finally:
            self._slots_held -= 1
            self._semaphore.release()

    @asynccontextmanager
    async def calling_provider(self) -> AsyncIterator[None]:
        """
        Context manager wrapping the active provider call.
        Increments in_flight only during the provider call, not during backoff sleep.
        """
        self._inc_in_flight()
        try:
            yield
        finally:
            self._in_flight -= 1
