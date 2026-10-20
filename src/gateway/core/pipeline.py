"""
Request processing pipeline.

Rules (from CLAUDE.md & requirements):
- Resolves provider via registry (core never imports concrete provider).
- Limiter slot acquisition -> client setup -> attempt loop.
- Records AttemptRecord per attempt (outcome kind, status, duration, will_retry).
- Final outcome separate from attempt failures.
- Slot is HELD during backoff (throttles retry traffic under stress).
- Timings: queue_wait, client_setup, provider_time, backoff_sleep,
  overhead = total - those four.
- connect_time = informational subset of provider_time, NOT summed.
- Count how often overhead was clamped; must be 0.
"""
from __future__ import annotations

import collections
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, Protocol

import asyncio
import httpx
from httpx import AsyncClient

from gateway import errors as errs
from gateway.config import Settings
from gateway.core.client_pool import ClientPool
from gateway.core.limiter import Limiter, QueueFullError, QueueTimeoutError
from gateway.core.retry import RetryBudget, RetryPolicy
from gateway.core.timing import RequestTimer, get_clamped_overhead_count
from gateway.metrics.collector import MetricsCollector
from gateway.providers.base import Provider, ProviderResult
from gateway.providers.registry import build_provider
from gateway.schemas import ChatRequest, ChatResponse, RequestTimings


class ProviderFactory(Protocol):
    def __call__(
        self,
        settings: Settings,
        /,
        *,
        client: AsyncClient | None = None,
    ) -> Provider:
        ...


@dataclass
class AttemptRecord:
    """Record of a single provider attempt within a request."""

    attempt_number: int
    outcome_kind: str
    status: int
    duration: float
    will_retry: bool
    error_message: Optional[str] = None


@dataclass
class PipelineStats:
    """Tracks per-attempt failure/success counts vs final outcomes."""

    attempt_counts: dict[str, int] = field(default_factory=lambda: collections.defaultdict(int))
    final_counts: dict[str, int] = field(default_factory=lambda: collections.defaultdict(int))
    clamped_overhead_count: int = 0
    total_requests: int = 0


class Pipeline:
    """
    Core pipeline orchestrating request flow to providers, retry loops,
    concurrency slot management, and timing breakdown.
    """

    def __init__(
        self,
        settings: Settings,
        client_pool: ClientPool,
        *,
        limiter: Optional[Limiter] = None,
        retry_policy: Optional[RetryPolicy] = None,
        provider_factory: Optional[ProviderFactory] = None,
        clock: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        collector: Optional[MetricsCollector] = None,
    ) -> None:
        self.settings = settings
        self.client_pool = client_pool
        self._provider_factory: ProviderFactory = provider_factory or build_provider
        self.clock: Callable[[], float] = clock or time.perf_counter
        self.collector: MetricsCollector = collector or MetricsCollector()

        self.limiter = limiter or Limiter(
            limit=settings.concurrency_limit,
            queue_size=settings.queue_size,
            queue_wait_timeout=settings.queue_wait_timeout,
            retry_after=settings.limiter_retry_after,
        )

        self.retry_policy = retry_policy or RetryPolicy(
            max_attempts=settings.max_attempts,
            initial_backoff=settings.initial_backoff,
            max_backoff=settings.max_backoff,
            backoff_factor=settings.backoff_factor,
            retry_after_cap=settings.retry_after_cap,
            total_deadline=settings.total_deadline,
            budget=RetryBudget(
                ratio=settings.retry_budget_ratio,
                floor=settings.retry_budget_floor,
            ),
            sleep=sleep,
            clock=self.clock,
        )

        self.stats = PipelineStats()
        self.recent_attempts: list[AttemptRecord] = []

    def get_stats(self) -> dict[str, Any]:
        """Return diagnostic counters for attempts, finals, and limiter state."""
        return {
            "attempt_counts": dict(self.stats.attempt_counts),
            "final_counts": dict(self.stats.final_counts),
            "clamped_overhead_count": get_clamped_overhead_count(),
            "total_requests": self.stats.total_requests,
            "limiter": {
                "waiting": self.limiter.waiting,
                "slots_held": self.limiter.slots_held,
                "in_flight": self.limiter.in_flight,
                "peak_waiting": self.limiter.peak_waiting,
                "peak_slots_held": self.limiter.peak_slots_held,
                "peak_in_flight": self.limiter.peak_in_flight,
            },
            "metrics": self.collector.get_metrics(self.limiter),
        }

    async def handle(self, request: ChatRequest, *, request_id: str) -> ChatResponse:
        """
        Execute a completion request through the limiter and retry-enabled attempt loop.
        Records into self.collector once per finished request (also on cancel/reject).
        """
        self.stats.total_requests += 1
        self.retry_policy.budget.record_request()

        t_start = self.clock()
        timer = RequestTimer(clock=self.clock, start_time=t_start)
        recorded = False
        attempts_for_req: list[AttemptRecord] = []
        t_provider_return: Optional[float] = None

        # Build settings with request-level provider override if requested
        if request.provider:
            req_settings = self.settings.model_copy(update={"provider_name": request.provider})
        else:
            req_settings = self.settings

        try:
            # 1. Acquire concurrency slot
            async with self.limiter.slot() as wait_time:
                timer.queue_wait = wait_time

                # 2. Client setup (client acquisition, own bucket)
                t_c = self.clock()
                async with self.client_pool.get_client() as client:
                    timer.client_setup = self.clock() - t_c
                    provider = self._provider_factory(req_settings, client=client)

                    # 3. Attempt loop
                    effective_deadline = self.retry_policy.total_deadline
                    if request.timeout is not None:
                        effective_deadline = min(effective_deadline, request.timeout)

                    result: Optional[ProviderResult] = None
                    final_outcome = "unknown"

                    for attempt_idx in range(1, self.retry_policy.max_attempts + 1):
                        # Provider call: in_flight tracked only while call is active
                        t_p = self.clock()
                        try:
                            async with self.limiter.calling_provider():
                                result = await provider.complete(request, timeout=request.timeout)

                            t_provider_return = self.clock()
                            call_duration = t_provider_return - t_p
                            timer.provider_time += call_duration
                            timer.connect_time += getattr(result, "connect_time", 0.0)

                            self.collector.record_attempt("success")
                            self.stats.attempt_counts["success"] += 1
                            attempt_rec = AttemptRecord(
                                attempt_number=attempt_idx,
                                outcome_kind="success",
                                status=result.http_status,
                                duration=call_duration,
                                will_retry=False,
                            )
                            attempts_for_req.append(attempt_rec)
                            self.recent_attempts.append(attempt_rec)
                            final_outcome = "success"
                            break
                        except Exception as exc:
                            t_fail = self.clock()
                            call_duration = t_fail - t_p
                            timer.provider_time += call_duration

                            if isinstance(exc, errs.ProviderError):
                                outcome_kind = exc.kind
                                status = exc.status
                            else:
                                outcome_kind = "unknown"
                                status = 500

                            self.collector.record_attempt(outcome_kind)
                            self.stats.attempt_counts[outcome_kind] += 1

                            # Evaluate retry eligibility
                            can_retry_err = self.retry_policy.is_retryable(exc)
                            has_remaining = attempt_idx < self.retry_policy.max_attempts
                            delay = (
                                self.retry_policy.calculate_backoff(attempt_idx, exc)
                                if can_retry_err
                                else 0.0
                            )
                            elapsed = self.clock() - timer.start_time
                            within_deadline = not self.retry_policy.would_exceed_deadline(
                                elapsed=elapsed,
                                delay=delay,
                                deadline_override=effective_deadline,
                            )
                            budget_ok = (
                                self.retry_policy.budget.can_retry()
                                if (can_retry_err and has_remaining and within_deadline)
                                else False
                            )

                            will_retry = can_retry_err and has_remaining and within_deadline and budget_ok

                            attempt_rec = AttemptRecord(
                                attempt_number=attempt_idx,
                                outcome_kind=outcome_kind,
                                status=status,
                                duration=call_duration,
                                will_retry=will_retry,
                                error_message=str(exc),
                            )
                            attempts_for_req.append(attempt_rec)
                            self.recent_attempts.append(attempt_rec)

                            if not will_retry:
                                final_outcome = outcome_kind
                                self.stats.final_counts[final_outcome] += 1
                                raise exc

                            # Slot is HELD during backoff unless release_slot_during_backoff is True
                            if self.settings.release_slot_during_backoff:
                                self.limiter._slots_held -= 1
                                self.limiter._semaphore.release()
                            t_b = self.clock()
                            try:
                                await self.retry_policy.sleep(delay)
                            finally:
                                t_sleep_done = self.clock()
                                timer.backoff_sleep += (t_sleep_done - t_b)
                                if self.settings.release_slot_during_backoff:
                                    await self.limiter._semaphore.acquire()
                                    self.limiter._slots_held += 1
                                    timer.queue_wait += (self.clock() - t_sleep_done)

            # Loop completed successfully
            self.stats.final_counts[final_outcome] += 1

            assert result is not None
            # Build ChatResponse (measure response_processing)
            chat_response = ChatResponse(
                text=result.text,
                provider=result.provider,
                model=result.model,
                usage=result.usage,
                request_id=request_id,
                timings=RequestTimings(),
            )
            t_built = self.clock()
            if t_provider_return is not None:
                timer.response_processing = max(0.0, t_built - t_provider_return)

            timings = timer.finish()
            chat_response.timings = timings
            self.stats.clamped_overhead_count = get_clamped_overhead_count()

            self.collector.record_request(
                request_id=request_id,
                success=True,
                final_outcome="success",
                total_duration=timings.total,
                start_time=t_start,
                end_time=t_built,
                attempts=len(attempts_for_req),
                timings=timings,
            )
            recorded = True
            return chat_response

        except (QueueFullError, QueueTimeoutError) as exc:
            if not recorded:
                self.collector.record_reject(
                    exc.reason,
                    request_id=request_id,
                    duration=self.clock() - t_start,
                    start_time=t_start,
                    end_time=self.clock(),
                )
                recorded = True
            raise

        except asyncio.CancelledError:
            if not recorded:
                self.collector.record_cancelled(
                    request_id=request_id,
                    duration=self.clock() - t_start,
                    start_time=t_start,
                    end_time=self.clock(),
                    attempts=len(attempts_for_req),
                )
                recorded = True
            raise

        except Exception as exc:
            if not recorded:
                outcome_kind = exc.kind if isinstance(exc, errs.ProviderError) else "unknown"
                self.collector.record_request(
                    request_id=request_id,
                    success=False,
                    final_outcome=outcome_kind,
                    total_duration=self.clock() - t_start,
                    start_time=t_start,
                    end_time=self.clock(),
                    attempts=len(attempts_for_req),
                    timings=None,
                )
                recorded = True
            raise
