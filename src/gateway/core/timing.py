"""
Request timing measurement using time.perf_counter() exclusively.

Per CLAUDE.md and specifications:
- total = queue_wait + client_setup + provider_time + backoff_sleep + overhead
- overhead = total - those four
- Intervals must not overlap
- connect_time = informational subset of provider_time, NOT summed
- Count how often overhead was clamped; must be 0.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from gateway.schemas import RequestTimings

# Global counter for how often overhead had to be clamped (< 0.0 before clamping)
_clamped_overhead_count: int = 0


def get_clamped_overhead_count() -> int:
    """Return the total number of times overhead was clamped to 0.0."""
    return _clamped_overhead_count


def reset_clamped_overhead_count() -> None:
    """Reset the clamped overhead counter to zero."""
    global _clamped_overhead_count
    _clamped_overhead_count = 0


def compute_timings(
    *,
    total: float,
    queue_wait: float = 0.0,
    client_setup: float = 0.0,
    provider_time: float = 0.0,
    backoff_sleep: float = 0.0,
    connect_time: float = 0.0,
    response_processing: float = 0.0,
) -> RequestTimings:
    """
    Compute RequestTimings ensuring overhead = total - non-overhead parts in one place.

    overhead = total - queue_wait - client_setup - provider_time - backoff_sleep - response_processing.
    connect_time is an informational subset of provider_time and is NOT summed.
    Overhead is clamped to >= 0.0; any clamping increments _clamped_overhead_count.
    """
    global _clamped_overhead_count
    sum_parts = queue_wait + client_setup + provider_time + backoff_sleep + response_processing
    raw_overhead = total - sum_parts
    if raw_overhead < 0.0:
        _clamped_overhead_count += 1
        overhead = 0.0
    else:
        overhead = raw_overhead

    return RequestTimings(
        queue_wait=queue_wait,
        client_setup=client_setup,
        provider_time=provider_time,
        provider_duration=provider_time,
        backoff_sleep=backoff_sleep,
        response_processing=response_processing,
        overhead=overhead,
        total=total,
        connect_time=connect_time,
    )


@dataclass
class RequestTimer:
    """
    Tracks elapsed intervals for a request using time.perf_counter() or an injected clock.
    """

    clock: Callable[[], float] = field(default=time.perf_counter)
    start_time: float = field(default=0.0)
    queue_wait: float = 0.0
    client_setup: float = 0.0
    provider_time: float = 0.0
    backoff_sleep: float = 0.0
    connect_time: float = 0.0
    response_processing: float = 0.0

    def __post_init__(self) -> None:
        if self.start_time == 0.0:
            self.start_time = self.clock()

    def finish(self) -> RequestTimings:
        """Finish timing and return a populated RequestTimings model."""
        now = self.clock()
        total = max(0.0, now - self.start_time)
        return compute_timings(
            total=total,
            queue_wait=self.queue_wait,
            client_setup=self.client_setup,
            provider_time=self.provider_time,
            backoff_sleep=self.backoff_sleep,
            connect_time=self.connect_time,
            response_processing=self.response_processing,
        )
