"""
Statistical calculations for gateway metrics.

Rules (from CLAUDE.md & requirements):
- ONE nearest-rank percentile function (p50/p95/p99), implemented once.
- Throughput: completed final-successes / (last_end - first_start), perf_counter only.
- Error rates for attempts and final outcomes.
"""
from __future__ import annotations

import math
from typing import Sequence


def nearest_rank_percentile(values: Sequence[float], p: float) -> float:
    """
    Compute the p-th percentile of values using the nearest-rank method.

    Definition:
      Given sorted values of size N and percentile p in (0, 100]:
      rank k = ceil((p / 100) * N)
      0-indexed position = k - 1

    If values is empty, returns 0.0.
    For p <= 0, returns the minimum value.
    For p >= 100, returns the maximum value.
    """
    if not values:
        return 0.0

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    if p <= 0.0:
        return float(sorted_vals[0])
    if p >= 100.0:
        return float(sorted_vals[-1])

    rank = math.ceil((p / 100.0) * n)
    idx = max(0, min(rank - 1, n - 1))
    return float(sorted_vals[idx])


def compute_percentiles(
    values: Sequence[float],
    percentiles: tuple[float, ...] = (50.0, 95.0, 99.0),
) -> dict[str, float]:
    """
    Compute p50, p95, p99 (or custom percentiles) using the nearest_rank_percentile function.
    Returns dictionary with keys such as 'p50', 'p95', 'p99'.
    """
    result: dict[str, float] = {}
    for p in percentiles:
        key = f"p{int(p) if p.is_integer() else p}"
        val = nearest_rank_percentile(values, p)
        result[key] = round(val, 6)
    return result


def compute_throughput(
    completed_successes: int,
    first_start: float,
    last_end: float,
) -> float:
    """
    Calculate throughput in requests per second.

    completed final-successes / (last_end - first_start), using perf_counter timestamps.
    Returns 0.0 if completed_successes is 0 or duration is <= 0.
    """
    if completed_successes <= 0:
        return 0.0
    duration = last_end - first_start
    if duration <= 0.0:
        return 0.0
    return float(completed_successes / duration)


def compute_error_rate(errors: int, total: int) -> float:
    """
    Calculate error rate as a fraction between 0.0 and 1.0.
    Returns 0.0 if total is 0.
    """
    if total <= 0:
        return 0.0
    return float(errors / total)
