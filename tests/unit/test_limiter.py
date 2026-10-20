"""
Unit tests for Concurrency Limiter (core/limiter.py).
No network access required.
"""
from __future__ import annotations

import asyncio
import pytest

from gateway.core.limiter import Limiter, QueueFullError, QueueTimeoutError


@pytest.mark.asyncio
async def test_never_exceeds_limit_with_100_concurrent_tasks() -> None:
    limit = 10
    limiter = Limiter(limit=limit, queue_size=200, queue_wait_timeout=5.0)

    max_seen_in_flight = 0
    max_seen_slots_held = 0
    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal max_seen_in_flight, max_seen_slots_held
        async with limiter.slot() as wait_time:
            async with lock:
                if limiter.slots_held > max_seen_slots_held:
                    max_seen_slots_held = limiter.slots_held

            async with limiter.calling_provider():
                async with lock:
                    if limiter.in_flight > max_seen_in_flight:
                        max_seen_in_flight = limiter.in_flight
                await asyncio.sleep(0.005)

    tasks = [asyncio.create_task(worker()) for _ in range(100)]
    await asyncio.gather(*tasks)

    assert max_seen_in_flight <= limit
    assert max_seen_slots_held <= limit
    assert limiter.slots_held == 0
    assert limiter.in_flight == 0
    assert limiter.waiting == 0


@pytest.mark.asyncio
async def test_queue_full_reject() -> None:
    limiter = Limiter(limit=2, queue_size=3, queue_wait_timeout=5.0, retry_after=2.0)
    release_event = asyncio.Event()

    async def slot_holder() -> None:
        async with limiter.slot():
            await release_event.wait()

    async def queued_worker() -> None:
        async with limiter.slot():
            pass

    # Occupy the 2 slots
    holders = [asyncio.create_task(slot_holder()) for _ in range(2)]
    await asyncio.sleep(0.01)
    assert limiter.slots_held == 2

    # Queue 3 tasks
    waiters = [asyncio.create_task(queued_worker()) for _ in range(3)]
    await asyncio.sleep(0.01)
    assert limiter.waiting == 3

    # 6th task must be rejected immediately with QueueFullError
    with pytest.raises(QueueFullError) as exc_info:
        async with limiter.slot():
            pass

    assert exc_info.value.reason == "queue_full"
    assert exc_info.value.status_code == 503
    assert exc_info.value.retry_after == 2.0
    assert exc_info.value.retryable is True

    # Cleanup
    release_event.set()
    await asyncio.gather(*holders, *waiters)
    assert limiter.slots_held == 0
    assert limiter.waiting == 0


@pytest.mark.asyncio
async def test_queue_timeout_reject() -> None:
    limiter = Limiter(limit=1, queue_size=10, queue_wait_timeout=0.05, retry_after=1.5)
    release_event = asyncio.Event()

    async def slot_holder() -> None:
        async with limiter.slot():
            await release_event.wait()

    holder = asyncio.create_task(slot_holder())
    await asyncio.sleep(0.01)
    assert limiter.slots_held == 1

    # Task waiting in queue should time out after 0.05s
    with pytest.raises(QueueTimeoutError) as exc_info:
        async with limiter.slot():
            pass

    assert exc_info.value.reason == "queue_timeout"
    assert exc_info.value.status_code == 503
    assert exc_info.value.retry_after == 1.5
    assert exc_info.value.retryable is True
    assert limiter.waiting == 0

    release_event.set()
    await holder
    assert limiter.slots_held == 0


@pytest.mark.asyncio
async def test_cancel_while_queued_frees_resources() -> None:
    limiter = Limiter(limit=1, queue_size=5, queue_wait_timeout=5.0)
    release_event = asyncio.Event()

    async def slot_holder() -> None:
        async with limiter.slot():
            await release_event.wait()

    holder = asyncio.create_task(slot_holder())
    await asyncio.sleep(0.01)
    assert limiter.slots_held == 1

    async def queued_task() -> None:
        async with limiter.slot():
            pass

    waiter = asyncio.create_task(queued_task())
    await asyncio.sleep(0.01)
    assert limiter.waiting == 1

    # Cancel while waiting in queue
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    # Must have freed its queue position
    assert limiter.waiting == 0

    release_event.set()
    await holder
    assert limiter.slots_held == 0


@pytest.mark.asyncio
async def test_cancel_in_flight_frees_resources() -> None:
    limiter = Limiter(limit=2, queue_size=5, queue_wait_timeout=5.0)
    started_event = asyncio.Event()
    hang_event = asyncio.Event()

    async def running_task() -> None:
        async with limiter.slot():
            async with limiter.calling_provider():
                started_event.set()
                await hang_event.wait()

    task = asyncio.create_task(running_task())
    await started_event.wait()

    assert limiter.slots_held == 1
    assert limiter.in_flight == 1

    # Cancel in-flight task
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Slot and in_flight must be freed
    assert limiter.slots_held == 0
    assert limiter.in_flight == 0
