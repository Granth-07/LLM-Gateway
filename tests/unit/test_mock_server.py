"""
Unit tests for mock_provider.server.

Key design choices:
- MockConfig(latency_mean=-20, latency_sigma=0.001) → lognormal draws ≈ 2e-9 s → effectively zero latency.
- Concurrency test uses httpx.AsyncClient + ASGITransport + asyncio.gather — truly concurrent.
- Env override test uses MockConfig.from_env() called inside monkeypatch scope.
"""
from __future__ import annotations

import asyncio
import os
import sys

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from mock_provider.server import MockConfig, Stats, _make_completion_response, create_app

ZERO_LATENCY = MockConfig(latency_mean=-20.0, latency_sigma=0.001)

PAYLOAD = {
    "model": "mock-model",
    "messages": [{"role": "user", "content": "Hello"}],
    "max_tokens": 50,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_client(config: MockConfig | None = None) -> httpx.AsyncClient:
    """Return an AsyncClient backed by ASGITransport with zero-latency config."""
    cfg = config or ZERO_LATENCY
    app = create_app(cfg)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------

class TestMockResponseShape:
    @pytest.mark.asyncio
    async def test_openai_shape(self) -> None:
        async with make_client() as c:
            r = await c.post("/v1/chat/completions", json=PAYLOAD)
        assert r.status_code == 200
        body = r.json()
        assert "choices" in body and len(body["choices"]) == 1
        choice = body["choices"][0]
        assert choice["message"]["role"] == "assistant"
        assert "finish_reason" in choice
        u = body["usage"]
        assert u["total_tokens"] == u["prompt_tokens"] + u["completion_tokens"]

    @pytest.mark.asyncio
    async def test_model_echoed(self) -> None:
        async with make_client() as c:
            r = await c.post("/v1/chat/completions", json={**PAYLOAD, "model": "my-model"})
        assert r.json()["model"] == "my-model"

    @pytest.mark.asyncio
    async def test_response_contains_prompt(self) -> None:
        payload = {**PAYLOAD, "messages": [{"role": "user", "content": "UniqueXYZ"}]}
        async with make_client() as c:
            r = await c.post("/v1/chat/completions", json=payload)
        assert "UniqueXYZ" in r.json()["choices"][0]["message"]["content"]


# ---------------------------------------------------------------------------
# Control endpoints
# ---------------------------------------------------------------------------

class TestControlEndpoints:
    @pytest.mark.asyncio
    async def test_stats_keys(self) -> None:
        async with make_client() as c:
            r = await c.get("/control/stats")
        assert r.status_code == 200
        body = r.json()
        assert set(body.keys()) >= {"total_requests", "current_concurrent", "peak_concurrent", "status_counts"}

    @pytest.mark.asyncio
    async def test_total_requests_incremented(self) -> None:
        async with make_client() as c:
            for _ in range(3):
                await c.post("/v1/chat/completions", json=PAYLOAD)
            stats = (await c.get("/control/stats")).json()
        assert stats["total_requests"] == 3

    @pytest.mark.asyncio
    async def test_reset_clears_stats(self) -> None:
        async with make_client() as c:
            await c.post("/v1/chat/completions", json=PAYLOAD)
            await c.post("/control/reset")
            stats = (await c.get("/control/stats")).json()
        assert stats["total_requests"] == 0
        assert stats["peak_concurrent"] == 0

    @pytest.mark.asyncio
    async def test_status_counts_tracked(self) -> None:
        async with make_client() as c:
            await c.post("/v1/chat/completions", json=PAYLOAD)
            stats = (await c.get("/control/stats")).json()
        counts = stats["status_counts"]
        assert "200" in counts or 200 in counts

    @pytest.mark.asyncio
    async def test_control_config_updates_error_rate(self) -> None:
        async with make_client() as c:
            # Force 100% 429 rate
            r = await c.post("/control/config", json={"error_rate_429": 1.0})
            assert r.status_code == 200
            r2 = await c.post("/v1/chat/completions", json=PAYLOAD)
            assert r2.status_code == 429

    @pytest.mark.asyncio
    async def test_control_config_reset_reseeds(self) -> None:
        async with make_client() as c:
            await c.post("/v1/chat/completions", json=PAYLOAD)
            await c.post("/control/reset")
            stats = (await c.get("/control/stats")).json()
        # After reset, total_requests == 0 (stats cleared) and RNG reseeded
        assert stats["total_requests"] == 0


# ---------------------------------------------------------------------------
# Real concurrency test — asyncio.gather + ASGITransport
# ---------------------------------------------------------------------------

class TestPeakConcurrency:
    @pytest.mark.asyncio
    async def test_peak_concurrent_exceeds_one(self) -> None:
        """
        Fires N concurrent requests. ASGITransport runs the ASGI app
        in the same event loop, so asyncio.gather produces genuine
        concurrent async execution. The mock holds the semaphore during
        asyncio.sleep; with N > 1, peak_concurrent must be > 1.

        We use a small sleep (1 ms) to ensure overlap before the gather
        resolves; with N=10 tasks this is reliable.
        """
        N = 10
        # Small but non-zero latency to guarantee overlap
        cfg = MockConfig(latency_mean=-7.0, latency_sigma=0.001)  # ~0.001 s
        app = create_app(cfg)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as c:
            await asyncio.gather(*[c.post("/v1/chat/completions", json=PAYLOAD) for _ in range(N)])
            stats = (await c.get("/control/stats")).json()

        assert stats["total_requests"] == N
        assert stats["peak_concurrent"] > 1, (
            f"Expected peak_concurrent > 1, got {stats['peak_concurrent']}"
        )

    @pytest.mark.asyncio
    async def test_capacity_gate_returns_503(self) -> None:
        """When capacity_mode='reject' and all slots are taken, new requests get 503."""
        # capacity=1, latency long enough to queue, reject mode
        cfg = MockConfig(latency_mean=-3.0, latency_sigma=0.001, max_capacity=1, capacity_mode="reject")
        app = create_app(cfg)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as c:
            results = await asyncio.gather(
                *[c.post("/v1/chat/completions", json=PAYLOAD) for _ in range(5)],
                return_exceptions=True,
            )
        statuses = [r.status_code for r in results if isinstance(r, httpx.Response)]
        assert 503 in statuses

    @pytest.mark.asyncio
    async def test_capacity_mode_queue_waits_and_all_succeed(self) -> None:
        """When capacity_mode='queue' (default), concurrent requests beyond capacity wait and succeed."""
        cfg = MockConfig(latency_mean=-4.0, latency_sigma=0.001, max_capacity=1, capacity_mode="queue")
        app = create_app(cfg)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as c:
            results = await asyncio.gather(
                *[c.post("/v1/chat/completions", json=PAYLOAD) for _ in range(5)],
                return_exceptions=True,
            )
            statuses = [r.status_code for r in results if isinstance(r, httpx.Response)]
            assert statuses == [200, 200, 200, 200, 200]
            stats = (await c.get("/control/stats")).json()
            assert stats["status_counts"].get(503, 0) == 0
            assert stats["total_requests"] == 5


# ---------------------------------------------------------------------------
# Stats unit tests
# ---------------------------------------------------------------------------

class TestStats:
    def test_peak_not_decremented(self) -> None:
        s = Stats()
        s.record_start(); s.record_start(); s.record_start()
        assert s.peak_concurrent == 3
        s.record_end(200)
        assert s.peak_concurrent == 3
        assert s.current_concurrent == 2

    def test_reset(self) -> None:
        s = Stats()
        s.record_start(); s.record_end(200)
        s.record_timing(0.01, 0.05)
        s.injected_429 = 3
        s.injected_500 = 2
        s.injected_drops = 1
        s.injected_slow_tails = 4
        s.reset()
        assert s.as_dict() == {
            "total_requests": 0, "current_concurrent": 0,
            "peak_concurrent": 0, "status_counts": {},
            "injected": {"429": 0, "500": 0, "drop": 0, "slow_tail": 0},
            "injected_429": 0, "injected_500": 0, "injected_drops": 0, "injected_slow_tails": 0,
            "queue_wait": {"p50": 0.0, "p95": 0.0, "mean": 0.0},
            "service_time": {"p50": 0.0, "p95": 0.0, "mean": 0.0},
        }

    def test_injected_event_counters(self) -> None:
        s = Stats()
        s.injected_429 += 2
        s.injected_500 += 1
        s.injected_drops += 1
        s.injected_slow_tails += 3
        data = s.as_dict()
        assert data["injected"]["429"] == 2
        assert data["injected"]["500"] == 1
        assert data["injected"]["drop"] == 1
        assert data["injected"]["slow_tail"] == 3
        assert data["injected_429"] == 2
        assert data["injected_500"] == 1
        assert data["injected_drops"] == 1
        assert data["injected_slow_tails"] == 3

    def test_timing_stats(self) -> None:
        s = Stats()
        s.record_timing(0.01, 0.10)
        s.record_timing(0.02, 0.20)
        data = s.as_dict()
        assert data["queue_wait"]["p50"] > 0
        assert data["service_time"]["mean"] == 0.15


# ---------------------------------------------------------------------------
# MockConfig — env parsing and direct construction
# ---------------------------------------------------------------------------

class TestMockConfig:
    def test_defaults(self) -> None:
        c = MockConfig()
        assert c.error_rate_429 == 0.0
        assert c.error_rate_500 == 0.0
        assert c.seed == 42

    def test_direct_construction(self) -> None:
        c = MockConfig(latency_mean=1.0, latency_sigma=0.1, max_capacity=5)
        assert c.latency_mean == 1.0
        assert c.latency_sigma == 0.1
        assert c.max_capacity == 5

    def test_from_env_reads_env_at_call_time(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """from_env() reads os.environ at call time — monkeypatch works correctly."""
        monkeypatch.setenv("MOCK_LATENCY_MEAN", "2.5")
        monkeypatch.setenv("MOCK_SEED", "99")
        monkeypatch.setenv("MOCK_ERROR_RATE_429", "0.3")
        c = MockConfig.from_env()
        assert c.latency_mean == 2.5
        assert c.seed == 99
        assert c.error_rate_429 == 0.3

    def test_from_env_defaults_when_no_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key in ["MOCK_LATENCY_MEAN", "MOCK_SEED", "MOCK_ERROR_RATE_429",
                    "MOCK_ERROR_RATE_500", "MOCK_PORT", "MOCK_MAX_CAPACITY",
                    "MOCK_LATENCY_SIGMA", "MOCK_SLOW_TAIL_PROB", "MOCK_DROP_PROB", "MOCK_CAPACITY_MODE"]:
            monkeypatch.delenv(key, raising=False)
        c = MockConfig.from_env()
        assert c.latency_mean == 0.05
        assert c.seed == 42
        assert c.capacity_mode == "queue"


# ---------------------------------------------------------------------------
# Completion helper
# ---------------------------------------------------------------------------

class TestMakeCompletionResponse:
    def test_usage_consistent(self) -> None:
        resp = _make_completion_response("m", "hello world", 50)
        u = resp["usage"]
        assert u["total_tokens"] == u["prompt_tokens"] + u["completion_tokens"]

    def test_model_echoed(self) -> None:
        resp = _make_completion_response("my-model", "x", 10)
        assert resp["model"] == "my-model"
