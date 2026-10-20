"""
Standalone mock provider — speaks OpenAI-compatible /v1/chat/completions.

Run with:
    uvicorn mock_provider.server:app --port 9000
or:
    python -m mock_provider.server --port 9000

Capacity modes:
- "queue" (default): requests beyond max_capacity wait on internal semaphore, latency rises.
- "reject": requests arriving when max_capacity slots are filled immediately receive 503 Overloaded.

All configurable parameters can also be set via env vars; see MockConfig.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse


# ---------------------------------------------------------------------------
# Configuration dataclass — plain values, no os.getenv at class body
# ---------------------------------------------------------------------------

@dataclass
class MockConfig:
    port: int = 9000
    latency_mean: float = 0.05   # lognormal log-space mean
    latency_sigma: float = 0.3   # lognormal log-space sigma
    max_capacity: int = 20
    capacity_mode: str = "queue"  # "queue" (default) or "reject"
    error_rate_429: float = 0.0
    error_rate_500: float = 0.0
    slow_tail_prob: float = 0.0
    slow_tail_latency_s: float = 0.0
    drop_prob: float = 0.0
    seed: int = 42

    @classmethod
    def from_env(cls) -> "MockConfig":
        """Build a MockConfig by reading os.environ at call time (not at import)."""
        return cls(
            port=int(os.environ.get("MOCK_PORT", "9000")),
            latency_mean=float(os.environ.get("MOCK_LATENCY_MEAN", "0.05")),
            latency_sigma=float(os.environ.get("MOCK_LATENCY_SIGMA", "0.3")),
            max_capacity=int(os.environ.get("MOCK_MAX_CAPACITY", "20")),
            capacity_mode=os.environ.get("MOCK_CAPACITY_MODE", "queue").strip().lower(),
            error_rate_429=float(os.environ.get("MOCK_ERROR_RATE_429", "0.0")),
            error_rate_500=float(os.environ.get("MOCK_ERROR_RATE_500", "0.0")),
            slow_tail_prob=float(os.environ.get("MOCK_SLOW_TAIL_PROB", "0.0")),
            slow_tail_latency_s=float(os.environ.get("MOCK_SLOW_TAIL_LATENCY_S", "0.0")),
            drop_prob=float(os.environ.get("MOCK_DROP_PROB", "0.0")),
            seed=int(os.environ.get("MOCK_SEED", "42")),
        )


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    total_requests: int = 0
    current_concurrent: int = 0
    peak_concurrent: int = 0
    status_counts: dict[int, int] = field(default_factory=dict)
    queue_waits: list[float] = field(default_factory=list)
    service_times: list[float] = field(default_factory=list)
    injected_429: int = 0
    injected_500: int = 0
    injected_drops: int = 0
    injected_slow_tails: int = 0

    def record_start(self) -> None:
        self.total_requests += 1
        self.current_concurrent += 1
        if self.current_concurrent > self.peak_concurrent:
            self.peak_concurrent = self.current_concurrent

    def record_end(self, status: int) -> None:
        self.current_concurrent -= 1
        self.status_counts[status] = self.status_counts.get(status, 0) + 1

    def record_timing(self, queue_wait: float, service_time: float) -> None:
        self.queue_waits.append(queue_wait)
        self.service_times.append(service_time)

    def reset(self) -> None:
        self.total_requests = 0
        self.current_concurrent = 0
        self.peak_concurrent = 0
        self.status_counts = {}
        self.queue_waits = []
        self.service_times = []
        self.injected_429 = 0
        self.injected_500 = 0
        self.injected_drops = 0
        self.injected_slow_tails = 0

    def _calc_stats(self, values: list[float]) -> dict[str, float]:
        if not values:
            return {"p50": 0.0, "p95": 0.0, "mean": 0.0}
        import math
        import statistics

        sorted_vals = sorted(values)
        n = len(sorted_vals)

        def nr_p(p: float) -> float:
            rank = math.ceil((p / 100.0) * n)
            idx = max(0, min(rank - 1, n - 1))
            return float(sorted_vals[idx])

        return {
            "p50": round(nr_p(50.0), 6),
            "p95": round(nr_p(95.0), 6),
            "mean": round(statistics.fmean(values), 6),
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "current_concurrent": self.current_concurrent,
            "peak_concurrent": self.peak_concurrent,
            "status_counts": dict(self.status_counts),
            "injected": {
                "429": self.injected_429,
                "500": self.injected_500,
                "drop": self.injected_drops,
                "slow_tail": self.injected_slow_tails,
            },
            "injected_429": self.injected_429,
            "injected_500": self.injected_500,
            "injected_drops": self.injected_drops,
            "injected_slow_tails": self.injected_slow_tails,
            "queue_wait": self._calc_stats(self.queue_waits),
            "service_time": self._calc_stats(self.service_times),
        }


# ---------------------------------------------------------------------------
# App state container (one per app instance)
# ---------------------------------------------------------------------------

class AppState:
    def __init__(self, config: MockConfig) -> None:
        self.config = config
        self.stats = Stats()
        self.rng = random.Random(config.seed)
        self._semaphore: asyncio.Semaphore | None = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_capacity)
        return self._semaphore

    @semaphore.setter
    def semaphore(self, value: asyncio.Semaphore | None) -> None:
        self._semaphore = value

    def reseed(self, seed: int | None = None) -> None:
        s = seed if seed is not None else self.config.seed
        self.rng = random.Random(s)

    def sample_latency(self) -> float:
        base = self.rng.lognormvariate(self.config.latency_mean, self.config.latency_sigma)
        if self.rng.random() < self.config.slow_tail_prob:
            self.stats.injected_slow_tails += 1
            if self.config.slow_tail_latency_s > 0.0:
                return self.config.slow_tail_latency_s
            base *= 3.0
        return base



# ---------------------------------------------------------------------------
# Factory function — all state lives on the FastAPI app instance
# ---------------------------------------------------------------------------

def create_app(config: MockConfig | None = None) -> FastAPI:
    """
    Create and return a FastAPI application for the mock provider.

    Args:
        config: MockConfig instance. Defaults to MockConfig() (all defaults).
    """
    if config is None:
        config = MockConfig()

    state = AppState(config)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        state.semaphore = asyncio.Semaphore(config.max_capacity)
        yield

    _app = FastAPI(title="Mock LLM Provider", lifespan=lifespan)

    # ------------------------------------------------------------------ routes

    @_app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        assert state.semaphore is not None

        # Connection drop (before acquiring slot)
        if state.rng.random() < state.config.drop_prob:
            state.stats.injected_drops += 1
            raise RuntimeError("Simulated connection drop")

        # 429 simulation
        if state.rng.random() < state.config.error_rate_429:
            state.stats.injected_429 += 1
            state.stats.total_requests += 1
            state.stats.status_counts[429] = state.stats.status_counts.get(429, 0) + 1
            return JSONResponse(
                status_code=429,
                content={"error": {"message": "Rate limit exceeded", "type": "rate_limit_error"}},
                headers={"Retry-After": "1"},
            )

        # Capacity gate (reject mode: non-blocking peek returns 503; queue mode: waits on semaphore)
        if state.config.capacity_mode == "reject" and state.semaphore._value <= 0:
            state.stats.total_requests += 1
            state.stats.status_counts[503] = state.stats.status_counts.get(503, 0) + 1
            return JSONResponse(
                status_code=503,
                content={"error": {"message": "Mock at capacity", "type": "overloaded"}},
                headers={"Retry-After": "2"},
            )

        t_arrive = time.perf_counter()
        state.stats.record_start()
        status = 200
        try:
            async with state.semaphore:
                t_acquired = time.perf_counter()
                queue_wait = t_acquired - t_arrive
                t_service_start = t_acquired
                try:
                    if state.rng.random() < state.config.error_rate_500:
                        state.stats.injected_500 += 1
                        status = 500
                        return JSONResponse(
                            status_code=500,
                            content={"error": {"message": "Internal mock error", "type": "server_error"}},
                        )

                    try:
                        body = await request.json()
                    except Exception:
                        status = 400
                        return JSONResponse(status_code=400, content={"error": {"message": "Invalid JSON"}})

                    model = body.get("model", "mock-model")
                    messages = body.get("messages", [])
                    prompt = messages[-1].get("content", "") if messages else ""
                    max_tokens = body.get("max_tokens", 256)

                    latency = state.sample_latency()
                    await asyncio.sleep(latency)

                    return JSONResponse(
                        status_code=200,
                        content=_make_completion_response(model, prompt, max_tokens),
                    )
                finally:
                    service_time = time.perf_counter() - t_service_start
                    state.stats.record_timing(queue_wait, service_time)
        finally:
            state.stats.record_end(status)

    @_app.get("/control/stats")
    async def control_stats() -> JSONResponse:
        return JSONResponse(content=state.stats.as_dict())

    @_app.post("/control/reset")
    async def control_reset() -> JSONResponse:
        state.stats.reset()
        state.reseed()
        return JSONResponse(content={"status": "reset"})

    @_app.post("/control/config")
    async def control_config(request: Request) -> JSONResponse:
        """
        Update mock runtime configuration without restarting.
        Accepts JSON body with any subset of MockConfig fields:
            latency_mean, latency_sigma, max_capacity, error_rate_429,
            error_rate_500, slow_tail_prob, drop_prob, seed
        Changing seed immediately reseeds the RNG.
        Changing max_capacity takes effect on the next request (semaphore
        cannot be resized live, but new limit is stored in config).
        """
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"error": "Invalid JSON"})

        cfg = state.config
        if "latency_mean" in body:
            cfg.latency_mean = float(body["latency_mean"])
        if "latency_sigma" in body:
            cfg.latency_sigma = float(body["latency_sigma"])
        if "max_capacity" in body:
            cfg.max_capacity = int(body["max_capacity"])
            state.semaphore = asyncio.Semaphore(cfg.max_capacity)
        if "capacity_mode" in body:
            mode = str(body["capacity_mode"]).strip().lower()
            if mode in ("queue", "reject"):
                cfg.capacity_mode = mode
        if "error_rate_429" in body:
            cfg.error_rate_429 = float(body["error_rate_429"])
        if "error_rate_500" in body:
            cfg.error_rate_500 = float(body["error_rate_500"])
        if "slow_tail_prob" in body:
            cfg.slow_tail_prob = float(body["slow_tail_prob"])
        if "slow_tail_latency_s" in body:
            cfg.slow_tail_latency_s = float(body["slow_tail_latency_s"])
        if "drop_prob" in body:
            cfg.drop_prob = float(body["drop_prob"])
        if "seed" in body:
            state.reseed(int(body["seed"]))

        return JSONResponse(content={"status": "ok", "config": {
            "latency_mean": cfg.latency_mean,
            "latency_sigma": cfg.latency_sigma,
            "max_capacity": cfg.max_capacity,
            "capacity_mode": cfg.capacity_mode,
            "error_rate_429": cfg.error_rate_429,
            "error_rate_500": cfg.error_rate_500,
            "slow_tail_prob": cfg.slow_tail_prob,
            "slow_tail_latency_s": cfg.slow_tail_latency_s,
            "drop_prob": cfg.drop_prob,
        }})

    return _app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_completion_response(model: str, prompt: str, max_tokens: int) -> dict:
    reply = f"Mock response to: {prompt[:80]}"
    prompt_toks = max(1, len(prompt.split()))
    completion_toks = max(1, len(reply.split()))
    return {
        "id": f"mock-{int(time.time() * 1000)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": reply},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_toks,
            "completion_tokens": completion_toks,
            "total_tokens": prompt_toks + completion_toks,
        },
    }


# ---------------------------------------------------------------------------
# Module-level app for uvicorn (env-config at import time, which is fine
# because uvicorn imports this module fresh in its own process)
# ---------------------------------------------------------------------------

app = create_app(MockConfig.from_env())


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> MockConfig:
    parser = argparse.ArgumentParser(description="Mock LLM provider")
    parser.add_argument("--port", type=int, default=int(os.environ.get("MOCK_PORT", "9000")))
    parser.add_argument("--latency-mean", type=float, default=float(os.environ.get("MOCK_LATENCY_MEAN", "0.05")))
    parser.add_argument("--latency-sigma", type=float, default=float(os.environ.get("MOCK_LATENCY_SIGMA", "0.3")))
    parser.add_argument("--max-capacity", type=int, default=int(os.environ.get("MOCK_MAX_CAPACITY", "20")))
    parser.add_argument(
        "--capacity-mode",
        type=str,
        choices=["queue", "reject"],
        default=os.environ.get("MOCK_CAPACITY_MODE", "queue"),
        help="Capacity handling mode: 'queue' (default: wait) or 'reject' (immediate 503)",
    )
    parser.add_argument("--error-rate-429", type=float, default=float(os.environ.get("MOCK_ERROR_RATE_429", "0.0")))
    parser.add_argument("--error-rate-500", type=float, default=float(os.environ.get("MOCK_ERROR_RATE_500", "0.0")))
    parser.add_argument("--slow-tail-prob", type=float, default=float(os.environ.get("MOCK_SLOW_TAIL_PROB", "0.0")))
    parser.add_argument("--slow-tail-latency-s", type=float, default=float(os.environ.get("MOCK_SLOW_TAIL_LATENCY_S", "0.0")))
    parser.add_argument("--drop-prob", type=float, default=float(os.environ.get("MOCK_DROP_PROB", "0.0")))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("MOCK_SEED", "42")))
    args = parser.parse_args()
    return MockConfig(
        port=args.port,
        latency_mean=args.latency_mean,
        latency_sigma=args.latency_sigma,
        max_capacity=args.max_capacity,
        capacity_mode=args.capacity_mode,
        error_rate_429=args.error_rate_429,
        error_rate_500=args.error_rate_500,
        slow_tail_prob=args.slow_tail_prob,
        slow_tail_latency_s=args.slow_tail_latency_s,
        drop_prob=args.drop_prob,
        seed=args.seed,
    )


if __name__ == "__main__":
    import uvicorn

    cfg = _parse_args()
    cli_app = create_app(cfg)
    uvicorn.run(cli_app, host="0.0.0.0", port=cfg.port, log_level="info")
