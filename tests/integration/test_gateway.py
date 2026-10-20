"""
Integration tests for LLM Gateway API using ASGITransport and in-process fake Provider.
No network access required.

Requirements verified:
- Success shape (text, provider, model, usage, request_id, timings)
- Validation errors (422) with ErrorBody and request_id
- Unknown provider (400) with ErrorBody and request_id
- Each error mapping:
    timeout => 504
    upstream 429 => 429 + Retry-After passthrough
    5xx/connection/upstream auth => 502 (never leak keys, never 401)
- Client creation counts for both reuse_http_client values (False => N clients, True => 1 client)
- Timings invariant: total >= parts, overhead >= 0
- request_id on every response (body + header)
- GET /health endpoint
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, Optional

import httpx
import pytest

from gateway import errors as errs
from gateway.app import create_app
from gateway.config import Settings
from gateway.core.client_pool import ClientPool
from gateway.core.pipeline import Pipeline, ProviderFactory
from gateway.core.timing import compute_timings
from gateway.providers.base import Provider, ProviderResult
from gateway.schemas import ChatRequest, TokenUsage


async def _fast_sleep(_: float) -> None:
    pass


def _make_app(fake: Provider, settings: Optional[Settings] = None) -> Any:
    s = settings or Settings()
    pool = ClientPool(s)
    pipe = Pipeline(s, client_pool=pool, provider_factory=_make_factory(fake), sleep=_fast_sleep)
    return create_app(settings=s, client_pool=pool, pipeline=pipe)


class FakeProvider(Provider):
    """Configurable in-process Provider for testing without network."""

    def __init__(
        self,
        *,
        name: str = "mock",
        model: str = "mock-model",
        text: str = "Fake generated response",
        usage: Optional[TokenUsage] = None,
        duration: float = 0.015,
        exc: Optional[Exception] = None,
    ) -> None:
        self._name = name
        self.model = model
        self.text = text
        self.usage = usage or TokenUsage(prompt_tokens=8, completion_tokens=12, total_tokens=20)
        self.duration = duration
        self.exc = exc

    @property
    def name(self) -> str:
        return self._name

    async def complete(self, request: ChatRequest, *, timeout: Optional[float] = None) -> ProviderResult:
        if self.exc is not None:
            raise self.exc
        if self.duration > 0:
            import time
            time.sleep(self.duration)
        return ProviderResult(
            text=self.text,
            provider=self._name,
            model=request.model or self.model,
            usage=self.usage,
            http_status=200,
            provider_duration=self.duration,
            connect_time=0.0,
            connection_reused=True,
            finish_reason="stop",
        )


def _make_factory(fake_provider: Provider) -> ProviderFactory:
    def factory(settings: Settings, client: Optional[httpx.AsyncClient] = None) -> Provider:
        name = settings.provider_name.lower()
        if name in {"mock", "openai", "fake"}:
            return fake_provider
        raise ValueError(f"Unknown provider {name!r}. Registered names: ['fake', 'mock', 'openai']")

    return factory


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health() -> None:
    app = create_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Success shape & timings invariant
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_chat_success_shape_and_timings() -> None:
    fake = FakeProvider(text="Hello world!", duration=0.01)
    app = create_app(provider_factory=_make_factory(fake))

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/chat", json={"prompt": "Say hi", "model": "gpt-custom"})

    assert r.status_code == 200
    data = r.json()

    # Core shape
    assert data["text"] == "Hello world!"
    assert data["provider"] == "mock"
    assert data["model"] == "gpt-custom"
    assert data["usage"] == {"prompt_tokens": 8, "completion_tokens": 12, "total_tokens": 20}
    assert "request_id" in data

    # Timings invariant
    timings = data["timings"]
    assert timings["provider_duration"] >= 0.01
    assert timings["queue_wait"] == 0.0
    assert timings["backoff_sleep"] == 0.0
    assert timings["overhead"] >= 0.0
    assert timings["total"] >= (
        timings["provider_duration"]
        + timings["queue_wait"]
        + timings["client_setup"]
        + timings["backoff_sleep"]
        + timings["response_processing"]
    )
    # Invariant: total = queue_wait + client_setup + provider_time + backoff_sleep + response_processing + overhead
    expected_sum = (
        timings["queue_wait"]
        + timings["client_setup"]
        + timings["provider_duration"]
        + timings["backoff_sleep"]
        + timings["response_processing"]
        + timings["overhead"]
    )
    assert pytest.approx(timings["total"], abs=1e-6) == expected_sum


# ---------------------------------------------------------------------------
# Request ID propagation and generation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_request_id_explicit_and_generated() -> None:
    fake = FakeProvider()
    app = create_app(provider_factory=_make_factory(fake))

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # 1. Custom request ID passed via header
        custom_id = "custom-req-id-12345"
        r1 = await client.post("/chat", json={"prompt": "Test"}, headers={"X-Request-ID": custom_id})
        assert r1.status_code == 200
        assert r1.json()["request_id"] == custom_id
        assert r1.headers["X-Request-ID"] == custom_id

        # 2. Automatically generated UUID
        r2 = await client.post("/chat", json={"prompt": "Test"})
        assert r2.status_code == 200
        gen_id = r2.json()["request_id"]
        assert r2.headers["X-Request-ID"] == gen_id
        # Must be valid UUID
        parsed_uuid = uuid.UUID(gen_id)
        assert str(parsed_uuid) == gen_id


# ---------------------------------------------------------------------------
# Validation error 422
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_validation_error_422() -> None:
    app = create_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # Missing prompt
        r = await client.post("/chat", json={})
        assert r.status_code == 422
        body = r.json()
        assert body["error"] == "validation_error"
        assert "Request validation failed" in body["message"]
        assert body["retryable"] is False
        assert body["request_id"] is not None
        assert r.headers["X-Request-ID"] == body["request_id"]

        # Prompt too short (empty)
        r2 = await client.post("/chat", json={"prompt": ""})
        assert r2.status_code == 422
        assert r2.json()["error"] == "validation_error"


# ---------------------------------------------------------------------------
# Unknown provider 400
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_provider_400() -> None:
    fake = FakeProvider()
    app = create_app(provider_factory=_make_factory(fake))

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/chat", json={"prompt": "Hello", "provider": "nonexistent_provider"})
        assert r.status_code == 400
        body = r.json()
        assert body["error"] == "unknown_provider"
        assert "nonexistent_provider" in body["message"]
        assert body["retryable"] is False
        assert body["request_id"] is not None


# ---------------------------------------------------------------------------
# Error mappings: 504, 429, 502
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_error_mapping_timeout_504() -> None:
    fake = FakeProvider(exc=errs.TimeoutError("Upstream socket timeout"))
    app = _make_app(fake)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/chat", json={"prompt": "Hello"})
        assert r.status_code == 504
        body = r.json()
        assert body["error"] == "upstream_timeout"
        assert body["retryable"] is True
        assert body["request_id"] is not None


@pytest.mark.asyncio
async def test_error_mapping_ratelimit_429_with_retry_after() -> None:
    fake = FakeProvider(exc=errs.RateLimitError("Rate limit exceeded", retry_after=4.5))
    app = _make_app(fake)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/chat", json={"prompt": "Hello"})
        assert r.status_code == 429
        assert r.headers.get("Retry-After") == "4.5"
        body = r.json()
        assert body["error"] == "rate_limit_exceeded"
        assert body["retryable"] is True
        assert body["retry_after"] == 4.5
        assert body["request_id"] is not None


@pytest.mark.asyncio
async def test_error_mapping_server_error_502() -> None:
    fake = FakeProvider(exc=errs.ServerError("Upstream 500 error"))
    app = _make_app(fake)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/chat", json={"prompt": "Hello"})
        assert r.status_code == 502
        body = r.json()
        assert body["error"] == "bad_gateway"
        assert body["retryable"] is True
        assert body["request_id"] is not None


@pytest.mark.asyncio
async def test_error_mapping_connection_error_502() -> None:
    fake = FakeProvider(exc=errs.ConnectionError("Connection refused"))
    app = _make_app(fake)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/chat", json={"prompt": "Hello"})
        assert r.status_code == 502
        body = r.json()
        assert body["error"] == "bad_gateway"
        assert body["retryable"] is True
        assert body["request_id"] is not None


@pytest.mark.asyncio
async def test_error_mapping_auth_error_never_401_and_no_leak() -> None:
    fake = FakeProvider(exc=errs.AuthError("secret_api_key_12345_invalid"))
    app = create_app(provider_factory=_make_factory(fake))

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/chat", json={"prompt": "Hello"})
        # MUST be 502, NEVER 401
        assert r.status_code == 502
        assert r.status_code != 401
        body = r.json()
        assert body["error"] == "bad_gateway"
        assert body["retryable"] is False
        # MUST NOT leak keys or secrets in response message
        assert "secret_api_key_12345" not in body["message"]
        assert body["request_id"] is not None


# ---------------------------------------------------------------------------
# Client creation counts for both flag values
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_client_creation_counts_flag_off() -> None:
    """reuse_http_client=False => fresh client per request."""
    settings = Settings(reuse_http_client=False)
    fake = FakeProvider()
    app = create_app(settings=settings, provider_factory=_make_factory(fake))

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        for _ in range(3):
            r = await client.post("/chat", json={"prompt": "Hello"})
            assert r.status_code == 200

    # 3 distinct requests => 3 clients created
    assert app.state.client_pool.created_count == 3


@pytest.mark.asyncio
async def test_client_creation_counts_flag_on() -> None:
    """reuse_http_client=True => single shared client across requests."""
    settings = Settings(reuse_http_client=True, pool_max_keepalive=10, pool_max_connections=50)
    fake = FakeProvider()
    app = create_app(settings=settings, provider_factory=_make_factory(fake))

    # Initialize lifespan so shared client is created
    await app.state.client_pool.start()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            for _ in range(3):
                r = await client.post("/chat", json={"prompt": "Hello"})
                assert r.status_code == 200

        # All 3 requests share 1 client created at startup
        assert app.state.client_pool.created_count == 1
    finally:
        await app.state.client_pool.close()


# ---------------------------------------------------------------------------
# Overflow & Limiter 503 tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_overflow_queue_full_returns_503_retry_after() -> None:
    settings = Settings(concurrency_limit=1, queue_size=0, limiter_retry_after=3.0)
    gate = asyncio.Event()

    class GatedProvider(Provider):
        name = "mock"
        async def complete(self, req: ChatRequest, *, timeout: Optional[float] = None) -> ProviderResult:
            await gate.wait()
            return ProviderResult(
                text="ok", provider="mock", model="mock-model",
                usage=TokenUsage(), http_status=200, provider_duration=0.01
            )

    app = create_app(settings=settings, provider_factory=lambda settings, client=None: GatedProvider())

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        t1 = asyncio.create_task(client.post("/chat", json={"prompt": "1"}))
        await asyncio.sleep(0.02)

        # 2nd request arrives when limit=1 and queue_size=0 -> immediate 503
        r2 = await client.post("/chat", json={"prompt": "2"})
        assert r2.status_code == 503
        assert r2.headers.get("Retry-After") == "3"
        body = r2.json()
        assert body["error"] == "queue_full"
        assert body["retryable"] is True
        assert body["retry_after"] == 3.0
        assert "request_id" in body

        gate.set()
        r1 = await t1
        assert r1.status_code == 200


@pytest.mark.asyncio
async def test_overflow_queue_timeout_returns_503_retry_after() -> None:
    settings = Settings(concurrency_limit=1, queue_size=5, queue_wait_timeout=0.05, limiter_retry_after=4.0)
    gate = asyncio.Event()

    class GatedProvider(Provider):
        name = "mock"
        async def complete(self, req: ChatRequest, *, timeout: Optional[float] = None) -> ProviderResult:
            await gate.wait()
            return ProviderResult(
                text="ok", provider="mock", model="mock-model",
                usage=TokenUsage(), http_status=200, provider_duration=0.01
            )

    app = create_app(settings=settings, provider_factory=lambda settings, client=None: GatedProvider())

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        t1 = asyncio.create_task(client.post("/chat", json={"prompt": "1"}))
        await asyncio.sleep(0.01)

        # 2nd request enters queue and times out after 0.05s
        r2 = await client.post("/chat", json={"prompt": "2"})
        assert r2.status_code == 503
        assert r2.headers.get("Retry-After") == "4"
        body = r2.json()
        assert body["error"] == "queue_timeout"
        assert body["retryable"] is True
        assert body["retry_after"] == 4.0

        gate.set()
        r1 = await t1
        assert r1.status_code == 200


@pytest.mark.asyncio
async def test_client_cancellation_releases_slot() -> None:
    settings = Settings(concurrency_limit=1, queue_size=5)
    in_call = asyncio.Event()
    gate = asyncio.Event()

    class HangingProvider(Provider):
        name = "mock"
        async def complete(self, req: ChatRequest, *, timeout: Optional[float] = None) -> ProviderResult:
            in_call.set()
            await gate.wait()
            return ProviderResult(
                text="ok", provider="mock", model="mock-model",
                usage=TokenUsage(), http_status=200, provider_duration=0.01
            )

    app = create_app(settings=settings, provider_factory=lambda settings, client=None: HangingProvider())

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        t1 = asyncio.create_task(client.post("/chat", json={"prompt": "hanging"}))
        await in_call.wait()

        # Slot is currently held
        assert app.state.pipeline.limiter.slots_held == 1

        # Client aborts / cancels request mid-flight
        t1.cancel()
        try:
            await t1
        except (asyncio.CancelledError, httpx.RequestError, Exception):
            pass

        gate.set()
        await asyncio.sleep(0.02)

        # Slot must be fully released!
        assert app.state.pipeline.limiter.slots_held == 0
        assert app.state.pipeline.limiter.in_flight == 0

        # Subsequent request can immediately acquire slot and succeed
        fake = FakeProvider(text="recovered", duration=0.0)
        app.state.pipeline._provider_factory = _make_factory(fake)
        r2 = await client.post("/chat", json={"prompt": "after cancel"})
        assert r2.status_code == 200
        assert r2.json()["text"] == "recovered"
        assert app.state.pipeline.limiter.slots_held == 0

