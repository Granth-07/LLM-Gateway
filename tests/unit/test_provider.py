"""
Unit tests for OpenAICompatProvider using httpx.MockTransport.
No network access required.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest


from gateway import errors as errs
from gateway.config import Settings
from gateway.providers.openai_compat import OpenAICompatProvider
from gateway.schemas import ChatRequest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _settings(**kwargs: Any) -> Settings:
    """Build Settings with test-safe defaults."""
    defaults: dict[str, Any] = {
        "provider_name": "mock",
        "provider_base_url": "http://testserver",
        "provider_api_key": "",
        "provider_model": "test-model",
        "request_timeout": 10.0,
        "max_output_tokens": 256,
        "reuse_http_client": False,
    }
    defaults.update(kwargs)
    return Settings(**defaults)


def _ok_body(model: str = "test-model", content: str = "Hello!", prompt_tokens: int = 5) -> dict:
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 1_700_000_000,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": 3,
            "total_tokens": prompt_tokens + 3,
        },
    }


def _make_transport(
    status_code: int,
    body: dict | None = None,
    headers: dict | None = None,
    raise_exc: Exception | None = None,
) -> httpx.MockTransport:
    """Build an httpx.MockTransport that returns a fixed response."""

    def handler(request: httpx.Request) -> httpx.Response:
        if raise_exc is not None:
            raise raise_exc
        return httpx.Response(
            status_code=status_code,
            headers=headers or {},
            json=body or {},
        )

    return httpx.MockTransport(handler)


def _client(transport: httpx.MockTransport, base_url: str = "http://testserver") -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=transport, base_url=base_url)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestOpenAICompatProviderSuccess:
    @pytest.mark.asyncio
    async def test_returns_text(self):
        transport = _make_transport(200, _ok_body(content="Hi there!"))
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            result = await provider.complete(ChatRequest(prompt="Hello"))
        assert result.text == "Hi there!"

    @pytest.mark.asyncio
    async def test_returns_model_from_response(self):
        transport = _make_transport(200, _ok_body(model="actual-model"))
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            result = await provider.complete(ChatRequest(prompt="Hello"))
        assert result.model == "actual-model"

    @pytest.mark.asyncio
    async def test_usage_parsed(self):
        transport = _make_transport(200, _ok_body(prompt_tokens=10))
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            result = await provider.complete(ChatRequest(prompt="Hello"))
        assert result.usage.prompt_tokens == 10
        assert result.usage.total_tokens == 13

    @pytest.mark.asyncio
    async def test_provider_duration_positive(self):
        transport = _make_transport(200, _ok_body())
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            result = await provider.complete(ChatRequest(prompt="Hello"))
        assert result.provider_duration >= 0.0

    @pytest.mark.asyncio
    async def test_finish_reason(self):
        transport = _make_transport(200, _ok_body())
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            result = await provider.complete(ChatRequest(prompt="Hello"))
        assert result.finish_reason == "stop"

    @pytest.mark.asyncio
    async def test_http_status_recorded(self):
        transport = _make_transport(200, _ok_body())
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            result = await provider.complete(ChatRequest(prompt="Hello"))
        assert result.http_status == 200

    @pytest.mark.asyncio
    async def test_provider_name_from_settings(self):
        transport = _make_transport(200, _ok_body())
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(provider_name="my-mock"), client=client)
            result = await provider.complete(ChatRequest(prompt="Hello"))
        assert result.provider == "my-mock"


# ---------------------------------------------------------------------------
# HTTP error mapping
# ---------------------------------------------------------------------------

class TestHttpErrorMapping:
    @pytest.mark.asyncio
    async def test_401_raises_auth_error(self):
        transport = _make_transport(401, {"error": {"message": "Invalid API key"}})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.AuthError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retryable is False
        assert exc_info.value.status == 401

    @pytest.mark.asyncio
    async def test_429_raises_rate_limit_error(self):
        transport = _make_transport(
            429,
            {"error": {"message": "Too many requests"}},
            headers={"Retry-After": "5"},
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.RateLimitError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        err = exc_info.value
        assert err.retryable is True
        assert err.retry_after == 5.0
        assert err.status == 429

    @pytest.mark.asyncio
    async def test_500_raises_server_error(self):
        transport = _make_transport(500, {"error": {"message": "Internal error"}})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ServerError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retryable is True
        assert exc_info.value.status == 500

    @pytest.mark.asyncio
    async def test_503_raises_server_error(self):
        transport = _make_transport(503, {"error": {"message": "Service unavailable"}})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ServerError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retryable is True

    @pytest.mark.asyncio
    async def test_400_raises_client_error(self):
        transport = _make_transport(400, {"error": {"message": "Bad request"}})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ClientError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retryable is False

    @pytest.mark.asyncio
    async def test_404_raises_client_error(self):
        transport = _make_transport(404, {"error": {"message": "Not found"}})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ClientError):
                await provider.complete(ChatRequest(prompt="Hello"))

    @pytest.mark.asyncio
    async def test_403_raises_client_error_not_retryable(self):
        transport = _make_transport(403, {"error": {"message": "Forbidden"}})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ClientError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retryable is False


# ---------------------------------------------------------------------------
# Transport-level error mapping
# ---------------------------------------------------------------------------

class TestTransportErrorMapping:
    @pytest.mark.asyncio
    async def test_connect_timeout_raises_timeout_error(self):
        transport = _make_transport(
            200,
            raise_exc=httpx.ConnectTimeout("timed out", request=None),
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.TimeoutError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retryable is True
        assert exc_info.value.kind == "timeout"

    @pytest.mark.asyncio
    async def test_read_timeout_raises_timeout_error(self):
        transport = _make_transport(
            200,
            raise_exc=httpx.ReadTimeout("read timeout", request=None),
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.TimeoutError):
                await provider.complete(ChatRequest(prompt="Hello"))

    @pytest.mark.asyncio
    async def test_connect_error_raises_connection_error(self):
        transport = _make_transport(
            200,
            raise_exc=httpx.ConnectError("connection refused"),
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ConnectionError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retryable is True
        assert exc_info.value.kind == "connection"

    @pytest.mark.asyncio
    async def test_remote_protocol_error_raises_connection_error(self):
        transport = _make_transport(
            200,
            raise_exc=httpx.RemoteProtocolError("connection dropped"),
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ConnectionError):
                await provider.complete(ChatRequest(prompt="Hello"))

    @pytest.mark.asyncio
    async def test_network_error_raises_connection_error(self):
        transport = _make_transport(
            200,
            raise_exc=httpx.NetworkError("network error"),
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ConnectionError):
                await provider.complete(ChatRequest(prompt="Hello"))


# ---------------------------------------------------------------------------
# Retry-After header parsing
# ---------------------------------------------------------------------------

class TestRetryAfterParsing:
    @pytest.mark.asyncio
    async def test_integer_retry_after(self):
        transport = _make_transport(
            429,
            {"error": {}},
            headers={"Retry-After": "30"},
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.RateLimitError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retry_after == 30.0

    @pytest.mark.asyncio
    async def test_no_retry_after_header(self):
        transport = _make_transport(429, {"error": {}})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.RateLimitError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retry_after is None

    @pytest.mark.asyncio
    async def test_float_retry_after(self):
        transport = _make_transport(
            429,
            {"error": {}},
            headers={"Retry-After": "2.5"},
        )
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.RateLimitError) as exc_info:
                await provider.complete(ChatRequest(prompt="Hello"))
        assert exc_info.value.retry_after == 2.5


# ---------------------------------------------------------------------------
# Malformed response handling
# ---------------------------------------------------------------------------

class TestMalformedResponse:
    @pytest.mark.asyncio
    async def test_missing_choices_raises_server_error(self):
        transport = _make_transport(200, {"id": "x", "model": "m"})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ServerError):
                await provider.complete(ChatRequest(prompt="Hello"))

    @pytest.mark.asyncio
    async def test_empty_choices_raises_server_error(self):
        transport = _make_transport(200, {"choices": []})
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            with pytest.raises(errs.ServerError):
                await provider.complete(ChatRequest(prompt="Hello"))


# ---------------------------------------------------------------------------
# Connect timing & connection reuse
# ---------------------------------------------------------------------------

@pytest.fixture
async def local_http_server():
    """Ephemeral TCP server supporting HTTP/1.1 keep-alive."""
    body = (
        b'{"id":"mock-1","object":"chat.completion","model":"test-model",'
        b'"choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],'
        b'"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}'
    )
    resp = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
        b"Connection: keep-alive\r\n\r\n" + body
    )

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                clen = 0
                while True:
                    h = await reader.readline()
                    if not h or h == b"\r\n":
                        break
                    if h.lower().startswith(b"content-length:"):
                        clen = int(h.split(b":", 1)[1].strip())
                if clen:
                    await reader.readexactly(clen)
                writer.write(resp)
                await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    try:
        yield url
    finally:
        server.close()
        await server.wait_closed()


class TestConnectTimingAndReuse:
    @pytest.mark.asyncio
    async def test_fresh_client_records_connect_time(self, local_http_server):
        settings = _settings(provider_base_url=local_http_server)
        provider = OpenAICompatProvider(settings)
        result = await provider.complete(ChatRequest(prompt="hello"))
        assert result.connect_time > 0.0
        assert result.connection_reused is False

    @pytest.mark.asyncio
    async def test_second_request_same_client_reuses_connection(self, local_http_server):
        settings = _settings(provider_base_url=local_http_server)
        async with httpx.AsyncClient(base_url=settings.provider_base_url) as client:
            provider = OpenAICompatProvider(settings, client=client)
            r1 = await provider.complete(ChatRequest(prompt="first"))
            assert r1.connect_time > 0.0
            assert r1.connection_reused is False

            r2 = await provider.complete(ChatRequest(prompt="second"))
            assert r2.connect_time == 0.0
            assert r2.connection_reused is True

    @pytest.mark.asyncio
    async def test_mock_transport_has_zero_connect_time(self):
        transport = _make_transport(200, _ok_body())
        async with _client(transport) as client:
            provider = OpenAICompatProvider(_settings(), client=client)
            result = await provider.complete(ChatRequest(prompt="hello"))
            assert result.connect_time == 0.0
            assert result.connection_reused is True

    @pytest.mark.asyncio
    async def test_connect_tracer_handles_tls_events(self):
        from gateway.providers.openai_compat import _ConnectTracer
        tracer = _ConnectTracer()
        assert tracer.connection_reused is True
        assert tracer.connect_time == 0.0

        await tracer("connection.connect_tcp.started", {})
        assert tracer.connection_reused is False
        await asyncio.sleep(0.002)
        await tracer("connection.connect_tcp.complete", {})
        tcp_time = tracer.connect_time
        assert tcp_time > 0.0

        await tracer("connection.start_tls.started", {})
        await asyncio.sleep(0.002)
        await tracer("connection.start_tls.complete", {})
        assert tracer.connect_time > tcp_time

    @pytest.mark.asyncio
    async def test_one_shared_client_100_sequential_requests_reuse_fraction_99_percent(self, local_http_server):
        settings = _settings(provider_base_url=local_http_server)
        limits = httpx.Limits(max_keepalive_connections=100, max_connections=100)
        async with httpx.AsyncClient(base_url=settings.provider_base_url, limits=limits) as client:
            provider = OpenAICompatProvider(settings, client=client)
            reused_count = 0
            for i in range(100):
                res = await provider.complete(ChatRequest(prompt=f"req-{i}"))
                if res.connection_reused and res.connect_time == 0.0:
                    reused_count += 1
            reuse_fraction = reused_count / 100.0
            assert reuse_fraction >= 0.98, f"Expected ~99% reuse, got {reuse_fraction}"


