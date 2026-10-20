"""
OpenAI-compatible provider implementation using httpx.

Design:
- Accepts injected httpx.AsyncClient (MockTransport for tests, real for production).
- Baseline: fresh client per request (reuse_http_client=False).
- Uses httpx trace extension to capture connect_time with perf_counter.
  Real event names (verified by running probe_httpx.py against a TCP server):
    'connection.connect_tcp.started'
    'connection.connect_tcp.complete'
  MockTransport does NOT fire trace events — connect_time=0.0, connection_reused=False
  for MockTransport-based tests, which is the correct semantic (no real TCP).
  ASGITransport similarly fires no trace events.
  Only httpx.AsyncHTTPTransport (real TCP) fires them.
- Retry-After parsed from header (int/float seconds or HTTP-date).
"""
from __future__ import annotations

import time
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

from gateway import errors as errs
from gateway.config import Settings
from gateway.providers.base import Provider, ProviderResult
from gateway.schemas import ChatRequest, TokenUsage


def _parse_retry_after(header: Optional[str]) -> Optional[float]:
    """Return Retry-After as float seconds, or None if unparseable."""
    if not header:
        return None
    header = header.strip()
    try:
        return max(0.0, float(header))
    except ValueError:
        pass
    try:
        import datetime
        dt = parsedate_to_datetime(header)
        delta = (dt - datetime.datetime.now(tz=datetime.timezone.utc)).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None


class _ConnectTracer:
    """
    Collects connect_tcp and start_tls timing from the httpx trace extension.

    Trace callback signature (verified against httpcore._trace):
        async def callback(event_name: str, info: dict) -> None

    Events fired by httpx.AsyncHTTPTransport (real TCP / TLS):
        'connection.connect_tcp.started'   — TCP connection start
        'connection.connect_tcp.complete'  — TCP connection complete
        'connection.connect_tcp.failed'    — TCP connection failed
        'connection.start_tls.started'     — TLS handshake start
        'connection.start_tls.complete'    — TLS handshake complete
        'connection.start_tls.failed'      — TLS handshake failed

    When connection is pooled (reused), none of these events fire → connect_time stays 0.0.
    MockTransport / ASGITransport fire no trace events → same result.
    """

    def __init__(self) -> None:
        self._tcp_start: Optional[float] = None
        self._tls_start: Optional[float] = None
        self.connect_time: float = 0.0
        self.connection_reused: bool = True  # assume reused until we see connect/tls started

    async def __call__(self, event_name: str, info: dict) -> None:
        now = time.perf_counter()
        if event_name == "connection.connect_tcp.started":
            self._tcp_start = now
            self.connection_reused = False
        elif event_name in ("connection.connect_tcp.complete", "connection.connect_tcp.failed"):
            if self._tcp_start is not None:
                self.connect_time += now - self._tcp_start
                self._tcp_start = None
        elif event_name == "connection.start_tls.started":
            self._tls_start = now
            self.connection_reused = False
        elif event_name in ("connection.start_tls.complete", "connection.start_tls.failed"):
            if self._tls_start is not None:
                self.connect_time += now - self._tls_start
                self._tls_start = None


class OpenAICompatProvider(Provider):
    """
    Provider for any OpenAI-compatible /v1/chat/completions endpoint.

    Args:
        settings: Application settings.
        client:   Optional pre-built httpx.AsyncClient. If None and
                  reuse_http_client=False, a fresh client is created and
                  closed per request (baseline mode).
    """

    def __init__(
        self,
        settings: Settings,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._settings = settings
        self._injected_client = client

    @property
    def name(self) -> str:
        return self._settings.provider_name

    def _build_client(self) -> httpx.AsyncClient:
        limits = httpx.Limits(
            max_keepalive_connections=self._settings.pool_max_keepalive,
            max_connections=self._settings.pool_max_connections,
        )
        headers: dict[str, str] = {"Content-Type": "application/json"}
        api_key = self._settings.provider_api_key
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return httpx.AsyncClient(
            base_url=self._settings.provider_base_url,
            headers=headers,
            limits=limits,
        )

    def _build_payload(self, request: ChatRequest) -> dict:
        model = request.model or self._settings.provider_model
        max_tokens = request.max_tokens or self._settings.max_output_tokens
        return {
            "model": model,
            "messages": [{"role": "user", "content": request.prompt}],
            "max_tokens": max_tokens,
        }

    def _map_error(
        self,
        exc: Exception,
        *,
        response: Optional[httpx.Response] = None,
    ) -> errs.ProviderError:
        if isinstance(exc, httpx.TimeoutException):
            return errs.TimeoutError(str(exc))
        if isinstance(exc, (httpx.ConnectError, httpx.RemoteProtocolError, httpx.NetworkError)):
            return errs.ConnectionError(str(exc))
        if response is not None:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            return errs.from_http_status(response.status_code, response.text[:512], retry_after=retry_after)
        return errs.ServerError(str(exc))

    async def complete(
        self,
        request: ChatRequest,
        *,
        timeout: Optional[float] = None,
    ) -> ProviderResult:
        effective_timeout = timeout or self._settings.request_timeout
        payload = self._build_payload(request)
        model = payload["model"]

        use_injected = self._injected_client is not None
        client: httpx.AsyncClient = self._injected_client or self._build_client()

        tracer = _ConnectTracer()
        start = time.perf_counter()
        try:
            response = await client.post(
                "/v1/chat/completions",
                json=payload,
                timeout=effective_timeout,
                extensions={"trace": tracer},
            )
        except (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.RemoteProtocolError,
            httpx.NetworkError,
        ) as exc:
            raise self._map_error(exc) from exc
        except Exception as exc:
            raise errs.ServerError(f"Unexpected transport error: {exc}") from exc
        finally:
            if not use_injected:
                await client.aclose()

        if response.status_code >= 400:
            raise self._map_error(Exception(response.text), response=response)

        try:
            body = response.json()
        except Exception as exc:
            raise errs.ServerError(f"Invalid JSON response: {exc}") from exc

        try:
            choice = body["choices"][0]
            text: str = choice["message"]["content"]
            actual_model: str = body.get("model", model)
            raw_usage = body.get("usage") or {}
            usage = TokenUsage(
                prompt_tokens=raw_usage.get("prompt_tokens", 0),
                completion_tokens=raw_usage.get("completion_tokens", 0),
                total_tokens=raw_usage.get("total_tokens", 0),
            )
        except (KeyError, IndexError, TypeError) as exc:
            raise errs.ServerError(f"Unexpected response shape: {exc}") from exc

        result = ProviderResult.create(
            text=text,
            provider=self._settings.provider_name,
            model=actual_model,
            usage=usage,
            http_status=response.status_code,
            start=start,
            connect_time=tracer.connect_time,
            connection_reused=tracer.connection_reused,
        )
        result.finish_reason = choice.get("finish_reason")
        return result
