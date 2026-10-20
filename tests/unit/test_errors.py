"""
Unit tests for gateway.errors.
"""
from __future__ import annotations

import pytest

from gateway import errors as errs


class TestErrorHierarchy:
    def test_timeout_error_kind(self):
        e = errs.TimeoutError("timeout occurred")
        assert e.kind == "timeout"
        assert e.retryable is True
        assert e.status == 504

    def test_rate_limit_error(self):
        e = errs.RateLimitError("too many", retry_after=5.0)
        assert e.kind == "rate_limit"
        assert e.retryable is True
        assert e.retry_after == 5.0
        assert e.status == 429

    def test_server_error(self):
        e = errs.ServerError("internal error")
        assert e.kind == "server"
        assert e.retryable is True
        assert e.status == 502

    def test_connection_error(self):
        e = errs.ConnectionError("refused")
        assert e.kind == "connection"
        assert e.retryable is True
        assert e.status == 503

    def test_auth_error(self):
        e = errs.AuthError("invalid key")
        assert e.kind == "auth"
        assert e.retryable is False
        assert e.status == 401

    def test_client_error(self):
        e = errs.ClientError("bad request")
        assert e.kind == "client"
        assert e.retryable is False
        assert e.status == 400

    def test_all_are_provider_errors(self):
        for cls in (
            errs.TimeoutError,
            errs.RateLimitError,
            errs.ServerError,
            errs.ConnectionError,
            errs.AuthError,
            errs.ClientError,
        ):
            assert issubclass(cls, errs.ProviderError)

    def test_status_override(self):
        e = errs.ServerError("custom", status=501)
        assert e.status == 501

    def test_retryable_override(self):
        e = errs.ServerError("custom", retryable=False)
        assert e.retryable is False

    def test_repr_contains_kind(self):
        e = errs.TimeoutError("t/o")
        assert "timeout" in repr(e)


class TestFromHttpStatus:
    def test_401_maps_to_auth(self):
        e = errs.from_http_status(401, "unauthorized")
        assert isinstance(e, errs.AuthError)
        assert e.retryable is False

    def test_429_maps_to_rate_limit(self):
        e = errs.from_http_status(429, "too many", retry_after=10.0)
        assert isinstance(e, errs.RateLimitError)
        assert e.retryable is True
        assert e.retry_after == 10.0

    def test_400_maps_to_client(self):
        e = errs.from_http_status(400, "bad request")
        assert isinstance(e, errs.ClientError)
        assert e.retryable is False

    def test_404_maps_to_client(self):
        e = errs.from_http_status(404, "not found")
        assert isinstance(e, errs.ClientError)

    def test_403_maps_to_client(self):
        e = errs.from_http_status(403, "forbidden")
        assert isinstance(e, errs.ClientError)

    def test_500_maps_to_server(self):
        e = errs.from_http_status(500, "internal server error")
        assert isinstance(e, errs.ServerError)
        assert e.retryable is True

    def test_503_maps_to_server(self):
        e = errs.from_http_status(503, "service unavailable")
        assert isinstance(e, errs.ServerError)

    def test_status_preserved(self):
        e = errs.from_http_status(422, "validation error")
        assert e.status == 422
