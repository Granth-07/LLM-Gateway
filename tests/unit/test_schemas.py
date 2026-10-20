"""
Unit tests for gateway.schemas.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from gateway.schemas import ChatRequest, ChatResponse, ErrorBody, RequestTimings, TokenUsage


class TestChatRequest:
    def test_minimal_valid(self):
        r = ChatRequest(prompt="Hello")
        assert r.prompt == "Hello"
        assert r.model is None
        assert r.provider is None
        assert r.max_tokens is None
        assert r.timeout is None

    def test_all_fields(self):
        r = ChatRequest(
            prompt="Hi",
            model="gpt-4",
            provider="openai",
            max_tokens=512,
            timeout=10.0,
        )
        assert r.model == "gpt-4"
        assert r.provider == "openai"
        assert r.max_tokens == 512
        assert r.timeout == 10.0

    def test_empty_prompt_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(prompt="")

    def test_prompt_too_long_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(prompt="x" * 32_769)

    def test_max_tokens_zero_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(prompt="hi", max_tokens=0)

    def test_max_tokens_upper_bound(self):
        with pytest.raises(ValidationError):
            ChatRequest(prompt="hi", max_tokens=65_537)

    def test_timeout_zero_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(prompt="hi", timeout=0.0)

    def test_timeout_upper_bound(self):
        with pytest.raises(ValidationError):
            ChatRequest(prompt="hi", timeout=301.0)

    def test_model_empty_string_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(prompt="hi", model="")

    def test_max_tokens_at_upper_bound(self):
        r = ChatRequest(prompt="hi", max_tokens=65_536)
        assert r.max_tokens == 65_536


class TestTokenUsage:
    def test_defaults_zero(self):
        u = TokenUsage()
        assert u.prompt_tokens == 0
        assert u.completion_tokens == 0
        assert u.total_tokens == 0

    def test_negative_rejected(self):
        with pytest.raises(ValidationError):
            TokenUsage(prompt_tokens=-1)


class TestRequestTimings:
    def test_all_defaults_zero(self):
        t = RequestTimings()
        assert t.queue_wait == 0.0
        assert t.provider_duration == 0.0
        assert t.backoff_sleep == 0.0
        assert t.overhead == 0.0
        assert t.total == 0.0

    def test_negative_rejected(self):
        with pytest.raises(ValidationError):
            RequestTimings(queue_wait=-1.0)


class TestChatResponse:
    def test_construct(self):
        r = ChatResponse(
            text="Hello!",
            provider="mock",
            model="mock-model",
            request_id="req-123",
        )
        assert r.text == "Hello!"
        assert r.usage.total_tokens == 0
        assert r.timings.total == 0.0

    def test_with_usage(self):
        r = ChatResponse(
            text="Hi",
            provider="mock",
            model="mock-model",
            request_id="req-456",
            usage=TokenUsage(prompt_tokens=5, completion_tokens=3, total_tokens=8),
        )
        assert r.usage.total_tokens == 8


class TestErrorBody:
    def test_minimal(self):
        e = ErrorBody(error="timeout", message="Timed out")
        assert e.retryable is False
        assert e.retry_after is None
        assert e.request_id is None

    def test_with_retry(self):
        e = ErrorBody(
            error="rate_limit",
            message="Too many requests",
            retryable=True,
            retry_after=5.0,
            request_id="req-789",
        )
        assert e.retryable is True
        assert e.retry_after == 5.0
