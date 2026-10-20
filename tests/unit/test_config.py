"""
Unit tests for gateway.config.Settings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from gateway.config import Settings, get_settings


class TestSettingsDefaults:
    def test_default_provider_name(self):
        s = Settings()
        assert s.provider_name == "mock"

    def test_default_base_url(self):
        s = Settings()
        assert s.provider_base_url == "http://localhost:9000"

    def test_default_api_key_empty(self):
        s = Settings()
        assert s.provider_api_key == ""

    def test_default_model(self):
        s = Settings()
        assert s.provider_model == "mock-model"

    def test_default_timeout(self):
        s = Settings()
        assert s.request_timeout == 30.0

    def test_default_max_output_tokens(self):
        s = Settings()
        assert s.max_output_tokens == 2048

    def test_default_concurrency_limit(self):
        s = Settings()
        assert s.concurrency_limit == 50

    def test_default_queue_size(self):
        s = Settings()
        assert s.queue_size == 200

    def test_default_queue_wait_timeout(self):
        s = Settings()
        assert s.queue_wait_timeout == 10.0

    def test_default_max_attempts(self):
        s = Settings()
        assert s.max_attempts == 3

    def test_feature_flags_off_by_default(self):
        s = Settings()
        assert s.reuse_http_client is False

    def test_pool_defaults(self):
        s = Settings()
        assert s.pool_max_keepalive == 100
        assert s.pool_max_connections == 100


class TestSettingsEnvOverride:
    def test_override_provider_name(self, monkeypatch):
        monkeypatch.setenv("PROVIDER_NAME", "openai")
        s = Settings()
        assert s.provider_name == "openai"

    def test_override_base_url(self, monkeypatch):
        monkeypatch.setenv("PROVIDER_BASE_URL", "https://api.openai.com")
        s = Settings()
        assert s.provider_base_url == "https://api.openai.com"

    def test_override_api_key(self, monkeypatch):
        monkeypatch.setenv("PROVIDER_API_KEY", "sk-test-key")
        s = Settings()
        assert s.provider_api_key == "sk-test-key"

    def test_override_timeout(self, monkeypatch):
        monkeypatch.setenv("REQUEST_TIMEOUT", "5.5")
        s = Settings()
        assert s.request_timeout == 5.5

    def test_override_max_attempts(self, monkeypatch):
        monkeypatch.setenv("MAX_ATTEMPTS", "5")
        s = Settings()
        assert s.max_attempts == 5

    def test_override_reuse_http_client(self, monkeypatch):
        monkeypatch.setenv("REUSE_HTTP_CLIENT", "true")
        s = Settings()
        assert s.reuse_http_client is True

    def test_override_concurrency_limit(self, monkeypatch):
        monkeypatch.setenv("CONCURRENCY_LIMIT", "10")
        s = Settings()
        assert s.concurrency_limit == 10


class TestSettingsValidation:
    def test_request_timeout_must_be_positive(self):
        with pytest.raises(ValidationError):
            Settings(request_timeout=0)

    def test_max_output_tokens_must_be_positive(self):
        with pytest.raises(ValidationError):
            Settings(max_output_tokens=0)

    def test_max_output_tokens_upper_bound(self):
        with pytest.raises(ValidationError):
            Settings(max_output_tokens=999_999)

    def test_max_attempts_minimum(self):
        with pytest.raises(ValidationError):
            Settings(max_attempts=0)

    def test_max_attempts_maximum(self):
        with pytest.raises(ValidationError):
            Settings(max_attempts=100)

    def test_concurrency_limit_must_be_positive(self):
        with pytest.raises(ValidationError):
            Settings(concurrency_limit=0)

    def test_queue_size_zero_allowed(self):
        # queue_size=0 means reject immediately at limit
        s = Settings(queue_size=0)
        assert s.queue_size == 0

    def test_get_settings_returns_settings(self):
        s = get_settings()
        assert isinstance(s, Settings)
