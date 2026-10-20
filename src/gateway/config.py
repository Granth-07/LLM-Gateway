"""
Gateway settings — all values sourced from environment variables.
Defaults are compatible with the local mock provider; no API key required.
"""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---------------------------------------------------------------------------
    # Provider identity
    # ---------------------------------------------------------------------------
    provider_name: str = Field(default="mock", description="Registered provider key")
    provider_base_url: str = Field(
        default="http://localhost:9000",
        description="Base URL for the upstream provider (no trailing slash)",
    )
    provider_api_key: str = Field(
        default="",
        description="API key — empty string is valid for the mock",
    )
    provider_model: str = Field(
        default="mock-model",
        description="Default model name sent to the provider",
    )

    # ---------------------------------------------------------------------------
    # Request timeouts
    # ---------------------------------------------------------------------------
    request_timeout: float = Field(
        default=30.0,
        gt=0,
        description="Total per-request HTTP timeout in seconds",
    )

    # ---------------------------------------------------------------------------
    # Generation limits
    # ---------------------------------------------------------------------------
    max_output_tokens: int = Field(
        default=2048,
        gt=0,
        le=65536,
        description="Default max tokens to generate",
    )

    # ---------------------------------------------------------------------------
    # Concurrency / queue
    # ---------------------------------------------------------------------------
    concurrency_limit: int = Field(
        default=50,
        gt=0,
        le=10_000,
        description="Maximum simultaneous in-flight provider requests",
    )
    queue_size: int = Field(
        default=200,
        ge=0,
        le=100_000,
        description="Bounded wait-queue depth; 0 means reject immediately when at limit",
    )
    queue_wait_timeout: float = Field(
        default=10.0,
        ge=0,
        description="Maximum seconds a request may wait in queue before 503",
    )
    limiter_retry_after: float = Field(
        default=1.0,
        ge=0,
        description="Default Retry-After in seconds when rejected by limiter",
    )

    # ---------------------------------------------------------------------------
    # Retry
    # ---------------------------------------------------------------------------
    max_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Total attempts per request (1 = no retries)",
    )
    initial_backoff: float = Field(
        default=0.5,
        ge=0.0,
        description="Initial backoff duration in seconds",
    )
    max_backoff: float = Field(
        default=10.0,
        ge=0.0,
        description="Maximum exponential backoff delay in seconds",
    )
    backoff_factor: float = Field(
        default=2.0,
        ge=1.0,
        description="Multiplier for exponential backoff growth",
    )
    retry_after_cap: float = Field(
        default=30.0,
        ge=0.0,
        description="Maximum cap for honored upstream Retry-After delays in seconds",
    )
    total_deadline: float = Field(
        default=60.0,
        gt=0.0,
        description="Total deadline for request in seconds; retries exceeding this are skipped",
    )
    retry_budget_ratio: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="Ratio of retries to total requests allowed by global budget",
    )
    retry_budget_floor: int = Field(
        default=10,
        ge=0,
        description="Baseline minimum retries allowed by global budget",
    )

    # ---------------------------------------------------------------------------
    # Feature flags — baseline profile has all OFF
    # ---------------------------------------------------------------------------
    reuse_http_client: bool = Field(
        default=False,
        description="Re-use a shared httpx.AsyncClient across requests (optimized profile)",
    )
    pool_max_keepalive: int = Field(
        default=100,
        gt=0,
        description="httpx connection pool max keepalive connections (optimized only)",
    )
    pool_max_connections: int = Field(
        default=100,
        gt=0,
        description="httpx connection pool max total connections (optimized only)",
    )
    release_slot_during_backoff: bool = Field(
        default=False,
        description="Release limiter slot during retry backoff sleep to let other requests execute (Exp3)",
    )


def get_settings() -> Settings:
    """Return a fresh Settings instance (reads env each call; cache externally if needed)."""
    return Settings()
