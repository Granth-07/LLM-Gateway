"""
Pydantic schemas for the gateway public API.
"""
from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    """Validated chat completion request from the client."""

    prompt: str = Field(
        ...,
        min_length=1,
        max_length=32_768,
        description="User prompt text",
    )
    model: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=256,
        description="Override the default model for this request",
    )
    provider: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=64,
        description="Override the default provider for this request",
    )
    max_tokens: Optional[int] = Field(
        default=None,
        gt=0,
        le=65_536,
        description="Maximum tokens to generate; falls back to settings default",
    )
    timeout: Optional[float] = Field(
        default=None,
        gt=0,
        le=300.0,
        description="Per-request timeout override in seconds",
    )


# ---------------------------------------------------------------------------
# Usage / timings sub-models
# ---------------------------------------------------------------------------

class TokenUsage(BaseModel):
    """Token counts as reported by the provider."""

    prompt_tokens: int = Field(default=0, ge=0)
    completion_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)


class RequestTimings(BaseModel):
    """
    Per-request timing breakdown (all in seconds, measured with time.perf_counter).
    Populated fully by the pipeline.
    """

    queue_wait: float = Field(default=0.0, ge=0.0)
    client_setup: float = Field(default=0.0, ge=0.0)
    provider_time: float = Field(default=0.0, ge=0.0)
    provider_duration: float = Field(default=0.0, ge=0.0)
    backoff_sleep: float = Field(default=0.0, ge=0.0)
    response_processing: float = Field(default=0.0, ge=0.0)
    overhead: float = Field(default=0.0, ge=0.0)
    total: float = Field(default=0.0, ge=0.0)
    connect_time: float = Field(default=0.0, ge=0.0)

    @model_validator(mode="after")
    def _sync_provider_duration(self) -> "RequestTimings":
        if self.provider_time > 0 and self.provider_duration == 0:
            self.provider_duration = self.provider_time
        elif self.provider_duration > 0 and self.provider_time == 0:
            self.provider_time = self.provider_duration
        return self


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------

class ChatResponse(BaseModel):
    """Successful chat completion response returned to the client."""

    text: str = Field(..., description="Generated text from the provider")
    provider: str = Field(..., description="Actual provider that served the request")
    model: str = Field(..., description="Actual model used")
    usage: TokenUsage = Field(default_factory=TokenUsage)
    request_id: str = Field(..., description="Opaque request identifier (UUID)")
    timings: RequestTimings = Field(default_factory=RequestTimings)


# ---------------------------------------------------------------------------
# Error body
# ---------------------------------------------------------------------------

class ErrorBody(BaseModel):
    """Structured error returned on non-2xx responses."""

    error: str = Field(..., description="Machine-readable error kind")
    message: str = Field(..., description="Human-readable description")
    retryable: bool = Field(default=False)
    retry_after: Optional[float] = Field(
        default=None,
        description="Seconds the client should wait before retrying (when retryable)",
    )
    request_id: Optional[str] = Field(default=None)
