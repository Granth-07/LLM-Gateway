"""
Provider ABC and ProviderResult dataclass.
"""
from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field
from typing import Optional

from gateway.schemas import ChatRequest, TokenUsage


@dataclass
class ProviderResult:
    """
    Returned by every provider implementation.

    Timing fields use time.perf_counter() exclusively (CLAUDE.md rule).
    - provider_duration: wall time from before send() to after response read.
    - connect_time: TCP connect duration (0.0 when connection was pooled/reused).
    - connection_reused: True when no new TCP connection was established.
    """

    text: str
    provider: str
    model: str
    usage: TokenUsage
    http_status: int
    provider_duration: float   # perf_counter seconds
    connect_time: float = field(default=0.0)        # 0.0 when pooled
    connection_reused: bool = field(default=False)  # True when pooled
    finish_reason: Optional[str] = field(default=None)

    @classmethod
    def create(
        cls,
        *,
        text: str,
        provider: str,
        model: str,
        usage: TokenUsage,
        http_status: int,
        start: float,
        connect_time: float = 0.0,
        connection_reused: bool = False,
    ) -> "ProviderResult":
        """
        Convenience constructor that records provider_duration automatically.
        Call with start=time.perf_counter() captured before the HTTP call.
        """
        return cls(
            text=text,
            provider=provider,
            model=model,
            usage=usage,
            http_status=http_status,
            provider_duration=time.perf_counter() - start,
            connect_time=connect_time,
            connection_reused=connection_reused,
        )


class Provider(abc.ABC):
    """Abstract base class all provider implementations must satisfy."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Unique provider identifier (matches registry key)."""
        ...

    @abc.abstractmethod
    async def complete(
        self,
        request: ChatRequest,
        *,
        timeout: Optional[float] = None,
    ) -> ProviderResult:
        """
        Send a chat completion request and return a ProviderResult.

        Raises:
            gateway.errors.ProviderError subclass on any failure.
        """
        ...
