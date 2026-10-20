"""
Typed provider error hierarchy.

Rules (from CLAUDE.md):
- kind: timeout | rate_limit | server | connection | auth | client
- Providers raise these; core decides retries.
- Never retry auth (kind=auth) or generic client errors (kind=client).
"""
from __future__ import annotations

from typing import Optional


class ProviderError(Exception):
    """Base class for all provider-raised errors."""

    # Subclasses must declare these
    kind: str = "unknown"
    default_status: int = 500
    default_retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        retryable: Optional[bool] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status: int = status if status is not None else self.default_status
        self.retryable: bool = retryable if retryable is not None else self.default_retryable
        self.retry_after: Optional[float] = retry_after

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"kind={self.kind!r}, "
            f"status={self.status}, "
            f"retryable={self.retryable}, "
            f"retry_after={self.retry_after}, "
            f"message={self.message!r})"
        )


# ---------------------------------------------------------------------------
# Concrete error types
# ---------------------------------------------------------------------------

class TimeoutError(ProviderError):
    """Provider did not respond within the configured timeout."""

    kind = "timeout"
    default_status = 504
    default_retryable = True


class RateLimitError(ProviderError):
    """Provider returned HTTP 429 Too Many Requests."""

    kind = "rate_limit"
    default_status = 429
    default_retryable = True


class ServerError(ProviderError):
    """Provider returned a 5xx error."""

    kind = "server"
    default_status = 502
    default_retryable = True


class ConnectionError(ProviderError):
    """Network-level connection failure (refused, reset, DNS, etc.)."""

    kind = "connection"
    default_status = 503
    default_retryable = True


class AuthError(ProviderError):
    """Provider rejected the API key (401)."""

    kind = "auth"
    default_status = 401
    default_retryable = False


class ClientError(ProviderError):
    """Provider returned a 4xx error other than 401/429."""

    kind = "client"
    default_status = 400
    default_retryable = False


# ---------------------------------------------------------------------------
# Mapping helper
# ---------------------------------------------------------------------------

_STATUS_MAP: dict[int, type[ProviderError]] = {
    401: AuthError,
    429: RateLimitError,
}


def from_http_status(
    status: int,
    message: str,
    *,
    retry_after: Optional[float] = None,
) -> ProviderError:
    """
    Construct the appropriate ProviderError subclass from an HTTP status code.

    4xx → AuthError (401), RateLimitError (429), ClientError (other)
    5xx → ServerError
    """
    if status in _STATUS_MAP:
        cls = _STATUS_MAP[status]
    elif 400 <= status < 500:
        cls = ClientError
    elif 500 <= status < 600:
        cls = ServerError
    else:
        cls = ProviderError

    return cls(message, status=status, retry_after=retry_after)
