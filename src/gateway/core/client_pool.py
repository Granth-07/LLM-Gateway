"""
HTTP client pool and lifecycle manager.

Rules (from CLAUDE.md & prompt):
- reuse_http_client OFF => fresh httpx client per request, closed after.
- reuse_http_client ON => one shared client from lifespan with pool limits.
- Supports client creation count tracking for validation.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

import httpx

from gateway.config import Settings


class ClientPool:
    """
    Manages HTTP client instances based on reuse_http_client setting.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.settings = settings
        self.reuse = settings.reuse_http_client
        self.transport = transport
        self.created_count: int = 0
        self._shared_client: Optional[httpx.AsyncClient] = None
        self._closed: bool = False

    def _make_client(self) -> httpx.AsyncClient:
        """Instantiate a new httpx.AsyncClient and increment creation counter."""
        self.created_count += 1
        kwargs: dict[str, Any] = {
            "base_url": self.settings.provider_base_url,
        }
        if self.settings.provider_api_key:
            kwargs["headers"] = {"Authorization": f"Bearer {self.settings.provider_api_key}"}
        if self.transport is not None:
            kwargs["transport"] = self.transport
        if self.reuse:
            kwargs["limits"] = httpx.Limits(
                max_keepalive_connections=self.settings.pool_max_keepalive,
                max_connections=self.settings.pool_max_connections,
            )
        return httpx.AsyncClient(**kwargs)


    async def start(self) -> None:
        """Called during application lifespan startup."""
        if self.reuse and self._shared_client is None:
            self._shared_client = self._make_client()

    async def close(self) -> None:
        """Called during application lifespan shutdown."""
        self._closed = True
        if self._shared_client is not None:
            await self._shared_client.aclose()
            self._shared_client = None

    @asynccontextmanager
    async def get_client(self) -> AsyncIterator[httpx.AsyncClient]:
        """
        Yield an httpx.AsyncClient according to reuse_http_client mode.

        If reuse is True: yields the shared client without closing it.
        If reuse is False: yields a fresh client and closes it on exit.
        """
        if self.reuse:
            if self._shared_client is None:
                self._shared_client = self._make_client()
            yield self._shared_client
        else:
            client = self._make_client()
            try:
                yield client
            finally:
                await client.aclose()
