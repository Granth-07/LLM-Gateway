"""
FastAPI application factory with lifespan management and zero global state.

Rules (from CLAUDE.md & prompt):
- create_app(settings) factory with lifespan.
- No global state.
"""
from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import Callable, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError

from gateway.api.routes import router, validation_exception_handler
from gateway.config import Settings, get_settings
from gateway.core.client_pool import ClientPool
from gateway.core.pipeline import Pipeline, ProviderFactory


def create_app(
    settings: Optional[Settings] = None,
    *,
    client_pool: Optional[ClientPool] = None,
    pipeline: Optional[Pipeline] = None,
    provider_factory: Optional[ProviderFactory] = None,
) -> FastAPI:
    """
    Application factory for the LLM Gateway.

    Args:
        settings: Application settings. Defaults to fresh Settings from env.
        client_pool: Optional pre-configured ClientPool.
        pipeline: Optional pre-configured Pipeline.
        provider_factory: Optional custom provider factory (for testing).

    Returns:
        Configured FastAPI instance with isolated state.
    """
    effective_settings = settings or get_settings()
    pool = client_pool or ClientPool(effective_settings)
    pipe = pipeline or Pipeline(
        effective_settings,
        client_pool=pool,
        provider_factory=provider_factory,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await pool.start()
        try:
            yield
        finally:
            await pool.close()

    app = FastAPI(
        title="LLM Gateway",
        description="High-concurrency LLM Gateway",
        lifespan=lifespan,
    )

    # Attach isolated state to app instance
    app.state.settings = effective_settings
    app.state.client_pool = pool
    app.state.pipeline = pipe

    class RequestIDMiddleware:
        def __init__(self, inner_app):
            self.inner_app = inner_app

        async def __call__(self, scope, receive, send):
            if scope["type"] != "http":
                await self.inner_app(scope, receive, send)
                return

            req_id = None
            headers = scope.get("headers", [])
            for key, val in headers:
                if key.lower() == b"x-request-id":
                    req_id = val.decode("latin1")
                    break

            if not req_id:
                req_id = str(uuid.uuid4())

            state = scope.setdefault("state", {})
            state["request_id"] = req_id

            async def send_with_request_id(message):
                if message["type"] == "http.response.start":
                    res_headers = list(message.get("headers", []))
                    if not any(k.lower() == b"x-request-id" for k, _ in res_headers):
                        res_headers.append((b"x-request-id", req_id.encode("latin1")))
                    message["headers"] = res_headers
                await send(message)

            await self.inner_app(scope, receive, send_with_request_id)

    app.add_middleware(RequestIDMiddleware)

    # Exception handlers
    app.add_exception_handler(RequestValidationError, validation_exception_handler)

    # Register API routes
    app.include_router(router)

    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("gateway.app:create_app", factory=True, host="127.0.0.1", port=8000)
