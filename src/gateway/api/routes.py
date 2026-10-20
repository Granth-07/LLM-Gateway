"""
Gateway API routes and error response handlers.

Rules (from CLAUDE.md & prompt):
- POST /chat, GET /health, GET /stats
- Errors: validation 422, unknown provider 400, timeout 504,
  upstream 429 => 429 + Retry-After passthrough,
  limiter full/timeout => 503 + Retry-After, distinct reason codes (queue_full, queue_timeout),
  5xx/connection/upstream auth => 502 (never leak keys, never 401).
- Cancellation: on client disconnect, cancel pipeline task (race against request.is_disconnected()).
  One watcher per request, no other spawned tasks. Slot released. Never swallow CancelledError.
- request_id on every response.
- One ErrorBody shape.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any

from fastapi import APIRouter, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from gateway import errors as errs
from gateway.core.limiter import LimiterError
from gateway.schemas import ChatRequest, ChatResponse, ErrorBody

router = APIRouter()


def _get_request_id(request: Request) -> str:
    """Extract or generate request_id."""
    return getattr(request.state, "request_id", None) or request.headers.get("X-Request-ID") or str(uuid.uuid4())


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Format Pydantic / FastAPI validation errors into standard ErrorBody with 422."""
    request_id = _get_request_id(request)
    details = "; ".join(f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in exc.errors())
    return JSONResponse(
        status_code=422,
        content=ErrorBody(
            error="validation_error",
            message=f"Request validation failed: {details}",
            retryable=False,
            request_id=request_id,
        ).model_dump(),
        headers={"X-Request-ID": request_id},
    )


@router.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@router.get("/stats")
async def stats(raw_request: Request) -> JSONResponse:
    """Gateway runtime stats including attempts, finals, and limiter metrics."""
    pipeline = raw_request.app.state.pipeline
    return JSONResponse(content=pipeline.get_stats())


@router.get("/metrics")
async def metrics(raw_request: Request) -> JSONResponse:
    """Gateway JSON metrics (percentiles, throughput, error rates, peak gauges)."""
    pipeline = raw_request.app.state.pipeline
    return JSONResponse(content=pipeline.collector.get_metrics(pipeline.limiter))


@router.post("/metrics/reset")
async def metrics_reset(raw_request: Request) -> JSONResponse:
    """Clear collector, limiter peak gauges, and overhead clamp counters."""
    from gateway.core.timing import reset_clamped_overhead_count

    pipeline = raw_request.app.state.pipeline
    pipeline.collector.reset()
    pipeline.limiter.reset_peaks()
    reset_clamped_overhead_count()
    return JSONResponse(content={"status": "reset"})


async def _disconnect_watcher(req: Request) -> bool:
    """
    Polls request.is_disconnected() until disconnected.
    One watcher per request.
    """
    while True:
        if await req.is_disconnected():
            return True
        await asyncio.sleep(0.02)


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, raw_request: Request) -> Any:
    """
    Handle chat completion request through the gateway pipeline.
    Races pipeline against client disconnect to promptly cancel and release slots.
    """
    request_id = _get_request_id(raw_request)
    pipeline = raw_request.app.state.pipeline

    # Spawn one pipeline task and one watcher task to race against client disconnect
    pipeline_task = asyncio.create_task(pipeline.handle(request, request_id=request_id))
    watcher_task = asyncio.create_task(_disconnect_watcher(raw_request))

    try:
        done, _ = await asyncio.wait(
            [pipeline_task, watcher_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if pipeline_task in done:
            # Normal completion (or exception raised in pipeline)
            watcher_task.cancel()
            response = await pipeline_task
            return response
        else:
            # Client disconnected before pipeline completed!
            # Cancel the pipeline task so slot is freed in its finally block
            pipeline_task.cancel()
            try:
                await pipeline_task
            except asyncio.CancelledError:
                pass
            watcher_task.cancel()
            raise asyncio.CancelledError()

    except asyncio.CancelledError:
        # Never swallow CancelledError; ensure tasks are cleaned up
        if not pipeline_task.done():
            pipeline_task.cancel()
            try:
                await pipeline_task
            except asyncio.CancelledError:
                pass
        if not watcher_task.done():
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass
        raise

    except LimiterError as exc:
        headers = {"X-Request-ID": request_id}
        if exc.retry_after is not None:
            headers["Retry-After"] = str(int(exc.retry_after) if exc.retry_after.is_integer() else exc.retry_after)
        return JSONResponse(
            status_code=503,
            content=ErrorBody(
                error=exc.reason,
                message=exc.message,
                retryable=True,
                retry_after=exc.retry_after,
                request_id=request_id,
            ).model_dump(),
            headers=headers,
        )

    except ValueError as exc:
        # Unknown provider requested
        return JSONResponse(
            status_code=400,
            content=ErrorBody(
                error="unknown_provider",
                message=str(exc),
                retryable=False,
                request_id=request_id,
            ).model_dump(),
            headers={"X-Request-ID": request_id},
        )

    except errs.TimeoutError:
        return JSONResponse(
            status_code=504,
            content=ErrorBody(
                error="upstream_timeout",
                message="Upstream provider timed out",
                retryable=True,
                request_id=request_id,
            ).model_dump(),
            headers={"X-Request-ID": request_id},
        )

    except errs.RateLimitError as exc:
        headers = {"X-Request-ID": request_id}
        if exc.retry_after is not None:
            headers["Retry-After"] = str(int(exc.retry_after) if exc.retry_after.is_integer() else exc.retry_after)
        return JSONResponse(
            status_code=429,
            content=ErrorBody(
                error="rate_limit_exceeded",
                message="Upstream rate limit exceeded",
                retryable=True,
                retry_after=exc.retry_after,
                request_id=request_id,
            ).model_dump(),
            headers=headers,
        )

    except errs.AuthError:
        # Never return 401; never leak upstream keys
        return JSONResponse(
            status_code=502,
            content=ErrorBody(
                error="bad_gateway",
                message="Upstream provider authentication failed",
                retryable=False,
                request_id=request_id,
            ).model_dump(),
            headers={"X-Request-ID": request_id},
        )

    except errs.ConnectionError:
        return JSONResponse(
            status_code=502,
            content=ErrorBody(
                error="bad_gateway",
                message="Failed to connect to upstream provider",
                retryable=True,
                request_id=request_id,
            ).model_dump(),
            headers={"X-Request-ID": request_id},
        )

    except errs.ServerError:
        return JSONResponse(
            status_code=502,
            content=ErrorBody(
                error="bad_gateway",
                message="Upstream provider returned an error",
                retryable=True,
                request_id=request_id,
            ).model_dump(),
            headers={"X-Request-ID": request_id},
        )

    except (errs.ClientError, errs.ProviderError) as exc:
        return JSONResponse(
            status_code=502,
            content=ErrorBody(
                error="bad_gateway",
                message="Upstream provider rejected the request",
                retryable=exc.retryable,
                request_id=request_id,
            ).model_dump(),
            headers={"X-Request-ID": request_id},
        )
