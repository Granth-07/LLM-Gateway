# LLM Gateway: project rules

Goal: high-concurrency LLM gateway + reproducible benchmarks + honest report.
Stack: Python 3.11+, FastAPI, asyncio, httpx, pytest (+pytest-asyncio). No other web/HTTP libs.

## Architecture rules
- Provider-specific code lives ONLY in src/gateway/providers/. Core/pipeline never imports a concrete provider.
- Mock and real providers share one HTTP class (openai_compat.py). The mock is a separate process (mock_provider/server.py) speaking OpenAI-compatible /v1/chat/completions.
- Providers raise typed errors from errors.py (kind: timeout|rate_limit|server|connection|auth|client; fields: status, retryable, retry_after). Core decides retries, never providers.
- Settings come from env via config.py. Never commit secrets. .env.example only.
- Optimizations are feature flags in Settings. The baseline profile has them all OFF (e.g. new httpx client per request). Same code runs baseline and optimized.

## Concurrency rules
- Concurrency limit enforced by a slot manager with a BOUNDED wait queue and a queue-wait timeout. Overflow policy: reject with 503 + Retry-After. No unbounded task creation anywhere.
- In-flight = requests actually holding a slot and calling the provider. Increment after slot acquired, decrement in finally.
- Never swallow asyncio.CancelledError. Release slots in finally. Client disconnect must cancel upstream work.

## Metrics rules
- Use time.perf_counter() only. Never wall-clock for durations.
- Per request: total = queue_wait + provider_time + backoff_sleep + overhead. Intervals must not overlap. overhead = total - the other three.
- Track per-attempt failures AND final outcomes separately.
- Percentiles: one documented method (nearest-rank), implemented once in metrics/stats.py.
- Every metric is defined in docs/metrics.md.

## Retry rules
- Retry only: timeout, connection error, 429, 5xx. Never retry auth/4xx.
- Max attempts configurable; exponential backoff with full jitter; honor Retry-After (capped); global retry budget so retries cannot multiply traffic.

## Benchmark rules
- Run gateway, mock provider and load generator as separate processes.
- Concurrency levels 1, 10, 25, 50; at least 3 trials at 50. Save raw per-request results (JSON/CSV) and a summary.
- Verify the gateway's in-flight tracking against the mock's own peak-concurrency stats.
- State clearly that plateau/saturation behavior comes from the mock's capacity model, not from a real provider.
- Real-provider runs are small and cost-bounded. Never invent provider comparison results.

## Quality rules
- Tests must run with no network and no API keys. Avoid wall-clock threshold asserts; use fake clocks/transports or assert counts/ordering.
- Type hints everywhere. Small modules. No dead code.
- ALWAYS run code and tests before claiming done; paste the actual command output. If something fails or cannot be verified, say so; never claim results you did not measure.
- Benchmarks: never fabricate numbers. Report what was measured, with trial variance. Separate measured facts from hypotheses in the report.
- Investigate root cause before applying a fix.
- Stay inside the file domain named in the prompt. Do not edit other domains; list required changes there instead.
- Keep responses short: summary of changes, real command output, open issues.

## Folder layout
llm-gateway/
- CLAUDE.md, README.md, REPORT.md, pyproject.toml, .env.example, .gitignore
- docs/metrics.md
- src/gateway/: config.py, schemas.py, errors.py, app.py
  - api/routes.py
  - core/: pipeline.py, limiter.py, retry.py, timing.py
  - metrics/: collector.py, stats.py
  - providers/: base.py, openai_compat.py, registry.py
- mock_provider/server.py
- bench/: runner.py, scenarios.py, aggregate.py, experiments.py
- tests/: conftest.py, unit/, integration/
- results/: raw/, summary/
