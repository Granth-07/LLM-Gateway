# LLM Gateway

High-concurrency LLM gateway with strict admission control, bounded queues, and reproducible benchmarks.

**System Environment**: Windows-11-10.0.26300, Intel64 Family 6 Model 151, 12 logical CPUs [Measured].

## Installation
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

## Running Components
Start mock provider:
```powershell
.venv\Scripts\python.exe -m mock_provider.server --port 9000
```
Start gateway (reads `.env` or defaults to mock):
```powershell
.venv\Scripts\uvicorn.exe src.gateway.app:app --port 8000
```
Curl completion request:
```powershell
curl -X POST http://localhost:8000/v1/chat/completions -H "Content-Type: application/json" -d "{\"model\":\"mock-model\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}"
```

## Running Tests
```powershell
.venv\Scripts\pytest.exe -q
```

## Benchmarks & Experiments
Run a quickstart benchmark trial:
```powershell
.venv\Scripts\python.exe -m bench.runner --name quickstart_run -C 2 -N 10 --warmup 2
# Output:
# Running benchmark: name=quickstart_run, C=2, N=10
# Done! Throughput: 5.41 rps | p50: 0.314624s | p95: 0.476613s | p99: 0.476613s | Errors: 0/10
```
Run full scenario matrix or A/B experiments:
```powershell
.venv\Scripts\python.exe -m bench.scenarios
.venv\Scripts\python.exe -m bench.experiments
.venv\Scripts\python.exe -m bench.runner --real --env-file .env.real
```

## Configuration & Profiles
- `.env.example`: Safe mock defaults, tracked in git.
- `.env.real`: Upstream API credentials and real provider settings, git-ignored.
- Environment variables: `PROVIDER_NAME`, `PROVIDER_BASE_URL`, `PROVIDER_API_KEY`, `PROVIDER_MODEL`, `CONCURRENCY_LIMIT`, `QUEUE_SIZE`, `REQUEST_TIMEOUT`.
- Profiles:
  - `baseline`: `reuse_http_client=false`, `pool_max_keepalive=20`, `pool_max_connections=100`.
  - `optimized`: `reuse_http_client=true`, `pool_max_keepalive=100`, `pool_max_connections=100`.

## Folder Layout
- `src/gateway/`: Gateway core, limiter, retries, metrics collector, providers.
- `mock_provider/`: Standalone mock server implementing OpenAI `/v1/chat/completions`.
- `bench/`: Closed-loop load generator, scenario matrix, experiment runners.
- `docs/`: Timing residual breakdown and metrics definitions (`metrics.md`).
- `tests/`: Unit and integration test suites.
- `results/`: Benchmark summaries, markdown tables (`tables.md`), and raw trial data.
