"""
Unit tests for bench runner, aggregate, scenarios, and experiments logic.
All tests run with NO real network (using in-process ASGITransport or pure data).
"""
from __future__ import annotations

import asyncio
import csv
import io
import pytest
import httpx

from bench.aggregate import (
    MetricDistribution,
    RequestRecord,
    aggregate_run_records,
    compute_experiment_verdict,
    compute_trial_distribution,
    serialize_records_to_csv,
)
from bench.runner import WorkloadConfig, run_closed_loop_workload
from mock_provider.server import MockConfig, create_app as create_mock_app


# ---------------------------------------------------------------------------
# 1. Aggregation of known data -> expected percentiles / throughput
# ---------------------------------------------------------------------------

def test_aggregation_known_data() -> None:
    # 10 records with known client latencies: 0.1 to 1.0 s
    records = []
    for i in range(1, 11):
        records.append(
            RequestRecord(
                id=f"r-{i}",
                start_offset=float(i - 1) * 0.1,
                client_latency=float(i) * 0.1,  # 0.1, 0.2, ..., 1.0
                http_status=200 if i <= 9 else 500,  # 9 successes, 1 error
                error_kind=None if i <= 9 else "server_error",
                gateway_total=float(i) * 0.09,
                queue_wait=0.01,
                client_setup=0.005,
                provider_time=float(i) * 0.07,
                connect_time=0.005,
                backoff_sleep=0.0,
                response_processing=0.002,
                overhead=0.003,
            )
        )

    start_time = 100.0
    end_time = 105.0  # 5.0 seconds duration
    summary = aggregate_run_records(records, concurrency_c=2, start_time=start_time, end_time=end_time)

    assert summary["total_requests"] == 10
    assert summary["success_count"] == 9
    assert summary["error_count"] == 1
    assert summary["error_rate"] == 0.1
    # Throughput: 9 successes / 5.0 s = 1.8 rps
    assert summary["throughput_rps"] == 1.8

    # Latency percentiles (10 values 0.1 to 1.0)
    # p50 -> index ceil(0.5*10)-1 = 4 -> 0.5s
    # p95 -> index ceil(0.95*10)-1 = 9 -> 1.0s
    lat = summary["client_latency"]
    assert pytest.approx(lat["p50"], abs=1e-4) == 0.5
    assert pytest.approx(lat["p95"], abs=1e-4) == 1.0
    assert pytest.approx(lat["p99"], abs=1e-4) == 1.0
    assert pytest.approx(lat["min"], abs=1e-4) == 0.1
    assert pytest.approx(lat["max"], abs=1e-4) == 1.0

    # Checks
    checks = summary["checks"]
    assert checks["latency_violations"] == 0
    # Mean latency = 0.55s. Little's Law L = 1.8 * 0.55 = 0.99
    assert pytest.approx(checks["littles_law_L"], abs=0.1) == 0.99
    assert checks["littles_law_C"] == 2


# ---------------------------------------------------------------------------
# 2. Trial median and range
# ---------------------------------------------------------------------------

def test_trial_median_and_range() -> None:
    # 5 trials
    vals = [10.0, 15.0, 12.0, 20.0, 8.0]
    dist = compute_trial_distribution(vals)
    # sorted: 8.0, 10.0, 12.0, 15.0, 20.0
    assert dist.median == 12.0
    assert dist.min == 8.0
    assert dist.max == 20.0
    assert dist.range == 12.0

    # 3 trials
    vals3 = [50.0, 52.0, 48.0]
    dist3 = compute_trial_distribution(vals3)
    assert dist3.median == 50.0
    assert dist3.min == 48.0
    assert dist3.max == 52.0
    assert dist3.range == 4.0


# ---------------------------------------------------------------------------
# 3. Verdict logic: inside range => "no measurable difference"
# ---------------------------------------------------------------------------

def test_verdict_logic() -> None:
    # Case 1: Difference inside range -> "no measurable difference"
    # Baseline: median 100, range 15 (min 90, max 105)
    # Candidate: median 108, range 12 (min 102, max 114)
    # diff = +8, max_range = 15. diff < max_range => no measurable difference
    base = MetricDistribution(median=100.0, min=90.0, max=105.0, range=15.0, values=[90.0, 100.0, 105.0])
    cand = MetricDistribution(median=108.0, min=102.0, max=114.0, range=12.0, values=[102.0, 108.0, 114.0])
    res = compute_experiment_verdict(base, cand, higher_is_better=True)
    assert res["verdict"] == "no measurable difference"

    # Case 2: Difference exceeds range in good direction -> "improved"
    # Candidate: median 130, range 10. diff = +30 > max_range(15) => improved
    cand2 = MetricDistribution(median=130.0, min=125.0, max=135.0, range=10.0, values=[125.0, 130.0, 135.0])
    res2 = compute_experiment_verdict(base, cand2, higher_is_better=True)
    assert res2["verdict"] == "improved"

    # Case 3: Lower is better (latency). Baseline: 0.5s, Candidate: 0.2s. diff = -0.3s. Range = 0.05s => improved
    lat_base = MetricDistribution(median=0.50, min=0.48, max=0.52, range=0.04, values=[0.48, 0.50, 0.52])
    lat_cand = MetricDistribution(median=0.20, min=0.18, max=0.22, range=0.04, values=[0.18, 0.20, 0.22])
    res_lat = compute_experiment_verdict(lat_base, lat_cand, higher_is_better=False)
    assert res_lat["verdict"] == "improved"

    # Case 4: Degraded
    cand_worse = MetricDistribution(median=70.0, min=65.0, max=75.0, range=10.0, values=[65.0, 70.0, 75.0])
    res_worse = compute_experiment_verdict(base, cand_worse, higher_is_better=True)
    assert res_worse["verdict"] == "degraded"

    # Case 5: Pre-declared every_trial rule
    # Overlapping values -> inconclusive
    d_a = MetricDistribution(median=10.0, min=8.0, max=12.0, range=4.0, values=[8.0, 10.0, 12.0])
    d_b = MetricDistribution(median=13.0, min=11.0, max=15.0, range=4.0, values=[11.0, 13.0, 15.0])
    assert compute_experiment_verdict(d_a, d_b, rule="every_trial")["verdict"] == "inconclusive"

    # Strictly non-overlapping -> improved
    d_c = MetricDistribution(median=16.0, min=13.0, max=18.0, range=5.0, values=[13.0, 16.0, 18.0])
    assert compute_experiment_verdict(d_a, d_c, rule="every_trial")["verdict"] == "improved"


# ---------------------------------------------------------------------------
# 4. CSV schema
# ---------------------------------------------------------------------------

def test_csv_schema() -> None:
    rec = RequestRecord(
        id="req-123",
        start_offset=0.123456,
        client_latency=0.345678,
        http_status=200,
        error_kind=None,
        gateway_total=0.33,
        queue_wait=0.01,
        client_setup=0.002,
        provider_time=0.30,
        provider_duration=0.30,
        connect_time=0.005,
        backoff_sleep=0.0,
        response_processing=0.001,
        overhead=0.017,
    )
    csv_str = serialize_records_to_csv([rec])
    reader = csv.DictReader(io.StringIO(csv_str))
    rows = list(reader)

    assert len(rows) == 1
    row = rows[0]
    expected_fields = RequestRecord.csv_fieldnames()
    assert list(row.keys()) == expected_fields
    assert row["id"] == "req-123"
    assert row["http_status"] == "200"
    assert float(row["client_latency"]) == 0.345678
    assert float(row["gateway_total"]) == 0.33


# ---------------------------------------------------------------------------
# 5. Worker count bounded at C
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_count_bounded_at_c() -> None:
    """Verify that closed-loop generator runs exactly C tasks and concurrency <= C."""
    c_limit = 4
    n_requests = 20
    warmup = 2

    active_tasks = 0
    max_active_tasks = 0
    task_lock = asyncio.Lock()

    # Create dummy app that tracks active concurrent handlers
    from fastapi import FastAPI
    app = FastAPI()

    @app.post("/chat")
    async def dummy_chat():
        nonlocal active_tasks, max_active_tasks
        async with task_lock:
            active_tasks += 1
            if active_tasks > max_active_tasks:
                max_active_tasks = active_tasks
        await asyncio.sleep(0.01)
        async with task_lock:
            active_tasks -= 1
        return {"text": "ok", "timings": {"total": 0.01}}

    # We will test the worker loop directly with ASGITransport
    workload = WorkloadConfig(concurrency=c_limit, total_requests=n_requests, warmup_requests=warmup)

    transport = httpx.ASGITransport(app=app)
    # Monkeypatch httpx.AsyncClient inside run_closed_loop_workload to use ASGITransport
    import bench.runner as runner_mod

    orig_client = httpx.AsyncClient

    def mock_client_factory(**kwargs):
        return orig_client(transport=transport, base_url="http://testserver", **kwargs)

    # Run closed-loop workload with patched client
    import unittest.mock
    with unittest.mock.patch("httpx.AsyncClient", side_effect=mock_client_factory):
        records, start, end, success, msg = await runner_mod.run_closed_loop_workload("http://testserver", workload)

    # Total records = warmup + measured
    assert len(records) == n_requests + warmup
    # Concurrency must never exceed C
    assert max_active_tasks <= c_limit
    assert max_active_tasks > 1


# ---------------------------------------------------------------------------
# 6. Capacity mode behavior
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_capacity_mode_behavior() -> None:
    # 1. Reject mode: returns 503 when at capacity
    cfg_reject = MockConfig(latency_mean=-3.0, latency_sigma=0.001, max_capacity=1, capacity_mode="reject")
    app_reject = create_mock_app(cfg_reject)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_reject), base_url="http://test") as c:
        results = await asyncio.gather(
            *[c.post("/v1/chat/completions", json={"prompt": "hi"}) for _ in range(5)],
            return_exceptions=True,
        )
        statuses = [r.status_code for r in results if isinstance(r, httpx.Response)]
        assert 503 in statuses

    # 2. Queue mode (default): requests queue up and all succeed
    cfg_queue = MockConfig(latency_mean=-4.0, latency_sigma=0.001, max_capacity=1, capacity_mode="queue")
    app_queue = create_mock_app(cfg_queue)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_queue), base_url="http://test") as c:
        results = await asyncio.gather(
            *[c.post("/v1/chat/completions", json={"prompt": "hi"}) for _ in range(5)],
            return_exceptions=True,
        )
        statuses = [r.status_code for r in results if isinstance(r, httpx.Response)]
        assert statuses == [200, 200, 200, 200, 200]


# ---------------------------------------------------------------------------
# 7. Env file parsing
# ---------------------------------------------------------------------------

def test_parse_env_file(tmp_path) -> None:
    from bench.runner import parse_env_file

    env_content = """# Comment line
PROVIDER_NAME=openai
PROVIDER_MODEL="gpt-4o-mini"
PROVIDER_BASE_URL='https://api.openai.com/v1'
PROVIDER_API_KEY=test-secret-key-123

# Empty lines above and below
REQUEST_TIMEOUT=15.0
"""
    env_file = tmp_path / ".env.test"
    env_file.write_text(env_content, encoding="utf-8")

    parsed = parse_env_file(env_file)
    assert parsed["PROVIDER_NAME"] == "openai"
    assert parsed["PROVIDER_MODEL"] == "gpt-4o-mini"
    assert parsed["PROVIDER_BASE_URL"] == "https://api.openai.com/v1"
    assert parsed["PROVIDER_API_KEY"] == "test-secret-key-123"
    assert parsed["REQUEST_TIMEOUT"] == "15.0"

