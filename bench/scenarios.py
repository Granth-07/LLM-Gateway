"""
Scenario definitions and matrix execution.

Scenarios:
- clean: mock capacity 100, queue mode, gateway limit 50
- capacity20: mock capacity 20, queue mode, gateway limit 50
- failures: 429 (10% with Retry-After), 500 (5%), slow tail (5%), conn drop (2%)
- overflow: gateway limit 10, queue 10, C=50

Matrix:
- clean & capacity20 at C=1,10,25,50 (N=30/150/300/500), 3 trials at C=50
- failures at C=10 (N=150) and C=50 (N=500, 3 trials)
- overflow once at C=50 (N=100)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from bench.aggregate import MetricDistribution, compute_trial_distribution
from bench.runner import (
    DEFAULT_RESULTS_DIR,
    ServerConfig,
    SubprocessManager,
    WorkloadConfig,
    run_trial,
)


# Mock median latency 0.3s -> lognormal mean = ln(0.3) ≈ -1.20397
MEDIAN_LATENCY_MEAN = -1.20397
DEFAULT_SIGMA = 0.25
FIXED_SEED = 42


def get_scenario_server_config(
    scenario_name: str,
    *,
    profile: str = "baseline",
    mock_port: int = 9000,
    gateway_port: int = 8000,
    seed: int = FIXED_SEED,
) -> ServerConfig:
    """Build ServerConfig for named scenario with chosen profile."""
    from bench.runner import apply_profile

    base = ServerConfig(
        mock_port=mock_port,
        gateway_port=gateway_port,
        mock_latency_mean=MEDIAN_LATENCY_MEAN,
        mock_latency_sigma=DEFAULT_SIGMA,
        mock_seed=seed,
    )
    apply_profile(base, profile)

    if scenario_name == "clean":
        base.mock_max_capacity = 100
        base.mock_capacity_mode = "queue"
        base.concurrency_limit = 50
        base.queue_size = 200

    elif scenario_name == "capacity20":
        base.mock_max_capacity = 20
        base.mock_capacity_mode = "queue"
        base.concurrency_limit = 50
        base.queue_size = 200

    elif scenario_name == "failures":
        base.mock_max_capacity = 100
        base.mock_capacity_mode = "queue"
        base.mock_error_rate_429 = 0.10
        base.mock_error_rate_500 = 0.05
        base.mock_slow_tail_prob = 0.05
        base.mock_drop_prob = 0.02
        base.concurrency_limit = 50
        base.queue_size = 200

    elif scenario_name == "failures_v2":
        base.mock_max_capacity = 100
        base.mock_capacity_mode = "queue"
        base.mock_error_rate_429 = 0.10
        base.mock_error_rate_500 = 0.05
        base.mock_slow_tail_prob = 0.05
        base.mock_slow_tail_latency_s = 4.0
        base.mock_drop_prob = 0.02
        base.concurrency_limit = 50
        base.queue_size = 200
        base.request_timeout = 1.5

    elif scenario_name == "overflow":
        base.mock_max_capacity = 100
        base.mock_capacity_mode = "queue"
        base.concurrency_limit = 10
        base.queue_size = 10

    else:
        raise ValueError(f"Unknown scenario: {scenario_name}")

    return base


@dataclass
class MatrixItem:
    scenario: str
    concurrency_c: int
    requests_n: int
    trials: int


def get_full_matrix_items() -> list[MatrixItem]:
    """Define the benchmark matrix items."""
    return [
        # clean at C=1, 10, 25, 50 (3 trials each)
        MatrixItem("clean", 1, 30, 3),
        MatrixItem("clean", 10, 150, 3),
        MatrixItem("clean", 25, 300, 3),
        MatrixItem("clean", 50, 500, 3),

        # capacity20 at C=1, 10, 25, 50 (3 trials each)
        MatrixItem("capacity20", 1, 30, 3),
        MatrixItem("capacity20", 10, 150, 3),
        MatrixItem("capacity20", 25, 300, 3),
        MatrixItem("capacity20", 50, 500, 3),

        # failures at C=10 (N=150, 3 trials) and C=50 (N=500, 3 trials)
        MatrixItem("failures", 10, 150, 3),
        MatrixItem("failures", 50, 500, 3),

        # overflow once at C=50 (N=100)
        MatrixItem("overflow", 50, 100, 1),
    ]


async def run_scenario_matrix(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    mock_port: int = 9000,
    gateway_port: int = 8000,
    seed: int = FIXED_SEED,
) -> dict[str, Any]:
    """
    Run the full benchmark matrix once.
    Reuses or manages servers per scenario.
    Returns structured results for reporting.
    """
    matrix_items = get_full_matrix_items()
    matrix_results: list[dict[str, Any]] = []
    t_start = time.perf_counter()

    for item in matrix_items:
        server_cfg = get_scenario_server_config(
            item.scenario,
            mock_port=mock_port,
            gateway_port=gateway_port,
            seed=seed,
        )

        subproc_mgr = SubprocessManager(server_cfg)
        subproc_mgr.start_mock()
        subproc_mgr.start_gateway()
        healthy = await subproc_mgr.wait_healthy()
        if not healthy:
            subproc_mgr.shutdown()
            raise RuntimeError(f"Failed to start servers for {item.scenario}")

        trial_summaries: list[dict[str, Any]] = []
        try:
            for t_idx in range(1, item.trials + 1):
                trial_name = f"{item.scenario}_C{item.concurrency_c}_t{t_idx}"
                workload_cfg = WorkloadConfig(
                    concurrency=item.concurrency_c,
                    total_requests=item.requests_n,
                    warmup_requests=10,
                    seed=seed + t_idx,
                )
                print(f"[MATRIX] Running {trial_name} (N={item.requests_n}, C={item.concurrency_c})...")
                res = await run_trial(
                    server_cfg,
                    workload_cfg,
                    trial_name=trial_name,
                    results_dir=results_dir,
                    subproc_mgr=subproc_mgr,
                )
                trial_summaries.append(res)
        finally:
            subproc_mgr.shutdown()

        # Aggregate across trials
        throughputs = [ts["summary"]["throughput_rps"] for ts in trial_summaries]
        p50s = [ts["summary"]["client_latency"]["p50"] for ts in trial_summaries]
        p95s = [ts["summary"]["client_latency"]["p95"] for ts in trial_summaries]
        p99s = [ts["summary"]["client_latency"]["p99"] for ts in trial_summaries]
        error_rates = [ts["summary"]["error_rate"] for ts in trial_summaries]

        item_result = {
            "scenario": item.scenario,
            "concurrency_C": item.concurrency_c,
            "requests_N": item.requests_n,
            "trials_count": item.trials,
            "throughput_dist": asdict(compute_trial_distribution(throughputs)),
            "p50_dist": asdict(compute_trial_distribution(p50s)),
            "p95_dist": asdict(compute_trial_distribution(p95s)),
            "p99_dist": asdict(compute_trial_distribution(p99s)),
            "error_rate_dist": asdict(compute_trial_distribution(error_rates)),
            "checks": trial_summaries[-1]["summary"]["checks"],
        }
        matrix_results.append(item_result)

    total_duration = time.perf_counter() - t_start
    final_output = {
        "timestamp": time.time(),
        "total_runtime_seconds": round(total_duration, 2),
        "matrix_results": matrix_results,
    }

    matrix_file = results_dir / "summary" / "matrix_summary.json"
    matrix_file.write_text(json.dumps(final_output, indent=2), encoding="utf-8")

    try:
        from bench.aggregate import generate_markdown_tables
        generate_markdown_tables(results_dir)
    except Exception as e:
        print(f"Warning: could not generate markdown tables: {e}")

    return final_output


async def rerun_capacity20(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    mock_port: int = 9000,
    gateway_port: int = 8000,
    seed: int = FIXED_SEED,
    cooldown_seconds: float = 20.0,
) -> dict[str, Any]:
    """
    Rerun capacity20 (mock capacity 20, queue mode) with:
    1. Optimized profile at C=1, 10, 25, 50 (3 trials each)
    2. Baseline capacity20 at C=50 (3 more trials)
    - Records trial index and connect_time p50 per trial.
    - Inserts a 20s cool-down between trials.
    - Archives superseded files with _run2 suffix.
    """
    sum_dir = results_dir / "summary"
    raw_dir = results_dir / "raw"
    sum_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    # 1. Archive superseded capacity20 baseline C=50 files from previous run
    for t_idx in (1, 2, 3):
        for directory, ext in [(sum_dir, ".json"), (raw_dir, ".json"), (raw_dir, ".csv")]:
            src = directory / f"capacity20_C50_t{t_idx}{ext}"
            dst = directory / f"capacity20_C50_t{t_idx}_run2{ext}"
            if src.exists() and not dst.exists():
                src.rename(dst)
                print(f"[ARCHIVE] Renamed superseded file {src.name} -> {dst.name}")

    trial_log: list[dict[str, Any]] = []
    global_trial_idx = 0
    t_start = time.perf_counter()

    # Step 1: Optimized profile at C=1, 10, 25, 50 (3 trials each)
    opt_matrix = [
        (1, 30),
        (10, 150),
        (25, 300),
        (50, 500),
    ]

    for c_val, n_req in opt_matrix:
        s_cfg = get_scenario_server_config(
            "capacity20",
            profile="optimized",
            mock_port=mock_port,
            gateway_port=gateway_port,
            seed=seed,
        )
        subproc = SubprocessManager(s_cfg)
        subproc.start_mock()
        subproc.start_gateway()
        healthy = await subproc.wait_healthy()
        if not healthy:
            subproc.shutdown()
            raise RuntimeError(f"Server failed to start for capacity20_opt C={c_val}")

        try:
            for t_idx in range(1, 4):
                global_trial_idx += 1
                trial_name = f"capacity20_opt_C{c_val}_t{t_idx}"
                w_cfg = WorkloadConfig(
                    concurrency=c_val,
                    total_requests=n_req,
                    warmup_requests=10,
                    seed=seed + t_idx,
                )
                print(f"\n[Trial #{global_trial_idx}] Running {trial_name} (profile=optimized, C={c_val}, N={n_req})...")
                res = await run_trial(
                    s_cfg,
                    w_cfg,
                    trial_name=trial_name,
                    results_dir=results_dir,
                    subproc_mgr=subproc,
                )
                sm = res["summary"]
                gw_lat = sm.get("gateway_latency", {})
                entry = {
                    "global_trial_index": global_trial_idx,
                    "trial_name": trial_name,
                    "profile": "optimized",
                    "concurrency_C": c_val,
                    "trial_num": t_idx,
                    "throughput_rps": sm.get("throughput_rps", 0.0),
                    "client_latency_p50": sm.get("client_latency", {}).get("p50", 0.0),
                    "client_latency_p95": sm.get("client_latency", {}).get("p95", 0.0),
                    "connect_time_p50": gw_lat.get("connect_time", {}).get("p50", 0.0),
                    "provider_time_p50": gw_lat.get("provider_time", {}).get("p50", 0.0),
                    "connection_reused_fraction": sm.get("connection_reused_fraction", 0.0),
                    "mock_peak_concurrent": sm.get("checks", {}).get("mock_peak_concurrent", 0),
                }
                trial_log.append(entry)
                print(f"  Result: Throughput={entry['throughput_rps']:.2f} rps | p95={entry['client_latency_p95']:.3f}s | "
                      f"connect_p50={entry['connect_time_p50']:.4f}s | reuse_frac={entry['connection_reused_fraction']:.2f}")

                # 20s cooldown between trials
                if cooldown_seconds > 0:
                    print(f"  Cooling down for {cooldown_seconds}s before next trial...")
                    await asyncio.sleep(cooldown_seconds)
        finally:
            subproc.shutdown()

    # Step 2: Baseline profile at C=50 (3 more trials)
    s_cfg_base = get_scenario_server_config(
        "capacity20",
        profile="baseline",
        mock_port=mock_port,
        gateway_port=gateway_port,
        seed=seed,
    )
    subproc_base = SubprocessManager(s_cfg_base)
    subproc_base.start_mock()
    subproc_base.start_gateway()
    healthy = await subproc_base.wait_healthy()
    if not healthy:
        subproc_base.shutdown()
        raise RuntimeError(f"Server failed to start for baseline capacity20 C=50")

    try:
        for t_idx in range(1, 4):
            global_trial_idx += 1
            trial_name = f"capacity20_C50_t{t_idx}"
            w_cfg = WorkloadConfig(
                concurrency=50,
                total_requests=500,
                warmup_requests=10,
                seed=seed + t_idx,
            )
            print(f"\n[Trial #{global_trial_idx}] Running {trial_name} (profile=baseline, C=50, N=500)...")
            res = await run_trial(
                s_cfg_base,
                w_cfg,
                trial_name=trial_name,
                results_dir=results_dir,
                subproc_mgr=subproc_base,
            )
            sm = res["summary"]
            gw_lat = sm.get("gateway_latency", {})
            entry = {
                "global_trial_index": global_trial_idx,
                "trial_name": trial_name,
                "profile": "baseline",
                "concurrency_C": 50,
                "trial_num": t_idx,
                "throughput_rps": sm.get("throughput_rps", 0.0),
                "client_latency_p50": sm.get("client_latency", {}).get("p50", 0.0),
                "client_latency_p95": sm.get("client_latency", {}).get("p95", 0.0),
                "connect_time_p50": gw_lat.get("connect_time", {}).get("p50", 0.0),
                "provider_time_p50": gw_lat.get("provider_time", {}).get("p50", 0.0),
                "connection_reused_fraction": sm.get("connection_reused_fraction", 0.0),
                "mock_peak_concurrent": sm.get("checks", {}).get("mock_peak_concurrent", 0),
            }
            trial_log.append(entry)
            print(f"  Result: Throughput={entry['throughput_rps']:.2f} rps | p95={entry['client_latency_p95']:.3f}s | "
                  f"connect_p50={entry['connect_time_p50']:.4f}s | provider_p50={entry['provider_time_p50']:.4f}s")

            # 20s cooldown between trials
            if t_idx < 3 and cooldown_seconds > 0:
                print(f"  Cooling down for {cooldown_seconds}s before next trial...")
                await asyncio.sleep(cooldown_seconds)
    finally:
        subproc_base.shutdown()

    total_duration = time.perf_counter() - t_start
    final_output = {
        "timestamp": time.time(),
        "total_runtime_seconds": round(total_duration, 2),
        "cooldown_seconds": cooldown_seconds,
        "trials": trial_log,
    }

    out_file = sum_dir / "capacity20_rerun_summary.json"
    out_file.write_text(json.dumps(final_output, indent=2), encoding="utf-8")

    # Generate markdown tables
    from bench.aggregate import generate_markdown_tables
    generate_markdown_tables(results_dir)

    return final_output


async def run_failures_v2(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    mock_port: int = 9000,
    gateway_port: int = 8000,
    seed: int = FIXED_SEED,
    cooldown_seconds: float = 20.0,
) -> dict[str, Any]:
    """
    Run failures_v2 scenario:
    - 429 10% with Retry-After, 500 5%, drop 2%, slow tail 5% at 4s
    - Gateway REQUEST_TIMEOUT = 1.5s (triggers real timeouts on slow tail)
    - Mock capacity 100, optimized profile
    - C=10 (N=150) and C=50 (N=500), 3 trials each, 20s cooldown between trials
    - Preserves older results
    """
    sum_dir = results_dir / "summary"
    raw_dir = results_dir / "raw"
    sum_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    items = [
        (10, 150),
        (50, 500),
    ]

    all_trials: list[dict[str, Any]] = []
    t_start = time.perf_counter()

    for c_val, n_req in items:
        server_cfg = get_scenario_server_config(
            "failures_v2",
            profile="optimized",
            mock_port=mock_port,
            gateway_port=gateway_port,
            seed=seed,
        )

        subproc = SubprocessManager(server_cfg)
        subproc.start_mock()
        subproc.start_gateway()
        healthy = await subproc.wait_healthy()
        if not healthy:
            subproc.shutdown()
            raise RuntimeError(f"Server failed to start for failures_v2 C={c_val}")

        try:
            for t_idx in range(1, 4):
                trial_name = f"failures_v2_C{c_val}_t{t_idx}"
                workload_cfg = WorkloadConfig(
                    concurrency=c_val,
                    total_requests=n_req,
                    warmup_requests=10,
                    seed=seed + t_idx,
                )
                print(f"\n[FAILURES_V2] Running {trial_name} (C={c_val}, N={n_req}, timeout=1.5s, slow_tail=4.0s)...")
                res = await run_trial(
                    server_cfg,
                    workload_cfg,
                    trial_name=trial_name,
                    results_dir=results_dir,
                    subproc_mgr=subproc,
                )
                all_trials.append(res)
                if t_idx < 3 or (c_val != items[-1][0]):
                    if cooldown_seconds > 0:
                        print(f"Cooling down for {cooldown_seconds}s before next trial...")
                        await asyncio.sleep(cooldown_seconds)
        finally:
            subproc.shutdown()

    total_duration = time.perf_counter() - t_start
    final_output = {
        "timestamp": time.time(),
        "total_runtime_seconds": round(total_duration, 2),
        "cooldown_seconds": cooldown_seconds,
        "trials": [t["summary"] for t in all_trials],
    }

    out_file = sum_dir / "failures_v2_summary.json"
    out_file.write_text(json.dumps(final_output, indent=2), encoding="utf-8")
    return final_output


def main() -> None:
    parser = argparse.ArgumentParser(description="Run LLM Gateway Benchmark Matrix")
    parser.add_argument("--mock-port", type=int, default=9000)
    parser.add_argument("--gateway-port", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=FIXED_SEED)
    parser.add_argument("--cooldown", type=float, default=20.0, help="Cooldown between trials in seconds")
    parser.add_argument("--rerun-capacity20", action="store_true", help="Rerun capacity20 optimized (C=1,10,25,50) and baseline (C=50)")
    parser.add_argument("--run-failures-v2", action="store_true", help="Run failures_v2 (C=10, 50, 3 trials each, 20s cooldown)")
    args = parser.parse_args()

    if args.rerun_capacity20:
        print("Launching capacity20 rerun (optimized C=1,10,25,50 + baseline C=50, 20s cooldown)...")
        out = asyncio.run(
            rerun_capacity20(
                mock_port=args.mock_port,
                gateway_port=args.gateway_port,
                seed=args.seed,
                cooldown_seconds=args.cooldown,
            )
        )
        print(f"Capacity20 rerun complete in {out['total_runtime_seconds']}s")
    elif args.run_failures_v2:
        print("Launching failures_v2 benchmark (C=10, C=50, 3 trials each, 20s cooldown)...")
        out = asyncio.run(
            run_failures_v2(
                mock_port=args.mock_port,
                gateway_port=args.gateway_port,
                seed=args.seed,
                cooldown_seconds=args.cooldown,
            )
        )
        print(f"failures_v2 benchmark complete in {out['total_runtime_seconds']}s")
    else:
        print("Launching full benchmark matrix...")
        out = asyncio.run(
            run_scenario_matrix(
                mock_port=args.mock_port,
                gateway_port=args.gateway_port,
                seed=args.seed,
            )
        )
        print(f"Matrix run complete in {out['total_runtime_seconds']}s")


if __name__ == "__main__":
    main()
