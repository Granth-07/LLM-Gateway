"""
Experiment execution module (bench/experiments.py).

Rules:
- Same seed/workload/mock.
- Interleaved A/B/A/B/A/B order (>= 3 trials each).
- Report median and min-max range of p95 and throughput.
- Verdict "improved" only if the difference between medians exceeds the trial range,
  else "no measurable difference".

Experiments:
1. Exp1: reuse_http_client off vs on (also report client_setup and connect_time).
2. Exp2: pool limits small (max_connections=10) vs adequate (>=C) at C=50 with reuse on.
3. Exp3: release_slot_during_backoff off vs on under the failures scenario.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from bench.aggregate import (
    MetricDistribution,
    compute_experiment_verdict,
    compute_trial_distribution,
)
from bench.runner import (
    DEFAULT_RESULTS_DIR,
    ServerConfig,
    SubprocessManager,
    WorkloadConfig,
    run_trial,
)
from bench.scenarios import MEDIAN_LATENCY_MEAN, DEFAULT_SIGMA, FIXED_SEED


async def run_ab_experiment(
    exp_name: str,
    cfg_a: ServerConfig,
    cfg_b: ServerConfig,
    workload: WorkloadConfig,
    *,
    trials_per_variant: int = 3,
    mock_port: int = 9000,
    gateway_port: int = 8000,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    cooldown_seconds: float = 0.0,
) -> dict[str, Any]:
    """
    Run an A/B experiment in interleaved A/B/A/B/A/B order.
    Returns aggregated metrics and verdict.
    """
    order: list[tuple[str, ServerConfig]] = []
    for i in range(trials_per_variant):
        order.append(("A", cfg_a))
        order.append(("B", cfg_b))

    results_a: list[dict[str, Any]] = []
    results_b: list[dict[str, Any]] = []

    a_idx = 0
    b_idx = 0

    print(f"\n=======================================================")
    print(f"Starting {exp_name} (A/B interleaved, {trials_per_variant} trials each)")
    print(f"=======================================================")

    for variant_label, s_cfg in order:
        if variant_label == "A":
            a_idx += 1
            trial_name = f"{exp_name}_A_trial{a_idx}"
        else:
            b_idx += 1
            trial_name = f"{exp_name}_B_trial{b_idx}"

        s_cfg.mock_port = mock_port
        s_cfg.gateway_port = gateway_port

        subproc = SubprocessManager(s_cfg)
        subproc.start_mock()
        subproc.start_gateway()
        healthy = await subproc.wait_healthy()
        if not healthy:
            subproc.shutdown()
            raise RuntimeError(f"Server failed to become healthy for {trial_name}")

        try:
            print(f"[{exp_name}] Running {trial_name}...")
            trial_res = await run_trial(
                s_cfg,
                workload,
                trial_name=trial_name,
                results_dir=results_dir,
                subproc_mgr=subproc,
            )
            if variant_label == "A":
                results_a.append(trial_res)
            else:
                results_b.append(trial_res)
        finally:
            subproc.shutdown()

        if cooldown_seconds > 0 and (a_idx + b_idx < len(order)):
            print(f"[{exp_name}] Cooling down for {cooldown_seconds}s before next trial...")
            await asyncio.sleep(cooldown_seconds)

    # Extract metrics for A and B
    thru_a = [r["summary"]["throughput_rps"] for r in results_a]
    thru_b = [r["summary"]["throughput_rps"] for r in results_b]

    p95_a = [r["summary"]["client_latency"]["p95"] for r in results_a]
    p95_b = [r["summary"]["client_latency"]["p95"] for r in results_b]

    client_setup_a = [r["summary"]["gateway_latency"]["client_setup"]["p50"] for r in results_a]
    client_setup_b = [r["summary"]["gateway_latency"]["client_setup"]["p50"] for r in results_b]

    connect_time_a = [r["summary"]["gateway_latency"]["connect_time"]["p50"] for r in results_a]
    connect_time_b = [r["summary"]["gateway_latency"]["connect_time"]["p50"] for r in results_b]

    connect_mean_a = [r["summary"]["gateway_latency"]["connect_time"].get("mean", 0.0) for r in results_a]
    connect_mean_b = [r["summary"]["gateway_latency"]["connect_time"].get("mean", 0.0) for r in results_b]

    mock_concurrent_a = [r["summary"]["checks"]["mock_peak_concurrent"] for r in results_a]
    mock_concurrent_b = [r["summary"]["checks"]["mock_peak_concurrent"] for r in results_b]

    conn_reused_a = [r["summary"].get("connection_reused_fraction", 0.0) for r in results_a]
    conn_reused_b = [r["summary"].get("connection_reused_fraction", 0.0) for r in results_b]

    qw_p95_a = [r["summary"]["gateway_latency"]["queue_wait"]["p95"] for r in results_a]
    qw_p95_b = [r["summary"]["gateway_latency"]["queue_wait"]["p95"] for r in results_b]

    dist_thru_a = compute_trial_distribution(thru_a)
    dist_thru_b = compute_trial_distribution(thru_b)

    dist_p95_a = compute_trial_distribution(p95_a)
    dist_p95_b = compute_trial_distribution(p95_b)

    dist_setup_a = compute_trial_distribution(client_setup_a)
    dist_setup_b = compute_trial_distribution(client_setup_b)

    dist_conn_a = compute_trial_distribution(connect_time_a)
    dist_conn_b = compute_trial_distribution(connect_time_b)

    dist_conn_mean_a = compute_trial_distribution(connect_mean_a)
    dist_conn_mean_b = compute_trial_distribution(connect_mean_b)

    dist_mock_c_a = compute_trial_distribution(mock_concurrent_a)
    dist_mock_c_b = compute_trial_distribution(mock_concurrent_b)

    # Compute verdicts: throughput (higher is better) and p95 (lower is better) using pre-declared rule
    verdict_thru = compute_experiment_verdict(dist_thru_a, dist_thru_b, higher_is_better=True, rule="every_trial")
    verdict_p95 = compute_experiment_verdict(dist_p95_a, dist_p95_b, higher_is_better=False, rule="every_trial")

    resources_a = [r["summary"].get("resource_stats", {}) for r in results_a]
    resources_b = [r["summary"].get("resource_stats", {}) for r in results_b]

    exp_result = {
        "experiment": exp_name,
        "throughput": {
            "variant_A": asdict(dist_thru_a),
            "variant_B": asdict(dist_thru_b),
            "trials_A": thru_a,
            "trials_B": thru_b,
            "verdict": verdict_thru["verdict"],
            "details": verdict_thru,
        },
        "p95_latency": {
            "variant_A": asdict(dist_p95_a),
            "variant_B": asdict(dist_p95_b),
            "trials_A": p95_a,
            "trials_B": p95_b,
            "verdict": verdict_p95["verdict"],
            "details": verdict_p95,
        },
        "client_setup_p50": {
            "variant_A": asdict(dist_setup_a),
            "variant_B": asdict(dist_setup_b),
            "trials_A": client_setup_a,
            "trials_B": client_setup_b,
        },
        "connect_time_p50": {
            "variant_A": asdict(dist_conn_a),
            "variant_B": asdict(dist_conn_b),
            "trials_A": connect_time_a,
            "trials_B": connect_time_b,
        },
        "connect_time_mean": {
            "variant_A": asdict(dist_conn_mean_a),
            "variant_B": asdict(dist_conn_mean_b),
            "trials_A": connect_mean_a,
            "trials_B": connect_mean_b,
        },
        "mock_peak_concurrent": {
            "variant_A": asdict(dist_mock_c_a),
            "variant_B": asdict(dist_mock_c_b),
        },
        "connection_reused_fraction": {
            "variant_A": conn_reused_a,
            "variant_B": conn_reused_b,
        },
        "gateway_queue_wait_p95": {
            "variant_A": qw_p95_a,
            "variant_B": qw_p95_b,
        },
        "resources": {
            "variant_A": resources_a,
            "variant_B": resources_b,
        },
    }

    print(f"\n--- {exp_name} Summary ---")
    print(f"Throughput: A={dist_thru_a.median} (range {dist_thru_a.range}) -> B={dist_thru_b.median} (range {dist_thru_b.range}) | Verdict: {verdict_thru['verdict']}")
    print(f"p95 Latency: A={dist_p95_a.median}s (range {dist_p95_a.range}s) -> B={dist_p95_b.median}s (range {dist_p95_b.range}s) | Verdict: {verdict_p95['verdict']}")
    return exp_result


async def run_all_experiments(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    mock_port: int = 9000,
    gateway_port: int = 8000,
    seed: int = FIXED_SEED,
) -> dict[str, Any]:
    """Run Exp1, Exp2, and Exp3 (7 trials per arm, A/B interleaved)."""
    workload = WorkloadConfig(
        concurrency=50,
        total_requests=250,
        warmup_requests=10,
        seed=seed,
    )

    all_exp_results: list[dict[str, Any]] = []
    t_start = time.perf_counter()

    # Exp 1: reuse_http_client off vs on (7 trials each)
    cfg1_a = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=False, mock_seed=seed,
    )
    cfg1_b = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=True, mock_seed=seed,
    )
    res1 = await run_ab_experiment(
        "Exp1_Client_Reuse",
        cfg1_a, cfg1_b, workload,
        trials_per_variant=7,
        mock_port=mock_port, gateway_port=gateway_port, results_dir=results_dir,
    )
    all_exp_results.append(res1)

    # Exp 2: pool limits small (10) vs adequate (100) at C=50 with reuse on (7 trials each)
    cfg2_a = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=True,
        pool_max_connections=10, pool_max_keepalive=5, mock_seed=seed,
    )
    cfg2_b = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=True,
        pool_max_connections=100, pool_max_keepalive=50, mock_seed=seed,
    )
    res2 = await run_ab_experiment(
        "Exp2_Pool_Limits",
        cfg2_a, cfg2_b, workload,
        trials_per_variant=7,
        mock_port=mock_port, gateway_port=gateway_port, results_dir=results_dir,
    )
    all_exp_results.append(res2)

    # Exp 3 redesign: failures scenario, gateway limit 25, queue_size >= 50, C=50 (7 trials each)
    cfg3_a = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=25, queue_size=100, reuse_http_client=True,
        mock_error_rate_429=0.10, mock_error_rate_500=0.05,
        mock_slow_tail_prob=0.05, mock_drop_prob=0.02,
        release_slot_during_backoff=False, mock_seed=seed,
    )
    cfg3_b = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=25, queue_size=100, reuse_http_client=True,
        mock_error_rate_429=0.10, mock_error_rate_500=0.05,
        mock_slow_tail_prob=0.05, mock_drop_prob=0.02,
        release_slot_during_backoff=True, mock_seed=seed,
    )
    res3 = await run_ab_experiment(
        "Exp3_Release_Slot_Backoff",
        cfg3_a, cfg3_b, workload,
        trials_per_variant=7,
        mock_port=mock_port, gateway_port=gateway_port, results_dir=results_dir,
    )
    all_exp_results.append(res3)

    total_duration = time.perf_counter() - t_start
    final_output = {
        "timestamp": time.time(),
        "total_runtime_seconds": round(total_duration, 2),
        "experiments": all_exp_results,
    }

    summary_file = results_dir / "summary" / "experiments_summary.json"
    summary_file.write_text(json.dumps(final_output, indent=2), encoding="utf-8")

    # Generate markdown tables
    try:
        from bench.aggregate import generate_markdown_tables
        generate_markdown_tables(results_dir)
    except Exception as e:
        print(f"Warning: could not generate markdown tables: {e}")

    return final_output


async def run_exp1b(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    mock_port: int = 9000,
    gateway_port: int = 8000,
    seed: int = FIXED_SEED,
    cooldown_seconds: float = 10.0,
) -> dict[str, Any]:
    """
    Exp1b: scenario clean (mock capacity 100, no failures), C=50, N=500,
    7 trials per arm A/B interleaved, 10s cooldown.
    Baseline profile (reuse off, max_connections=100, max_keepalive=20) vs
    FINAL optimized profile (reuse on, max_connections=100, max_keepalive=100).
    """
    workload = WorkloadConfig(
        concurrency=50,
        total_requests=500,
        warmup_requests=10,
        seed=seed,
    )
    t_start = time.perf_counter()

    cfg1b_a = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=False,
        pool_max_connections=100, pool_max_keepalive=20, mock_seed=seed,
    )
    cfg1b_b = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=True,
        pool_max_connections=100, pool_max_keepalive=100, mock_seed=seed,
    )
    res = await run_ab_experiment(
        "Exp1b_Client_Reuse_Final",
        cfg1b_a, cfg1b_b, workload,
        trials_per_variant=7,
        mock_port=mock_port, gateway_port=gateway_port, results_dir=results_dir,
        cooldown_seconds=cooldown_seconds,
    )

    total_duration = time.perf_counter() - t_start
    final_output = {
        "timestamp": time.time(),
        "total_runtime_seconds": round(total_duration, 2),
        "cooldown_seconds": cooldown_seconds,
        "exp1b": res,
    }

    summary_file = results_dir / "summary" / "exp1b_summary.json"
    summary_file.write_text(json.dumps(final_output, indent=2), encoding="utf-8")

    # Generate markdown tables
    try:
        from bench.aggregate import generate_markdown_tables
        generate_markdown_tables(results_dir)
    except Exception as e:
        print(f"Warning: could not generate markdown tables: {e}")

    return final_output


async def run_exp1c(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    mock_port: int = 9000,
    gateway_port: int = 8000,
    seed: int = FIXED_SEED,
    cooldown_seconds: float = 10.0,
) -> dict[str, Any]:
    """
    Exp1c: clean scenario, C=50, N=500, 5 pairs baseline vs final optimized
    (reuse on, max_connections=100, keepalive=100), A/B interleaved, 10s cooldown,
    FRESH mock+gateway processes for every trial.
    """
    workload = WorkloadConfig(
        concurrency=50,
        total_requests=500,
        warmup_requests=10,
        seed=seed,
    )
    t_start = time.perf_counter()

    cfg1c_a = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=False,
        pool_max_connections=100, pool_max_keepalive=20, mock_seed=seed,
    )
    cfg1c_b = ServerConfig(
        mock_port=mock_port, gateway_port=gateway_port, mock_max_capacity=100,
        concurrency_limit=50, queue_size=200, reuse_http_client=True,
        pool_max_connections=100, pool_max_keepalive=100, mock_seed=seed,
    )
    res = await run_ab_experiment(
        "Exp1c_Client_Reuse_Resources",
        cfg1c_a, cfg1c_b, workload,
        trials_per_variant=5,
        mock_port=mock_port, gateway_port=gateway_port, results_dir=results_dir,
        cooldown_seconds=cooldown_seconds,
    )

    total_duration = time.perf_counter() - t_start
    final_output = {
        "timestamp": time.time(),
        "total_runtime_seconds": round(total_duration, 2),
        "cooldown_seconds": cooldown_seconds,
        "exp1c": res,
    }

    summary_file = results_dir / "summary" / "exp1c_summary.json"
    summary_file.write_text(json.dumps(final_output, indent=2), encoding="utf-8")

    # Generate markdown tables
    try:
        from bench.aggregate import generate_markdown_tables
        generate_markdown_tables(results_dir)
    except Exception as e:
        print(f"Warning: could not generate markdown tables: {e}")

    return final_output


def main() -> None:
    parser = argparse.ArgumentParser(description="Run LLM Gateway Experiments")
    parser.add_argument("--mock-port", type=int, default=9000)
    parser.add_argument("--gateway-port", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=FIXED_SEED)
    parser.add_argument("--exp1b", action="store_true", help="Run Exp1b only (C=50, N=500, 7 trials, 10s cooldown)")
    parser.add_argument("--exp1c", action="store_true", help="Run Exp1c only (C=50, N=500, 5 trials, 10s cooldown, resources)")
    parser.add_argument("--cooldown", type=float, default=10.0, help="Cooldown between trials in seconds")
    args = parser.parse_args()

    if args.exp1c:
        print("Launching Exp1c (Clean, C=50, N=500, 5 pairs with psutil resource stats)...")
        out = asyncio.run(
            run_exp1c(
                mock_port=args.mock_port,
                gateway_port=args.gateway_port,
                seed=args.seed,
                cooldown_seconds=args.cooldown,
            )
        )
        print(f"\nExp1c complete in {out['total_runtime_seconds']}s")
    elif args.exp1b:
        print("Launching Exp1b (Client Reuse Final Profile: baseline vs reuse-on pool=100, C=50, N=500, 7 trials)...")
        out = asyncio.run(
            run_exp1b(
                mock_port=args.mock_port,
                gateway_port=args.gateway_port,
                seed=args.seed,
                cooldown_seconds=args.cooldown,
            )
        )
        print(f"\nExp1b complete in {out['total_runtime_seconds']}s")
    else:
        print("Launching all experiments (Exp1, Exp2, Exp3)...")
        out = asyncio.run(
            run_all_experiments(
                mock_port=args.mock_port,
                gateway_port=args.gateway_port,
                seed=args.seed,
            )
        )
        print(f"\nAll experiments complete in {out['total_runtime_seconds']}s")


if __name__ == "__main__":
    main()


