"""
Benchmark runner: launches mock & gateway as separate subprocesses,
runs closed-loop workload with exactly C workers pulling from a counter,
and writes raw CSV/JSON and summary JSON to results/.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bench.aggregate import RequestRecord, aggregate_run_records, serialize_records_to_csv


DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
DEFAULT_RAW_DIR = DEFAULT_RESULTS_DIR / "raw"
DEFAULT_SUMMARY_DIR = DEFAULT_RESULTS_DIR / "summary"


@dataclass
class ServerConfig:
    mock_port: int = 9000
    gateway_port: int = 8000
    mock_latency_mean: float = -1.20397   # exp(-1.20397) ≈ 0.3s median
    mock_latency_sigma: float = 0.25
    mock_max_capacity: int = 100
    mock_capacity_mode: str = "queue"
    mock_error_rate_429: float = 0.0
    mock_error_rate_500: float = 0.0
    mock_slow_tail_prob: float = 0.0
    mock_slow_tail_latency_s: float = 0.0
    mock_drop_prob: float = 0.0
    mock_seed: int = 42

    # Gateway settings / flags
    concurrency_limit: int = 50
    queue_size: int = 200
    queue_wait_timeout: float = 10.0
    request_timeout: float = 30.0
    reuse_http_client: bool = False
    pool_max_keepalive: int = 100
    pool_max_connections: int = 100
    release_slot_during_backoff: bool = False
    profile: str = "baseline"

    # Real provider / env settings
    is_real: bool = False
    env_file: Optional[str] = None
    env_overrides: dict[str, str] = field(default_factory=dict)


PROFILES: dict[str, dict[str, Any]] = {
    "baseline": {
        "reuse_http_client": False,
        "pool_max_connections": 100,
        "pool_max_keepalive": 20,
    },
    "optimized": {
        "reuse_http_client": True,
        "pool_max_connections": 100,
        "pool_max_keepalive": 100,
    },
}


def apply_profile(cfg: ServerConfig, profile_name: str) -> ServerConfig:
    """Apply named profile settings ('baseline' or 'optimized') to ServerConfig."""
    if profile_name not in PROFILES:
        raise ValueError(f"Unknown profile: '{profile_name}'. Available: {list(PROFILES.keys())}")
    for k, v in PROFILES[profile_name].items():
        setattr(cfg, k, v)
    cfg.profile = profile_name
    return cfg


@dataclass
class WorkloadConfig:
    concurrency: int = 10            # C workers
    total_requests: int = 100        # N measured requests
    warmup_requests: int = 10        # Exactly 10 warmup requests (excluded from results)
    model: str = "mock-model"
    prompt: str = "Benchmark query prompt for testing concurrency."
    max_tokens: int = 128
    timeout: float = 30.0
    seed: int = 42


class SubprocessManager:
    """Manages mock provider and gateway server subprocesses."""

    def __init__(self, cfg: ServerConfig, python_exe: str = sys.executable) -> None:
        self.cfg = cfg
        self.python_exe = python_exe
        self.mock_proc: Optional[subprocess.Popen[Any]] = None
        self.gateway_proc: Optional[subprocess.Popen[Any]] = None

    def start_mock(self) -> None:
        if self.cfg.is_real:
            return
        cmd = [
            self.python_exe,
            "-m",
            "mock_provider.server",
            "--port",
            str(self.cfg.mock_port),
            "--latency-mean",
            str(self.cfg.mock_latency_mean),
            "--latency-sigma",
            str(self.cfg.mock_latency_sigma),
            "--max-capacity",
            str(self.cfg.mock_max_capacity),
            "--capacity-mode",
            str(self.cfg.mock_capacity_mode),
            "--error-rate-429",
            str(self.cfg.mock_error_rate_429),
            "--error-rate-500",
            str(self.cfg.mock_error_rate_500),
            "--slow-tail-prob",
            str(self.cfg.mock_slow_tail_prob),
            "--slow-tail-latency-s",
            str(self.cfg.mock_slow_tail_latency_s),
            "--drop-prob",
            str(self.cfg.mock_drop_prob),
            "--seed",
            str(self.cfg.mock_seed),
        ]
        self.mock_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ.copy(),
        )

    def start_gateway(self) -> None:
        env = os.environ.copy()
        src_path = str(Path(__file__).resolve().parent.parent / "src")
        cur_pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{src_path}{os.pathsep}{cur_pythonpath}" if cur_pythonpath else src_path

        if self.cfg.is_real:
            if self.cfg.env_overrides:
                env.update(self.cfg.env_overrides)
            env["CONCURRENCY_LIMIT"] = str(self.cfg.concurrency_limit)
            env["QUEUE_SIZE"] = str(self.cfg.queue_size)
            env["QUEUE_WAIT_TIMEOUT"] = str(self.cfg.queue_wait_timeout)
            env["REQUEST_TIMEOUT"] = str(self.cfg.request_timeout)
            env["REUSE_HTTP_CLIENT"] = "true" if self.cfg.reuse_http_client else "false"
            env["POOL_MAX_KEEPALIVE"] = str(self.cfg.pool_max_keepalive)
            env["POOL_MAX_CONNECTIONS"] = str(self.cfg.pool_max_connections)
            env["RELEASE_SLOT_DURING_BACKOFF"] = "true" if self.cfg.release_slot_during_backoff else "false"
        else:
            env["PROVIDER_NAME"] = "mock"
            env["PROVIDER_BASE_URL"] = f"http://127.0.0.1:{self.cfg.mock_port}"
            env["PROVIDER_MODEL"] = "mock-model"
            env["CONCURRENCY_LIMIT"] = str(self.cfg.concurrency_limit)
            env["QUEUE_SIZE"] = str(self.cfg.queue_size)
            env["QUEUE_WAIT_TIMEOUT"] = str(self.cfg.queue_wait_timeout)
            env["REQUEST_TIMEOUT"] = str(self.cfg.request_timeout)
            env["REUSE_HTTP_CLIENT"] = "true" if self.cfg.reuse_http_client else "false"
            env["POOL_MAX_KEEPALIVE"] = str(self.cfg.pool_max_keepalive)
            env["POOL_MAX_CONNECTIONS"] = str(self.cfg.pool_max_connections)
            env["RELEASE_SLOT_DURING_BACKOFF"] = "true" if self.cfg.release_slot_during_backoff else "false"

        cmd = [
            self.python_exe,
            "-m",
            "uvicorn",
            "gateway.app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.cfg.gateway_port),
            "--log-level",
            "warning",
        ]
        self.gateway_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=env,
        )

    async def wait_healthy(self, timeout: float = 12.0) -> bool:
        """Poll /health on both mock and gateway until ready."""
        deadline = time.perf_counter() + timeout
        mock_url = f"http://127.0.0.1:{self.cfg.mock_port}/control/stats"
        gateway_url = f"http://127.0.0.1:{self.cfg.gateway_port}/health"

        async with httpx.AsyncClient(timeout=0.5) as client:
            while time.perf_counter() < deadline:
                if self.gateway_proc and self.gateway_proc.poll() is not None:
                    err = ""
                    if self.gateway_proc.stderr:
                        try:
                            err = self.gateway_proc.stderr.read().decode("utf-8", errors="replace")
                        except Exception:
                            pass
                    raise RuntimeError(f"Gateway process exited prematurely (code {self.gateway_proc.returncode}): {err}")
                if not self.cfg.is_real and self.mock_proc and self.mock_proc.poll() is not None:
                    raise RuntimeError(f"Mock process exited prematurely (code {self.mock_proc.returncode})")

                mock_ok = self.cfg.is_real
                gateway_ok = False
                if not self.cfg.is_real:
                    try:
                        r_m = await client.get(mock_url)
                        mock_ok = (r_m.status_code == 200)
                    except Exception:
                        mock_ok = False

                try:
                    r_g = await client.get(gateway_url)
                    gateway_ok = (r_g.status_code == 200)
                except Exception:
                    gateway_ok = False

                if mock_ok and gateway_ok:
                    return True
                await asyncio.sleep(0.05)

        return False

    def shutdown(self) -> None:
        """Cleanly terminate both subprocesses."""
        for p in (self.gateway_proc, self.mock_proc):
            if p is not None and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait(timeout=1.0)
            if p is not None and getattr(p, "stderr", None) is not None:
                try:
                    p.stderr.close()
                except Exception:
                    pass


async def run_closed_loop_workload(
    gateway_url: str,
    workload: WorkloadConfig,
) -> tuple[list[RequestRecord], float, float, bool, str]:
    """
    Run closed-loop workload with EXACTLY C worker tasks pulling from a counter.
    Never creates N tasks.
    10 warmup requests are executed first, then marked as is_warmup=True.
    """
    total_target = workload.warmup_requests + workload.total_requests
    next_idx = 0
    counter_lock = asyncio.Lock()
    records: list[RequestRecord] = []
    records_lock = asyncio.Lock()
    any_retry_after_on_429 = False
    last_error_body = ""

    t_run_start = time.perf_counter()
    measured_start_time: float = t_run_start

    async def worker(worker_id: int) -> None:
        nonlocal next_idx, measured_start_time, any_retry_after_on_429, last_error_body

        # Closed-loop client per worker or shared
        async with httpx.AsyncClient(timeout=workload.timeout + 5.0) as client:
            while True:
                async with counter_lock:
                    if next_idx >= total_target:
                        break
                    req_num = next_idx
                    next_idx += 1
                    if req_num == workload.warmup_requests:
                        measured_start_time = time.perf_counter()

                is_warmup = (req_num < workload.warmup_requests)
                t_req_start = time.perf_counter()
                start_offset = t_req_start - t_run_start
                status = 0
                error_kind: Optional[str] = None
                timings_dict: dict[str, float] = {}

                payload = {
                    "prompt": workload.prompt,
                    "model": workload.model,
                    "max_tokens": workload.max_tokens,
                }
                headers = {"X-Request-ID": f"bench-{worker_id}-{req_num}"}

                raw_err_text = ""
                has_retry_after = False
                try:
                    resp = await client.post(
                        f"{gateway_url}/chat",
                        json=payload,
                        headers=headers,
                        timeout=workload.timeout,
                    )
                    status = resp.status_code
                    if status == 200:
                        body = resp.json()
                        timings_dict = body.get("timings", {})
                    else:
                        raw_err_text = resp.text
                        if status == 429 and ("retry-after" in resp.headers or resp.headers.get("Retry-After")):
                            has_retry_after = True
                        try:
                            err_body = resp.json()
                            if isinstance(err_body, dict):
                                if err_body.get("retry_after") is not None:
                                    has_retry_after = True
                                error_kind = str(err_body.get("error") or err_body.get("detail") or f"http_{status}")
                                err_code = err_body.get("code") or err_body.get("error_code")
                                if err_code and str(err_code) not in error_kind:
                                    error_kind = f"{error_kind} (code: {err_code})"
                            else:
                                error_kind = str(err_body)
                        except Exception:
                            error_kind = f"http_{status}"
                except httpx.TimeoutException:
                    status = 504
                    error_kind = "client_timeout"
                except httpx.ConnectError:
                    status = 502
                    error_kind = "client_connect_error"
                except Exception as exc:
                    status = 0
                    error_kind = f"client_exc_{type(exc).__name__}"

                t_req_end = time.perf_counter()
                client_lat = t_req_end - t_req_start

                rec = RequestRecord(
                    id=f"req-{req_num}",
                    start_offset=start_offset,
                    client_latency=client_lat,
                    http_status=status,
                    error_kind=error_kind,
                    gateway_total=timings_dict.get("total", 0.0),
                    queue_wait=timings_dict.get("queue_wait", 0.0),
                    client_setup=timings_dict.get("client_setup", 0.0),
                    provider_time=timings_dict.get("provider_time", 0.0),
                    provider_duration=timings_dict.get("provider_duration", 0.0),
                    connect_time=timings_dict.get("connect_time", 0.0),
                    backoff_sleep=timings_dict.get("backoff_sleep", 0.0),
                    response_processing=timings_dict.get("response_processing", 0.0),
                    overhead=timings_dict.get("overhead", 0.0),
                    is_warmup=is_warmup,
                )

                async with records_lock:
                    if has_retry_after:
                        any_retry_after_on_429 = True
                    if raw_err_text:
                        last_error_body = raw_err_text
                    records.append(rec)

    # Spawn EXACTLY C worker tasks
    workers = [asyncio.create_task(worker(w_id)) for w_id in range(workload.concurrency)]
    await asyncio.gather(*workers)
    t_run_end = time.perf_counter()

    return records, measured_start_time, t_run_end, any_retry_after_on_429, last_error_body


async def run_trial(
    server_cfg: ServerConfig,
    workload_cfg: WorkloadConfig,
    *,
    trial_name: str = "trial",
    results_dir: Path = DEFAULT_RESULTS_DIR,
    subproc_mgr: Optional[SubprocessManager] = None,
) -> dict[str, Any]:
    """
    Run one full benchmark trial:
    - start subprocesses (or reuse running ones)
    - reset gateway and mock stats
    - execute workload
    - fetch final metrics from gateway and mock
    - write raw CSV and JSON, summary JSON
    """
    raw_dir = results_dir / "raw"
    summary_dir = results_dir / "summary"
    raw_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)

    owns_subproc = False
    if subproc_mgr is None:
        subproc_mgr = SubprocessManager(server_cfg)
        subproc_mgr.start_mock()
        subproc_mgr.start_gateway()
        healthy = await subproc_mgr.wait_healthy()
        if not healthy:
            subproc_mgr.shutdown()
            raise RuntimeError("Mock or Gateway failed to become healthy within timeout.")
        owns_subproc = True

    gateway_url = f"http://127.0.0.1:{server_cfg.gateway_port}"
    mock_url = f"http://127.0.0.1:{server_cfg.mock_port}"

    try:
        # Reset gateway and mock stats between trials
        async with httpx.AsyncClient(timeout=2.0) as client:
            try:
                await client.post(f"{gateway_url}/metrics/reset")
            except Exception:
                pass
            if not server_cfg.is_real:
                try:
                    await client.post(f"{mock_url}/control/reset")
                except Exception:
                    pass

        # Start psutil sampler (1 Hz) for system, gateway, mock, and runner process trees
        # CPU% normalization: percent of one core (e.g. 100% = 1 full logical core utilized).
        sample_data: list[dict[str, Any]] = []
        sampler_stop = threading.Event()
        bench_logger = logging.getLogger("bench.runner")

        def _psutil_sampler_worker() -> None:
            try:
                import psutil
            except ImportError:
                return

            root_procs: dict[str, Optional[psutil.Process]] = {}
            try:
                root_procs["runner"] = psutil.Process(os.getpid())
            except Exception:
                pass
            if subproc_mgr and subproc_mgr.gateway_proc and subproc_mgr.gateway_proc.pid:
                try:
                    root_procs["gateway"] = psutil.Process(subproc_mgr.gateway_proc.pid)
                except Exception:
                    pass
            if subproc_mgr and subproc_mgr.mock_proc and subproc_mgr.mock_proc.pid:
                try:
                    root_procs["mock"] = psutil.Process(subproc_mgr.mock_proc.pid)
                except Exception:
                    pass

            # Track Process objects by PID across samples so cpu_percent(interval=None) maintains state
            tracked_procs: dict[str, dict[int, psutil.Process]] = {
                "runner": {},
                "gateway": {},
                "mock": {},
            }

            def _get_tree(role: str, root_p: Optional[psutil.Process]) -> list[psutil.Process]:
                if root_p is None:
                    return []
                try:
                    if not root_p.is_running():
                        return []
                    if role == "runner":
                        # Sample runner's OWN PID only (exclude child subprocesses: gateway, mock, and their interpreters)
                        tree = [root_p]
                    else:
                        tree = [root_p] + root_p.children(recursive=True)
                except Exception:
                    tree = [root_p] if root_p.is_running() else []

                alive: list[psutil.Process] = []
                current_pids = set()
                for p in tree:
                    try:
                        if p.is_running():
                            current_pids.add(p.pid)
                            if p.pid not in tracked_procs[role]:
                                p.cpu_percent(interval=None)  # prime CPU counter
                                tracked_procs[role][p.pid] = p
                            alive.append(tracked_procs[role][p.pid])
                    except Exception:
                        pass
                for pid in list(tracked_procs[role].keys()):
                    if pid not in current_pids:
                        tracked_procs[role].pop(pid, None)
                return alive

            # Prime initial counters for process trees
            for role, rp in root_procs.items():
                _get_tree(role, rp)

            # Prime system CPU
            try:
                psutil.cpu_percent(interval=None)
            except Exception:
                pass

            benchmark_pids = set()
            for r_procs in tracked_procs.values():
                benchmark_pids.update(r_procs.keys())

            # Prime other system processes
            other_tracked: dict[int, psutil.Process] = {}
            try:
                for p in psutil.process_iter(["name"]):
                    try:
                        if p.pid != 0 and p.pid not in benchmark_pids:
                            p.cpu_percent(interval=None)
                            other_tracked[p.pid] = p
                    except Exception:
                        pass
            except Exception:
                pass

            while not sampler_stop.wait(timeout=1.0):
                snap: dict[str, Any] = {}
                try:
                    snap["system_cpu"] = psutil.cpu_percent(interval=None)
                    all_bench_pids = set()

                    for role, rp in root_procs.items():
                        alive_procs = _get_tree(role, rp)
                        all_bench_pids.update(p.pid for p in alive_procs)
                        if alive_procs:
                            tree_cpu = 0.0
                            tree_rss_mb = 0.0
                            tree_threads = 0
                            tree_handles = 0
                            for p in alive_procs:
                                try:
                                    # CPU% is normalized to percent of one core (100% = 1 core)
                                    tree_cpu += p.cpu_percent(interval=None)
                                    tree_rss_mb += p.memory_info().rss / (1024 * 1024)
                                    tree_threads += p.num_threads()
                                    if hasattr(p, "num_handles"):
                                        tree_handles += p.num_handles()
                                except Exception:
                                    pass
                            snap[role] = {
                                "cpu_percent": round(tree_cpu, 2),
                                "rss_mb": round(tree_rss_mb, 2),
                                "threads": tree_threads,
                                "handles": tree_handles,
                            }

                    # Top-3 OTHER system processes by CPU (excluding benchmark process tree and idle process)
                    other_candidates: list[tuple[float, str]] = []
                    for pid, p in list(other_tracked.items()):
                        if pid in all_bench_pids or pid == 0:
                            continue
                        try:
                            if p.is_running():
                                c = p.cpu_percent(interval=None)
                                name = p.name()
                                if name and name != "System Idle Process":
                                    other_candidates.append((c, name))
                            else:
                                other_tracked.pop(pid, None)
                        except Exception:
                            other_tracked.pop(pid, None)

                    other_candidates.sort(key=lambda x: x[0], reverse=True)
                    top_3_other = [name for _, name in other_candidates[:3]]
                    snap["top_other_processes"] = top_3_other

                    bench_logger.info("1 Hz sample: top-3 other system processes by CPU: %s", top_3_other)
                    print(f"[psutil sampler 1Hz] top-3 other processes by CPU: {top_3_other}")

                except Exception:
                    pass

                if snap:
                    sample_data.append(snap)

        sampler_thread = threading.Thread(target=_psutil_sampler_worker, daemon=True)
        sampler_thread.start()

        # Run closed-loop workload
        records, start_time, end_time, retry_after_429, last_err_body = await run_closed_loop_workload(gateway_url, workload_cfg)

        # Stop sampler
        sampler_stop.set()
        await asyncio.to_thread(sampler_thread.join, 2.0)

        # Aggregate resource stats
        resource_stats: dict[str, Any] = {}
        if sample_data:
            sys_cpus = [s.get("system_cpu", 0.0) for s in sample_data if "system_cpu" in s]
            if sys_cpus:
                resource_stats["system"] = {
                    "cpu_percent_mean": round(sum(sys_cpus) / len(sys_cpus), 2),
                    "cpu_percent_max": round(max(sys_cpus), 2),
                }
            for role in ("gateway", "mock", "runner"):
                role_snaps = [s[role] for s in sample_data if role in s]
                if role_snaps:
                    cpus = [rs["cpu_percent"] for rs in role_snaps]
                    rsss = [rs["rss_mb"] for rs in role_snaps]
                    threads = [rs["threads"] for rs in role_snaps]
                    handles = [rs["handles"] for rs in role_snaps]
                    resource_stats[role] = {
                        "cpu_percent_mean": round(sum(cpus) / len(cpus), 2),
                        "cpu_percent_max": round(max(cpus), 2),
                        "rss_mb_mean": round(sum(rsss) / len(rsss), 2),
                        "rss_mb_max": round(max(rsss), 2),
                        "threads_mean": round(sum(threads) / len(threads), 1),
                        "threads_max": max(threads),
                        "handles_mean": round(sum(handles) / len(handles), 1),
                        "handles_max": max(handles),
                    }

        # Fetch gateway /metrics and mock /control/stats
        gateway_metrics: dict[str, Any] = {}
        mock_stats: dict[str, Any] = {}
        async with httpx.AsyncClient(timeout=2.0) as client:
            try:
                r_m = await client.get(f"{gateway_url}/metrics")
                if r_m.status_code == 200:
                    gateway_metrics = r_m.json()
            except Exception:
                pass

            if not server_cfg.is_real:
                try:
                    r_s = await client.get(f"{mock_url}/control/stats")
                    if r_s.status_code == 200:
                        mock_stats = r_s.json()
                except Exception:
                    pass

        # Aggregate records
        summary = aggregate_run_records(
            records,
            concurrency_c=workload_cfg.concurrency,
            start_time=start_time,
            end_time=end_time,
        )
        summary["resource_stats"] = resource_stats

        # Compare peak in-flight: gateway peak_in_flight vs mock peak_concurrent
        gw_peak_in_flight = gateway_metrics.get("limiter", {}).get("peak_in_flight", 0)
        mock_peak_concurrent = mock_stats.get("peak_concurrent", 0) if not server_cfg.is_real else 0
        summary["checks"]["gateway_peak_in_flight"] = gw_peak_in_flight
        summary["checks"]["mock_peak_concurrent"] = mock_peak_concurrent

        # Collect error kinds per status
        error_kinds_by_status: dict[str, list[str]] = {}
        for r in records:
            if not r.is_warmup and r.error_kind:
                error_kinds_by_status.setdefault(str(r.http_status), []).append(r.error_kind)
        summary["error_kinds"] = {k: sorted(set(v)) for k, v in error_kinds_by_status.items()}
        summary["retry_after_present_on_429"] = retry_after_429
        summary["last_error_body"] = last_err_body

        from gateway.metrics.stats import compute_percentiles
        succ_lats = [r.client_latency for r in records if not r.is_warmup and 200 <= r.http_status < 300]
        if succ_lats:
            summary["client_latency_success"] = compute_percentiles(succ_lats)
        else:
            summary["client_latency_success"] = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "min": 0.0, "max": 0.0}

        backoff_429_list = [r.backoff_sleep for r in records if not r.is_warmup and r.http_status == 429]
        if backoff_429_list:
            summary["backoff_sleep_429"] = compute_percentiles(backoff_429_list)
        else:
            summary["backoff_sleep_429"] = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "min": 0.0, "max": 0.0}

        # Add configuration & metadata
        provider_name = (
            server_cfg.env_overrides.get("PROVIDER_NAME", "openai")
            if server_cfg.is_real
            else "mock"
        )
        full_summary = {
            "trial_name": trial_name,
            "timestamp": time.time(),
            "python_version": sys.version,
            "provider": {
                "name": provider_name,
                "model": workload_cfg.model,
                "is_real": server_cfg.is_real,
            },
            "config": {
                "concurrency_C": workload_cfg.concurrency,
                "total_requests_N": workload_cfg.total_requests,
                "warmup_requests": workload_cfg.warmup_requests,
                "model": workload_cfg.model,
                "max_tokens": workload_cfg.max_tokens,
                "timeout": workload_cfg.timeout,
                "seed": workload_cfg.seed,
                "flags": {
                    "profile": server_cfg.profile,
                    "concurrency_limit": server_cfg.concurrency_limit,
                    "queue_size": server_cfg.queue_size,
                    "reuse_http_client": server_cfg.reuse_http_client,
                    "pool_max_keepalive": server_cfg.pool_max_keepalive,
                    "pool_max_connections": server_cfg.pool_max_connections,
                    "release_slot_during_backoff": server_cfg.release_slot_during_backoff,
                },
                "mock_settings": {
                    "latency_mean": server_cfg.mock_latency_mean,
                    "latency_sigma": server_cfg.mock_latency_sigma,
                    "max_capacity": server_cfg.mock_max_capacity,
                    "capacity_mode": server_cfg.mock_capacity_mode,
                    "error_rate_429": server_cfg.mock_error_rate_429,
                    "error_rate_500": server_cfg.mock_error_rate_500,
                    "slow_tail_prob": server_cfg.mock_slow_tail_prob,
                    "slow_tail_latency_s": server_cfg.mock_slow_tail_latency_s,
                    "drop_prob": server_cfg.mock_drop_prob,
                    "seed": server_cfg.mock_seed,
                } if not server_cfg.is_real else {},
            },
            "summary": summary,
            "gateway_metrics": gateway_metrics,
            "mock_stats": mock_stats,
        }

        # Save files
        csv_path = raw_dir / f"{trial_name}.csv"
        json_path = raw_dir / f"{trial_name}.json"
        summary_path = summary_dir / f"{trial_name}.json"

        # Filter out warmup requests from saved raw artifacts
        measured_records = [r for r in records if not r.is_warmup]
        csv_path.write_text(serialize_records_to_csv(measured_records), encoding="utf-8")
        json_path.write_text(json.dumps([asdict(r) for r in measured_records], indent=2), encoding="utf-8")
        summary_path.write_text(json.dumps(full_summary, indent=2), encoding="utf-8")

        return full_summary

    finally:
        if owns_subproc:
            subproc_mgr.shutdown()


def parse_env_file(path: Path | str) -> dict[str, str]:
    """Parse key-value pairs from an env file without logging or leaking secrets."""
    env_vars: dict[str, str] = {}
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Env file not found: {path}")
    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                v = v[1:-1]
            env_vars[k] = v
    return env_vars


async def run_real_smoke_benchmark(
    env_file: str = ".env.real",
    results_dir: Path = DEFAULT_RESULTS_DIR,
    profiles: Optional[list[str]] = None,
) -> dict[str, dict[str, Any]]:
    """
    Run the real-provider smoke benchmark:
    - HARD CAP 30 requests
    - Concurrency 3, max_tokens 16
    - Baseline and/or optimized profile
    - Never prints or logs the API key
    - Saves raw/summary under results/ with real provider name and model
    """
    if profiles is None:
        profiles = ["baseline", "optimized"]
    env_vars = parse_env_file(env_file)
    key = (
        env_vars.get("PROVIDER_API_KEY")
        or env_vars.get("OPENAI_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or os.environ.get("PROVIDER_API_KEY")
    )
    if not key:
        raise ValueError(f"No API key found in {env_file} or environment (PROVIDER_API_KEY or OPENAI_API_KEY)")

    provider_name = env_vars.get("PROVIDER_NAME", "openai").lower()
    openai_compat_names = {"mock", "openai", "openai_compat", "azure"}
    if provider_name not in openai_compat_names:
        raise ValueError(
            f"Provider {provider_name!r} not accepted by registry. "
            f"Available OpenAI-compatible providers: {sorted(openai_compat_names)}"
        )

    base_url = env_vars.get("PROVIDER_BASE_URL", "https://api.openai.com")
    from urllib.parse import urlparse
    parsed_host = urlparse(base_url).netloc.lower()
    model = env_vars.get("PROVIDER_MODEL", "gpt-4o-mini")

    print(f"EFFECTIVE HOST: {parsed_host}")
    print(f"EFFECTIVE MODEL: {model}")

    if parsed_host == "api.openai.com":
        raise RuntimeError("Aborted: effective host is api.openai.com")

    if base_url.rstrip("/").endswith("/v1"):
        base_url = base_url.rstrip("/")[:-3]
    env_vars["PROVIDER_BASE_URL"] = base_url

    provider_slug = "groq" if "groq" in parsed_host else provider_name
    model_slug = model.replace("/", "_").replace(":", "_")

    concurrency = 3
    max_tokens = 16
    total_requests = 30
    warmup_requests = 0

    assert total_requests + warmup_requests <= 30, "HARD CAP 30: Total requests cannot exceed 30"

    print("=" * 60)
    print("Starting Real-Provider Smoke Benchmark")
    print(f"Provider: {provider_slug} (registered as {provider_name}) | Model: {model}")
    print(f"Concurrency: {concurrency} | Total requests: {total_requests} (HARD CAP 30) | max_tokens: {max_tokens}")
    print("=" * 60)

    results: dict[str, dict[str, Any]] = {}

    for profile_name in profiles:
        print(f"\n--- Running Profile: {profile_name} ---")
        server_cfg = ServerConfig(
            is_real=True,
            env_file=env_file,
            env_overrides=env_vars,
            concurrency_limit=50,
            request_timeout=float(env_vars.get("REQUEST_TIMEOUT", 30.0)),
        )
        apply_profile(server_cfg, profile_name)

        workload_cfg = WorkloadConfig(
            concurrency=concurrency,
            total_requests=total_requests,
            warmup_requests=warmup_requests,
            model=model,
            prompt="Say hello in one word.",
            max_tokens=max_tokens,
            timeout=float(env_vars.get("REQUEST_TIMEOUT", 30.0)),
        )

        trial_name = f"real_{provider_slug}_{model_slug}_{profile_name}"
        summary = await run_trial(server_cfg, workload_cfg, trial_name=trial_name, results_dir=results_dir)
        results[profile_name] = summary

        st = summary["summary"]
        gw_lat = st.get("gateway_latency", {})
        conn_p50 = gw_lat.get("connect_time", {}).get("p50", 0.0)
        status_counts = st.get("status_counts", {})
        gw_metrics = summary.get("gateway_metrics", {})
        attempts_dict = gw_metrics.get("counters", {}).get("attempts", {})
        total_attempts = sum(attempts_dict.values())
        total_reqs = st.get("total_requests", 0)
        attempts_per_req = (total_attempts / total_reqs) if total_reqs > 0 else 0.0
        retry_after_429 = summary.get("summary", {}).get("retry_after_present_on_429", False)
        succ_lat = summary.get("summary", {}).get("client_latency_success", {})
        reuse_frac = st.get("connection_reused_fraction", 0.0)
        backoff_429_p50 = summary.get("summary", {}).get("backoff_sleep_429", {}).get("p50", 0.0)

        print(f"[{profile_name}] Actual Provider / Model: {provider_slug} / {model}")
        print(f"[{profile_name}] Success: {st['success_count']} | Errors: {st['error_count']}")
        print(f"[{profile_name}] Status counts: {status_counts}")
        print(f"[{profile_name}] Throughput: {st['throughput_rps']:.2f} rps | Duration: {st['duration_seconds']:.2f}s")
        print(f"[{profile_name}] Client latency (successes): p50={succ_lat.get('p50', 0.0):.4f}s | p95={succ_lat.get('p95', 0.0):.4f}s | p99={succ_lat.get('p99', 0.0):.4f}s")
        print(f"[{profile_name}] Connect time p50: {conn_p50:.4f}s")
        print(f"[{profile_name}] Connection reuse fraction: {reuse_frac:.4f}")
        print(f"[{profile_name}] Attempts per request: {attempts_per_req:.2f} ({total_attempts}/{total_reqs})")
        print(f"[{profile_name}] Retry-After present on any 429: {retry_after_429}")
        print(f"[{profile_name}] Backoff sleep p50 on 429 requests: {backoff_429_p50:.4f}s")

        if st["success_count"] == 0:
            err_body = summary.get("summary", {}).get("last_error_body", "")
            if key and key in err_body:
                err_body = err_body.replace(key, "[REDACTED_KEY]")
            print(f"\n[{profile_name}] Every request failed. Upstream error body:\n{err_body}")
            print("Stopping benchmark as all requests failed.")
            break

        await asyncio.sleep(2.0)

    print("\n" + "=" * 60)
    print("Real-Provider Smoke Benchmark Completed.")
    print(f"Saved artifacts to {results_dir / 'raw'} and {results_dir / 'summary'}")
    print("=" * 60)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM Gateway Benchmark Runner")
    parser.add_argument("--concurrency", "-C", type=int, default=10, help="Concurrency (worker count)")
    parser.add_argument("--requests", "-N", type=int, default=100, help="Measured request count")
    parser.add_argument("--warmup", type=int, default=10, help="Warmup requests (excluded)")
    parser.add_argument("--max-tokens", type=int, default=128, help="Max tokens")
    parser.add_argument("--timeout", type=float, default=30.0, help="Timeout in seconds")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed")
    parser.add_argument("--profile", type=str, choices=["baseline", "optimized"], default=None, help="Profile name ('baseline' or 'optimized')")
    parser.add_argument("--name", type=str, default="benchmark_run", help="Run name")
    parser.add_argument("--real", action="store_true", help="Run against real provider (smoke test, hard cap 60)")
    parser.add_argument("--env-file", type=str, default=None, help="Path to environment file (e.g. .env.real)")
    args = parser.parse_args()

    if args.real:
        env_file = args.env_file or ".env.real"
        profiles = [args.profile] if args.profile else None
        asyncio.run(run_real_smoke_benchmark(env_file=env_file, profiles=profiles))
        return

    profile = args.profile or "baseline"

    if args.env_file:
        env_vars = parse_env_file(args.env_file)
        server_cfg = ServerConfig(
            is_real=True,
            env_file=args.env_file,
            env_overrides=env_vars,
            concurrency_limit=50,
            request_timeout=float(env_vars.get("REQUEST_TIMEOUT", 30.0)),
        )
        apply_profile(server_cfg, profile)
        model = env_vars.get("PROVIDER_MODEL", "gpt-4o-mini")
        workload_cfg = WorkloadConfig(
            concurrency=args.concurrency,
            total_requests=args.requests,
            warmup_requests=args.warmup,
            model=model,
            max_tokens=args.max_tokens,
            timeout=args.timeout,
            seed=args.seed,
        )
    else:
        server_cfg = ServerConfig()
        apply_profile(server_cfg, profile)
        workload_cfg = WorkloadConfig(
            concurrency=args.concurrency,
            total_requests=args.requests,
            warmup_requests=args.warmup,
            max_tokens=args.max_tokens,
            timeout=args.timeout,
            seed=args.seed,
        )

    print(f"Running benchmark: name={args.name}, C={args.concurrency}, N={args.requests}")
    summary = asyncio.run(run_trial(server_cfg, workload_cfg, trial_name=args.name))
    stats = summary["summary"]
    print(f"Done! Throughput: {stats['throughput_rps']} rps | "
          f"p50: {stats['client_latency']['p50']}s | "
          f"p95: {stats['client_latency']['p95']}s | "
          f"p99: {stats['client_latency']['p99']}s | "
          f"Errors: {stats['error_count']}/{stats['total_requests']}")


if __name__ == "__main__":
    main()
