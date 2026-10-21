# LLM Gateway Performance & Architecture Evaluation Report

## 1. System Environment & Execution Context
- **Hardware & OS**: Windows-11-10.0.26300, Intel64 Family 6 Model 151, 12 logical CPUs [Measured]. Each component (gateway, mock provider, load generator) is a single-process asyncio event loop running on one core [Measured]. Single-thread CPU limit as the throughput plateau explanation is a [Hypothesis], not measured.
- **Scope & Workload**: Evaluates gateway architectural configurations under synthetic mock regimes and real upstream endpoints [Measured].
- **TTFT & Streaming**: TTFT is not measured because streaming is not implemented; all measurements reflect complete request-response payloads [Measured].
- **Timing Integrity**: Client latency $\ge$ gateway total across all trials (0 violations observed) [Measured]. Disconnect tests verify concurrency slot release upon client abort, but do not prove upstream server-side cancellation [Measured]. Mock `peak_concurrent` records arrival-concurrency, not instantaneous requests in service [Measured].
- **Resource Sampling**: Process and system resources sampled at 1 Hz via psutil during benchmark trials [Measured].

## 2. Provenance of Result Sets
- **run1**: Initial continuous benchmark execution without inter-trial pauses; showed baseline C=50 collapse (18.5 rps) and keepalive churn in Exp1 (`connection_reused = 0.00` in reuse-ON arm due to `max_keepalive=20 < C=50`) [Measured].
- **diagnostic**: Isolated sequential requests verifying keepalive mechanics; demonstrated 99% reuse on a single client [Measured].
- **rerun with 20s cooldown**: Benchmark matrix rerun with 20s idle intervals between trials, eliminating trial drift [Measured]. Used for Table 1 capacity20/capacity20_opt.
- **Exp1b (Final Profile, 7 pairs)**: Interleaved A/B benchmark (7 trials per arm, 10s cooldown, C=50, N=500) comparing baseline (`reuse=off, keepalive=20`) against final optimized profile (`reuse=on, pool=100, keepalive=100`) [Measured].
- **Exp1c (Resources & Fresh Processes, 5 pairs)**: Interleaved A/B benchmark (5 trials per arm, 10s cooldown, C=50, N=500) with 1 Hz psutil telemetry and fresh mock+gateway processes per trial [Measured].
- **real-provider runs**: Real-world validation runs on Groq and OpenAI using cost-bounded workloads (N=30..60, C=3) [Measured].

## 3. Baseline vs. Optimized Performance Matrix
Table below reproduces measured results under `capacity20` (mock capacity = 20, queue mode) from `results/summary/tables.md` [Measured]:

| Concurrency C | Profile | N | Trials | Throughput rps (Median [Range]) | p95 Latency (s) (Median [Range]) | Connect Time p50 (s) | Conn Reused Frac | Mock Peak |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| C=1 | Baseline (reuse=off) | 30 | 3 | 2.97 [0.03] | 0.439 [0.017] | 0.0015 | 0.00 | 1 |
| C=1 | Optimized (reuse=on, pool=100) | 30 | 3 | 3.12 [0.05] | 0.430 [0.010] | 0.0000 | 1.00 | 1 |
| C=10 | Baseline (reuse=off) | 150 | 3 | 26.76 [1.21] | 0.493 [0.014] | 0.0038 | 0.00 | 10 |
| C=10 | Optimized (reuse=on, pool=100) | 150 | 3 | 28.77 [0.68] | 0.463 [0.000] | 0.0000 | 1.00 | 10 |
| C=25 | Baseline (reuse=off) | 300 | 3 | 50.97 [0.26] | 0.677 [0.078] | 0.0145 | 0.00 | 25 |
| C=25 | Optimized (reuse=on, pool=100) | 300 | 3 | 56.12 [0.94] | 0.557 [0.018] | 0.0000 | 0.95 | 25 |
| C=50 | Baseline (reuse=off) | 500 | 3 | 53.13 [0.57] | 1.266 [0.265] | 0.0183 | 0.00 | 50 |
| C=50 | Optimized (reuse=on, pool=100) | 500 | 3 | 56.49 [0.97] | 1.001 [0.016] | 0.0000 | 0.92 | 50 |

### Latency Bucket Decomposition (Clean Scenario: C=1 vs C=50, Baseline vs Optimized)
Table below shows median p50 and p95 timings across trials from results files [Measured]:

| Latency Bucket | C=1 Base p50 (s) | C=1 Base p95 (s) | C=1 Opt p50 (s) | C=1 Opt p95 (s) | C=50 Base p50 (s) | C=50 Base p95 (s) | C=50 Opt p50 (s) | C=50 Opt p95 (s) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| queue_wait | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| client_setup | 0.0119 | 0.0161 | 0.0000 | 0.0000 | 0.0077 | 0.0092 | 0.0000 | 0.0000 |
| connect_time | 0.0011 | 0.0070 | 0.0000 | 0.0000 | 0.0971 | 0.2104 | 0.0000 | 0.0296 |
| provider_time | 0.3089 | 0.4151 | 0.3093 | 0.4255 | 0.5750 | 0.7831 | 0.3472 | 0.5454 |
| response_processing | 0.0008 | 0.0014 | 0.0000 | 0.0001 | 0.0082 | 0.0429 | 0.0000 | 0.0000 |
| overhead | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |

## 4. Evaluation of the Seven Core Questions
1. **p95 Latency vs. Concurrency (C=1..50)**:
   - On clean unconstrained mock, p95 scales from 0.434s (C=1) to 0.486s (C=10), 0.654s (C=25), and 1.301s (C=50) [Measured].
   - On capacity20 optimized, p95 increases from 0.430s (C=1) to 0.463s (C=10), 0.557s (C=25), and 1.001s (C=50) [Measured].
2. **Throughput Plateau**:
   - Optimized profile on capacity20 plateaus at ~56 rps (56.12 rps at C=25, 56.49 rps at C=50; ranges <1 rps) [Measured].
   - This is consistent with theoretical saturation: $\text{Capacity} / \text{Service Time} = 20 / 0.35\text{s} \approx 57.1\text{ rps}$, derived from mock-side service time (p50 0.332s), approximate [Measured].
   - Run1 baseline collapse (18.5 rps, provider_time 0.83 $\to$ 1.8 $\to$ 1.9s over consecutive trials) did not reproduce with 20s cooldowns (53.1 rps, provider_time 0.758 $\to$ 0.752 $\to$ 0.749s) [Measured]. Cause of run1 collapse was previously hypothesized as ephemeral port/socket exhaustion, but this hypothesis is weakened because pooled keepalive connections also degraded under rapid trials [Hypothesis].
3. **Where Request Time Goes**:
   - Total latency strictly partitions into: `total = queue_wait + client_setup + provider_time + backoff_sleep + response_processing + overhead` [Measured].
   - In uncongested runs (clean C=10), time is dominated by `provider_time` (p50 0.334s of 0.343s total) [Measured].
   - In baseline unpooled mode, `client_setup` consumes 8-13ms and `connect_time` consumes 6-109ms [Measured].
   - In pooled mode (`reuse_http_client=True`), `client_setup` and `connect_time` drop to 0.0000s [Measured].
   - Under limiter saturation (overflow C=50), `queue_wait` spikes to p95 0.526s, accounting for the delay prior to rejection [Measured].
4. **Failure Modes Under Load**:
   - In earlier `failures`, timeouts never triggered because slow-tail latency (~0.9s) never reached the 30.0s gateway timeout [Measured].
   - In `failures_v2` (REQUEST_TIMEOUT=1.5s, slow-tail=4.0s), 4s latency spikes successfully triggered gateway timeouts: 26 timeout attempts at C=10 (3 final failures) and 73 timeout attempts at C=50 (9 final failures) [Measured]. Over 87% of timeout attempts recovered on subsequent retries [Measured].
   - Retry budget enforcement (`retry_budget_ratio=0.2`, `retry_budget_floor=10`, `max_attempts=3` from `config.py` [Measured]): capped retries at 112 (510 total requests) at C=50 and 42 (160 total requests) at C=10 [Measured]. Under compounding errors, cumulative retries reached exactly the budget cap (316/316 at C=50 across 3 trials: 111, 214, 316; and 105/106 at C=10: 39, 74, 105), proving the budget was binding [Measured]. Across 36 measured final failures, 16 exhausted `max_attempts` while 20 were denied retries by the budget (`attempts < max_attempts and retryable`) [Measured].
   - **Tradeoff**: Global retry budget strictly protects upstream providers from amplification storms, but converts potentially recoverable failures into final client errors under heavy bursts [Measured]. Recommend tuning the floor/ratio or implementing an adaptive budget based on error rates [Recommendation].
   - Retried requests experienced median backoff sleep of 1.0008s (C=10) and 1.0005s (C=50), consistent with 429 Retry-After headers [Measured].
   - In `overflow` (C=50, limit=10, queue=10), 81 of 100 measured requests (excluding warmup) were deliberate HTTP 503 limiter rejections, not provider failures [Measured]. Closed-loop client workers resubmit immediately upon rejection [Measured].
5. **Impact of Architectural Optimizations & Experiments**:
   - In capacity-bound regime (`capacity20`), connection reuse gave modest gains: 53.1 $\to$ 56.5 rps and p95 1.27 $\to$ 1.00s [Measured].
   - **Exp1b (Final Profile, 7 pairs, C=50, N=500, clean mock)**: Pairs 1-4 showed Arm B ~108-116 rps vs Arm A 57-63 rps (p95 ~0.65-0.70s vs 1.2-1.4s) [Measured]. From trial 5, BOTH arms degraded to 20-32 rps with p95 3-6s, including the pooled arm (reuse 0.83-0.90) [Measured]. Cause of onset degradation from trial 5 is [Unexplained]; candidate mechanisms (system noise like OneDrive sync, antivirus scans, power/thermal throttling, or state accumulation in runner) are [Hypothesis]. Under the pre-declared rule (every trial of one arm beats every trial of the other), Exp1b verdict is **inconclusive** [Measured].
   - **Exp1c (Resources & Fresh Processes, 5 pairs, C=50, N=500)**: Fresh mock and gateway processes launched per trial with 10s cooldown and 1 Hz psutil sampling [Measured]. Under the pre-declared rule (every candidate trial beats every baseline trial), the Exp1c verdict is **inconclusive** (Arm A med=61.23, range 21.11..64.59 rps; Arm B med=110.81, range 27.94..115.49 rps) [Measured]. Transient throughput dips occurred in trial A2 (21.1 rps, p95 2.83s) and trial B4 (27.9 rps, p95 5.37s); both dips recovered immediately in subsequent trials (A3 reached 64.6 rps; B5 reached 115.5 rps) [Measured]. Because fresh mock and gateway processes were launched for every trial, this rules out in-process state accumulation; the root cause of these dips remains [Unexplained].

     | Pair | Arm A (Baseline) Throughput (rps) | Arm A p95 (s) | Arm B (Optimized) Throughput (rps) | Arm B p95 (s) | Arm B / Arm A Throughput Ratio |
     | :--- | :--- | :--- | :--- | :--- | :--- |
     | Pair 1 | 60.34 | 1.285 | 110.81 | 0.679 | 1.84x |
     | Pair 2 | 21.11 (dip) | 2.832 | 108.15 | 0.683 | 5.12x |
     | Pair 3 | 64.59 | 1.209 | 114.23 | 0.664 | 1.77x |
     | Pair 4 | 61.23 | 1.252 | 27.94 (dip) | 5.366 | 0.46x |
     | Pair 5 | 63.51 | 1.187 | 115.49 | 0.646 | 1.82x |

   - **Descriptive Post-Hoc Summary (Clearly Labeled)**: While pre-declared rules label Exp1b and Exp1c inconclusive due to isolated dips, in every non-dip trial of Exp1, Exp1b, and Exp1c, the optimized arm consistently ran at ~1.8-1.9x baseline throughput (Exp1 medians: 89.3 vs 49.6 rps, ~1.80x; Exp1b non-dip pairs 1-4: 108-116 vs 57-63 rps, ~1.8-1.9x; Exp1c non-dip pairs 1, 3, 5: 111-115 vs 60-65 rps, ~1.77-1.84x) [Post-Hoc Analysis].
   - **CPU per Request**: Derived from $(\text{system CPU\%} \times 12\text{ logical CPUs}) / \text{throughput}$, baseline consumed ~0.044 CPU-s per request vs optimized consuming ~0.019 CPU-s per request (approximate, since system-wide CPU includes background OS activity) [Derived Metric].
   - **Process-Tree Resource Profile & Single-Thread Bottleneck Analysis**: The earlier gateway RSS measurement of 5.1 MB reflected only the Windows virtual environment launcher stub (`.venv\Scripts\python.exe`), not the real interpreter process tree. When measured across complete process trees (normalized to percent of one core, where 100% = 1 full core):
     - **Gateway**: baseline CPU% mean 55.9% (max 70.4%), RSS mean 118.7 MB (max 122.2 MB), 9 threads; optimized CPU% mean 50.3% (max 62.8%), RSS mean 77.8 MB (max 78.3 MB), 8 threads [Measured].
     - **Mock**: baseline CPU% mean 2.6% (max 5.9%), RSS mean 68.2 MB (max 75.6 MB), 9 threads; optimized CPU% mean 4.6% (max 6.9%), RSS mean 60.7 MB (max 60.7 MB), 9 threads [Measured].
     - **Runner**: baseline CPU% mean 105.4% (max 121.2%), RSS mean 272.6 MB (max 284.7 MB), 24 threads; optimized CPU% mean 108.3% (max 117.4%), RSS mean 264.1 MB (max 264.8 MB), 24 threads [Measured].
     - **Bottleneck Interpretation**: The load generator (runner) operates near and above 100% of one core (mean ~105-108%, max ~117-121%), and the gateway peaks at 63-70% of one core. Aggregate system-wide CPU alone (14-17% mean across 12 cores) cannot rule out a single-thread bottleneck, as an individual single-threaded event loop or thread can reach full core saturation even when multi-core host utilization remains low [Measured/Analysis].
   - **Original Exp1**: Used `keepalive=20` on both arms; Arm B reported 0.00 reuse due to pool churn (`max_keepalive=20 < C=50`) [Measured].
   - **Original Exp2**: Used `keepalive=5` (pool=10) restricting throughput to 27.1 rps vs `keepalive=50` (pool=100) achieving 81.8 rps [Measured].
   - **Original Exp3**: Release slot during backoff yielded medians 37.85 vs 47.69 rps; inconclusive due to overlapping ranges [Measured].
6. **Blaming Provider vs. Gateway/Client**:
   - Provider fault: `provider_time` spikes while `queue_wait`, `client_setup`, and `connect_time` remain flat [Recommendation, based on latency bucket decomposition in Table 2].
   - Gateway queue bottleneck: `queue_wait` dominates while `provider_time` remains low [Recommendation, based on overflow scenario where queue_wait spikes to 0.526s].
   - Gateway/Network transport bottleneck: elevated `connect_time` and `client_setup` while provider mock service time is normal [Recommendation, based on Exp1 baseline connect_time spiking to 109ms].
   - Client bottleneck: large positive gap between client round-trip and gateway `total` (0 violations observed across benchmark runs) [Recommendation, based on client-gateway timing invariance]. In run1 trial 2-3, mock handler service time remained ~0.33s while gateway provider_time spiked to ~1.8s, indicating delay outside the mock handler with cause unverified [Hypothesis].
7. **Required Production Enhancements**:
   - Implement persistent HTTP client pooling with `pool_max_keepalive >= concurrency_limit` [Recommendation, based on Exp1/Exp1b/Exp1c findings].
   - Add upstream error-body inspection to classify unrecoverable errors (`insufficient_quota`, `credit_balance_exhausted`) as fatal non-retryable [Recommendation, based on OpenAI 60/60 429 exhaustion].
   - Configure bounded admission queues with adaptive `Retry-After` signaling [Recommendation, based on overflow rejection preventing queue bloat].
   - Add circuit breaker to trip open on sustained upstream 5xx/timeouts and fail fast [Recommendation].
   - Export real-time Prometheus metrics and streaming histograms (e.g., HDRHistogram) instead of an in-memory bounded deque [Recommendation].
   - Propagate downstream client cancellation upstream via HTTP transport abort [Recommendation].
   - Support streaming responses (SSE/chunked) with time-to-first-token (TTFT) tracking [Recommendation].
   - Implement gateway-level authentication, per-API-key rate limiting, and graceful drain on shutdown [Recommendation].

## 5. Failure-Rate Accounting & Injection Mechanics
- **Configured vs. Mock-Injected Counts**: In `failures_v2`, configured fault rates (10% 429, 5% 500, 2% drop, 5% slow-tail) yielded exact mock server injection counts across 3 trials: 63 429s, 18 500s, 12 drops, 26 slow-tails at C=10 (N=480); 162 429s, 80 500s, 27 drops, 73 slow-tails at C=50 (N=1530) [Measured].
- **Gateway Attempt Matching**: Gateway attempt counts matched mock injections with precision: 429 attempts matched 63/63 (C=10) and 162/162 (C=50); timeout attempts matched slow-tails 26/26 (C=10) and 73/73 (C=50) [Measured].
- **Resolution of Injected Drops vs. 5xx/Connection Errors**: Rather than connection errors inherently scaling with load, mock drops (`RuntimeError`) are intercepted by ASGI server middleware and returned as HTTP 500 unless an abrupt TCP reset occurs [Hypothesis; only the counts are Measured]. At C=10, all 12 injected drops surfaced as HTTP 500s (18 mock 500s + 12 drops = 30 gateway 5xx attempts; 0 connection attempts) [Measured]. At C=50, of 27 injected drops, 12 surfaced as connection drops and 15 surfaced as 5xx attempts (80 mock 500s + 15 drops = 95 gateway 5xx attempts) [Measured]. This explains the earlier 5xx discrepancy (29 vs 19 at C=10, 90 vs 81 at C=50) [Measured].
- **Final Outcomes & Latency**: Final unrecovered failures remained minimal: 3 timeouts, 6 429s, 5 5xx at C=10; 9 timeouts, 9 429s, 8 5xx at C=50 (0 connection failures) [Measured]. Client latency medians under `failures_v2` were: C=10 p50 0.348s, p95 2.169s, p99 2.644s (throughput 14.54 rps); C=50 p50 0.361s, p95 1.993s, p99 2.540s (throughput 65.52 rps) [Measured]. Unverifiable without kernel packet capture: the exact TCP socket conditions that cause uvicorn to reset a connection versus completing a 500 response on unhandled exceptions [Hypothesis].

## 6. Root Cause Bottleneck Diagnoses
- **Most Significant Bottleneck Diagnosis**: In an unconstrained upstream regime (clean mock capacity 100), the most significant bottleneck is per-request HTTP client instantiation and undersized keepalive connection pooling (`max_keepalive < C`), which forces continuous TCP reconnect churn (consuming 8-13ms client setup and up to 109ms connect time per request) and caps throughput at ~57 rps; sizing `pool_max_keepalive = 100 >= C` eliminates connection churn and raises peak throughput to 108-116 rps [Measured]. In a capacity-constrained regime (capacity 20), upstream provider capacity (theoretical ~57.1 rps, derived from mock-side service time p50 0.332s approximate) is the hard ceiling; client pooling yields only marginal throughput gains (53.1 $\to$ 56.5 rps) because queuing shifts upstream [Measured].
- **Unconstrained Regime & Pool Churn**: Baseline 0.00 connection reuse is by design (new client per request). In original Exp1 reuse-ON arm, `connection_reused` reported 0.00 because `max_keepalive=20 < C=50`, causing continuous connection eviction and TCP reconnects (mock is plain HTTP, no TLS) [Measured]. Sizing `pool_max_keepalive=100 >= C` in Exp1b/Exp1c eliminated pool churn and restored 92% connection reuse [Measured].

## 7. Real-Provider Validation (Groq & OpenAI)
- **OpenAI (gpt-4o-mini)**: Returned 60/60 HTTP 429 `insufficient_quota` / `credit_balance_exhausted` (0 successes) [Measured].
- **Groq (allam-2-7b, N=30, C=3, max_tokens=16)**:
  - *Baseline* (1 trial): 30/30 success, 3.69 rps, p50 0.74s, p95 1.54s, p99 2.03s, connect+TLS p50 0.33s (~45% of median latency) [Measured].
  - *Clean Optimized* (after 180s cooldown): 30/30 success, 5.38 rps, p50 0.38s, p95 1.69s, p99 1.71s, reuse 90.0% (3 initial connections for 30 requests), connect p50 0.00s, 1.00 attempts/request [Measured].
  - *Nonexistent Model 404*: A Groq request configured with a nonexistent model returned HTTP 404 with 1 attempt (non-retryable 4xx correctly rejected without retrying) [Measured].
  - *Interpretation*: Median latency and throughput improved by roughly the connect+TLS handshake time (~0.33s) [Measured]. Differences in p95/p99 are not statistically supported due to small sample size (N=30, single trial, fixed order, public internet variance) [Hypothesis].
  - *Quota Confound*: An intermediate optimized run immediately following baseline was throttled by Groq free-tier rate limits (11/30 successes, 429 with Retry-After 2.0s, 1.53 attempts/req vs max 3, confirming retry budget enforcement) and is discarded as an order confound [Measured].
- **Provider Note**: This evaluates gateway configurations on a single provider, not relative provider superiority [Measured].

## 8. Prioritized Recommendations & Limitations
1. **Priority 1 - Keepalive Pool Sizing**: Mandate `pool_max_keepalive >= concurrency_limit` in production HTTP clients to avoid silent connection pool churn [Recommendation, based on Exp1/Exp1b/Exp1c pool churn findings].
2. **Priority 2 - Error Body Inspection**: Inspect upstream 429 response JSON bodies to detect permanent exhaustion (`insufficient_quota`, `credit_balance_exhausted`) and fail fast without exhausting retry budgets [Recommendation, based on OpenAI 60/60 exhaustion audit].
3. **Priority 3 - Bounded Concurrency Queues**: Enforce strict gateway-level concurrency limits with bounded queues and immediate 503 + Retry-After rejection to protect backends from queue-buildup degradation [Recommendation, based on overflow scenario].

### Limitations
- **RNG Replay Across Trials**: Mock server `/control/reset` reseeds the RNG (and experiment trials restart mock subprocesses with the same seed), meaning trials replay the exact same synthetic latency and fault injection sequences; inter-trial variance reflects process/transport timing, not random workload variation [Limitation].
- **Closed-Loop Load Generator**: Closed-loop workers wait for request completion before sending the next request, introducing coordinated omission under queue delays [Limitation].
- **Single Machine Execution**: Gateway, mock provider, and load generator share CPU, memory, and OS network stack on a single host, making runs vulnerable to transient OS/antivirus/background interference [Limitation].
- **TTFT & Streaming**: TTFT is not measured due to complete synchronous payload transfer [Limitation].
- **Handshake Failures**: Connect time is not recorded on failed attempts where handshake fails before transport completion [Limitation].
- **Cancellation**: Upstream server cancellation upon client disconnect is unverified across network boundaries [Limitation].
- **Sample Size**: Single-trial real provider tests reflect transient network variance rather than definitive provider benchmarks [Limitation].
- **1 Hz Sampling Granularity**: Resource metrics are sampled at 1 Hz, which averages utilization over 1-second intervals and may smooth out sub-second utilization bursts or short event loop freezes [Limitation].
- **Single Resource-Profile Trial Per Arm**: Process-tree resource profiles were collected during a single paired trial run per arm (C=50, N=500) rather than multi-trial distributions [Limitation].
- **Unexplained Dips**: Isolated throughput collapses (trial A2 at 21.1 rps, trial B4 at 27.9 rps) occurring with fresh processes remain [Unexplained]; transient background OS activity, antivirus interference, or thread scheduling artifacts cannot be ruled out [Limitation].
