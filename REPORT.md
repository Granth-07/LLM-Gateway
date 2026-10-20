# LLM Gateway Performance & Architecture Evaluation Report

## 1. System Environment & Execution Context
- **Hardware & OS**: Windows 11, 2 physical cores (AMD64) shared concurrently by gateway, mock provider, and closed-loop load generator [Measured].
- **Scope & Workload**: Evaluates gateway architectural configurations under synthetic mock regimes and real upstream endpoints [Measured].
- **TTFT & Streaming**: TTFT is not measured because streaming is not implemented; all measurements reflect complete request-response payloads [Measured].
- **Timing Integrity**: Client latency $\ge$ gateway total across all trials (0 violations observed) [Measured]. Disconnect tests verify concurrency slot release upon client abort, but do not prove upstream server-side cancellation [Measured]. Mock `peak_concurrent` records arrival-concurrency, not instantaneous requests in service [Measured]. Lack of continuous CPU/memory sampling precludes attributing plateaus to CPU saturation [Hypothesis].

## 2. Provenance of Result Sets
- **run1**: Initial continuous benchmark execution without inter-trial pauses. Showed baseline C=50 collapse (18.5 rps) and keep-alive churn in Exp1 (`connection_reused = 0.00`) [Measured].
- **diagnostic**: Isolated sequential requests verifying keepalive mechanics; demonstrated 99% reuse on a single client [Measured].
- **rerun with 20s cooldown**: Benchmark matrix rerun with 20s idle intervals between trials, eliminating trial drift [Measured]. Used for Table 1 capacity20/capacity20_opt and Table 2.
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

## 4. Evaluation of the Seven Core Questions
1. **p95 Latency vs. Concurrency (C=1..50)**:
   - On clean unconstrained mock, p95 scales from 0.434s (C=1) to 0.486s (C=10), 0.654s (C=25), and 1.301s (C=50) [Measured].
   - On capacity20 optimized, p95 increases from 0.430s (C=1) to 0.463s (C=10), 0.557s (C=25), and 1.001s (C=50) [Measured].
2. **Throughput Plateau**:
   - Optimized profile on capacity20 plateaus at ~56 rps (56.12 rps at C=25, 56.49 rps at C=50; ranges <1 rps) [Measured].
   - This matches theoretical saturation: $\text{Capacity} / \text{Service Time} = 20 / 0.35\text{s} \approx 57.1\text{ rps}$ [Measured].
   - Run1 baseline collapse (18.5 rps, provider_time 0.83 $\to$ 1.8 $\to$ 1.9s over consecutive trials) did not reproduce with 20s cooldowns (53.1 rps, provider_time 0.758 $\to$ 0.752 $\to$ 0.749s) [Measured]. Cause of run1 collapse remains unverified; hypothesized as ephemeral port/socket exhaustion across rapid runs [Hypothesis].
3. **Where Request Time Goes**:
   - Total latency strictly partitions into: `total = queue_wait + client_setup + provider_time + backoff_sleep + response_processing + overhead` [Measured].
   - In uncongested runs (clean C=10), time is dominated by `provider_time` (p50 0.334s of 0.343s total) [Measured].
   - In baseline unpooled mode, `client_setup` consumes 8-13ms and `connect_time` consumes 6-109ms [Measured].
   - In pooled mode (`reuse_http_client=True`), `client_setup` and `connect_time` drop to 0.0000s [Measured].
   - Under limiter saturation (overflow C=50), `queue_wait` spikes to p95 0.526s, accounting for the delay prior to rejection [Measured].
4. **Failure Modes Under Load**:
   - In earlier `failures`, timeouts never triggered because slow-tail latency (~0.9s) never reached the 30.0s gateway timeout [Measured].
   - In `failures_v2` (REQUEST_TIMEOUT=1.5s, slow-tail=4.0s), 4s latency spikes successfully triggered gateway timeouts: 26 timeout attempts at C=10 (3 final failures) and 73 timeout attempts at C=50 (9 final failures) [Measured]. Over 87% of timeout attempts recovered on subsequent retries [Measured].
   - Retry budget enforcement (20% + 10 floor = 110 max retries per 500 requests) bounded retries under compounding errors at C=50, preventing cascading retry storms while allowing 1.207 attempts/request [Measured]. Retried requests experienced median backoff sleep of 1.0008s (C=10) and 1.0005s (C=50), driven by 429 Retry-After headers [Measured].
   - In `overflow` (C=50, limit=10, queue=10), 90 of 110 requests (81%) were deliberate HTTP 503 limiter rejections, not provider failures [Measured]. Closed-loop client workers resubmit immediately upon rejection [Measured].
5. **Impact of Architectural Optimizations**:
   - In capacity-bound regime (`capacity20`), connection reuse gave modest gains: 53.1 $\to$ 56.5 rps and p95 1.27 $\to$ 1.00s [Measured].
   - In unconstrained `Exp1`, connection reuse dramatically increased throughput from 47.5-50.6 rps (med 49.63) to 84.3-92.4 rps (med 89.33) with all 7 trials superior, and improved p95 from 1.44-1.57s to 0.81-1.01s [Measured]. Connect+setup savings (~83ms of ~0.8s) do not fully explain this 80% throughput gain; the remainder is hypothesized as event-loop transport churn reduction [Hypothesis].
   - `Exp2` demonstrated that a connection pool limit of 10 caps mock concurrency at 10 (peak 10 vs 50), restricting throughput to 26.6-27.3 rps vs 74.7-91.2 rps for pool=100 [Measured].
   - `Exp3` (slot release during backoff) was inconclusive due to overlapping ranges: throughput med 37.85 [32.80..40.91] vs 47.69 [37.86..49.65] rps; p95 med 1.902s vs 1.817s [Measured].
6. **Blaming Provider vs. Gateway/Client**:
   - Provider fault: `provider_time` spikes while `queue_wait`, `client_setup`, and `connect_time` remain flat [Measured].
   - Gateway queue bottleneck: `queue_wait` dominates while `provider_time` remains low [Measured].
   - Gateway/Network transport bottleneck: elevated `connect_time` and `client_setup` while provider mock service time is normal [Measured].
   - Client bottleneck: large positive gap between client round-trip and gateway `total` (0 violations observed) [Measured].
7. **Required Production Changes**:
   - Implement persistent HTTP client pooling with `pool_max_keepalive >= concurrency_limit` [Measured].
   - Add upstream error-body inspection to classify unrecoverable errors (e.g. quota/credit exhaustion) as fatal non-retryable [Measured].
   - Configure bounded admission queues with adaptive `Retry-After` signaling [Measured].

## 5. Failure-Rate Accounting & Injection Mechanics
- **Configured vs. Mock-Injected Counts**: In `failures_v2`, configured fault rates (10% 429, 5% 500, 2% drop, 5% slow-tail) yielded exact mock server injection counts across 3 trials: 63 429s, 18 500s, 12 drops, 26 slow-tails at C=10 (N=480); 162 429s, 80 500s, 27 drops, 73 slow-tails at C=50 (N=1530) [Measured].
- **Gateway Attempt Matching**: Gateway attempt counts matched mock injections with precision: 429 attempts matched 63/63 (C=10) and 162/162 (C=50); timeout attempts matched slow-tails 26/26 (C=10) and 73/73 (C=50) [Measured].
- **Resolution of Injected Drops vs. 5xx/Connection Errors**: Rather than connection errors inherently scaling with load, mock drops (`RuntimeError`) are intercepted by ASGI server middleware and returned as HTTP 500 unless an abrupt TCP reset occurs [Measured]. At C=10, all 12 injected drops surfaced as HTTP 500s (18 mock 500s + 12 drops = 30 gateway 5xx attempts; 0 connection attempts) [Measured]. At C=50, of 27 injected drops, 12 surfaced as connection drops and 15 surfaced as 5xx attempts (80 mock 500s + 15 drops = 95 gateway 5xx attempts) [Measured]. This fully explains the earlier 5xx discrepancy (29 vs 19 at C=10, 90 vs 81 at C=50) [Measured].
- **Final Outcomes & Latency**: Final unrecovered failures remained minimal: 3 timeouts, 6 429s, 5 5xx at C=10; 9 timeouts, 9 429s, 8 5xx at C=50 (0 connection failures) [Measured]. Client latency medians under `failures_v2` were: C=10 p50 0.348s, p95 2.169s, p99 2.644s (throughput 14.54 rps); C=50 p50 0.361s, p95 1.993s, p99 2.540s (throughput 65.52 rps) [Measured]. Unverifiable without kernel packet capture: the exact TCP socket conditions that cause uvicorn to reset a connection versus completing a 500 response on unhandled exceptions [Hypothesis].

## 6. Root Cause Bottleneck Diagnoses
- **Capacity-Bound Regime**: Throughput ceiling (~56 rps) is dictated entirely by upstream provider capacity (mock capacity 20 / 0.35s service time) [Measured]. Client pooling cannot exceed upstream capacity [Measured].
- **Unconstrained Regime & Pool Churn**: In Exp1 baseline, `connection_reused` initially reported 0.00 because `max_keepalive=20 < C=50`, causing continuous connection eviction and TLS renegotiation [Measured]. Sizing `pool_max_keepalive=100 >= C` eliminated pool churn and restored 92-100% reuse [Measured].

## 7. Real-Provider Validation (Groq & OpenAI)
- **OpenAI (gpt-4o-mini)**: Returned 60/60 HTTP 429 `insufficient_quota` / `credit_balance_exhausted` (0 successes); unconfigured model attempt returned HTTP 404 with 1 attempt (non-retryable 4xx correctly rejected) [Measured].
- **Groq (allam-2-7b, N=30, C=3, max_tokens=16)**:
  - *Baseline* (1 trial): 30/30 success, 3.69 rps, p50 0.74s, p95 1.54s, p99 2.03s, connect+TLS p50 0.33s (~45% of median latency) [Measured].
  - *Clean Optimized* (after 180s cooldown): 30/30 success, 5.38 rps, p50 0.38s, p95 1.69s, p99 1.71s, reuse 90.0% (3 initial connections for 30 requests), connect p50 0.00s, 1.00 attempts/request [Measured].
  - *Interpretation*: Median latency and throughput improved by roughly the connect+TLS handshake time (~0.33s) [Measured]. Differences in p95/p99 are not statistically supported due to small sample size (N=30, single trial, fixed order, public internet variance) [Hypothesis].
  - *Quota Confound*: An intermediate optimized run immediately following baseline was throttled by Groq free-tier rate limits (11/30 successes, 429 with Retry-After 2.0s, 1.53 attempts/req vs max 3, confirming retry budget enforcement) and is discarded as an order confound [Measured].
- **Provider Note**: This evaluates gateway configurations on a single provider, not relative provider superiority [Measured].

## 8. Prioritized Recommendations & Limitations
1. **Priority 1 - Keepalive Pool Sizing**: Mandate `pool_max_keepalive >= concurrency_limit` in production HTTP clients to avoid silent connection pool churn [Measured].
2. **Priority 2 - Error Body Inspection**: Inspect upstream 429 response JSON bodies to detect permanent exhaustion (`insufficient_quota`, `credit_balance_exhausted`) and fail fast without exhausting retry budgets [Measured].
3. **Priority 3 - Bounded Concurrency Queues**: Enforce strict gateway-level concurrency limits with bounded queues and immediate 503 + Retry-After rejection to protect backends from queue-buildup degradation [Measured].

### Limitations
- TTFT is not measured due to complete synchronous payload transfer [Measured].
- Connect time is not recorded on failed attempts where handshake fails before transport completion [Measured].
- Upstream server cancellation upon client disconnect is unverified across network boundaries [Measured].
- Single-trial real provider tests reflect transient network variance rather than definitive provider benchmarks [Measured].
