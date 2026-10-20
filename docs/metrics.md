# LLM Gateway Metrics Specification

This document defines all performance metrics, timing intervals, counters, statistical methods, and accounting rules implemented in the LLM Gateway.

---

## 1. Request Timing Breakdown

All durations are measured exclusively using `time.perf_counter()`. Durations are reported in seconds with microsecond resolution.

### Timing Formula and Non-Overlapping Residual
Every completed request satisfies the strict non-overlapping sum invariant:
```
total = queue_wait + client_setup + provider_time + backoff_sleep + response_processing + overhead
overhead = total - (queue_wait + client_setup + provider_time + backoff_sleep + response_processing)
```

| Metric | What it Starts Measuring | What it Stops Measuring | Units | Exclusions / Notes |
| :--- | :--- | :--- | :--- | :--- |
| `total` | Request entry into `Pipeline.handle()`. | `ChatResponse` model fully constructed. | seconds | Total end-to-end gateway latency for the request. |
| `queue_wait` | Inbound task enters limiter queue waiting for a concurrency slot. | Slot acquired (or 0.0 if slot immediately available). | seconds | Excludes provider execution and backoff. 0.0 when slots are immediately free. |
| `client_setup` | Request begins acquiring HTTP client from `ClientPool`. | HTTP client yielded and provider instance created. | seconds | Excludes queue wait and provider network calls. Near 0 when client pooling is enabled. |
| `provider_time` | Immediately prior to calling `provider.complete()`. | Provider call returns response or raises exception. | seconds | Summed across all retry attempts for the request. Includes HTTP network I/O and provider generation time. |
| `connect_time` | TCP handshake / TLS negotiation begins. | Connection established. | seconds | **Informational subset of `provider_time`**. NEVER summed into `total` or residual. Reported by provider transport when new connection is opened (0.0 on pooled keep-alive). |
| `backoff_sleep` | Start of backoff delay between retry attempts. | Backoff sleep completes. | seconds | Summed across all retry attempts. Concurrency slot is intentionally held during backoff to throttle retry storms. |
| `response_processing`| Provider call returns result to pipeline. | `ChatResponse` Pydantic model is fully constructed. | seconds | Measures gateway serialization/deserialization and response assembly. |
| `overhead` | Residual interval. | Residual interval. | seconds | Computed as `total - (queue_wait + client_setup + provider_time + backoff_sleep + response_processing)`. Represents internal pipeline dispatch and async event loop overhead. Must be $\ge 0.0$. |
| `clamped_overhead_count` | N/A | N/A | integer | Global counter incremented if raw overhead is $< 0.0$ prior to clamping to 0.0. Must always be `0` in correct runs. |

### Streaming and Time to First Token (TTFT)
> [!IMPORTANT]
> **TTFT (Time to First Token) is explicitly NOT measured** because streaming responses are not implemented in this gateway. The gateway currently processes synchronous, complete chat completion responses.

---

## 2. Statistical Methodology

### Nearest-Rank Percentiles
Percentiles ($p50$, $p95$, $p99$) are calculated using the single canonical **nearest-rank method** defined in `src/gateway/metrics/stats.py`:

$$\text{rank}\ k = \left\lceil \frac{p}{100} \times N \right\rceil$$

Where:
- $N$ is the number of samples in the sorted sample list.
- $k$ is the 1-based index (0-based index: $k - 1$).
- Clamped to $[0, N - 1]$. For empty sets, returns `0.0`.

### Separation of Successful vs. Failed Request Latency
- **Successful Requests**: Per-bucket timing breakdowns (`queue_wait`, `provider_time`, `connect_time`, `response_processing`, `overhead`, and `total`) are computed **exclusively over successful requests**. This prevents failed requests (which abort early or timeout) from distorting normal latency distributions.
- **Failed Requests**: Durations for failed requests are tracked and reported in a separate latency section (`latency.failed.total`), preserving visibility into timeout and failure times.

### Throughput Calculation
Throughput is defined strictly as:
$$\text{Throughput} = \frac{\text{Completed Final Successes}}{\text{last\_end} - \text{first\_start}}$$

- **Measurement**: Computed solely using `time.perf_counter()` timestamps.
- **Window**: Spans from the arrival timestamp (`first_start`) of the earliest request to the completion timestamp (`last_end`) of the final request in the trial.
- **Units**: Requests per second (rps).

---

## 3. Error and Rejection Accounting

The gateway strictly distinguishes between **attempt-level events**, **final request outcomes**, **overflow rejections**, and **client cancellations**.

### Attempt-Level vs. Final-Level Counters
1. **Attempt Level (`counters.attempts`)**:
   - Increments on **every individual call** made to the upstream provider.
   - Buckets: `success`, `timeout`, `429`, `5xx`, `connection`, `auth_4xx`.
   - A single client request with 2 retries contributes 3 attempt records.
2. **Final Level (`counters.finals`)**:
   - Increments **only once per completed request**, recording its ultimate outcome.
   - Buckets: `success`, `timeout`, `429`, `5xx`, `connection`, `auth_4xx`.
   - If attempt 1 returned 429 and attempt 2 succeeded, the attempt counters show `429: 1, success: 1`, while the final counters show `success: 1`.

### Overflow Rejects vs. Provider Failures
- **Overflow Rejects (`counters.rejects`)**:
  - Occur **before** a request acquires a concurrency slot.
  - `queue_full`: Inbound request arrived when the wait queue reached `queue_size` capacity. Rejected immediately with HTTP 503 + `Retry-After`.
  - `queue_timeout`: Queued request waited longer than `queue_wait_timeout` without acquiring a slot. Rejected with HTTP 503 + `Retry-After`.
  - These never invoke the provider and are counted separately from provider failures.
- **Provider Failures**:
  - Occur **after** acquiring a concurrency slot.
  - Either non-retryable errors (e.g., auth failure, 4xx) or retryable errors whose retry budget, max attempts, or total deadline were exhausted.
- **Client Cancellations (`counters.client_cancelled`)**:
  - Occur when the HTTP client disconnects while the request is queued or in-flight.
  - Promptly cancels upstream work, releases the concurrency slot, and increments `client_cancelled`.

---

## 4. Concurrency Limiter Gauges

The concurrency limiter tracks both real-time instantaneous states and persistent peak gauges:

| Gauge | Description | Resettable |
| :--- | :--- | :--- |
| `waiting` | Current number of tasks waiting in queue for a slot. | No (real-time gauge) |
| `slots_held` | Current number of concurrency slots held. | No (real-time gauge) |
| `in_flight` | Current number of active provider calls (excludes backoff sleep). | No (real-time gauge) |
| `peak_waiting` | Maximum observed tasks waiting in queue simultaneously. | Yes (via `POST /metrics/reset`) |
| `peak_slots_held` | Maximum observed concurrency slots held simultaneously. | Yes (via `POST /metrics/reset`) |
| `peak_in_flight` | Maximum observed concurrent upstream calls. Matches mock provider peak concurrency. | Yes (via `POST /metrics/reset`) |

---

## 5. Endpoints

- `GET /metrics`: Returns JSON object with complete metrics breakdown (requests, counters, rates, percentiles, limiter gauges, timing integrity).
- `POST /metrics/reset`: Clears the metrics collector (counters, bounded deque, throughput window), resets limiter peak gauges to current gauge values, and resets the clamped overhead counter. Used before starting benchmark trials.
