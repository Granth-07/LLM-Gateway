# LLM Gateway Benchmark & Experiment Summary Tables

## 1. Scenario Concurrency Matrix

| Scenario | C | N | Trials | Throughput rps (Median [Range]) | p50 Latency (s) | p95 Latency (s) | p99 Latency (s) | Attempt Failures | Final Failures | Rejected (Limiter) | Queue Wait p95 (s) | Client Setup p50/p95 (s) | Connect Mean (s) | Conn Reused Frac | Little's Law L (vs C) | Peak In-Flight (GW / Mock) | Client-GW Gap (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| clean | 1 | 30 | 3 | 3.02 [0.03] (3.02, 2.99, 3.02) | 0.330 [0.010] | 0.434 [0.016] | 0.453 [0.012] | none | none | 0 | 0.0000 [0.0000] | 0.0119 / 0.0161 | 0.0017 | 0.00 | L=1.00 (C=1) | GW:1 / Mock:1 | 0.0050 |
| clean | 10 | 150 | 3 | 27.27 [0.23] (27.27, 27.12, 27.35) | 0.343 [0.005] | 0.486 [0.011] | 0.565 [0.038] | none | none | 0 | 0.0000 [0.0000] | 0.0083 / 0.0134 | 0.0066 | 0.00 | L=9.53 (C=10) | GW:10 / Mock:10 | 0.0062 |
| clean | 25 | 300 | 3 | 51.55 [3.53] (49.46, 51.55, 52.99) | 0.405 [0.020] | 0.654 [0.135] | 0.848 [0.031] | none | none | 0 | 0.0000 [0.0000] | 0.0080 / 0.0094 | 0.0252 | 0.00 | L=22.31 (C=25) | GW:25 / Mock:25 | 0.0257 |
| clean | 50 | 500 | 3 | 59.14 [2.33] (59.14, 59.5, 57.17) | 0.724 [0.037] | 1.301 [0.133] | 1.572 [0.073] | none | none | 0 | 0.0000 [0.0000] | 0.0077 / 0.0092 | 0.1094 | 0.00 | L=45.49 (C=50) | GW:50 / Mock:50 | 0.1204 |
| capacity20 | 1 | 30 | 3 | 2.97 [0.03] (2.99, 2.97, 2.96) | 0.332 [0.005] | 0.439 [0.017] | 0.467 [0.003] | none | none | 0 | 0.0000 [0.0000] | 0.0133 / 0.0213 | 0.0023 | 0.00 | L=1.00 (C=1) | GW:1 / Mock:1 | 0.0063 |
| capacity20 | 10 | 150 | 3 | 26.76 [1.21] (27.93, 26.72, 26.76) | 0.346 [0.013] | 0.493 [0.014] | 0.585 [0.100] | none | none | 0 | 0.0000 [0.0000] | 0.0080 / 0.0137 | 0.0071 | 0.00 | L=9.59 (C=10) | GW:10 / Mock:10 | 0.0076 |
| capacity20 | 25 | 300 | 3 | 50.97 [0.26] (50.97, 51.15, 50.89) | 0.414 [0.014] | 0.677 [0.078] | 0.956 [0.034] | none | none | 0 | 0.0000 [0.0000] | 0.0079 / 0.0095 | 0.0215 | 0.00 | L=22.33 (C=25) | GW:25 / Mock:25 | 0.0225 |
| capacity20 | 50 | 500 | 3 | 53.13 [0.57] (52.89, 53.13, 53.46) | 0.795 [0.014] | 1.266 [0.265] | 1.476 [0.281] | none | none | 0 | 0.0000 [0.0000] | 0.0078 / 0.0090 | 0.0253 | 0.00 | L=44.57 (C=50) | GW:50 / Mock:50 | 0.0250 |
| failures | 10 | 150 | 3 | 17.46 [0.54] (17.8, 17.26, 17.46) | 0.376 [0.014] | 1.381 [0.056] | 2.053 [0.579] | 429:59, 5xx:29 | 429:2, 5xx:2 | 0 | 0.0000 [0.0000] | 0.0087 / 0.0140 | 0.0051 | 0.12 | L=8.64 (C=10) | GW:10 / Mock:10 | 0.0061 |
| failures | 50 | 500 | 3 | 49.95 [2.04] (48.97, 49.95, 51.01) | 0.661 [0.007] | 1.791 [0.031] | 2.483 [0.154] | 429:155, 5xx:90, connection:23 | 429:1, 5xx:1 | 0 | 0.0000 [0.0000] | 0.0078 / 0.0095 | 0.0707 | 0.13 | L=42.01 (C=50) | GW:50 / Mock:42 | 0.0931 |
| overflow | 50 | 100 | 1 | 6.72 [0.00] (6.72) | 0.065 [0.000] | 1.296 [0.000] | 1.404 [0.000] | none | none | 90 | 0.5263 [0.0000] | 0.0084 / 0.0103 | 0.0143 | 0.00 | L=1.94 (C=50) | GW:10 / Mock:10 | 0.5575 |
| capacity20_opt | 1 | 30 | 3 | 3.12 [0.05] (3.15, 3.1, 3.12) | 0.315 [0.008] | 0.430 [0.010] | 0.447 [0.014] | none | none | 0 | 0.0000 [0.0000] | 0.0000 / 0.0000 | 0.0000 | 1.00 | L=1.00 (C=1) | GW:1 / Mock:1 | 0.0062 |
| capacity20_opt | 10 | 150 | 3 | 28.77 [0.68] (28.41, 28.77, 29.09) | 0.322 [0.003] | 0.463 [0.000] | 0.549 [0.009] | none | none | 0 | 0.0000 [0.0000] | 0.0000 / 0.0000 | 0.0000 | 1.00 | L=9.47 (C=10) | GW:10 / Mock:10 | 0.0072 |
| capacity20_opt | 25 | 300 | 3 | 56.12 [0.94] (55.35, 56.12, 56.29) | 0.394 [0.006] | 0.557 [0.018] | 0.679 [0.031] | none | none | 0 | 0.0000 [0.0000] | 0.0000 / 0.0000 | 0.0008 | 0.95 | L=22.52 (C=25) | GW:25 / Mock:25 | 0.0077 |
| capacity20_opt | 50 | 500 | 3 | 56.49 [0.97] (56.49, 56.83, 55.86) | 0.789 [0.004] | 1.001 [0.016] | 1.082 [0.024] | none | none | 0 | 0.0000 [0.0000] | 0.0000 / 0.0000 | 0.0033 | 0.92 | L=45.24 (C=50) | GW:50 / Mock:50 | 0.0076 |

## 2. Capacity20 Profile Comparison: Optimized vs Baseline

| Concurrency C | Profile | N | Trials | Throughput rps (Median [Range]) | p95 Latency (s) (Median [Range]) | Connect Time p50 (s) | Conn Reused Frac | Mock Peak (Capacity 20) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C=1 | Baseline (reuse=off) | 30 | 3 | 2.97 [0.03] | 0.439 [0.017] | 0.0015 | 0.00 | 1 |
| C=1 | Optimized (reuse=on, pool=100) | 30 | 3 | 3.12 [0.05] | 0.430 [0.010] | 0.0000 | 1.00 | 1 |
| C=10 | Baseline (reuse=off) | 150 | 3 | 26.76 [1.21] | 0.493 [0.014] | 0.0038 | 0.00 | 10 |
| C=10 | Optimized (reuse=on, pool=100) | 150 | 3 | 28.77 [0.68] | 0.463 [0.000] | 0.0000 | 1.00 | 10 |
| C=25 | Baseline (reuse=off) | 300 | 3 | 50.97 [0.26] | 0.677 [0.078] | 0.0145 | 0.00 | 25 |
| C=25 | Optimized (reuse=on, pool=100) | 300 | 3 | 56.12 [0.94] | 0.557 [0.018] | 0.0000 | 0.95 | 25 |
| C=50 | Baseline (reuse=off) | 500 | 3 | 53.13 [0.57] | 1.266 [0.265] | 0.0183 | 0.00 | 50 |
| C=50 | Optimized (reuse=on, pool=100) | 500 | 3 | 56.49 [0.97] | 1.001 [0.016] | 0.0000 | 0.92 | 50 |

## 3. Experiments (7 trials per arm, A/B interleaved)

### Exp1_Client_Reuse

**Pre-declared Rule Verdict**:
- Throughput: **improved** (Arm A med=49.63 [range 47.47..50.56] vs Arm B med=89.33 [range 84.34..92.36])
- Latency p95: **improved** (Arm A med=1.490523s [range 1.43483..1.566701] vs Arm B med=0.830304s [range 0.812895..1.013029])

| Arm | Trials Throughput (rps) | Median | Min..Max | Trials p95 (s) | Median | Min..Max | Connection Reused Frac | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Arm A (reuse=off) | 50.15, 49.91, 47.6, 50.56, 47.47, 49.37, 49.63 | 49.63 | 47.47..50.56 | 1.452, 1.567, 1.491, 1.443, 1.525, 1.535, 1.435 | 1.491 | 1.435..1.567 | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 (med=0.00) | Baseline |
| Arm B (reuse=on) | 89.33, 92.36, 85.55, 84.78, 90.47, 92.01, 84.34 | 89.33 | 84.34..92.36 | 0.827, 0.841, 0.863, 0.813, 0.816, 0.830, 1.013 | 0.830 | 0.813..1.013 | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 (med=0.00) | **improved** |


### Exp2_Pool_Limits

**Pre-declared Rule Verdict**:
- Throughput: **improved** (Arm A med=27.08 [range 26.55..27.26] vs Arm B med=81.78 [range 74.68..91.24])
- Latency p95: **improved** (Arm A med=1.834789s [range 1.803077..1.861003] vs Arm B med=0.854102s [range 0.802331..0.925912])

| Arm | Trials Throughput (rps) | Median | Min..Max | Trials p95 (s) | Median | Min..Max | Mock Peak Concurrent | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Arm A (pool=10) | 27.26, 26.55, 26.76, 26.82, 27.08, 27.11, 27.26 | 27.08 | 26.55..27.26 | 1.803, 1.847, 1.861, 1.849, 1.835, 1.817, 1.815 | 1.835 | 1.803..1.861 | 10, 10, 10, 10, 10, 10, 10 | Baseline |
| Arm B (pool=100) | 74.68, 85.47, 81.78, 84.91, 79.66, 81.7, 91.24 | 81.78 | 74.68..91.24 | 0.906, 0.844, 0.926, 0.858, 0.842, 0.854, 0.802 | 0.854 | 0.802..0.926 | 50, 50, 50, 50, 50, 50, 50 | **improved** |


### Exp3_Release_Slot_Backoff

**Pre-declared Rule Verdict**:
- Throughput: **inconclusive** (Arm A med=37.85 [range 32.8..40.91] vs Arm B med=47.69 [range 37.86..49.65])
- Latency p95: **inconclusive** (Arm A med=1.901664s [range 1.808114..1.920665] vs Arm B med=1.817439s [range 1.69609..1.986366])

| Arm | Trials Throughput (rps) | Median | Min..Max | Trials p95 (s) | Median | Min..Max | Gateway Queue Wait p95 (s) | Mock Peak Concurrency | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Arm A (release=off) | 36.26, 36.57, 40.03, 40.91, 40.28, 32.8, 37.85 | 37.85 | 32.80..40.91 | 1.911, 1.859, 1.921, 1.882, 1.808, 1.902, 1.907 | 1.902 | 1.808..1.921 | 0.583, 0.571, 0.601, 0.615, 0.574, 0.585, 0.593 (med=0.585) | 22, 22, 21, 21, 22, 21, 22 | Baseline |
| Arm B (release=on) | 48.74, 45.89, 48.37, 49.65, 37.86, 47.18, 47.69 | 47.69 | 37.86..49.65 | 1.878, 1.817, 1.856, 1.696, 1.986, 1.744, 1.810 | 1.817 | 1.696..1.986 | 0.344, 0.389, 0.440, 0.419, 0.483, 0.383, 0.443 (med=0.419) | 25, 25, 25, 25, 25, 25, 25 | **inconclusive** |


## 4. Observations & Diagnostic Analysis

### 1. Connection Reuse Root Cause
- **Finding**: Root cause was real pool churn (`max_keepalive=20 < C=50`), not a metric bug. Excess idle connections were closed, forcing new TCP handshakes.
- **Proof**: 100 sequential requests on 1 shared client achieved 99% reuse (connect_time=0.0s for 99/100 requests).
- **Fix**: Added named 'optimized' profile with `reuse_http_client=True`, `pool_max_connections=100`, `pool_max_keepalive=100 (>= C)`.

### 2. Capacity20 Plateau Analysis
- In queue mode with connection churn eliminated, throughput plateaus at **56.12 rps** at C=25 and **56.49 rps** at C=50.
- This closely matches the theoretical mock saturation limit (capacity 20 / mean service time ~0.35s ≈ 57 rps).

### 3. Baseline Trial-Drift Observation
- Across the 3 consecutive baseline C=50 trials with a 20s cooldown, provider_time p50 was **0.758s -> 0.752s -> 0.749s** (throughput 52.89 -> 53.13 -> 53.46 rps).
- No degradation across consecutive trials was observed under 20s cooldown conditions.

### Capacity20 Rerun Trial Log (20s Cooldown)

| Trial # | Trial Name | Profile | C | N | Throughput (rps) | Latency p95 (s) | Connect p50 (s) | Provider p50 (s) | Conn Reused Frac |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | capacity20_opt_C1_t1 | optimized | 1 | 30 | 3.15 | 0.430 | 0.0000 | 0.3103 | 1.00 |
| 2 | capacity20_opt_C1_t2 | optimized | 1 | 30 | 3.10 | 0.431 | 0.0000 | 0.3024 | 1.00 |
| 3 | capacity20_opt_C1_t3 | optimized | 1 | 30 | 3.12 | 0.421 | 0.0000 | 0.3093 | 1.00 |
| 4 | capacity20_opt_C10_t1 | optimized | 10 | 150 | 28.41 | 0.463 | 0.0000 | 0.3133 | 1.00 |
| 5 | capacity20_opt_C10_t2 | optimized | 10 | 150 | 28.77 | 0.463 | 0.0000 | 0.3166 | 1.00 |
| 6 | capacity20_opt_C10_t3 | optimized | 10 | 150 | 29.09 | 0.463 | 0.0000 | 0.3147 | 1.00 |
| 7 | capacity20_opt_C25_t1 | optimized | 25 | 300 | 55.35 | 0.557 | 0.0000 | 0.3833 | 0.95 |
| 8 | capacity20_opt_C25_t2 | optimized | 25 | 300 | 56.12 | 0.565 | 0.0000 | 0.3795 | 0.95 |
| 9 | capacity20_opt_C25_t3 | optimized | 25 | 300 | 56.29 | 0.547 | 0.0000 | 0.3801 | 0.95 |
| 10 | capacity20_opt_C50_t1 | optimized | 50 | 500 | 56.49 | 0.993 | 0.0000 | 0.7716 | 0.92 |
| 11 | capacity20_opt_C50_t2 | optimized | 50 | 500 | 56.83 | 1.001 | 0.0000 | 0.7701 | 0.92 |
| 12 | capacity20_opt_C50_t3 | optimized | 50 | 500 | 55.86 | 1.009 | 0.0000 | 0.7744 | 0.92 |
| 13 | capacity20_C50_t1 | baseline | 50 | 500 | 52.89 | 1.525 | 0.0183 | 0.7577 | 0.00 |
| 14 | capacity20_C50_t2 | baseline | 50 | 500 | 53.13 | 1.260 | 0.0201 | 0.7516 | 0.00 |
| 15 | capacity20_C50_t3 | baseline | 50 | 500 | 53.46 | 1.266 | 0.0176 | 0.7492 | 0.00 |

