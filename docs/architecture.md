# Telemetry & Queue Architecture

This document details the queue processing lifecycle, OpenTelemetry instrumentation signals, measurement semantics, and distributed trace propagation across the producer and worker pipeline.

---

## 1. System Architecture

```text
[HTTP Client / Benchmark]
          │
          ▼ POST /jobs
   [Express API Server] ──(bullmq-otel traceparent)──► [Redis 7 Queue]
          │                                                  │
          │                                                  ▼
          │                                           [Worker Service]
          ▼ OTLP HTTP                                        │ OTLP HTTP
   [SigNoz Collector (:4318)] ◄──────────────────────────────┘
```

1. **Producer (`src/app.ts` & `src/server.ts`)**: `src/app.ts` accepts batch job enqueue requests, validates the schema and parameters via `validateEnqueueRequest()`, injects the active W3C trace context into BullMQ job metadata using `bullmq-otel`, and atomically enqueues jobs via `getDiagnosticsQueue().addBulk()`. `src/server.ts` manages HTTP port listening and graceful shutdown.
2. **Message Broker (Redis 7)**: Persists queues, hash sets, and delayed/retry zsets with blocking command support (`maxRetriesPerRequest: null`).
3. **Consumer (`src/worker.ts` & `src/jobs/processor.ts`)**: Worker processes poll Redis, extract parent trace context from incoming jobs, record the custom enqueue-to-attempt age metric (`job_age_at_attempt_ms`), annotate active parent and child spans, and execute the scenario workload.
4. **Telemetry Collector (SigNoz)**: Ingests OTLP HTTP traces (`/v1/traces`) and metrics (`/v1/metrics`) over port `4318` or via SigNoz Cloud with `signoz-ingestion-key` headers. Telemetry is constructed by `src/instrumentation.ts` and preloaded for services via `src/bootstrap.ts`.

---

## 2. Measurement Definitions & Metric Semantics

Understanding the differences between durations is critical for accurate diagnosis:

```text
Timeline of a Job Attempt:

├── Job Enqueued (`job.timestamp`)
│
│   ◄─────── [ Queue Wait Backlog / Delay / Retries ] ───────►
│
├── Processing Starts (`job.processedOn`) ── [Record `job_age_at_attempt_ms`]
│   │
│   │   ◄── In-Worker Workload Execution ──►
│   │
└── Processing Completes ─────────────────── [Record `processor_elapsed_ms` & `bullmq.job.duration`]
```

| Metric / Value | Source & Scope | What It Measures | What It Does NOT Measure |
| :--- | :--- | :--- | :--- |
| **`job_age_at_attempt_ms`** | Custom OTel Histogram (`src/telemetry/job-age.ts`) | Elapsed wall-clock time from initial job enqueue (`job.timestamp`) to processing start (`job.processedOn`). | Does not measure worker processing runtime; does not isolate individual backoff intervals on retries. |
| **`bullmq.job.duration`** | Built-in BullMQ OTel Histogram (`bullmq-otel`) | Internal processor function runtime from start to resolution/rejection. | Does not capture queue waiting backlog, scheduling delays, or retry intervals. |
| **`processor_elapsed_ms`** | Local High-Resolution Benchmark Measurement (`performance.now()`) | Precise timer around `processDiagnosticJob` within the local process. | Is not an OpenTelemetry histogram aggregation; does not reflect OTLP bucket approximations. |

### Critical Semantic Notes:

1. **First Attempts vs Retries (`bullmq.job.attempt_type`)**:
   - On `first_attempt`, `job_age_at_attempt_ms` approximates **initial queue waiting time** only for immediately eligible jobs. For delayed/scheduled jobs, it includes the intentional delay interval.
   - On `retry_attempt`, `job_age_at_attempt_ms` represents **cumulative elapsed wall-clock age** across all previous execution attempts, failures, and backoff wait intervals. It does **not** isolate only the most recent backoff sleep.

2. **Wall-Clock & Clock Skew Assumptions**:
   - `job_age_at_attempt_ms` calculates $\text{job.processedOn} - \text{job.timestamp}$.
   - Because `timestamp` is recorded by the producer machine and `processedOn` is recorded by the worker machine, system clock drift (NTP skew) across separate physical hosts can affect readings.
   - Negative skew (`processedOn < timestamp`) is explicitly intercepted: the histogram observation is skipped rather than clamped to 0, and a rate-limited warning is logged to maintain metric data integrity.

---

## 3. Distributed Tracing & Span Hierarchy

### Span Context Propagation
When a client sends a request to the Express API:
1. Express auto-instrumentation creates the root HTTP server span (`POST /jobs`).
2. BullMQ-OTel attaches the active W3C trace context (`traceparent`) into the BullMQ job's `opts.telemetry` metadata.
3. When the worker picks up the job, BullMQ instrumentation extracts the traceparent and creates the consumer parent span.
4. The worker attaches diagnostic attributes (`bullmq.job.name`, `bullmq.job.attempt_number`, `bullmq.job.age_at_attempt_ms`, `app.run_id`) to **both** the active parent consumer span and the child scenario span (`execute_scenario.${scenario}`).

### Root Span vs Asynchronous Waterfall Duration
- The root HTTP producer span completes and ends as soon as `getDiagnosticsQueue().addBulk()` finishes and the HTTP 202 response is sent (< 30 ms).
- The worker executes asynchronously in the background. While the overall trace waterfall in SigNoz encompasses the subsequent worker job spans, the root HTTP span itself remains short, accurately reflecting the non-blocking nature of the producer.

---

## 4. Visualizing in SigNoz (Query Recipes & Trace Explorer)

### SigNoz Dashboard Queries (PromQL Mode)

You can visualize both the custom `job_age_at_attempt_ms` metric and the built-in `bullmq.job.duration` metric in custom SigNoz dashboards:

#### 1. P95 & P50 Queue Backlog Delay (Enqueue-to-Attempt Age)
```promql
# P95 Wall-Clock Queue Delay across all queues
histogram_quantile(0.95, sum(rate(job_age_at_attempt_ms_bucket[5m])) by (le))

# P50 Wall-Clock Queue Delay across all queues
histogram_quantile(0.50, sum(rate(job_age_at_attempt_ms_bucket[5m])) by (le))
```

#### 2. Worker Execution Duration vs Queue Delay
Compare in-worker execution duration against queue delay to immediately pinpoint whether latency stems from worker concurrency starvation or slow task execution:
```promql
# P95 Workload Execution Duration (built-in bullmq-otel histogram)
histogram_quantile(0.95, sum(rate(bullmq_job_duration_bucket[5m])) by (le))
```

#### 3. Job Processing Rate by Scenario
```promql
# Rate of jobs processed per second by scenario
sum(rate(job_age_at_attempt_ms_count[1m])) by (bullmq_job_name)
```

#### 4. Retry vs First-Attempt Distribution
```promql
# Track retry frequency compared to initial attempts
sum(rate(job_age_at_attempt_ms_count[5m])) by (bullmq_job_attempt_type)
```

### SigNoz Trace Explorer Filters

To inspect traces for specific queue behaviors in the SigNoz Trace Explorer:

1. **Find High-Backlog Jobs**:
   - Filter: `serviceName = order-worker` AND `bullmq.job.age_at_attempt_ms > 500`
   - Highlights traces where jobs waited in the queue for more than 500ms before a worker picked them up.
2. **Find Retried Jobs**:
   - Filter: `serviceName = order-worker` AND `bullmq.job.attempt_number > 1`
   - Inspect the retry trace waterfalls and backoff delays.
3. **Isolate Exhausted Failures**:
   - Filter: `serviceName = order-worker` AND `responseStatusCode = ERROR`
   - Inspect the `execute_scenario.exhausted-failure` span and its recorded `exception` event attributes.
