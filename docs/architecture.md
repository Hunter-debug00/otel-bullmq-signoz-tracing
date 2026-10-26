# Architecture & Telemetry Design

This document details the system design, signal definitions, distributed tracing flow, and SigNoz query recipes for monitoring BullMQ with OpenTelemetry.

---

## System Architecture

```text
[HTTP Client / Benchmark]
          │
          ▼ POST /jobs
   [Express API Server] ──(traceparent in job options)──► [Redis 7.4 Queue]
          │                                                       │
          │                                                       ▼
          │                                                [Worker Service]
          ▼ OTLP/HTTP                                             │ OTLP/HTTP
   [SigNoz Collector (:4318)] ◄───────────────────────────────────┘
```

1. **Producer (`order-api`)**: An Express service (`src/app.ts`, `src/server.ts`) that validates incoming requests, injects the active W3C trace context (`traceparent`) into BullMQ job metadata using `bullmq-otel`, and enqueues jobs in bulk with `getDiagnosticsQueue().addBulk()`.
2. **Message Broker (Redis 7.4)**: Manages job queues, hash states, and retry zsets.
3. **Consumer (`order-worker`)**: A BullMQ worker (`src/worker.ts`, `src/jobs/processor.ts`) that dequeues jobs, extracts the producer trace context, records the wall-clock queue age metric (`job_age_at_attempt_ms`), adds diagnostic attributes to active spans, and executes the scenario task.
4. **Telemetry Collector (SigNoz)**: Ingests OTLP traces (`/v1/traces`) and metrics (`/v1/metrics`) over HTTP. The OpenTelemetry SDK is initialized via `src/bootstrap.ts` before application code runs.

---

## Signal Definitions

Standard queue metrics like `bullmq.job.duration` only measure how long a worker spends executing a job handler. They do not capture how long jobs spend waiting in Redis before processing starts.

```text
Timeline of a Job Attempt:

├── Enqueued in Redis (`job.timestamp`)
│
│   ◄─────── [ Queue Wait / Scheduling Delay / Retry Backoff ] ───────►
│
├── Attempt Starts (`job.processedOn`) ──── [Record `job_age_at_attempt_ms`]
│   │
│   │   ◄── In-Worker Execution ──►
│   │
└── Attempt Completes ───────────────────── [Record `bullmq.job.duration`]
```

| Signal | Source | What It Measures | What It Does Not Measure |
| :--- | :--- | :--- | :--- |
| `job_age_at_attempt_ms` | Custom OTel Histogram (`src/telemetry/job-age.ts`) | Elapsed wall-clock time from enqueue (`job.timestamp`) to attempt start (`job.processedOn`). | Handler execution duration; does not isolate individual backoff intervals on retries. |
| `bullmq.job.duration` | Built-in OTel Histogram (`bullmq-otel`) | Worker processor function runtime from start to resolution/rejection. | Queue wait time, scheduling delays, or retry intervals. |
| `processor_elapsed_ms` | Local timer (`performance.now()`) | Process-local handler duration, written to benchmark CSV results. | Not exported to SigNoz; unaggregated timer for benchmark validation. |

### First Attempts vs. Retries

The histogram attaches a `bullmq.job.attempt_type` label with two possible values:
- `first_attempt`: Measures initial queue wait time for immediate jobs. For delayed jobs, this includes the intentional scheduling delay.
- `retry_attempt`: Measures cumulative wall-clock time from original enqueue across all previous attempts and backoff periods.

### Clock Skew

BullMQ records `job.timestamp` on the producer machine and `job.processedOn` on the worker machine. If host clocks drift:
- Negative skew (`processedOn < timestamp`) is dropped rather than clamped to zero, avoiding artificial zero-millisecond spikes in metric distributions.
- A rate-limited warning is logged once per minute when skew or missing timestamps occur.

### Cardinality Control

To keep metrics efficient in SigNoz / ClickHouse:
- `bullmq.job.name` is restricted to known scenario names. Unrecognized names fall back to `'unknown'`.
- High-cardinality identifiers (`job.id`, `run_id`) are attached exclusively to trace spans, never to metric dimensions.
- `bullmq.queue.name` is included as a label. If your system generates dynamic per-tenant queue names, remove this label to prevent label explosion.

---

## Distributed Tracing Flow

Each request produces a connected distributed trace across both services:

```text
POST /jobs                          (order-api, Express HTTP span)
└─ addBulk diagnostics-job-queue    (order-api, bullmq-otel producer span)
   └─ process diagnostics-job-queue (order-worker, bullmq-otel consumer span)
      └─ execute_scenario.<name>    (order-worker, child scenario span)
```

- **Trace Context Propagation**: `bullmq-otel` injects the W3C `traceparent` into job metadata. The worker extracts this context, continuing the trace across process boundaries.
- **Root Span Duration**: The producer HTTP span ends as soon as the batch is committed to Redis and the HTTP 202 response is returned. It finishes quickly (typically a few milliseconds after initial connection warmup), while the worker continues asynchronously.
- **Diagnostic Span Attributes**: Both the consumer span and the child scenario span include:
  - `bullmq.job.name`: scenario identifier
  - `bullmq.job.attempt_number`: attempt count (1, 2, 3...)
  - `bullmq.job.age_at_attempt_ms`: recorded queue age (omitted if dropped due to clock skew)
  - `app.run_id`: batch identifier

---

## Visualizing in SigNoz

SigNoz stores OpenTelemetry metrics using PromQL with quotes and curly braces for dotted metric and label names.

### Dashboard Queries (PromQL)

#### 1. P95 & P50 Enqueue-to-Attempt Age per Queue
Grouping by `(le, "bullmq.queue.name")` computes separate quantiles for each queue rather than flattening all queues into one:

```promql
# P95 Enqueue-to-attempt age per queue (approximates queue wait for initial attempts)
histogram_quantile(
  0.95,
  sum by (le, "bullmq.queue.name") (
    rate({"job_age_at_attempt_ms.bucket"}[5m])
  )
)

# P50 Enqueue-to-attempt age per queue
histogram_quantile(
  0.50,
  sum by (le, "bullmq.queue.name") (
    rate({"job_age_at_attempt_ms.bucket"}[5m])
  )
)
```

*(Note: To compute a fleet-wide aggregate quantile across all queues combined, group solely `by (le)`).*

#### 2. Handler Execution Duration vs. Enqueue-to-Attempt Age
Put these two queries on the same dashboard panel to correlate queue backlog against task processing:

```promql
# P95 Handler execution duration per queue (bullmq-otel)
histogram_quantile(
  0.95,
  sum by (le, "bullmq.queue.name") (
    rate({"bullmq.job.duration.bucket"}[5m])
  )
)

# P95 Enqueue-to-attempt age per queue
histogram_quantile(
  0.95,
  sum by (le, "bullmq.queue.name") (
    rate({"job_age_at_attempt_ms.bucket"}[5m])
  )
)
```

- **If queue age rises while execution duration stays flat**: The worker pool has reached concurrency saturation. Jobs are queuing up waiting for available worker slots. Scale out worker concurrency or instances.
- **If execution duration rises**: Downstream dependencies or job handlers are slow.

#### 3. Attempt Throughput by Scenario
Because `job_age_at_attempt_ms` is recorded at the start of each attempt, this query measures total attempt throughput per queue:

```promql
sum by ("bullmq.job.name", "bullmq.queue.name") (rate({"job_age_at_attempt_ms.count"}[5m]))
```

*Note: To measure only initial job executions (excluding retries), filter by `{"bullmq.job.attempt_type" = "first_attempt"}`.*

#### 4. Attempt Rate by Attempt Type
Track attempt rates separated into initial attempts and retries:

```promql
sum by ("bullmq.job.attempt_type", "bullmq.queue.name") (rate({"job_age_at_attempt_ms.count"}[5m]))
```

---

### Trace Explorer Filters

In the SigNoz Trace Explorer, filter spans to investigate anomalies:

1. **Find high-backlog jobs** (queued for > 500 ms):
   ```text
   service.name = 'order-worker' AND bullmq.job.age_at_attempt_ms > 500
   ```
2. **Find retried attempts**:
   ```text
   service.name = 'order-worker' AND bullmq.job.attempt_number > 1
   ```
3. **Isolate failed attempts**:
   ```text
   service.name = 'order-worker' AND has_error = true
   ```

---

## Architectural Limits & Operational Notes

- **Observer Bias**: `job_age_at_attempt_ms` is recorded when a worker picks up a job. If all workers crash or are paused, jobs sit in Redis unobserved until a worker recovers. To alert on stalled queues when workers are offline, monitor BullMQ queue counts (`bullmq.queue.jobs`).
- **NTP Synchronization**: Host clock accuracy determines the precision of wall-clock age calculations. In multi-host production environments, ensure all instances run synchronized NTP/Chrony daemons.
- **Payload Limits**: The Express API enforces a 16 KB body limit (`express.json({ limit: '16kb' })`) and returns `413 Payload Too Large` if exceeded.
