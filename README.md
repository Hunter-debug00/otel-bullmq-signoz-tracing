# BullMQ + OpenTelemetry Distributed Tracing & Diagnostics

A reproducible reference implementation demonstrating OpenTelemetry distributed tracing and custom queue-latency instrumentation with **BullMQ**, **Redis 7**, and **SigNoz**.

This repository highlights a critical observability distinction in asynchronous queues: the difference between in-worker processing execution (`bullmq.job.duration` / `processor_elapsed_ms`) and total queue backlog latency (`job_age_at_attempt_ms`).

---

## Architecture Overview

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

1. **Producer (`src/server.ts`)**: Validates enqueue payloads, injects W3C trace context (`traceparent`) into job metadata via `bullmq-otel`, and enqueues batches atomically using `diagnosticsQueue.addBulk()`.
2. **Broker (Redis 7)**: Manages FIFO queue states, job payload persistence, and retry delays.
3. **Consumer (`src/worker.ts` & `src/jobs/processor.ts`)**: Polls jobs, extracts trace context, records the custom `job_age_at_attempt_ms` metric, decorates both parent and child spans, and runs the scenario work.
4. **Telemetry (`src/instrumentation.ts`)**: Exports OTLP traces (`/v1/traces`) and metrics (`/v1/metrics`) to SigNoz (local or Cloud).

---

## Core Telemetry Semantics

| Metric / Timer | Type & Scope | What It Measures | What It Does NOT Measure |
| :--- | :--- | :--- | :--- |
| **`job_age_at_attempt_ms`** | Custom OTel Histogram (`src/telemetry/job-age.ts`) | Elapsed wall-clock time from initial enqueue (`job.timestamp`) to processing start (`job.processedOn`). | Does not measure worker workload execution duration; does not isolate individual backoff intervals on retries. |
| **`bullmq.job.duration`** | Built-in OTel Histogram (`bullmq-otel`) | Worker processing runtime from processor function invocation to resolution/rejection. | Does not capture queue waiting backlog, scheduling delays, or retry sleep intervals. |
| **`processor_elapsed_ms`** | Local High-Resolution Timer (`performance.now()`) | Exact in-process benchmark execution timer around `processDiagnosticJob`. | Is not an exported OTel histogram; does not reflect OTLP bucket quantile estimations. |

### Semantic Precision Notes
- **First Attempts vs Retries (`bullmq.job.attempt_type`)**:
  - `first_attempt`: Approximates **initial queue waiting time** only for immediately eligible jobs. For delayed jobs, it includes the intentional scheduling delay.
  - `retry_attempt`: Represents **cumulative elapsed wall-clock age** across all previous execution attempts, failures, and backoff wait intervals. It does not measure only the most recent backoff sleep.
- **Wall-Clock Timestamps & Clock Skew**:
  - `job_age_at_attempt_ms` is calculated as $\text{job.processedOn} - \text{job.timestamp}$.
  - When `job.processedOn` is missing/invalid or produces negative clock skew ($\text{job.processedOn} < \text{job.timestamp}$ due to node NTP drift), the histogram observation is explicitly **skipped** rather than fabricated or clamped to `0`, and a rate-limited diagnostic warning is logged to maintain metric data integrity.

> [!NOTE]
> **Operational Note on Label Cardinality & Security**: `job_age_at_attempt_ms` bounds `bullmq.job.name` via a four-value allowlist (`ALLOWED_SCENARIOS`) and excludes high-cardinality identifiers (`job.id`, `run_id`). It retains `bullmq.queue.name` under the architectural assumption that queue names are static and operationally bounded; deployments that dynamically create per-tenant queues should omit `bullmq.queue.name` from metric attributes. Finally, the Express API in this repository is an unauthenticated local diagnostic harness and is not hardened for direct public internet exposure.

---

## Clean-Clone Quickstart

### 1. Prerequisites
- **Node.js**: `v24.21.0` (see [`.nvmrc`](.nvmrc))
- **Docker & Docker Compose**

### 2. Installation & Configuration

```bash
# 1. Install pinned dependencies
npm ci

# 2. Configure environment
cp .env.example .env
```

Review `.env`:
- **Local SigNoz**: Leave `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318`.
- **SigNoz Cloud**: Set your cloud regional ingestion endpoint and ingestion key:
  ```bash
  OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443
  OTEL_EXPORTER_OTLP_HEADERS=signoz-ingestion-key=<your-signoz-ingestion-key>
  ```

### 3. Start Redis

```bash
docker compose up -d redis
```

Verify Redis connectivity:
```bash
docker compose ps
```

### 4. Start the Application

In Terminal 1, start the API producer:
```bash
npm run start:api
```

In Terminal 2, start the worker:
```bash
npm run start:worker
```
*(To test constrained single-concurrency queueing, run `npm run start:worker:constrained`)*

### 5. Trigger Scenarios

In Terminal 3, dispatch diagnostic scenarios (`success`, `transient-retry`, `exhausted-failure`, `slow-worker`, and backlog burst):
```bash
npm run scenarios
```

### 6. Run Tests & Validation

```bash
# Typecheck source, scripts, and tests
npm run typecheck

# Run unit and integration tests (exits cleanly without force-exit)
npm test

# Build production bundle
npm run build
```

---

## Controlled Synchronized Burst Experiment

Run the isolated benchmark comparing baseline concurrency ($c=5$) against constrained concurrency ($c=1$):

```bash
npm run experiment
```

### Benchmark Methodology
- **Controlled Burst**: Each trial pauses the worker, atomically enqueues a batch of 20 jobs ($100\text{ ms}$ simulated async work each), attaches listeners, and resumes the worker.
- **Alternating Trial Order**: Trials alternate condition order ($c=5$ then $c=1$, then $c=1$ then $c=5$) across unique trial queues (`diag-c5-trial-01`, `diag-c1-trial-01`, etc.) to mitigate host warming bias.
- **Percentiles**: Computed via the deterministic `"nearest-rank"` sample convention.

### Benchmark Results (5 Trials Baseline $c=5$ vs 5 Trials Constrained $c=1$)

| Condition | Concurrency | Enqueue Batch Latency (Mean) | In-Worker Elapsed Duration P50 / P95 (Median) | Enqueue-to-Attempt Age P50 / P95 (Median) |
| :--- | :---: | :---: | :---: | :---: |
| **Baseline (`diag-c5-trial-*`)** | **5** | **`6.92 ms`** | **`100.7 ms` / `102.4 ms`** | **`116 ms` / `324 ms`** |
| **Constrained (`diag-c1-trial-*`)** | **1** | **`8.38 ms`** | **`100.2 ms` / `100.9 ms`** | **`938 ms` / `1,859 ms`** |

> [!NOTE]
> While worker execution duration (`processor_elapsed_ms`) remained constant at $\approx 100\text{ ms}$ under both conditions, `job_age_at_attempt_ms` P95 jumped from **`324 ms`** under concurrency 5 to **`1,859 ms`** under constrained concurrency 1 due to head-of-line queue backlog.

Generated artifacts:
- [`results/sample-job-observations.csv`](results/sample-job-observations.csv): 200 per-job observations (`condition`, `queue_name`, `concurrency`, `trial`, `job_id`, `attempt_number`, `enqueue_to_attempt_age_ms`, `processor_elapsed_ms`).
- [`results/sample-trial-summary.csv`](results/sample-trial-summary.csv): 10 trial summaries with P50 and P95 statistics.
- [`results/sample-manifest.json`](results/sample-manifest.json): Machine, OS, dependency versions, and workload metadata.

---

## Additional Documentation

- [**Architecture & Telemetry Signals**](docs/architecture.md): Deep-dive into trace context propagation and metric aggregation advice.
- [**Troubleshooting & Codespaces/DinD**](docs/TROUBLESHOOTING.md): GitHub Codespaces port-forwarding, Docker-in-Docker networking, and collector connectivity.

---

## License

[ISC](LICENSE)
