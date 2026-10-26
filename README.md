# BullMQ Distributed Tracing & Job Age Telemetry with SigNoz

In asynchronous queues, a worker pool can appear healthy while jobs sit waiting in Redis for seconds. The standard `bullmq.job.duration` metric only starts when a worker begins executing the job handler, so it completely misses queue waiting time.

This repository demonstrates how to instrument BullMQ with OpenTelemetry and export signals to SigNoz over OTLP/HTTP. It adds a custom histogram, `job_age_at_attempt_ms`, to measure the elapsed time from job enqueue to the start of each attempt. Two services (`order-api` and `order-worker`) propagate distributed W3C trace contexts so producer HTTP requests and worker job executions appear in a single trace waterfall.

---

## What the Benchmark Shows

The included experiment runs 20 jobs of 100 ms simulated work across two conditions: baseline concurrency ($c=5$) and constrained concurrency ($c=1$). Each condition ran 5 trials on a 2-CPU Linux host using unique queues to avoid cache warming bias. The P50 and P95 figures represent the median of five per-trial nearest-rank percentiles:

| Measurement | Concurrency 5 | Concurrency 1 |
| :--- | :---: | :---: |
| Mean enqueue-batch latency | 6.53 ms | 3.93 ms |
| Processor elapsed P50 | 100.6 ms | 100.3 ms |
| Processor elapsed P95 | 101.6 ms | 100.9 ms |
| Enqueue-to-attempt age P50 | 118 ms | 923 ms |
| Enqueue-to-attempt age P95 | 328 ms | 1,844 ms |

### Key Takeaway
Processing duration remains constant at approximately 100 ms under both conditions. However, under constrained concurrency, P95 queue wait age jumps from 328 ms to 1,844 ms because the 20th job must wait for the 19 jobs before it ($19 \times 100\text{ ms} = 1.9\text{ s}$, with the full 20-job batch taking $\approx 2\text{ s}$ to drain). 

A monitoring dashboard that only plots `bullmq.job.duration` would show healthy 100 ms execution times while users experience multi-second delays. Plotting `job_age_at_attempt_ms` reveals the queue backlog immediately.

---

## Quickstart

### Prerequisites
- **Node.js**: `24.21.0` (see [`.nvmrc`](.nvmrc))
- **Docker**: For running Redis
- **SigNoz**: Either [SigNoz Cloud](https://signoz.io/docs/ingestion/signoz-cloud/overview/) or a [local Docker installation](https://signoz.io/docs/install/docker/)

### 1. Setup Environment & Redis

```bash
# Clone and install dependencies
npm ci

# Configure environment
cp .env.example .env

# Start Redis 7.4
docker compose up -d redis
```

Local SigNoz accepts OTLP on `http://localhost:4318` by default. For SigNoz Cloud, update `.env` with your regional ingestion endpoint and ingestion key:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443
OTEL_EXPORTER_OTLP_HEADERS=signoz-ingestion-key=<your-signoz-ingestion-key>
```

### 2. Start the Services

In terminal 1, start the API producer:
```bash
npm run start:api
```

In terminal 2, start the BullMQ worker (concurrency 5 by default):
```bash
npm run start:worker
```

### 3. Dispatch Scenarios

In terminal 3, run the scenario runner:
```bash
npm run scenarios
```

This sends five diagnostic scenarios:
1. `success`: standard 50 ms job completing on attempt 1.
2. `transient-retry`: fails attempt 1, retries with exponential backoff (1,500 ms), and succeeds on attempt 2.
3. `exhausted-failure`: fails all 3 attempts with exponential backoff (1,000 ms), marking the span with status `ERROR` and attaching exception events.
4. `slow-worker`: simulated slow handler (2,000 ms).
5. Backlog burst: batch of 20 concurrent jobs (100 ms work each) demonstrating queue backlog wait.

### 4. Inspect in SigNoz

In SigNoz:
- **Services**: You should see two active services: `order-api` and `order-worker`.
- **Trace Waterfall**: Clicking into a trace shows the root `POST /jobs` HTTP span from `order-api` linked to the worker's `process diagnostics-job-queue` and `execute_scenario.*` spans.
- **Dashboards**: See [docs/architecture.md](docs/architecture.md) for PromQL queries comparing `bullmq.job.duration` with `job_age_at_attempt_ms`.

---

## Running the Benchmark Experiment

To reproduce the benchmark numbers:

```bash
npm run experiment
```

The experiment script pauses workers, atomically enqueues batches of 20 jobs, resumes processing, and measures percentiles using the nearest-rank method across alternating $c=5$ and $c=1$ trials.

Execution takes ~30 seconds and updates three files in `results/`:
- `results/sample-job-observations.csv`: Individual measurements for all 200 jobs.
- `results/sample-trial-summary.csv`: Summary statistics per trial.
- `results/sample-manifest.json`: Node runtime, dependency versions, and workload configuration.

*Note: Running the experiment updates the local files in `results/`. Restore original sample files with `git checkout results/`.*

---

## Sending Custom Jobs via HTTP

You can enqueue jobs directly against `order-api`:

```bash
curl -X POST http://localhost:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "scenario": "success",
    "count": 5,
    "simulatedWorkMs": 100,
    "backoffDelayMs": 1500
  }'
```

The API responds with HTTP `202 Accepted` and the created job IDs:

```json
{
  "status": "enqueued",
  "runId": "run_1792955500000",
  "scenario": "success",
  "count": 5,
  "jobIds": ["1", "2", "3", "4", "5"]
}
```

### Request Parameters

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `scenario` | string | `'success'` | One of `'success'`, `'transient-retry'`, `'exhausted-failure'`, or `'slow-worker'`. |
| `count` | integer (1–100) | `1` | Number of jobs to enqueue in bulk. |
| `simulatedWorkMs` | integer (1–10,000) | `100` (`2000` for slow-worker) | Simulated task processing time. |
| `backoffDelayMs` | integer (1–60,000) | `1500` | Base delay for exponential retry backoff. |
| `runId` | string (max 128 chars) | `run_<timestamp>` | Grouping identifier attached to spans. |

Payloads larger than 16 KB return `413 Payload Too Large`.

---

## Configuration

All configuration is managed via environment variables (or `.env`):

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `3000` | Express API listener port. |
| `HOST` | `0.0.0.0` | API bind address. |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL. |
| `WORKER_CONCURRENCY` | `5` | BullMQ worker concurrency (1–50). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Base URL for OTLP/HTTP export. |
| `OTEL_EXPORTER_OTLP_HEADERS` | *empty* | Comma-separated headers (e.g. `signoz-ingestion-key=...`). |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `5000` | Exporter timeout in ms before dropping telemetry on shutdown. |

---

## Testing & Verification

```bash
# Typecheck source, scripts, and tests
npm run typecheck

# Run test suite (requires Redis on localhost:6379)
npm test

# Compile TypeScript
npm run build
```

The test suite covers:
- Express route validation, error handling, and 413 oversized body protection.
- Wall-clock age calculation, retry accumulation, and negative clock skew detection.
- Distributed W3C trace propagation from producer to consumer.
- OTLP HTTP trace and metric export pipeline verification.

---

## Documentation

- [**Architecture & Telemetry Design**](docs/architecture.md): Trace context flow, metric cardinality controls, and ready-to-use SigNoz PromQL queries and Trace Explorer filters.
- [**Troubleshooting**](docs/TROUBLESHOOTING.md): Redis connectivity, SigNoz Cloud setup, and collector timeout diagnostics.

---

## License

[ISC](LICENSE)
