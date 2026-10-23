# BullMQ OpenTelemetry Tracing & Diagnostics

Asynchronous job processing pipeline instrumented with OpenTelemetry traces and metrics for SigNoz. Differentiates in-worker execution duration from queue latency and retry backoff delays using `job_age_at_attempt_ms`.

## Prerequisites

- Node.js >= 20
- Docker & Docker Compose
- SigNoz collector (listening on OTLP HTTP port 4318) or compatible OTLP endpoint

## Setup

```bash
npm install
cp .env.example .env
docker compose up -d
```

## Running Services

Start the Express API producer:
```bash
npm run start:api
```

Start the BullMQ worker:
```bash
npm run start:worker
```

Run worker with constrained concurrency (`concurrency=1`):
```bash
npm run start:worker:constrained
```

## Testing & Benchmarks

Run test suite:
```bash
npm test
```

Trigger diagnostic test scenarios against a running API:
```bash
npm run scenarios
```

Execute the controlled concurrency experiment (outputs metrics to `results/experiment-results.csv`):
```bash
npm run experiment
```

### Benchmark Summary (20 Jobs × 100ms Work, 5 Trials Each)

| Queue / Condition | Concurrency | Batch Enqueue (Mean) | Processing Duration P50 / P95 | Job Age at Attempt P50 / P95 |
| :--- | :--- | :--- | :--- | :--- |
| `diagnostics-baseline-c5` | `5` | `21.13 ms` | `100.86 ms` / `104.78 ms` | **`140.0 ms` / `350.4 ms`** |
| `diagnostics-constrained-c1` | `1` | `17.03 ms` | `100.43 ms` / `101.37 ms` | **`946.2 ms` / `1,875.8 ms`** |