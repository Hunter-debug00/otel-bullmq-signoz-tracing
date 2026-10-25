# Troubleshooting

Common operational issues, environment setup questions, and diagnostic steps.

---

## 1. Redis Connection Failures (`ECONNREFUSED 6379`)

### Cause
Redis is not running or the application cannot reach port 6379 on localhost.

### Resolution
1. Verify the Redis container is running:
   ```bash
   docker compose ps
   ```
2. If Redis is down, start it:
   ```bash
   docker compose up -d redis
   ```
3. If running inside **GitHub Codespaces**, ensure port `6379` is forwarded to `localhost` in the Codespace **Ports** tab.
4. If using an external Redis instance, set `REDIS_URL` in `.env`:
   ```bash
   REDIS_URL=redis://your-redis-host:6379
   ```

---

## 2. No Data in SigNoz

### Step 1: Check Collector Reachability
Send an HTTP request to the configured OTLP endpoint:

```bash
# For local collector
curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:4318/v1/traces
```

- Any HTTP response status (such as `405 Method Not Allowed` or `200 OK`) confirms a collector is listening.
- `Failed to connect` indicates no collector is active on that port.

### Step 2: Verify Instrumentation Startup
Both services must load `src/bootstrap.ts` prior to application code. When starting via `npm run start:api` or `npm run start:worker`, verify the console logs:

```text
[OTel] Traces & Metrics active for service: order-api
```

### Step 3: SigNoz Cloud Configuration
For SigNoz Cloud, verify the following variables in `.env`:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443
OTEL_EXPORTER_OTLP_HEADERS=signoz-ingestion-key=<your-signoz-ingestion-key>
```

- Replace `<region>` with your SigNoz cloud region (`us`, `eu`, or `in`).
- Ensure no surrounding quotes are present in the header value.

### Step 4: Metric Export Interval
Metrics are batched and exported periodically (default: every 5 seconds). Wait at least 10–15 seconds after dispatching jobs before checking SigNoz dashboards.

---

## 3. Collector Is Down or Unreachable

When the telemetry collector is offline:
- Both `order-api` and `order-worker` continue processing requests and queue jobs normally. Telemetry spans and metrics are dropped.
- On process shutdown, the OpenTelemetry SDK attempts to flush pending telemetry. If the collector does not respond within the exporter timeout (default: 5 seconds, configurable via `OTEL_EXPORTER_OTLP_TIMEOUT`), the shutdown logs a warning and exits cleanly without hanging indefinitely:
  ```text
  [OTel] Collector unreachable at http://localhost:4318. Telemetry shut down.
  ```

---

## 4. Resetting Benchmark Results

Running `npm run experiment` executes a live benchmark and updates files in `results/`:
- `results/sample-job-observations.csv`
- `results/sample-trial-summary.csv`
- `results/sample-manifest.json`

To discard local run output and restore the repository's checked-in benchmark baseline:

```bash
git checkout results/
```

---

## 5. Adjusting Worker Concurrency

The default worker runs with concurrency 5. You can customize concurrency using `.env`:

```bash
# In .env
WORKER_CONCURRENCY=10
```

Alternatively, pass it directly in the command line:

```bash
WORKER_CONCURRENCY=10 npm run start:worker
```

To run a single-worker constrained queue to observe head-of-line backlog, use the built-in script:

```bash
npm run start:worker:constrained
```

---

## 6. Docker-in-Docker (DinD) & Bridge Networking

If developing inside nested Docker environments where bridge networks fail to route between container and host:
1. Verify network port mappings:
   ```bash
   docker compose ps
   ```
2. If necessary on Linux hosts, enable host networking via a Compose override:
   ```yaml
   # docker-compose.override.yml
   services:
     redis:
       network_mode: host
       ports: !reset []
   ```
   *(Note: Host networking is supported on Linux only.)*
