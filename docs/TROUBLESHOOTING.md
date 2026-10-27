# Troubleshooting

This guide organizes common failure modes using a structured decision path:
**Symptom → Check → Expected Result → Likely Cause → Next Action**.

---

## 1. Redis Connection Failure (`ECONNREFUSED 6379`)

- **Symptom**: `Error: connect ECONNREFUSED 127.0.0.1:6379` logged on API or worker startup.
- **Check**: Inspect container status:
  ```bash
  docker compose ps
  ```
- **Expected Result**: `bullmq-redis` is listed with status `Up` and port mapping `0.0.0.0:6379->6379/tcp`.
- **Likely Cause**: The Redis container is stopped, or port 6379 is not forwarded from the container host.
- **Next Action**:
  1. Start Redis: `docker compose up -d redis`.
  2. If running inside GitHub Codespaces, ensure port `6379` is forwarded to `localhost` in the **Ports** tab.
  3. If using an external Redis instance, set `REDIS_URL` in `.env` (e.g. `REDIS_URL=redis://your-host:6379`).

---

## 2. No Data Appears in SigNoz (Decision Path)

Follow this diagnostic sequence to isolate where the telemetry pipeline is broken.

### Branch A: Collector is Unreachable
- **Symptom**: No services or spans appear in SigNoz; process shutdown logs `[OTel] Collector unreachable at...`
- **Check**: Send a probe request to the configured OTLP endpoint:
  ```bash
  # Local collector
  curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:4318/v1/traces

  # SigNoz Cloud
  curl -sS -o /dev/null -w "%{http_code}\n" https://ingest.<region>.signoz.cloud:443/v1/traces
  ```
- **Expected Result**: HTTP `405 Method Not Allowed` or `200 OK` (proves an OTLP server is listening).
- **Likely Cause**: Local SigNoz is not running, or network firewall blocks port 4318.
- **Next Action**:
  1. For local development, install and start SigNoz ([SigNoz Docker Guide](https://signoz.io/docs/install/docker/)).
  2. For cloud evaluation, configure SigNoz Cloud in `.env` (see Branch B).

### Branch B: Collector Connects, but Ingestion Fails
- **Symptom**: The collector endpoint responds with HTTP 405/200 via `curl`, but no services appear in SigNoz Cloud.
- **Check**: Inspect the ingestion variables in `.env`:
  ```bash
  grep OTEL_EXPORTER_OTLP .env
  ```
- **Expected Result**:
  - `OTEL_EXPORTER_OTLP_ENDPOINT` contains the matching region (`us`, `eu`, `in`).
  - `OTEL_EXPORTER_OTLP_HEADERS` has format `signoz-ingestion-key=<key>` without surrounding quotes.
- **Likely Cause**: Invalid ingestion key, incorrect cloud region subdomain, or accidental quotes in header definition.
- **Next Action**: Copy the endpoint and ingestion key directly from SigNoz Cloud under **Settings > Ingestion** and update `.env`.

### Branch C: Telemetry Instrumentation Not Initialized
- **Symptom**: Services run and process jobs, but no telemetry is sent and no shutdown log appears.
- **Check**: Observe the first line printed when starting the service.
- **Expected Result**: `[OTel] Traces & Metrics active for service: <service-name>`.
- **Likely Cause**: The process was started with raw `node src/server.ts` or `tsx src/server.ts` without loading `src/bootstrap.ts`.
- **Next Action**: Start services using the designated npm scripts (`npm run start:api`, `npm run start:worker`), which include `--import ./src/bootstrap.ts`.

### Branch D: Periodic Metric Export Delay
- **Symptom**: Trace spans appear in the Trace Explorer, but metric dashboards show "No Data".
- **Check**: Wait 15 seconds after sending job traffic.
- **Expected Result**: The OpenTelemetry metric reader flushes metrics every 5,000 ms (`exportIntervalMillis: 5000`).
- **Likely Cause**: Normal periodic export batching delay.
- **Next Action**: Dispatch traffic using `npm run scenarios` and wait at least two export cycles (10-15 seconds) before querying dashboard panels.

---

## 3. Slow Process Shutdown Without a Collector

- **Symptom**: `order-api` or `order-worker` takes ~5 seconds to exit after receiving `SIGTERM` or `SIGINT` when the collector is offline.
- **Check**: Verify if port 4318 is listening.
- **Expected Result**: Process logs `[OTel] Collector unreachable at http://localhost:4318. Telemetry shut down.` and exits cleanly.
- **Likely Cause**: The OpenTelemetry SDK performs a graceful flush on shutdown. Each exporter request waits for `timeoutMillis` (default: 5,000 ms) before timing out.
- **Next Action**: This is expected graceful-shutdown behavior when no collector is running. To shorten the shutdown delay in local offline environments, configure a smaller timeout in `.env`:
  ```bash
  OTEL_EXPORTER_OTLP_TIMEOUT=1000
  ```

---

## 4. Worker Concurrency Override

- **Symptom**: Setting `WORKER_CONCURRENCY=10` in `.env` does not alter concurrency when running `npm run start:worker:constrained`.
- **Check**: Inspect `package.json` scripts.
- **Expected Result**:
  - `npm run start:worker` reads `WORKER_CONCURRENCY` from `.env` (defaulting to 5 if unset).
  - `npm run start:worker:constrained` intentionally hardcodes `WORKER_CONCURRENCY=1`.
- **Likely Cause**: `start:worker:constrained` is an explicit test script designed to force single-worker queue bottlenecking.
- **Next Action**: Use `npm run start:worker` for general testing where `.env` controls concurrency. Use `npm run start:worker:constrained` only when reproducing constrained head-of-line blocking.

---

## 5. Benchmark Overwrote Checked-In Results

- **Symptom**: `git status` displays modified files under `results/` after running `npm run experiment`.
- **Check**: Inspect git status:
  ```bash
  git status results/
  ```
- **Expected Result**: The benchmark updates `sample-job-observations.csv`, `sample-trial-summary.csv`, and `sample-manifest.json` with fresh machine timings.
- **Likely Cause**: `scripts/run-experiment.ts` records real measurements from your current hardware.
- **Next Action**: To discard local run output and restore the repository's published baseline:
  ```bash
  git checkout results/
  ```

---

## 6. Docker-in-Docker (DinD) Bridge Network Isolation

- **Symptom**: `connect ECONNREFUSED 127.0.0.1:6379` occurs inside nested Linux dev containers despite `docker compose ps` showing Redis running.
- **Check**: Inspect container networking:
  ```bash
  docker compose ps
  ```
- **Expected Result**: Port published to `0.0.0.0:6379`.
- **Likely Cause**: Container bridge networks in nested Docker environments may fail to route traffic between the outer container host and inner Docker network.
- **Next Action**: On Linux hosts, enable host networking via a Compose override:
  ```yaml
  # docker-compose.override.yml
  services:
    redis:
      network_mode: host
      ports: !reset []
  ```
  *(Note: Host networking is supported on Linux only).*
