# Troubleshooting & Environment Workarounds

This document covers common environment issues, network edge-cases, and troubleshooting steps when developing with Docker, BullMQ, and OpenTelemetry collectors.

---

## 1. GitHub Codespaces & Docker-in-Docker (DinD) Networking

When running inside **GitHub Codespaces** or a containerized development container using **Docker-in-Docker (DinD)**, container bridge networks (`bridge` mode) may fail to route traffic between the outer Codespace host (where your Node.js processes run) and nested sibling Docker containers.

### Symptoms
- `Error: connect ECONNREFUSED 127.0.0.1:6379` when connecting to Redis.
- OpenTelemetry traces or metrics failing with `AggregateError [ECONNREFUSED] ::1:4318` or `127.0.0.1:4318`.
- DNS resolution failures between nested Docker containers.

### Recommended Approaches

#### Option A: Run Redis as a Minimal Service (Recommended)
This repository isolates Redis from full-scale telemetry collector deployments. The default `docker-compose.yml` runs only Redis on `6379:6379`:

```bash
docker compose up -d redis
```

In GitHub Codespaces, ensure port `6379` is forwarded to `localhost` in the VS Code / Codespaces Ports tab.

#### Option B: Use SigNoz Cloud Instead of Local SigNoz
Running a full local SigNoz stack (ClickHouse, Alertmanager, Query Service, Collector) inside a Codespace requires significant RAM and CPU. Instead, export telemetry to **SigNoz Cloud**:

1. In `.env`:
   ```bash
   OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.<region>.signoz.cloud:443
   OTEL_EXPORTER_OTLP_HEADERS=signoz-ingestion-key=<your-signoz-ingestion-key>
   ```
2. Start your services normally:
   ```bash
   npm run start:api
   npm run start:worker
   ```

#### Option C: Host Networking for Nested Containers (Targeted Workaround)
If running a local collector container inside Linux DinD and bridge port publishing fails, you can set `network_mode: host` in a Compose override:

```yaml
# docker-compose.override.yml
services:
  redis:
    network_mode: host
    ports: !reset []
```

> [!WARNING]
> **Host Networking Limitations**:
> - `network_mode: host` is supported on **Linux only** (it does not work on Docker Desktop for macOS or Windows).
> - Containers using host networking bypass Docker's bridge network isolation and cannot use Docker's internal DNS service-name resolution (`http://collector:4318`). All endpoints must resolve to `localhost` or `127.0.0.1`.
> - Check for port collisions with existing services before enabling host networking.

Before and after applying an override, verify your configuration:
```bash
docker compose config
```

To revert changes and return to standard Docker bridge networking:
```bash
rm -f docker-compose.override.yml
docker compose down -v
docker compose up -d redis
```

---

## 2. Telemetry Collector Down / Unreachable

If the OpenTelemetry collector (`http://localhost:4318`) is not running:
- The Express API and BullMQ worker will still function normally; OpenTelemetry exporter errors are logged at shutdown without throwing unhandled rejections.
- The benchmark script (`scripts/run-experiment.ts`) catches exporter flush errors gracefully during `shutdownTelemetry()`.

To start local SigNoz or verify collector status:
```bash
curl -I http://localhost:4318/v1/traces
```
A reachable collector will respond with HTTP `405 Method Not Allowed` (to GET/HEAD) or `200 OK` (to valid POST).

---

## 3. Clock Skew and Timestamp Caveats

The `job_age_at_attempt_ms` metric relies on wall-clock time:
$$\text{age} = \text{job.processedOn} - \text{job.timestamp}$$

- If producer instances and worker instances run on separate physical hosts whose system clocks are out of synchronization (e.g. NTP drift), the subtraction may yield anomalous readings.
- The implementation in [`src/telemetry/job-age.ts`](../src/telemetry/job-age.ts) detects negative skew (`processedOn < timestamp`), logs a rate-limited warning, and skips the metric observation to prevent recording misleading data.
