# Telemetry & Queue Architecture

## System Flow

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

## Telemetry Signals

### Distributed Tracing
Producer context is injected into BullMQ job metadata using `bullmq-otel`. When a worker dequeues a job, the trace context is extracted and set as the parent of the execution span, establishing an end-to-end trace from the initial HTTP request through queue processing.

### Metrics
1. **Execution Duration (`bullmq.job.duration`)**: Measures worker processing latency. This metric reflects execution runtime only and does not capture queuing delays or retry backoff intervals.
2. **Job Age at Attempt (`job_age_at_attempt_ms`)**: Explicit-bucket histogram recording elapsed time from job creation (`timestamp`) to the start of processing (`processedOn`). Tagged with `bullmq.job.attempt_type` (`first_attempt` vs `retry_attempt`) to distinguish backlog queuing delay from exponential backoff wait times.
3. **Queue Depth (`bullmq.queue.jobs`)**: Observable state gauge updated periodically via `recordJobCountsMetric()`, reporting counts for `waiting`, `active`, `delayed`, and `failed` states.
