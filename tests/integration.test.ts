import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Queue, Worker, QueueEvents } from 'bullmq';
import { BullMQOtel } from 'bullmq-otel';
import { trace } from '@opentelemetry/api';
import { createRedisConnection } from '../src/queue';
import { processDiagnosticJob, DiagnosticJobData, DiagnosticJobResult } from '../src/jobs/processor';

const TEST_QUEUE_NAME = 'integration-test-queue';

describe('BullMQ + OpenTelemetry Integration Verification', () => {
  let queue: Queue<DiagnosticJobData, DiagnosticJobResult>;
  let worker: Worker<DiagnosticJobData, DiagnosticJobResult>;
  let queueEvents: QueueEvents;

  before(async () => {
    const telemetry = new BullMQOtel({ tracerName: 'test-tracer', enableMetrics: false });

    queue = new Queue(TEST_QUEUE_NAME, {
      connection: createRedisConnection(),
      telemetry,
    });
    await queue.obliterate({ force: true });

    queueEvents = new QueueEvents(TEST_QUEUE_NAME, {
      connection: createRedisConnection(),
    });

    worker = new Worker(
      TEST_QUEUE_NAME,
      async (job) => processDiagnosticJob(job),
      {
        connection: createRedisConnection(),
        telemetry,
        concurrency: 2,
      }
    );
    await worker.waitUntilReady();
  });

  after(async () => {
    await worker.close();
    await queueEvents.close();
    await queue.close();
  });

  it('propagates producer trace context to the worker span', async () => {
    const tracer = trace.getTracer('producer-test');
    let producerTraceId = '';

    const result = await tracer.startActiveSpan('http.post.jobs', async (span) => {
      producerTraceId = span.spanContext().traceId;
      const job = await queue.add('success', {
        runId: 'int_success',
        scenario: 'success',
        simulatedWorkMs: 20,
      });
      span.end();
      return job.waitUntilFinished(queueEvents, 5000);
    });

    assert.equal(result.outcome, 'completed');
    assert.equal(result.attemptNumber, 1);
    assert.equal(result.traceId, producerTraceId, 'Worker traceId must match Producer traceId');
  });

  it('retries transient failures and completes on attempt 2', async () => {
    const job = await queue.add(
      'transient-retry',
      { runId: 'int_transient', scenario: 'transient-retry', simulatedWorkMs: 20 },
      { attempts: 3, backoff: { type: 'fixed', delay: 100 } }
    );

    const result = await job.waitUntilFinished(queueEvents, 5000);
    const updatedJob = await queue.getJob(job.id!);

    assert.equal(result.outcome, 'completed');
    assert.equal(result.attemptNumber, 2);
    assert.equal(await updatedJob?.getState(), 'completed');
  });

  it('marks job failed after exhausting maximum retry attempts', async () => {
    const job = await queue.add(
      'exhausted-failure',
      { runId: 'int_exhausted', scenario: 'exhausted-failure', simulatedWorkMs: 20 },
      { attempts: 3, backoff: { type: 'fixed', delay: 100 } }
    );

    await assert.rejects(() => job.waitUntilFinished(queueEvents, 5000));
    const failedJob = await queue.getJob(job.id!);

    assert.equal(await failedJob?.getState(), 'failed');
    assert.equal(failedJob?.attemptsMade, 3);
  });
});
