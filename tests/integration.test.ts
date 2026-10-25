import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Queue, Worker, QueueEvents, Job } from 'bullmq';
import { BullMQOtel } from 'bullmq-otel';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import {
  MeterProvider,
  PeriodicExportingMetricReader,
  InMemoryMetricExporter,
  AggregationTemporality,
} from '@opentelemetry/sdk-metrics';
import { createRedisConnection } from '../src/queue';
import {
  processDiagnosticJob,
  DiagnosticJobData,
  DiagnosticJobResult,
} from '../src/jobs/processor';
import {
  createJobAgeHistogram,
  recordJobAgeAtAttempt,
  JOB_AGE_BUCKET_BOUNDARIES,
} from '../src/telemetry/job-age';
import { SimpleSpanProcessor, InMemorySpanExporter } from '@opentelemetry/sdk-trace-base';
import { createTelemetrySdk } from '../src/instrumentation';

const TEST_QUEUE_NAME = 'integration-test-queue';

describe('BullMQ + OpenTelemetry Integration Verification', () => {
  let queue: Queue<DiagnosticJobData, DiagnosticJobResult>;
  let worker: Worker<DiagnosticJobData, DiagnosticJobResult>;
  let queueEvents: QueueEvents;

  const inMemorySpanExporter = new InMemorySpanExporter();
  const spanProcessor = new SimpleSpanProcessor(inMemorySpanExporter);
  const testSdk = createTelemetrySdk({
    serviceName: 'test-runner',
    spanProcessors: [spanProcessor],
  });

  const connections = {
    queue: createRedisConnection(),
    events: createRedisConnection(),
    worker: createRedisConnection(),
  };

  before(async () => {
    testSdk.start();

    const telemetry = new BullMQOtel({ tracerName: 'test-tracer', enableMetrics: false });

    queue = new Queue(TEST_QUEUE_NAME, {
      connection: connections.queue,
      telemetry,
    });
    await queue.obliterate({ force: true });

    queueEvents = new QueueEvents(TEST_QUEUE_NAME, {
      connection: connections.events,
    });
    await queueEvents.waitUntilReady();

    worker = new Worker(
      TEST_QUEUE_NAME,
      async (job) => processDiagnosticJob(job),
      {
        connection: connections.worker,
        telemetry,
        concurrency: 2,
      }
    );
    await worker.waitUntilReady();
  });

  after(async () => {
    await Promise.allSettled([
      worker?.close(),
      queueEvents?.close(),
      queue?.close(),
    ]);
    await Promise.allSettled([
      connections.queue.quit(),
      connections.events.quit(),
      connections.worker.quit(),
    ]);
    await testSdk.shutdown();
  });

  it('1. propagates producer trace context to the worker span and confirms root producer span remains short', async () => {
    const tracer = trace.getTracer('producer-test');
    let producerTraceId = '';
    let producerDurationMs = 0;

    const startPerf = performance.now();
    const result = await tracer.startActiveSpan('http.post.jobs', async (span) => {
      producerTraceId = span.spanContext().traceId;
      const job = await queue.add('success', {
        runId: 'int_success',
        scenario: 'success',
        simulatedWorkMs: 100,
      });
      span.end();
      producerDurationMs = performance.now() - startPerf;
      return job.waitUntilFinished(queueEvents, 5000);
    });

    assert.equal(result.outcome, 'completed');
    assert.equal(result.attemptNumber, 1);
    assert.equal(result.traceId, producerTraceId, 'Worker traceId must match Producer traceId');
    assert.equal(typeof result.enqueueToAttemptAgeMs, 'number');
    assert.equal(typeof result.processorElapsedMs, 'number');
    // Confirm producer root span finished fast without being artificially prolonged by async worker execution
    assert.ok(
      producerDurationMs < 60,
      `Producer span duration (${producerDurationMs.toFixed(2)}ms) should finish quickly while worker runs asynchronously`
    );
    assert.ok(
      result.processorElapsedMs >= 80,
      `Worker processor execution (${result.processorElapsedMs}ms) should reflect the simulated work duration`
    );
  });

  it('2. retries transient failures and completes on attempt 2', async () => {
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

  it('3. exhausted-failure ends in failed state after 3 attempts and carries SpanStatusCode.ERROR with exception events', async () => {
    const job = await queue.add(
      'exhausted-failure',
      { runId: 'int_exhausted', scenario: 'exhausted-failure', simulatedWorkMs: 20 },
      { attempts: 3, backoff: { type: 'fixed', delay: 100 } }
    );

    await assert.rejects(() => job.waitUntilFinished(queueEvents, 5000));
    const failedJob = await queue.getJob(job.id!);

    assert.equal(await failedJob?.getState(), 'failed');
    assert.equal(failedJob?.attemptsMade, 3);

    // Flush in-memory span exporter and verify span attributes and error status
    await spanProcessor.forceFlush();
    const finishedSpans = inMemorySpanExporter.getFinishedSpans();
    const exhaustedSpans = finishedSpans.filter(
      (s) => s.name === 'execute_scenario.exhausted-failure'
    );

    assert.ok(exhaustedSpans.length >= 3, 'Must record execution spans for all 3 attempts');
    for (const span of exhaustedSpans) {
      assert.equal(
        span.status.code,
        SpanStatusCode.ERROR,
        'Failed scenario spans must have ERROR status code'
      );
      assert.match(
        span.status.message || '',
        /Persistent downstream failure/,
        'Error status message must record failure reason'
      );
      const exceptionEvents = span.events.filter((e) => e.name === 'exception');
      assert.ok(exceptionEvents.length > 0, 'Span must contain an exception event');
    }
  });

  it('4. malformed job data fails immediately via UnrecoverableError after 1 attempt without retries', async () => {
    const malformedJob = await queue.add(
      'bad-payload-job',
      { runId: 'invalid\nnewline', scenario: 'success' } as any,
      { attempts: 3, backoff: { type: 'fixed', delay: 100 } }
    );

    await assert.rejects(() => malformedJob.waitUntilFinished(queueEvents, 5000));
    const failedJob = await queue.getJob(malformedJob.id!);

    assert.equal(await failedJob?.getState(), 'failed');
    // UnrecoverableError must fail the job on attempt 1 without consuming the remaining 2 retries
    assert.equal(
      failedJob?.attemptsMade,
      1,
      'UnrecoverableError must stop further retry attempts immediately'
    );
  });

  it('5. exports job_age_at_attempt_ms metric with unit ms, explicit bucket boundaries, and low-cardinality attributes', async () => {
    const memoryMetricExporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
    const metricReader = new PeriodicExportingMetricReader({
      exporter: memoryMetricExporter,
      exportIntervalMillis: 60000,
    });
    const isolatedMeterProvider = new MeterProvider({ readers: [metricReader] });
    const isolatedMeter = isolatedMeterProvider.getMeter('isolated-test-meter');
    const isolatedHistogram = createJobAgeHistogram(isolatedMeter);

    const testJob = {
      id: 'sensitive-customer-job-id-9999',
      queueName: 'diagnostics-job-queue',
      timestamp: Date.now() - 350,
      processedOn: Date.now(),
      attemptsMade: 0,
      data: {
        runId: 'sensitive-run-id-should-not-be-in-metrics',
        scenario: 'success',
      },
    } as unknown as Job;

    recordJobAgeAtAttempt(testJob, undefined, isolatedHistogram);

    await metricReader.forceFlush();
    const resourceMetrics = memoryMetricExporter.getMetrics();
    const scopeMetrics = resourceMetrics.flatMap((rm) => rm.scopeMetrics);
    const metricsList = scopeMetrics.flatMap((sm) => sm.metrics);

    const ageHistogramMetric = metricsList.find(
      (m) => m.descriptor.name === 'job_age_at_attempt_ms'
    );

    assert.ok(ageHistogramMetric, 'job_age_at_attempt_ms metric must be exported');
    assert.equal(ageHistogramMetric.descriptor.unit, 'ms');

    // Verify bucket boundaries from descriptor advice and exported buckets
    const descriptorAdvice = (
      ageHistogramMetric.descriptor as {
        advice?: { explicitBucketBoundaries?: number[] };
      }
    ).advice;
    assert.deepEqual(
      descriptorAdvice?.explicitBucketBoundaries,
      JOB_AGE_BUCKET_BOUNDARIES,
      'Exported explicit bucket boundaries advice must match JOB_AGE_BUCKET_BOUNDARIES'
    );

    // Verify low-cardinality dimensions and absence of high-cardinality identifiers
    const dataPoints = ageHistogramMetric.dataPoints;
    assert.ok(dataPoints.length > 0, 'Metric must contain recorded data points');
    const bucketBoundaries = (
      dataPoints[0].value as { buckets?: { boundaries?: number[] } }
    )?.buckets?.boundaries;
    assert.deepEqual(
      bucketBoundaries,
      JOB_AGE_BUCKET_BOUNDARIES,
      'Exported histogram bucket boundaries must match JOB_AGE_BUCKET_BOUNDARIES'
    );

    for (const dp of dataPoints) {
      const attrs = dp.attributes;
      assert.equal(attrs['bullmq.queue.name'], 'diagnostics-job-queue');
      assert.equal(attrs['bullmq.job.name'], 'success');
      assert.equal(attrs['bullmq.job.attempt_type'], 'first_attempt');

      // Assert high-cardinality attributes are strictly omitted
      assert.equal(attrs['job.id'], undefined, 'Must not export job.id attribute');
      assert.equal(attrs['runId'], undefined, 'Must not export runId attribute');
      assert.equal(attrs['app.run_id'], undefined, 'Must not export app.run_id attribute');
    }

    await isolatedMeterProvider.shutdown();
  });
});
