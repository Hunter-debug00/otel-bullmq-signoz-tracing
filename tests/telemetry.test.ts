import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Job } from 'bullmq';
import { Histogram } from '@opentelemetry/api';
import {
  calculateEnqueueToAttemptAgeMs,
  recordJobAgeAtAttempt,
  JOB_AGE_BUCKET_BOUNDARIES,
} from '../src/telemetry/job-age';
import { parseOtlpHeaders, normalizeOtlpBaseUrl } from '../src/instrumentation';

describe('calculateEnqueueToAttemptAgeMs', () => {
  it('calculates accurate duration between enqueue and attempt timestamps', () => {
    assert.equal(calculateEnqueueToAttemptAgeMs(1000, 1500), 500);
    assert.equal(calculateEnqueueToAttemptAgeMs(1000, 1000), 0);
  });

  it('throws on missing or non-finite enqueueTimestamp', () => {
    assert.throws(
      () => calculateEnqueueToAttemptAgeMs(0, 1500),
      /Invalid or missing enqueueTimestamp/
    );
    assert.throws(
      () => calculateEnqueueToAttemptAgeMs(-100, 1500),
      /Invalid or missing enqueueTimestamp/
    );
    assert.throws(
      () => calculateEnqueueToAttemptAgeMs(NaN, 1500),
      /Invalid or missing enqueueTimestamp/
    );
  });

  it('throws on missing or non-finite processedOnTimestamp', () => {
    assert.throws(
      () => calculateEnqueueToAttemptAgeMs(1000, undefined),
      /Invalid or missing processedOnTimestamp/
    );
    assert.throws(
      () => calculateEnqueueToAttemptAgeMs(1000, NaN),
      /Invalid or missing processedOnTimestamp/
    );
  });

  it('detects negative clock skew (processedOn < enqueueTimestamp) and returns null', () => {
    const age = calculateEnqueueToAttemptAgeMs(1500, 1200);
    assert.equal(age, null);
  });
});

describe('recordJobAgeAtAttempt', () => {
  it('calculates initial queue delay on the first attempt', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: 1450,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'success' },
    } as unknown as Job;

    const res = recordJobAgeAtAttempt(fakeJob);
    assert.equal(res.recorded, true);
    assert.equal(res.ageMs, 450);
  });

  it('calculates cumulative job age including backoff on retry attempts', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: 3255,
      attemptsMade: 1,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'transient-retry' },
    } as unknown as Job;

    const res = recordJobAgeAtAttempt(fakeJob);
    assert.equal(res.recorded, true);
    assert.equal(res.ageMs, 2255);
  });

  it('calculates cumulative job age for intentionally delayed jobs including intentional delay', () => {
    const fakeJob = {
      timestamp: 10000,
      processedOn: 15250,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'success' },
    } as unknown as Job;

    const res = recordJobAgeAtAttempt(fakeJob);
    assert.equal(res.recorded, true);
    assert.equal(res.ageMs, 5250);
  });

  it('skips metric observation when processedOn is missing without failing', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: undefined,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'success' },
    } as unknown as Job;

    const res = recordJobAgeAtAttempt(fakeJob);
    assert.deepEqual(res, {
      recorded: false,
      ageMs: null,
      anomalyReason: 'missing_processed_on',
    });
  });

  it('skips metric observation when negative clock skew is detected', () => {
    const fakeJob = {
      timestamp: 2000,
      processedOn: 1800,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'success' },
    } as unknown as Job;

    const res = recordJobAgeAtAttempt(fakeJob);
    assert.deepEqual(res, {
      recorded: false,
      ageMs: null,
      anomalyReason: 'negative_clock_skew',
    });
  });

  it('falls back to unknown scenario on unlisted scenario strings to bound cardinality', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: 1200,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'malicious-cardinality-injection' },
    } as unknown as Job;

    let recordedValue: number | null = null;
    let recordedAttributes: Record<string, unknown> | null = null;
    const mockHistogram = {
      record: (val: number, attrs: Record<string, unknown>) => {
        recordedValue = val;
        recordedAttributes = attrs;
      },
    } as unknown as Histogram;

    const res = recordJobAgeAtAttempt(fakeJob, undefined, mockHistogram);
    assert.equal(res.recorded, true);
    assert.equal(res.ageMs, 200);
    assert.equal(recordedValue, 200);
    assert.equal(recordedAttributes?.['bullmq.job.name'], 'unknown');
    assert.equal(recordedAttributes?.['bullmq.job.attempt_type'], 'first_attempt');
  });
});

describe('JOB_AGE_BUCKET_BOUNDARIES', () => {
  it('defines strictly increasing positive bucket boundaries up to 10000ms', () => {
    assert.ok(Array.isArray(JOB_AGE_BUCKET_BOUNDARIES));
    assert.equal(JOB_AGE_BUCKET_BOUNDARIES.length, 15);
    assert.deepEqual(JOB_AGE_BUCKET_BOUNDARIES, [
      5, 10, 25, 50, 100, 150, 250, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000,
    ]);
    for (let i = 0; i < JOB_AGE_BUCKET_BOUNDARIES.length - 1; i++) {
      assert.ok(
        JOB_AGE_BUCKET_BOUNDARIES[i] < JOB_AGE_BUCKET_BOUNDARIES[i + 1],
        `Boundaries must be strictly ascending: ${JOB_AGE_BUCKET_BOUNDARIES[i]} < ${JOB_AGE_BUCKET_BOUNDARIES[i + 1]}`
      );
    }
  });
});

describe('parseOtlpHeaders', () => {
  it('parses comma-separated key=value pairs and trims whitespace', () => {
    const parsed = parseOtlpHeaders(
      'signoz-ingestion-key=token_123, x-custom-header = test_val , other=456 '
    );
    assert.deepEqual(parsed, {
      'signoz-ingestion-key': 'token_123',
      'x-custom-header': 'test_val',
      'other': '456',
    });
  });

  it('returns empty object for empty or missing headers', () => {
    assert.deepEqual(parseOtlpHeaders(''), {});
    assert.deepEqual(parseOtlpHeaders(undefined), {});
  });
});

describe('normalizeOtlpBaseUrl', () => {
  it('strips trailing slashes from endpoint URL', () => {
    assert.equal(normalizeOtlpBaseUrl('http://localhost:4318/'), 'http://localhost:4318');
    assert.equal(normalizeOtlpBaseUrl('http://localhost:4318///'), 'http://localhost:4318');
  });

  it('preserves clean URL without trailing slashes', () => {
    assert.equal(normalizeOtlpBaseUrl('http://localhost:4318'), 'http://localhost:4318');
    assert.equal(
      normalizeOtlpBaseUrl('https://ingest.us.signoz.cloud:443'),
      'https://ingest.us.signoz.cloud:443'
    );
  });

  it('falls back to default localhost:4318 when undefined or empty', () => {
    assert.equal(normalizeOtlpBaseUrl(undefined), 'http://localhost:4318');
    assert.equal(normalizeOtlpBaseUrl(''), 'http://localhost:4318');
    assert.equal(normalizeOtlpBaseUrl('   '), 'http://localhost:4318');
  });
});
