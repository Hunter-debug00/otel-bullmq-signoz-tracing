import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Job } from 'bullmq';
import {
  calculateEnqueueToAttemptAgeMs,
  recordJobAgeAtAttempt,
  JOB_AGE_BUCKET_BOUNDARIES,
} from '../src/telemetry/job-age';
import { parseOtlpHeaders } from '../src/instrumentation';

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

  it('detects negative clock skew (processedOn < enqueueTimestamp) and clamps duration to 0', () => {
    // processedOn earlier than enqueue due to clock skew across nodes
    const age = calculateEnqueueToAttemptAgeMs(1500, 1200);
    assert.equal(age, 0);
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

    const ageMs = recordJobAgeAtAttempt(fakeJob);
    assert.equal(ageMs, 450);
  });

  it('calculates cumulative job age including backoff on retry attempts', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: 3255,
      attemptsMade: 1,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'transient-retry' },
    } as unknown as Job;

    const ageMs = recordJobAgeAtAttempt(fakeJob);
    assert.equal(ageMs, 2255);
  });

  it('calculates cumulative job age for intentionally delayed jobs including intentional delay', () => {
    // Job scheduled with 5000ms delay, picked up 250ms after eligibility
    const fakeJob = {
      timestamp: 10000,
      processedOn: 15250,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'success' },
    } as unknown as Job;

    const ageMs = recordJobAgeAtAttempt(fakeJob);
    assert.equal(ageMs, 5250);
  });

  it('handles missing processedOn by falling back to current local timestamp', () => {
    const before = Date.now();
    const fakeJob = {
      timestamp: before - 200,
      processedOn: undefined,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'success' },
    } as unknown as Job;

    const ageMs = recordJobAgeAtAttempt(fakeJob);
    assert.ok(ageMs >= 200 && ageMs <= 500);
  });

  it('falls back to unknown scenario on unlisted scenario strings to bound cardinality', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: 1200,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
      data: { scenario: 'malicious-cardinality-injection' },
    } as unknown as Job;

    const ageMs = recordJobAgeAtAttempt(fakeJob);
    assert.equal(ageMs, 200);
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
      'signoz-access-token=token_123, x-custom-header = test_val , other=456 '
    );
    assert.deepEqual(parsed, {
      'signoz-access-token': 'token_123',
      'x-custom-header': 'test_val',
      'other': '456',
    });
  });

  it('returns empty object for empty or missing headers', () => {
    assert.deepEqual(parseOtlpHeaders(''), {});
    assert.deepEqual(parseOtlpHeaders(undefined), {});
  });
});
