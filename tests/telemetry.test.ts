import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Job } from 'bullmq';
import { recordJobAgeAtAttempt } from '../src/telemetry/job-age';

describe('recordJobAgeAtAttempt', () => {
  it('calculates initial queue delay on the first attempt', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: 1450,
      attemptsMade: 0,
      queueName: 'diagnostics-job-queue',
    } as unknown as Job;

    const ageMs = recordJobAgeAtAttempt(fakeJob, 'success');
    assert.equal(ageMs, 450);
  });

  it('calculates cumulative job age including backoff on retry attempts', () => {
    const fakeJob = {
      timestamp: 1000,
      processedOn: 3255,
      attemptsMade: 1,
      queueName: 'diagnostics-job-queue',
    } as unknown as Job;

    const ageMs = recordJobAgeAtAttempt(fakeJob, 'transient-retry');
    assert.equal(ageMs, 2255);
  });
});
