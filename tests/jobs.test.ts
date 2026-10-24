import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Job, UnrecoverableError } from 'bullmq';
import {
  processDiagnosticJob,
  DiagnosticJobData,
  setSleepForTesting,
  resetSleepForTesting,
} from '../src/jobs/processor';

function createMockJob(
  data: DiagnosticJobData,
  attemptsMade: number
): Job<DiagnosticJobData> {
  return {
    id: 'test-job-mock',
    timestamp: Date.now() - 150,
    processedOn: Date.now(),
    attemptsMade,
    queueName: 'diagnostics-job-queue',
    data,
  } as unknown as Job<DiagnosticJobData>;
}

describe('processDiagnosticJob scenarios', () => {
  it('completes normal success scenario on attempt 1', async () => {
    const job = createMockJob(
      { runId: 'test_1', scenario: 'success', simulatedWorkMs: 5 },
      0
    );
    const result = await processDiagnosticJob(job);
    assert.equal(result.outcome, 'completed');
    assert.equal(result.attemptNumber, 1);
    assert.equal(typeof result.processorElapsedMs, 'number');
    assert.equal(typeof result.enqueueToAttemptAgeMs, 'number');
  });

  it('completes slow-worker scenario with outcome completed_slowly', async () => {
    const job = createMockJob(
      { runId: 'test_slow', scenario: 'slow-worker', simulatedWorkMs: 15 },
      0
    );
    const result = await processDiagnosticJob(job);
    assert.equal(result.outcome, 'completed_slowly');
    assert.equal(result.attemptNumber, 1);
    assert.ok(
      result.processorElapsedMs >= 10,
      `Expected processorElapsedMs (${result.processorElapsedMs}) to reflect simulated work delay`
    );
  });

  it('throws on attempt 1 of transient-retry and succeeds on attempt 2', async () => {
    const attempt1Job = createMockJob(
      { runId: 'test_2', scenario: 'transient-retry', simulatedWorkMs: 5 },
      0
    );
    await assert.rejects(
      () => processDiagnosticJob(attempt1Job),
      /Transient downstream 503 timeout on attempt 1/
    );

    const attempt2Job = createMockJob(
      { runId: 'test_2', scenario: 'transient-retry', simulatedWorkMs: 5 },
      1
    );
    const result = await processDiagnosticJob(attempt2Job);
    assert.equal(result.outcome, 'completed');
    assert.equal(result.attemptNumber, 2);
  });

  it('throws persistent downstream failure on exhausted-failure attempts', async () => {
    for (let attempt = 0; attempt < 3; attempt++) {
      const job = createMockJob(
        { runId: 'test_exhausted', scenario: 'exhausted-failure', simulatedWorkMs: 5 },
        attempt
      );
      await assert.rejects(
        () => processDiagnosticJob(job),
        new RegExp(`Persistent downstream failure on attempt ${attempt + 1}`)
      );
    }
  });

  it('throws UnrecoverableError immediately when job data fails validation', async () => {
    const malformedJob = createMockJob(
      { runId: 'invalid\nnewline', scenario: 'success' },
      0
    );
    await assert.rejects(
      () => processDiagnosticJob(malformedJob),
      (err: unknown) =>
        err instanceof UnrecoverableError && /Malformed job data/.test(err.message)
    );
  });

  it('normalizes non-Error thrown values into an Error instance', async () => {
    const job = createMockJob(
      { runId: 'test_non_error', scenario: 'success', simulatedWorkMs: 5 },
      0
    );

    setSleepForTesting(async () => {
      // Throw a raw string instead of an Error
      throw 'Raw string exception payload';
    });

    try {
      await assert.rejects(
        () => processDiagnosticJob(job),
        (err: unknown) => {
          assert.ok(err instanceof Error, 'Caught error must be normalized to Error instance');
          assert.equal(err.message, 'Raw string exception payload');
          return true;
        }
      );
    } finally {
      resetSleepForTesting();
    }
  });
});
