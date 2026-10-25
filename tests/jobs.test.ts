import { describe, it, beforeEach, afterEach } from 'node:test';
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
  beforeEach(() => {
    setSleepForTesting(async () => {});
  });

  afterEach(() => {
    resetSleepForTesting();
  });

  it('completes normal success scenario on attempt 1', async () => {
    const job = createMockJob(
      { runId: 'test_1', scenario: 'success', simulatedWorkMs: 50 },
      0
    );
    const result = await processDiagnosticJob(job);
    assert.equal(result.outcome, 'completed');
    assert.equal(result.attemptNumber, 1);
    assert.ok(Number.isFinite(result.processorElapsedMs) && result.processorElapsedMs >= 0);
    assert.equal(typeof result.enqueueToAttemptAgeMs, 'number');
  });

  it('completes slow-worker scenario with outcome completed_slowly', async () => {
    const job = createMockJob(
      { runId: 'test_slow', scenario: 'slow-worker', simulatedWorkMs: 2000 },
      0
    );
    const result = await processDiagnosticJob(job);
    assert.equal(result.outcome, 'completed_slowly');
    assert.equal(result.attemptNumber, 1);
    assert.ok(
      Number.isFinite(result.processorElapsedMs) && result.processorElapsedMs >= 0,
      `Expected processorElapsedMs (${result.processorElapsedMs}) to be non-negative finite number`
    );
  });

  it('throws on attempt 1 of transient-retry and succeeds on attempt 2', async () => {
    const attempt1Job = createMockJob(
      { runId: 'test_2', scenario: 'transient-retry', simulatedWorkMs: 50 },
      0
    );
    await assert.rejects(
      () => processDiagnosticJob(attempt1Job),
      /Transient downstream 503 timeout on attempt 1/
    );

    const attempt2Job = createMockJob(
      { runId: 'test_2', scenario: 'transient-retry', simulatedWorkMs: 50 },
      1
    );
    const result = await processDiagnosticJob(attempt2Job);
    assert.equal(result.outcome, 'completed');
    assert.equal(result.attemptNumber, 2);
  });

  it('throws persistent downstream failure on exhausted-failure attempts', async () => {
    for (let attempt = 0; attempt < 3; attempt++) {
      const job = createMockJob(
        { runId: 'test_exhausted', scenario: 'exhausted-failure', simulatedWorkMs: 50 },
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
      { runId: 'test_non_error', scenario: 'success', simulatedWorkMs: 50 },
      0
    );

    setSleepForTesting(async () => {
      throw 'Raw string exception payload';
    });

    await assert.rejects(
      () => processDiagnosticJob(job),
      (err: unknown) => {
        assert.ok(err instanceof Error, 'Caught error must be normalized to Error instance');
        assert.equal(err.message, 'Raw string exception payload');
        return true;
      }
    );
  });
});
