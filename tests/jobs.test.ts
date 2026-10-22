import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Job } from 'bullmq';
import { processDiagnosticJob, DiagnosticJobData } from '../src/jobs/processor';

function createMockJob(
  data: DiagnosticJobData,
  attemptsMade: number
): Job<DiagnosticJobData> {
  return {
    id: 'test-job-1',
    timestamp: Date.now() - 100,
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
  });
});
