import { Job } from 'bullmq';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { recordJobAgeAtAttempt } from '../telemetry/job-age';

export type JobScenario =
  | 'success'
  | 'transient-retry'
  | 'exhausted-failure'
  | 'slow-worker';

export interface DiagnosticJobData {
  runId: string;
  scenario: JobScenario;
  simulatedWorkMs?: number;
}

export interface DiagnosticJobResult {
  outcome: 'completed' | 'completed_slowly';
  attemptNumber: number;
  jobAgeMs: number;
  executionDurationMs: number;
  traceId?: string;
}

const tracer = trace.getTracer('bullmq-diagnostics');
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function processDiagnosticJob(
  job: Job<DiagnosticJobData>
): Promise<DiagnosticJobResult> {
  const startPerf = performance.now();
  const { runId, scenario, simulatedWorkMs } = job.data;
  const attemptNumber = job.attemptsMade + 1;

  const jobAgeMs = recordJobAgeAtAttempt(job, scenario);

  const activeSpan = trace.getActiveSpan();
  const traceId = activeSpan?.spanContext().traceId;

  activeSpan?.setAttributes({
    'bullmq.job.name': scenario,
    'bullmq.job.attempt_number': attemptNumber,
    'bullmq.job.age_at_attempt_ms': jobAgeMs,
    'app.run_id': runId,
  });

  console.log(
    `[Worker] jobId=${job.id} runId=${runId} bullmq.job.name=${scenario} attempt=${attemptNumber} jobAgeMs=${jobAgeMs}ms`
  );

  return tracer.startActiveSpan(`execute_scenario.${scenario}`, async (span) => {
    try {
      const workMs = scenario === 'slow-worker' ? (simulatedWorkMs ?? 2000) : (simulatedWorkMs ?? 100);
      await sleep(workMs);

      if (scenario === 'transient-retry' && attemptNumber === 1) {
        throw new Error('Transient downstream 503 timeout on attempt 1');
      }

      if (scenario === 'exhausted-failure') {
        throw new Error(`Persistent downstream failure on attempt ${attemptNumber}`);
      }

      const executionDurationMs = Number((performance.now() - startPerf).toFixed(2));
      span.setStatus({ code: SpanStatusCode.OK });
      const result: DiagnosticJobResult = {
        outcome: scenario === 'slow-worker' ? 'completed_slowly' : 'completed',
        attemptNumber,
        jobAgeMs,
        executionDurationMs,
        traceId,
      };
      return result;
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      throw err;
    } finally {
      span.end();
    }
  });
}
