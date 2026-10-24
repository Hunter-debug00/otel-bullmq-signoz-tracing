import { Job, UnrecoverableError } from 'bullmq';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { recordJobAgeAtAttempt } from '../telemetry/job-age';
import {
  DiagnosticJobData,
  JobScenario,
  validateDiagnosticJobData,
} from '../validation';

export type { JobScenario, DiagnosticJobData };

export interface DiagnosticJobResult {
  outcome: 'completed' | 'completed_slowly';
  attemptNumber: number;
  enqueueToAttemptAgeMs: number;
  processorElapsedMs: number;
  // Aliases preserved for backwards compatibility
  jobAgeMs?: number;
  executionDurationMs?: number;
  traceId?: string;
}

const tracer = trace.getTracer('bullmq-diagnostics');
let sleepImpl = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function setSleepForTesting(fn: (ms: number) => Promise<void>) {
  sleepImpl = fn;
}

export function resetSleepForTesting() {
  sleepImpl = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
}

export async function processDiagnosticJob(
  job: Job<DiagnosticJobData>
): Promise<DiagnosticJobResult> {
  const startPerf = performance.now();

  // Validate job.data at worker boundary
  const validation = validateDiagnosticJobData(job.data);
  if (!validation.success || !validation.data) {
    throw new UnrecoverableError(`Malformed job data: ${validation.error}`);
  }

  const { runId, scenario, simulatedWorkMs } = validation.data;
  const attemptNumber = job.attemptsMade + 1;

  const enqueueToAttemptAgeMs = recordJobAgeAtAttempt(job, scenario);

  const activeSpan = trace.getActiveSpan();
  const traceId = activeSpan?.spanContext().traceId;

  const diagnosticSpanAttributes = {
    'bullmq.job.name': scenario,
    'bullmq.job.attempt_number': attemptNumber,
    'bullmq.job.age_at_attempt_ms': enqueueToAttemptAgeMs,
    'app.run_id': runId,
  };

  // Set attributes on the active parent consumer span
  activeSpan?.setAttributes(diagnosticSpanAttributes);

  console.log(
    `[Worker] jobId=${job.id} runId=${runId} bullmq.job.name=${scenario} attempt=${attemptNumber} enqueueToAttemptAgeMs=${enqueueToAttemptAgeMs}ms`
  );

  return tracer.startActiveSpan(`execute_scenario.${scenario}`, async (span) => {
    // Set attributes inside the child scenario span as well
    span.setAttributes(diagnosticSpanAttributes);

    try {
      const workMs =
        scenario === 'slow-worker'
          ? (simulatedWorkMs ?? 2000)
          : (simulatedWorkMs ?? 100);
      await sleepImpl(workMs);

      if (scenario === 'transient-retry' && attemptNumber === 1) {
        throw new Error('Transient downstream 503 timeout on attempt 1');
      }

      if (scenario === 'exhausted-failure') {
        throw new Error(`Persistent downstream failure on attempt ${attemptNumber}`);
      }

      const processorElapsedMs = Number((performance.now() - startPerf).toFixed(2));
      span.setStatus({ code: SpanStatusCode.OK });
      const result: DiagnosticJobResult = {
        outcome: scenario === 'slow-worker' ? 'completed_slowly' : 'completed',
        attemptNumber,
        enqueueToAttemptAgeMs,
        processorElapsedMs,
        jobAgeMs: enqueueToAttemptAgeMs,
        executionDurationMs: processorElapsedMs,
        traceId,
      };
      return result;
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      span.recordException(error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
      throw error;
    } finally {
      span.end();
    }
  });
}
