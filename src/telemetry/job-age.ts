import { metrics, Meter, Histogram } from '@opentelemetry/api';
import { Job } from 'bullmq';
import { ALLOWED_SCENARIOS, JobScenario } from '../validation';

const meter = metrics.getMeter('bullmq-diagnostics', '1.0.0');

export const JOB_AGE_BUCKET_BOUNDARIES = [
  5, 10, 25, 50, 100, 150, 250, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000,
];

export function createJobAgeHistogram(targetMeter: Meter = meter): Histogram {
  return targetMeter.createHistogram('job_age_at_attempt_ms', {
    description:
      'Elapsed wall-clock time (ms) from initial job enqueue to the start of the current processing attempt',
    unit: 'ms',
    advice: {
      explicitBucketBoundaries: JOB_AGE_BUCKET_BOUNDARIES,
    },
  });
}

export const jobAgeHistogram = createJobAgeHistogram(meter);

/**
 * Pure function to calculate enqueue-to-attempt age in milliseconds.
 * Throws on missing or non-finite timestamps.
 * Warns on negative clock skew (processedOn < enqueueTimestamp) and clamps at 0.
 */
export function calculateEnqueueToAttemptAgeMs(
  enqueueTimestamp: number,
  processedOnTimestamp: number | undefined
): number {
  if (
    typeof enqueueTimestamp !== 'number' ||
    !Number.isFinite(enqueueTimestamp) ||
    enqueueTimestamp <= 0
  ) {
    throw new Error(`Invalid or missing enqueueTimestamp: ${enqueueTimestamp}`);
  }
  if (
    processedOnTimestamp === undefined ||
    typeof processedOnTimestamp !== 'number' ||
    !Number.isFinite(processedOnTimestamp)
  ) {
    throw new Error(`Invalid or missing processedOnTimestamp: ${processedOnTimestamp}`);
  }

  const diff = processedOnTimestamp - enqueueTimestamp;
  if (diff < 0) {
    console.warn(
      `[Telemetry] Negative clock skew detected: processedOn (${processedOnTimestamp}) < enqueueTimestamp (${enqueueTimestamp}). Clamping to 0.`
    );
    return 0;
  }

  return diff;
}

/**
 * Records enqueue-to-attempt age metric using a validated scenario allowlist to prevent high cardinality.
 */
export function recordJobAgeAtAttempt(
  job: Job,
  jobName?: string,
  histogram: Histogram = jobAgeHistogram
): number {
  const processedOn = job.processedOn ?? Date.now();
  if (job.processedOn === undefined) {
    console.warn(
      `[Telemetry] job.processedOn is missing for job id=${job.id ?? 'unknown'}, using current local timestamp`
    );
  }

  const jobAgeMs = calculateEnqueueToAttemptAgeMs(job.timestamp, processedOn);

  // Derive scenario from job.data.scenario or provided jobName, enforcing finite allowlist
  const rawScenario = (job.data as any)?.scenario ?? jobName ?? job.name;
  const validatedScenario: JobScenario | 'unknown' = ALLOWED_SCENARIOS.includes(
    rawScenario as JobScenario
  )
    ? (rawScenario as JobScenario)
    : 'unknown';

  histogram.record(jobAgeMs, {
    'bullmq.queue.name': job.queueName,
    'bullmq.job.name': validatedScenario,
    'bullmq.job.attempt_type': job.attemptsMade === 0 ? 'first_attempt' : 'retry_attempt',
  });

  return jobAgeMs;
}
