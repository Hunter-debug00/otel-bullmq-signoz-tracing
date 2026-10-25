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

const lastWarningTimestampByReason = new Map<string, number>();
const WARNING_COOLDOWN_MS = 60_000;

function warnRateLimited(reason: string, message: string): void {
  const now = Date.now();
  const lastWarned = lastWarningTimestampByReason.get(reason) ?? 0;
  if (now - lastWarned >= WARNING_COOLDOWN_MS) {
    lastWarningTimestampByReason.set(reason, now);
    console.warn(`[Telemetry] ${message}`);
  }
}

export interface JobAgeMeasurementResult {
  recorded: boolean;
  ageMs: number | null;
  anomalyReason?: 'missing_processed_on' | 'invalid_enqueue_timestamp' | 'negative_clock_skew';
}

export function calculateEnqueueToAttemptAgeMs(
  enqueueTimestamp: number,
  processedOnTimestamp: number | undefined
): number | null {
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
    warnRateLimited(
      'negative_clock_skew',
      `Negative clock skew detected: processedOn (${processedOnTimestamp}) < enqueueTimestamp (${enqueueTimestamp}). Skipping metric recording.`
    );
    return null;
  }

  return diff;
}

export function recordJobAgeAtAttempt(
  job: Job,
  jobName?: string,
  histogram: Histogram = jobAgeHistogram
): JobAgeMeasurementResult {
  if (
    job.processedOn === undefined ||
    typeof job.processedOn !== 'number' ||
    !Number.isFinite(job.processedOn)
  ) {
    warnRateLimited(
      'missing_processed_on',
      `Skipped job_age_at_attempt_ms observation because job.processedOn is missing or invalid (jobId=${job.id ?? 'unknown'}).`
    );
    return { recorded: false, ageMs: null, anomalyReason: 'missing_processed_on' };
  }

  let ageMs: number | null = null;
  try {
    ageMs = calculateEnqueueToAttemptAgeMs(job.timestamp, job.processedOn);
  } catch (err) {
    warnRateLimited(
      'invalid_enqueue_timestamp',
      `Skipped job_age_at_attempt_ms observation due to invalid timestamp: ${err instanceof Error ? err.message : String(err)}`
    );
    return { recorded: false, ageMs: null, anomalyReason: 'invalid_enqueue_timestamp' };
  }

  if (ageMs === null) {
    return { recorded: false, ageMs: null, anomalyReason: 'negative_clock_skew' };
  }

  const rawScenario = (job.data as { scenario?: string })?.scenario ?? jobName ?? job.name;
  const validatedScenario: JobScenario | 'unknown' = ALLOWED_SCENARIOS.includes(
    rawScenario as JobScenario
  )
    ? (rawScenario as JobScenario)
    : 'unknown';

  histogram.record(ageMs, {
    'bullmq.queue.name': job.queueName,
    'bullmq.job.name': validatedScenario,
    'bullmq.job.attempt_type': job.attemptsMade === 0 ? 'first_attempt' : 'retry_attempt',
  });

  return { recorded: true, ageMs };
}
