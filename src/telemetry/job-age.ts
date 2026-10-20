import { metrics } from '@opentelemetry/api';
import { Job } from 'bullmq';

const meter = metrics.getMeter('bullmq-diagnostics', '1.0.0');

export const JOB_AGE_BUCKET_BOUNDARIES = [
  5, 10, 25, 50, 100, 150, 250, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000,
];

const jobAgeHistogram = meter.createHistogram('job_age_at_attempt_ms', {
  description: 'Time elapsed (ms) from initial job enqueue to the start of the current processing attempt',
  unit: 'ms',
  advice: {
    explicitBucketBoundaries: JOB_AGE_BUCKET_BOUNDARIES,
  },
});

export function recordJobAgeAtAttempt(job: Job, jobName: string): number {
  const now = job.processedOn ?? Date.now();
  const jobAgeMs = Math.max(0, now - job.timestamp);

  jobAgeHistogram.record(jobAgeMs, {
    'bullmq.queue.name': job.queueName,
    'bullmq.job.name': jobName,
    'bullmq.job.attempt_type': job.attemptsMade === 0 ? 'first_attempt' : 'retry_attempt',
  });

  return jobAgeMs;
}
