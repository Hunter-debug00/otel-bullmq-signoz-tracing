import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { Queue, Worker, QueueEvents } from 'bullmq';
import { createRedisConnection, createBullMQTelemetry } from '../src/queue';
import {
  processDiagnosticJob,
  DiagnosticJobData,
  DiagnosticJobResult,
} from '../src/jobs/processor';
import { shutdownTelemetry } from '../src/instrumentation';

interface JobObservation {
  condition: string;
  queue_name: string;
  concurrency: number;
  trial: number;
  job_id: string;
  attempt_number: number;
  enqueue_to_attempt_age_ms: number;
  processor_elapsed_ms: number;
}

interface TrialSummary {
  condition: string;
  queue_name: string;
  concurrency: number;
  trial: number;
  jobs_submitted: number;
  jobs_completed: number;
  enqueue_batch_ms: number;
  enqueue_to_attempt_age_p50_ms: number;
  enqueue_to_attempt_age_p95_ms: number;
  processor_elapsed_p50_ms: number;
  processor_elapsed_p95_ms: number;
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return Number(sorted[Math.max(0, idx)].toFixed(2));
}

async function runSingleTrial(
  condition: 'baseline_concurrency_5' | 'constrained_concurrency_1',
  queueName: string,
  concurrency: number,
  trialNumber: number,
  jobCount = 20,
  workMs = 100
): Promise<{ summary: TrialSummary; observations: JobObservation[] }> {
  const connections = {
    queue: createRedisConnection(),
    events: createRedisConnection(),
    worker: createRedisConnection(),
  };

  let queue: Queue<DiagnosticJobData, DiagnosticJobResult> | undefined;
  let queueEvents: QueueEvents | undefined;
  let worker: Worker<DiagnosticJobData, DiagnosticJobResult> | undefined;

  try {
    queue = new Queue<DiagnosticJobData, DiagnosticJobResult>(queueName, {
      connection: connections.queue,
      telemetry: createBullMQTelemetry(),
    });
    queue.setMaxListeners(jobCount * 2 + 10);

    queueEvents = new QueueEvents(queueName, {
      connection: connections.events,
    });
    // Set max listeners on the specific QueueEvents emitter per trial to avoid warnings
    queueEvents.setMaxListeners(jobCount * 2 + 10);

    worker = new Worker<DiagnosticJobData, DiagnosticJobResult>(
      queueName,
      async (job) => processDiagnosticJob(job),
      {
        connection: connections.worker,
        telemetry: createBullMQTelemetry(),
        concurrency,
      }
    );

    await Promise.all([
      queue.waitUntilReady(),
      queueEvents.waitUntilReady(),
      worker.waitUntilReady(),
    ]);

    await worker.pause();

    const runId = `${condition}_trial_${trialNumber}_${Date.now()}`;
    const jobPayloads = Array.from({ length: jobCount }, (_, i) => ({
      name: 'success',
      data: {
        runId: `${runId}_job_${i + 1}`,
        scenario: 'success' as const,
        simulatedWorkMs: workMs,
      },
      opts: {
        attempts: 3,
      },
    }));

    const enqueueStart = performance.now();
    const jobs = await queue.addBulk(jobPayloads);
    const enqueueBatchMs = Number((performance.now() - enqueueStart).toFixed(2));

    const completionPromises = jobs.map(async (job, idx) => {
      const result = await job.waitUntilFinished(queueEvents!, 15000);
      return {
        jobId: job.id ?? `job_${idx + 1}`,
        result,
      };
    });

    await worker.resume();
    const completedResults = await Promise.all(completionPromises);

    const observations: JobObservation[] = completedResults.map((c) => {
      const ageMs = c.result.enqueueToAttemptAgeMs;
      if (ageMs === null || !Number.isFinite(ageMs) || ageMs < 0) {
        throw new Error(
          `Invalid enqueue-to-attempt age for job ${c.jobId}; refusing to record a synthetic measurement.`
        );
      }
      return {
        condition,
        queue_name: queueName,
        concurrency,
        trial: trialNumber,
        job_id: c.jobId,
        attempt_number: c.result.attemptNumber,
        enqueue_to_attempt_age_ms: ageMs,
        processor_elapsed_ms: c.result.processorElapsedMs,
      };
    });

    const ageValues = observations.map((o) => o.enqueue_to_attempt_age_ms);
    const elapsedValues = observations.map((o) => o.processor_elapsed_ms);

    const summary: TrialSummary = {
      condition,
      queue_name: queueName,
      concurrency,
      trial: trialNumber,
      jobs_submitted: jobCount,
      jobs_completed: observations.length,
      enqueue_batch_ms: enqueueBatchMs,
      enqueue_to_attempt_age_p50_ms: percentile(ageValues, 50),
      enqueue_to_attempt_age_p95_ms: percentile(ageValues, 95),
      processor_elapsed_p50_ms: percentile(elapsedValues, 50),
      processor_elapsed_p95_ms: percentile(elapsedValues, 95),
    };

    return { summary, observations };
  } finally {
    await Promise.allSettled([
      worker?.close(),
      queueEvents?.close(),
    ].filter(Boolean) as Promise<unknown>[]);

    if (queue) {
      try {
        await queue.obliterate({ force: true });
      } catch (err) {
        console.warn(`[Experiment] Warning: Obliterate failed on ${queueName}:`, err);
      }
      await Promise.allSettled([queue.close()]);
    }

    await Promise.allSettled([
      connections.queue.quit(),
      connections.events.quit(),
      connections.worker.quit(),
    ]);
  }
}

async function main() {
  console.log('Running Isolated Controlled Synchronized Burst Experiment');
  console.log('5 Trials Baseline (c=5) vs 5 Trials Constrained (c=1), alternating order\n');

  const allObservations: JobObservation[] = [];
  const allSummaries: TrialSummary[] = [];

  const totalTrials = 5;

  for (let t = 1; t <= totalTrials; t++) {
    const padTrial = String(t).padStart(2, '0');

    // Alternate order between c=5 and c=1 per trial
    if (t % 2 === 1) {
      // Odd trials: run c=5 first, then c=1
      const q5Name = `diag-c5-trial-${padTrial}`;
      console.log(`[Trial ${t}/5] Running baseline c=5 on ${q5Name}...`);
      const res5 = await runSingleTrial('baseline_concurrency_5', q5Name, 5, t);
      allSummaries.push(res5.summary);
      allObservations.push(...res5.observations);
      console.log(
        `  -> Completed ${res5.summary.jobs_completed}/${res5.summary.jobs_submitted} | batchEnqueue=${res5.summary.enqueue_batch_ms}ms | ageP50=${res5.summary.enqueue_to_attempt_age_p50_ms}ms | ageP95=${res5.summary.enqueue_to_attempt_age_p95_ms}ms | procP50=${res5.summary.processor_elapsed_p50_ms}ms`
      );

      const q1Name = `diag-c1-trial-${padTrial}`;
      console.log(`[Trial ${t}/5] Running constrained c=1 on ${q1Name}...`);
      const res1 = await runSingleTrial('constrained_concurrency_1', q1Name, 1, t);
      allSummaries.push(res1.summary);
      allObservations.push(...res1.observations);
      console.log(
        `  -> Completed ${res1.summary.jobs_completed}/${res1.summary.jobs_submitted} | batchEnqueue=${res1.summary.enqueue_batch_ms}ms | ageP50=${res1.summary.enqueue_to_attempt_age_p50_ms}ms | ageP95=${res1.summary.enqueue_to_attempt_age_p95_ms}ms | procP50=${res1.summary.processor_elapsed_p50_ms}ms`
      );
    } else {
      // Even trials: run c=1 first, then c=5
      const q1Name = `diag-c1-trial-${padTrial}`;
      console.log(`[Trial ${t}/5] Running constrained c=1 on ${q1Name}...`);
      const res1 = await runSingleTrial('constrained_concurrency_1', q1Name, 1, t);
      allSummaries.push(res1.summary);
      allObservations.push(...res1.observations);
      console.log(
        `  -> Completed ${res1.summary.jobs_completed}/${res1.summary.jobs_submitted} | batchEnqueue=${res1.summary.enqueue_batch_ms}ms | ageP50=${res1.summary.enqueue_to_attempt_age_p50_ms}ms | ageP95=${res1.summary.enqueue_to_attempt_age_p95_ms}ms | procP50=${res1.summary.processor_elapsed_p50_ms}ms`
      );

      const q5Name = `diag-c5-trial-${padTrial}`;
      console.log(`[Trial ${t}/5] Running baseline c=5 on ${q5Name}...`);
      const res5 = await runSingleTrial('baseline_concurrency_5', q5Name, 5, t);
      allSummaries.push(res5.summary);
      allObservations.push(...res5.observations);
      console.log(
        `  -> Completed ${res5.summary.jobs_completed}/${res5.summary.jobs_submitted} | batchEnqueue=${res5.summary.enqueue_batch_ms}ms | ageP50=${res5.summary.enqueue_to_attempt_age_p50_ms}ms | ageP95=${res5.summary.enqueue_to_attempt_age_p95_ms}ms | procP50=${res5.summary.processor_elapsed_p50_ms}ms`
      );
    }

    if (t < totalTrials) {
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  const resultsDir = path.join(process.cwd(), 'results');
  fs.mkdirSync(resultsDir, { recursive: true });

  const obsHeader =
    'condition,queue_name,concurrency,trial,job_id,attempt_number,enqueue_to_attempt_age_ms,processor_elapsed_ms\n';
  const obsRows = allObservations
    .map(
      (o) =>
        `${o.condition},${o.queue_name},${o.concurrency},${o.trial},${o.job_id},${o.attempt_number},${o.enqueue_to_attempt_age_ms},${o.processor_elapsed_ms}`
    )
    .join('\n');
  const obsPath = path.join(resultsDir, 'sample-job-observations.csv');
  fs.writeFileSync(obsPath, obsHeader + obsRows + '\n', 'utf8');
  console.log(`\n[OK] Saved job observations to: ${obsPath}`);

  const summaryHeader =
    'condition,queue_name,concurrency,trial,jobs_submitted,jobs_completed,enqueue_batch_ms,enqueue_to_attempt_age_p50_ms,enqueue_to_attempt_age_p95_ms,processor_elapsed_p50_ms,processor_elapsed_p95_ms\n';
  const summaryRows = allSummaries
    .map(
      (s) =>
        `${s.condition},${s.queue_name},${s.concurrency},${s.trial},${s.jobs_submitted},${s.jobs_completed},${s.enqueue_batch_ms},${s.enqueue_to_attempt_age_p50_ms},${s.enqueue_to_attempt_age_p95_ms},${s.processor_elapsed_p50_ms},${s.processor_elapsed_p95_ms}`
    )
    .join('\n');
  const summaryPath = path.join(resultsDir, 'sample-trial-summary.csv');
  fs.writeFileSync(summaryPath, summaryHeader + summaryRows + '\n', 'utf8');
  console.log(`[OK] Saved trial summary to: ${summaryPath}`);

  function getResolvedVersion(packageName: string): string {
    try {
      const lockPath = path.join(process.cwd(), 'package-lock.json');
      if (fs.existsSync(lockPath)) {
        const lock = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
        const pkgEntry = lock.packages?.[`node_modules/${packageName}`];
        if (pkgEntry?.version) {
          return pkgEntry.version;
        }
      }
    } catch {
      // Fall through to require fallback
    }
    try {
      const pkgJsonPath = require.resolve(`${packageName}/package.json`);
      const pkg = JSON.parse(fs.readFileSync(pkgJsonPath, 'utf8'));
      if (pkg.version) return pkg.version;
    } catch {
      // Fall through to unknown
    }
    return 'unknown';
  }

  const manifest = {
    timestamp: new Date().toISOString(),
    nodeVersion: process.version,
    osArch: `${process.platform}-${process.arch}`,
    cpuCount: os.cpus().length,
    workload: {
      type: 'simulated-asynchronous-work',
      jobCountPerTrial: 20,
      simulatedWorkMs: 100,
    },
    percentileMethod: 'nearest-rank',
    dependencies: {
      bullmq: getResolvedVersion('bullmq'),
      bullmqOtel: getResolvedVersion('bullmq-otel'),
      ioredis: getResolvedVersion('ioredis'),
      otelSdkNode: getResolvedVersion('@opentelemetry/sdk-node'),
    },
  };
  const manifestPath = path.join(resultsDir, 'sample-manifest.json');
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + '\n', 'utf8');
  console.log(`[OK] Saved experiment manifest to: ${manifestPath}`);
}

async function run() {
  try {
    await main();
  } catch (err) {
    console.error('[Experiment] Error running experiment:', err);
    process.exitCode = 1;
  } finally {
    try {
      await shutdownTelemetry();
    } catch (shutdownErr) {
      console.error('[Experiment] Error during telemetry shutdown:', shutdownErr);
    }
    process.exit(process.exitCode ?? 0);
  }
}

void run();
