import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { Queue, Worker, QueueEvents } from 'bullmq';
import { createRedisConnection, createBullMQTelemetry } from '../src/queue';
import { processDiagnosticJob, DiagnosticJobData, DiagnosticJobResult } from '../src/jobs/processor';
import { sdk } from '../src/instrumentation';

EventEmitter.defaultMaxListeners = 50;

function percentile(values: number[], p: number): number {
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
) {
  const queueConn = createRedisConnection();
  const eventsConn = createRedisConnection();
  const workerConn = createRedisConnection();

  const queue = new Queue<DiagnosticJobData, DiagnosticJobResult>(queueName, {
    connection: queueConn,
    telemetry: createBullMQTelemetry(),
  });
  const queueEvents = new QueueEvents(queueName, { connection: eventsConn });

  await queue.obliterate({ force: true });

  const worker = new Worker<DiagnosticJobData, DiagnosticJobResult>(
    queueName,
    async (job) => processDiagnosticJob(job),
    {
      connection: workerConn,
      telemetry: createBullMQTelemetry(),
      concurrency,
    }
  );
  await worker.waitUntilReady();

  const runId = `${condition}_trial_${trialNumber}_${Date.now()}`;
  const enqueueStart = performance.now();

  const jobs = await Promise.all(
    Array.from({ length: jobCount }, (_, i) =>
      queue.add('success', {
        runId: `${runId}_job_${i + 1}`,
        scenario: 'success',
        simulatedWorkMs: workMs,
      })
    )
  );
  const enqueueTotalMs = Number((performance.now() - enqueueStart).toFixed(2));

  const results = await Promise.all(jobs.map((j) => j.waitUntilFinished(queueEvents, 15000)));

  const jobAges = results.map((r) => r.jobAgeMs);
  const durations = results.map((r) => r.executionDurationMs);

  await worker.close();
  await queueEvents.close();
  await queue.close();
  await Promise.all([queueConn.quit(), eventsConn.quit(), workerConn.quit()]);

  return {
    condition,
    queueName,
    concurrency,
    trialNumber,
    jobsSubmitted: jobCount,
    jobsCompleted: results.length,
    enqueueBatchLatencyMs: enqueueTotalMs,
    jobAgeP50Ms: percentile(jobAges, 50),
    jobAgeP95Ms: percentile(jobAges, 95),
    durationP50Ms: percentile(durations, 50),
    durationP95Ms: percentile(durations, 95),
  };
}

async function main() {
  console.log('Running Isolated Controlled Experiment (5 Trials Baseline c=5 vs 5 Trials Constrained c=1)...\n');
  const baselineRows = [];
  const constrainedRows = [];

  for (let t = 1; t <= 5; t++) {
    const baseRes = await runSingleTrial('baseline_concurrency_5', 'diagnostics-baseline-c5', 5, t);
    baselineRows.push(baseRes);
    console.log(
      `[Baseline Trial ${t}]    completed=${baseRes.jobsCompleted}/20 | ageP50=${baseRes.jobAgeP50Ms}ms | ageP95=${baseRes.jobAgeP95Ms}ms | durP50=${baseRes.durationP50Ms}ms | durP95=${baseRes.durationP95Ms}ms`
    );

    const consRes = await runSingleTrial('constrained_concurrency_1', 'diagnostics-constrained-c1', 1, t);
    constrainedRows.push(consRes);
    console.log(
      `[Constrained Trial ${t}] completed=${consRes.jobsCompleted}/20 | ageP50=${consRes.jobAgeP50Ms}ms | ageP95=${consRes.jobAgeP95Ms}ms | durP50=${consRes.durationP50Ms}ms | durP95=${consRes.durationP95Ms}ms`
    );

    if (t < 5) {
      await new Promise((r) => setTimeout(r, 10000));
    }
  }

  const rows = [...baselineRows, ...constrainedRows];

  fs.mkdirSync(path.join(process.cwd(), 'results'), { recursive: true });
  const csvHeader =
    'condition,queue_name,concurrency,trial,jobs_submitted,jobs_completed,enqueue_batch_ms,job_age_p50_ms,job_age_p95_ms,duration_p50_ms,duration_p95_ms\n';
  const csvBody = rows
    .map(
      (r) =>
        `${r.condition},${r.queueName},${r.concurrency},${r.trialNumber},${r.jobsSubmitted},${r.jobsCompleted},${r.enqueueBatchLatencyMs},${r.jobAgeP50Ms},${r.jobAgeP95Ms},${r.durationP50Ms},${r.durationP95Ms}`
    )
    .join('\n');

  const csvPath = path.join(process.cwd(), 'results', 'experiment-results.csv');
  fs.writeFileSync(csvPath, csvHeader + csvBody, 'utf8');
  console.log(`\n[OK] Saved reproducible CSV dataset to: ${csvPath}`);
  await sdk.shutdown();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
