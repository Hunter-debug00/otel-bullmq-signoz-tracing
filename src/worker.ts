import { Worker } from 'bullmq';
import { QUEUE_NAME, createRedisConnection, createBullMQTelemetry } from './queue';
import { processDiagnosticJob, DiagnosticJobData } from './jobs/processor';
import { sdk } from './instrumentation';

const concurrency = Number(process.env.WORKER_CONCURRENCY || 5);

const worker = new Worker<DiagnosticJobData>(
  QUEUE_NAME,
  async (job) => processDiagnosticJob(job),
  {
    connection: createRedisConnection(),
    telemetry: createBullMQTelemetry(),
    concurrency,
  }
);

worker.on('failed', (job, err) => {
  console.error(
    `[Worker] Job ${job?.id} (${job?.data.scenario}) failed on attempt ${job?.attemptsMade}: ${err.message}`
  );
});

worker.on('ready', () => {
  console.log(`[Worker] Ready on queue="${QUEUE_NAME}" with concurrency=${concurrency}`);
});

async function shutdown(signal: string) {
  console.log(`[Worker] Received ${signal}, shutting down gracefully...`);
  await worker.close();
  await sdk.shutdown();
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

