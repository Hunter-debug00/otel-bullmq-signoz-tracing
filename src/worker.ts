import { Worker } from 'bullmq';
import { QUEUE_NAME, createRedisConnection, createBullMQTelemetry } from './queue';
import { processDiagnosticJob, DiagnosticJobData } from './jobs/processor';
import { shutdownTelemetry } from './instrumentation';
import { validateWorkerConcurrency } from './validation';

const concurrency = validateWorkerConcurrency(process.env.WORKER_CONCURRENCY || 5);

const workerConnection = createRedisConnection();

const worker = new Worker<DiagnosticJobData>(
  QUEUE_NAME,
  async (job) => processDiagnosticJob(job),
  {
    connection: workerConnection,
    telemetry: createBullMQTelemetry(),
    concurrency,
  }
);

worker.on('error', (err) => {
  console.error(`[Worker] Internal worker/connection error: ${err.message}`);
});

worker.on('failed', (job, err) => {
  console.error(
    `[Worker] Job ${job?.id} (${job?.data.scenario}) failed on attempt ${job?.attemptsMade}: ${err.message}`
  );
});

worker.on('ready', () => {
  console.log(`[Worker] Ready on queue="${QUEUE_NAME}" with concurrency=${concurrency}`);
});

let shutdownPromise: Promise<void> | null = null;

export async function shutdown(signal = 'SIGTERM'): Promise<void> {
  if (shutdownPromise) {
    return shutdownPromise;
  }

  shutdownPromise = (async () => {
    console.log(`[Worker] Received ${signal}, shutting down gracefully...`);
    try {
      await worker.close();
      console.log('[Worker] Worker closed.');
    } catch (err) {
      console.error('[Worker] Error closing worker:', err);
    }

    try {
      await workerConnection.quit();
      console.log('[Worker] Redis connection quit.');
    } catch (err) {
      console.error('[Worker] Error quitting Redis connection:', err);
    }

    try {
      await shutdownTelemetry();
    } catch (err) {
      console.error('[Worker] Error shutting down telemetry:', err);
    }

    process.exitCode = 0;
  })();

  return shutdownPromise;
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

export { worker, workerConnection };
