import express, { Request, Response, NextFunction } from 'express';
import { diagnosticsQueue, queueConnection } from './queue';
import { validateEnqueueRequest } from './validation';
import { shutdownTelemetry } from './instrumentation';

const app = express();
app.use(express.json({ limit: '16kb' }));

app.post('/jobs', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const validation = validateEnqueueRequest(req.body);
    if (!validation.success) {
      return res.status(400).json({
        error: validation.error,
        code: validation.code,
      });
    }

    const { runId, scenario, count, backoffDelayMs, simulatedWorkMs } = validation.data;

    const jobPayloads = Array.from({ length: count }, (_, i) => ({
      name: scenario,
      data: {
        runId: count > 1 ? `${runId}_job_${i + 1}` : runId,
        scenario,
        simulatedWorkMs,
      },
      opts: {
        attempts: 3,
        backoff: { type: 'exponential', delay: backoffDelayMs },
      },
    }));

    // Atomically enqueue the batch using addBulk
    const jobs = await diagnosticsQueue.addBulk(jobPayloads);
    const createdJobIds = jobs
      .map((job) => job.id)
      .filter((id): id is string => Boolean(id));

    console.log(
      `[API] Enqueued ${createdJobIds.length} job(s) for scenario=${scenario} (runId=${runId})`
    );

    return res.status(202).json({
      status: 'enqueued',
      runId,
      scenario,
      count: createdJobIds.length,
      jobIds: createdJobIds,
    });
  } catch (err) {
    return next(err);
  }
});

// Express error middleware
app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
  if (
    err instanceof SyntaxError &&
    'status' in err &&
    (err as { status?: unknown }).status === 400 &&
    'body' in err
  ) {
    return res.status(400).json({
      error: 'Malformed JSON payload',
      code: 'MALFORMED_JSON',
    });
  }

  const errMessage = err instanceof Error ? err.message : String(err);
  console.error('[API] Server error occurred:', errMessage);

  const isRedisError =
    errMessage.includes('ECONNREFUSED') ||
    errMessage.includes('Redis') ||
    (err instanceof Error && err.name === 'MaxRetriesPerRequestError');

  if (isRedisError) {
    return res.status(503).json({
      error: 'Queue service unavailable',
      code: 'SERVICE_UNAVAILABLE',
    });
  }

  return res.status(500).json({
    error: 'Internal server error',
    code: 'INTERNAL_SERVER_ERROR',
  });
});

const metricsInterval = setInterval(async () => {
  try {
    await diagnosticsQueue.recordJobCountsMetric();
  } catch (err) {
    console.error('[Metrics] Failed to record queue job counts:', err);
  }
}, 5000);
metricsInterval.unref();

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';

const server = app.listen(PORT, HOST, () => {
  console.log(`[API] Diagnostics API listening on http://${HOST}:${PORT}`);
});

let shutdownPromise: Promise<void> | null = null;

export async function shutdown(signal = 'SIGTERM'): Promise<void> {
  if (shutdownPromise) {
    return shutdownPromise;
  }

  shutdownPromise = (async () => {
    console.log(`[API] Received ${signal}, shutting down gracefully...`);
    clearInterval(metricsInterval);

    try {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => {
          if (err) return reject(err);
          resolve();
        });
      });
      console.log('[API] HTTP server closed.');
    } catch (err) {
      console.error('[API] Error closing HTTP server:', err);
    }

    try {
      await diagnosticsQueue.close();
      await queueConnection.quit();
      console.log('[API] Diagnostics queue and Redis connection closed.');
    } catch (err) {
      console.error('[API] Error closing queue connection:', err);
    }

    try {
      await shutdownTelemetry();
    } catch (err) {
      console.error('[API] Error shutting down telemetry:', err);
    }

    process.exitCode = 0;
  })();

  return shutdownPromise;
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

export { app, server };
