import express, { Express, Request, Response, NextFunction } from 'express';
import { getDiagnosticsQueue } from './queue';
import { validateEnqueueRequest } from './validation';

export function isQueueUnavailableError(err: unknown): boolean {
  if (err instanceof Error) {
    if ('code' in err && (err as { code?: unknown }).code === 'ECONNREFUSED') {
      return true;
    }
    if (
      err.name === 'MaxRetriesPerRequestError' ||
      err.name === 'ClusterError' ||
      err.name === 'RedisError'
    ) {
      return true;
    }
  }
  const msg = err instanceof Error ? err.message : String(err);
  return msg.includes('ECONNREFUSED') || msg.includes('Redis') || msg.includes('Connection is closed');
}

export function createApp(): Express {
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

      const queue = getDiagnosticsQueue();
      const jobs = await queue.addBulk(jobPayloads);
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

    if (isQueueUnavailableError(err)) {
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

  return app;
}

export const app = createApp();
