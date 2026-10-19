import express, { Request, Response } from 'express';
import { diagnosticsQueue } from './queue';
import { JobScenario } from './jobs/processor';
import { sdk } from './instrumentation';

const app = express();
app.use(express.json());

app.post('/jobs', async (req: Request, res: Response) => {
  const {
    runId = `run_${Date.now()}`,
    scenario = 'success',
    count = 1,
    backoffDelayMs = 1500,
    simulatedWorkMs,
  } = req.body as {
    runId?: string;
    scenario?: JobScenario;
    count?: number;
    backoffDelayMs?: number;
    simulatedWorkMs?: number;
  };

  const createdJobIds: string[] = [];

  for (let i = 0; i < count; i++) {
    const job = await diagnosticsQueue.add(
      scenario,
      { runId, scenario, simulatedWorkMs },
      {
        attempts: 3,
        backoff: { type: 'exponential', delay: backoffDelayMs },
      }
    );
    if (job.id) createdJobIds.push(job.id);
  }

  console.log(`[API] Enqueued ${createdJobIds.length} job(s) for scenario=${scenario} (runId=${runId})`);
  return res.status(202).json({
    status: 'enqueued',
    runId,
    scenario,
    count: createdJobIds.length,
    jobIds: createdJobIds,
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
const server = app.listen(PORT, () => {
  console.log(`[API] Diagnostics API listening on http://localhost:${PORT}`);
});

async function shutdown(signal: string) {
  console.log(`[API] Received ${signal}, shutting down gracefully...`);
  clearInterval(metricsInterval);
  await new Promise<void>((resolve) => server.close(() => resolve()));
  await diagnosticsQueue.close();
  await sdk.shutdown();
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

