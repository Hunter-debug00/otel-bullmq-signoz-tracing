import { app } from './app';
import { getDiagnosticsQueue, closeQueueResources } from './queue';
import { shutdownTelemetry } from './instrumentation';

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '127.0.0.1';

const metricsInterval = setInterval(async () => {
  try {
    const queue = getDiagnosticsQueue();
    await queue.recordJobCountsMetric();
  } catch (err) {
    console.error('[Metrics] Failed to record queue job counts:', err);
  }
}, 5000);
metricsInterval.unref();

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
      await closeQueueResources();
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

export { server };
