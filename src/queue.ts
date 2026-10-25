import { Queue } from 'bullmq';
import { BullMQOtel } from 'bullmq-otel';
import IORedis from 'ioredis';
import { DiagnosticJobData } from './validation';

export const QUEUE_NAME = 'diagnostics-job-queue';

export function createRedisConnection(): IORedis {
  return new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
    // BullMQ requires maxRetriesPerRequest to be null for blocking commands
    maxRetriesPerRequest: null,
  });
}

export function createBullMQTelemetry() {
  return new BullMQOtel({
    tracerName: 'bullmq-diagnostics',
    meterName: 'bullmq-diagnostics',
    version: '1.0.0',
    enableMetrics: true,
  });
}

let _queueConnection: IORedis | null = null;
export function getQueueConnection(): IORedis {
  if (!_queueConnection) {
    _queueConnection = createRedisConnection();
  }
  return _queueConnection;
}

let _diagnosticsQueue: Queue<DiagnosticJobData> | null = null;
export function getDiagnosticsQueue(): Queue<DiagnosticJobData> {
  if (!_diagnosticsQueue) {
    _diagnosticsQueue = new Queue<DiagnosticJobData>(QUEUE_NAME, {
      connection: getQueueConnection(),
      telemetry: createBullMQTelemetry(),
    });
  }
  return _diagnosticsQueue;
}

export async function closeQueueResources(): Promise<void> {
  if (_diagnosticsQueue) {
    await _diagnosticsQueue.close();
    _diagnosticsQueue = null;
  }
  if (_queueConnection) {
    await _queueConnection.quit();
    _queueConnection = null;
  }
}
