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

export const queueConnection = new Proxy({} as IORedis, {
  get(_target, prop) {
    const conn = getQueueConnection() as any;
    const value = conn[prop];
    if (typeof value === 'function') {
      return value.bind(conn);
    }
    return value;
  },
});

export const diagnosticsQueue = new Proxy({} as Queue<DiagnosticJobData>, {
  get(_target, prop) {
    const queue = getDiagnosticsQueue() as any;
    const value = queue[prop];
    if (typeof value === 'function') {
      return value.bind(queue);
    }
    return value;
  },
});
