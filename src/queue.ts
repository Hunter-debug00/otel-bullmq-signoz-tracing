import { Queue } from 'bullmq';
import { BullMQOtel } from 'bullmq-otel';
import IORedis from 'ioredis';
import { DiagnosticJobData } from './jobs/processor';

export const QUEUE_NAME = 'diagnostics-job-queue';

export function createRedisConnection() {
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

export const diagnosticsQueue = new Queue<DiagnosticJobData>(QUEUE_NAME, {
  connection: createRedisConnection(),
  telemetry: createBullMQTelemetry(),
});
