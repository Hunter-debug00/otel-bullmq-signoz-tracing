import fs from 'node:fs';
import path from 'node:path';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import { SimpleSpanProcessor, InMemorySpanExporter } from '@opentelemetry/sdk-trace-base';

// Load .env into process.env before configuring OTel or services
const envPath = path.resolve(process.cwd(), '.env');
if (fs.existsSync(envPath)) {
  try {
    process.loadEnvFile(envPath);
  } catch (err) {
    console.warn(`[OTel] Warning: Failed to load .env file from ${envPath}:`, err);
  }
}

export function parseOtlpHeaders(rawHeaders?: string): Record<string, string> {
  if (!rawHeaders || typeof rawHeaders !== 'string') {
    return {};
  }
  const headers: Record<string, string> = {};
  for (const part of rawHeaders.split(',')) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx > 0) {
      const key = trimmed.slice(0, eqIdx).trim();
      const value = trimmed.slice(eqIdx + 1).trim();
      if (key) {
        headers[key] = value;
      }
    }
  }
  return headers;
}

const serviceName = process.env.OTEL_SERVICE_NAME || 'bullmq-diagnostics';
const otlpBaseUrl = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318';
const otlpHeaders = parseOtlpHeaders(process.env.OTEL_EXPORTER_OTLP_HEADERS);

const traceExporter = new OTLPTraceExporter({
  url: `${otlpBaseUrl}/v1/traces`,
  headers: otlpHeaders,
});

const metricExporter = new OTLPMetricExporter({
  url: `${otlpBaseUrl}/v1/metrics`,
  headers: otlpHeaders,
});

const metricReader = new PeriodicExportingMetricReader({
  exporter: metricExporter,
  exportIntervalMillis: 5000,
});

export const inMemorySpanExporter = new InMemorySpanExporter();
export const spanProcessor = new SimpleSpanProcessor(inMemorySpanExporter);

export const sdk = new NodeSDK({
  resource: resourceFromAttributes({
    [ATTR_SERVICE_NAME]: serviceName,
  }),
  traceExporter,
  metricReader,
  spanProcessors: [spanProcessor],
  instrumentations: [
    getNodeAutoInstrumentations({
      '@opentelemetry/instrumentation-fs': { enabled: false },
    }),
  ],
});

let isStarted = false;
let shutdownPromise: Promise<void> | null = null;

export function startTelemetry(): void {
  if (isStarted) return;
  sdk.start();
  isStarted = true;
  console.log(`[OTel] Traces & Metrics active for service: ${serviceName}`);
}

export async function shutdownTelemetry(): Promise<void> {
  if (!isStarted) {
    return;
  }
  if (shutdownPromise) {
    return shutdownPromise;
  }
  shutdownPromise = (async () => {
    try {
      await sdk.shutdown();
      console.log(`[OTel] Telemetry shut down successfully for service: ${serviceName}`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes('ECONNREFUSED')) {
        console.warn(`[OTel] Collector unreachable at ${otlpBaseUrl}. Telemetry shut down.`);
      } else {
        console.error(`[OTel] Error shutting down telemetry:`, err);
      }
    } finally {
      isStarted = false;
    }
  })();
  return shutdownPromise;
}

// Auto-start when imported as an instrumentation preload
startTelemetry();
