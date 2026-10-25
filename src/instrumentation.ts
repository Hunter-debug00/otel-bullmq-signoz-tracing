import fs from 'node:fs';
import path from 'node:path';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import type { SpanProcessor } from '@opentelemetry/sdk-trace-base';

const envPath = path.resolve(process.cwd(), '.env');
if (fs.existsSync(envPath)) {
  try {
    process.loadEnvFile(envPath);
  } catch (err) {
    console.warn(`[OTel] Warning: Failed to load .env file from ${envPath}:`, err);
  }
}

export function normalizeOtlpBaseUrl(rawUrl?: string): string {
  const fallback = 'http://localhost:4318';
  const trimmed = rawUrl?.trim();
  if (!trimmed) return fallback;
  return trimmed.replace(/\/+$/, '');
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

export interface CreateTelemetrySdkOptions {
  serviceName?: string;
  otlpEndpoint?: string;
  otlpHeaders?: Record<string, string>;
  metricExportIntervalMillis?: number;
  /**
   * Intentional test-only override (e.g. for in-memory span assertions in tests).
   * In @opentelemetry/sdk-node 0.223.0, providing spanProcessors takes precedence
   * over traceExporter. Production startup must omit this to use the OTLP exporter.
   */
  spanProcessors?: SpanProcessor[];
}

export function createTelemetrySdk(options: CreateTelemetrySdkOptions = {}): NodeSDK {
  const serviceName =
    options.serviceName ?? process.env.OTEL_SERVICE_NAME ?? 'bullmq-telemetry-service';
  const baseUrl = normalizeOtlpBaseUrl(
    options.otlpEndpoint ?? process.env.OTEL_EXPORTER_OTLP_ENDPOINT
  );
  const headers =
    options.otlpHeaders ?? parseOtlpHeaders(process.env.OTEL_EXPORTER_OTLP_HEADERS);

  const traceExporter = new OTLPTraceExporter({
    url: `${baseUrl}/v1/traces`,
    headers,
  });

  const metricExporter = new OTLPMetricExporter({
    url: `${baseUrl}/v1/metrics`,
    headers,
  });

  const metricReader = new PeriodicExportingMetricReader({
    exporter: metricExporter,
    exportIntervalMillis: options.metricExportIntervalMillis ?? 5000,
  });

  return new NodeSDK({
    serviceName,
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: serviceName,
    }),
    traceExporter,
    ...(options.spanProcessors ? { spanProcessors: options.spanProcessors } : {}),
    metricReader,
    instrumentations: [
      getNodeAutoInstrumentations({
        '@opentelemetry/instrumentation-fs': { enabled: false },
      }),
    ],
  });
}

let defaultSdk: NodeSDK | null = null;
export function getDefaultSdk(): NodeSDK {
  if (!defaultSdk) {
    defaultSdk = createTelemetrySdk();
  }
  return defaultSdk;
}

let isStarted = false;
let shutdownPromise: Promise<void> | null = null;

export function startTelemetry(customSdk?: NodeSDK): void {
  if (isStarted) return;
  const sdk = customSdk ?? getDefaultSdk();
  sdk.start();
  isStarted = true;
  const serviceName = process.env.OTEL_SERVICE_NAME || 'bullmq-diagnostics';
  console.log(`[OTel] Traces & Metrics active for service: ${serviceName}`);
}

export async function shutdownTelemetry(customSdk?: NodeSDK): Promise<void> {
  if (!isStarted) {
    return;
  }
  if (shutdownPromise) {
    return shutdownPromise;
  }
  const sdk = customSdk ?? getDefaultSdk();
  const serviceName = process.env.OTEL_SERVICE_NAME || 'bullmq-diagnostics';
  const otlpBaseUrl = normalizeOtlpBaseUrl(process.env.OTEL_EXPORTER_OTLP_ENDPOINT);
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
      shutdownPromise = null;
    }
  })();
  return shutdownPromise;
}
