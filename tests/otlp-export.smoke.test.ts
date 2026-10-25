import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { AddressInfo } from 'node:net';
import { trace, metrics } from '@opentelemetry/api';
import { createTelemetrySdk } from '../src/instrumentation';

interface CapturedRequest {
  url: string;
  headers: http.IncomingHttpHeaders;
  body: string;
}

describe('OTLP HTTP Export Smoke Test (Production Pipeline)', () => {
  let server: http.Server;
  let baseUrl: string;
  let capturedRequests: CapturedRequest[] = [];

  beforeEach(async () => {
    capturedRequests = [];
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
      req.on('end', () => {
        capturedRequests.push({
          url: req.url ?? '',
          headers: req.headers,
          body: Buffer.concat(chunks).toString('utf8'),
        });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ partialSuccess: {} }));
      });
    });

    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${port}/`;
  });

  afterEach(async () => {
    await new Promise<void>((resolve, reject) =>
      server.close((err) => (err ? reject(err) : resolve()))
    );
  });

  it('exports spans to /v1/traces and metrics to /v1/metrics on SDK shutdown', async () => {
    trace.disable();
    metrics.disable();

    const sdk = createTelemetrySdk({
      serviceName: 'smoke-test-worker',
      otlpEndpoint: baseUrl,
      otlpHeaders: { 'signoz-ingestion-key': 'test-secret-key' },
      metricExportIntervalMillis: 60_000,
    });

    sdk.start();

    const tracer = trace.getTracer('smoke-test-tracer');
    const span = tracer.startSpan('smoke.operation', {
      attributes: { 'bullmq.job.name': 'fast-success' },
    });
    span.end();

    const meter = metrics.getMeter('smoke-test-meter');
    const histogram = meter.createHistogram('job_age_at_attempt_ms');
    histogram.record(142, { 'bullmq.job.name': 'fast-success' });

    await sdk.shutdown();

    const traceRequests = capturedRequests.filter((r) => r.url === '/v1/traces');
    const metricRequests = capturedRequests.filter((r) => r.url === '/v1/metrics');

    assert.ok(traceRequests.length >= 1, 'Expected at least one /v1/traces request');
    assert.equal(traceRequests[0].headers['signoz-ingestion-key'], 'test-secret-key');
    assert.ok(traceRequests[0].body.includes('smoke.operation'));
    assert.ok(traceRequests[0].body.includes('smoke-test-worker'));
    assert.ok(traceRequests[0].body.includes('fast-success'));

    assert.ok(metricRequests.length >= 1, 'Expected at least one /v1/metrics request');
    assert.ok(metricRequests[0].body.includes('job_age_at_attempt_ms'));
  });
});
