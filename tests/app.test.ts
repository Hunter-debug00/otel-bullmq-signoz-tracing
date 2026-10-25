import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { AddressInfo } from 'node:net';
import { app, isQueueUnavailableError } from '../src/app';
import { closeQueueResources } from '../src/queue';

describe('isQueueUnavailableError', () => {
  it('identifies ECONNREFUSED error codes', () => {
    const err = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:6379'), {
      code: 'ECONNREFUSED',
    });
    assert.equal(isQueueUnavailableError(err), true);
  });

  it('identifies MaxRetriesPerRequestError and ClusterError by name', () => {
    const err1 = new Error('Reached max retries');
    err1.name = 'MaxRetriesPerRequestError';
    assert.equal(isQueueUnavailableError(err1), true);

    const err2 = new Error('Cluster is down');
    err2.name = 'ClusterError';
    assert.equal(isQueueUnavailableError(err2), true);
  });

  it('identifies Redis connection errors from message text fallback', () => {
    assert.equal(isQueueUnavailableError(new Error('Connection is closed')), true);
    assert.equal(isQueueUnavailableError(new Error('Redis connection lost')), true);
  });

  it('returns false for unrelated application errors', () => {
    assert.equal(isQueueUnavailableError(new TypeError('Cannot read properties of undefined')), false);
    assert.equal(isQueueUnavailableError(new Error('Syntax error')), false);
  });
});

describe('Express HTTP App Routes (src/app.ts)', () => {
  let server: http.Server;
  let baseUrl: string;

  before(async () => {
    server = http.createServer(app);
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  after(async () => {
    await new Promise<void>((resolve, reject) =>
      server.close((err) => (err ? reject(err) : resolve()))
    );
    await closeQueueResources();
  });

  it('POST /jobs accepts valid enqueue requests and returns 202 with job IDs', async () => {
    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        scenario: 'success',
        count: 2,
        simulatedWorkMs: 50,
      }),
    });

    assert.equal(res.status, 202);
    const body = await res.json();
    assert.equal(body.status, 'enqueued');
    assert.equal(body.scenario, 'success');
    assert.equal(body.count, 2);
    assert.ok(Array.isArray(body.jobIds));
    assert.equal(body.jobIds.length, 2);
  });

  it('POST /jobs rejects invalid payload with 400 and structured error code', async () => {
    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        scenario: 'invalid-scenario',
        count: 10,
      }),
    });

    assert.equal(res.status, 400);
    const body = await res.json();
    assert.equal(body.code, 'INVALID_SCENARIO');
    assert.ok(typeof body.error === 'string');
  });

  it('POST /jobs returns 400 MALFORMED_JSON on invalid JSON syntax', async () => {
    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{"scenario": "success", count: invalid_json}',
    });

    assert.equal(res.status, 400);
    const body = await res.json();
    assert.equal(body.code, 'MALFORMED_JSON');
    assert.equal(body.error, 'Malformed JSON payload');
  });

  it('POST /jobs returns 413 PAYLOAD_TOO_LARGE when body exceeds size limit', async () => {
    const largePayload = JSON.stringify({
      scenario: 'success',
      padding: 'x'.repeat(20 * 1024),
    });

    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: largePayload,
    });

    assert.equal(res.status, 413);
    const body = await res.json();
    assert.equal(body.code, 'PAYLOAD_TOO_LARGE');
    assert.equal(body.error, 'Payload too large');
  });
});
