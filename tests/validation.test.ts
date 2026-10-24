import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  validateEnqueueRequest,
  validateDiagnosticJobData,
  validateWorkerConcurrency,
  ALLOWED_SCENARIOS,
} from '../src/validation';

describe('validateWorkerConcurrency', () => {
  it('accepts valid integers between 1 and 50 as numbers or numeric strings', () => {
    assert.equal(validateWorkerConcurrency(1), 1);
    assert.equal(validateWorkerConcurrency(5), 5);
    assert.equal(validateWorkerConcurrency(50), 50);
    assert.equal(validateWorkerConcurrency('10'), 10);
    assert.equal(validateWorkerConcurrency(' 25 '), 25);
  });

  it('rejects numbers outside 1..50, floats, non-numeric strings, and invalid types', () => {
    assert.throws(() => validateWorkerConcurrency(0), /must be an integer between 1 and 50/);
    assert.throws(() => validateWorkerConcurrency(51), /must be an integer between 1 and 50/);
    assert.throws(() => validateWorkerConcurrency(-1), /must be an integer between 1 and 50/);
    assert.throws(() => validateWorkerConcurrency(2.5), /must be an integer between 1 and 50/);
    assert.throws(() => validateWorkerConcurrency('abc'), /must be a positive integer/);
    assert.throws(() => validateWorkerConcurrency(null), /must be a number or string/);
    assert.throws(() => validateWorkerConcurrency(undefined), /must be a number or string/);
    assert.throws(() => validateWorkerConcurrency(NaN), /must be an integer between 1 and 50/);
    assert.throws(() => validateWorkerConcurrency(Infinity), /must be an integer between 1 and 50/);
  });
});

describe('validateEnqueueRequest', () => {
  it('accepts every scenario in ALLOWED_SCENARIOS', () => {
    for (const scenario of ALLOWED_SCENARIOS) {
      const res = validateEnqueueRequest({ scenario });
      assert.equal(res.success, true);
      if (res.success) {
        assert.equal(res.data.scenario, scenario);
      }
    }
  });

  it('accepts boundary counts 1 and 100', () => {
    const res1 = validateEnqueueRequest({ count: 1 });
    assert.equal(res1.success, true);
    if (res1.success) assert.equal(res1.data.count, 1);

    const res100 = validateEnqueueRequest({ count: 100 });
    assert.equal(res100.success, true);
    if (res100.success) assert.equal(res100.data.count, 100);
  });

  it('rejects boundary counts 0, 101, negative numbers, and fractions', () => {
    const invalidCounts = [0, 101, -5, 1.5, NaN, Infinity];
    for (const count of invalidCounts) {
      const res = validateEnqueueRequest({ count });
      assert.equal(res.success, false, `Count ${count} must fail validation`);
      if (!res.success) assert.equal(res.code, 'INVALID_COUNT');
    }
  });

  it('rejects invalid backoff durations (0, negative, NaN, Infinity, > 60000)', () => {
    const invalidDelays = [0, -10, NaN, Infinity, 60001, 70000];
    for (const backoffDelayMs of invalidDelays) {
      const res = validateEnqueueRequest({ backoffDelayMs });
      assert.equal(res.success, false, `backoffDelayMs ${backoffDelayMs} must fail`);
      if (!res.success) assert.equal(res.code, 'INVALID_BACKOFF_DELAY');
    }
  });

  it('rejects invalid simulatedWorkMs (0, negative, NaN, Infinity, > 10000)', () => {
    const invalidWork = [0, -10, NaN, Infinity, 10001, 70000];
    for (const simulatedWorkMs of invalidWork) {
      const res = validateEnqueueRequest({ simulatedWorkMs });
      assert.equal(res.success, false, `simulatedWorkMs ${simulatedWorkMs} must fail`);
      if (!res.success) assert.equal(res.code, 'INVALID_SIMULATED_WORK');
    }
  });

  it('rejects control characters in runId (newlines, carriage return, null bytes, DEL)', () => {
    const invalidRunIds = [
      '',
      'a'.repeat(129),
      'line\nbreak',
      'carriage\rreturn',
      'null\x00byte',
      'control\x1fchar',
      'delete\x7fchar',
    ];
    for (const runId of invalidRunIds) {
      const res = validateEnqueueRequest({ runId });
      assert.equal(res.success, false, `runId ${JSON.stringify(runId)} must fail`);
    }
  });

  it('rejects null, array, and primitive bodies', () => {
    const primitives = [null, undefined, [], [1, 2], 'string', 123, true, Symbol('test')];
    for (const body of primitives) {
      const res = validateEnqueueRequest(body);
      assert.equal(res.success, false);
      if (!res.success) assert.equal(res.code, 'INVALID_BODY');
    }
  });
});

describe('validateDiagnosticJobData', () => {
  it('accepts valid job data for all allowed scenarios', () => {
    for (const scenario of ALLOWED_SCENARIOS) {
      const res = validateDiagnosticJobData({
        runId: `run_${scenario}`,
        scenario,
        simulatedWorkMs: 250,
      });
      assert.equal(res.success, true);
    }
  });

  it('rejects invalid scenarios, malformed data, and null/primitive bodies', () => {
    assert.equal(validateDiagnosticJobData(null).success, false);
    assert.equal(validateDiagnosticJobData([]).success, false);
    assert.equal(validateDiagnosticJobData('job-data').success, false);
    assert.equal(validateDiagnosticJobData(12345).success, false);
    assert.equal(validateDiagnosticJobData({ runId: '1', scenario: 'unknown' }).success, false);
    assert.equal(validateDiagnosticJobData({ runId: 'has\nnewline', scenario: 'success' }).success, false);
    assert.equal(validateDiagnosticJobData({ runId: '1', scenario: 'success', simulatedWorkMs: 70000 }).success, false);
    assert.equal(validateDiagnosticJobData({ runId: '1', scenario: 'success', simulatedWorkMs: NaN }).success, false);
  });
});
