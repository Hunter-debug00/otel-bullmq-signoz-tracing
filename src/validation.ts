export const ALLOWED_SCENARIOS = [
  'success',
  'transient-retry',
  'exhausted-failure',
  'slow-worker',
] as const;

export type JobScenario = (typeof ALLOWED_SCENARIOS)[number];

export interface EnqueueRequestData {
  runId: string;
  scenario: JobScenario;
  count: number;
  backoffDelayMs: number;
  simulatedWorkMs?: number;
}

export interface DiagnosticJobData {
  runId: string;
  scenario: JobScenario;
  simulatedWorkMs?: number;
}

export type ValidationResult<T> =
  | { success: true; data: T }
  | { success: false; error: string; code: string };

function isPlainObject(val: unknown): val is Record<string, unknown> {
  return typeof val === 'object' && val !== null && !Array.isArray(val);
}

const CONTROL_CHARS_REGEX = /[\x00-\x1F\x7F]/;

export function validateWorkerConcurrency(raw: unknown): number {
  let num: number;
  if (typeof raw === 'number') {
    num = raw;
  } else if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (!/^\d+$/.test(trimmed)) {
      throw new Error(`WORKER_CONCURRENCY must be a positive integer, received: "${raw}"`);
    }
    num = Number(trimmed);
  } else {
    throw new Error(`WORKER_CONCURRENCY must be a number or string, received: ${typeof raw}`);
  }

  if (!Number.isInteger(num) || num < 1 || num > 50) {
    throw new Error(`WORKER_CONCURRENCY must be an integer between 1 and 50, received: ${num}`);
  }

  return num;
}

export function validateEnqueueRequest(body: unknown): ValidationResult<EnqueueRequestData> {
  if (!isPlainObject(body)) {
    return {
      success: false,
      error: 'Request body must be a JSON object',
      code: 'INVALID_BODY',
    };
  }

  const scenario = body.scenario !== undefined ? body.scenario : 'success';
  if (typeof scenario !== 'string' || !ALLOWED_SCENARIOS.includes(scenario as JobScenario)) {
    return {
      success: false,
      error: `Invalid scenario "${String(scenario)}". Allowed: ${ALLOWED_SCENARIOS.join(', ')}`,
      code: 'INVALID_SCENARIO',
    };
  }

  const count = body.count !== undefined ? body.count : 1;
  if (typeof count !== 'number' || !Number.isInteger(count) || count < 1 || count > 100) {
    return {
      success: false,
      error: 'count must be an integer between 1 and 100',
      code: 'INVALID_COUNT',
    };
  }

  const backoffDelayMs = body.backoffDelayMs !== undefined ? body.backoffDelayMs : 1500;
  if (
    typeof backoffDelayMs !== 'number' ||
    !Number.isInteger(backoffDelayMs) ||
    backoffDelayMs < 1 ||
    backoffDelayMs > 60000
  ) {
    return {
      success: false,
      error: 'backoffDelayMs must be an integer between 1 and 60000',
      code: 'INVALID_BACKOFF_DELAY',
    };
  }

  let simulatedWorkMs: number | undefined;
  if (body.simulatedWorkMs !== undefined) {
    if (
      typeof body.simulatedWorkMs !== 'number' ||
      !Number.isInteger(body.simulatedWorkMs) ||
      body.simulatedWorkMs < 1 ||
      body.simulatedWorkMs > 10000
    ) {
      return {
        success: false,
        error: 'simulatedWorkMs must be an integer between 1 and 10000 if provided',
        code: 'INVALID_SIMULATED_WORK',
      };
    }
    simulatedWorkMs = body.simulatedWorkMs;
  }

  const runId = body.runId !== undefined ? body.runId : `run_${Date.now()}`;
  if (typeof runId !== 'string') {
    return {
      success: false,
      error: 'runId must be a string',
      code: 'INVALID_RUN_ID',
    };
  }

  if (runId.length < 1 || runId.length > 128) {
    return {
      success: false,
      error: 'runId length must be between 1 and 128 characters',
      code: 'INVALID_RUN_ID_LENGTH',
    };
  }

  if (CONTROL_CHARS_REGEX.test(runId)) {
    return {
      success: false,
      error: 'runId must not contain control or newline characters',
      code: 'INVALID_RUN_ID_FORMAT',
    };
  }

  return {
    success: true,
    data: {
      runId,
      scenario: scenario as JobScenario,
      count,
      backoffDelayMs,
      simulatedWorkMs,
    },
  };
}

export function validateDiagnosticJobData(data: unknown): {
  success: boolean;
  data?: DiagnosticJobData;
  error?: string;
} {
  if (!isPlainObject(data)) {
    return { success: false, error: 'Job data must be a plain object' };
  }

  if (
    typeof data.scenario !== 'string' ||
    !ALLOWED_SCENARIOS.includes(data.scenario as JobScenario)
  ) {
    return {
      success: false,
      error: `Invalid job scenario: "${String(data.scenario)}"`,
    };
  }

  if (typeof data.runId !== 'string' || data.runId.length < 1 || data.runId.length > 128) {
    return {
      success: false,
      error: 'Job runId must be a string between 1 and 128 characters',
    };
  }

  if (CONTROL_CHARS_REGEX.test(data.runId)) {
    return {
      success: false,
      error: 'Job runId must not contain control or newline characters',
    };
  }

  if (data.simulatedWorkMs !== undefined) {
    if (
      typeof data.simulatedWorkMs !== 'number' ||
      !Number.isInteger(data.simulatedWorkMs) ||
      data.simulatedWorkMs < 1 ||
      data.simulatedWorkMs > 10000
    ) {
      return {
        success: false,
        error: 'simulatedWorkMs must be an integer between 1 and 10000',
      };
    }
  }

  return {
    success: true,
    data: {
      runId: data.runId,
      scenario: data.scenario as JobScenario,
      simulatedWorkMs: data.simulatedWorkMs as number | undefined,
    },
  };
}
