const API_URL = process.env.API_URL || 'http://localhost:3000/jobs';

interface ScenarioRequest {
  name: string;
  body: {
    runId: string;
    scenario: 'success' | 'transient-retry' | 'exhausted-failure' | 'slow-worker';
    count?: number;
    backoffDelayMs?: number;
    simulatedWorkMs?: number;
  };
}

async function triggerScenario(req: ScenarioRequest) {
  const response = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req.body),
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
  }

  const data = await response.json();
  console.log(
    `[OK] [${req.name.padEnd(26)}] runId=${data.runId} | jobs=${data.count} | jobIds=${JSON.stringify(data.jobIds)}`
  );
}

async function main() {
  const timestamp = Date.now();
  console.log('[Runner] Dispatching diagnostic scenarios...\n');

  const coreScenarios: ScenarioRequest[] = [
    {
      name: 'Normal Fast Success',
      body: { runId: `run_success_${timestamp}`, scenario: 'success', simulatedWorkMs: 50 },
    },
    {
      name: 'Transient Failure (Retry)',
      body: {
        runId: `run_transient_${timestamp}`,
        scenario: 'transient-retry',
        backoffDelayMs: 1500,
        simulatedWorkMs: 50,
      },
    },
    {
      name: 'Exhausted Failure',
      body: {
        runId: `run_exhausted_${timestamp}`,
        scenario: 'exhausted-failure',
        backoffDelayMs: 1000,
        simulatedWorkMs: 50,
      },
    },
    {
      name: 'Slow Worker',
      body: {
        runId: `run_slow_${timestamp}`,
        scenario: 'slow-worker',
        simulatedWorkMs: 2000,
      },
    },
  ];

  for (const s of coreScenarios) {
    await triggerScenario(s);
  }

  console.log('\n[Runner] Waiting for retries and slow worker to finish...');
  await new Promise((r) => setTimeout(r, 8000));

  console.log('\n[Runner] Dispatching queue backlog burst (20 x 100ms)...\n');
  await triggerScenario({
    name: 'Backlog Burst (20x 100ms)',
    body: {
      runId: `run_backlog_${timestamp}`,
      scenario: 'success',
      count: 20,
      simulatedWorkMs: 100,
    },
  });

  console.log('\n[Runner] Scenarios dispatched successfully.');
}

main().catch((err) => {
  console.error('Scenario runner failed:', err);
  process.exit(1);
});
