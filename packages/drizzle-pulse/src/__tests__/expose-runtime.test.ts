import { describe, expect, test } from 'bun:test';
import { PulseRuntime } from '../server/expose.js';
import { createPulseRegistry } from '../server/pulse-registry.js';
import type { PulseSourceDb } from '../server/pulse-sql.js';
import { makePulseRuntime } from './mock-runtime.js';

describe('wal config resolution', () => {
  test('wal.publicationName/slotName are exposed on the runtime', () => {
    // makePulseRuntime passes wal: { publicationName: 'test_pub', slotName: 'test_slot' }.
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:p%40ss@localhost:5433/my_db?sslmode=require',
    });

    expect(runtime.publicationName).toBe('test_pub');
    expect(runtime.slotName).toBe('test_slot');
  });

  test('defaults to drizzle_pulse/drizzle_pulse when wal is omitted', () => {
    const emptyRegistry = createPulseRegistry({});
    const runtime = new PulseRuntime(emptyRegistry as any, {
      databaseUrl: 'postgresql://user:pass@localhost/test',
      sourceDb: {} as PulseSourceDb,
      pull: true,
    });

    expect(runtime.publicationName).toBe('drizzle_pulse');
    expect(runtime.slotName).toBe('drizzle_pulse');
  });
});

describe('start() failure rolls back to a restartable state', () => {
  test('a throw after the guard resets isRunning and tears down the pool instead of leaving a zombie', async () => {
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:pass@localhost/test',
    }) as any;

    let poolEnded = 0;
    runtime.initializeDatabaseServices();
    const firstPool = runtime.pool;
    firstPool.end = async () => {
      poolEnded++;
    };

    runtime.reconcile = async () => {};
    runtime.ensureBaselines = async () => {
      throw new Error('sourceDb briefly unavailable');
    };

    await expect(runtime.start()).rejects.toThrow('sourceDb briefly unavailable');

    expect(runtime.isRunning).toBe(false);
    expect(runtime.pool).toBeNull();
    expect(runtime.pulseStore).toBeNull();
    expect(poolEnded).toBe(1);

    // A retry must not hit the "Already running" early return and silently no-op — it
    // must re-attempt the guard and baseline steps.
    let secondAttemptRan = false;
    runtime.reconcile = async () => {};
    runtime.ensureBaselines = async () => {
      secondAttemptRan = true;
    };
    runtime.getPulseStore = () => ({
      getLatestSnapshot: async () => 0,
    });
    runtime.connectReplication = async () => {};

    await runtime.start();

    expect(secondAttemptRan).toBe(true);
    expect(runtime.isRunning).toBe(true);
  });
});
