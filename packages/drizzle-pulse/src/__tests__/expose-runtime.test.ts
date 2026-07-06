import { describe, expect, test } from 'bun:test';
import { RealtimeRuntime } from '../server/expose.js';
import { createPulseRegistry } from '../server/pulse-registry.js';
import type { PulseSourceDb } from '../server/pulse-sql.js';

// Construct a real RealtimeRuntime with an empty registry (no DB required) — mirrors
// resilience.test.ts's makeRealtimeRuntime helper.
function makeRuntime(databaseUrl: string): RealtimeRuntime<any> {
  const emptyRegistry = createPulseRegistry({});
  return new RealtimeRuntime(emptyRegistry as any, {
    databaseUrl,
    sourceDb: {} as PulseSourceDb,
    wal: { publicationName: 'test_pub', slotName: 'test_slot' },
  });
}

describe('connection-string handling', () => {
  test('pgConfig/pgPoolConfig pass the raw connectionString through instead of re-parsing it', () => {
    const databaseUrl = 'postgresql://user:p%40ss@localhost:5433/my_db?sslmode=require';
    const runtime = makeRuntime(databaseUrl) as any;

    expect(runtime.pgConfig.connectionString).toBe(databaseUrl);
    expect(runtime.pgPoolConfig.connectionString).toBe(databaseUrl);

    // No hand-parsed discrete fields that could silently drop percent-encoding or
    // query params — `pg` derives these itself from connectionString.
    expect(runtime.pgConfig.host).toBeUndefined();
    expect(runtime.pgConfig.port).toBeUndefined();
    expect(runtime.pgConfig.user).toBeUndefined();
    expect(runtime.pgConfig.password).toBeUndefined();
    expect(runtime.pgConfig.database).toBeUndefined();
  });

  test('replication flag is still layered onto pgConfig only', () => {
    const runtime = makeRuntime('postgresql://user:pass@localhost/test') as any;
    expect(runtime.pgConfig.replication).toBe('database');
    expect(runtime.pgPoolConfig.replication).toBeUndefined();
  });
});

describe('start() failure rolls back to a restartable state', () => {
  test('a throw after the guard resets _isRunning and tears down the pool instead of leaving a zombie', async () => {
    const runtime = makeRuntime('postgresql://user:pass@localhost/test') as any;

    let poolEnded = 0;
    runtime.initializeDatabaseServices();
    const firstPool = runtime.pool;
    firstPool.end = async () => {
      poolEnded++;
    };

    runtime.runStartupGuard = async () => {};
    runtime.ensureBaselines = async () => {
      throw new Error('sourceDb briefly unavailable');
    };

    await expect(runtime.start()).rejects.toThrow('sourceDb briefly unavailable');

    expect(runtime._isRunning).toBe(false);
    expect(runtime.pool).toBeNull();
    expect(runtime.realtimeService).toBeNull();
    expect(poolEnded).toBe(1);

    // A retry must not hit the "Already running" early return and silently no-op — it
    // must re-attempt the guard and baseline steps.
    let secondAttemptRan = false;
    runtime.runStartupGuard = async () => {};
    runtime.ensureBaselines = async () => {
      secondAttemptRan = true;
    };
    runtime.getRealtimeService = () => ({
      getLatestSnapshot: async () => 0,
    });
    runtime.connectReplication = async () => {};

    await runtime.start();

    expect(secondAttemptRan).toBe(true);
    expect(runtime._isRunning).toBe(true);
  });
});

describe('subscription idle sweep lifecycle', () => {
  test('subscriptionTtl config falls back to defaults and honors overrides', () => {
    const defaultRuntime = makeRuntime('postgresql://user:pass@localhost/test') as any;
    expect(defaultRuntime.subscriptionTtlConfig).toEqual({
      idleMs: 24 * 60 * 60 * 1000,
      sweepIntervalMs: 5 * 60 * 1000,
    });

    const emptyRegistry = createPulseRegistry({});
    const overridden = new RealtimeRuntime(emptyRegistry as any, {
      databaseUrl: 'postgresql://user:pass@localhost/test',
      sourceDb: {} as PulseSourceDb,
      subscriptionTtl: { idleMs: 1_000, sweepIntervalMs: 500 },
    }) as any;
    expect(overridden.subscriptionTtlConfig).toEqual({ idleMs: 1_000, sweepIntervalMs: 500 });
  });

  test('start() begins the sweep timer and stop() clears it', async () => {
    const runtime = makeRuntime('postgresql://user:pass@localhost/test') as any;

    runtime.runStartupGuard = async () => {};
    runtime.ensureBaselines = async () => {};
    runtime.getRealtimeService = () => ({ getLatestSnapshot: async () => 0 });
    runtime.connectReplication = async () => {};

    expect(runtime.subscriptionSweepTimer).toBeNull();
    await runtime.start();
    expect(runtime.subscriptionSweepTimer).not.toBeNull();

    await runtime.stop();
    expect(runtime.subscriptionSweepTimer).toBeNull();
  });

  test('a failed start() never leaves a dangling sweep timer', async () => {
    const runtime = makeRuntime('postgresql://user:pass@localhost/test') as any;

    runtime.runStartupGuard = async () => {};
    runtime.ensureBaselines = async () => {
      throw new Error('boom');
    };

    await expect(runtime.start()).rejects.toThrow('boom');
    expect(runtime.subscriptionSweepTimer).toBeNull();
  });
});
