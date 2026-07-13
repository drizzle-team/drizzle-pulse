import { describe, expect, test } from 'bun:test';
import { makePulseRuntime } from './mock-runtime.js';

describe('connection-string handling', () => {
  test('pgConfig passes the raw connectionString through instead of re-parsing it', () => {
    const databaseUrl = 'postgresql://user:p%40ss@localhost:5433/my_db?sslmode=require';
    const runtime = makePulseRuntime({ databaseUrl }) as any;

    expect(runtime.pgConfig.connectionString).toBe(databaseUrl);

    // No hand-parsed discrete fields that could silently drop percent-encoding or
    // query params — `pg` derives these itself from connectionString.
    expect(runtime.pgConfig.host).toBeUndefined();
    expect(runtime.pgConfig.port).toBeUndefined();
    expect(runtime.pgConfig.user).toBeUndefined();
    expect(runtime.pgConfig.password).toBeUndefined();
    expect(runtime.pgConfig.database).toBeUndefined();
  });

  test('replication flag is still layered onto pgConfig only', () => {
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:pass@localhost/test',
    }) as any;
    expect(runtime.pgConfig.replication).toBe('database');
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
