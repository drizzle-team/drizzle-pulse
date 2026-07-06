import { describe, expect, test } from 'bun:test';
import { types } from 'pg';
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

describe('connection-string handling (WR-03)', () => {
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

describe('start() failure rolls back to a restartable state (WR-04)', () => {
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

describe('type-parser scoping (WR-05)', () => {
  test('pgConfig/pgPoolConfig scope raw-text OIDs to this runtime only, not the global pg registry', () => {
    const runtime = makeRuntime('postgresql://user:pass@localhost/test') as any;

    const scoped = runtime.pgConfig.types;
    expect(scoped).toBeDefined();
    expect(runtime.pgPoolConfig.types).toBe(scoped);

    for (const oid of [1082, 1114, 1184, 1186, 600]) {
      const parser = scoped.getTypeParser(oid);
      expect(parser('2024-01-01T00:00:00.000Z')).toBe('2024-01-01T00:00:00.000Z');
    }

    // Non-overridden OIDs delegate to pg's real getTypeParser (e.g. int4 stays numeric).
    const int4Parser = scoped.getTypeParser(23);
    expect(int4Parser('42')).toBe(42);

    // The global `pg` module registry itself must be untouched — a host application's
    // own `pg` client sharing this process still gets parsed Date objects for
    // timestamptz (1184), not raw text.
    const globalTimestampTzParser = types.getTypeParser(1184 as any);
    expect(globalTimestampTzParser('2024-01-01 00:00:00+00')).toBeInstanceOf(Date);
  });
});
