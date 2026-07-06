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
