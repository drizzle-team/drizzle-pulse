import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseClient } from 'drizzle-pulse/client/embedded';
import { createPulseRegistry } from 'drizzle-pulse/server';
import type { Pool } from 'pg';
import { fullOrdersFixture } from './fixtures/full-orders/index.js';
import type { HarnessProcessDbOperations, RuntimeOf } from './helpers/test-harness.js';
import {
  cleanupBetweenTestsForFixture,
  insertTestUser,
  setupTestSuiteForFixture,
  teardownTestSuiteForFixture,
} from './helpers/test-harness.js';

// ---------------------------------------------------------------------------
// Bounded async poller — avoids fixed sleeps while bounding test duration.
// ---------------------------------------------------------------------------

async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  pollIntervalMs = 20,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, pollIntervalMs));
  }
}

// ---------------------------------------------------------------------------
// Resilience suite — isolated variantName so its runtime/database is
// independent of the embedded-collection and lifecycle suites.
// ---------------------------------------------------------------------------

describe('Resilience', () => {
  const fixture = { ...fullOrdersFixture, variantName: 'resilience' as const };
  const { orders } = fixture.tables;

  let pool: Pool;
  let db: PostgresJsDatabase;
  let runtime!: RuntimeOf<typeof registry>;
  let processDbOperations: HarnessProcessDbOperations;

  const ordersByStatus = pulse(orders)
    .query()
    .args(fixture.schemas.ordersByStatusArgs)
    .order('asc')
    .query((ctx) => ctx.query({ status: ctx.args.status }));
  const registry = createPulseRegistry({ ordersByStatus });

  beforeAll(async () => {
    const setup = await setupTestSuiteForFixture(fixture, registry);
    pool = setup.pool;
    db = setup.db;
    runtime = setup.runtime;
    processDbOperations = setup.processDbOperations;
  });

  afterAll(async () => {
    await teardownTestSuiteForFixture(fixture, registry);
  });

  beforeEach(async () => {
    await cleanupBetweenTestsForFixture(fixture, pool);
    await insertTestUser(db, `driver_${randomUUID().slice(0, 8)}`);
  });

  // SC1 — reconnect drives a re-baseline of all live collections.
  //
  // The re-baseline edge fires onChange with empty events (list() ref changed)
  // and the collection must stay ready — the handshake is not re-run from scratch.
  test('reconnect re-baselines the bound collection through the handshake', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.ordersByStatus({ status: 'accepted' });

    // Establish live state: 2 rows via the WAL tap path.
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P1', dropoff: 'D1', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P2', dropoff: 'D2', price: 20, status: 'accepted' }),
    ]);

    // Third row — DB now has 3 accepted rows; collection has them via WAL tap.
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P3', dropoff: 'D3', price: 30, status: 'accepted' }),
    ]);

    expect(collection.list()).toHaveLength(3);

    // Register onChange AFTER setup so the counter only captures rebaseline edges.
    let changeCount = 0;
    collection.onChange(() => {
      changeCount++;
    });

    // Simulate a WAL reconnect: the runtime broadcasts the reconnect edge and the
    // embedded client debounces it (50 ms) and re-baselines its live collections.
    (runtime as any).onReplicationStart();

    // Wait for the single re-baseline edge onChange to fire.
    await waitFor(() => changeCount > 0, 500);

    expect(changeCount).toBeGreaterThanOrEqual(1);
    // Re-baseline must reflect the full current DB state.
    expect(collection.list()).toHaveLength(3);
    // The collection must stay ready — re-baseline never re-runs the handshake.
    expect(collection.isReady).toBe(true);

    collection.dispose();
  });

  // SC2 — rapid re-baseline triggers coalesce to a single re-baseline pass.
  //
  // N synchronous calls in the same tick all land inside the 50 ms debounce
  // window → one _runRebaselines invocation → one rebaseline() call →
  // one _fireOnChange([], snapshot) → exactly one edge onChange.
  // (Concurrent re-baselines being capped at 3 is proven in the unit test;
  // this integration test proves real-pipeline coalescing on a live WAL stack.)
  test('rapid re-baseline triggers coalesce to a single re-baseline edge onChange', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.ordersByStatus({ status: 'accepted' });

    // Seed real baseline state so the SELECT returns rows.
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'PA', dropoff: 'DA', price: 11, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'PB', dropoff: 'DB', price: 22, status: 'accepted' }),
    ]);

    expect(collection.list()).toHaveLength(2);

    let edgeCount = 0;
    collection.onChange(() => {
      edgeCount++;
    });

    // 5 synchronous reconnect edges — all within the same JS tick, all before the
    // client's 50 ms debounce fires. The debounce collapses them to one run.
    const N = 5;
    for (let i = 0; i < N; i++) {
      (runtime as any).onReplicationStart();
    }

    await waitFor(() => edgeCount > 0, 50 + 400);

    // Exactly ONE edge onChange — proving coalescing with no fan-out.
    expect(edgeCount).toBe(1);
    // Post-coalesce list() still equals DB state.
    expect(collection.list()).toHaveLength(2);

    collection.dispose();
  });
});
