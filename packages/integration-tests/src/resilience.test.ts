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
//
// The bespoke rebaseline/debounce engine is gone: reconnect now just nudges the
// collection's PulseQuery to poll, catching up through the one pull pipeline.
// ---------------------------------------------------------------------------

describe('Resilience', () => {
  const fixture = { ...fullOrdersFixture, variantName: 'resilience' as const };
  const { orders } = fixture.tables;

  let pool: Pool;
  let db: PostgresJsDatabase;
  let runtime!: RuntimeOf<typeof registry>;
  let processDbOperations: HarnessProcessDbOperations;

  const ordersByStatus = pulse(orders)
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

  // A reconnect edge triggers a catch-up poll, and the live collection keeps
  // reflecting DB state before and after the edge — through the single pipeline.
  test('reconnect edge triggers a catch-up poll and the collection stays consistent', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.ordersByStatus({ status: 'accepted' });

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P1', dropoff: 'D1', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P2', dropoff: 'D2', price: 20, status: 'accepted' }),
    ]);

    await waitFor(() => collection.list().length === 2, 2000);

    // Simulate a WAL reconnect: the runtime broadcasts the reconnect edge, which the
    // collection wires to query.poll(). State must stay consistent (no loss, no dup).
    (runtime as any).onReplicationStart();
    await waitFor(() => collection.list().length === 2, 2000);
    expect(new Set(collection.list().map((r) => r.id)).size).toBe(2);

    // The pipeline keeps delivering after the reconnect edge.
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P3', dropoff: 'D3', price: 30, status: 'accepted' }),
    ]);
    await waitFor(() => collection.list().length === 3, 2000);

    collection.dispose();
  });
});
