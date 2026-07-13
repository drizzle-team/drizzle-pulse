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

const LSN_PATTERN = /^[0-9A-Fa-f]+\/[0-9A-Fa-f]+$/;

// ---------------------------------------------------------------------------
// Resilience suite — isolated variantName so its runtime/database is
// independent of the embedded-collection and lifecycle suites.
//
// A WAL reconnect edge re-runs the same watermark/baseline handshake collections use on
// initial load (see client/embedded/index.ts runHandshake): re-select the baseline, read a
// fresh watermark, rebuild the merge core, then drain buffered tap payloads at-or-above it.
// No events-table read, no poll — the collection is fed straight off runtime.onReconnect.
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

  // A reconnect edge triggers a full re-baseline (watermark + baseline SELECT), and the live
  // collection keeps reflecting DB state before and after the edge — through the same
  // handshake the initial load uses, not a pull.
  test('reconnect edge triggers a re-baseline and the collection stays consistent', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.ordersByStatus({ status: 'accepted' });

    const changes: Array<{ events: readonly unknown[]; lsn: string }> = [];
    collection.onChange((c) => changes.push(c));

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P1', dropoff: 'D1', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'P2', dropoff: 'D2', price: 20, status: 'accepted' }),
    ]);

    await waitFor(() => collection.list().length === 2, 2000);
    const changesBeforeReconnect = changes.length;

    // Simulate a WAL reconnect: the runtime broadcasts the reconnect edge, which the
    // collection wires to a re-baseline handshake. State must stay consistent (no loss, no
    // dup), and the re-baseline itself fires onChange with an empty event batch and the
    // fresh watermark lsn — a plain re-render carrying the new checkpoint, no delta.
    (runtime as any).onReplicationStart();
    await waitFor(() => changes.length === changesBeforeReconnect + 1, 2000);

    const rebaselineChange = changes[changesBeforeReconnect]!;
    expect(rebaselineChange.events).toEqual([]);
    expect(rebaselineChange.lsn).toMatch(LSN_PATTERN);

    expect(collection.list().length).toBe(2);
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
