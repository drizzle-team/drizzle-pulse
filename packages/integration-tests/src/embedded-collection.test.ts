import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { eq } from 'drizzle-orm';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseClient } from 'drizzle-pulse/client/embedded';
import { createPulseRegistry } from 'drizzle-pulse/server';
import type { Pool } from 'pg';
import { fullOrdersFixture } from './fixtures/full-orders/index.js';
import { pgDataTypesFixture } from './fixtures/pg-data-types/index.js';
import { pgDataTypeInsertValues } from './fixtures/pg-data-types/inventory.js';
import type { HarnessProcessDbOperations, RuntimeOf } from './helpers/test-harness.js';
import {
  cleanupBetweenTestsForFixture,
  insertTestUser,
  setupTestSuiteForFixture,
  teardownTestSuiteForFixture,
} from './helpers/test-harness.js';

// Push-driven updates flow through query.poll() (async), so state converges a scheduling
// beat after processDbOperations resolves — poll for it rather than assert synchronously.
async function waitFor(
  predicate: () => boolean,
  timeoutMs = 2000,
  pollIntervalMs = 20,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await new Promise<void>((resolve) => setTimeout(resolve, pollIntervalMs));
  }
}

// ---------------------------------------------------------------------------
// Main suite — facade behavior over the direct transport.
// ---------------------------------------------------------------------------

describe('Embedded Collection', () => {
  let pool: Pool;
  let db: PostgresJsDatabase;
  let runtime!: RuntimeOf<typeof registry>;
  let processDbOperations: HarnessProcessDbOperations;

  const fixture = fullOrdersFixture;
  const { orders } = fixture.tables;

  const ordersByStatus = pulse(orders)
    .args(fixture.schemas.ordersByStatusArgs)
    .order('asc')
    .query((ctx) => ctx.query({ status: ctx.args.status }));
  // ordersByStatusLimited (a `.limit()` query) is gone: embedded collections now reject
  // `.limit()` at creation (SPLIT-02) — the loadMore() coverage it fed is obsolete.
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

  // A row committed while the collection is being created appears exactly once — the
  // baseline SELECT and the catch-up/live pull dedupe by $pk.
  test('a concurrently-inserted row appears exactly once in list()', async () => {
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'Pre', dropoff: 'Pre', price: 10, status: 'accepted' }),
    ]);

    const client = createPulseClient(runtime);
    const collectionPromise = client.ordersByStatus({ status: 'accepted' });

    const { results } = await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'Race', dropoff: 'Race', price: 20, status: 'accepted' })
        .returning(),
    ]);
    const [raceInserted] = results[0] as Array<{ id: number }>;

    const collection = await collectionPromise;
    await waitFor(() => collection.list().length === 2);

    const ids = collection.list().map((r) => r.id as number);
    expect(new Set(ids).size).toBe(2); // no duplicate of the race-inserted row
    expect(ids).toContain(raceInserted!.id);

    collection.dispose();
  });

  // Insert into the source table → onChange fires (through the WAL signal → poll), with no
  // polling interval anywhere. Covers insert/update/delete op shapes and state === list().
  test('push-shaped INSERT/UPDATE/DELETE reach onChange without any interval', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.ordersByStatus({ status: 'accepted' });

    const changes: Array<{ events: readonly any[]; state: readonly any[]; lsn: string }> = [];
    collection.onChange((c) => changes.push(c));

    // INSERT
    const { results: r1 } = await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'Ins', dropoff: 'Ins', price: 30, status: 'accepted' })
        .returning(),
    ]);
    const [inserted] = r1[0] as Array<{ id: number }>;

    await waitFor(() => changes.length === 1);
    expect(changes[0]!.events[0]!.op).toBe('insert');
    expect(changes[0]!.state).toBe(collection.list());
    expect(collection.list()).toHaveLength(1);

    // UPDATE out of filter (accepted → completed): matchesOld=true, matchesNew=false
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.id, inserted!.id)),
    ]);

    await waitFor(() => changes.length === 2);
    const updateEvt = changes[1]!.events[0];
    expect(updateEvt.op).toBe('update');
    expect(updateEvt.matchesOld).toBe(true);
    expect(updateEvt.matchesNew).toBe(false);
    expect(collection.list()).toHaveLength(0);
    expect(changes[1]!.state).toBe(collection.list());

    // INSERT a second accepted row, then DELETE it
    const { results: r2 } = await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'Del', dropoff: 'Del', price: 40, status: 'accepted' })
        .returning(),
    ]);
    const [toDelete] = r2[0] as Array<{ id: number }>;

    await waitFor(() => changes.length === 3);
    expect(changes[2]!.events[0]!.op).toBe('insert');
    expect(collection.list()).toHaveLength(1);

    await processDbOperations([db.delete(orders).where(eq(orders.id, toDelete!.id))]);

    await waitFor(() => changes.length === 4);
    expect(changes[3]!.events[0]!.op).toBe('delete');
    expect(collection.list()).toHaveLength(0);
    expect(changes[3]!.state).toBe(collection.list());

    collection.dispose();
  });
});

// ---------------------------------------------------------------------------
// Lifecycle suite — separate variantName so its runtime can be stopped in-test.
// ---------------------------------------------------------------------------

describe('Embedded Collection lifecycle', () => {
  const lifecycleFixture = { ...fullOrdersFixture, variantName: 'embedded-lifecycle' as const };
  const { orders } = lifecycleFixture.tables;

  const ordersByStatusLC = pulse(orders)
    .args(lifecycleFixture.schemas.ordersByStatusArgs)
    .order('asc')
    .query((ctx) => ctx.query({ status: ctx.args.status }));
  const lcRegistry = createPulseRegistry({ ordersByStatusLC });

  let lcPool: Pool;
  let lcDb: PostgresJsDatabase;
  let lcRuntime!: RuntimeOf<typeof lcRegistry>;

  beforeAll(async () => {
    const setup = await setupTestSuiteForFixture(lifecycleFixture, lcRegistry);
    lcPool = setup.pool;
    lcDb = setup.db;
    lcRuntime = setup.runtime;
  });

  afterAll(async () => {
    await teardownTestSuiteForFixture(lifecycleFixture, lcRegistry);
  });

  beforeEach(async () => {
    await cleanupBetweenTestsForFixture(lifecycleFixture, lcPool);
    await insertTestUser(lcDb, `driver_${randomUUID().slice(0, 8)}`);
  });

  test('runtime.stop() disposes live collections and dispose() is idempotent', async () => {
    const client = createPulseClient(lcRuntime);
    const collection = await client.ordersByStatusLC({ status: 'accepted' });

    let firedAfterStop = false;
    collection.onChange(() => {
      firedAfterStop = true;
    });

    // runtime.stop() broadcasts onStop; each collection wires it to dispose(), which
    // synchronously detaches its WAL signal and destroys the query.
    await lcRuntime.stop();

    // dispose() must be idempotent — no throw on repeated calls
    expect(() => collection.dispose()).not.toThrow();
    expect(() => collection.dispose()).not.toThrow();

    // The signal was detached by dispose(); no onChange fired during stop
    expect(firedAfterStop).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Exotic-type coverage: the WAL → events-table → pull path must deliver every pg
// data type in the same normalized JS shape a baseline SELECT produces. The raw
// pgoutput WAL arrives as text; the server normalizes it with Drizzle's codecs
// (see server/wal-normalization.ts) before persisting the event this pull reads.
// ---------------------------------------------------------------------------

describe('Embedded Collection — PG Data Types (WAL normalization)', () => {
  let pool: Pool;
  let db: PostgresJsDatabase;
  let runtime!: RuntimeOf<typeof registry>;
  let processDbOperations: HarnessProcessDbOperations;

  const fixture = pgDataTypesFixture;
  const { pgDataTypes } = fixture.tables;

  const allPgDataTypes = pulse(pgDataTypes).query(() => null);
  const registry = createPulseRegistry({ allPgDataTypes });

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
  });

  test('push delta rows carry every pg data type in its normalized JS shape', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.allPgDataTypes();
    expect(collection.list()).toEqual([]);

    await processDbOperations([db.insert(pgDataTypes).values(pgDataTypeInsertValues)]);

    await waitFor(() => collection.list().length === 1);

    expect(collection.list()).toEqual([expect.objectContaining({ ...pgDataTypeInsertValues })]);

    collection.dispose();
  });
});
