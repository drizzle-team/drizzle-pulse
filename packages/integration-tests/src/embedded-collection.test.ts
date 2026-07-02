import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { createPulseClient } from '@drizzle-pulse/client/embedded';
import { createPulse, createPulseRegistry } from '@drizzle-pulse/client/server';
import { eq } from 'drizzle-orm';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
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

// ---------------------------------------------------------------------------
// SC3 + SC4: main suite
// ---------------------------------------------------------------------------

describe('Embedded Collection', () => {
  let pool: Pool;
  let db: PostgresJsDatabase;
  let runtime!: RuntimeOf<typeof registry>;
  let processDbOperations: HarnessProcessDbOperations;

  const fixture = fullOrdersFixture;
  const { orders } = fixture.tables;

  const pulse = createPulse();
  const ordersByStatus = pulse(orders)
    .$eventsTable(fixture.tables.eventsPublicOrders)
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
    await teardownTestSuiteForFixture(fixture);
  });

  beforeEach(async () => {
    await cleanupBetweenTestsForFixture(fixture, pool);
    await insertTestUser(db, `driver_${randomUUID().slice(0, 8)}`);
  });

  // SC3 — A row committed after the tap-first baseline snapshot read appears exactly
  // once in list() — never lost, never double-applied.
  //
  // Both operations use processDbOperations so we wait for each WAL event to
  // be written to the events table before proceeding. That guarantees:
  //   • sinceSnapshot for the concurrent insert is correctly set to the
  //     pre-insert's snapshot, so processDbOperations waits specifically for
  //     the concurrent row's WAL event (not the pre-insert's).
  //   • By the time processDbOperations resolves, applyTapPayload has already
  //     run (buffer mode if baseline is still in flight, live mode if baseline
  //     already completed). The $pk backstop prevents any double-counting in
  //     the buffer case.
  test('mid-baseline insert appears exactly once in list()', async () => {
    // Pre-insert Row A via processDbOperations so its WAL event is fully
    // processed (snapshot = 1) before we create the collection.
    await processDbOperations([
      db.insert(orders).values({
        driverId: 1,
        pickup: 'SC3 Pre Pickup',
        dropoff: 'SC3 Pre Dropoff',
        price: 10,
        status: 'accepted',
      }),
    ]);

    const client = createPulseClient(runtime);
    // baseline snapshot = lastPersistedSnapshot = 1 (Row A's event processed)
    // Tap listener registered synchronously by the factory call; baseline SELECT is now
    // in flight — the promise is deliberately NOT awaited until after the racing insert.
    const collectionPromise = client.ordersByStatus({ status: 'accepted' });

    // Insert Row B concurrently. processDbOperations now starts with
    // sinceSnapshot = 1 and waits for snapshot >= 2, which is Row B's event.
    // By the time it resolves, the tap has dispatched Row B: either buffered
    // (baseline still running) or applied live (baseline already done).
    const { results } = await processDbOperations([
      db
        .insert(orders)
        .values({
          driverId: 1,
          pickup: 'SC3 Race Pickup',
          dropoff: 'SC3 Race Dropoff',
          price: 20,
          status: 'accepted',
        })
        .returning(),
    ]);
    const [raceInserted] = results[0] as Array<{ id: number }>;

    const collection = await collectionPromise;

    const rows = collection.list();
    const ids = rows.map((r) => r.id as number);
    expect(ids).toHaveLength(2);
    expect(new Set(ids).size).toBe(2); // no duplicate of the race-inserted row
    expect(ids).toContain(raceInserted!.id);

    collection.dispose();
  });

  // SC4 — Post-baseline WAL changes reach onChange synchronously, once per
  // applied WAL change, in WAL order, with state === list() (same reference).
  test('post-baseline INSERT/UPDATE/DELETE reach onChange synchronously in WAL order', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.ordersByStatus({ status: 'accepted' });

    // processDbOperations waits for the events-table row, which is written
    // before _walEventEmitter.emit() dispatches synchronously — so onChange
    // has already fired by the time processDbOperations resolves.
    const changes: Array<{ events: readonly any[]; state: readonly any[]; snapshot: number }> = [];
    collection.onChange((c) => changes.push(c));

    // INSERT ── one onChange, op='insert', state === list()
    const { results: r1 } = await processDbOperations([
      db
        .insert(orders)
        .values({
          driverId: 1,
          pickup: 'SC4 Insert Pickup',
          dropoff: 'SC4 Insert Dropoff',
          price: 30,
          status: 'accepted',
        })
        .returning(),
    ]);
    const [inserted] = r1[0] as Array<{ id: number }>;

    expect(changes).toHaveLength(1);
    expect(changes[0]!.events[0]!.op).toBe('insert');
    expect(changes[0]!.state).toBe(collection.list());
    expect(collection.list()).toHaveLength(1);

    // UPDATE out of filter (accepted → completed): matchesOld=true, matchesNew=false
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.id, inserted!.id)),
    ]);

    expect(changes).toHaveLength(2);
    const updateEvt = changes[1]!.events[0];
    expect(updateEvt.op).toBe('update');
    expect(updateEvt.matchesOld).toBe(true);
    expect(updateEvt.matchesNew).toBe(false);
    expect(collection.list()).toHaveLength(0);
    expect(changes[1]!.state).toBe(collection.list());

    // INSERT a second accepted row to set up the DELETE
    const { results: r2 } = await processDbOperations([
      db
        .insert(orders)
        .values({
          driverId: 1,
          pickup: 'SC4 Delete Pickup',
          dropoff: 'SC4 Delete Dropoff',
          price: 40,
          status: 'accepted',
        })
        .returning(),
    ]);
    const [toDelete] = r2[0] as Array<{ id: number }>;

    expect(changes).toHaveLength(3);
    expect(changes[2]!.events[0]!.op).toBe('insert');
    expect(collection.list()).toHaveLength(1);

    // DELETE ── one onChange, op='delete', list empty afterwards
    await processDbOperations([db.delete(orders).where(eq(orders.id, toDelete!.id))]);

    expect(changes).toHaveLength(4);
    expect(changes[3]!.events[0]!.op).toBe('delete');
    expect(collection.list()).toHaveLength(0);
    expect(changes[3]!.state).toBe(collection.list());

    collection.dispose();
  });
});

// ---------------------------------------------------------------------------
// Lifecycle suite — separate fixture variantName so its runtime can be stopped
// without touching the SC3/SC4 context (teardownTestSuiteForFixture uses
// fixture.variantName as the key prefix).
// ---------------------------------------------------------------------------

describe('Embedded Collection lifecycle', () => {
  const lifecycleFixture = { ...fullOrdersFixture, variantName: 'embedded-lifecycle' as const };
  const { orders } = lifecycleFixture.tables;

  const lcPulse = createPulse();
  const ordersByStatusLC = lcPulse(orders)
    .$eventsTable(lifecycleFixture.tables.eventsPublicOrders)
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
    // Decrements activeSuiteUsers for the lifecycle context only (key:
    // 'embedded-lifecycle::ordersByStatusLC'); stop() is idempotent so
    // calling it again after the test's runtime.stop() is safe.
    await teardownTestSuiteForFixture(lifecycleFixture);
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

    // runtime.stop() iterates _liveCollections and calls dispose() on each,
    // which synchronously unsubscribes the tap listener.
    await lcRuntime.stop();

    // dispose() must be idempotent — no throw on repeated calls
    expect(() => collection.dispose()).not.toThrow();
    expect(() => collection.dispose()).not.toThrow();

    // The tap listener was detached by dispose(); no onChange fired during stop
    expect(firedAfterStop).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Exotic-type coverage: the embedded WAL tap must deliver every pg data type in
// the same normalized JS shape a baseline SELECT produces. The raw pgoutput WAL
// arrives as text; the server normalizes it with Drizzle's codecs before emit
// (see server/wal-normalization.ts). This locks that path — the HTTP-client
// pg-data-types tests never exercise it (their values arrive pre-typed).
// ---------------------------------------------------------------------------

describe('Embedded Collection — PG Data Types (WAL tap normalization)', () => {
  let pool: Pool;
  let db: PostgresJsDatabase;
  let runtime!: RuntimeOf<typeof registry>;
  let processDbOperations: HarnessProcessDbOperations;

  const fixture = pgDataTypesFixture;
  const { pgDataTypes } = fixture.tables;

  const pulse = createPulse();
  const allPgDataTypes = pulse(pgDataTypes)
    .$eventsTable(fixture.tables.eventsPublicPgDataTypes)
    .query(() => null);
  const registry = createPulseRegistry({ allPgDataTypes });

  beforeAll(async () => {
    const setup = await setupTestSuiteForFixture(fixture, registry);
    pool = setup.pool;
    db = setup.db;
    runtime = setup.runtime;
    processDbOperations = setup.processDbOperations;
  });

  afterAll(async () => {
    await teardownTestSuiteForFixture(fixture);
  });

  beforeEach(async () => {
    await cleanupBetweenTestsForFixture(fixture, pool);
  });

  test('WAL-tap delta rows carry every pg data type in its normalized JS shape', async () => {
    const client = createPulseClient(runtime);
    const collection = await client.allPgDataTypes();
    expect(collection.list()).toEqual([]);

    await processDbOperations([db.insert(pgDataTypes).values(pgDataTypeInsertValues)]);

    // The tap applies on WAL emit; absorb the scheduling beat before asserting.
    for (let attempt = 0; attempt < 20 && collection.list().length === 0; attempt++) {
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
    }

    expect(collection.list()).toEqual([expect.objectContaining({ ...pgDataTypeInsertValues })]);

    collection.dispose();
  });
});
