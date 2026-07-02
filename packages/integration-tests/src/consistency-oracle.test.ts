// HTTP is authoritative: embedded list() must equal both runtime.handlers.subscribe rows
// (baseline SELECT) AND the HTTP incremental-pull merged state for every non-transformed
// operator incl. explicit null/undefined. A divergence on either comparison means the
// embedded WAL-tap + evaluateCondition pipeline, or the incremental-pull merge, has a bug
// (or a pre-existing bug the oracle surfaces). The incremental-pull comparison exists
// because the baseline-SELECT-only oracle previously missed a camelCase column-drop bug
// that only manifested on the `handlers.pull` path.

import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { PulseCollection } from '@drizzle-pulse/client/embedded';
import { createPulseClient as createEmbeddedClient } from '@drizzle-pulse/client/embedded';
import {
  createPulseClient as createHttpClient,
  type PulseQuery,
} from '@drizzle-pulse/client/react';
import type { PulseAuthContext } from '@drizzle-pulse/client/server';
import { createPulse, createPulseRegistry } from '@drizzle-pulse/client/server';
import { eq } from 'drizzle-orm';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import type { Pool } from 'pg';
import { fullOrdersFixture } from './fixtures/full-orders/index.js';
import type {
  HarnessInitTestQuery,
  HarnessProcessDbOperations,
  RuntimeOf,
} from './helpers/test-harness.js';
import {
  cleanupBetweenTestsForFixture,
  insertTestUser,
  setupTestSuiteForFixture,
  teardownTestSuiteForFixture,
} from './helpers/test-harness.js';

// ---------------------------------------------------------------------------
// Fixture — own variantName so this suite gets an isolated runtime + database
// ---------------------------------------------------------------------------

const oracleFixture = { ...fullOrdersFixture, variantName: 'consistency-oracle' as const };
const { orders } = oracleFixture.tables;

// ---------------------------------------------------------------------------
// Registry — all operator queries defined once so the suite key is stable
// ---------------------------------------------------------------------------

const pulse = createPulse();

// eq (fixed predicate)
const ordersEqAccepted = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ status: 'accepted' }));

// eq (args — parity with the embedded-collection suite)
const ordersByStatus = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .args(oracleFixture.schemas.ordersByStatusArgs)
  .order('asc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

// ne over text column
const ordersNeCompleted = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ status: { ne: 'completed' } }));

// ne over nullable integer — must EXCLUDE rows where driverId IS NULL
const ordersDriverNe1 = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ driverId: { ne: 1 } }));

// gt / gte / lt / lte
const ordersPriceGt10 = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ price: { gt: 10 } }));

const ordersPriceGte10 = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ price: { gte: 10 } }));

const ordersPriceLt50 = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ price: { lt: 50 } }));

const ordersPriceLte50 = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ price: { lte: 50 } }));

// in (non-empty)
const ordersStatusIn = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) =>
    ctx.query({
      status: {
        in: ['accepted', 'requested'] as ('accepted' | 'requested' | 'completed' | 'cancelled')[],
      },
    }),
  );

// in (empty → always false, zero rows)
const ordersStatusInEmpty = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) =>
    ctx.query({ status: { in: [] as ('requested' | 'accepted' | 'completed' | 'cancelled')[] } }),
  );

// isNull / isNotNull on nullable column
const ordersDriverIsNull = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ driverId: { isNull: true } }));

const ordersDriverIsNotNull = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ driverId: { isNotNull: true } }));

// NOT (incl. a NULL-driverId row so NOT-with-null-field is exercised)
const ordersNotCompleted = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ NOT: { status: 'completed' } }));

// AND — both conjuncts must hold
const ordersAnd = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ AND: [{ status: 'accepted' }, { price: { gt: 5 } }] }));

// OR — either disjunct suffices
const ordersOr = pulse(orders)
  .$eventsTable(oracleFixture.tables.eventsPublicOrders)
  .order('asc')
  .query((ctx) => ctx.query({ OR: [{ status: 'accepted' }, { status: 'requested' }] }));

const registry = createPulseRegistry({
  ordersEqAccepted,
  ordersByStatus,
  ordersNeCompleted,
  ordersDriverNe1,
  ordersPriceGt10,
  ordersPriceGte10,
  ordersPriceLt50,
  ordersPriceLte50,
  ordersStatusIn,
  ordersStatusInEmpty,
  ordersDriverIsNull,
  ordersDriverIsNotNull,
  ordersNotCompleted,
  ordersAnd,
  ordersOr,
});

// ---------------------------------------------------------------------------
// Oracle assertion helper
// ---------------------------------------------------------------------------

const sortByPk = (rows: readonly Record<string, unknown>[]) =>
  [...rows].sort((a, b) => String(a.$pk).localeCompare(String(b.$pk)));

// Deep-equality predicate reusing bun's toEqual semantics (order-insensitive on keys,
// Date/Decimal-aware) so the retry loop's convergence check matches the final assertion
// exactly. Returns false instead of throwing so it can drive the bounded retry.
function rowsMatch(actual: unknown, expected: unknown): boolean {
  try {
    expect(actual).toEqual(expected);
    return true;
  } catch {
    return false;
  }
}

async function assertOracleMatch(
  runtime: RuntimeOf<typeof registry>,
  collection: PulseCollection<Record<string, unknown> & { $pk: unknown }>,
  pullQuery: PulseQuery<Record<string, unknown> & { $pk: unknown }>,
  queryName: string,
  args: unknown = {},
  auth: PulseAuthContext = { userId: null },
): Promise<void> {
  const httpResult = await runtime.handlers.subscribe(
    { queryName, args: args ?? {}, clientId: randomUUID() },
    auth,
  );
  expect(httpResult.status).toBe(200);
  const httpRows = (httpResult.body as { rows: Record<string, unknown>[] }).rows;

  const sortedHttp = sortByPk(httpRows);

  // Both the in-process embedded WAL-tap AND the HTTP incremental pull can lag the harness's
  // "events committed" signal by a scheduling beat: processDbOperations resolves once the
  // events-table row is visible, which can precede the WAL handler's subsequent (synchronous)
  // tap emit and the pull cursor catching up. Retry both until they converge on the
  // authoritative subscribe baseline. A real, persistent divergence (e.g. dropped camelCase
  // columns on the pull path) NEVER converges and still fails deterministically with the clean
  // diff below — so this stays a genuine gate, not a weakened one.
  const maxAttempts = 15;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    await pullQuery.poll();
    const sortedEmbed = sortByPk([...collection.list()] as Record<string, unknown>[]);
    const sortedPull = sortByPk([...pullQuery.getState().data] as Record<string, unknown>[]);
    if (rowsMatch(sortedEmbed, sortedHttp) && rowsMatch(sortedPull, sortedHttp)) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 50));
  }

  // Never converged within the retry budget → a real, persistent divergence. Assert strictly
  // for a readable diff (the HTTP subscribe baseline is authoritative).
  expect(sortByPk([...collection.list()] as Record<string, unknown>[])).toEqual(sortedHttp);
  expect(sortByPk([...pullQuery.getState().data] as Record<string, unknown>[])).toEqual(sortedHttp);
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe('Consistency Oracle', () => {
  let pool: Pool;
  let db: PostgresJsDatabase;
  let runtime!: RuntimeOf<typeof registry>;
  let processDbOperations: HarnessProcessDbOperations;
  let initTestQuery: HarnessInitTestQuery;
  const httpClient = createHttpClient<typeof registry.$client>({ url: 'http://localhost' });

  beforeAll(async () => {
    const setup = await setupTestSuiteForFixture(oracleFixture, registry);
    pool = setup.pool;
    db = setup.db;
    runtime = setup.runtime;
    processDbOperations = setup.processDbOperations;
    initTestQuery = setup.initTestQuery;
  });

  afterAll(async () => {
    await teardownTestSuiteForFixture(oracleFixture);
  });

  beforeEach(async () => {
    await cleanupBetweenTestsForFixture(oracleFixture, pool);
    // RESTART IDENTITY in cleanup makes this insert produce user id=1 deterministically
    await insertTestUser(db, `driver_${randomUUID().slice(0, 8)}`);
  });

  // ---- eq (fixed) ----------------------------------------------------------

  test('eq (fixed): status = accepted', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersEqAccepted();
    const httpQuery = await initTestQuery(httpClient.ordersEqAccepted());

    // Seed: one matching, one non-matching
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'completed' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersEqAccepted');

    // Update a non-filter camelCase column (driverId) while the row stays IN the filter
    // (matchesOld=true, matchesNew=true). This is the exact class of bug the incremental-pull
    // comparison exists to catch: the pull path must replace with the full new-value row,
    // preserving camelCase columns like driverId.
    await insertTestUser(db, `driver2_${randomUUID().slice(0, 8)}`);
    await processDbOperations([
      db.update(orders).set({ driverId: 2 }).where(eq(orders.status, 'accepted')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersEqAccepted');

    // Update accepted → completed (moves row out of filter: matchesOld=true, matchesNew=false)
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.status, 'accepted')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersEqAccepted');

    // Insert a new accepted row then delete it
    const { results } = await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 30, status: 'accepted' })
        .returning(),
    ]);
    const inserted = (results[0] as Array<{ id: number }>)[0]!;
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersEqAccepted');

    await processDbOperations([db.delete(orders).where(eq(orders.id, inserted.id))]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersEqAccepted');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- eq (args) -----------------------------------------------------------

  test('eq (args): ordersByStatus', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersByStatus({ status: 'requested' });
    const httpQuery = await initTestQuery(httpClient.ordersByStatus({ status: 'requested' }));

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'accepted' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersByStatus', {
      status: 'requested',
    });

    // Update requested → accepted (out of filter)
    await processDbOperations([
      db.update(orders).set({ status: 'accepted' }).where(eq(orders.status, 'requested')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersByStatus', {
      status: 'requested',
    });

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- ne (text) -----------------------------------------------------------

  test('ne (text): status != completed', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersNeCompleted();
    const httpQuery = await initTestQuery(httpClient.ordersNeCompleted());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 30, status: 'completed' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersNeCompleted');

    // Update accepted → completed (row exits filter)
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.status, 'accepted')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersNeCompleted');

    // Update requested → cancelled (stays in filter, different value)
    await processDbOperations([
      db.update(orders).set({ status: 'cancelled' }).where(eq(orders.status, 'requested')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersNeCompleted');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- ne (nullable integer: MUST exclude NULL rows) -----------------------

  test('ne (nullable int): driverId != 1 excludes NULL rows', async () => {
    // Insert a second user so we can reference driverId=2
    await insertTestUser(db, `driver2_${randomUUID().slice(0, 8)}`);

    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersDriverNe1();
    const httpQuery = await initTestQuery(httpClient.ordersDriverNe1());

    // Seed: driverId=null (excluded by ne), driverId=1 (excluded by ne), driverId=2 (included)
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: null, pickup: 'A', dropoff: 'B', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 2, pickup: 'E', dropoff: 'F', price: 30, status: 'requested' }),
    ]);
    // Oracle asserts embedded equals HTTP: only the driverId=2 row should appear
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersDriverNe1');

    // Update driverId=2 row to driverId=1 (exits filter)
    await processDbOperations([
      db.update(orders).set({ driverId: 1 }).where(eq(orders.driverId, 2)),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersDriverNe1');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- gt ------------------------------------------------------------------

  test('gt: price > 10', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersPriceGt10();
    const httpQuery = await initTestQuery(httpClient.ordersPriceGt10());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 5, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 20, status: 'requested' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceGt10');

    // Update price=20 to price=5 (exits filter)
    await processDbOperations([db.update(orders).set({ price: 5 }).where(eq(orders.price, 20))]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceGt10');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- gte -----------------------------------------------------------------

  test('gte: price >= 10', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersPriceGte10();
    const httpQuery = await initTestQuery(httpClient.ordersPriceGte10());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 5, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 50, status: 'requested' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceGte10');

    // Update price=10 to price=9 (exits filter at the boundary)
    await processDbOperations([db.update(orders).set({ price: 9 }).where(eq(orders.price, 10))]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceGte10');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- lt ------------------------------------------------------------------

  test('lt: price < 50', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersPriceLt50();
    const httpQuery = await initTestQuery(httpClient.ordersPriceLt50());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 50, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 100, status: 'requested' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceLt50');

    // Update price=10 to price=75 (exits filter)
    await processDbOperations([db.update(orders).set({ price: 75 }).where(eq(orders.price, 10))]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceLt50');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- lte -----------------------------------------------------------------

  test('lte: price <= 50', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersPriceLte50();
    const httpQuery = await initTestQuery(httpClient.ordersPriceLte50());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 50, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 100, status: 'requested' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceLte50');

    // Update price=50 to price=51 (exits filter at the boundary)
    await processDbOperations([db.update(orders).set({ price: 51 }).where(eq(orders.price, 50))]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersPriceLte50');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- in (non-empty) ------------------------------------------------------

  test('in: status in [accepted, requested]', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersStatusIn();
    const httpQuery = await initTestQuery(httpClient.ordersStatusIn());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 30, status: 'completed' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersStatusIn');

    // Update accepted → completed (exits filter)
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.status, 'accepted')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersStatusIn');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- in (empty → zero rows) -----------------------------------------------

  test('in (empty): always returns zero rows', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersStatusInEmpty();
    const httpQuery = await initTestQuery(httpClient.ordersStatusInEmpty());

    // Insert rows — none should match the empty `in`
    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersStatusInEmpty');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- isNull --------------------------------------------------------------

  test('isNull: driverId IS NULL', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersDriverIsNull();
    const httpQuery = await initTestQuery(httpClient.ordersDriverIsNull());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: null, pickup: 'A', dropoff: 'B', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersDriverIsNull');

    // Set driverId on the NULL row (exits filter)
    await processDbOperations([
      db.update(orders).set({ driverId: 1 }).where(eq(orders.status, 'requested')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersDriverIsNull');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- isNotNull -----------------------------------------------------------

  test('isNotNull: driverId IS NOT NULL', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersDriverIsNotNull();
    const httpQuery = await initTestQuery(httpClient.ordersDriverIsNotNull());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: null, pickup: 'A', dropoff: 'B', price: 10, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersDriverIsNotNull');

    // Delete the non-null row
    await processDbOperations([db.delete(orders).where(eq(orders.driverId, 1))]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersDriverIsNotNull');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- NOT -----------------------------------------------------------------

  test('NOT: NOT { status: completed } (incl. NULL-driverId row)', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersNotCompleted();
    const httpQuery = await initTestQuery(httpClient.ordersNotCompleted());

    await processDbOperations([
      // NULL driverId — must appear in NOT(completed) result
      db
        .insert(orders)
        .values({ driverId: null, pickup: 'A', dropoff: 'B', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 30, status: 'completed' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersNotCompleted');

    // Update accepted → completed (exits filter)
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.status, 'accepted')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersNotCompleted');

    // Update completed → cancelled (enters filter)
    await processDbOperations([
      db.update(orders).set({ status: 'cancelled' }).where(eq(orders.status, 'requested')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersNotCompleted');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- AND -----------------------------------------------------------------

  test('AND: status=accepted AND price > 5', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersAnd();
    const httpQuery = await initTestQuery(httpClient.ordersAnd());

    await processDbOperations([
      // satisfies both conjuncts
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'accepted' }),
      // satisfies only price > 5
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
      // satisfies only status=accepted (price=3 fails gt:5)
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 3, status: 'accepted' }),
      // satisfies neither
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'G', dropoff: 'H', price: 2, status: 'completed' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersAnd');

    // Update the matching row to exit on status (accepted → completed)
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.price, 10)),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersAnd');

    collection.dispose();
    httpQuery.destroy();
  });

  // ---- OR ------------------------------------------------------------------

  test('OR: status=accepted OR status=requested', async () => {
    const client = createEmbeddedClient(runtime);
    const collection = await client.ordersOr();
    const httpQuery = await initTestQuery(httpClient.ordersOr());

    await processDbOperations([
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'A', dropoff: 'B', price: 10, status: 'accepted' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'C', dropoff: 'D', price: 20, status: 'requested' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'E', dropoff: 'F', price: 30, status: 'completed' }),
      db
        .insert(orders)
        .values({ driverId: 1, pickup: 'G', dropoff: 'H', price: 40, status: 'cancelled' }),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersOr');

    // Update accepted → completed (exits disjunction)
    await processDbOperations([
      db.update(orders).set({ status: 'completed' }).where(eq(orders.status, 'accepted')),
    ]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersOr');

    // Delete a requested row
    await processDbOperations([db.delete(orders).where(eq(orders.status, 'requested'))]);
    await assertOracleMatch(runtime, collection, httpQuery, 'ordersOr');

    collection.dispose();
    httpQuery.destroy();
  });
});
