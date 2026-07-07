import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { eq, sql } from 'drizzle-orm';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseRegistry } from 'drizzle-pulse/server';
import type { Hono } from 'hono';
import type { Pool } from 'pg';
import SuperJSON from 'superjson';
import { fullOrdersFixture } from './fixtures/full-orders/index.js';
import type { HarnessProcessDbOperations, RuntimeOf } from './helpers/test-harness.js';
import {
  cleanupBetweenTestsForFixture,
  createRealtimeRouterWithAuth,
  insertTestUser,
  pullClient,
  setupTestSuiteForFixture,
  subscribeClient,
  teardownTestSuiteForFixture,
  waitForEventsForFixture,
} from './helpers/test-harness.js';

describe('Runtime Contracts', () => {
  let router: Hono;
  let pool: Pool;
  let db: PostgresJsDatabase;
  let processDbOperations: HarnessProcessDbOperations;
  let runtime!: RuntimeOf<typeof registry>;

  const fixture = fullOrdersFixture;
  const { orders } = fixture.tables;
  const ordersByStatus = pulse(orders)
    .args(fixture.schemas.ordersByStatusArgs)
    .order('desc')
    .limit(5)
    .query((ctx) => ctx.query({ status: ctx.args.status }));
  const authScopedByStatus = pulse(orders)
    .args(fixture.schemas.ordersByStatusArgs)
    .order('desc')
    .limit(2)
    .query((ctx) =>
      ctx.query({
        AND: [
          { status: ctx.args.status },
          { OR: [{ driverId: { isNull: true } }, { driverId: ctx.auth.userId! }] },
        ],
      }),
    );
  const registry = createPulseRegistry({ ordersByStatus, authScopedByStatus });

  beforeAll(async () => {
    const setup = await setupTestSuiteForFixture(fixture, registry);
    router = setup.router;
    pool = setup.pool;
    db = setup.db;
    processDbOperations = setup.processDbOperations;
    runtime = setup.runtime;
  });

  afterAll(async () => {
    await teardownTestSuiteForFixture(fixture, registry);
  });

  beforeEach(async () => {
    await cleanupBetweenTestsForFixture(fixture, pool);
    await insertTestUser(db, `driver_${randomUUID().slice(0, 8)}`);
  });

  test('subscribe to empty table returns exact empty response contract', async () => {
    const subscription = await subscribeClient(router, 'ordersByStatus', { status: 'requested' });

    expect(subscription.rows).toEqual([]);
    expect(subscription.rangeStart).toBe(null);
    expect(subscription.rangeEnd).toBe(null);
    expect(subscription.snapshot).toBe(0);
  });

  test('subscribe after insert returns exact single-row ranges', async () => {
    await processDbOperations([
      db.insert(orders).values({
        driverId: 1,
        pickup: 'Smoke Pickup',
        dropoff: 'Smoke Dropoff',
        price: 55,
        status: 'requested',
      }),
    ]);

    const subscription = await subscribeClient(router, 'ordersByStatus', { status: 'requested' });

    expect(subscription.rows).toHaveLength(1);
    expect(subscription.rangeStart).toBe(1);
    expect(subscription.rangeEnd).toBe(1);
    expect(subscription.snapshot).toBe(1);
  });

  test('pull response has exact insert event contract', async () => {
    const subscription = await subscribeClient(router, 'ordersByStatus', { status: 'requested' });

    await processDbOperations([
      db.insert(orders).values({
        driverId: 1,
        pickup: 'Pull Contract Pickup',
        dropoff: 'Pull Contract Dropoff',
        price: 65,
        status: 'requested',
      }),
    ]);

    const pullResponse = await pullClient(
      router,
      subscription.clientId,
      subscription.subscriptionId,
      subscription.snapshot,
    );

    expect(pullResponse.events).toHaveLength(1);
    expect(pullResponse.events[0]).toEqual(
      expect.objectContaining({
        op: 'insert',
        pk: 1,
        row: expect.objectContaining({
          pickup: 'Pull Contract Pickup',
          dropoff: 'Pull Contract Dropoff',
          price: 65,
          status: 'requested',
        }),
      }),
    );
    expect(pullResponse.rangeStart).toBe(1);
    expect(pullResponse.rangeEnd).toBe(1);
    expect(pullResponse.snapshot).toBe(1);
  });

  test('transition out of filter exposes exact update flags', async () => {
    const seededRows = await db
      .insert(orders)
      .values({
        driverId: 1,
        pickup: 'Transition Out Pickup',
        dropoff: 'Transition Out Dropoff',
        price: 110,
        status: 'requested',
      })
      .returning({ id: orders.id });
    expect(seededRows).toHaveLength(1);
    const seededId = seededRows[0]!.id;

    await waitForEventsForFixture(fixture, pool, 0, 1);

    const subscription = await subscribeClient(router, 'ordersByStatus', { status: 'requested' });
    expect(subscription.rows).toHaveLength(1);

    await processDbOperations([
      db.update(orders).set({ status: 'accepted' }).where(eq(orders.id, seededId)),
    ]);

    const updatePull = await pullClient(
      router,
      subscription.clientId,
      subscription.subscriptionId,
      subscription.snapshot,
    );

    expect(updatePull.events).toHaveLength(1);
    expect(updatePull.events[0]).toEqual(
      expect.objectContaining({
        op: 'update',
        matchesOld: true,
        matchesNew: false,
      }),
    );
  });

  test('transition into filter exposes exact update flags', async () => {
    const seededRows = await db
      .insert(orders)
      .values({
        driverId: 1,
        pickup: 'Transition In Pickup',
        dropoff: 'Transition In Dropoff',
        price: 120,
        status: 'completed',
      })
      .returning({ id: orders.id });
    expect(seededRows).toHaveLength(1);
    const seededId = seededRows[0]!.id;

    await waitForEventsForFixture(fixture, pool, 0, 1);

    const subscription = await subscribeClient(router, 'ordersByStatus', { status: 'requested' });
    expect(subscription.rows).toEqual([]);

    await processDbOperations([
      db.update(orders).set({ status: 'requested' }).where(eq(orders.id, seededId)),
    ]);

    const updatePull = await pullClient(
      router,
      subscription.clientId,
      subscription.subscriptionId,
      subscription.snapshot,
    );

    expect(updatePull.events).toHaveLength(1);
    expect(updatePull.events[0]).toEqual(
      expect.objectContaining({
        op: 'update',
        matchesOld: false,
        matchesNew: true,
      }),
    );
  });

  test('stale snapshot gets exact reset response after snapshot baseline reset', async () => {
    const subscription = await subscribeClient(router, 'ordersByStatus', { status: 'requested' });
    const staleSnapshot = subscription.snapshot;
    const staleSubscriptionId = subscription.subscriptionId;

    await teardownTestSuiteForFixture(fixture, registry);
    const restarted = await setupTestSuiteForFixture(fixture, registry);
    router = restarted.router;
    pool = restarted.pool;
    db = restarted.db;
    processDbOperations = restarted.processDbOperations;
    runtime = restarted.runtime;
    await insertTestUser(db, `driver_${randomUUID().slice(0, 8)}`);

    await router.request('/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId: subscription.clientId,
        queryName: 'ordersByStatus',
        args: { status: 'requested' },
        subscriptionId: staleSubscriptionId,
      }),
    });

    await db.execute(sql`TRUNCATE TABLE drizzle_pulse.__events_public_orders RESTART IDENTITY`);
    await db.execute(
      sql`INSERT INTO drizzle_pulse.__events_public_orders (id, "$op") OVERRIDING SYSTEM VALUE VALUES (1, 'snapshot')`,
    );
    await db.execute(
      sql`SELECT setval(pg_get_serial_sequence('drizzle_pulse.__events_public_orders', '$snapshot'), 1, true)`,
    );

    const pullResponse = await pullClient(
      router,
      subscription.clientId,
      subscription.subscriptionId,
      staleSnapshot,
    );
    expect(pullResponse.reset).toBe(true);
    expect(pullResponse.reason).toBe('snapshot');
    expect(pullResponse.events).toEqual([]);
  });

  test('two subscriptions only receive events matching their status filters', async () => {
    const requestedSubscription = await subscribeClient(router, 'ordersByStatus', {
      status: 'requested',
    });
    const acceptedSubscription = await subscribeClient(router, 'ordersByStatus', {
      status: 'accepted',
    });

    const requestedInsertRows = await db
      .insert(orders)
      .values({
        driverId: 1,
        pickup: 'Isolation Requested Pickup',
        dropoff: 'Isolation Requested Dropoff',
        price: 101,
        status: 'requested',
      })
      .returning({ id: orders.id });
    expect(requestedInsertRows).toHaveLength(1);
    const requestedInsertId = requestedInsertRows[0]!.id;

    await waitForEventsForFixture(fixture, pool, requestedSubscription.snapshot, 1);

    const requestedPhaseOne = await pullClient(
      router,
      requestedSubscription.clientId,
      requestedSubscription.subscriptionId,
      requestedSubscription.snapshot,
    );
    const acceptedPhaseOne = await pullClient(
      router,
      acceptedSubscription.clientId,
      acceptedSubscription.subscriptionId,
      acceptedSubscription.snapshot,
    );

    expect(requestedPhaseOne.events).toHaveLength(1);
    expect(requestedPhaseOne.events[0]).toEqual(
      expect.objectContaining({
        op: 'insert',
        pk: requestedInsertId,
        row: expect.objectContaining({ status: 'requested' }),
      }),
    );
    expect(acceptedPhaseOne.events).toEqual([]);

    const acceptedInsertRows = await db
      .insert(orders)
      .values({
        driverId: 1,
        pickup: 'Isolation Accepted Pickup',
        dropoff: 'Isolation Accepted Dropoff',
        price: 102,
        status: 'accepted',
      })
      .returning({ id: orders.id });
    expect(acceptedInsertRows).toHaveLength(1);
    const acceptedInsertId = acceptedInsertRows[0]!.id;

    await waitForEventsForFixture(fixture, pool, requestedPhaseOne.snapshot, 1);

    const requestedPhaseTwo = await pullClient(
      router,
      requestedSubscription.clientId,
      requestedSubscription.subscriptionId,
      requestedPhaseOne.snapshot,
    );
    const acceptedPhaseTwo = await pullClient(
      router,
      acceptedSubscription.clientId,
      acceptedSubscription.subscriptionId,
      acceptedPhaseOne.snapshot,
    );

    expect(requestedPhaseTwo.events).toEqual([]);
    expect(acceptedPhaseTwo.events).toHaveLength(1);
    expect(acceptedPhaseTwo.events[0]).toEqual(
      expect.objectContaining({
        op: 'insert',
        pk: acceptedInsertId,
        row: expect.objectContaining({ status: 'accepted' }),
      }),
    );
  });

  test('single batched pull returns results for multiple subscriptions under one client', async () => {
    const clientId = randomUUID();
    const requestedSubscribe = await router.request('/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId,
        queryName: 'ordersByStatus',
        args: { status: 'requested' },
      }),
    });
    const acceptedSubscribe = await router.request('/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId,
        queryName: 'ordersByStatus',
        args: { status: 'accepted' },
      }),
    });
    const requestedSubscription = SuperJSON.parse(await requestedSubscribe.text()) as {
      clientId: string;
      subscriptionId: string;
      snapshot: number;
    };
    const acceptedSubscription = SuperJSON.parse(await acceptedSubscribe.text()) as {
      clientId: string;
      subscriptionId: string;
      snapshot: number;
    };

    await processDbOperations([
      db.insert(orders).values({
        driverId: 1,
        pickup: 'Batched Requested Pickup',
        dropoff: 'Batched Requested Dropoff',
        price: 301,
        status: 'requested',
      }),
      db.insert(orders).values({
        driverId: 1,
        pickup: 'Batched Accepted Pickup',
        dropoff: 'Batched Accepted Dropoff',
        price: 302,
        status: 'accepted',
      }),
    ]);

    const response = await router.request('/pull', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId,
        subscriptions: [
          {
            subscriptionId: requestedSubscription.subscriptionId,
            snapshot: requestedSubscription.snapshot,
          },
          {
            subscriptionId: acceptedSubscription.subscriptionId,
            snapshot: acceptedSubscription.snapshot,
          },
        ],
      }),
    });
    const parsed = SuperJSON.parse(await response.text()) as {
      results: Record<string, { events: unknown[] }>;
    };

    expect(response.status).toBe(200);
    expect(Object.keys(parsed.results).sort()).toEqual(
      [requestedSubscription.subscriptionId, acceptedSubscription.subscriptionId].sort(),
    );
    expect(parsed.results[requestedSubscription.subscriptionId]?.events).toHaveLength(1);
    expect(parsed.results[acceptedSubscription.subscriptionId]?.events).toHaveLength(1);
  });

  test('auth-scoped subscriptions keep subscribe, pull, and load-more tied to the same user', async () => {
    // Reuse the suite's existing runtime (from beforeAll) instead of calling
    // setupTestSuiteForFixture() again — an extra call here acquired a second reference
    // that this test never released, leaving activeSuiteUsers stuck above 0 after
    // afterAll's single teardown call.
    const driverOne = await insertTestUser(db, `auth_driver_${randomUUID().slice(0, 8)}`);
    const driverTwo = await insertTestUser(db, `auth_driver_${randomUUID().slice(0, 8)}`);

    const driverOneRouter = createRealtimeRouterWithAuth(runtime, {
      userId: driverOne.id,
    });
    const driverTwoRouter = createRealtimeRouterWithAuth(runtime, {
      userId: driverTwo.id,
    });

    await processDbOperations([
      db.insert(orders).values({
        driverId: driverOne.id,
        pickup: 'Auth Driver One Existing Pickup',
        dropoff: 'Auth Driver One Existing Dropoff',
        price: 201,
        status: 'requested',
      }),
      db.insert(orders).values({
        driverId: driverTwo.id,
        pickup: 'Auth Driver Two Existing Pickup',
        dropoff: 'Auth Driver Two Existing Dropoff',
        price: 202,
        status: 'requested',
      }),
      db.insert(orders).values({
        driverId: null,
        pickup: 'Auth Shared Existing Pickup',
        dropoff: 'Auth Shared Existing Dropoff',
        price: 203,
        status: 'requested',
      }),
    ]);

    const driverOneSubscription = await subscribeClient(driverOneRouter, 'authScopedByStatus', {
      status: 'requested',
    });
    const driverTwoSubscription = await subscribeClient(driverTwoRouter, 'authScopedByStatus', {
      status: 'requested',
    });

    expect(driverOneSubscription.rows.map((row) => row.price)).toEqual([203, 201]);
    expect(driverTwoSubscription.rows.map((row) => row.price)).toEqual([203, 202]);

    await processDbOperations([
      db.insert(orders).values({
        driverId: driverOne.id,
        pickup: 'Auth Driver One New Pickup',
        dropoff: 'Auth Driver One New Dropoff',
        price: 204,
        status: 'requested',
      }),
    ]);

    const driverOnePull = await pullClient(
      driverOneRouter,
      driverOneSubscription.clientId,
      driverOneSubscription.subscriptionId,
      driverOneSubscription.snapshot,
    );
    const driverTwoPull = await pullClient(
      driverTwoRouter,
      driverTwoSubscription.clientId,
      driverTwoSubscription.subscriptionId,
      driverTwoSubscription.snapshot,
    );

    expect(driverOnePull.events).toHaveLength(1);
    expect(driverOnePull.events[0]).toEqual(
      expect.objectContaining({
        op: 'insert',
        row: expect.objectContaining({ price: 204 }),
      }),
    );
    expect(driverTwoPull.events).toEqual([]);

    const crossUserPull = await pullClient(
      driverTwoRouter,
      driverOneSubscription.clientId,
      driverOneSubscription.subscriptionId,
      driverOneSubscription.snapshot,
    );
    expect(crossUserPull.reset).toBe(true);
    expect(crossUserPull.reason).toBe('subscription_not_found');

    const driverOneLoadMore = await driverOneRouter.request('/load-more', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId: driverOneSubscription.clientId,
        subscriptionId: driverOneSubscription.subscriptionId,
        cursor: 201,
      }),
    });
    expect(driverOneLoadMore.status).toBe(200);
    const driverOneLoadMoreBody = (await driverOneLoadMore.json()) as {
      json: { rows: Array<{ price: number }> };
    };
    expect(driverOneLoadMoreBody.json.rows.map((row) => row.price)).toEqual([204, 203]);

    const crossUserLoadMore = await driverTwoRouter.request('/load-more', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId: driverOneSubscription.clientId,
        subscriptionId: driverOneSubscription.subscriptionId,
        cursor: 201,
      }),
    });
    expect(crossUserLoadMore.status).toBe(404);
    const crossUserLoadMoreBody = (await crossUserLoadMore.json()) as {
      json: { error: string };
    };
    expect(crossUserLoadMoreBody.json.error).toBe('subscription_not_found');
  });
});
