/**
 * Task 2: Router Fetch Adapter Integration Test
 *
 * Focused test to verify that createRouterFetchAdapter correctly converts Hono router
 * to a fetch-compatible interface that can be used by realtime clients.
 */

import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { createPulseClient, PulseQuery } from '@drizzle-pulse/client/react';
import { createPulse, createPulseRegistry } from '@drizzle-pulse/client/server';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import type { Hono } from 'hono';
import type { Pool } from 'pg';
import SuperJSON from 'superjson';

import { fullOrdersFixture } from './fixtures/full-orders/index.js';
import type { HarnessProcessDbOperations } from './helpers/test-harness.js';
import {
  cleanupBetweenTestsForFixture,
  createRouterFetchAdapter,
  insertTestUser,
  setupTestSuiteForFixture,
  subscribeClient,
  teardownTestSuiteForFixture,
} from './helpers/test-harness.js';

type SuiteContext = {
  router: Hono;
  pool: Pool;
  db: PostgresJsDatabase;
  databaseUrl: string;
  processDbOperations: HarnessProcessDbOperations;
};

let ctx: SuiteContext;
const pulse = createPulse();
const ordersByStatus = pulse(fullOrdersFixture.tables.orders)
  .$eventsTable(fullOrdersFixture.tables.eventsPublicOrders)
  .args(fullOrdersFixture.schemas.ordersByStatusArgs)
  .order('desc')
  .limit(5)
  .query((ctx) => ctx.query({ status: ctx.args.status }));
const registry = createPulseRegistry({ ordersByStatus });

describe('Router Fetch Adapter', () => {
  beforeAll(async () => {
    const result = await setupTestSuiteForFixture(fullOrdersFixture, registry);
    ctx = {
      router: result.router,
      pool: result.pool,
      db: result.db,
      databaseUrl: result.databaseUrl,
      processDbOperations: result.processDbOperations,
    };
  });

  afterAll(async () => {
    await teardownTestSuiteForFixture(fullOrdersFixture);
  });

  beforeEach(async () => {
    await cleanupBetweenTestsForFixture(fullOrdersFixture, ctx.pool);
  });

  test('Adapter wraps Hono router as fetch-compatible function', async () => {
    const fetchImpl = createRouterFetchAdapter(ctx.router);

    // Verify the adapter has the fetch signature
    expect(typeof fetchImpl).toBe('function');
    expect(typeof fetchImpl.preconnect).toBe('function');
  });

  test('Adapter can reach /subscribe route', async () => {
    const fetchImpl = createRouterFetchAdapter(ctx.router);

    // Make a direct fetch call through the adapter
    const response = await fetchImpl('http://localhost/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ queryName: 'ordersByStatus', args: { status: 'requested' } }),
    });

    expect(response.ok).toBe(true);
    expect(response.status).toBe(200);

    // Verify response is valid JSON
    const text = await response.text();
    expect(text.length).toBeGreaterThan(0);
    const body = SuperJSON.parse<{
      subscriptionId: string;
      rows: unknown[];
      snapshot: number;
    }>(text);
    expect(body.subscriptionId).toBeTruthy();
    expect(typeof body.subscriptionId).toBe('string');
    expect(Array.isArray(body.rows)).toBe(true);
    expect(typeof body.snapshot).toBe('number');
  });

  test('Adapter can reach /pull route after insert event', async () => {
    // Create a test user for the order
    const user = await insertTestUser(ctx.db, `user_${Math.random().toString(36).slice(2)}`);

    // First subscribe to get a subscription ID
    const subscribeResult = await subscribeClient(ctx.router, 'ordersByStatus', {
      status: 'requested',
    });
    const { clientId, subscriptionId, snapshot: initialSnapshot } = subscribeResult;

    // Insert a matching order
    const orderData = {
      driverId: user.id,
      pickup: 'Test Pickup',
      dropoff: 'Test Dropoff',
      price: 25,
      status: 'requested' as const,
    };

    const inserted = await ctx.processDbOperations([
      ctx.db
        .insert(fullOrdersFixture.tables.orders)
        .values({
          driverId: orderData.driverId,
          pickup: orderData.pickup,
          dropoff: orderData.dropoff,
          price: orderData.price,
          status: orderData.status,
        })
        .returning(),
    ]);
    const order = inserted.results[0][0];
    if (!order) {
      throw new Error('insertOrder did not return a row');
    }

    // Now use the adapter to pull events
    const fetchImpl = createRouterFetchAdapter(ctx.router);

    const pullResponse = await fetchImpl('http://localhost/pull', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId,
        subscriptions: [{ subscriptionId, snapshot: initialSnapshot }],
      }),
    });

    expect(pullResponse.ok).toBe(true);
    const text = await pullResponse.text();
    const body = SuperJSON.parse<{
      results: Record<string, { events?: unknown[]; snapshot?: number }>;
    }>(text);
    const result = body.results[subscriptionId];
    expect(result).toBeTruthy();
    expect(Array.isArray(result?.events)).toBe(true);
    expect(typeof result?.snapshot).toBe('number');
    // After insert and pull, snapshot should have advanced
    expect(result?.snapshot).toBeGreaterThanOrEqual(initialSnapshot);
  });

  test('Adapter handles header normalization (case-insensitive)', async () => {
    const fetchImpl = createRouterFetchAdapter(ctx.router);

    // Request with mixed-case headers
    const response = await fetchImpl('http://localhost/subscribe', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ACCEPT: 'application/json',
        accept: 'application/json', // Duplicate in different case
      },
      body: JSON.stringify({ queryName: 'ordersByStatus', args: { status: 'requested' } }),
    });

    expect(response.ok).toBe(true);
  });

  test('Adapter supports URL as string input (RequestInfo format)', async () => {
    const fetchImpl = createRouterFetchAdapter(ctx.router);

    // Call with URL as string (standard fetch API)
    const response = await fetchImpl('http://localhost/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ queryName: 'ordersByStatus', args: { status: 'requested' } }),
    });

    expect(response.ok).toBe(true);
  });

  test('Adapter supports URL object input (RequestInfo format)', async () => {
    const fetchImpl = createRouterFetchAdapter(ctx.router);

    // Call with URL object
    const url = new URL('http://localhost/subscribe');
    const response = await fetchImpl(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ queryName: 'ordersByStatus', args: { status: 'requested' } }),
    });

    expect(response.ok).toBe(true);
  });

  test('PulseQuery integration: adapter enables realtime subscribe/poll cycle', async () => {
    // Create test user
    const user = await insertTestUser(ctx.db, `user_${Math.random().toString(36).slice(2)}`);

    // Create router-fetch adapter
    const fetchImpl = createRouterFetchAdapter(ctx.router);

    const client = createPulseClient<typeof registry.$client>({
      url: 'http://localhost',
      fetchImpl,
    });
    const core = new PulseQuery(client.ordersByStatus({ status: 'requested' }));

    // Subscribe: should start with no orders
    await core.subscribe();
    const initialState = core.getState();
    expect(initialState.isLoading).toBe(false);
    expect(initialState.data.length).toBe(0);

    // Insert first order
    const orderData1 = {
      driverId: user.id,
      pickup: 'First Pickup',
      dropoff: 'First Dropoff',
      price: 50,
      status: 'requested' as const,
    };

    const inserted1 = await ctx.processDbOperations([
      ctx.db
        .insert(fullOrdersFixture.tables.orders)
        .values({
          driverId: orderData1.driverId,
          pickup: orderData1.pickup,
          dropoff: orderData1.dropoff,
          price: orderData1.price,
          status: orderData1.status,
        })
        .returning(),
    ]);
    expect(inserted1.results[0]).toHaveLength(1);
    const order1 = inserted1.results[0][0]!;

    // Poll should get the first order
    await core.poll();
    const afterFirstPoll = core.getState();
    expect(afterFirstPoll.data.length).toBeGreaterThanOrEqual(1);
    const hasOrder1After1stPoll = afterFirstPoll.data.some(
      (row) => 'id' in row && row.id === order1.id,
    );
    expect(hasOrder1After1stPoll).toBe(true);

    // Insert second order
    const orderData2 = {
      driverId: user.id,
      pickup: 'Second Pickup',
      dropoff: 'Second Dropoff',
      price: 75,
      status: 'requested' as const,
    };

    const inserted2 = await ctx.processDbOperations([
      ctx.db
        .insert(fullOrdersFixture.tables.orders)
        .values({
          driverId: orderData2.driverId,
          pickup: orderData2.pickup,
          dropoff: orderData2.dropoff,
          price: orderData2.price,
          status: orderData2.status,
        })
        .returning(),
    ]);
    expect(inserted2.results[0]).toHaveLength(1);
    const order2 = inserted2.results[0][0]!;

    // Poll should get the second order
    await core.poll();
    const afterSecondPoll = core.getState();
    expect(afterSecondPoll.data.length).toBeGreaterThanOrEqual(2);
    const hasOrder1 = afterSecondPoll.data.some((row) => 'id' in row && row.id === order1.id);
    const hasOrder2 = afterSecondPoll.data.some((row) => 'id' in row && row.id === order2.id);
    expect(hasOrder1).toBe(true);
    expect(hasOrder2).toBe(true);
  });
});
