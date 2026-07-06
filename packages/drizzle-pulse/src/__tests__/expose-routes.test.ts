import { describe, expect, test } from 'bun:test';
import type { PgAsyncDatabase, PgQueryResultHKT } from 'drizzle-orm/pg-core';
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { Hono } from 'hono';
import SuperJSON from 'superjson';
import { z } from 'zod';
import { pulse } from '../pulse-table.js';
import { resolveEventsTable } from '../server/events-table-resolver.js';
import { RealtimeRequestHandler } from '../server/handlers.js';
import type { PulseAuthContext } from '../server/index.js';
import { createPulseRegistry } from '../server/pulse-registry.js';
import type { RealtimeService } from '../server/realtime-store.js';
import { SubscriptionManager } from '../server/realtime-store.js';
import { serializeResponse } from '../server/superjson-utils.js';
import type {
  PullResponse,
  PullResponseErrorResult,
  SubscribeResponse,
} from '../shared/protocol-types.js';

const ordersTable = pgTable('orders', {
  id: serial('id').primaryKey(),
  status: text('status').notNull(),
  price: integer('price'),
});

const QUERY_DATA = {
  queryName: 'testQuery',
  args: { status: 'requested' },
};

type MockSourceDb = {
  takeRows: () => Record<string, unknown>[];
};

type MockPulseSourceDb = PgAsyncDatabase<PgQueryResultHKT, never>;

type MockDynamicQuery = Promise<Record<string, unknown>[]> & {
  $dynamic(): MockDynamicQuery;
  orderBy(order: unknown): MockDynamicQuery;
  limit(limit: number): Promise<Record<string, unknown>[]>;
};

type SubscribeResponseBody = SubscribeResponse<Record<string, unknown>>;
type PullResponseBody = {
  results?: Record<
    string,
    PullResponse<Record<string, unknown>, Record<string, unknown>> | PullResponseErrorResult
  >;
};

type ErrorBody = {
  error?: string;
};

function getPullResult(
  body: PullResponseBody,
  subscriptionId: string,
): PullResponse<Record<string, unknown>, Record<string, unknown>> | PullResponseErrorResult {
  const result = body.results?.[subscriptionId];
  if (!result) {
    throw new Error(`Missing pull result for subscription ${subscriptionId}`);
  }

  return result;
}

function createRegistry() {
  return createPulseRegistry({
    testQuery: pulse(ordersTable)
      .query()
      .args(z.object({ status: z.string() }))
      .order('asc')
      .limit(2)
      .query((ctx) => ctx.query({ status: ctx.args.status })),
  });
}

function createMockSourceDb(batches: Record<string, unknown>[][]): MockSourceDb {
  let idx = 0;

  const takeRows = () => {
    const rows = batches[idx] ?? [];
    idx += 1;
    return rows;
  };

  return {
    takeRows() {
      return takeRows();
    },
  };
}

function createPulseSourceDbMock(rowsProvider: () => Record<string, unknown>[]): MockPulseSourceDb {
  const createQuery = (): MockDynamicQuery => {
    const rows = rowsProvider();
    const query = Promise.resolve(rows) as MockDynamicQuery;
    query.$dynamic = () => query;
    query.orderBy = () => query;
    query.limit = () => Promise.resolve(rows);

    return query;
  };

  return {
    select() {
      return {
        from() {
          return {
            where() {
              return createQuery();
            },
          };
        },
      };
    },
  } as unknown as MockPulseSourceDb;
}

function createRealtimeServiceMock(data?: {
  latestSnapshot?: number;
  events?: Record<string, unknown>[];
}): Pick<RealtimeService, 'getDb' | 'getLatestSnapshot'> {
  const latestSnapshot = data?.latestSnapshot ?? 0;
  const events = data?.events ?? [];

  return {
    getDb() {
      const createQuery = () => {
        const promise = Promise.resolve(events) as Promise<Record<string, unknown>[]> & {
          orderBy(order: unknown): Promise<Record<string, unknown>[]>;
        };
        promise.orderBy = () => Promise.resolve(events);
        return promise;
      };

      return {
        select() {
          return {
            from() {
              return {
                where() {
                  return createQuery();
                },
              };
            },
          };
        },
      } as unknown as ReturnType<RealtimeService['getDb']>;
    },
    async getLatestSnapshot(_table: unknown) {
      return latestSnapshot;
    },
  };
}

async function decodeBody<T extends Record<string, unknown>>(response: Response): Promise<T> {
  const raw = await response.text();
  const parsed = SuperJSON.parse(raw);
  if (parsed && typeof parsed === 'object') {
    return parsed as T;
  }
  return {} as T;
}

function createRouterHarness(params?: {
  sourceRows?: Record<string, unknown>[][];
  latestSnapshot?: number;
  events?: Record<string, unknown>[];
}) {
  const registry = createRegistry();
  const subscriptionManager = new SubscriptionManager();
  const sourceDb = createMockSourceDb(params?.sourceRows ?? [[]]);
  const pulseSourceDb = createPulseSourceDbMock(() => sourceDb.takeRows());
  const realtimeService = createRealtimeServiceMock({
    latestSnapshot: params?.latestSnapshot,
    events: params?.events,
  });
  const requestHandler = new RealtimeRequestHandler(
    registry,
    pulseSourceDb,
    subscriptionManager,
    () => realtimeService,
    () => resolveEventsTable(ordersTable),
  );

  const router = new Hono();
  const auth: PulseAuthContext = { userId: null };
  const clientId = 'client-1';
  const toResponse = (body: unknown, status: number) =>
    new Response(serializeResponse(body), {
      status,
      headers: { 'Content-Type': 'application/json' },
    });

  router.post('/subscribe', async (c) => {
    const request = await c.req.json();
    const result = await requestHandler.subscribe(request, auth);
    return toResponse(result.body, result.status);
  });

  router.post('/pull', async (c) => {
    const request = await c.req.json();
    const result = await requestHandler.pull(request, auth);
    return toResponse(result.body, result.status);
  });

  router.post('/load-more', async (c) => {
    const request = await c.req.json();
    const result = await requestHandler.loadMore(request, auth);
    return toResponse(result.body, result.status);
  });

  return { router, clientId };
}

// These tests cover request-shape validation and guard clauses that need no
// real DB. Result-shaping behavior (row/event content produced by an actual
// query) is proven against live PostgreSQL in
// packages/integration-tests/src/{runtime-contracts,client-state,consistency-oracle}.test.ts
// rather than re-tested here against a fabricated drizzle mock.
describe('expose routes request validation', () => {
  test('subscribe returns HTTP 400 for invalid queryName', async () => {
    const { router, clientId } = createRouterHarness({
      sourceRows: [[{ id: 101, status: 'requested', price: 20 }]],
    });

    const response = await router.request('/subscribe', {
      method: 'POST',
      body: JSON.stringify({ clientId, queryName: 'nope', args: { status: 'requested' } }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await decodeBody<ErrorBody>(response);

    expect(response.status).toBe(400);
    expect(body.error).toBe('Unknown query: "nope"');
  });

  test('subscribe re-uses existing subscriptionId when provided', async () => {
    const { router, clientId } = createRouterHarness({
      sourceRows: [
        [{ id: 101, status: 'requested', price: 20 }],
        [{ id: 102, status: 'requested', price: 25 }],
      ],
      latestSnapshot: 2,
    });

    const first = await router.request('/subscribe', {
      method: 'POST',
      body: JSON.stringify({ ...QUERY_DATA, clientId }),
      headers: { 'Content-Type': 'application/json' },
    });
    const firstBody = await decodeBody<SubscribeResponseBody>(first);

    const second = await router.request('/subscribe', {
      method: 'POST',
      body: JSON.stringify({ ...QUERY_DATA, clientId, subscriptionId: firstBody.subscriptionId }),
      headers: { 'Content-Type': 'application/json' },
    });
    const secondBody = await decodeBody<SubscribeResponseBody>(second);

    expect(second.status).toBe(200);
    expect(secondBody.subscriptionId).toBe(firstBody.subscriptionId);
  });

  test('pull returns empty results when no subscriptions provided', async () => {
    const { router, clientId } = createRouterHarness();

    const response = await router.request('/pull', {
      method: 'POST',
      body: JSON.stringify({ clientId, subscriptions: [] }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await decodeBody<PullResponseBody>(response);

    expect(response.status).toBe(200);
    expect(body.results).toEqual({});
  });

  test('pull returns reset for unknown subscriptionId', async () => {
    const { router, clientId } = createRouterHarness();

    const first = await router.request('/subscribe', {
      method: 'POST',
      body: JSON.stringify({ ...QUERY_DATA, clientId }),
      headers: { 'Content-Type': 'application/json' },
    });
    await decodeBody<SubscribeResponseBody>(first);

    const response = await router.request('/pull', {
      method: 'POST',
      body: JSON.stringify({
        clientId,
        subscriptions: [{ subscriptionId: 'missing-sub', snapshot: 1 }],
      }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await decodeBody<PullResponseBody>(response);
    const result = getPullResult(body, 'missing-sub');

    expect(response.status).toBe(200);
    expect(result.reset).toBe(true);
    if (!('reason' in result)) {
      throw new Error('Expected pull error result with reason');
    }
    expect(result.reason).toBe('subscription_not_found');
  });

  test('load-more returns 400 for missing subscriptionId', async () => {
    const { router } = createRouterHarness();

    const response = await router.request('/load-more', {
      method: 'POST',
      body: JSON.stringify({ cursor: 100 }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await decodeBody<ErrorBody>(response);

    expect(response.status).toBe(400);
    expect(body.error).toBe('missing_client_id');
  });

  test('load-more returns 404 for unknown subscriptionId', async () => {
    const { router, clientId } = createRouterHarness();

    const response = await router.request('/load-more', {
      method: 'POST',
      body: JSON.stringify({ clientId, subscriptionId: 'missing-sub', cursor: 100 }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await decodeBody<ErrorBody>(response);

    expect(response.status).toBe(404);
    expect(body.error).toBe('subscription_not_found');
  });

  test('load-more returns 400 for missing cursor', async () => {
    const { router, clientId } = createRouterHarness({
      sourceRows: [[{ id: 101, status: 'requested', price: 20 }]],
      latestSnapshot: 2,
    });

    const subscribe = await router.request('/subscribe', {
      method: 'POST',
      body: JSON.stringify({ ...QUERY_DATA, clientId }),
      headers: { 'Content-Type': 'application/json' },
    });
    const subscribeBody = await decodeBody<SubscribeResponseBody>(subscribe);

    const response = await router.request('/load-more', {
      method: 'POST',
      body: JSON.stringify({ clientId, subscriptionId: subscribeBody.subscriptionId }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await decodeBody<ErrorBody>(response);

    expect(response.status).toBe(400);
    expect(body.error).toBe('missing_cursor');
  });

  test('load-more returns 400 for invalid cursor type', async () => {
    const { router, clientId } = createRouterHarness({
      sourceRows: [[{ id: 101, status: 'requested', price: 20 }]],
      latestSnapshot: 2,
    });

    const subscribe = await router.request('/subscribe', {
      method: 'POST',
      body: JSON.stringify({ ...QUERY_DATA, clientId }),
      headers: { 'Content-Type': 'application/json' },
    });
    const subscribeBody = await decodeBody<SubscribeResponseBody>(subscribe);

    const response = await router.request('/load-more', {
      method: 'POST',
      body: JSON.stringify({
        clientId,
        subscriptionId: subscribeBody.subscriptionId,
        cursor: { bad: true },
      }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await decodeBody<ErrorBody>(response);

    expect(response.status).toBe(400);
    expect(body.error).toBe('invalid_cursor');
  });
});
