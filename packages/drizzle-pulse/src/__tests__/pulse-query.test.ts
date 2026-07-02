import { describe, expect, test } from 'bun:test';
import SuperJSON from 'superjson';
import { createPulseClient } from '../react/create-client.js';
import { PulseQuery } from '../react/pulse-query.js';
import type { PullResponse } from '../shared/protocol-types.js';
import type { QueryDescriptor } from '../types.js';

type TestRow = {
  $pk: number;
  label: string;
};

type FetchCall = {
  url: string;
  body: unknown;
};

function serialize(value: unknown): string {
  return SuperJSON.stringify(value);
}

function createQueuedFetch(responses: Response[]): { fetchImpl: typeof fetch; calls: FetchCall[] } {
  const queue = [...responses];
  const calls: FetchCall[] = [];

  const fetchImpl = Object.assign(
    async (input: URL | RequestInfo, init?: RequestInit) => {
      const next = queue.shift();
      if (!next) {
        throw new Error('No queued response available for fetch call');
      }

      const url = typeof input === 'string' ? input : input.toString();
      const bodyText = typeof init?.body === 'string' ? init.body : '';
      const body = bodyText.length > 0 ? JSON.parse(bodyText) : null;
      calls.push({ url, body });
      return next;
    },
    { preconnect() {} },
  ) satisfies typeof fetch;

  return { fetchImpl, calls };
}

function createTestClient(fetchImpl: typeof fetch) {
  return createPulseClient<{
    ordersByStatus(args?: Record<string, unknown>): QueryDescriptor<TestRow>;
  }>({
    url: '/api/realtime',
    fetchImpl,
  });
}

function makePullResponse(results: Record<string, PullResponse<TestRow, unknown>>): Response {
  return new Response(serialize({ results }), { status: 200 });
}

describe('PulseQuery runtime characterization', () => {
  test('subscribe initializes runtime state', async () => {
    const subscribeResponse = new Response(
      serialize({
        subscriptionId: 'sub-1',
        rows: [
          { $pk: 1, label: 'a' },
          { $pk: 2, label: 'b' },
        ],
        rangeStart: 1,
        rangeEnd: 2,
        snapshot: 4,
        order: 'asc',
        limit: 2,
        hasMore: true,
      }),
      { status: 200 },
    );

    const { fetchImpl, calls } = createQueuedFetch([subscribeResponse]);
    const core = new PulseQuery(
      createTestClient(fetchImpl).ordersByStatus({ status: 'requested' }),
    );

    await core.subscribe();

    expect(core.getState().isLoading).toBe(false);
    expect(core.getState().hasMore).toBe(true);
    expect(core.getState().data).toEqual([
      { $pk: 1, label: 'a' },
      { $pk: 2, label: 'b' },
    ]);
    expect(calls[0]?.body).toEqual({
      clientId: expect.any(String),
      queryName: 'ordersByStatus',
      args: { status: 'requested' },
    });
  });

  test('applyEvents follows insert/update/delete merge intent', async () => {
    const subscribeResponse = new Response(
      serialize({
        subscriptionId: 'sub-2',
        rows: [{ $pk: 2, label: 'b' }],
        rangeStart: 2,
        rangeEnd: 2,
        snapshot: 5,
        order: 'asc',
        limit: 10,
      }),
      { status: 200 },
    );

    const { fetchImpl } = createQueuedFetch([subscribeResponse]);
    const core = new PulseQuery(createTestClient(fetchImpl).ordersByStatus({}));

    await core.subscribe();

    core.applyEvents([
      { op: 'insert', row: { $pk: 3, label: 'c' }, pk: 3 },
      { op: 'insert', row: { $pk: 1, label: 'a' }, pk: 1 },
      {
        op: 'update',
        row: { $pk: 1, label: 'a2' },
        old_row: { $pk: 1, label: 'a' },
        pk: 1,
        matchesNew: true,
        matchesOld: true,
      },
      {
        op: 'update',
        row: { $pk: 2, label: 'b2' },
        old_row: { $pk: 2, label: 'b' },
        pk: 2,
        matchesNew: false,
        matchesOld: true,
      },
      {
        op: 'update',
        row: { $pk: 4, label: 'd' },
        old_row: { $pk: 4, label: 'd-old' },
        pk: 4,
        matchesNew: true,
        matchesOld: false,
      },
      {
        op: 'delete',
        old_row: { $pk: 4, label: 'd' },
        pk: 4,
        matchesOld: true,
      },
    ]);

    expect(core.getState().data).toEqual([{ $pk: 1, label: 'a2' }]);
  });

  test('loadMore appends unseen rows and dedupes existing rows', async () => {
    const subscribeResponse = new Response(
      serialize({
        subscriptionId: 'sub-3',
        rows: [
          { $pk: 1, label: 'a' },
          { $pk: 2, label: 'b' },
        ],
        rangeStart: 1,
        rangeEnd: 2,
        snapshot: 8,
        order: 'asc',
        limit: 2,
      }),
      { status: 200 },
    );

    const loadMoreResponse = new Response(
      serialize({
        rows: [
          { $pk: 2, label: 'b-duplicate' },
          { $pk: 3, label: 'c' },
        ],
        rangeStart: 1,
        rangeEnd: 3,
        hasMore: false,
      }),
      { status: 200 },
    );

    const { fetchImpl, calls } = createQueuedFetch([subscribeResponse, loadMoreResponse]);
    const core = new PulseQuery(createTestClient(fetchImpl).ordersByStatus({}));

    await core.subscribe();
    await core.loadMore();

    expect(calls[1]?.url).toBe('/api/realtime/load-more');
    expect(calls[1]?.body).toEqual({
      clientId: expect.any(String),
      subscriptionId: 'sub-3',
      cursor: 2,
    });
    expect(core.getState().data).toEqual([
      { $pk: 1, label: 'a' },
      { $pk: 2, label: 'b' },
      { $pk: 3, label: 'c' },
    ]);
    expect(core.getState().hasMore).toBe(false);
  });

  test('poll handles reset from batched pull response', async () => {
    const subscribeResponse = new Response(
      serialize({
        subscriptionId: 'sub-4',
        clientId: 'client-1',
        rows: [{ $pk: 10, label: 'x' }],
        rangeStart: 10,
        rangeEnd: 10,
        snapshot: 1,
        order: 'asc',
        limit: null,
      }),
      { status: 200 },
    );

    const pullReset = makePullResponse({
      'sub-4': {
        events: [],
        rows: [{ $pk: 11, label: 'y' }],
        rangeStart: 11,
        rangeEnd: 11,
        snapshot: 3,
        reset: true,
        reason: 'snapshot',
        order: 'asc',
        limit: null,
        hasMore: false,
      },
    });

    const { fetchImpl, calls } = createQueuedFetch([subscribeResponse, pullReset]);
    const core = new PulseQuery(createTestClient(fetchImpl).ordersByStatus({}));

    await core.subscribe();
    await core.poll();

    expect(calls.map((call) => call.url)).toEqual([
      '/api/realtime/subscribe',
      '/api/realtime/pull',
    ]);
    expect(calls[1]?.body).toEqual({
      clientId: expect.any(String),
      subscriptions: [{ subscriptionId: 'sub-4', snapshot: 1 }],
    });
    expect(core.getState().data).toEqual([{ $pk: 11, label: 'y' }]);
  });

  test('two active queries share one pull request', async () => {
    const subscribeOne = new Response(
      serialize({
        subscriptionId: 'sub-a',
        clientId: 'client-1',
        rows: [{ $pk: 1, label: 'a' }],
        rangeStart: 1,
        rangeEnd: 1,
        snapshot: 1,
        order: 'asc',
        limit: null,
      }),
      { status: 200 },
    );

    const subscribeTwo = new Response(
      serialize({
        subscriptionId: 'sub-b',
        clientId: 'client-1',
        rows: [{ $pk: 2, label: 'b' }],
        rangeStart: 2,
        rangeEnd: 2,
        snapshot: 4,
        order: 'asc',
        limit: null,
      }),
      { status: 200 },
    );

    const pullResponse = makePullResponse({
      'sub-a': {
        events: [{ op: 'insert', row: { $pk: 3, label: 'a2' }, pk: 3 }],
        rangeStart: 1,
        rangeEnd: 3,
        snapshot: 2,
      },
      'sub-b': {
        events: [{ op: 'insert', row: { $pk: 5, label: 'b2' }, pk: 5 }],
        rangeStart: 2,
        rangeEnd: 5,
        snapshot: 5,
      },
    });

    const { fetchImpl, calls } = createQueuedFetch([subscribeOne, subscribeTwo, pullResponse]);
    const client = createTestClient(fetchImpl);
    const first = new PulseQuery(client.ordersByStatus({ status: 'requested' }));
    const second = new PulseQuery(client.ordersByStatus({ status: 'accepted' }));

    await first.subscribe();
    await second.subscribe();
    await Promise.all([first.poll(), second.poll()]);

    expect(calls.map((call) => call.url)).toEqual([
      '/api/realtime/subscribe',
      '/api/realtime/subscribe',
      '/api/realtime/pull',
    ]);
    expect(calls[2]?.body).toEqual({
      clientId: expect.any(String),
      subscriptions: expect.arrayContaining([
        { subscriptionId: 'sub-a', snapshot: 1 },
        { subscriptionId: 'sub-b', snapshot: 4 },
      ]),
    });
    expect(first.getState().data).toEqual([
      { $pk: 1, label: 'a' },
      { $pk: 3, label: 'a2' },
    ]);
    expect(second.getState().data).toEqual([
      { $pk: 2, label: 'b' },
      { $pk: 5, label: 'b2' },
    ]);
  });

  test('reset restores initial state', async () => {
    const subscribeResponse = new Response(
      serialize({
        subscriptionId: 'sub-5',
        rows: [{ $pk: 1, label: 'a' }],
        rangeStart: 1,
        rangeEnd: 1,
        snapshot: 1,
        order: 'asc',
        limit: null,
      }),
      { status: 200 },
    );

    const { fetchImpl } = createQueuedFetch([subscribeResponse]);
    const core = new PulseQuery(createTestClient(fetchImpl).ordersByStatus({}));

    await core.subscribe();
    core.reset();

    expect(core.getState()).toEqual({
      data: [],
      isLoading: true,
      isLoadingMore: false,
      hasMore: false,
      error: null,
    });
  });

  test('destroy prevents delayed subscribe from registering state', async () => {
    let releaseResponse!: () => void;
    const responseGate = new Promise<void>((resolve) => {
      releaseResponse = resolve;
    });
    const subscribeResponse = new Response(
      serialize({
        subscriptionId: 'sub-destroy',
        clientId: 'client-1',
        rows: [{ $pk: 1, label: 'a' }],
        rangeStart: 1,
        rangeEnd: 1,
        snapshot: 1,
      }),
      { status: 200 },
    );

    const fetchImpl = Object.assign(
      async () => {
        await responseGate;
        return subscribeResponse;
      },
      { preconnect() {} },
    ) satisfies typeof fetch;

    const core = new PulseQuery(createTestClient(fetchImpl).ordersByStatus({}));
    const subscribePromise = core.subscribe();
    core.destroy();
    releaseResponse();
    await subscribePromise;

    expect(core.getState()).toEqual({
      data: [],
      isLoading: true,
      isLoadingMore: false,
      hasMore: false,
      error: null,
    });
  });
});
