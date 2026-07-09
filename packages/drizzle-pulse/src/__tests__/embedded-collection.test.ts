import { describe, expect, test } from 'bun:test';
import { createPulseClient } from '../client/embedded/index.js';
import { WalEventEmitter } from '../server/wal-event-emitter.js';
import { makeRegistryStub, makeResolvedQuery } from './mock-runtime.js';

// ---------------------------------------------------------------------------
// No DB required: these cover the embedded client's user-facing error paths.
// The initial load and live pulls run against a mock SDK handler; real WAL push
// behavior is covered by the integration suite.
// ---------------------------------------------------------------------------

type HandlerResult = { status: number; body: unknown };

function okSubscribeBody(rows: Record<string, unknown>[] = []) {
  return {
    rows,
    rangeStart: null,
    rangeEnd: null,
    snapshot: 'e:0',
    order: 'asc' as const,
    limit: null,
    hasMore: false,
  };
}

function makeMockHandlers(
  overrides: {
    subscribe?: (req: unknown, auth: unknown) => Promise<HandlerResult> | HandlerResult;
  } = {},
) {
  return {
    subscribe: overrides.subscribe ?? (async () => ({ status: 200, body: okSubscribeBody() })),
    pull: async () => ({ status: 200, body: { results: {} } }),
    loadMore: async () => ({
      status: 200,
      body: { rows: [], rangeStart: null, rangeEnd: null, hasMore: false },
    }),
  };
}

function makeMockRuntime(
  opts: {
    isRunning?: boolean;
    hasTransform?: boolean;
    handlers?: ReturnType<typeof makeMockHandlers>;
  } = {},
) {
  const walEventEmitter = new WalEventEmitter();
  const registryStub = makeRegistryStub({ hasTransform: opts.hasTransform ?? false });
  const resolved = makeResolvedQuery();
  return {
    isRunning: opts.isRunning ?? true,
    walEventEmitter,
    handlers: opts.handlers ?? makeMockHandlers(),
    registry: {
      getPulseQuery: () => registryStub,
      resolve: () => resolved,
    },
    onReconnect: (_listener: () => void) => () => {},
    onStop: (_listener: () => void) => () => {},
  };
}

describe('embedded client — user-facing error paths', () => {
  test('creating a .transform() query rejects (unsupported in the embedded client)', async () => {
    const runtime = makeMockRuntime({ hasTransform: true });
    const client = createPulseClient(runtime as any);
    await expect((client as any).orders()).rejects.toThrow(/\.transform\(\)/);
  });

  test('creating a collection before runtime.start() rejects', async () => {
    const runtime = makeMockRuntime({ isRunning: false });
    const client = createPulseClient(runtime as any);
    await expect((client as any).orders()).rejects.toThrow(/after runtime\.start\(\)/);
  });

  test('an unknown query name rejects', async () => {
    const runtime = makeMockRuntime();
    // The shared mock registry answers every name; unknown-query needs a miss.
    runtime.registry = { ...runtime.registry, getPulseQuery: () => undefined } as any;
    const client = createPulseClient(runtime as any);
    await expect((client as any).nope()).rejects.toThrow('Unknown query: "nope"');
  });

  test('the factory rejects when the initial load fails and the change signal is detached', async () => {
    const runtime = makeMockRuntime({
      handlers: makeMockHandlers({
        subscribe: async () => ({ status: 500, body: { error: 'DB connection failed' } }),
      }),
    });

    let unsubCount = 0;
    const realSubscribe = runtime.walEventEmitter.subscribe.bind(runtime.walEventEmitter);
    runtime.walEventEmitter.subscribe = (key: string, listener: any) => {
      const inner = realSubscribe(key, listener);
      return () => {
        unsubCount++;
        inner();
      };
    };

    const client = createPulseClient(runtime as any);
    await expect((client as any).orders()).rejects.toThrow('DB connection failed');
    // dispose() ran on the failed load, detaching the WAL signal.
    expect(unsubCount).toBe(1);
  });

  test('the factory rejects when the runtime stops mid-load', async () => {
    let releaseSubscribe!: () => void;
    const gate = new Promise<void>((resolve) => {
      releaseSubscribe = resolve;
    });
    const stopListeners: Array<() => void> = [];

    const runtime = makeMockRuntime({
      handlers: makeMockHandlers({
        subscribe: async () => {
          await gate;
          return { status: 200, body: okSubscribeBody() };
        },
      }),
    });
    runtime.onStop = (listener: () => void) => {
      stopListeners.push(listener);
      return () => {};
    };

    const client = createPulseClient(runtime as any);
    const pending = (client as any).orders();
    // Simulate runtime.stop() broadcasting while the initial load is in flight.
    for (const listener of stopListeners) listener();
    releaseSubscribe();

    await expect(pending).rejects.toThrow(/disposed before the initial sync completed/);
  });

  test('the client proxy is not accidentally thenable — awaiting it settles instead of hanging', async () => {
    const runtime = makeMockRuntime();
    const client = createPulseClient(runtime as any);

    expect((client as any).then).toBeUndefined();
    // Race against a short timeout: pre-fix, `await client` never settles because the
    // proxy's `then` triggers a rejected-but-ignored promise instead of resolving/rejecting
    // the awaiting one. `Promise.race` proves the await itself resolves promptly.
    const resolvedInTime = await Promise.race([
      Promise.resolve(client).then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 200)),
    ]);
    expect(resolvedInTime).toBe(true);
  });
});
