import { describe, expect, test } from 'bun:test';
import { getColumns } from 'drizzle-orm';
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { createPulseClient, PulseCollection } from '../client/embedded/index.js';
import type { PulseSourceDb } from '../server/pulse-sql.js';
import { WalEventEmitter } from '../server/wal-event-emitter.js';
import type { PulseRegistryQuery, ResolvedPulseQuery } from '../types.js';

// ---------------------------------------------------------------------------
// Minimal inline fixtures — no DB required. Real WAL/rebaseline scenarios are
// covered by the integration suite and resilience.test.ts; this file covers the
// user-facing error paths of the embedded client only.
// ---------------------------------------------------------------------------

const ordersTable = pgTable('orders', {
  id: serial('id').primaryKey(),
  status: text('status').notNull(),
  price: integer('price'),
});

function makeMockSourceDb(rows: Record<string, unknown>[] = []): PulseSourceDb {
  const dynamicQuery: any = Object.assign(Promise.resolve(rows), {
    $dynamic() {
      return dynamicQuery;
    },
    orderBy() {
      return dynamicQuery;
    },
    limit(n: number) {
      return Promise.resolve(rows.slice(0, n));
    },
  });
  return {
    select() {
      return {
        from() {
          return {
            where() {
              return {
                $dynamic() {
                  return dynamicQuery;
                },
              };
            },
          };
        },
      };
    },
  } as unknown as PulseSourceDb;
}

const mockSourceDb = makeMockSourceDb();

// Baseline SELECT blocks until release() so a dispose can race the in-flight handshake.
function makeBlockingSourceDb(rows: Record<string, unknown>[] = []): {
  db: PulseSourceDb;
  release: () => void;
} {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const resultP = gate.then(() => rows);
  const dynamicQuery: any = {
    $dynamic() {
      return dynamicQuery;
    },
    orderBy() {
      return resultP;
    },
    limit() {
      return resultP;
    },
  };
  const db = {
    select() {
      return {
        from() {
          return {
            where() {
              return {
                $dynamic() {
                  return dynamicQuery;
                },
              };
            },
          };
        },
      };
    },
  } as unknown as PulseSourceDb;
  return { db, release };
}

function makeMockSourceDbThatThrows(error: Error): PulseSourceDb {
  const dynamicQuery: any = {
    $dynamic() {
      return dynamicQuery;
    },
    orderBy() {
      return Promise.reject(error);
    },
    limit() {
      return Promise.reject(error);
    },
  };
  return {
    select() {
      return {
        from() {
          return {
            where() {
              return {
                $dynamic() {
                  return dynamicQuery;
                },
              };
            },
          };
        },
      };
    },
  } as unknown as PulseSourceDb;
}

const ordersColumns = getColumns(ordersTable);

function makeResolvedQuery(overrides: Partial<ResolvedPulseQuery> = {}): ResolvedPulseQuery {
  return {
    table: ordersTable,
    pkColumn: ordersColumns.id,
    columns: ordersColumns,
    selectedColumns: ordersColumns,
    allowedColumnNames: new Set(Object.keys(ordersColumns)),
    order: 'asc',
    limit: null,
    argsSchema: null,
    where: null,
    hasTransform: false,
    transformRows: async (rows) => rows,
    ...overrides,
  };
}

function makeRegistryStub(overrides: Partial<PulseRegistryQuery> = {}): PulseRegistryQuery {
  return {
    table: ordersTable,
    pkColumn: ordersColumns.id,
    columns: ordersColumns,
    selectedColumns: ordersColumns,
    allowedColumnNames: new Set(Object.keys(ordersColumns)),
    order: 'asc',
    limit: null,
    argsSchema: null,
    queryFn: null,
    hasTransform: false,
    transformRows: async (rows) => rows,
    ...overrides,
  };
}

function makeMockRuntime(opts: { isRunning?: boolean; hasTransform?: boolean } = {}) {
  const walEventEmitter = new WalEventEmitter();
  const registryStub = makeRegistryStub({ hasTransform: opts.hasTransform ?? false });
  const resolved = makeResolvedQuery();
  return {
    isRunning: opts.isRunning ?? true,
    walEventEmitter,
    lastPersistedSnapshot: 0,
    sourceDb: mockSourceDb,
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

  test('startHandshake() rejects when the baseline SELECT fails and the tap listener is detached', async () => {
    const runtime = makeMockRuntime();
    runtime.sourceDb = makeMockSourceDbThatThrows(new Error('DB connection failed'));

    let unsubCount = 0;
    const realSubscribe = runtime.walEventEmitter.subscribe.bind(runtime.walEventEmitter);
    runtime.walEventEmitter.subscribe = (key: string, listener: any) => {
      const inner = realSubscribe(key, listener);
      return () => {
        unsubCount++;
        inner();
      };
    };

    const collection = new PulseCollection(runtime as any, makeResolvedQuery());

    await expect(collection.startHandshake()).rejects.toThrow('DB connection failed');
    // Tap detached so the buffer can't grow unbounded, and a later dispose() is safe.
    expect(unsubCount).toBe(1);
    expect(() => collection.dispose()).not.toThrow();
    expect(unsubCount).toBe(1);
  });

  test('the factory promise rejects when the baseline SELECT fails', async () => {
    const runtime = makeMockRuntime();
    runtime.sourceDb = makeMockSourceDbThatThrows(new Error('DB connection failed'));
    const client = createPulseClient(runtime as any);

    await expect((client as any).orders()).rejects.toThrow('DB connection failed');
  });

  test('the factory promise rejects when the runtime stops mid-handshake', async () => {
    const { db, release } = makeBlockingSourceDb();
    const stopListeners: Array<() => void> = [];
    const runtime = makeMockRuntime();
    runtime.sourceDb = db;
    runtime.onStop = (listener: () => void) => {
      stopListeners.push(listener);
      return () => {};
    };
    const client = createPulseClient(runtime as any);

    const pending = (client as any).orders();
    // Simulate runtime.stop() broadcasting while the baseline SELECT is in flight:
    // PulseClient.disposeAll() disposes the not-yet-ready collection.
    for (const listener of stopListeners) listener();
    release();

    await expect(pending).rejects.toThrow(/disposed before the initial sync completed/);
  });
});
