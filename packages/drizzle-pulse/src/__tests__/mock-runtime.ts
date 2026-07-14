import { getColumns } from 'drizzle-orm';
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { PulseRuntime } from '../server/expose.js';
import { createPulseRegistry } from '../server/pulse-registry.js';
import type { PulseSourceDb } from '../server/pulse-sql.js';
import { WalEventEmitter } from '../server/wal-event-emitter.js';
import type { PulseRegistryQuery, ResolvedPulseQuery } from '../types.js';

// Shared inline fixtures (no DB required) — used by embedded-collection.test.ts and
// resilience.test.ts, which mock the same PulseSourceDb/registry/resolved-query shapes
// but build different runtime wrappers around them.

export const ordersTable = pgTable('orders', {
  id: serial('id').primaryKey(),
  status: text('status').notNull(),
  price: integer('price'),
});

export const ordersColumns = getColumns(ordersTable);

export function makeMockSourceDb(rows: Record<string, unknown>[] = []): PulseSourceDb {
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

export function makeResolvedQuery(overrides: Partial<ResolvedPulseQuery> = {}): ResolvedPulseQuery {
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

export function makeRegistryStub(overrides: Partial<PulseRegistryQuery> = {}): PulseRegistryQuery {
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

export interface MockRuntimeOptions {
  isRunning?: boolean;
  hasTransform?: boolean;
  limit?: number | null;
  baselineRows?: Record<string, unknown>[];
  watermark?: string;
  where?: ResolvedPulseQuery['where'];
}

// Shared mock runtime for createPulseClient/createPulseEvents tests — deliberately has no
// `handlers` property: the tap-direct embedded path must not need the SDK/wire-protocol
// surface (SPLIT-03). Tests override individual methods (registry, onStop, etc.) post-
// construction the same way they already override walEventEmitter.subscribe.
export function makeMockRuntime(opts: MockRuntimeOptions = {}) {
  const walEventEmitter = new WalEventEmitter();
  const registryStub = makeRegistryStub({
    hasTransform: opts.hasTransform ?? false,
    limit: opts.limit ?? null,
  });
  const resolved = makeResolvedQuery({ limit: opts.limit ?? null, where: opts.where ?? null });

  return {
    isRunning: opts.isRunning ?? true,
    walEventEmitter,
    readCollectionBaseline: async () => ({
      rows: opts.baselineRows ?? [],
      watermark: opts.watermark ?? '0/100',
    }),
    registry: {
      getPulseQuery: () => registryStub,
      resolve: () => resolved,
    },
    onReconnect: (_listener: () => void) => () => {},
    onStop: (_listener: () => void) => () => {},
    onTerminalError: (_listener: (error: Error) => void) => () => {},
  };
}

// Construct a real PulseRuntime with an empty registry (no DB required).
export function makePulseRuntime(
  opts: {
    databaseUrl?: string;
    sourceDb?: PulseSourceDb;
    pull?: boolean | { eventsSchema?: string; eventLimit?: number };
  } = {},
): PulseRuntime<any> {
  const emptyRegistry = createPulseRegistry({});
  return new PulseRuntime(emptyRegistry as any, {
    databaseUrl: opts.databaseUrl ?? 'postgresql://user:pass@localhost/test',
    sourceDb: opts.sourceDb ?? ({} as PulseSourceDb),
    pull: opts.pull ?? true,
    wal: { publicationName: 'test_pub', slotName: 'test_slot' },
  });
}
