import { randomUUID } from 'node:crypto';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import { drizzle, type PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';
import type { QueryDescriptor } from 'drizzle-pulse';
import { createPulseClient, PulseQuery } from 'drizzle-pulse/client';
import {
  type AnyQueries,
  expose,
  LogLevel,
  type PulseAuthContext,
  type PulseRegistry,
  type PulseRuntime,
} from 'drizzle-pulse/server';
import { createPulseRouter as createServerRouter } from 'drizzle-pulse/server/router';
import type { Hono } from 'hono';
import { Pool } from 'pg';
import postgres from 'postgres';
import SuperJSON from 'superjson';
import { z } from 'zod';
import type { ProcessDbOperationsOptions } from './db-helpers.js';
import { insertTestUser, processDbOperations } from './db-helpers.js';

// Re-export shared helpers so downstream tests can import from one place
export { processDbOperations, insertTestUser };

const DEFAULT_DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/postgres';
const TEST_DATABASE_PREFIX = 'drizzle_pulse_test';

type PlainRecord = Record<string, unknown>;

export type HarnessEvent = {
  snapshot: number;
  pk: unknown;
  op: string;
  timestamp: string;
};

type FixtureTableMap = Record<string, unknown>;
type FixtureSchemaMap = Record<string, unknown>;

export type IntegrationTestFixture = {
  variantName: string;
  migrationsPath: string;
  eventsTable: PgTable;
  pulsedTables: PgTable[];
  cleanupTables: readonly string[];
  publicationName: string;
  tables: FixtureTableMap;
  schemas: FixtureSchemaMap;
};

type TestRuntime<TQueries extends AnyQueries> = PulseRuntime<TQueries> & {
  sourceSql: ReturnType<typeof postgres>;
};

/** Infer the harness runtime type from a concrete registry, for use in test variable declarations. */
export type RuntimeOf<TRegistry extends PulseRegistry<AnyQueries>> =
  TRegistry extends PulseRegistry<infer TQueries>
    ? PulseRuntime<TQueries> & { sourceSql: ReturnType<typeof postgres> }
    : never;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TestSuiteContext<
  TFixture extends IntegrationTestFixture,
  TQueries extends AnyQueries = any,
> = {
  fixture: TFixture;
  databaseName: string;
  databaseUrl: string;
  publicationName: string;
  slotName: string;
  adminPool: Pool;
  testPool: Pool;
  runtime: TestRuntime<TQueries>;
  router: Hono;
  db: PostgresJsDatabase;
  dbSql: ReturnType<typeof postgres>;
  activeSuiteUsers: number;
  runtimeStartupError: Error | null;
};

function isFixtureContext<TFixture extends IntegrationTestFixture>(
  ctx: TestSuiteContext<IntegrationTestFixture>,
  fixture: TFixture,
): ctx is TestSuiteContext<TFixture, any> {
  return ctx.fixture === fixture;
}

type DbEventOperation = PromiseLike<unknown>;
type DbEventResults<TOperations extends ReadonlyArray<DbEventOperation>> = {
  [TIndex in keyof TOperations]: Awaited<TOperations[TIndex]>;
};

export type HarnessDbOperation = DbEventOperation;
export type HarnessDbEventResults<TOperations extends ReadonlyArray<HarnessDbOperation>> =
  DbEventResults<TOperations>;
export type HarnessProcessDbOperations = <
  const TOperations extends ReadonlyArray<HarnessDbOperation>,
>(
  operations: TOperations,
  options?: ProcessDbOperationsOptions,
) => Promise<{ events: HarnessEvent[]; results: HarnessDbEventResults<TOperations> }>;
export type HarnessInitTestQuery = <T extends PulseRow>(
  descriptor: QueryDescriptor<T>,
) => Promise<PulseQuery<T>>;

const plainRecordSchema = z.record(z.string(), z.unknown());

const subscribeResponseSchema = z.object({
  rows: z.array(plainRecordSchema),
  rangeStart: z
    .number()
    .nullable()
    .optional()
    .transform((value) => value ?? null),
  rangeEnd: z
    .number()
    .nullable()
    .optional()
    .transform((value) => value ?? null),
  // epoch:snapshot cursor token.
  snapshot: z.string(),
});

const pullEventSchema = z.object({
  op: z.string(),
  pk: z.unknown(),
  row: plainRecordSchema.nullish().transform((value) => value ?? undefined),
  old_row: plainRecordSchema.nullish().transform((value) => value ?? undefined),
  matchesNew: z.boolean().optional(),
  matchesOld: z.boolean().optional(),
});

const pullResponseSchema = z.object({
  events: z.array(pullEventSchema).optional(),
  rangeStart: z
    .number()
    .nullable()
    .optional()
    .transform((value) => value ?? null),
  rangeEnd: z
    .number()
    .nullable()
    .optional()
    .transform((value) => value ?? null),
  // epoch:snapshot cursor token (both incremental and reset responses carry it).
  snapshot: z
    .string()
    .nullable()
    .optional()
    .transform((value) => value ?? null),
  reset: z.boolean().optional(),
  reason: z.string().optional(),
});

const suiteContexts = new Map<string, TestSuiteContext<IntegrationTestFixture>>();

// Per-registry-object identity token so contexts never collide across registries with
// identical query names.
const registryIdentities = new WeakMap<PulseRegistry<any>, string>();
let nextRegistryIdentity = 0;

function getRegistryIdentity(registry: PulseRegistry<any>): string {
  const existing = registryIdentities.get(registry);
  if (existing !== undefined) {
    return existing;
  }

  const identity = `r${nextRegistryIdentity++}`;
  registryIdentities.set(registry, identity);
  return identity;
}

function getSuiteContextKey(fixture: IntegrationTestFixture, registry: PulseRegistry<any>): string {
  const queryNames = registry.getQueryNames().slice().sort().join(',');
  return `${fixture.variantName}::${queryNames}::${getRegistryIdentity(registry)}`;
}

export function baseDatabaseUrl(): string {
  return process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;
}

export function buildDatabaseUrl(baseUrl: string, databaseName: string): string {
  const url = new URL(baseUrl);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

export function randomSuffix(): string {
  return randomUUID().replaceAll('-', '').slice(0, 10);
}

export function withQuietPostgresUrl(databaseUrl: string): string {
  const url = new URL(databaseUrl);
  url.searchParams.set('options', '-c client_min_messages=warning');
  return url.toString();
}

export function createQuietPool(databaseUrl: string): Pool {
  return new Pool({ connectionString: withQuietPostgresUrl(databaseUrl) });
}

export function createQuietPostgresClient(databaseUrl: string) {
  return postgres(withQuietPostgresUrl(databaseUrl));
}

async function ensureCleanTestDatabase(adminPool: Pool, databaseName: string): Promise<void> {
  await adminPool.query(
    `
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = $1
        AND pid <> pg_backend_pid()
    `,
    [databaseName],
  );

  await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
}

async function cleanupStaleTestSlots(adminPool: Pool): Promise<void> {
  const { rows } = await adminPool.query<{
    slot_name: string;
    active_pid: number | null;
  }>(
    `
      SELECT slot_name, active_pid
      FROM pg_replication_slots
      WHERE slot_name LIKE 'test\\_%' ESCAPE '\\'
    `,
  );

  for (const row of rows) {
    // Never touch active slots — they may belong to other workers/processes
    if (row.active_pid !== null) {
      continue;
    }

    await adminPool.query('SELECT pg_drop_replication_slot($1)', [row.slot_name]);
  }
}

async function applyFixtureMigrations(databaseUrl: string, migrationsPath: string): Promise<void> {
  const migrationClient = postgres(withQuietPostgresUrl(databaseUrl), { max: 1 });
  const migrationDb = drizzle({ client: migrationClient });

  try {
    await migrate(migrationDb, { migrationsFolder: migrationsPath });
  } finally {
    await migrationClient.end();
  }
}

async function waitForWalStartup(
  adminPool: Pool,
  slotName: string,
  runtimeStartupError: { current: Error | null },
  opts?: { timeoutMs?: number; pollIntervalMs?: number },
): Promise<void> {
  const timeoutMs = opts?.timeoutMs ?? 5000;
  const pollIntervalMs = opts?.pollIntervalMs ?? 50;
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    if (runtimeStartupError.current) {
      throw runtimeStartupError.current;
    }

    const { rows } = await adminPool.query<{ slot_name: string }>(
      'SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1',
      [slotName],
    );

    if (rows.length > 0) {
      return;
    }

    await new Promise<void>((resolve) => {
      setTimeout(resolve, pollIntervalMs);
    });
  }

  throw new Error(`Timeout: WAL listener did not initialize slot ${slotName} in ${timeoutMs}ms`);
}

function createTestRuntime<TQueries extends AnyQueries>(
  databaseUrl: string,
  fixture: IntegrationTestFixture,
  registry: PulseRegistry<TQueries>,
): TestRuntime<TQueries> {
  const publicationName = fixture.publicationName;
  const slotName = `test_slot_${randomSuffix()}`;

  const sourceSql = createQuietPostgresClient(databaseUrl);
  const sourceDb = drizzle({ client: sourceSql });

  const runtime = expose(registry, {
    databaseUrl,
    sourceDb,
    pull: true,
    wal: { publicationName, slotName },
    logLevel: LogLevel.Error,
  });

  return Object.assign(runtime, { sourceSql });
}

function createPulseRouter(
  runtime: PulseRuntime<any>,
  auth: PulseAuthContext = { userId: null },
): Hono {
  return createServerRouter(runtime.handlers, auth);
}

export function createPulseRouterWithAuth(
  runtime: PulseRuntime<any>,
  auth: PulseAuthContext,
): Hono {
  return createPulseRouter(runtime, auth);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TestSuiteResult<
  TFixture extends IntegrationTestFixture = IntegrationTestFixture,
  TQueries extends AnyQueries = any,
> = {
  runtime: TestRuntime<TQueries>;
  router: Hono;
  pool: Pool;
  db: PostgresJsDatabase;
  databaseUrl: string;
  publicationName: string;
  slotName: string;
  fixture: TFixture;
  processDbOperations: HarnessProcessDbOperations;
  initTestQuery: HarnessInitTestQuery;
};

function createFixtureLocalProcessDbOperations(
  ctx: TestSuiteContext<IntegrationTestFixture>,
): HarnessProcessDbOperations {
  return async function processFixtureDbOperations<
    const TOperations extends ReadonlyArray<DbEventOperation>,
  >(
    operations: TOperations,
    options?: ProcessDbOperationsOptions,
  ): Promise<{ events: HarnessEvent[]; results: HarnessDbEventResults<TOperations> }> {
    return processDbOperations(ctx.fixture, ctx.testPool, operations, options);
  };
}

function createFixtureLocalInitTestQuery(
  ctx: TestSuiteContext<IntegrationTestFixture>,
): HarnessInitTestQuery {
  const fetchImpl = createRouterFetchAdapter(ctx.router);

  return async function initTestQuery<T extends PulseRow>(
    descriptor: QueryDescriptor<T>,
  ): Promise<PulseQuery<T>> {
    const client = createPulseClient<{
      [queryName: string]: (args?: Record<string, unknown>) => QueryDescriptor<T>;
    }>({ url: 'http://localhost', fetchImpl, pollIntervalMs: 0 });
    const descriptorFactory = client[descriptor.queryName];
    if (typeof descriptorFactory !== 'function') {
      throw new Error(`Missing client query factory for ${descriptor.queryName}`);
    }

    const query = new PulseQuery(descriptorFactory(descriptor.args));
    await query.subscribe();
    return query;
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function toTestSuiteResult<
  TFixture extends IntegrationTestFixture,
  TQueries extends AnyQueries = any,
>(ctx: TestSuiteContext<TFixture, TQueries>): TestSuiteResult<TFixture, TQueries> {
  return {
    runtime: ctx.runtime,
    router: ctx.router,
    pool: ctx.testPool,
    db: ctx.db,
    databaseUrl: ctx.databaseUrl,
    publicationName: ctx.publicationName,
    slotName: ctx.slotName,
    fixture: ctx.fixture,
    processDbOperations: createFixtureLocalProcessDbOperations(ctx),
    initTestQuery: createFixtureLocalInitTestQuery(ctx),
  };
}

export async function setupTestSuiteForFixture<
  TFixture extends IntegrationTestFixture,
  TQueries extends AnyQueries,
>(
  fixture: TFixture,
  registry: PulseRegistry<TQueries>,
): Promise<TestSuiteResult<TFixture, TQueries>> {
  const contextKey = getSuiteContextKey(fixture, registry);
  const existing = suiteContexts.get(contextKey);

  if (existing && isFixtureContext(existing, fixture)) {
    existing.activeSuiteUsers += 1;
    // Assert TQueries at the cache boundary — `any` in the cache type is a storage convenience.
    return toTestSuiteResult(existing) as TestSuiteResult<TFixture, TQueries>;
  }

  const base = baseDatabaseUrl();
  const adminPool = createQuietPool(base);
  const databaseName = `${TEST_DATABASE_PREFIX}_${randomSuffix()}`;
  const runtimeStartupError = { current: null as Error | null };

  await cleanupStaleTestSlots(adminPool);
  await ensureCleanTestDatabase(adminPool, databaseName);

  const databaseUrl = buildDatabaseUrl(base, databaseName);
  const testPool = createQuietPool(databaseUrl);

  // Events tables (and their pulse_meta bookkeeping) are runtime-owned: the migrations set up
  // the source table + publication + replica identity, and runtime.start() below reconciles
  // the events tables at boot.
  await applyFixtureMigrations(databaseUrl, fixture.migrationsPath);

  const runtime = createTestRuntime(databaseUrl, fixture, registry);

  void runtime.start().catch((error: unknown) => {
    const startupError = error instanceof Error ? error : new Error(String(error));
    runtimeStartupError.current = startupError;
    console.error(
      `[Integration Harness][${fixture.variantName}] Failed to start WAL listener:`,
      startupError,
    );
  });

  await waitForWalStartup(adminPool, runtime.slotName, runtimeStartupError);

  const dbSql = createQuietPostgresClient(databaseUrl);
  const db = drizzle({ client: dbSql });

  const ctx: TestSuiteContext<TFixture, TQueries> = {
    fixture,
    databaseName,
    databaseUrl,
    publicationName: runtime.publicationName,
    slotName: runtime.slotName,
    adminPool,
    testPool,
    runtime,
    router: createPulseRouter(runtime),
    db,
    dbSql,
    activeSuiteUsers: 1,
    runtimeStartupError: null,
  };
  suiteContexts.set(contextKey, ctx);

  return toTestSuiteResult(ctx);
}

export async function teardownTestSuiteForFixture<
  TFixture extends IntegrationTestFixture,
  TQueries extends AnyQueries,
>(fixture: TFixture, registry: PulseRegistry<TQueries>): Promise<void> {
  // Keyed on the exact context setup used, so registries sharing a variantName don't
  // decrement each other's ref count.
  const contextKey = getSuiteContextKey(fixture, registry);
  const ctx = suiteContexts.get(contextKey);
  if (!ctx) {
    return;
  }

  ctx.activeSuiteUsers = Math.max(ctx.activeSuiteUsers - 1, 0);
  if (ctx.activeSuiteUsers > 0) {
    return;
  }

  await ctx.runtime.stop();
  await ctx.testPool.query(`DROP PUBLICATION IF EXISTS ${ctx.publicationName}`);
  await ctx.adminPool.query(
    'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
    [ctx.slotName],
  );

  await ctx.testPool.end();
  await ctx.runtime.sourceSql.end();
  await ctx.dbSql.end();

  await ctx.adminPool.query(
    `
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = $1
        AND pid <> pg_backend_pid()
    `,
    [ctx.databaseName],
  );
  await ctx.adminPool.query(`DROP DATABASE IF EXISTS "${ctx.databaseName}"`);
  await ctx.adminPool.end();

  suiteContexts.delete(contextKey);
}

export async function cleanupBetweenTestsForFixture(
  fixture: IntegrationTestFixture,
  pool?: Pool,
): Promise<void> {
  const matchingContexts = Array.from(suiteContexts.values()).filter(
    (ctx) => ctx.fixture === fixture,
  );
  const ctx =
    pool === undefined
      ? matchingContexts[0]
      : matchingContexts.find((candidate) => candidate.testPool === pool);
  if (!ctx) {
    throw new Error('cleanupBetweenTestsForFixture() requires setupTestSuiteForFixture() first');
  }

  const targetPool = pool ?? ctx.testPool;

  const eventsTableConfig = getTableConfig(fixture.eventsTable);
  const eventsTable = `"${eventsTableConfig.schema ?? 'public'}"."${eventsTableConfig.name}"`;
  const tables = fixture.cleanupTables;
  const tableList = tables.map((t) => `"${t}"`).join(', ');
  await targetPool.query(`TRUNCATE TABLE ${tableList} RESTART IDENTITY CASCADE`);
  await targetPool.query(`TRUNCATE TABLE ${eventsTable} RESTART IDENTITY`);
  await ctx.runtime.ensureBaselines();
}

export async function waitForEventsForFixture(
  fixture: IntegrationTestFixture,
  pool: Pool,
  sinceSnapshot: number,
  expectedCount: number,
  opts?: { timeoutMs?: number; pollIntervalMs?: number },
): Promise<HarnessEvent[]> {
  const eventsTableConfig = getTableConfig(fixture.eventsTable);
  const eventsTable = `"${eventsTableConfig.schema ?? 'public'}"."${eventsTableConfig.name}"`;
  const timeoutMs = opts?.timeoutMs ?? 5000;
  const pollIntervalMs = opts?.pollIntervalMs ?? 50;
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const { rows } = await pool.query<HarnessEvent>(
      `
        SELECT "$snapshot"::int AS snapshot, id AS pk, "$op" AS op, "$timestamp"::text AS timestamp
        FROM ${eventsTable}
        WHERE "$snapshot" > $1
          AND "$op" <> 'snapshot'
        ORDER BY "$snapshot" ASC
      `,
      [sinceSnapshot],
    );

    if (rows.length >= expectedCount) {
      return rows;
    }

    await new Promise<void>((resolve) => {
      setTimeout(resolve, pollIntervalMs);
    });
  }

  const { rows } = await pool.query<{ count: string }>(
    `
      SELECT COUNT(*)::text AS count
      FROM "${eventsTableConfig.schema ?? 'public'}"."${eventsTableConfig.name}"
      WHERE "$snapshot" > $1
        AND "$op" <> 'snapshot'
    `,
    [sinceSnapshot],
  );
  const actual = Number(rows[0]?.count ?? '0');
  throw new Error(
    `Timeout: expected ${expectedCount} events after snapshot ${sinceSnapshot}, got ${actual} in ${timeoutMs}ms`,
  );
}

function parseSuperJsonResponse(raw: string): PlainRecord {
  const parsed = SuperJSON.parse(raw);
  return plainRecordSchema.parse(parsed);
}

export function createRouterFetchAdapter(router: Hono): typeof fetch {
  const fetchImpl = async (
    input: string | URL | Request,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const path = url.pathname + url.search;
    const method = init?.method ?? (input instanceof Request ? input.method : 'GET');
    const headers = init?.headers ?? (input instanceof Request ? input.headers : {});
    const body = init?.body ?? (input instanceof Request ? await input.text() : undefined);

    return router.request(path, {
      method,
      headers,
      body,
    });
  };

  // Add preconnect method (no-op for test adapter, but required by fetch spec)
  Object.assign(fetchImpl, { preconnect: () => {} });

  return fetchImpl as typeof fetch;
}

// The stateless per-request pull identity: query + client window + cursor token. A subscribe
// or pull result is itself a valid cursor, so it can be fed straight back into pullClient().
export type PullCursor = {
  queryName: string;
  args: PlainRecord;
  rangeStart: number | null;
  rangeEnd: number | null;
  token: string;
};

// The token is `epoch:snapshot`; epoch is a uuid (no colon), snapshot is digits.
function parseSnapshotToken(token: string): number {
  const separator = token.indexOf(':');
  return separator >= 0 ? Number(token.slice(separator + 1)) : 0;
}

export async function subscribeClient(
  router: Hono,
  queryName: string,
  args: PlainRecord,
): Promise<
  PullCursor & {
    rows: PlainRecord[];
    // Parsed snapshot number, for assertions and waitForEventsForFixture.
    snapshot: number;
  }
> {
  const response = await router.request('/subscribe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ queryName, args }),
  });

  if (!response.ok) {
    throw new Error(`subscribe failed with status ${response.status}`);
  }

  const raw = await response.text();
  const body = subscribeResponseSchema.parse(parseSuperJsonResponse(raw));

  return {
    queryName,
    args,
    rows: body.rows,
    rangeStart: body.rangeStart,
    rangeEnd: body.rangeEnd,
    token: body.snapshot,
    snapshot: parseSnapshotToken(body.snapshot),
  };
}

function parsePullEvents(value: unknown): Array<{
  op: string;
  pk: unknown;
  row?: PlainRecord;
  old_row?: PlainRecord;
  matchesNew?: boolean;
  matchesOld?: boolean;
}> {
  return z.array(pullEventSchema).parse(value);
}

export async function pullClient(
  router: Hono,
  cursor: PullCursor,
): Promise<
  PullCursor & {
    events: Array<{
      op: string;
      pk: unknown;
      row?: PlainRecord;
      old_row?: PlainRecord;
      matchesNew?: boolean;
      matchesOld?: boolean;
    }>;
    // Parsed snapshot number, for assertions and waitForEventsForFixture.
    snapshot: number;
    reset?: boolean;
    reason?: string;
  }
> {
  const response = await router.request('/pull', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      subscriptions: [
        {
          key: 'k',
          queryName: cursor.queryName,
          args: cursor.args,
          rangeStart: cursor.rangeStart,
          rangeEnd: cursor.rangeEnd,
          snapshot: cursor.token,
        },
      ],
    }),
  });

  if (!response.ok) {
    throw new Error(`pull failed with status ${response.status}`);
  }

  const raw = await response.text();
  const envelope = parseSuperJsonResponse(raw);
  const resultMap = z.record(z.string(), pullResponseSchema).parse(envelope.results ?? {});
  const body = pullResponseSchema.parse(resultMap.k ?? { events: [] });

  const events =
    body.events === undefined && body.reset === true ? [] : parsePullEvents(body.events);
  // Both incremental and reset carry a token; fall back to the incoming one only if absent.
  const nextToken = body.snapshot ?? cursor.token;

  return {
    queryName: cursor.queryName,
    args: cursor.args,
    events,
    rangeStart: body.rangeStart,
    rangeEnd: body.rangeEnd,
    token: nextToken,
    snapshot: parseSnapshotToken(nextToken),
    reset: body.reset,
    reason: body.reason,
  };
}

export type PulseRow = Record<string, unknown> & { $pk: unknown };
