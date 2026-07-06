import { randomUUID } from 'node:crypto';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import { drizzle, type PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';
import type { QueryDescriptor } from 'drizzle-pulse';
import { createPulseClient, PulseQuery } from 'drizzle-pulse/client';
import {
  type AnyQueries,
  emitEventsTableDdl,
  expose,
  type LoadMoreRequest,
  type PullRequest,
  type PulseAuthContext,
  type PulseRegistry,
  type RealtimeRuntime,
  type SubscribeRequest,
  serializeResponse,
} from 'drizzle-pulse/server';
import type { Hono } from 'hono';
import { Hono as HonoRouter } from 'hono';
import { Pool } from 'pg';
import postgres from 'postgres';
import SuperJSON from 'superjson';
import { z } from 'zod';
import type { ProcessDbOperationsOptions } from './db-helpers.js';
import { getLastEventSnapshot, insertTestUser, processDbOperations } from './db-helpers.js';

// Re-export shared helpers so downstream tests can import from one place
export { processDbOperations, insertTestUser, getLastEventSnapshot };

const DEFAULT_DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/postgres';
const TEST_DATABASE_PREFIX = 'drizzle_realtime_test';

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
  sourceTable: string;
  eventsTable: PgTable;
  pulsedTables: PgTable[];
  cleanupTables: readonly string[];
  publicationName: string;
  tables: FixtureTableMap;
  schemas: FixtureSchemaMap;
};

type TestRuntime<TQueries extends AnyQueries> = RealtimeRuntime<TQueries> & {
  publicationName: string;
  slotName: string;
  sourceSql: ReturnType<typeof postgres>;
};

/** Infer the harness runtime type from a concrete registry, for use in test variable declarations. */
export type RuntimeOf<TRegistry extends PulseRegistry<AnyQueries>> =
  TRegistry extends PulseRegistry<infer TQueries>
    ? RealtimeRuntime<TQueries> & {
        publicationName: string;
        slotName: string;
        sourceSql: ReturnType<typeof postgres>;
      }
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
  clientId: z.string(),
  subscriptionId: z.string(),
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
  snapshot: z.number(),
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
  snapshot: z
    .number()
    .nullable()
    .optional()
    .transform((value) => value ?? null),
  reset: z.boolean().optional(),
  reason: z.string().optional(),
});

const suiteContexts = new Map<string, TestSuiteContext<IntegrationTestFixture>>();

function getSuiteContextKey(fixture: IntegrationTestFixture, registry: PulseRegistry<any>): string {
  const queryNames = registry.getQueryNames().slice().sort().join(',');
  return `${fixture.variantName}::${queryNames}`;
}

function buildDatabaseUrl(baseDatabaseUrl: string, databaseName: string): string {
  const url = new URL(baseDatabaseUrl);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

function randomSuffix(): string {
  return randomUUID().replaceAll('-', '').slice(0, 10);
}

function withQuietPostgresUrl(databaseUrl: string): string {
  const url = new URL(databaseUrl);
  url.searchParams.set('options', '-c client_min_messages=warning');
  return url.toString();
}

function createQuietPool(databaseUrl: string): Pool {
  return new Pool({ connectionString: withQuietPostgresUrl(databaseUrl) });
}

function createQuietPostgresClient(databaseUrl: string) {
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

/**
 * D-09: the harness never hand-mirrors events-table SQL — every fixture's events tables
 * are created by executing the DDL emitter's output against the pulsed source tables.
 */
async function createFixtureEventsTables(pool: Pool, fixture: IntegrationTestFixture): Promise<void> {
  for (const pulsedTable of fixture.pulsedTables) {
    const statements = emitEventsTableDdl(pulsedTable);
    for (const statement of statements) {
      await pool.query(statement);
    }
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
    wal: {
      publicationName,
      slotName,
      logging: {
        events: false,
      },
    },
  });

  return Object.assign(runtime, {
    publicationName,
    slotName,
    sourceSql,
  });
}

function createRealtimeRouter(
  runtime: RealtimeRuntime<any>,
  auth: PulseAuthContext = { userId: null },
): Hono {
  const router = new HonoRouter();

  const toResponse = (data: unknown, status = 200) =>
    new Response(serializeResponse(data), {
      status,
      headers: { 'Content-Type': 'application/json' },
    });

  router.post('/subscribe', async (c) => {
    const request = (await c.req.json()) as SubscribeRequest;
    const result = await runtime.handlers.subscribe(request, auth);
    return toResponse(result.body, result.status);
  });

  router.post('/pull', async (c) => {
    const request = (await c.req.json()) as PullRequest;
    const result = await runtime.handlers.pull(request, auth);
    return toResponse(result.body, result.status);
  });

  router.post('/load-more', async (c) => {
    const request = (await c.req.json()) as LoadMoreRequest;
    const result = await runtime.handlers.loadMore(request, auth);
    return toResponse(result.body, result.status);
  });

  return router;
}

export function createRealtimeRouterWithAuth(
  runtime: RealtimeRuntime<any>,
  auth: PulseAuthContext,
): Hono {
  return createRealtimeRouter(runtime, auth);
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
    // Single type assertion at the cache-retrieval boundary: the context was stored with the
    // correct TQueries for this key; `any` in the cache type is a storage convenience.
    return toTestSuiteResult(existing) as TestSuiteResult<TFixture, TQueries>;
  }

  const baseDatabaseUrl = process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;
  const adminPool = createQuietPool(baseDatabaseUrl);
  const databaseName = `${TEST_DATABASE_PREFIX}_${randomSuffix()}`;
  const runtimeStartupError = { current: null as Error | null };

  await cleanupStaleTestSlots(adminPool);
  await ensureCleanTestDatabase(adminPool, databaseName);

  const databaseUrl = buildDatabaseUrl(baseDatabaseUrl, databaseName);
  const testPool = createQuietPool(databaseUrl);

  await applyFixtureMigrations(databaseUrl, fixture.migrationsPath);
  await createFixtureEventsTables(testPool, fixture);

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
    router: createRealtimeRouter(runtime),
    db,
    dbSql,
    activeSuiteUsers: 1,
    runtimeStartupError: null,
  };
  suiteContexts.set(contextKey, ctx);

  return toTestSuiteResult(ctx);
}

export async function teardownTestSuiteForFixture<TFixture extends IntegrationTestFixture>(
  fixture: TFixture,
): Promise<void> {
  const matchingEntries = Array.from(suiteContexts.entries()).filter(([key]) =>
    key.startsWith(`${fixture.variantName}::`),
  );

  for (const [contextKey, ctx] of matchingEntries) {
    ctx.activeSuiteUsers = Math.max(ctx.activeSuiteUsers - 1, 0);
    if (ctx.activeSuiteUsers > 0) {
      continue;
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

export async function subscribeClient(
  router: Hono,
  queryName: string,
  args: PlainRecord,
): Promise<{
  clientId: string;
  subscriptionId: string;
  rows: PlainRecord[];
  rangeStart: number | null;
  rangeEnd: number | null;
  snapshot: number;
}> {
  const response = await router.request('/subscribe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ queryName, args, clientId: randomUUID() }),
  });

  if (!response.ok) {
    throw new Error(`subscribe failed with status ${response.status}`);
  }

  const raw = await response.text();
  const body = subscribeResponseSchema.parse(parseSuperJsonResponse(raw));

  return {
    clientId: z.string().parse((body as PlainRecord).clientId),
    subscriptionId: body.subscriptionId,
    rows: body.rows,
    rangeStart: body.rangeStart,
    rangeEnd: body.rangeEnd,
    snapshot: body.snapshot,
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
  clientId: string,
  subscriptionId: string,
  snapshot: number,
): Promise<{
  events: Array<{
    op: string;
    pk: unknown;
    row?: PlainRecord;
    old_row?: PlainRecord;
    matchesNew?: boolean;
    matchesOld?: boolean;
  }>;
  rangeStart: number | null;
  rangeEnd: number | null;
  snapshot: number;
  reset?: boolean;
  reason?: string;
}> {
  const response = await router.request('/pull', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ clientId, subscriptions: [{ subscriptionId, snapshot }] }),
  });

  if (!response.ok) {
    throw new Error(`pull failed with status ${response.status}`);
  }

  const raw = await response.text();
  const envelope = parseSuperJsonResponse(raw);
  const resultMap = z.record(z.string(), pullResponseSchema).parse(envelope.results ?? {});
  const body = pullResponseSchema.parse(resultMap[subscriptionId] ?? { events: [] });

  const events =
    body.events === undefined && body.reset === true ? [] : parsePullEvents(body.events);
  const nextSnapshot = body.snapshot ?? (body.reset === true ? snapshot : null);

  if (nextSnapshot === null) {
    throw new Error('Expected snapshot to be a number');
  }

  return {
    events,
    rangeStart: body.rangeStart,
    rangeEnd: body.rangeEnd,
    snapshot: nextSnapshot,
    reset: body.reset,
    reason: body.reason,
  };
}

export type PulseRow = Record<string, unknown> & { $pk: unknown };
