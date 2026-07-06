/**
 * Integration proof: expose()'s aggregating startup guard against real Postgres.
 *
 * Unlike the other suites, these scenarios build fresh, deliberately-misconfigured
 * standalone databases (never `setupTestSuiteForFixture`, which now guarantees a healthy
 * setup) — the whole point is to exercise the guard's fail-closed paths. Each scenario
 * gets its own randomly-named database/publication/slot per the isolation convention
 * (CON-integration-tests-isolation) and tears itself down in a `finally` block.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseRegistry, emitEventsTableDdl, expose } from 'drizzle-pulse/server';
import { Pool } from 'pg';
import postgres from 'postgres';
import { orders } from './fixtures/minimal-orders/schema.js';

const DEFAULT_DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/postgres';

function baseDatabaseUrl(): string {
  return process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;
}

function buildDatabaseUrl(base: string, databaseName: string): string {
  const url = new URL(base);
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

// A single shared admin pool (connected to the base `postgres` database) creates/drops
// every scenario's standalone database and cleans up cluster-wide replication slots —
// mirrors the harness's own adminPool idiom.
const adminPool = createQuietPool(baseDatabaseUrl());

afterAll(async () => {
  await adminPool.end();
});

type GuardScenarioContext = {
  databaseName: string;
  databaseUrl: string;
  pool: Pool;
};

async function setupGuardScenario(scenario: string): Promise<GuardScenarioContext> {
  const base = baseDatabaseUrl();
  const databaseName = `pulse_guard_${scenario}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const databaseUrl = buildDatabaseUrl(base, databaseName);
  const pool = createQuietPool(databaseUrl);
  return { databaseName, databaseUrl, pool };
}

async function teardownGuardScenario(ctx: GuardScenarioContext): Promise<void> {
  await ctx.pool.end();
  await adminPool.query(
    `
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = $1
        AND pid <> pg_backend_pid()
    `,
    [ctx.databaseName],
  );
  await adminPool.query(`DROP DATABASE IF EXISTS "${ctx.databaseName}"`);
}

async function dropSlotIfExists(slotName: string): Promise<void> {
  await adminPool.query(
    'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
    [slotName],
  );
}

async function createOrdersSourceTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE "orders" (
      "id" serial PRIMARY KEY,
      "driver_id" integer,
      "status" text DEFAULT 'requested' NOT NULL,
      "price" numeric NOT NULL,
      "created_at" timestamp with time zone DEFAULT now() NOT NULL
    )
  `);
}

async function createEventsTable(pool: Pool): Promise<void> {
  // The events table is created from the emitter's own output, never hand-written SQL.
  for (const statement of emitEventsTableDdl(orders)) {
    await pool.query(statement);
  }
}

async function setReplicaIdentityFull(pool: Pool): Promise<void> {
  await pool.query('ALTER TABLE "orders" REPLICA IDENTITY FULL');
}

function failureLines(message: string): string[] {
  return message.split('\n').filter((line) => line.startsWith('- '));
}

async function captureStartRejection(runtime: { start: () => Promise<void> }): Promise<Error> {
  try {
    await runtime.start();
  } catch (error) {
    return error instanceof Error ? error : new Error(String(error));
  }
  throw new Error('Expected runtime.start() to reject, but it resolved');
}

describe('Startup Guard', () => {
  test('(a) missing publication AND missing events table: one aggregated rejection naming both', async () => {
    const ctx = await setupGuardScenario('a');
    const publicationName = `guard_pub_a_${randomSuffix()}`;
    const slotName = `guard_slot_a_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      await setReplicaIdentityFull(ctx.pool);
      // Deliberately absent: the publication and the events table.

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
      });

      const error = await captureStartRejection(runtime);

      expect(error.message).toContain(publicationName);
      expect(error.message).toContain('__events_public_orders');
      // Aggregation proof: two independent failures surface as two distinct lines in
      // the SAME thrown error, not two separate throws.
      expect(failureLines(error.message).length).toBeGreaterThanOrEqual(2);
    } finally {
      await sourceSql.end();
      await teardownGuardScenario(ctx);
    }
  });

  test('(b) defaults: wal omitted entirely defaults the publication name to drizzle_pulse', async () => {
    const ctx = await setupGuardScenario('b');
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      await setReplicaIdentityFull(ctx.pool);
      await createEventsTable(ctx.pool);
      // Deliberately absent: a "drizzle_pulse" publication — proves the default
      // flows all the way into the guard's failure message with no wal config supplied.

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
      });

      const error = await captureStartRejection(runtime);

      expect(error.message).toContain('drizzle_pulse');
    } finally {
      await sourceSql.end();
      await teardownGuardScenario(ctx);
    }
  });

  test('(c) FOR ALL TABLES publication passes membership check; runtime starts then stops cleanly', async () => {
    const ctx = await setupGuardScenario('c');
    const publicationName = `guard_pub_c_${randomSuffix()}`;
    const slotName = `guard_slot_c_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      await setReplicaIdentityFull(ctx.pool);
      await createEventsTable(ctx.pool);
      await ctx.pool.query(`CREATE PUBLICATION ${publicationName} FOR ALL TABLES`);

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
      });

      await runtime.start();
      expect(runtime.isRunning).toBe(true);

      await runtime.stop();
      expect(runtime.isRunning).toBe(false);
    } finally {
      await sourceSql.end();
      await ctx.pool.query(`DROP PUBLICATION IF EXISTS ${publicationName}`).catch(() => {});
      await dropSlotIfExists(slotName).catch(() => {});
      await teardownGuardScenario(ctx);
    }
  });

  test('(d) membership failure: publication exists but does not include the pulsed table', async () => {
    const ctx = await setupGuardScenario('d');
    const publicationName = `guard_pub_d_${randomSuffix()}`;
    const slotName = `guard_slot_d_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      await setReplicaIdentityFull(ctx.pool);
      await createEventsTable(ctx.pool);
      await ctx.pool.query('CREATE TABLE "users" ("id" serial PRIMARY KEY)');
      await ctx.pool.query(`CREATE PUBLICATION ${publicationName} FOR TABLE "users"`);

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
      });

      const error = await captureStartRejection(runtime);

      expect(error.message).toContain('public.orders');
      expect(error.message).toContain(publicationName);
    } finally {
      await sourceSql.end();
      await ctx.pool.query(`DROP PUBLICATION IF EXISTS ${publicationName}`).catch(() => {});
      await teardownGuardScenario(ctx);
    }
  });

  test('(e) replica identity failure: source table left at default replica identity', async () => {
    const ctx = await setupGuardScenario('e');
    const publicationName = `guard_pub_e_${randomSuffix()}`;
    const slotName = `guard_slot_e_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      // Deliberately absent: REPLICA IDENTITY FULL stays at Postgres's default.
      await createEventsTable(ctx.pool);
      await ctx.pool.query(`CREATE PUBLICATION ${publicationName} FOR ALL TABLES`);

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
      });

      const error = await captureStartRejection(runtime);

      expect(error.message).toContain('REPLICA IDENTITY FULL');
    } finally {
      await sourceSql.end();
      await ctx.pool.query(`DROP PUBLICATION IF EXISTS ${publicationName}`).catch(() => {});
      await teardownGuardScenario(ctx);
    }
  });

  test('(f) events table existence check rejects a same-named view (relkind restriction)', async () => {
    const ctx = await setupGuardScenario('f');
    const publicationName = `guard_pub_f_${randomSuffix()}`;
    const slotName = `guard_slot_f_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      await setReplicaIdentityFull(ctx.pool);
      await ctx.pool.query(`CREATE PUBLICATION ${publicationName} FOR ALL TABLES`);
      // Deliberately a VIEW, not a TABLE, at the exact events-table name/schema: before the
      // relkind restriction, this false-passed the existence check (any pg_class relkind
      // matched), deferring the failure to a confusing insert-time error instead.
      await ctx.pool.query('CREATE SCHEMA IF NOT EXISTS "drizzle"');
      await ctx.pool.query(
        'CREATE VIEW "drizzle"."__events_public_orders" AS SELECT 1 AS placeholder',
      );

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
      });

      const error = await captureStartRejection(runtime);

      expect(error.message).toContain('__events_public_orders');
      expect(error.message).toContain('does not exist');
    } finally {
      await sourceSql.end();
      await ctx.pool.query(`DROP PUBLICATION IF EXISTS ${publicationName}`).catch(() => {});
      await teardownGuardScenario(ctx);
    }
  });
});
