/**
 * Integration proof: expose()'s boot reconciliation against real Postgres, through the full
 * start() path (WAL stream and all).
 *
 * Most preconditions the guard once only asserted are now self-provisioned inside reconcile():
 * a missing publication is created, a missing member is added, a source without REPLICA
 * IDENTITY FULL is altered. These scenarios prove start() heals a bare/partial setup and then
 * boots. wal_level stays the one fail-closed assert (the runtime can't fix a server-wide
 * setting), but it can't be toggled on the shared test server, so it has no live case here.
 * Finer membership/RI coverage lives in reconcile-publication.test.ts (provision() path). Each
 * scenario gets its own randomly-named database/publication/slot and tears itself down in a
 * `finally` block.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseRegistry, expose } from 'drizzle-pulse/server';
import type { Pool } from 'pg';
import postgres from 'postgres';
import { orders } from './fixtures/minimal-orders/schema.js';
import {
  baseDatabaseUrl,
  buildDatabaseUrl,
  createQuietPool,
  randomSuffix,
  withQuietPostgresUrl,
} from './helpers/test-harness.js';

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

async function setReplicaIdentityFull(pool: Pool): Promise<void> {
  await pool.query('ALTER TABLE "orders" REPLICA IDENTITY FULL');
}

async function publicationMembers(pool: Pool, publicationName: string): Promise<string[]> {
  const { rows } = await pool.query<{ qualified: string }>(
    `SELECT schemaname || '.' || tablename AS qualified FROM pg_publication_tables WHERE pubname = $1 ORDER BY qualified`,
    [publicationName],
  );
  return rows.map((row) => row.qualified);
}

async function ordersReplicaIdentity(pool: Pool): Promise<string | undefined> {
  const { rows } = await pool.query<{ relreplident: string }>(
    `SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'orders'`,
  );
  return rows[0]?.relreplident;
}

describe('Startup reconcile (self-provisioning)', () => {
  test('(a) bare database: start() self-provisions the publication + REPLICA IDENTITY, then boots', async () => {
    const ctx = await setupGuardScenario('a');
    const publicationName = `guard_pub_a_${randomSuffix()}`;
    const slotName = `guard_slot_a_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      // Deliberately absent: the publication AND REPLICA IDENTITY FULL — reconcile() creates
      // both, so start() succeeds where it once rejected.

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
        logLevel: 'error',
      });

      await runtime.start();
      expect(runtime.isRunning).toBe(true);
      expect(await publicationMembers(ctx.pool, publicationName)).toEqual(['public.orders']);
      expect(await ordersReplicaIdentity(ctx.pool)).toBe('f');

      await runtime.stop();
      expect(runtime.isRunning).toBe(false);
    } finally {
      await sourceSql.end();
      await ctx.pool.query(`DROP PUBLICATION IF EXISTS ${publicationName}`).catch(() => {});
      await dropSlotIfExists(slotName).catch(() => {});
      await teardownGuardScenario(ctx);
    }
  });

  test('(b) defaults: wal omitted entirely self-provisions a publication named drizzle_pulse', async () => {
    const ctx = await setupGuardScenario('b');
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      // No wal config supplied — proves the default publication name (drizzle_pulse) flows all
      // the way into the CREATE PUBLICATION reconcile() runs. provision() avoids slot setup.

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        logLevel: 'error',
      });

      await runtime.provision();
      expect(await publicationMembers(ctx.pool, 'drizzle_pulse')).toEqual(['public.orders']);
    } finally {
      await sourceSql.end();
      await teardownGuardScenario(ctx);
    }
  });

  test('(c) healthy FOR ALL TABLES setup: boot creates the events table + pulse_meta, runtime starts then stops cleanly', async () => {
    const ctx = await setupGuardScenario('c');
    const publicationName = `guard_pub_c_${randomSuffix()}`;
    const slotName = `guard_slot_c_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      await setReplicaIdentityFull(ctx.pool);
      await ctx.pool.query(`CREATE PUBLICATION ${publicationName} FOR ALL TABLES`);
      // Deliberately absent: the events table — the runtime creates it at boot.

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
      });

      await runtime.start();
      expect(runtime.isRunning).toBe(true);

      // Runtime-owned DDL: boot created the events table and its pulse_meta bookkeeping row.
      const eventsTable = await ctx.pool.query(
        `SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse' AND c.relname = 'public_orders' AND c.relkind = 'r'`,
      );
      expect(eventsTable.rows).toHaveLength(1);
      const metaRow = await ctx.pool.query<{ epoch: string }>(
        `SELECT epoch FROM drizzle_pulse.pulse_meta WHERE table_name = 'public_orders'`,
      );
      expect(metaRow.rows).toHaveLength(1);
      expect(runtime.getEpochForQuery('orders')).toBe(metaRow.rows[0]?.epoch);

      await runtime.stop();
      expect(runtime.isRunning).toBe(false);
    } finally {
      await sourceSql.end();
      await ctx.pool.query(`DROP PUBLICATION IF EXISTS ${publicationName}`).catch(() => {});
      await dropSlotIfExists(slotName).catch(() => {});
      await teardownGuardScenario(ctx);
    }
  });

  test('(d) pulse-owned FOR TABLE publication: start() adds the registered source and un-pulses foreign members', async () => {
    const ctx = await setupGuardScenario('d');
    const publicationName = `guard_pub_d_${randomSuffix()}`;
    const slotName = `guard_slot_d_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(ctx.databaseUrl));

    try {
      await createOrdersSourceTable(ctx.pool);
      await setReplicaIdentityFull(ctx.pool);
      await ctx.pool.query('CREATE TABLE "users" ("id" serial PRIMARY KEY)');
      // A FOR TABLE publication missing the pulsed table but carrying an unregistered one.
      // pulse owns the publication: reconcile() ADDs orders and un-pulses (DROPs) users.
      await ctx.pool.query(`CREATE PUBLICATION ${publicationName} FOR TABLE "users"`);

      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      const runtime = expose(registry, {
        databaseUrl: ctx.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName },
        logLevel: 'error',
      });

      await runtime.start();
      expect(runtime.isRunning).toBe(true);
      expect(await publicationMembers(ctx.pool, publicationName)).toEqual(['public.orders']);

      await runtime.stop();
      expect(runtime.isRunning).toBe(false);
    } finally {
      await sourceSql.end();
      await ctx.pool.query(`DROP PUBLICATION IF EXISTS ${publicationName}`).catch(() => {});
      await dropSlotIfExists(slotName).catch(() => {});
      await teardownGuardScenario(ctx);
    }
  });
});
