/**
 * Integration proof: runtime-owned events-table DDL. expose().provision() (the same
 * reconcile path start() runs, minus the replication stream) creates/recreates events tables
 * and their pulse_meta bookkeeping against real Postgres, rotating an epoch on every recreate
 * and sweeping orphans. Each scenario builds its own healthy standalone database per the
 * test-isolation convention and tears itself down in a finally block.
 */

import { afterAll, describe, expect, spyOn, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseRegistry, expose, LogLevel } from 'drizzle-pulse/server';
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

const adminPool = createQuietPool(baseDatabaseUrl());

afterAll(async () => {
  await adminPool.end();
});

async function setupHealthyScenario(label: string, logLevel: LogLevel = LogLevel.Error) {
  const databaseName = `pulse_reconcile_${label}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const databaseUrl = buildDatabaseUrl(baseDatabaseUrl(), databaseName);
  const pool = createQuietPool(databaseUrl);

  await pool.query(`
    CREATE TABLE "orders" (
      "id" serial PRIMARY KEY,
      "driver_id" integer,
      "status" text DEFAULT 'requested' NOT NULL,
      "price" numeric NOT NULL,
      "created_at" timestamp with time zone DEFAULT now() NOT NULL
    )
  `);
  await pool.query('ALTER TABLE "orders" REPLICA IDENTITY FULL');
  await pool.query(`CREATE PUBLICATION reconcile_pub_${label} FOR ALL TABLES`);

  const sourceSql = postgres(withQuietPostgresUrl(databaseUrl));
  const registry = createPulseRegistry({ orders: pulse(orders).query() });
  const runtime = expose(registry, {
    databaseUrl,
    sourceDb: drizzle({ client: sourceSql }),
    pull: true,
    wal: { publicationName: `reconcile_pub_${label}`, slotName: `reconcile_slot_${label}` },
    logLevel,
  });

  return { databaseName, pool, sourceSql, runtime };
}

async function teardownScenario(scenario: {
  databaseName: string;
  pool: Pool;
  sourceSql: ReturnType<typeof postgres>;
}): Promise<void> {
  await scenario.sourceSql.end();
  await scenario.pool.end();
  await adminPool.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [scenario.databaseName],
  );
  await adminPool.query(`DROP DATABASE IF EXISTS "${scenario.databaseName}"`);
}

async function metaEpoch(pool: Pool): Promise<string | undefined> {
  const { rows } = await pool.query<{ epoch: string }>(
    `SELECT epoch FROM drizzle_pulse.pulse_meta WHERE table_name = 'public_orders'`,
  );
  return rows[0]?.epoch;
}

describe('runtime-owned events-table reconcile', () => {
  test('fresh provision() creates the schema, events table, and a pulse_meta row', async () => {
    const s = await setupHealthyScenario('fresh');
    try {
      await s.runtime.provision();

      const table = await s.pool.query(
        `SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse' AND c.relname = 'public_orders' AND c.relkind = 'r'`,
      );
      expect(table.rows).toHaveLength(1);

      const meta = await s.pool.query<{ ddl_hash: string; epoch: string }>(
        `SELECT ddl_hash, epoch FROM drizzle_pulse.pulse_meta WHERE table_name = 'public_orders'`,
      );
      expect(meta.rows).toHaveLength(1);
      expect(meta.rows[0]?.ddl_hash).toBeTruthy();
      expect(s.runtime.getEpochForQuery('orders')).toBe(meta.rows[0]?.epoch);
    } finally {
      await teardownScenario(s);
    }
  });

  test('second provision() is a no-op: the epoch stays stable', async () => {
    const s = await setupHealthyScenario('noop');
    try {
      await s.runtime.provision();
      const firstEpoch = s.runtime.getEpochForQuery('orders');
      const firstDbEpoch = await metaEpoch(s.pool);

      await s.runtime.provision();
      expect(s.runtime.getEpochForQuery('orders')).toBe(firstEpoch);
      expect(await metaEpoch(s.pool)).toBe(firstDbEpoch);
    } finally {
      await teardownScenario(s);
    }
  });

  test('a diverged DDL hash triggers recreate and rotates the epoch', async () => {
    const s = await setupHealthyScenario('rotate');
    try {
      await s.runtime.provision();
      const firstEpoch = s.runtime.getEpochForQuery('orders');
      expect(firstEpoch).toBeTruthy();

      // Simulate a shape change without redefining the source table: corrupt the stored hash
      // so it no longer matches the freshly rendered DDL.
      await s.pool.query(
        `UPDATE drizzle_pulse.pulse_meta SET ddl_hash = 'stale' WHERE table_name = 'public_orders'`,
      );

      await s.runtime.provision();
      const secondEpoch = s.runtime.getEpochForQuery('orders');
      expect(secondEpoch).toBeTruthy();
      expect(secondEpoch).not.toBe(firstEpoch);
      expect(await metaEpoch(s.pool)).toBe(secondEpoch);
    } finally {
      await teardownScenario(s);
    }
  });

  test('orphan sweep drops meta-registered tables and warns (only) about unmanaged ones', async () => {
    const s = await setupHealthyScenario('orphan', LogLevel.Info);
    const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});
    try {
      await s.runtime.provision();

      // A meta-registered orphan (a table + pulse_meta row for a source no longer registered)
      // and an unmanaged physical table (no pulse_meta row) sharing the events schema.
      await s.pool.query('CREATE TABLE drizzle_pulse.public_ghost ("id" integer)');
      await s.pool.query(
        `INSERT INTO drizzle_pulse.pulse_meta (table_name, ddl_hash, epoch) VALUES ('public_ghost', 'h', gen_random_uuid())`,
      );
      await s.pool.query('CREATE TABLE drizzle_pulse.stray ("id" integer)');

      warnSpy.mockClear();
      await s.runtime.provision();

      const ghostTable = await s.pool.query(
        `SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse' AND c.relname = 'public_ghost'`,
      );
      expect(ghostTable.rows).toHaveLength(0);
      const ghostMeta = await s.pool.query(
        `SELECT 1 FROM drizzle_pulse.pulse_meta WHERE table_name = 'public_ghost'`,
      );
      expect(ghostMeta.rows).toHaveLength(0);

      // Unmanaged table is left untouched, but warned about.
      const strayTable = await s.pool.query(
        `SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse' AND c.relname = 'stray'`,
      );
      expect(strayTable.rows).toHaveLength(1);
      const warnedStray = warnSpy.mock.calls.some((call) => String(call[0]).includes('stray'));
      expect(warnedStray).toBe(true);

      // The registered events table survives the sweep.
      const ordersTable = await s.pool.query(
        `SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse' AND c.relname = 'public_orders'`,
      );
      expect(ordersTable.rows).toHaveLength(1);
    } finally {
      warnSpy.mockRestore();
      await teardownScenario(s);
    }
  });
});
