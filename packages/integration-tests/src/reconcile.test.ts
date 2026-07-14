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

// Bounded async poller — avoids fixed sleeps while bounding test duration.
async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10000,
  pollIntervalMs = 100,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await new Promise<void>((resolve) => setTimeout(resolve, pollIntervalMs));
  }
}

// Poll-retry a slot drop (cloned from slot-recovery.test.ts): the previous owning backend's
// "active" flag can lag its actual termination by a beat, so a single attempt can spuriously
// hit 55006 (object_in_use). Reconcile scenarios never created a persistent slot before (only
// provision() was exercised) — a full start() does, and G6 must drop it or leak against the
// shared container's 4-slot budget.
async function dropSlotWithRetry(pool: Pool, slotName: string): Promise<void> {
  await waitFor(async () => {
    try {
      await pool.query(`SELECT pg_drop_replication_slot($1)`, [slotName]);
      return true;
    } catch (e) {
      const code = (e as { code?: string }).code;
      if (code === '42704') return true; // undefined_object — already gone
      if (code === '55006') return false; // object_in_use — walsender still attached, retry
      throw e;
    }
  }, 5000);
}

async function streamLastLsn(pool: Pool, slotName: string): Promise<string | undefined> {
  const { rows } = await pool.query<{ last_lsn: string }>(
    `SELECT last_lsn FROM drizzle_pulse.pulse_stream WHERE slot_name = $1`,
    [slotName],
  );
  return rows[0]?.last_lsn;
}

async function snapshotRowCount(pool: Pool): Promise<number> {
  const { rows } = await pool.query(
    `SELECT 1 FROM drizzle_pulse.public_orders WHERE "$op" = 'snapshot'`,
  );
  return rows.length;
}

async function nonSnapshotEventCount(pool: Pool): Promise<number> {
  const { rows } = await pool.query(
    `SELECT 1 FROM drizzle_pulse.public_orders WHERE "$op" <> 'snapshot'`,
  );
  return rows.length;
}

// DISCOVERY (see slot-resume.test.ts G1/G2 for the full empirical trail against unmodified
// expose.ts + minipg): resolveSlotStartup's continuity gate compares the persisted
// pulse_stream.last_lsn watermark (a commit's own record LSN) against the slot's
// confirmed_flush_lsn (the transaction's end LSN, always strictly greater after a normal ack) —
// unreachable via ordinary stop/restart once any commit has landed. Seeding the watermark to the
// observed confirmed_flush_lsn reproduces the precondition deterministically, isolating the
// reconcile()-level DDL-divergence recreate this test targets from the separate (and here
// irrelevant) question of whether the slot itself gets recreated. No production code changes.
async function seedContinuousWatermark(pool: Pool, slotName: string): Promise<string> {
  const { rows } = await pool.query<{ confirmed_flush_lsn: string | null }>(
    `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1`,
    [slotName],
  );
  const confirmedFlushLsn = rows[0]?.confirmed_flush_lsn;
  if (!confirmedFlushLsn) {
    throw new Error(`no confirmed_flush_lsn found for slot '${slotName}'`);
  }
  await pool.query(`UPDATE drizzle_pulse.pulse_stream SET last_lsn = $2 WHERE slot_name = $1`, [
    slotName,
    confirmedFlushLsn,
  ]);
  return confirmedFlushLsn;
}

// Clones setupHealthyScenario's runtime construction against the SAME database/publication/slot
// (derived deterministically from `label`) — a second boot targeting the first scenario's
// database, not a second scenario.
function buildSecondRuntime(
  databaseUrl: string,
  label: string,
  logLevel: LogLevel = LogLevel.Error,
) {
  const sourceSql = postgres(withQuietPostgresUrl(databaseUrl));
  const registry = createPulseRegistry({ orders: pulse(orders).query() });
  const runtime = expose(registry, {
    databaseUrl,
    sourceDb: drizzle({ client: sourceSql }),
    pull: true,
    wal: { publicationName: `reconcile_pub_${label}`, slotName: `reconcile_slot_${label}` },
    logLevel,
  });
  return { runtime, sourceSql };
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

  test('DDL divergence at boot with an intact slot: start() recreates, reseeds via ensureBaselines, resumes the slot, and streams (G6)', async () => {
    const label = 'g6full';
    const s = await setupHealthyScenario(label);
    const databaseUrl = buildDatabaseUrl(baseDatabaseUrl(), s.databaseName);
    const slotName = `reconcile_slot_${label}`;

    let epoch1: string | undefined;
    let lsn1: string | undefined;

    try {
      try {
        // Full boot: reconcile() provisions, then connectReplication() creates the persistent
        // slot (unlike the provision()-only tests above, which never open a replication stream).
        await s.runtime.start();

        await s.pool.query(
          `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
        );
        await waitFor(async () => (await nonSnapshotEventCount(s.pool)) >= 1);

        epoch1 = await metaEpoch(s.pool);
        expect(epoch1).toBeDefined();
        lsn1 = await streamLastLsn(s.pool, slotName);
        expect(lsn1).toBeDefined();
      } finally {
        // Clean stop — the pull:true slot is persistent and survives it, intact.
        await s.runtime.stop();
      }

      // Corrupt the stored hash exactly as the divergence test above does, forcing reconcile()
      // to recreate the events table and rotate the epoch on the next boot.
      await s.pool.query(
        `UPDATE drizzle_pulse.pulse_meta SET ddl_hash = 'stale' WHERE table_name = 'public_orders'`,
      );

      // See seedContinuousWatermark's DISCOVERY comment: closes the structural
      // watermark-vs-confirmed_flush gap so resolveSlotStartup's continuity precondition
      // actually holds — this test's "slot resumed, not recreated" assertion needs the real
      // resume branch, isolated from the reconcile()-level recreate it targets.
      lsn1 = await seedContinuousWatermark(s.pool, slotName);

      const errorSpy = spyOn(console, 'error').mockImplementation(() => {});
      const second = buildSecondRuntime(databaseUrl, label);
      try {
        await second.runtime.start();

        // Epoch rotated exactly once by reconcile()'s DDL-divergence recreate.
        const epoch2 = await metaEpoch(s.pool);
        expect(epoch2).toBeDefined();
        expect(epoch2).not.toBe(epoch1);
        expect(second.runtime.getEpochForQuery('orders')).toBe(epoch2);

        // The recreated table holds ensureBaselines' snapshot seed and none of the
        // pre-divergence event rows (TRUNCATEd by reconcile()'s recreate).
        expect(await snapshotRowCount(s.pool)).toBeGreaterThanOrEqual(1);
        expect(await nonSnapshotEventCount(s.pool)).toBe(0);

        // The slot itself was resumed, not recreated: no recreate log, and pulse_stream.last_lsn
        // is exactly the value it held before this boot (recoverSlot would have overwritten it
        // with a fresh consistentPoint).
        expect(
          errorSpy.mock.calls.some((call: unknown[]) => String(call[0]).includes('recreated')),
        ).toBe(false);
        expect(await streamLastLsn(s.pool, slotName)).toBe(lsn1);

        // The pipeline is live end-to-end on the recreated events table.
        await s.pool.query(
          `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
        );
        await waitFor(async () => (await nonSnapshotEventCount(s.pool)) >= 1);
      } finally {
        errorSpy.mockRestore();
        await second.runtime.stop();
        await second.sourceSql.end();
      }
    } finally {
      await dropSlotWithRetry(s.pool, slotName).catch(() => {});
      await teardownScenario(s);
    }
  });
});
