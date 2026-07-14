/**
 * Integration proof: the LOCKED backfill/resume mechanism (STATE.md §Decisions, phase-19
 * plan-04) auto-heals a lost or invalidated replication slot — mid-run and across a restart —
 * without ever terminating the runtime or consuming a reconnect retry (D-02), recreating via an
 * exported snapshot with epoch rotation and eager events-table seeding (D-03), and gaplessly
 * re-baselining any live embedded collection anchored at the same snapshot (D-01/D-02).
 *
 * Each scenario builds its own standalone ephemeral database (bare `orders` table only —
 * reconcile() self-provisions the publication + REPLICA IDENTITY FULL + events schema exactly
 * as it does under normal boot) so parallel runs cannot collide, and tears itself down in a
 * `finally` block.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseClient } from 'drizzle-pulse/client/embedded';
import { createPulseRegistry, expose, LogLevel } from 'drizzle-pulse/server';
import { createPulseRouter as createServerRouter } from 'drizzle-pulse/server/router';
import type { Hono } from 'hono';
import type { Pool } from 'pg';
import postgres from 'postgres';
import { orders, ordersByStatusArgsSchema } from './fixtures/minimal-orders/schema.js';
import {
  baseDatabaseUrl,
  buildDatabaseUrl,
  createQuietPool,
  type PullCursor,
  pullClient,
  randomSuffix,
  subscribeClient,
  withQuietPostgresUrl,
} from './helpers/test-harness.js';

const adminPool = createQuietPool(baseDatabaseUrl());

afterAll(async () => {
  await adminPool.end();
});

// Bounded async poller — avoids fixed sleeps while bounding test duration.
async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 8000,
  pollIntervalMs = 50,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await new Promise<void>((resolve) => setTimeout(resolve, pollIntervalMs));
  }
}

const ordersByStatus = pulse(orders)
  .args(ordersByStatusArgsSchema)
  .order('asc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

function buildRegistry() {
  return createPulseRegistry({ ordersByStatus });
}

async function createScenarioDatabase(label: string) {
  const databaseName = `pulse_slotrec_${label}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const databaseUrl = buildDatabaseUrl(baseDatabaseUrl(), databaseName);
  const pool = createQuietPool(databaseUrl);

  // Deliberately absent: the publication AND REPLICA IDENTITY FULL — reconcile() self-
  // provisions both at boot (embedded needs WAL, A3), same as every other scenario in this
  // suite that starts from a bare table.
  await pool.query(`
    CREATE TABLE "orders" (
      "id" serial PRIMARY KEY,
      "driver_id" integer,
      "status" text DEFAULT 'requested' NOT NULL,
      "price" numeric NOT NULL,
      "created_at" timestamp with time zone DEFAULT now() NOT NULL
    )
  `);

  return { databaseName, databaseUrl, pool };
}

function buildRuntime(databaseUrl: string, publicationName: string, slotName: string) {
  const sourceSql = postgres(withQuietPostgresUrl(databaseUrl));
  const sourceDb = drizzle({ client: sourceSql });
  const registry = buildRegistry();
  const runtime = expose(registry, {
    databaseUrl,
    sourceDb,
    pull: true,
    wal: { publicationName, slotName },
    logLevel: LogLevel.Error,
  });
  const router: Hono = createServerRouter(runtime.handlers, { userId: null });
  return { runtime, router, sourceSql };
}

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

// Terminates the walsender backing `slotName` then drops the slot, poll-retrying on 55006
// (object_in_use — the "active" flag can lag the backend's actual termination by a beat).
async function forceSlotLoss(pool: Pool, slotName: string): Promise<void> {
  const { rows } = await pool.query<{ active_pid: number | null }>(
    `SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1`,
    [slotName],
  );
  const activePid = rows[0]?.active_pid;
  if (activePid) {
    await pool.query(`SELECT pg_terminate_backend($1)`, [activePid]);
  }
  await dropSlotWithRetry(pool, slotName);
}

async function dropScenarioDatabase(databaseName: string): Promise<void> {
  // A just-recreated slot on this database can still be settling — don't race DROP DATABASE
  // against it.
  await waitFor(async () => {
    const { rows } = await adminPool.query(
      `SELECT 1 FROM pg_replication_slots WHERE database = $1`,
      [databaseName],
    );
    return rows.length === 0;
  }).catch(() => {
    // Best-effort: fall through to the terminate-and-drop below regardless.
  });
  await adminPool.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [databaseName],
  );
  await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
}

async function eventsTableEpoch(pool: Pool): Promise<string | undefined> {
  const { rows } = await pool.query<{ epoch: string }>(
    `SELECT epoch FROM "drizzle_pulse"."pulse_meta" WHERE table_name = 'public_orders'`,
  );
  return rows[0]?.epoch;
}

async function snapshotSeedCount(pool: Pool): Promise<number> {
  const { rows } = await pool.query(
    `SELECT 1 FROM "drizzle_pulse"."public_orders" WHERE "$op" = 'snapshot'`,
  );
  return rows.length;
}

async function pullUntilEvents(
  router: Hono,
  cursor: PullCursor,
  timeoutMs = 8000,
): Promise<Awaited<ReturnType<typeof pullClient>>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await pullClient(router, cursor);
    if (result.events.length > 0 || result.reset) return result;
    if (Date.now() >= deadline) throw new Error(`pullUntilEvents timed out after ${timeoutMs}ms`);
    await new Promise<void>((resolve) => setTimeout(resolve, 100));
  }
}

describe('Slot recovery (DRIVER-02): LOCKED backfill/resume auto-heal', () => {
  test('mid-run: slot loss auto-heals — no terminal error, gapless embedded convergence, PullResetResponse on the stale cursor, fresh $op=snapshot seed', async () => {
    const { databaseName, databaseUrl, pool } = await createScenarioDatabase('midrun');
    const publicationName = `slotrec_midrun_pub_${randomSuffix()}`;
    const slotName = `slotrec_midrun_slot_${randomSuffix()}`;
    const { runtime, router, sourceSql } = buildRuntime(databaseUrl, publicationName, slotName);

    let terminalError: Error | null = null;
    runtime.onTerminalError((error) => {
      terminalError = error;
    });

    try {
      await runtime.start();

      const client = createPulseClient(runtime);
      const collection = await client.ordersByStatus({ status: 'accepted' });

      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );
      await waitFor(() => collection.list().length === 1);

      const staleCursor = await subscribeClient(router, 'ordersByStatus', { status: 'accepted' });
      expect(staleCursor.rows).toHaveLength(1);

      const epochBefore = await eventsTableEpoch(pool);
      expect(epochBefore).toBeDefined();

      await forceSlotLoss(pool, slotName);

      // Downtime delta — written while the slot is gone, before the runtime reconnects.
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );

      // A new slot row exists and the embedded collection converges to full source truth,
      // including the downtime row — the snapshot-anchored re-baseline is gapless.
      await waitFor(async () => {
        const { rows } = await pool.query(
          `SELECT 1 FROM pg_replication_slots WHERE slot_name = $1`,
          [slotName],
        );
        return rows.length > 0;
      });
      await waitFor(() => collection.list().length === 2);
      expect(new Set(collection.list().map((row) => row.driverId))).toEqual(new Set([1, 2]));

      // No terminal error — the recreate auto-healed inside the reconnect cycle.
      expect(terminalError).toBeNull();

      // Epoch rotated (D-03) and a fresh $op='snapshot' seed row was written from the exported
      // snapshot into the recreated events table.
      const epochAfter = await eventsTableEpoch(pool);
      expect(epochAfter).toBeDefined();
      expect(epochAfter).not.toBe(epochBefore);
      expect(await snapshotSeedCount(pool)).toBeGreaterThan(0);

      // The pre-loss HTTP cursor is stale against the rotated epoch — its next pull resets.
      const pulled = await pullClient(router, staleCursor);
      expect(pulled.reset).toBe(true);

      // A fresh subscribe reflects full current state (subscribe reads the live source table,
      // not the events table).
      const freshCursor = await subscribeClient(router, 'ordersByStatus', { status: 'accepted' });
      expect(freshCursor.rows).toHaveLength(2);

      // Post-recovery events carry on.
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (3, 'accepted', 30)`,
      );
      await waitFor(() => collection.list().length === 3);
      const postRecoveryPull = await pullUntilEvents(router, freshCursor);
      expect(postRecoveryPull.events.length).toBeGreaterThan(0);

      collection.dispose();
    } finally {
      await runtime.stop();
      await sourceSql.end();
      await pool.end();
      await dropScenarioDatabase(databaseName);
    }
  });

  test('boot-time: slot loss while stopped recreates + rotates the epoch on next boot, and the pipeline is live afterwards', async () => {
    const { databaseName, databaseUrl, pool } = await createScenarioDatabase('boot');
    const publicationName = `slotrec_boot_pub_${randomSuffix()}`;
    const slotName = `slotrec_boot_slot_${randomSuffix()}`;

    const first = buildRuntime(databaseUrl, publicationName, slotName);
    let epochBefore: string | undefined;

    try {
      // reconcile() (which sets pulse_meta's initial epoch) completes inside start() before it
      // resolves — the epoch is already readable here, no poll needed.
      await first.runtime.start();
      epochBefore = await eventsTableEpoch(pool);
      expect(epochBefore).toBeDefined();

      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );
      const preStopCursor = await subscribeClient(first.router, 'ordersByStatus', {
        status: 'accepted',
      });
      const preStopPull = await pullUntilEvents(first.router, preStopCursor);
      expect(preStopPull.events.length).toBeGreaterThan(0);
    } finally {
      await first.runtime.stop();
      await first.sourceSql.end();
    }

    // Drop the slot while stopped — first-boot-equivalent continuity breakage (no active
    // walsender to terminate; the runtime is fully stopped).
    await dropSlotWithRetry(pool, slotName);

    const second = buildRuntime(databaseUrl, publicationName, slotName);
    try {
      await second.runtime.start();

      await waitFor(async () => {
        const { rows } = await pool.query(
          `SELECT 1 FROM pg_replication_slots WHERE slot_name = $1`,
          [slotName],
        );
        return rows.length > 0;
      });

      const epochAfter = await eventsTableEpoch(pool);
      expect(epochAfter).toBeDefined();
      expect(epochAfter).not.toBe(epochBefore);

      const cursor = await subscribeClient(second.router, 'ordersByStatus', { status: 'accepted' });
      expect(cursor.rows).toHaveLength(1);

      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );
      const pulled = await pullUntilEvents(second.router, cursor);
      expect(pulled.events.length).toBeGreaterThan(0);
    } finally {
      await second.runtime.stop();
      await second.sourceSql.end();
      await pool.end();
      await dropScenarioDatabase(databaseName);
    }
  });
});
