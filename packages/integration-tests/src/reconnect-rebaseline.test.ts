/**
 * G7 (WAL-01): a REAL dropped walsender socket — not the private onReplicationStart hook —
 * must still trigger the embedded collection's re-baseline handshake: one onChange with an
 * empty event batch and a fresh watermark lsn, no row loss or duplication, and continued
 * delivery afterwards. Supersedes the deleted resilience.test.ts, which drove the same
 * assertions off a faked edge ((runtime as any).onReplicationStart()).
 *
 * G5 (WAL-01): a reconnect re-baseline whose collection baseline SELECT outlives the 5s
 * REBASELINE_PIN_WINDOW_MS still converges gaplessly — pins maybeReleaseSnapshotBaseline's
 * release-awaits-in-flight-read rule (expose.ts): the window timer firing while `inFlight > 0`
 * must defer the release rather than tear down the pin under the live read.
 *
 * Uses the same split URL configuration as copydone-reconnect.test.ts: the runtime's
 * `databaseUrl` (walsender + admin pool) routes through the test-only TCP proxy, `sourceDb`
 * stays on the direct connection.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseClient } from 'drizzle-pulse/client/embedded';
import { createPulseRegistry, expose, LogLevel } from 'drizzle-pulse/server';
import type { Pool } from 'pg';
import postgres from 'postgres';
import { orders, ordersByStatusArgsSchema } from './fixtures/minimal-orders/schema.js';
import {
  baseDatabaseUrl,
  buildDatabaseUrl,
  createQuietPool,
  randomSuffix,
  withQuietPostgresUrl,
} from './helpers/test-harness.js';
import { proxiedDatabaseUrl, startWalProxy } from './helpers/wal-proxy.js';

const adminPool = createQuietPool(baseDatabaseUrl());

afterAll(async () => {
  await adminPool.end();
});

const LSN_PATTERN = /^[0-9A-Fa-f]+\/[0-9A-Fa-f]+$/;

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
  const databaseName = `pulse_reconnrb_${label}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const directUrl = buildDatabaseUrl(baseDatabaseUrl(), databaseName);
  const pool = createQuietPool(directUrl);

  // Deliberately absent: the publication AND REPLICA IDENTITY FULL — reconcile() self-
  // provisions both at boot, same as every other self-managed scenario in this suite.
  await pool.query(`
    CREATE TABLE "orders" (
      "id" serial PRIMARY KEY,
      "driver_id" integer,
      "status" text DEFAULT 'requested' NOT NULL,
      "price" numeric NOT NULL,
      "created_at" timestamp with time zone DEFAULT now() NOT NULL
    )
  `);

  return { databaseName, directUrl, pool };
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

describe('Reconnect re-baseline (WAL-01, G7/G5)', () => {
  test('G7: a real dropped socket triggers a re-baseline and the collection stays consistent', async () => {
    const base = new URL(baseDatabaseUrl());
    const proxy = startWalProxy(base.hostname, Number(base.port));
    const proxyPort = await proxy.listen();

    const { databaseName, directUrl, pool } = await createScenarioDatabase('g7');
    const publicationName = `reconnrb_g7_pub_${randomSuffix()}`;
    const slotName = `reconnrb_g7_slot_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(directUrl));

    const runtime = expose(buildRegistry(), {
      databaseUrl: proxiedDatabaseUrl(directUrl, proxyPort),
      sourceDb: drizzle({ client: sourceSql }),
      pull: true,
      wal: { publicationName, slotName },
      logLevel: LogLevel.Error,
    });

    let terminalError: Error | null = null;
    runtime.onTerminalError((error) => {
      terminalError = error;
    });

    try {
      await runtime.start();

      const client = createPulseClient(runtime);
      const collection = await client.ordersByStatus({ status: 'accepted' });

      const changes: Array<{ events: readonly unknown[]; lsn: string }> = [];
      collection.onChange((c) => changes.push(c));

      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );
      await waitFor(() => collection.list().length === 2);
      const changesBeforeReconnect = changes.length;

      // Real edge: destroys the walsender's client socket — the minipg iterator fails,
      // handleDisconnect schedules the reconnect (backoff ~1-2s). resolveSlotStartup's
      // continuity gate is unreachable once any commit has landed (STATE.md: watermark only
      // advances on recreate, confirmed_flush always outruns it), so this takes the recreate
      // path in practice — onReplicationStart still fires the reconnect listeners either way.
      proxy.dropClient();

      // waitFor timeout 10000ms — the backoff makes the old 2000ms timeouts too tight.
      await waitFor(() => changes.length === changesBeforeReconnect + 1, 10000);

      const rebaselineChange = changes[changesBeforeReconnect]!;
      expect(rebaselineChange.events).toEqual([]);
      expect(rebaselineChange.lsn).toMatch(LSN_PATTERN);

      expect(collection.list().length).toBe(2);
      expect(new Set(collection.list().map((r) => r.id)).size).toBe(2);

      // The pipeline keeps delivering after the reconnect edge.
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (3, 'accepted', 30)`,
      );
      await waitFor(() => collection.list().length === 3, 10000);

      expect(terminalError).toBeNull();

      collection.dispose();
    } finally {
      await runtime.stop();
      await sourceSql.end();
      await dropSlotWithRetry(pool, slotName).catch(() => {});
      await pool.end();
      await proxy.close();
      await dropScenarioDatabase(databaseName);
    }
  });

  test('G5: a reconnect re-baseline whose SELECT outlives the 5s pin window still converges gaplessly', async () => {
    const base = new URL(baseDatabaseUrl());
    const proxy = startWalProxy(base.hostname, Number(base.port));
    const proxyPort = await proxy.listen();

    const { databaseName, directUrl, pool } = await createScenarioDatabase('g5');
    const publicationName = `reconnrb_g5_pub_${randomSuffix()}`;
    const slotName = `reconnrb_g5_slot_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(directUrl));

    const runtime = expose(buildRegistry(), {
      databaseUrl: proxiedDatabaseUrl(directUrl, proxyPort),
      sourceDb: drizzle({ client: sourceSql }),
      pull: true,
      wal: { publicationName, slotName },
      logLevel: LogLevel.Error,
    });

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

      const epochBefore = await eventsTableEpoch(pool);
      expect(epochBefore).toBeDefined();

      // Arm the stall BEFORE forcing the recreate path. recoverSlot's sequence is
      // rotateAndSeedEvents (admin traffic, must NOT stall) -> openRebaselinePin (5s window
      // timer starts) -> rep.start() sends START_REPLICATION on the walsender, the first
      // observable event after the pin opens — arming here guarantees the window is already
      // running while the collection's baseline SELECT response is held.
      proxy.stallAdminOnStartReplication(6500);

      await forceSlotLoss(pool, slotName);
      const tEdge = Date.now();

      // Downtime delta — the gap the pinned snapshot must cover.
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );

      // Gapless convergence: both orders arrive through the pinned baseline, exactly what
      // breaks if the pin were released while the read is still in flight.
      await waitFor(() => collection.list().length === 2, 15000);
      const tConverged = Date.now();

      expect(new Set(collection.list().map((r) => r.driverId))).toEqual(new Set([1, 2]));
      // Convergence happened after the window closed underneath the held read — proves the
      // SELECT actually outlived REBASELINE_PIN_WINDOW_MS rather than completing inside it.
      expect(tConverged - tEdge).toBeGreaterThan(5000);

      const epochAfter = await eventsTableEpoch(pool);
      expect(epochAfter).toBeDefined();
      expect(epochAfter).not.toBe(epochBefore);

      expect(terminalError).toBeNull();

      // Post-recovery delivery still arrives.
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (3, 'accepted', 30)`,
      );
      await waitFor(() => collection.list().length === 3, 10000);

      collection.dispose();
    } finally {
      await runtime.stop();
      await sourceSql.end();
      await dropSlotWithRetry(pool, slotName).catch(() => {});
      await pool.end();
      await proxy.close();
      await dropScenarioDatabase(databaseName);
    }
  });
});
