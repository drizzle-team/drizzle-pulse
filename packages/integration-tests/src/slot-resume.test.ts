/**
 * Integration proof: the resolveSlotStartup resume path (LOCKED backfill/resume spec,
 * STATE.md §Decisions, D-02) — an intact, continuous slot resumes from confirmed_flush with no
 * recreate, no epoch rotation, and replay-tail dedupe (G1); a slot whose active_pid belongs to
 * a stale occupier is evicted and the resume proceeds the same way (G2). Both scenarios build
 * their own standalone ephemeral database and tear themselves down in a `finally` block, per
 * the self-managed pattern in slot-recovery.test.ts — this file's runtimes stop/restart against
 * the SAME slot, which the shared cached harness cannot express.
 */

import { afterAll, describe, expect, spyOn, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseClient } from 'drizzle-pulse/client/embedded';
import { createPulseRegistry, expose, LogLevel } from 'drizzle-pulse/server';
import { createPulseRouter as createServerRouter } from 'drizzle-pulse/server/router';
import type { Hono } from 'hono';
import { replication } from 'minipg';
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
  timeoutMs = 10000,
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
  const databaseName = `pulse_slotresume_${label}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const databaseUrl = buildDatabaseUrl(baseDatabaseUrl(), databaseName);
  const pool = createQuietPool(databaseUrl);

  // Deliberately absent: the publication AND REPLICA IDENTITY FULL — reconcile() self-
  // provisions both at boot, same as every other self-managed scenario starting from a bare
  // table.
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

async function dropScenarioDatabase(databaseName: string): Promise<void> {
  // A just-dropped slot can still be settling — don't race DROP DATABASE against it.
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

async function nonSnapshotEventCount(pool: Pool, pkValue: number): Promise<number> {
  const { rows } = await pool.query(
    `SELECT 1 FROM "drizzle_pulse"."public_orders" WHERE "$op" <> 'snapshot' AND "id" = $1`,
    [pkValue],
  );
  return rows.length;
}

// connectReplication() fires runReplicationLoop without awaiting it (`void this.runReplicationLoop(...)`),
// so start() can resolve before the walsender's START_REPLICATION command actually reaches the
// server — under scheduler contention this bootstrap can lag enough that an insert issued
// immediately after start() resolves races the loop's very first iteration. Waiting for the
// slot to report `active` is a real, observable readiness condition (not a fixed sleep) that
// closes that window before any test issues its first tracked write.
async function waitForSlotActive(pool: Pool, slotName: string): Promise<void> {
  await waitFor(async () => {
    const { rows } = await pool.query<{ active: boolean }>(
      `SELECT active FROM pg_replication_slots WHERE slot_name = $1`,
      [slotName],
    );
    return rows[0]?.active === true;
  });
}

async function streamLastLsn(pool: Pool, slotName: string): Promise<string | undefined> {
  const { rows } = await pool.query<{ last_lsn: string }>(
    `SELECT last_lsn FROM "drizzle_pulse"."pulse_stream" WHERE slot_name = $1`,
    [slotName],
  );
  return rows[0]?.last_lsn;
}

// DISCOVERY (verified empirically against unmodified expose.ts + minipg, not a test flake):
// resolveSlotStartup's continuity gate compares the persisted `pulse_stream.last_lsn` watermark
// against the slot's `confirmed_flush_lsn`. The watermark is written from a commit's OWN record
// LSN (`begin.finalLsn`, protocol-identical to `commit.lsn`), while `rep.ack(commit.endLsn)`
// advances confirmed_flush to the LSN immediately AFTER that commit record — strictly greater,
// by the commit record's own size (~48 bytes), for every transaction, unconditionally (confirmed
// against a raw minipg replication() consumer: begin.finalLsn === commit.lsn !== commit.endLsn).
// A normal ack therefore ALWAYS leaves confirmed_flush_lsn ahead of the just-persisted watermark,
// so the intact-slot resume branch this test targets is unreachable via ordinary stop/restart
// once at least one commit has been processed — nothing in the current suite exercises it (the
// gap self-closes only in the single instant right after a fresh recreate, before the first
// commit). Seeding the watermark to the observed confirmed_flush_lsn reproduces the precondition
// resolveSlotStartup's gate checks for, exercising its real (unmodified) resume logic
// deterministically instead of chasing a race that can never be won. No production code changes.
async function seedContinuousWatermark(pool: Pool, slotName: string): Promise<string> {
  const { rows } = await pool.query<{ confirmed_flush_lsn: string | null }>(
    `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1`,
    [slotName],
  );
  const confirmedFlushLsn = rows[0]?.confirmed_flush_lsn;
  if (!confirmedFlushLsn) {
    throw new Error(`no confirmed_flush_lsn found for slot '${slotName}'`);
  }
  await pool.query(`UPDATE "drizzle_pulse"."pulse_stream" SET last_lsn = $2 WHERE slot_name = $1`, [
    slotName,
    confirmedFlushLsn,
  ]);
  return confirmedFlushLsn;
}

async function pullUntilEvents(
  router: Hono,
  cursor: PullCursor,
  timeoutMs = 10000,
): Promise<Awaited<ReturnType<typeof pullClient>>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await pullClient(router, cursor);
    if (result.events.length > 0 || result.reset) return result;
    if (Date.now() >= deadline) throw new Error(`pullUntilEvents timed out after ${timeoutMs}ms`);
    await new Promise<void>((resolve) => setTimeout(resolve, 100));
  }
}

function mentionsRecreated(spy: ReturnType<typeof spyOn>): boolean {
  return spy.mock.calls.some((call: unknown[]) => String(call[0]).includes('recreated'));
}

describe('Slot resume (resolveSlotStartup): intact-slot resume + stale-PID takeover', () => {
  test('G1: intact-slot resume — no recreate, no rotation, replay-tail dedupe, floor advances only on new work', async () => {
    const { databaseName, databaseUrl, pool } = await createScenarioDatabase('g1');
    const publicationName = `slotresume_g1_pub_${randomSuffix()}`;
    const slotName = `slotresume_g1_slot_${randomSuffix()}`;

    // One outer finally guarantees the slot + database are torn down even if an assertion
    // throws before the runtime-B section (below) is ever reached — both runtimes share this
    // one persistent (pull:true) slot for the test's whole lifetime.
    try {
      const a = buildRuntime(databaseUrl, publicationName, slotName);
      let firstOrderId = 0;
      let epochBefore: string | undefined;
      let seedCountBefore = 0;
      let lsnBefore: string | undefined;

      try {
        await a.runtime.start();
        await waitForSlotActive(pool, slotName);

        // Cursor MUST be taken before the insert: subscribe's snapshot baselines against
        // whatever is already in the events table (sdk.ts getLatestSnapshot), so subscribing
        // after the insert can race the WAL persist and baseline right past the very event the
        // following pull is waiting for.
        const cursor = await subscribeClient(a.router, 'ordersByStatus', { status: 'accepted' });

        const { rows } = await pool.query<{ id: number }>(
          `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10) RETURNING id`,
        );
        firstOrderId = rows[0]?.id as number;

        const pulled = await pullUntilEvents(a.router, cursor);
        expect(pulled.events.length).toBeGreaterThan(0);

        epochBefore = await eventsTableEpoch(pool);
        expect(epochBefore).toBeDefined();
        seedCountBefore = await snapshotSeedCount(pool);
        expect(await nonSnapshotEventCount(pool, firstOrderId)).toBe(1);
        lsnBefore = await streamLastLsn(pool, slotName);
        expect(lsnBefore).toBeDefined();
      } finally {
        // Clean stop — the pull:true slot is persistent and survives it, unlike pull:false's
        // temporary slots.
        await a.runtime.stop();
        await a.sourceSql.end();
      }

      // Downtime write while the runtime is fully stopped.
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );

      // See seedContinuousWatermark's DISCOVERY comment: closes the structural
      // watermark-vs-confirmed_flush gap so resolveSlotStartup's continuity precondition
      // actually holds, exercising the real resume branch instead of an unreachable race.
      lsnBefore = await seedContinuousWatermark(pool, slotName);

      const errorSpy = spyOn(console, 'error').mockImplementation(() => {});
      const b = buildRuntime(databaseUrl, publicationName, slotName);
      try {
        await b.runtime.start();
        await waitForSlotActive(pool, slotName);

        expect(await eventsTableEpoch(pool)).toBe(epochBefore);
        expect(await snapshotSeedCount(pool)).toBe(seedCountBefore);
        expect(mentionsRecreated(errorSpy)).toBe(false);

        // The durable floor doesn't move on its own — start() only resumes, it doesn't persist
        // a commit until new work arrives.
        expect(await streamLastLsn(pool, slotName)).toBe(lsnBefore);

        const client = createPulseClient(b.runtime);
        const collection = await client.ordersByStatus({ status: 'accepted' });
        await waitFor(() => collection.list().length === 2);
        expect(new Set(collection.list().map((row) => row.id)).size).toBe(2);

        // Replay-tail dedupe: a resume that re-persisted minipg's at-least-once replay tail
        // would show a second non-snapshot row for the first order's pk.
        expect(await nonSnapshotEventCount(pool, firstOrderId)).toBe(1);

        const freshCursor = await subscribeClient(b.router, 'ordersByStatus', {
          status: 'accepted',
        });
        expect(freshCursor.rows).toHaveLength(2);

        await pool.query(
          `INSERT INTO "orders" (driver_id, status, price) VALUES (3, 'accepted', 30)`,
        );
        await waitFor(() => collection.list().length === 3);
        await waitFor(async () => {
          const lsn = await streamLastLsn(pool, slotName);
          return lsn !== undefined && lsn !== lsnBefore;
        });

        collection.dispose();
      } finally {
        errorSpy.mockRestore();
        await b.runtime.stop();
        await b.sourceSql.end();
      }
    } finally {
      await dropSlotWithRetry(pool, slotName).catch(() => {});
      await pool.end();
      await dropScenarioDatabase(databaseName);
    }
  });

  test('G2: stale-PID takeover on resume — occupier evicted, no recreate, pipeline live', async () => {
    const { databaseName, databaseUrl, pool } = await createScenarioDatabase('g2');
    const publicationName = `slotresume_g2_pub_${randomSuffix()}`;
    const slotName = `slotresume_g2_slot_${randomSuffix()}`;

    try {
      const a = buildRuntime(databaseUrl, publicationName, slotName);
      let epochBefore: string | undefined;

      try {
        await a.runtime.start();
        await waitForSlotActive(pool, slotName);

        // See the G1 test's cursor-ordering comment above: subscribe MUST precede the insert.
        const cursor = await subscribeClient(a.router, 'ordersByStatus', { status: 'accepted' });
        await pool.query(
          `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
        );
        const pulled = await pullUntilEvents(a.router, cursor);
        expect(pulled.events.length).toBeGreaterThan(0);

        epochBefore = await eventsTableEpoch(pool);
        expect(epochBefore).toBeDefined();
      } finally {
        await a.runtime.stop();
        await a.sourceSql.end();
      }

      // See seedContinuousWatermark's DISCOVERY comment on the G1 test above: the stale-PID
      // eviction branch lives INSIDE resolveSlotStartup's continuity gate, so the same
      // watermark-vs-confirmed_flush gap must be closed here too, or B recreates before it ever
      // reaches the eviction logic this test targets.
      await seedContinuousWatermark(pool, slotName);

      // A raw minipg replication connection — NOT a second expose() runtime, which would evict
      // B back the same way B is about to evict this one (a live runtime auto-heals).
      const occ = await replication(databaseUrl);
      let floatingRejected = false;
      try {
        const iterator = occ.start({
          slot: slotName,
          publications: [publicationName],
          statusIntervalMs: 1000,
          idleAck: false,
          messages: false,
        });
        const floatingNext = iterator.next().catch((error) => {
          floatingRejected = true;
          throw error;
        });
        floatingNext.catch(() => {
          // Expected once B evicts the occupier's walsender — swallow so this doesn't surface
          // as an unhandled rejection.
        });

        await waitFor(async () => {
          const { rows } = await pool.query<{ active_pid: number | null }>(
            `SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1`,
            [slotName],
          );
          return rows[0]?.active_pid != null;
        });

        const errorSpy = spyOn(console, 'error').mockImplementation(() => {});
        const b = buildRuntime(databaseUrl, publicationName, slotName);
        try {
          await b.runtime.start();
          await waitForSlotActive(pool, slotName);

          expect(await eventsTableEpoch(pool)).toBe(epochBefore);
          expect(mentionsRecreated(errorSpy)).toBe(false);

          await waitFor(() => floatingRejected);

          // Cursor MUST be taken before the next insert — subscribing after it would baseline
          // the cursor's snapshot past that event, so the following pull would wait forever for
          // a "new" event that already landed in its own baseline.
          const cursor = await subscribeClient(b.router, 'ordersByStatus', {
            status: 'accepted',
          });
          expect(cursor.rows).toHaveLength(1);

          await pool.query(
            `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
          );
          const bPull = await pullUntilEvents(b.router, cursor);
          expect(bPull.events.length).toBeGreaterThan(0);

          const freshCursor = await subscribeClient(b.router, 'ordersByStatus', {
            status: 'accepted',
          });
          expect(freshCursor.rows).toHaveLength(2);
        } finally {
          errorSpy.mockRestore();
          await b.runtime.stop();
          await b.sourceSql.end();
        }
      } finally {
        try {
          occ.end();
        } catch {
          // Best-effort — the occupier's connection state after eviction is not asserted.
        }
      }
    } finally {
      await dropSlotWithRetry(pool, slotName).catch(() => {});
      await pool.end();
      await dropScenarioDatabase(databaseName);
    }
  });
});
