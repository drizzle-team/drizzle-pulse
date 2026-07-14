/**
 * Integration proof: `pull: false` runtimes provision and write zero events-table
 * infrastructure while embedded collections and stateless events subscriptions stay fully
 * live over the WAL tap (DRIVER-06) — and their replication slot is temporary with a
 * randomized suffix so a crashed process can never leak WAL-retaining slot state (D-01).
 * Each scenario builds its own standalone database (bare — no pre-existing publication or
 * REPLICA IDENTITY) so reconcile()'s self-provisioning of the WAL prerequisites is exercised
 * the same way it is under `pull: true`, and tears itself down in a `finally` block.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseClient, createPulseEvents } from 'drizzle-pulse/client/embedded';
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

const adminPool = createQuietPool(baseDatabaseUrl());

afterAll(async () => {
  await adminPool.end();
});

// Bounded async poller — WAL tap delivery and walsender slot teardown both lag a scheduling
// beat behind the triggering statement, so poll rather than assert synchronously.
async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 5000,
  pollIntervalMs = 25,
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

async function setupPullFalseScenario(label: string) {
  const databaseName = `pulse_pullfalse_${label}_${randomSuffix()}`;
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
  // Deliberately absent: the publication AND REPLICA IDENTITY FULL — reconcile() still
  // self-provisions both under pull:false (embedded needs WAL, per A3).

  const publicationName = `pullfalse_pub_${label}`;
  const slotName = `pullfalse_slot_${label}`;
  const sourceSql = postgres(withQuietPostgresUrl(databaseUrl));

  const registry = buildRegistry();
  const runtime = expose(registry, {
    databaseUrl,
    sourceDb: drizzle({ client: sourceSql }),
    pull: false,
    wal: { publicationName, slotName },
    logLevel: LogLevel.Error,
  });

  return { databaseName, pool, sourceSql, publicationName, slotName, runtime };
}

type PullFalseScenario = Awaited<ReturnType<typeof setupPullFalseScenario>>;

async function teardownScenario(
  s: PullFalseScenario,
  opts: { alreadyStopped?: boolean } = {},
): Promise<void> {
  if (!opts.alreadyStopped) {
    await s.runtime.stop();
  }
  await s.sourceSql.end();
  await s.pool.end();

  // The temporary slot (D-01) is dropped by Postgres once the replication connection's
  // backend actually terminates, which lags stop()'s rep.end() by a beat — DROP DATABASE
  // fails with "used by an active logical replication slot" if it races ahead of that.
  await waitFor(async () => {
    const { rows } = await adminPool.query(
      `SELECT 1 FROM pg_replication_slots WHERE database = $1`,
      [s.databaseName],
    );
    return rows.length === 0;
  });

  await adminPool.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [s.databaseName],
  );
  await adminPool.query(`DROP DATABASE IF EXISTS "${s.databaseName}"`);
}

async function eventsSchemaRelationCount(pool: Pool): Promise<number> {
  const { rows } = await pool.query(
    `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse'`,
  );
  return rows.length;
}

describe('pull: false — embedded-only runtime writes nothing to events tables (DRIVER-06)', () => {
  test('provisioning: zero events-schema relations; publication + REPLICA IDENTITY FULL still self-provisioned', async () => {
    const s = await setupPullFalseScenario('provision');
    try {
      await s.runtime.start();
      expect(s.runtime.isRunning).toBe(true);

      expect(await eventsSchemaRelationCount(s.pool)).toBe(0);

      const members = await s.pool.query<{ tablename: string }>(
        `SELECT tablename FROM pg_publication_tables WHERE pubname = $1`,
        [s.publicationName],
      );
      expect(members.rows.map((row) => row.tablename)).toEqual(['orders']);

      const replicaIdentity = await s.pool.query<{ relreplident: string }>(
        `SELECT relreplident FROM pg_class WHERE relname = 'orders'`,
      );
      expect(replicaIdentity.rows[0]?.relreplident).toBe('f');
    } finally {
      await teardownScenario(s);
    }
  });

  test('runtime.handlers throws, naming pull, on an embedded-only runtime', async () => {
    const s = await setupPullFalseScenario('handlers');
    try {
      await s.runtime.start();
      expect(() => s.runtime.handlers).toThrow(/pull/i);
    } finally {
      await teardownScenario(s);
    }
  });

  test('slot hygiene (D-01): a temporary, randomized-suffix slot while running; gone after stop', async () => {
    const s = await setupPullFalseScenario('slot');
    try {
      await s.runtime.start();

      const slots = await s.pool.query<{ slot_name: string; temporary: boolean }>(
        `SELECT slot_name, temporary FROM pg_replication_slots WHERE slot_name LIKE $1`,
        [`${s.slotName}\\_%`],
      );
      expect(slots.rows).toHaveLength(1);
      expect(slots.rows[0]?.slot_name).not.toBe(s.slotName);
      expect(slots.rows[0]?.temporary).toBe(true);

      await s.runtime.stop();

      await waitFor(async () => {
        const remaining = await s.pool.query(
          `SELECT 1 FROM pg_replication_slots WHERE slot_name LIKE $1`,
          [`${s.slotName}\\_%`],
        );
        return remaining.rows.length === 0;
      });
    } finally {
      await teardownScenario(s, { alreadyStopped: true });
    }
  });

  test('embedded collection convergence + stateless events delivery, live over the WAL tap with no events tables', async () => {
    const s = await setupPullFalseScenario('live');
    try {
      await s.runtime.start();

      const client = createPulseClient(s.runtime);
      const events = createPulseEvents(s.runtime);

      const collectionPromise = client.ordersByStatus({ status: 'accepted' });
      const eventLog: Array<{ op: string; lsn: string }> = [];
      const unsub = events.ordersByStatus({ status: 'accepted' }, (event, lsn) => {
        eventLog.push({ op: event.op, lsn });
      });

      const collection = await collectionPromise;

      await s.pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );
      await waitFor(() => collection.list().length === 1 && eventLog.length === 1);
      expect(eventLog[0]?.op).toBe('insert');
      expect(eventLog[0]?.lsn).toMatch(/^[0-9A-Fa-f]+\/[0-9A-Fa-f]+$/);

      const insertedId = collection.list()[0]?.id as number;

      await s.pool.query(`UPDATE "orders" SET status = 'completed' WHERE id = $1`, [insertedId]);
      await waitFor(() => collection.list().length === 0 && eventLog.length === 2);
      expect(eventLog[1]?.op).toBe('update');

      await s.pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 20)`,
      );
      await waitFor(() => collection.list().length === 1 && eventLog.length === 3);
      expect(eventLog[2]?.op).toBe('insert');
      const secondId = collection.list()[0]?.id as number;

      await s.pool.query(`DELETE FROM "orders" WHERE id = $1`, [secondId]);
      await waitFor(() => collection.list().length === 0 && eventLog.length === 4);
      expect(eventLog[3]?.op).toBe('delete');

      // No events-table infrastructure appeared as a side effect of the insert/update/delete
      // cycle above — persistence stayed off for the entire lifecycle, not just at boot.
      expect(await eventsSchemaRelationCount(s.pool)).toBe(0);

      unsub();
      collection.dispose();
    } finally {
      await teardownScenario(s);
    }
  });
});
