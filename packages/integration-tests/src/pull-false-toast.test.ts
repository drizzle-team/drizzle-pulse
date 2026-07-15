/**
 * Integration proof for the pull:false TOAST fill (RIF-02's fillUnchangedByPk): under
 * REPLICA IDENTITY DEFAULT, an UPDATE that never touches a TOASTed column omits it from
 * pgoutput's new tuple — the one parameterized by-pk SELECT on the admin pool must carry it
 * forward, a WHERE evaluated against that filled row must still work, and a same-commit
 * UPDATE+DELETE whose fill misses (row already gone by SELECT time) must be absorbed silently
 * by the trailing delete. Own scenario-local database + table (a TOASTable `note` column that
 * the shared minimal-orders fixture doesn't carry) — mirrors pull-false.test.ts's scenario
 * discipline (bare DDL, no publication/replica-identity setup, temp-slot teardown drain).
 */

import { afterAll, describe, expect, spyOn, test } from 'bun:test';
import { randomBytes } from 'node:crypto';
import { decimal, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { drizzle } from 'drizzle-orm/postgres-js';
import { createSelectSchema } from 'drizzle-orm/zod';
import { pulse } from 'drizzle-pulse';
import { createPulseClient } from 'drizzle-pulse/client/embedded';
import { createPulseRegistry, expose, LogLevel } from 'drizzle-pulse/server';
import type { Pool } from 'pg';
import postgres from 'postgres';
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

// Bounded async poller — WAL tap delivery lags a scheduling beat behind the triggering
// statement, so poll rather than assert synchronously (pull-false.test.ts idiom).
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

const orders = pgTable('orders', {
  id: serial('id').primaryKey(),
  status: text('status').notNull(),
  price: decimal('price', { mode: 'number' }).notNull(),
  note: text('note').notNull(),
});

const orderSchema = createSelectSchema(orders);

const ordersByStatus = pulse(orders)
  .args(orderSchema.pick({ status: true }))
  .order('asc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

const ordersByNote = pulse(orders)
  .args(orderSchema.pick({ note: true }))
  .order('asc')
  .query((ctx) => ctx.query({ note: ctx.args.note }));

function buildRegistry() {
  return createPulseRegistry({ ordersByStatus, ordersByNote });
}

async function setupScenario(label: string) {
  const databaseName = `pulse_toastfalse_${label}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const databaseUrl = buildDatabaseUrl(baseDatabaseUrl(), databaseName);
  const pool = createQuietPool(databaseUrl);

  await pool.query(`
    CREATE TABLE "orders" (
      "id" serial PRIMARY KEY,
      "status" text NOT NULL,
      "price" numeric NOT NULL,
      "note" text NOT NULL
    )
  `);
  // Deliberately absent: the publication — reconcile() self-provisions it under pull:false.
  // REPLICA IDENTITY is left untouched (RIF-02): the tap decodes old-tuple data via
  // oldKind/unchanged, and the TOAST fill runs a by-pk SELECT instead of relying on FULL.

  const publicationName = `toastfalse_pub_${label}`;
  const slotName = `toastfalse_slot_${label}`;
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

type Scenario = Awaited<ReturnType<typeof setupScenario>>;

async function teardownScenario(s: Scenario): Promise<void> {
  await s.runtime.stop();
  await s.sourceSql.end();
  await s.pool.end();

  // The temporary slot is dropped by Postgres once the replication connection's backend
  // actually terminates, which lags stop()'s rep.end() by a beat.
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

// Incompressible so TOAST can't inline-compress it away — must be pushed out-of-line to
// actually exercise pgoutput's omitted-column carry-forward (driver-minipg.test.ts idiom).
function toastableValue(): string {
  return randomBytes(5000).toString('hex');
}

describe('pull: false — TOAST-omitted column fill by pk (RIF-02)', () => {
  test('carry-forward: an unrelated-column update does not drop the TOAST-omitted column', async () => {
    const s = await setupScenario('carry');
    try {
      await s.runtime.start();

      // Proves the fill path ran, not the FULL old-under-new spread — RIF-02 never forces FULL.
      const replicaIdentity = await s.pool.query<{ relreplident: string }>(
        `SELECT relreplident FROM pg_class WHERE relname = 'orders'`,
      );
      expect(replicaIdentity.rows[0]?.relreplident).toBe('d');

      const client = createPulseClient(s.runtime);
      const collection = await client.ordersByStatus({ status: 'accepted' });

      const note = toastableValue();
      await s.pool.query(
        `INSERT INTO "orders" (status, price, note) VALUES ('accepted', 10, $1)`,
        [note],
      );
      await waitFor(() => collection.list().length === 1);
      const insertedId = collection.list()[0]?.id as number;

      await s.pool.query(`UPDATE "orders" SET price = 20 WHERE id = $1`, [insertedId]);
      await waitFor(() => {
        const row = collection.list()[0] as { price?: number } | undefined;
        return row?.price === 20;
      });

      const row = collection.list()[0] as { note?: string } | undefined;
      expect(row?.note).toBe(note);
      expect(row?.note?.length).toBe(note.length);

      expect(await eventsSchemaRelationCount(s.pool)).toBe(0);

      collection.dispose();
    } finally {
      await teardownScenario(s);
    }
  });

  test('WHERE on the TOASTed column: membership survives an unrelated-column update, and leaves when the filtered column itself changes', async () => {
    const s = await setupScenario('filter');
    try {
      await s.runtime.start();

      const client = createPulseClient(s.runtime);
      const note = toastableValue();
      const collection = await client.ordersByNote({ note });

      await s.pool.query(
        `INSERT INTO "orders" (status, price, note) VALUES ('requested', 10, $1)`,
        [note],
      );
      await waitFor(() => collection.list().length === 1);
      const insertedId = collection.list()[0]?.id as number;

      // Without the fill the omitted `note` evaluates as non-matching (filter-ast treats a
      // missing column as non-matching) and the pk-stable update would evict the row via
      // membership even though `note` never changed.
      await s.pool.query(`UPDATE "orders" SET price = 20 WHERE id = $1`, [insertedId]);
      await waitFor(() => {
        const row = collection.list()[0] as { price?: number } | undefined;
        return row?.price === 20;
      });
      expect(collection.list()).toHaveLength(1);

      // Changing the filtered column itself still leaves — the new tuple genuinely carries the
      // non-matching value, no fill involved.
      const otherNote = toastableValue();
      await s.pool.query(`UPDATE "orders" SET note = $1 WHERE id = $2`, [otherNote, insertedId]);
      await waitFor(() => collection.list().length === 0);

      expect(await eventsSchemaRelationCount(s.pool)).toBe(0);

      collection.dispose();
    } finally {
      await teardownScenario(s);
    }
  });

  test('fill-miss absorption: a same-commit UPDATE+DELETE converges with no fill-related error', async () => {
    const s = await setupScenario('miss');
    try {
      await s.runtime.start();

      const client = createPulseClient(s.runtime);
      const collection = await client.ordersByStatus({ status: 'accepted' });

      const note = toastableValue();
      await s.pool.query(
        `INSERT INTO "orders" (status, price, note) VALUES ('accepted', 10, $1)`,
        [note],
      );
      await waitFor(() => collection.list().length === 1);
      const insertedId = collection.list()[0]?.id as number;

      const errorSpy = spyOn(console, 'error').mockImplementation(() => {});
      try {
        // By the time the fill SELECT runs (decoding the UPDATE), the row is already gone —
        // the whole transaction, including the DELETE, has already committed in Postgres before
        // the replication stream delivers either change. The trailing delete in the same commit
        // batch must absorb the miss.
        await s.pool.query(
          `BEGIN; UPDATE "orders" SET price = 30 WHERE id = ${insertedId}; DELETE FROM "orders" WHERE id = ${insertedId}; COMMIT;`,
        );

        await waitFor(() => collection.list().length === 0);

        expect(
          errorSpy.mock.calls.some((call: unknown[]) => String(call[0]).includes('fill miss')),
        ).toBe(false);
      } finally {
        errorSpy.mockRestore();
      }

      expect(await eventsSchemaRelationCount(s.pool)).toBe(0);

      collection.dispose();
    } finally {
      await teardownScenario(s);
    }
  });
});
