/**
 * Integration proof: reconcile() self-provisions the publication and REPLICA IDENTITY.
 * expose().provision() (the same reconcile path start() runs, minus the replication stream)
 * creates the publication owning exactly the registered sources, keeps its membership in sync
 * (adding new sources, un-pulsing removed ones), and forces REPLICA IDENTITY FULL — restoring
 * it after drift. Each scenario builds its own standalone database and drops it in a finally
 * block, so the publication/schema it creates go with the database.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { integer, pgTable, serial } from 'drizzle-orm/pg-core';
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

// A second pulsable source, used to prove membership add/drop against a FOR TABLE publication.
const extras = pgTable('extras', { id: serial('id').primaryKey(), n: integer('n') });

const adminPool = createQuietPool(baseDatabaseUrl());

afterAll(async () => {
  await adminPool.end();
});

type Scenario = {
  databaseName: string;
  databaseUrl: string;
  pool: Pool;
  sourceSql: ReturnType<typeof postgres>;
};

// Creates a fresh database with the orders source table but NO publication and NO replica
// identity — reconcile() must self-provision both.
async function setupBareScenario(label: string): Promise<Scenario> {
  const databaseName = `pulse_pub_${label}_${randomSuffix()}`;
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
  await pool.query('CREATE TABLE "extras" ("id" serial PRIMARY KEY, "n" integer)');

  const sourceSql = postgres(withQuietPostgresUrl(databaseUrl));
  return { databaseName, databaseUrl, pool, sourceSql };
}

function makeRuntime(s: Scenario, label: string, tables: 'orders' | 'both') {
  const registry =
    tables === 'both'
      ? createPulseRegistry({ orders: pulse(orders).query(), extras: pulse(extras).query() })
      : createPulseRegistry({ orders: pulse(orders).query() });
  return expose(registry, {
    databaseUrl: s.databaseUrl,
    sourceDb: drizzle({ client: s.sourceSql }),
    wal: { publicationName: `pulse_pub_${label}`, slotName: `pulse_slot_${label}` },
    logLevel: LogLevel.Error,
  });
}

async function teardown(s: Scenario): Promise<void> {
  await s.sourceSql.end();
  await s.pool.end();
  await adminPool.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [s.databaseName],
  );
  await adminPool.query(`DROP DATABASE IF EXISTS "${s.databaseName}"`);
}

async function members(pool: Pool, pubName: string): Promise<string[]> {
  const { rows } = await pool.query<{ qualified: string }>(
    `SELECT schemaname || '.' || tablename AS qualified FROM pg_publication_tables WHERE pubname = $1 ORDER BY qualified`,
    [pubName],
  );
  return rows.map((row) => row.qualified);
}

async function replicaIdentity(pool: Pool, table: string): Promise<string | undefined> {
  const { rows } = await pool.query<{ relreplident: string }>(
    `SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = $1`,
    [table],
  );
  return rows[0]?.relreplident;
}

describe('reconcile publication + replica identity self-provisioning', () => {
  test('fresh provision() creates the publication with exact membership and RI FULL', async () => {
    const s = await setupBareScenario('fresh');
    const pubName = 'pulse_pub_fresh';
    try {
      await makeRuntime(s, 'fresh', 'orders').provision();

      const pub = await s.pool.query<{ puballtables: boolean; ops: string }>(
        `SELECT puballtables,
                (pubinsert::text || pubupdate::text || pubdelete::text) AS ops
         FROM pg_publication WHERE pubname = $1`,
        [pubName],
      );
      expect(pub.rows).toHaveLength(1);
      expect(pub.rows[0]?.puballtables).toBe(false);
      // insert + update + delete all published, truncate not.
      expect(pub.rows[0]?.ops).toBe('truetruetrue');

      expect(await members(s.pool, pubName)).toEqual(['public.orders']);
      expect(await replicaIdentity(s.pool, 'orders')).toBe('f');
      // extras is registered by no runtime here, so it is left at the default identity.
      expect(await replicaIdentity(s.pool, 'extras')).not.toBe('f');
    } finally {
      await teardown(s);
    }
  });

  test('second provision() is idempotent: membership and RI unchanged', async () => {
    const s = await setupBareScenario('idem');
    const pubName = 'pulse_pub_idem';
    try {
      const runtime = makeRuntime(s, 'idem', 'orders');
      await runtime.provision();
      const firstEpoch = runtime.getEpochForQuery('orders');
      const firstRel = await s.pool.query(
        `SELECT prrelid FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid WHERE p.pubname = $1 ORDER BY prrelid`,
        [pubName],
      );

      await runtime.provision();

      expect(runtime.getEpochForQuery('orders')).toBe(firstEpoch);
      expect(await members(s.pool, pubName)).toEqual(['public.orders']);
      expect(await replicaIdentity(s.pool, 'orders')).toBe('f');
      const secondRel = await s.pool.query(
        `SELECT prrelid FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid WHERE p.pubname = $1 ORDER BY prrelid`,
        [pubName],
      );
      expect(secondRel.rows).toEqual(firstRel.rows);
    } finally {
      await teardown(s);
    }
  });

  test('un-pulse: dropping a source removes membership, resets RI, and orphan-drops the events table', async () => {
    const s = await setupBareScenario('unpulse');
    const pubName = 'pulse_pub_unpulse';
    try {
      await makeRuntime(s, 'unpulse', 'both').provision();
      expect(await members(s.pool, pubName)).toEqual(['public.extras', 'public.orders']);
      expect(await replicaIdentity(s.pool, 'extras')).toBe('f');
      const extrasEventsBefore = await s.pool.query(
        `SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse' AND c.relname = 'public_extras'`,
      );
      expect(extrasEventsBefore.rows).toHaveLength(1);

      // A second runtime registering only orders should un-pulse extras.
      await makeRuntime(s, 'unpulse', 'orders').provision();

      expect(await members(s.pool, pubName)).toEqual(['public.orders']);
      expect(await replicaIdentity(s.pool, 'orders')).toBe('f');
      expect(await replicaIdentity(s.pool, 'extras')).not.toBe('f');

      const extrasEventsAfter = await s.pool.query(
        `SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drizzle_pulse' AND c.relname = 'public_extras'`,
      );
      expect(extrasEventsAfter.rows).toHaveLength(0);
      const extrasMeta = await s.pool.query(
        `SELECT 1 FROM drizzle_pulse.pulse_meta WHERE table_name = 'public_extras'`,
      );
      expect(extrasMeta.rows).toHaveLength(0);
    } finally {
      await teardown(s);
    }
  });

  test('FOR ALL TABLES publication: provision() leaves membership alone and succeeds', async () => {
    const s = await setupBareScenario('allt');
    const pubName = 'pulse_pub_allt';
    try {
      await s.pool.query(`CREATE PUBLICATION ${pubName} FOR ALL TABLES`);

      const runtime = makeRuntime(s, 'allt', 'orders');
      await runtime.provision();

      const pub = await s.pool.query<{ puballtables: boolean }>(
        `SELECT puballtables FROM pg_publication WHERE pubname = $1`,
        [pubName],
      );
      // Membership is implicit for FOR ALL TABLES — pulse must not run ALTER PUBLICATION
      // against it (which would fail). It stays FOR ALL TABLES and orders is a member.
      expect(pub.rows[0]?.puballtables).toBe(true);
      expect(await members(s.pool, pubName)).toContain('public.orders');
      // RI is still forced on the registered source.
      expect(await replicaIdentity(s.pool, 'orders')).toBe('f');
      // Events table still provisioned.
      expect(runtime.getEpochForQuery('orders')).toBeTruthy();
    } finally {
      await teardown(s);
    }
  });

  test('RI drift: manually resetting to DEFAULT is restored to FULL on next provision()', async () => {
    const s = await setupBareScenario('drift');
    try {
      const runtime = makeRuntime(s, 'drift', 'orders');
      await runtime.provision();
      expect(await replicaIdentity(s.pool, 'orders')).toBe('f');

      await s.pool.query('ALTER TABLE "orders" REPLICA IDENTITY DEFAULT');
      expect(await replicaIdentity(s.pool, 'orders')).not.toBe('f');

      await runtime.provision();
      expect(await replicaIdentity(s.pool, 'orders')).toBe('f');
    } finally {
      await teardown(s);
    }
  });
});
