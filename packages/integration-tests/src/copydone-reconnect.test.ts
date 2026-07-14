/**
 * G3 (BUG-01 red-first regression test): a clean walsender CopyDone end (minipg's `'c'`
 * message-type return, e.g. Postgres restart) must reconnect the same way a thrown
 * replication error already does, instead of silently ending replication. Injecting a real
 * CopyDone frame is the only way to distinguish this from `pg_terminate_backend`, which
 * produces an `ErrorResponse`/socket-close — the already-working throw path (see
 * `wal-proxy.ts`).
 *
 * Uses a split URL configuration: the runtime's `databaseUrl` (walsender + admin pool) is
 * routed through the test-only TCP proxy so the injected frame reaches minipg, while
 * `sourceDb` stays on the direct connection (postgres.js never needs proxying).
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
  const databaseName = `pulse_copydone_${label}_${randomSuffix()}`;
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

describe('CopyDone reconnect (BUG-01, G3)', () => {
  test('a clean walsender CopyDone end reconnects instead of silently ending replication', async () => {
    const base = new URL(baseDatabaseUrl());
    const proxy = startWalProxy(base.hostname, Number(base.port));
    const proxyPort = await proxy.listen();

    const { databaseName, directUrl, pool } = await createScenarioDatabase('g3');
    const publicationName = `copydone_pub_${randomSuffix()}`;
    const slotName = `copydone_slot_${randomSuffix()}`;
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

      // Let the walsender go idle (keepalive-only frames) before injecting — the CopyDone
      // frame must land between protocol messages, which idle guarantees in practice.
      await new Promise<void>((resolve) => setTimeout(resolve, 1500));
      proxy.injectCopyDone();

      // THE ASSERTION (red today, green after the BUG-01 fix): on unpatched code the loop
      // returns silently at the 'c' frame — no reconnect is scheduled, this insert never
      // arrives, and waitFor throws. After the fix, handleDisconnect(rep) reconnects
      // (backoff ~1-2s) and resolveSlotStartup resumes the intact slot.
      await pool.query(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );
      await waitFor(() => collection.list().length === 2, 10000);

      // A third insert after the reconnect proves the pipeline is genuinely live again, not
      // just draining a buffered event.
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
});
