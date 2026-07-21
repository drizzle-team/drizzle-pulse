/**
 * Red-first regression test: a clean walsender CopyDone end (minipg's `'c'`
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

import { describe, expect, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseClient } from 'drizzle-pulse/client/embedded';
import { createPulseRegistry, LogLevel, PulseRuntime } from 'drizzle-pulse/server';
import postgres from 'postgres';
import { orders, ordersByStatusArgsSchema } from './fixtures/minimal-orders/schema.js';
import { createScenarioDb, waitFor } from './helpers/scenario.js';
import { baseDatabaseUrl, randomSuffix, withQuietPostgresUrl } from './helpers/test-harness.js';
import { proxiedDatabaseUrl, startWalProxy } from './helpers/wal-proxy.js';

const ordersByStatus = pulse(orders)
  .args(ordersByStatusArgsSchema)
  .order('asc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

function buildRegistry() {
  return createPulseRegistry({ ordersByStatus });
}

async function dropSlotWithRetry(
  sql: ReturnType<typeof postgres>,
  slotName: string,
): Promise<void> {
  await waitFor(async () => {
    try {
      await sql.unsafe(`SELECT pg_drop_replication_slot($1)`, [slotName]);
      return true;
    } catch (e) {
      const code = (e as { code?: string }).code;
      if (code === '42704') return true; // undefined_object — already gone
      if (code === '55006') return false; // object_in_use — walsender still attached, retry
      throw e;
    }
  }, 5000);
}

describe('CopyDone reconnect', () => {
  test('a clean walsender CopyDone end reconnects instead of silently ending replication', async () => {
    const base = new URL(baseDatabaseUrl());
    const proxy = startWalProxy(base.hostname, Number(base.port));
    const proxyPort = await proxy.listen();

    // Deliberately absent: the publication AND REPLICA IDENTITY FULL — bootstrap() self-
    // provisions both at boot, same as every other self-managed scenario in this suite.
    const scenario = await createScenarioDb('pulse_copydone_g3');
    const publicationName = `copydone_pub_${randomSuffix()}`;
    const slotName = `copydone_slot_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(scenario.databaseUrl));

    const runtime = new PulseRuntime(buildRegistry(), {
      databaseUrl: proxiedDatabaseUrl(scenario.databaseUrl, proxyPort),
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

      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );
      await waitFor(() => collection.list().length === 1);

      // Let the walsender go idle (keepalive-only frames) before injecting — the CopyDone
      // frame must land between protocol messages, which idle guarantees in practice.
      await new Promise<void>((resolve) => setTimeout(resolve, 1500));
      proxy.injectCopyDone();

      // THE ASSERTION (red today, green after the fix): on unpatched code the loop
      // returns silently at the 'c' frame — no reconnect is scheduled, this insert never
      // arrives, and waitFor throws. After the fix, handleDisconnect(rep) reconnects
      // (backoff ~1-2s) and resolveSlotStartup resumes the intact slot.
      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );
      await waitFor(() => collection.list().length === 2, 10000);

      // A third insert after the reconnect proves the pipeline is genuinely live again, not
      // just draining a buffered event.
      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (3, 'accepted', 30)`,
      );
      await waitFor(() => collection.list().length === 3, 10000);

      expect(terminalError).toBeNull();

      collection.dispose();
    } finally {
      await runtime.stop();
      await sourceSql.end();
      await dropSlotWithRetry(scenario.sql, slotName).catch(() => {});
      await proxy.close();
      await scenario.drop();
    }
  });
});
