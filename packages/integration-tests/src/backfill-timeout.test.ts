/**
 * Integration proof: nothing consumes WAL until the pre-stream backfill window returns, so an
 * unbounded window would retain WAL for as long as a hung baseline does. The window is bounded at
 * 60 seconds and deliberately not configurable, which makes exceeding it the only honest way to
 * observe the bound. This test stalls every admin connection right after the replication slot is
 * created, holding the events-table seed hostage until the driver's own backfill timeout gives up
 * and `start()` rejects.
 */

import { describe, expect, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseRegistry, LogLevel, PulseRuntime } from 'drizzle-pulse/server';
import { BackfillTimeoutError } from 'minipg/cdc';
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

describe('Backfill timeout bound', () => {
  test('a stalled admin window exceeding 60 seconds fails start() terminally, near the mark', async () => {
    const base = new URL(baseDatabaseUrl());
    const proxy = startWalProxy(base.hostname, Number(base.port));
    const proxyPort = await proxy.listen();

    const scenario = await createScenarioDb('pulse_backfilltimeout');
    const publicationName = `backfilltimeout_pub_${randomSuffix()}`;
    const slotName = `backfilltimeout_slot_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(scenario.databaseUrl));

    const runtime = new PulseRuntime(buildRegistry(), {
      databaseUrl: proxiedDatabaseUrl(scenario.databaseUrl, proxyPort),
      sourceDb: drizzle({ client: sourceSql }),
      pull: true,
      wal: { publicationName, slotName },
      logLevel: LogLevel.Error,
    });

    let terminalError: Error | null = null;
    let terminalAt = 0;
    runtime.onTerminalError((error) => {
      terminalError = error;
      terminalAt = Date.now();
    });

    try {
      // The one-shot stall triggers when the replication connection creates its slot, then
      // freezes every admin connection's inbound bytes — exactly the window the events-table
      // seed reads on, so the backfill hangs while the stream side stays healthy.
      proxy.stallAdminOnSlotCreate(70_000);
      const startedAt = Date.now();

      await expect(runtime.start()).rejects.toBeInstanceOf(BackfillTimeoutError);

      expect(terminalError).toBeInstanceOf(BackfillTimeoutError);
      expect(terminalAt).toBeGreaterThan(0);

      // Measured at the terminal callback, not at the rejection: the hung admin query the
      // driver's timeout aborted doesn't actually unblock until the stall lifts at 70 seconds,
      // so the terminal timestamp trails the 60-second bound rather than landing right on it.
      const boundMs = terminalAt - startedAt;
      expect(boundMs).toBeGreaterThanOrEqual(55_000);
      expect(boundMs).toBeLessThanOrEqual(75_000);
    } finally {
      await runtime.stop();
      await sourceSql.end();
      await dropSlotWithRetry(scenario.sql, slotName).catch(() => {});
      // Force-releases any still-buffered stall so nothing outlives this test.
      await proxy.close();
      await scenario.drop();
    }
  }, 120_000);
});
