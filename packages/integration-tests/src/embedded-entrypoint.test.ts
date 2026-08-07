/**
 * Integration proof for the drizzle-pulse/embedded entrypoint. The primitives it composes
 * (collection convergence, events delivery, pull:false provisioning) are covered by
 * pull-false.test.ts — this suite exercises only what the factory itself adds: baseline
 * reads over its internal pool (there is no sourceDb knob), full connection release on
 * stop(), and the one-shot handle contract.
 */

import { describe, expect, test } from 'bun:test';
import { pulse } from 'drizzle-pulse';
import { createRuntime, LogLevel } from 'drizzle-pulse/embedded';
import { orders, ordersByStatusArgsSchema } from './fixtures/minimal-orders/schema.js';
import { createScenarioDb, waitFor } from './helpers/scenario.js';

const ordersByStatus = pulse(orders)
  .args(ordersByStatusArgsSchema)
  .order('asc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

describe('drizzle-pulse/embedded — createRuntime', () => {
  test('baselines read over the internal pool; stop() releases every connection; a stopped handle is terminal', async () => {
    const scenario = await createScenarioDb('pulse_embedded_entry');
    try {
      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );

      const runtime = createRuntime(
        { ordersByStatus },
        {
          databaseUrl: scenario.databaseUrl,
          wal: { publicationName: 'embedded_entry_pub', slotName: 'embedded_entry_slot' },
          logLevel: LogLevel.Error,
        },
      );

      const backendCount = async (): Promise<number> => {
        const rows = await scenario.sql.unsafe<{ count: string }[]>(
          `SELECT count(*)::text AS count FROM pg_stat_activity
           WHERE datname = current_database() AND pid <> pg_backend_pid()`,
        );
        return Number(rows[0]?.count ?? '0');
      };
      const floor = await backendCount();

      await runtime.start();

      const collection = await runtime.client.ordersByStatus({ status: 'accepted' });
      expect(collection.list()).toHaveLength(1);
      expect(collection.list()[0]?.status).toBe('accepted');

      // provision() on a running runtime would close the live admin store out from under the
      // replication loop — it must refuse instead.
      await expect(runtime.provision()).rejects.toThrow(/before start/);

      collection.dispose();
      // Overlapping stops both settle only once teardown is complete.
      await Promise.all([runtime.stop(), runtime.stop()]);

      // Everything the entrypoint opened — admin store, replication connection, and the
      // internal source pool the baseline read on — must be gone.
      await waitFor(async () => (await backendCount()) <= floor);

      await expect(runtime.start()).rejects.toThrow(/stopped/);
      await runtime.stop(); // stop after stop still resolves
    } finally {
      await scenario.drop();
    }
  });
});
