import { describe, expect, test } from 'bun:test';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createRuntime, LogLevel, type TelemetryEvent } from 'drizzle-pulse/embedded';
import postgres from 'postgres';
import { orders, ordersByStatusArgsSchema } from './fixtures/minimal-orders/schema.js';
import { createScenarioDb, waitFor } from './helpers/scenario.js';
import { withQuietPostgresUrl } from './helpers/test-harness.js';

const ordersByStatus = pulse(orders)
  .args(ordersByStatusArgsSchema)
  .order('asc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

describe('drizzle-pulse/embedded — telemetry', () => {
  test('reports schema, table, op, and microsecond stamps for an applied insert', async () => {
    const scenario = await createScenarioDb('pulse_telemetry');
    const sourceSql = postgres(withQuietPostgresUrl(scenario.databaseUrl));
    const received: TelemetryEvent[] = [];
    try {
      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );

      const runtime = createRuntime({
        queries: { ordersByStatus },
        databaseUrl: scenario.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName: 'telemetry_pub', slotName: 'telemetry_slot' },
        logLevel: LogLevel.Error,
        telemetry: (event) => {
          received.push(event);
        },
      });

      await runtime.start();

      const collection = await runtime.client.ordersByStatus({ status: 'accepted' });
      expect(collection.list()).toHaveLength(1);
      // Baseline reads emit no telemetry — only newly applied WAL row events do.
      expect(received.length).toBe(0);

      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );
      await waitFor(() => received.length === 1);

      const event = received[0]!;
      expect(Object.keys(event).sort()).toEqual([
        'appliedAt',
        'committedAt',
        'op',
        'schema',
        'table',
      ]);
      expect(event.op).toBe('insert');
      expect(event.schema).toBe('public');
      expect(event.table).toBe('orders');
      expect(Number.isInteger(event.committedAt)).toBe(true);
      expect(Number.isInteger(event.appliedAt)).toBe(true);
      expect(event.committedAt).toBeGreaterThan(0);
      // Two different clocks (database host vs. app host) — generous bound, not a precision check.
      expect(Math.abs(event.appliedAt - event.committedAt)).toBeLessThan(60_000_000);
      // Pins the UNIT: a millisecond value or a PG-epoch value both blow this bound; only a
      // correct epoch-microsecond value stays within it.
      expect(Math.abs(event.committedAt - Date.now() * 1000)).toBeLessThan(60_000_000);

      collection.dispose();
      await runtime.stop();
    } finally {
      await sourceSql.end();
      await scenario.drop();
    }
  });

  test('a throwing callback does not stop subsequent events from applying', async () => {
    const scenario = await createScenarioDb('pulse_telemetry_throw');
    const sourceSql = postgres(withQuietPostgresUrl(scenario.databaseUrl));
    let calls = 0;
    const received: TelemetryEvent[] = [];
    try {
      const runtime = createRuntime({
        queries: { ordersByStatus },
        databaseUrl: scenario.databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName: 'telemetry_throw_pub', slotName: 'telemetry_throw_slot' },
        logLevel: LogLevel.Silent,
        telemetry: (event) => {
          calls += 1;
          if (calls === 1) throw new Error('boom');
          received.push(event);
        },
      });

      await runtime.start();

      const collection = await runtime.client.ordersByStatus({ status: 'accepted' });
      expect(collection.list()).toHaveLength(0);

      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (1, 'accepted', 10)`,
      );
      await scenario.sql.unsafe(
        `INSERT INTO "orders" (driver_id, status, price) VALUES (2, 'accepted', 20)`,
      );

      await waitFor(() => received.length === 1);
      expect(received[0]?.op).toBe('insert');

      await waitFor(() => collection.list().length === 2);

      collection.dispose();
      await runtime.stop();
    } finally {
      await sourceSql.end();
      await scenario.drop();
    }
  });
});
