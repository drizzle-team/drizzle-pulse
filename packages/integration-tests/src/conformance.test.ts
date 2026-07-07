/**
 * INTG-01: the milestone's central anti-drift proof.
 *
 * Runs the LOCAL pulse-branch drizzle-kit `generate` CLI against a pulse(orders) fixture,
 * applies the KIT-GENERATED migration SQL to real Postgres (the docker WAL harness, never
 * the runtime's own emitEventsTableDdl — that would make the proof circular), boots the
 * drizzle-pulse runtime via expose() against that generated infrastructure, writes to the
 * source table, and asserts the resulting WAL events land in the kit-generated
 * drizzle.__events_public_orders table and are readable through the runtime's own pull
 * surface. Two independent implementations (kit synthesis vs. runtime resolver) cannot
 * silently drift while this test is green.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { eq } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pulse } from 'drizzle-pulse';
import { createPulseRegistry, expose } from 'drizzle-pulse/server';
import { Pool } from 'pg';
import postgres from 'postgres';
import { pulseConformanceFixture } from './fixtures/pulse-conformance/index.js';
import { orders } from './fixtures/pulse-conformance/schema.js';
import { generatePulseMigrationSql, kitBinExists } from './helpers/kit-generate.js';
import {
  createRealtimeRouterWithAuth,
  processDbOperations,
  pullClient,
  subscribeClient,
} from './helpers/test-harness.js';

const DEFAULT_DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/postgres';
// Absolute path: `bun test` may run from the workspace root, not this package's directory.
const FIXTURE_CONFIG_PATH = fileURLToPath(
  new URL('./fixtures/pulse-conformance/drizzle.config.ts', import.meta.url),
);

function baseDatabaseUrl(): string {
  return process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;
}

function randomSuffix(): string {
  return randomUUID().replaceAll('-', '').slice(0, 10);
}

function buildDatabaseUrl(base: string, databaseName: string): string {
  const url = new URL(base);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

function withQuietPostgresUrl(databaseUrl: string): string {
  const url = new URL(databaseUrl);
  url.searchParams.set('options', '-c client_min_messages=warning');
  return url.toString();
}

function createQuietPool(databaseUrl: string): Pool {
  return new Pool({ connectionString: withQuietPostgresUrl(databaseUrl) });
}

// `processDbOperations` reads a "since" snapshot baseline BEFORE running the operations
// array — but a raw `pg.Pool.query()` call fires its network request the instant it's
// constructed (not when awaited), racing that baseline read if passed directly in the
// array literal. Drizzle's query builder is lazy (nothing runs until awaited), which is
// why every other suite in this package writes through `db.insert(...)`/`db.update(...)`
// rather than a raw Pool — this test does the same, never `testPool.query()` for writes
// that need snapshot-baseline correctness.
function createQuietPostgresClient(databaseUrl: string) {
  return postgres(withQuietPostgresUrl(databaseUrl));
}

const adminPool = createQuietPool(baseDatabaseUrl());

afterAll(async () => {
  await adminPool.end();
});

async function waitForWalSlot(slotName: string, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const { rows } = await adminPool.query<{ slot_name: string }>(
      'SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1',
      [slotName],
    );
    if (rows.length > 0) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timeout: WAL listener did not initialize slot ${slotName} in ${timeoutMs}ms`);
}

// Skips (not fails) when the local pulse-branch drizzle-kit build isn't present — e.g. a
// fresh clone or CI without the sibling ~/dev/drizzle-orm checkout. Mirrors how the
// absent-fixture-repo suites skip. Build the kit (or set DRIZZLE_KIT_PULSE_BIN) to run it.
describe.skipIf(!kitBinExists())('INTG-01: kit-generate to WAL events conformance', () => {
  test('local pulse kit generates real infra; runtime boots on it and streams a real write into the kit-generated events table', async () => {
    // --- Setup guard (Pitfall 4): fail loudly if the resolved kit is pulse-blind. A
    // passing conformance test against the npm rc.4 kit would prove nothing. ---
    const migrationSql = generatePulseMigrationSql(
      FIXTURE_CONFIG_PATH,
      pulseConformanceFixture.migrationsPath,
    );
    expect(migrationSql).toContain('CREATE PUBLICATION');
    expect(migrationSql).toContain('CREATE TABLE "drizzle"."__events_public_orders"');

    // --- Isolate a fresh database (randomized name, per CON-integration-tests-isolation) ---
    const databaseName = `pulse_conformance_${randomSuffix()}`;
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    const databaseUrl = buildDatabaseUrl(baseDatabaseUrl(), databaseName);
    const testPool = createQuietPool(databaseUrl);
    const publicationName = pulseConformanceFixture.publicationName;
    const slotName = `pulse_conformance_slot_${randomSuffix()}`;
    const sourceSql = postgres(withQuietPostgresUrl(databaseUrl));
    const writeSql = createQuietPostgresClient(databaseUrl);
    const db = drizzle({ client: writeSql });

    let runtime: ReturnType<typeof expose> | undefined;

    try {
      // --- Apply the KIT-GENERATED SQL as-is: source table, events table, REPLICA
      // IDENTITY FULL, publication, and the wal_level guard all come from kit's own
      // output. Never the runtime's emitEventsTableDdl. ---
      await testPool.query(migrationSql);

      // --- Boot the runtime against the kit-generated publication ---
      const registry = createPulseRegistry({ orders: pulse(orders).query() });
      runtime = expose(registry, {
        databaseUrl,
        sourceDb: drizzle({ client: sourceSql }),
        wal: { publicationName, slotName, logging: { events: false } },
      });

      const runtimeStartupError: { current: Error | null } = { current: null };
      void runtime.start().catch((error: unknown) => {
        runtimeStartupError.current = error instanceof Error ? error : new Error(String(error));
      });
      await waitForWalSlot(slotName);
      if (runtimeStartupError.current) {
        throw runtimeStartupError.current;
      }
      expect(runtime.isRunning).toBe(true);

      // --- Write to the source table: insert then update ---
      // Subscribe BEFORE writing, so the pull below observes both writes as new events
      // (the runtime's own read layer, not just a raw SQL poll on the events table).
      const router = createRealtimeRouterWithAuth(runtime, { userId: null });
      const subscribed = await subscribeClient(router, 'orders', {});
      expect(subscribed.rows).toHaveLength(0);

      const { events, results } = await processDbOperations(
        { eventsTable: pulseConformanceFixture.eventsTable },
        testPool,
        [db.insert(orders).values({ driverId: 1, status: 'requested' }).returning()],
      );
      const insertResult = results[0];
      const orderId = insertResult[0]?.id;
      expect(orderId).toBeDefined();
      if (orderId === undefined) throw new Error('unreachable: asserted above');
      expect(events).toHaveLength(1);
      expect(events[0]?.op).toBe('insert');
      expect(events[0]?.pk).toBe(orderId);

      const sinceInsertSnapshot = events[0]?.snapshot ?? 0;
      const { events: updateEvents } = await processDbOperations(
        { eventsTable: pulseConformanceFixture.eventsTable },
        testPool,
        [db.update(orders).set({ status: 'accepted' }).where(eq(orders.id, orderId))],
      );
      expect(updateEvents).toHaveLength(1);
      expect(updateEvents[0]?.op).toBe('update');
      expect(updateEvents[0]?.pk).toBe(orderId);
      expect(updateEvents[0]?.snapshot).toBeGreaterThan(sinceInsertSnapshot);

      // --- The runtime reads them too: pull through the kit-generated publication/
      // events table via the runtime's own handler, proving byte parity at the read layer. ---
      const pulled = await pullClient(
        router,
        subscribed.clientId,
        subscribed.subscriptionId,
        subscribed.snapshot,
      );
      const pulledOps = pulled.events.map((event) => ({ op: event.op, pk: event.pk }));
      expect(pulledOps).toEqual([
        { op: 'insert', pk: orderId },
        { op: 'update', pk: orderId },
      ]);
    } finally {
      if (runtime) {
        await runtime.stop().catch(() => {});
      }
      await testPool.query(`DROP PUBLICATION IF EXISTS "${publicationName}"`).catch(() => {});
      await adminPool
        .query(
          'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
          [slotName],
        )
        .catch(() => {});
      await testPool.end();
      await sourceSql.end();
      await writeSql.end();
      await adminPool.query(
        `
          SELECT pg_terminate_backend(pid)
          FROM pg_stat_activity
          WHERE datname = $1
            AND pid <> pg_backend_pid()
        `,
        [databaseName],
      );
      await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
    }
  });
});
