import { sql } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import type { Pool } from 'pg';
import type { HarnessEvent } from './test-harness.js';

type FixtureWithEventsTable = {
  eventsTable: PgTable;
};

type DbEventOperation = PromiseLike<unknown>;
type DbEventResults<TOperations extends ReadonlyArray<DbEventOperation>> = {
  [TIndex in keyof TOperations]: Awaited<TOperations[TIndex]>;
};

export type ProcessDbOperationsOptions = {
  mode?: 'sequential' | 'concurrent';
};

export async function processDbOperations<
  const TOperations extends ReadonlyArray<DbEventOperation>,
>(
  fixture: FixtureWithEventsTable,
  pool: Pool,
  operations: TOperations,
  options?: ProcessDbOperationsOptions,
): Promise<{ events: HarnessEvent[]; results: DbEventResults<TOperations> }> {
  if (operations.length === 0) {
    return { events: [], results: [] as DbEventResults<TOperations> };
  }

  const sinceSnapshot = await getCurrentSnapshotForFixture(fixture, pool);
  const collectedResults: unknown[] = [];

  if (options?.mode === 'concurrent') {
    const results = await Promise.all(operations);
    for (const [index, result] of results.entries()) {
      collectedResults[index] = result;
    }
  } else {
    for (const [index, operation] of operations.entries()) {
      collectedResults[index] = await operation;
    }
  }

  const results = collectedResults as DbEventResults<TOperations>;

  const events = await waitForProcessedEventsForFixture(
    fixture,
    pool,
    sinceSnapshot,
    operations.length,
  );

  return { events, results };
}

async function getCurrentSnapshotForFixture(
  fixture: FixtureWithEventsTable,
  pool: Pool,
): Promise<number> {
  const eventsTableConfig = getTableConfig(fixture.eventsTable);
  const eventsTable = `"${eventsTableConfig.schema ?? 'public'}"."${eventsTableConfig.name}"`;
  const { rows } = await pool.query<{ max_snapshot: string }>(
    `SELECT COALESCE(MAX("$snapshot"), 0)::text AS max_snapshot FROM ${eventsTable}`,
  );
  return Number(rows[0]?.max_snapshot ?? '0');
}

async function waitForProcessedEventsForFixture(
  fixture: FixtureWithEventsTable,
  pool: Pool,
  sinceSnapshot: number,
  expectedEventCount: number,
  opts?: { timeoutMs?: number; pollIntervalMs?: number },
): Promise<HarnessEvent[]> {
  const eventsTableConfig = getTableConfig(fixture.eventsTable);
  const eventsTable = `"${eventsTableConfig.schema ?? 'public'}"."${eventsTableConfig.name}"`;
  const timeoutMs = opts?.timeoutMs ?? 15000;
  const pollIntervalMs = opts?.pollIntervalMs ?? 50;
  const targetSnapshot = sinceSnapshot + expectedEventCount;
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const { rows: snapshotRows } = await pool.query<{ max_snapshot: string }>(
      `SELECT COALESCE(MAX("$snapshot"), 0)::text AS max_snapshot FROM ${eventsTable}`,
    );
    const currentSnapshot = Number(snapshotRows[0]?.max_snapshot ?? '0');

    if (currentSnapshot >= targetSnapshot) {
      const { rows } = await pool.query<HarnessEvent>(
        `
          SELECT "$snapshot"::int AS snapshot, id AS pk, "$op" AS op, "$timestamp"::text AS timestamp
          FROM ${eventsTable}
          WHERE "$snapshot" > $1
            AND "$op" <> 'snapshot'
          ORDER BY "$snapshot" ASC
        `,
        [sinceSnapshot],
      );
      return rows;
    }

    await new Promise<void>((resolve) => {
      setTimeout(resolve, pollIntervalMs);
    });
  }

  const { rows } = await pool.query<{ max_snapshot: string }>(
    `SELECT COALESCE(MAX("$snapshot"), 0)::text AS max_snapshot FROM ${eventsTable}`,
  );
  const actualSnapshot = Number(rows[0]?.max_snapshot ?? '0');

  throw new Error(
    `Timeout: expected snapshot to reach at least ${targetSnapshot} after ${expectedEventCount} processed events, got ${actualSnapshot} in ${timeoutMs}ms`,
  );
}

/**
 * Insert a test user via Drizzle raw SQL.
 *
 * Replaces the `pg`-backed `insertTestUser` from the fixture helpers
 * module, keeping the same contract: creates a user with the given
 * username + `'test123'` password hash and returns `{ id, username }`.
 */
export async function insertTestUser(
  db: PostgresJsDatabase,
  username: string,
): Promise<{ id: number; username: string }> {
  const result = await db.execute<{ id: number; username: string }>(
    sql`INSERT INTO "users" (username, password_hash) VALUES (${username}, 'test123') RETURNING id, username`,
  );

  const row = result[0];
  if (!row) {
    throw new Error('insertTestUser did not return a row');
  }
  return row;
}
