/**
 * SPIKE-01 (Phase 17 GO/NO-GO): proves the vendored minipg driver streams WAL changes live
 * against the real Docker harness — runtime-created slot on a `drizzle_pulse` publication,
 * insert/update/delete delivered in commit order with full old-row images, and
 * `confirmed_flush_lsn` observably advancing both under acked sustained writes and on idle
 * (idleAck). Throwaway proof code: no reusable adapter, deleted or absorbed by Phase 19.
 */

import { afterAll, expect, test } from 'bun:test';
import type { ReplicationConnection, ReplicationEvent } from 'minipg';
import { lsnFromString, replication } from 'minipg';
import type { Pool } from 'pg';
import { buildDatabaseUrl, createQuietPool, randomSuffix } from '../helpers/test-harness.js';

// The harness's own DEFAULT_DATABASE_URL falls back to :5432 (only test:integration overrides
// it to :5433) — this spike must also pass when run directly, so it hardcodes its own :5433
// fallback rather than reusing baseDatabaseUrl().
function spikeBaseUrl(): string {
  return process.env.DATABASE_URL ?? 'postgresql://postgres:postgres@localhost:5433/postgres';
}

// A SPIKE-01 gate term, not a style choice.
const PUBLICATION_NAME = 'drizzle_pulse';

type InsertEvent = Extract<ReplicationEvent, { kind: 'insert' }>;
type UpdateEvent = Extract<ReplicationEvent, { kind: 'update' }>;
type DeleteEvent = Extract<ReplicationEvent, { kind: 'delete' }>;
type CommitEvent = Extract<ReplicationEvent, { kind: 'commit' }>;
type Transaction = ReplicationEvent[];

type Scenario = {
  databaseName: string;
  databaseUrl: string;
  writePool: Pool;
  rep: ReplicationConnection;
  slotName: string;
};

const adminPool = createQuietPool(spikeBaseUrl());

afterAll(async () => {
  await adminPool.end();
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function pollUntil<T>(
  fn: () => Promise<T>,
  predicate: (value: T) => boolean,
  opts?: { timeoutMs?: number; pollIntervalMs?: number },
): Promise<T> {
  const timeoutMs = opts?.timeoutMs ?? 5000;
  const pollIntervalMs = opts?.pollIntervalMs ?? 200;
  const deadline = Date.now() + timeoutMs;

  while (true) {
    const value = await fn();
    if (predicate(value)) return value;
    if (Date.now() >= deadline) {
      throw new Error('pollUntil: timed out waiting for condition');
    }
    await sleep(pollIntervalMs);
  }
}

async function sampleConfirmedFlush(slotName: string): Promise<bigint | null> {
  const { rows } = await adminPool.query<{ confirmed_flush_lsn: string | null }>(
    'SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1',
    [slotName],
  );
  const value = rows[0]?.confirmed_flush_lsn;
  return value ? lsnFromString(value) : null;
}

// Creates a fresh ephemeral database with its own spike_orders table, REPLICA IDENTITY FULL
// applied BEFORE the publication is created (mirrors expose.ts's reconcile ordering), and a
// runtime-created replication slot — never a migration-provisioned one.
async function createScenario(label: string): Promise<Scenario> {
  const base = spikeBaseUrl();
  const databaseName = `drizzle_pulse_test_${label}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);

  const databaseUrl = buildDatabaseUrl(base, databaseName);
  const writePool = createQuietPool(databaseUrl);

  await writePool.query(`
    CREATE TABLE spike_orders (
      id serial PRIMARY KEY,
      label text NOT NULL,
      amount integer NOT NULL
    )
  `);
  await writePool.query('ALTER TABLE spike_orders REPLICA IDENTITY FULL');
  await writePool.query(
    `CREATE PUBLICATION ${PUBLICATION_NAME} FOR TABLE spike_orders WITH (publish = 'insert, update, delete')`,
  );

  // Replication must target the ephemeral database itself (slots are cluster-wide, but the
  // publication + WAL decoding are per-database).
  const slotName = `test_slot_spike_${randomSuffix()}`;
  const rep = await replication(databaseUrl);
  await rep.createSlot(slotName, { temporary: false, snapshot: 'nothing' });

  return { databaseName, databaseUrl, writePool, rep, slotName };
}

// Reachable on assertion failure, not just success — callers always run this in a finally block.
async function teardownScenario(scenario: Scenario): Promise<void> {
  scenario.rep.end();

  try {
    await adminPool.query('SELECT pg_drop_replication_slot($1)', [scenario.slotName]);
  } catch {
    // Already dropped (e.g. an earlier assertion failure tore down mid-stream) — tolerate.
  }

  try {
    await scenario.writePool.query(`DROP PUBLICATION IF EXISTS ${PUBLICATION_NAME}`);
  } catch {
    // The ephemeral DB may already be unreachable after an earlier failure — DROP DATABASE
    // below removes the publication regardless.
  }

  await scenario.writePool.end();

  await adminPool.query(
    `
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = $1
        AND pid <> pg_backend_pid()
    `,
    [scenario.databaseName],
  );
  await adminPool.query(`DROP DATABASE IF EXISTS "${scenario.databaseName}"`);

  // Teardown proof: no stale slot survives this scenario.
  const remaining = await adminPool.query<{ count: string }>(
    'SELECT count(*)::text AS count FROM pg_replication_slots WHERE slot_name = $1',
    [scenario.slotName],
  );
  expect(remaining.rows[0]?.count).toBe('0');
}

// Bounded consumption: rejects instead of hanging so a stuck stream fails fast rather than
// eating the whole bun test timeout.
async function collectEventsUntilCommits(
  rep: ReplicationConnection,
  opts: { slot: string; publications: string[]; minCommits: number; timeoutMs?: number },
): Promise<ReplicationEvent[]> {
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const iterator = rep.start({
    slot: opts.slot,
    publications: opts.publications,
    statusIntervalMs: 1000,
  });

  const events: ReplicationEvent[] = [];
  let commits = 0;
  const deadline = Date.now() + timeoutMs;

  while (commits < opts.minCommits) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(
        `collectEventsUntilCommits: timed out after ${commits}/${opts.minCommits} commits (${events.length} events collected)`,
      );
    }

    const result = await Promise.race([
      iterator.next(),
      sleep(remaining).then((): { done: true; value: undefined } => ({
        done: true,
        value: undefined,
      })),
    ]);

    if (result.done) {
      throw new Error(
        `collectEventsUntilCommits: timed out or stream ended after ${commits}/${opts.minCommits} commits`,
      );
    }

    events.push(result.value);
    if (result.value.kind === 'commit') commits += 1;
  }

  return events;
}

// Splits a flat event stream into per-transaction chunks bounded by begin/commit, dropping
// any stray relation/message events that arrive outside a transaction boundary.
function groupIntoTransactions(events: ReplicationEvent[]): Transaction[] {
  const transactions: Transaction[] = [];
  let current: Transaction | null = null;

  for (const ev of events) {
    if (ev.kind === 'begin') {
      current = [ev];
      continue;
    }
    if (!current) continue;
    current.push(ev);
    if (ev.kind === 'commit') {
      transactions.push(current);
      current = null;
    }
  }

  return transactions;
}

test('streams insert/update/delete in commit order from a runtime-created slot on the drizzle_pulse publication', async () => {
  const scenario = await createScenario('order');
  try {
    const insertResult = await scenario.writePool.query<{ id: number }>(
      `INSERT INTO spike_orders (label, amount) VALUES ($1, $2), ($3, $4), ($5, $6) RETURNING id`,
      ['a', 1, 'b', 2, 'c', 3],
    );
    const ids = insertResult.rows.map((row) => row.id);

    await scenario.writePool.query(
      'UPDATE spike_orders SET amount = amount + 100 WHERE id = ANY($1)',
      [[ids[0], ids[1]]],
    );

    await scenario.writePool.query('DELETE FROM spike_orders WHERE id = $1', [ids[2]]);

    const events = await collectEventsUntilCommits(scenario.rep, {
      slot: scenario.slotName,
      publications: [PUBLICATION_NAME],
      minCommits: 3,
    });

    const transactions = groupIntoTransactions(events);
    expect(transactions).toHaveLength(3);
    const [insertTx, updateTx, deleteTx] = transactions;

    const inserts = insertTx!.filter((ev): ev is InsertEvent => ev.kind === 'insert');
    expect(inserts).toHaveLength(3);
    expect(inserts.map((ev) => ev.new.label).sort()).toEqual(['a', 'b', 'c']);

    const updates = updateTx!.filter((ev): ev is UpdateEvent => ev.kind === 'update');
    expect(updates).toHaveLength(2);
    for (const ev of updates) {
      expect(ev.oldKind).toBe('full');
      expect(ev.old).not.toBeNull();
    }
    const updatedAmounts = new Set(updates.map((ev) => ev.new.amount));
    expect(updatedAmounts.has(101)).toBe(true);
    expect(updatedAmounts.has(102)).toBe(true);

    const deletes = deleteTx!.filter((ev): ev is DeleteEvent => ev.kind === 'delete');
    expect(deletes).toHaveLength(1);
    expect(deletes[0]!.oldKind).toBe('full');
    expect(deletes[0]!.old).not.toBeNull();

    const commitLsns = transactions.map((tx) => {
      const commit = tx.find((ev): ev is CommitEvent => ev.kind === 'commit')!;
      return lsnFromString(commit.lsn);
    });
    expect(commitLsns[0]! < commitLsns[1]!).toBe(true);
    expect(commitLsns[1]! < commitLsns[2]!).toBe(true);
  } finally {
    await teardownScenario(scenario);
  }
});

test('confirmed_flush_lsn advances under sustained acked writes and on idle via idleAck', async () => {
  const scenario = await createScenario('flush');
  try {
    const before = await pollUntil(
      () => sampleConfirmedFlush(scenario.slotName),
      (value) => value !== null,
      { timeoutMs: 2000, pollIntervalMs: 100 },
    );

    // Gate A: advancement under sustained acked writes.
    const burstSize = 50;
    for (let i = 0; i < burstSize; i++) {
      await scenario.writePool.query('INSERT INTO spike_orders (label, amount) VALUES ($1, $2)', [
        `burst-${i}`,
        i,
      ]);
    }

    const iterator = scenario.rep.start({
      slot: scenario.slotName,
      publications: [PUBLICATION_NAME],
      statusIntervalMs: 1000,
    });

    // API surprise (see SUMMARY): idleAck's internal "fully delivered" gate tracks each
    // commit's endLsn, not its lsn — acking ev.lsn alone means flushed never reaches
    // lastDeliveredEnd, so idleAck never engages. Must ack endLsn for idle catch-up to work.
    let lastAckedEndLsn: string | null = null;
    let commits = 0;
    const burstDeadline = Date.now() + 12_000;
    while (commits < burstSize) {
      if (Date.now() > burstDeadline) {
        throw new Error(`Gate A: only observed ${commits}/${burstSize} commits before timeout`);
      }
      const result = await iterator.next();
      if (result.done) throw new Error('Gate A: replication stream ended unexpectedly');
      const ev = result.value;
      if (ev.kind === 'commit') {
        commits += 1;
        lastAckedEndLsn = ev.endLsn;
        // Process-then-ack: the commit is already recorded above before we acknowledge it.
        scenario.rep.ack(ev.endLsn);
      }
    }

    expect(commits).toBe(burstSize);
    expect(lsnFromString(scenario.rep.flushedLsn)).toBe(lsnFromString(lastAckedEndLsn!));

    const afterBurst = await pollUntil(
      () => sampleConfirmedFlush(scenario.slotName),
      (value) => value !== null && before !== null && value > before,
      { timeoutMs: 8000, pollIntervalMs: 200 },
    );
    expect(afterBurst !== null && before !== null && afterBurst > before).toBe(true);

    // Gate B: idle advancement. No more writes to the published table; a couple of unrelated
    // writes move the server's WAL position so idleAck (default true) has somewhere new to
    // catch up to without any further explicit acks. The driver's internal read loop only
    // progresses while a next() call is pending (it processes keepalives without yielding), so
    // a background pump keeps draining the stream while we wait — a fixed-time single next()
    // call would miss keepalives that arrive after it settles.
    let pumping = true;
    const pump = (async () => {
      try {
        while (pumping) {
          const result = await iterator.next();
          if (result.done) break;
        }
      } catch {
        // rep.end() during teardown intentionally interrupts a pending next() call.
      }
    })();

    await scenario.writePool.query('CREATE TABLE spike_scratch (id serial PRIMARY KEY, n integer)');
    await sleep(150);
    await scenario.writePool.query('INSERT INTO spike_scratch (n) VALUES (1)');
    await sleep(300);
    await scenario.writePool.query('INSERT INTO spike_scratch (n) VALUES (2)');

    await pollUntil(
      async () =>
        lsnFromString(scenario.rep.flushedLsn) >= lsnFromString(scenario.rep.lastReceivedLsn),
      (caughtUp) => caughtUp,
      { timeoutMs: 5000, pollIntervalMs: 150 },
    );
    pumping = false;

    expect(lsnFromString(scenario.rep.flushedLsn)).toBe(
      lsnFromString(scenario.rep.lastReceivedLsn),
    );

    const expectedFlushed = lsnFromString(scenario.rep.flushedLsn);
    const idleConfirmed = await pollUntil(
      () => sampleConfirmedFlush(scenario.slotName),
      (value) => value !== null && value >= expectedFlushed,
      // A status heartbeat only ticks once per statusIntervalMs (1000ms here); give it a few.
      { timeoutMs: 5000, pollIntervalMs: 200 },
    );
    expect(idleConfirmed).not.toBeNull();
    expect(idleConfirmed! >= expectedFlushed).toBe(true);
    void pump; // fire-and-forget: settles once teardown's rep.end() interrupts the pending read
  } finally {
    await teardownScenario(scenario);
  }
});
