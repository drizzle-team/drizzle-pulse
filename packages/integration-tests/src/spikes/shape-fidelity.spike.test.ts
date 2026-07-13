/**
 * SPIKE-02 (Phase 17 GO/NO-GO): proves — or precisely characterizes — that rows decoded
 * through drizzle-orm's `buildShape.fromTableOrView` bridge to minipg's replication stream
 * match today's `wal-normalization.ts` output, over the full `pg_data_types` matrix (numerics,
 * timestamps, arrays, PostGIS, pgvector), including `$old_*` images under REPLICA IDENTITY FULL
 * and TOAST-omitted columns via `unchanged[]`. Throwaway proof code: no reusable adapter,
 * deleted or absorbed by Phase 19.
 *
 * The bridge under test ("one stream, two decoders"):
 *   - The replication connection's `types` option is left at minipg's default (`defaultDecoders`
 *     — see `minipg/dist/codec.js`): only 9 basic OIDs (bool/bytea/int2/int4/oid/float4/float8/
 *     json/jsonb) get a non-string decode; EVERY other OID (bigint, numeric, date/timestamp(tz),
 *     interval, uuid, point/line/geometry/vector/halfvec/sparsevec, cidr/inet/macaddr, bit, enums,
 *     text/varchar/char) falls back to raw pg text automatically. This is deliberate, not an
 *     omission: both decoders below see the SAME wire value per column — either the natively
 *     decoded JS value for the 9 basic OIDs (which needs no further processing on either side),
 *     or the shared raw-text string for everything else, exactly as `pg-logical-replication` +
 *     `scopeRawTextWalParsers` presents column values to today's `wal-normalization.ts` oracle.
 *     No `config.types` override was needed for the column-name-specific distinctions the shape
 *     needs either (e.g. point_tuple_col vs point_object_col share OID 600) — those live in the
 *     CANDIDATE decode below, keyed by column NAME via `Shape(...).$cols`, not by OID.
 *   - CANDIDATE: `Shape(buildShape.fromTableOrView(pgDataTypes)).$cols` gives, per column name,
 *     a `CodegenCol` carrying an optional `xform` — the exact composed decode function
 *     (driver codec normalize + drizzle's own mapFromDriverValue) buildShape would apply for a
 *     live query. When `xform` is absent (buildShape assumed minipg's OWN query-time decode of
 *     that OID+js-target already produces the final shape — true only for query results, NOT
 *     replication, since replication decode is OID-only with no js-target awareness), we fall
 *     back to the SAME per-column `codec` key drizzle uses to build that xform in the first
 *     place, resolved against minipg's own driver codec table (`miniPgCodecs`, from
 *     `drizzle-orm/postgres/codecs` — a public `./*` wildcard export, not a dist deep-import).
 *   - ORACLE: reconstruct what pg-logical-replication + scopeRawTextWalParsers deliver today —
 *     `pg`'s `types.getTypeParser(oid)` per column except the five raw-text OIDs (1082 date,
 *     1114 timestamp, 1184 timestamptz, 1186 interval, 600 point) — then run
 *     `createWalRowNormalizer(pgDataTypes)` over it, unmodified.
 */

import { afterAll, expect, test } from 'bun:test';
import { randomBytes } from 'node:crypto';
import { eq, getColumns } from 'drizzle-orm';
import { miniPgCodecs } from 'drizzle-orm/postgres/codecs';
import { buildShape } from 'drizzle-orm/postgres/shape';
import { drizzle } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';
import type { ReplicationConnection, ReplicationEvent } from 'minipg';
import { replication, Shape } from 'minipg';
import type { Pool } from 'pg';
import { types } from 'pg';
import postgres from 'postgres';
// Not exported from the built package surface (server-internal); .js resolves to the .ts
// source under both bun and this repo's bundler-mode tsconfig.
import { createWalRowNormalizer } from '../../../drizzle-pulse/src/server/wal-normalization.js';
import { pgDataTypesFixture } from '../fixtures/pg-data-types/index.js';
import { pgDataTypeInsertValues } from '../fixtures/pg-data-types/inventory.js';
import { pgDataTypes } from '../fixtures/pg-data-types/schema.js';
import { buildDatabaseUrl, createQuietPool, randomSuffix } from '../helpers/test-harness.js';

function spikeBaseUrl(): string {
  return process.env.DATABASE_URL ?? 'postgresql://postgres:postgres@localhost:5433/postgres';
}

// date/timestamp/timestamptz/interval/point: pg-types' defaults yield non-text JS values the
// oracle's from-text codecs cannot consume. Mirrors expose.ts's RAW_TEXT_PG_OIDS exactly.
const RAW_TEXT_PG_OIDS = new Set([1082, 1114, 1184, 1186, 600]);

type Row = Record<string, unknown>;
type InsertEvent = Extract<ReplicationEvent, { kind: 'insert' }>;
type UpdateEvent = Extract<ReplicationEvent, { kind: 'update' }>;
type DeleteEvent = Extract<ReplicationEvent, { kind: 'delete' }>;
type RelationEvent = Extract<ReplicationEvent, { kind: 'relation' }>;

type Classification = 'accept' | 'override' | 'block';
type DivergenceEntry = { classification: Classification; note: string };

// Empirically enumerated (Task 1 run against the real harness): every column NOT listed here
// must decode identically to the oracle; every column listed here MUST diverge (a stale entry
// — one that stops diverging — fails the completeness assertion below), and none may be
// 'block'-classified.
const EXPECTED_DIVERGENCES: Record<string, DivergenceEntry> = {};

const adminPool = createQuietPool(spikeBaseUrl());

afterAll(async () => {
  await adminPool.end();
});

function withQuietPostgresUrl(databaseUrl: string): string {
  const url = new URL(databaseUrl);
  url.searchParams.set('options', '-c client_min_messages=warning');
  return url.toString();
}

async function applyFixtureMigrations(databaseUrl: string, migrationsPath: string): Promise<void> {
  const migrationClient = postgres(withQuietPostgresUrl(databaseUrl), { max: 1 });
  const migrationDb = drizzle({ client: migrationClient });
  try {
    await migrate(migrationDb, { migrationsFolder: migrationsPath });
  } finally {
    await migrationClient.end();
  }
}

type Scenario = {
  databaseName: string;
  databaseUrl: string;
  writeSql: ReturnType<typeof postgres>;
  // Scoped to the scenario's OWN database — pg_class/pg_publication are per-database catalogs,
  // unlike pg_replication_slots (cluster-wide, queried via the shared adminPool instead).
  scenarioPool: Pool;
  rep: ReplicationConnection;
  slotName: string;
};

async function loadColumnOids(pool: Pool, tableName: string): Promise<Map<string, number>> {
  const { rows } = await pool.query<{ attname: string; atttypid: number }>(
    `
      SELECT a.attname, a.atttypid
      FROM pg_attribute a
      WHERE a.attrelid = $1::regclass
        AND a.attnum > 0
        AND NOT a.attisdropped
    `,
    [tableName],
  );
  return new Map(rows.map((row) => [row.attname, Number(row.atttypid)]));
}

async function createScenario(
  label: string,
): Promise<Scenario & { oidByName: Map<string, number> }> {
  const base = spikeBaseUrl();
  const databaseName = `drizzle_pulse_test_${label}_${randomSuffix()}`;
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);

  const databaseUrl = buildDatabaseUrl(base, databaseName);
  await applyFixtureMigrations(databaseUrl, pgDataTypesFixture.migrationsPath);

  const scenarioPool = createQuietPool(databaseUrl);
  const oidByName = await loadColumnOids(scenarioPool, 'pg_data_types');

  const writeSql = postgres(withQuietPostgresUrl(databaseUrl));

  // Deliberately no `types` override — minipg's default decoders are exactly the shared wire
  // input both decoders below diff against (see module doc "one stream, two decoders").
  const slotName = `test_slot_shape_${randomSuffix()}`;
  const rep = await replication({ url: databaseUrl });
  await rep.createSlot(slotName, { temporary: false, snapshot: 'nothing' });

  return { databaseName, databaseUrl, writeSql, scenarioPool, rep, slotName, oidByName };
}

async function teardownScenario(scenario: Scenario): Promise<void> {
  scenario.rep.end();

  try {
    await adminPool.query('SELECT pg_drop_replication_slot($1)', [scenario.slotName]);
  } catch {
    // Already dropped (e.g. an earlier assertion failure tore down mid-stream) — tolerate.
  }

  await scenario.writeSql.end();
  await scenario.scenarioPool.end();

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

  const remaining = await adminPool.query<{ count: string }>(
    'SELECT count(*)::text AS count FROM pg_replication_slots WHERE slot_name = $1',
    [scenario.slotName],
  );
  expect(remaining.rows[0]?.count).toBe('0');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Bounded consumption: rejects instead of hanging so a stuck stream fails fast rather than
// eating the whole bun test timeout.
async function collectUntil(
  rep: ReplicationConnection,
  iterator: AsyncGenerator<ReplicationEvent>,
  predicate: (events: ReplicationEvent[]) => boolean,
  opts?: { timeoutMs?: number },
): Promise<ReplicationEvent[]> {
  const timeoutMs = opts?.timeoutMs ?? 10_000;
  const deadline = Date.now() + timeoutMs;
  const events: ReplicationEvent[] = [];

  while (!predicate(events)) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(`collectUntil: timed out after ${events.length} events collected`);
    }
    const result = await Promise.race([
      iterator.next(),
      sleep(remaining).then((): { done: true; value: undefined } => ({
        done: true,
        value: undefined,
      })),
    ]);
    if (result.done) {
      throw new Error('collectUntil: timed out or stream ended');
    }
    events.push(result.value);
    void rep;
  }

  return events;
}

// --- The candidate decoder (Shape/CodegenCol-driven) -----------------------------------------

type ShapeCol = ReturnType<typeof Shape>['$cols'][number];

function buildCandidateDecoder(): (row: Row) => Row {
  const spec = buildShape.fromTableOrView(pgDataTypes);
  const mapper = Shape(spec);
  const colsByName = new Map<string, ShapeCol>(mapper.$cols.map((col) => [col.name, col]));

  const columnBySqlName = new Map<string, unknown>();
  for (const column of Object.values(getColumns(pgDataTypes))) {
    columnBySqlName.set((column as { name: string }).name, column);
  }

  return (row: Row): Row => {
    const out: Row = {};
    for (const [name, value] of Object.entries(row)) {
      // Only string (raw-text) values need further decode — minipg's default decoders already
      // produced the final JS shape for the 9 basic OIDs (bool/bytea/int/float/json), and both
      // sides treat that pre-decoded value identically (see module doc).
      if (value === null || value === undefined || typeof value !== 'string') {
        out[name] = value;
        continue;
      }
      const col = colsByName.get(name);
      if (col?.xform) {
        out[name] = col.xform(value);
        continue;
      }
      // buildShape assumed minipg's own query-time decode of this OID+js-target already
      // produces the final shape (e.g. point/vector) — true for query results, not
      // replication (OID-only decode, no js-target awareness). Fall back to the same
      // per-column codec key drizzle would have composed the xform from.
      const column = columnBySqlName.get(name) as { codec?: string } | undefined;
      const codec = column?.codec;
      const normalize = codec
        ? (miniPgCodecs as Record<string, { normalize?: (v: unknown) => unknown } | undefined>)[
            codec
          ]?.normalize
        : undefined;
      out[name] = normalize ? normalize(value) : value;
    }
    return out;
  };
}

// --- The oracle (today's output, reconstructed composition-identically) -----------------------

function buildOracleDecoder(oidByName: Map<string, number>): (row: Row) => Row {
  const normalize = createWalRowNormalizer(pgDataTypes);

  return (row: Row): Row => {
    const reconstructed: Row = {};
    for (const [name, value] of Object.entries(row)) {
      // Same guard as the candidate side: minipg already gave a final JS value for the 9 basic
      // OIDs, and wal-normalization's own normalizeWalValue is a no-op on non-string input too.
      if (value === null || value === undefined || typeof value !== 'string') {
        reconstructed[name] = value;
        continue;
      }
      const oid = oidByName.get(name);
      if (oid === undefined || RAW_TEXT_PG_OIDS.has(oid)) {
        reconstructed[name] = value;
        continue;
      }
      const parser = types.getTypeParser(oid) as (v: string) => unknown;
      reconstructed[name] = parser(value);
    }
    return normalize(reconstructed);
  };
}

// --- Per-column comparison ---------------------------------------------------------------------

type ColumnDiffRow = {
  column: string;
  match: boolean;
  classification?: Classification;
  note?: string;
  oracle?: unknown;
  candidate?: unknown;
};

function diffRows(label: string, candidateRow: Row, oracleRow: Row, matrix: ColumnDiffRow[]): void {
  const columnNames = new Set([...Object.keys(candidateRow), ...Object.keys(oracleRow)]);
  const undocumented: string[] = [];
  const blocking: string[] = [];

  for (const name of columnNames) {
    const oracleValue = oracleRow[name];
    const candidateValue = candidateRow[name];
    const matches = deepEqual(candidateValue, oracleValue);
    const expected = EXPECTED_DIVERGENCES[name];

    if (matches) {
      if (expected) {
        // Stale entry: this column no longer diverges. Fail loudly so the map can't rot.
        undocumented.push(`${name} (stale EXPECTED_DIVERGENCES entry — no longer diverges)`);
      }
      matrix.push({ column: name, match: true, oracle: oracleValue, candidate: candidateValue });
      continue;
    }

    if (!expected) {
      undocumented.push(name);
      matrix.push({
        column: name,
        match: false,
        oracle: oracleValue,
        candidate: candidateValue,
      });
      continue;
    }

    if (expected.classification === 'block') {
      blocking.push(name);
    }

    matrix.push({
      column: name,
      match: false,
      classification: expected.classification,
      note: expected.note,
      oracle: oracleValue,
      candidate: candidateValue,
    });
  }

  if (undocumented.length > 0) {
    throw new Error(
      `[${label}] undocumented divergence(s): ${undocumented.join(', ')} — classify in EXPECTED_DIVERGENCES`,
    );
  }
  if (blocking.length > 0) {
    throw new Error(`[${label}] block-class divergence(s): ${blocking.join(', ')}`);
  }
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a === 'bigint' || typeof b === 'bigint') return String(a) === String(b);
  if (a instanceof Date || b instanceof Date) {
    return a instanceof Date && b instanceof Date && a.getTime() === b.getTime();
  }
  if (Buffer.isBuffer(a) || Buffer.isBuffer(b)) {
    return Buffer.isBuffer(a) && Buffer.isBuffer(b) && a.equals(b);
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((value, index) => deepEqual(value, b[index]));
  }
  if (typeof a === 'object' && a !== null && typeof b === 'object' && b !== null) {
    const aKeys = Object.keys(a as Record<string, unknown>);
    const bKeys = Object.keys(b as Record<string, unknown>);
    if (aKeys.length !== bKeys.length) return false;
    return aKeys.every((key) =>
      deepEqual((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]),
    );
  }
  return false;
}

test('shape-decoded rows match the wal-normalization oracle across the pg_data_types matrix', async () => {
  const scenario = await createScenario('shape');
  const matrix: ColumnDiffRow[] = [];
  try {
    const relCheck = await scenario.scenarioPool.query<{ relreplident: string }>(
      `SELECT relreplident::text FROM pg_class WHERE oid = 'pg_data_types'::regclass`,
    );
    expect(relCheck.rows[0]?.relreplident).toBe('f');

    const pubCheck = await scenario.scenarioPool.query<{ pubname: string }>(
      `SELECT pubname FROM pg_publication WHERE pubname = $1`,
      [pgDataTypesFixture.publicationName],
    );
    expect(pubCheck.rows).toHaveLength(1);

    expect(typeof buildShape.fromTableOrView).toBe('function');
    const spec = buildShape.fromTableOrView(pgDataTypes);
    expect(spec).toBeDefined();

    const candidateDecode = buildCandidateDecoder();
    const oracleDecode = buildOracleDecoder(scenario.oidByName);

    const db = drizzle({ client: scenario.writeSql });

    const iterator = scenario.rep.start({
      slot: scenario.slotName,
      publications: [pgDataTypesFixture.publicationName],
      statusIntervalMs: 1000,
    });

    // Capture the relation event first (Task 1 read_first: the wire source of truth for
    // per-column OIDs) before driving any writes.
    const [insertedRow] = await db.insert(pgDataTypes).values(pgDataTypeInsertValues).returning({
      id: pgDataTypes.id,
    });
    const insertedId = insertedRow!.id;

    const updateValues: Partial<typeof pgDataTypes.$inferInsert> = {
      smallIntCol: 321,
      integerCol: 987654,
      bigIntNumberCol: 9007199254740111,
      bigIntBigIntCol: 9007199254741111n,
      bigIntStringCol: '9007199254742222',
      realCol: 2.75,
      doublePrecisionCol: 54321.123456,
      numericStringCol: '43.21',
      numericBigIntCol: 123456789012345678n,
      booleanCol: false,
      textCol: 'updated text value',
      dateCol: new Date('2024-06-07T00:00:00.000Z'),
      timestampCol: new Date('2024-06-07T08:09:10.000Z'),
      timestampTzCol: new Date('2024-06-07T08:09:10.000Z'),
      intervalCol: '2 days 03:04:05',
      pointTupleCol: [9.5, 8.5],
      pointObjectCol: { x: 7.5, y: 6.5 },
      lineTupleCol: [4, 5, 6],
      lineAbcCol: { a: 7, b: 8, c: 9 },
      geometryTupleCol: [50, 60],
      geometryObjectCol: { x: 70, y: 80 },
      vectorCol: [0.4, 0.5, 0.6],
      halfvecCol: [4, 5, 6],
      moodCol: 'sad',
    };
    await db.update(pgDataTypes).set(updateValues).where(eq(pgDataTypes.id, insertedId));

    await db.delete(pgDataTypes).where(eq(pgDataTypes.id, insertedId));

    // TOAST case: a second row with a large out-of-line text_col, then an update to a
    // DIFFERENT small column — text_col must surface via unchanged[].
    // Random (incompressible) so TOAST can't just inline-compress it away — it must be pushed
    // out-of-line to actually exercise the unchanged[] carry-forward below.
    const largeText = randomBytes(70_000).toString('hex');
    const [toastRow] = await db
      .insert(pgDataTypes)
      .values({ ...pgDataTypeInsertValues, textCol: largeText })
      .returning({ id: pgDataTypes.id });
    const toastId = toastRow!.id;
    await db.update(pgDataTypes).set({ integerCol: 42 }).where(eq(pgDataTypes.id, toastId));

    // 5 commits: insert, update, delete (row1) + insert, update (toast row).
    const events = await collectUntil(
      scenario.rep,
      iterator,
      (collected) => collected.filter((ev) => ev.kind === 'commit').length >= 5,
      { timeoutMs: 15_000 },
    );

    const relation = events.find((ev): ev is RelationEvent => ev.kind === 'relation');
    expect(relation).toBeDefined();
    for (const col of relation!.relation.columns) {
      expect(scenario.oidByName.get(col.name)).toBe(col.oid);
    }

    const inserts = events.filter((ev): ev is InsertEvent => ev.kind === 'insert');
    const updates = events.filter((ev): ev is UpdateEvent => ev.kind === 'update');
    const deletes = events.filter((ev): ev is DeleteEvent => ev.kind === 'delete');

    expect(inserts.length).toBeGreaterThanOrEqual(2);
    expect(updates.length).toBeGreaterThanOrEqual(2);
    expect(deletes.length).toBeGreaterThanOrEqual(1);

    // insert new-rows
    for (const ev of inserts) {
      diffRows('insert.new', candidateDecode(ev.new), oracleDecode(ev.new), matrix);
    }

    // update new-rows AND old-rows; oldKind must be 'full' under REPLICA IDENTITY FULL
    for (const ev of updates) {
      expect(ev.oldKind).toBe('full');
      expect(ev.old).not.toBeNull();
      diffRows('update.new', candidateDecode(ev.new), oracleDecode(ev.new), matrix);
      diffRows('update.old', candidateDecode(ev.old!), oracleDecode(ev.old!), matrix);
    }

    // delete old-rows
    for (const ev of deletes) {
      expect(ev.oldKind).toBe('full');
      diffRows('delete.old', candidateDecode(ev.old), oracleDecode(ev.old), matrix);
    }

    // TOAST assertion: text_col must appear in unchanged[] on the toast row's update event.
    const toastUpdate = updates.find(
      (ev) => ev.new.integer_col === '42' || ev.new.integer_col === 42,
    );
    expect(toastUpdate).toBeDefined();
    expect(toastUpdate!.unchanged).toContain('text_col');

    console.table(
      matrix.map((row) => ({
        column: row.column,
        match: row.match,
        classification: row.classification ?? '',
        note: row.note ?? '',
      })),
    );

    // Acked-then-torn-down hygiene: this spike doesn't gate on retention (that's SPIKE-01), but
    // a consumer should still ack what it processed before ending the connection.
    const commits = events.filter(
      (ev): ev is Extract<ReplicationEvent, { kind: 'commit' }> => ev.kind === 'commit',
    );
    const lastCommit = commits[commits.length - 1];
    if (lastCommit) {
      scenario.rep.ack(lastCommit.endLsn);
    }
  } finally {
    await teardownScenario(scenario);
  }
});
