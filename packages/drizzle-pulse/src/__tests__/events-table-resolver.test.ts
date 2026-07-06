import { describe, expect, test } from 'bun:test';
import { getColumns, is } from 'drizzle-orm';
import type { PgColumn, PgTable as PgTableType } from 'drizzle-orm/pg-core';
import {
  bigint,
  bigserial,
  bit,
  char,
  decimal,
  geometry,
  getTableConfig,
  halfvec,
  integer,
  numeric,
  PgTable,
  pgEnum,
  pgSchema,
  pgTable,
  point,
  serial,
  smallserial,
  sparsevec,
  text,
  timestamp,
  varchar,
  vector,
} from 'drizzle-orm/pg-core';
import { emitEventsTableDdl } from '../server/events-table-ddl.js';
import {
  DEFAULT_EVENTS_SCHEMA,
  getEventsTableName,
  resolveEventsTable,
} from '../server/events-table-resolver.js';

const moodEnum = pgEnum('resolver_test_mood', ['sad', 'ok', 'happy']);

const sourceTable = pgTable('resolver_fixture', {
  id: serial('id').primaryKey(),
  smallSerialCol: smallserial('small_serial_col'),
  bigSerialNumberCol: bigserial('big_serial_number_col', { mode: 'number' }),
  bigSerialBigIntCol: bigserial('big_serial_bigint_col', { mode: 'bigint' }),
  bigIntNumberCol: bigint('big_int_number_col', { mode: 'number' }),
  bigIntBigIntCol: bigint('big_int_bigint_col', { mode: 'bigint' }),
  bigIntStringCol: bigint('big_int_string_col', { mode: 'string' }),
  numericStringCol: numeric('numeric_string_col', { precision: 10, scale: 2 }),
  numericNumberCol: numeric('numeric_number_col', { mode: 'number' }),
  decimalBigIntCol: decimal('decimal_bigint_col', { mode: 'bigint' }),
  varcharCol: varchar('varchar_col', { length: 64, enum: ['red', 'green', 'blue'] }),
  charCol: char('char_col', { length: 3 }),
  moodCol: moodEnum('mood_col'),
  timestampTzCol: timestamp('timestamp_tz_col', { withTimezone: true }),
  timestampStringCol: timestamp('timestamp_string_col', { mode: 'string' }),
  pointTupleCol: point('point_tuple_col'),
  pointObjectCol: point('point_object_col', { mode: 'xy' }),
  geometryCol: geometry('geometry_col', { type: 'point', srid: 4326 }),
  vectorCol: vector('vector_col', { dimensions: 3 }),
  halfvecCol: halfvec('halfvec_col', { dimensions: 3 }),
  sparsevecCol: sparsevec('sparsevec_col', { dimensions: 3 }),
  bitCol: bit('bit_col', { dimensions: 8 }),
  notNullCol: text('not_null_col').notNull(),
  defaultedCol: text('defaulted_col').default('hello'),
  identityCol: integer('identity_col').generatedAlwaysAsIdentity(),
  tagsCol: text('tags_col').array(),
  timesCol: timestamp('times_col', { withTimezone: true }).array(),
});

const schemaEnum = pgSchema('resolver_test_schema').enum('resolver_test_status', [
  'open',
  'closed',
]);

const enumInSchemaTable = pgTable('resolver_enum_schema_fixture', {
  id: serial('id').primaryKey(),
  statusCol: schemaEnum('status_col'),
});

function getColumnByName(table: PgTableType, name: string): PgColumn {
  const column = Object.values(getColumns(table)).find((candidate) => candidate.name === name);
  if (!column) {
    throw new Error(`Column "${name}" not found on table`);
  }
  return column;
}

const SERIAL_COLUMN_TYPES = new Set([
  'PgSerial',
  'PgSmallSerial',
  'PgBigSerial53',
  'PgBigSerial64',
]);

describe('resolveEventsTable', () => {
  test('derives the events table name and schema by convention', () => {
    const eventsTable = resolveEventsTable(sourceTable);
    const config = getTableConfig(eventsTable);
    expect(config.name).toBe('__events_public_resolver_fixture');
    expect(config.schema).toBe(DEFAULT_EVENTS_SCHEMA);
    expect(DEFAULT_EVENTS_SCHEMA).toBe('drizzle');
    expect(getEventsTableName(sourceTable)).toBe('__events_public_resolver_fixture');

    const customSchema = resolveEventsTable(sourceTable, { eventsSchema: 'custom_schema' });
    expect(getTableConfig(customSchema).schema).toBe('custom_schema');
  });

  test('produces 2x source columns + 3 metadata columns, every name present', () => {
    const eventsTable = resolveEventsTable(sourceTable);
    const sourceColumns = Object.values(getColumns(sourceTable));
    const eventsColumns = getColumns(eventsTable);
    const eventsColumnNames = Object.values(eventsColumns).map((column) => column.name);

    expect(Object.keys(eventsColumns).length).toBe(sourceColumns.length * 2 + 3);

    for (const sourceColumn of sourceColumns) {
      expect(eventsColumnNames).toContain(sourceColumn.name);
      expect(eventsColumnNames).toContain(`$old_${sourceColumn.name}`);
    }
    for (const metadataName of ['$snapshot', '$op', '$timestamp']) {
      expect(eventsColumnNames).toContain(metadataName);
    }
  });

  test('only the PK new-value column is notNull; primaryKey is stripped everywhere', () => {
    const eventsTable = resolveEventsTable(sourceTable);

    const idColumn = getColumnByName(eventsTable, 'id');
    expect(idColumn.notNull).toBe(true);
    expect(idColumn.primary).toBe(false);

    const notNullCol = getColumnByName(eventsTable, 'not_null_col');
    expect(notNullCol.notNull).toBe(false);

    const oldId = getColumnByName(eventsTable, '$old_id');
    expect(oldId.notNull).toBe(false);

    for (const column of Object.values(getColumns(eventsTable))) {
      expect(column.primary).toBe(false);
    }
  });

  test('non-serial columns preserve the source getSQLType(); serial family relaxes to integer/bigint', () => {
    const eventsTable = resolveEventsTable(sourceTable);

    for (const sourceColumn of Object.values(getColumns(sourceTable))) {
      if (SERIAL_COLUMN_TYPES.has(sourceColumn.columnType)) continue;
      const clone = getColumnByName(eventsTable, sourceColumn.name);
      expect(clone.getSQLType()).toBe(sourceColumn.getSQLType());
    }

    expect(getColumnByName(eventsTable, 'id').getSQLType()).toBe('integer');
    expect(getColumnByName(eventsTable, 'small_serial_col').getSQLType()).toBe('integer');
    expect(getColumnByName(eventsTable, 'big_serial_number_col').getSQLType()).toBe('bigint');
    expect(getColumnByName(eventsTable, 'big_serial_bigint_col').getSQLType()).toBe('bigint');
  });

  test('cloned columns carry no default/generated/identity config; metadata columns match the spec', () => {
    const eventsTable = resolveEventsTable(sourceTable);
    const metadataNames = new Set(['$snapshot', '$op', '$timestamp']);

    for (const column of Object.values(getColumns(eventsTable))) {
      if (metadataNames.has(column.name)) continue;
      expect(column.hasDefault).toBe(false);
      expect(column.generatedIdentity).toBeUndefined();
      expect(column.generated).toBeUndefined();
      expect(column.primary).toBe(false);
    }

    const snapshot = getColumnByName(eventsTable, '$snapshot');
    expect(snapshot.generatedIdentity?.type).toBe('always');
    expect(snapshot.getSQLType()).toBe('integer');

    const op = getColumnByName(eventsTable, '$op');
    expect(op.notNull).toBe(true);
    expect(op.getSQLType()).toBe('text');
    expect(op.generatedIdentity).toBeUndefined();

    const timestampCol = getColumnByName(eventsTable, '$timestamp');
    expect(timestampCol.notNull).toBe(true);
    expect(timestampCol.hasDefault).toBe(true);
    expect(timestampCol.getSQLType()).toBe('timestamp with time zone');
  });

  test('enum columns reference the same enum instance on the twin', () => {
    const eventsTable = resolveEventsTable(sourceTable);
    const sourceMood = getColumnByName(sourceTable, 'mood_col');
    const moodNew = getColumnByName(eventsTable, 'mood_col');
    const moodOld = getColumnByName(eventsTable, '$old_mood_col');

    expect(moodNew.getSQLType()).toBe(sourceMood.getSQLType());
    expect(moodOld.getSQLType()).toBe(sourceMood.getSQLType());
    expect(moodNew.enumValues).toEqual(['sad', 'ok', 'happy']);
    expect(moodOld.enumValues).toEqual(['sad', 'ok', 'happy']);
  });

  test('throws loudly when the derived table name exceeds 63 bytes', () => {
    const longTable = pgTable(`resolver_fixture_${'x'.repeat(60)}`, {
      id: serial('id').primaryKey(),
    });

    expect(() => resolveEventsTable(longTable)).toThrow(
      /exceeding Postgres's 63-byte identifier limit/,
    );
  });

  test('throws loudly when a derived $old_ column name exceeds 63 bytes', () => {
    const longColumnName = 'a'.repeat(60);
    const longColumnTable = pgTable('resolver_long_col_fixture', {
      id: serial('id').primaryKey(),
      longCol: text(longColumnName),
    });

    expect(() => resolveEventsTable(longColumnTable)).toThrow(
      /\$old_a+.*exceeding Postgres's 63-byte identifier limit/,
    );
  });

  test("the synthesized table passes drizzle's is(result, PgTable) check", () => {
    const eventsTable = resolveEventsTable(sourceTable);
    expect(is(eventsTable, PgTable)).toBe(true);
  });

  test('array columns preserve dimensions and getSQLType on both new-value and $old_ clones (CR-01)', () => {
    const eventsTable = resolveEventsTable(sourceTable);
    const sourceTags = getColumnByName(sourceTable, 'tags_col');
    const sourceTimes = getColumnByName(sourceTable, 'times_col');

    const tagsClone = getColumnByName(eventsTable, 'tags_col');
    const oldTagsClone = getColumnByName(eventsTable, '$old_tags_col');
    expect(tagsClone.dimensions).toBe(sourceTags.dimensions);
    expect(tagsClone.dimensions).toBe(1);
    expect(tagsClone.getSQLType()).toBe(sourceTags.getSQLType());
    expect(oldTagsClone.dimensions).toBe(sourceTags.dimensions);

    const timesClone = getColumnByName(eventsTable, 'times_col');
    expect(timesClone.dimensions).toBe(sourceTimes.dimensions);
    expect(timesClone.getSQLType()).toBe(sourceTimes.getSQLType());
  });

  test('cloned array columns keep the source column array (de)serialization codec (CR-01)', () => {
    const eventsTable = resolveEventsTable(sourceTable);
    const timesClone = getColumnByName(eventsTable, 'times_col');
    const now = new Date('2024-01-01T00:00:00.000Z');

    // Without postBuild(), mapToDriverValue receives the raw array and calls
    // value.toISOString() on it directly (reproduced in review CR-01: throws
    // "value.toISOString is not a function"). postBuild() wraps the codec to map
    // each array element instead.
    expect(() => timesClone.mapToDriverValue([now])).not.toThrow();
    expect(timesClone.mapToDriverValue([now])).toEqual([now.toISOString()]);
  });
});

describe('emitEventsTableDdl', () => {
  test('returns CREATE SCHEMA followed by a CREATE TABLE targeting the synthesized table', () => {
    const statements = emitEventsTableDdl(sourceTable);
    expect(statements).toHaveLength(2);
    expect(statements[0]).toBe('CREATE SCHEMA IF NOT EXISTS "drizzle"');
    expect(statements[1]).toStartWith(
      'CREATE TABLE "drizzle"."__events_public_resolver_fixture" (',
    );
  });

  test('renders the PK line NOT NULL and the serial source column as plain integer', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    expect(createTable).toContain('"id" integer NOT NULL');
    expect(createTable).not.toMatch(/"id"\s+serial/);
  });

  test('renders $snapshot as an identity column with no trailing NOT NULL', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    expect(createTable).toContain('"$snapshot" integer GENERATED ALWAYS AS IDENTITY');
    expect(createTable).not.toMatch(/"\$snapshot"[^,]*NOT NULL/);
  });

  test('renders $op as text NOT NULL', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    expect(createTable).toContain('"$op" text NOT NULL');
  });

  test('renders $timestamp with both DEFAULT now() and NOT NULL', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    expect(createTable).toContain('"$timestamp" timestamp with time zone DEFAULT now() NOT NULL');
  });

  test('renders an $old_ twin line as nullable (no NOT NULL suffix)', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    const oldIdLine = createTable?.split('\n').find((line) => line.includes('"$old_id"'));
    expect(oldIdLine).toBeDefined();
    expect(oldIdLine).not.toMatch(/NOT NULL/);
  });

  test('honors an explicit eventsSchema override', () => {
    const statements = emitEventsTableDdl(sourceTable, { eventsSchema: 'custom_schema' });
    expect(statements[0]).toBe('CREATE SCHEMA IF NOT EXISTS "custom_schema"');
    expect(statements[1]).toStartWith('CREATE TABLE "custom_schema".');
  });

  test('renders array source columns with a [] suffix, not a bare scalar type (CR-01)', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    expect(createTable).toContain('"tags_col" text[]');
    expect(createTable).toContain('"times_col" timestamp with time zone[]');
    expect(createTable).not.toMatch(/"tags_col"\s+text\s*(,|$)/m);
  });

  test('renders enum type identifiers quoted and schema-qualified (WR-01)', () => {
    const [, createTable] = emitEventsTableDdl(enumInSchemaTable);
    expect(createTable).toContain('"status_col" "resolver_test_schema"."resolver_test_status"');
    expect(createTable).not.toContain('resolver_test_schema.resolver_test_status"');
  });

  test('renders a schema-less enum quoted but unqualified (WR-01)', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    const moodLine = createTable?.split('\n').find((line) => line.includes('"mood_col"'));
    expect(moodLine).toBeDefined();
    expect(moodLine).toContain('"mood_col" "resolver_test_mood"');
  });
});
