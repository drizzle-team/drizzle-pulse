import { describe, expect, test } from 'bun:test';
import { pgEnum, pgSchema, pgTable, serial, text, timestamp } from 'drizzle-orm/pg-core';
import { emitEventsTableDdl } from './helpers/events-table-ddl.js';

const moodEnum = pgEnum('ddl_test_mood', ['sad', 'ok', 'happy']);
const schemaEnum = pgSchema('ddl_test_schema').enum('ddl_test_status', ['open', 'closed']);

const sourceTable = pgTable('ddlfixture', {
  id: serial('id').primaryKey(),
  moodCol: moodEnum('mood_col'),
  tagsCol: text('tags_col').array(),
  timesCol: timestamp('times_col', { withTimezone: true }).array(),
});

const enumInSchemaTable = pgTable('ddlenumfixture', {
  id: serial('id').primaryKey(),
  statusCol: schemaEnum('status_col'),
});

describe('emitEventsTableDdl', () => {
  test('returns CREATE SCHEMA followed by a CREATE TABLE targeting the synthesized table', () => {
    const statements = emitEventsTableDdl(sourceTable);
    expect(statements).toHaveLength(2);
    expect(statements[0]).toBe('CREATE SCHEMA IF NOT EXISTS "drizzle_pulse"');
    expect(statements[1]).toStartWith('CREATE TABLE "drizzle_pulse"."public_ddlfixture" (');
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

  test('renders array source columns with a [] suffix, not a bare scalar type', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    expect(createTable).toContain('"tags_col" text[]');
    expect(createTable).toContain('"times_col" timestamp with time zone[]');
    expect(createTable).not.toMatch(/"tags_col"\s+text\s*(,|$)/m);
  });

  test('renders enum type identifiers quoted and schema-qualified', () => {
    const [, createTable] = emitEventsTableDdl(enumInSchemaTable);
    expect(createTable).toContain('"status_col" "ddl_test_schema"."ddl_test_status"');
    expect(createTable).not.toContain('ddl_test_schema.ddl_test_status"');
  });

  test('renders a schema-less enum quoted but unqualified', () => {
    const [, createTable] = emitEventsTableDdl(sourceTable);
    const moodLine = createTable?.split('\n').find((line) => line.includes('"mood_col"'));
    expect(moodLine).toBeDefined();
    expect(moodLine).toContain('"mood_col" "ddl_test_mood"');
  });
});
