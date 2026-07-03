import { describe, expect, test } from 'bun:test';
import { getColumns, getTableUniqueName } from 'drizzle-orm';
import { boolean, integer, pgSchema, primaryKey, serial, text, varchar } from 'drizzle-orm/pg-core';
import { z } from 'zod';
import { createPulse } from '../server/pulse.js';
import { PulseBuilder } from '../server/pulse-builder.js';
import { createPulseRegistry } from '../server/pulse-registry.js';
import type { PulseQueryConfig, PulseQueryContext } from '../server/pulse-types.js';

const testSchema = pgSchema('test');

const testTable = testSchema.table('test_items', {
  id: serial('id').primaryKey(),
  name: text('name').notNull(),
  status: text('status').notNull(),
  score: integer('score'),
});

const realtimeSchema = pgSchema('realtime');
const eventsTestTable = realtimeSchema.table('events_test_test_items', {
  id: integer('id').notNull(),
  name: text('name'),
  status: text('status'),
  score: integer('score'),
  oldId: integer('$old_id'),
  oldName: text('$old_name'),
  oldStatus: text('$old_status'),
  oldScore: integer('$old_score'),
  $snapshot: integer('$snapshot').generatedAlwaysAsIdentity(),
  $op: text('$op').notNull(),
  $timestamp: text('$timestamp').notNull(),
});

function makeEmptyConfig(): PulseQueryConfig<
  typeof testTable,
  Record<string, boolean>,
  Record<never, never>
> {
  const columns = getColumns(testTable);
  return {
    table: {
      source: testTable,
      events: null,
    },
    pkColumn: columns.id,
    columns,
    selectedColumns: columns,
    argsSchema: null,
    queryFn: null,
    transformFn: null,
    order: null,
    limit: null,
  };
}

const compositePkTable = testSchema.table(
  'composite_items',
  {
    leftId: integer('left_id').notNull(),
    rightId: integer('right_id').notNull(),
  },
  (table) => [primaryKey({ columns: [table.leftId, table.rightId] })],
);

const unsupportedPkTable = testSchema.table('unsupported_items', {
  id: boolean('id').primaryKey(),
  name: text('name').notNull(),
});

const varcharPkTable = testSchema.table('varchar_items', {
  code: varchar('code', { length: 64 }).primaryKey(),
  name: text('name').notNull(),
});

describe('PulseBuilder', () => {
  test('.columns() stores resolved selected columns and returns new instance', () => {
    const builder = new PulseBuilder(makeEmptyConfig());
    const b2 = builder.columns({ name: true, status: true });
    expect(b2).not.toBe(builder);
    expect(Object.keys(b2.config.selectedColumns)).toEqual(['name', 'status']);
    expect(Object.keys(builder.config.selectedColumns)).toEqual(['id', 'name', 'status', 'score']);
  });

  test('.args() stores Zod schema', () => {
    const schema = z.object({ driverId: z.number() });
    const builder = new PulseBuilder(makeEmptyConfig()).args(schema);
    expect(builder.config.argsSchema).toBe(schema);
  });

  test('.transform() stores transform function', () => {
    const transformFn = (rows: Record<string, unknown>[]) => rows;
    const builder = new PulseBuilder(makeEmptyConfig()).transform(transformFn);
    expect(builder.config.transformFn).toBe(transformFn);
  });

  test('.order() stores sort direction', () => {
    const builder = new PulseBuilder(makeEmptyConfig()).order('desc');
    expect(builder.config.order).toBe('desc');
  });

  test('.limit() stores row limit', () => {
    const builder = new PulseBuilder(makeEmptyConfig()).limit(10);
    expect(builder.config.limit).toBe(10);
  });

  test('.query() stores queryFn and returns PulseBuilder', () => {
    const queryFn = (_ctx: PulseQueryContext<Record<never, never>, Record<string, unknown>>) =>
      null;
    const builder = new PulseBuilder(makeEmptyConfig());
    const query = builder.query(queryFn);
    expect(query).toBeDefined();
    expect(query).toBeInstanceOf(PulseBuilder);
    expect(query.config).toBeDefined();
    expect(query.config.queryFn).toBe(queryFn);
  });

  test('.query() result has builder fields', () => {
    const query = new PulseBuilder(makeEmptyConfig()).query(() => null);
    expect(query).toHaveProperty('_');
    expect(query).toHaveProperty('config');
  });

  test('chain immutability: each method returns new instance', () => {
    const b0 = new PulseBuilder(makeEmptyConfig());
    const b1 = b0.columns({ name: true });
    const b2 = b1.limit(5);
    expect(b0).not.toBe(b1);
    expect(b1).not.toBe(b2);
    expect(Object.keys(b0.config.selectedColumns)).toEqual(['id', 'name', 'status', 'score']);
    expect(b0.config.limit).toBeNull();
  });

  test('full chain: .columns().args().order().limit().query() produces valid query', () => {
    const schema = z.object({ driverId: z.number() });
    const query = new PulseBuilder(makeEmptyConfig())
      .columns({ name: true, status: true })
      .args(schema)
      .order('desc')
      .limit(5)
      .query((ctx) => ({ driverId: ctx.args.driverId }));
    expect(Object.keys(query.config.selectedColumns)).toEqual(['name', 'status']);
    expect(query.config.argsSchema).toBe(schema);
    expect(query.config.queryFn).toBeDefined();
    expect(query.config.order).toBe('desc');
    expect(query.config.limit).toBe(5);
  });

  test('query callback can use ctx.query(...) with args', () => {
    const schema = z.object({ status: z.string() });
    const query = new PulseBuilder(makeEmptyConfig())
      .columns({ name: true })
      .args(schema)
      .query((ctx) => ctx.query({ status: ctx.args.status }));
    expect(Object.keys(query.config.selectedColumns)).toEqual(['name']);
    expect(query.config.argsSchema).toBe(schema);
    expect(query.config.queryFn).toBeDefined();
  });

  test('config is exposed on the builder', () => {
    const builder = new PulseBuilder(makeEmptyConfig());
    expect(builder.config).toEqual(makeEmptyConfig());
  });
});

describe('createPulse', () => {
  test('returns callable factory', () => {
    const pulse = createPulse();
    expect(typeof pulse).toBe('function');
  });

  test('pulse(table) returns PulseBuilder', () => {
    const pulse = createPulse();
    const builder = pulse(testTable);
    expect(builder).toBeInstanceOf(PulseBuilder);
    expect(getTableUniqueName(builder.config.table.source)).toBe('test.test_items');
  });

  test('pulse(table) creates builder with full selected columns and null optional fields', () => {
    const pulse = createPulse();
    const builder = pulse(testTable);
    const config = builder.config;
    expect(Object.keys(config.selectedColumns)).toEqual(['id', 'name', 'status', 'score']);
    expect(config.argsSchema).toBeNull();
    expect(config.queryFn).toBeNull();
    expect(config.transformFn).toBeNull();
    expect(config.order).toBeNull();
    expect(config.limit).toBeNull();
  });

  test('builder chain from createPulse produces valid query', () => {
    const pulse = createPulse();
    const query = pulse(testTable)
      .columns({ name: true, status: true })
      .args(z.object({ status: z.string() }))
      .query((ctx) => ctx.query({ status: ctx.args.status }));
    expect(Object.keys(query.config.selectedColumns)).toEqual(['name', 'status']);
    expect(query.config.queryFn).toBeDefined();
  });

  test('pulse(table) is a valid default query when added to registry', () => {
    const pulse = createPulse();
    const registry = createPulseRegistry({
      allOrders: pulse(testTable).$eventsTable(eventsTestTable),
    });

    const query = registry.resolve('allOrders', undefined, { userId: null });

    expect(query.where).toBeNull();
    expect(Object.keys(query.selectedColumns)).toEqual(
      expect.arrayContaining(['id', 'name', 'status', 'score']),
    );
    expect(Object.keys(query.selectedColumns).length).toBe(4);
    expect(query.order).toBe('asc');
  });

  test('pulse(table) rejects composite primary keys when no custom pk is provided', () => {
    const pulse = createPulse();

    expect(() => pulse(compositePkTable)).toThrowError('has multiple primary keys');
  });

  test('pulse(table) rejects unsupported default PK SQL types', () => {
    const pulse = createPulse();

    expect(() => pulse(unsupportedPkTable)).toThrowError('unsupported SQL type');
  });

  test('pulse(table) accepts PK types with length modifiers', () => {
    const pulse = createPulse();

    expect(() => pulse(varcharPkTable)).not.toThrow();
  });
});
