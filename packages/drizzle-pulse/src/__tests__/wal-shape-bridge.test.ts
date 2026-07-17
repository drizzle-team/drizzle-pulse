import { describe, expect, test } from 'bun:test';
import { bigint, integer, numeric, pgTable, point, text, timestamp } from 'drizzle-orm/pg-core';
import {
  buildTableShape,
  indexColumnsBySqlName,
  reKeyToJsProps,
} from '../server/wal-shape-bridge.js';

// TS property keys deliberately differ from SQL names to exercise re-keying; columns pair up by
// mode (int8 number vs bigint, timestamp date vs string) to prove the shape carries the mode, not
// just the base type. tagsCol is an array; locationCol a point — both mode-carrying non-scalars.
const fixtureTable = pgTable('shape_bridge_fixture', {
  id: integer('id').primaryKey(),
  bigIntNum: bigint('big_int_num', { mode: 'number' }),
  bigIntBig: bigint('big_int_big', { mode: 'bigint' }),
  numCol: numeric('num_col', { mode: 'number' }),
  countCol: integer('count_col'),
  createdAt: timestamp('created_at'),
  createdAtStr: timestamp('created_at_str', { mode: 'string' }),
  tagsCol: text('tags_col').array(),
  labelCol: text('label_col'),
  locationCol: point('location_col'),
});

describe('buildTableShape', () => {
  test('keys the shape by SQL column name with mode-aware decode specs', () => {
    const { schema, table, shape } = buildTableShape(fixtureTable);
    expect(schema).toBe('public');
    expect(table).toBe('shape_bridge_fixture');
    expect(shape).toEqual({
      id: 'int4',
      big_int_num: 'int8:number',
      big_int_big: 'int8:bigint',
      num_col: 'numeric:number',
      count_col: 'int4',
      created_at: 'timestamp:date',
      created_at_str: 'timestamp:string',
      tags_col: 'text[]',
      label_col: 'text',
      location_col: 'point:tuple',
    });
  });
});

describe('indexColumnsBySqlName / reKeyToJsProps', () => {
  test('maps each SQL name to its JS property key and column', () => {
    const map = indexColumnsBySqlName(fixtureTable);
    expect(map.get('big_int_num')?.jsKey).toBe('bigIntNum');
    expect(map.get('big_int_num')?.column).toBe(fixtureTable.bigIntNum);
    expect(map.get('location_col')?.jsKey).toBe('locationCol');
  });

  test('re-keys a SQL-name-keyed row to JS property keys, passing values through untouched', () => {
    const map = indexColumnsBySqlName(fixtureTable);
    expect(reKeyToJsProps({ big_int_num: 5, created_at: null, tags_col: ['a', 'b'] }, map)).toEqual(
      { bigIntNum: 5, createdAt: null, tagsCol: ['a', 'b'] },
    );
  });

  test('falls back to the raw key for a column not in the map', () => {
    const map = indexColumnsBySqlName(fixtureTable);
    expect(reKeyToJsProps({ not_a_column: 7 }, map)).toEqual({ not_a_column: 7 });
  });
});
