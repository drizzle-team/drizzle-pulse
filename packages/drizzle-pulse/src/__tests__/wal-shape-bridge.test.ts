import { describe, expect, test } from 'bun:test';
import { bigint, integer, numeric, pgTable, point, text, timestamp } from 'drizzle-orm/pg-core';
import { buildTableShape } from '../server/wal-shape-bridge.js';

// TS property keys deliberately differ from SQL names to exercise the columns mapping; columns
// pair up by mode (int8 number vs bigint, timestamp date vs string) to prove the shape carries
// the mode, not just the base type. tagsCol is an array; locationCol a point — both
// mode-carrying non-scalars.
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
  test('keys the shape by JS property name with mode-aware decode specs', () => {
    const { schema, table, shape } = buildTableShape(fixtureTable);
    expect(schema).toBe('public');
    expect(table).toBe('shape_bridge_fixture');
    expect(shape).toEqual({
      id: 'int4',
      bigIntNum: 'int8:number',
      bigIntBig: 'int8:bigint',
      numCol: 'numeric:number',
      countCol: 'int4',
      createdAt: 'timestamp:date',
      createdAtStr: 'timestamp:string',
      tagsCol: 'text[]',
      labelCol: 'text',
      locationCol: 'point:tuple',
    });
  });

  test('maps only the property keys whose SQL column name differs', () => {
    const { columns } = buildTableShape(fixtureTable);
    expect(columns).toEqual({
      bigIntNum: 'big_int_num',
      bigIntBig: 'big_int_big',
      numCol: 'num_col',
      countCol: 'count_col',
      createdAt: 'created_at',
      createdAtStr: 'created_at_str',
      tagsCol: 'tags_col',
      labelCol: 'label_col',
      locationCol: 'location_col',
    });
    expect(columns).not.toHaveProperty('id');
  });
});
