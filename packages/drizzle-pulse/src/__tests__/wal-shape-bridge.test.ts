import { describe, expect, test } from 'bun:test';
import {
  bigint,
  integer,
  numeric,
  pgTable,
  point,
  serial,
  text,
  timestamp,
} from 'drizzle-orm/pg-core';
import { createShapeRowNormalizer } from '../server/wal-shape-bridge.js';

// Inline fixture (no DB): bigIntCol's TS property key deliberately differs from its SQL name
// (big_int_col) to exercise the SPIKE-02 3ac9af0 translation bug. numCol goes through the
// codec-normalize fallback (buildShape gives numeric:number no xform); countCol/labelCol/tagsCol
// have neither an xform nor a codec.normalize (basic-OID or driver-decoded-elsewhere), so they
// must pass through unchanged — real `integer` columns are one of minipg's 9 natively-decoded
// OIDs and never arrive as WAL text in production, so they're the passthrough case here, not a
// coercion case.
const fixtureTable = pgTable('shape_bridge_fixture', {
  id: serial('id').primaryKey(),
  bigIntCol: bigint('big_int_col', { mode: 'number' }),
  numCol: numeric('num_col', { mode: 'number' }),
  countCol: integer('count_col'),
  createdAt: timestamp('created_at'),
  tagsCol: text('tags_col').array(),
  locationCol: point('location_col'),
  labelCol: text('label_col'),
});

describe('createShapeRowNormalizer', () => {
  test('normalizes a value keyed by SQL name even though its TS property key differs (SPIKE-02 3ac9af0)', () => {
    const normalize = createShapeRowNormalizer(fixtureTable);
    const result = normalize({ big_int_col: '9007199254740' });
    expect(result['big_int_col']).toBe(9007199254740);
    expect(typeof result['big_int_col']).toBe('number');
  });

  test('coerces text through the xform and codec-normalize fallback chains', () => {
    const normalize = createShapeRowNormalizer(fixtureTable);
    const result = normalize({ num_col: '43.21', big_int_col: '9007199254740' });
    expect(result['num_col']).toBe(43.21);
    expect(result['big_int_col']).toBe(9007199254740);
  });

  test('passes null/undefined/non-string values through untouched, and leaves a no-xform/no-codec-normalize column unchanged', () => {
    const normalize = createShapeRowNormalizer(fixtureTable);
    const result = normalize({
      count_col: null,
      created_at: undefined,
      location_col: 5,
      label_col: 'hello world',
    });
    expect(result['count_col']).toBeNull();
    expect(result['created_at']).toBeUndefined();
    expect(result['location_col']).toBe(5);
    expect(result['label_col']).toBe('hello world');
  });

  // Concrete-value pin (DRIVER-03 gate passed — wal-normalization.ts is now deleted): these are
  // createWalRowNormalizer's captured outputs for this exact row, recorded once before its
  // removal. count_col/tags_col stay as raw text (no codec.normalize registered for plain
  // integer/text-array in this oracle); big_int_col/created_at/location_col decode via xform.
  test('normalizes integer, bigint-number, timestamp, text-array, and point columns to the values the deleted hand-rolled oracle produced', () => {
    const shapeNormalize = createShapeRowNormalizer(fixtureTable);

    const row = {
      count_col: '42',
      big_int_col: '9007199254740',
      created_at: '2024-06-07 08:09:10',
      tags_col: '{a,b,c}',
      location_col: '(1,2)',
    };

    expect(shapeNormalize(row)).toEqual({
      count_col: '42',
      big_int_col: 9007199254740,
      created_at: new Date('2024-06-07T08:09:10.000Z'),
      tags_col: '{a,b,c}',
      location_col: [1, 2],
    });
  });
});
