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
// codec-normalize fallback (buildShape gives numeric:number no xform); countCol/labelCol have
// neither an xform nor a codec.normalize (basic-OID or driver-decoded-elsewhere), so they must
// pass through unchanged — real `integer` columns are one of minipg's 9 natively-decoded OIDs
// and never arrive as WAL text in production, so they're the passthrough case here, not a
// coercion case. tagsCol/intsCol are plain array columns (text[]/int[]): unlike scalar basic
// OIDs, minipg never wire-decodes arrays during replication, so they always arrive as raw
// '{...}' text and need the codec's normalizeArray parse+decode (WR-01).
const fixtureTable = pgTable('shape_bridge_fixture', {
  id: serial('id').primaryKey(),
  bigIntCol: bigint('big_int_col', { mode: 'number' }),
  numCol: numeric('num_col', { mode: 'number' }),
  countCol: integer('count_col'),
  createdAt: timestamp('created_at'),
  tagsCol: text('tags_col').array(),
  intsCol: integer('ints_col').array(),
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
  // removal. count_col stays as raw text (no codec.normalize registered for plain integer in
  // this oracle); big_int_col/created_at/location_col decode via xform.
  test('normalizes integer, bigint-number, timestamp, and point columns to the values the deleted hand-rolled oracle produced', () => {
    const shapeNormalize = createShapeRowNormalizer(fixtureTable);

    const row = {
      count_col: '42',
      big_int_col: '9007199254740',
      created_at: '2024-06-07 08:09:10',
      location_col: '(1,2)',
    };

    expect(shapeNormalize(row)).toEqual({
      count_col: '42',
      big_int_col: 9007199254740,
      created_at: new Date('2024-06-07T08:09:10.000Z'),
      location_col: [1, 2],
    });
  });

  // WR-01: array columns never arrive wire-decoded from replication (unlike the 9 basic scalar
  // OIDs) — every array value is raw '{...}' text regardless of element type, so it must go
  // through the codec's normalizeArray parse+decode to converge with a baseline SELECT's shape
  // instead of passing the raw text straight through.
  test('decodes raw-text array columns (text[], int[]) to the same shape a baseline SELECT would produce', () => {
    const normalize = createShapeRowNormalizer(fixtureTable);
    const result = normalize({ tags_col: '{a,b,c}', ints_col: '{1,2,3}' });
    expect(result['tags_col']).toEqual(['a', 'b', 'c']);
    expect(result['ints_col']).toEqual([1, 2, 3]);
  });
});
