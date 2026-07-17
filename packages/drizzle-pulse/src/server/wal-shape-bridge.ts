import { type Column, getColumns } from 'drizzle-orm';
import type { PgTable } from 'drizzle-orm/pg-core';
import { minipgCodecs } from 'drizzle-orm/postgres/codecs';
import { buildShape } from 'drizzle-orm/postgres/shape';
import { type Decoder, Shape } from 'minipg';

// The normalizer below reproduces query-time decode from a replication tuple's RAW TEXT, keyed
// off each column's drizzle `mode`. minipg's tuple decoder natively turns int8 -> bigint and
// date/timestamp/timestamptz -> Date at the OID's DEFAULT js target, discarding both the raw text
// and the mode — so a mode:number/string or mode:date/string column lands on the wrong JS shape,
// and a decoded Date can't be re-serialized back to its source text for a :string column. These
// are the only non-basic OIDs whose native decode isn't already the raw string this bridge
// expects; wiring these into `replication({ types: walTextDecoders })` keeps them as raw text.
const asWalText: Decoder = (buf) => buf.toString('utf8');
export const walTextDecoders: Record<number, Decoder> = {
  20: asWalText, // int8
  1082: asWalText, // date
  1114: asWalText, // timestamp
  1184: asWalText, // timestamptz
};

type ShapeCol = ReturnType<typeof Shape>['$cols'][number];
type CodecNormalize = (value: unknown) => unknown;
type CodecNormalizeArray = (value: string, dimensions: number) => unknown;

// Decodes a replication row's raw-text values into the same JS shapes drizzle's query-time
// decode produces, via the table's Shape spec plus per-column codec fallbacks. Input rows are
// SQL-column-name-keyed (as they arrive off pgoutput); output rows are re-keyed to the source
// table's JS property keys, matching what the drizzle query builder reads and writes.
export function createShapeRowNormalizer(
  sourceTable: PgTable,
): (row: Record<string, unknown>) => Record<string, unknown> {
  const spec = buildShape.fromTableOrView(sourceTable);
  const mapper = Shape(spec);
  const columns = getColumns(sourceTable);

  // Internal lookups stay keyed by SQL name (the input keyspace); output is re-keyed via this
  // SQL-name -> JS-property-key map (an unknown input key falls back to itself).
  const jsKeyBySqlName = new Map<string, string>(
    Object.entries(columns).map(([jsKey, column]) => [(column as Column).name, jsKey]),
  );

  // $cols[].name is the TS property key (e.g. "bigIntCol"); WAL rows are keyed by SQL column
  // name (e.g. "big_int_col") — translate via getColumns(). Keying directly by $cols[].name
  // silently misses every column whose two names differ, leaving only the codec-normalize
  // fallback below.
  const colBySqlName = new Map<string, ShapeCol>(
    mapper.$cols.map((col) => [(columns[col.name as keyof typeof columns] as Column).name, col]),
  );
  const codecBySqlName = new Map<string, string | undefined>(
    Object.values(columns).map((column) => [
      (column as Column).name,
      (column as { codec?: string }).codec,
    ]),
  );
  // .array() dimension count (0 for a plain scalar column) — replication never wire-decodes
  // array columns the way it does the 9 basic scalar OIDs, so every array value (even of an
  // otherwise natively-decoded element type) arrives as raw '{...}' text and needs the
  // codec's array-aware parse+normalize below, not the scalar xform/normalize fallback.
  const dimensionsBySqlName = new Map<string, number>(
    Object.values(columns).map((column) => [
      (column as Column).name,
      (column as { dimensions?: number }).dimensions ?? 0,
    ]),
  );

  return (row) => {
    const out: Record<string, unknown> = {};
    for (const [name, value] of Object.entries(row)) {
      const jsKey = jsKeyBySqlName.get(name) ?? name;
      // minipg's default decoders already produce the final JS value for the 9 basic OIDs
      // (bool/bytea/int2/int4/oid/float4/float8/json/jsonb) — only raw-text strings need
      // further decode here.
      if (value === null || value === undefined || typeof value !== 'string') {
        out[jsKey] = value;
        continue;
      }
      const xform = colBySqlName.get(name)?.xform;
      if (xform) {
        out[jsKey] = xform(value);
        continue;
      }
      // buildShape assumed minipg's own query-time decode of this OID+js-target already
      // produced the final shape (true for query results, not replication) — fall back to the
      // same per-column codec key drizzle would have composed the xform from.
      const codec = codecBySqlName.get(name);
      const codecEntry = codec
        ? (
            minipgCodecs as Record<
              string,
              { normalize?: CodecNormalize; normalizeArray?: CodecNormalizeArray } | undefined
            >
          )[codec]
        : undefined;
      const dimensions = dimensionsBySqlName.get(name) ?? 0;
      if (dimensions > 0 && codecEntry?.normalizeArray) {
        out[jsKey] = codecEntry.normalizeArray(value, dimensions);
        continue;
      }
      out[jsKey] = codecEntry?.normalize ? codecEntry.normalize(value) : value;
    }
    return out;
  };
}
