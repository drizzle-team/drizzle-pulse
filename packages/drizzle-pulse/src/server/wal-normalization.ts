import { type Column, getColumns } from 'drizzle-orm';
import { nodePgCodecs } from 'drizzle-orm/node-postgres';
import type { PgTable } from 'drizzle-orm/pg-core';

type CodecNormalize = (value: unknown) => unknown;

// pg-logical-replication delivers WAL column values in Postgres' text form — the same
// wire form Drizzle's from-DB codecs consume on a text-cast SELECT. Reusing those codec
// `normalize` functions makes WAL-tap rows land in the exact JS shapes a baseline SELECT
// produces (numbers, BigInts, Dates, point/line/geometry tuples, vectors), instead of a
// hand-maintained per-dataType switch that lagged behind (missed bigint:number, left
// point/line/geometry/vector as raw strings). This lives server-side because the codec
// module is a driver-specific `drizzle-orm/*` subpath the platform-agnostic embedded
// entrypoint may not import (see platform-imports.test.ts).
function normalizeWalValue(value: unknown, column: Column | undefined): unknown {
  if (value === null || value === undefined || typeof value !== 'string' || !column) {
    return value;
  }
  const codec = (column as { codec?: string }).codec;
  const normalize = codec
    ? (nodePgCodecs as Record<string, { normalize?: CodecNormalize } | undefined>)[codec]?.normalize
    : undefined;
  return normalize ? normalize(value) : value;
}

export function createWalRowNormalizer(
  sourceTable: PgTable,
): (row: Record<string, unknown>) => Record<string, unknown> {
  const columnsBySqlName: Record<string, Column> = {};
  for (const column of Object.values(getColumns(sourceTable))) {
    columnsBySqlName[column.name] = column;
  }
  return (row) => {
    const normalized: Record<string, unknown> = {};
    for (const [sqlName, value] of Object.entries(row)) {
      normalized[sqlName] = normalizeWalValue(value, columnsBySqlName[sqlName]);
    }
    return normalized;
  };
}
