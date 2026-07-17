import { getColumns } from 'drizzle-orm';
import { getTableConfig, type PgColumn, type PgTable } from 'drizzle-orm/pg-core';
import { buildShape } from 'drizzle-orm/postgres/shape';
import type { TableShape } from 'minipg';

// The decode shape handed to minipg's replication start() for one source table: minipg decodes
// each declared column exactly as the same spec would in a query() select — per-column modes
// honored, on new and old (REPLICA IDENTITY) tuples alike — so a WAL tuple lands in the identical
// JS types a query select produces. buildShape keys its spec by the source table's TS property
// names; minipg matches shapes to tuples by SQL column name, so re-key every entry to its SQL
// name. A plain table can't yield a Collect() group, so the spec never carries one.
export function buildTableShape(sourceTable: PgTable): TableShape {
  const spec = buildShape.fromTableOrView(sourceTable);
  const columns = getColumns(sourceTable);
  const { schema, name } = getTableConfig(sourceTable);
  const shape: TableShape['shape'] = {};
  for (const [jsKey, columnSpec] of Object.entries(spec)) {
    shape[(columns[jsKey] as PgColumn).name] = columnSpec as TableShape['shape'][string];
  }
  return { schema: schema ?? 'public', table: name, shape };
}

// SQL column name -> { JS property key, column } for a source table. WAL tuples arrive keyed by
// SQL name (already decoded by the shape above); every in-memory row and all query-builder DML
// use JS property keys — this map bridges both directions: re-keying a decoded WAL row, and
// building a JS-keyed selection for a pk read-back.
export function indexColumnsBySqlName(
  sourceTable: PgTable,
): Map<string, { jsKey: string; column: PgColumn }> {
  return new Map(
    Object.entries(getColumns(sourceTable)).map(([jsKey, column]) => [
      (column as PgColumn).name,
      { jsKey, column: column as PgColumn },
    ]),
  );
}

// Re-key a decoded WAL row from its SQL column names to the source table's JS property keys, the
// keyspace every in-memory row and query-builder select uses. An unmapped key (not a declared
// column) falls back to itself.
export function reKeyToJsProps(
  row: Record<string, unknown>,
  columnsBySqlName: Map<string, { jsKey: string; column: PgColumn }>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [sqlName, value] of Object.entries(row)) {
    out[columnsBySqlName.get(sqlName)?.jsKey ?? sqlName] = value;
  }
  return out;
}
