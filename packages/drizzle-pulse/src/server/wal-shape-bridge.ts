import { getColumns } from 'drizzle-orm';
import { getTableConfig, type PgColumn, type PgTable } from 'drizzle-orm/pg-core';
import { buildShape } from 'drizzle-orm/postgres/shape';
import type { TableShape } from 'minipg';

// The decode shape handed to minipg's replication start() for one source table: minipg decodes
// each declared column exactly as the same spec would in a query() select — per-column modes
// honored, on new and old (REPLICA IDENTITY) tuples alike — and keys the emitted rows by the
// shape's keys, so WAL rows arrive in the identical JS types AND the identical JS-property
// keyspace a query select produces. buildShape's spec passes through verbatim; `columns` maps
// each property key to its SQL column name where the two differ. A plain table can't yield a
// Collect() group, so the spec never carries one.
export function buildTableShape(sourceTable: PgTable): TableShape {
  const tableColumns = getColumns(sourceTable);
  const { schema, name } = getTableConfig(sourceTable);
  const columns: Record<string, string> = {};
  for (const [jsKey, column] of Object.entries(tableColumns)) {
    const sqlName = (column as PgColumn).name;
    if (sqlName !== jsKey) columns[jsKey] = sqlName;
  }
  return {
    schema: schema ?? 'public',
    table: name,
    columns,
    shape: buildShape.fromTableOrView(sourceTable) as TableShape['shape'],
  };
}
