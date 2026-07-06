import { getColumns } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
import {
  DEFAULT_EVENTS_SCHEMA,
  getEventsTableName,
  resolveEventsTable,
} from './events-table-resolver.js';

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

// Enum columns are the one column family whose `getSQLType()` returns a bare,
// developer-controlled identifier (the enum's name) rather than a fixed SQL type keyword
// — unlike every other type keyword, it must go through the same quoting/schema-
// qualification as table/column identifiers. Duck-typed (rather than an `instanceof`
// check against an imported enum-column class) so it works across duplicated drizzle-orm
// package copies, matching the resolver's existing approach to internal drizzle-orm shapes.
interface ColumnWithEnum {
  enum: { enumName: string; schema: string | undefined };
}

function getEnumInstance(
  column: PgColumn,
): { enumName: string; schema: string | undefined } | null {
  const candidate = (column as unknown as Partial<ColumnWithEnum>).enum;
  return candidate && typeof candidate.enumName === 'string' ? candidate : null;
}

// Renders the column's base SQL type, quoting/schema-qualifying enum type names (WR-01)
// and appending one `[]` per array dimension (`column.dimensions`) so array-typed source
// columns (CR-01) don't lose their dimensionality in the emitted DDL.
function renderColumnSqlType(column: PgColumn): string {
  const enumInstance = getEnumInstance(column);
  const baseType = enumInstance
    ? `${enumInstance.schema ? `${quoteIdentifier(enumInstance.schema)}.` : ''}${quoteIdentifier(enumInstance.enumName)}`
    : column.getSQLType();
  return baseType + '[]'.repeat(column.dimensions);
}

// Only `$timestamp` is ever given a runtime default in this pipeline (defaultNow()) —
// every other column, including cloned ones, has its default stripped by the resolver.
function renderColumnDdl(column: PgColumn): string {
  const parts = [quoteIdentifier(column.name), renderColumnSqlType(column)];

  if (column.generatedIdentity) {
    parts.push('GENERATED ALWAYS AS IDENTITY');
    return parts.join(' ');
  }

  if (column.name === '$timestamp') {
    parts.push('DEFAULT now()');
  }
  if (column.notNull) {
    parts.push('NOT NULL');
  }

  return parts.join(' ');
}

export function emitEventsTableDdl(
  sourceTable: PgTable,
  options?: { eventsSchema?: string },
): string[] {
  const eventsSchema = options?.eventsSchema ?? DEFAULT_EVENTS_SCHEMA;
  const tableName = getEventsTableName(sourceTable);
  const eventsTable = resolveEventsTable(sourceTable, options);
  const columns = Object.values(getColumns(eventsTable));

  const createSchema = `CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(eventsSchema)}`;
  const createTable = [
    `CREATE TABLE ${quoteIdentifier(eventsSchema)}.${quoteIdentifier(tableName)} (`,
    columns.map((column) => `\t${renderColumnDdl(column)}`).join(',\n'),
    ')',
  ].join('\n');

  return [createSchema, createTable];
}
