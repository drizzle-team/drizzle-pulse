import { getColumns } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
import {
  buildEventsTable,
  DEFAULT_EVENTS_SCHEMA,
  getEventsTableName,
} from 'drizzle-pulse/server';

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

// Enum columns are the one column family whose `getSQLType()` returns a bare,
// developer-controlled identifier (the enum's name) rather than a fixed SQL type keyword —
// it must go through the same quoting/schema-qualification as table/column identifiers.
interface ColumnWithEnum {
  enum: { enumName: string; schema: string | undefined };
}

function getEnumInstance(
  column: PgColumn,
): { enumName: string; schema: string | undefined } | null {
  const candidate = (column as unknown as Partial<ColumnWithEnum>).enum;
  return candidate && typeof candidate.enumName === 'string' ? candidate : null;
}

function renderColumnSqlType(column: PgColumn): string {
  const enumInstance = getEnumInstance(column);
  const baseType = enumInstance
    ? `${enumInstance.schema ? `${quoteIdentifier(enumInstance.schema)}.` : ''}${quoteIdentifier(enumInstance.enumName)}`
    : column.getSQLType();
  return baseType + '[]'.repeat(column.dimensions);
}

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

/**
 * Test-only utility: renders `CREATE SCHEMA`/`CREATE TABLE` for an events table strictly
 * from `buildEventsTable`'s output. Production events-table DDL is drizzle-kit's job; this
 * exists so the integration harness can create events tables without hand-mirrored SQL.
 */
export function emitEventsTableDdl(
  sourceTable: PgTable,
  options?: { eventsSchema?: string },
): string[] {
  const eventsSchema = options?.eventsSchema ?? DEFAULT_EVENTS_SCHEMA;
  const tableName = getEventsTableName(sourceTable);
  const eventsTable = buildEventsTable(sourceTable, options);
  const columns = Object.values(getColumns(eventsTable));

  const createSchema = `CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(eventsSchema)}`;
  const createTable = [
    `CREATE TABLE ${quoteIdentifier(eventsSchema)}.${quoteIdentifier(tableName)} (`,
    columns.map((column) => `\t${renderColumnDdl(column)}`).join(',\n'),
    ')',
  ].join('\n');

  return [createSchema, createTable];
}
