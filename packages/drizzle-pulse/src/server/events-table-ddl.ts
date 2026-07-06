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

// Only `$timestamp` is ever given a runtime default in this pipeline (defaultNow()) —
// every other column, including cloned ones, has its default stripped by the resolver.
function renderColumnDdl(column: PgColumn): string {
  const parts = [quoteIdentifier(column.name), column.getSQLType()];

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
