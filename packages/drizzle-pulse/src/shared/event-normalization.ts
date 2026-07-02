import type { PgColumn } from 'drizzle-orm/pg-core';

/**
 * Extracts a query-keyed row from an SQL-name-keyed event record.
 *
 * Iterates over `columns` (mapping query key → PgColumn) and copies
 * `rawEvent[keyPrefix + sqlName]` into the result under the query key,
 * skipping keys whose source value is `undefined`.
 *
 * Values arrive already in their JS types: the HTTP-pull path reads the events
 * table via a typed Drizzle select, and the embedded WAL tap receives rows the
 * server has normalized with Drizzle's codecs (see server/wal-normalization.ts).
 * So this only re-keys and prunes — no type coercion happens here.
 *
 * Returns `null` when all column values are absent (all-undefined event).
 */
export function extractRow(
  rawEvent: Record<string, unknown>,
  columns: Record<string, PgColumn>,
  keyPrefix = '',
): Record<string, unknown> | null {
  const row: Record<string, unknown> = {};
  for (const [queryKey, sourceColumn] of Object.entries(columns)) {
    const value = rawEvent[`${keyPrefix}${sourceColumn.name}`];
    if (value !== undefined) {
      row[queryKey] = value;
    }
  }
  return Object.keys(row).length > 0 ? row : null;
}
