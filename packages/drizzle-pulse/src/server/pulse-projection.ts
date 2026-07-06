import { getTableUniqueName } from 'drizzle-orm';
import type { PgTable } from 'drizzle-orm/pg-core';
import type { ResolvedPulseQuery } from '../types.js';
import { applyColumnFilter } from './pulse-types.js';

// Kept in its own module (bare `drizzle-orm` value imports only, `drizzle-orm/pg-core`
// type-only) because the embedded client entrypoint value-imports applyProjectionPipeline
// directly — platform-imports.test.ts walks every value import reachable from there, so
// this file must stay free of drizzle-orm/pg-core VALUE imports (the registry-construction
// logic in pulse-registry.ts, which needs getTableConfig, is not reachable from embedded).
function getQualifiedTableName(table: PgTable) {
  return getTableUniqueName(table);
}

export function addPrimaryKey(row: Record<string, unknown>, pulseQuery: ResolvedPulseQuery) {
  const pkValue = row[pulseQuery.pkColumn.name];
  if (pkValue === undefined) {
    throw new Error(
      `Primary key column "${pulseQuery.pkColumn.name}" on "${getQualifiedTableName(pulseQuery.table)}" is missing`,
    );
  }

  return { ...row, $pk: pkValue };
}

export async function applyResponsePipeline(
  rows: Record<string, unknown>[],
  pulseQuery: ResolvedPulseQuery,
) {
  const transformedRows = await pulseQuery.transformRows(rows);
  return applyProjectionPipeline(transformedRows, pulseQuery);
}

// Synchronous projection helper for the embedded path — skips async transformRows.
export function applyProjectionPipeline(
  rows: Record<string, unknown>[],
  pulseQuery: Pick<ResolvedPulseQuery, 'pkColumn' | 'selectedColumns' | 'table'>,
): Record<string, unknown>[] {
  return rows
    .map((row) => addPrimaryKey(row, pulseQuery as ResolvedPulseQuery))
    .map((row) => applyColumnFilter(row, pulseQuery.selectedColumns));
}
