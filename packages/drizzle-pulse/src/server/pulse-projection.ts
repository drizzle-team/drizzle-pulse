import { getTableUniqueName } from 'drizzle-orm';
import type { ResolvedPulseQuery } from '../types.js';
import { applyColumnFilter, getQueryColumnKey } from './pulse-types.js';

// Kept in its own module (bare `drizzle-orm` value imports only, no `drizzle-orm/pg-core`
// value imports) because the embedded client entrypoint value-imports applyProjectionPipeline
// directly — platform-imports.test.ts walks every value import reachable from there, so
// this file must stay free of drizzle-orm/pg-core VALUE imports (the registry-construction
// logic in pulse-registry.ts, which needs getTableConfig, is not reachable from embedded).
export function addPrimaryKey(row: Record<string, unknown>, pulseQuery: ResolvedPulseQuery) {
  // `row` is SELECT-shaped (keyed by JS property name), which diverges from the PK
  // column's own SQL name whenever a table declares e.g. `orderId: serial('order_id')` —
  // resolve the JS query key once and index by that instead.
  const pkQueryKey =
    getQueryColumnKey(pulseQuery.columns, pulseQuery.pkColumn) ?? pulseQuery.pkColumn.name;
  const pkValue = row[pkQueryKey];
  if (pkValue === undefined) {
    throw new Error(
      `Primary key column "${pulseQuery.pkColumn.name}" on "${getTableUniqueName(pulseQuery.table)}" is missing`,
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
  pulseQuery: Pick<ResolvedPulseQuery, 'pkColumn' | 'selectedColumns' | 'table' | 'columns'>,
): Record<string, unknown>[] {
  return rows
    .map((row) => addPrimaryKey(row, pulseQuery as ResolvedPulseQuery))
    .map((row) => applyColumnFilter(row, pulseQuery.selectedColumns));
}
