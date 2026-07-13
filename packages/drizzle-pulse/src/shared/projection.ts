import { getTableUniqueName } from 'drizzle-orm';
import type { ResolvedPulseQuery } from '../types.js';
import { applyColumnFilter, getQueryColumnKey } from './column-filter.js';

// Must stay free of drizzle-orm/pg-core VALUE imports (bare `drizzle-orm` only): the embedded
// client entrypoint value-imports applyProjectionPipeline directly, and platform-imports.test.ts
// enforces purity across everything reachable from there.
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

// Synchronous projection helper for the embedded path — skips async transformRows.
export function applyProjectionPipeline(
  rows: Record<string, unknown>[],
  pulseQuery: Pick<ResolvedPulseQuery, 'pkColumn' | 'selectedColumns' | 'table' | 'columns'>,
): Record<string, unknown>[] {
  return rows
    .map((row) => addPrimaryKey(row, pulseQuery as ResolvedPulseQuery))
    .map((row) => applyColumnFilter(row, pulseQuery.selectedColumns));
}
