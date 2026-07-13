import type { WalTapPayload } from '../../server/wal-event-emitter.js';
import { extractRow } from '../../shared/event-normalization.js';
import { evaluateCondition } from '../../shared/filter-ast.js';
import { applyProjectionPipeline } from '../../shared/projection.js';
import type { PulseEvent } from '../../shared/pulse-events.js';
import type { ResolvedPulseQuery } from '../../types.js';

// Must stay free of drizzle-orm/pg-core VALUE imports (bare `drizzle-orm` only): both
// createPulseClient's collections and createPulseEvents value-import buildTapEvent, and
// platform-imports.test.ts enforces purity across the embedded entrypoint's import graph.

export type TapRow = Record<string, unknown> & { $pk: unknown };

/**
 * Builds a `PulseEvent` from a raw WAL tap payload, or `null` when the row (before and after
 * the change) falls outside `resolved.where` — the same auth-scoped filter the SQL path
 * enforces, evaluated here in-memory instead of by Postgres.
 */
export function buildTapEvent(
  payload: WalTapPayload,
  resolved: ResolvedPulseQuery,
): PulseEvent<TapRow> | null {
  // Delete payloads carry an empty rowData object (not null); extractRow returns null when
  // every column is absent, so the delete case falls out of this call for free.
  const newRow = extractRow(payload.rowData, resolved.columns);
  const oldRow = payload.oldRowData ? extractRow(payload.oldRowData, resolved.columns) : null;

  const matchesNew = newRow ? evaluateCondition(resolved.where, newRow) : false;
  const matchesOld = oldRow ? evaluateCondition(resolved.where, oldRow) : false;

  if (payload.operation === 'insert') {
    if (!matchesNew || !newRow) return null;
    const row = applyProjectionPipeline([newRow], resolved)[0] as TapRow;
    return { op: 'insert', row, pk: row.$pk };
  }

  if (payload.operation === 'update') {
    if (!matchesNew && !matchesOld) return null;
    const projectedNew = newRow ? (applyProjectionPipeline([newRow], resolved)[0] as TapRow) : null;
    const projectedOld = oldRow ? (applyProjectionPipeline([oldRow], resolved)[0] as TapRow) : null;
    const row = projectedNew ?? projectedOld;
    if (!row) return null;
    return {
      op: 'update',
      row,
      old_row: projectedOld ?? {},
      pk: row.$pk,
      matchesNew,
      matchesOld,
    };
  }

  // delete
  if (!matchesOld || !oldRow) return null;
  const projectedOld = applyProjectionPipeline([oldRow], resolved)[0] as TapRow;
  return { op: 'delete', old_row: projectedOld, pk: projectedOld.$pk, matchesOld };
}
