import type { PendingWalEvent } from '../../server/pulse-runtime.js';
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
 * Builds a `PulseEvent` from a decoded WAL event, or `null` when the event should not be
 * delivered at all. Insert is gated on `query.where` as always. Update/delete are gated too,
 * but only when `event.oldRowComplete` makes that evaluable (a full old tuple — RID FULL, or
 * pull:true which always forces it): if the old tuple fully misses `where` and the new row (or
 * absence of one, for deletes) doesn't match either, the event is suppressed. When the old tuple
 * is absent or a key-only degradation (RID DEFAULT under pull:false), `where` can't be evaluated
 * against it, so the event is still delivered for membership correctness, but a non-matching new
 * row is redacted to pk-only rather than leaking out-of-scope column data to every subscriber.
 */
export function buildTapEvent(
  event: PendingWalEvent,
  query: ResolvedPulseQuery,
): PulseEvent<TapRow> | null {
  // Delete events carry an empty `row` object (not null); extractRow returns null when
  // every column is absent, so the delete case falls out of this call for free.
  const newRow = extractRow(event.row, query.columns);
  const oldRow = event.oldRow ? extractRow(event.oldRow, query.columns) : null;

  const matchesNew = newRow ? evaluateCondition(query.where, newRow) : false;
  const oldEvaluable = event.oldRowComplete && oldRow !== null;
  const matchesOld = oldEvaluable && oldRow ? evaluateCondition(query.where, oldRow) : false;

  if (event.op === 'insert') {
    if (!matchesNew || !newRow) return null;
    const row = applyProjectionPipeline([newRow], query)[0] as TapRow;
    return { op: 'insert', row, pk: row.$pk };
  }

  if (event.op === 'update') {
    const projectedNew = newRow ? (applyProjectionPipeline([newRow], query)[0] as TapRow) : null;
    const projectedOld = oldRow ? (applyProjectionPipeline([oldRow], query)[0] as TapRow) : null;
    const fallback = projectedNew ?? projectedOld;
    if (!fallback) return null;
    // Full old tuple and neither side matches: the row was never visible to this subscriber —
    // suppress entirely (restores pre-phase behavior for the evaluable case).
    if (!matchesNew && oldEvaluable && !matchesOld) return null;
    // Otherwise deliver: matchesNew true is the normal in-scope update; matchesNew false with
    // matchesOld true means the row left the filter (needed for membership removal); matchesNew false with
    // old tuple non-evaluable can't be classified, so deliver defensively but redact the row.
    const row = matchesNew ? (projectedNew as TapRow) : ({ $pk: fallback.$pk } as TapRow);
    return {
      op: 'update',
      row,
      old_row: projectedOld ?? {},
      pk: fallback.$pk,
      matchesNew,
    };
  }

  // delete
  if (!oldRow) return null;
  const projectedOld = applyProjectionPipeline([oldRow], query)[0] as TapRow;
  // Full old tuple: restore the pre-phase matchesOld gate. Key-only/pk-only old tuple (non-full
  // identity under pull:false): can't evaluate where, so deliver — projectedOld is already
  // pk-only in that case since the raw old tuple never carried anything else.
  if (oldEvaluable && !matchesOld) return null;
  return { op: 'delete', old_row: projectedOld, pk: projectedOld.$pk };
}
