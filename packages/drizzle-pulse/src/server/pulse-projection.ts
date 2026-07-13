import type { ResolvedPulseQuery } from '../types.js';
import { applyProjectionPipeline } from '../shared/projection.js';

// Relocated to shared/projection.ts (value-pure, no drizzle-orm/pg-core imports) so the
// embedded client entrypoint can value-import them without dragging in server/ modules.
// Re-exported here so existing server import paths keep resolving unchanged.
export { addPrimaryKey, applyProjectionPipeline } from '../shared/projection.js';

export async function applyResponsePipeline(
  rows: Record<string, unknown>[],
  pulseQuery: ResolvedPulseQuery,
) {
  const transformedRows = await pulseQuery.transformRows(rows);
  return applyProjectionPipeline(transformedRows, pulseQuery);
}
