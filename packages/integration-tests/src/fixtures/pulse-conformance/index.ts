import { fileURLToPath } from 'node:url';
import { resolveEventsTable } from 'drizzle-pulse/server';
import { orders } from './schema.js';

const dir = fileURLToPath(new URL('.', import.meta.url));

export const pulseConformanceFixture = {
  // The `out` dir kit-generate.ts wipes + generates the fresh migration into.
  migrationsPath: `${dir}drizzle`,
  eventsTable: resolveEventsTable(orders),
  // MUST be the name kit's synthesis hardcodes (drizzle.ts), not a fixture-specific name —
  // conformance.test.ts's expose() call must subscribe to the publication kit actually generates.
  publicationName: 'drizzle_pulse' as const,
};
