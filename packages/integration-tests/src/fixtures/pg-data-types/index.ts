import { fileURLToPath } from 'node:url';
import { resolveEventsTable } from 'drizzle-pulse/server';
import type { IntegrationTestFixture } from '../../helpers/test-harness.js';
import { pgDataTypes } from './schema.js';

const migrationsDir = fileURLToPath(new URL('./drizzle', import.meta.url));

export const pgDataTypesFixture = {
  variantName: 'pg-data-types',
  migrationsPath: migrationsDir,
  eventsTable: resolveEventsTable(pgDataTypes),
  pulsedTables: [pgDataTypes],
  cleanupTables: ['pg_data_types'] as const,
  publicationName: 'fixture_pg_data_types_pub',
  tables: {
    pgDataTypes,
  },
  schemas: {},
} satisfies IntegrationTestFixture;
