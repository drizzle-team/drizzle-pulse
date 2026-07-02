import { fileURLToPath } from 'node:url';
import type { IntegrationTestFixture } from '../../helpers/test-harness.js';
import { eventsPublicPgDataTypes, pgDataTypes, pgDataTypesRowSchema } from './schema.js';

const migrationsDir = fileURLToPath(new URL('./drizzle', import.meta.url));

export const pgDataTypesFixture = {
  variantName: 'pg-data-types',
  migrationsPath: migrationsDir,
  sourceTable: 'pg_data_types',
  eventsTable: eventsPublicPgDataTypes,
  cleanupTables: ['pg_data_types'] as const,
  publicationName: 'fixture_pg_data_types_pub',
  tables: {
    eventsPublicPgDataTypes,
    pgDataTypes,
  },
  schemas: {
    row: pgDataTypesRowSchema,
  },
} satisfies IntegrationTestFixture;
