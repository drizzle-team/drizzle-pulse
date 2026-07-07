import { fileURLToPath } from 'node:url';
import { resolveEventsTable } from 'drizzle-pulse/server';
import { orders, ordersByStatusArgsSchema } from './schema.js';

const migrationsDir = fileURLToPath(new URL('./drizzle', import.meta.url));

export const minimalOrdersFixture = {
  variantName: 'minimal-orders' as const,
  migrationsPath: migrationsDir,
  eventsTable: resolveEventsTable(orders),
  pulsedTables: [orders],
  cleanupTables: ['orders'] as const,
  publicationName: 'fixture_minimal_orders_pub' as const,
  tables: {
    orders,
  },
  schemas: {
    ordersByStatusArgs: ordersByStatusArgsSchema,
  },
};
