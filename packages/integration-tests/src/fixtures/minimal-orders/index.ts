import { fileURLToPath } from 'node:url';
import { resolveEventsTable } from 'drizzle-pulse/server';
import { orderSchema, orders, ordersByStatusArgsSchema } from './schema.js';

const migrationsDir = fileURLToPath(new URL('./drizzle', import.meta.url));

export const minimalOrdersFixture = {
  variantName: 'minimal-orders' as const,
  migrationsPath: migrationsDir,
  sourceTable: 'orders' as const,
  eventsTable: resolveEventsTable(orders),
  pulsedTables: [orders],
  cleanupTables: ['orders'] as const,
  publicationName: 'fixture_minimal_orders_pub' as const,
  tables: {
    orders,
  },
  schemas: {
    order: orderSchema,
    ordersByStatusArgs: ordersByStatusArgsSchema,
  },
};
