import { fileURLToPath } from 'node:url';
import { eventsPublicOrders, orderSchema, orders, ordersByStatusArgsSchema } from './schema.js';

const migrationsDir = fileURLToPath(new URL('./drizzle', import.meta.url));

export const minimalOrdersFixture = {
  variantName: 'minimal-orders' as const,
  migrationsPath: migrationsDir,
  sourceTable: 'orders' as const,
  eventsTable: eventsPublicOrders,
  cleanupTables: ['orders'] as const,
  publicationName: 'fixture_minimal_orders_pub' as const,
  tables: {
    eventsPublicOrders,
    orders,
  },
  schemas: {
    order: orderSchema,
    ordersByStatusArgs: ordersByStatusArgsSchema,
  },
};
