import { fileURLToPath } from 'node:url';
import { resolveEventsTable } from 'drizzle-pulse/server';
import { orderSchema, orders, ordersByStatusArgsSchema, userSchema, users } from './schema.js';

export type HarnessOrderStatus = 'requested' | 'accepted' | 'completed' | 'cancelled';

const migrationsDir = fileURLToPath(new URL('./drizzle', import.meta.url));

export const fullOrdersFixture = {
  variantName: 'full-orders' as const,
  migrationsPath: migrationsDir,
  sourceTable: 'orders' as const,
  eventsTable: resolveEventsTable(orders),
  pulsedTables: [orders],
  cleanupTables: ['orders', 'users'] as const,
  publicationName: 'fixture_full_orders_pub' as const,
  tables: {
    orders,
    users,
  },
  schemas: {
    order: orderSchema,
    ordersByStatusArgs: ordersByStatusArgsSchema,
    user: userSchema,
  },
};
