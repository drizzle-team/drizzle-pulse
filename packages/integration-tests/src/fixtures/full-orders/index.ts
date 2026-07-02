import { fileURLToPath } from 'node:url';
import {
  eventsPublicOrders,
  orderSchema,
  orders,
  ordersByStatusArgsSchema,
  userSchema,
  users,
} from './schema.js';

export type HarnessOrderStatus = 'requested' | 'accepted' | 'completed' | 'cancelled';

const migrationsDir = fileURLToPath(new URL('./drizzle', import.meta.url));

export const fullOrdersFixture = {
  variantName: 'full-orders' as const,
  migrationsPath: migrationsDir,
  sourceTable: 'orders' as const,
  eventsTable: eventsPublicOrders,
  cleanupTables: ['orders', 'users'] as const,
  publicationName: 'fixture_full_orders_pub' as const,
  tables: {
    eventsPublicOrders,
    orders,
    users,
  },
  schemas: {
    order: orderSchema,
    ordersByStatusArgs: ordersByStatusArgsSchema,
    user: userSchema,
  },
};
