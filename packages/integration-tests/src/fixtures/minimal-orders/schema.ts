import { decimal, integer, pgSchema, pgTable, serial, text, timestamp } from 'drizzle-orm/pg-core';
import { createSelectSchema } from 'drizzle-orm/zod';

export const orders = pgTable('orders', {
  id: serial('id').primaryKey(),
  driverId: integer('driver_id'),
  status: text('status', { enum: ['requested', 'accepted', 'completed', 'cancelled'] })
    .notNull()
    .default('requested'),
  price: decimal('price', { mode: 'number' }).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
});

export const realtimeSchema = pgSchema('realtime');

export const eventsPublicOrders = realtimeSchema.table('events_public_orders', {
  id: integer('id').notNull(),
  driverId: integer('driver_id'),
  status: text('status', { enum: ['requested', 'accepted', 'completed', 'cancelled'] }),
  price: decimal('price', { mode: 'number' }),
  createdAt: timestamp('created_at', { withTimezone: true }),
  $old_id: integer('$old_id'),
  $old_driver_id: integer('$old_driver_id'),
  $old_status: text('$old_status', {
    enum: ['requested', 'accepted', 'completed', 'cancelled'],
  }),
  $old_price: decimal('$old_price', { mode: 'number' }),
  $old_created_at: timestamp('$old_created_at', { withTimezone: true }),
  $snapshot: integer('$snapshot').generatedAlwaysAsIdentity(),
  $op: text('$op').notNull(),
  $timestamp: timestamp('$timestamp', { withTimezone: true }).notNull().defaultNow(),
});

export const orderSchema = createSelectSchema(orders);
export const ordersByStatusArgsSchema = orderSchema.pick({ status: true });
