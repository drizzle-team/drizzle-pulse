import {
  decimal,
  index,
  integer,
  pgSchema,
  pgTable,
  serial,
  text,
  timestamp,
} from 'drizzle-orm/pg-core';
import { createSelectSchema } from 'drizzle-orm/zod';

export const users = pgTable('users', {
  id: serial('id').primaryKey(),
  username: text('username').notNull().unique(),
  passwordHash: text('password_hash').notNull(),
});

export const orders = pgTable(
  'orders',
  {
    id: serial('id').primaryKey(),
    driverId: integer('driver_id').references(() => users.id, { onDelete: 'cascade' }),
    pickup: text('pickup').notNull(),
    dropoff: text('dropoff').notNull(),
    price: decimal('price', { mode: 'number' }).notNull(),
    status: text('status', { enum: ['requested', 'accepted', 'completed', 'cancelled'] })
      .notNull()
      .default('requested'),
    acceptedAt: timestamp('accepted_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    index('idx_orders_driver_id').on(table.driverId),
    index('idx_orders_status').on(table.status),
  ],
);

export const realtimeSchema = pgSchema('realtime');

export const eventsPublicOrders = realtimeSchema.table('events_public_orders', {
  id: integer('id').notNull(),
  driverId: integer('driver_id'),
  pickup: text('pickup'),
  dropoff: text('dropoff'),
  price: decimal('price', { mode: 'number' }),
  status: text('status', { enum: ['requested', 'accepted', 'completed', 'cancelled'] }),
  acceptedAt: timestamp('accepted_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }),
  updatedAt: timestamp('updated_at', { withTimezone: true }),
  $old_id: integer('$old_id'),
  $old_driver_id: integer('$old_driver_id'),
  $old_pickup: text('$old_pickup'),
  $old_dropoff: text('$old_dropoff'),
  $old_price: decimal('$old_price', { mode: 'number' }),
  $old_status: text('$old_status', {
    enum: ['requested', 'accepted', 'completed', 'cancelled'],
  }),
  $old_accepted_at: timestamp('$old_accepted_at', { withTimezone: true }),
  $old_created_at: timestamp('$old_created_at', { withTimezone: true }),
  $old_updated_at: timestamp('$old_updated_at', { withTimezone: true }),
  $snapshot: integer('$snapshot').generatedAlwaysAsIdentity(),
  $op: text('$op').notNull(),
  $timestamp: timestamp('$timestamp', { withTimezone: true }).notNull().defaultNow(),
});

export const userSchema = createSelectSchema(users);
export const orderSchema = createSelectSchema(orders);
export const ordersByStatusArgsSchema = orderSchema.pick({ status: true });
