import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { pulse } from 'drizzle-pulse';

export const orders = pgTable('orders', {
  id: serial('id').primaryKey(),
  driverId: integer('driver_id'),
  status: text('status').notNull().default('requested'),
});

export const ordersPulse = pulse(orders);
