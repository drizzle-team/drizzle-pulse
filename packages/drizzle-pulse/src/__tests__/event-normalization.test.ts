import { describe, expect, test } from 'bun:test';
import { getColumns } from 'drizzle-orm';
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { extractRow } from '../shared/event-normalization.js';

// Fixture: SQL column names differ from query keys (driverId → driver_id in SQL).
const ordersTable = pgTable('orders', {
  id: serial('id').primaryKey(),
  status: text('status').notNull(),
  price: integer('price'),
  driverId: integer('driver_id'),
});

const columns = getColumns(ordersTable);

describe('extractRow', () => {
  test('renames SQL column names to query keys', () => {
    const rawEvent: Record<string, unknown> = {
      id: 1,
      status: 'accepted',
      price: 100,
      driver_id: 42,
    };

    const result = extractRow(rawEvent, columns);

    expect(result).toEqual({
      id: 1,
      status: 'accepted',
      price: 100,
      // SQL name 'driver_id' → query key 'driverId'
      driverId: 42,
    });
  });

  test('$old_ prefix pulls $old_<sqlname> values', () => {
    const rawEvent: Record<string, unknown> = {
      // current row values
      id: 2,
      status: 'completed',
      price: 200,
      driver_id: 7,
      // old row values with $old_ prefix on the SQL column name
      $old_id: 2,
      $old_status: 'accepted',
      $old_price: 150,
      $old_driver_id: 7,
    };

    const result = extractRow(rawEvent, columns, '$old_');

    expect(result).toEqual({
      id: 2,
      status: 'accepted',
      price: 150,
      driverId: 7,
    });
  });

  test('undefined source values are omitted from the result', () => {
    const rawEvent: Record<string, unknown> = {
      id: 3,
      status: 'requested',
      // price and driver_id absent (undefined)
    };

    const result = extractRow(rawEvent, columns);

    expect(result).toEqual({ id: 3, status: 'requested' });
    expect(result).not.toHaveProperty('price');
    expect(result).not.toHaveProperty('driverId');
  });

  test('all-undefined event yields null', () => {
    const rawEvent: Record<string, unknown> = {};

    const result = extractRow(rawEvent, columns);

    expect(result).toBeNull();
  });

  test('null values are included (only undefined is omitted)', () => {
    const rawEvent: Record<string, unknown> = {
      id: 4,
      status: 'requested',
      price: null,
      driver_id: null,
    };

    const result = extractRow(rawEvent, columns);

    expect(result).toEqual({ id: 4, status: 'requested', price: null, driverId: null });
  });
});
