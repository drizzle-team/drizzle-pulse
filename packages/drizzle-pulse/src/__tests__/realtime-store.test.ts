import { describe, expect, test } from 'bun:test';
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core';
import { SubscriptionManager } from '../server/realtime-store.js';
import type { ResolvedPulseQuery } from '../types.js';

const ordersTable = pgTable('orders', {
  id: serial('id').primaryKey(),
  status: text('status').notNull(),
  price: integer('price'),
});

function makeQuery(): ResolvedPulseQuery {
  const columns = { id: ordersTable.id, status: ordersTable.status, price: ordersTable.price };
  return {
    table: ordersTable,
    pkColumn: ordersTable.id,
    columns,
    selectedColumns: columns,
    allowedColumnNames: new Set(Object.keys(columns)),
    order: 'asc',
    limit: null,
    argsSchema: null,
    where: null,
    hasTransform: false,
    transformRows: async (rows) => rows,
  };
}

describe('SubscriptionManager idle tracking', () => {
  test('create() stamps lastSeenAt; touch() bumps it forward', () => {
    const manager = new SubscriptionManager();
    const query = makeQuery();

    const t0 = 1_000;
    const subscription = manager.create('client-1', 'orders', query, { userId: 1 }, undefined);
    expect(subscription.lastSeenAt).toBeGreaterThan(0);

    manager.touch('client-1', subscription.id, t0 + 5_000);
    const touched = manager.get('client-1', subscription.id);
    expect(touched?.lastSeenAt).toBe(t0 + 5_000);
  });

  test('touch() on an unknown client/subscription is a no-op', () => {
    const manager = new SubscriptionManager();
    expect(() => manager.touch('no-such-client', 'no-such-sub')).not.toThrow();
    expect(manager.get('no-such-client', 'no-such-sub')).toBeNull();
  });

  test('sweepIdle() evicts only subscriptions past the idle threshold', () => {
    const manager = new SubscriptionManager();
    const query = makeQuery();
    const now = 1_000_000;

    const stale = manager.create('client-1', 'orders', query, { userId: 1 });
    manager.touch('client-1', stale.id, now - 10_000);

    const fresh = manager.create('client-1', 'orders', query, { userId: 1 });
    manager.touch('client-1', fresh.id, now - 1_000);

    const removed = manager.sweepIdle(5_000, now);

    expect(removed).toBe(1);
    expect(manager.get('client-1', stale.id)).toBeNull();
    expect(manager.get('client-1', fresh.id)).not.toBeNull();
  });

  test('sweepIdle() drops the client entry entirely once its last subscription is evicted', () => {
    const manager = new SubscriptionManager();
    const query = makeQuery();
    const now = 1_000_000;

    const stale = manager.create('client-1', 'orders', query, { userId: 1 });
    manager.touch('client-1', stale.id, now - 10_000);

    manager.sweepIdle(5_000, now);

    expect(manager.getClient('client-1')).toBeNull();
  });

  test('delete() removes a single subscription without affecting others for the same client', () => {
    const manager = new SubscriptionManager();
    const query = makeQuery();

    const a = manager.create('client-1', 'orders', query, { userId: 1 });
    const b = manager.create('client-1', 'orders', query, { userId: 1 });

    manager.delete('client-1', a.id);

    expect(manager.get('client-1', a.id)).toBeNull();
    expect(manager.get('client-1', b.id)).not.toBeNull();
  });
});
