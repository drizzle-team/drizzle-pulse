import * as crypto from 'node:crypto';
import { getColumns, getTableUniqueName, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { PgTable } from 'drizzle-orm/pg-core';
import { getTableConfig } from 'drizzle-orm/pg-core';
import type { Pool } from 'pg';
import type { PulseAuthContext, ResolvedPulseQuery } from '../types.js';

export interface Subscription {
  id: string;
  clientId: string;
  queryName: string;
  query: ResolvedPulseQuery;
  auth: PulseAuthContext;
  rangeStart: unknown | null;
  rangeEnd: unknown | null;
  hasMore: boolean;
  /**
   * Updated on every successful pull() for this subscription; the idle sweep uses it to
   * evict subscriptions abandoned by clients that never call unsubscribe (e.g. a closed tab).
   */
  lastSeenAt: number;
}

type ClientSubscriptions = {
  clientId: string;
  subscriptions: Map<string, Subscription>;
};

export class SubscriptionManager {
  private readonly clients = new Map<string, ClientSubscriptions>();

  create(
    clientId: string,
    queryName: string,
    query: ResolvedPulseQuery,
    auth: PulseAuthContext,
    id?: string,
  ): Subscription {
    const client = this.getOrCreateClient(clientId);
    const subscription: Subscription = {
      id: id ?? crypto.randomUUID(),
      clientId,
      queryName,
      query,
      auth,
      rangeStart: null,
      rangeEnd: null,
      hasMore: false,
      lastSeenAt: Date.now(),
    };

    client.subscriptions.set(subscription.id, subscription);
    return subscription;
  }

  getClient(clientId: string): ClientSubscriptions | null {
    return this.clients.get(clientId) ?? null;
  }

  get(clientId: string, id: string): Subscription | null {
    return this.clients.get(clientId)?.subscriptions.get(id) ?? null;
  }

  update(
    clientId: string,
    id: string,
    updates: Partial<
      Pick<Subscription, 'rangeStart' | 'rangeEnd' | 'hasMore' | 'query' | 'auth' | 'queryName'>
    >,
  ): void {
    const current = this.get(clientId, id);
    if (!current) return;

    const client = this.clients.get(clientId);
    if (!client) return;

    client.subscriptions.set(id, { ...current, ...updates });
  }

  delete(clientId: string, id: string): void {
    const client = this.clients.get(clientId);
    if (!client) {
      return;
    }

    client.subscriptions.delete(id);
    if (client.subscriptions.size === 0) {
      this.clients.delete(clientId);
    }
  }

  /**
   * Marks a subscription as recently active — called from pull() on every request that
   * resolves to a real (owned) subscription, so the sweep only evicts truly idle ones.
   */
  touch(clientId: string, id: string, now = Date.now()): void {
    const client = this.clients.get(clientId);
    const current = client?.subscriptions.get(id);
    if (!client || !current) return;

    client.subscriptions.set(id, { ...current, lastSeenAt: now });
  }

  /**
   * Evicts every subscription whose lastSeenAt is older than maxIdleMs, freeing memory
   * held by clients that disconnected without calling unsubscribe. Returns the count removed.
   */
  sweepIdle(maxIdleMs: number, now = Date.now()): number {
    let removed = 0;
    for (const [clientId, client] of this.clients) {
      for (const [id, subscription] of client.subscriptions) {
        if (now - subscription.lastSeenAt > maxIdleMs) {
          client.subscriptions.delete(id);
          removed++;
        }
      }
      if (client.subscriptions.size === 0) {
        this.clients.delete(clientId);
      }
    }
    return removed;
  }

  private getOrCreateClient(clientId: string): ClientSubscriptions {
    const existing = this.clients.get(clientId);
    if (existing) {
      return existing;
    }

    const created: ClientSubscriptions = {
      clientId,
      subscriptions: new Map(),
    };
    this.clients.set(clientId, created);
    return created;
  }
}

export class RealtimeService {
  private readonly db: ReturnType<typeof drizzle>;

  constructor(pool: Pool) {
    this.db = drizzle({ client: pool });
  }

  getDb(): ReturnType<typeof drizzle> {
    return this.db;
  }

  async createBaselineSnapshot(
    table: PgTable,
    pkColumnName: string,
    baselineRow: Record<string, unknown> | null,
  ): Promise<number> {
    const eventsTableConfig = getTableConfig(table);
    const eventsTableIdentifier = sql`${sql.identifier(eventsTableConfig.schema ?? 'public')}.${sql.identifier(eventsTableConfig.name)}`;
    const existingRows = await this.db.execute<{ has_rows: boolean }>(sql`
      select exists(
        select 1
        from ${eventsTableIdentifier}
      ) as has_rows
    `);
    if (existingRows.rows[0]?.has_rows) return 0;

    const row = baselineRow;
    if (!row) return 0;
    const pkValue = row[pkColumnName];
    if (pkValue === undefined) {
      throw new Error(`Baseline snapshot missing primary key ${pkColumnName}`);
    }

    return await this.insertEventRow(
      table,
      this.buildEventRow({
        pkColumnName,
        pkValue,
        rowData: row,
        oldRowData: row,
        op: 'snapshot',
      }),
    );
  }

  async persistInsertEvent(
    table: PgTable,
    pkColumnName: string,
    pkValue: unknown,
    rowData: Record<string, unknown>,
  ): Promise<number> {
    return await this.insertEventRow(
      table,
      this.buildEventRow({
        pkColumnName,
        pkValue,
        rowData,
        op: 'insert',
      }),
    );
  }

  async persistUpdateEvent(
    table: PgTable,
    pkColumnName: string,
    pkValue: unknown,
    rowData: Record<string, unknown>,
    oldRowData: Record<string, unknown>,
  ): Promise<number> {
    return await this.insertEventRow(
      table,
      this.buildEventRow({
        pkColumnName,
        pkValue,
        rowData,
        oldRowData,
        op: 'update',
      }),
    );
  }

  async persistDeleteEvent(
    table: PgTable,
    pkColumnName: string,
    pkValue: unknown,
    oldRowData: Record<string, unknown>,
  ): Promise<number> {
    return await this.insertEventRow(
      table,
      this.buildEventRow({
        pkColumnName,
        pkValue,
        oldRowData,
        op: 'delete',
      }),
    );
  }

  async getLatestSnapshot(table: PgTable): Promise<number> {
    const eventsTableConfig = getTableConfig(table);
    const eventsTableIdentifier = sql`${sql.identifier(eventsTableConfig.schema ?? 'public')}.${sql.identifier(eventsTableConfig.name)}`;
    const result = await this.db.execute<{ snapshot: number | null }>(sql`
      select max("$snapshot")::int as snapshot
      from ${eventsTableIdentifier}
    `);
    return result.rows[0]?.snapshot ?? 0;
  }

  private async insertEventRow(table: PgTable, values: Record<string, unknown>): Promise<number> {
    if (Object.keys(values).length === 0) {
      return 0;
    }

    const eventsColumns = getColumns(table);
    const insertValues = Object.fromEntries(
      Object.entries(values).map(([columnName, value]) => {
        const matchingEntry = Object.entries(eventsColumns).find(
          ([, candidate]) => candidate.name === columnName,
        );
        if (!matchingEntry) {
          throw new Error(
            `Missing events column "${columnName}" for table "${getTableUniqueName(table)}"`,
          );
        }

        const [columnKey] = matchingEntry;

        return [columnKey, value];
      }),
    );

    const snapshotEntry = Object.entries(eventsColumns).find(([, col]) => col.name === '$snapshot');
    if (!snapshotEntry) {
      throw new Error(`${getTableUniqueName(table)} missing $snapshot column`);
    }
    const [, snapshotColumn] = snapshotEntry;

    const result = await this.db
      .insert(table)
      .values(insertValues)
      .returning({ $snapshot: snapshotColumn });

    return (result[0]?.$snapshot as number | undefined) ?? 0;
  }

  private toOldRowValues(row: Record<string, unknown>) {
    return Object.fromEntries(Object.entries(row).map(([key, value]) => [`$old_${key}`, value]));
  }

  private buildEventRow(
    input:
      | {
          pkColumnName: string;
          pkValue: unknown;
          rowData: Record<string, unknown>;
          op: 'insert';
        }
      | {
          pkColumnName: string;
          pkValue: unknown;
          rowData: Record<string, unknown>;
          oldRowData: Record<string, unknown>;
          op: 'update' | 'snapshot';
        }
      | {
          pkColumnName: string;
          pkValue: unknown;
          oldRowData: Record<string, unknown>;
          op: 'delete';
        },
  ): Record<string, unknown> {
    const { pkColumnName, pkValue, op } = input;
    const nextRowData =
      'rowData' in input ? this.withPrimaryKeyValue(input.rowData, pkColumnName, pkValue) : null;
    const nextOldRowData =
      'oldRowData' in input
        ? this.withPrimaryKeyValue(input.oldRowData, pkColumnName, pkValue)
        : nextRowData;

    return {
      [pkColumnName]: pkValue,
      ...(nextRowData ?? nextOldRowData),
      ...this.toOldRowValues(nextOldRowData ?? {}),
      $op: op,
    };
  }

  private withPrimaryKeyValue(
    row: Record<string, unknown>,
    pkColumnName: string,
    pkValue: unknown,
  ): Record<string, unknown> {
    return row[pkColumnName] === undefined ? { ...row, [pkColumnName]: pkValue } : row;
  }
}
