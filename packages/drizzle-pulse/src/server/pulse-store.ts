import { sql } from 'drizzle-orm';
import type { PgTable } from 'drizzle-orm/pg-core';
import { getTableConfig } from 'drizzle-orm/pg-core';
import { drizzle } from 'drizzle-orm/postgres';
import { type Connection, createPool, type Pool } from 'minipg';
import type { PendingWalEvent } from './expose.js';

type DbHandle = ReturnType<typeof drizzle>;
type TxHandle = Parameters<Parameters<DbHandle['transaction']>[0]>[0];

export class PulseStore {
  private readonly pool: Pool;
  private readonly db: DbHandle;
  private readonly eventsSchema: string;

  constructor(databaseUrl: string, eventsSchema: string) {
    this.pool = createPool(databaseUrl);
    this.db = drizzle({ client: this.pool });
    this.eventsSchema = eventsSchema;
  }

  getDb(): DbHandle {
    return this.db;
  }

  async end(): Promise<void> {
    await this.pool.end();
  }

  checkout(): Promise<{ client: Connection; release: () => void }> {
    return this.pool.connect();
  }

  private streamTableIdentifier() {
    return sql`${sql.identifier(this.eventsSchema)}.${sql.identifier('pulse_stream')}`;
  }

  async getStreamWatermark(slotName: string): Promise<string | null> {
    const result = await this.db.execute<{ last_lsn: string }>(sql`
      select last_lsn
      from ${this.streamTableIdentifier()}
      where slot_name = ${slotName}
    `);
    return result.rows[0]?.last_lsn ?? null;
  }

  // Transaction-atomic persist for a WAL commit: every buffered row event inserts, then the
  // durable dedupe watermark upserts, all in one db.transaction — rows and watermark move
  // together, and the caller (expose.ts) acks only after this resolves. An empty `events` array
  // still upserts the watermark (a data-less commit advances the dedupe floor).
  async persistCommit(
    events: PendingWalEvent[],
    slotName: string,
    commitLsn: string,
  ): Promise<void> {
    await this.db.transaction(async (tx) => {
      for (const event of events) {
        await this.insertEventRow(
          event.eventsTable,
          this.buildEventRow({
            op: event.op,
            pkKey: event.pkKey,
            pkValue: event.pkValue,
            row: event.row,
            oldRow: event.oldRow,
          }),
          tx,
        );
      }

      await tx.execute(sql`
        insert into ${this.streamTableIdentifier()} (slot_name, last_lsn)
        values (${slotName}, ${commitLsn})
        on conflict (slot_name) do update set last_lsn = excluded.last_lsn
      `);
    });
  }

  async createBaselineSnapshot(
    table: PgTable,
    pkKey: string,
    baselineRow: Record<string, unknown> | null,
    dbHandle: DbHandle | TxHandle = this.db,
  ): Promise<void> {
    const eventsTableConfig = getTableConfig(table);
    const eventsTableIdentifier = sql`${sql.identifier(eventsTableConfig.schema ?? 'public')}.${sql.identifier(eventsTableConfig.name)}`;
    const existingRows = await dbHandle.execute<{ has_rows: boolean }>(sql`
      select exists(
        select 1
        from ${eventsTableIdentifier}
      ) as has_rows
    `);
    if (existingRows.rows[0]?.has_rows) return;

    const row = baselineRow;
    if (!row) return;
    const pkValue = row[pkKey];
    if (pkValue === undefined) {
      throw new Error(`Baseline snapshot missing primary key ${pkKey}`);
    }

    await this.insertEventRow(
      table,
      this.buildEventRow({
        op: 'snapshot',
        pkKey,
        pkValue,
        row,
        oldRow: row,
      }),
      dbHandle,
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

  private async insertEventRow(
    table: PgTable,
    values: Record<string, unknown>,
    dbHandle: DbHandle | TxHandle = this.db,
  ): Promise<void> {
    if (Object.keys(values).length === 0) {
      return;
    }

    await dbHandle.insert(table).values(values);
  }

  private toOldRowValues(row: Record<string, unknown>) {
    return Object.fromEntries(Object.entries(row).map(([key, value]) => [`$old_${key}`, value]));
  }

  // Consolidated builder for all four event-row shapes (insert/update/delete/snapshot),
  // driven by `op` rather than which fields are present — replaces the prior 3-branch union.
  // `row` is always passed (delete's is `{}` per PendingWalEvent's tap contract, but is
  // deliberately ignored below since a delete's persisted row is old-row-derived, matching the
  // original persistDeleteEvent behavior verbatim).
  private buildEventRow(input: {
    op: 'insert' | 'update' | 'delete' | 'snapshot';
    pkKey: string;
    pkValue: unknown;
    row: Record<string, unknown>;
    oldRow: Record<string, unknown> | null;
  }): Record<string, unknown> {
    const { pkKey, pkValue, op, row, oldRow } = input;

    const nextRowData = op === 'delete' ? null : this.withPrimaryKeyValue(row, pkKey, pkValue);
    const nextOldRowData =
      op === 'insert' ? nextRowData : this.withPrimaryKeyValue(oldRow ?? row, pkKey, pkValue);

    return {
      [pkKey]: pkValue,
      ...(nextRowData ?? nextOldRowData),
      ...this.toOldRowValues(nextOldRowData ?? {}),
      $op: op,
    };
  }

  private withPrimaryKeyValue(
    row: Record<string, unknown>,
    pkKey: string,
    pkValue: unknown,
  ): Record<string, unknown> {
    return row[pkKey] === undefined ? { ...row, [pkKey]: pkValue } : row;
  }
}
