import { getColumns, getTableUniqueName, sql } from 'drizzle-orm';
import type { PgTable } from 'drizzle-orm/pg-core';
import { getTableConfig } from 'drizzle-orm/pg-core';
import { drizzle } from 'drizzle-orm/postgres';
import type { Pool } from 'minipg';
import type { PendingWalEvent } from './expose.js';

type DbHandle = ReturnType<typeof drizzle>;
type TxHandle = Parameters<Parameters<DbHandle['transaction']>[0]>[0];

export class PulseStore {
  private readonly db: DbHandle;
  private readonly eventsSchema: string;

  constructor(pool: Pool, eventsSchema: string) {
    this.db = drizzle({ client: pool });
    this.eventsSchema = eventsSchema;
  }

  getDb(): DbHandle {
    return this.db;
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
  ): Promise<number[]> {
    return await this.db.transaction(async (tx) => {
      const snapshots: number[] = [];
      for (const event of events) {
        const snapshot = await this.insertEventRow(
          event.eventsTable,
          this.buildEventRow({
            op: event.op,
            pkColumnName: event.pkColumnName,
            pkValue: event.pkValue,
            row: event.row,
            oldRow: event.oldRow,
          }),
          tx,
        );
        snapshots.push(snapshot);
      }

      await tx.execute(sql`
        insert into ${this.streamTableIdentifier()} (slot_name, last_lsn)
        values (${slotName}, ${commitLsn})
        on conflict (slot_name) do update set last_lsn = excluded.last_lsn
      `);

      return snapshots;
    });
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
        op: 'snapshot',
        pkColumnName,
        pkValue,
        row,
        oldRow: row,
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

  private async insertEventRow(
    table: PgTable,
    values: Record<string, unknown>,
    dbHandle: DbHandle | TxHandle = this.db,
  ): Promise<number> {
    if (Object.keys(values).length === 0) {
      return 0;
    }

    const eventsColumns = getColumns(table);
    const columnKeyBySqlName = new Map(
      Object.entries(eventsColumns).map(([columnKey, column]) => [column.name, columnKey]),
    );

    const insertValues = Object.fromEntries(
      Object.entries(values).map(([columnName, value]) => {
        const columnKey = columnKeyBySqlName.get(columnName);
        if (!columnKey) {
          throw new Error(
            `Missing events column "${columnName}" for table "${getTableUniqueName(table)}"`,
          );
        }

        return [columnKey, value];
      }),
    );

    const snapshotColumnKey = columnKeyBySqlName.get('$snapshot');
    const snapshotColumn = snapshotColumnKey
      ? eventsColumns[snapshotColumnKey as keyof typeof eventsColumns]
      : undefined;
    if (!snapshotColumn) {
      throw new Error(`${getTableUniqueName(table)} missing $snapshot column`);
    }

    const result = await dbHandle
      .insert(table)
      .values(insertValues)
      .returning({ $snapshot: snapshotColumn });

    return (result[0]?.['$snapshot'] as number | undefined) ?? 0;
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
    pkColumnName: string;
    pkValue: unknown;
    row: Record<string, unknown>;
    oldRow: Record<string, unknown> | null;
  }): Record<string, unknown> {
    const { pkColumnName, pkValue, op, row, oldRow } = input;

    const nextRowData =
      op === 'delete' ? null : this.withPrimaryKeyValue(row, pkColumnName, pkValue);
    const nextOldRowData =
      op === 'insert'
        ? nextRowData
        : this.withPrimaryKeyValue(oldRow ?? row, pkColumnName, pkValue);

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
