import { getColumns, getTableUniqueName, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import type { PgTable } from 'drizzle-orm/pg-core';
import { getTableConfig } from 'drizzle-orm/pg-core';
import type { Pool } from 'pg';

export class PulseStore {
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
