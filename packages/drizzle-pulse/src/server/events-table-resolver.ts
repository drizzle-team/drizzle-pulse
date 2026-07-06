import { getColumns } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
import {
  getTableConfig,
  integer,
  PgBigInt53,
  PgBigInt64,
  PgInteger,
  PgTable as PgTableClass,
  text,
  timestamp,
} from 'drizzle-orm/pg-core';

export const DEFAULT_EVENTS_SCHEMA = 'drizzle';

const POSTGRES_IDENTIFIER_BYTE_LIMIT = 63;

function assertIdentifierLength(identifier: string): void {
  const byteLength = Buffer.byteLength(identifier, 'utf8');
  if (byteLength > POSTGRES_IDENTIFIER_BYTE_LIMIT) {
    throw new Error(
      `Derived identifier "${identifier}" is ${byteLength} bytes, exceeding Postgres's ${POSTGRES_IDENTIFIER_BYTE_LIMIT}-byte identifier limit`,
    );
  }
}

export function getEventsTableName(sourceTable: PgTable): string {
  const sourceConfig = getTableConfig(sourceTable);
  const sourceSchema = sourceConfig.schema ?? 'public';
  const name = `__events_${sourceSchema}_${sourceConfig.name}`;
  assertIdentifierLength(name);
  return name;
}

// Serial-family columns must be relaxed to their plain int equivalent in the events
// table (D-10/Pitfall 3): a naive class-preserving clone would carry over auto-increment
// defaults/identity semantics that reject explicitly-supplied insert values.
const SERIAL_SWAP: Record<
  string,
  { Cls: ConcretePgColumnConstructor; dataType: string; columnType: string }
> = {
  PgSerial: {
    Cls: PgInteger as unknown as ConcretePgColumnConstructor,
    dataType: 'number int32',
    columnType: 'PgInteger',
  },
  PgSmallSerial: {
    Cls: PgInteger as unknown as ConcretePgColumnConstructor,
    dataType: 'number int32',
    columnType: 'PgInteger',
  },
  PgBigSerial53: {
    Cls: PgBigInt53 as unknown as ConcretePgColumnConstructor,
    dataType: 'number int53',
    columnType: 'PgBigInt53',
  },
  PgBigSerial64: {
    Cls: PgBigInt64 as unknown as ConcretePgColumnConstructor,
    dataType: 'bigint int64',
    columnType: 'PgBigInt64',
  },
};

type ConcretePgColumnConstructor = new (
  table: PgTable,
  config: Record<string, unknown>,
) => PgColumn;

// Drizzle's `Column`/`PgColumn` classes keep their full builder-time config (mode,
// precision/scale, dimensions, srid, withTimezone, enum instance, ...) on a `protected
// config` field with no public read API. Re-instantiating the same (or a relaxed)
// column class against that config is the only way to reconstruct a column that reuses
// the source's own `mapToDriverValue`/`mapFromDriverValue` codec (RESEARCH Pitfall 2) —
// there is no public clone utility. This narrows through `unknown` rather than `any`.
interface ColumnWithConfig {
  config: Record<string, unknown>;
}

function getColumnConfig(column: PgColumn): Record<string, unknown> {
  return (column as unknown as ColumnWithConfig).config;
}

function getConcreteColumnConstructor(column: PgColumn): ConcretePgColumnConstructor {
  return Object.getPrototypeOf(column).constructor as unknown as ConcretePgColumnConstructor;
}

function cloneColumn(
  eventsTable: PgTable,
  sourceColumn: PgColumn,
  name: string,
  notNull: boolean,
): PgColumn {
  const swap = SERIAL_SWAP[sourceColumn.columnType];
  const Cls = swap?.Cls ?? getConcreteColumnConstructor(sourceColumn);
  const sourceConfig = getColumnConfig(sourceColumn);

  const config: Record<string, unknown> = {
    ...sourceConfig,
    name,
    notNull,
    primaryKey: false,
    hasDefault: false,
    default: undefined,
    defaultFn: undefined,
    onUpdateFn: undefined,
    generated: undefined,
    generatedIdentity: undefined,
    isUnique: false,
    uniqueName: undefined,
    uniqueType: undefined,
    ...(swap ? { dataType: swap.dataType, columnType: swap.columnType } : {}),
  };

  return new Cls(eventsTable, config);
}

// `Table.Symbol.Columns` is marked `/** @internal */` in drizzle-orm and stripped from
// the published `.d.ts`, but it is defined via the stable global-registry key
// `Symbol.for('drizzle:Columns')` — reconstructing that key directly is safe (it is the
// same mechanism the entity brand pattern relies on) and avoids a blanket `any` cast.
const TABLE_COLUMNS_SYMBOL = Symbol.for('drizzle:Columns');

interface ColumnBuilderLike {
  build(table: PgTable): PgColumn;
}

// `ColumnBuilder.build()` is likewise `/** @internal */` and absent from the public
// `.d.ts`, even though it is the exact method `pgTable()`/`pgSchema().table()` call
// internally to turn a builder into a column. Metadata columns are built with plain
// builders (not config-clones), so we go through the same internal method.
function buildColumn(builder: unknown, table: PgTable): PgColumn {
  return (builder as unknown as ColumnBuilderLike).build(table);
}

function attachColumns(table: PgTable, columns: Record<string, PgColumn>): void {
  Object.assign(table, columns);
  (table as unknown as Record<symbol, unknown>)[TABLE_COLUMNS_SYMBOL] = columns;
}

export function resolveEventsTable(
  sourceTable: PgTable,
  options?: { eventsSchema?: string },
): PgTable {
  const eventsSchema = options?.eventsSchema ?? DEFAULT_EVENTS_SCHEMA;
  const tableName = getEventsTableName(sourceTable);

  const eventsTable = new PgTableClass(tableName, eventsSchema, tableName);
  const sourceColumns = getColumns(sourceTable);

  const newValueColumns: Record<string, PgColumn> = {};
  const oldValueColumns: Record<string, PgColumn> = {};

  for (const sourceColumn of Object.values(sourceColumns)) {
    newValueColumns[sourceColumn.name] = cloneColumn(
      eventsTable,
      sourceColumn,
      sourceColumn.name,
      sourceColumn.primary,
    );

    const oldName = `$old_${sourceColumn.name}`;
    assertIdentifierLength(oldName);
    oldValueColumns[oldName] = cloneColumn(eventsTable, sourceColumn, oldName, false);
  }

  const columns: Record<string, PgColumn> = {
    ...newValueColumns,
    ...oldValueColumns,
    $snapshot: buildColumn(integer('$snapshot').generatedAlwaysAsIdentity(), eventsTable),
    $op: buildColumn(text('$op').notNull(), eventsTable),
    $timestamp: buildColumn(
      timestamp('$timestamp', { withTimezone: true }).notNull().defaultNow(),
      eventsTable,
    ),
  };

  attachColumns(eventsTable, columns);

  return eventsTable;
}
