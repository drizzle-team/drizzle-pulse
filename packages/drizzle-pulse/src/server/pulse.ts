import { getColumns, getTableUniqueName } from 'drizzle-orm';
import {
  getTableConfig,
  type PgTable,
  type PgTableWithColumns,
  type TableConfig,
} from 'drizzle-orm/pg-core';
import { PulseBuilder } from './pulse-builder.js';
import type { ColumnsSelection, PulseQueryConfig } from './pulse-types.js';

function getPrimaryKeyColumn(table: PgTableWithColumns<TableConfig>) {
  const tableConfig = getTableConfig(table);
  const inlinePrimaryKeyColumns = tableConfig.columns.filter((column) => column.primary);
  const compositePrimaryKeyColumns = tableConfig.primaryKeys.flatMap(
    (primaryKey) => primaryKey.columns,
  );
  const primaryKeyColumns = Array.from(
    new Map(
      [...inlinePrimaryKeyColumns, ...compositePrimaryKeyColumns].map((column) => [
        column.name,
        column,
      ]),
    ).values(),
  );

  if (primaryKeyColumns.length === 0) {
    throw new Error(`Table "${getTableUniqueName(table)}" has no primary key`);
  }

  if (primaryKeyColumns.length > 1) {
    throw new Error(`Table "${getTableUniqueName(table)}" has multiple primary keys`);
  }

  const [pkColumn] = primaryKeyColumns;
  if (!pkColumn) {
    throw new Error(`Table "${getTableUniqueName(table)}" has no primary key`);
  }

  return pkColumn;
}

export type PulseFactory = <TTable extends PgTable>(
  table: TTable,
) => PulseBuilder<
  TTable,
  Record<never, boolean>,
  Record<never, never>,
  ColumnsSelection<TTable, Record<never, boolean>>
>;

function makeConfig<TTable extends PgTableWithColumns<TableConfig>>(
  table: TTable,
): PulseQueryConfig<
  TTable,
  Record<never, boolean>,
  Record<never, never>,
  ColumnsSelection<TTable, Record<never, boolean>>
> {
  const columns = getColumns(table);
  const pkColumn = getPrimaryKeyColumn(table);

  const pkSqlType = pkColumn.getSQLType().toLowerCase();
  // getSQLType() includes length modifiers, e.g. "varchar(64)"
  const pkBaseSqlType = pkSqlType.replace(/\s*\(.*\)$/, '');
  const supportedPkSqlTypes = new Set([
    'smallint',
    'integer',
    'bigint',
    'serial',
    'bigserial',
    'text',
    'varchar',
    'character varying',
    'char',
    'character',
    'uuid',
  ]);
  if (!supportedPkSqlTypes.has(pkBaseSqlType)) {
    throw new Error(
      `Table "${getTableUniqueName(table)}" primary key "${pkColumn.name}" has unsupported SQL type "${pkSqlType}". Supported types: ${Array.from(
        supportedPkSqlTypes,
      ).join(', ')}`,
    );
  }

  return {
    table: {
      source: table,
      events: null,
    },
    pkColumn,
    columns,
    selectedColumns: columns,
    argsSchema: null,
    queryFn: null,
    transformFn: null,
    order: null,
    limit: null,
  };
}

export function createPulse(): PulseFactory {
  function factory<TTable extends PgTableWithColumns<TableConfig>>(
    table: TTable,
  ): PulseBuilder<
    TTable,
    Record<never, boolean>,
    Record<never, never>,
    ColumnsSelection<TTable, Record<never, boolean>>
  > {
    return new PulseBuilder(makeConfig<TTable>(table));
  }

  return factory as PulseFactory;
}
