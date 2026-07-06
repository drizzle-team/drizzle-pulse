import { getTableUniqueName } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import { getPulsePkColumn, isPulseTable } from '../pulse-table.js';
import type {
  PulseAuthContext,
  PulseRegistryQuery,
  ResolvedPulseQuery,
  WhereClause,
} from '../types.js';
import type { PulseBuilder } from './pulse-builder.js';
import { applyColumnFilter, type PulseClientContract } from './pulse-types.js';

export type AnyPulseBuilder = PulseBuilder<
  PgTable,
  Record<string, boolean>,
  unknown,
  Record<string, unknown>
>;

export type AnyPulseBuilders = Record<string, AnyPulseBuilder>;

type BuiltQuery = {
  pulseQuery: PulseRegistryQuery;
  sourceTable: PgTable;
};

function getQualifiedTableName(table: PgTable) {
  return getTableUniqueName(table);
}

// Defensive re-check (D-06): `PulseTable.query()` only sees inline `.primaryKey()` columns
// (a pure module can't value-import `getTableConfig`). Registries can receive a hand-built
// AnyPulseBuilder that bypassed that gate, so union inline + table-extras `primaryKey()`
// entries by name here and reject anything resolving to more than one distinct column.
function assertSinglePrimaryKeyName(table: PgTable): void {
  const tableConfig = getTableConfig(table);
  const inlinePrimaryKeyColumns = tableConfig.columns.filter((column) => column.primary);
  const compositePrimaryKeyColumns = tableConfig.primaryKeys.flatMap(
    (primaryKey) => primaryKey.columns,
  );
  const primaryKeyColumnNames = new Set(
    [...inlinePrimaryKeyColumns, ...compositePrimaryKeyColumns].map((column) => column.name),
  );

  if (primaryKeyColumnNames.size > 1) {
    throw new Error(`Table "${getTableUniqueName(table)}" has multiple primary keys`);
  }
}

function buildPulseQuery(query: AnyPulseBuilder) {
  const { config } = query;
  const sourceTable = config.table.source;

  assertSinglePrimaryKeyName(sourceTable);
  getPulsePkColumn(sourceTable);

  const pulseQuery: PulseRegistryQuery = {
    table: sourceTable,
    pkColumn: config.pkColumn,
    columns: config.columns,
    selectedColumns: config.selectedColumns,
    allowedColumnNames: new Set(Object.keys(config.selectedColumns)),
    order: config.order ?? 'asc',
    limit: config.limit ?? null,
    argsSchema: config.argsSchema,
    queryFn: config.queryFn,
    hasTransform: config.transformFn !== null,
    transformRows: (rows: Record<string, unknown>[]) =>
      config.transformFn ? config.transformFn(rows) : rows,
  };

  return {
    pulseQuery,
    sourceTable,
  };
}

export function addPrimaryKey(row: Record<string, unknown>, pulseQuery: ResolvedPulseQuery) {
  const pkValue = row[pulseQuery.pkColumn.name];
  if (pkValue === undefined) {
    throw new Error(
      `Primary key column "${pulseQuery.pkColumn.name}" on "${getQualifiedTableName(pulseQuery.table)}" is missing`,
    );
  }

  return { ...row, $pk: pkValue };
}

export async function applyResponsePipeline(
  rows: Record<string, unknown>[],
  pulseQuery: ResolvedPulseQuery,
) {
  const transformedRows = await pulseQuery.transformRows(rows);
  return applyProjectionPipeline(transformedRows, pulseQuery);
}

// Synchronous projection helper for the embedded path — skips async transformRows.
export function applyProjectionPipeline(
  rows: Record<string, unknown>[],
  pulseQuery: Pick<ResolvedPulseQuery, 'pkColumn' | 'selectedColumns' | 'table'>,
): Record<string, unknown>[] {
  return rows
    .map((row) => addPrimaryKey(row, pulseQuery as ResolvedPulseQuery))
    .map((row) => applyColumnFilter(row, pulseQuery.selectedColumns));
}

export class PulseRegistry<TQueries extends AnyPulseBuilders> {
  readonly $client!: PulseClientContract<TQueries>;
  private readonly pulseQueries: Record<string, PulseRegistryQuery>;
  private readonly sourceTables: Record<string, PgTable>;

  constructor(queries: TQueries) {
    for (const [name, query] of Object.entries(queries)) {
      if (isPulseTable(query)) {
        throw new Error(
          `Query "${name}" is a bare collection — call \`.query()\` on it first to derive a query (e.g. pulse(table).query())`,
        );
      }
    }

    const builtEntries = Object.entries(queries).map<[string, BuiltQuery]>(([name, query]) => [
      name,
      buildPulseQuery(query),
    ]);

    this.pulseQueries = Object.fromEntries(
      builtEntries.map(([name, built]) => [name, built.pulseQuery]),
    );
    this.sourceTables = Object.fromEntries(
      builtEntries.map(([name, built]) => [name, built.sourceTable]),
    );
  }

  getPulseQuery(name: string) {
    return this.pulseQueries[name] ?? null;
  }

  getSourceTable(name: string) {
    return this.sourceTables[name] ?? null;
  }

  getQueryNames() {
    return Object.keys(this.pulseQueries);
  }

  resolve(name: string, rawArgs: unknown, auth: PulseAuthContext): ResolvedPulseQuery {
    const registryQuery = this.pulseQueries[name];
    if (!registryQuery) throw new Error(`Unknown query: "${name}"`);
    const args = registryQuery.argsSchema
      ? registryQuery.argsSchema.parse(rawArgs)
      : (rawArgs ?? {});
    const where =
      registryQuery.queryFn?.({
        query: (clause: WhereClause) => clause,
        args,
        auth,
      }) ?? null;
    return {
      table: registryQuery.table,
      pkColumn: registryQuery.pkColumn,
      columns: registryQuery.columns,
      selectedColumns: registryQuery.selectedColumns,
      allowedColumnNames: registryQuery.allowedColumnNames,
      order: registryQuery.order,
      limit: registryQuery.limit,
      argsSchema: registryQuery.argsSchema,
      where,
      hasTransform: registryQuery.hasTransform,
      transformRows: registryQuery.transformRows,
    };
  }
}

export function createPulseRegistry<TQueries extends AnyPulseBuilders>(queries: TQueries) {
  return new PulseRegistry(queries);
}
