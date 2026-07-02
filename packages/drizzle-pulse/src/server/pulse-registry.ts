import { getColumns, getTableUniqueName } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
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
  eventsTable: PgTable;
};

function getQualifiedTableName(table: PgTable) {
  return getTableUniqueName(table);
}

const REQUIRED_EVENT_METADATA_COLUMNS = ['$snapshot', '$op', '$timestamp'] as const;

function getColumnByName(columns: Record<string, PgColumn>, columnName: string) {
  for (const column of Object.values(columns)) {
    if (column.name === columnName) {
      return column;
    }
  }

  return null;
}

function validateEventsTable(query: AnyPulseBuilder) {
  const { config } = query;
  const eventsTable = config.table.events;
  if (!eventsTable) {
    throw new Error(
      `Query "${getQualifiedTableName(config.table.source)}" is missing .$eventsTable() linkage`,
    );
  }

  const sourceColumns = config.columns;
  const eventsColumns = getColumns(eventsTable);
  const sourcePkColumn = config.pkColumn.name;

  for (const requiredColumn of REQUIRED_EVENT_METADATA_COLUMNS) {
    if (!getColumnByName(eventsColumns, requiredColumn)) {
      throw new Error(
        `Events table "${getQualifiedTableName(eventsTable)}" is missing required column "${requiredColumn}"`,
      );
    }
  }

  if (!getColumnByName(eventsColumns, sourcePkColumn)) {
    throw new Error(
      `Events table "${getQualifiedTableName(eventsTable)}" is missing primary key column "${sourcePkColumn}"`,
    );
  }

  for (const sourceColumn of Object.values(sourceColumns)) {
    if (!getColumnByName(eventsColumns, sourceColumn.name)) {
      throw new Error(
        `Events table "${getQualifiedTableName(eventsTable)}" is missing column "${sourceColumn.name}"`,
      );
    }

    if (!getColumnByName(eventsColumns, `$old_${sourceColumn.name}`)) {
      throw new Error(
        `Events table "${getQualifiedTableName(eventsTable)}" is missing column "$old_${sourceColumn.name}"`,
      );
    }
  }

  return eventsTable;
}

function buildPulseQuery(query: AnyPulseBuilder) {
  const { config } = query;
  const eventsTable = validateEventsTable(query);

  const pulseQuery: PulseRegistryQuery = {
    table: config.table.source,
    eventsTable,
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
    sourceTable: config.table.source,
    eventsTable,
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
  private readonly eventsTables: Record<string, PgTable>;

  constructor(queries: TQueries) {
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
    this.eventsTables = Object.fromEntries(
      builtEntries.map(([name, built]) => [name, built.eventsTable]),
    );
  }

  getPulseQuery(name: string) {
    return this.pulseQueries[name] ?? null;
  }

  getSourceTable(name: string) {
    return this.sourceTables[name] ?? null;
  }

  getEventsTable(name: string) {
    return this.eventsTables[name] ?? null;
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
      eventsTable: registryQuery.eventsTable,
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
