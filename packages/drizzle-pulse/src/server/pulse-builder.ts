import type { InferModelFromColumns, InferSelectModel } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
import type { z } from 'zod';
import type { WhereClause } from '../types.js';
import type {
  ColumnsSelection,
  PulseQueryConfig,
  PulseQueryContext,
  QueryFn,
} from './pulse-types.js';

function columnsToSelectedColumns(
  selection: Record<string, boolean>,
  columns: Record<string, PgColumn>,
): Record<string, PgColumn> {
  const keys = Object.keys(selection);
  if (keys.length === 0) return columns;
  const columnEntries = Object.entries(columns);

  const hasInclude = keys.some((key) => selection[key] === true);
  if (hasInclude) {
    const includedColumnEntries = columnEntries.filter(([name]) => selection[name] === true);

    return Object.fromEntries(includedColumnEntries);
  }

  const remainingColumnEntries = columnEntries.filter(([name]) => selection[name] !== false);

  return Object.fromEntries(remainingColumnEntries);
}

export class PulseBuilder<
  TTable extends PgTable,
  TSelection extends Record<string, boolean>,
  TArgs,
  TResult extends Record<string, unknown> = ColumnsSelection<TTable, TSelection>,
> {
  readonly _!: {
    result: TResult;
  };

  readonly config: PulseQueryConfig<TTable, TSelection, TArgs, TResult>;

  constructor(config: PulseQueryConfig<TTable, TSelection, TArgs, TResult>) {
    this.config = config;
  }

  columns<TNewSelection extends Record<string, boolean>>(
    selection: TNewSelection,
  ): PulseBuilder<TTable, TNewSelection, TArgs, ColumnsSelection<TTable, TNewSelection>> {
    // .columns() re-parameterizes TResult to ColumnsSelection<TTable, TNewSelection> — a
    // previously-chained .transform() was typed against the OLD TResult, so silently
    // carrying it over here would either type-check against the wrong shape or (as the
    // prior implementation did) get silently dropped at runtime while callers still
    // believe it's active (WR-08). Fail loudly instead: require .columns() before
    // .transform() in the chain.
    if (this.config.transformFn !== null) {
      throw new Error(
        '.columns() cannot follow .transform(); call .columns() before .transform() instead',
      );
    }

    const selectedColumns = columnsToSelectedColumns(selection, this.config.columns);

    return new PulseBuilder<TTable, TNewSelection, TArgs, ColumnsSelection<TTable, TNewSelection>>({
      table: this.config.table,
      pkColumn: this.config.pkColumn,
      columns: this.config.columns,
      selectedColumns,
      argsSchema: this.config.argsSchema,
      queryFn: this.config.queryFn,
      transformFn: null,
      order: this.config.order,
      limit: this.config.limit,
    });
  }

  args<TNewArgs>(schema: z.ZodType<TNewArgs>): PulseBuilder<TTable, TSelection, TNewArgs, TResult> {
    return new PulseBuilder<TTable, TSelection, TNewArgs, TResult>({
      table: this.config.table,
      pkColumn: this.config.pkColumn,
      columns: this.config.columns,
      selectedColumns: this.config.selectedColumns,
      argsSchema: schema,
      // The second QueryFn type param is the full source row type (PulseQueryConfig's
      // `queryFn: QueryFn<TArgs, InferSelectModel<TTable>>`), not the column-selection
      // map TSelection — the row shape doesn't change across .args(), only TArgs does.
      queryFn: this.config.queryFn as QueryFn<TNewArgs, InferSelectModel<TTable>> | null,
      transformFn: this.config.transformFn,
      order: this.config.order,
      limit: this.config.limit,
    });
  }

  transform<TTransformed extends Record<string, unknown>>(
    // Plain (non-Bivariant) function type: the Bivariant wrapper hides TTransformed inside
    // ReturnType<T>, which blocks its inference at the call site (falls back to the constraint).
    // The stored config field keeps the Bivariant PulseTransformFn for cross-state assignability.
    fn: (
      rows: InferModelFromColumns<TTable['_']['columns']>[],
    ) => Promise<TTransformed[]> | TTransformed[],
  ): PulseBuilder<TTable, TSelection, TArgs, TTransformed> {
    return new PulseBuilder<TTable, TSelection, TArgs, TTransformed>({
      table: this.config.table,
      pkColumn: this.config.pkColumn,
      columns: this.config.columns,
      selectedColumns: this.config.selectedColumns,
      argsSchema: this.config.argsSchema,
      queryFn: this.config.queryFn,
      transformFn: fn,
      order: this.config.order,
      limit: this.config.limit,
    });
  }

  order(direction: 'asc' | 'desc'): PulseBuilder<TTable, TSelection, TArgs, TResult> {
    return new PulseBuilder<TTable, TSelection, TArgs, TResult>({
      table: this.config.table,
      pkColumn: this.config.pkColumn,
      columns: this.config.columns,
      selectedColumns: this.config.selectedColumns,
      argsSchema: this.config.argsSchema,
      queryFn: this.config.queryFn,
      transformFn: this.config.transformFn,
      order: direction,
      limit: this.config.limit,
    });
  }

  limit(n: number): PulseBuilder<TTable, TSelection, TArgs, TResult> {
    return new PulseBuilder<TTable, TSelection, TArgs, TResult>({
      table: this.config.table,
      pkColumn: this.config.pkColumn,
      columns: this.config.columns,
      selectedColumns: this.config.selectedColumns,
      argsSchema: this.config.argsSchema,
      queryFn: this.config.queryFn,
      transformFn: this.config.transformFn,
      order: this.config.order,
      limit: n,
    });
  }

  query(
    fn: (ctx: PulseQueryContext<TArgs, InferSelectModel<TTable>>) => WhereClause | null,
  ): PulseBuilder<TTable, TSelection, TArgs, TResult> {
    return new PulseBuilder<TTable, TSelection, TArgs, TResult>({
      table: this.config.table,
      pkColumn: this.config.pkColumn,
      columns: this.config.columns,
      selectedColumns: this.config.selectedColumns,
      argsSchema: this.config.argsSchema,
      queryFn: fn,
      transformFn: this.config.transformFn,
      order: this.config.order,
      limit: this.config.limit,
    });
  }
}
