import type { InferSelectModel, Simplify } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
import type { z } from 'zod';
import type { PulseAuthContext, QueryDescriptor, WhereClause, WhereCondition } from '../types.js';
import type { PulseBuilder } from './pulse-builder.js';

export type PulseColumnSelection<TRow extends Record<string, unknown>> = {
  [K in keyof TRow]?: boolean;
};

export type IsIncludeMode<TSelection extends Record<string, boolean>> =
  TSelection[keyof TSelection] extends false ? false : true;

export type ApplyColumns<
  TRow extends Record<string, unknown>,
  TSelection extends Record<string, boolean>,
> = [keyof TSelection] extends [never]
  ? TRow
  : true extends TSelection[keyof TSelection]
    ? {
        [K in keyof TRow as K extends string
          ? TSelection[K] extends true
            ? K
            : never
          : never]: TRow[K];
      }
    : {
        [K in keyof TRow as K extends string
          ? TSelection[K] extends false
            ? never
            : K
          : never]: TRow[K];
      };

export type InferColumnSelection<TSelection extends Record<string, boolean>> = {
  [K in keyof TSelection]: TSelection[K] extends true ? K : never;
}[keyof TSelection];

export type ColumnsSelection<
  TTable extends PgTable,
  TSelection extends Record<string, boolean>,
> = ApplyColumns<InferSelectModel<TTable>, TSelection>;

export interface PulseQueryContext<TArgs, TRow extends Record<string, unknown>> {
  query: (where: WhereCondition<TRow>) => WhereClause;
  args: TArgs;
  auth: PulseAuthContext;
}

// Builder callbacks are stored on widened config/registry types and later reassigned across
// generic builder states. A plain function type becomes too contravariant there, which breaks
// `query()`/`transform()` assignability and downstream registry/client typing.
type Bivariant<T extends (...args: never) => unknown> = {
  bivariant(...args: Parameters<T>): ReturnType<T>;
}['bivariant'];

export type QueryFn<TArgs, TRow extends Record<string, unknown>> = Bivariant<
  (ctx: PulseQueryContext<TArgs, TRow>) => WhereClause | null
>;

export type PulseTransformFn<
  TInput extends Record<string, unknown>,
  TOutput extends Record<string, unknown> = TInput,
> = Bivariant<(rows: TInput[]) => Promise<TOutput[]> | TOutput[]>;

export type WithPk<TRow extends Record<string, unknown>> = { $pk: unknown } & TRow;

export type PulseBuilderTable<TTable extends PgTable> = {
  readonly source: TTable;
  readonly events: PgTable | null;
};

export type PulseQueryConfig<
  TTable extends PgTable,
  TColumns extends Record<string, boolean>,
  TArgs,
  TResult extends Record<string, unknown> = ColumnsSelection<TTable, TColumns>,
> = {
  readonly table: PulseBuilderTable<TTable>;
  readonly pkColumn: PgColumn;
  readonly columns: Record<string, PgColumn>;
  readonly selectedColumns: Record<string, PgColumn>;
  readonly argsSchema: z.ZodType<TArgs> | null;
  readonly queryFn: QueryFn<TArgs, InferSelectModel<TTable>> | null;
  readonly transformFn: PulseTransformFn<Record<string, unknown>, TResult> | null;
  readonly order: 'asc' | 'desc' | null;
  readonly limit: number | null;
};

export type PulseClientContract<TShapes extends Record<string, unknown>> = Simplify<{
  [K in keyof TShapes]: TShapes[K] extends PulseBuilder<
    infer TTable,
    infer TColumns,
    infer TArgs,
    infer TResult
  >
    ? TTable extends PgTable
      ? TColumns extends Record<string, boolean>
        ? [keyof TArgs] extends [never]
          ? () => QueryDescriptor<Simplify<WithPk<TResult>>>
          : (args: TArgs) => QueryDescriptor<Simplify<WithPk<TResult>>>
        : never
      : never
    : never;
}>;

export function getQueryColumnKey(columns: Record<string, PgColumn>, targetColumn: PgColumn) {
  for (const [queryKey, column] of Object.entries(columns)) {
    if (column === targetColumn || column.name === targetColumn.name) {
      return queryKey;
    }
  }

  return null;
}

export function applyColumnFilter(
  row: Record<string, unknown>,
  selectedColumns: Record<string, PgColumn>,
) {
  const keys = Object.keys(selectedColumns);
  if (keys.length === 0) return row;
  const result: Record<string, unknown> & { $pk?: unknown } = {};

  if ('$pk' in row) {
    const rowWithPk = row as WithPk<typeof row>;
    result.$pk = rowWithPk.$pk;
  }

  for (const [k, v] of Object.entries(row)) {
    if (k === '$pk') continue;
    if (k in selectedColumns) {
      result[k] = v;
    }
  }

  return result;
}
