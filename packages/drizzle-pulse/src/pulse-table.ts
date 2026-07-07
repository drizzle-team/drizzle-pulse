import type { InferModelFromColumns, InferSelectModel } from 'drizzle-orm';
import { entityKind, getColumns, getTableUniqueName } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
import type { z } from 'zod';
import { PulseBuilder } from './server/pulse-builder.js';
import type {
  ColumnsSelection,
  PulseQueryConfig,
  PulseQueryContext,
  QueryFn,
} from './server/pulse-types.js';
import type { WhereClause } from './types.js';

// Global symbol registry (not a module-local Symbol()) so the brand survives duplicated
// package copies across node_modules trees — mirrors drizzle-orm's entity.ts mechanism,
// but pulse-owned; this is the sole recognition marker (no drizzle entity-kind tag).
const PulseTableBrand = Symbol.for('drizzle-pulse:isPulseTable');

const SUPPORTED_PK_SQL_TYPES = new Set([
  'smallint',
  'integer',
  'bigint',
  'smallserial',
  'serial',
  'bigserial',
  'text',
  'varchar',
  'character varying',
  'char',
  'character',
  'uuid',
]);

// Global-registry keys for drizzle's table-extras config (stable `Symbol.for(...)` keys,
// the same reflection mechanism the events-table resolver and the isPulseTable brand use).
const EXTRA_CONFIG_BUILDER_SYMBOL = Symbol.for('drizzle:ExtraConfigBuilder');
const EXTRA_CONFIG_COLUMNS_SYMBOL = Symbol.for('drizzle:ExtraConfigColumns');

// Reads table-extras `primaryKey({ columns: [...] })` declarations without importing
// pg-core's `getTableConfig` (banned from this platform-pure module). Runs the table's
// extra-config builder and picks out PrimaryKeyBuilder entries by their drizzle entityKind.
function getExtrasPrimaryKeyColumnNames(table: PgTable): string[] {
  const reflected = table as unknown as Record<symbol, unknown>;
  const extraConfigBuilder = reflected[EXTRA_CONFIG_BUILDER_SYMBOL];
  if (typeof extraConfigBuilder !== 'function') return [];

  const extraConfig = (extraConfigBuilder as (columns: unknown) => unknown)(
    reflected[EXTRA_CONFIG_COLUMNS_SYMBOL],
  );
  const builders = Array.isArray(extraConfig)
    ? extraConfig.flat(1)
    : Object.values(extraConfig as object);

  const names: string[] = [];
  for (const builder of builders) {
    const kind = (builder as { constructor?: Record<symbol, unknown> })?.constructor?.[entityKind];
    if (kind !== 'PgPrimaryKeyBuilder') continue;
    for (const column of (builder as { columns?: readonly { name: string }[] }).columns ?? []) {
      names.push(column.name);
    }
  }
  return names;
}

/**
 * Resolves the single primary-key column drizzle-pulse tracks a table by. Prefers an
 * inline `.primaryKey()` column; falls back to a table-extras `primaryKey({ columns: [...] })`
 * declaration. Exactly one PK column is required — a true composite (multiple columns) is
 * rejected — and its SQL type must be in the supported allowlist.
 */
export function getPulsePkColumn(table: PgTable): PgColumn {
  const columns = Object.values(getColumns(table)) as PgColumn[];
  const inlinePkColumns = columns.filter((column) => column.primary);

  let pkColumns = inlinePkColumns;
  if (pkColumns.length === 0) {
    const extrasNames = new Set(getExtrasPrimaryKeyColumnNames(table));
    pkColumns = columns.filter((column) => extrasNames.has(column.name));
  }

  if (pkColumns.length === 0) {
    throw new Error(
      `Table "${getTableUniqueName(table)}" has no primary key — declare one via .primaryKey() or primaryKey({ columns: [...] })`,
    );
  }

  if (pkColumns.length > 1) {
    throw new Error(`Table "${getTableUniqueName(table)}" has multiple primary keys`);
  }

  const [pkColumn] = pkColumns;
  if (!pkColumn) {
    throw new Error(`Table "${getTableUniqueName(table)}" has no resolvable primary key`);
  }

  const pkSqlType = pkColumn.getSQLType().toLowerCase();
  // getSQLType() includes length modifiers, e.g. "varchar(64)"
  const pkBaseSqlType = pkSqlType.replace(/\s*\(.*\)$/, '');
  if (!SUPPORTED_PK_SQL_TYPES.has(pkBaseSqlType)) {
    throw new Error(
      `Table "${getTableUniqueName(table)}" primary key "${pkColumn.name}" has unsupported SQL type "${pkSqlType}". Supported types: ${Array.from(
        SUPPORTED_PK_SQL_TYPES,
      ).join(', ')}`,
    );
  }

  return pkColumn;
}

export class PulseTable<TTable extends PgTable = PgTable> {
  readonly [PulseTableBrand] = true as const;

  constructor(readonly table: TTable) {}

  /**
   * `ctx.args` is typed `any` here (not `Record<never, never>`) so the collection-level
   * spelling `pulse(t).query(({ args }) => ...)` can read args before a schema is seeded.
   * That is runtime-safe only when a schema is later chained via `.args()` — `resolve()`
   * substitutes an empty object for a schemaless query rather than passing raw client
   * input through, so `ctx.args` is always `{}` without `.args(schema)`. For a fully-typed
   * `ctx.args`, start the chain with `pulse(t).args(schema).query(fn)`, where the
   * builder-level `.query()` sees the real `TArgs` generic.
   */
  query(
    fn?: (
      ctx: PulseQueryContext<
        // biome-ignore lint/suspicious/noExplicitAny: permissive collection-level args, see comment above
        any,
        InferSelectModel<TTable>
      >,
    ) => WhereClause | null,
  ): PulseBuilder<
    TTable,
    Record<never, boolean>,
    Record<never, never>,
    ColumnsSelection<TTable, Record<never, boolean>>
  > {
    const columns = getColumns(this.table);
    const pkColumn = getPulsePkColumn(this.table);

    const config: PulseQueryConfig<
      TTable,
      Record<never, boolean>,
      Record<never, never>,
      ColumnsSelection<TTable, Record<never, boolean>>
    > = {
      table: {
        source: this.table,
      },
      pkColumn,
      columns,
      selectedColumns: columns,
      argsSchema: null,
      queryFn: (fn ?? null) as QueryFn<Record<never, never>, InferSelectModel<TTable>> | null,
      transformFn: null,
      order: null,
      limit: null,
    };

    return new PulseBuilder(config);
  }

  /** Start a builder chain with a typed args schema — `pulse(t).args(schema).query(fn)`. */
  args<TNewArgs>(schema: z.ZodType<TNewArgs>) {
    return this.query().args(schema);
  }

  /** Start a builder chain with a column selection. */
  columns<TNewSelection extends Record<string, boolean>>(selection: TNewSelection) {
    return this.query().columns(selection);
  }

  /** Start a builder chain with a sort direction. */
  order(direction: 'asc' | 'desc') {
    return this.query().order(direction);
  }

  /** Start a builder chain with a row limit. */
  limit(n: number) {
    return this.query().limit(n);
  }

  /** Start a builder chain with a row transform. */
  transform<TTransformed extends Record<string, unknown>>(
    fn: (
      rows: InferModelFromColumns<TTable['_']['columns']>[],
    ) => Promise<TTransformed[]> | TTransformed[],
  ) {
    return this.query().transform(fn);
  }
}

export function pulse<TTable extends PgTable>(table: TTable): PulseTable<TTable> {
  return new PulseTable(table);
}

export function isPulseTable(value: unknown): value is PulseTable {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as Record<symbol, unknown>)[PulseTableBrand] === true
  );
}

export function getPulseTableConfig<TTable extends PgTable>(
  entity: PulseTable<TTable>,
): { table: TTable } {
  return { table: entity.table };
}
