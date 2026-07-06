import type { InferSelectModel } from 'drizzle-orm';
import { getColumns, getTableUniqueName } from 'drizzle-orm';
import type { PgColumn, PgTable } from 'drizzle-orm/pg-core';
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
// but pulse-owned; this is the sole recognition marker (no drizzle entity-kind tag, D-14).
const PulseTableBrand = Symbol.for('drizzle-pulse:isPulseTable');

const SUPPORTED_PK_SQL_TYPES = new Set([
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

/**
 * Pure lazy PK validation (D-06): only sees inline `.primaryKey()` columns via bare
 * `getColumns`. Composite PKs declared via `primaryKey()` in table extras require
 * `getTableConfig` (a pg-core value import banned from this platform-pure module) —
 * that defensive check is added server-side by the registry/expose cutover (RESEARCH A3).
 */
export function getPulsePkColumn(table: PgTable): PgColumn {
  const columns = Object.values(getColumns(table)) as PgColumn[];
  const inlinePkColumns = columns.filter((column) => column.primary);

  if (inlinePkColumns.length === 0) {
    throw new Error(`Table "${getTableUniqueName(table)}" has no primary key`);
  }

  if (inlinePkColumns.length > 1) {
    throw new Error(`Table "${getTableUniqueName(table)}" has multiple primary keys`);
  }

  const [pkColumn] = inlinePkColumns;
  if (!pkColumn) {
    throw new Error(`Table "${getTableUniqueName(table)}" has no primary key`);
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
   * `ctx.args` is typed `any` here (not `Record<never, never>`) so the user-canonical
   * spelling `pulse(t).query(({ args }) => ...)` can read args before `.args()` has
   * seeded a schema anywhere in the chain. This is runtime-safe because args are
   * schema-parsed at `resolve()` time; callers who want a fully-typed `ctx.args` use
   * `pulse(t).query().args(schema).query(fn)` instead, where the builder-level
   * `.query()` sees the real `TArgs` generic.
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
        events: null,
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
