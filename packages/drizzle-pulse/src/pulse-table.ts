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

/**
 * Pure lazy PK validation: only sees inline `.primaryKey()` columns via bare
 * `getColumns`. Composite PKs declared via `primaryKey()` in table extras require
 * `getTableConfig` (a pg-core value import banned from this platform-pure module) —
 * that defensive check is added server-side by the registry/expose cutover.
 */
export function getPulsePkColumn(table: PgTable): PgColumn {
  const columns = Object.values(getColumns(table)) as PgColumn[];
  const inlinePkColumns = columns.filter((column) => column.primary);

  if (inlinePkColumns.length === 0) {
    // This function only ever sees inline `.primaryKey()` columns (see doc comment
    // above), so it cannot tell "genuinely no PK" apart from "PK declared via table
    // extras' `primaryKey({ columns: [...] })`" — the latter is a real PK, just not
    // one this pure module can see. Name both possibilities so a single-column extras
    // declaration doesn't send users hunting for a nonexistent missing PK.
    throw new Error(
      `Table "${getTableUniqueName(table)}" has no inline .primaryKey() column (composite/extras primaryKey() declarations are not supported — declare the PK inline)`,
    );
  }

  if (inlinePkColumns.length > 1) {
    throw new Error(`Table "${getTableUniqueName(table)}" has multiple primary keys`);
  }

  const [pkColumn] = inlinePkColumns;
  if (!pkColumn) {
    throw new Error(
      `Table "${getTableUniqueName(table)}" has no inline .primaryKey() column (composite/extras primaryKey() declarations are not supported — declare the PK inline)`,
    );
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
   * seeded a schema anywhere in the chain. This is runtime-safe ONLY if a schema is
   * later chained via `.args()` — `resolve()` schema-parses args when an `argsSchema`
   * is present, but otherwise substitutes an empty object rather than passing raw
   * client input through, so `ctx.args` is safe to read but will always be `{}` for a
   * query that never chains `.args(schema)`. Callers who want a fully-typed, non-empty
   * `ctx.args` use `pulse(t).query().args(schema).query(fn)` instead, where the
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
