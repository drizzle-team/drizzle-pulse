# Events-table convention

This document pins the naming and column-derivation contract for the events table each
`pulse()`-tracked source table gets, by convention, with no hand-declared mirror table.
It is the interface contract two other consumers rely on byte-for-byte:

- **drizzle-kit** (Phase 15, `~/dev/drizzle-orm/drizzle-kit`) generates the equivalent
  `CREATE TABLE` migration from the same source table and must match this document
  exactly.
- **The cross-repo conformance test** (INTG-01) diffs drizzle-kit's generated SQL
  against `emitEventsTableDdl`'s output and fails on any drift.

The normative implementation is
[`packages/drizzle-pulse/src/server/events-table-resolver.ts`](../packages/drizzle-pulse/src/server/events-table-resolver.ts)
(`resolveEventsTable`, `getEventsTableName`, `DEFAULT_EVENTS_SCHEMA`). The SQL rendering
reference — the exact text Phase 15's generated migrations must byte-match — is
[`packages/drizzle-pulse/src/server/events-table-ddl.ts`](../packages/drizzle-pulse/src/server/events-table-ddl.ts)
(`emitEventsTableDdl`). Both are pure functions: no database connection, no I/O.

## 1. Name derivation

The events table name is:

```
__events_<sourceSchema>_<sourceTable>
```

- `sourceSchema` is the source table's schema; if the source table has no explicit
  schema (i.e. it lives in `public`), `sourceSchema` falls back to the literal string
  `public`.
- `sourceTable` is the source table's SQL name (not its JS export name).

The events table itself lives in a separate **events schema** — never in the source
table's own schema, and never in a dedicated `realtime` schema (the pre-restructure
convention). The events schema defaults to **`drizzle`**, matching a project's
`drizzle.config` `migrations.schema` by manual parity. It is configured at runtime via
`ExposeConfig.eventsSchema` — the runtime never loads `drizzle.config` itself, so the
value must be kept in sync by the project author (or by drizzle-kit's Phase 15
codegen, which reads `drizzle.config` directly).

**Worked example:** a source table `orders` in the default `public` schema, with
`eventsSchema` left at its default, resolves to:

```
"drizzle"."__events_public_orders"
```

## 2. Column derivation

Every source column produces **two** columns in the events table: a new-value column
and an `$old_` twin. The table also gets three fixed metadata columns (section 3).

### 2.1 New-value columns

One new-value column per source column, same SQL name, same type/mode/enum/length/
precision/dimensions/srid as the source column — with two systematic differences:

- **Nullability:** every new-value column is nullable, **except** the source table's
  primary-key column, which stays `NOT NULL`. This holds regardless of the source
  column's own nullability — a `NOT NULL` non-PK source column still becomes nullable
  in the events table (an events row only ever has one "side" populated on insert/
  update/delete, so blanket-nullable is required for every non-PK column).
- **No constraints carried over:** `PRIMARY KEY`, `DEFAULT`, `GENERATED` and identity
  clauses are all stripped from every cloned column — the events table has no primary
  key of its own (see section 3 for the one exception, `$snapshot`).

### 2.2 Serial-family relaxation

Serial-family source columns are relaxed to their plain integer/bigint equivalent,
because a literal clone would carry over auto-increment/identity semantics that reject
explicit insert values (the events writer always supplies `$snapshot` separately and
the row's actual primary-key value explicitly):

| Source column type | Events-table column type |
|---|---|
| `serial` | `integer` |
| `smallserial` | `integer` (**not** `smallint` — see worked example below) |
| `bigserial` (`mode: 'number'`) | `bigint` (`mode: 'number'`) |
| `bigserial` (`mode: 'bigint'`) | `bigint` (`mode: 'bigint'`) |

Every other column type (including `bigint`/`numeric`/`decimal` in all their modes,
`varchar`/`char` with length or enum constraints, `timestamp`/`time` with or without
timezone, `point`/`line`/`geometry` in either mode, `vector`/`halfvec`/`sparsevec` with
their dimensions, and `bit` with its dimensions) is preserved exactly — same
`getSQLType()`, same driver-value codec as the source column.

A `.array()` source column (any dimension) is likewise preserved exactly: the cloned
column keeps the source's element type, dimension count, and array (de)serialization
codec, and the emitted DDL renders the element type followed by one `[]` per dimension
(e.g. a 1-dimensional `text('tags').array()` renders as `"tags" text[]`).

### 2.3 `$old_<sqlColumnName>` twins

Each source column also produces an `$old_`-prefixed twin, keyed off the source
column's **SQL name** (not its JS property name): source column `small_int_col`
produces twin `$old_small_int_col`. The twin is always nullable and uses the same
relaxed type as the new-value column (i.e. the twin of a `serial` PK is a nullable
`integer`, not a `serial`).

### 2.4 Enum columns

An enum source column's new-value column and its `$old_` twin both **reference the
same `pgEnum` instance** the source column uses — the enum type is never redeclared.

### 2.5 Worked examples

Lifted directly from the `pg-data-types` integration fixture
(`packages/integration-tests/src/fixtures/pg-data-types/schema.ts`), which is the
byte-level acceptance spec for this section:

```ts
// Source table
id: serial('id').primaryKey(),                                    // PK, serial
smallSerialCol: smallserial('small_serial_col'),                  // non-PK, smallserial
bigSerialNumberCol: bigserial('big_serial_number_col', { mode: 'number' }),
moodCol: moodEnum('mood_col'),

// Events table (new-value columns)
id: integer('id').notNull(),                                      // PK stays NOT NULL, serial -> integer
smallSerialCol: integer('small_serial_col'),                      // nullable, smallserial -> integer
bigSerialNumberCol: bigint('big_serial_number_col', { mode: 'number' }),
moodCol: moodEnum('mood_col'),                                    // same enum instance

// Events table ($old_ twins)
$old_id: integer('$old_id'),
$old_small_serial_col: integer('$old_small_serial_col'),
$old_big_serial_number_col: bigint('$old_big_serial_number_col', { mode: 'number' }),
$old_mood_col: moodEnum('$old_mood_col'),                         // same enum instance again
```

### 2.6 Enum type identifiers in emitted DDL

An enum column's SQL type is a developer-controlled identifier, not a fixed type
keyword, so `emitEventsTableDdl` renders it like any other identifier: quoted, and
schema-qualified when the enum was declared in a non-default schema (e.g.
`"app"."order_status"` for `pgSchema('app').enum('order_status', ...)`, or
`"order_status"` for an enum with no schema). Every other column type keyword
(`integer`, `timestamp with time zone`, ...) is rendered unquoted, as returned by
`getSQLType()`.

## 3. Metadata columns

Every events table gets exactly these three columns, in addition to the new-value and
`$old_` twin columns above:

| Column | Type | Notes |
|---|---|---|
| `$snapshot` | `integer` | `GENERATED ALWAYS AS IDENTITY` — the only identity column on the table; a monotonic ordinal used as the event stream's watermark/cursor. |
| `$op` | `text` | `NOT NULL` — one of `insert` \| `update` \| `delete` \| `snapshot`. |
| `$timestamp` | `timestamp with time zone` | `NOT NULL DEFAULT now()` — the only column in the table with a runtime default. |

These three are built with plain column builders (not config-clones of any source
column) — they are constant across every events table regardless of the source
table's shape.

## 4. Identifier length guard (error contract)

Postgres silently truncates identifiers over 63 bytes, which would let the resolver
and a DDL statement disagree on the real table/column name without either side
noticing (a `CREATE TABLE` targeting one truncated name while the runtime queries a
different truncated name — a silent zero-events failure). To avoid this, the resolver
checks every identifier it derives — the events table name and every `$old_<name>`
twin — and **throws immediately** if it exceeds 63 bytes (UTF-8), rather than letting
Postgres truncate it:

```
Derived identifier "<identifier>" is <N> bytes, exceeding Postgres's 63-byte identifier limit
```

New-value column names are not re-checked, because they reuse the source column's SQL
name verbatim — if it fit in the source table, it fits here too. Only names the
resolver actually *derives* (the table name and the `$old_` prefix) can newly exceed
the limit.

**This is a hard failure, not a warning.** There is no deterministic truncation or
shortening scheme in this phase — an events table (or column) whose derived name
overflows 63 bytes cannot be created at all until a shortening scheme ships. Phase 15's
kit implementation must mirror this exact loud-failure behavior (same error shape) until
a truncation scheme is designed; silently truncating on the kit side while the runtime
rejects would reintroduce the exact drift this guard exists to prevent.

## 5. Reference implementation

- [`events-table-resolver.ts`](../packages/drizzle-pulse/src/server/events-table-resolver.ts)
  is normative for every rule in sections 1–4. `resolveEventsTable(sourceTable, options?)`
  returns a genuine Drizzle `PgTable` — the runtime inserts events into it directly
  (`db.insert(eventsTable).values(...)` in `realtime-store.ts`), so codec-faithful
  column reconstruction (reusing each source column's own `mapToDriverValue`/
  `mapFromDriverValue`) is part of this contract, not an implementation detail.
- [`events-table-ddl.ts`](../packages/drizzle-pulse/src/server/events-table-ddl.ts)'s
  `emitEventsTableDdl(sourceTable, options?)` is the **byte-match reference** for
  Phase 15 (GEN-01): it derives every column strictly from `resolveEventsTable`'s
  output — never re-deriving names or types from the source table independently — so
  the resolver and its DDL can never drift from each other. Its output is also what
  this repo's integration-test harness uses to create events tables in test databases
  (no hand-mirrored SQL fixtures).
- Unit tests over the full edge-case type matrix live in
  [`events-table-resolver.test.ts`](../packages/drizzle-pulse/src/__tests__/events-table-resolver.test.ts).
  Any change to the derivation rules above must keep those tests passing, and any
  change to the tests must be reflected back into this document.
