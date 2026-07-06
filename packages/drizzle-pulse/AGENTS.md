# AGENTS.md — packages/drizzle-pulse

## Role

Type-safe realtime SDK shared by server, client, React, and embedded layers.

- root side: `pulse`, `PulseTable`, `isPulseTable`, `getPulseTableConfig` — the collection entity, exported once per table from schema files
- server side: `PulseBuilder` (seeded via `PulseTable.query(fn?)`), `createPulseRegistry`, `expose`, events-table convention resolver (`resolveEventsTable`, `emitEventsTableDdl`)
- client side: `createPulseClient`, `PulseQuery`
- React side: `usePulseQuery`
- embedded side: `createPulseClient` (in-process, runtime-backed), `PulseCollection`

## Package Name

`drizzle-pulse` — workspace package with built `dist/` exports.

## Dependencies

`pg`, `pg-logical-replication`, and `hono` are hard `dependencies` even though only the `./server` entrypoint imports them. Client-only consumers (`./client`, `./client/react`, `./client/embedded` type surface) therefore install them but never bundle them — nothing on a client value-import path references them (enforced by the platform-imports purity test). Making them optional peer dependencies (as done for `react`) would slim client installs but changes consumer install semantics; that decision is deliberately deferred to the publish-readiness pass and must not be made silently.

## Source Layout

| Path | Purpose |
|------|---------|
| `src/types.ts` | shared public types such as `QueryDescriptor`, `ResolvedPulseQuery`, `WhereClause`, `PullResponse`, `LoadMoreResponse` |
| `src/pulse-table.ts` | root-barrel collection entity: `pulse(table)` → `PulseTable`, `isPulseTable()`, `getPulseTableConfig()`; lazy PK validation at `.query()` time; platform-pure (bare `drizzle-orm` value imports only, no `pg-core`) |
| `src/shared/` | protocol request/response types, filter AST helpers, PK utilities, `pulse-merge-core.ts` (merge state machine reused by HTTP `PulseQuery` and embedded `PulseCollection`) |
| `src/client/create-client.ts` | proxy-based typed client |
| `src/client/pulse-query.ts` | framework-agnostic subscribe/poll/load-more state machine (`PulseQuery` class) |
| `src/client/superjson.ts` | response deserialization helper |
| `src/client/react/use-pulse-query.ts` | `usePulseQuery` wrapper around `PulseQuery` |
| `src/server/pulse-builder.ts` | immutable query builder (`.columns/.args/.order/.limit/.transform/.query`), seeded by `PulseTable.query(fn?)` |
| `src/server/pulse-registry.ts` | registry finalization + `$client` phantom contract; rejects a bare `PulseTable`; defensive composite-PK re-check |
| `src/server/pulse-projection.ts` | projection/response-shaping helpers split out of `pulse-registry.ts` to preserve platform purity for the embedded client entrypoint |
| `src/server/events-table-resolver.ts` | convention resolver: synthesizes the events `PgTable` (`__events_<schema>_<table>`) from a source table by cloning each column's own class/config |
| `src/server/events-table-ddl.ts` | DDL emitter: derives `CREATE SCHEMA`/`CREATE TABLE` SQL strictly from the resolver's output (D-09 — no hand-mirrored SQL) |
| `src/server/pulse-sql.ts` | query compilation / row predicate evaluation |
| `src/server/expose.ts` | `RealtimeRuntime` assembly, `ExposeConfig` defaults (`drizzle_pulse` publication/slot, `drizzle` events schema), the aggregating startup guard, WAL listener lifecycle |
| `src/server/handlers.ts` | `RealtimeRequestHandler` — subscribe/pull/loadMore request handling, consumes an injected `getEventsTable(queryName)` lookup |
| `src/server/realtime-store.ts` | `RealtimeService` and `SubscriptionManager` |
| `src/client/embedded/index.ts` | in-process embedded client: `createPulseClient(runtime)` → `PulseCollection` live collections fed directly by the WAL tap (no HTTP) |
| `src/__tests__/` | runtime/unit tests for SDK internals |

## Contract Flow

```text
Schema (root barrel):
  pulse(table)
    → PulseTable (unconditional construction; no PK validation yet)

Server (derive queries outside the schema file):
  collection.query(fn?)
    → PulseBuilder (lazy PK validation happens here)
    → .args().order().limit().query(...)
    → PulseBuilder

  createPulseRegistry({ queryName })
    → rejects a bare PulseTable; defensive composite-PK re-check
    → runtime queries + phantom $client type contract

  expose(registry, config)
    → RealtimeRuntime resolves each source table's events table via
      resolveEventsTable(sourceTable, { eventsSchema }) — no hand-declared
      events tables, no registry-stored eventsTable field
    → runtime.start() runs the aggregating startup guard (publication
      exists, membership, events tables exist, REPLICA IDENTITY FULL,
      wal_level=logical) before connecting WAL

Client:
  createPulseClient<RealtimeClient>({ url })
    → QueryDescriptor<TResult>

  new PulseQuery(descriptor, { onStateChange })
    → subscribe / poll / loadMore runtime

React:
  usePulseQuery(descriptor)
    → { data, isLoading, isLoadingMore, hasMore, error, loadMore, refetch }
```

## Runtime Notes

- `PulseQuery` is the canonical merge engine used by both tests and `usePulseQuery`
- subscribe responses include `order` and `limit`; pull responses may include `{ reset, reason }`
- range tracking uses PK boundaries (`rangeStart`, `rangeEnd`) plus a monotonic `snapshot`
- `load-more` uses a cursor request and updates the subscription's stored range
- SuperJSON is used across server/client boundaries

## Public Imports

```ts
// drizzle-pulse (root)
pulse, PulseTable, isPulseTable, getPulseTableConfig
QueryDescriptor
type ColumnOperators, ResolvedPulseQuery, WhereClause, WhereCondition
type RealtimeEvent, RealtimeInsertEvent, RealtimeUpdateEvent, RealtimeDeleteEvent
type SubscribeRequest, SubscribeResponse, PullRequest, PullSubscriptionRequest
type PullResponse, PullIncrementalResponse, PullResetResponse, PullResponseError, PullResponseErrorResult
type LoadMoreRequest, LoadMoreResponse

// drizzle-pulse/client
createPulseClient
PulseQuery
deserializeResponse
type CreatePulseQueryOptions, PulseQueryState, PulseEvent, PulseInsertEvent, PulseUpdateEvent, PulseDeleteEvent

// drizzle-pulse/client/react
usePulseQuery
type UsePulseQueryResult

// drizzle-pulse/server
createPulseRegistry
PulseBuilder
PulseRegistry
expose
RealtimeRuntime
RealtimeService
SubscriptionManager
buildSelectQuery
resolveEventsTable, getEventsTableName, DEFAULT_EVENTS_SCHEMA, emitEventsTableDdl
type ExposeConfig, WalListenerConfig

// drizzle-pulse/client/embedded
createPulseClient
PulseCollection
type EmbeddedPulseClient, PulseCollectionOptions, PulseCollectionChange, PulseRow
```

## Import Rules

- Internal source imports require explicit `.js` extensions
- Keep server-only code out of the `client` and `client/react` entrypoints
- If `PulseQuery` or protocol types change, update both runtime handlers and client consumers together

## DO NOT

- ❌ Import server modules from client/runtime codepaths
- ❌ Change merge/event semantics in `PulseQuery` without updating integration coverage
- ❌ Forget that `usePulseQuery` is a thin wrapper over `PulseQuery`, not a separate merge implementation
