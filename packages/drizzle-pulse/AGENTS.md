# AGENTS.md — packages/drizzle-pulse

## Role

Type-safe realtime SDK shared by server, client, React, and embedded layers.

- root side: `pulse`, `PulseTable` — the collection entity, exported once per table from schema files
- server side: `PulseBuilder` (seeded via `PulseTable.query(fn?)`), `createPulseRegistry`, `expose`, the transport-agnostic request handler (SDK), and the events-table machinery (`buildEventsTable` resolver + internal DDL renderer + `reconcile`/`provision`)
- server/router side: `createRealtimeRouter` — an optional Hono wrapper over the SDK on the `./server/router` subpath
- client side: `createPulseClient`, `PulseQuery` (over a pluggable transport)
- React side: `usePulseQuery`
- embedded side: `createPulseClient` (in-process, runtime-backed), `PulseCollection` facade

## Package Name

`drizzle-pulse` — workspace package with built `dist/` exports.

## Dependencies

`pg` and `pg-logical-replication` are hard `dependencies`, imported only by the `./server`
entrypoint. Client-only consumers (`./client`, `./client/react`, `./client/embedded`) install
them but never bundle them — nothing on a client value-import path references them (enforced by
the platform-imports purity test).

`hono` and `react` are **optional peer dependencies**: `hono` is needed only to mount the
`./server/router` Hono wrapper (the SDK handler itself is transport-agnostic), `react` only for
`./client/react`. `drizzle-orm` and `zod` are required peers.

## Source Layout

| Path | Purpose |
|------|---------|
| `src/index.ts` | root barrel: exports `pulse`, `PulseTable`, and the public protocol/event/query **types** only |
| `src/types.ts` | shared public types such as `QueryDescriptor`, `ResolvedPulseQuery`, `WhereClause`, `PullResponse`, `LoadMoreResponse`, `PulseAuthContext` |
| `src/pulse-table.ts` | collection entity: `pulse(table)` → `PulseTable`; internal `isPulseTable()`/`getPulseTableConfig()` (test-only now, no longer public); lazy PK validation at `.query()` time; value-imports `drizzle-orm/pg-core` (`getTableConfig`) — the sole client-unreachable pg-core exemption in the purity test |
| `src/shared/` | protocol request/response types, filter AST helpers, PK utilities, `pulse-merge-core.ts` (merge state machine reused by HTTP `PulseQuery` and embedded `PulseCollection`) |
| `src/client/create-client.ts` | proxy-based typed HTTP client + `PullClient` (batched auto-poll, default 1s, `pollIntervalMs: 0` disables) |
| `src/client/transport.ts` | `PulseQueryTransport` interface (`subscribe`/`pull`/`loadMore`) + `createHttpTransport` (fetch+superjson); decouples `PulseQuery` from how requests travel |
| `src/client/pulse-query.ts` | framework-agnostic subscribe/poll/load-more state machine (`PulseQuery`); `destroy()` stops polling — the client holds no server-side state to release |
| `src/client/superjson.ts` | response deserialization helper |
| `src/client/react/use-pulse-query.ts` | `usePulseQuery` wrapper around `PulseQuery` |
| `src/client/embedded/index.ts` | in-process embedded client: `createPulseClient(runtime)` → `PulseCollection` facade (`list`/`getState`/`loadMore`/`onChange`/`dispose`) over a `PulseQuery` on a direct in-process transport; re-pulls on the runtime's WAL change signal (no polling interval) |
| `src/server/pulse-builder.ts` | immutable query builder (`.columns/.args/.order/.limit/.transform/.query`), seeded by `PulseTable.query(fn?)` |
| `src/server/pulse-registry.ts` | registry finalization + `$client` phantom contract; rejects a bare `PulseTable`; defensive composite-PK re-check |
| `src/server/pulse-projection.ts` | projection/response-shaping helpers split out to preserve platform purity for the embedded client entrypoint |
| `src/server/events-table-resolver.ts` | convention resolver: synthesizes the events `PgTable` (`<schema>_<table>`, `_` escaped to `__`) from a source table by cloning each column via its public `toBuilder()` |
| `src/server/events-table-ddl.ts` | internal `emitEventsTableDdl`: renders the recreate DDL (`CREATE SCHEMA`/`DROP TABLE`/`CREATE TABLE`) strictly from the resolver's output; `reconcile()` hashes its text to detect divergence (not a public export) |
| `src/server/cursor.ts` | opaque cursor tokens `"<epoch>:<snapshot>"` — `formatCursor`/`parseCursor`; epoch rotates on events-table recreate so stale tokens are detectable |
| `src/server/pulse-sql.ts` | query compilation / row predicate evaluation |
| `src/server/sdk.ts` | `RealtimeRequestHandler` — the transport-agnostic SDK core: subscribe/pull/loadMore, cursor-token mint/validate, `DEFAULT_PULL_EVENT_LIMIT` overflow→reset. Stateless: auth re-resolved per pull, no subscription registry |
| `src/server/router.ts` | `createRealtimeRouter` — optional Hono wrapper over the SDK's three routes (`/subscribe`, `/pull`, `/load-more`); superjson-encoded responses; `./server/router` subpath |
| `src/server/expose.ts` | `RealtimeRuntime` assembly, `ExposeConfig` (publication/slot default `drizzle_pulse`, `eventsSchema`, `pullEventLimit`, `logLevel`), `reconcile()` self-provisioning + `provision()`, WAL listener lifecycle |
| `src/server/realtime-store.ts` | `RealtimeService` — events-table reads/writes over the pulse-owned pool |
| `src/__tests__/` | runtime/unit tests for SDK internals |

## Provisioning (reconcile / provision)

The runtime **self-provisions all its infrastructure** — the app no longer migrates events
tables. `RealtimeRuntime.reconcile()` runs inside one transaction under a per-events-schema
advisory lock and: asserts `wal_level=logical` (the one precondition it can't fix), sets
`REPLICA IDENTITY FULL` on each source (resets to `DEFAULT` on un-pulse), creates/diffs the
publication, creates the events schema + `pulse_meta` bookkeeping, creates/recreates each events
table whose rendered-DDL sha256 diverges (rotating its epoch), and sweeps orphans. `start()`
runs it then opens WAL; `provision()` runs it and returns (split-role deploys). Failed DDL throws
naming the exact statement and the grant it most likely needs. See
[`../../docs/events-table-convention.md`](../../docs/events-table-convention.md) sections 5–8.

## Contract Flow

```text
Schema (root barrel):
  pulse(table)
    → PulseTable (unconditional construction; no PK validation yet)

Server (derive queries outside the schema file):
  collection.query(fn?)
    → PulseBuilder (lazy PK validation here) → .args().order().limit().query(...)

  createPulseRegistry({ queryName })
    → rejects a bare PulseTable; defensive composite-PK re-check

  expose(registry, config) → RealtimeRuntime
    → resolves each source table's events table via buildEventsTable (no hand-declared
      events tables)
    → start() runs reconcile() (self-provision), then connects WAL
    → provision() runs reconcile() only (no WAL) — for elevated-role deploy steps

  runtime.handlers → transport-agnostic SDK (subscribe/pull/loadMore)
  createRealtimeRouter(runtime.handlers)  [optional Hono wrapper, ./server/router]

Client:
  createPulseClient<RealtimeClient>({ url }) → QueryDescriptor<TResult>
  new PulseQuery(descriptor, { onStateChange }) → subscribe / poll / loadMore

React:
  usePulseQuery(descriptor) → { data, isLoading, isLoadingMore, hasMore, error, loadMore, refetch }

Embedded (in-process):
  createPulseClient(runtime).queryName(args?, { auth? })
    → PulseCollection facade over a PulseQuery on a direct transport
    → list() / getState() / loadMore() / onChange() / dispose()
```

## Runtime Notes

- `PulseQuery` is the canonical merge engine used by tests, `usePulseQuery`, and the embedded facade
- The server is **stateless**: no `SubscriptionManager`, no `unsubscribe`, no TTL/idle sweep; auth is re-resolved per pull
- Cursor tokens are opaque `"<epoch>:<snapshot>"`; a pull echoes its token, the handler compares its epoch to the current one. A recreate rotates the epoch → stale tokens reset (re-baseline). A pull exceeding `pullEventLimit` (default 1000) also resets.
- Every migration that changes a pulsed table's shape recreates its events table and thus resets that table's subscribers (accepted design)
- range tracking uses PK boundaries (`rangeStart`, `rangeEnd`) plus a monotonic `snapshot`; SuperJSON crosses the server/client boundary

## Public Imports

```ts
// drizzle-pulse (root)
pulse, PulseTable
QueryDescriptor
type ColumnOperators, ResolvedPulseQuery, WhereClause, WhereCondition
type RealtimeEvent, RealtimeInsertEvent, RealtimeUpdateEvent, RealtimeDeleteEvent
type SubscribeRequest, SubscribeResponse, PullRequest, PullSubscriptionRequest
type PullResponse, PullIncrementalResponse, PullResetResponse, PullResponseError, PullResponseErrorResult
type LoadMoreRequest, LoadMoreResponse

// drizzle-pulse/server
expose, RealtimeRuntime, type ExposeConfig, WalListenerConfig
createPulseRegistry, PulseRegistry
PulseBuilder, type AnyPulseBuilder, AnyQueries
buildEventsTable, getEventsTableName, DEFAULT_EVENTS_SCHEMA
buildSelectQuery
RealtimeService
serializeResponse
applyColumnFilter, type PulseClientContract, PulseQueryContext, WithPk, ColumnsSelection, ...
type PulseAuthContext

// drizzle-pulse/server/router
createRealtimeRouter, type PulseRouterHandlers

// drizzle-pulse/client
createPulseClient
PulseQuery
deserializeResponse
type CreatePulseQueryOptions, PulseQueryState, PulseEvent, PulseInsertEvent, PulseUpdateEvent, PulseDeleteEvent

// drizzle-pulse/client/react
usePulseQuery
type UsePulseQueryResult

// drizzle-pulse/client/embedded
createPulseClient
PulseCollection
type EmbeddedPulseClient, PulseCollectionOptions, PulseCollectionChange, PulseRow
```

`buildEventsTable` moved off the root (its only cross-package consumer, drizzle-kit, is gone) and
is now server-only. `isPulseTable`/`getPulseTableConfig` are no longer exported from any
entrypoint.

## Import Rules

- Internal source imports require explicit `.js` extensions
- Keep server-only code out of the `client`, `client/react`, and `client/embedded` entrypoints (the embedded client reaches the runtime through `import type` edges only)
- If `PulseQuery`, the transport interface, or protocol types change, update the SDK handler, both transports, and client consumers together

## DO NOT

- ❌ Import server modules from client/runtime codepaths
- ❌ Change merge/event semantics in `PulseQuery` without updating integration coverage
- ❌ Forget that `usePulseQuery` and the embedded `PulseCollection` are thin wrappers over `PulseQuery`, not separate merge implementations
- ❌ Re-export `buildEventsTable` / the DDL renderer from the root or client entrypoints — they are server-only and pull in `drizzle-orm/pg-core`
