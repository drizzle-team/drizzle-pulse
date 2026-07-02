# AGENTS.md — packages/drizzle-pulse

## Role

Type-safe realtime SDK shared by server, client, React, and embedded layers.

- server side: `createPulse`, `PulseBuilder`, `createPulseRegistry`, `expose`
- client side: `createPulseClient`, `PulseQuery`
- React side: `usePulseQuery`
- embedded side: `createPulseClient` (in-process, runtime-backed), `PulseCollection`

## Package Name

`drizzle-pulse` — workspace package with built `dist/` exports.

## Source Layout

| Path | Purpose |
|------|---------|
| `src/types.ts` | shared public types such as `QueryDescriptor`, `ResolvedPulseQuery`, `WhereClause`, `PullResponse`, `LoadMoreResponse` |
| `src/shared/` | protocol request/response types, filter AST helpers, PK utilities, `pulse-merge-core.ts` (merge state machine reused by HTTP `PulseQuery` and embedded `PulseCollection`) |
| `src/client/create-client.ts` | proxy-based typed client |
| `src/client/pulse-query.ts` | framework-agnostic subscribe/poll/load-more state machine (`PulseQuery` class) |
| `src/client/superjson.ts` | response deserialization helper |
| `src/client/react/use-pulse-query.ts` | `usePulseQuery` wrapper around `PulseQuery` |
| `src/server/pulse.ts` | `createPulse` factory |
| `src/server/pulse-builder.ts` | immutable query builder |
| `src/server/pulse-registry.ts` | registry finalization + `$client` phantom contract |
| `src/server/pulse-sql.ts` | query compilation / row predicate evaluation |
| `src/server/expose.ts` | Hono router + runtime assembly |
| `src/server/realtime-store.ts` | `RealtimeService` and `SubscriptionManager` |
| `src/server/wal-listener.ts` | logical replication listener |
| `src/client/embedded/index.ts` | in-process embedded client: `createPulseClient(runtime)` → `PulseCollection` live collections fed directly by the WAL tap (no HTTP) |
| `src/__tests__/` | runtime/unit tests for SDK internals |

## Contract Flow

```text
Server:
  createPulse()
    → pulse(table)
    → .args().order().limit().query(...)
    → PulseBuilder

  createPulseRegistry({ queryName })
    → runtime queries + phantom $client type contract

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
QueryDescriptor
type ColumnOperators, ResolvedPulseQuery, RealtimeEvent, WhereClause, WhereCondition
type SubscribeRequest, SubscribeResponse, PullRequest, PullResponse, LoadMoreRequest, LoadMoreResponse

// drizzle-pulse/client
createPulseClient
PulseQuery
deserializeResponse
type CreatePulseQueryOptions, PulseQueryState, PulseEvent, PulseInsertEvent, PulseUpdateEvent, PulseDeleteEvent

// drizzle-pulse/client/react
usePulseQuery
type UsePulseQueryResult

// drizzle-pulse/server
createPulse
createPulseRegistry
PulseBuilder
PulseRegistry
expose
RealtimeRuntime
RealtimeService
SubscriptionManager
buildSelectQuery

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
