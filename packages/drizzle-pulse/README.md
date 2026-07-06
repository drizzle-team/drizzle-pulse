# drizzle-pulse

Type-safe realtime SDK for Drizzle ORM — server-defined queries that stream live PostgreSQL changes (via WAL logical replication) to remote clients over HTTP polling, or directly in-process as live in-memory collections. One query definition, one merge implementation, two consumption paths.

## Install

```bash
npm install drizzle-pulse
```

`drizzle-pulse` declares peer dependencies your app must also install — see the [Compatibility](#compatibility) table below for exact ranges. At minimum:

```bash
npm install drizzle-orm zod
```

`react` is only required if you use the [`drizzle-pulse/client/react`](#drizzle-pulseclientreact) entrypoint.

## 60-second quickstart

**Schema** — export a `pulse(table)` collection once per table, alongside its source table:

```ts
// schema.ts
import { pulse } from 'drizzle-pulse';
import { pgTable, serial, text } from 'drizzle-orm/pg-core';

export const orders = pgTable('orders', {
  id: serial('id').primaryKey(),
  status: text('status').notNull(),
});

export const ordersCollection = pulse(orders);
```

**Server** — derive queries from the collection, register them, and expose over WAL:

```ts
// server.ts
import { createPulseRegistry, expose } from 'drizzle-pulse/server';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { ordersCollection } from './schema.js';

const activeOrders = ordersCollection
  .query() // convention-resolved events table — see "Events tables" below
  .order('asc')
  .query((ctx) => ctx.query({ status: 'active' }));

const registry = createPulseRegistry({ activeOrders });

const sourceDb = drizzle({ client: postgres(process.env.DATABASE_URL!) });

const runtime = expose(registry, {
  databaseUrl: process.env.DATABASE_URL!, // must have wal_level=logical
  sourceDb,
});

await runtime.start(); // rejects loudly if the publication, events tables, or
// REPLICA IDENTITY FULL aren't in place — see "Events tables" below

// wire runtime.handlers.subscribe / .pull / .loadMore / .unsubscribe into your HTTP router
```

**Client** — poll the server over HTTP with `createPulseClient`:

```ts
// client.ts
import { createPulseClient } from 'drizzle-pulse/client';

const client = createPulseClient<{
  activeOrders: (args?: Record<string, unknown>) => import('drizzle-pulse').QueryDescriptor<Order>;
}>({
  url: 'https://api.example.com',
});

const query = client.activeOrders();
// consume `query` with `PulseQuery` (framework-agnostic) or `usePulseQuery` (React) — see below
```

## `drizzle-pulse`

The root entrypoint owns the collection: `pulse(table)` wraps a Drizzle table into a `PulseTable`, exported once from your schema file (the cross-package contract drizzle-kit recognizes for codegen).

- `pulse(table)` — returns a `PulseTable` wrapping `table`; construct unconditionally, at schema-definition time
- `PulseTable` — the collection; call `.query(fn?)` to derive a query (`.query()` for match-all, `.query(fn)` to seed a where-clause)
- `isPulseTable(value)` / `getPulseTableConfig(entity)` — recognition + payload accessors (used by drizzle-kit; rarely needed directly)

## `drizzle-pulse/server`

Derive queries from collections outside the schema file, register them, and expose a WAL-fed runtime that serves subscribe/pull requests.

- `PulseBuilder` — fluent builder returned by `.query()`: `.columns()`, `.args(zodSchema)`, `.query(ctx => WhereClause)`, `.order()`, `.limit()`, `.transform()`
- Queries that read `ctx.args` in their `queryFn` MUST chain `.args(zodSchema)` first. Without a schema, `ctx.args` is always `{}` at runtime (the registry never forwards unvalidated client input as args) — reading `ctx.args` on a schemaless query silently sees no fields rather than attacker-controlled data.
- `.columns()` must be called before `.transform()` in the chain — calling it after throws, rather than silently discarding the transform.
- `createPulseRegistry(queries)` — collects builders into a `PulseRegistry`; rejects a bare `PulseTable` (queries must be derived via `.query()` first)
- `expose(registry, config)` — returns a `RealtimeRuntime`; call `.start()` to connect WAL, `runtime.handlers.{subscribe,pull,loadMore,unsubscribe}` to serve requests
- `RealtimeRuntime` — WAL listener + request handlers; `.start()` / `.stop()`. Subscriptions not explicitly released via `unsubscribe` are evicted by an idle sweep after `subscriptionTtl.idleMs` (default 24h, checked every `subscriptionTtl.sweepIntervalMs`, default 5m) of no `pull()` activity — both configurable via `expose(registry, { subscriptionTtl: { idleMs, sweepIntervalMs } })`.

```ts
import { pulse } from 'drizzle-pulse';
import { createPulseRegistry, expose } from 'drizzle-pulse/server';

const ordersByStatus = pulse(orders)
  .query()
  .args(z.object({ status: z.string() }))
  .order('desc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

const registry = createPulseRegistry({ ordersByStatus });
const runtime = expose(registry, { databaseUrl, sourceDb }); // publication/slot default to 'drizzle_pulse'
await runtime.start();
```

### Events tables

Every pulsed source table needs a matching **events table** — WAL changes are persisted there and replayed to clients. Events tables are generated infrastructure, resolved entirely by convention (no hand-declared Drizzle table, no `.$eventsTable()` linkage):

- **Location:** `<eventsSchema>.__events_<sourceSchema>_<sourceTable>` — `eventsSchema` defaults to `'drizzle'` (override via `expose()`'s `eventsSchema` option, matching your drizzle.config `migrations.schema`)
- **Creation:** as of this version, generate them from `emitEventsTableDdl(sourceTable)` (exported from `drizzle-pulse/server`) and run the returned `CREATE SCHEMA`/`CREATE TABLE` statements as a migration; Phase 14+ drizzle-kit codegen automates this from your `pulse()` schema exports
- **Startup guard:** `runtime.start()` asserts — in one aggregated, loudly-thrown error listing every unmet precondition — that the publication exists, every pulsed table is a member of it (or the publication is `FOR ALL TABLES`), every events table exists, every pulsed table has `REPLICA IDENTITY FULL`, and `wal_level = logical`

See [`docs/events-table-convention.md`](../../docs/events-table-convention.md) for the full name-derivation and column-mapping contract.

## `drizzle-pulse/client`

The framework-agnostic HTTP polling client. `createPulseClient` returns a typed proxy of query-descriptor factories; wrap a descriptor in `PulseQuery` to subscribe and poll (~1s by default).

```ts
import { createPulseClient, PulseQuery } from 'drizzle-pulse/client';

const client = createPulseClient<MyClientContract>({ url: 'https://api.example.com' });

const query = new PulseQuery(client.ordersByStatus({ status: 'active' }));
await query.subscribe();
query.getState(); // { data, isLoading, isLoadingMore, hasMore, error }
```

## `drizzle-pulse/client/react`

A React hook wrapping `PulseQuery` with automatic subscribe/cleanup on mount/unmount.

```tsx
import { usePulseQuery } from 'drizzle-pulse/client/react';
import { createPulseClient } from 'drizzle-pulse/client';

const client = createPulseClient<MyClientContract>({ url: 'https://api.example.com' });

function OrdersList() {
  const { data, isLoading, error, loadMore, hasMore } = usePulseQuery(
    client.ordersByStatus({ status: 'active' }),
  );

  if (isLoading) return <p>Loading…</p>;
  if (error) return <p>Error: {error.message}</p>;

  return (
    <ul>
      {data.map((order) => (
        <li key={order.$pk as string}>{order.status}</li>
      ))}
      {hasMore && <button onClick={loadMore}>Load more</button>}
    </ul>
  );
}
```

## `drizzle-pulse/client/embedded`

For server-side consumers that live in the same process as the WAL runtime: `createPulseClient(runtime)` returns live, WAL-fed `PulseCollection`s with no HTTP round trip and no additional DB reads after the initial baseline.

```ts
import { createPulseClient } from 'drizzle-pulse/client/embedded';

const client = createPulseClient(runtime); // same RealtimeRuntime from drizzle-pulse/server

const collection = await client.ordersByStatus({ status: 'active' });

collection.list(); // synchronous full filtered set
collection.onChange(({ events, state }) => {
  console.log('changed:', events, 'now:', state);
});

collection.dispose(); // stop the collection when done
```

`drizzle-pulse/client` and `drizzle-pulse/client/embedded` are two import-path-selected flavors of the same `createPulseClient` concept: pick `/client` for remote/browser consumers over HTTP, or `/client/embedded` for in-process server-side consumers with synchronous live data.

## Compatibility

| Dependency | Range | Notes |
|---|---|---|
| `drizzle-orm` | `^1.0.0-rc.4` | Tested against `1.0.0-rc.4` |
| `zod` | `^4.0.0` | |
| `react` | `>=18.0.0` | Optional — only required for `drizzle-pulse/client/react` |
| `node` | `>=20` | |
| PostgreSQL | `16` | Requires `wal_level=logical` |

## Transport & guardrails

- **Transport:** HTTP polling only (`drizzle-pulse/client`, `drizzle-pulse/client/react`) — no SSE or WebSocket transport.
- **Embedded collections** (`drizzle-pulse/client/embedded`) are in-process only: no `limit`/pagination (the full filtered set is materialized), no `.transform()` (transforms stay HTTP-only), and no dedupe (each call creates an independent collection — create once, hold, `dispose()`).
