# drizzle-pulse

Type-safe Pulse SDK for Drizzle ORM — server-defined queries that stream live PostgreSQL changes (via WAL logical replication) to remote clients over HTTP polling, or directly in-process as live in-memory collections. One query definition, one merge implementation, two consumption paths.

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

await runtime.start(); // self-provisions its infrastructure (publication, events tables,
// REPLICA IDENTITY FULL) inside one transaction on first boot — see "Events tables" below

// wire runtime.handlers.subscribe / .pull / .loadMore into your HTTP router, or mount the
// optional first-party Hono router — see "drizzle-pulse/server/router" below
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

The root entrypoint owns the collection: `pulse(table)` wraps a Drizzle table into a `PulseTable`, exported once from your schema file.

- `pulse(table)` — returns a `PulseTable` wrapping `table`; construct unconditionally, at schema-definition time
- `PulseTable` — the collection; call `.query(fn?)` to derive a query (`.query()` for match-all, `.query(fn)` to seed a where-clause)

## `drizzle-pulse/server`

Derive queries from collections outside the schema file, register them, and expose a WAL-fed runtime that serves subscribe/pull requests.

- `PulseBuilder` — fluent builder returned by `.query()`: `.columns()`, `.args(zodSchema)`, `.query(ctx => WhereClause)`, `.order()`, `.limit()`, `.transform()`
- Queries that read `ctx.args` in their `queryFn` MUST chain `.args(zodSchema)` first. Without a schema, `ctx.args` is always `{}` at runtime (the registry never forwards unvalidated client input as args) — reading `ctx.args` on a schemaless query silently sees no fields rather than attacker-controlled data.
- `.columns()` must be called before `.transform()` in the chain — calling it after throws, rather than silently discarding the transform.
- `createPulseRegistry(queries)` — collects builders into a `PulseRegistry`; rejects a bare `PulseTable` (queries must be derived via `.query()` first)
- `expose(registry, config)` — returns a `PulseRuntime`; call `.start()` to self-provision infrastructure and connect WAL, `runtime.handlers.{subscribe,pull,loadMore}` to serve requests
- `PulseRuntime` — WAL listener + request handlers; `.start()` / `.stop()`. The server is stateless: each pull re-resolves auth and validates its own opaque cursor token, so there's no per-subscription server state, no TTL, and no `unsubscribe` — a client simply stops pulling.
- `PulseRuntime.provision()` — runs the same infrastructure reconciliation as `.start()` without opening the WAL stream, for split-role deploys (see "Provisioning & privileges" below)

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

Every pulsed source table gets a matching **events table** — WAL changes are persisted there and replayed to clients. Events tables are runtime-owned infrastructure, resolved entirely by convention (no hand-declared Drizzle table, no `.$eventsTable()` linkage, no drizzle-kit migration):

- **Location:** `<eventsSchema>.<sourceSchema>_<sourceTable>`, with each component's `_` doubled to `__` before joining — `eventsSchema` defaults to `'drizzle_pulse'` (override via `expose()`'s `eventsSchema` option)
- **Self-provisioning:** `runtime.start()` reconciles everything itself inside one advisory-locked transaction — creates the events schema, the events tables and their `pulse_meta` bookkeeping, the publication (plus membership diff), and `REPLICA IDENTITY FULL` on each source. An events table is recreated when the sha256 of its rendered DDL diverges (a source-column change), which rotates a per-table epoch so stale client cursors reset. `wal_level = logical` is the one precondition the runtime can't fix — it stays a fail-fast assert.

See [`docs/events-table-convention.md`](../../docs/events-table-convention.md) for the full name-derivation, column-mapping, and reconcile contract.

### `drizzle.config` — exclude the pulse schema

drizzle-kit does not manage the pulse events schema, so keep it out of your `push`/`pull`. kit's `schemaFilter` is an allowlist of schemas it manages (default `['public']`): make sure your pulse events schema — `'drizzle_pulse'` by default, or your configured `eventsSchema` — is not in it, so kit never tries to drop pulse-owned tables.

```ts
// drizzle.config.ts
export default defineConfig({
  // ...
  schemaFilter: ['public'], // omits 'drizzle_pulse' — pulse owns that schema
});
```

### Provisioning & privileges

By default the app's own role owns its tables and can `CREATE`, so `runtime.start()` self-provisions everything on first boot and no-ops on later boots. A failed self-provisioning statement throws with the exact statement and the grant it most likely needs (e.g. *ownership of `public.orders`*, *the database `CREATE` privilege*).

For split-role deploys where the app role is deliberately unprivileged, call `runtime.provision()` once from a migration/deploy step under an elevated role — it runs the same reconciliation without opening the WAL stream, and the app's later `start()` then no-ops the DDL. The reconcile role needs ownership of each pulsed source table, the database `CREATE` privilege, and (once it exists) ownership of the publication; the WAL streaming connection additionally needs the `REPLICATION` attribute.

## `drizzle-pulse/server/router`

`runtime.handlers` is a transport-agnostic SDK — plug its `subscribe`/`pull`/`loadMore` methods into any HTTP framework. For Hono, the optional first-party router wraps them (superjson-encoded responses over three POST routes: `/subscribe`, `/pull`, `/load-more`):

```ts
import { createPulseRouter } from 'drizzle-pulse/server/router';

const router = createPulseRouter(runtime.handlers, { userId: null }); // Hono instance
app.route('/pulse', router);
```

`hono` is an optional peer dependency — only installed if you mount this router.

## `drizzle-pulse/client`

The framework-agnostic HTTP polling client. `createPulseClient` returns a typed proxy of query-descriptor factories; wrap a descriptor in `PulseQuery` to subscribe and poll (~1s by default, configurable via `pollIntervalMs`).

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

const client = createPulseClient(runtime); // same PulseRuntime from drizzle-pulse/server

const collection = await client.ordersByStatus({ status: 'active' });

collection.list(); // synchronous current filtered set
collection.getState(); // { data, isLoading, isLoadingMore, hasMore, error }
collection.onChange(({ events, state }) => {
  console.log('changed:', events, 'now:', state);
});

await collection.loadMore(); // next page of a `.limit()` query — extends list() in place

collection.dispose(); // stop the collection when done
```

Updates are push-shaped: the collection re-pulls when the runtime's WAL tap signals a change on its source table (or on reconnect), with no polling interval.

`drizzle-pulse/client` and `drizzle-pulse/client/embedded` are two import-path-selected flavors of the same `createPulseClient` concept: pick `/client` for remote/browser consumers over HTTP, or `/client/embedded` for in-process server-side consumers with synchronous live data.

## Compatibility

| Dependency | Range | Notes |
|---|---|---|
| `drizzle-orm` | `^1.0.0-rc.4` | Tested against `1.0.0-rc.4` |
| `zod` | `^4.0.0` | |
| `react` | `>=18.0.0` | Optional — only required for `drizzle-pulse/client/react` |
| `hono` | `^4.6.0` | Optional — only required for `drizzle-pulse/server/router` |
| `node` | `>=20` | |
| PostgreSQL | `16` | Requires `wal_level=logical` |

## Transport & guardrails

- **Transport:** HTTP polling only (`drizzle-pulse/client`, `drizzle-pulse/client/react`) — no SSE or WebSocket transport.
- **Embedded collections** (`drizzle-pulse/client/embedded`) are in-process only: no `.transform()` (transforms stay HTTP-only), and no dedupe (each call creates an independent collection — create once, hold, `dispose()`). `.limit()` queries are supported and paginate via `loadMore()`.
