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

**Server** — define a query with `createPulse`, register it, and expose it over WAL:

```ts
// server.ts
import { createPulse, createPulseRegistry, expose } from 'drizzle-pulse/server';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { orders } from './schema.js';

const pulse = createPulse();

const activeOrders = pulse(orders)
  .$eventsTable(ordersEvents) // realtime.events_orders — see "Server" section
  .order('asc')
  .query((ctx) => ctx.query({ status: 'active' }));

const registry = createPulseRegistry({ activeOrders });

const sourceDb = drizzle({ client: postgres(process.env.DATABASE_URL!) });

const runtime = expose(registry, {
  databaseUrl: process.env.DATABASE_URL!, // must have wal_level=logical
  sourceDb,
  wal: { publicationName: 'drizzle_pulse', slotName: 'drizzle_pulse_slot' },
});

await runtime.start();

// wire runtime.handlers.subscribe / .pull / .loadMore into your HTTP router
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

## `drizzle-pulse/server`

The server runtime: define queries, register them, and expose a WAL-fed runtime that serves subscribe/pull requests.

- `createPulse()` — returns a `PulseFactory` that wraps a Drizzle table into a `PulseBuilder`
- `PulseBuilder` — fluent builder: `.columns()`, `.args(zodSchema)`, `.query(ctx => WhereClause)`, `.order()`, `.limit()`, `.transform()`, `.$eventsTable(table)`
- `createPulseRegistry(queries)` — collects builders into a `PulseRegistry`
- `expose(registry, config)` — returns a `RealtimeRuntime`; call `.start()` to connect WAL, `runtime.handlers.{subscribe,pull,loadMore}` to serve requests
- `RealtimeRuntime` — WAL listener + request handlers; `.start()` / `.stop()`

```ts
import { createPulse, createPulseRegistry, expose } from 'drizzle-pulse/server';

const pulse = createPulse();

const ordersByStatus = pulse(orders)
  .$eventsTable(ordersEvents)
  .args(z.object({ status: z.string() }))
  .order('desc')
  .query((ctx) => ctx.query({ status: ctx.args.status }));

const registry = createPulseRegistry({ ordersByStatus });
const runtime = expose(registry, { databaseUrl, sourceDb, wal: { publicationName, slotName } });
await runtime.start();
```

Every source table needs a matching `realtime.events_<table>` table (created via migration) linked with `.$eventsTable()` — WAL changes are persisted there and replayed to clients.

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
