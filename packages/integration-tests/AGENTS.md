# AGENTS.md — packages/integration-tests

## Role

Live PostgreSQL + WAL integration coverage for the realtime SDK runtime. These tests validate subscribe, pull, load-more, fetch-adapter, event-merging, embedded-collection, HTTP-vs-embedded consistency, and WAL-reconnect resilience behavior against a real database and logical replication slot.

## Entry Points

- `bun test`
- shared harness: `src/helpers/test-harness.ts`

## File Map

| File | Purpose |
|------|---------|
| `src/helpers/test-harness.ts` | suite setup/teardown, fixture-local helper binding, router fetch adapter, and typed subscribe/pull helpers |
| `src/helpers/db-helpers.ts` | shared DB mutation/event helpers used by the harness |
| `src/client-state.test.ts` | client-state behavior through `PulseQuery` and `initTestQuery()` |
| `src/runtime-contracts.test.ts` | lower-level subscribe/pull/load-more/reset protocol coverage |
| `src/property.test.ts` | property-based WAL/state validation via `fast-check` |
| `src/router-fetch-adapter.test.ts` | verifies `createRouterFetchAdapter()` and selected `PulseQuery` integration paths |
| `src/embedded-collection.test.ts` | embedded client (`@drizzle-pulse/client/embedded`) `PulseCollection` behavior, lifecycle, and PG data-type normalization through the WAL tap |
| `src/consistency-oracle.test.ts` | oracle asserting embedded `list()` state equals both the HTTP baseline-subscribe rows and the HTTP incremental-pull merged state per operator |
| `src/resilience.test.ts` | embedded-collection rebaseline behavior across WAL reconnect gaps |
| `src/fixtures/` | fixture variants, migrations path, source table wiring |

## Harness Lifecycle

```text
beforeAll
  → setupTestSuiteForFixture(fixture, registry)
    → create unique database
    → apply fixture migrations
    → create runtime via expose(...)
    → void runtime.start()
    → waitForWalStartup(...)
    → runtime ensures baseline snapshots

beforeEach
  → cleanupBetweenTestsForFixture(fixture, pool)
    → truncates fixture tables/events
    → runtime.ensureBaselines()

afterAll
  → teardownTestSuiteForFixture(fixture)
```

Suite contexts are reference-counted per fixture variant and reused safely across tests in the same file.

## Key Helpers

- `setupTestSuiteForFixture(fixture, registry)` → returns `{ router, pool, db, databaseUrl, publicationName, slotName, processDbOperations, initTestQuery }`
- `teardownTestSuiteForFixture(fixture)` → stops runtime, drops slot/publication, drops database
- `cleanupBetweenTestsForFixture(fixture, pool)` → truncates fixture tables and events table, then recreates baseline snapshot if needed
- `waitForEventsForFixture(fixture, pool, sinceSnapshot, expectedCount, opts?)` → polls events table until enough non-snapshot events arrive
- `createRouterFetchAdapter(router)` → wraps `router.request()` as a fetch-compatible function with `preconnect()`
- `subscribeClient(router, queryName, args)` → typed `/subscribe` helper
- `pullClient(router, subscriptionId, snapshot)` → typed `/pull` helper with reset handling
- `processDbOperations(operations)` from `setupTestSuiteForFixture(...)` is the preferred fixture-local mutation helper in tests
- `processDbOperations(fixture, pool, operations)`, `insertTestUser(...)`, `getLastEventSnapshot(...)` are shared helpers from `db-helpers.ts`
- `initTestQuery(descriptor)` creates a `PulseQuery` runtime against the fixture router and subscribes it for state-focused tests

## Query / Client Pattern

- Tests build registries with the same `createPulse` + `createPulseRegistry` API as production and pass them into `setupTestSuiteForFixture(...)`
- Client-state assertions should prefer `PulseQuery` / `initTestQuery(...)` over hand-rolled merge logic
- `router.request()` is enough for most endpoint tests; use `createRouterFetchAdapter()` when a real fetch implementation is needed
- Prefer fixture-local `processDbOperations(...)` in test files so fixture/pool plumbing stays inside the harness

## Known Behaviors

- `price` is a number
- Empty subscribe can return `snapshot: 0` with `rangeStart/rangeEnd = null`
- Snapshot rows in the events table trigger `{ reset: true, reason: 'snapshot' }` on pull
- Each test needing valid `orders.driver_id` should create a unique user first
- WAL startup is asynchronous; wait for slot readiness instead of sleeping

## Anti-Patterns (DO NOT)

- ❌ Hardcode publication or slot names
- ❌ Await `runtime.start()` directly; start it non-blocking and poll readiness
- ❌ Use fixed sleeps for WAL propagation; use `waitForEventsForFixture(...)`
- ❌ Reintroduce manual `pkMap` merge assertions where `PulseQuery` already covers the production path
- ❌ Weaken test assertions just to make runtime changes pass
