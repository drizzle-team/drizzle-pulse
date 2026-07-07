import { describe, expect, test } from 'bun:test';
import { PulseCollection } from '../client/embedded/index.js';
import type { PulseSourceDb } from '../server/pulse-sql.js';
import type { WalTapPayload } from '../server/wal-event-emitter.js';
import { WalEventEmitter } from '../server/wal-event-emitter.js';
import type { PulseAuthContext, PulseRegistryQuery, ResolvedPulseQuery } from '../types.js';
import {
  makeMockSourceDb,
  makePulseRuntime,
  makeRegistryStub,
  makeResolvedQuery,
} from './mock-runtime.js';

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

// Blocking mock: the FIRST select (startHandshake) resolves immediately;
// subsequent selects (rebaseline) block until release() is called.
function makeBlockingDb(rows: Record<string, unknown>[]): {
  db: PulseSourceDb;
  release: () => void;
  state: { selectCount: number };
} {
  const state = { selectCount: 0 };
  let releaseRebaseline!: () => void;
  const rebaselineGate = new Promise<void>((resolve) => {
    releaseRebaseline = resolve;
  });

  const db = {
    select() {
      state.selectCount++;
      const isFirst = state.selectCount === 1;
      const resultP: Promise<Record<string, unknown>[]> = isFirst
        ? Promise.resolve([...rows])
        : rebaselineGate.then(() => [...rows]);

      // Thenable wrapper: $dynamic() and orderBy() chain back to this same object;
      // await resolution is delegated to resultP via the Promise protocol.
      const thenable: any = {
        // biome-ignore lint/suspicious/noThenProperty: mocks Drizzle's awaitable query builder
        then(f: any, r: any) {
          return resultP.then(f, r);
        },
        catch(r: any) {
          return resultP.catch(r);
        },
        $dynamic() {
          return thenable;
        },
        orderBy() {
          return thenable;
        },
        limit(n: number) {
          return resultP.then((rs) => rs.slice(0, n));
        },
      };

      return {
        from: () => ({ where: () => ({ $dynamic: () => thenable }) }),
      };
    },
  } as unknown as PulseSourceDb;

  return { db, release: releaseRebaseline, state };
}

type MockRuntime = {
  isRunning: boolean;
  walEventEmitter: WalEventEmitter;
  lastPersistedSnapshot: number;
  sourceDb: PulseSourceDb;
  registry: {
    getPulseQuery(name: string): PulseRegistryQuery | null;
    resolve(name: string, args: unknown, auth: PulseAuthContext): ResolvedPulseQuery;
  };
};

function makeMockRuntime(
  opts: { isRunning?: boolean; sourceDb?: PulseSourceDb } = {},
): MockRuntime {
  const walEventEmitter = new WalEventEmitter();
  const registryStub = makeRegistryStub();
  const resolved = makeResolvedQuery();

  return {
    isRunning: opts.isRunning ?? true,
    walEventEmitter,
    lastPersistedSnapshot: 0,
    sourceDb: opts.sourceDb ?? makeMockSourceDb(),
    registry: {
      getPulseQuery: () => registryStub,
      resolve: () => resolved,
    },
  };
}

// ---------------------------------------------------------------------------
// Group A: Runtime reconnect edge. The debounce/concurrency rebaseline orchestration
// now lives in the embedded client (PulseClient) and is exercised by the integration
// reconnect flow; here we only assert the runtime broadcasts the reconnect edge.
// ---------------------------------------------------------------------------

describe('runtime reconnect edge', () => {
  test('onReconnect fires on reconnect but not on the first connect', () => {
    const runtime = makePulseRuntime();
    let fired = 0;
    runtime.onReconnect(() => {
      fired++;
    });

    (runtime as any).isRunning = true;
    (runtime as any).onReplicationStart(); // first connect — no reconnect edge
    expect(fired).toBe(0);
    (runtime as any).onReplicationStart(); // reconnect
    expect(fired).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Group C: PulseCollection.rebaseline() behavior
// ---------------------------------------------------------------------------

describe('PulseCollection.rebaseline()', () => {
  test('updates list(), fires onChange, stays ready, nulls rebaselineBuffer', async () => {
    const liveRows: Record<string, unknown>[] = [{ id: 1, status: 'accepted', price: 100 }];
    const sourceDb = makeMockSourceDb(liveRows);
    const mockRuntime = makeMockRuntime({ sourceDb });
    const resolved = makeResolvedQuery();
    const collection = new PulseCollection(mockRuntime as any, resolved);
    await collection.startHandshake();

    expect(collection.list()).toHaveLength(1);

    // Swap the backing rows for the re-baseline
    liveRows.splice(
      0,
      liveRows.length,
      { id: 2, status: 'pending', price: 50 },
      { id: 3, status: 'done', price: 75 },
    );

    const onChangeCalls: number[] = [];
    collection.onChange(() => onChangeCalls.push(1));

    await collection.rebaseline();

    expect(collection.list()).toHaveLength(2);
    expect(onChangeCalls).toHaveLength(1); // exactly one onChange on the re-baseline edge
    expect(collection.isReady).toBe(true); // rebaseline never takes the collection out of the ready state
    expect((collection as any).rebaselineBuffer).toBeNull();
    expect((collection as any).isRebaselining).toBe(false);
  });

  test('concurrency guard: second concurrent call is a no-op (no extra SELECT)', async () => {
    const { db, release, state } = makeBlockingDb([]);
    const mockRuntime = makeMockRuntime({ sourceDb: db });
    const resolved = makeResolvedQuery();
    const collection = new PulseCollection(mockRuntime as any, resolved);
    await collection.startHandshake();

    expect(state.selectCount).toBe(1); // handshake used one SELECT

    // Start rebaseline #1 — blocks waiting for the SELECT to resolve
    const r1 = collection.rebaseline();

    // isRebaselining is true synchronously (before the first await resolves)
    expect((collection as any).isRebaselining).toBe(true);

    // Start rebaseline #2 — should return immediately due to the guard
    const r2 = collection.rebaseline();

    // Unblock rebaseline #1's SELECT
    release();
    await Promise.all([r1, r2]);

    // Only one additional SELECT beyond the handshake (rebaseline #2 was a no-op)
    expect(state.selectCount).toBe(2);
    expect((collection as any).isRebaselining).toBe(false);
    expect((collection as any).rebaselineBuffer).toBeNull();
  });

  test('error during rebaseline SELECT drains buffered WAL events — none lost', async () => {
    let rejectRebaseline!: (err: Error) => void;
    let selectCount = 0;

    const failingDb = (() => {
      const mkThenable = (p: Promise<Record<string, unknown>[]>) => {
        const t: any = {
          // biome-ignore lint/suspicious/noThenProperty: mocks Drizzle's awaitable query builder
          then: (f: any, r: any) => p.then(f, r),
          catch: (r: any) => p.catch(r),
          $dynamic: () => t,
          orderBy: () => t,
          limit: (n: number) => p.then((rs) => rs.slice(0, n)),
        };
        return t;
      };
      return {
        select() {
          selectCount++;
          const p: Promise<Record<string, unknown>[]> =
            selectCount === 1
              ? Promise.resolve([{ id: 1, status: 'active', price: 100 }])
              : new Promise<never>((_, reject) => {
                  rejectRebaseline = reject;
                });
          return { from: () => ({ where: () => ({ $dynamic: () => mkThenable(p) }) }) };
        },
      } as unknown as PulseSourceDb;
    })();

    const mockRuntime = makeMockRuntime({ sourceDb: failingDb });
    const resolved = makeResolvedQuery();
    const collection = new PulseCollection(mockRuntime as any, resolved);
    await collection.startHandshake();
    expect(collection.list()).toHaveLength(1);

    // Start rebaseline — the SELECT will block then throw
    const rebaselinePromise = collection.rebaseline();

    // rebaselineBuffer is open synchronously; this event should be buffered
    const buffered: WalTapPayload = {
      operation: 'insert',
      rowData: { id: 2, status: 'pending', price: 50 },
      oldRowData: null,
      $snapshot: 1,
    };
    collection.applyTapPayload(buffered);
    expect((collection as any).rebaselineBuffer).toHaveLength(1);

    // Fail the SELECT
    rejectRebaseline(new Error('transient DB error'));
    await rebaselinePromise;

    // Buffer must have been drained: the insert is reflected in live state
    expect((collection as any).rebaselineBuffer).toBeNull();
    expect((collection as any).isRebaselining).toBe(false);
    expect(collection.list()).toHaveLength(2);
    const inserted = collection.list().find((r: any) => r.id === 2);
    expect(inserted).toBeDefined();
    expect((inserted as any)?.status).toBe('pending');
  });

  test('dispose mid-rebaseline: no throw, no state mutation after disposal', async () => {
    const { db, release } = makeBlockingDb([]);
    const mockRuntime = makeMockRuntime({ sourceDb: db });
    const resolved = makeResolvedQuery();
    const collection = new PulseCollection(mockRuntime as any, resolved);
    await collection.startHandshake();

    // Start rebaseline — will block on the SELECT
    const rebaselinePromise = collection.rebaseline();

    // Dispose while the SELECT is still pending
    collection.dispose();

    // Resolve the SELECT; rebaseline should detect disposal and exit cleanly
    release();
    await expect(rebaselinePromise).resolves.toBeUndefined();

    expect((collection as any).rebaselineBuffer).toBeNull();
    expect((collection as any).isRebaselining).toBe(false);
  });
});
