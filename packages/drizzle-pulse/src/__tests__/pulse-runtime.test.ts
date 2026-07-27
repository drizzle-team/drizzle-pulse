import { describe, expect, test } from 'bun:test';
import { createPulseRegistry } from '../server/pulse-registry.js';
import { PulseRuntime } from '../server/pulse-runtime.js';
import type { PulseSourceDb } from '../server/pulse-sql.js';
import { makePulseRuntime } from './mock-runtime.js';

describe('wal config resolution', () => {
  test('wal.publicationName/slotName are exposed on the runtime', () => {
    // makePulseRuntime passes wal: { publicationName: 'test_pub', slotName: 'test_slot' }.
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:p%40ss@localhost:5433/my_db?sslmode=require',
    });

    expect(runtime.publicationName).toBe('test_pub');
    expect(runtime.slotName).toBe('test_slot');
  });

  test('defaults to drizzle_pulse/drizzle_pulse when wal is omitted', () => {
    const emptyRegistry = createPulseRegistry({});
    const runtime = new PulseRuntime(emptyRegistry as any, {
      databaseUrl: 'postgresql://user:pass@localhost/test',
      sourceDb: {} as PulseSourceDb,
      pull: true,
    });

    expect(runtime.publicationName).toBe('drizzle_pulse');
    expect(runtime.slotName).toBe('drizzle_pulse');
  });
});

describe('start() failure rolls back to a restartable state', () => {
  test('a throw from bootstrap() resets isRunning and tears down the store instead of leaving a zombie', async () => {
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:pass@localhost/test',
    }) as any;

    // Baseline seeding moved inside the replication loop's connect (resolveSlot's resume branch) —
    // a failure there now becomes a replication-loop retry, not a start()
    // rejection. bootstrap() is the only await left in start()'s try block before the guard
    // resolves, so it's the seam that must throw to exercise the rollback path.
    let storeEnded = 0;
    runtime.store = {
      end: async () => {
        storeEnded++;
      },
    };
    runtime.bootstrap = async () => {
      throw new Error('sourceDb briefly unavailable');
    };

    await expect(runtime.start()).rejects.toThrow('sourceDb briefly unavailable');

    expect(runtime.isRunning).toBe(false);
    expect(runtime.store).toBeNull();
    expect(storeEnded).toBe(1);

    // A retry must not hit the "Already running" early return and silently no-op — it
    // must re-attempt bootstrap() and actually start the replication loop.
    let secondAttemptRan = false;
    runtime.bootstrap = async () => {
      secondAttemptRan = true;
    };
    runtime.runReplicationLoop = async (_run: unknown, startupSettled: { resolve: () => void }) => {
      startupSettled.resolve();
    };

    await runtime.start();

    expect(secondAttemptRan).toBe(true);
    expect(runtime.isRunning).toBe(true);
  });
});

describe('rebaselineCollections', () => {
  function makeRuntimeWithSnapshotClient(query: (statement: string) => Promise<unknown>) {
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:pass@localhost/test',
    }) as any;
    let releaseCalls = 0;
    runtime.getPulseStore = () => ({
      checkout: async () => ({
        client: { query },
        release: () => {
          releaseCalls++;
        },
      }),
    });
    return { runtime, releases: () => releaseCalls };
  }

  test('a failing SET TRANSACTION SNAPSHOT rejects and releases the checked-out connection exactly once', async () => {
    // Mirrors the real checked-out connection: BEGIN succeeds, SET TRANSACTION SNAPSHOT throws —
    // the exact SQL that can't be bind-parameterized (assertSnapshotName guards it upstream, this
    // exercises the query failure itself).
    const { runtime, releases } = makeRuntimeWithSnapshotClient(async (statement) => {
      if (statement.startsWith('SET TRANSACTION SNAPSHOT')) {
        throw new Error('invalid snapshot identifier');
      }
    });
    runtime.onReconnect(() => {});

    await expect(
      runtime.rebaselineCollections('00000000-0000-0000-0000-000000000000', '0/100'),
    ).rejects.toThrow('invalid snapshot identifier');

    // The connection-leak case: release() must fire even though setup failed, or the
    // checked-out connection is gone for good.
    expect(releases()).toBe(1);
  });

  test('a rejecting listener still lets the round finish, and the connection is rolled back and released once', async () => {
    const statements: string[] = [];
    const { runtime, releases } = makeRuntimeWithSnapshotClient(async (statement) => {
      statements.push(statement);
    });

    let secondRan = false;
    runtime.onReconnect(async () => {
      throw new Error('rebaseline boom');
    });
    runtime.onReconnect(async () => {
      secondRan = true;
    });

    await runtime.rebaselineCollections('00000000-0000-0000-0000-000000000000', '0/100');

    // A listener rejecting on its own (before touching the connection) is fully isolated. A
    // listener whose SELECT errors is not — it poisons the shared snapshot transaction — which is
    // why this asserts the round completing, not per-collection isolation.
    expect(secondRan).toBe(true);
    expect(releases()).toBe(1);
    expect(statements[0]).toBe('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    expect(statements.at(-1)).toBe('ROLLBACK');
  });

  test('bounds each baseline read so a stuck one cannot hold the stream closed', async () => {
    const statements: string[] = [];
    const { runtime } = makeRuntimeWithSnapshotClient(async (statement) => {
      statements.push(statement);
    });
    runtime.onReconnect(() => {});

    await runtime.rebaselineCollections('00000000-0000-0000-0000-000000000000', '0/100');

    expect(statements).toContain('SET LOCAL statement_timeout = 30000');
    // Ordering matters: the bound has to be in force before any listener reads.
    expect(statements.indexOf('SET LOCAL statement_timeout = 30000')).toBeLessThan(
      statements.indexOf('ROLLBACK'),
    );
  });

  test('an intact resumed slot exports no snapshot — listeners run with null and no connection is checked out', async () => {
    const { runtime, releases } = makeRuntimeWithSnapshotClient(async () => {
      throw new Error('no connection should be checked out without a snapshot');
    });

    let received: unknown = 'unset';
    runtime.onReconnect((snapshot: unknown) => {
      received = snapshot;
    });

    await runtime.rebaselineCollections(undefined, undefined);

    expect(received).toBeNull();
    expect(releases()).toBe(0);
  });
});
