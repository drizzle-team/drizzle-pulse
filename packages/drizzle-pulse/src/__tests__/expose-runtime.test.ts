import { describe, expect, test } from 'bun:test';
import { PulseRuntime } from '../server/expose.js';
import { createPulseRegistry } from '../server/pulse-registry.js';
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
  test('a throw from reconcile() resets isRunning and tears down the store instead of leaving a zombie', async () => {
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:pass@localhost/test',
    }) as any;

    // Baseline seeding moved inside the supervised connect (resolveSlot's resume branch,
    // documented delta b) — a failure there now becomes a supervised retry, not a start()
    // rejection. reconcile() is the only await left in start()'s try block before the guard
    // resolves, so it's the seam that must throw to exercise the rollback path.
    let storeEnded = 0;
    runtime.store = {
      end: async () => {
        storeEnded++;
      },
    };
    runtime.reconcile = async () => {
      throw new Error('sourceDb briefly unavailable');
    };

    await expect(runtime.start()).rejects.toThrow('sourceDb briefly unavailable');

    expect(runtime.isRunning).toBe(false);
    expect(runtime.store).toBeNull();
    expect(storeEnded).toBe(1);

    // A retry must not hit the "Already running" early return and silently no-op — it
    // must re-attempt reconcile() and actually start supervision.
    let secondAttemptRan = false;
    runtime.reconcile = async () => {
      secondAttemptRan = true;
    };
    runtime.supervise = async (_run: unknown, first: { resolve: () => void }) => {
      first.resolve();
    };

    await runtime.start();

    expect(secondAttemptRan).toBe(true);
    expect(runtime.isRunning).toBe(true);
  });
});

describe('openPin', () => {
  test('a failing SET TRANSACTION SNAPSHOT rejects and releases the checked-out connection exactly once', async () => {
    const runtime = makePulseRuntime({
      databaseUrl: 'postgresql://user:pass@localhost/test',
    }) as any;

    let releaseCalls = 0;
    runtime.getPulseStore = () => ({
      checkout: async () => ({
        client: {
          // Mirrors the real checked-out connection: BEGIN succeeds, SET TRANSACTION SNAPSHOT
          // throws — the exact SQL that can't be bind-parameterized (assertSnapshotName guards
          // it upstream, this exercises the query failure itself).
          query: async (statement: string) => {
            if (statement.startsWith('SET TRANSACTION SNAPSHOT')) {
              throw new Error('invalid snapshot identifier');
            }
          },
        },
        release: () => {
          releaseCalls++;
        },
      }),
    });

    await expect(
      runtime.openPin('00000000-0000-0000-0000-000000000000', '0/100'),
    ).rejects.toThrow('invalid snapshot identifier');

    // The connection-leak case the old transaction-callback mock couldn't express: release()
    // must fire even though setup failed, or the checked-out connection leaks.
    expect(releaseCalls).toBe(1);
  });
});
