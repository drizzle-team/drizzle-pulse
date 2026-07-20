import { afterAll, describe, expect, mock, test } from 'bun:test';
import * as realMinipg from 'minipg';
import { makePulseRuntime } from './mock-runtime.js';

// ---------------------------------------------------------------------------
// Runtime reconnect edge. The embedded client wires this edge to query.poll()
// (catch up on events missed while the WAL stream was down); the rebaseline
// engine that used to live here is gone. The push pipeline itself is covered by
// the embedded-collection integration tests.
//
// runReplicationLoop() opens its connection via minipg's free `replication()` function, not a
// method on the runtime — there's no instance seam to override for it, so the module
// itself is mocked (live-binding update, per Bun's mock.module docs) with every other
// minipg export passed through untouched. `resolveSlot`/`stream` ARE instance methods
// and are overridden directly per test, same technique as the rest of this file's mocks.
// ---------------------------------------------------------------------------

let replicationImpl: (databaseUrl: string) => Promise<{
  start: (...args: unknown[]) => unknown;
  end: () => void;
}> = async () => {
  throw new Error('replicationImpl not configured for this test');
};

mock.module('minipg', () => ({
  ...realMinipg,
  replication: (databaseUrl: string) => replicationImpl(databaseUrl),
}));

afterAll(() => {
  mock.module('minipg', () => realMinipg);
});

function makeFakeRep() {
  return {
    start: () => ({}) as unknown,
    end: () => {},
  };
}

// runReplicationLoop()'s `startupSettled` parameter only needs to be resolve()-able — its promise
// isn't consumed by these tests, which await runReplicationLoop() itself.
function makeStartupSettled(): { promise: Promise<void>; resolve: () => void } {
  return { promise: Promise.resolve(), resolve: () => {} };
}

// The backoff sleep between reconnect rounds uses real setTimeout delays (seconds, growing
// exponentially) — fast-forward it for these tests so multi-round scenarios stay well under
// a second instead of tens of seconds.
function withFastTimers<T>(fn: () => Promise<T>): Promise<T> {
  const realSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = ((cb: (...args: unknown[]) => void, _ms?: number, ...args: unknown[]) =>
    realSetTimeout(cb, 0, ...args)) as typeof setTimeout;
  return fn().finally(() => {
    globalThis.setTimeout = realSetTimeout;
  });
}

describe('runtime reconnect edge', () => {
  test('onReconnect fires on reconnect but not on the first connect', async () => {
    const runtime = makePulseRuntime() as any;
    let fired = 0;
    runtime.onReconnect(() => {
      fired++;
    });

    replicationImpl = async () => makeFakeRep();
    runtime.resolveSlot = async () => ({ slot: 'test_slot', from: undefined });

    const run = { abort: new AbortController(), attempts: 0 };
    let streamCalls = 0;
    runtime.stream = async () => {
      streamCalls++;
      // Clean end each time (zero commits) — after the second connect's round has fired,
      // stop the loop so the test doesn't run a third round.
      if (streamCalls >= 2) run.abort.abort();
    };

    await withFastTimers(() => runtime.runReplicationLoop(run, makeStartupSettled()));

    expect(streamCalls).toBe(2);
    expect(fired).toBe(1);
  });
});

describe('zero-progress connections never reset attempts', () => {
  test('connections that clean-end with zero commits exhaust attempts and reach the terminal path', async () => {
    const runtime = makePulseRuntime() as any;
    let terminalError: Error | null = null;
    runtime.onTerminalError((error: Error) => {
      terminalError = error;
    });

    replicationImpl = async () => makeFakeRep();
    runtime.resolveSlot = async () => ({ slot: 'test_slot', from: undefined });
    // Every connection clean-ends without processing a single commit — run.attempts is only
    // reset inside stream()'s commit branch, so a runtime whose connections never land a
    // commit must still exhaust RECONNECT_MAX_RETRIES and reach giveUp().
    runtime.stream = async () => {};

    const run = { abort: new AbortController(), attempts: 0 };

    await withFastTimers(() => runtime.runReplicationLoop(run, makeStartupSettled()));

    // RECONNECT_MAX_RETRIES is a hardcoded module constant in pulse-runtime.ts (no reconnect
    // knobs), not a per-runtime config surface — mirror its value (10) directly.
    expect(run.attempts).toBe(10);
    expect(terminalError).toBeInstanceOf(Error);
  });
});
