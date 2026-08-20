import { afterAll, describe, expect, mock, test } from 'bun:test';
import * as realCdc from 'minipg/cdc';
import { makePulseRuntime } from './mock-runtime.js';

// ---------------------------------------------------------------------------
// The reconnect edge that stayed in pulse after the CDC layer took the connection loop.
// minipg's `replicate()` owns connect, slot administration, backoff and the retry budget, and
// tests those itself. What is still pulse policy — and only observable here — is WHICH session
// events rebuild consumer state: a rebaseline round must fire when a stream re-opens under live
// collections, and must not fire on the very first one, when no collection exists yet.
//
// `replicate()` is a free function, not a method on the runtime, so there is no instance seam to
// override — the module is mocked (live-binding update, per Bun's mock.module docs) to capture
// the options object, and the test drives the driver's callbacks directly.
// ---------------------------------------------------------------------------

type ReplicateOpts = Parameters<typeof realCdc.replicate>[0];

let captured: ReplicateOpts | null = null;

mock.module('minipg/cdc', () => ({
  ...realCdc,
  replicate: (opts: ReplicateOpts) => {
    captured = opts;
    return { stop: async () => {} };
  },
}));

afterAll(() => {
  mock.module('minipg/cdc', () => realCdc);
});

describe('runtime reconnect edge', () => {
  test('the rebaseline round fires when a stream re-opens, not on the first one', async () => {
    // pull:false: no events tables and no store, so the backfill callback's only job here
    // is the collection rebaseline round this test observes.
    const runtime = makePulseRuntime({ pull: false });
    // rebaselineCollections pins the exported snapshot in an admin transaction before firing the
    // round; the store is normally opened by start(), which this test bypasses.
    (runtime as any).store = {
      getDb: () => ({
        transaction: async (fn: (tx: unknown) => Promise<void>) =>
          fn({ execute: async () => ({ rows: [] }) }),
      }),
    };
    let fired = 0;
    runtime.onReconnect(() => {
      fired++;
    });

    // openSession is the private seam that builds the replicate() options; awaiting it settles
    // once the mocked driver reports the slot administered.
    const opening = (runtime as any).openSession();
    const opts = captured;
    if (!opts?.backfill) throw new Error('replicate() was called without a backfill callback');

    // A temporary slot is recreated per session, so every stream open arrives as a backfill.
    const window = {
      snapshot: 'snap-1',
      streamStartLsn: '0/1000000',
      isReconnect: false,
      signal: new AbortController().signal,
    };

    await opts.backfill(window);
    await opening;
    expect(fired).toBe(0);

    await opts.backfill({ ...window, snapshot: 'snap-2', isReconnect: true });
    expect(fired).toBe(1);
  });
});
