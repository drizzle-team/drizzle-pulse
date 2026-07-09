import { describe, expect, test } from 'bun:test';
import { makePulseRuntime } from './mock-runtime.js';

// ---------------------------------------------------------------------------
// Runtime reconnect edge. The embedded client wires this edge to query.poll()
// (catch up on events missed while the WAL stream was down); the rebaseline
// engine that used to live here is gone. The push pipeline itself is covered by
// the embedded-collection integration tests.
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
