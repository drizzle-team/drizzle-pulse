import { describe, expect, test } from 'bun:test';
import { getTableUniqueName } from 'drizzle-orm';
import { createPulseEvents } from '../client/embedded/events.js';
import { makeMockRuntime, ordersTable } from './mock-runtime.js';

// ---------------------------------------------------------------------------
// No DB required: createPulseEvents is synchronous and stateless — no baseline,
// no merge core, no per-subscription error surface. Real WAL delivery order is
// covered by the integration suite.
// ---------------------------------------------------------------------------

const tableKey = getTableUniqueName(ordersTable);

describe('createPulseEvents — sync rejection paths', () => {
  test('an unknown query name throws synchronously', () => {
    const runtime = makeMockRuntime();
    runtime.registry = { ...runtime.registry, getPulseQuery: () => undefined } as any;
    const events = createPulseEvents(runtime as any);
    expect(() => (events as any).nope(() => {})).toThrow('Unknown query: "nope"');
  });

  test('subscribing before runtime.start() throws synchronously', () => {
    const runtime = makeMockRuntime({ isRunning: false });
    const events = createPulseEvents(runtime as any);
    expect(() => (events as any).orders(() => {})).toThrow(/after runtime\.start\(\)/);
  });

  test('a .transform() query throws synchronously', () => {
    const runtime = makeMockRuntime({ hasTransform: true });
    const events = createPulseEvents(runtime as any);
    expect(() => (events as any).orders(() => {})).toThrow(/\.transform\(\)/);
  });

  test('a .limit() query throws synchronously', () => {
    const runtime = makeMockRuntime({ limit: 2 });
    const events = createPulseEvents(runtime as any);
    expect(() => (events as any).orders(() => {})).toThrow(/\.limit\(\)/);
  });

  test('a missing/wrong-arity callback throws synchronously instead of failing silently per event (WR-04)', () => {
    const runtime = makeMockRuntime();
    const events = createPulseEvents(runtime as any);
    // No-args query called as `events.orders(optionsObject)` — the "callback" is really options.
    expect(() => (events as any).orders({ auth: { userId: 'u1' } })).toThrow(
      /expected a callback function/,
    );
  });
});

describe('createPulseEvents — WHERE-filtered per-event delivery', () => {
  test('never calls readCollectionBaseline — the subscription needs no baseline', () => {
    const runtime = makeMockRuntime();
    runtime.readCollectionBaseline = async () => {
      throw new Error('readCollectionBaseline must never be called by createPulseEvents');
    };
    const events = createPulseEvents(runtime as any);
    const received: unknown[] = [];

    expect(() =>
      (events as any).orders((event: unknown) => {
        received.push(event);
      }),
    ).not.toThrow();

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 1, status: 'accepted', price: 10 },
      null,
      0,
      '0/100',
    );
    expect(received).toHaveLength(1);
  });

  test('only rows matching the resolved WHERE produce callbacks', () => {
    const runtime = makeMockRuntime({ where: { status: { eq: 'accepted' } } });
    const events = createPulseEvents(runtime as any);
    const received: unknown[] = [];
    (events as any).orders((event: unknown) => received.push(event));

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 1, status: 'requested', price: 10 },
      null,
      0,
      '0/100',
    );
    expect(received).toHaveLength(0);

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 2, status: 'accepted', price: 20 },
      null,
      0,
      '0/200',
    );
    expect(received).toHaveLength(1);
  });

  test('the callback receives (event, lsn) with lsn equal to the emitted payload lsn', () => {
    const runtime = makeMockRuntime();
    const events = createPulseEvents(runtime as any);
    const received: Array<[string, string]> = [];
    (events as any).orders((event: any, lsn: string) => received.push([event.op, lsn]));

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 1, status: 'accepted', price: 10 },
      null,
      0,
      '0/1A2B',
    );
    expect(received).toEqual([['insert', '0/1A2B']]);
  });

  test('an update carries matchesNew/matchesOld as the row moves in/out of the filter', () => {
    const runtime = makeMockRuntime({ where: { status: { eq: 'accepted' } } });
    const events = createPulseEvents(runtime as any);
    const received: Array<{ matchesNew: boolean; matchesOld: boolean }> = [];
    (events as any).orders((event: any) => received.push(event));

    runtime.walEventEmitter.emit(
      tableKey,
      'update',
      { id: 1, status: 'completed', price: 10 },
      { id: 1, status: 'accepted', price: 10 },
      0,
      '0/300',
    );

    expect(received).toHaveLength(1);
    expect(received[0]!.matchesNew).toBe(false);
    expect(received[0]!.matchesOld).toBe(true);
  });

  test('unsubscribe stops delivery and is idempotent', () => {
    const runtime = makeMockRuntime();
    const events = createPulseEvents(runtime as any);
    let count = 0;
    const unsub = (events as any).orders(() => count++);

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 1, status: 'accepted', price: 10 },
      null,
      0,
      '0/100',
    );
    expect(count).toBe(1);

    unsub();
    unsub(); // idempotent — no throw, no double-detach error

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 2, status: 'accepted', price: 20 },
      null,
      0,
      '0/200',
    );
    expect(count).toBe(1);
  });

  test('the captured onStop listener tears the subscription down', () => {
    let stopListener: (() => void) | undefined;
    const runtime = makeMockRuntime();
    runtime.onStop = (listener: () => void) => {
      stopListener = listener;
      return () => {
        stopListener = undefined;
      };
    };

    const events = createPulseEvents(runtime as any);
    let count = 0;
    (events as any).orders(() => count++);

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 1, status: 'accepted', price: 10 },
      null,
      0,
      '0/100',
    );
    expect(count).toBe(1);

    stopListener?.();

    runtime.walEventEmitter.emit(
      tableKey,
      'insert',
      { id: 2, status: 'accepted', price: 20 },
      null,
      0,
      '0/200',
    );
    expect(count).toBe(1);
  });
});
