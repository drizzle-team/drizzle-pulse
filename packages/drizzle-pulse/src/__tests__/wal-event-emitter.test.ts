import { describe, expect, it } from 'bun:test';
import type { WalTapPayload } from '../server/wal-event-emitter.js';
import { WalEventEmitter } from '../server/wal-event-emitter.js';

describe('WalEventEmitter', () => {
  const TABLE_A = 'public.orders';
  const TABLE_B = 'public.users';

  it('delivers exact payload to a single listener', () => {
    const emitter = new WalEventEmitter();
    const received: WalTapPayload[] = [];
    emitter.subscribe(TABLE_A, (p) => received.push(p));

    emitter.emit(TABLE_A, 'insert', { id: 1 }, null, 10);

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual({
      operation: 'insert',
      rowData: { id: 1 },
      oldRowData: null,
      $snapshot: 10,
    });
  });

  it('fans out to multiple listeners on the same table in registration order', () => {
    const emitter = new WalEventEmitter();
    const order: string[] = [];
    emitter.subscribe(TABLE_A, () => order.push('first'));
    emitter.subscribe(TABLE_A, () => order.push('second'));
    emitter.subscribe(TABLE_A, () => order.push('third'));

    emitter.emit(TABLE_A, 'update', { id: 1 }, { id: 1, old: true }, 20);

    expect(order).toEqual(['first', 'second', 'third']);
  });

  it('does not call a listener registered on a different table', () => {
    const emitter = new WalEventEmitter();
    const callsA: WalTapPayload[] = [];
    const callsB: WalTapPayload[] = [];
    emitter.subscribe(TABLE_A, (p) => callsA.push(p));
    emitter.subscribe(TABLE_B, (p) => callsB.push(p));

    emitter.emit(TABLE_A, 'delete', {}, { id: 2 }, 30);

    expect(callsA).toHaveLength(1);
    expect(callsB).toHaveLength(0);
  });

  it('is a no-op when no listeners are registered for the table', () => {
    const emitter = new WalEventEmitter();
    expect(() => emitter.emit(TABLE_A, 'insert', { id: 1 }, null, 5)).not.toThrow();
  });

  it('stops delivering to a listener after unsubscribe', () => {
    const emitter = new WalEventEmitter();
    const received: WalTapPayload[] = [];
    const unsub = emitter.subscribe(TABLE_A, (p) => received.push(p));

    emitter.emit(TABLE_A, 'insert', { id: 1 }, null, 1);
    unsub();
    emitter.emit(TABLE_A, 'insert', { id: 2 }, null, 2);

    expect(received).toHaveLength(1);
    expect(received[0]?.$snapshot).toBe(1);
  });

  it('passes oldRowData: null through unchanged for insert-shaped payloads', () => {
    const emitter = new WalEventEmitter();
    let captured: WalTapPayload | null = null;
    emitter.subscribe(TABLE_A, (p) => {
      captured = p;
    });

    emitter.emit(TABLE_A, 'insert', { id: 99 }, null, 77);

    expect(captured).not.toBeNull();
    expect((captured as unknown as WalTapPayload).oldRowData).toBeNull();
  });

  it('isolates listener errors — a throwing listener does not prevent others or propagate out of emit', () => {
    const emitter = new WalEventEmitter();
    const received: WalTapPayload[] = [];

    emitter.subscribe(TABLE_A, () => {
      throw new Error('listener boom');
    });
    emitter.subscribe(TABLE_A, (p) => received.push(p));

    // emit must not throw even though the first listener throws
    expect(() => emitter.emit(TABLE_A, 'insert', { id: 1 }, null, 5)).not.toThrow();
    // second listener still received the event
    expect(received).toHaveLength(1);
  });
});
