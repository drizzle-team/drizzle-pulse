import { getTableUniqueName } from 'drizzle-orm';
import type { PulseRuntime } from '../../server/expose.js';
import type { AnyPulseBuilders } from '../../server/pulse-registry.js';
import type { PulseClientContract } from '../../server/pulse-types.js';
import type { WalTapPayload } from '../../server/wal-event-emitter.js';
import { compareLsn } from '../../shared/lsn.js';
import { applyProjectionPipeline } from '../../shared/projection.js';
import type { PulseEvent } from '../../shared/pulse-events.js';
import { PulseMergeCore } from '../../shared/pulse-merge-core.js';
import type { PulseAuthContext, QueryDescriptor } from '../../types.js';
import { buildTapEvent, type TapRow } from './tap-events.js';

export type { EmbeddedPulseEvents, PulseEventsCallback, PulseEventsOptions } from './events.js';
export { createPulseEvents } from './events.js';

// ---------------------------------------------------------------------------
// Public type surface
// ---------------------------------------------------------------------------

export interface PulseCollectionOptions {
  auth?: PulseAuthContext;
}

export interface PulseCollectionChange<T> {
  events: readonly PulseEvent<T>[];
  state: readonly T[];
  lsn: string;
}

export type PulseRow<C> =
  C extends Promise<infer P> ? PulseRow<P> : C extends PulseCollection<infer R> ? R : never;

export type EmbeddedPulseClient<TQueries extends AnyPulseBuilders> = {
  [K in keyof PulseClientContract<TQueries>]: PulseClientContract<TQueries>[K] extends (
    ...args: infer A
  ) => QueryDescriptor<infer R>
    ? // R always includes $pk (QueryDescriptor is created with WithPk<TResult>); the
      // intersection makes this explicit so PulseCollection<T>'s constraint is satisfied.
      (
        ...args: [...A, options?: PulseCollectionOptions]
      ) => Promise<PulseCollection<R & { $pk: unknown }>>
    : never;
};

type AnyRow = TapRow;

// ---------------------------------------------------------------------------
// PulseCollection — a full-set merge core fed tap-direct (no HTTP wire protocol).
// ---------------------------------------------------------------------------

export class PulseCollection<TRow extends { $pk: unknown }> {
  private disposed = false;
  private readonly onChangeListeners = new Set<(change: PulseCollectionChange<TRow>) => void>();
  private readonly onErrorListeners = new Set<(error: Error) => void>();

  /** @internal */
  constructor(
    private readonly core: PulseMergeCore<TRow & Record<string, unknown>>,
    private readonly teardown: () => void = () => {},
  ) {}

  /** @internal */
  get isDisposed(): boolean {
    return this.disposed;
  }

  list(): readonly TRow[] {
    return this.core.data as readonly TRow[];
  }

  onChange(listener: (change: PulseCollectionChange<TRow>) => void): () => void {
    this.onChangeListeners.add(listener);
    return () => this.onChangeListeners.delete(listener);
  }

  onError(listener: (error: Error) => void): () => void {
    this.onErrorListeners.add(listener);
    return () => this.onErrorListeners.delete(listener);
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.teardown();
  }

  /** @internal fed by the tap-direct handshake whenever applying events mutates state. */
  fireOnChange(events: readonly PulseEvent<TRow>[], lsn: string): void {
    const change: PulseCollectionChange<TRow> = {
      events,
      state: this.core.data as readonly TRow[],
      lsn,
    };
    for (const listener of this.onChangeListeners) {
      try {
        listener(change);
      } catch (e) {
        console.error('[PulseCollection] onChange error:', e);
      }
    }
  }

  /** @internal fed on re-baseline failure (post-reconnect) and on runtime terminal error. */
  fireOnError(error: Error): void {
    for (const listener of this.onErrorListeners) {
      try {
        listener(error);
      } catch (e) {
        console.error('[PulseCollection] onError error:', e);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// createPulseClient factory
// ---------------------------------------------------------------------------

export function createPulseClient<TQueries extends AnyPulseBuilders>(
  runtime: PulseRuntime<TQueries>,
): EmbeddedPulseClient<TQueries> {
  return new Proxy({} as EmbeddedPulseClient<TQueries>, {
    get(_target, prop: string | symbol) {
      if (typeof prop !== 'string') return undefined;
      // Returning a function for `then` makes the proxy an accidental thenable:
      // `await client` / `Promise.resolve(client)` would invoke `then(resolve, reject)`,
      // which resolves to the "Unknown query" rejection of an ignored promise instead of
      // settling the awaiting promise — a silent hang rather than a throw.
      if (prop === 'then') return undefined;
      // async so every failure — unknown query, stopped runtime, args validation inside
      // resolve() — surfaces as a rejection of the returned promise, never a sync throw.
      return async (...callArgs: unknown[]) => {
        const pulseQuery = runtime.registry.getPulseQuery(prop);
        if (!pulseQuery) throw new Error(`Unknown query: "${prop}"`);
        if (!runtime.isRunning)
          throw new Error('PulseCollection can only be created after runtime.start()');
        if (pulseQuery.hasTransform)
          throw new Error('Queries with .transform() are not supported in the embedded client');
        if (pulseQuery.limit !== null)
          throw new Error('Queries with .limit() are not supported in the embedded client');

        // Split the optional trailing PulseCollectionOptions from query args.
        // Args queries: (queryArgs, options?) — options is callArgs[1].
        // No-args queries: (options?) — options is callArgs[0].
        let rawArgs: unknown;
        let options: PulseCollectionOptions | undefined;

        if (pulseQuery.argsSchema !== null) {
          rawArgs = callArgs[0];
          options = callArgs[1] as PulseCollectionOptions | undefined;
        } else {
          rawArgs = {};
          options = callArgs[0] as PulseCollectionOptions | undefined;
        }

        const auth: PulseAuthContext = options?.auth ?? { userId: null };
        // resolve() validates args and yields the source table + auth-scoped WHERE — the
        // same gate must scope both the baseline read and every tapped event below.
        const resolved = runtime.registry.resolve(prop, rawArgs, auth);
        const tableKey = getTableUniqueName(resolved.table);

        const core = new PulseMergeCore<AnyRow>({ order: resolved.order });

        // Tap-direct handshake state: while `baselining` is up, tapped payloads are buffered
        // instead of applied so nothing committed during the baseline SELECT is lost or
        // double-applied — the buffer is drained afterward against the read watermark.
        let baselining = true;
        let buffer: WalTapPayload[] = [];
        let collection!: PulseCollection<AnyRow>;

        function applyPayload(payload: WalTapPayload): void {
          const event = buildTapEvent(payload, resolved);
          if (!event) return;
          const mutated = core.applyEvents([event]);
          if (mutated) collection.fireOnChange([event], payload.lsn);
        }

        function handleTapPayload(payload: WalTapPayload): void {
          if (baselining) {
            buffer.push(payload);
            return;
          }
          applyPayload(payload);
        }

        // Same handshake for the initial load AND every re-baseline (reconnect): subscribe
        // first (buffering), read the baseline + watermark, rebuild state, then drain the
        // buffer. A payload at-or-above the watermark is applied; below-watermark payloads
        // are guaranteed already present in the baseline. At-or-above (not strictly greater)
        // is required because a commit written exactly at the watermark position was
        // necessarily written at-or-after the watermark read — strict greater-than would risk
        // dropping a commit landing exactly there. Any baseline/tap overlap this admits is
        // absorbed by the merge core's $pk dedup, making the handshake exactly-once.
        //
        // `baselining`/`buffer` are shared closure state, so overlapping invocations (a
        // reconnect racing the initial load, or reconnect flapping) must not both act on them:
        // the generation token lets a superseded handshake detect it lost the race and
        // abandon its (possibly stale) results instead of rebuilding over/under a newer
        // handshake's state. Returns `null` when superseded — the caller must not treat that
        // as "no watermark", only as "a newer handshake owns the collection now".
        let handshakeGen = 0;
        async function runHandshake(): Promise<string | null> {
          const gen = ++handshakeGen;
          baselining = true;
          buffer = [];
          const { rows, watermark } = await runtime.readCollectionBaseline(resolved);
          if (gen !== handshakeGen) return null;
          core.rebuildFromRows(applyProjectionPipeline(rows, resolved) as AnyRow[]);
          const pending = buffer;
          buffer = [];
          baselining = false;
          for (const payload of pending) {
            if (compareLsn(payload.lsn, watermark) < 0) continue;
            applyPayload(payload);
          }
          return watermark;
        }

        const unsubs: Array<() => void> = [];
        collection = new PulseCollection(core, () => {
          for (const unsub of unsubs) unsub();
        });

        unsubs.push(runtime.walEventEmitter.subscribe(tableKey, handleTapPayload));
        unsubs.push(
          runtime.onReconnect(() => {
            void (async () => {
              try {
                const watermark = await runHandshake();
                if (watermark === null) return; // superseded by a newer reconnect handshake
                if (collection.isDisposed) return;
                collection.fireOnChange([], watermark);
              } catch (err) {
                if (collection.isDisposed) return;
                collection.fireOnError(err instanceof Error ? err : new Error(String(err)));
              }
            })();
          }),
        );
        unsubs.push(runtime.onTerminalError((error) => collection.fireOnError(error)));
        unsubs.push(runtime.onStop(() => collection.dispose()));

        // Initial load via the watermark handshake; init failures reject the factory promise
        // (not onError — there is no collection to report to yet).
        let initError: unknown;
        try {
          await runHandshake();
        } catch (err) {
          initError = err;
        }
        if (collection.isDisposed) {
          throw new Error('PulseCollection was disposed before the initial sync completed');
        }
        if (initError !== undefined) {
          collection.dispose();
          throw initError instanceof Error ? initError : new Error(String(initError));
        }
        return collection;
      };
    },
  });
}
