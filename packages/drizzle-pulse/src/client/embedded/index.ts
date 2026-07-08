import { getTableUniqueName } from 'drizzle-orm';
import type { RealtimeRuntime } from '../../server/expose.js';
import { applyProjectionPipeline } from '../../server/pulse-projection.js';
import type { AnyPulseBuilders } from '../../server/pulse-registry.js';
import { buildSelectQuery } from '../../server/pulse-sql.js';
import type { PulseClientContract } from '../../server/pulse-types.js';
import type { WalTapPayload } from '../../server/wal-event-emitter.js';
import { extractRow } from '../../shared/event-normalization.js';
import { evaluateCondition } from '../../shared/filter-ast.js';
import { PulseMergeCore } from '../../shared/pulse-merge-core.js';
import type { PulseAuthContext, QueryDescriptor, ResolvedPulseQuery } from '../../types.js';
import type { PulseEvent } from '../pulse-query.js';

// ---------------------------------------------------------------------------
// Public type surface
// ---------------------------------------------------------------------------

export interface PulseCollectionOptions {
  auth?: PulseAuthContext;
}

export interface PulseCollectionChange<T> {
  events: readonly PulseEvent<T>[];
  state: readonly T[];
  snapshot: number;
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

// ---------------------------------------------------------------------------
// PulseCollection
// ---------------------------------------------------------------------------

export class PulseCollection<TRow extends { $pk: unknown }> {
  /** @internal */
  readonly core: PulseMergeCore<TRow & Record<string, unknown>>;
  // True while the collection is live: set once the initial sync completes (before the
  // factory promise resolves), cleared again by dispose().
  isReady = false;
  private disposed = false;
  private unsubscribe: (() => void) | null = null;
  private readonly onChangeListeners = new Set<(change: PulseCollectionChange<TRow>) => void>();
  /** @internal */
  baselineSnapshot = 0;
  private rebaselineBuffer: WalTapPayload[] | null = null;
  private isRebaselining = false;

  /** @internal */
  constructor(
    /** @internal */
    readonly runtime: RealtimeRuntime<AnyPulseBuilders>,
    /** @internal */
    readonly query: ResolvedPulseQuery,
    private readonly deregister: () => void = () => {},
  ) {
    this.core = new PulseMergeCore<TRow & Record<string, unknown>>({
      order: query.order,
      limit: null,
      rangeStart: null,
      rangeEnd: null,
    });
  }

  list(): readonly TRow[] {
    return this.core.data;
  }

  onChange(listener: (change: PulseCollectionChange<TRow>) => void): () => void {
    this.onChangeListeners.add(listener);
    return () => this.onChangeListeners.delete(listener);
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.isReady = false;
    this.unsubscribe?.();
    this.unsubscribe = null;
    this.core.clear();
    this.deregister();
  }

  /** @internal */
  fireOnChange(events: PulseEvent<TRow>[], snapshot: number): void {
    const change: PulseCollectionChange<TRow> = { events, state: this.core.data, snapshot };
    for (const listener of this.onChangeListeners) {
      try {
        listener(change);
      } catch (e) {
        console.error('[PulseCollection] onChange error:', e);
      }
    }
  }

  /** One-time initial sync; the factory awaits it, so callers only ever see a ready collection. @internal */
  async startHandshake(): Promise<void> {
    try {
      const buffer: WalTapPayload[] = [];
      const tableKey = getTableUniqueName(this.query.table);

      // [1] Register tap listener FIRST — events are buffered until baseline completes.
      this.unsubscribe = this.runtime.walEventEmitter.subscribe(tableKey, (payload) => {
        if (!this.isReady) {
          buffer.push(payload);
        } else {
          this.applyTapPayload(payload);
        }
      });

      // [2] Read baselineSnapshot AFTER tap registration.
      // lastPersistedSnapshot is updated before every emit, so the baselineSnapshot is a valid
      // lower bound for any event that could have been missed between here and the SELECT.
      const baselineSnapshot = this.runtime.lastPersistedSnapshot;
      this.baselineSnapshot = baselineSnapshot;

      if (this.disposed) return;

      // [3] Baseline SELECT — the ONLY DB read in this collection's lifetime.
      const projected = await this.fetchBaselineRows();

      if (this.disposed) return;

      this.core.rebuildFromRows(projected);

      // [4] Drain buffer: apply only events with $snapshot > baselineSnapshot.
      // The merge core's $pk idempotent backstop handles any row committed mid-SELECT
      // that appears in both the baseline rows and the buffer.
      for (const payload of buffer) {
        if (payload.$snapshot > baselineSnapshot) {
          this.applyTapPayload(payload);
        }
      }

      if (this.disposed) return;

      // [5] Mark ready; subsequent tap events are applied live.
      this.isReady = true;
    } catch (err) {
      // Detach tap listener so buffered events don't accumulate.
      this.unsubscribe?.();
      this.unsubscribe = null;
      // If dispose() ran concurrently, the caller no longer cares about the outcome —
      // swallow instead of rejecting the factory promise.
      if (!this.disposed) throw err;
    }
  }

  private async fetchBaselineRows(): Promise<(TRow & Record<string, unknown>)[]> {
    // Override limit to null: full set regardless of any query-level limit.
    const rawRows = await buildSelectQuery(this.runtime.sourceDb, this.query.table, {
      ...this.query,
      limit: null,
    });
    return applyProjectionPipeline(rawRows as Record<string, unknown>[], this.query) as (TRow &
      Record<string, unknown>)[];
  }

  /** @internal */
  applyTapPayload(payload: WalTapPayload): void {
    if (this.rebaselineBuffer !== null) {
      this.rebaselineBuffer.push(payload);
      return;
    }
    const { operation, rowData, oldRowData, $snapshot } = payload;
    const query = this.query;

    // Normalize rows from WAL using plain SQL column names — NO $old_ prefix.
    // (The $old_ prefix applies only when reading from the events TABLE; WAL
    // oldRowData is keyed by the original SQL column names.)
    const newRow = extractRow(rowData, query.columns);
    const oldRow = oldRowData ? extractRow(oldRowData, query.columns) : null;

    const matchesNew = newRow ? evaluateCondition(query.where, newRow) : false;
    const matchesOld = oldRow ? evaluateCondition(query.where, oldRow) : false;

    const pkValue =
      operation === 'delete' ? oldRowData?.[query.pkColumn.name] : rowData[query.pkColumn.name];

    let event: PulseEvent<TRow & Record<string, unknown>>;

    if (operation === 'insert' && newRow && matchesNew) {
      const row = applyProjectionPipeline([newRow], query)[0] as TRow & Record<string, unknown>;
      event = { op: 'insert', row, pk: pkValue };
    } else if (operation === 'update') {
      if (!matchesNew && !matchesOld) return;
      const row = newRow
        ? (applyProjectionPipeline([newRow], query)[0] as TRow & Record<string, unknown>)
        : ({} as TRow & Record<string, unknown>);
      const old_row = oldRow
        ? (applyProjectionPipeline([oldRow], query)[0] as Record<string, unknown>)
        : {};
      event = { op: 'update', row, old_row, pk: pkValue, matchesNew, matchesOld };
    } else if (operation === 'delete' && oldRow && matchesOld) {
      const old_row = applyProjectionPipeline([oldRow], query)[0] as Record<string, unknown>;
      event = { op: 'delete', old_row, pk: pkValue, matchesOld };
    } else {
      return;
    }

    const mutated = this.core.applyEvents([event]);
    if (mutated && this.isReady) {
      this.fireOnChange([event], $snapshot);
    }
  }

  /** Re-syncs a live collection after a WAL reconnect gap, keeping listeners intact — never re-runs the handshake. @internal */
  async rebaseline(): Promise<void> {
    if (this.disposed) return;
    if (this.isRebaselining) return;
    // A collection still completing startHandshake establishes its own fresh baseline;
    // rebaselining it concurrently would race the handshake and drop events. Skip until
    // ready — the handshake already reflects the reconnect's committed state.
    if (!this.isReady) return;
    this.isRebaselining = true;
    // Both kept in this scope so the catch can recover events the drain (step [4]) left
    // unapplied when interrupted by a throw, using the same baselineSnapshot filter.
    let baselineSnapshot = 0;
    let pendingDrain: WalTapPayload[] | null = null;
    try {
      // [1] Open buffer synchronously before any await — no WAL event can slip past.
      this.rebaselineBuffer = [];

      // [2] Baseline snapshot after buffer is open (safe lower bound for drain filter).
      baselineSnapshot = this.runtime.lastPersistedSnapshot;

      if (this.disposed) {
        this.rebaselineBuffer = null;
        return;
      }

      // [3] Fresh baseline SELECT — same pattern as startHandshake; WAL events push to buffer.
      const projected = await this.fetchBaselineRows();

      if (this.disposed) {
        this.rebaselineBuffer = null;
        return;
      }

      this.core.rebuildFromRows(projected);

      // [4] Drain buffer: null the field first so live events resume, but keep the
      //     drained array in `pendingDrain` (this scope) so the catch can recover it if
      //     applyTapPayload throws mid-drain. Same $snapshot > baselineSnapshot filter as
      //     startHandshake.
      pendingDrain = this.rebaselineBuffer;
      this.rebaselineBuffer = null;
      while (pendingDrain.length > 0) {
        // shift() so an applied event leaves pendingDrain — if the loop throws, the
        // catch recovers only the un-applied remainder (no duplicate onChange re-fires).
        const payload = pendingDrain.shift();
        if (payload === undefined) break;
        if (!this.disposed && payload.$snapshot > baselineSnapshot) {
          this.applyTapPayload(payload);
        }
      }
      pendingDrain = null;

      // [5] Fire onChange to signal list() ref changed — the collection stays live.
      if (!this.disposed) {
        this.fireOnChange([], this.runtime.lastPersistedSnapshot);
      }
    } catch (err) {
      // Recover only events NOT yet applied — the interrupted drain shift()ed applied
      // ones out of pendingDrain, so they are not re-fired. If the error preceded the
      // drain, recover the whole buffer. Same baselineSnapshot filter as the drain.
      const remaining = pendingDrain ?? this.rebaselineBuffer;
      this.rebaselineBuffer = null;
      if (remaining) {
        for (const payload of remaining) {
          if (!this.disposed && payload.$snapshot > baselineSnapshot) {
            this.applyTapPayload(payload);
          }
        }
      }
      console.error('[PulseCollection] rebaseline error:', err);
    } finally {
      this.isRebaselining = false;
    }
  }
}

// ---------------------------------------------------------------------------
// PulseClient — owns the collections it creates, rebaselines them when the WAL
// stream reconnects, and disposes them when the runtime stops. The runtime only
// broadcasts those edges (RealtimeRuntime.onReconnect / onStop); it does not know
// about collections.
// ---------------------------------------------------------------------------

type AnyPulseCollection = PulseCollection<Record<string, unknown> & { $pk: unknown }>;

class PulseClient {
  private readonly collections = new Set<AnyPulseCollection>();
  private readonly unsubscribeReconnect: () => void;
  private readonly unsubscribeStop: () => void;

  private rebaselineDebounceTimer: ReturnType<typeof setTimeout> | null = null;
  private rebaselinePending = false;
  private rebaselinesInFlight = false;
  private readonly rebaselineConcurrencyLimit = 3;
  private readonly rebaselineDebounceMs = 50;

  constructor(private readonly runtime: RealtimeRuntime<AnyPulseBuilders>) {
    this.unsubscribeReconnect = runtime.onReconnect(() => this.scheduleRebaseline());
    this.unsubscribeStop = runtime.onStop(() => this.disposeAll());
  }

  async create(resolved: ResolvedPulseQuery): Promise<AnyPulseCollection> {
    const collection: AnyPulseCollection = new PulseCollection(this.runtime, resolved, () => {
      this.collections.delete(collection);
    });
    this.collections.add(collection);
    // startHandshake()'s body runs synchronously up to its baseline SELECT, so the WAL tap
    // is registered before the factory call returns — no event between the two is missed.
    try {
      await collection.startHandshake();
    } catch (err) {
      collection.dispose();
      throw err;
    }
    // A dispose() racing the handshake (runtime.stop() mid-SELECT) makes startHandshake
    // bail without marking ready; reject rather than resolve with a dead collection.
    if (!collection.isReady) {
      collection.dispose();
      throw new Error('PulseCollection was disposed before the initial sync completed');
    }
    return collection;
  }

  private disposeAll(): void {
    for (const collection of [...this.collections]) {
      collection.dispose();
    }
    this.collections.clear();
    this.unsubscribeReconnect();
    this.unsubscribeStop();
    if (this.rebaselineDebounceTimer !== null) {
      clearTimeout(this.rebaselineDebounceTimer);
      this.rebaselineDebounceTimer = null;
    }
    this.rebaselinePending = false;
    this.rebaselinesInFlight = false;
  }

  // Debounced, concurrency-limited rebaseline of every live collection. Triggers that
  // arrive while a pass is in flight set `rebaselinePending` and are drained by the loop.
  private scheduleRebaseline(): void {
    this.rebaselinePending = true;
    if (this.rebaselineDebounceTimer !== null || this.rebaselinesInFlight) return;
    this.rebaselineDebounceTimer = setTimeout(() => {
      this.rebaselineDebounceTimer = null;
      void this.flushRebaselineQueue();
    }, this.rebaselineDebounceMs);
  }

  private async flushRebaselineQueue(): Promise<void> {
    if (this.rebaselinesInFlight) return;
    this.rebaselinesInFlight = true;
    try {
      while (this.rebaselinePending) {
        this.rebaselinePending = false;
        await this.runRebaselines();
      }
    } finally {
      this.rebaselinesInFlight = false;
    }
  }

  private async runRebaselines(): Promise<void> {
    const eligible = [...this.collections];
    for (let i = 0; i < eligible.length; i += this.rebaselineConcurrencyLimit) {
      const batch = eligible.slice(i, i + this.rebaselineConcurrencyLimit);
      await Promise.all(batch.map((collection) => collection.rebaseline()));
    }
  }
}

// ---------------------------------------------------------------------------
// createPulseClient factory
// ---------------------------------------------------------------------------

export function createPulseClient<TQueries extends AnyPulseBuilders>(
  runtime: RealtimeRuntime<TQueries>,
): EmbeddedPulseClient<TQueries> {
  const client = new PulseClient(runtime as unknown as RealtimeRuntime<AnyPulseBuilders>);
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
      // The body still runs synchronously up to create()'s baseline SELECT, preserving the
      // register-tap-before-returning invariant.
      return async (...callArgs: unknown[]) => {
        const pulseQuery = runtime.registry.getPulseQuery(prop);
        if (!pulseQuery) throw new Error(`Unknown query: "${prop}"`);
        if (!runtime.isRunning)
          throw new Error('PulseCollection can only be created after runtime.start()');
        if (pulseQuery.hasTransform)
          throw new Error('Queries with .transform() are not supported in the embedded client');

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
        const resolved = runtime.registry.resolve(prop, rawArgs, auth);

        return client.create(resolved);
      };
    },
  });
}
