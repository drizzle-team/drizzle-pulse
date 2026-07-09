import { getTableUniqueName } from 'drizzle-orm';
import type { RealtimeRuntime } from '../../server/expose.js';
import type { AnyPulseBuilders } from '../../server/pulse-registry.js';
import type { PulseClientContract } from '../../server/pulse-types.js';
import type { PulseRouterHandlers } from '../../server/router.js';
import type { PulseAuthContext } from '../../types.js';
import { QueryDescriptor } from '../../types.js';
import type { PulseEvent, PulseQueryState } from '../pulse-query.js';
import { PulseQuery } from '../pulse-query.js';
import type { PullResultMap, PulseQueryTransport } from '../transport.js';

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

type AnyRow = Record<string, unknown> & { $pk: unknown };

// ---------------------------------------------------------------------------
// Direct transport — the in-process SDK handler, no HTTP, no superjson, no PullClient.
// Constructed per collection so its calls carry that collection's auth.
// ---------------------------------------------------------------------------

function createDirectTransport(
  handlers: PulseRouterHandlers,
  auth: PulseAuthContext,
): PulseQueryTransport {
  return {
    async subscribe(request) {
      const { status, body } = await handlers.subscribe(request, auth);
      if ('error' in body) throw new Error(body.error);
      if (status !== 200) throw new Error(`Subscribe failed: ${status}`);
      return body as Awaited<ReturnType<PulseQueryTransport['subscribe']>>;
    },
    async pull(entries) {
      const { status, body } = await handlers.pull({ subscriptions: entries }, auth);
      if ('error' in body) throw new Error(body.error);
      if (status !== 200) throw new Error(`Pull failed: ${status}`);
      return body.results as PullResultMap;
    },
    async loadMore(request) {
      const { status, body } = await handlers.loadMore(request, auth);
      if ('error' in body) throw new Error(body.error);
      if (status !== 200) throw new Error(`Load more failed: ${status}`);
      return body as Awaited<ReturnType<PulseQueryTransport['loadMore']>>;
    },
  };
}

// Cursor tokens are `epoch:snapshot`; PulseCollectionChange exposes the snapshot number.
function snapshotNumber(token: string): number {
  const separator = token.indexOf(':');
  return separator >= 0 ? Number(token.slice(separator + 1)) || 0 : 0;
}

// ---------------------------------------------------------------------------
// PulseCollection — a thin facade over a PulseQuery driven by the direct transport.
// ---------------------------------------------------------------------------

export class PulseCollection<TRow extends { $pk: unknown }> {
  private disposed = false;
  private readonly onChangeListeners = new Set<(change: PulseCollectionChange<TRow>) => void>();

  /** @internal */
  constructor(
    /** @internal */
    readonly query: PulseQuery<TRow & Record<string, unknown>>,
    private readonly teardown: () => void = () => {},
  ) {}

  /** @internal */
  get isDisposed(): boolean {
    return this.disposed;
  }

  list(): readonly TRow[] {
    return this.query.getState().data as readonly TRow[];
  }

  /** The full query state: `data` plus the loading/pagination/error flags. */
  getState(): PulseQueryState<TRow> {
    return this.query.getState() as PulseQueryState<TRow>;
  }

  /** Fetches the next page of a `.limit()` query, extending `list()` in place. */
  loadMore(): Promise<void> {
    return this.query.loadMore();
  }

  onChange(listener: (change: PulseCollectionChange<TRow>) => void): () => void {
    this.onChangeListeners.add(listener);
    return () => this.onChangeListeners.delete(listener);
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.query.destroy();
    this.teardown();
  }

  /** @internal fed by the PulseQuery onEvents hook. */
  fireOnChange(
    events: readonly PulseEvent<TRow>[],
    state: readonly TRow[],
    snapshot: number,
  ): void {
    const change: PulseCollectionChange<TRow> = { events, state, snapshot };
    for (const listener of this.onChangeListeners) {
      try {
        listener(change);
      } catch (e) {
        console.error('[PulseCollection] onChange error:', e);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// createPulseClient factory
// ---------------------------------------------------------------------------

export function createPulseClient<TQueries extends AnyPulseBuilders>(
  runtime: RealtimeRuntime<TQueries>,
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
        // resolve() validates args and yields the source table for the change signal.
        const resolved = runtime.registry.resolve(prop, rawArgs, auth);
        const tableKey = getTableUniqueName(resolved.table);

        const transport = createDirectTransport(runtime.handlers, auth);
        const descriptor = new QueryDescriptor<AnyRow>(
          prop,
          (rawArgs ?? {}) as Record<string, unknown>,
          '',
          transport,
        );

        const unsubs: Array<() => void> = [];
        let collection!: PulseCollection<AnyRow>;
        const query = new PulseQuery(descriptor, {
          onEvents: (events, state, token) =>
            collection.fireOnChange(events, state, snapshotNumber(token)),
        });
        collection = new PulseCollection(query, () => {
          for (const unsub of unsubs) unsub();
        });

        // Pull only when nudged: a WAL commit on the source table, or a reconnect (catch up on
        // events missed while the stream was down). Stop tears the collection down.
        unsubs.push(runtime.walEventEmitter.subscribe(tableKey, () => void query.poll()));
        unsubs.push(runtime.onReconnect(() => void query.poll()));
        unsubs.push(runtime.onStop(() => collection.dispose()));

        // Initial load via the server's baseline SELECT; the factory resolves once it lands.
        await query.subscribe();
        if (collection.isDisposed) {
          throw new Error('PulseCollection was disposed before the initial sync completed');
        }
        const error = query.getState().error;
        if (error) {
          collection.dispose();
          throw error;
        }
        // Catch up on any event committed between subscribe's snapshot read and now.
        await query.poll();
        return collection;
      };
    },
  });
}
