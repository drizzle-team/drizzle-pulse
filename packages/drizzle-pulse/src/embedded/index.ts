import { drizzle } from 'drizzle-orm/postgres';
import { createPool } from 'minipg';
import {
  createPulseClient,
  createPulseEvents,
  type EmbeddedPulseClient,
  type EmbeddedPulseEvents,
} from '../client/embedded/index.js';
import { type AnyPulseBuilders, createPulseRegistry } from '../server/pulse-registry.js';
import {
  type LogLevel,
  PulseRuntime,
  type PulseRuntimeWalConfig,
} from '../server/pulse-runtime.js';

export interface EmbeddedRuntimeConfig {
  /** Must have `wal_level=logical`. Every connection the runtime opens comes from this URL. */
  databaseUrl: string;
  wal?: PulseRuntimeWalConfig;
  logLevel?: LogLevel;
}

export interface EmbeddedRuntime<TQueries extends AnyPulseBuilders> {
  client: EmbeddedPulseClient<TQueries>;
  events: EmbeddedPulseEvents<TQueries>;
  start(): Promise<void>;
  stop(): Promise<void>;
  /** Provisions replication prerequisites without opening WAL — for elevated-role deploy steps. */
  provision(): Promise<void>;
  /** Fires once replication gives up permanently (reconnect attempts exhausted). */
  onFatalError(listener: (error: Error) => void): () => void;
}

/**
 * Embedded-only pulse: collections and event subscriptions served in-process over the WAL tap,
 * with no HTTP pull protocol and no events-table storage. The registry and the `pull: false`
 * runtime are wired internally; baseline reads run on a pulse-owned pool built from
 * `databaseUrl`, so row scoping comes from each query's resolve-time auth-scoped WHERE, not
 * from any session context (RLS, search_path) an app connection would carry.
 */
export function createRuntime<TQueries extends AnyPulseBuilders>(
  queries: TQueries,
  config: EmbeddedRuntimeConfig,
): EmbeddedRuntime<TQueries> {
  // Lazy pool: no connection opens before the first collection baseline, so an unstarted or
  // failed-start handle holds no sockets and start() stays retryable.
  const pool = createPool(config.databaseUrl);
  const runtime = new PulseRuntime(createPulseRegistry(queries), {
    databaseUrl: config.databaseUrl,
    sourceDb: drizzle({ client: pool }),
    pull: false,
    wal: config.wal,
    logLevel: config.logLevel,
  });

  // Latched on the first stop() call: overlapping stop()s await the same teardown instead of
  // resolving while connections are still open, and start() reads it as "terminal".
  let stopping: Promise<void> | null = null;
  const handle: EmbeddedRuntime<TQueries> = {
    client: createPulseClient(runtime),
    events: createPulseEvents(runtime),
    async start() {
      // pool.end() is permanent in minipg, so a stopped handle can never stream again.
      if (stopping) {
        throw new Error('This runtime has been stopped — create a new one to start again');
      }
      await runtime.start();
    },
    stop() {
      stopping ??= (async () => {
        try {
          await runtime.stop();
        } finally {
          await pool.end();
        }
      })();
      return stopping;
    },
    provision: () => runtime.provision(),
    onFatalError: (listener) => runtime.onTerminalError(listener),
  };

  // A terminal replication error self-stops the runtime, but the source pool is this
  // factory's to release — run the handle's own teardown so the pool dies with it. Deferred
  // a microtask so the runtime's terminal-listener loop (and any app onFatalError handler
  // registered after this one) finishes firing before teardown starts.
  runtime.onTerminalError(() => {
    queueMicrotask(() => void handle.stop());
  });

  return handle;
}

export {
  type EmbeddedPulseClient,
  type EmbeddedPulseEvents,
  PulseCollection,
  type PulseCollectionChange,
  type PulseCollectionOptions,
  type PulseEventsCallback,
  type PulseEventsOptions,
  type PulseRow,
} from '../client/embedded/index.js';
export type { PulseBuilder } from '../server/pulse-builder.js';
export type { AnyPulseBuilders } from '../server/pulse-registry.js';
export { LogLevel, type PulseRuntimeWalConfig } from '../server/pulse-runtime.js';
export type {
  PulseDeleteEvent,
  PulseEvent,
  PulseInsertEvent,
  PulseUpdateEvent,
} from '../shared/pulse-events.js';
export type { PulseAuthContext } from '../types.js';
