import { getTableUniqueName } from 'drizzle-orm';
import type { PulseRuntime } from '../../server/expose.js';
import type { AnyPulseBuilders } from '../../server/pulse-registry.js';
import type { PulseClientContract } from '../../server/pulse-types.js';
import type { PulseEvent } from '../../shared/pulse-events.js';
import type { PulseAuthContext, QueryDescriptor } from '../../types.js';
import { buildTapEvent, type TapRow } from './tap-events.js';

// Must stay free of merge-core/collection value imports (SPLIT-06): there is no baseline and
// no state to reconcile here, only a WHERE-filtered tap. Value imports limited to ./tap-events.js
// and bare drizzle-orm; platform-imports.test.ts enforces purity across the embedded graph.

export type PulseEventsCallback<TRow> = (event: PulseEvent<TRow>, lsn: string) => void;

export interface PulseEventsOptions {
  auth?: PulseAuthContext;
}

export type EmbeddedPulseEvents<TQueries extends AnyPulseBuilders> = {
  [K in keyof PulseClientContract<TQueries>]: PulseClientContract<TQueries>[K] extends (
    ...args: infer A
  ) => QueryDescriptor<infer R>
    ? A extends []
      ? (
          callback: PulseEventsCallback<R & { $pk: unknown }>,
          options?: PulseEventsOptions,
        ) => () => void
      : (
          ...args: [
            ...A,
            callback: PulseEventsCallback<R & { $pk: unknown }>,
            options?: PulseEventsOptions,
          ]
        ) => () => void
    : never;
};

// ---------------------------------------------------------------------------
// createPulseEvents factory — stateless, per-event WAL subscription (SPLIT-06).
// ---------------------------------------------------------------------------

export function createPulseEvents<TQueries extends AnyPulseBuilders>(
  runtime: PulseRuntime<TQueries>,
): EmbeddedPulseEvents<TQueries> {
  return new Proxy({} as EmbeddedPulseEvents<TQueries>, {
    get(_target, prop: string | symbol) {
      if (typeof prop !== 'string') return undefined;
      if (prop === 'then') return undefined;
      // Synchronous: materialization failures (unknown query, stopped runtime, .transform()/
      // .limit() rejections, args validation inside resolve()) throw immediately — there is
      // no baseline to await, so nothing here is asynchronous.
      return (...callArgs: unknown[]): (() => void) => {
        const pulseQuery = runtime.registry.getPulseQuery(prop);
        if (!pulseQuery) throw new Error(`Unknown query: "${prop}"`);
        if (!runtime.isRunning)
          throw new Error('PulseEvents subscription can only be created after runtime.start()');
        if (pulseQuery.hasTransform)
          throw new Error('Queries with .transform() are not supported in the embedded client');
        if (pulseQuery.limit !== null)
          throw new Error('Queries with .limit() are not supported in the embedded client');

        // Split (rawArgs?, callback, options?) the same way collections split (rawArgs?, options?).
        let rawArgs: unknown;
        let callback: PulseEventsCallback<TapRow>;
        let options: PulseEventsOptions | undefined;

        if (pulseQuery.argsSchema !== null) {
          rawArgs = callArgs[0];
          callback = callArgs[1] as PulseEventsCallback<TapRow>;
          options = callArgs[2] as PulseEventsOptions | undefined;
        } else {
          rawArgs = {};
          callback = callArgs[0] as PulseEventsCallback<TapRow>;
          options = callArgs[1] as PulseEventsOptions | undefined;
        }

        if (typeof callback !== 'function') {
          throw new Error(
            `createPulseEvents: expected a callback function, got ${typeof callback}`,
          );
        }

        const auth: PulseAuthContext = options?.auth ?? { userId: null };
        // resolve() validates args and yields the auth-scoped WHERE — the same gate every
        // tapped event is filtered through below.
        const resolved = runtime.registry.resolve(prop, rawArgs, auth);
        const tableKey = getTableUniqueName(resolved.table);

        // The WAL listener processes pgoutput messages sequentially and the emitter dispatches
        // synchronously off that, so subscription-time delivery is already commit order — no
        // reordering buffer needed (unlike the collections' baseline/watermark handshake).
        const tapUnsub = runtime.walEventEmitter.subscribe(tableKey, (payload) => {
          const event = buildTapEvent(payload, resolved);
          if (!event) return;
          callback(event, payload.lsn);
        });

        let detached = false;
        const detach = (): void => {
          if (detached) return;
          detached = true;
          tapUnsub();
          stopUnsub();
        };
        const stopUnsub = runtime.onStop(() => detach());

        return detach;
      };
    },
  });
}
