import { QueryDescriptor } from '../types.js';
import { deserializeResponse } from './superjson.js';

type PullSubscription = {
  subscriptionId: string;
  snapshot: number;
};

type PullSubscriberHandle = {
  getSubscriptionState: () => PullSubscription | null;
  applyPullResult: (result: unknown) => void;
  onPullError: (error: Error) => void;
};

type Fetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

function createClientId() {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }

  return `pulse-client-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export class PullClient {
  readonly clientId = createClientId();

  private readonly handles = new Map<string, PullSubscriberHandle>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private inFlight: Promise<void> | null = null;
  readonly fetch: Fetch;

  constructor(
    private readonly endpoint: string,
    fetchImpl = fetch,
    private readonly pollIntervalMs = 1000,
  ) {
    this.fetch = (input, init) => fetchImpl(input, init);
  }

  register(key: string, handle: PullSubscriberHandle) {
    this.handles.set(key, handle);
    this.ensureTimer();
  }

  unregister(key: string) {
    this.handles.delete(key);
    this.clearTimerIfIdle();
  }

  async pollNow() {
    if (this.inFlight) {
      return this.inFlight;
    }

    const subscriptions: PullSubscription[] = [];
    const handleBySubscriptionId = new Map<string, PullSubscriberHandle>();
    for (const handle of this.handles.values()) {
      const state = handle.getSubscriptionState();
      if (!state) {
        continue;
      }

      subscriptions.push(state);
      handleBySubscriptionId.set(state.subscriptionId, handle);
    }

    if (subscriptions.length === 0) {
      return;
    }

    this.inFlight = (async () => {
      try {
        const response = await this.fetch(`${this.endpoint}/pull`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            clientId: this.clientId,
            subscriptions,
          } satisfies {
            clientId: string;
            subscriptions: PullSubscription[];
          }),
          credentials: 'include',
        });

        if (!response.ok) {
          throw new Error(`Pull failed: ${response.status}`);
        }

        const result = deserializeResponse<{
          results: Record<string, unknown>;
        }>(await response.text());
        for (const subscription of subscriptions) {
          const handle = handleBySubscriptionId.get(subscription.subscriptionId);
          const pullResult = result.results[subscription.subscriptionId];
          if (!handle || pullResult === undefined) {
            continue;
          }

          handle.applyPullResult(pullResult);
        }
      } catch (error) {
        const normalizedError = error instanceof Error ? error : new Error(String(error));
        for (const handle of this.handles.values()) {
          handle.onPullError(normalizedError);
        }
      } finally {
        this.inFlight = null;
      }
    })();

    return this.inFlight;
  }

  private ensureTimer() {
    if (this.timer !== null || this.handles.size === 0 || this.pollIntervalMs <= 0) {
      return;
    }

    this.timer = setInterval(() => {
      void this.pollNow();
    }, this.pollIntervalMs);
  }

  private clearTimerIfIdle() {
    if (this.timer !== null && this.handles.size === 0) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

/**
 * Creates a type-safe client proxy for calling pulse queries.
 * TClient is typically `ClientContract<...>` from the server's `typeof drizzlePulse.$client`.
 * At runtime, returns a Proxy that constructs QueryDescriptor objects.
 */
export function createPulseClient<TClient>(config: {
  url: string;
  fetchImpl?: typeof fetch;
  /** Background auto-poll interval in ms (default 1000). Set to 0 to disable auto-polling and drive `poll()` manually. */
  pollIntervalMs?: number;
}): TClient {
  const fetchImpl = config.fetchImpl ?? fetch;
  const pullClient = new PullClient(config.url, fetchImpl, config.pollIntervalMs);

  return new Proxy(
    {},
    {
      get(_target, prop: string) {
        return (args?: Record<string, unknown>) =>
          new QueryDescriptor(prop, args ?? {}, config.url, pullClient);
      },
    },
  ) as TClient;
}
