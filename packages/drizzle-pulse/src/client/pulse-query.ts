import { comparePkValues, isPkComparable } from '../shared/pk-utils.js';
import type {
  LoadMoreResponse as ProtocolLoadMoreResponse,
  PullResponse as ProtocolPullResponse,
  SubscribeResponse as ProtocolSubscribeResponse,
} from '../shared/protocol-types.js';
import type { PulseEvent } from '../shared/pulse-events.js';
import { PulseMergeCore } from '../shared/pulse-merge-core.js';
import type { QueryDescriptor } from '../types.js';
import type { PullClient } from './create-client.js';
import { deserializeResponse } from './superjson.js';

export type {
  PulseDeleteEvent,
  PulseEvent,
  PulseInsertEvent,
  PulseUpdateEvent,
} from '../shared/pulse-events.js';

export interface PulseQueryState<TResult> {
  data: readonly TResult[];
  isLoading: boolean;
  isLoadingMore: boolean;
  hasMore: boolean;
  error: Error | null;
}

export type CreatePulseQueryOptions<TResult extends Record<string, unknown> & { $pk: unknown }> = {
  onStateChange?: (state: PulseQueryState<TResult>) => void;
};

export class PulseQuery<TResult extends Record<string, unknown> & { $pk: unknown }> {
  private readonly onStateChange?: (state: PulseQueryState<TResult>) => void;
  private readonly pullClient: PullClient;
  private readonly queryKey: string;

  private state: PulseQueryState<TResult> = {
    data: [],
    isLoading: true,
    isLoadingMore: false,
    hasMore: false,
    error: null,
  };

  private readonly core: PulseMergeCore<TResult>;
  private subscriptionId: string | null = null;
  private snapshot = 0;
  private destroyed = false;

  constructor(
    private readonly descriptor: QueryDescriptor<TResult>,
    options?: CreatePulseQueryOptions<TResult>,
  ) {
    this.onStateChange = options?.onStateChange;
    this.pullClient = descriptor.pullClient;
    this.queryKey = `${descriptor.queryName}:${JSON.stringify(descriptor.args)}:${Math.random().toString(36).slice(2, 10)}`;
    this.core = new PulseMergeCore({ order: 'asc', limit: null, rangeStart: null, rangeEnd: null });
  }

  getState() {
    return this.state;
  }

  async subscribe() {
    try {
      const body: {
        clientId: string;
        queryName: string;
        args: Record<string, unknown>;
        subscriptionId?: string;
      } = {
        clientId: this.pullClient.clientId,
        queryName: this.descriptor.queryName,
        args: this.descriptor.args,
      };

      if (this.subscriptionId) {
        body.subscriptionId = this.subscriptionId;
      }

      const response = await this.pullClient.fetch(`${this.descriptor.url}/subscribe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        credentials: 'include',
      });

      if (!response.ok) {
        throw new Error(`Subscribe failed: ${response.status}`);
      }

      const result = deserializeResponse<ProtocolSubscribeResponse<TResult>>(await response.text());
      if (this.destroyed) {
        return;
      }

      this.core.order = result.order;
      this.core.limit = result.limit;
      this.core.rebuildFromRows(result.rows);
      this.subscriptionId = result.subscriptionId;
      this.registerWithPullClient();
      this.core.rangeStart = isPkComparable(result.rangeStart) ? result.rangeStart : null;
      this.core.rangeEnd = isPkComparable(result.rangeEnd) ? result.rangeEnd : null;
      this.snapshot = result.snapshot;

      this.setState({
        data: this.core.data,
        hasMore: result.hasMore,
        isLoading: false,
        error: null,
      });
    } catch (error) {
      const normalizedError = error instanceof Error ? error : new Error(String(error));
      this.setState({
        error: normalizedError,
        isLoading: false,
      });
    }
  }

  async poll() {
    if (!this.subscriptionId) {
      return;
    }

    await this.pullClient.pollNow();
  }

  applyEvents(events: PulseEvent<TResult>[]) {
    const changed = this.core.applyEvents(events);
    if (changed) this.setState({ data: this.core.data });
  }

  async loadMore() {
    if (this.state.isLoadingMore || !this.subscriptionId) {
      return;
    }

    const visiblePks = this.state.data.map((row) => row.$pk).filter(isPkComparable);
    const cursor =
      this.core.order === 'desc'
        ? visiblePks.length > 0
          ? visiblePks.reduce((min, current) => (comparePkValues(current, min) < 0 ? current : min))
          : this.core.rangeStart
        : visiblePks.length > 0
          ? visiblePks.reduce((max, current) => (comparePkValues(current, max) > 0 ? current : max))
          : this.core.rangeEnd;

    if (cursor === null) {
      return;
    }

    this.setState({ isLoadingMore: true });

    try {
      const response = await this.pullClient.fetch(`${this.descriptor.url}/load-more`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          clientId: this.pullClient.clientId,
          subscriptionId: this.subscriptionId,
          cursor,
        }),
        credentials: 'include',
      });

      if (!response.ok) {
        throw new Error(`Load more failed: ${response.status}`);
      }

      const result = deserializeResponse<ProtocolLoadMoreResponse<TResult>>(await response.text());
      if (this.destroyed) {
        return;
      }

      this.core.rangeStart = isPkComparable(result.rangeStart) ? result.rangeStart : null;
      this.core.rangeEnd = isPkComparable(result.rangeEnd) ? result.rangeEnd : null;
      this.setState({ hasMore: result.hasMore });

      const appended = this.core.appendRows(result.rows ?? []);
      if (appended) {
        this.setState({ data: this.core.data });
      }
    } catch (error) {
      const normalizedError = error instanceof Error ? error : new Error(String(error));
      this.setState({ error: normalizedError });
    } finally {
      this.setState({ isLoadingMore: false });
    }
  }

  reset() {
    this.unregisterFromPullClient();
    this.subscriptionId = null;
    this.core.clear();
    this.snapshot = 0;
    this.setState({
      data: [],
      isLoading: true,
      isLoadingMore: false,
      hasMore: false,
      error: null,
    });
  }

  destroy() {
    this.unregisterFromPullClient();
    this.notifyUnsubscribe();
    this.destroyed = true;
  }

  private setState(next: Partial<PulseQueryState<TResult>>) {
    if (this.destroyed) {
      return;
    }

    this.state = { ...this.state, ...next };
    this.onStateChange?.(this.state);
  }

  private applyPullResult(result: ProtocolPullResponse<TResult, PulseEvent<TResult>>) {
    if (result.reset === true) {
      this.core.order = result.order;
      this.core.limit = result.limit;
      this.core.rebuildFromRows(result.rows);
      this.snapshot = result.snapshot;
      this.core.rangeStart = isPkComparable(result.rangeStart) ? result.rangeStart : null;
      this.core.rangeEnd = isPkComparable(result.rangeEnd) ? result.rangeEnd : null;
      this.setState({
        data: this.core.data,
        hasMore: result.hasMore,
        isLoading: false,
        error: null,
      });
      return;
    }

    this.snapshot = result.snapshot;
    if (result.events.length > 0) {
      this.applyEvents(result.events);
    }
    this.core.rangeStart = isPkComparable(result.rangeStart) ? result.rangeStart : null;
    this.core.rangeEnd = isPkComparable(result.rangeEnd) ? result.rangeEnd : null;
  }

  private unregisterFromPullClient() {
    this.pullClient.unregister(this.queryKey);
  }

  // Best-effort teardown call: frees the subscription server-side immediately
  // instead of relying solely on the idle sweep. Fire-and-forget — destroy() is
  // synchronous, and a network failure here just means the sweep (or a later reused
  // subscriptionId) cleans it up instead.
  private notifyUnsubscribe() {
    if (!this.subscriptionId) {
      return;
    }

    void this.pullClient
      .fetch(`${this.descriptor.url}/unsubscribe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          clientId: this.pullClient.clientId,
          subscriptionId: this.subscriptionId,
        }),
        credentials: 'include',
      })
      .catch(() => {});
  }

  private registerWithPullClient() {
    this.pullClient.register(this.queryKey, {
      getSubscriptionState: () => {
        if (!this.subscriptionId) {
          return null;
        }

        return {
          subscriptionId: this.subscriptionId,
          snapshot: this.snapshot,
        };
      },
      applyPullResult: (result) => {
        this.applyPullResult(result as ProtocolPullResponse<TResult, PulseEvent<TResult>>);
      },
      onPullError: (error) => {
        this.setState({ error });
      },
    });
  }
}
