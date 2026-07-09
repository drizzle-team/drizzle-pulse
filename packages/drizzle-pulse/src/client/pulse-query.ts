import { comparePkValues, isPkComparable } from '../shared/pk-utils.js';
import type {
  LoadMoreResponse as ProtocolLoadMoreResponse,
  PullResponse as ProtocolPullResponse,
  PullResponseErrorResult,
  SubscribeResponse as ProtocolSubscribeResponse,
  PullSubscriptionRequest,
} from '../shared/protocol-types.js';
import type { PulseEvent } from '../shared/pulse-events.js';
import { PulseMergeCore } from '../shared/pulse-merge-core.js';
import type { QueryDescriptor } from '../types.js';
import type { PullClient } from './create-client.js';
import type { PulseQueryTransport } from './transport.js';

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
  // Fired after a pull applies events (with the response events) and after a reset (with []).
  // The embedded facade drives its onChange fan-out from this; the HTTP client ignores it.
  onEvents?: (
    events: readonly PulseEvent<TResult>[],
    state: readonly TResult[],
    snapshotToken: string,
  ) => void;
};

export class PulseQuery<TResult extends Record<string, unknown> & { $pk: unknown }> {
  private readonly onStateChange?: (state: PulseQueryState<TResult>) => void;
  private readonly onEvents?: CreatePulseQueryOptions<TResult>['onEvents'];
  private readonly transport: PulseQueryTransport;
  // Present only on the HTTP path; when absent, poll() drives a single-query direct pull.
  private readonly pullClient?: PullClient;
  private readonly queryKey: string;

  private state: PulseQueryState<TResult> = {
    data: [],
    isLoading: true,
    isLoadingMore: false,
    hasMore: false,
    error: null,
  };

  private readonly core: PulseMergeCore<TResult>;
  // Cursor token from the server; also the "subscribed" signal — empty until subscribe()
  // resolves, reset to empty on reset(). Echoed back verbatim on each pull.
  private snapshot = '';
  private destroyed = false;

  // Direct-poll coalescing: a nudge arriving while a pull is in flight sets `pollAgain` so
  // exactly one follow-up pull runs — no WAL event is dropped between polls.
  private directPollInFlight: Promise<void> | null = null;
  private directPollAgain = false;

  constructor(
    private readonly descriptor: QueryDescriptor<TResult>,
    options?: CreatePulseQueryOptions<TResult>,
  ) {
    this.onStateChange = options?.onStateChange;
    this.onEvents = options?.onEvents;
    this.transport = descriptor.transport;
    this.pullClient = descriptor.pullClient;
    this.queryKey = `${descriptor.queryName}:${JSON.stringify(descriptor.args)}:${Math.random().toString(36).slice(2, 10)}`;
    this.core = new PulseMergeCore({ order: 'asc', limit: null, rangeStart: null, rangeEnd: null });
  }

  getState() {
    return this.state;
  }

  async subscribe() {
    try {
      const result = (await this.transport.subscribe({
        queryName: this.descriptor.queryName,
        args: this.descriptor.args,
      })) as ProtocolSubscribeResponse<TResult>;
      if (this.destroyed) {
        return;
      }

      this.core.order = result.order;
      this.core.limit = result.limit;
      this.core.rebuildFromRows(result.rows);
      this.core.rangeStart = isPkComparable(result.rangeStart) ? result.rangeStart : null;
      this.core.rangeEnd = isPkComparable(result.rangeEnd) ? result.rangeEnd : null;
      this.snapshot = result.snapshot;
      if (this.pullClient) {
        this.registerWithPullClient(this.pullClient);
      }

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
    if (!this.snapshot) {
      return;
    }

    if (this.pullClient) {
      await this.pullClient.pollNow();
      return;
    }

    await this.directPoll();
  }

  applyEvents(events: PulseEvent<TResult>[]) {
    const changed = this.core.applyEvents(events);
    if (changed) {
      this.setState({ data: this.core.data });
      this.onEvents?.(events, this.core.data, this.snapshot);
    }
  }

  async loadMore() {
    if (this.state.isLoadingMore || !this.snapshot) {
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
      const result = (await this.transport.loadMore({
        queryName: this.descriptor.queryName,
        args: this.descriptor.args,
        rangeStart: this.core.rangeStart,
        rangeEnd: this.core.rangeEnd,
        cursor,
      })) as ProtocolLoadMoreResponse<TResult>;
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
    this.core.clear();
    this.snapshot = '';
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
    this.destroyed = true;
  }

  private setState(next: Partial<PulseQueryState<TResult>>) {
    if (this.destroyed) {
      return;
    }

    this.state = { ...this.state, ...next };
    this.onStateChange?.(this.state);
  }

  // Single-query pull for the direct (embedded) path, coalesced so overlapping nudges collapse
  // into one follow-up pull without dropping any.
  private async directPoll(): Promise<void> {
    if (this.directPollInFlight) {
      this.directPollAgain = true;
      return this.directPollInFlight;
    }

    this.directPollInFlight = (async () => {
      try {
        do {
          this.directPollAgain = false;
          const state = this.buildPullEntry();
          if (!state) {
            return;
          }
          const results = await this.transport.pull([{ key: this.queryKey, ...state }]);
          if (this.destroyed) {
            return;
          }
          const result = results[this.queryKey];
          if (result !== undefined) {
            this.applyPullResult(result as ProtocolPullResponse<TResult, PulseEvent<TResult>> | PullResponseErrorResult);
          }
        } while (this.directPollAgain);
      } catch (error) {
        // Surface as query error state (setState no-ops once destroyed), never an unhandled
        // rejection — background nudges fire poll() fire-and-forget.
        this.setState({ error: error instanceof Error ? error : new Error(String(error)) });
      } finally {
        this.directPollInFlight = null;
      }
    })();

    return this.directPollInFlight;
  }

  private applyPullResult(
    result: ProtocolPullResponse<TResult, PulseEvent<TResult>> | PullResponseErrorResult,
  ) {
    // Error results (e.g. query_resolution_failed after an auth revoke) must not touch the
    // cursor — the next nudge/poll retries with the same token, so access restored later
    // resumes delivery instead of freezing the query.
    if ('error' in result) {
      this.setState({ error: new Error(result.error) });
      return;
    }
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
      this.onEvents?.([], this.core.data, this.snapshot);
      return;
    }

    this.snapshot = result.snapshot;
    if (result.events.length > 0) {
      this.applyEvents(result.events);
    }
    this.core.rangeStart = isPkComparable(result.rangeStart) ? result.rangeStart : null;
    this.core.rangeEnd = isPkComparable(result.rangeEnd) ? result.rangeEnd : null;
    if (this.state.error !== null) {
      this.setState({ error: null });
    }
  }

  // The self-describing pull entry (sans `key`): query identity + the client's current window.
  // order/limit are omitted — the server re-derives them from resolve().
  private buildPullEntry(): Omit<PullSubscriptionRequest, 'key'> | null {
    if (!this.snapshot) {
      return null;
    }
    return {
      queryName: this.descriptor.queryName,
      args: this.descriptor.args,
      rangeStart: this.core.rangeStart,
      rangeEnd: this.core.rangeEnd,
      hasMore: this.state.hasMore,
      snapshot: this.snapshot,
    };
  }

  private unregisterFromPullClient() {
    this.pullClient?.unregister(this.queryKey);
  }

  private registerWithPullClient(pullClient: PullClient) {
    pullClient.register(this.queryKey, {
      getSubscriptionState: () => this.buildPullEntry(),
      applyPullResult: (result) => {
        this.applyPullResult(result as ProtocolPullResponse<TResult, PulseEvent<TResult>> | PullResponseErrorResult);
      },
      onPullError: (error) => {
        this.setState({ error });
      },
    });
  }
}
