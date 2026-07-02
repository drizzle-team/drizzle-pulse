export type SubscribeRequest = {
  clientId?: string;
  queryName: string;
  args: unknown;
  subscriptionId?: string;
};

export type SubscribeResponse<T> = {
  clientId: string;
  subscriptionId: string;
  rows: T[];
  rangeStart: unknown | null;
  rangeEnd: unknown | null;
  snapshot: number;
  order: 'asc' | 'desc';
  limit: number | null;
  hasMore: boolean;
};

export type PullSubscriptionRequest = {
  subscriptionId?: string;
  snapshot?: number;
};
export type PullRequest = {
  clientId?: string;
  subscriptions?: PullSubscriptionRequest[];
};

export type LoadMoreRequest = {
  clientId?: string;
  subscriptionId?: string;
  cursor?: unknown;
};

export type LoadMoreResponse<T> = {
  rows: T[];
  rangeStart: unknown | null;
  rangeEnd: unknown | null;
  hasMore: boolean;
};

export type PullIncrementalResponse<TEvent> = {
  reset?: false;
  events: TEvent[];
  rangeStart: unknown | null;
  rangeEnd: unknown | null;
  snapshot: number;
};

export type PullResetResponse<TRow, TEvent> = {
  reset: true;
  reason: string;
  events: TEvent[];
  rows: TRow[];
  rangeStart: unknown | null;
  rangeEnd: unknown | null;
  snapshot: number;
  order: 'asc' | 'desc';
  limit: number | null;
  hasMore: boolean;
};

export type PullResponse<TRow, TEvent> =
  | PullIncrementalResponse<TEvent>
  | PullResetResponse<TRow, TEvent>;

export type PullResponseError = {
  error: string;
};

export type PullResponseErrorResult = PullResponseError & {
  reset?: true;
  reason?: string;
};
