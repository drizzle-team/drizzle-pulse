import * as crypto from 'node:crypto';
import { and, eq, getColumns, gt, or, sql } from 'drizzle-orm';
import type { PgTable } from 'drizzle-orm/pg-core';
import { extractRow } from '../shared/event-normalization.js';
import { comparePkValues, isInsertPrepend, isPkComparable } from '../shared/pk-utils.js';
import type {
  LoadMoreRequest,
  LoadMoreResponse,
  PullRequest,
  PullResetResponse,
  PullResponse,
  PullResponseError,
  PullResponseErrorResult,
  SubscribeRequest,
  SubscribeResponse,
  UnsubscribeRequest,
  UnsubscribeResponse,
} from '../shared/protocol-types.js';

import type { PulseAuthContext, RealtimeEvent, ResolvedPulseQuery, WhereClause } from '../types.js';
import { buildWhereClausePredicate } from './drizzle-utils.js';
import type { AnyPulseBuilders, PulseRegistry } from './pulse-registry.js';
import { applyResponsePipeline } from './pulse-registry.js';
import type { PulseSourceDb } from './pulse-sql.js';
import { buildSelectQuery } from './pulse-sql.js';
import { getQueryColumnKey } from './pulse-types.js';
import type { RealtimeService, Subscription, SubscriptionManager } from './realtime-store.js';

export type SubscribeHandlerResponseBody =
  | SubscribeResponse<Record<string, unknown>>
  | { error: string };
export type LoadMoreHandlerResponseBody =
  | LoadMoreResponse<Record<string, unknown>>
  | { error: string };
export type UnsubscribeHandlerResponseBody = UnsubscribeResponse | { error: string };
export type PullHandlerResponseBody =
  | {
      results: Record<
        string,
        PullResponse<Record<string, unknown>, RealtimeEvent> | PullResponseErrorResult
      >;
    }
  | { error: string };

export type RealtimeHandlerResult<TBody> = {
  status: number;
  body: TBody;
};

type NormalizedEvent = {
  snapshot: number;
  pk: unknown;
  op: 'insert' | 'update' | 'delete' | 'snapshot';
  matchesNew: boolean;
  matchesOld: boolean;
  row: Record<string, unknown> | null;
  old_row: Record<string, unknown> | null;
};

export class RealtimeRequestHandler {
  constructor(
    private readonly registry: PulseRegistry<AnyPulseBuilders>,
    private readonly sourceDb: PulseSourceDb,
    private readonly subscriptionManager: SubscriptionManager,
    private readonly getRealtimeService: () => Pick<RealtimeService, 'getDb' | 'getLatestSnapshot'>,
    private readonly getEventsTable: (queryName: string) => PgTable,
  ) {}

  async subscribe(
    request: SubscribeRequest,
    auth: PulseAuthContext,
  ): Promise<RealtimeHandlerResult<SubscribeHandlerResponseBody>> {
    try {
      const { clientId: rawClientId, queryName, args, subscriptionId } = request;
      const clientId = this.getClientId(rawClientId);

      let resolvedQuery: ResolvedPulseQuery;
      let sourceTable: PgTable;
      try {
        resolvedQuery = this.registry.resolve(queryName, args, auth);
        // resolve() throws on an unknown query, so this is unreachable in practice.
        const table = this.registry.getSourceTable(queryName);
        if (!table) throw new Error(`Unknown query: "${queryName}"`);
        sourceTable = table;
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Validation failed';
        return { status: 400, body: { error: message } };
      }

      // Read the snapshot cursor BEFORE the baseline SELECT (matches the embedded path's
      // startHandshake ordering): a write whose event lands between these two reads must
      // still be covered by the cursor returned here, or it would be lost until an
      // unrelated reset (CR-04). Duplicate replays are already idempotent client-side.
      const snapshot = await this.getRealtimeService().getLatestSnapshot(
        this.getEventsTable(queryName),
      );

      // Fetch limit+1 so hasMore is authoritative (mirrors loadMore): a full page of
      // exactly `limit` rows must not report hasMore unless a further row exists.
      const pageLimit = typeof resolvedQuery.limit === 'number' ? resolvedQuery.limit : undefined;
      const fetchQuery =
        pageLimit !== undefined ? { ...resolvedQuery, limit: pageLimit + 1 } : resolvedQuery;
      const fetchedRows = await buildSelectQuery(this.sourceDb, sourceTable, fetchQuery);
      const hasMore = pageLimit !== undefined ? fetchedRows.length > pageLimit : false;
      const rows =
        pageLimit !== undefined && hasMore ? fetchedRows.slice(0, pageLimit) : fetchedRows;

      // Rows here are SELECT-shaped (keyed by JS property name), not events/WAL-shaped
      // (keyed by SQL name) — index by the PK's JS query key, not pkColumn.name (CR-02).
      const pkQueryKey = this.getPkQueryKey(resolvedQuery);
      const rawRangeStart = rows[0]?.[pkQueryKey] ?? null;
      const rawRangeEnd = rows[rows.length - 1]?.[pkQueryKey] ?? null;
      let rangeStart = isPkComparable(rawRangeStart) ? rawRangeStart : null;
      let rangeEnd = isPkComparable(rawRangeEnd) ? rawRangeEnd : null;
      if (resolvedQuery.order === 'desc') {
        [rangeStart, rangeEnd] = [rangeEnd, rangeStart];
      }

      let subscription =
        subscriptionId && clientId ? this.subscriptionManager.get(clientId, subscriptionId) : null;
      // A client-supplied clientId/subscriptionId pair from a different user must not
      // overwrite an existing subscription's query/auth. Discard the found subscription
      // AND refuse to reuse the disputed subscriptionId for creation — reusing it would
      // still silently overwrite the victim's entry in the store under the same key
      // (SubscriptionManager.create() unconditionally .set()s), so this falls all the
      // way through to the final branch, which mints a genuinely fresh random id (WR-06).
      const ownershipMismatch = subscription !== null && subscription.auth.userId !== auth.userId;
      if (ownershipMismatch) {
        subscription = null;
      }
      if (subscription) {
        this.subscriptionManager.update(clientId, subscription.id, {
          query: resolvedQuery,
          queryName,
          auth,
        });
      } else if (subscriptionId && !ownershipMismatch) {
        subscription = this.subscriptionManager.create(
          clientId,
          queryName,
          resolvedQuery,
          auth,
          subscriptionId,
        );
      } else {
        subscription = this.subscriptionManager.create(clientId, queryName, resolvedQuery, auth);
      }

      this.subscriptionManager.update(clientId, subscription.id, {
        rangeStart,
        rangeEnd,
        hasMore,
      });
      const pipelinedRows = await applyResponsePipeline(rows, resolvedQuery);

      const response: SubscribeResponse<Record<string, unknown>> = {
        clientId,
        subscriptionId: subscription.id,
        rows: pipelinedRows,
        rangeStart,
        rangeEnd,
        snapshot,
        order: resolvedQuery.order,
        limit: resolvedQuery.limit,
        hasMore,
      };

      return { status: 200, body: response };
    } catch {
      return { status: 500, body: { error: 'server_error' } };
    }
  }

  async loadMore(
    request: LoadMoreRequest,
    auth: PulseAuthContext,
  ): Promise<RealtimeHandlerResult<LoadMoreHandlerResponseBody>> {
    try {
      const { clientId, subscriptionId, cursor } = request;
      if (!clientId) return { status: 400, body: { error: 'missing_client_id' } };
      if (!subscriptionId) return { status: 400, body: { error: 'missing_subscription_id' } };

      const subscription = this.subscriptionManager.get(clientId, subscriptionId);
      if (!subscription) return { status: 404, body: { error: 'subscription_not_found' } };
      if (subscription.auth.userId !== auth.userId) {
        return { status: 404, body: { error: 'subscription_not_found' } };
      }
      if (cursor === undefined || cursor === null) {
        return { status: 400, body: { error: 'missing_cursor' } };
      }
      if (!isPkComparable(cursor)) {
        return { status: 400, body: { error: 'invalid_cursor' } };
      }

      // WhereClause keys are matched against query.columns (JS property names), not SQL
      // names — use the PK's JS query key here, not pkColumn.name (CR-02).
      const cursorCondition: WhereClause = {
        [this.getPkQueryKey(subscription.query)]:
          subscription.query.order === 'asc' ? { gt: cursor } : { lt: cursor },
      };
      const cursorWhere: WhereClause = subscription.query.where
        ? { AND: [subscription.query.where, cursorCondition] }
        : cursorCondition;
      const fetchQuery: ResolvedPulseQuery = {
        ...subscription.query,
        where: cursorWhere,
        allowedColumnNames: this.getInternalAllowedColumnNames(subscription.query),
        limit:
          typeof subscription.query.limit === 'number'
            ? subscription.query.limit + 1
            : subscription.query.limit,
      };

      const sourceTable = subscription.query.table;
      const rows = await buildSelectQuery(this.sourceDb, sourceTable, fetchQuery);
      const pageLimit = subscription.query.limit ?? undefined;
      const hasMore = pageLimit !== undefined ? rows.length > pageLimit : false;
      const fetchedRows = pageLimit !== undefined && hasMore ? rows.slice(0, pageLimit) : rows;

      // fetchedRows are SELECT-shaped (JS property keys) — index by the PK's JS query key,
      // not pkColumn.name (CR-02).
      const pkRowKey = this.getPkQueryKey(subscription.query);
      const ids = fetchedRows
        .map((row) => row[pkRowKey])
        .filter((pk) => pk !== undefined)
        .filter(isPkComparable);
      let newRangeStart: unknown | null = null;
      let newRangeEnd: unknown | null = null;
      for (const id of ids) {
        if (newRangeStart === null || comparePkValues(id, newRangeStart) < 0) newRangeStart = id;
        if (newRangeEnd === null || comparePkValues(id, newRangeEnd) > 0) newRangeEnd = id;
      }

      const updatedRangeStart =
        newRangeStart !== null
          ? subscription.rangeStart !== null
            ? comparePkValues(newRangeStart, subscription.rangeStart) < 0
              ? newRangeStart
              : subscription.rangeStart
            : newRangeStart
          : subscription.rangeStart;
      const updatedRangeEnd =
        newRangeEnd !== null
          ? subscription.rangeEnd !== null
            ? comparePkValues(newRangeEnd, subscription.rangeEnd) > 0
              ? newRangeEnd
              : subscription.rangeEnd
            : newRangeEnd
          : subscription.rangeEnd;

      this.subscriptionManager.update(clientId, subscriptionId, {
        rangeStart: updatedRangeStart,
        rangeEnd: updatedRangeEnd,
        hasMore,
      });

      const pipelinedRows = await applyResponsePipeline(fetchedRows, subscription.query);
      const response: LoadMoreResponse<Record<string, unknown>> = {
        rows: pipelinedRows,
        rangeStart: updatedRangeStart,
        rangeEnd: updatedRangeEnd,
        hasMore,
      };
      return { status: 200, body: response };
    } catch {
      return { status: 500, body: { error: 'server_error' } };
    }
  }

  // Explicit teardown counterpart to subscribe() (WR-07): without this, the only way a
  // subscription is ever removed from the store is the idle sweep in pull(), so a client
  // that unmounts cleanly should proactively free its slot rather than waiting it out.
  async unsubscribe(
    request: UnsubscribeRequest,
    auth: PulseAuthContext,
  ): Promise<RealtimeHandlerResult<UnsubscribeHandlerResponseBody>> {
    try {
      const { clientId, subscriptionId } = request;
      if (!clientId) return { status: 400, body: { error: 'missing_client_id' } };
      if (!subscriptionId) return { status: 400, body: { error: 'missing_subscription_id' } };

      const subscription = this.subscriptionManager.get(clientId, subscriptionId);
      if (!subscription || subscription.auth.userId !== auth.userId) {
        // Same trust boundary as loadMore/pull: don't reveal whether a subscription
        // exists under a different owner, and don't let a caller delete it either.
        return { status: 404, body: { error: 'subscription_not_found' } };
      }

      this.subscriptionManager.delete(clientId, subscriptionId);
      return { status: 200, body: { ok: true } };
    } catch {
      return { status: 500, body: { error: 'server_error' } };
    }
  }

  async pull(
    request: PullRequest,
    auth: PulseAuthContext,
  ): Promise<RealtimeHandlerResult<PullHandlerResponseBody>> {
    try {
      const { clientId, subscriptions } = request;
      if (!clientId) {
        return { status: 400, body: { error: 'missing_client_id' } };
      }
      if (!Array.isArray(subscriptions)) {
        return { status: 400, body: { error: 'missing_subscriptions' } };
      }

      const client = this.subscriptionManager.getClient(clientId);
      if (!client) {
        return { status: 200, body: { results: {} } };
      }

      const realtimeService = this.getRealtimeService();
      const results: Record<
        string,
        PullResponse<Record<string, unknown>, RealtimeEvent> | PullResponseErrorResult
      > = {};

      for (const subscriptionRequest of subscriptions) {
        const { subscriptionId } = subscriptionRequest;
        if (!subscriptionId) {
          continue;
        }

        const subscription = this.subscriptionManager.get(clientId, subscriptionId);
        if (!subscription || subscription.auth.userId !== auth.userId) {
          results[subscriptionId] = {
            error: 'subscription_not_found',
            reset: true,
            reason: 'subscription_not_found',
          };
          continue;
        }

        // A subscription only stays alive as long as its client keeps pulling — this is
        // the sole activity signal the idle sweep (SubscriptionManager.sweepIdle) uses
        // to evict abandoned subscriptions (WR-07).
        this.subscriptionManager.touch(clientId, subscriptionId);

        const sinceSnapshot =
          typeof subscriptionRequest.snapshot === 'number' ? subscriptionRequest.snapshot : 0;
        const incremental = await this.buildIncrementalResponse(
          subscription,
          sinceSnapshot,
          realtimeService,
        );
        if ('error' in incremental) {
          results[subscriptionId] = { error: incremental.error };
          continue;
        }
        if ('reset' in incremental && incremental.reset === true) {
          const latestSnapshot = await realtimeService.getLatestSnapshot(
            this.getEventsTable(subscription.queryName),
          );
          const rerun = await this.buildResetResponse(subscription, latestSnapshot);
          if ('error' in rerun) {
            results[subscriptionId] = { error: 'reset_failed' };
            continue;
          }

          this.subscriptionManager.update(clientId, subscriptionId, {
            rangeStart: rerun.rangeStart,
            rangeEnd: rerun.rangeEnd,
            hasMore: rerun.hasMore,
          });
          results[subscriptionId] = rerun;
          continue;
        }

        this.subscriptionManager.update(clientId, subscriptionId, {
          rangeStart: incremental.rangeStart,
          rangeEnd: incremental.rangeEnd,
        });
        results[subscriptionId] = incremental;
      }

      return { status: 200, body: { results } };
    } catch (error) {
      console.error('Error handling pull request:', error);
      return { status: 500, body: { error: 'server_error' } };
    }
  }

  private getClientId(rawClientId: string | undefined): string {
    return rawClientId && rawClientId.length > 0 ? rawClientId : crypto.randomUUID();
  }

  // Rows/WhereClauses built against SELECT-shaped data (query.columns) are keyed by the
  // PK's JS property name, not its SQL name — the two diverge whenever a table declares
  // e.g. `orderId: serial('order_id')` (CR-02). Falls back to pkColumn.name only for the
  // (unreachable in practice) case where the PK isn't present in query.columns at all.
  private getPkQueryKey(query: ResolvedPulseQuery): string {
    return getQueryColumnKey(query.columns, query.pkColumn) ?? query.pkColumn.name;
  }

  private getInternalAllowedColumnNames(query: ResolvedPulseQuery): ReadonlySet<string> {
    const pkQueryKey = getQueryColumnKey(query.columns, query.pkColumn);
    return pkQueryKey
      ? new Set([...query.allowedColumnNames, pkQueryKey])
      : query.allowedColumnNames;
  }

  private normalizeEvent(
    rawEvent: Record<string, unknown> & {
      $snapshot?: unknown;
      $op?: unknown;
      $matches_new?: unknown;
      $matches_old?: unknown;
    },
    query: ResolvedPulseQuery,
  ): NormalizedEvent {
    return {
      snapshot: Number(rawEvent.$snapshot ?? 0),
      pk: rawEvent[query.pkColumn.name],
      op: String(rawEvent.$op) as NormalizedEvent['op'],
      matchesNew: Boolean(rawEvent.$matches_new),
      matchesOld: Boolean(rawEvent.$matches_old),
      row: extractRow(rawEvent, query.columns),
      old_row: extractRow(rawEvent, query.columns, '$old_'),
    };
  }

  private async applyPipelineToRow(
    query: ResolvedPulseQuery,
    row: Record<string, unknown> | null,
  ): Promise<Record<string, unknown> | null> {
    if (!row) {
      return null;
    }
    const [pipelinedRow] = await applyResponsePipeline([row], query);
    // A transform that filters a row out yields undefined; propagate that (callers
    // treat a falsy result as "filtered"), never the un-pipelined original row.
    return pipelinedRow ?? null;
  }

  private withOpPredicate(
    opColumn: PgTable['_']['columns'][string],
    op: 'insert' | 'update' | 'delete' | 'snapshot',
    predicate?: ReturnType<typeof buildWhereClausePredicate>,
  ) {
    return predicate ? and(eq(opColumn, op), predicate) : eq(opColumn, op);
  }

  private matchesLimitedInsertWindow(subscription: Subscription, pkValue: unknown): boolean {
    if (typeof subscription.query.limit !== 'number') {
      return true;
    }
    return isInsertPrepend(
      subscription.query.order,
      pkValue,
      subscription.rangeStart,
      subscription.rangeEnd,
    );
  }

  private computeUpdatedRange(
    subscription: Subscription,
    events: RealtimeEvent[],
  ): { rangeStart: unknown | null; rangeEnd: unknown | null } {
    let nextRangeStart = subscription.rangeStart;
    let nextRangeEnd = subscription.rangeEnd;
    for (const event of events) {
      if (event.op !== 'insert') {
        continue;
      }
      const pkValue = event.pk;
      if (!isPkComparable(pkValue)) {
        continue;
      }
      if (nextRangeStart === null || comparePkValues(pkValue, nextRangeStart) < 0) {
        nextRangeStart = pkValue;
      }
      if (nextRangeEnd === null || comparePkValues(pkValue, nextRangeEnd) > 0) {
        nextRangeEnd = pkValue;
      }
    }
    return { rangeStart: nextRangeStart, rangeEnd: nextRangeEnd };
  }

  private buildResetWhereClause(subscription: Subscription): WhereClause | null {
    // WhereClause keys are matched against query.columns (JS property names) — use the
    // PK's JS query key here, not pkColumn.name (CR-02).
    const pkColumnName = this.getPkQueryKey(subscription.query);
    if (subscription.query.order === 'desc') {
      if (!isPkComparable(subscription.rangeStart)) {
        return subscription.query.where;
      }
      const pkWhere: WhereClause = { [pkColumnName]: { gte: subscription.rangeStart } };
      return subscription.query.where ? { AND: [subscription.query.where, pkWhere] } : pkWhere;
    }
    if (!isPkComparable(subscription.rangeEnd)) {
      return subscription.query.where;
    }
    const pkWhere: WhereClause = { [pkColumnName]: { lte: subscription.rangeEnd } };
    return subscription.query.where ? { AND: [subscription.query.where, pkWhere] } : pkWhere;
  }

  private async buildResetResponse(
    subscription: Subscription,
    latestSnapshot: number,
  ): Promise<PullResetResponse<Record<string, unknown>, RealtimeEvent> | PullResponseError> {
    const sourceTable = this.registry.getSourceTable(subscription.queryName);
    if (!sourceTable) {
      return { error: 'source_table_not_found' };
    }
    const rerunQuery: ResolvedPulseQuery = {
      ...subscription.query,
      where: this.buildResetWhereClause(subscription),
      allowedColumnNames: this.getInternalAllowedColumnNames(subscription.query),
      limit: null,
    };
    const rows = await buildSelectQuery(this.sourceDb, sourceTable, rerunQuery);
    const pipelinedRows = await applyResponsePipeline(rows, subscription.query);
    const rerunPks = pipelinedRows
      .map((row: Record<string, unknown> & { $pk?: unknown }) => row.$pk)
      .filter(isPkComparable);
    let rangeStart: unknown | null = null;
    let rangeEnd: unknown | null = null;
    for (const pk of rerunPks) {
      if (rangeStart === null || comparePkValues(pk, rangeStart) < 0) {
        rangeStart = pk;
      }
      if (rangeEnd === null || comparePkValues(pk, rangeEnd) > 0) {
        rangeEnd = pk;
      }
    }
    return {
      events: [],
      rows: pipelinedRows,
      rangeStart,
      rangeEnd,
      snapshot: latestSnapshot,
      reset: true,
      reason: 'snapshot',
      order: subscription.query.order,
      limit: subscription.query.limit,
      hasMore: subscription.hasMore,
    };
  }

  private async buildIncrementalResponse(
    subscription: Subscription,
    sinceSnapshot: number,
    realtimeService: Pick<RealtimeService, 'getDb' | 'getLatestSnapshot'>,
  ): Promise<
    | PullResponse<Record<string, unknown>, RealtimeEvent>
    | { reset: true; reason: string }
    | PullResponseError
  > {
    const latestSnapshot = await realtimeService.getLatestSnapshot(
      this.getEventsTable(subscription.queryName),
    );
    if (sinceSnapshot >= latestSnapshot) {
      return {
        events: [],
        rangeStart: subscription.rangeStart,
        rangeEnd: subscription.rangeEnd,
        snapshot: sinceSnapshot,
      };
    }

    const eventsDb = realtimeService.getDb();
    // Total for registered queries — the runtime resolves an events table for every
    // source table at construction, so this throws rather than returning null for a
    // genuinely unknown query (subscription lookup upstream already guards that case).
    const eventsTable = this.getEventsTable(subscription.queryName);
    const eventTableColumns = getColumns(eventsTable);
    const eventTableColumnsBySqlName = Object.fromEntries(
      Object.values(eventTableColumns).map((column) => [column.name, column]),
    );
    const eventColumns = Object.fromEntries(
      Object.entries(subscription.query.columns).map(([queryKey, sourceColumn]) => {
        const eventColumn = eventTableColumnsBySqlName[sourceColumn.name];
        if (!eventColumn) {
          throw new Error(`Missing events column "${sourceColumn.name}" for query`);
        }
        return [queryKey, eventColumn] as const;
      }),
    );
    const oldEventColumns = Object.fromEntries(
      Object.entries(subscription.query.columns).map(([queryKey, sourceColumn]) => {
        const oldColumnName = `$old_${sourceColumn.name}`;
        const oldEventColumn = eventTableColumnsBySqlName[oldColumnName];
        if (!oldEventColumn) {
          throw new Error(`Missing events old column "${oldColumnName}" for query`);
        }
        return [queryKey, oldEventColumn] as const;
      }),
    );
    const currentPredicate = buildWhereClausePredicate(
      subscription.query.where,
      eventColumns,
      subscription.query.allowedColumnNames,
    );
    const oldPredicate = buildWhereClausePredicate(
      subscription.query.where,
      oldEventColumns,
      subscription.query.allowedColumnNames,
    );

    const systemColumns = eventTableColumns as typeof eventTableColumns & {
      $snapshot?: PgTable['_']['columns'][string];
      $op?: PgTable['_']['columns'][string];
    };
    const snapshotColumn = systemColumns.$snapshot;
    const opColumn = systemColumns.$op;
    if (!snapshotColumn || !opColumn) {
      return { error: 'events_table_not_found' };
    }
    const updatePredicate =
      currentPredicate && oldPredicate
        ? or(currentPredicate, oldPredicate)
        : (currentPredicate ?? oldPredicate);
    const rawEvents = await eventsDb
      .select({
        // Key the projection by SQL column name (not events-table JS property name)
        // so the raw record matches extractRow's contract: rawEvent[$old_ + sourceColumn.name].
        // resolveEventsTable() already keys both new-value and $old_ columns by SQL name
        // (driver_id / $old_driver_id), so eventTableColumnsBySqlName is a defensive
        // no-op re-keying today — but callers must not rely on that and should always
        // re-key through it rather than assuming getColumns()'s own JS keys.
        ...eventTableColumnsBySqlName,
        $matches_new: currentPredicate ? sql<boolean>`(${currentPredicate})` : sql<boolean>`true`,
        $matches_old: oldPredicate ? sql<boolean>`(${oldPredicate})` : sql<boolean>`true`,
      })
      .from(eventsTable)
      .where(
        and(
          gt(snapshotColumn, sinceSnapshot),
          or(
            this.withOpPredicate(opColumn, 'insert', currentPredicate),
            this.withOpPredicate(opColumn, 'update', updatePredicate),
            this.withOpPredicate(opColumn, 'delete', oldPredicate),
            this.withOpPredicate(opColumn, 'snapshot'),
          ),
        ),
      )
      .orderBy(snapshotColumn);
    const normalizedEvents = rawEvents.map((rawEvent) =>
      this.normalizeEvent(rawEvent, subscription.query),
    );
    if (normalizedEvents.some((event: NormalizedEvent) => event.op === 'snapshot')) {
      return { reset: true, reason: 'snapshot' };
    }

    const events: RealtimeEvent[] = [];
    let nextSnapshot = sinceSnapshot;
    for (const event of normalizedEvents) {
      nextSnapshot = Math.max(nextSnapshot, event.snapshot);
      if (!isPkComparable(event.pk)) {
        continue;
      }
      const row = event.row;
      const oldRow = event.old_row;
      if (event.op === 'insert') {
        if (!row || !this.matchesLimitedInsertWindow(subscription, event.pk)) {
          continue;
        }
        const pipelinedRow = await this.applyPipelineToRow(subscription.query, row);
        if (!pipelinedRow) {
          continue;
        }
        events.push({ op: 'insert', row: pipelinedRow, pk: event.pk });
        continue;
      }
      if (event.op === 'update') {
        if (!row && !oldRow) {
          continue;
        }
        const matchesNew = event.matchesNew && !!row;
        const matchesOld = event.matchesOld && !!oldRow;
        if (!matchesNew && !matchesOld) {
          continue;
        }
        const pipelinedRow = await this.applyPipelineToRow(subscription.query, row);
        const pipelinedOldRow = await this.applyPipelineToRow(subscription.query, oldRow);
        if ((matchesNew && !pipelinedRow) || (matchesOld && !pipelinedOldRow)) {
          continue;
        }
        events.push({
          op: 'update',
          row: pipelinedRow ?? row ?? {},
          old_row: pipelinedOldRow ?? oldRow ?? {},
          pk: event.pk,
          matchesNew,
          matchesOld,
        });
        continue;
      }
      if (event.op === 'delete' && oldRow) {
        if (!event.matchesOld) {
          continue;
        }
        const pipelinedOldRow = await this.applyPipelineToRow(subscription.query, oldRow);
        if (!pipelinedOldRow) {
          continue;
        }
        events.push({
          op: 'delete',
          old_row: pipelinedOldRow,
          pk: event.pk,
          matchesOld: event.matchesOld,
        });
      }
    }

    const nextRange = this.computeUpdatedRange(subscription, events);
    return {
      events,
      rangeStart: nextRange.rangeStart,
      rangeEnd: nextRange.rangeEnd,
      snapshot: nextSnapshot,
    };
  }
}
