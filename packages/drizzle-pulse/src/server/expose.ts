import { createHash, randomBytes } from 'node:crypto';
import { desc, type EmptyRelations, getColumns, getTableUniqueName, sql } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import { drizzle } from 'drizzle-orm/postgres';
import {
  type Connection,
  lsnFromString,
  type ReplicationConnection,
  type ReplicationEvent,
  replication,
} from 'minipg';
import type { ResolvedPulseQuery } from '../types.js';
import { emitEventsTableDdl } from './events-table-ddl.js';
import { buildEventsTable, DEFAULT_EVENTS_SCHEMA } from './events-table-resolver.js';
import type { AnyPulseBuilders, PulseRegistry } from './pulse-registry.js';
import { buildSelectQuery, type PulseSourceDb } from './pulse-sql.js';
import { PulseStore } from './pulse-store.js';
import { DEFAULT_PULL_EVENT_LIMIT, PulseRequestHandler } from './sdk.js';
import { WalEventEmitter } from './wal-event-emitter.js';
import { createShapeRowNormalizer } from './wal-shape-bridge.js';

type RuntimeLifecycleListener = () => void;
// Reconnect listeners receive the mid-round re-baseline pin (or null under pull:false / no
// recovery) so they can read through it instead of racing the ordinary watermark path.
type ReconnectListener = (pin: BaselinePin | null) => Promise<void> | void;

export type ExposeWalConfig = {
  publicationName?: string;
  slotName?: string;
};

// Ordered so each log method gates on `this.logLevel >= LogLevel.X`. Debug adds per-WAL-event
// traces; Info (default) adds listener-lifecycle messages; Error keeps only failures.
export enum LogLevel {
  Silent = 0,
  Error = 1,
  Info = 2,
  Debug = 3,
}

export type ExposeConfig = {
  databaseUrl: string;
  /**
   * The app's own drizzle connection; baseline and query reads run on it to keep its session
   * context (RLS, search_path) — except the post-reconnect re-baseline pin, which reads on the
   * admin connection (see `readCollectionBaseline`'s pin-path note). Row scoping there relies on
   * the resolve-time auth-scoped WHERE, not on sourceDb-session RLS.
   */
  sourceDb: PulseSourceDb;
  /**
   * HTTP pull protocol. `true` serves it with defaults; an object serves it configured;
   * `false` runs embedded-only — no events tables provisioned or written, `runtime.handlers`
   * unavailable.
   */
  pull:
    | boolean
    | {
        /** Schema for the `__events_*` tables (default {@link DEFAULT_EVENTS_SCHEMA}). */
        eventsSchema?: string;
        /**
         * Max events a single pull may replay before it falls back to a full reset instead of
         * streaming an unbounded batch. Defaults to {@link DEFAULT_PULL_EVENT_LIMIT} (1000).
         */
        eventLimit?: number;
      };
  wal?: ExposeWalConfig;
  logLevel?: LogLevel;
};

type SourceTableMetadata = {
  sourceTable: PgTable;
  pkColumnName: string;
  eventsTable: PgTable;
  normalizeRow: (row: Record<string, unknown>) => Record<string, unknown>;
};

// A decoded WAL row event, buffered between a transaction's `begin` and `commit` so the whole
// transaction persists atomically and acks together. `row`/`oldRow` are already normalized via
// the shape bridge — for delete, `row` is deliberately `{}` (the tap's dedupe-by-absence
// contract); the persisted events-table row still carries the old row's data (PulseStore's
// buildEventRow), so persistence and the tap emit stay correctly divergent for deletes.
export type PendingWalEvent = {
  eventsTable: PgTable;
  pkColumnName: string;
  pkValue: unknown;
  op: 'insert' | 'update' | 'delete';
  row: Record<string, unknown>;
  oldRow: Record<string, unknown> | null;
  tableQualifiedName: string;
};

// A TOAST fill-by-pk that returned zero rows — recorded transiently in decodeInto, resolved at
// commit time in stream() against a trailing delete on the same pk (Pattern 3).
type FillMiss = { pkValue: unknown; tableQualifiedName: string };

// D-04: no reconnect tuning knobs — fixed internal defaults, same values as the prior
// config-driven defaults.
const RECONNECT_MAX_RETRIES = 10;
const RECONNECT_BASE_DELAY_MS = 1000;
const RECONNECT_MAX_DELAY_MS = 30000;

const DEFAULT_PUBLICATION_NAME = 'drizzle_pulse';
const DEFAULT_SLOT_NAME = 'drizzle_pulse';

// D-04: no knobs — fixed window a live collection's reconnect-debounce round comfortably fits
// inside; a collection materialized after it just takes the Phase 18 watermark handshake.
const REBASELINE_PIN_WINDOW_MS = 5000;

// One connection lifecycle attempt: aborting closes the socket (the only way to stop a parked
// minipg next() — see supervise()) and wakes an in-progress backoff sleep. `attempts` is the
// terminal-path test seam replacing the old reconnectAttempts field.
type Run = { abort: AbortController; attempts: number };

// The snapshot-anchored embedded re-baseline pin: a checked-out admin connection holding the
// recreate's exported snapshot via SET TRANSACTION SNAPSHOT, consumed by readCollectionBaseline
// until the reconnect round settles or it's forced shut (stop()/the next recoverSlot()).
export type BaselinePin = {
  db: ReturnType<typeof drizzle<EmptyRelations, Connection>>;
  watermark: string;
  round: Promise<unknown>;
  close: () => Promise<void>;
};

// drizzle-orm's postgres executor wraps every driver error in a DrizzleQueryError, which does
// not forward the underlying PgError's `code` — it lives on `.cause` instead. Pg-error-code
// switches (55006/42704 retry logic) must unwrap it or every branch always misses.
function getPgErrorCode(error: unknown): string | undefined {
  const cause = (error as { cause?: unknown } | null | undefined)?.cause;
  return (
    (cause as { code?: string } | null | undefined)?.code ??
    (error as { code?: string } | null | undefined)?.code
  );
}

// T-19-10 (defense-in-depth): the exported snapshot name comes from Postgres itself, but `SET
// TRANSACTION SNAPSHOT` cannot take a bind parameter — validate its charset before it is ever
// interpolated into a raw SQL string.
function assertSnapshotName(name: string): void {
  if (!/^[0-9A-Fa-f-]+$/.test(name)) {
    throw new Error(
      `Refusing to use exported snapshot name "${name}": expected only hex digits and dashes`,
    );
  }
}

// A plain `db.select().from(sourceTable)` maps result rows by JS property key (e.g.
// `driverId`), but PulseStore.insertEventRow/buildEventRow expect SQL-column-name keys — the
// same shape WAL rows already carry. Translate a baseline SELECT's row before handing it to
// createBaselineSnapshot.
function toSqlKeyedRow(
  sourceTable: PgTable,
  row: Record<string, unknown>,
): Record<string, unknown> {
  const columns = getColumns(sourceTable);
  const out: Record<string, unknown> = {};
  for (const [jsKey, value] of Object.entries(row)) {
    const column = columns[jsKey as keyof typeof columns];
    out[column ? column.name : jsKey] = value;
  }
  return out;
}

// A resolve() with idempotent settling — supervise() resolves `first` from several exits
// (connected, retry scheduled, gave up) and only the first must count.
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolveFn!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolveFn = res;
  });
  let settled = false;
  return {
    promise,
    resolve: (value: T) => {
      if (settled) return;
      settled = true;
      resolveFn(value);
    },
  };
}

// minipg's `release()` is already idempotent; `once` makes the ROLLBACK-then-release around it
// idempotent too, so releasePin()/the backstop timer/a forced stop() can never double-run it.
function once<T>(fn: () => Promise<T>): () => Promise<T> {
  let result: Promise<T> | undefined;
  return () => {
    result ??= fn();
    return result;
  };
}

// stop()-during-backoff must be the abort waking this sleep, never a timer-vs-flag race.
function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function backoffDelay(attempts: number): number {
  const exponential = RECONNECT_BASE_DELAY_MS * 2 ** attempts;
  const jitter = Math.random() * 1000;
  return Math.min(exponential + jitter, RECONNECT_MAX_DELAY_MS);
}

export class PulseRuntime<TQueries extends AnyPulseBuilders> {
  private readonly sourceTableMetadata: Map<string, SourceTableMetadata>;
  private readonly requestHandler: PulseRequestHandler;
  private readonly logLevel: LogLevel;
  readonly publicationName: string;
  readonly slotName: string;
  private readonly eventsSchema: string;
  // `pull: false` runs embedded-only: no events tables, no persistence, handlers unavailable.
  private readonly pullEnabled: boolean;
  // Populated by reconcile(): events-table name -> current epoch (uuid, rotated on every DDL
  // recreate). Handlers read it via getEpochForQuery to mint/validate cursor tokens.
  private eventsEpochs = new Map<string, string>();

  private store: PulseStore | null = null;
  private run: Run | null = null;
  private pin: BaselinePin | null = null;
  // In-memory mirror of the durable pulse_stream watermark — dedupes at-least-once replay after
  // a reconnect without a store round trip on every commit.
  private lastPersistedCommitLsn: string | null = null;
  readonly walEventEmitter = new WalEventEmitter();
  private readonly reconnectListeners = new Set<ReconnectListener>();
  private readonly stopListeners = new Set<RuntimeLifecycleListener>();
  private readonly terminalErrorListeners = new Set<(error: Error) => void>();

  get isRunning(): boolean {
    return this.run !== null;
  }

  get sourceDb(): PulseSourceDb {
    return this.config.sourceDb;
  }

  // Lifecycle edges the in-process (embedded) client subscribes to. The runtime does
  // not own collections — reconnect fires after the WAL stream re-establishes (live
  // collections must rebaseline to catch up on events missed while disconnected), and
  // stop fires as the runtime tears down (collections must dispose).
  onReconnect(listener: ReconnectListener): () => void {
    this.reconnectListeners.add(listener);
    return () => this.reconnectListeners.delete(listener);
  }

  onStop(listener: RuntimeLifecycleListener): () => void {
    this.stopListeners.add(listener);
    return () => this.stopListeners.delete(listener);
  }

  // Fires once replication gives up permanently (reconnect attempts exhausted) — the runtime
  // then stops, so onStop also fires right after (D-04: terminal error, then teardown).
  onTerminalError(listener: (error: Error) => void): () => void {
    this.terminalErrorListeners.add(listener);
    return () => this.terminalErrorListeners.delete(listener);
  }

  constructor(
    readonly registry: PulseRegistry<TQueries>,
    private readonly config: ExposeConfig,
  ) {
    const wal = this.config.wal ?? {};
    this.publicationName = wal.publicationName ?? DEFAULT_PUBLICATION_NAME;
    this.slotName = wal.slotName ?? DEFAULT_SLOT_NAME;
    this.pullEnabled = this.config.pull !== false;
    this.eventsSchema =
      (typeof this.config.pull === 'object' ? this.config.pull.eventsSchema : undefined) ??
      DEFAULT_EVENTS_SCHEMA;

    this.sourceTableMetadata = new Map();
    // Two different source tables can produce the same events-table name (the `_`→`__`
    // escaping collides for names like `a_`+`b` vs `a`+`_b`), so reject duplicates here where
    // the full set is known.
    const eventsNameOrigin = new Map<string, string>();
    for (const queryName of this.registry.getQueryNames()) {
      const pulseQuery = this.registry.getPulseQuery(queryName);
      const sourceTable = this.registry.getSourceTable(queryName);
      if (
        !pulseQuery ||
        !sourceTable ||
        this.sourceTableMetadata.has(getTableUniqueName(pulseQuery.table))
      ) {
        continue;
      }

      const eventsTable = buildEventsTable(sourceTable, { eventsSchema: this.eventsSchema });

      const eventsName = getTableConfig(eventsTable).name;
      const sourceConfig = getTableConfig(sourceTable);
      const sourceName = `${sourceConfig.schema ?? 'public'}.${sourceConfig.name}`;
      const priorSourceName = eventsNameOrigin.get(eventsName);
      if (priorSourceName && priorSourceName !== sourceName) {
        throw new Error(
          `Source tables ${priorSourceName} and ${sourceName} both derive the same events-table name ${this.eventsSchema}.${eventsName}; rename one to avoid the collision`,
        );
      }
      eventsNameOrigin.set(eventsName, sourceName);

      this.sourceTableMetadata.set(getTableUniqueName(pulseQuery.table), {
        sourceTable,
        pkColumnName: pulseQuery.pkColumn.name,
        eventsTable,
        normalizeRow: createShapeRowNormalizer(sourceTable),
      });
    }

    this.logLevel = this.config.logLevel ?? LogLevel.Info;

    this.requestHandler = new PulseRequestHandler(
      this.registry,
      this.config.sourceDb,
      () => this.getPulseStore(),
      (queryName: string) => this.getEventsTableForQuery(queryName),
      (queryName: string) => this.getEpochForQuery(queryName),
      (typeof this.config.pull === 'object' ? this.config.pull.eventLimit : undefined) ??
        DEFAULT_PULL_EVENT_LIMIT,
      (message: string, ...args: unknown[]) => this.logError(message, ...args),
    );
  }

  get handlers() {
    if (!this.pullEnabled) {
      throw new Error(
        'pull is disabled (pull: false) — HTTP handlers are unavailable on an embedded-only runtime',
      );
    }
    return this.requestHandler;
  }

  // Total for registered queries — every source table gets an events table resolved
  // at construction, so an unknown queryName is the only throwing case.
  private getEventsTableForQuery(queryName: string): PgTable {
    const sourceTable = this.registry.getSourceTable(queryName);
    if (!sourceTable) {
      throw new Error(`Unknown query: "${queryName}"`);
    }

    const metadata = this.sourceTableMetadata.get(getTableUniqueName(sourceTable));
    if (!metadata) {
      throw new Error(`No events table resolved for query "${queryName}"`);
    }

    return metadata.eventsTable;
  }

  /**
   * Current epoch for a query's events table, or `undefined` before {@link start} /
   * {@link provision} has reconciled it. The epoch rotates on every events-table recreate;
   * cursor tokens embed it so a token minted against a since-dropped table is detectable.
   */
  getEpochForQuery(queryName: string): string | undefined {
    const eventsTable = this.getEventsTableForQuery(queryName);
    return this.eventsEpochs.get(getTableConfig(eventsTable).name);
  }

  // Shared per-table seeding step: skip-if-already-seeded is createBaselineSnapshot's own job;
  // this just carries a baseline row (or none, for an empty source) from whichever handle the
  // caller reads on (sourceDb for the resume path, the snapshot-pinned admin tx for a recreate)
  // to the write handle the caller writes on (undefined = the admin db; a tx during rotation).
  private async seedBaseline(
    fetchLatest: () => Promise<Record<string, unknown> | undefined>,
    writeHandle: Parameters<PulseStore['createBaselineSnapshot']>[3] | undefined,
    meta: { sourceTable: PgTable; pkColumnName: string; eventsTable: PgTable },
  ): Promise<void> {
    const baselineRow = await fetchLatest();
    await this.getPulseStore().createBaselineSnapshot(
      meta.eventsTable,
      meta.pkColumnName,
      baselineRow ? toSqlKeyedRow(meta.sourceTable, baselineRow) : null,
      writeHandle,
    );
  }

  async ensureBaselines(): Promise<void> {
    const sourceDb = this.config.sourceDb;

    for (const queryName of this.registry.getQueryNames()) {
      const pulseQuery = this.registry.getPulseQuery(queryName);
      const sourceTable = this.registry.getSourceTable(queryName);
      if (!pulseQuery || !sourceTable) continue;

      await this.seedBaseline(
        async () => {
          const [row] = await sourceDb
            .select()
            .from(sourceTable)
            .orderBy(desc(pulseQuery.pkColumn))
            .limit(1);
          return row;
        },
        undefined,
        {
          sourceTable,
          pkColumnName: pulseQuery.pkColumn.name,
          eventsTable: this.getEventsTableForQuery(queryName),
        },
      );
    }
  }

  /**
   * @internal Consumed by the embedded (tap-direct) client through the runtime value. Reads the
   * watermark BEFORE running the baseline SELECT — a row committed between the SELECT completing
   * and a later watermark read would land in neither the baseline nor the accepted tap stream, so
   * this ordering is load-bearing for the exactly-once handshake, not incidental.
   *
   * Accepted bound: `pg_current_wal_lsn()` returns as soon as a commit's WAL record is written,
   * which can be microseconds before that transaction becomes visible to a new snapshot (the
   * procarray exit happens after the WAL write). If the baseline SELECT's snapshot lands in that
   * window, the row is in neither the baseline (not yet visible) nor the accepted tap stream (its
   * buffered payload's lsn is below this watermark and gets dropped by the drain filter) — it's
   * silently missing until its next change. The window is a handful of microseconds per handshake;
   * closing it fully would require reading the watermark inside the same transaction/snapshot as
   * the baseline SELECT, which is not attempted here.
   *
   * `pin` (passed by the reconnect edge, `client/embedded/index.ts`) reads through the recreate's
   * exported-snapshot connection instead — same visibility class as the WAL stream itself, so the
   * recreate boundary is gapless by construction and closes the residual race above. A collection
   * materialized after the pin's window closes falls through to the ordinary path.
   *
   * Note: the pin's connection is checked out of the admin pool (`databaseUrl`), not the app's
   * `sourceDb` session — so this path does not carry sourceDb's session context (RLS, search_path).
   * The resolve-time auth-scoped WHERE is what enforces row scoping here, not RLS.
   */
  async readCollectionBaseline(
    resolved: ResolvedPulseQuery,
    pin?: BaselinePin | null,
  ): Promise<{ rows: Record<string, unknown>[]; watermark: string }> {
    if (pin) {
      const rows = await buildSelectQuery(pin.db, resolved.table, resolved);
      return { rows, watermark: pin.watermark };
    }

    // 'objects' mode is the one execute() overload whose return type doesn't route through the
    // driver-specific PgQueryResultKind mapping — the only shape that type-checks generically
    // across every driver PulseSourceDb may wrap (pg, postgres.js, minipg, ...).
    const watermarkRows = await this.config.sourceDb.execute<{ lsn: string }>(
      sql`SELECT pg_current_wal_lsn()::text AS lsn`,
      'objects',
    );
    const watermark = watermarkRows[0]?.lsn;
    if (!watermark) {
      throw new Error('pg_current_wal_lsn() returned no watermark row');
    }

    const rows = await buildSelectQuery(this.config.sourceDb, resolved.table, resolved);
    return { rows, watermark };
  }

  async start(): Promise<void> {
    if (this.run) {
      this.logInfo('[WAL Listener] Already running');
      return;
    }

    this.store ??= new PulseStore(this.config.databaseUrl, this.eventsSchema);

    try {
      await this.reconcile();

      const run: Run = { abort: new AbortController(), attempts: 0 };
      this.run = run;
      const first = deferred<void>();
      void this.supervise(run, first);
      await first.promise;
    } catch (error) {
      // Mirrors stop()'s pool teardown so a failed guard doesn't leak connections — callers
      // await start() rejections and then discard the runtime.
      this.run = null;
      const store = this.store;
      this.store = null;
      await store?.end();
      throw error;
    }
  }

  async stop(): Promise<void> {
    for (const listener of [...this.stopListeners]) {
      try {
        listener();
      } catch (err) {
        this.logError('[WAL Listener] onStop listener error:', err);
      }
    }

    const run = this.run;
    this.run = null;
    run?.abort.abort(); // closes the socket via supervise()'s abort hook, wakes a backoff sleep

    await this.releasePin();

    const store = this.store;
    this.store = null;
    await store?.end();

    this.logInfo('[WAL Listener] Stopped');
  }

  /**
   * Reconciles this runtime's events tables and their bookkeeping against the live database
   * without opening a replication stream: runs the same schema path as {@link start} —
   * create/recreate diverged events tables, sweep orphans, rotate epochs — over a short-lived
   * admin connection, then closes it. Call from a deploy/migration step to provision
   * infrastructure ahead of booting the listener. Fail-closed: rejects (rolling back) on any
   * unmet precondition.
   */
  async provision(): Promise<void> {
    this.store ??= new PulseStore(this.config.databaseUrl, this.eventsSchema);
    try {
      await this.reconcile();
    } finally {
      const store = this.store;
      this.store = null;
      await store?.end();
    }
  }

  // Fail-closed boot reconciliation, wrapped in one transaction under a schema-scoped advisory
  // lock. wal_level is the only precondition the runtime can't fix, so it stays an assert;
  // everything else pulse self-provisions: REPLICA IDENTITY FULL on each source, then the
  // publication (create it owning exactly the sources, or — unless it's FOR ALL TABLES — diff
  // its membership, adding registered sources and un-pulsing members that no longer are). Then
  // it brings the events tables and their pulse_meta bookkeeping in line with the sources
  // (create/recreate on DDL-hash divergence, drop orphans), rotating an epoch on every recreate.
  // Any throw rolls the whole transaction back, so a database it can't fully provision is left
  // untouched. Runtime-owned events-table DDL: the app no longer migrates these tables.
  private async reconcile(): Promise<void> {
    const adminDb = this.getPulseStore().getDb();
    const eventsSchema = this.eventsSchema;
    const schema = sql.identifier(eventsSchema);
    const metaTable = sql`${schema}.${sql.identifier('pulse_meta')}`;
    const streamTable = sql`${schema}.${sql.identifier('pulse_stream')}`;

    const epochs = await adminDb.transaction(async (tx) => {
      // Serializes concurrent boots targeting the same events schema (one lock per schema) so
      // two runtimes can't race the same DROP+CREATE. Auto-released at transaction end.
      await tx.execute(
        sql`SELECT pg_advisory_xact_lock(hashtext(${'drizzle_pulse'}), hashtext(${eventsSchema}))`,
      );

      // Postgres identifier quoting for the DDL statements below (sql`` params are values, not
      // identifiers). Matches sql.identifier's behavior; kept as strings so a failure can name
      // the exact statement.
      const quoteIdent = (name: string) => `"${name.replaceAll('"', '""')}"`;

      // Runs a self-provisioning DDL statement, rethrowing on failure with the exact statement
      // and the grant it most likely needs — the error a misconfigured deploy actually hits.
      const execDdl = async (statement: string, missingGrant: string): Promise<void> => {
        try {
          await tx.execute(sql.raw(statement));
        } catch (cause) {
          throw new Error(
            `pulse could not self-provision replication: \`${statement}\` failed — the connection likely lacks ${missingGrant} (${(cause as Error).message})`,
            { cause },
          );
        }
      };

      // wal_level=logical is server-wide (postgresql.conf + restart); the runtime can't fix it,
      // so it stays a fail-closed assert.
      const walLevelResult = await tx.execute<{ wal_level: string }>(
        sql`SELECT current_setting('wal_level') AS wal_level`,
      );
      const walLevel = walLevelResult.rows[0]?.wal_level;
      if (walLevel !== 'logical') {
        throw new Error(
          `wal_level is "${walLevel ?? 'unknown'}", but must be "logical" — set wal_level=logical in postgresql.conf and restart Postgres`,
        );
      }

      const registeredSources = [...this.sourceTableMetadata.values()].map((meta) => {
        const config = getTableConfig(meta.sourceTable);
        const schemaName = config.schema ?? 'public';
        return {
          name: `${schemaName}.${config.name}`,
          quoted: `${quoteIdent(schemaName)}.${quoteIdent(config.name)}`,
          schemaName,
          tableName: config.name,
        };
      });

      // REPLICA IDENTITY FULL on every source BEFORE any publication ADD below, so the first
      // published change already carries complete old-row data. pull:true only — pull:false
      // decodes old-tuple data via oldKind/unchanged instead, so forcing FULL here would just
      // be an unwanted durable mutation (and the ACCESS EXCLUSIVE lock that comes with it).
      if (this.pullEnabled) {
        for (const source of registeredSources) {
          const replicaIdentityResult = await tx.execute<{ relreplident: string }>(
            sql`SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ${source.schemaName} AND c.relname = ${source.tableName}`,
          );
          if (replicaIdentityResult.rows[0]?.relreplident !== 'f') {
            await execDdl(
              `ALTER TABLE ${source.quoted} REPLICA IDENTITY FULL`,
              `ownership of ${source.name}`,
            );
          }
        }
      }

      // Publication: create it owning exactly the sources, or (unless it's FOR ALL TABLES) diff
      // its membership against them.
      const pubIdent = quoteIdent(this.publicationName);
      const publicationResult = await tx.execute<{ puballtables: boolean }>(
        sql`SELECT puballtables FROM pg_publication WHERE pubname = ${this.publicationName}`,
      );
      const publicationRow = publicationResult.rows[0];
      if (!publicationRow) {
        const forTables =
          registeredSources.length > 0
            ? ` FOR TABLE ${registeredSources.map((source) => source.quoted).join(', ')}`
            : '';
        await execDdl(
          `CREATE PUBLICATION ${pubIdent}${forTables} WITH (publish = 'insert, update, delete')`,
          'the database CREATE privilege and ownership of the published tables',
        );
      } else if (!publicationRow.puballtables) {
        const membershipResult = await tx.execute<{ schemaname: string; tablename: string }>(
          sql`SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ${this.publicationName}`,
        );
        const members = membershipResult.rows.map((row) => ({
          name: `${row.schemaname}.${row.tablename}`,
          quoted: `${quoteIdent(row.schemaname)}.${quoteIdent(row.tablename)}`,
        }));
        const memberNames = new Set(members.map((member) => member.name));
        const registeredNames = new Set(registeredSources.map((source) => source.name));

        for (const source of registeredSources) {
          if (!memberNames.has(source.name)) {
            await execDdl(
              `ALTER PUBLICATION ${pubIdent} ADD TABLE ${source.quoted}`,
              `ownership of the publication and of ${source.name}`,
            );
          }
        }

        // Un-pulse members no longer registered: DROP from the publication (mode-independent —
        // membership isn't identity), THEN reset REPLICA IDENTITY, pull:true only (a member row
        // implies the table still exists — pg_publication_tables joins pg_class, so a dropped
        // table has already left membership on its own). Under pull:false the table was never
        // forced to FULL, so there's nothing to reset — and resetting would still take the
        // ACCESS EXCLUSIVE lock this phase removes.
        for (const member of members) {
          if (registeredNames.has(member.name)) continue;
          await execDdl(
            `ALTER PUBLICATION ${pubIdent} DROP TABLE ${member.quoted}`,
            'ownership of the publication',
          );
          if (this.pullEnabled) {
            await execDdl(
              `ALTER TABLE ${member.quoted} REPLICA IDENTITY DEFAULT`,
              `ownership of ${member.name}`,
            );
          }
        }
      }

      // pull:false runs embedded-only: the events schema, pulse_meta/pulse_stream bookkeeping,
      // and events tables themselves are never provisioned or written — only the publication
      // above is needed for the tap (REPLICA IDENTITY stays untouched under pull:false).
      const epochByName = new Map<string, string>();
      if (this.pullEnabled) {
        await tx.execute(sql`CREATE SCHEMA IF NOT EXISTS ${schema}`);
        await tx.execute(
          sql`CREATE TABLE IF NOT EXISTS ${metaTable} (table_name text PRIMARY KEY, ddl_hash text NOT NULL, epoch uuid NOT NULL)`,
        );
        // Durable commit-LSN dedupe watermark (research Open Question 1): survives a full
        // process restart so resuming an intact slot doesn't re-persist minipg's at-least-once
        // replay tail.
        await tx.execute(
          sql`CREATE TABLE IF NOT EXISTS ${streamTable} (slot_name text PRIMARY KEY, last_lsn text NOT NULL)`,
        );

        const metaResult = await tx.execute<{
          table_name: string;
          ddl_hash: string;
          epoch: string;
        }>(sql`SELECT table_name, ddl_hash, epoch FROM ${metaTable}`);
        const metaByName = new Map(metaResult.rows.map((row) => [row.table_name, row] as const));

        const physicalResult = await tx.execute<{ relname: string }>(
          sql`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ${eventsSchema} AND c.relkind IN ('r', 'p')`,
        );
        const physical = new Set(physicalResult.rows.map((row) => row.relname));

        const desired = new Set<string>();

        for (const meta of this.sourceTableMetadata.values()) {
          const eventsName = getTableConfig(meta.eventsTable).name;
          desired.add(eventsName);

          const statements = emitEventsTableDdl(meta.sourceTable, { eventsSchema });
          const ddlHash = createHash('sha256').update(statements.join('\n')).digest('hex');

          const existing = metaByName.get(eventsName);
          if (existing && existing.ddl_hash === ddlHash && physical.has(eventsName)) {
            epochByName.set(eventsName, existing.epoch);
            continue;
          }

          for (const statement of statements) {
            await tx.execute(sql.raw(statement));
          }
          const upsert = await tx.execute<{ epoch: string }>(
            sql`INSERT INTO ${metaTable} (table_name, ddl_hash, epoch) VALUES (${eventsName}, ${ddlHash}, gen_random_uuid()) ON CONFLICT (table_name) DO UPDATE SET ddl_hash = excluded.ddl_hash, epoch = gen_random_uuid() RETURNING epoch`,
          );
          const epoch = upsert.rows[0]?.epoch;
          if (!epoch) {
            throw new Error(`pulse_meta upsert for "${eventsName}" returned no epoch`);
          }
          epochByName.set(eventsName, epoch);
        }

        // Orphans: a meta row with no registered source drops its table + row; a physical table
        // with neither a meta row nor a registered source is left alone (warn only — it may be
        // an unrelated table hand-created in the events schema).
        for (const row of metaByName.values()) {
          if (desired.has(row.table_name)) continue;
          await tx.execute(sql`DROP TABLE IF EXISTS ${schema}.${sql.identifier(row.table_name)}`);
          await tx.execute(sql`DELETE FROM ${metaTable} WHERE table_name = ${row.table_name}`);
        }
        for (const relname of physical) {
          if (
            relname === 'pulse_meta' ||
            relname === 'pulse_stream' ||
            desired.has(relname) ||
            metaByName.has(relname)
          )
            continue;
          this.logWarn(
            `[reconcile] table "${eventsSchema}.${relname}" shares the events schema but has no pulse_meta row; leaving it untouched`,
          );
        }
      }

      return epochByName;
    });

    this.eventsEpochs = epochs;
  }

  // The linear supervisor loop: one connection attempt per iteration, `try/catch/finally` in
  // statement order instead of a callback ring. `signal.aborted` is the single successor to the
  // three superseded-connection guards the old code needed — exactly one connection exists per
  // iteration, so there is nothing left to supersede.
  private async supervise(run: Run, first: { promise: Promise<void>; resolve: () => void }) {
    const { signal } = run.abort;
    // Per-run, not per-instance (documented delta a): a stopped-then-restarted runtime fires no
    // reconnect edge on its first connect.
    let everConnected = false;

    while (!signal.aborted) {
      let rep: ReplicationConnection | undefined;
      // minipg's parked next() only stops when the socket closes — end() is the only thing
      // that wakes a pending pull, so stop() must reach here via the abort signal, never by
      // awaiting supervise() itself.
      const kill = () => rep?.end();
      try {
        rep = await replication(this.config.databaseUrl);
        signal.addEventListener('abort', kill);

        const { slot, from } = this.pullEnabled
          ? await this.resolveSlot(rep) // recovery lives INSIDE the try — never consumes a retry
          : await this.createTempSlot(rep);

        this.logInfo(`[WAL Listener] Subscribing to slot '${slot}'`);
        const iterator = rep.start({
          slot,
          publications: [this.publicationName],
          from,
          statusIntervalMs: 1000,
          idleAck: true,
          messages: false,
        });

        const round = everConnected
          ? Promise.allSettled(
              [...this.reconnectListeners].map(async (listener) => listener(this.pin)),
            )
          : Promise.resolve([]);
        if (this.pin) this.pin.round = round;
        void round.then(() => this.releasePin());

        everConnected = true;
        first.resolve();
        this.logInfo('[WAL Listener] Replication started');

        await this.stream(rep, iterator, run); // returns on clean end too — falls into retry below
      } catch (error) {
        if (signal.aborted) return;
        this.logError('[WAL Listener] Replication error:', error);
      } finally {
        signal.removeEventListener('abort', kill);
        rep?.end();
      }

      if (signal.aborted) return;

      if (run.attempts >= RECONNECT_MAX_RETRIES) {
        first.resolve();
        this.giveUp();
        return;
      }

      run.attempts += 1;
      first.resolve(); // first-connect failure still resolves start() (today's behavior)
      this.logInfo(
        `[WAL Listener] Reconnecting (attempt ${run.attempts}/${RECONNECT_MAX_RETRIES})`,
      );
      await abortableSleep(backoffDelay(run.attempts - 1), signal);
    }
  }

  // The terminal path: reachable only once run.attempts exhausts RECONNECT_MAX_RETRIES.
  private giveUp(): void {
    this.logError('[WAL Listener] Max reconnection attempts reached. Giving up.');
    const terminalError = new Error(
      `WAL replication failed permanently after ${RECONNECT_MAX_RETRIES} reconnect attempts`,
    );
    for (const listener of [...this.terminalErrorListeners]) {
      try {
        listener(terminalError);
      } catch (err) {
        this.logError('[WAL Listener] onTerminalError listener error:', err);
      }
    }
    void this.stop();
  }

  // pull:true only — the resume-vs-recover decision table. The intact-slot resume branch stays
  // structurally dead in production (the persisted watermark is the commit record's own LSN,
  // and ack always advances confirmed_flush past it); it exists for the case Postgres itself
  // guards against below and as documentation of intent. Tests reach it via seeded watermarks.
  private async resolveSlot(rep: ReplicationConnection): Promise<{ slot: string; from?: string }> {
    const adminDb = this.getPulseStore().getDb();
    const result = await adminDb.execute<{
      slot_name: string;
      active: boolean;
      active_pid: number | null;
      wal_status: string | null;
      confirmed_flush_lsn: string | null;
    }>(
      sql`SELECT slot_name, active, active_pid, wal_status, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = ${this.slotName}`,
    );
    const slot = result.rows[0];

    if (!slot || slot.wal_status === 'lost') {
      return { slot: this.slotName, from: await this.recoverSlot(rep) };
    }

    const watermark = await this.getPulseStore().getStreamWatermark(this.slotName);
    // The null guard on confirmed_flush_lsn is load-bearing: lsnFromString throws on null, and
    // null must route to recovery, not a caught exception that would burn a reconnect retry.
    if (
      watermark === null ||
      !slot.confirmed_flush_lsn ||
      lsnFromString(watermark) < lsnFromString(slot.confirmed_flush_lsn)
    ) {
      return { slot: this.slotName, from: await this.recoverSlot(rep) };
    }

    if (slot.active && slot.active_pid) {
      this.logInfo(
        `[WAL Listener] Terminating stale connection on slot '${this.slotName}' (PID ${slot.active_pid})`,
      );
      await this.evictWalsender(this.slotName);
    }

    await this.ensureBaselines();

    this.logInfo(`[WAL Listener] Replication slot '${this.slotName}' ready`);
    this.lastPersistedCommitLsn = watermark;
    return { slot: this.slotName, from: undefined }; // server resumes from confirmed_flush
  }

  // Full recovery machine (pull:true only now): drop any broken persistent slot, recreate with
  // an exported snapshot, rotate-and-seed the events tables from it, and open the embedded
  // re-baseline pin from the SAME snapshot when live collections exist — all before the caller
  // issues rep.start(), because the export dies on this connection's next command.
  private async recoverSlot(rep: ReplicationConnection): Promise<string> {
    await this.releasePin(); // two pins never coexist

    await this.evictWalsender(this.slotName);
    await this.dropSlotWithRetry(this.slotName);

    const { consistentPoint, snapshot } = await rep.createSlot(this.slotName, {
      temporary: false,
      snapshot: 'export',
    });
    if (!snapshot) {
      throw new Error(
        `createSlot('${this.slotName}', { snapshot: 'export' }) returned no exported snapshot name`,
      );
    }

    await this.rotateAndSeedEvents(snapshot, consistentPoint);

    // Live collections only exist mid-run (the embedded factory requires a running runtime, so
    // there are none at boot).
    if (this.reconnectListeners.size > 0) {
      await this.openPin(snapshot, consistentPoint);
    }

    // Operators must see this: a recreate resets events/baselines. Name the slot, never
    // databaseUrl (V7 — info disclosure).
    this.logError(
      `[WAL Listener] Replication slot '${this.slotName}' was missing or invalidated and has been recreated`,
    );

    return consistentPoint;
  }

  // pull:false (D-01): a fresh session-scoped, randomized-suffix temporary slot on every
  // (re)connect, never persisted — a crashed process can never leak a WAL-retaining slot. Never
  // threads the pull:true recovery machine (no continuity to check for a slot that never
  // survives past its own connection).
  private async createTempSlot(
    rep: ReplicationConnection,
  ): Promise<{ slot: string; from: string }> {
    await this.releasePin();

    const slot = `${this.slotName}_${randomBytes(4).toString('hex')}`;
    const { consistentPoint, snapshot } = await rep.createSlot(slot, {
      temporary: true,
      snapshot: 'export',
    });
    if (this.reconnectListeners.size > 0) {
      if (!snapshot) {
        throw new Error(
          `createSlot('${slot}', { snapshot: 'export' }) returned no exported snapshot name`,
        );
      }
      await this.openPin(snapshot, consistentPoint);
    }

    this.logInfo(`[WAL Listener] Created temporary slot '${slot}'`);
    return { slot, from: consistentPoint };
  }

  // Poll-retry a slot drop: the previous owning backend's "active" flag can lag its actual
  // termination by a beat, so a single attempt can spuriously hit 55006 (object_in_use).
  private async dropSlotWithRetry(
    slotName: string,
    opts: { timeoutMs?: number; pollIntervalMs?: number } = {},
  ): Promise<void> {
    const adminDb = this.getPulseStore().getDb();
    const timeoutMs = opts.timeoutMs ?? 3000;
    const pollIntervalMs = opts.pollIntervalMs ?? 100;
    const deadline = Date.now() + timeoutMs;

    for (;;) {
      try {
        await adminDb.execute(sql`SELECT pg_drop_replication_slot(${slotName})`);
        return;
      } catch (error) {
        const code = getPgErrorCode(error);
        if (code === '42704') return; // undefined_object — already gone
        if (code !== '55006' || Date.now() >= deadline) throw error; // object_in_use exhausted, or unexpected
        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
      }
    }
  }

  // Shared zombie-walsender remedy: terminate the backend still attached to the slot, then poll
  // pg_replication_slots until it reports inactive — replaces a fixed sleep with the same
  // poll-retry shape dropSlotWithRetry uses above. Degrades to today's proceed-regardless
  // behavior if the deadline passes with the walsender still marked active.
  private async evictWalsender(
    slotName: string,
    opts: { timeoutMs?: number; pollIntervalMs?: number } = {},
  ): Promise<void> {
    const adminDb = this.getPulseStore().getDb();
    const timeoutMs = opts.timeoutMs ?? 3000;
    const pollIntervalMs = opts.pollIntervalMs ?? 100;
    const deadline = Date.now() + timeoutMs;

    const existing = await adminDb.execute<{ active_pid: number | null }>(
      sql`SELECT active_pid FROM pg_replication_slots WHERE slot_name = ${slotName}`,
    );
    const activePid = existing.rows[0]?.active_pid;
    if (!activePid) return;

    await adminDb.execute(sql`SELECT pg_terminate_backend(${activePid})`);

    for (;;) {
      const poll = await adminDb.execute<{ active: boolean | null }>(
        sql`SELECT active FROM pg_replication_slots WHERE slot_name = ${slotName}`,
      );
      if (!poll.rows[0]?.active) return;
      if (Date.now() >= deadline) return;
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }
  }

  // D-03: one pinned repeatable-read admin transaction rotates every registered events table's
  // epoch, truncates it, and seeds it from the exported snapshot — the sole write path outside
  // reconcile()/the WAL loop; sdk.ts pull handlers stay strictly read-only (D-03 invariant).
  private async rotateAndSeedEvents(snapshotName: string, consistentPoint: string): Promise<void> {
    assertSnapshotName(snapshotName);
    const adminDb = this.getPulseStore().getDb();
    const metaTable = sql`${sql.identifier(this.eventsSchema)}.${sql.identifier('pulse_meta')}`;
    const streamTable = sql`${sql.identifier(this.eventsSchema)}.${sql.identifier('pulse_stream')}`;

    const epochByName = await adminDb.transaction(
      async (tx) => {
        await tx.execute(sql.raw(`SET TRANSACTION SNAPSHOT '${snapshotName}'`));

        const epochs = new Map<string, string>();
        for (const queryName of this.registry.getQueryNames()) {
          const pulseQuery = this.registry.getPulseQuery(queryName);
          const sourceTable = this.registry.getSourceTable(queryName);
          if (!pulseQuery || !sourceTable) continue;

          const eventsTable = this.getEventsTableForQuery(queryName);
          const eventsTableConfig = getTableConfig(eventsTable);
          if (epochs.has(eventsTableConfig.name)) continue; // two queries, one source table

          const eventsIdentifier = sql`${sql.identifier(eventsTableConfig.schema ?? this.eventsSchema)}.${sql.identifier(eventsTableConfig.name)}`;

          const rotated = await tx.execute<{ epoch: string }>(
            sql`UPDATE ${metaTable} SET epoch = gen_random_uuid() WHERE table_name = ${eventsTableConfig.name} RETURNING epoch`,
          );
          const epoch = rotated.rows[0]?.epoch;
          if (!epoch) {
            throw new Error(
              `pulse_meta rotation found no row for "${eventsTableConfig.name}" — reconcile() should have created it`,
            );
          }
          epochs.set(eventsTableConfig.name, epoch);

          await tx.execute(sql`TRUNCATE TABLE ${eventsIdentifier}`);

          await this.seedBaseline(
            async () => {
              const [row] = await tx
                .select()
                .from(sourceTable)
                .orderBy(desc(pulseQuery.pkColumn))
                .limit(1);
              return row;
            },
            tx,
            { sourceTable, pkColumnName: pulseQuery.pkColumn.name, eventsTable },
          );
        }

        await tx.execute(sql`
          insert into ${streamTable} (slot_name, last_lsn)
          values (${this.slotName}, ${consistentPoint})
          on conflict (slot_name) do update set last_lsn = excluded.last_lsn
        `);

        return epochs;
      },
      { isolationLevel: 'repeatable read' },
    );

    this.eventsEpochs = new Map([...this.eventsEpochs, ...epochByName]);
    this.lastPersistedCommitLsn = consistentPoint;
  }

  // Checked-out-connection pin: a dedicated admin connection running BEGIN READ ONLY + SET
  // TRANSACTION SNAPSHOT sequentially, so a setup failure is an ordinary rejection (release()
  // then rethrow) instead of the old parked-deferred hang class. Must be called BEFORE the
  // caller issues rep.start() — the exported snapshot dies on this connection's next command.
  private async openPin(snapshot: string, watermark: string): Promise<void> {
    assertSnapshotName(snapshot);
    const { client, release } = await this.getPulseStore().checkout();

    try {
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      await client.query(`SET TRANSACTION SNAPSHOT '${snapshot}'`);
    } catch (error) {
      release();
      throw error;
    }

    const close = once(async () => {
      try {
        await client.query('ROLLBACK');
      } catch {
        // Connection may already be dead — release() below still runs.
      } finally {
        release();
      }
    });

    const pin: BaselinePin = {
      db: drizzle({ client }),
      watermark,
      round: Promise.resolve(),
      close,
    };
    this.pin = pin;

    // Backstop, armed at creation (covers rep.start() throwing before any reconnect round
    // fires) — identity-guarded so a stale backstop from a superseded pin can never release a
    // newer one.
    setTimeout(() => {
      if (this.pin === pin) void this.releasePin();
    }, REBASELINE_PIN_WINDOW_MS);
  }

  // One release path: linear, never rolls back under a still-in-flight read. stop(),
  // recoverSlot(), createTempSlot(), the round-settled hook, and the backstop all route here.
  private async releasePin(): Promise<void> {
    const pin = this.pin;
    if (!pin) return;
    this.pin = null;
    await pin.round; // a baseline SELECT outliving the pin window still completes
    await pin.close();
  }

  // One local per connection (`tx`) replaces the old currentCommitLsn + pending fields — this
  // also closes the stale-buffer-across-reconnect quirk for free. A clean iterator end simply
  // returns; the supervisor's loop tail supplies the reconnect (the CopyDone fix stays closed).
  private async stream(
    rep: ReplicationConnection,
    iterator: AsyncGenerator<ReplicationEvent>,
    run: Run,
  ): Promise<void> {
    let tx: { commitLsn: string; events: PendingWalEvent[]; fillMisses: FillMiss[] } | null = null;

    for await (const ev of iterator) {
      if (ev.kind === 'begin') {
        tx = { commitLsn: ev.finalLsn, events: [], fillMisses: [] };
        continue;
      }
      if (ev.kind === 'insert' || ev.kind === 'update' || ev.kind === 'delete') {
        if (!tx) {
          // pgoutput streams transactions whole (begin always precedes its row events), so this
          // is a protocol anomaly, not routine — there is no per-message LSN to fall back to.
          this.logError(
            `[WAL Listener] Protocol anomaly: no tracked begin.finalLsn for ${ev.schema}.${ev.table}; skipping event`,
          );
          continue;
        }
        await this.decodeInto(tx, ev);
        continue;
      }
      if (ev.kind !== 'commit') continue; // relation/truncate/message: ignored

      const t = tx;
      tx = null;

      if (
        !t ||
        (this.lastPersistedCommitLsn !== null &&
          lsnFromString(t.commitLsn) <= lsnFromString(this.lastPersistedCommitLsn))
      ) {
        // No begin was observed, or this is a replayed transaction (at-least-once reconnect)
        // already durably persisted — skip persist AND the tap emits.
        rep.ack(ev.endLsn);
        continue;
      }

      if (this.pullEnabled) {
        await this.getPulseStore().persistCommit(t.events, this.slotName, t.commitLsn);
        this.lastPersistedCommitLsn = t.commitLsn;
      }

      // Reset only after real progress, not right after rep.start() — an instantly-clean-ending
      // server (zero commits per connection) must still exhaust run.attempts and reach the
      // terminal path instead of reconnecting forever (WR-02).
      run.attempts = 0;

      // A fill-miss reads as "row deleted" in SQL (zero rows), which is indistinguishable from
      // a real RLS/grant visibility failure — but a trailing delete on the same pk in this same
      // commit batch proves the miss was legitimate (the row really was deleted, just not yet
      // decoded when the fill ran). Only an unabsorbed miss is a real problem.
      for (const miss of t.fillMisses) {
        const absorbed = t.events.some(
          (event) =>
            event.op === 'delete' &&
            event.tableQualifiedName === miss.tableQualifiedName &&
            event.pkValue === miss.pkValue,
        );
        if (!absorbed) {
          this.logError(
            `[WAL Listener] TOAST fill miss on ${miss.tableQualifiedName} pk=${String(miss.pkValue)}: zero rows on admin-pool SELECT with no trailing delete in this commit — check the admin role has SELECT (owner or BYPASSRLS under RLS)`,
          );
        }
      }

      for (const event of t.events) {
        this.walEventEmitter.emit(
          event.tableQualifiedName,
          event.op,
          event.row,
          event.oldRow,
          t.commitLsn,
        );
      }

      // endLsn, never lsn — idleAck's gate tracks endLsn, and only after persist resolves.
      rep.ack(ev.endLsn);
    }
  }

  private async decodeInto(
    t: { events: PendingWalEvent[]; fillMisses: FillMiss[] },
    ev: Extract<ReplicationEvent, { kind: 'insert' | 'update' | 'delete' }>,
  ): Promise<void> {
    const tableQualifiedName = `${ev.schema}.${ev.table}`;

    const metadata = this.sourceTableMetadata.get(tableQualifiedName);
    if (!metadata) {
      return;
    }

    let row: Record<string, unknown>;
    let oldRow: Record<string, unknown> | null;

    // Reading the pk off a 'key' old tuple is safe (pk columns are always real in both 'key'
    // and 'full' tuples) — but reading any OTHER column off a 'key' tuple is not: minipg
    // null-renders them, indistinguishable from a real SQL null. `rawOld` may therefore only be
    // used in full for oldKind === 'full'; elsewhere only its pk column may be read.
    const rawOld = ev.kind !== 'insert' && ev.old ? metadata.normalizeRow(ev.old) : null;

    if (ev.kind === 'insert') {
      row = metadata.normalizeRow(ev.new);
      oldRow = null;
    } else if (ev.kind === 'update') {
      const base = metadata.normalizeRow(ev.new);
      row = base;
      if (ev.oldKind === 'full' && rawOld) {
        // pgoutput omits an UPDATE's unchanged TOASTed columns from the new tuple; the
        // old-under-new spread carries them forward under REPLICA IDENTITY FULL (DRIVER-04).
        row = { ...rawOld, ...base };
      } else if (ev.unchanged.length > 0) {
        // Under a non-full identity the omitted columns are absent from `new` too — a WHERE on
        // one of them would silently drop matching events without this fill (filter-ast treats
        // a missing column as non-matching), so this is required, not an optimization.
        const pkValue = base[metadata.pkColumnName];
        const filled = await this.fillUnchangedByPk(metadata, pkValue, ev.unchanged);
        if (filled) {
          row = { ...base, ...filled };
        } else {
          t.fillMisses.push({ pkValue, tableQualifiedName });
        }
      }
      // A partially-null-rendered old row (non-key columns null from a 'key' tuple) must never
      // reach persistCommit or the tap — only a genuinely full old tuple is emitted.
      oldRow = ev.oldKind === 'full' ? rawOld : null;
    } else {
      // Deliberately empty: the tap's dedupe-by-absence contract for deletes. The persisted
      // events-table row still carries the old row's data via PulseStore's buildEventRow.
      row = {};
      // Sanctioned pk-only delete degradation under a non-full identity — every other emitted
      // row stays keyed the same way, so downstream membership/pk consumers are unaffected.
      oldRow =
        ev.oldKind === 'full'
          ? rawOld
          : rawOld
            ? { [metadata.pkColumnName]: rawOld[metadata.pkColumnName] }
            : null;
    }

    const pkSource = ev.kind === 'delete' ? oldRow : row;
    const pkValue = pkSource?.[metadata.pkColumnName];
    if (pkValue === undefined || pkValue === null) {
      this.logDebug(
        `[WAL Listener] Skipping ${ev.kind} on ${tableQualifiedName}: missing pk (${String(pkValue)})`,
      );
      return;
    }

    // Narrowed to deletes only: under REPLICA IDENTITY DEFAULT a pk-stable update legitimately
    // arrives with no old tuple at all (ev.old === null) and MUST still proceed.
    if (ev.kind === 'delete' && !oldRow) {
      this.logDebug(
        `[WAL Listener] Skipping delete on ${tableQualifiedName}: missing old row data for pk=${pkValue}`,
      );
      return;
    }

    const oldPk = rawOld?.[metadata.pkColumnName];
    const pkChanged =
      oldPk !== pkValue &&
      !(
        oldPk instanceof Date &&
        pkValue instanceof Date &&
        oldPk.getTime() === pkValue.getTime()
      ) &&
      !(Buffer.isBuffer(oldPk) && Buffer.isBuffer(pkValue) && oldPk.equals(pkValue));
    if (ev.kind === 'update' && oldPk != null && pkChanged) {
      // pk-changing UPDATE (BUG-02): a single update entry keyed by the new pk leaves every
      // consumer holding a ghost row under the old pk. Synthesize delete(oldPk) then
      // insert(newPk) — delete MUST precede insert since stream()'s commit case fans out
      // `events` in order (per-index snapshots for the events table, emit order for the tap).
      const base = {
        eventsTable: metadata.eventsTable,
        pkColumnName: metadata.pkColumnName,
        tableQualifiedName,
      };
      const oldRowForDelete =
        ev.oldKind === 'full' ? rawOld : { [metadata.pkColumnName]: oldPk };
      t.events.push({ ...base, op: 'delete', pkValue: oldPk, row: {}, oldRow: oldRowForDelete });
      t.events.push({ ...base, op: 'insert', pkValue, row, oldRow: null });
      return;
    }

    t.events.push({
      eventsTable: metadata.eventsTable,
      pkColumnName: metadata.pkColumnName,
      pkValue,
      op: ev.kind,
      row,
      oldRow,
      tableQualifiedName,
    });
  }

  // One pk-SELECT of exactly the omitted TOASTed columns, on the admin pool — required for
  // correctness under a non-full identity (see decodeInto), not an optimization. No cache, no
  // batcher, no retries (D-04): read-your-latest is the accepted consistency model, same as the
  // baseline MVCC race — any later change arrives explicitly in a later WAL event.
  private async fillUnchangedByPk(
    metadata: SourceTableMetadata,
    pkValue: unknown,
    unchanged: string[],
  ): Promise<Record<string, unknown> | null> {
    const sourceConfig = getTableConfig(metadata.sourceTable);
    const sourceIdentifier = sql`${sql.identifier(sourceConfig.schema ?? 'public')}.${sql.identifier(sourceConfig.name)}`;
    const columnsIdentifier = sql.join(
      unchanged.map((name) => sql`${sql.identifier(name)}`),
      sql`, `,
    );
    const result = await this.getPulseStore().getDb().execute<Record<string, unknown>>(sql`
      select ${columnsIdentifier}
      from ${sourceIdentifier}
      where ${sql.identifier(metadata.pkColumnName)} = ${pkValue}
      limit 1
    `);
    const row = result.rows[0];
    return row ? metadata.normalizeRow(row) : null;
  }

  private getPulseStore(): PulseStore {
    if (!this.store) {
      throw new Error('PulseStore has not been initialized');
    }

    return this.store;
  }

  private logInfo(message: string, ...args: unknown[]): void {
    if (this.logLevel >= LogLevel.Info) console.log(message, ...args);
  }

  private logError(message: string, ...args: unknown[]): void {
    if (this.logLevel >= LogLevel.Error) console.error(message, ...args);
  }

  private logWarn(message: string, ...args: unknown[]): void {
    if (this.logLevel >= LogLevel.Info) console.warn(message, ...args);
  }

  private logDebug(message: string, ...args: unknown[]): void {
    if (this.logLevel >= LogLevel.Debug) console.log(message, ...args);
  }
}

export function expose<TQueries extends AnyPulseBuilders>(
  registry: PulseRegistry<TQueries>,
  config: ExposeConfig,
) {
  return new PulseRuntime(registry, config);
}
