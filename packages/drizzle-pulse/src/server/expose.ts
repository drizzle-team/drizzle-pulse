import { createHash } from 'node:crypto';
import { desc, getTableUniqueName, sql } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import {
  createPool,
  lsnFromString,
  type Pool,
  replication,
  type ReplicationConnection,
  type ReplicationEvent,
} from 'minipg';
import type { ResolvedPulseQuery } from '../types.js';
import { emitEventsTableDdl } from './events-table-ddl.js';
import { buildEventsTable, DEFAULT_EVENTS_SCHEMA } from './events-table-resolver.js';
import type { AnyPulseBuilders, PulseRegistry } from './pulse-registry.js';
import { buildSelectQuery, type PulseSourceDb } from './pulse-sql.js';
import { PulseStore } from './pulse-store.js';
import { DEFAULT_PULL_EVENT_LIMIT, PulseRequestHandler } from './sdk.js';
import { createShapeRowNormalizer } from './wal-shape-bridge.js';
import { WalEventEmitter } from './wal-event-emitter.js';

type RuntimeLifecycleListener = () => void;

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
   * context (RLS, search_path). node-postgres pools must set `types` to deliver
   * date/timestamp/timestamptz/interval/point as raw text (postgres-js does so natively).
   */
  sourceDb: PulseSourceDb;
  eventsSchema?: string;
  wal?: ExposeWalConfig;
  /**
   * Max events a single pull may replay before it falls back to a full reset instead of
   * streaming an unbounded batch. Defaults to {@link DEFAULT_PULL_EVENT_LIMIT} (1000).
   */
  pullEventLimit?: number;
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

// D-04: no reconnect tuning knobs — fixed internal defaults, same values as the prior
// config-driven defaults.
const RECONNECT_MAX_RETRIES = 10;
const RECONNECT_BASE_DELAY_MS = 1000;
const RECONNECT_MAX_DELAY_MS = 30000;

const DEFAULT_PUBLICATION_NAME = 'drizzle_pulse';
const DEFAULT_SLOT_NAME = 'drizzle_pulse';

export class PulseRuntime<TQueries extends AnyPulseBuilders> {
  private readonly sourceTableMetadata: Map<string, SourceTableMetadata>;
  private readonly requestHandler: PulseRequestHandler;
  private readonly logLevel: LogLevel;
  readonly publicationName: string;
  readonly slotName: string;
  private readonly eventsSchema: string;
  // Populated by reconcile(): events-table name -> current epoch (uuid, rotated on every DDL
  // recreate). Handlers read it via getEpochForQuery to mint/validate cursor tokens.
  private eventsEpochs = new Map<string, string>();

  private pool: Pool | null = null;
  private pulseStore: PulseStore | null = null;
  private replication: ReplicationConnection | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  isRunning = false;
  private reconnectAttempts = 0;
  // Set on the replication stream's 'begin' event (finalLsn), cleared once its 'commit' is
  // handled — every insert/update/delete between the two shares this transaction's commit LSN.
  // Row events carry no per-message LSN under minipg, so a null here (begin never observed) is
  // a protocol anomaly, not a routine case.
  private currentCommitLsn: string | null = null;
  // Buffered row events for the in-flight transaction; persisted atomically on commit.
  private pending: PendingWalEvent[] = [];
  // In-memory mirror of the durable pulse_stream watermark — dedupes at-least-once replay after
  // a reconnect without a store round trip on every commit.
  private lastPersistedCommitLsn: string | null = null;
  lastPersistedSnapshot = 0;
  readonly walEventEmitter = new WalEventEmitter();
  private everConnected = false;
  private readonly reconnectListeners = new Set<RuntimeLifecycleListener>();
  private readonly stopListeners = new Set<RuntimeLifecycleListener>();
  private readonly terminalErrorListeners = new Set<(error: Error) => void>();

  get sourceDb(): PulseSourceDb {
    return this.config.sourceDb;
  }

  // Lifecycle edges the in-process (embedded) client subscribes to. The runtime does
  // not own collections — reconnect fires after the WAL stream re-establishes (live
  // collections must rebaseline to catch up on events missed while disconnected), and
  // stop fires as the runtime tears down (collections must dispose).
  onReconnect(listener: RuntimeLifecycleListener): () => void {
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
    this.eventsSchema = this.config.eventsSchema ?? DEFAULT_EVENTS_SCHEMA;

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
      this.config.pullEventLimit ?? DEFAULT_PULL_EVENT_LIMIT,
    );
  }

  get handlers() {
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

  async ensureBaselines(): Promise<void> {
    const currentPulseStore = this.getPulseStore();
    const sourceDb = this.config.sourceDb;

    for (const queryName of this.registry.getQueryNames()) {
      const pulseQuery = this.registry.getPulseQuery(queryName);
      const sourceTable = this.registry.getSourceTable(queryName);
      if (!pulseQuery || !sourceTable) continue;

      const [baselineRow] = await sourceDb
        .select()
        .from(sourceTable)
        .orderBy(desc(pulseQuery.pkColumn))
        .limit(1);

      await currentPulseStore.createBaselineSnapshot(
        this.getEventsTableForQuery(queryName),
        pulseQuery.pkColumn.name,
        baselineRow ?? null,
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
   */
  async readCollectionBaseline(
    resolved: ResolvedPulseQuery,
  ): Promise<{ rows: Record<string, unknown>[]; watermark: string }> {
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
    if (this.isRunning) {
      this.logInfo('[WAL Listener] Already running');
      return;
    }

    this.initializeDatabaseServices();

    try {
      await this.reconcile();

      this.isRunning = true;
      await this.ensureBaselines();

      let maxSnapshot = 0;
      const service = this.getPulseStore();
      for (const meta of this.sourceTableMetadata.values()) {
        const snap = await service.getLatestSnapshot(meta.eventsTable);
        maxSnapshot = Math.max(maxSnapshot, snap);
      }
      this.lastPersistedSnapshot = maxSnapshot;

      await this.connectReplication();
    } catch (error) {
      this.isRunning = false;
      await this.teardownFailedStart();
      throw error;
    }
  }

  async stop(): Promise<void> {
    for (const listener of [...this.stopListeners]) {
      listener();
    }

    this.isRunning = false;
    this.clearReconnectTimer();

    const rep = this.replication;
    this.replication = null;
    rep?.end();

    await this.closePool();

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
    this.initializeDatabaseServices();
    try {
      await this.reconcile();
    } finally {
      await this.teardownFailedStart();
    }
  }

  private initializeDatabaseServices(): void {
    if (this.pool && this.pulseStore) {
      return;
    }

    const pool = createPool(this.config.databaseUrl);
    this.pool = pool;
    this.pulseStore = new PulseStore(pool, this.eventsSchema);
  }

  // Mirrors stop()'s pool teardown so a failed guard doesn't leak connections — callers await
  // start() rejections and then discard the runtime.
  private async teardownFailedStart(): Promise<void> {
    await this.closePool();
  }

  private async closePool(): Promise<void> {
    const pool = this.pool;
    this.pool = null;
    this.pulseStore = null;

    if (pool) {
      await pool.end();
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
      // published change already carries complete old-row data.
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

        // Un-pulse members no longer registered: DROP from the publication, THEN reset REPLICA
        // IDENTITY (a member row implies the table still exists — pg_publication_tables joins
        // pg_class, so a dropped table has already left membership on its own).
        for (const member of members) {
          if (registeredNames.has(member.name)) continue;
          await execDdl(
            `ALTER PUBLICATION ${pubIdent} DROP TABLE ${member.quoted}`,
            'ownership of the publication',
          );
          await execDdl(
            `ALTER TABLE ${member.quoted} REPLICA IDENTITY DEFAULT`,
            `ownership of ${member.name}`,
          );
        }
      }

      await tx.execute(sql`CREATE SCHEMA IF NOT EXISTS ${schema}`);
      await tx.execute(
        sql`CREATE TABLE IF NOT EXISTS ${metaTable} (table_name text PRIMARY KEY, ddl_hash text NOT NULL, epoch uuid NOT NULL)`,
      );
      // Durable commit-LSN dedupe watermark (research Open Question 1): survives a full process
      // restart so resuming an intact slot doesn't re-persist minipg's at-least-once replay tail.
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
      const epochByName = new Map<string, string>();

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
        if (relname === 'pulse_meta' || relname === 'pulse_stream' || desired.has(relname) || metaByName.has(relname))
          continue;
        this.logWarn(
          `[reconcile] table "${eventsSchema}.${relname}" shares the events schema but has no pulse_meta row; leaving it untouched`,
        );
      }

      return epochByName;
    });

    this.eventsEpochs = epochs;
  }

  private onReplicationStart(): void {
    if (!this.isRunning) return;
    this.logInfo('[WAL Listener] Replication started');
    if (this.everConnected) {
      for (const listener of [...this.reconnectListeners]) {
        listener();
      }
    }
    this.everConnected = true;
    this.reconnectAttempts = 0;
  }

  // Branches every (re)connect on slot state (LOCKED backfill/resume spec, STATE.md
  // §Decisions): an intact slot resumes from confirmed_flush with commit-LSN-deduped replay; a
  // missing slot is recreated (full recovery machine incl. re-baseline lands in plan 04 — this
  // plan only wires the branch and the resulting `from`/watermark seed).
  private async connectReplication(): Promise<void> {
    try {
      const adminDb = this.getPulseStore().getDb();
      const rep = await replication(this.config.databaseUrl);
      this.replication = rep;

      const slotName = this.slotName;
      const result = await adminDb.execute<{
        slot_name: string;
        active: boolean;
        active_pid: number | null;
      }>(
        sql`SELECT slot_name, active, active_pid FROM pg_replication_slots WHERE slot_name = ${slotName}`,
      );

      let from: string | undefined;
      if (result.rows.length > 0) {
        const slot = result.rows[0];
        if (slot?.active && slot.active_pid) {
          this.logInfo(
            `[WAL Listener] Terminating stale connection on slot '${slotName}' (PID ${slot.active_pid})`,
          );
          await adminDb.execute(sql`SELECT pg_terminate_backend(${slot.active_pid})`);
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        this.logInfo(`[WAL Listener] Replication slot '${slotName}' ready`);
        from = undefined; // server resumes from the slot's confirmed_flush position
        this.lastPersistedCommitLsn = await this.getPulseStore().getStreamWatermark(slotName);
      } else {
        this.logInfo(`[WAL Listener] Creating replication slot '${slotName}'`);
        const { consistentPoint } = await rep.createSlot(slotName, {
          temporary: false,
          snapshot: 'export',
        });
        from = consistentPoint;
        this.lastPersistedCommitLsn = null;
      }

      this.logInfo(`[WAL Listener] Subscribing to slot '${slotName}'`);
      const iterator = rep.start({
        slot: slotName,
        publications: [this.publicationName],
        from,
        statusIntervalMs: 1000,
        idleAck: true,
        messages: false,
      });

      this.onReplicationStart();
      void this.runReplicationLoop(rep, iterator);
    } catch (error) {
      this.logError('[WAL Listener] Connection error:', error);
      await this.handleDisconnect(this.replication);
    }
  }

  // The tight for-await loop: minipg's internal read loop only advances while a next() pull is
  // pending, so this never parks between pulls — persisting inside the commit branch is fine
  // (it's still driven by the same next() call), background work between pulls is not.
  private async runReplicationLoop(
    rep: ReplicationConnection,
    iterator: AsyncGenerator<ReplicationEvent>,
  ): Promise<void> {
    try {
      for await (const ev of iterator) {
        if (ev.kind === 'begin') {
          this.currentCommitLsn = ev.finalLsn;
          this.pending = [];
          continue;
        }
        if (ev.kind === 'insert' || ev.kind === 'update' || ev.kind === 'delete') {
          this.bufferWalEvent(ev);
          continue;
        }
        if (ev.kind === 'commit') {
          await this.handleCommit(rep, ev);
          continue;
        }
        // relation/truncate/message: ignored
      }
    } catch (error) {
      if (this.replication !== rep) return; // superseded by a newer connection — expected
      this.logError('[WAL Listener] Replication error:', error);
      await this.handleDisconnect(rep);
      return;
    }

    if (this.replication !== rep) return; // superseded; the old connection's stream ended cleanly
  }

  private bufferWalEvent(
    ev: Extract<ReplicationEvent, { kind: 'insert' | 'update' | 'delete' }>,
  ): void {
    const tableQualifiedName = `${ev.schema}.${ev.table}`;

    if (this.currentCommitLsn === null) {
      // pgoutput streams transactions whole (begin always precedes its row events), so this is
      // a protocol anomaly, not routine — there is no per-message LSN to fall back to under
      // minipg. Skip the event entirely; a later re-baseline recovers it.
      this.logError(
        `[WAL Listener] Protocol anomaly: no tracked begin.finalLsn for ${tableQualifiedName}; skipping event`,
      );
      return;
    }

    const metadata = this.sourceTableMetadata.get(tableQualifiedName);
    if (!metadata) {
      return;
    }

    let row: Record<string, unknown>;
    let oldRow: Record<string, unknown> | null;

    if (ev.kind === 'insert') {
      row = metadata.normalizeRow(ev.new);
      oldRow = null;
    } else if (ev.kind === 'update') {
      const normalizedOld = ev.old ? metadata.normalizeRow(ev.old) : null;
      // pgoutput omits an UPDATE's unchanged TOASTed columns from the new tuple; the old-under-
      // new spread carries them forward under REPLICA IDENTITY FULL (DRIVER-04). `ev.unchanged`
      // names exactly those columns but is not needed as the mechanism.
      row = normalizedOld
        ? { ...normalizedOld, ...metadata.normalizeRow(ev.new) }
        : metadata.normalizeRow(ev.new);
      oldRow = normalizedOld;
    } else {
      // Deliberately empty: the tap's dedupe-by-absence contract for deletes. The persisted
      // events-table row still carries the old row's data via PulseStore's buildEventRow.
      row = {};
      oldRow = ev.old ? metadata.normalizeRow(ev.old) : null;
    }

    const pkSource = ev.kind === 'delete' ? oldRow : row;
    const pkValue = pkSource?.[metadata.pkColumnName];
    if (pkValue === undefined || pkValue === null) {
      this.logDebug(
        `[WAL Listener] Skipping ${ev.kind} on ${tableQualifiedName}: missing pk (${String(pkValue)})`,
      );
      return;
    }

    if ((ev.kind === 'update' || ev.kind === 'delete') && !oldRow) {
      this.logDebug(
        `[WAL Listener] Skipping ${ev.kind} on ${tableQualifiedName}: missing old row data for pk=${pkValue}`,
      );
      return;
    }

    this.pending.push({
      eventsTable: metadata.eventsTable,
      pkColumnName: metadata.pkColumnName,
      pkValue,
      op: ev.kind,
      row,
      oldRow,
      tableQualifiedName,
    });
  }

  private async handleCommit(
    rep: ReplicationConnection,
    ev: Extract<ReplicationEvent, { kind: 'commit' }>,
  ): Promise<void> {
    const commitLsn = this.currentCommitLsn;
    const pending = this.pending;
    this.currentCommitLsn = null;
    this.pending = [];

    if (commitLsn === null) {
      // No begin was observed for this transaction (see bufferWalEvent) — nothing was buffered
      // to persist.
      rep.ack(ev.endLsn);
      return;
    }

    if (
      this.lastPersistedCommitLsn !== null &&
      lsnFromString(commitLsn) <= lsnFromString(this.lastPersistedCommitLsn)
    ) {
      // Replayed transaction (at-least-once reconnect) — already durably persisted; skip persist
      // AND the tap emits.
      rep.ack(ev.endLsn);
      return;
    }

    const snapshots = await this.getPulseStore().persistCommit(pending, this.slotName, commitLsn);
    if (snapshots.length > 0) {
      this.lastPersistedSnapshot = Math.max(this.lastPersistedSnapshot, ...snapshots);
    }
    this.lastPersistedCommitLsn = commitLsn;

    pending.forEach((event, index) => {
      this.walEventEmitter.emit(
        event.tableQualifiedName,
        event.op,
        event.row,
        event.oldRow,
        snapshots[index] ?? 0,
        commitLsn,
      );
    });

    // endLsn, never lsn — idleAck's gate tracks endLsn, and only after persist resolves.
    rep.ack(ev.endLsn);
  }

  private async handleDisconnect(rep: ReplicationConnection | null): Promise<void> {
    if (rep && this.replication === rep) {
      this.replication = null;
    }
    rep?.end();

    if (!this.isRunning) {
      return;
    }

    this.clearReconnectTimer();

    if (this.reconnectAttempts >= RECONNECT_MAX_RETRIES) {
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
      return;
    }

    const exponentialDelay = RECONNECT_BASE_DELAY_MS * 2 ** this.reconnectAttempts;
    const jitter = Math.random() * 1000;
    const delay = Math.min(exponentialDelay + jitter, RECONNECT_MAX_DELAY_MS);

    this.reconnectAttempts += 1;
    this.logInfo(
      `[WAL Listener] Reconnecting in ${Math.round(delay)}ms (attempt ${this.reconnectAttempts}/${RECONNECT_MAX_RETRIES})`,
    );

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.isRunning) {
        void this.connectReplication();
      }
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (!this.reconnectTimer) {
      return;
    }

    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private getPulseStore(): PulseStore {
    if (!this.pulseStore) {
      throw new Error('PulseStore has not been initialized');
    }

    return this.pulseStore;
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
