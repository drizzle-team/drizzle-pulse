import { createHash } from 'node:crypto';
import { desc, getTableUniqueName, sql } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import { createPool, type Pool } from 'minipg';
import type { ClientConfig, PoolConfig } from 'pg';
import { LogicalReplicationService, type Pgoutput, PgoutputPlugin } from 'pg-logical-replication';
import type { ResolvedPulseQuery } from '../types.js';
import { emitEventsTableDdl } from './events-table-ddl.js';
import { buildEventsTable, DEFAULT_EVENTS_SCHEMA } from './events-table-resolver.js';
import type { AnyPulseBuilders, PulseRegistry } from './pulse-registry.js';
import { buildSelectQuery, type PulseSourceDb } from './pulse-sql.js';
import { PulseStore } from './pulse-store.js';
import { DEFAULT_PULL_EVENT_LIMIT, PulseRequestHandler } from './sdk.js';
import { WalEventEmitter } from './wal-event-emitter.js';
import { createWalRowNormalizer } from './wal-normalization.js';

type RuntimeLifecycleListener = () => void;

// date/timestamp/timestamptz/interval/point: pg-types' defaults yield non-text JS values
// that Drizzle's from-text codecs cannot consume.
const RAW_TEXT_PG_OIDS = new Set([1082, 1114, 1184, 1186, 600]);

const rawText = (value: string): string => value;

// Keeps WAL values for the raw-text OIDs as text without mutating the process-global pg-types
// registry pg-logical-replication decodes through. Couples to pg-logical-replication internals
// (relation.columns[].parser); the runtime guard below fails loud if that shape changes.
function scopeRawTextWalParsers(plugin: PgoutputPlugin): PgoutputPlugin {
  const parse = plugin.parse.bind(plugin);
  plugin.parse = (buffer: Buffer) => {
    const message = parse(buffer);
    if (message.tag === 'relation') {
      if (!Array.isArray(message.columns)) {
        throw new Error('pg-logical-replication relation message shape changed: no columns[]');
      }
      for (const column of message.columns) {
        if (RAW_TEXT_PG_OIDS.has(column.typeOid)) {
          column.parser = rawText;
        }
      }
    }
    return message;
  };
  return plugin;
}

export interface WalListenerConfig {
  pgConfig: ClientConfig & { replication: 'database' };
  publicationName: string;
  slotName: string;
  reconnect?: {
    maxRetries?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
  };
}

export type ExposeWalConfig = Partial<
  Pick<WalListenerConfig, 'publicationName' | 'slotName' | 'reconnect'>
>;

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

type WalEvent = {
  tableQualifiedName: string;
  operation: 'insert' | 'update' | 'delete';
  rowData: Record<string, unknown>;
  oldRowData: Record<string, unknown> | null;
  lsn: string;
};

const DEFAULT_RECONNECT = {
  maxRetries: 10,
  baseDelayMs: 1000,
  maxDelayMs: 30000,
};

const DEFAULT_PUBLICATION_NAME = 'drizzle_pulse';
const DEFAULT_SLOT_NAME = 'drizzle_pulse';

function withQuietPgOptions<T extends ClientConfig | PoolConfig>(config: T): T {
  return {
    ...config,
    options: [config.options, '-c client_min_messages=warning'].filter(Boolean).join(' '),
  };
}

export class PulseRuntime<TQueries extends AnyPulseBuilders> {
  private readonly sourceTableMetadata: Map<string, SourceTableMetadata>;
  private readonly requestHandler: PulseRequestHandler;
  private readonly pgConfig: ClientConfig & { replication: 'database' };
  private readonly reconnectConfig: Required<NonNullable<ExposeWalConfig['reconnect']>>;
  private readonly logLevel: LogLevel;
  readonly publicationName: string;
  readonly slotName: string;
  private readonly eventsSchema: string;
  // Populated by reconcile(): events-table name -> current epoch (uuid, rotated on every DDL
  // recreate). Handlers read it via getEpochForQuery to mint/validate cursor tokens.
  private eventsEpochs = new Map<string, string>();

  private pool: Pool | null = null;
  private pulseStore: PulseStore | null = null;
  private replicationService: LogicalReplicationService | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  isRunning = false;
  private reconnectAttempts = 0;
  // Set on pgoutput's 'begin' message, cleared on 'commit' — every insert/update/delete between
  // the two shares this transaction's commit LSN. Null when no begin was observed (e.g. the
  // stream started mid-transaction), in which case handleMessage falls back to the per-message lsn.
  private currentCommitLsn: string | null = null;
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
        normalizeRow: createWalRowNormalizer(sourceTable),
      });
    }

    const pg = { connectionString: this.config.databaseUrl };

    this.pgConfig = {
      ...withQuietPgOptions(pg),
      replication: 'database',
    };
    this.reconnectConfig = { ...DEFAULT_RECONNECT, ...wal.reconnect };
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

    const replicationService = this.replicationService;
    this.replicationService = null;
    if (replicationService) {
      await replicationService.stop();
    }

    const pool = this.pool;
    this.pool = null;
    this.pulseStore = null;

    if (pool) {
      await pool.end();
    }

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
    this.pulseStore = new PulseStore(pool);
  }

  // Mirrors stop()'s pool/service teardown so a failed guard doesn't leak connections —
  // callers await start() rejections and then discard the runtime.
  private async teardownFailedStart(): Promise<void> {
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
        if (relname === 'pulse_meta' || desired.has(relname) || metaByName.has(relname)) continue;
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

  private async connectReplication(): Promise<void> {
    try {
      const adminDb = this.getPulseStore().getDb();

      const replicationService = new LogicalReplicationService(this.pgConfig, {
        acknowledge: { auto: false, timeoutSeconds: 10 },
        flowControl: { enabled: true },
      });
      this.replicationService = replicationService;

      const plugin = scopeRawTextWalParsers(
        new PgoutputPlugin({
          protoVersion: 1,
          publicationNames: [this.publicationName],
        }),
      );

      replicationService.on('data', async (lsn: string, log: Pgoutput.Message) => {
        try {
          await this.handleMessage(lsn, log);
          if (this.replicationService === replicationService) {
            await replicationService.acknowledge(lsn);
          }
        } catch (error) {
          this.logError('[WAL Listener] Error processing message:', error);
        }
      });

      replicationService.on('error', async (error: Error) => {
        if (this.replicationService !== replicationService) {
          return;
        }

        this.logError('[WAL Listener] Replication error:', error);
        await this.handleDisconnect(replicationService);
      });

      replicationService.on('start', () => {
        this.onReplicationStart();
      });

      replicationService.on('acknowledge', (lsn: string) => {
        this.logDebug(`[WAL Listener] Acknowledged LSN: ${lsn}`);
      });

      replicationService.on(
        'heartbeat',
        (lsn: string, timestamp: number, shouldRespond: boolean) => {
          if (shouldRespond) {
            this.logDebug(`[WAL Listener] Heartbeat at LSN: ${lsn}, timestamp: ${timestamp}`);
          }
        },
      );

      const slotName = this.slotName;
      const result = await adminDb.execute<{
        slot_name: string;
        active: boolean;
        active_pid: number | null;
      }>(
        sql`SELECT slot_name, active, active_pid FROM pg_replication_slots WHERE slot_name = ${slotName}`,
      );

      if (result.rows.length === 0) {
        this.logInfo(`[WAL Listener] Creating replication slot '${slotName}'`);
        await adminDb.execute(
          sql`SELECT pg_create_logical_replication_slot(${slotName}, ${'pgoutput'})`,
        );
      } else {
        const slot = result.rows[0];
        if (slot?.active && slot.active_pid) {
          this.logInfo(
            `[WAL Listener] Terminating stale connection on slot '${slotName}' (PID ${slot.active_pid})`,
          );
          await adminDb.execute(sql`SELECT pg_terminate_backend(${slot.active_pid})`);
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        this.logInfo(`[WAL Listener] Replication slot '${slotName}' ready`);
      }

      this.logInfo(`[WAL Listener] Subscribing to slot '${this.slotName}'`);
      const streaming = replicationService.subscribe(plugin, this.slotName);
      await Promise.race([
        new Promise((resolve) => replicationService.once('start', resolve)),
        streaming, // settles first only on immediate failure → propagates the error
      ]);
      void streaming.catch(() => {}); // stream-end errors already handled via the 'error' listener
    } catch (error) {
      this.logError('[WAL Listener] Connection error:', error);
      await this.handleDisconnect(this.replicationService);
    }
  }

  private async handleDisconnect(
    replicationService: LogicalReplicationService | null,
  ): Promise<void> {
    if (replicationService && this.replicationService === replicationService) {
      this.replicationService = null;
      try {
        await replicationService.stop();
      } catch (error) {
        this.logError('[WAL Listener] Error during stop:', error);
      }
    }

    if (!this.isRunning) {
      return;
    }

    this.clearReconnectTimer();

    if (this.reconnectAttempts >= this.reconnectConfig.maxRetries) {
      this.logError('[WAL Listener] Max reconnection attempts reached. Giving up.');
      const terminalError = new Error(
        `WAL replication failed permanently after ${this.reconnectConfig.maxRetries} reconnect attempts`,
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

    const exponentialDelay = this.reconnectConfig.baseDelayMs * 2 ** this.reconnectAttempts;
    const jitter = Math.random() * 1000;
    const delay = Math.min(exponentialDelay + jitter, this.reconnectConfig.maxDelayMs);

    this.reconnectAttempts += 1;
    this.logInfo(
      `[WAL Listener] Reconnecting in ${Math.round(delay)}ms (attempt ${this.reconnectAttempts}/${this.reconnectConfig.maxRetries})`,
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

  private async handleMessage(lsn: string, log: Pgoutput.Message): Promise<void> {
    if (log.tag === 'begin') {
      this.currentCommitLsn = log.commitLsn;
      return;
    }
    if (log.tag === 'commit') {
      this.currentCommitLsn = null;
      return;
    }
    if (log.tag !== 'insert' && log.tag !== 'update' && log.tag !== 'delete') {
      return;
    }

    const event = this.parseWalEvent(lsn, log);
    if (!event) {
      return;
    }

    const metadata = this.sourceTableMetadata.get(event.tableQualifiedName);
    if (!metadata) {
      return;
    }

    let commitLsn = this.currentCommitLsn;
    // A change message's own lsn is always below its transaction's commit lsn, so this fallback
    // stamps an undervalued watermark: a tap payload could then carry an lsn below the embedded
    // client's watermark for a row not actually in its baseline, and get silently dropped
    // (unrecoverable) instead of buffered. pgoutput streams transactions whole (`begin` first),
    // so this should be unreachable — treat it as a protocol anomaly, not routine, and skip the
    // tap emit (the events-table row is still persisted; a later re-baseline picks it up).
    let commitLsnIsFallback = false;
    if (!commitLsn) {
      commitLsnIsFallback = true;
      this.logError(
        `[WAL Listener] Protocol anomaly: no tracked begin.commitLsn for ${event.tableQualifiedName}; falling back to per-message lsn and skipping the tap emit`,
      );
      commitLsn = lsn;
    }

    this.logDebug(
      `[WAL Listener] ${event.operation.toUpperCase()} on ${event.tableQualifiedName}:`,
      {
        lsn: event.lsn,
        row_data: event.rowData,
        old_row_data: event.oldRowData,
      },
    );

    await this.persistWalEvent(event, metadata, commitLsn, commitLsnIsFallback);
  }

  private parseWalEvent(
    lsn: string,
    log: Pgoutput.MessageInsert | Pgoutput.MessageUpdate | Pgoutput.MessageDelete,
  ): WalEvent | null {
    const tableQualifiedName = `${log.relation.schema}.${log.relation.name}`;

    switch (log.tag) {
      case 'insert':
        return {
          tableQualifiedName,
          operation: 'insert',
          rowData: log.new as Record<string, unknown>,
          oldRowData: null,
          lsn,
        };
      case 'update':
        return {
          tableQualifiedName,
          operation: 'update',
          rowData: log.new as Record<string, unknown>,
          oldRowData: (log.old as Record<string, unknown>) ?? null,
          lsn,
        };
      case 'delete':
        return {
          tableQualifiedName,
          operation: 'delete',
          rowData: {},
          oldRowData: (log.old as Record<string, unknown>) ?? null,
          lsn,
        };
      default:
        return null;
    }
  }

  private async persistWalEvent(
    event: WalEvent,
    metadata: SourceTableMetadata,
    commitLsn: string,
    commitLsnIsFallback = false,
  ): Promise<void> {
    const currentPulseStore = this.getPulseStore();
    const pkSource = event.operation === 'delete' ? event.oldRowData : event.rowData;
    const pkValue = pkSource?.[metadata.pkColumnName];

    if (pkValue === undefined || pkValue === null) {
      this.logDebug(
        `[WAL Listener] Skipping ${event.operation} on ${event.tableQualifiedName}: missing pk (${String(pkValue)})`,
      );
      return;
    }

    // Normalize the raw WAL row to JS types once with Drizzle's codecs, then feed that
    // single representation to BOTH the events-table insert (normal Drizzle mapping, no
    // raw ::type cast) and the tap.
    const normalizedOldRowData = event.oldRowData ? metadata.normalizeRow(event.oldRowData) : null;
    // pgoutput omits an UPDATE's unchanged TOASTed columns from the new tuple entirely; without
    // REPLICA IDENTITY FULL backfilling those keys from the old tuple, the column would go
    // missing from normalizedRowData and PulseMergeCore.applyEvents (which replaces the stored
    // row wholesale) would drop a previously-known large value from collection state. Insert has
    // no old row (spreading null is a no-op) and delete's rowData is deliberately empty for the
    // tap's dedup-by-absence contract (see tap-events.ts), so this only applies to updates.
    const normalizedRowData =
      event.operation === 'update' && normalizedOldRowData
        ? { ...normalizedOldRowData, ...metadata.normalizeRow(event.rowData) }
        : metadata.normalizeRow(event.rowData);

    let snapshot: number;
    if (event.operation === 'insert') {
      snapshot = await currentPulseStore.persistInsertEvent(
        metadata.eventsTable,
        metadata.pkColumnName,
        pkValue,
        normalizedRowData,
      );
    } else if (event.operation === 'update') {
      if (!normalizedOldRowData) {
        this.logDebug(
          `[WAL Listener] Skipping update on ${event.tableQualifiedName}: missing old row data for pk=${pkValue}`,
        );
        return;
      }

      snapshot = await currentPulseStore.persistUpdateEvent(
        metadata.eventsTable,
        metadata.pkColumnName,
        pkValue,
        normalizedRowData,
        normalizedOldRowData,
      );
    } else {
      // pkValue for a delete is extracted from event.oldRowData above, so a non-null
      // pk guarantees normalizedOldRowData is present; the guard is defensive only.
      if (!normalizedOldRowData) {
        this.logDebug(
          `[WAL Listener] Skipping delete on ${event.tableQualifiedName}: missing old row data for pk=${pkValue}`,
        );
        return;
      }

      snapshot = await currentPulseStore.persistDeleteEvent(
        metadata.eventsTable,
        metadata.pkColumnName,
        pkValue,
        normalizedOldRowData,
      );
    }

    this.lastPersistedSnapshot = Math.max(this.lastPersistedSnapshot, snapshot);
    // An undervalued (fallback) commitLsn would let the embedded client's watermark filter
    // silently drop this event instead of buffering it — the persisted events-table row is
    // the recoverable path (a re-baseline picks it up); skip only the live tap emit.
    if (!commitLsnIsFallback) {
      this.walEventEmitter.emit(
        event.tableQualifiedName,
        event.operation,
        normalizedRowData,
        normalizedOldRowData,
        snapshot,
        commitLsn,
      );
    }

    this.logDebug(
      `[WAL Listener] Persisted ${event.operation} event on ${event.tableQualifiedName} (pk=${pkValue})`,
    );
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
