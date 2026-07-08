import { desc, getTableUniqueName, sql } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import { type ClientConfig, Pool, type PoolConfig, types } from 'pg';
import { LogicalReplicationService, type Pgoutput, PgoutputPlugin } from 'pg-logical-replication';
import { buildEventsTable, DEFAULT_EVENTS_SCHEMA } from './events-table-resolver.js';
import { RealtimeRequestHandler } from './handlers.js';
import type { AnyPulseBuilders, PulseRegistry } from './pulse-registry.js';
import type { PulseSourceDb } from './pulse-sql.js';
import { RealtimeService, SubscriptionManager } from './realtime-store.js';
import { WalEventEmitter } from './wal-event-emitter.js';
import { createWalRowNormalizer } from './wal-normalization.js';

type RuntimeLifecycleListener = () => void;

// date/timestamp/timestamptz/interval/point: pg-types' defaults yield non-text JS values
// that Drizzle's from-text codecs cannot consume.
const RAW_TEXT_PG_OIDS = new Set([1082, 1114, 1184, 1186, 600]);

const rawText = (value: string): string => value;

// Identity for the raw-text OIDs, pg default otherwise — passed as the `types` option to
// pulse-owned Pools so their reads (events-table decode) match the WAL text-decode path.
const rawTextTypes = {
  getTypeParser: (oid: number, format?: unknown) =>
    RAW_TEXT_PG_OIDS.has(oid)
      ? rawText
      : (types.getTypeParser as (oid: number, format?: unknown) => unknown)(oid, format),
} as PoolConfig['types'];

// Keeps WAL values for the raw-text OIDs as text without mutating the process-global
// pg-types registry pg-logical-replication decodes through.
// ponytail: couples to pg-logical-replication internals (relation.columns[].parser); the
// runtime guard below fails loud if that shape changes on upgrade.
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
  pgPoolConfig: PoolConfig;
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

/**
 * Runtime log verbosity. `debug` adds per-WAL-event traces, `info` (default) adds
 * listener-lifecycle messages, `error` keeps only failures, `silent` disables all output.
 */
export type LogLevel = 'debug' | 'info' | 'error' | 'silent';

const LOG_LEVEL_RANK: Record<LogLevel, number> = { silent: 0, error: 1, info: 2, debug: 3 };

export type SubscriptionTtlConfig = {
  // How long a subscription may go without a pull() before the sweep evicts it:
  // subscriptions otherwise live forever, since nothing else ever calls
  // SubscriptionManager.delete() for a client that disconnects without unsubscribing.
  idleMs?: number;
  sweepIntervalMs?: number;
};

export type ExposeConfig = {
  databaseUrl: string;
  /**
   * The app's own drizzle connection; baseline and query reads run on it so they keep the
   * app's session context (RLS, search_path). A node-postgres-backed instance must
   * configure its pool `types` to deliver date/timestamp/timestamptz/interval/point as
   * raw text (postgres-js does so natively).
   */
  sourceDb: PulseSourceDb;
  eventsSchema?: string;
  wal?: ExposeWalConfig;
  subscriptionTtl?: SubscriptionTtlConfig;
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

const DEFAULT_WAL_NAME = 'drizzle_pulse';

const DEFAULT_SUBSCRIPTION_TTL: Required<SubscriptionTtlConfig> = {
  idleMs: 24 * 60 * 60 * 1000, // 24h — comfortably longer than any realistic client-side pull cadence
  sweepIntervalMs: 5 * 60 * 1000, // 5m
};

function withQuietPgOptions<T extends ClientConfig | PoolConfig>(config: T): T {
  return {
    ...config,
    options: [config.options, '-c client_min_messages=warning'].filter(Boolean).join(' '),
  };
}

export class RealtimeRuntime<TQueries extends AnyPulseBuilders> {
  private readonly sourceTableMetadata: Map<string, SourceTableMetadata>;
  private readonly subscriptionManager = new SubscriptionManager();
  private readonly requestHandler: RealtimeRequestHandler;
  private readonly pgConfig: ClientConfig & { replication: 'database' };
  private readonly pgPoolConfig: PoolConfig;
  private readonly reconnectConfig: Required<NonNullable<ExposeWalConfig['reconnect']>>;
  private readonly logLevel: LogLevel;
  private readonly subscriptionTtlConfig: Required<SubscriptionTtlConfig>;
  private subscriptionSweepTimer: ReturnType<typeof setInterval> | null = null;
  readonly publicationName: string;
  readonly slotName: string;
  private readonly eventsSchema: string;

  private pool: Pool | null = null;
  private realtimeService: RealtimeService | null = null;
  private replicationService: LogicalReplicationService | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  isRunning = false;
  private reconnectAttempts = 0;
  lastPersistedSnapshot = 0;
  readonly walEventEmitter = new WalEventEmitter();
  private everConnected = false;
  private readonly reconnectListeners = new Set<RuntimeLifecycleListener>();
  private readonly stopListeners = new Set<RuntimeLifecycleListener>();

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

  constructor(
    readonly registry: PulseRegistry<TQueries>,
    private readonly config: ExposeConfig,
  ) {
    const wal = this.config.wal ?? {};
    this.publicationName = wal.publicationName ?? DEFAULT_WAL_NAME;
    this.slotName = wal.slotName ?? DEFAULT_WAL_NAME;
    this.eventsSchema = this.config.eventsSchema ?? DEFAULT_EVENTS_SCHEMA;

    this.sourceTableMetadata = new Map();
    // The `_`->`__` escaping is not injective (see events-table-resolver.ts), so distinct
    // source tables can derive the same events-table name; reject that here, where the full
    // set of source tables is known, rather than let one table silently shadow another.
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
    this.pgPoolConfig = { ...withQuietPgOptions(pg), types: rawTextTypes };
    this.reconnectConfig = { ...DEFAULT_RECONNECT, ...wal.reconnect };
    this.logLevel = this.config.logLevel ?? 'info';
    this.subscriptionTtlConfig = {
      ...DEFAULT_SUBSCRIPTION_TTL,
      ...this.config.subscriptionTtl,
    };

    this.requestHandler = new RealtimeRequestHandler(
      this.registry,
      this.config.sourceDb,
      this.subscriptionManager,
      () => this.getRealtimeService(),
      (queryName: string) => this.getEventsTableForQuery(queryName),
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

  async ensureBaselines(): Promise<void> {
    const currentRealtimeService = this.getRealtimeService();
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

      await currentRealtimeService.createBaselineSnapshot(
        this.getEventsTableForQuery(queryName),
        pulseQuery.pkColumn.name,
        baselineRow ?? null,
      );
    }
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      this.logInfo('[WAL Listener] Already running');
      return;
    }

    this.initializeDatabaseServices();

    try {
      await this.runStartupGuard();

      this.isRunning = true;
      await this.ensureBaselines();

      let maxSnapshot = 0;
      const service = this.getRealtimeService();
      for (const meta of this.sourceTableMetadata.values()) {
        const snap = await service.getLatestSnapshot(meta.eventsTable);
        maxSnapshot = Math.max(maxSnapshot, snap);
      }
      this.lastPersistedSnapshot = maxSnapshot;

      await this.connectReplication();
      this.startSubscriptionSweep();
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
    this.stopSubscriptionSweep();

    const replicationService = this.replicationService;
    this.replicationService = null;
    if (replicationService) {
      await replicationService.stop();
    }

    const pool = this.pool;
    this.pool = null;
    this.realtimeService = null;

    if (pool) {
      await pool.end();
    }

    this.logInfo('[WAL Listener] Stopped');
  }

  private initializeDatabaseServices(): void {
    if (this.pool && this.realtimeService) {
      return;
    }

    const pool = new Pool(this.pgPoolConfig);
    this.pool = pool;
    this.realtimeService = new RealtimeService(pool);
  }

  // Mirrors stop()'s pool/service teardown so a failed guard doesn't leak connections —
  // callers await start() rejections and then discard the runtime.
  private async teardownFailedStart(): Promise<void> {
    this.stopSubscriptionSweep();

    const pool = this.pool;
    this.pool = null;
    this.realtimeService = null;

    if (pool) {
      await pool.end();
    }
  }

  // Fail-closed startup guard: aggregates every unmet precondition into a
  // single thrown Error instead of silently starting a runtime that will emit zero events.
  private async runStartupGuard(): Promise<void> {
    const adminDb = this.getRealtimeService().getDb();
    const failures: string[] = [];

    // Publication exists, and (unless it's FOR ALL TABLES) every source table is a member.
    const publicationResult = await adminDb.execute<{ puballtables: boolean }>(
      sql`SELECT puballtables FROM pg_publication WHERE pubname = ${this.publicationName}`,
    );
    const publicationRow = publicationResult.rows[0];

    if (!publicationRow) {
      failures.push(
        `publication "${this.publicationName}" does not exist — create it (e.g. CREATE PUBLICATION ${this.publicationName} FOR ALL TABLES) before starting`,
      );
    } else if (!publicationRow.puballtables) {
      const membershipResult = await adminDb.execute<{ schemaname: string; tablename: string }>(
        sql`SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ${this.publicationName}`,
      );
      const members = new Set(
        membershipResult.rows.map((row) => `${row.schemaname}.${row.tablename}`),
      );

      for (const meta of this.sourceTableMetadata.values()) {
        const sourceConfig = getTableConfig(meta.sourceTable);
        const qualifiedName = `${sourceConfig.schema ?? 'public'}.${sourceConfig.name}`;
        if (!members.has(qualifiedName)) {
          failures.push(
            `table "${qualifiedName}" is not a member of publication "${this.publicationName}"`,
          );
        }
      }
    }

    // Each source table's events table has been created (migrations/codegen ran).
    for (const meta of this.sourceTableMetadata.values()) {
      const eventsConfig = getTableConfig(meta.eventsTable);
      const eventsSchemaName = eventsConfig.schema ?? this.eventsSchema;
      const existsResult = await adminDb.execute<{ relname: string }>(
        sql`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ${eventsSchemaName} AND c.relname = ${eventsConfig.name} AND c.relkind IN ('r', 'p')`,
      );
      if (existsResult.rows.length === 0) {
        failures.push(
          `events table "${eventsSchemaName}.${eventsConfig.name}" does not exist — run migrations/codegen to create it`,
        );
      }
    }

    // Each source table has REPLICA IDENTITY FULL (so old-row data reaches the WAL).
    for (const meta of this.sourceTableMetadata.values()) {
      const sourceConfig = getTableConfig(meta.sourceTable);
      const sourceSchemaName = sourceConfig.schema ?? 'public';
      const replicaIdentityResult = await adminDb.execute<{ relreplident: string }>(
        sql`SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ${sourceSchemaName} AND c.relname = ${sourceConfig.name}`,
      );
      const replicaIdentityRow = replicaIdentityResult.rows[0];
      if (!replicaIdentityRow || replicaIdentityRow.relreplident !== 'f') {
        failures.push(
          `table "${sourceSchemaName}.${sourceConfig.name}" does not have REPLICA IDENTITY FULL — run ALTER TABLE ${sourceSchemaName}.${sourceConfig.name} REPLICA IDENTITY FULL`,
        );
      }
    }

    // wal_level=logical (logical replication is enabled server-wide).
    const walLevelResult = await adminDb.execute<{ wal_level: string }>(
      sql`SELECT current_setting('wal_level') AS wal_level`,
    );
    const walLevel = walLevelResult.rows[0]?.wal_level;
    if (walLevel !== 'logical') {
      failures.push(`wal_level is "${walLevel ?? 'unknown'}", but must be "logical"`);
    }

    if (failures.length > 0) {
      throw new Error(
        ['expose() startup guard failed:', ...failures.map((failure) => `- ${failure}`)].join('\n'),
      );
    }
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
      const adminDb = this.getRealtimeService().getDb();

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
      this.isRunning = false;
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

  // Without this sweep, SubscriptionManager.delete() is only ever reached via the
  // explicit unsubscribe() handler — a client that disconnects without calling it (closed
  // tab, crashed process, ...) would otherwise leak its subscription forever.
  private startSubscriptionSweep(): void {
    if (this.subscriptionSweepTimer) {
      return;
    }

    this.subscriptionSweepTimer = setInterval(() => {
      this.subscriptionManager.sweepIdle(this.subscriptionTtlConfig.idleMs);
    }, this.subscriptionTtlConfig.sweepIntervalMs);
    this.subscriptionSweepTimer.unref?.();
  }

  private stopSubscriptionSweep(): void {
    if (!this.subscriptionSweepTimer) {
      return;
    }

    clearInterval(this.subscriptionSweepTimer);
    this.subscriptionSweepTimer = null;
  }

  private async handleMessage(lsn: string, log: Pgoutput.Message): Promise<void> {
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

    this.logDebug(
      `[WAL Listener] ${event.operation.toUpperCase()} on ${event.tableQualifiedName}:`,
      {
        lsn: event.lsn,
        row_data: event.rowData,
        old_row_data: event.oldRowData,
      },
    );

    await this.persistWalEvent(event, metadata);
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

  private async persistWalEvent(event: WalEvent, metadata: SourceTableMetadata): Promise<void> {
    const currentRealtimeService = this.getRealtimeService();
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
    const normalizedRowData = metadata.normalizeRow(event.rowData);
    const normalizedOldRowData = event.oldRowData ? metadata.normalizeRow(event.oldRowData) : null;

    let snapshot: number;
    if (event.operation === 'insert') {
      snapshot = await currentRealtimeService.persistInsertEvent(
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

      snapshot = await currentRealtimeService.persistUpdateEvent(
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

      snapshot = await currentRealtimeService.persistDeleteEvent(
        metadata.eventsTable,
        metadata.pkColumnName,
        pkValue,
        normalizedOldRowData,
      );
    }

    this.lastPersistedSnapshot = Math.max(this.lastPersistedSnapshot, snapshot);
    this.walEventEmitter.emit(
      event.tableQualifiedName,
      event.operation,
      normalizedRowData,
      normalizedOldRowData,
      snapshot,
    );

    this.logDebug(
      `[WAL Listener] Persisted ${event.operation} event on ${event.tableQualifiedName} (pk=${pkValue})`,
    );
  }

  private getRealtimeService(): RealtimeService {
    if (!this.realtimeService) {
      throw new Error('RealtimeService has not been initialized');
    }

    return this.realtimeService;
  }

  private logInfo(message: string, ...args: unknown[]): void {
    if (LOG_LEVEL_RANK[this.logLevel] >= LOG_LEVEL_RANK.info) console.log(message, ...args);
  }

  private logError(message: string, ...args: unknown[]): void {
    if (LOG_LEVEL_RANK[this.logLevel] >= LOG_LEVEL_RANK.error) console.error(message, ...args);
  }

  private logDebug(message: string, ...args: unknown[]): void {
    if (LOG_LEVEL_RANK[this.logLevel] >= LOG_LEVEL_RANK.debug) console.log(message, ...args);
  }
}

export function expose<TQueries extends AnyPulseBuilders>(
  registry: PulseRegistry<TQueries>,
  config: ExposeConfig,
) {
  return new RealtimeRuntime(registry, config);
}
