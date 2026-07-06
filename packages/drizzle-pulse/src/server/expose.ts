import { desc, getTableUniqueName, sql } from 'drizzle-orm';
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core';
import { type ClientConfig, Pool, type PoolConfig, types } from 'pg';
import { LogicalReplicationService, type Pgoutput, PgoutputPlugin } from 'pg-logical-replication';
import { DEFAULT_EVENTS_SCHEMA, resolveEventsTable } from './events-table-resolver.js';
import { RealtimeRequestHandler } from './handlers.js';
import type { AnyPulseBuilders, PulseRegistry } from './pulse-registry.js';
import type { PulseSourceDb } from './pulse-sql.js';
import { RealtimeService, SubscriptionManager } from './realtime-store.js';
import { WalEventEmitter } from './wal-event-emitter.js';
import { createWalRowNormalizer } from './wal-normalization.js';

type RuntimeLifecycleListener = () => void;

// OIDs forced to raw PG text so the WAL normalizer can feed Drizzle's from-text codecs:
// date/timestamp/timestamptz/interval, plus point (600) — the one geometric type pg-types
// parses into an object ({x,y}), which can't feed the point:tuple codec. Drizzle's own
// SELECTs cast these to text regardless, so this only shapes the replication decode path.
const RAW_TEXT_PG_OIDS = [1082, 1114, 1184, 1186, 600] as const;

let rawTextParsersConfigured = false;

export interface WalListenerConfig {
  pgConfig: ClientConfig & { replication: 'database' };
  pgPoolConfig: PoolConfig;
  publicationName: string;
  slotName: string;
  logging?: WalLoggingConfig;
  reconnect?: {
    maxRetries?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
  };
}

export interface WalLoggingConfig {
  events?: boolean;
}

export type ExposeWalConfig = Partial<
  Pick<WalListenerConfig, 'publicationName' | 'slotName' | 'reconnect' | 'logging'>
>;

export type ExposeConfig = {
  databaseUrl: string;
  sourceDb: PulseSourceDb;
  eventsSchema?: string;
  wal?: ExposeWalConfig;
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

const DEFAULT_LOGGING: Required<WalLoggingConfig> = {
  events: true,
};

const DEFAULT_WAL_NAME = 'drizzle_pulse';

function parseDatabaseUrl(databaseUrl: string) {
  const url = new URL(databaseUrl);

  return {
    host: url.hostname,
    port: parseInt(url.port || '5432', 10),
    database: url.pathname.slice(1) || 'postgres',
    user: url.username || 'postgres',
    password: url.password || 'postgres',
  };
}

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
  private readonly loggingConfig: Required<WalLoggingConfig>;
  // Public (not private): the integration-test harness augments a RealtimeRuntime
  // instance with its own `{ publicationName, slotName }` via Object.assign for test
  // introspection (see packages/integration-tests/src/helpers/test-harness.ts). A
  // private field of the same name makes that intersection type collapse to `never`
  // (TS treats private members as nominal), so these two stay public readonly.
  readonly publicationName: string;
  readonly slotName: string;
  private readonly eventsSchema: string;

  private pool: Pool | null = null;
  private realtimeService: RealtimeService | null = null;
  private replicationService: LogicalReplicationService | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private _isRunning = false;
  private reconnectAttempts = 0;
  private _lastPersistedSnapshot = 0;
  private readonly _walEventEmitter = new WalEventEmitter();
  private everConnected = false;
  private readonly reconnectListeners = new Set<RuntimeLifecycleListener>();
  private readonly stopListeners = new Set<RuntimeLifecycleListener>();

  get walEventEmitter(): WalEventEmitter {
    return this._walEventEmitter;
  }

  get lastPersistedSnapshot(): number {
    return this._lastPersistedSnapshot;
  }

  get registry(): PulseRegistry<TQueries> {
    return this._registry;
  }

  get sourceDb(): PulseSourceDb {
    return this.getSourceDb();
  }

  get isRunning(): boolean {
    return this._isRunning;
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
    private readonly _registry: PulseRegistry<TQueries>,
    private readonly config: ExposeConfig,
  ) {
    const wal = this.config.wal ?? {};
    this.publicationName = wal.publicationName ?? DEFAULT_WAL_NAME;
    this.slotName = wal.slotName ?? DEFAULT_WAL_NAME;
    this.eventsSchema = this.config.eventsSchema ?? DEFAULT_EVENTS_SCHEMA;

    this.sourceTableMetadata = new Map();
    for (const queryName of this._registry.getQueryNames()) {
      const pulseQuery = this._registry.getPulseQuery(queryName);
      const sourceTable = this._registry.getSourceTable(queryName);
      if (
        !pulseQuery ||
        !sourceTable ||
        this.sourceTableMetadata.has(getTableUniqueName(pulseQuery.table))
      ) {
        continue;
      }

      const eventsTable = resolveEventsTable(sourceTable, { eventsSchema: this.eventsSchema });

      this.sourceTableMetadata.set(getTableUniqueName(pulseQuery.table), {
        sourceTable,
        pkColumnName: pulseQuery.pkColumn.name,
        eventsTable,
        normalizeRow: createWalRowNormalizer(sourceTable),
      });
    }

    const pg = parseDatabaseUrl(this.config.databaseUrl);

    this.pgConfig = {
      ...withQuietPgOptions(pg),
      replication: 'database',
    };
    this.pgPoolConfig = withQuietPgOptions(pg);
    this.reconnectConfig = { ...DEFAULT_RECONNECT, ...wal.reconnect };
    this.loggingConfig = { ...DEFAULT_LOGGING, ...wal.logging };

    this.requestHandler = new RealtimeRequestHandler(
      this._registry,
      this.getSourceDb(),
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
    const sourceTable = this._registry.getSourceTable(queryName);
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
    const sourceDb = this.getSourceDb();

    for (const queryName of this._registry.getQueryNames()) {
      const pulseQuery = this._registry.getPulseQuery(queryName);
      const sourceTable = this._registry.getSourceTable(queryName);
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
    if (!rawTextParsersConfigured) {
      for (const oid of RAW_TEXT_PG_OIDS) {
        // @types/pg's TypeId union omits some builtin OIDs (e.g. point=600); the
        // runtime accepts any numeric OID.
        types.setTypeParser(oid as Parameters<typeof types.setTypeParser>[0], (value) => value);
      }

      rawTextParsersConfigured = true;
    }

    if (this._isRunning) {
      this.log('[WAL Listener] Already running');
      return;
    }

    this.initializeDatabaseServices();

    try {
      await this.runStartupGuard();
    } catch (error) {
      await this.teardownFailedStart();
      throw error;
    }

    this._isRunning = true;
    await this.ensureBaselines();

    let maxSnapshot = 0;
    const service = this.getRealtimeService();
    for (const meta of this.sourceTableMetadata.values()) {
      const snap = await service.getLatestSnapshot(meta.eventsTable);
      maxSnapshot = Math.max(maxSnapshot, snap);
    }
    this._lastPersistedSnapshot = maxSnapshot;

    await this.connectReplication();
  }

  async stop(): Promise<void> {
    for (const listener of [...this.stopListeners]) {
      listener();
    }

    this._isRunning = false;
    this.clearReconnectTimer();

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

    this.log('[WAL Listener] Stopped');
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
  // callers await start() rejections and then discard the runtime (D-12).
  private async teardownFailedStart(): Promise<void> {
    const pool = this.pool;
    this.pool = null;
    this.realtimeService = null;

    if (pool) {
      await pool.end();
    }
  }

  // Fail-closed startup guard (D-12/API-04): aggregates every unmet precondition into a
  // single thrown Error instead of silently starting a runtime that will emit zero events.
  private async runStartupGuard(): Promise<void> {
    const adminDb = this.getRealtimeService().getDb();
    const failures: string[] = [];

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

    for (const meta of this.sourceTableMetadata.values()) {
      const eventsConfig = getTableConfig(meta.eventsTable);
      const eventsSchemaName = eventsConfig.schema ?? this.eventsSchema;
      const existsResult = await adminDb.execute<{ relname: string }>(
        sql`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ${eventsSchemaName} AND c.relname = ${eventsConfig.name}`,
      );
      if (existsResult.rows.length === 0) {
        failures.push(
          `events table "${eventsSchemaName}.${eventsConfig.name}" does not exist — run migrations/codegen to create it`,
        );
      }
    }

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
    if (!this._isRunning) return;
    this.log('[WAL Listener] Replication started');
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

      const plugin = new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: [this.publicationName],
      });

      replicationService.on('data', async (lsn: string, log: Pgoutput.Message) => {
        try {
          await this.handleMessage(lsn, log);
          if (this.replicationService === replicationService) {
            await replicationService.acknowledge(lsn);
          }
        } catch (error) {
          this.error('[WAL Listener] Error processing message:', error);
        }
      });

      replicationService.on('error', async (error: Error) => {
        if (this.replicationService !== replicationService) {
          return;
        }

        this.error('[WAL Listener] Replication error:', error);
        await this.handleDisconnect(replicationService);
      });

      replicationService.on('start', () => {
        this.onReplicationStart();
      });

      replicationService.on('acknowledge', (lsn: string) => {
        this.log(`[WAL Listener] Acknowledged LSN: ${lsn}`);
      });

      replicationService.on(
        'heartbeat',
        (lsn: string, timestamp: number, shouldRespond: boolean) => {
          if (shouldRespond) {
            this.log(`[WAL Listener] Heartbeat at LSN: ${lsn}, timestamp: ${timestamp}`);
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
        this.log(`[WAL Listener] Creating replication slot '${slotName}'`);
        await adminDb.execute(
          sql`SELECT pg_create_logical_replication_slot(${slotName}, ${'pgoutput'})`,
        );
      } else {
        const slot = result.rows[0];
        if (slot?.active && slot.active_pid) {
          this.log(
            `[WAL Listener] Terminating stale connection on slot '${slotName}' (PID ${slot.active_pid})`,
          );
          await adminDb.execute(sql`SELECT pg_terminate_backend(${slot.active_pid})`);
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        this.log(`[WAL Listener] Replication slot '${slotName}' ready`);
      }

      this.log(`[WAL Listener] Subscribing to slot '${this.slotName}'`);
      const streaming = replicationService.subscribe(plugin, this.slotName);
      await Promise.race([
        new Promise((resolve) => replicationService.once('start', resolve)),
        streaming, // settles first only on immediate failure → propagates the error
      ]);
      void streaming.catch(() => {}); // stream-end errors already handled via the 'error' listener
    } catch (error) {
      this.error('[WAL Listener] Connection error:', error);
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
        this.error('[WAL Listener] Error during stop:', error);
      }
    }

    if (!this._isRunning) {
      return;
    }

    this.clearReconnectTimer();

    if (this.reconnectAttempts >= this.reconnectConfig.maxRetries) {
      this.error('[WAL Listener] Max reconnection attempts reached. Giving up.');
      this._isRunning = false;
      return;
    }

    const exponentialDelay = this.reconnectConfig.baseDelayMs * 2 ** this.reconnectAttempts;
    const jitter = Math.random() * 1000;
    const delay = Math.min(exponentialDelay + jitter, this.reconnectConfig.maxDelayMs);

    this.reconnectAttempts += 1;
    this.log(
      `[WAL Listener] Reconnecting in ${Math.round(delay)}ms (attempt ${this.reconnectAttempts}/${this.reconnectConfig.maxRetries})`,
    );

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this._isRunning) {
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

    this.logEvent(
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
      this.logEvent(
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
        this.logEvent(
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
        this.logEvent(
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

    this._lastPersistedSnapshot = Math.max(this._lastPersistedSnapshot, snapshot);
    this._walEventEmitter.emit(
      event.tableQualifiedName,
      event.operation,
      normalizedRowData,
      normalizedOldRowData,
      snapshot,
    );

    this.logEvent(
      `[WAL Listener] Persisted ${event.operation} event on ${event.tableQualifiedName} (pk=${pkValue})`,
    );
  }

  private getRealtimeService(): RealtimeService {
    if (!this.realtimeService) {
      throw new Error('RealtimeService has not been initialized');
    }

    return this.realtimeService;
  }

  private getSourceDb(): PulseSourceDb {
    if (!this.config.sourceDb) {
      throw new Error('source db has not been provided');
    }

    return this.config.sourceDb;
  }

  private log(message: string, ...args: unknown[]): void {
    console.log(message, ...args);
  }

  private error(message: string, ...args: unknown[]): void {
    console.error(message, ...args);
  }

  private logEvent(message: string, ...args: unknown[]): void {
    if (!this.loggingConfig.events) {
      return;
    }

    this.log(message, ...args);
  }
}

export function expose<TQueries extends AnyPulseBuilders>(
  registry: PulseRegistry<TQueries>,
  config: ExposeConfig,
) {
  return new RealtimeRuntime(registry, config);
}
