import { desc, getTableUniqueName, sql } from 'drizzle-orm';
import type { PgTable } from 'drizzle-orm/pg-core';
import { type ClientConfig, Pool, type PoolConfig, types } from 'pg';
import { LogicalReplicationService, type Pgoutput, PgoutputPlugin } from 'pg-logical-replication';
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

export type ExposeConfig = {
  databaseUrl: string;
  sourceDb: PulseSourceDb;
  wal: Pick<WalListenerConfig, 'publicationName' | 'slotName' | 'reconnect' | 'logging'>;
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
  private readonly reconnectConfig: Required<NonNullable<ExposeConfig['wal']['reconnect']>>;
  private readonly loggingConfig: Required<WalLoggingConfig>;

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

      const eventsTable = this._registry.getEventsTable(queryName);
      if (!eventsTable) {
        continue;
      }

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
    this.reconnectConfig = { ...DEFAULT_RECONNECT, ...this.config.wal.reconnect };
    this.loggingConfig = { ...DEFAULT_LOGGING, ...this.config.wal.logging };

    this.requestHandler = new RealtimeRequestHandler(
      this._registry,
      this.getSourceDb(),
      this.subscriptionManager,
      () => this.getRealtimeService(),
    );
  }

  get handlers() {
    return this.requestHandler;
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
        pulseQuery.eventsTable,
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
        publicationNames: [this.config.wal.publicationName],
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

      const slotName = this.config.wal.slotName;
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

      this.log(`[WAL Listener] Subscribing to slot '${this.config.wal.slotName}'`);
      const streaming = replicationService.subscribe(plugin, this.config.wal.slotName);
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
