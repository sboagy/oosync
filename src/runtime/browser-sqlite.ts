import { drizzle } from "drizzle-orm/sql-js";
import type { Database as SqlJsDatabase, SqlJsStatic } from "sql.js";
import initSqlJs from "sql.js";
import type {
  IOutboxBackup,
  IOutboxBackupItem,
  SqliteRawDatabase,
  SyncPushQueueTable,
  SyncRuntime,
  SyncSchemaDescription,
} from "../sync/runtime-context";
import type { ILogger } from "./platform/types";

export type { IOutboxBackup, SqliteRawDatabase } from "../sync/runtime-context";

export type BrowserSqliteDatabase = ReturnType<typeof drizzle>;

export interface IBrowserSqliteDebugInfo {
  hasModule: boolean;
  wasmHeapBytes?: number;
}

export interface IClientSqliteDebugState {
  initEpoch: number;
  isClearing: boolean;
  isInitializingDb: boolean;
  dbReady: boolean;
  hasSqliteDb: boolean;
  hasDrizzleDb: boolean;
  currentUser?: string | null;
}

export interface IBrowserForceResetQueryParam {
  key: string;
  value?: string;
}

export interface IBrowserSqliteStorageConfig {
  indexedDbName: string;
  indexedDbStore: string;
  dbKeyPrefix: string;
  dbVersionKeyPrefix: string;
  outboxBackupKeyPrefix: string;
  lastSyncTimestampKeyPrefix: string;
  schemaVersionStorageKey?: string;
}

export interface IBrowserSqliteTestConfig {
  testApiWindowProperty?: string;
  clearInProgressWindowProperty?: string;
  persistWindowHookProperty?: string;
}

export interface IBrowserSqliteHookContext {
  phase: "loaded" | "created";
  rawDb: SqlJsDatabase;
  userId: string;
}

export interface IBrowserSqliteHooks {
  logger: ILogger;
  onExistingDatabaseLoaded?: (
    db: BrowserSqliteDatabase,
    context: IBrowserSqliteHookContext
  ) => Promise<void> | void;
  onDatabaseReady?: (
    db: BrowserSqliteDatabase,
    context: IBrowserSqliteHookContext
  ) => Promise<void> | void;
  clearLocalDataForMigration?: (
    db: BrowserSqliteDatabase,
    context: { userId: string }
  ) => Promise<void> | void;
}

export interface IBrowserSqliteClientConfig<
  Schema extends Record<string, unknown>,
> {
  schema: Schema;
  syncSchema: SyncSchemaDescription;
  syncPushQueue: SyncPushQueueTable;
  sqlWasmUrl: string;
  storage: IBrowserSqliteStorageConfig;
  databaseVersion: number;
  schemaVersion: string;
  migrationFiles: string[];
  hooks: IBrowserSqliteHooks;
  forceResetQueryParams?: IBrowserForceResetQueryParam[];
  diagnosticsEnabled?: boolean;
  testConfig?: IBrowserSqliteTestConfig;
}

export interface IBrowserSqliteClient<Schema extends Record<string, unknown>> {
  schema: Schema;
  logger: ILogger;
  initializeDb: (userId: string) => Promise<BrowserSqliteDatabase>;
  getDb: () => BrowserSqliteDatabase;
  persistDb: () => Promise<void>;
  closeDb: () => Promise<void>;
  clearDb: () => Promise<void>;
  setupAutoPersist: () => () => void;
  getSqliteInstance: () => Promise<SqliteRawDatabase | null>;
  getSqlJsDebugInfo: () => IBrowserSqliteDebugInfo;
  getDebugState: () => IClientSqliteDebugState;
  loadOutboxBackupForUser: (userId: string) => Promise<IOutboxBackup | null>;
  clearOutboxBackupForUser: (userId: string) => Promise<void>;
  replayOutboxBackup: (
    db: SqliteRawDatabase,
    backup: IOutboxBackup
  ) => { applied: number; skipped: number; errors: string[] };
  suppressSyncTriggers: (db: SqliteRawDatabase) => void;
  enableSyncTriggers: (db: SqliteRawDatabase) => void;
  syncPushQueue: SyncPushQueueTable;
}

interface ITableTriggerConfig {
  tableName: string;
  primaryKey: string | string[];
  supportsIncremental: boolean;
}

type SqlValue = string | number | null | Uint8Array;

function getWindowRecord(): Record<string, unknown> | null {
  if (typeof window === "undefined") return null;
  return window as unknown as Record<string, unknown>;
}

function hasWindowValue(name: string | undefined): boolean {
  if (!name) return false;
  const win = getWindowRecord();
  return Boolean(win?.[name]);
}

function hasWindowFlag(name: string | undefined): boolean {
  if (!name) return false;
  const win = getWindowRecord();
  return win?.[name] === true;
}

function isTestMode(config: IBrowserSqliteTestConfig | undefined): boolean {
  return hasWindowValue(config?.testApiWindowProperty);
}

function isExternalClearInProgress(
  config: IBrowserSqliteTestConfig | undefined
): boolean {
  return hasWindowFlag(config?.clearInProgressWindowProperty);
}

function registerPersistHook(
  config: IBrowserSqliteTestConfig | undefined,
  callback: () => Promise<void> | void,
  logger: ILogger
): void {
  const hookName = config?.persistWindowHookProperty;
  const win = getWindowRecord();
  if (!hookName || !win || hookName in win) return;
  win[hookName] = () => {
    try {
      return callback();
    } catch (error) {
      logger.warn(`${hookName} failed`, error);
      return undefined;
    }
  };
}

function getSchemaVersionStorageKey(
  config: IBrowserSqliteStorageConfig
): string {
  return config.schemaVersionStorageKey ?? "schema_version";
}

function getWindowSearchParams(): URLSearchParams | null {
  if (typeof window === "undefined") return null;
  return new URLSearchParams(window.location.search);
}

function matchesForceResetParam(
  searchParams: URLSearchParams,
  param: IBrowserForceResetQueryParam
): boolean {
  const value = searchParams.get(param.key);
  if (value === null) return false;
  if (typeof param.value === "undefined") return true;
  return value === param.value;
}

function needsMigration(params: {
  schemaVersion: string;
  storage: IBrowserSqliteStorageConfig;
  forceResetQueryParams: IBrowserForceResetQueryParam[];
  logger: ILogger;
}): boolean {
  if (typeof window === "undefined") return false;

  const searchParams = getWindowSearchParams();
  if (searchParams) {
    const forced = params.forceResetQueryParams.some((entry) =>
      matchesForceResetParam(searchParams, entry)
    );
    if (forced) {
      params.logger.warn("Migration forced via URL parameter");
      return true;
    }
  }

  const localVersion = window.localStorage.getItem(
    getSchemaVersionStorageKey(params.storage)
  );
  if (localVersion === null) return false;

  const shouldMigrate = localVersion !== params.schemaVersion;
  if (shouldMigrate) {
    params.logger.warn(
      `Schema version mismatch: local=${localVersion}, current=${params.schemaVersion}`
    );
  }
  return shouldMigrate;
}

function isForcedReset(params: {
  forceResetQueryParams: IBrowserForceResetQueryParam[];
}): boolean {
  const searchParams = getWindowSearchParams();
  if (!searchParams) return false;
  return params.forceResetQueryParams.some((entry) =>
    matchesForceResetParam(searchParams, entry)
  );
}

function clearMigrationParams(
  forceResetQueryParams: IBrowserForceResetQueryParam[],
  logger: ILogger
): void {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  let changed = false;
  for (const entry of forceResetQueryParams) {
    if (url.searchParams.has(entry.key)) {
      url.searchParams.delete(entry.key);
      changed = true;
    }
  }
  if (!changed) return;
  window.history.replaceState({}, "", url.toString());
  logger.info("Migration URL parameters cleared");
}

function setLocalSchemaVersion(
  storage: IBrowserSqliteStorageConfig,
  version: string
): void {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(getSchemaVersionStorageKey(storage), version);
}

function getLocalSchemaVersion(
  storage: IBrowserSqliteStorageConfig
): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(getSchemaVersionStorageKey(storage));
}

function clearLastSyncTimestampForUser(
  storage: IBrowserSqliteStorageConfig,
  userId: string,
  logger: ILogger
): void {
  try {
    if (typeof localStorage === "undefined") return;
    localStorage.removeItem(`${storage.lastSyncTimestampKeyPrefix}_${userId}`);
  } catch (error) {
    logger.warn("Failed to clear last sync timestamp", error);
  }
}

function encodeJsonToBytes(value: unknown): Uint8Array {
  const encoder = new TextEncoder();
  return encoder.encode(JSON.stringify(value));
}

function decodeJsonFromBytes(data: Uint8Array): unknown {
  const decoder = new TextDecoder();
  return JSON.parse(decoder.decode(data));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function toSqlValue(value: unknown): SqlValue {
  if (value === null || typeof value === "undefined") return null;
  if (typeof value === "string") return value;
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (value instanceof Uint8Array) return value;

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function parseRowIdToPkObject(
  syncSchema: SyncSchemaDescription,
  tableName: string,
  rowId: string
): Record<string, unknown> {
  if (rowId.startsWith("{")) {
    const parsed = JSON.parse(rowId) as unknown;
    if (!isRecord(parsed)) {
      throw new Error(`Invalid composite rowId JSON for ${tableName}`);
    }
    return parsed;
  }

  const primaryKeyValue = syncSchema.tableRegistry[tableName]?.primaryKey;
  if (!primaryKeyValue) {
    throw new Error(`Unknown table meta for ${tableName}`);
  }

  if (typeof primaryKeyValue !== "string") {
    throw new Error(
      `Expected JSON rowId for composite PK table ${tableName}, got string rowId`
    );
  }

  const primaryKeyColumn = primaryKeyValue;
  return { [primaryKeyColumn]: rowId };
}

function getExistingColumns(db: SqlJsDatabase, tableName: string): Set<string> {
  const stmt = db.prepare(`PRAGMA table_info("${tableName}")`);
  const columns = new Set<string>();
  try {
    while (stmt.step()) {
      const row = stmt.getAsObject() as Record<string, unknown>;
      const name = row.name;
      if (typeof name === "string") {
        columns.add(name);
      }
    }
  } finally {
    stmt.free();
  }
  return columns;
}

function selectRowByPk(
  db: SqlJsDatabase,
  tableName: string,
  pk: Record<string, unknown>
): Record<string, unknown> | null {
  const keys = Object.keys(pk);
  if (keys.length === 0) return null;
  const where = keys.map((key) => `"${key}" = ?`).join(" AND ");
  const stmt = db.prepare(
    `SELECT * FROM "${tableName}" WHERE ${where} LIMIT 1`
  );
  stmt.bind(keys.map((key) => toSqlValue(pk[key])));
  try {
    if (!stmt.step()) return null;
    return stmt.getAsObject() as Record<string, unknown>;
  } finally {
    stmt.free();
  }
}

function filterRowDataToExistingColumns(
  rowData: Record<string, unknown>,
  existingColumns: Set<string>
): Record<string, unknown> {
  const filtered: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(rowData)) {
    if (existingColumns.has(key)) {
      filtered[key] = value;
    }
  }
  return filtered;
}

function createOutboxBackup(
  db: SqlJsDatabase,
  syncSchema: SyncSchemaDescription
): IOutboxBackup {
  const createdAt = new Date().toISOString();
  const items: IOutboxBackupItem[] = [];
  const stmt = db.prepare(`
    SELECT table_name, row_id, operation, changed_at
    FROM sync_push_queue
    WHERE status IN ('pending', 'failed', 'in_progress')
    ORDER BY changed_at ASC
  `);

  try {
    while (stmt.step()) {
      const row = stmt.getAsObject() as Record<string, unknown>;
      const tableName = row.table_name;
      const rowId = row.row_id;
      const operation = row.operation;
      const changedAt = row.changed_at;

      if (
        typeof tableName !== "string" ||
        typeof rowId !== "string" ||
        typeof operation !== "string" ||
        typeof changedAt !== "string"
      ) {
        continue;
      }

      if (!(tableName in syncSchema.tableRegistry)) {
        continue;
      }

      const item: IOutboxBackupItem = {
        tableName,
        rowId,
        operation,
        changedAt,
      };

      if (operation.toLowerCase() !== "delete") {
        try {
          const pk = parseRowIdToPkObject(syncSchema, tableName, rowId);
          const rowData = selectRowByPk(db, tableName, pk);
          if (rowData) {
            item.rowData = rowData;
          }
        } catch {
          // Ignore snapshot failures and preserve queue metadata only.
        }
      }

      items.push(item);
    }
  } finally {
    stmt.free();
  }

  return {
    version: 1,
    createdAt,
    items,
  };
}

function replayOutboxBackup(
  db: SqlJsDatabase,
  syncSchema: SyncSchemaDescription,
  backup: IOutboxBackup
): { applied: number; skipped: number; errors: string[] } {
  const errors: string[] = [];
  let applied = 0;
  let skipped = 0;

  for (const item of backup.items) {
    try {
      const tableName = item.tableName;
      if (!(tableName in syncSchema.tableRegistry)) {
        skipped += 1;
        continue;
      }

      const existingColumns = getExistingColumns(db, tableName);
      if (existingColumns.size === 0) {
        skipped += 1;
        continue;
      }

      const meta = syncSchema.tableRegistry[tableName];
      const pkCols = Array.isArray(meta.primaryKey)
        ? meta.primaryKey
        : [meta.primaryKey];

      if (item.operation.toLowerCase() === "delete") {
        const pk = parseRowIdToPkObject(syncSchema, tableName, item.rowId);
        const whereCols = pkCols.filter((column) => column in pk);
        if (whereCols.length === 0) {
          skipped += 1;
          continue;
        }

        const where = whereCols
          .map((column) => `"${column}" = ?`)
          .join(" AND ");
        const stmt = db.prepare(`DELETE FROM "${tableName}" WHERE ${where}`);
        stmt.bind(whereCols.map((column) => toSqlValue(pk[column])));
        try {
          stmt.step();
          applied += 1;
        } finally {
          stmt.free();
        }
        continue;
      }

      if (!item.rowData) {
        skipped += 1;
        continue;
      }

      const rowData = filterRowDataToExistingColumns(
        item.rowData,
        existingColumns
      );
      const columns = Object.keys(rowData);
      if (columns.length === 0) {
        skipped += 1;
        continue;
      }

      const columnList = columns.map((column) => `"${column}"`).join(", ");
      const placeholders = columns.map(() => "?").join(", ");
      const conflictColumns = pkCols
        .filter((column) => existingColumns.has(column))
        .map((column) => `"${column}"`)
        .join(", ");

      if (conflictColumns.length === 0) {
        skipped += 1;
        continue;
      }

      const updateColumns = columns
        .filter((column) => !pkCols.includes(column))
        .map((column) => `"${column}" = excluded."${column}"`)
        .join(", ");

      const sql =
        updateColumns.length > 0
          ? `INSERT INTO "${tableName}" (${columnList}) VALUES (${placeholders}) ON CONFLICT(${conflictColumns}) DO UPDATE SET ${updateColumns}`
          : `INSERT INTO "${tableName}" (${columnList}) VALUES (${placeholders}) ON CONFLICT(${conflictColumns}) DO NOTHING`;

      const stmt = db.prepare(sql);
      stmt.bind(columns.map((column) => toSqlValue(rowData[column])));
      try {
        stmt.step();
        applied += 1;
      } finally {
        stmt.free();
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`${item.tableName}:${item.rowId}: ${message}`);
      skipped += 1;
    }
  }

  return { applied, skipped, errors };
}

function getTriggerConfigs(
  syncSchema: SyncSchemaDescription
): ITableTriggerConfig[] {
  return syncSchema.syncableTables.map((tableName) => {
    const primaryKeyValue = syncSchema.tableRegistry[tableName].primaryKey;
    const primaryKey: string | string[] =
      typeof primaryKeyValue === "string"
        ? primaryKeyValue
        : Array.from(primaryKeyValue);
    return {
      tableName,
      primaryKey,
      supportsIncremental:
        syncSchema.tableRegistry[tableName].supportsIncremental,
    };
  });
}

function createSyncTriggerControlTable(db: SqlJsDatabase): void {
  db.run(`
    CREATE TABLE IF NOT EXISTS sync_trigger_control (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      disabled INTEGER NOT NULL DEFAULT 0
    )
  `);
  db.run(`
    INSERT OR IGNORE INTO sync_trigger_control (id, disabled) VALUES (1, 0)
  `);
}

function createSyncPushQueueTable(db: SqlJsDatabase): void {
  db.run(`
    CREATE TABLE IF NOT EXISTS sync_push_queue (
      id TEXT PRIMARY KEY NOT NULL,
      table_name TEXT NOT NULL,
      row_id TEXT NOT NULL,
      operation TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      changed_at TEXT NOT NULL,
      synced_at TEXT,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT
    )
  `);
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_push_queue_status_changed ON sync_push_queue(status, changed_at)`
  );
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_push_queue_table_row ON sync_push_queue(table_name, row_id)`
  );
}

function generateRowIdExpression(
  primaryKey: string | string[],
  prefix: "NEW" | "OLD"
): string {
  if (typeof primaryKey === "string") {
    return `${prefix}.${primaryKey}`;
  }
  const parts = primaryKey
    .map((column) => `'${column}', ${prefix}.${column}`)
    .join(", ");
  return `json_object(${parts})`;
}

function generatePkWhereClause(
  primaryKey: string | string[],
  prefix: "NEW" | "OLD"
): string {
  if (typeof primaryKey === "string") {
    return `${primaryKey} = ${prefix}.${primaryKey}`;
  }
  return primaryKey
    .map((column) => `${column} = ${prefix}.${column}`)
    .join(" AND ");
}

function createTriggersForTable(
  db: SqlJsDatabase,
  config: ITableTriggerConfig
): void {
  const { tableName, primaryKey, supportsIncremental } = config;
  const newRowId = generateRowIdExpression(primaryKey, "NEW");
  const oldRowId = generateRowIdExpression(primaryKey, "OLD");

  db.run(`DROP TRIGGER IF EXISTS trg_${tableName}_insert`);
  db.run(`DROP TRIGGER IF EXISTS trg_${tableName}_update`);
  db.run(`DROP TRIGGER IF EXISTS trg_${tableName}_delete`);
  db.run(`DROP TRIGGER IF EXISTS trg_${tableName}_auto_modified`);

  if (supportsIncremental) {
    db.run(`
      CREATE TRIGGER trg_${tableName}_auto_modified
      AFTER UPDATE ON ${tableName}
      FOR EACH ROW
      WHEN NEW.last_modified_at = OLD.last_modified_at
        OR NEW.last_modified_at IS NULL
      BEGIN
        UPDATE ${tableName}
        SET last_modified_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE ${generatePkWhereClause(primaryKey, "NEW")};
      END
    `);
  }

  db.run(`
    CREATE TRIGGER trg_${tableName}_insert
    AFTER INSERT ON ${tableName}
    WHEN (SELECT disabled FROM sync_trigger_control WHERE id = 1) = 0
    BEGIN
      INSERT INTO sync_push_queue (id, table_name, row_id, operation, changed_at)
      VALUES (
        lower(hex(randomblob(16))),
        '${tableName}',
        ${newRowId},
        'INSERT',
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      );
    END
  `);

  db.run(`
    CREATE TRIGGER trg_${tableName}_update
    AFTER UPDATE ON ${tableName}
    WHEN (SELECT disabled FROM sync_trigger_control WHERE id = 1) = 0
    BEGIN
      INSERT INTO sync_push_queue (id, table_name, row_id, operation, changed_at)
      VALUES (
        lower(hex(randomblob(16))),
        '${tableName}',
        ${newRowId},
        'UPDATE',
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      );
    END
  `);

  db.run(`
    CREATE TRIGGER trg_${tableName}_delete
    AFTER DELETE ON ${tableName}
    WHEN (SELECT disabled FROM sync_trigger_control WHERE id = 1) = 0
    BEGIN
      INSERT INTO sync_push_queue (id, table_name, row_id, operation, changed_at)
      VALUES (
        lower(hex(randomblob(16))),
        '${tableName}',
        ${oldRowId},
        'DELETE',
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      );
    END
  `);
}

function installSyncTriggers(
  db: SqlJsDatabase,
  syncSchema: SyncSchemaDescription,
  logger: ILogger
): void {
  logger.debug("Installing sync push queue triggers...");
  createSyncTriggerControlTable(db);
  createSyncPushQueueTable(db);

  for (const config of getTriggerConfigs(syncSchema)) {
    createTriggersForTable(db, config);
  }
}

function suppressSyncTriggers(db: SqlJsDatabase): void {
  db.run(`UPDATE sync_trigger_control SET disabled = 1 WHERE id = 1`);
}

function enableSyncTriggers(db: SqlJsDatabase): void {
  db.run(`UPDATE sync_trigger_control SET disabled = 0 WHERE id = 1`);
}

async function saveToIndexedDB(
  indexedDbName: string,
  indexedDbStore: string,
  key: string,
  data: Uint8Array
): Promise<void> {
  return await new Promise((resolve, reject) => {
    const request = indexedDB.open(indexedDbName);

    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(indexedDbStore)) {
        db.createObjectStore(indexedDbStore);
      }
    };

    request.onsuccess = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(indexedDbStore)) {
        const currentVersion = db.version || 1;
        db.close();
        const upgradeRequest = indexedDB.open(
          indexedDbName,
          currentVersion + 1
        );
        upgradeRequest.onupgradeneeded = () => {
          const upgradeDb = upgradeRequest.result;
          if (!upgradeDb.objectStoreNames.contains(indexedDbStore)) {
            upgradeDb.createObjectStore(indexedDbStore);
          }
        };
        upgradeRequest.onsuccess = () => {
          const upgradeDb = upgradeRequest.result;
          const transaction = upgradeDb.transaction(
            indexedDbStore,
            "readwrite"
          );
          transaction.objectStore(indexedDbStore).put(data, key);
          transaction.oncomplete = () => {
            upgradeDb.close();
            resolve();
          };
          transaction.onerror = () => {
            upgradeDb.close();
            reject(transaction.error);
          };
        };
        upgradeRequest.onerror = () => reject(upgradeRequest.error);
        return;
      }

      const transaction = db.transaction(indexedDbStore, "readwrite");
      transaction.objectStore(indexedDbStore).put(data, key);
      transaction.oncomplete = () => {
        db.close();
        resolve();
      };
      transaction.onerror = () => {
        db.close();
        reject(transaction.error);
      };
    };

    request.onerror = () => reject(request.error);
  });
}

async function loadFromIndexedDB(
  indexedDbName: string,
  indexedDbStore: string,
  key: string
): Promise<Uint8Array | null> {
  return await new Promise((resolve, reject) => {
    const request = indexedDB.open(indexedDbName);

    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(indexedDbStore)) {
        db.createObjectStore(indexedDbStore);
      }
    };

    request.onsuccess = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(indexedDbStore)) {
        db.close();
        resolve(null);
        return;
      }

      const transaction = db.transaction(indexedDbStore, "readonly");
      const getRequest = transaction.objectStore(indexedDbStore).get(key);
      getRequest.onsuccess = () => {
        db.close();
        resolve(getRequest.result || null);
      };
      getRequest.onerror = () => {
        db.close();
        reject(getRequest.error);
      };
    };

    request.onerror = () => reject(request.error);
  });
}

async function deleteFromIndexedDB(
  indexedDbName: string,
  indexedDbStore: string,
  key: string
): Promise<void> {
  return await new Promise((resolve, reject) => {
    const request = indexedDB.open(indexedDbName);

    request.onsuccess = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(indexedDbStore)) {
        db.close();
        resolve();
        return;
      }

      const transaction = db.transaction(indexedDbStore, "readwrite");
      transaction.objectStore(indexedDbStore).delete(key);
      transaction.oncomplete = () => {
        db.close();
        resolve();
      };
      transaction.onerror = () => {
        db.close();
        reject(transaction.error);
      };
    };

    request.onerror = () => reject(request.error);
  });
}

async function clearSyncableTablesForMigration(
  db: BrowserSqliteDatabase,
  syncSchema: SyncSchemaDescription,
  logger: ILogger
): Promise<void> {
  logger.info("Schema migration detected - clearing local sync tables...");
  const orderedTables = [...syncSchema.syncableTables].sort(
    (left, right) =>
      (syncSchema.tableSyncOrder[right] ?? 0) -
      (syncSchema.tableSyncOrder[left] ?? 0)
  );

  await db.run("PRAGMA foreign_keys = OFF");
  try {
    for (const tableName of orderedTables) {
      try {
        await db.run(`DELETE FROM "${tableName}"`);
      } catch (error) {
        logger.warn(`Failed to clear table ${tableName}`, error);
      }
    }
  } finally {
    await db.run("PRAGMA foreign_keys = ON");
  }
}

export function ensureColumnExists(
  db: SqlJsDatabase,
  table: string,
  column: string,
  definition: string,
  logger?: ILogger
): void {
  try {
    const result = db.exec(`PRAGMA table_info(${table})`);
    const values = result[0]?.values ?? [];
    const hasColumn = values.some((row) => row?.[1] === column);
    if (hasColumn) return;
    logger?.debug?.(`Adding missing column ${table}.${column}`);
    db.run(`ALTER TABLE ${table} ADD COLUMN ${definition}`);
  } catch (error) {
    logger?.error?.(`Failed to ensure column ${table}.${column}`, error);
  }
}

export function createBrowserSqliteClient<
  Schema extends Record<string, unknown>,
>(config: IBrowserSqliteClientConfig<Schema>): IBrowserSqliteClient<Schema> {
  let sqliteDb: SqlJsDatabase | null = null;
  let drizzleDb: BrowserSqliteDatabase | null = null;
  let dbReady = false;
  let isClearing = false;
  let isInitializingDb = false;
  let clearDbPromise: Promise<void> | null = null;
  let initEpoch = 0;
  let sqlJsInitPromise: Promise<SqlJsStatic> | null = null;
  let sqlJsModule: SqlJsStatic | null = null;
  let sqlJsInitAttempts = 0;
  const sqlJsMaxRetries = 3;
  let cachedWasmBinary: ArrayBuffer | null = null;
  let initializeDbPromise: Promise<BrowserSqliteDatabase> | null = null;
  let currentUserId: string | null = null;
  let e2ePersistCount = 0;
  let e2eCumulativeExportBytes = 0;
  const logger = config.hooks.logger;

  const diagLog = (...args: unknown[]): void => {
    if (config.diagnosticsEnabled) {
      logger.debug(...args);
    }
  };

  const getDbKey = (userId: string): string =>
    `${config.storage.dbKeyPrefix}-${userId}`;
  const getDbVersionKey = (userId: string): string =>
    `${config.storage.dbVersionKeyPrefix}-${userId}`;
  const getOutboxBackupKey = (userId: string): string =>
    `${config.storage.outboxBackupKeyPrefix}-${userId}`;

  async function getSqlJs(): Promise<SqlJsStatic> {
    if (sqlJsModule) return sqlJsModule;
    if (sqlJsInitPromise) return sqlJsInitPromise;

    sqlJsInitAttempts += 1;
    diagLog("initSqlJs attempt", sqlJsInitAttempts);
    if (!cachedWasmBinary) {
      try {
        const response = await fetch(config.sqlWasmUrl, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(
            `Failed to fetch sql.js wasm binary ${response.status}`
          );
        }
        cachedWasmBinary = await response.arrayBuffer();
      } catch (error) {
        logger.warn("Failed to prefetch sql.js wasm binary", error);
      }
    }

    sqlJsInitPromise = initSqlJs({
      locateFile: (file) => {
        if (file === "sql-wasm.wasm") return config.sqlWasmUrl;
        return `/sql-wasm/${file}`;
      },
      wasmBinary: cachedWasmBinary || undefined,
    })
      .then((module) => {
        sqlJsModule = module;
        return module;
      })
      .catch((error: unknown) => {
        logger.error("sql.js init failed", error);
        sqlJsInitPromise = null;
        const message = error instanceof Error ? error.message : String(error);
        if (
          sqlJsInitAttempts < sqlJsMaxRetries &&
          /callback is no longer runnable/i.test(message)
        ) {
          logger.warn("Retrying sql.js init after callback error...");
          return new Promise<SqlJsStatic>((resolve, reject) => {
            setTimeout(() => {
              getSqlJs().then(resolve).catch(reject);
            }, 50);
          });
        }
        throw error;
      });

    return await sqlJsInitPromise;
  }

  async function saveOutboxBackupForUser(
    userId: string,
    backup: IOutboxBackup
  ): Promise<void> {
    await saveToIndexedDB(
      config.storage.indexedDbName,
      config.storage.indexedDbStore,
      getOutboxBackupKey(userId),
      encodeJsonToBytes(backup)
    );
  }

  async function loadOutboxBackupForUser(
    userId: string
  ): Promise<IOutboxBackup | null> {
    const bytes = await loadFromIndexedDB(
      config.storage.indexedDbName,
      config.storage.indexedDbStore,
      getOutboxBackupKey(userId)
    );
    if (!bytes) return null;

    const parsed = decodeJsonFromBytes(bytes);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      (parsed as { version?: unknown }).version !== 1
    ) {
      return null;
    }
    return parsed as IOutboxBackup;
  }

  async function clearOutboxBackupForUser(userId: string): Promise<void> {
    await deleteFromIndexedDB(
      config.storage.indexedDbName,
      config.storage.indexedDbStore,
      getOutboxBackupKey(userId)
    );
  }

  async function backupPendingOutboxBestEffort(
    userId: string,
    db: SqlJsDatabase
  ): Promise<void> {
    try {
      await clearOutboxBackupForUser(userId);
      const backup = createOutboxBackup(db, config.syncSchema);
      if (backup.items.length === 0) return;
      await saveOutboxBackupForUser(userId, backup);
      diagLog(`Backed up ${backup.items.length} outbox item(s) for replay`);
    } catch (error) {
      logger.warn("Failed to back up outbox before migration/recreate", error);
    }
  }

  function isDbInitAbortedError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return (
      message.includes("Database initialization aborted") ||
      message.includes("clearDb() was called during initialization")
    );
  }

  function isTransientDbInitError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return (
      message.includes("Failed to fetch") ||
      message.includes("NetworkError") ||
      message.includes("ERR_INTERNET_DISCONNECTED") ||
      message.includes("The network connection was lost") ||
      message.includes("Load failed") ||
      message.includes("callback is no longer runnable")
    );
  }

  async function initializeDb(userId: string): Promise<BrowserSqliteDatabase> {
    if (isClearing && clearDbPromise) {
      await clearDbPromise;
    }

    const myInitEpoch = initEpoch;
    const ensureNotCleared = (): void => {
      if (initEpoch !== myInitEpoch) {
        throw new Error(
          "Database initialization aborted: clearDb() was called during initialization. " +
            `initEpoch=${initEpoch} myInitEpoch=${myInitEpoch} ` +
            `isClearing=${isClearing} isInitializingDb=${isInitializingDb} ` +
            `e2eClearing=${isExternalClearInProgress(config.testConfig)}`
        );
      }
    };

    if (drizzleDb && currentUserId && currentUserId !== userId) {
      try {
        await persistDb();
      } catch (error) {
        logger.warn("Failed to persist previous user's DB", error);
      }
      if (sqliteDb) {
        sqliteDb.close();
        sqliteDb = null;
      }
      drizzleDb = null;
      dbReady = false;
      initializeDbPromise = null;
    }

    if (drizzleDb && currentUserId === userId) {
      dbReady = true;
      return drizzleDb;
    }
    if (initializeDbPromise) {
      return await initializeDbPromise;
    }

    isInitializingDb = true;
    initializeDbPromise = (async () => {
      try {
        const SQL = await getSqlJs();
        ensureNotCleared();

        const requireSqliteDb = (): SqlJsDatabase => {
          ensureNotCleared();
          if (!sqliteDb) {
            throw new Error(
              "Database initialization aborted: sqliteDb was cleared during initialization."
            );
          }
          return sqliteDb;
        };

        const forceResetQueryParams = config.forceResetQueryParams ?? [];
        const migrationNeeded = needsMigration({
          schemaVersion: config.schemaVersion,
          storage: config.storage,
          forceResetQueryParams,
          logger,
        });
        const forcedReset = isForcedReset({ forceResetQueryParams });

        if (forcedReset) {
          try {
            await clearOutboxBackupForUser(userId);
          } catch (error) {
            logger.warn(
              "Failed to clear outbox backup for forced reset",
              error
            );
          }
        }

        const dbKey = getDbKey(userId);
        const dbVersionKey = getDbVersionKey(userId);
        const existingData = await loadFromIndexedDB(
          config.storage.indexedDbName,
          config.storage.indexedDbStore,
          dbKey
        );
        const storedVersion = await loadFromIndexedDB(
          config.storage.indexedDbName,
          config.storage.indexedDbStore,
          dbVersionKey
        );
        ensureNotCleared();
        const storedVersionNum =
          storedVersion && storedVersion.length > 0 ? storedVersion[0] : 0;

        let phase: "loaded" | "created" = "created";
        if (existingData && storedVersionNum === config.databaseVersion) {
          sqliteDb = new SQL.Database(existingData);
          phase = "loaded";
          drizzleDb = drizzle(sqliteDb, { schema: { ...config.schema } });
          if (config.hooks.onExistingDatabaseLoaded) {
            await config.hooks.onExistingDatabaseLoaded(drizzleDb, {
              phase,
              rawDb: requireSqliteDb(),
              userId,
            });
          }
        } else {
          if (existingData) {
            try {
              const oldDb = new SQL.Database(existingData);
              await backupPendingOutboxBestEffort(userId, oldDb);
              oldDb.close();
            } catch (error) {
              logger.warn("Failed to inspect old DB for outbox backup", error);
            }
            await deleteFromIndexedDB(
              config.storage.indexedDbName,
              config.storage.indexedDbStore,
              dbKey
            );
            await deleteFromIndexedDB(
              config.storage.indexedDbName,
              config.storage.indexedDbStore,
              dbVersionKey
            );
          }

          clearLastSyncTimestampForUser(config.storage, userId, logger);
          sqliteDb = new SQL.Database();

          for (const migrationPath of config.migrationFiles) {
            const response = await fetch(migrationPath, { cache: "no-store" });
            if (!response.ok) {
              throw new Error(
                `Failed to load migration ${migrationPath}: ${response.status} ${response.statusText}`
              );
            }
            const migrationSql = await response.text();
            const statements = migrationSql
              .split("--> statement-breakpoint")
              .map((statement) => statement.trim())
              .filter((statement) => statement.length > 0);
            for (const statement of statements) {
              if (statement && !statement.startsWith("--")) {
                requireSqliteDb().run(statement);
              }
            }
          }

          drizzleDb = drizzle(requireSqliteDb(), {
            schema: { ...config.schema },
          });
        }

        if (!drizzleDb) {
          throw new Error("Failed to initialize browser SQLite database");
        }

        if (config.hooks.onDatabaseReady) {
          await config.hooks.onDatabaseReady(drizzleDb, {
            phase,
            rawDb: requireSqliteDb(),
            userId,
          });
        }

        installSyncTriggers(requireSqliteDb(), config.syncSchema, logger);
        ensureNotCleared();
        currentUserId = userId;
        dbReady = true;

        if (migrationNeeded) {
          clearLastSyncTimestampForUser(config.storage, userId, logger);
          if (!forcedReset && sqliteDb) {
            await backupPendingOutboxBestEffort(userId, sqliteDb);
          }

          if (config.hooks.clearLocalDataForMigration) {
            await config.hooks.clearLocalDataForMigration(drizzleDb, {
              userId,
            });
          } else {
            await clearSyncableTablesForMigration(
              drizzleDb,
              config.syncSchema,
              logger
            );
          }

          try {
            sqliteDb?.run("DELETE FROM sync_push_queue");
            sqliteDb?.run(
              "UPDATE sync_trigger_control SET disabled = 0 WHERE id = 1"
            );
          } catch (error) {
            logger.warn("Failed to clear sync outbox after migration", error);
          }

          setLocalSchemaVersion(config.storage, config.schemaVersion);
          clearMigrationParams(forceResetQueryParams, logger);
        } else if (!getLocalSchemaVersion(config.storage)) {
          setLocalSchemaVersion(config.storage, config.schemaVersion);
        }

        return drizzleDb;
      } catch (error) {
        if (
          isDbInitAbortedError(error) &&
          (isClearing || isExternalClearInProgress(config.testConfig))
        ) {
          logger.warn("initializeDb aborted during clear", error);
        } else if (isTransientDbInitError(error)) {
          logger.warn("initializeDb transient failure", error);
        } else {
          logger.error("initializeDb failed", error);
        }
        initializeDbPromise = null;
        throw error;
      }
    })();

    return await initializeDbPromise.finally(() => {
      isInitializingDb = false;
      if (initEpoch !== myInitEpoch && drizzleDb) {
        try {
          sqliteDb?.close();
        } catch {
          // ignore cleanup errors after cancellation
        }
        sqliteDb = null;
        drizzleDb = null;
        dbReady = false;
        initializeDbPromise = null;
      }
    });
  }

  function getDb(): BrowserSqliteDatabase {
    if (isClearing) {
      throw new Error(
        "SQLite database is clearing. Wait for clearDb() to finish before using the database."
      );
    }
    if (!drizzleDb) {
      throw new Error(
        "SQLite database not initialized. Call initializeDb() first."
      );
    }
    return drizzleDb;
  }

  async function persistDb(): Promise<void> {
    if (isClearing) {
      logger.warn("Skipping persist while clearing local DB");
      return;
    }
    if (!sqliteDb) {
      throw new Error("SQLite database not initialized");
    }
    const dbToPersist = sqliteDb;
    if (!dbReady) {
      logger.warn("Skipping persist until DB initialization completes");
      return;
    }
    if (!currentUserId) {
      throw new Error("Cannot persist database: no user ID set");
    }
    const data = dbToPersist.export();
    if (isTestMode(config.testConfig) && config.diagnosticsEnabled) {
      e2ePersistCount += 1;
      e2eCumulativeExportBytes += data.byteLength;
      diagLog(
        `[E2E Persist #${e2ePersistCount}] Export size ${(data.byteLength / 1024).toFixed(1)}KB, cumulative ${(e2eCumulativeExportBytes / 1024 / 1024).toFixed(2)}MB`
      );
    }
    await saveToIndexedDB(
      config.storage.indexedDbName,
      config.storage.indexedDbStore,
      getDbKey(currentUserId),
      data
    );
    await saveToIndexedDB(
      config.storage.indexedDbName,
      config.storage.indexedDbStore,
      getDbVersionKey(currentUserId),
      new Uint8Array([config.databaseVersion])
    );
  }

  registerPersistHook(config.testConfig, persistDb, logger);

  async function closeDb(): Promise<void> {
    if (sqliteDb && dbReady) {
      try {
        await persistDb();
      } catch (error) {
        logger.warn("Failed to persist before close", error);
      }
    }
    if (sqliteDb) {
      sqliteDb.close();
      sqliteDb = null;
    }
    drizzleDb = null;
    currentUserId = null;
    dbReady = false;
    initializeDbPromise = null;
  }

  async function clearDb(): Promise<void> {
    if (clearDbPromise) return await clearDbPromise;
    isClearing = true;
    dbReady = false;
    initEpoch += 1;
    initializeDbPromise = null;
    const userIdToClear = currentUserId;
    currentUserId = null;

    clearDbPromise = (async () => {
      if (sqliteDb) {
        sqliteDb.close();
        sqliteDb = null;
        drizzleDb = null;
        e2ePersistCount = 0;
        e2eCumulativeExportBytes = 0;
      } else {
        drizzleDb = null;
      }

      if (userIdToClear) {
        await deleteFromIndexedDB(
          config.storage.indexedDbName,
          config.storage.indexedDbStore,
          getDbKey(userIdToClear)
        );
        await deleteFromIndexedDB(
          config.storage.indexedDbName,
          config.storage.indexedDbStore,
          getDbVersionKey(userIdToClear)
        );
        clearLastSyncTimestampForUser(config.storage, userIdToClear, logger);
      }
    })().finally(() => {
      isClearing = false;
      clearDbPromise = null;
    });

    return await clearDbPromise;
  }

  function setupAutoPersist(): () => void {
    if (isTestMode(config.testConfig)) {
      diagLog("Auto-persist disabled in test mode");
      return () => {};
    }

    const persistHandler = (): void => {
      if (drizzleDb) {
        void persistDb().catch((error) =>
          logger.error("Persist failed", error)
        );
      }
    };

    window.addEventListener("beforeunload", persistHandler);
    const visibilityHandler = (): void => {
      if (document.hidden) {
        persistHandler();
      }
    };
    document.addEventListener("visibilitychange", visibilityHandler);
    const intervalId = window.setInterval(persistHandler, 30_000);

    return () => {
      window.removeEventListener("beforeunload", persistHandler);
      document.removeEventListener("visibilitychange", visibilityHandler);
      window.clearInterval(intervalId);
    };
  }

  async function getSqliteInstance(): Promise<SqliteRawDatabase | null> {
    return sqliteDb;
  }

  function getSqlJsDebugInfo(): IBrowserSqliteDebugInfo {
    try {
      const heapView = (
        sqlJsModule as unknown as { HEAP8?: { buffer?: ArrayBufferLike } } | null
      )?.HEAP8;
      const heapBytes = Number(heapView?.buffer?.byteLength ?? 0);
      return {
        hasModule: !!sqlJsModule,
        wasmHeapBytes: heapBytes > 0 ? heapBytes : undefined,
      };
    } catch {
      return { hasModule: !!sqlJsModule };
    }
  }

  function getDebugState(): IClientSqliteDebugState {
    return {
      initEpoch,
      isClearing,
      isInitializingDb,
      dbReady,
      hasSqliteDb: !!sqliteDb,
      hasDrizzleDb: !!drizzleDb,
      currentUser: currentUserId,
    };
  }

  return {
    schema: config.schema,
    logger,
    initializeDb,
    getDb,
    persistDb,
    closeDb,
    clearDb,
    setupAutoPersist,
    getSqliteInstance,
    getSqlJsDebugInfo,
    getDebugState,
    loadOutboxBackupForUser,
    clearOutboxBackupForUser,
    replayOutboxBackup: (db, backup) =>
      replayOutboxBackup(db, config.syncSchema, backup),
    suppressSyncTriggers,
    enableSyncTriggers,
    syncPushQueue: config.syncPushQueue,
  };
}

export function createBrowserSyncRuntime<
  Schema extends Record<string, unknown>,
>(params: {
  client: IBrowserSqliteClient<Schema>;
  schema: SyncSchemaDescription;
  localSchema: Record<string, unknown>;
}): SyncRuntime {
  return {
    schema: params.schema,
    syncPushQueue: params.client.syncPushQueue,
    localSchema: params.localSchema,
    getSqliteInstance: params.client.getSqliteInstance,
    loadOutboxBackupForUser: params.client.loadOutboxBackupForUser,
    clearOutboxBackupForUser: params.client.clearOutboxBackupForUser,
    replayOutboxBackup: params.client.replayOutboxBackup,
    enableSyncTriggers: params.client.enableSyncTriggers,
    suppressSyncTriggers: params.client.suppressSyncTriggers,
    persistDb: params.client.persistDb,
    logger: params.client.logger,
  };
}
