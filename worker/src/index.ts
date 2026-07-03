/**
 * Cloudflare Worker Sync Endpoint
 *
 * Architecture:
 * - PUSH: Client sends changes → Worker applies to Postgres
 * - PULL: Worker queries sync_change_log → returns changed rows to client
 *
 * @module worker/index
 */
import { and, eq, gt, lte } from "drizzle-orm";
import type {
  AnyPgColumn,
  PgQueryResultHKT,
  PgTransaction,
} from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/postgres-js";
import { createRemoteJWKSet, jwtVerify } from "jose";
import postgres from "postgres";
import type {
  SyncChange,
  SyncRequest,
  SyncResponse,
} from "../../src/shared/protocol";
import { debug, setDebugEnabled } from "./debug";
import type {
  DynamicPgTable,
  IPushTableRule,
  PullTableRule,
  SchemaTables,
  SyncSchemaDeps,
  WorkerTransaction,
} from "./sync-schema";
import { createSyncSchema } from "./sync-schema";

const CORE_COL_DELETED = "deleted";
const CORE_COL_LAST_MODIFIED_AT = "last_modified_at";

export interface WorkerArtifacts extends SyncSchemaDeps {
  schemaTables: SchemaTables;
}

function notInitialized(name: string): never {
  throw new Error(
    `oosync worker not initialized: missing ${name}. ` +
      "Call createWorker({ schemaTables, syncableTables, tableRegistryCore, workerSyncConfig }) in the consumer worker package."
  );
}

let schemaTables: SchemaTables | null = null;
let SYNCABLE_TABLES: readonly string[] = [];
let TABLE_REGISTRY: Record<string, { relationKind?: string }> = {};

let getPrimaryKey: (tableName: string) => string | string[] = () =>
  notInitialized("getPrimaryKey");
let getConflictTarget: (tableName: string) => string[] = () =>
  notInitialized("getConflictTarget");
let getBooleanColumns: (tableName: string) => string[] = () =>
  notInitialized("getBooleanColumns");
let hasDeletedFlag: (tableName: string) => boolean = () =>
  notInitialized("hasDeletedFlag");
let buildUserFilter: (params: {
  tableName: string;
  table: DynamicPgTable;
  userId: string;
  collections: Record<string, Set<string>>;
}) => unknown[] | null = () => notInitialized("buildUserFilter");
let loadUserCollections: (params: {
  tx: WorkerTransaction;
  userId: string;
  tables: SchemaTables;
}) => Promise<Record<string, Set<string>>> = async () =>
  notInitialized("loadUserCollections");
let minimalPayload: (
  data: Record<string, unknown>,
  keep: string[]
) => Record<string, unknown> = () => notInitialized("minimalPayload");
let normalizeRowForSync: (
  tableName: string,
  row: Record<string, unknown>
) => Record<string, unknown> = () => notInitialized("normalizeRowForSync");
let snakeToCamel: (snake: string) => string = () =>
  notInitialized("snakeToCamel");
let sanitizeForPush: (params: {
  tableName: string;
  changeLastModifiedAt: string;
  data: Record<string, unknown>;
}) => { data: Record<string, unknown>; changed: string[] } = () =>
  notInitialized("sanitizeForPush");
let getPushRule: (tableName: string) => IPushTableRule | undefined = () =>
  notInitialized("getPushRule");
let getPullRule: (tableName: string) => PullTableRule | undefined = () =>
  notInitialized("getPullRule");

function getSchemaTables(): SchemaTables {
  if (!schemaTables) {
    notInitialized("schemaTables");
  }
  return schemaTables;
}

export function createWorker(artifacts: WorkerArtifacts) {
  schemaTables = artifacts.schemaTables;

  const schema = createSyncSchema({
    syncableTables: artifacts.syncableTables,
    tableRegistryCore: artifacts.tableRegistryCore,
    workerSyncConfig: artifacts.workerSyncConfig,
    schemaTables: artifacts.schemaTables,
  });

  SYNCABLE_TABLES = schema.SYNCABLE_TABLES;
  TABLE_REGISTRY = schema.TABLE_REGISTRY;
  getPrimaryKey = schema.getPrimaryKey;
  getConflictTarget = schema.getConflictTarget;
  getBooleanColumns = schema.getBooleanColumns;
  hasDeletedFlag = schema.hasDeletedFlag;
  buildUserFilter = schema.buildUserFilter;
  loadUserCollections = schema.loadUserCollections;
  minimalPayload = schema.minimalPayload;
  normalizeRowForSync = schema.normalizeRowForSync;
  snakeToCamel = schema.snakeToCamel;
  sanitizeForPush = schema.sanitizeForPush;
  getPushRule = schema.getPushRule;
  getPullRule = schema.getPullRule;

  return { fetch };
}

export default createWorker;

// ============================================================================
// DB CONNECTION
// ============================================================================

type PostgresClient = ReturnType<typeof postgres>;
type DrizzleDb = ReturnType<typeof drizzle>;

type PostgresJsErrorLike = {
  message?: unknown;
  code?: unknown;
  detail?: unknown;
  hint?: unknown;
  table_name?: unknown;
  column_name?: unknown;
  constraint_name?: unknown;
  cause?: unknown;
};

function isPostgresJsErrorLike(error: unknown): error is PostgresJsErrorLike {
  return typeof error === "object" && error !== null;
}

function toOptionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function getErrorCause(error: unknown): unknown {
  if (!isPostgresJsErrorLike(error)) return undefined;
  return error.cause;
}

function findPostgresErrorLike(
  error: unknown
): PostgresJsErrorLike | undefined {
  // Some libraries wrap the underlying Postgres error under `cause`.
  let current: unknown = error;
  for (let depth = 0; depth < 4; depth += 1) {
    if (isPostgresJsErrorLike(current)) {
      // Heuristic: if it has any of the Postgres-ish fields, treat it as one.
      if (
        current.code !== undefined ||
        current.detail !== undefined ||
        current.constraint_name !== undefined ||
        current.table_name !== undefined ||
        current.column_name !== undefined
      ) {
        return current;
      }
    }
    const next = getErrorCause(current);
    if (next === undefined) break;
    current = next;
  }
  return undefined;
}

function formatDbError(error: unknown): string {
  const fallback = error instanceof Error ? error.message : String(error);
  const pgErr = findPostgresErrorLike(error);
  if (!pgErr) return fallback;

  const code = toOptionalString(pgErr.code);
  const detail = toOptionalString(pgErr.detail);
  const hint = toOptionalString(pgErr.hint);
  const table = toOptionalString(pgErr.table_name);
  const column = toOptionalString(pgErr.column_name);
  const constraint = toOptionalString(pgErr.constraint_name);

  // Prefer structured Postgres fields over verbose wrapped messages that may
  // include raw SQL/params.
  const parts = [
    code ? `code=${code}` : undefined,
    table ? `table=${table}` : undefined,
    column ? `column=${column}` : undefined,
    constraint ? `constraint=${constraint}` : undefined,
    detail ? `detail=${detail}` : undefined,
    hint ? `hint=${hint}` : undefined,
  ].filter((p): p is string => typeof p === "string");

  if (parts.length > 0) {
    return parts.join(" | ");
  }

  // Last resort: strip params from known wrapped format.
  if (fallback.includes("\nparams:")) {
    return fallback.split("\nparams:")[0];
  }
  return fallback;
}

function perfLog(enabled: boolean, message: string): void {
  if (enabled) {
    console.log(`[PERF] ${message}`);
  }
}

function perfLogDuration(
  enabled: boolean,
  minDurationMs: number,
  durationMs: number,
  message: string
): void {
  if (!enabled || durationMs < minDurationMs) {
    return;
  }
  console.log(`[PERF] ${message} durationMs=${durationMs}`);
}

function parsePerfMinDurationMs(value: string | undefined): number {
  if (!value) {
    return 100;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 100;
  }
  return Math.floor(parsed);
}

function resolveConnectionString(env: Env): string {
  const bypassHyperdrive = env.BYPASS_HYPERDRIVE === "true";
  if (bypassHyperdrive) {
    if (!env.DATABASE_URL) {
      throw new Error(
        "Database configuration error: BYPASS_HYPERDRIVE=true requires DATABASE_URL"
      );
    }
    return env.DATABASE_URL;
  }

  const connectionString = env.HYPERDRIVE?.connectionString ?? env.DATABASE_URL;
  if (!connectionString) {
    const msg = env.HYPERDRIVE
      ? "HYPERDRIVE binding has no connectionString"
      : "DATABASE_URL not configured";
    throw new Error(`Database configuration error: ${msg}`);
  }

  return connectionString;
}

function createDb(env: Env): {
  client: PostgresClient;
  db: DrizzleDb;
  close: () => Promise<void>;
} {
  const connectionString = resolveConnectionString(env);

  // IMPORTANT (Cloudflare Workers): Do NOT cache/reuse database clients across requests.
  // Newer Workers runtimes enforce request-scoped I/O; reusing a client can trigger:
  // "Cannot perform I/O on behalf of a different request" (I/O type: Writable).
  // - max: 1 is appropriate here because this handler uses one request-scoped client and
  //   mostly sequential DB work inside one transaction.
  // - prepare: false is safer with pooled/proxied connections (Hyperdrive layer), and
  //   usually avoids prepared-statement churn on short-lived clients.
  const client = postgres(connectionString, { max: 1, prepare: false });
  const db = drizzle(client, { schema: getSchemaTables() });

  const close = async () => {
    try {
      await client.end({ timeout: 5 });
    } catch {
      // ignore
    }
  };

  return { client, db, close };
}

// ============================================================================
// TYPES
// ============================================================================

// Cloudflare Workers Hyperdrive binding type.
// We keep this local/minimal to avoid requiring global workers type packages
// for editor typechecking.
type Hyperdrive = {
  connectionString?: string;
};

type IncomingSyncChange = SyncChange<string>;

function isClientSyncChange(change: IncomingSyncChange): boolean {
  return (
    change.table !== "sync_push_queue" && change.table !== "sync_change_log"
  );
}

export interface Env {
  HYPERDRIVE?: Hyperdrive;
  DATABASE_URL?: string;
  /** When "true", ignores HYPERDRIVE and connects via DATABASE_URL directly. */
  BYPASS_HYPERDRIVE?: string;
  SUPABASE_URL: string;
  SUPABASE_JWT_SECRET?: string;
  /** When "true", enables debug logging (console.log statements). */
  WORKER_DEBUG?: string;
  /** When "true", emits perf timing logs for sync phases. */
  WORKER_DEBUG_PERF?: string;
  /** Optional minimum duration threshold (ms) for WORKER_DEBUG_PERF logs. */
  WORKER_DEBUG_PERF_MIN_MS?: string;
  /** When "true", emits extra sync diagnostics logs (initial sync only). */
  SYNC_DIAGNOSTICS?: string;
  /** Optional: only emit diagnostics when JWT sub matches this value. */
  SYNC_DIAGNOSTICS_USER_ID?: string;
}

/** Context passed through sync operations */
interface SyncContext {
  /** Authenticated user identifier from JWT subject. */
  userId: string;
  /** Authenticated user identifier from JWT subject. */
  authUserId: string;
  collections: Record<string, Set<string>>;
  rpcParamOverrides?: Record<string, Record<string, unknown>>;
  pullTables?: Set<string>;
  now: string;
  diagnosticsEnabled: boolean;
  perfDebugEnabled: boolean;
  perfMinDurationMs: number;
}

interface InitialPullPage {
  changes: SyncChange[];
  diagnostics: string[];
  timing?: InitialPageTiming;
}

function getRelationKind(tableName: string): string {
  return TABLE_REGISTRY[tableName]?.relationKind ?? "relation";
}

function estimatePayloadBytes(changes: SyncChange[]): number {
  return estimateJsonBytes(changes);
}

function estimateJsonBytes(value: unknown): number {
  try {
    return new TextEncoder().encode(JSON.stringify(value)).length;
  } catch {
    return -1;
  }
}

interface InitialPageTiming {
  tableName: string;
  relationKind: string;
  ruleKind: string;
  offset: number;
  limit: number;
  rows: number;
  queryDurationMs: number;
  transformDurationMs: number;
  totalDurationMs: number;
  payloadBytes: number;
}

interface InitialTimingAggregate {
  pages: number;
  relations: Set<string>;
  rows: number;
  queryDurationMs: number;
  transformDurationMs: number;
  totalDurationMs: number;
  payloadBytes: number;
}

function createInitialPageTiming(params: {
  tableName: string;
  ruleKind: string;
  offset: number;
  limit: number;
  rows: number;
  queryDurationMs: number;
  transformDurationMs: number;
  totalDurationMs: number;
  payloadBytes: number;
}): InitialPageTiming {
  return {
    ...params,
    relationKind: getRelationKind(params.tableName),
  };
}

function createInitialPageDiagnostics(timing: InitialPageTiming): string {
  return [
    "[WorkerSyncDiag]",
    "initialPage",
    `relation=${timing.tableName}`,
    `kind=${timing.relationKind}`,
    `rule=${timing.ruleKind}`,
    `offset=${timing.offset}`,
    `limit=${timing.limit}`,
    `rows=${timing.rows}`,
    `queryMs=${timing.queryDurationMs}`,
    `transformMs=${timing.transformDurationMs}`,
    `payloadBytes=${timing.payloadBytes}`,
    `totalMs=${timing.totalDurationMs}`,
  ].join(" ");
}

function addInitialTimingAggregate(
  aggregates: Map<string, InitialTimingAggregate>,
  timing: InitialPageTiming
): void {
  const aggregate = aggregates.get(timing.relationKind) ?? {
    pages: 0,
    relations: new Set<string>(),
    rows: 0,
    queryDurationMs: 0,
    transformDurationMs: 0,
    totalDurationMs: 0,
    payloadBytes: 0,
  };
  aggregate.pages += 1;
  aggregate.relations.add(timing.tableName);
  aggregate.rows += timing.rows;
  aggregate.queryDurationMs += timing.queryDurationMs;
  aggregate.transformDurationMs += timing.transformDurationMs;
  aggregate.totalDurationMs += timing.totalDurationMs;
  aggregate.payloadBytes += timing.payloadBytes;
  aggregates.set(timing.relationKind, aggregate);
}

function createInitialKindDiagnostics(
  aggregates: Map<string, InitialTimingAggregate>
): string[] {
  return [...aggregates.entries()]
    .sort(([kindA], [kindB]) => kindA.localeCompare(kindB))
    .map(([kind, aggregate]) =>
      [
        "[WorkerSyncDiag]",
        "initialKind",
        `kind=${kind}`,
        `pages=${aggregate.pages}`,
        `relations=${aggregate.relations.size}`,
        `rows=${aggregate.rows}`,
        `queryMs=${aggregate.queryDurationMs}`,
        `transformMs=${aggregate.transformDurationMs}`,
        `payloadBytes=${aggregate.payloadBytes}`,
        `totalMs=${aggregate.totalDurationMs}`,
      ].join(" ")
    );
}

function createInitialRelationTopDiagnostics(
  timings: InitialPageTiming[]
): string | undefined {
  const top = [...timings]
    .sort((a, b) => b.totalDurationMs - a.totalDurationMs)
    .slice(0, 8)
    .map((timing) =>
      [
        timing.tableName,
        `kind=${timing.relationKind}`,
        `totalMs=${timing.totalDurationMs}`,
        `queryMs=${timing.queryDurationMs}`,
        `rows=${timing.rows}`,
        `payloadBytes=${timing.payloadBytes}`,
      ].join(":")
    )
    .join(",");

  if (!top) {
    return undefined;
  }
  return `[WorkerSyncDiag] initialRelationTop sort=totalMs relations=${top}`;
}

interface InitialSyncCursorV1 {
  v: 1;
  tableIndex: number;
  offset: number;
  syncStartedAt: string;
}

interface InitialSyncPageState {
  tableIndex: number;
  offset: number;
}

interface InitialSyncPageBudget {
  pageSize: number;
  pageCount: number;
}

function encodeCursor(cursor: InitialSyncCursorV1): string {
  return btoa(JSON.stringify(cursor));
}

function decodeCursor(raw: string): InitialSyncCursorV1 {
  let parsed: unknown;
  try {
    parsed = JSON.parse(atob(raw));
  } catch {
    throw new Error("Invalid pullCursor");
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error("Invalid pullCursor");
  }

  const obj = parsed as Record<string, unknown>;
  if (obj.v !== 1) throw new Error("Unsupported pullCursor version");
  const tableIndex = Number(obj.tableIndex);
  const offset = Number(obj.offset);
  const syncStartedAt = String(obj.syncStartedAt);

  if (!Number.isFinite(tableIndex) || tableIndex < 0) {
    throw new Error("Invalid pullCursor.tableIndex");
  }
  if (!Number.isFinite(offset) || offset < 0) {
    throw new Error("Invalid pullCursor.offset");
  }
  if (!syncStartedAt || Number.isNaN(Date.parse(syncStartedAt))) {
    throw new Error("Invalid pullCursor.syncStartedAt");
  }

  return { v: 1, tableIndex, offset, syncStartedAt };
}

function encodeNextInitialSyncCursor(
  state: InitialSyncPageState,
  syncStartedAt: string
): string | undefined {
  if (state.tableIndex >= SYNCABLE_TABLES.length) {
    return undefined;
  }

  return encodeCursor({
    v: 1,
    tableIndex: state.tableIndex,
    offset: state.offset,
    syncStartedAt,
  });
}

function normalizePositiveIntegerHint(
  value: number | undefined,
  defaultValue: number,
  maxValue: number
): number {
  const requested =
    typeof value === "number" && Number.isFinite(value)
      ? Math.max(1, Math.floor(value))
      : defaultValue;
  return Math.min(requested, maxValue);
}

function getInitialSyncPageBudget(
  pageSizeHint: number | undefined,
  pageCountHint: number | undefined
): InitialSyncPageBudget {
  return {
    pageSize: normalizePositiveIntegerHint(pageSizeHint, 500, 500),
    pageCount: normalizePositiveIntegerHint(pageCountHint, 1, 32),
  };
}

function getInitialSyncStartState(params: {
  cursorRaw?: string;
  syncStartedAtHint?: string;
  now: string;
}): InitialSyncPageState & { syncStartedAt: string } {
  if (!params.cursorRaw) {
    return {
      tableIndex: 0,
      offset: 0,
      syncStartedAt: params.syncStartedAtHint ?? params.now,
    };
  }

  const cursor = decodeCursor(params.cursorRaw);
  return {
    tableIndex: cursor.tableIndex,
    offset: cursor.offset,
    syncStartedAt: cursor.syncStartedAt,
  };
}

type DrizzleColumn = AnyPgColumn;
type Transaction = PgTransaction<
  PgQueryResultHKT,
  Record<string, unknown>,
  Record<string, never>
>;

interface PostgresUnsafeClient {
  unsafe(query: string, params: unknown[]): Promise<unknown>;
}

type TransactionWithSession = Transaction & {
  session?: {
    client?: PostgresUnsafeClient;
  };
};

// ============================================================================
// UTILITY: Primary Key Helpers
// ============================================================================

/**
 * Get column definitions for a table's conflict target.
 *
 * This is used for UPSERT operations (onConflict target).
 */
function getConflictKeyColumns(
  tableName: string,
  table: DynamicPgTable
): { col: DrizzleColumn; prop: string }[] {
  const snakeKeys = getConflictTarget(tableName);
  return snakeKeys.map((snakeKey) => {
    const camelKey = snakeToCamel(snakeKey);
    const col = table[camelKey];
    if (!col) {
      throw new Error(`Column '${camelKey}' not found in '${tableName}'`);
    }
    return { col, prop: camelKey };
  });
}

/**
 * Get column definitions for a table's primary key.
 *
 * This is preferred for DELETE operations because the client may only send PK
 * values (e.g., daily_practice_queue sends only `id`).
 */
function getPrimaryKeyColumns(
  tableName: string,
  table: DynamicPgTable
): { col: DrizzleColumn; prop: string }[] {
  const primaryKey = getPrimaryKey(tableName);
  const snakeKeys = Array.isArray(primaryKey) ? primaryKey : [primaryKey];
  return snakeKeys.map((snakeKey) => {
    const camelKey = snakeToCamel(snakeKey);
    const col = table[camelKey];
    if (!col) {
      throw new Error(`Column '${camelKey}' not found in '${tableName}'`);
    }
    return { col, prop: camelKey };
  });
}

/**
 * Extract rowId string from a row object.
 */
function extractRowId(
  tableName: string,
  row: Record<string, unknown>,
  table: DynamicPgTable
): string {
  // Most tables have 'id' as primary key
  if (typeof row.id === "string" || typeof row.id === "number") {
    return String(row.id);
  }
  if (typeof row.id === "boolean" || typeof row.id === "bigint") {
    return row.id.toString();
  }

  // Composite key - serialize to JSON
  const pkCols = getConflictKeyColumns(tableName, table);
  const pkValues: Record<string, unknown> = {};
  for (const pk of pkCols) {
    pkValues[pk.prop] = row[pk.prop];
  }
  return JSON.stringify(pkValues);
}

// ============================================================================
// UTILITY: RPC Fetch
// ============================================================================

/**
 * Fetch table data via Postgres RPC function.
 * Used for tables with complex filtering requirements (e.g., JOINs).
 */
async function fetchViaRPC(
  tx: Transaction,
  functionName: string,
  params: Record<string, unknown>
): Promise<Record<string, unknown>[]> {
  try {
    // Build: SELECT * FROM function_name($1::TYPE, $2::TYPE, ...)
    const paramKeys = Object.keys(params);
    const paramValues = Object.values(params);

    const inferCast = (paramName: string, value: unknown): string | null => {
      if (value == null) return null;
      if (Array.isArray(value)) return "TEXT[]";

      if (typeof value === "number") {
        return Number.isInteger(value) ? "INTEGER" : "NUMERIC";
      }

      if (typeof value !== "string") {
        return null;
      }

      if (/^(p_)?(user|owner|auth_user)_id$/i.test(paramName)) {
        return "UUID";
      }

      if (/(timestamp|_at|date|time)/i.test(paramName)) {
        return "TIMESTAMPTZ";
      }

      if (/(limit|offset|count|size|page)/i.test(paramName)) {
        return "INTEGER";
      }

      return null;
    };

    // Build SQL query string with placeholders and type casts
    const placeholders: string[] = [];
    for (let i = 0; i < paramValues.length; i++) {
      const paramNum = i + 1;
      const paramName = paramKeys[i];
      const cast = inferCast(paramName, paramValues[i]);
      placeholders.push(cast ? `$${paramNum}::${cast}` : `$${paramNum}`);
    }

    const queryString = `SELECT * FROM ${functionName}(${placeholders.join(", ")})`;

    // Execute using the underlying session (bypassing Drizzle's sql template to avoid array wrapping)
    // Access the postgres-js client through the transaction's internal session
    const session = (tx as TransactionWithSession).session;
    if (!session?.client) {
      throw new Error(
        "Cannot access underlying postgres client from transaction"
      );
    }

    // Execute raw query with parameter binding
    const result = await session.client.unsafe(queryString, paramValues);

    return result as Record<string, unknown>[];
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`RPC ${functionName} failed: ${message}`);
  }
}

function resolveRpcParamsForRule(params: {
  rule: Extract<import("./sync-schema").PullTableRule, { kind: "rpc" }>;
  ctx: SyncContext;
  lastSyncAt: string | null;
  pageLimit: number;
  pageOffset: number;
}): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const ruleOverrides =
    params.ctx.rpcParamOverrides?.[params.rule.functionName] ?? {};

  for (const [paramName, binding] of Object.entries(params.rule.paramMap)) {
    if (Object.hasOwn(ruleOverrides, paramName)) {
      out[paramName] = ruleOverrides[paramName];
      continue;
    }

    switch (binding.source) {
      case "authUserId":
        out[paramName] = params.ctx.authUserId;
        break;
      case "collection": {
        const collection = params.ctx.collections[binding.collection];
        out[paramName] = collection ? Array.from(collection) : [];
        break;
      }
      case "lastSyncAt":
        out[paramName] = params.lastSyncAt;
        break;
      case "pageLimit":
        out[paramName] = params.pageLimit;
        break;
      case "pageOffset":
        out[paramName] = params.pageOffset;
        break;
      case "literal":
        out[paramName] = binding.value;
        break;
      case "requestOverride": {
        const overrideKey = binding.key ?? paramName;
        out[paramName] = ruleOverrides[overrideKey] ?? null;
        break;
      }
      default:
        out[paramName] = null;
        break;
    }
  }

  return out;
}

// ============================================================================
// UTILITY: Boolean Conversion (SQLite ↔ Postgres)
// ============================================================================

/**
 * Convert SQLite integers (0/1) to Postgres booleans.
 */
function sqliteToPostgres(
  tableName: string,
  data: Record<string, unknown>
): Record<string, unknown> {
  const boolCols = getBooleanColumns(tableName);
  if (boolCols.length === 0) return data;

  const result = { ...data };
  for (const snakeCol of boolCols) {
    const camelCol = snakeToCamel(snakeCol);
    if (camelCol in result && typeof result[camelCol] === "number") {
      result[camelCol] = result[camelCol] !== 0;
    }
  }
  return result;
}

function remapUserRefsForPush(
  data: Record<string, unknown>,
  _ctx: SyncContext
): Record<string, unknown> {
  // Reserved for optional future generic remapping hooks.
  return data;
}

/**
 * Convert Postgres booleans to SQLite integers (0/1).
 */
function postgresToSqlite(
  tableName: string,
  data: Record<string, unknown>
): Record<string, unknown> {
  const boolCols = getBooleanColumns(tableName);
  if (boolCols.length === 0) return data;

  const result = { ...data };
  for (const snakeCol of boolCols) {
    const camelCol = snakeToCamel(snakeCol);
    if (camelCol in result && typeof result[camelCol] === "boolean") {
      result[camelCol] = result[camelCol] ? 1 : 0;
    }
  }
  return result;
}

function preparePushData(
  change: IncomingSyncChange,
  pushRule: IPushTableRule | undefined,
  ctx: SyncContext
): Record<string, unknown> {
  let data = sqliteToPostgres(change.table, change.data);
  data = remapUserRefsForPush(data, ctx);

  const bindAuthUserIdProps = pushRule?.bindAuthUserIdProps ?? [];
  if (bindAuthUserIdProps.length > 0 && ctx.authUserId) {
    const boundData = { ...data };
    for (const prop of bindAuthUserIdProps) {
      boundData[prop] = ctx.authUserId;
    }
    data = boundData;
  }

  // Normalize timestamps for Postgres (add 'Z' suffix if missing, etc.)
  data = normalizeRowForSync(change.table, data);

  const sanitized = sanitizeForPush({
    tableName: change.table,
    changeLastModifiedAt: change.lastModifiedAt,
    data,
  });

  if (sanitized.changed.length > 0) {
    console.warn(
      `[PUSH] Sanitized ${change.table} rowId=${change.rowId}: ${sanitized.changed.join(", ")}`
    );
  }

  return sanitized.data;
}

async function applyUpsertWithRetry(
  tx: Transaction,
  table: DynamicPgTable,
  keyCols: { col: DrizzleColumn; prop: string }[],
  data: Record<string, unknown>,
  change: IncomingSyncChange,
  pushRule: IPushTableRule | undefined
): Promise<void> {
  const omitSetProps = pushRule?.upsert?.omitSetProps;
  const keepProps = pushRule?.upsert?.retryMinimalPayloadKeepProps;
  const upsertOpts = omitSetProps ? ({ omitSetProps } as const) : undefined;

  try {
    // Use a savepoint so a failed statement doesn't abort the outer transaction.
    await tx.transaction(async (sp) => {
      await applyUpsert(sp, table, keyCols, data, upsertOpts);
    });
  } catch (e) {
    // Only retry when the error matches a configured retriable SQLSTATE or constraint.
    const retryOnSqlStates = pushRule?.upsert?.retryOnSqlStates;
    const retryOnConstraints = pushRule?.upsert?.retryOnConstraints;
    const hasRetryStates = retryOnSqlStates && retryOnSqlStates.length > 0;
    const hasRetryConstraints =
      retryOnConstraints && retryOnConstraints.length > 0;

    if (!keepProps || keepProps.length === 0) throw e;
    if (!hasRetryStates && !hasRetryConstraints) throw e;

    const pgErr = findPostgresErrorLike(e);
    const sqlState = toOptionalString(pgErr?.code);
    const constraint = toOptionalString(pgErr?.constraint_name);

    const matchesState =
      hasRetryStates && sqlState && retryOnSqlStates?.includes(sqlState);
    const matchesConstraint =
      hasRetryConstraints &&
      constraint &&
      retryOnConstraints?.includes(constraint);

    if (!matchesState && !matchesConstraint) throw e;

    console.warn(
      `[PUSH] ${change.table} upsert retry (minimal payload) rowId=${change.rowId}: ${formatDbError(
        e
      )}`
    );
    const minimal = minimalPayload(data, keepProps);
    await tx.transaction(async (sp) => {
      await applyUpsert(sp, table, keyCols, minimal, upsertOpts);
    });
  }
}

// ============================================================================
// UTILITY: Authentication
// ============================================================================

// Cached JWKS key set — lazily initialized per Supabase URL
let cachedJwks: ReturnType<typeof createRemoteJWKSet> | null = null;
let cachedJwksUrl: string | null = null;

function getJwks(supabaseUrl: string): ReturnType<typeof createRemoteJWKSet> {
  const jwksUrl = `${supabaseUrl}/auth/v1/.well-known/jwks.json`;
  if (!cachedJwks || cachedJwksUrl !== jwksUrl) {
    cachedJwks = createRemoteJWKSet(new URL(jwksUrl));
    cachedJwksUrl = jwksUrl;
  }
  return cachedJwks;
}

async function verifyJwt(
  request: Request,
  secret: string | undefined,
  supabaseUrl: string
): Promise<string | null> {
  const authHeader = request.headers.get("Authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    debug.log("[AUTH] No Bearer token in Authorization header");
    return null;
  }

  const token = authHeader.split(" ")[1];
  try {
    // Peek at the JWT header to choose verification strategy
    const [headerB64] = token.split(".");
    const headerJson = JSON.parse(
      atob(headerB64.replaceAll("-", "+").replaceAll("_", "/"))
    );
    const algorithm = headerJson.alg;

    let payload: { sub?: string };

    if (algorithm === "HS256") {
      // HS256 (Supabase CLI < 2.75): symmetric secret
      if (!secret) {
        console.error(
          "[AUTH] HS256 token received but SUPABASE_JWT_SECRET is not configured"
        );
        return null;
      }
      debug.log("[AUTH] Using HS256 verification");
      const result = await jwtVerify(token, new TextEncoder().encode(secret));
      payload = result.payload;
    } else {
      // Asymmetric Supabase JWTs (for example RS256/ES256) publish public keys via JWKS.
      debug.log(`[AUTH] Using ${algorithm} verification via JWKS`);
      const result = await jwtVerify(token, getJwks(supabaseUrl));
      payload = result.payload;
    }

    debug.log("[AUTH] JWT verified successfully, user:", payload.sub);
    return payload.sub ?? null;
  } catch (e) {
    console.error("[AUTH] JWT verification failed:", e);
    return null;
  }
}

// ============================================================================
// PUSH: Apply Client Changes to Postgres
// ============================================================================

/**
 * Apply a single change (insert/update/delete) to Postgres.
 */
async function applyChange(
  tx: Transaction,
  change: IncomingSyncChange,
  ctx: SyncContext
): Promise<void> {
  // Skip sync infrastructure tables
  if (!isClientSyncChange(change)) {
    debug.log(`[PUSH] Skipping sync infrastructure table: ${change.table}`);
    return;
  }

  const pushRule = getPushRule(change.table);
  if (pushRule?.denyDelete && change.deleted) {
    console.warn(
      `[PUSH] Refusing DELETE for ${change.table} rowId=${change.rowId}`
    );
    return;
  }

  const table = getSchemaTables()[change.table];
  if (!table) {
    debug.log(`[PUSH] Unknown table: ${change.table}`);
    return;
  }

  const t = table;
  if (!t.lastModifiedAt) {
    debug.log(
      `[PUSH] Table ${change.table} has no lastModifiedAt column, skipping`
    );
    return;
  }

  const upsertKeyCols = getConflictKeyColumns(change.table, t);
  const deleteKeyCols = getPrimaryKeyColumns(change.table, t);
  const data = preparePushData(change, pushRule, ctx);

  debug.log(
    `[PUSH] Applying ${change.deleted ? "DELETE" : "UPSERT"} to ${change.table}, rowId: ${change.rowId}`
  );

  if (change.deleted) {
    await applyDelete(
      tx,
      change.table,
      table,
      deleteKeyCols,
      upsertKeyCols,
      change
    );
  } else {
    await applyUpsertWithRetry(
      tx,
      table,
      upsertKeyCols,
      data,
      change,
      pushRule
    );
  }
}

async function applyDelete(
  tx: Transaction,
  tableName: string,
  table: DynamicPgTable,
  primaryKeyCols: { col: DrizzleColumn; prop: string }[],
  conflictKeyCols: { col: DrizzleColumn; prop: string }[],
  change: SyncChange
): Promise<void> {
  const changeData = change.data;

  const buildWhere = (cols: { col: DrizzleColumn; prop: string }[]) => {
    const missing: string[] = [];
    const whereConditions = cols.map((pk) => {
      const value = changeData[pk.prop];
      if (value === undefined || value === null) {
        missing.push(pk.prop);
      }
      return eq(pk.col, value);
    });
    return { whereConditions, missing };
  };

  // Prefer primary key columns for deletes; fall back to conflict key columns.
  let { whereConditions, missing } = buildWhere(primaryKeyCols);
  if (missing.length > 0) {
    const fallback = buildWhere(conflictKeyCols);
    if (fallback.missing.length === 0) {
      whereConditions = fallback.whereConditions;
      missing = [];
    }
  }

  if (missing.length > 0) {
    throw new Error(
      `Missing delete key(s) for ${tableName}: ${missing.join(", ")} (rowId=${change.rowId})`
    );
  }

  if (hasDeletedFlag(tableName)) {
    // Soft delete: set deleted = true
    await tx
      .update(table)
      .set({
        [CORE_COL_DELETED]: true,
        [CORE_COL_LAST_MODIFIED_AT]: change.lastModifiedAt,
      })
      .where(and(...whereConditions));
  } else {
    // Hard delete
    await tx.delete(table).where(and(...whereConditions));
  }
}

async function applyUpsert(
  tx: Transaction,
  table: DynamicPgTable,
  pkCols: { col: DrizzleColumn; prop: string }[],
  data: Record<string, unknown>,
  opts?: {
    /**
     * Properties to omit from the UPDATE set (still inserted on first write).
     * This is critical when upserting on a non-PK unique key.
     */
    omitSetProps?: readonly string[];
  }
): Promise<void> {
  const targetCols = pkCols.map((pk) => pk.col);

  let setData: Record<string, unknown> = data;
  const omit = opts?.omitSetProps ?? [];
  if (omit.length > 0) {
    setData = { ...data };
    for (const prop of omit) {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
      delete setData[prop];
    }
  }

  await tx.insert(table).values(data).onConflictDoUpdate({
    target: targetCols,
    set: setData,
  });
}

/**
 * Process all PUSH changes from the client.
 */
async function processPushChanges(
  tx: Transaction,
  ctx: SyncContext,
  changes: IncomingSyncChange[]
): Promise<void> {
  debug.log(`[PUSH] Processing ${changes.length} changes from client`);
  for (const change of changes) {
    try {
      await applyChange(tx, change, ctx);
    } catch (e) {
      throw new Error(
        `[PUSH] Failed applying ${change.table} rowId=${change.rowId}: ${formatDbError(e)}`
      );
    }
  }
  debug.log(`[PUSH] Completed processing ${changes.length} changes`);
}

// ============================================================================
// PULL: Gather Changes for Client
// ============================================================================

/**
 * Convert a Postgres row to a SyncChange for the client.
 */
function rowToSyncChange(
  tableName: string,
  rowId: string,
  row: Record<string, unknown>
): SyncChange {
  // Normalize timestamps generically (schema-driven).
  let data = normalizeRowForSync(tableName, row);

  // Convert booleans for SQLite
  data = postgresToSqlite(tableName, data);

  return {
    table: tableName,
    rowId,
    data,
    deleted: !!data.deleted,
    lastModifiedAt:
      toOptionalString(data.lastModifiedAt) ?? new Date(0).toISOString(),
  };
}

// ============================================================================
// PULL: Initial Sync (Full Table Scan)
// ============================================================================

async function fetchTableForInitialSyncPage(
  tx: Transaction,
  tableName: string,
  ctx: SyncContext,
  syncStartedAt: string,
  offset: number,
  limit: number
): Promise<InitialPullPage> {
  const startedAt = Date.now();
  const table = getSchemaTables()[tableName];
  if (!table) return { changes: [], diagnostics: [] };

  const t = table;
  const meta = TABLE_REGISTRY[tableName];
  if (!meta) return { changes: [], diagnostics: [] };

  // Check if table uses RPC for filtering
  const rule = getPullRule(tableName);
  if (rule?.kind === "rpc") {
    // Use RPC to fetch data (handles all filtering server-side)
    const rpcParams = resolveRpcParamsForRule({
      rule,
      ctx,
      lastSyncAt: null,
      pageLimit: limit,
      pageOffset: offset,
    });

    const queryStartedAt = Date.now();
    const rows = await fetchViaRPC(tx, rule.functionName, rpcParams);
    const queryDurationMs = Date.now() - queryStartedAt;

    debug.log(
      `[PULL:INITIAL] ${tableName} (RPC): fetched ${rows.length} rows via ${rule.functionName}`
    );

    const transformStartedAt = Date.now();
    const changes: SyncChange[] = [];
    for (const row of rows) {
      // Apply adapter transformation: Postgres (snake_case) -> Client (camelCase)
      const transformed = normalizeRowForSync(tableName, row);
      const rowId = extractRowId(tableName, transformed, t);
      changes.push(rowToSyncChange(tableName, rowId, transformed));
    }
    const transformDurationMs = Date.now() - transformStartedAt;
    const totalDurationMs = Date.now() - startedAt;
    const timing = createInitialPageTiming({
      tableName,
      ruleKind: `rpc:${rule.functionName}`,
      offset,
      limit,
      rows: changes.length,
      queryDurationMs,
      transformDurationMs,
      totalDurationMs,
      payloadBytes: estimatePayloadBytes(changes),
    });
    return {
      changes,
      diagnostics: [createInitialPageDiagnostics(timing)],
      timing,
    };
  }

  // Standard SQL-based fetch (non-RPC tables)
  const conditions = buildUserFilter({
    tableName,
    table: t,
    userId: ctx.userId,
    collections: ctx.collections,
  });
  if (conditions === null) {
    debug.log(
      `[PULL:INITIAL] Skipping ${tableName} (no matching rows for user)`
    );
    const timing = createInitialPageTiming({
      tableName,
      ruleKind: "skipped:no-filter-match",
      offset,
      limit,
      rows: 0,
      queryDurationMs: 0,
      transformDurationMs: 0,
      totalDurationMs: Date.now() - startedAt,
      payloadBytes: 0,
    });
    return {
      changes: [],
      diagnostics: [createInitialPageDiagnostics(timing)],
      timing,
    };
  }

  const whereConditions: unknown[] = [...conditions];

  // Make a best-effort snapshot for multi-page initial sync.
  // If the table has lastModifiedAt, only include rows up to syncStartedAt.
  if (t.lastModifiedAt) {
    whereConditions.push(lte(t.lastModifiedAt, syncStartedAt));
  }

  let query = tx.select().from(table);
  if (whereConditions.length > 0) {
    // @ts-expect-error - dynamic where
    query = query.where(and(...whereConditions));
  }

  const queryStartedAt = Date.now();
  const rows = await query.limit(limit).offset(offset);
  const queryDurationMs = Date.now() - queryStartedAt;

  debug.log(
    `[PULL:INITIAL] ${tableName}: fetched page rows=${rows.length} offset=${offset} limit=${limit}`
  );

  const transformStartedAt = Date.now();
  const changes: SyncChange[] = [];
  for (const row of rows) {
    const r = row;
    const rowId = extractRowId(tableName, r, t);
    changes.push(rowToSyncChange(tableName, rowId, r));
  }
  const transformDurationMs = Date.now() - transformStartedAt;
  const totalDurationMs = Date.now() - startedAt;
  const timing = createInitialPageTiming({
    tableName,
    ruleKind: rule?.kind ?? "default",
    offset,
    limit,
    rows: changes.length,
    queryDurationMs,
    transformDurationMs,
    totalDurationMs,
    payloadBytes: estimatePayloadBytes(changes),
  });
  return {
    changes,
    diagnostics: [createInitialPageDiagnostics(timing)],
    timing,
  };
}

async function processInitialSyncPaged(
  tx: Transaction,
  ctx: SyncContext,
  cursorRaw: string | undefined,
  syncStartedAtHint: string | undefined,
  pageSizeHint: number | undefined,
  pageCountHint: number | undefined
): Promise<{
  changes: SyncChange[];
  nextCursor?: string;
  syncStartedAt: string;
  diagnostics: string[];
}> {
  const { pageSize, pageCount } = getInitialSyncPageBudget(
    pageSizeHint,
    pageCountHint
  );
  const startState = getInitialSyncStartState({
    cursorRaw,
    syncStartedAtHint,
    now: ctx.now,
  });
  let { tableIndex, offset } = startState;
  const { syncStartedAt } = startState;
  const allChanges: SyncChange[] = [];
  const diagnostics: string[] = [];
  const timings: InitialPageTiming[] = [];
  const timingAggregates = new Map<string, InitialTimingAggregate>();
  let pagesCollected = 0;

  // Advance until we fill the requested page budget or finish all tables.
  while (tableIndex < SYNCABLE_TABLES.length && pagesCollected < pageCount) {
    const tableName = SYNCABLE_TABLES[tableIndex];
    if (ctx.pullTables && !ctx.pullTables.has(tableName)) {
      tableIndex += 1;
      offset = 0;
      continue;
    }
    const pageStartedAt = Date.now();
    const page = await fetchTableForInitialSyncPage(
      tx,
      tableName,
      ctx,
      syncStartedAt,
      offset,
      pageSize
    );
    const { changes } = page;
    const pageDurationMs = Date.now() - pageStartedAt;
    perfLogDuration(
      ctx.perfDebugEnabled,
      ctx.perfMinDurationMs,
      pageDurationMs,
      `initial.page relation=${tableName} kind=${getRelationKind(tableName)} offset=${offset} limit=${pageSize} rows=${changes.length}`
    );
    if (ctx.diagnosticsEnabled) {
      diagnostics.push(...page.diagnostics);
      if (page.timing) {
        timings.push(page.timing);
        addInitialTimingAggregate(timingAggregates, page.timing);
      }
    }

    if (changes.length === 0) {
      // Either table is empty / skipped OR we've paged past the end.
      // Move to next table.
      tableIndex += 1;
      offset = 0;
      continue;
    }

    const nextOffset = offset + changes.length;
    const isLastPageForTable = changes.length < pageSize;
    allChanges.push(...changes);
    pagesCollected += 1;

    if (isLastPageForTable) {
      tableIndex += 1;
      offset = 0;
      continue;
    }

    offset = nextOffset;
  }

  if (ctx.diagnosticsEnabled) {
    diagnostics.push(...createInitialKindDiagnostics(timingAggregates));
    const topRelations = createInitialRelationTopDiagnostics(timings);
    if (topRelations) {
      diagnostics.push(topRelations);
    }
  }

  return {
    changes: allChanges,
    syncStartedAt,
    nextCursor: encodeNextInitialSyncCursor(
      { tableIndex, offset },
      syncStartedAt
    ),
    diagnostics,
  };
}

// ============================================================================
// PULL: Incremental Sync (Table-Level Change Log)
// ============================================================================

/**
 * Get list of tables that have changed since lastSyncAt.
 * sync_change_log now has ONE ROW PER TABLE (table_name is the primary key).
 */
async function getChangedTables(
  tx: Transaction,
  lastSyncAt: string
): Promise<string[]> {
  const syncChangeLog = getSchemaTables().sync_change_log;
  if (!syncChangeLog) {
    throw new Error("sync_change_log table not found in worker schema");
  }
  const tableNameCol = syncChangeLog.tableName;
  const changedAtCol = syncChangeLog.changedAt;
  if (!tableNameCol || !changedAtCol) {
    throw new Error("sync_change_log is missing required columns");
  }
  const entries = await tx
    .select({
      tableName: tableNameCol,
    })
    .from(syncChangeLog)
    .where(gt(changedAtCol, lastSyncAt));

  const tables = entries.map((entry) => String(entry.tableName));
  debug.log(
    `[PULL:INCR] Tables changed since ${lastSyncAt}: [${tables.join(", ")}]`
  );
  return tables;
}

/**
 * Fetch all rows from a table that have changed since lastSyncAt.
 * Uses the table's last_modified_at column.
 */
async function fetchChangedRowsFromTable(
  tx: Transaction,
  tableName: string,
  lastSyncAt: string,
  ctx: SyncContext
): Promise<SyncChange[]> {
  const table = getSchemaTables()[tableName];
  if (!table) return [];

  const t = table;
  if (!t.lastModifiedAt) {
    debug.log(`[PULL:INCR] ${tableName} has no lastModifiedAt, skipping`);
    return []; // Table doesn't support incremental sync
  }

  // Check if table uses RPC for filtering
  const rule = getPullRule(tableName);
  if (rule?.kind === "rpc") {
    // Use RPC to fetch changed rows (handles all filtering server-side)
    const rpcParams = resolveRpcParamsForRule({
      rule,
      ctx,
      lastSyncAt,
      pageLimit: 1000,
      pageOffset: 0,
    });

    const rows = await fetchViaRPC(tx, rule.functionName, rpcParams);

    debug.log(
      `[PULL:INCR] ${tableName} (RPC): fetched ${rows.length} changed rows via ${rule.functionName} since ${lastSyncAt}`
    );

    const changes: SyncChange[] = [];
    for (const row of rows) {
      // Apply adapter transformation: Postgres (snake_case) -> Client (camelCase)
      const transformed = normalizeRowForSync(tableName, row);
      const rowId = extractRowId(tableName, transformed, t);
      changes.push(rowToSyncChange(tableName, rowId, transformed));
    }
    return changes;
  }

  // Standard SQL-based incremental sync (non-RPC tables)
  // Build conditions: last_modified_at > lastSyncAt AND user_filter
  const userConditions = buildUserFilter({
    tableName,
    table: t,
    userId: ctx.userId,
    collections: ctx.collections,
  });
  if (userConditions === null) {
    debug.log(`[PULL:INCR] Skipping ${tableName} (no matching rows for user)`);
    return [];
  }

  const timeCondition = gt(t.lastModifiedAt, lastSyncAt);
  const allConditions =
    userConditions.length > 0
      ? // @ts-expect-error - dynamic where conditions with unknown types
        and(timeCondition, ...userConditions)
      : timeCondition;

  const rows = await tx.select().from(table).where(allConditions);
  debug.log(
    `[PULL:INCR] ${tableName}: fetched ${rows.length} changed rows since ${lastSyncAt}`
  );

  // Convert rows to SyncChanges
  const changes: SyncChange[] = [];
  for (const row of rows) {
    const r = row;
    const rowId = extractRowId(tableName, r, t);
    changes.push(rowToSyncChange(tableName, rowId, r));
  }

  return changes;
}

/**
 * Incremental sync: fetch only rows that changed since lastSyncAt.
 * 1. Query sync_change_log for tables with changed_at > lastSyncAt
 * 2. For each changed table, query rows with last_modified_at > lastSyncAt
 */
async function processIncrementalSync(
  tx: Transaction,
  lastSyncAt: string,
  ctx: SyncContext
): Promise<SyncChange[]> {
  debug.log(
    `[PULL:INCR] Starting incremental sync for user ${ctx.userId} since ${lastSyncAt}`
  );
  const changedTables = await getChangedTables(tx, lastSyncAt);

  if (changedTables.length === 0) {
    debug.log(`[PULL:INCR] No tables changed since ${lastSyncAt}`);
    return [];
  }

  const allChanges: SyncChange[] = [];
  for (const tableName of changedTables) {
    // Only process tables we know about
    if (!SYNCABLE_TABLES.includes(tableName)) {
      debug.log(`[PULL:INCR] Skipping unknown table: ${tableName}`);
      continue;
    }
    if (ctx.pullTables && !ctx.pullTables.has(tableName)) {
      continue;
    }
    const tableChanges = await fetchChangedRowsFromTable(
      tx,
      tableName,
      lastSyncAt,
      ctx
    );
    allChanges.push(...tableChanges);
  }

  debug.log(
    `[PULL:INCR] Completed incremental sync: ${allChanges.length} total changes`
  );
  return allChanges;
}

// ============================================================================
// MAIN SYNC HANDLER
// ============================================================================

type SyncType = "INITIAL" | "INCREMENTAL";

interface SyncTransactionResult {
  responseChanges: SyncChange[];
  nextCursor?: string;
  syncStartedAt?: string;
  diagnostics?: string[];
}

function getCursorSummary(payload: SyncRequest): string {
  if (!payload.pullCursor) {
    return "cursor=none";
  }

  try {
    const cursor = decodeCursor(payload.pullCursor);
    return `tableIndex=${cursor.tableIndex} offset=${cursor.offset}`;
  } catch {
    return "cursor=invalid";
  }
}

function addStartDiagnostics(
  diag: string[],
  payload: SyncRequest,
  syncType: SyncType
): void {
  diag.push(
    `[WorkerSyncDiag] type=${syncType} changesIn=${payload.changes.length} lastSyncAt=${payload.lastSyncAt ?? "null"} pageSize=${payload.pageSize ?? "null"} initialPageCount=${payload.initialPageCount ?? "null"} ${getCursorSummary(payload)}`
  );
}

function addResponseDiagnostics(
  diag: string[],
  responseChanges: SyncChange[],
  nextCursor: string | undefined,
  syncStartedAt: string | undefined
): void {
  const tableCounts: Record<string, number> = {};
  for (const change of responseChanges) {
    tableCounts[change.table] = (tableCounts[change.table] ?? 0) + 1;
  }
  const top = Object.entries(tableCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([tableName, count]) => `${tableName}:${count}`)
    .join(",");
  diag.push(
    `[WorkerSyncDiag] changesOut=${responseChanges.length} payloadBytes=${estimatePayloadBytes(responseChanges)} nextCursor=${nextCursor ? "yes" : "no"} syncStartedAt=${syncStartedAt ?? "null"} topRelations=${top || "(none)"}`
  );
}

function applyCollectionOverrides(
  collections: Record<string, Set<string>>,
  overrides: SyncRequest["collectionsOverride"]
): void {
  if (!overrides) {
    return;
  }

  for (const [collectionName, values] of Object.entries(overrides)) {
    const asArray = Array.isArray(values) ? values : [];
    collections[collectionName] = new Set(asArray.map(String));
  }
}

function buildRpcParamOverrides(
  overrides: SyncRequest["rpcParamOverrides"]
): Record<string, Record<string, unknown>> | undefined {
  if (!overrides) {
    return undefined;
  }

  return Object.fromEntries(
    Object.entries(overrides).map(([functionName, mapping]) => [
      functionName,
      mapping ?? {},
    ])
  );
}

function buildPullTables(
  pullTables: SyncRequest["pullTables"]
): Set<string> | undefined {
  return pullTables ? new Set(pullTables.map(String)) : undefined;
}

function logPullTableMode(pullTables: Set<string> | undefined): void {
  if (pullTables) {
    debug.log(
      `[Worker] 🚨 pullTables override received: ${Array.from(pullTables).join(", ")}`
    );
    return;
  }

  debug.log("[Worker] 🚨 No pullTables override - syncing all tables");
}

async function loadCollectionsForSync(
  tx: Transaction,
  authUserId: string,
  payload: SyncRequest,
  perfDebugEnabled: boolean,
  perfMinDurationMs: number,
  diag?: string[]
): Promise<Record<string, Set<string>>> {
  const collectionsStartedAt = Date.now();
  const collections = await loadUserCollections({
    tx,
    userId: authUserId,
    tables: getSchemaTables(),
  });
  perfLogDuration(
    perfDebugEnabled,
    perfMinDurationMs,
    Date.now() - collectionsStartedAt,
    "sync.collections"
  );
  diag?.push(
    `[WorkerSyncDiag] collections loadMs=${Date.now() - collectionsStartedAt}`
  );
  applyCollectionOverrides(collections, payload.collectionsOverride);
  if (diag && payload.collectionsOverride) {
    diag.push(
      `[WorkerSyncDiag] collections overrideKeys=${Object.keys(payload.collectionsOverride).join(",") || "(none)"}`
    );
  }
  return collections;
}

async function reloadCollectionsAfterPush(params: {
  ctx: SyncContext;
  tx: Transaction;
  authUserId: string;
  payload: SyncRequest;
  diagnosticsEnabled: boolean;
  diag: string[];
  perfDebugEnabled: boolean;
  perfMinDurationMs: number;
}): Promise<void> {
  if (params.payload.changes.length === 0) {
    if (params.diagnosticsEnabled) {
      params.diag.push(
        "[WorkerSyncDiag] collections reuseAfterPush=yes reason=no-changes"
      );
    }
    return;
  }

  params.ctx.collections = await loadCollectionsForSync(
    params.tx,
    params.authUserId,
    params.payload,
    params.perfDebugEnabled,
    params.perfMinDurationMs,
    params.diagnosticsEnabled ? params.diag : undefined
  );
}

async function processPullForSync(
  tx: Transaction,
  payload: SyncRequest,
  ctx: SyncContext,
  perfDebugEnabled: boolean,
  perfMinDurationMs: number
): Promise<SyncTransactionResult> {
  const pullStartedAt = Date.now();
  if (payload.lastSyncAt) {
    const responseChanges = await processIncrementalSync(
      tx,
      payload.lastSyncAt,
      ctx
    );
    perfLogDuration(
      perfDebugEnabled,
      perfMinDurationMs,
      Date.now() - pullStartedAt,
      `sync.pull.incremental changesOut=${responseChanges.length}`
    );
    return { responseChanges };
  }

  // Paginate initial sync to avoid oversized payloads / timeouts.
  const page = await processInitialSyncPaged(
    tx,
    ctx,
    payload.pullCursor,
    payload.syncStartedAt,
    payload.pageSize,
    payload.initialPageCount
  );
  perfLogDuration(
    perfDebugEnabled,
    perfMinDurationMs,
    Date.now() - pullStartedAt,
    `sync.pull.initial pageChanges=${page.changes.length} pageCount=${payload.initialPageCount ?? "default"} nextCursor=${page.nextCursor ? "yes" : "no"}`
  );
  return {
    responseChanges: page.changes,
    nextCursor: page.nextCursor,
    syncStartedAt: page.syncStartedAt,
    diagnostics: page.diagnostics,
  };
}

async function runSyncTransaction(params: {
  tx: Transaction;
  payload: SyncRequest;
  userId: string;
  now: string;
  diagnosticsEnabled: boolean;
  perfDebugEnabled: boolean;
  perfMinDurationMs: number;
  syncType: SyncType;
  diag: string[];
}): Promise<SyncTransactionResult> {
  const txStartedAt = Date.now();
  const authUserId = params.userId;
  const collections = await loadCollectionsForSync(
    params.tx,
    authUserId,
    params.payload,
    params.perfDebugEnabled,
    params.perfMinDurationMs,
    params.diagnosticsEnabled ? params.diag : undefined
  );
  const pullTables = buildPullTables(params.payload.pullTables);
  logPullTableMode(pullTables);

  const ctx: SyncContext = {
    userId: authUserId,
    authUserId,
    collections,
    rpcParamOverrides: buildRpcParamOverrides(params.payload.rpcParamOverrides),
    pullTables,
    now: params.now,
    diagnosticsEnabled: params.diagnosticsEnabled,
    perfDebugEnabled: params.perfDebugEnabled,
    perfMinDurationMs: params.perfMinDurationMs,
  };

  const pushStartedAt = Date.now();
  await processPushChanges(params.tx, ctx, params.payload.changes);
  perfLogDuration(
    params.perfDebugEnabled,
    params.perfMinDurationMs,
    Date.now() - pushStartedAt,
    `sync.push changes=${params.payload.changes.length}`
  );

  // Reload collections after push so the pull phase sees any collection/ownership
  // changes written during the push (e.g. newly created rows in collection-backed tables).
  await reloadCollectionsAfterPush({
    ctx,
    tx: params.tx,
    authUserId,
    payload: params.payload,
    diagnosticsEnabled: params.diagnosticsEnabled,
    diag: params.diag,
    perfDebugEnabled: params.perfDebugEnabled,
    perfMinDurationMs: params.perfMinDurationMs,
  });

  const result = await processPullForSync(
    params.tx,
    params.payload,
    ctx,
    params.perfDebugEnabled,
    params.perfMinDurationMs
  );

  if (params.diagnosticsEnabled) {
    if (result.diagnostics) {
      params.diag.push(...result.diagnostics);
    }
    addResponseDiagnostics(
      params.diag,
      result.responseChanges,
      result.nextCursor,
      result.syncStartedAt
    );
  }

  // NO GARBAGE COLLECTION NEEDED!
  // sync_change_log now has at most ~20 rows (one per table)
  if (params.diagnosticsEnabled) {
    params.diag.push(
      `[WorkerSyncDiag] transaction totalMs=${Date.now() - txStartedAt}`
    );
  }
  perfLogDuration(
    params.perfDebugEnabled,
    params.perfMinDurationMs,
    Date.now() - txStartedAt,
    `sync.transaction type=${params.syncType}`
  );

  return result;
}

async function handleSync(
  db: ReturnType<typeof drizzle>,
  payload: SyncRequest,
  userId: string,
  diagnosticsEnabled: boolean,
  perfDebugEnabled: boolean,
  perfMinDurationMs: number,
  initialDiagnostics: string[] = []
): Promise<SyncResponse> {
  const now = new Date().toISOString();
  const diag: string[] = [...initialDiagnostics];
  let responseChanges: SyncChange[] = [];
  let nextCursor: string | undefined;
  let syncStartedAt: string | undefined;
  const syncType = payload.lastSyncAt ? "INCREMENTAL" : "INITIAL";
  const syncStartedAtMs = Date.now();

  perfLog(
    perfDebugEnabled,
    `sync.start type=${syncType} changesIn=${payload.changes.length} lastSyncAt=${payload.lastSyncAt ?? "null"} pageSize=${payload.pageSize ?? "null"} initialPageCount=${payload.initialPageCount ?? "null"} hasCursor=${payload.pullCursor ? "yes" : "no"}`
  );

  if (diagnosticsEnabled) {
    addStartDiagnostics(diag, payload, syncType);
  }

  debug.log(`[SYNC] === Starting ${syncType} sync for user ${userId} ===`);
  debug.log(
    `[SYNC] Request: lastSyncAt=${payload.lastSyncAt ?? "null"}, changes=${payload.changes.length}`
  );

  await db.transaction(async (tx) => {
    const result = await runSyncTransaction({
      tx,
      payload,
      userId,
      now,
      diagnosticsEnabled,
      perfDebugEnabled,
      perfMinDurationMs,
      syncType,
      diag,
    });
    responseChanges = result.responseChanges;
    nextCursor = result.nextCursor;
    syncStartedAt = result.syncStartedAt;
  });

  debug.log(
    `[SYNC] === Completed ${syncType} sync: returning ${responseChanges.length} changes, syncedAt=${now} ===`
  );

  perfLogDuration(
    perfDebugEnabled,
    perfMinDurationMs,
    Date.now() - syncStartedAtMs,
    `sync.complete type=${syncType} changesOut=${responseChanges.length} total`
  );
  if (diagnosticsEnabled) {
    diag.push(`[WorkerSyncDiag] sync totalMs=${Date.now() - syncStartedAtMs}`);
  }

  return {
    changes: responseChanges,
    syncedAt: now,
    nextCursor,
    syncStartedAt,
    debug: diagnosticsEnabled ? diag : undefined,
  };
}

// ============================================================================
// HTTP HANDLER
// ============================================================================

/**
 * Get appropriate CORS headers for the request origin.
 * Allows all origins for backward compatibility and ease of deployment.
 */
function getCorsHeaders(request: Request): Record<string, string> {
  const origin = request.headers.get("Origin") || "*";
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers":
      "Content-Type, Authorization, apikey, x-client-info",
    "Access-Control-Allow-Credentials": "true",
    Vary: "Origin",
    "Access-Control-Max-Age": "86400", // 24 hours
  };
}

function jsonResponse(
  data: unknown,
  corsHeaders: Record<string, string>,
  status = 200
): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function errorResponse(
  message: string,
  corsHeaders: Record<string, string>,
  status = 500
): Response {
  return jsonResponse({ error: message }, corsHeaders, status);
}

interface SyncPostRequestParams {
  request: Request;
  env: Env;
  corsHeaders: Record<string, string>;
  perfDebugEnabled: boolean;
  perfMinDurationMs: number;
  requestStartedAt: number;
}

function isSyncDiagnosticsEnabled(
  env: Env,
  userId: string,
  payload: SyncRequest
): boolean {
  return (
    payload.diagnostics === true &&
    env.SYNC_DIAGNOSTICS === "true" &&
    (!env.SYNC_DIAGNOSTICS_USER_ID || env.SYNC_DIAGNOSTICS_USER_ID === userId)
  );
}

async function authorizeSyncRequest(
  request: Request,
  env: Env,
  perfDebugEnabled: boolean,
  perfMinDurationMs: number
): Promise<{ userId: string | null; authDurationMs: number }> {
  const authStartedAt = Date.now();
  const userId = await verifyJwt(
    request,
    env.SUPABASE_JWT_SECRET,
    env.SUPABASE_URL
  );
  const authDurationMs = Date.now() - authStartedAt;
  perfLogDuration(
    perfDebugEnabled,
    perfMinDurationMs,
    authDurationMs,
    `http.sync.auth authorized=${userId ? "yes" : "no"}`
  );
  return { userId, authDurationMs };
}

async function parseSyncPayload(
  request: Request,
  perfDebugEnabled: boolean,
  perfMinDurationMs: number
): Promise<{ payload: SyncRequest; payloadParseDurationMs: number }> {
  const payloadStartedAt = Date.now();
  const payload: SyncRequest = await request.json();
  const payloadParseDurationMs = Date.now() - payloadStartedAt;
  perfLogDuration(
    perfDebugEnabled,
    perfMinDurationMs,
    payloadParseDurationMs,
    `http.sync.payload.parse changesIn=${payload.changes.length}`
  );
  return { payload, payloadParseDurationMs };
}

function appendHttpDiagnostics(
  response: SyncResponse,
  dbCloseDurationMs: number | undefined,
  handleSyncStartedAt: number,
  requestStartedAt: number
): void {
  response.debug ??= [];
  response.debug.push(
    `[WorkerSyncDiag] http dbCloseMs=${dbCloseDurationMs ?? "unknown"}`,
    `[WorkerSyncDiag] http handleMs=${Date.now() - handleSyncStartedAt} requestTotalMs=${Date.now() - requestStartedAt} responseBytesApprox=${estimateJsonBytes(response)}`
  );
}

async function runSyncWithDb(params: {
  env: Env;
  payload: SyncRequest;
  userId: string;
  diagnosticsEnabled: boolean;
  perfDebugEnabled: boolean;
  perfMinDurationMs: number;
  requestDiagnostics: string[];
}): Promise<{ response: SyncResponse; dbCloseDurationMs: number | undefined }> {
  const {
    env,
    payload,
    userId,
    diagnosticsEnabled,
    perfDebugEnabled,
    perfMinDurationMs,
    requestDiagnostics,
  } = params;
  debug.log(`[HTTP] Sync request parsed, calling handleSync`);
  const createDbStartedAt = Date.now();
  const { db, close } = createDb(env);
  const dbCreateDurationMs = Date.now() - createDbStartedAt;
  if (diagnosticsEnabled) {
    requestDiagnostics.push(
      `[WorkerSyncDiag] http dbCreateMs=${dbCreateDurationMs}`
    );
  }
  perfLogDuration(
    perfDebugEnabled,
    perfMinDurationMs,
    dbCreateDurationMs,
    "http.sync.db.create"
  );

  let dbCloseDurationMs: number | undefined;
  let response: SyncResponse;
  try {
    response = await handleSync(
      db,
      payload,
      userId,
      diagnosticsEnabled,
      perfDebugEnabled,
      perfMinDurationMs,
      requestDiagnostics
    );
  } finally {
    const closeStartedAt = Date.now();
    await close();
    dbCloseDurationMs = Date.now() - closeStartedAt;
    perfLogDuration(
      perfDebugEnabled,
      perfMinDurationMs,
      dbCloseDurationMs,
      "http.sync.db.close"
    );
  }
  return { response, dbCloseDurationMs };
}

async function handleSyncPostRequest(
  params: SyncPostRequestParams
): Promise<Response> {
  const {
    request,
    env,
    corsHeaders,
    perfDebugEnabled,
    perfMinDurationMs,
    requestStartedAt,
  } = params;
  debug.log(`[HTTP] POST /api/sync received`);
  perfLog(perfDebugEnabled, "http.sync.request.received");

  const { userId, authDurationMs } = await authorizeSyncRequest(
    request,
    env,
    perfDebugEnabled,
    perfMinDurationMs
  );
  if (!userId) {
    debug.log(`[HTTP] Unauthorized - JWT verification failed`);
    return errorResponse("Unauthorized", corsHeaders, 401);
  }

  const { payload, payloadParseDurationMs } = await parseSyncPayload(
    request,
    perfDebugEnabled,
    perfMinDurationMs
  );
  const diagnosticsEnabled = isSyncDiagnosticsEnabled(env, userId, payload);
  const requestDiagnostics = diagnosticsEnabled
    ? [
        `[WorkerSyncDiag] http authMs=${authDurationMs} payloadParseMs=${payloadParseDurationMs}`,
      ]
    : [];

  try {
    const handleSyncStartedAt = Date.now();
    const { response, dbCloseDurationMs } = await runSyncWithDb({
      env,
      payload,
      userId,
      diagnosticsEnabled,
      perfDebugEnabled,
      perfMinDurationMs,
      requestDiagnostics,
    });
    if (diagnosticsEnabled) {
      appendHttpDiagnostics(
        response,
        dbCloseDurationMs,
        handleSyncStartedAt,
        requestStartedAt
      );
    }
    perfLogDuration(
      perfDebugEnabled,
      perfMinDurationMs,
      Date.now() - handleSyncStartedAt,
      "http.sync.handle"
    );
    perfLogDuration(
      perfDebugEnabled,
      perfMinDurationMs,
      Date.now() - requestStartedAt,
      "http.sync.request.total"
    );
    debug.log(`[HTTP] Sync completed successfully`);
    return jsonResponse(response, corsHeaders);
  } catch (error) {
    perfLogDuration(
      perfDebugEnabled,
      perfMinDurationMs,
      Date.now() - requestStartedAt,
      "http.sync.request.failed"
    );
    console.error("[HTTP] Sync error:", error);
    return errorResponse(formatDbError(error), corsHeaders);
  }
}

async function fetch(request: Request, env: Env): Promise<Response> {
  // Initialize debug logging based on environment variable
  setDebugEnabled(env);
  const perfDebugEnabled = env.WORKER_DEBUG_PERF === "true";
  const perfMinDurationMs = parsePerfMinDurationMs(
    env.WORKER_DEBUG_PERF_MIN_MS
  );
  const requestStartedAt = Date.now();

  // Get CORS headers for this request (needed for all responses)
  const corsHeaders = getCorsHeaders(request);

  try {
    const url = new URL(request.url);

    // CORS preflight - handle immediately
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders,
      });
    }

    // Health check
    if (request.method === "GET" && url.pathname === "/health") {
      return new Response("OK", { status: 200, headers: corsHeaders });
    }

    // Sync endpoint
    if (request.method === "POST" && url.pathname === "/api/sync") {
      return await handleSyncPostRequest({
        request,
        env,
        corsHeaders,
        perfDebugEnabled,
        perfMinDurationMs,
        requestStartedAt,
      });
    }

    return new Response("Not Found", { status: 404, headers: corsHeaders });
  } catch (error) {
    console.error("[HTTP] Unhandled error:", error);
    return errorResponse("Internal Server Error", corsHeaders);
  }
}
