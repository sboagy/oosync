/**
 * Shared Sync Types
 *
 * Defines the API contract between the Client (SQLite) and the Worker (Postgres).
 * These types ensure that both sides agree on the structure of the sync payload
 * and response, preventing protocol mismatches.
 */

export interface SyncChange<TTableName extends string = string> {
  table: TTableName;
  rowId: string; // UUID or JSON composite key
  data: Record<string, unknown>; // The full row data
  deleted: boolean;
  lastModifiedAt: string; // ISO timestamp from client
}

export type SyncCollectionsOverride = Record<string, string[]>;

export type SyncRpcParamOverrides = Record<string, Record<string, unknown>>;

export interface SyncRequestOverrides {
  /** Optional per-request collection values keyed by collection name. */
  collectionsOverride?: SyncCollectionsOverride;
  /** Optional per-request RPC param overrides keyed by RPC function name. */
  rpcParamOverrides?: SyncRpcParamOverrides;
  /** Optional allowlist of tables to pull for this sync. */
  pullTables?: string[];
}

export interface SyncRequest<TTableName extends string = string> {
  changes: Array<SyncChange<TTableName>>;
  lastSyncAt?: string; // ISO timestamp of last successful sync
  schemaVersion: number;

  /**
   * Optional cursor for paginated pulls (primarily initial sync).
   * When present, the worker returns a page of changes plus `nextCursor`.
   */
  pullCursor?: string;

  /**
   * Watermark for a multi-page initial sync. The worker sets this on the first
   * page, and the client echoes it back on subsequent pages.
   */
  syncStartedAt?: string;

  /**
   * Optional page size hint for paginated pull responses.
   * The worker may clamp this to a safe maximum.
   */
  pageSize?: number;

  /**
   * Optional number of initial-sync table pages to coalesce into one response.
   * The worker may clamp this to a safe maximum. Omit or set to 1 for the
   * legacy one-table-page-per-response behavior.
   */
  initialPageCount?: number;

  /** Optional per-request collection values keyed by collection name. */
  collectionsOverride?: SyncCollectionsOverride;
  /** Optional per-request RPC param overrides keyed by RPC function name. */
  rpcParamOverrides?: SyncRpcParamOverrides;
  /** Optional allowlist of tables to pull for this sync. */
  pullTables?: string[];

  /**
   * Optional request-scoped diagnostics hint. Workers may ignore this unless
   * diagnostics are allowed by environment/configuration.
   */
  diagnostics?: boolean;
}

export interface SyncResponse<TTableName extends string = string> {
  changes: Array<SyncChange<TTableName>>;
  syncedAt: string; // ISO timestamp of this sync
  error?: string;
  /**
   * Human-readable diagnostic lines emitted only when diagnostics are enabled.
   * These lines are not part of sync correctness and may change between
   * versions; consumers should log them rather than parse them for behavior.
   */
  debug?: string[];

  /**
   * When present, indicates there are more pull pages to fetch.
   * Client should call `/api/sync` again with `pullCursor` (and same `syncStartedAt`).
   */
  nextCursor?: string;

  /**
   * Watermark for a multi-page initial sync.
   * Client should use this as its first `lastSyncAt` once pagination is complete.
   */
  syncStartedAt?: string;
}
