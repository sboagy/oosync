declare module "@supabase/supabase-js" {
  export type RealtimeSubscribeStatus =
    | "SUBSCRIBED"
    | "TIMED_OUT"
    | "CHANNEL_ERROR"
    | "CLOSED"
    | string;

  export interface RealtimePostgresChangesPayload<
    TRecord extends Record<string, unknown>,
  > {
    eventType: string;
    schema: string;
    table: string;
    commit_timestamp?: string;
    errors?: string[];
    new: TRecord;
    old: Partial<TRecord>;
  }

  export interface RealtimeChannel {
    on<TRecord extends Record<string, unknown>>(
      event: "postgres_changes",
      filter: {
        event: string;
        schema: string;
        table: string;
        filter?: string;
      },
      callback: (payload: RealtimePostgresChangesPayload<TRecord>) => void
    ): RealtimeChannel;
    subscribe(
      callback: (status: RealtimeSubscribeStatus) => void
    ): RealtimeChannel;
  }

  export interface Session {
    access_token: string;
  }

  export interface SupabaseClient {
    auth: {
      getSession(): Promise<{
        data: {
          session: Session | null;
        };
      }>;
    };
    channel(name: string): RealtimeChannel;
    removeChannel(channel: RealtimeChannel): Promise<unknown>;
  }
}

declare module "sql.js" {
  export interface SqlJsInitConfig {
    locateFile?: (file: string) => string;
    wasmBinary?: ArrayBuffer;
  }

  export interface Statement {
    bind(values?: unknown): boolean;
    step(): boolean;
    getAsObject(params?: unknown): Record<string, unknown>;
    free(): void;
  }

  export interface QueryExecResult {
    columns: string[];
    values: unknown[][];
  }

  export interface Database {
    exec(sql: string): QueryExecResult[];
    run(sql: string, params?: unknown): void;
    prepare(sql: string): Statement;
    export(): Uint8Array;
    close(): void;
  }

  export interface SqlJsStatic {
    Database: new (data?: Uint8Array | ArrayBuffer) => Database;
    HEAP8?: {
      buffer?: ArrayBufferLike;
    };
  }

  export default function initSqlJs(
    config?: SqlJsInitConfig
  ): Promise<SqlJsStatic>;
}
