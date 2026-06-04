import sqlite3InitModule from "@sqlite.org/sqlite-wasm";

export type SqliteCompatibleValue =
  | string
  | number
  | bigint
  | boolean
  | null
  | undefined
  | Uint8Array
  | Int8Array
  | ArrayBuffer;

export interface IRawSqliteExecResult {
  columns: string[];
  values: SqliteCompatibleValue[][];
}

export interface IRawSqliteStatement {
  run(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): void;
  bind(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): boolean;
  step(): boolean;
  get(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): SqliteCompatibleValue[];
  getAsObject(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): Record<string, SqliteCompatibleValue>;
  free(): void;
}

export interface IRawSqliteDatabase {
  run(
    sql: string,
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): void;
  exec(sql: string): IRawSqliteExecResult[];
  prepare(sql: string): IRawSqliteStatement;
  export(): Uint8Array;
  close(): void;
}

type Sqlite3Module = Awaited<ReturnType<typeof sqlite3InitModule>>;
type Sqlite3Oo1Database = InstanceType<Sqlite3Module["oo1"]["DB"]>;
type Sqlite3PreparedStatement = ReturnType<Sqlite3Oo1Database["prepare"]>;

export interface ISqliteWasmDebugInfo {
  hasModule: boolean;
  version?: string;
}

export interface ISqliteWasmModuleState {
  module: Sqlite3Module | null;
  initPromise: Promise<Sqlite3Module> | null;
}

function normalizeParams(
  params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
): SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue> | undefined {
  if (!params) return undefined;
  if (Array.isArray(params)) {
    return params.map((value) =>
      typeof value === "boolean" ? Number(value) : value
    );
  }

  return Object.fromEntries(
    Object.entries(params).map(([key, value]) => [
      key,
      typeof value === "boolean" ? Number(value) : value,
    ])
  );
}

function normalizeResultValue(value: unknown): SqliteCompatibleValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    value instanceof Uint8Array ||
    value instanceof Int8Array ||
    value instanceof ArrayBuffer
  ) {
    return value;
  }
  if (typeof value === "boolean") return Number(value);
  if (value === undefined) return null;
  return JSON.stringify(value);
}

function normalizeRow(row: unknown[]): SqliteCompatibleValue[] {
  return row.map((value) => normalizeResultValue(value));
}

function bindIfProvided(
  statement: Sqlite3PreparedStatement,
  params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
): void {
  const normalized = normalizeParams(params);
  if (Array.isArray(normalized) && normalized.length === 0) return;
  if (
    normalized &&
    !Array.isArray(normalized) &&
    Object.keys(normalized).length === 0
  ) {
    return;
  }
  if (normalized) {
    statement.bind(normalized);
  }
}

class SqliteWasmStatementAdapter implements IRawSqliteStatement {
  private readonly statement: Sqlite3PreparedStatement;

  constructor(statement: Sqlite3PreparedStatement) {
    this.statement = statement;
  }

  run(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): void {
    bindIfProvided(this.statement, params);
    this.statement.step();
    this.statement.reset(true);
  }

  bind(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): boolean {
    bindIfProvided(this.statement, params);
    return true;
  }

  step(): boolean {
    return this.statement.step();
  }

  get(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): SqliteCompatibleValue[] {
    bindIfProvided(this.statement, params);
    return normalizeRow(this.statement.get([]));
  }

  getAsObject(
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): Record<string, SqliteCompatibleValue> {
    bindIfProvided(this.statement, params);
    const row = this.statement.get({}) as Record<string, unknown>;
    return Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        normalizeResultValue(value),
      ])
    );
  }

  free(): void {
    this.statement.finalize();
  }
}

export class SqliteWasmDatabaseAdapter implements IRawSqliteDatabase {
  private readonly sqlite3: Sqlite3Module;
  private readonly db: Sqlite3Oo1Database;

  constructor(sqlite3: Sqlite3Module, db: Sqlite3Oo1Database) {
    this.sqlite3 = sqlite3;
    this.db = db;
  }

  run(
    sql: string,
    params?: SqliteCompatibleValue[] | Record<string, SqliteCompatibleValue>
  ): void {
    if (params) {
      const statement = this.prepare(sql);
      try {
        statement.run(params);
      } finally {
        statement.free();
      }
      return;
    }
    this.db.exec(sql);
  }

  exec(sql: string): IRawSqliteExecResult[] {
    const columns: string[] = [];
    const rows = this.db.exec({
      sql,
      rowMode: "array",
      returnValue: "resultRows",
      columnNames: columns,
    });
    if (rows.length === 0 && columns.length === 0) return [];
    return [{ columns, values: rows.map((row) => normalizeRow(row)) }];
  }

  prepare(sql: string): IRawSqliteStatement {
    return new SqliteWasmStatementAdapter(this.db.prepare(sql));
  }

  export(): Uint8Array {
    return this.sqlite3.capi.sqlite3_js_db_export(this.db);
  }

  close(): void {
    this.db.close();
  }
}

export async function initSqliteWasm(): Promise<Sqlite3Module> {
  return await sqlite3InitModule();
}

export function createSqliteWasmDatabase(
  sqlite3: Sqlite3Module,
  data?: Uint8Array
): IRawSqliteDatabase {
  const db = new sqlite3.oo1.DB(":memory:");
  if (data && data.length > 0) {
    const dataPointer = sqlite3.wasm.allocFromTypedArray(data);
    const flags =
      sqlite3.capi.SQLITE_DESERIALIZE_FREEONCLOSE |
      sqlite3.capi.SQLITE_DESERIALIZE_RESIZEABLE;
    const result = sqlite3.capi.sqlite3_deserialize(
      db,
      "main",
      dataPointer,
      data.byteLength,
      data.byteLength,
      flags
    );
    if (result !== sqlite3.capi.SQLITE_OK) {
      sqlite3.wasm.dealloc(dataPointer);
      throw new Error(
        `Failed to deserialize SQLite database: ${sqlite3.capi.sqlite3_js_rc_str(result)}`
      );
    }
  }
  return new SqliteWasmDatabaseAdapter(sqlite3, db);
}
