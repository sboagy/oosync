import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import postgres from "postgres";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_OUTPUT_SQLITE_SCHEMA_FILE = path.join(
  __dirname,
  "../../drizzle/schema-sqlite.generated.ts"
);

const DEFAULT_OUTPUT_TABLE_META_FILE = path.join(
  __dirname,
  "../../shared/generated/sync/table-meta.generated.ts"
);

const DEFAULT_OUTPUT_WORKER_PG_SCHEMA_FILE = path.join(
  __dirname,
  "../../worker/src/generated/schema-postgres.generated.ts"
);

const DEFAULT_OUTPUT_WORKER_CONFIG_FILE = path.join(
  __dirname,
  "../../worker/src/generated/worker-config.generated.ts"
);

const LOCAL_SUPABASE_DATABASE_URL =
  "postgresql://postgres:postgres@127.0.0.1:54322/postgres";

function resolveBiomeBin(): string {
  const binDir = path.join(__dirname, "../../node_modules/.bin");
  const binName = process.platform === "win32" ? "biome.cmd" : "biome";
  const localBin = path.join(binDir, binName);
  return fs.existsSync(localBin) ? localBin : binName;
}

function formatWithBiome(targetPath: string, content: string): string {
  const biomeBin = resolveBiomeBin();
  const tempDir = fs.mkdtempSync(path.join(process.cwd(), ".codegen-biome-"));
  const tempFile = path.join(
    tempDir,
    `format${path.extname(targetPath) || ".ts"}`
  );

  try {
    fs.writeFileSync(tempFile, content, "utf8");
    const result = spawnSync(biomeBin, ["format", "--write", tempFile], {
      encoding: "utf8",
      stdio: "pipe",
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      const stderr = result.stderr ? String(result.stderr) : "";
      throw new Error(
        `Biome format failed for ${targetPath}: ${stderr || "unknown error"}`
      );
    }

    const formatted = fs.readFileSync(tempFile, "utf8");
    if (content.length > 0 && formatted.length === 0) {
      throw new Error(
        `Biome format produced empty output for ${targetPath} with non-empty input.`
      );
    }

    return formatted;
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

function resolveDrizzleKitBin(): string {
  const binDir = path.join(__dirname, "../../node_modules/.bin");
  const binName =
    process.platform === "win32" ? "drizzle-kit.cmd" : "drizzle-kit";
  const localBin = path.join(binDir, binName);
  return fs.existsSync(localBin) ? localBin : binName;
}

function runDrizzleKitGenerate(drizzleConfigPath: string): void {
  const bin = resolveDrizzleKitBin();
  const result = spawnSync(bin, ["generate", "--config", drizzleConfigPath], {
    encoding: "utf8",
    stdio: "inherit",
    cwd: process.cwd(),
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const configExists = fs.existsSync(drizzleConfigPath);
    throw new Error(
      `drizzle-kit generate failed (exit ${result.status}). Config: ${drizzleConfigPath}${
        configExists ? "" : " (file not found)"
      }. Check the drizzle-kit output above for details.`
    );
  }
}

function isLocalSupabaseDatabaseUrl(databaseUrl: string): boolean {
  try {
    const url = new URL(databaseUrl);
    const hostname = url.hostname;
    const port = url.port;

    const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost";
    return isLocalHost && port === "54322";
  } catch {
    return false;
  }
}

function getDefaultDatabaseUrl(): string {
  // Prefer an explicit env var to avoid accidentally introspecting a remote DB.
  const oosyncUrl = process.env.OOSYNC_DATABASE_URL;
  if (oosyncUrl) return oosyncUrl;

  // Only trust DATABASE_URL if it points at local Supabase.
  const databaseUrl = process.env.DATABASE_URL;
  if (databaseUrl && isLocalSupabaseDatabaseUrl(databaseUrl))
    return databaseUrl;

  return LOCAL_SUPABASE_DATABASE_URL;
}

const DEFAULT_DATABASE_URL = getDefaultDatabaseUrl();

interface IArgs {
  check: boolean;
  databaseUrl: string;
  schema: string;
  strict: boolean;
  configPath: string | null;
}

interface ITableMetaCore {
  primaryKey: string | string[];
  uniqueKeys: string[] | null;
  timestamps: string[];
  booleanColumns: string[];
  supportsIncremental: boolean;
  hasDeletedFlag: boolean;
}

type ChangeCategory = string | null;

interface ICodegenConfigFile {
  outputs?: {
    sqliteSchemaFile?: string;
    /** Optional wrapper around the generated SQLite schema with sync runtime tables. */
    sqliteSchemaWrapperFile?: string;
    /** Optional browser SQLite WASM client wrapper. */
    sqliteClientFile?: string;
    tableMetaFile?: string;
    /** Generates/overwrites shared/table-meta.ts (consumer-owned). */
    appTableMetaFile?: string;
    /** Optional app runtime-config wrapper that injects browser SQLite runtime into oosync. */
    syncRuntimeConfigFile?: string;
    workerPgSchemaFile?: string;
    workerConfigFile?: string;
    /** Optional worker entrypoint that injects generated artifacts into oosync. */
    workerEntrypointFile?: string;
    /**
     * Path to a drizzle.config.*.ts file for the SQLite target.
     * When set, codegen runs `drizzle-kit generate --config <path>` after
     * writing the SQLite schema, producing the incremental .sql migration file.
     */
    sqliteDrizzleConfig?: string;
  };
  tableMeta?: {
    /** Legacy whitelist (prefer excludeTables). */
    syncableTables?: string[];
    /** Legacy full registry (prefer overrides). */
    tableRegistryCore?: Record<string, ITableMetaCore>;

    /** Opinionated default: all tables w/ PK are syncable unless excluded. */
    excludeTables?: string[];

    /** Per-table overrides layered over inferred metadata. */
    overrides?: Record<string, Partial<ITableMetaCore>>;

    /** UI-only hint; defaults to null. */
    changeCategoryByTable?: Record<string, ChangeCategory>;

    /** Optional datetime normalization per table (snake_case column names). */
    normalizeDatetimeByTable?: Record<string, string[]>;

    /** Override tableName -> schema key (camelCase) mapping. */
    tableToSchemaKeyOverrides?: Record<string, string>;

    /** Override dependency sort ordering for specific tables. */
    tableSyncOrderOverrides?: Record<string, number>;
  };
  worker?: {
    /**
     * Worker-only, application-specific rules.
     * This is intentionally opaque to `oosync` itself.
     */
    config?: unknown;
  };
  browserSqlite?: {
    hooksModule: string;
    hooksExportName?: string;
    indexedDbName: string;
    indexedDbStore: string;
    dbKeyPrefix: string;
    dbVersionKeyPrefix: string;
    outboxBackupKeyPrefix: string;
    lastSyncTimestampKeyPrefix: string;
    schemaVersion: string;
    databaseVersion: number;
    migrationDirectory?: string;
    migrationFiles?: string[];
    forceResetQueryParams?: Array<{
      key: string;
      value?: string;
    }>;
    testApiWindowProperty?: string;
    clearInProgressWindowProperty?: string;
    persistWindowHookProperty?: string;
  };
}

interface IColumnRow {
  table_name: string;
  column_name: string;
  data_type: string;
  udt_name: string;
  is_nullable: "YES" | "NO";
  column_default: string | null;
  ordinal_position: number;
}

interface IConstraintColumnRow {
  table_name: string;
  constraint_name: string;
  column_name: string;
  position: number;
}

interface IIndexKeyRow {
  table_name: string;
  index_name: string;
  keydef: string;
  position: number;
}

interface IForeignKeyRow {
  table_name: string;
  constraint_name: string;
  column_name: string;
  ref_table_name: string;
  ref_column_name: string;
  position: number;
}

interface ITableCommentRow {
  table_name: string;
  comment: string | null;
}

interface IColumnCommentRow {
  table_name: string;
  column_name: string;
  comment: string | null;
}

interface IViewColumnCommentRow {
  view_name: string;
  column_name: string;
  comment: string | null;
}

const IGNORED_SCHEMA_TABLES = new Set<string>([
  "schema_migrations",
  "drizzle_migrations",
]);
const EMPTY_TABLE_META_OVERRIDE: Partial<ITableMetaCore> = Object.freeze({});
const EMPTY_TABLE_META_OVERRIDES: Record<
  string,
  Partial<ITableMetaCore>
> = Object.freeze({});
const EMPTY_TABLE_REGISTRY_CORE: Record<string, ITableMetaCore> = Object.freeze(
  {}
);
const EMPTY_CHANGE_CATEGORY_BY_TABLE: Record<string, ChangeCategory> =
  Object.freeze({});
const EMPTY_NORMALIZE_DATETIME_BY_TABLE: Record<string, string[]> =
  Object.freeze({});
const EMPTY_STRING_RECORD: Record<string, string> = Object.freeze({});
const EMPTY_NUMBER_RECORD: Record<string, number> = Object.freeze({});

function parseArgs(argv: string[]): IArgs {
  const check = argv.includes("--check");
  const strict = !argv.includes("--lenient");

  const schemaArg = argv.find((a) => a.startsWith("--schema="));
  const schema = schemaArg ? schemaArg.split("=", 2)[1] : "public";

  const urlArg = argv.find((a) => a.startsWith("--databaseUrl="));
  const databaseUrl = urlArg ? urlArg.split("=", 2)[1] : DEFAULT_DATABASE_URL;

  const configArg = argv.find((a) => a.startsWith("--config="));
  const configPath = configArg ? configArg.split("=", 2)[1] : null;

  return { check, databaseUrl, schema, strict, configPath };
}

function createHeader(params: { schema: string }): string {
  return `/**
 * AUTO-GENERATED FILE — DO NOT EDIT.
 *
 * Source: Postgres catalogs (schema: ${params.schema})
 * Generated by: oosync/src/codegen-schema.ts
 */\n\n`;
}

function toImportPath(params: { fromFile: string; toFile: string }): string {
  const relativePath = path
    .relative(path.dirname(params.fromFile), params.toFile)
    .split(path.sep)
    .join("/");
  const withoutExtension = relativePath.replace(/\.[^.]+$/, "");
  return withoutExtension.startsWith(".")
    ? withoutExtension
    : `./${withoutExtension}`;
}

function toAbsoluteFromCwd(filePath: string): string {
  return path.isAbsolute(filePath)
    ? filePath
    : path.join(process.cwd(), filePath);
}

function toWebAssetPath(filePath: string): string {
  return `/${path.relative(process.cwd(), filePath).split(path.sep).join("/")}`;
}

function listSqliteMigrationFiles(directoryPath: string): string[] {
  if (!fs.existsSync(directoryPath)) {
    return [];
  }
  return fs
    .readdirSync(directoryPath)
    .filter((fileName) => fileName.endsWith(".sql"))
    .sort((left, right) => left.localeCompare(right))
    .map((fileName) => toWebAssetPath(path.join(directoryPath, fileName)));
}

function toCamelCase(value: string): string {
  return value.replace(/_([a-z0-9])/g, (_, c: string) => c.toUpperCase());
}

function safeIdentifier(raw: string): string {
  const candidate = toCamelCase(raw);
  if (/^[a-zA-Z_$][\w$]*$/.test(candidate)) return candidate;
  return `t_${candidate.replace(/[^\w$]/g, "_")}`;
}

function normalizeType(params: { dataType: string; udtName: string }): string {
  const dt = params.dataType.toLowerCase();
  const udt = params.udtName.toLowerCase();

  if (dt === "user-defined") return udt; // enums and domains
  if (dt.startsWith("timestamp")) return "timestamp";
  if (dt === "timestamp with time zone") return "timestamptz";
  if (dt === "uuid") return "uuid";
  if (dt === "boolean") return "boolean";
  if (dt === "json" || dt === "jsonb") return dt;
  if (dt === "date") return "date";
  if (dt === "time without time zone") return "time";
  if (dt === "time with time zone") return "timetz";

  // numbers
  if (dt === "integer" || dt === "smallint" || dt === "bigint") return "int";
  if (dt === "real" || dt === "double precision") return "real";
  if (dt === "numeric" || dt === "decimal") return "numeric";

  // text-ish
  if (
    dt === "text" ||
    dt === "character varying" ||
    dt === "character" ||
    dt === "citext"
  ) {
    return "text";
  }

  return dt;
}

function sqliteBuilderForPgType(pgType: string): "text" | "integer" | "real" {
  switch (pgType) {
    case "uuid":
    case "timestamp":
    case "timestamptz":
    case "date":
    case "time":
    case "timetz":
    case "json":
    case "jsonb":
    case "text":
      return "text";
    case "boolean":
    case "int":
      return "integer";
    case "real":
    case "numeric":
      return "real";
    default:
      // enums and other user-defined types: store as TEXT
      return "text";
  }
}

function isKnownPgType(pgType: string): boolean {
  return new Set([
    "uuid",
    "timestamp",
    "timestamptz",
    "date",
    "time",
    "timetz",
    "json",
    "jsonb",
    "text",
    "boolean",
    "int",
    "real",
    "numeric",
  ]).has(pgType);
}

function isTimestampLikePgType(pgType: string): boolean {
  return (
    pgType === "timestamp" ||
    pgType === "timestamptz" ||
    pgType === "date" ||
    pgType === "time" ||
    pgType === "timetz"
  );
}

type ParsedPgDefault =
  | { kind: "default"; value: string }
  | { kind: "$defaultFn"; value: string };

function parseUuidDefault(
  pgType: string,
  trimmed: string
): ParsedPgDefault | null {
  if (
    pgType === "uuid" &&
    /(gen_random_uuid\(\)|uuid_generate_v4\(\))/i.test(trimmed)
  ) {
    // Avoid any app imports in generated code.
    return { kind: "$defaultFn", value: "() => crypto.randomUUID()" };
  }
  return null;
}

function parseTimestampDefault(
  pgType: string,
  trimmed: string
): ParsedPgDefault | null {
  if (
    (pgType === "timestamp" || pgType === "timestamptz") &&
    /^(now\(\)|current_timestamp)$/i.test(trimmed)
  ) {
    return { kind: "$defaultFn", value: "() => new Date().toISOString()" };
  }
  return null;
}

function parseBooleanDefault(
  pgType: string,
  trimmed: string
): ParsedPgDefault | null {
  if (pgType !== "boolean") return null;
  if (/^false(\b|::)/i.test(trimmed) || /^'false'::/i.test(trimmed)) {
    return { kind: "default", value: "false" };
  }
  if (/^true(\b|::)/i.test(trimmed) || /^'true'::/i.test(trimmed)) {
    return { kind: "default", value: "true" };
  }
  if (/^'f'::/i.test(trimmed)) return { kind: "default", value: "false" };
  if (/^'t'::/i.test(trimmed)) return { kind: "default", value: "true" };
  return null;
}

function parseNumericDefault(
  pgType: string,
  trimmed: string
): ParsedPgDefault | null {
  if (pgType !== "int" && pgType !== "real" && pgType !== "numeric") {
    return null;
  }
  const match = /^(-?\d+(?:\.\d+)?)(?:::.*|\b)$/.exec(trimmed);
  return match ? { kind: "default", value: match[1] } : null;
}

function parseStringLiteralDefault(
  pgType: string,
  trimmed: string
): ParsedPgDefault | null {
  if (pgType !== "text" && pgType !== "uuid") return null;
  if (!trimmed.startsWith("'")) return null;

  let literal = "";
  for (let i = 1; i < trimmed.length; i += 1) {
    const char = trimmed[i];
    if (char !== "'") {
      literal += char;
      continue;
    }

    if (trimmed[i + 1] === "'") {
      literal += "'";
      i += 1;
      continue;
    }

    const suffix = trimmed.slice(i + 1);
    if (suffix !== "" && !suffix.startsWith("::")) return null;
    return { kind: "default", value: JSON.stringify(literal) };
  }

  return null;
}

function parsePgDefault(params: {
  pgType: string;
  columnDefault: string | null;
}): ParsedPgDefault | null {
  const def = params.columnDefault;
  if (!def) return null;

  const trimmed = def.trim();
  const parsed =
    parseUuidDefault(params.pgType, trimmed) ??
    parseTimestampDefault(params.pgType, trimmed) ??
    parseBooleanDefault(params.pgType, trimmed) ??
    parseNumericDefault(params.pgType, trimmed) ??
    parseStringLiteralDefault(params.pgType, trimmed);
  if (parsed) return parsed;

  // Unknown/defaults we don't confidently map: skip.
  return null;
}

function parseIndexKeyToColumnName(keydef: string): string | null {
  // Typical outputs: "col", "col DESC", "col" COLLATE "C"
  const match = /^"?([a-zA-Z_][\w$]*)"?/.exec(keydef.trim());
  if (!match) return null;
  return match[1];
}

function stableSort<T>(items: T[], key: (item: T) => string): T[] {
  return [...items].sort((a, b) => key(a).localeCompare(key(b)));
}

function sortInPlaceByLocale<T>(items: T[], key: (item: T) => string): void {
  items.sort((left, right) => key(left).localeCompare(key(right)));
}

function getSchemaTableNames(columns: IColumnRow[]): string[] {
  return stableSort(
    [...groupByKey(columns, (column) => column.table_name).keys()],
    (tableName) => tableName
  ).filter((tableName) => !IGNORED_SCHEMA_TABLES.has(tableName));
}

function createTableIdentifierMap(tables: string[]): Map<string, string> {
  const tableIdentByName = new Map<string, string>();
  const usedIdents = new Set<string>();
  for (const tableName of tables) {
    let ident = safeIdentifier(tableName);
    while (usedIdents.has(ident)) ident = `${ident}_`;
    usedIdents.add(ident);
    tableIdentByName.set(tableName, ident);
  }
  return tableIdentByName;
}

function getSortedRowsByPosition<T extends { position: number }>(
  rows: T[] | undefined
): T[] {
  return [...(rows ?? [])].sort(
    (left, right) => left.position - right.position
  );
}

function getSortedTableColumns(
  columnsByTable: Map<string, IColumnRow[]>,
  tableName: string
): IColumnRow[] {
  return [...(columnsByTable.get(tableName) ?? [])].sort(
    (left, right) => left.ordinal_position - right.ordinal_position
  );
}

function resolveConfigPath(params: { cliPath: string | null }): string | null {
  if (params.cliPath) return params.cliPath;
  const envPath = process.env.OOSYNC_CODEGEN_CONFIG;
  if (envPath) return envPath;

  const defaultPath = path.join(process.cwd(), "oosync.codegen.config.json");
  if (fs.existsSync(defaultPath)) return defaultPath;

  return null;
}

function loadCodegenConfig(configPath: string | null): ICodegenConfigFile {
  if (!configPath) return {};
  const abs = path.isAbsolute(configPath)
    ? configPath
    : path.join(process.cwd(), configPath);
  const raw = fs.readFileSync(abs, "utf8");
  const parsed: unknown = JSON.parse(raw);
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error(`Invalid config file (expected object): ${abs}`);
  }
  return parsed;
}

function createTableMetaInterfaceLines(): string[] {
  return [
    "export interface TableMetaCore {",
    "  primaryKey: string | string[];",
    "  uniqueKeys: string[] | null;",
    "  timestamps: string[];",
    "  booleanColumns: string[];",
    "  supportsIncremental: boolean;",
    "  hasDeletedFlag: boolean;",
    "  columnDescriptions?: Record<string, string>;",
    "}",
    "",
  ];
}

function formatStringArrayLiteral(values: string[]): string {
  return `[${values.map((value) => JSON.stringify(value)).join(", ")}]`;
}

function assertColumnsExist(params: {
  tableName: string;
  label: string;
  columns: string[];
  colSet: Set<string>;
}): void {
  for (const column of params.columns) {
    if (!params.colSet.has(column)) {
      throw new Error(
        `${params.label} column not found: ${params.tableName}.${column}`
      );
    }
  }
}

function getUniqueCandidates(params: {
  tableName: string;
  uniqueByTableConstraint: Map<string, IConstraintColumnRow[]>;
}): string[][] {
  return [...params.uniqueByTableConstraint.entries()]
    .filter(([key]) => key.startsWith(`${params.tableName}::`))
    .map(([, rows]) =>
      [...rows]
        .sort((left, right) => left.position - right.position)
        .map((row) => row.column_name)
    );
}

function uniqueKeysMatchConstraint(params: {
  tableName: string;
  uniqueKeys: string[];
  uniqueByTableConstraint: Map<string, IConstraintColumnRow[]>;
}): boolean {
  return getUniqueCandidates({
    tableName: params.tableName,
    uniqueByTableConstraint: params.uniqueByTableConstraint,
  }).some(
    (columns) =>
      columns.length === params.uniqueKeys.length &&
      columns.every((column, index) => column === params.uniqueKeys[index])
  );
}

function uniqueKeysMatchCompositePrimaryKey(core: ITableMetaCore): boolean {
  const primaryKey = core.primaryKey;
  const uniqueKeys = core.uniqueKeys;
  if (!Array.isArray(primaryKey) || !uniqueKeys) return false;
  return (
    uniqueKeys.length === primaryKey.length &&
    uniqueKeys.every((column, index) => column === primaryKey[index])
  );
}

function assertUniqueKeysValid(params: {
  tableName: string;
  core: ITableMetaCore;
  uniqueByTableConstraint: Map<string, IConstraintColumnRow[]>;
}): void {
  if (!params.core.uniqueKeys) return;
  if (
    uniqueKeysMatchConstraint({
      tableName: params.tableName,
      uniqueKeys: params.core.uniqueKeys,
      uniqueByTableConstraint: params.uniqueByTableConstraint,
    }) ||
    uniqueKeysMatchCompositePrimaryKey(params.core)
  ) {
    return;
  }

  throw new Error(
    `uniqueKeys for ${params.tableName} does not match any UNIQUE constraint: [${params.core.uniqueKeys.join(", ")}]`
  );
}

function assertTableMetaCoreValid(params: {
  tableName: string;
  core: ITableMetaCore;
  colSet: Set<string>;
  uniqueByTableConstraint: Map<string, IConstraintColumnRow[]>;
}): void {
  const primaryKeyColumns = Array.isArray(params.core.primaryKey)
    ? params.core.primaryKey
    : [params.core.primaryKey];
  assertColumnsExist({
    tableName: params.tableName,
    label: "Primary key",
    columns: primaryKeyColumns,
    colSet: params.colSet,
  });
  if (params.core.uniqueKeys) {
    assertColumnsExist({
      tableName: params.tableName,
      label: "Unique key",
      columns: params.core.uniqueKeys,
      colSet: params.colSet,
    });
  }
  assertColumnsExist({
    tableName: params.tableName,
    label: "Timestamp",
    columns: params.core.timestamps,
    colSet: params.colSet,
  });
  assertColumnsExist({
    tableName: params.tableName,
    label: "Boolean",
    columns: params.core.booleanColumns,
    colSet: params.colSet,
  });

  // If supportsIncremental is true, enforce presence of last_modified_at.
  if (
    params.core.supportsIncremental &&
    !params.colSet.has("last_modified_at")
  ) {
    throw new Error(
      `supportsIncremental=true but last_modified_at missing: ${params.tableName}.last_modified_at`
    );
  }

  assertUniqueKeysValid({
    tableName: params.tableName,
    core: params.core,
    uniqueByTableConstraint: params.uniqueByTableConstraint,
  });
}

function tableMetaCoreToLines(params: {
  tableName: string;
  core: ITableMetaCore;
  columnDescriptions: Record<string, string> | undefined;
}): string[] {
  const pkLiteral = Array.isArray(params.core.primaryKey)
    ? formatStringArrayLiteral(params.core.primaryKey)
    : JSON.stringify(params.core.primaryKey);
  const uniqueLiteral = params.core.uniqueKeys
    ? formatStringArrayLiteral(params.core.uniqueKeys)
    : "null";
  const tsLiteral = formatStringArrayLiteral(params.core.timestamps);
  const boolLiteral = formatStringArrayLiteral(params.core.booleanColumns);

  const columnDescriptionLines =
    params.columnDescriptions &&
    Object.keys(params.columnDescriptions).length > 0
      ? [
          `    columnDescriptions: ${JSON.stringify(
            Object.fromEntries(
              Object.entries(params.columnDescriptions).sort((left, right) =>
                left[0].localeCompare(right[0])
              )
            ),
            null,
            2
          )},`,
        ]
      : [];

  return [
    `  ${JSON.stringify(params.tableName)}: {`,
    `    primaryKey: ${pkLiteral},`,
    `    uniqueKeys: ${uniqueLiteral},`,
    `    timestamps: ${tsLiteral},`,
    `    booleanColumns: ${boolLiteral},`,
    `    supportsIncremental: ${params.core.supportsIncremental ? "true" : "false"},`,
    `    hasDeletedFlag: ${params.core.hasDeletedFlag ? "true" : "false"},`,
    ...columnDescriptionLines,
    "  },",
  ];
}

function buildTableMetaTs(params: {
  schema: string;
  columns: IColumnRow[];
  primaryKeys: IConstraintColumnRow[];
  uniqueConstraints: IConstraintColumnRow[];
  strict: boolean;
  syncableTables: string[];
  tableRegistryCore: Record<string, ITableMetaCore>;
  columnDescriptionsByTable: Record<string, Record<string, string>>;
}): string {
  const colsByTable = groupByKey(params.columns, (c) => c.table_name);
  const uniqueByTableConstraint = groupByKey(
    params.uniqueConstraints,
    (r) => `${r.table_name}::${r.constraint_name}`
  );

  const availableTables = new Set(colsByTable.keys());
  const missingTables = params.syncableTables.filter(
    (t) => !availableTables.has(t)
  );
  if (params.strict && missingTables.length > 0) {
    throw new Error(
      `Missing tables for sync metadata: ${missingTables.join(", ")}`
    );
  }

  const tableRegistryLines = params.syncableTables.flatMap((tableName) => {
    const core = params.tableRegistryCore[tableName];
    if (!core) {
      throw new Error(`Missing tableRegistryCore entry for ${tableName}`);
    }
    const tableCols = colsByTable.get(tableName) ?? [];
    const colSet = new Set(tableCols.map((c) => c.column_name));

    if (params.strict) {
      assertTableMetaCoreValid({
        tableName,
        core,
        colSet,
        uniqueByTableConstraint,
      });
    }

    return tableMetaCoreToLines({
      tableName,
      core,
      columnDescriptions: params.columnDescriptionsByTable[tableName],
    });
  });

  const lines: string[] = [
    createHeader({ schema: params.schema }),
    ...createTableMetaInterfaceLines(),
    "export const SYNCABLE_TABLES = [",
    ...params.syncableTables.map(
      (tableName) => `  ${JSON.stringify(tableName)},`
    ),
    "] as const;",
    "",
    "export type SyncableTableName = (typeof SYNCABLE_TABLES)[number];",
    "",
    "export const TABLE_REGISTRY_CORE: Record<SyncableTableName, TableMetaCore> = {",
    ...tableRegistryLines,
    "};",
    "",
  ];

  return lines.join("\n").replace(/\n{3,}/g, "\n\n");
}

async function introspect(params: {
  databaseUrl: string;
  schema: string;
}): Promise<{
  columns: IColumnRow[];
  primaryKeys: IConstraintColumnRow[];
  uniqueConstraints: IConstraintColumnRow[];
  indexes: IIndexKeyRow[];
  foreignKeys: IForeignKeyRow[];
  tableComments: ITableCommentRow[];
  columnComments: IColumnCommentRow[];
  viewColumnComments: IViewColumnCommentRow[];
}> {
  const sql = postgres(params.databaseUrl, {
    prepare: false,
    max: 1,
  });

  try {
    const columns = await sql<IColumnRow[]>`
      select
        c.table_name,
        c.column_name,
        c.data_type,
        c.udt_name,
        c.is_nullable,
        c.column_default,
        c.ordinal_position
      from information_schema.columns c
      join information_schema.tables t
        on t.table_schema = c.table_schema
        and t.table_name = c.table_name
      where c.table_schema = ${params.schema}
        and t.table_type = 'BASE TABLE'
      order by c.table_name, c.ordinal_position;
    `;

    const primaryKeys = await sql<IConstraintColumnRow[]>`
      select
        cl.relname as table_name,
        con.conname as constraint_name,
        a.attname as column_name,
        k.ordinality as position
      from pg_constraint con
      join pg_class cl on cl.oid = con.conrelid
      join pg_namespace n on n.oid = cl.relnamespace
      join unnest(con.conkey) with ordinality as k(attnum, ordinality) on true
      join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.attnum
      where con.contype = 'p'
        and n.nspname = ${params.schema}
      order by cl.relname, con.conname, k.ordinality;
    `;

    const uniqueConstraints = await sql<IConstraintColumnRow[]>`
      select
        cl.relname as table_name,
        con.conname as constraint_name,
        a.attname as column_name,
        k.ordinality as position
      from pg_constraint con
      join pg_class cl on cl.oid = con.conrelid
      join pg_namespace n on n.oid = cl.relnamespace
      join unnest(con.conkey) with ordinality as k(attnum, ordinality) on true
      join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.attnum
      where con.contype = 'u'
        and n.nspname = ${params.schema}
      order by cl.relname, con.conname, k.ordinality;
    `;

    const indexes = await sql<IIndexKeyRow[]>`
      select
        t.relname as table_name,
        ix.relname as index_name,
        pg_get_indexdef(i.indexrelid, k.n, true) as keydef,
        k.n as position
      from pg_index i
      join pg_class t on t.oid = i.indrelid
      join pg_namespace n on n.oid = t.relnamespace
      join pg_class ix on ix.oid = i.indexrelid
      join generate_series(1, i.indnkeyatts) as k(n) on true
      where n.nspname = ${params.schema}
        and i.indisprimary = false
        and i.indisunique = false
      order by t.relname, ix.relname, k.n;
    `;

    const foreignKeys = await sql<IForeignKeyRow[]>`
      select
        cl.relname as table_name,
        con.conname as constraint_name,
        src.attname as column_name,
        refcl.relname as ref_table_name,
        refatt.attname as ref_column_name,
        srccols.ordinality as position
      from pg_constraint con
      join pg_class cl on cl.oid = con.conrelid
      join pg_namespace n on n.oid = cl.relnamespace
      join pg_class refcl on refcl.oid = con.confrelid
      join unnest(con.conkey) with ordinality as srccols(attnum, ordinality) on true
      join pg_attribute src on src.attrelid = con.conrelid and src.attnum = srccols.attnum
      join unnest(con.confkey) with ordinality as refcols(attnum, ordinality) on refcols.ordinality = srccols.ordinality
      join pg_attribute refatt on refatt.attrelid = con.confrelid and refatt.attnum = refcols.attnum
      where con.contype = 'f'
        and n.nspname = ${params.schema}
      order by cl.relname, con.conname, srccols.ordinality;
    `;

    const tableComments = await sql<ITableCommentRow[]>`
      select
        cl.relname as table_name,
        d.description as comment
      from pg_class cl
      join pg_namespace n on n.oid = cl.relnamespace
      left join pg_description d
        on d.objoid = cl.oid
        and d.objsubid = 0
      where n.nspname = ${params.schema}
        and cl.relkind = 'r'
      order by cl.relname;
    `;

    const columnComments = await sql<IColumnCommentRow[]>`
      select
        cl.relname as table_name,
        a.attname as column_name,
        d.description as comment
      from pg_attribute a
      join pg_class cl on cl.oid = a.attrelid
      join pg_namespace n on n.oid = cl.relnamespace
      left join pg_description d
        on d.objoid = a.attrelid
        and d.objsubid = a.attnum
      where n.nspname = ${params.schema}
        and cl.relkind = 'r'
        and a.attnum > 0
        and not a.attisdropped
      order by cl.relname, a.attnum;
    `;

    const viewColumnComments = await sql<IViewColumnCommentRow[]>`
      select
        cl.relname as view_name,
        a.attname as column_name,
        d.description as comment
      from pg_attribute a
      join pg_class cl on cl.oid = a.attrelid
      join pg_namespace n on n.oid = cl.relnamespace
      left join pg_description d
        on d.objoid = a.attrelid
        and d.objsubid = a.attnum
      where n.nspname = ${params.schema}
        and cl.relkind in ('v', 'm')
        and a.attnum > 0
        and not a.attisdropped
      order by cl.relname, a.attnum;
    `;

    return {
      columns,
      primaryKeys,
      uniqueConstraints,
      indexes,
      foreignKeys,
      tableComments,
      columnComments,
      viewColumnComments,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function groupByKey<T>(items: T[], key: (item: T) => string): Map<string, T[]> {
  const map = new Map<string, T[]>();
  for (const item of items) {
    const k = key(item);
    const arr = map.get(k);
    if (arr) arr.push(item);
    else map.set(k, [item]);
  }
  return map;
}

function getAllTableNames(columns: IColumnRow[]): string[] {
  return stableSort([...new Set(columns.map((c) => c.table_name))], (t) => t);
}

function getTableColumns(columns: IColumnRow[]): Map<string, IColumnRow[]> {
  const byTable = groupByKey(columns, (c) => c.table_name);
  for (const [t, cols] of byTable.entries()) {
    byTable.set(
      t,
      [...cols].sort((a, b) => a.ordinal_position - b.ordinal_position)
    );
  }
  return byTable;
}

function getPrimaryKeyByTable(
  primaryKeys: IConstraintColumnRow[]
): Map<string, string[]> {
  const byTable = groupByKey(primaryKeys, (r) => r.table_name);
  const result = new Map<string, string[]>();
  for (const [t, rows] of byTable.entries()) {
    result.set(
      t,
      [...rows]
        .sort((a, b) => a.position - b.position)
        .map((r) => r.column_name)
    );
  }
  return result;
}

function getUniqueConstraintsByTable(
  unique: IConstraintColumnRow[]
): Map<string, string[][]> {
  const byTableConstraint = groupByKey(
    unique,
    (r) => `${r.table_name}::${r.constraint_name}`
  );
  const keys = stableSort([...byTableConstraint.keys()], (k) => k);
  const byTable = new Map<string, string[][]>();
  for (const k of keys) {
    const tableName = k.split("::", 1)[0];
    const rows = byTableConstraint.get(k) ?? [];
    const cols = [...rows]
      .sort((a, b) => a.position - b.position)
      .map((r) => r.column_name);
    const arr = byTable.get(tableName);
    if (arr) arr.push(cols);
    else byTable.set(tableName, [cols]);
  }
  return byTable;
}

function chooseUniqueKeys(params: {
  pkCols: string[];
  uniqueCandidates: string[][];
}): string[] | null {
  if (params.pkCols.length > 1) return params.pkCols;

  const candidates = params.uniqueCandidates
    .filter((cols) => cols.length >= 2)
    .sort((a, b) => {
      if (a.length !== b.length) return a.length - b.length;
      return a.join(",").localeCompare(b.join(","));
    });
  return candidates.length > 0 ? candidates[0] : null;
}

function inferTableMetaCore(params: {
  columnsByTable: Map<string, IColumnRow[]>;
  pkByTable: Map<string, string[]>;
  uniqueByTable: Map<string, string[][]>;
  overrides: Record<string, Partial<ITableMetaCore>>;
}): Record<string, ITableMetaCore> {
  const registry: Record<string, ITableMetaCore> = {};

  for (const [tableName, cols] of params.columnsByTable.entries()) {
    const pkCols = params.pkByTable.get(tableName) ?? [];
    if (pkCols.length === 0) continue;

    const normalizedCols = cols.map((c) => ({
      name: c.column_name,
      pgType: normalizeType({ dataType: c.data_type, udtName: c.udt_name }),
      nullable: c.is_nullable === "YES",
    }));

    const timestamps = normalizedCols
      .filter((c) => isTimestampLikePgType(c.pgType))
      .map((c) => c.name);

    const booleanColumns = normalizedCols
      .filter((c) => c.pgType === "boolean")
      .map((c) => c.name);

    const supportsIncremental = normalizedCols.some(
      (c) => c.name === "last_modified_at"
    );

    const hasDeletedFlag = normalizedCols.some(
      (c) => c.name === "deleted" && c.pgType === "boolean"
    );

    const uniqueCandidates = params.uniqueByTable.get(tableName) ?? [];
    const uniqueKeys = chooseUniqueKeys({ pkCols, uniqueCandidates });

    const core: ITableMetaCore = {
      primaryKey: pkCols.length === 1 ? pkCols[0] : pkCols,
      uniqueKeys,
      timestamps,
      booleanColumns,
      supportsIncremental,
      hasDeletedFlag,
    };

    const override = params.overrides[tableName] ?? EMPTY_TABLE_META_OVERRIDE;
    registry[tableName] = { ...core, ...override };
  }

  return registry;
}

function inferSyncableTables(params: {
  allTables: string[];
  pkByTable: Map<string, string[]>;
  legacyWhitelist: string[] | null;
  excluded: Set<string>;
}): string[] {
  if (params.legacyWhitelist && params.legacyWhitelist.length > 0) {
    return stableSort([...params.legacyWhitelist], (t) => t);
  }

  return stableSort(
    params.allTables
      .filter((t) => !params.excluded.has(t))
      .filter((t) => (params.pkByTable.get(t) ?? []).length > 0),
    (t) => t
  );
}

function parseOosyncTableTags(comment: string | null): {
  exclude?: boolean;
  changeCategory?: ChangeCategory;
  normalizeDatetime?: string[];
  ownerColumn?: string;
} {
  if (!comment) return {};
  const exclude = /@oosync\.exclude\b/i.test(comment);

  const m = /@oosync\.changeCategory\s*=\s*([^\s]+)/i.exec(comment);
  const changeCategory: ChangeCategory = m?.[1] ?? null;

  const nm = /@oosync\.normalizeDatetime\s*=\s*([^\n\r]+)/i.exec(comment);
  const normalizeDatetime = nm
    ? nm[1]
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0)
    : undefined;

  const om = /@oosync\.ownerColumn\s*=\s*([^\s]+)/i.exec(comment);
  const ownerColumn = om?.[1] ?? undefined;

  return {
    exclude: exclude || undefined,
    changeCategory: m ? changeCategory : undefined,
    normalizeDatetime,
    ownerColumn,
  };
}

function buildTableToSchemaKeyMap(params: {
  tables: string[];
  overrides: Record<string, string>;
}): Record<string, string> {
  const result: Record<string, string> = {};
  for (const t of params.tables) {
    result[t] = params.overrides[t] ?? toCamelCase(t);
  }
  return result;
}

interface ITableDependencyGraph {
  deps: Map<string, Set<string>>;
  reverse: Map<string, Set<string>>;
}

function createDependencyGraph(tables: string[]): ITableDependencyGraph {
  const deps = new Map<string, Set<string>>();
  const reverse = new Map<string, Set<string>>();
  for (const tableName of tables) {
    deps.set(tableName, new Set());
    reverse.set(tableName, new Set());
  }
  return { deps, reverse };
}

function addForeignKeyDependency(params: {
  graph: ITableDependencyGraph;
  tableSet: Set<string>;
  foreignKey: IForeignKeyRow;
}): void {
  const fk = params.foreignKey;
  if (!params.tableSet.has(fk.table_name)) return;
  if (!params.tableSet.has(fk.ref_table_name)) return;

  params.graph.deps.get(fk.table_name)?.add(fk.ref_table_name);
  params.graph.reverse.get(fk.ref_table_name)?.add(fk.table_name);
}

function buildDependencyGraph(params: {
  tables: string[];
  foreignKeys: IForeignKeyRow[];
}): ITableDependencyGraph {
  const graph = createDependencyGraph(params.tables);
  const tableSet = new Set(params.tables);
  for (const foreignKey of params.foreignKeys) {
    addForeignKeyDependency({ graph, tableSet, foreignKey });
  }
  return graph;
}

function getInitialReadyTables(params: {
  tables: string[];
  deps: Map<string, Set<string>>;
  inDegree: Map<string, number>;
}): string[] {
  for (const tableName of params.tables) {
    const dependencies = params.deps.get(tableName);
    if (!dependencies) {
      throw new Error(`Missing dependency set for table: ${tableName}`);
    }
    params.inDegree.set(tableName, dependencies.size);
  }
  return stableSort(
    params.tables.filter((tableName) => params.inDegree.get(tableName) === 0),
    (tableName) => tableName
  );
}

function consumeReadyTables(params: {
  ready: string[];
  reverse: Map<string, Set<string>>;
  inDegree: Map<string, number>;
}): string[] {
  const ordered: string[] = [];
  while (params.ready.length > 0) {
    const tableName = params.ready.shift();
    if (!tableName) break;
    ordered.push(tableName);
    for (const child of params.reverse.get(tableName) ?? []) {
      const nextDegree = (params.inDegree.get(child) ?? 0) - 1;
      params.inDegree.set(child, nextDegree);
      if (nextDegree === 0) {
        params.ready.push(child);
        sortInPlaceByLocale(params.ready, (value) => value);
      }
    }
  }
  return ordered;
}

function appendUnorderedTables(params: {
  ordered: string[];
  tables: string[];
}): void {
  for (const tableName of stableSort(params.tables, (value) => value)) {
    if (!params.ordered.includes(tableName)) {
      params.ordered.push(tableName);
    }
  }
}

function buildTableSyncOrder(params: {
  tables: string[];
  foreignKeys: IForeignKeyRow[];
  overrides: Record<string, number>;
}): Record<string, number> {
  const graph = buildDependencyGraph(params);
  const inDegree = new Map<string, number>();
  const ready = getInitialReadyTables({
    tables: params.tables,
    deps: graph.deps,
    inDegree,
  });
  const ordered = consumeReadyTables({
    ready,
    reverse: graph.reverse,
    inDegree,
  });
  appendUnorderedTables({ ordered, tables: params.tables });

  const result: Record<string, number> = {};
  ordered.forEach((tableName, index) => {
    result[tableName] = index + 1;
  });
  for (const [tableName, order] of Object.entries(params.overrides)) {
    result[tableName] = order;
  }
  return result;
}

function buildNormalizeDatetimeFieldsLines(): string[] {
  return [
    "function normalizeDatetimeFields(row: Record<string, unknown>, fields: string[]): Record<string, unknown> {",
    "  const normalized = { ...row };",
    "  for (const field of fields) {",
    "    const value = normalized[field];",
    '    if (typeof value === "string") {',
    '      let result = value.includes(" ") ? value.replace(" ", "T") : value;',
    String.raw`      if (/Z$/i.test(result) || /[+-]\d{2}:?\d{2}$/.test(result)) {`,
    "        normalized[field] = result;",
    "        continue;",
    "      }",
    String.raw`      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(result)) {`,
    "        result = `${" + "result}Z`;",
    "      }",
    "      normalized[field] = result;",
    "    }",
    "  }",
    "  return normalized;",
    "}",
    "",
  ];
}

function buildAppTableMetaInterfaceLines(): string[] {
  return [
    "export type ChangeCategory = string | null;",
    "export type SyncableTableName = GeneratedSyncableTableName;",
    "",
    "export interface TableMeta {",
    "  primaryKey: string | string[];",
    "  uniqueKeys: string[] | null;",
    "  timestamps: string[];",
    "  booleanColumns: string[];",
    "  supportsIncremental: boolean;",
    "  hasDeletedFlag: boolean;",
    "  changeCategory: ChangeCategory;",
    "  normalize?: (row: Record<string, unknown>) => Record<string, unknown>;",
    "  columnDescriptions?: Record<string, string>;",
    "}",
    "",
    "export const SYNCABLE_TABLES = SYNCABLE_TABLES_GENERATED;",
    "",
    ...buildNormalizeDatetimeFieldsLines(),
  ];
}

function tableExtrasToLines(params: {
  tableName: string;
  category: ChangeCategory;
  datetimeFields: string[] | undefined;
  columnDescriptions: Record<string, string> | undefined;
}): string[] {
  const normalizeLines =
    params.datetimeFields && params.datetimeFields.length > 0
      ? [
          `    normalize: (row) => normalizeDatetimeFields(row, ${formatStringArrayLiteral(params.datetimeFields)}),`,
        ]
      : [];
  const descriptionLines = params.columnDescriptions
    ? tableColumnDescriptionsToLines(params.columnDescriptions)
    : [];

  return [
    `  ${JSON.stringify(params.tableName)}: {`,
    `    changeCategory: ${params.category === null ? "null" : JSON.stringify(params.category)},`,
    ...normalizeLines,
    ...descriptionLines,
    "  },",
  ];
}

function tableColumnDescriptionsToLines(
  columnDescriptions: Record<string, string>
): string[] {
  if (Object.keys(columnDescriptions).length === 0) return [];
  const sortedDescriptions = Object.fromEntries(
    Object.entries(columnDescriptions).sort((left, right) =>
      left[0].localeCompare(right[0])
    )
  );
  return [
    `    columnDescriptions: ${JSON.stringify(sortedDescriptions, null, 2)},`,
  ];
}

function buildTableExtrasLines(params: {
  syncableTables: string[];
  changeCategoryByTable: Record<string, ChangeCategory>;
  normalizeDatetimeByTable: Record<string, string[]>;
  columnDescriptionsByTable: Record<string, Record<string, string>>;
}): string[] {
  return [
    'const TABLE_EXTRAS: Record<SyncableTableName, Pick<TableMeta, "changeCategory" | "normalize" | "columnDescriptions">> = {',
    ...params.syncableTables.flatMap((tableName) =>
      tableExtrasToLines({
        tableName,
        category: params.changeCategoryByTable[tableName] ?? null,
        datetimeFields: params.normalizeDatetimeByTable[tableName],
        columnDescriptions: params.columnDescriptionsByTable[tableName],
      })
    ),
    "};",
    "",
  ];
}

function buildTableRegistryHelperLines(): string[] {
  return [
    "export const TABLE_REGISTRY_MERGED: Record<SyncableTableName, TableMeta> = Object.fromEntries(",
    "  Object.entries(TABLE_REGISTRY_CORE).map(([tableName, core]) => {",
    "    const extras = TABLE_EXTRAS[tableName as SyncableTableName];",
    "    return [tableName, { ...(core as TableMetaCore), ...extras }];",
    "  })",
    ") as Record<SyncableTableName, TableMeta>;",
    "",
    "export const TABLE_REGISTRY: Record<string, TableMeta> = TABLE_REGISTRY_MERGED;",
    "",
    "function getRequiredMeta(tableName: string): TableMeta {",
    "  const meta = TABLE_REGISTRY[tableName];",
    "  if (!meta) {",
    "    throw new Error(`Unknown table: ${" + "tableName}`);",
    "  }",
    "  return meta;",
    "}",
    "",
    "export const COMPOSITE_PK_TABLES: SyncableTableName[] = (() => {",
    "  const tables: SyncableTableName[] = [];",
    "  for (const tableName of SYNCABLE_TABLES) {",
    "    const pk = TABLE_REGISTRY_MERGED[tableName].primaryKey;",
    "    if (Array.isArray(pk)) {",
    "      tables.push(tableName);",
    "    }",
    "  }",
    "  return tables;",
    "})();",
    "",
    "export const NON_STANDARD_PK_TABLES: Partial<Record<SyncableTableName, string>> = (() => {",
    "  const map: Partial<Record<SyncableTableName, string>> = {};",
    "  for (const tableName of SYNCABLE_TABLES) {",
    "    const pk = TABLE_REGISTRY_MERGED[tableName].primaryKey;",
    '    if (typeof pk === "string" && pk !== "id") {',
    "      map[tableName] = pk;",
    "    }",
    "  }",
    "  return map;",
    "})();",
    "",
  ];
}

function buildAppTableMetaAccessorLines(): string[] {
  return [
    "export function getPrimaryKey(tableName: string): string | string[] {",
    "  const pk = getRequiredMeta(tableName).primaryKey;",
    "  return Array.isArray(pk) ? [...pk] : pk;",
    "}",
    "",
    "export function getUniqueKeys(tableName: string): string[] | null {",
    "  const uniqueKeys = getRequiredMeta(tableName).uniqueKeys;",
    "  return uniqueKeys ? [...uniqueKeys] : null;",
    "}",
    "",
    "export function getConflictTarget(tableName: string): string[] {",
    "  const meta = getRequiredMeta(tableName);",
    "  if (meta.uniqueKeys) return [...meta.uniqueKeys];",
    "",
    "  const pk = meta.primaryKey;",
    "  return Array.isArray(pk) ? [...pk] : [pk];",
    "}",
    "",
    "export function supportsIncremental(tableName: string): boolean {",
    "  return TABLE_REGISTRY[tableName]?.supportsIncremental ?? false;",
    "}",
    "",
    "export function hasDeletedFlag(tableName: string): boolean {",
    "  return TABLE_REGISTRY[tableName]?.hasDeletedFlag ?? false;",
    "}",
    "",
    "export function getBooleanColumns(tableName: string): string[] {",
    "  return [...(TABLE_REGISTRY[tableName]?.booleanColumns ?? [])];",
    "}",
    "",
    "export function getNormalizer(tableName: string):",
    "  | ((row: Readonly<Record<string, unknown>>) => Record<string, unknown>)",
    "  | undefined {",
    "  const normalize = TABLE_REGISTRY[tableName]?.normalize;",
    "  return normalize ? (row) => normalize(row as Record<string, unknown>) : undefined;",
    "}",
    "",
    "export function isRegisteredTable(tableName: string): boolean {",
    "  return tableName in TABLE_REGISTRY;",
    "}",
    "",
    "export function hasCompositePK(tableName: string): boolean {",
    "  return Array.isArray(TABLE_REGISTRY[tableName]?.primaryKey);",
    "}",
    "",
    "export function buildRowIdForOutbox(",
    "  tableName: string,",
    "  row: Readonly<Record<string, unknown>>",
    "): string {",
    "  const pk = getPrimaryKey(tableName);",
    "  if (Array.isArray(pk)) {",
    "    const keyObj: Record<string, unknown> = {};",
    "    for (const col of pk) {",
    "      keyObj[col] = row[col];",
    "    }",
    "    return JSON.stringify(keyObj);",
    "  }",
    "  return String(row[pk]);",
    "}",
    "",
    "export function parseOutboxRowId(",
    "  tableName: string,",
    "  rowId: string",
    "): Record<string, unknown> | string {",
    "  const pk = getPrimaryKey(tableName);",
    "  if (Array.isArray(pk)) {",
    "    try {",
    "      return JSON.parse(rowId) as Record<string, unknown>;",
    "    } catch {",
    "      throw new Error(`Invalid JSON row_id for composite key table ${" +
      "tableName}: ${" +
      "rowId}`);",
    "    }",
    "  }",
    "  return rowId;",
    "}",
    "",
  ];
}

function recordLiteralLines(params: {
  declaration: string;
  entries: [string, string | number][];
  valueToLiteral: (value: string | number) => string;
}): string[] {
  const sortedEntries = [...params.entries];
  sortInPlaceByLocale(sortedEntries, ([key]) => key);
  return [
    params.declaration,
    ...sortedEntries.map(
      ([key, value]) =>
        `  ${JSON.stringify(key)}: ${params.valueToLiteral(value)},`
    ),
    "};",
    "",
  ];
}

function buildAppTableMetaTs(params: {
  schema: string;
  generatedTableMetaImportPath: string;
  syncableTables: string[];
  changeCategoryByTable: Record<string, ChangeCategory>;
  normalizeDatetimeByTable: Record<string, string[]>;
  tableSyncOrder: Record<string, number>;
  tableToSchemaKey: Record<string, string>;
  columnDescriptionsByTable: Record<string, Record<string, string>>;
}): string {
  return [
    createHeader({ schema: params.schema }),
    "import {" +
      "\n  type SyncableTableName as GeneratedSyncableTableName," +
      "\n  SYNCABLE_TABLES as SYNCABLE_TABLES_GENERATED," +
      "\n  TABLE_REGISTRY_CORE," +
      "\n  type TableMetaCore," +
      `\n} from ${JSON.stringify(params.generatedTableMetaImportPath)};`,
    "",
    ...buildAppTableMetaInterfaceLines(),
    ...buildTableExtrasLines({
      syncableTables: params.syncableTables,
      changeCategoryByTable: params.changeCategoryByTable,
      normalizeDatetimeByTable: params.normalizeDatetimeByTable,
      columnDescriptionsByTable: params.columnDescriptionsByTable,
    }),
    ...buildTableRegistryHelperLines(),
    ...buildAppTableMetaAccessorLines(),
    ...recordLiteralLines({
      declaration: "export const TABLE_SYNC_ORDER: Record<string, number> = {",
      entries: Object.entries(params.tableSyncOrder),
      valueToLiteral: String,
    }),
    ...recordLiteralLines({
      declaration:
        "export const TABLE_TO_SCHEMA_KEY: Record<string, string> = {",
      entries: Object.entries(params.tableToSchemaKey),
      valueToLiteral: (value) => JSON.stringify(value),
    }),
  ]
    .join("\n")
    .replace(/\n{3,}/g, "\n\n");
}

type NumericCoercion = { prop: string; kind: "int" | "float" };
type WorkerConfigRecord = Record<string, unknown>;
type WorkerConfigRoot = WorkerConfigRecord & {
  collections?: WorkerConfigRecord;
  pull?: {
    tableRules?: Record<string, unknown>;
  };
  push?: {
    tableRules?: Record<string, unknown>;
  };
};
type SanitizeConfig = WorkerConfigRecord & {
  nullIfEmptyStringProps?: string[];
  coerceNumericProps?: NumericCoercion[];
};
type PushTableRuleConfig = WorkerConfigRecord & {
  sanitize?: SanitizeConfig;
};
type OwnerRuleKind = "eqUserId" | "orNullEqUserId";
type TableOwnerColumn = { column: string; kind: OwnerRuleKind };
type PullRule =
  | { kind: "eqUserId"; column: string }
  | { kind: "orNullEqUserId"; column: string }
  | { kind: "inCollection"; column: string; collection: string }
  | {
      kind: "rpc";
      functionName: string;
      paramMap: Record<
        string,
        | { source: "authUserId" }
        | { source: "collection"; collection: string }
        | { source: "lastSyncAt" }
        | { source: "pageLimit" }
        | { source: "pageOffset" }
        | { source: "literal"; value: unknown }
        | { source: "requestOverride"; key?: string }
      >;
    };
type PushRule = {
  denyDelete?: boolean;
  sanitize?: {
    coerceNumericProps?: NumericCoercion[];
  };
};

const EMPTY_WORKER_CONFIG_RECORD: WorkerConfigRecord = Object.freeze({});
const OWNER_COLUMN_CANDIDATES = [
  "user_ref",
  "user_id",
  "private_to_user",
  "private_for",
] as const;

function getOwnerRuleKind(params: {
  columnName: string;
  isNullable: boolean;
}): OwnerRuleKind {
  const isPrivate =
    params.columnName === "private_to_user" ||
    params.columnName === "private_for";
  return isPrivate || params.isNullable ? "orNullEqUserId" : "eqUserId";
}

function getNumericCoercionProps(columns: IColumnRow[]): NumericCoercion[] {
  const numericProps: NumericCoercion[] = [];
  for (const column of columns) {
    const pgType = normalizeType({
      dataType: column.data_type,
      udtName: column.udt_name,
    });
    if (pgType === "int") {
      numericProps.push({ prop: toCamelCase(column.column_name), kind: "int" });
    } else if (pgType === "real" || pgType === "numeric") {
      numericProps.push({
        prop: toCamelCase(column.column_name),
        kind: "float",
      });
    }
  }
  return stableSort(
    numericProps,
    (coercion) => `${coercion.kind}::${coercion.prop}`
  );
}

function getOwnerColumnFromName(params: {
  columnName: string;
  colByName: Map<string, IColumnRow>;
}): TableOwnerColumn | null {
  const column = params.colByName.get(params.columnName);
  if (!column) return null;
  return {
    column: params.columnName,
    kind: getOwnerRuleKind({
      columnName: params.columnName,
      isNullable: column.is_nullable === "YES",
    }),
  };
}

function inferOwnerColumn(params: {
  colByName: Map<string, IColumnRow>;
  overrideOwnerColumn: string | undefined;
}): TableOwnerColumn | null {
  if (params.overrideOwnerColumn) {
    const override = getOwnerColumnFromName({
      columnName: params.overrideOwnerColumn,
      colByName: params.colByName,
    });
    if (override) return override;
  }

  for (const columnName of OWNER_COLUMN_CANDIDATES) {
    const inferred = getOwnerColumnFromName({
      columnName,
      colByName: params.colByName,
    });
    if (inferred) return inferred;
  }
  return null;
}

function buildDefaultPushRule(params: {
  core: ITableMetaCore | undefined;
  numericProps: NumericCoercion[];
}): PushRule | null {
  const nextPushRule: PushRule = {};
  if (params.core && !params.core.hasDeletedFlag) {
    // Opinionated safety: deny hard deletes unless table has a deleted flag.
    nextPushRule.denyDelete = true;
  }
  if (params.numericProps.length > 0) {
    nextPushRule.sanitize = { coerceNumericProps: params.numericProps };
  }
  return Object.keys(nextPushRule).length > 0 ? nextPushRule : null;
}

function buildOwnerColumnsAndPushRules(params: {
  syncableTables: string[];
  columnsByTable: Map<string, IColumnRow[]>;
  tableRegistryCore: Record<string, ITableMetaCore>;
  ownerColumnOverrideByTable: Record<string, string>;
}): {
  tableOwnerColumn: Map<string, TableOwnerColumn>;
  pushRules: Record<string, PushRule>;
} {
  const tableOwnerColumn = new Map<string, TableOwnerColumn>();
  const pushRules: Record<string, PushRule> = {};

  for (const tableName of params.syncableTables) {
    const cols = params.columnsByTable.get(tableName) ?? [];
    const colByName = new Map(
      cols.map((column) => [column.column_name, column])
    );
    const owner = inferOwnerColumn({
      colByName,
      overrideOwnerColumn: params.ownerColumnOverrideByTable[tableName],
    });
    if (owner) tableOwnerColumn.set(tableName, owner);

    const pushRule = buildDefaultPushRule({
      core: params.tableRegistryCore[tableName],
      numericProps: getNumericCoercionProps(cols),
    });
    if (pushRule) pushRules[tableName] = pushRule;
  }

  return { tableOwnerColumn, pushRules };
}

function buildDefaultCollections(params: {
  tableOwnerColumn: Map<string, TableOwnerColumn>;
  tableRegistryCore: Record<string, ITableMetaCore>;
}): Record<string, { table: string; idColumn: string; ownerColumn: string }> {
  const collections: Record<
    string,
    { table: string; idColumn: string; ownerColumn: string }
  > = {};
  for (const [tableName, owner] of stableSort(
    [...params.tableOwnerColumn.entries()],
    ([tableName]) => tableName
  )) {
    const core = params.tableRegistryCore[tableName];
    if (!core || Array.isArray(core.primaryKey)) continue;
    collections[`${toCamelCase(tableName)}Ids`] = {
      table: tableName,
      idColumn: core.primaryKey,
      ownerColumn: owner.column,
    };
  }
  return collections;
}

function getCollectionPullRule(params: {
  tableName: string;
  foreignKeys: IForeignKeyRow[];
  tableOwnerColumn: Map<string, TableOwnerColumn>;
  tableRegistryCore: Record<string, ITableMetaCore>;
  collections: Record<string, unknown>;
}): PullRule | null {
  const candidates: Array<{ column: string; collection: string }> = [];
  for (const fk of params.foreignKeys) {
    const parentOwner = params.tableOwnerColumn.get(fk.ref_table_name);
    const parentCore = params.tableRegistryCore[fk.ref_table_name];
    if (!parentOwner || !parentCore || Array.isArray(parentCore.primaryKey)) {
      continue;
    }
    const collection = `${toCamelCase(fk.ref_table_name)}Ids`;
    if (!(collection in params.collections)) continue;
    candidates.push({ column: fk.column_name, collection });
  }
  const sortedCandidates = stableSort(
    candidates,
    (candidate) => `${candidate.collection}::${candidate.column}`
  );
  const candidate = sortedCandidates[0];
  return candidate
    ? {
        kind: "inCollection",
        column: candidate.column,
        collection: candidate.collection,
      }
    : null;
}

function buildDefaultPullRules(params: {
  syncableTables: string[];
  foreignKeys: IForeignKeyRow[];
  tableOwnerColumn: Map<string, TableOwnerColumn>;
  tableRegistryCore: Record<string, ITableMetaCore>;
  collections: Record<string, unknown>;
}): Record<string, PullRule> {
  const pullRules: Record<string, PullRule> = {};
  const fksByTable = groupByKey(params.foreignKeys, (fk) => fk.table_name);
  for (const tableName of params.syncableTables) {
    const owner = params.tableOwnerColumn.get(tableName);
    if (owner) {
      pullRules[tableName] = { kind: owner.kind, column: owner.column };
      continue;
    }
    const pullRule = getCollectionPullRule({
      tableName,
      foreignKeys: fksByTable.get(tableName) ?? [],
      tableOwnerColumn: params.tableOwnerColumn,
      tableRegistryCore: params.tableRegistryCore,
      collections: params.collections,
    });
    if (pullRule) pullRules[tableName] = pullRule;
  }
  return pullRules;
}

function buildDefaultWorkerConfig(params: {
  syncableTables: string[];
  columnsByTable: Map<string, IColumnRow[]>;
  foreignKeys: IForeignKeyRow[];
  tableRegistryCore: Record<string, ITableMetaCore>;
  ownerColumnOverrideByTable: Record<string, string>;
}): unknown {
  const { tableOwnerColumn, pushRules } = buildOwnerColumnsAndPushRules(params);
  const collections = buildDefaultCollections({
    tableOwnerColumn,
    tableRegistryCore: params.tableRegistryCore,
  });
  const pullRules = buildDefaultPullRules({
    syncableTables: params.syncableTables,
    foreignKeys: params.foreignKeys,
    tableOwnerColumn,
    tableRegistryCore: params.tableRegistryCore,
    collections,
  });

  return {
    collections,
    pull: { tableRules: pullRules },
    push: { tableRules: pushRules },
  } as const;
}

function asRecord(value: unknown): WorkerConfigRecord {
  return typeof value === "object" && value !== null
    ? (value as WorkerConfigRecord)
    : EMPTY_WORKER_CONFIG_RECORD;
}

function mergeRecords(base: unknown, override: unknown): WorkerConfigRecord {
  return {
    ...asRecord(base),
    ...asRecord(override),
  };
}

function getSanitizeConfig(value: unknown): SanitizeConfig | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  return value as SanitizeConfig;
}

function uniqStrings(values: unknown): string[] {
  if (!Array.isArray(values)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    if (typeof value !== "string") continue;
    if (seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

function mergeNumericCoercions(
  base: unknown,
  extra: unknown
): NumericCoercion[] | undefined {
  const baseItems = Array.isArray(base) ? base : [];
  const extraItems = Array.isArray(extra) ? extra : [];
  const merged: NumericCoercion[] = [];
  const seen = new Set<string>();
  for (const value of [...baseItems, ...extraItems]) {
    if (!value || typeof value !== "object") continue;
    const record = value as WorkerConfigRecord;
    const prop = record.prop;
    const kind = record.kind;
    if (typeof prop !== "string") continue;
    if (kind !== "int" && kind !== "float") continue;
    const key = `${prop}::${kind}`;
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push({ prop, kind });
  }
  return merged.length > 0 ? merged : undefined;
}

function mergeSanitizeConfig(params: {
  baseSanitize: SanitizeConfig | undefined;
  overrideSanitize: SanitizeConfig | undefined;
}): SanitizeConfig | undefined {
  if (!params.baseSanitize && !params.overrideSanitize) return undefined;
  const nullIfEmptyStringProps = uniqStrings([
    ...uniqStrings(params.baseSanitize?.nullIfEmptyStringProps),
    ...uniqStrings(params.overrideSanitize?.nullIfEmptyStringProps),
  ]);
  const coerceNumericProps = mergeNumericCoercions(
    params.baseSanitize?.coerceNumericProps,
    params.overrideSanitize?.coerceNumericProps
  );
  return {
    ...asRecord(params.baseSanitize),
    ...asRecord(params.overrideSanitize),
    nullIfEmptyStringProps:
      nullIfEmptyStringProps.length > 0 ? nullIfEmptyStringProps : undefined,
    coerceNumericProps,
  };
}

function mergePushTableRule(
  base: unknown,
  override: unknown
): PushTableRuleConfig {
  const baseRule = asRecord(base) as PushTableRuleConfig;
  const overrideRule = asRecord(override) as PushTableRuleConfig;
  return {
    ...baseRule,
    ...overrideRule,
    sanitize: mergeSanitizeConfig({
      baseSanitize: getSanitizeConfig(baseRule.sanitize),
      overrideSanitize: getSanitizeConfig(overrideRule.sanitize),
    }),
  };
}

function mergePushTableRules(
  defaultRules: unknown,
  overrideRules: unknown
): Record<string, unknown> {
  const defaultRulesObj = asRecord(defaultRules);
  const overrideRulesObj = asRecord(overrideRules);
  const keys = stableSort(
    [
      ...new Set([
        ...Object.keys(defaultRulesObj),
        ...Object.keys(overrideRulesObj),
      ]),
    ],
    (key) => key
  );
  const out: Record<string, unknown> = {};
  for (const tableName of keys) {
    const base = defaultRulesObj[tableName];
    const override = overrideRulesObj[tableName];
    out[tableName] =
      base && override
        ? mergePushTableRule(base, override)
        : (override ?? base);
  }
  return out;
}

function mergeWorkerConfigs(defaults: unknown, overrides: unknown): unknown {
  const d = asRecord(defaults) as WorkerConfigRoot;
  const o = asRecord(overrides) as WorkerConfigRoot;

  return {
    ...d,
    ...o,
    collections: mergeRecords(d.collections, o.collections),
    pull: {
      ...mergeRecords(d.pull, o.pull),
      tableRules: mergeRecords(d.pull?.tableRules, o.pull?.tableRules),
    },
    push: {
      ...mergeRecords(d.push, o.push),
      tableRules: mergePushTableRules(d.push?.tableRules, o.push?.tableRules),
    },
  };
}

function buildForeignKeyByTableColumn(
  foreignKeysByConstraint: Map<string, IForeignKeyRow[]>,
  strict: boolean
): Map<string, IForeignKeyRow> {
  const fkByTableCol = new Map<string, IForeignKeyRow>();
  for (const [key, rows] of foreignKeysByConstraint.entries()) {
    if (rows.length !== 1) {
      if (strict) {
        throw new Error(
          `Unsupported composite foreign key: ${key} (${rows.length} columns). Use --lenient to skip.`
        );
      }
      continue;
    }
    const foreignKey = rows[0];
    fkByTableCol.set(
      `${foreignKey.table_name}::${foreignKey.column_name}`,
      foreignKey
    );
  }
  return fkByTableCol;
}

function assertKnownColumnType(params: {
  strict: boolean;
  tableName: string;
  column: IColumnRow;
  pgType: string;
}): void {
  if (!params.strict) return;
  if (params.column.data_type.toLowerCase() === "user-defined") return;
  if (isKnownPgType(params.pgType)) return;
  throw new Error(
    `Unmappable Postgres type for ${params.tableName}.${params.column.column_name}: data_type=${params.column.data_type} udt_name=${params.column.udt_name} (normalized=${params.pgType}).`
  );
}

function chainBuilderPieces(pieces: string[]): string {
  return pieces
    .map((piece, index) => (index === 0 ? piece : `.${piece}`))
    .join("");
}

function getSqliteDefaultValue(params: {
  builder: string;
  pgType: string;
  value: string;
}): string {
  if (params.builder !== "integer" || params.pgType !== "boolean") {
    return params.value;
  }
  return params.value === "true" ? "1" : "0";
}

function appendDefaultBuilderPiece(params: {
  pieces: string[];
  builder: string;
  pgType: string;
  parsedDefault: ParsedPgDefault | null;
}): void {
  const parsedDefault = params.parsedDefault;
  if (!parsedDefault) return;
  if (parsedDefault.kind === "default") {
    const defaultValue = getSqliteDefaultValue({
      builder: params.builder,
      pgType: params.pgType,
      value: parsedDefault.value,
    });
    params.pieces.push(`default(${defaultValue})`);
  } else {
    params.pieces.push(`$defaultFn(${parsedDefault.value})`);
  }
}

function buildSqliteColumnLine(params: {
  tableName: string;
  column: IColumnRow;
  pkSet: Set<string>;
  isSinglePk: boolean;
  fkByTableCol: Map<string, IForeignKeyRow>;
  tableIdentByName: Map<string, string>;
  strict: boolean;
}): string {
  const pgType = normalizeType({
    dataType: params.column.data_type,
    udtName: params.column.udt_name,
  });
  assertKnownColumnType({
    strict: params.strict,
    tableName: params.tableName,
    column: params.column,
    pgType,
  });

  const builder = sqliteBuilderForPgType(pgType);
  const pieces: string[] = [`${builder}("${params.column.column_name}")`];
  if (params.column.is_nullable === "NO") pieces.push("notNull()");

  const fk = params.fkByTableCol.get(
    `${params.tableName}::${params.column.column_name}`
  );
  const refTableIdent = fk
    ? params.tableIdentByName.get(fk.ref_table_name)
    : null;
  if (fk && refTableIdent) {
    pieces.push(
      `references(() => ${refTableIdent}.${safeIdentifier(fk.ref_column_name)})`
    );
  }
  if (params.pkSet.has(params.column.column_name) && params.isSinglePk) {
    pieces.push("primaryKey()");
  }

  const parsedDefault = parsePgDefault({
    pgType,
    columnDefault: params.column.column_default,
  });
  appendDefaultBuilderPiece({ pieces, builder, pgType, parsedDefault });
  if (!parsedDefault && params.strict && params.column.column_default) {
    throw new Error(
      `Unsupported default for ${params.tableName}.${params.column.column_name}: ${params.column.column_default}. Use --lenient to skip.`
    );
  }

  return `  ${safeIdentifier(params.column.column_name)}: ${chainBuilderPieces(pieces)},`;
}

function hasSyncColumns(tableColumns: IColumnRow[]): boolean {
  const columnNames = new Set(tableColumns.map((column) => column.column_name));
  return (
    columnNames.has("sync_version") &&
    columnNames.has("last_modified_at") &&
    columnNames.has("device_id")
  );
}

function getColumnsToEmit(tableColumns: IColumnRow[]): IColumnRow[] {
  if (!hasSyncColumns(tableColumns)) return tableColumns;
  return tableColumns.filter(
    (column) =>
      column.column_name !== "sync_version" &&
      column.column_name !== "last_modified_at" &&
      column.column_name !== "device_id"
  );
}

function getGroupedRowsForTable<T>(
  groupedRows: Map<string, T[]>,
  tableName: string
): T[][] {
  return [...groupedRows.entries()]
    .filter(([key]) => key.startsWith(`${tableName}::`))
    .map(([, rows]) => rows);
}

function buildUniqueConstraintItems(
  groupedConstraints: Map<string, IConstraintColumnRow[]>,
  tableName: string
): string[] {
  return getGroupedRowsForTable(groupedConstraints, tableName).flatMap(
    (rows) => {
      const sorted = getSortedRowsByPosition(rows);
      const name = sorted[0]?.constraint_name;
      if (!name) return [];
      const cols = sorted
        .map((row) => `t.${safeIdentifier(row.column_name)}`)
        .join(", ");
      return [`uniqueIndex("${name}").on(${cols})`];
    }
  );
}

function buildIndexItems(params: {
  groupedIndexes: Map<string, IIndexKeyRow[]>;
  tableName: string;
  strict: boolean;
}): string[] {
  const configItems: string[] = [];
  for (const rows of getGroupedRowsForTable(
    params.groupedIndexes,
    params.tableName
  )) {
    const sorted = getSortedRowsByPosition(rows);
    const indexName = sorted[0]?.index_name;
    if (!indexName) continue;

    const colRefs = sorted.map((row) => parseIndexKeyToColumnName(row.keydef));
    if (colRefs.includes(null)) {
      if (params.strict) {
        const badRow = sorted.find(
          (row) => !parseIndexKeyToColumnName(row.keydef)
        );
        throw new Error(
          `Unsupported index key for ${params.tableName}.${indexName}: ${badRow?.keydef}. Use --lenient to skip.`
        );
      }
      continue;
    }
    configItems.push(
      `index("${indexName}").on(${colRefs
        .map((columnName) => `t.${safeIdentifier(columnName ?? "")}`)
        .join(", ")})`
    );
  }
  return configItems;
}

function hasEmittableIndexItems(
  groupedIndexes: Map<string, IIndexKeyRow[]>,
  tableNames: string[]
): boolean {
  return tableNames.some((tableName) =>
    getGroupedRowsForTable(groupedIndexes, tableName).some((rows) => {
      const sorted = getSortedRowsByPosition(rows);
      return (
        !!sorted[0]?.index_name &&
        sorted.every((row) => parseIndexKeyToColumnName(row.keydef) !== null)
      );
    })
  );
}

function buildSqliteConfigItems(params: {
  tableName: string;
  pkCols: IConstraintColumnRow[];
  uniqueByTableConstraint: Map<string, IConstraintColumnRow[]>;
  idxByTableIndex: Map<string, IIndexKeyRow[]>;
  strict: boolean;
}): string[] {
  const configItems: string[] = [];
  if (params.pkCols.length > 1) {
    const pkProps = params.pkCols
      .map((row) => `t.${safeIdentifier(row.column_name)}`)
      .join(", ");
    configItems.push(`primaryKey({ columns: [${pkProps}] })`);
  }
  configItems.push(
    ...buildUniqueConstraintItems(
      params.uniqueByTableConstraint,
      params.tableName
    ),
    ...buildIndexItems({
      groupedIndexes: params.idxByTableIndex,
      tableName: params.tableName,
      strict: params.strict,
    })
  );
  return configItems;
}

function buildSqliteTableLines(params: {
  tableName: string;
  ident: string;
  tableColumns: IColumnRow[];
  pkCols: IConstraintColumnRow[];
  fkByTableCol: Map<string, IForeignKeyRow>;
  tableIdentByName: Map<string, string>;
  uniqueByTableConstraint: Map<string, IConstraintColumnRow[]>;
  idxByTableIndex: Map<string, IIndexKeyRow[]>;
  strict: boolean;
}): string[] {
  const syncColumnsPresent = hasSyncColumns(params.tableColumns);
  const pkSet = new Set(params.pkCols.map((row) => row.column_name));
  const configItems = buildSqliteConfigItems(params);
  const tableCloseLines =
    configItems.length > 0
      ? [", (t) => [", ...configItems.map((item) => `  ${item},`), "]", ");"]
      : [");"];
  return [
    `export const ${params.ident} = sqliteTable("${params.tableName}", {`,
    ...getColumnsToEmit(params.tableColumns).map((column) =>
      buildSqliteColumnLine({
        tableName: params.tableName,
        column,
        pkSet,
        isSinglePk: params.pkCols.length === 1,
        fkByTableCol: params.fkByTableCol,
        tableIdentByName: params.tableIdentByName,
        strict: params.strict,
      })
    ),
    ...(syncColumnsPresent ? ["  ...sqliteSyncColumns,"] : []),
    "}",
    ...tableCloseLines,
    "",
  ];
}

function buildSchemaTs(params: {
  schema: string;
  columns: IColumnRow[];
  primaryKeys: IConstraintColumnRow[];
  uniqueConstraints: IConstraintColumnRow[];
  indexes: IIndexKeyRow[];
  foreignKeys: IForeignKeyRow[];
  strict: boolean;
}): string {
  const colsByTable = groupByKey(params.columns, (c) => c.table_name);
  const pkByTable = groupByKey(params.primaryKeys, (r) => r.table_name);
  const uniqueByTableConstraint = groupByKey(
    params.uniqueConstraints,
    (r) => `${r.table_name}::${r.constraint_name}`
  );
  const idxByTableIndex = groupByKey(
    params.indexes,
    (r) => `${r.table_name}::${r.index_name}`
  );
  const fkByTableConstraint = groupByKey(
    params.foreignKeys,
    (r) => `${r.table_name}::${r.constraint_name}`
  );
  const tables = getSchemaTableNames(params.columns);
  const tableIdentByName = createTableIdentifierMap(tables);
  const fkByTableCol = buildForeignKeyByTableColumn(
    fkByTableConstraint,
    params.strict
  );

  const sqliteCoreImports = [
    ...(hasEmittableIndexItems(idxByTableIndex, tables) ? ["index"] : []),
    "integer",
    "primaryKey",
    "real",
    "sqliteTable",
    "text",
    "uniqueIndex",
  ].join(", ");

  return [
    createHeader({ schema: params.schema }),
    `import { ${sqliteCoreImports} } from "drizzle-orm/sqlite-core";`,
    'import { sqliteSyncColumns } from "oosync/shared/sync-columns";',
    "",
    ...tables.flatMap((tableName) => {
      const ident = tableIdentByName.get(tableName);
      return ident
        ? buildSqliteTableLines({
            tableName,
            ident,
            tableColumns: getSortedTableColumns(colsByTable, tableName),
            pkCols: getSortedRowsByPosition(pkByTable.get(tableName)),
            fkByTableCol,
            tableIdentByName,
            uniqueByTableConstraint,
            idxByTableIndex,
            strict: params.strict,
          })
        : [];
    }),
  ]
    .join("\n")
    .replace(/\n{3,}/g, "\n\n");
}

function pgBuilderForPgType(
  pgType: string
): "text" | "integer" | "real" | "boolean" | "jsonb" | "uuid" {
  // IMPORTANT: the worker stores timestamp-like values as TEXT (ISO strings)
  // for consistency with the client-side SQLite schema.
  // UUIDs should remain UUID-typed in Postgres worker schema for correct SQL operators.
  switch (pgType) {
    case "uuid":
      return "uuid";
    case "timestamp":
    case "timestamptz":
    case "date":
    case "time":
    case "timetz":
    case "text":
      return "text";
    case "boolean":
      return "boolean";
    case "int":
      return "integer";
    case "real":
    case "numeric":
      return "real";
    case "json":
    case "jsonb":
      return "jsonb";
    default:
      return "text";
  }
}

function getUsedPgBuilders(params: {
  tables: string[];
  colsByTable: Map<string, IColumnRow[]>;
}): Set<string> {
  const usedBuilders = new Set<string>();
  for (const tableName of params.tables) {
    for (const column of params.colsByTable.get(tableName) ?? []) {
      usedBuilders.add(
        pgBuilderForPgType(
          normalizeType({
            dataType: column.data_type,
            udtName: column.udt_name,
          })
        )
      );
    }
  }
  return usedBuilders;
}

function buildPgImportLines(params: {
  schema: string;
  usedBuilders: Set<string>;
}): string[] {
  const isNonPublicSchema = params.schema !== "public";
  const importTypes = [
    ...Array.from(params.usedBuilders),
    ...(isNonPublicSchema ? ["PgSchema", "pgSchema"] : ["pgTable"]),
  ].sort((left, right) =>
    left.localeCompare(right, undefined, { sensitivity: "base" })
  );
  return [
    `import { ${importTypes.join(", ")} } from "drizzle-orm/pg-core";`,
    "",
    ...(isNonPublicSchema
      ? [
          `const ${params.schema}Schema = pgSchema("${params.schema}");`,
          'const publicSchema = new PgSchema("public");',
          "",
        ]
      : []),
  ];
}

function buildPgColumnLine(params: {
  tableName: string;
  column: IColumnRow;
  pkSet: Set<string>;
  isSinglePk: boolean;
  strict: boolean;
}): string {
  const pgType = normalizeType({
    dataType: params.column.data_type,
    udtName: params.column.udt_name,
  });
  assertKnownColumnType({
    strict: params.strict,
    tableName: params.tableName,
    column: params.column,
    pgType,
  });

  const pieces: string[] = [
    `${pgBuilderForPgType(pgType)}("${params.column.column_name}")`,
  ];
  if (params.column.is_nullable === "NO") pieces.push("notNull()");
  if (params.pkSet.has(params.column.column_name) && params.isSinglePk) {
    pieces.push("primaryKey()");
  }

  const parsedDefault = parsePgDefault({
    pgType,
    columnDefault: params.column.column_default,
  });
  if (parsedDefault?.kind === "default") {
    pieces.push(`default(${parsedDefault.value})`);
  } else if (parsedDefault) {
    pieces.push(`$defaultFn(${parsedDefault.value})`);
  } else if (params.strict && params.column.column_default) {
    throw new Error(
      `Unsupported default for ${params.tableName}.${params.column.column_name}: ${params.column.column_default}. Use --lenient to skip.`
    );
  }

  return `  ${safeIdentifier(params.column.column_name)}: ${chainBuilderPieces(pieces)},`;
}

function buildPgTableLines(params: {
  schema: string;
  tableName: string;
  ident: string;
  tableColumns: IColumnRow[];
  pkCols: IConstraintColumnRow[];
  strict: boolean;
}): string[] {
  const tableDecl =
    params.schema === "public"
      ? `pgTable("${params.tableName}", {`
      : `${params.schema}Schema.table("${params.tableName}", {`;
  const pkSet = new Set(params.pkCols.map((row) => row.column_name));
  return [
    `export const ${params.ident} = ${tableDecl}`,
    ...params.tableColumns.map((column) =>
      buildPgColumnLine({
        tableName: params.tableName,
        column,
        pkSet,
        isSinglePk: params.pkCols.length === 1,
        strict: params.strict,
      })
    ),
    "});",
    "",
  ];
}

function buildSyncChangeLogLines(schema: string): string[] {
  if (schema === "public") return [];
  return [
    "// sync infrastructure table (public schema — required by oosync worker)",
    'export const syncChangeLog = publicSchema.table("sync_change_log", {',
    '  tableName: text("table_name").notNull().primaryKey(),',
    '  changedAt: text("changed_at").notNull(),',
    "});",
    "",
  ];
}

function buildPgTablesMapLines(params: {
  tables: string[];
  tableIdentByName: Map<string, string>;
  schema: string;
}): string[] {
  return [
    "export const tables = {",
    ...params.tables.flatMap((tableName) => {
      const ident = params.tableIdentByName.get(tableName);
      return ident ? [`  ${JSON.stringify(tableName)}: ${ident},`] : [];
    }),
    ...(params.schema === "public"
      ? []
      : ['  "sync_change_log": syncChangeLog,']),
    "} as const;",
    "",
  ];
}

function buildPgSchemaTs(params: {
  schema: string;
  columns: IColumnRow[];
  primaryKeys: IConstraintColumnRow[];
  strict: boolean;
}): string {
  const colsByTable = groupByKey(params.columns, (c) => c.table_name);
  const pkByTable = groupByKey(params.primaryKeys, (r) => r.table_name);
  const tables = getSchemaTableNames(params.columns);
  const tableIdentByName = createTableIdentifierMap(tables);
  return [
    createHeader({ schema: params.schema }),
    ...buildPgImportLines({
      schema: params.schema,
      usedBuilders: getUsedPgBuilders({ tables, colsByTable }),
    }),
    ...tables.flatMap((tableName) => {
      const ident = tableIdentByName.get(tableName);
      return ident
        ? buildPgTableLines({
            schema: params.schema,
            tableName,
            ident,
            tableColumns: getSortedTableColumns(colsByTable, tableName),
            pkCols: getSortedRowsByPosition(pkByTable.get(tableName)),
            strict: params.strict,
          })
        : [];
    }),
    ...buildSyncChangeLogLines(params.schema),
    ...buildPgTablesMapLines({
      tables,
      tableIdentByName,
      schema: params.schema,
    }),
  ]
    .join("\n")
    .replace(/\n{3,}/g, "\n\n");
}

function buildWorkerConfigTs(params: {
  schema: string;
  config: unknown;
}): string {
  return [
    createHeader({ schema: params.schema }),
    "export const WORKER_SYNC_CONFIG = ",
    `${JSON.stringify(params.config ?? EMPTY_WORKER_CONFIG_RECORD, null, 2)} as const;`,
    "",
  ].join("\n");
}

function buildSqliteSchemaWrapperTs(params: {
  schema: string;
  sqliteSchemaImportPath: string;
}): string {
  return [
    createHeader({ schema: params.schema }),
    `export * from ${JSON.stringify(params.sqliteSchemaImportPath)};`,
    "",
    'import { index, integer, sqliteTable, text } from "drizzle-orm/sqlite-core";',
    "",
    "/**",
    " * Client-side outbox used by the oosync runtime.",
    " */",
    "export const syncPushQueue = sqliteTable(",
    '  "sync_push_queue",',
    "  {",
    '    id: text("id").primaryKey().notNull(),',
    '    tableName: text("table_name").notNull(),',
    '    rowId: text("row_id").notNull(),',
    '    operation: text("operation").notNull(),',
    '    status: text("status").default("pending").notNull(),',
    '    changedAt: text("changed_at").notNull(),',
    '    syncedAt: text("synced_at"),',
    '    attempts: integer("attempts").default(0).notNull(),',
    '    lastError: text("last_error"),',
    "  },",
    "  (t) => [",
    '    index("idx_push_queue_status_changed").on(t.status, t.changedAt),',
    '    index("idx_push_queue_table_row").on(t.tableName, t.rowId),',
    "  ]",
    ");",
    "",
  ].join("\n");
}

function buildWorkerEntrypointTs(params: {
  schema: string;
  workerSchemaImportPath: string;
  workerConfigImportPath: string;
  tableMetaImportPath: string;
}): string {
  return [
    createHeader({ schema: params.schema }),
    'import createWorker from "oosync/worker";',
    `import { SYNCABLE_TABLES, TABLE_REGISTRY_CORE } from ${JSON.stringify(params.tableMetaImportPath)};`,
    `import { tables as schemaTables } from ${JSON.stringify(params.workerSchemaImportPath)};`,
    `import { WORKER_SYNC_CONFIG } from ${JSON.stringify(params.workerConfigImportPath)};`,
    "",
    "export default createWorker({",
    "  schemaTables,",
    "  syncableTables: SYNCABLE_TABLES,",
    "  tableRegistryCore: TABLE_REGISTRY_CORE,",
    "  workerSyncConfig: WORKER_SYNC_CONFIG,",
    "});",
    "",
  ].join("\n");
}

function forceResetQueryParamLines(
  entries: NonNullable<
    NonNullable<ICodegenConfigFile["browserSqlite"]>["forceResetQueryParams"]
  >
): string[] {
  return entries.map((entry) =>
    typeof entry.value === "string"
      ? `    { key: ${JSON.stringify(entry.key)}, value: ${JSON.stringify(entry.value)} },`
      : `    { key: ${JSON.stringify(entry.key)} },`
  );
}

function browserTestConfigLines(
  browserConfig: NonNullable<ICodegenConfigFile["browserSqlite"]>
): string[] {
  return [
    ...(browserConfig.testApiWindowProperty
      ? [
          `    testApiWindowProperty: ${JSON.stringify(browserConfig.testApiWindowProperty)},`,
        ]
      : []),
    ...(browserConfig.clearInProgressWindowProperty
      ? [
          `    clearInProgressWindowProperty: ${JSON.stringify(browserConfig.clearInProgressWindowProperty)},`,
        ]
      : []),
    ...(browserConfig.persistWindowHookProperty
      ? [
          `    persistWindowHookProperty: ${JSON.stringify(browserConfig.persistWindowHookProperty)},`,
        ]
      : []),
  ];
}

function buildSqliteClientTs(params: {
  schema: string;
  localSchemaImportPath: string;
  appTableMetaImportPath: string;
  hooksImportPath: string;
  hooksExportName: string;
  migrationFiles: string[];
  browserConfig: NonNullable<ICodegenConfigFile["browserSqlite"]>;
}): string {
  const forceResetParams = params.browserConfig.forceResetQueryParams ?? [];
  return [
    createHeader({ schema: params.schema }),
    'import type { BrowserSqliteDatabase } from "oosync/runtime/browser-sqlite";',
    'import { createBrowserSqliteClient } from "oosync/runtime/browser-sqlite";',
    `import * as schema from ${JSON.stringify(params.localSchemaImportPath)};`,
    `import { SYNCABLE_TABLES, TABLE_REGISTRY, TABLE_SYNC_ORDER, TABLE_TO_SCHEMA_KEY } from ${JSON.stringify(params.appTableMetaImportPath)};`,
    `import { ${params.hooksExportName} } from ${JSON.stringify(params.hooksImportPath)};`,
    "",
    "export const browserSqliteClient = createBrowserSqliteClient({",
    "  schema,",
    "  syncPushQueue: schema.syncPushQueue,",
    "  syncSchema: {",
    "    syncableTables: SYNCABLE_TABLES,",
    "    tableRegistry: TABLE_REGISTRY,",
    "    tableSyncOrder: TABLE_SYNC_ORDER,",
    "    tableToSchemaKey: TABLE_TO_SCHEMA_KEY,",
    "  },",
    `  hooks: ${params.hooksExportName},`,
    '  diagnosticsEnabled: import.meta.env.VITE_SYNC_DIAGNOSTICS === "true",',
    "  storage: {",
    `    indexedDbName: ${JSON.stringify(params.browserConfig.indexedDbName)},`,
    `    indexedDbStore: ${JSON.stringify(params.browserConfig.indexedDbStore)},`,
    `    dbKeyPrefix: ${JSON.stringify(params.browserConfig.dbKeyPrefix)},`,
    `    dbVersionKeyPrefix: ${JSON.stringify(params.browserConfig.dbVersionKeyPrefix)},`,
    `    outboxBackupKeyPrefix: ${JSON.stringify(params.browserConfig.outboxBackupKeyPrefix)},`,
    `    lastSyncTimestampKeyPrefix: ${JSON.stringify(params.browserConfig.lastSyncTimestampKeyPrefix)},`,
    "  },",
    `  databaseVersion: ${params.browserConfig.databaseVersion},`,
    `  schemaVersion: ${JSON.stringify(params.browserConfig.schemaVersion)},`,
    "  migrationFiles: [",
    ...params.migrationFiles.map(
      (migrationFile) => `    ${JSON.stringify(migrationFile)},`
    ),
    "  ],",
    "  forceResetQueryParams: [",
    ...forceResetQueryParamLines(forceResetParams),
    "  ],",
    "  testConfig: {",
    ...browserTestConfigLines(params.browserConfig),
    "  },",
    "});",
    "",
    "export const initializeDb = browserSqliteClient.initializeDb;",
    "export const getDb = browserSqliteClient.getDb;",
    "export const persistDb = browserSqliteClient.persistDb;",
    "export const closeDb = browserSqliteClient.closeDb;",
    "export const clearDb = browserSqliteClient.clearDb;",
    "export const setupAutoPersist = browserSqliteClient.setupAutoPersist;",
    "export const getSqliteInstance = browserSqliteClient.getSqliteInstance;",
    "export const getSqliteDebugInfo = browserSqliteClient.getSqliteDebugInfo;",
    "export const getClientSqliteDebugState = browserSqliteClient.getDebugState;",
    "export const loadOutboxBackupForUser = browserSqliteClient.loadOutboxBackupForUser;",
    "export const clearOutboxBackupForUser = browserSqliteClient.clearOutboxBackupForUser;",
    "",
    "export { schema };",
    "export type SqliteDatabase = BrowserSqliteDatabase;",
    "export type Schema = typeof schema;",
    "",
  ].join("\n");
}

function buildSyncRuntimeConfigTs(params: {
  schema: string;
  localSchemaImportPath: string;
  appTableMetaImportPath: string;
  clientImportPath: string;
}): string {
  return [
    createHeader({ schema: params.schema }),
    'import { type SyncRuntime, setSyncRuntime as setAliasedSyncRuntime } from "@oosync/sync";',
    'import { createBrowserSyncRuntime } from "oosync/runtime/browser-sqlite";',
    'import { setSyncRuntime as setPackageSyncRuntime } from "oosync/sync";',
    `import * as localSchema from ${JSON.stringify(params.localSchemaImportPath)};`,
    `import { SYNCABLE_TABLES, TABLE_REGISTRY, TABLE_SYNC_ORDER, TABLE_TO_SCHEMA_KEY } from ${JSON.stringify(params.appTableMetaImportPath)};`,
    `import { browserSqliteClient } from ${JSON.stringify(params.clientImportPath)};`,
    "",
    "let configured = false;",
    "",
    "export function ensureSyncRuntimeConfigured(): void {",
    "  if (configured) return;",
    "  const runtime: SyncRuntime = createBrowserSyncRuntime({",
    "    client: browserSqliteClient,",
    "    schema: {",
    "      syncableTables: SYNCABLE_TABLES,",
    "      tableRegistry: TABLE_REGISTRY,",
    "      tableSyncOrder: TABLE_SYNC_ORDER,",
    "      tableToSchemaKey: TABLE_TO_SCHEMA_KEY,",
    "    },",
    "    localSchema,",
    "  });",
    "  setAliasedSyncRuntime(runtime);",
    "  setPackageSyncRuntime(runtime);",
    "  configured = true;",
    "}",
    "",
    "ensureSyncRuntimeConfigured();",
    "",
  ].join("\n");
}

function resolveOutputPath(
  configuredPath: string | undefined,
  defaultPath: string
): string {
  return configuredPath ? toAbsoluteFromCwd(configuredPath) : defaultPath;
}

function resolveOptionalOutputPath(
  configuredPath: string | undefined
): string | null {
  return configuredPath ? toAbsoluteFromCwd(configuredPath) : null;
}

function getOutputPaths(config: ICodegenConfigFile): {
  outputSchemaFile: string;
  outputSqliteSchemaWrapperFile: string | null;
  outputSqliteClientFile: string | null;
  outputTableMetaFile: string;
  outputAppTableMetaFile: string;
  outputWorkerPgSchemaFile: string;
  outputWorkerConfigFile: string;
  outputWorkerEntrypointFile: string | null;
  outputSyncRuntimeConfigFile: string | null;
  outputSqliteDrizzleConfig: string | null;
} {
  return {
    outputSchemaFile: resolveOutputPath(
      config.outputs?.sqliteSchemaFile,
      DEFAULT_OUTPUT_SQLITE_SCHEMA_FILE
    ),
    outputSqliteSchemaWrapperFile: resolveOptionalOutputPath(
      config.outputs?.sqliteSchemaWrapperFile
    ),
    outputSqliteClientFile: resolveOptionalOutputPath(
      config.outputs?.sqliteClientFile
    ),
    outputTableMetaFile: resolveOutputPath(
      config.outputs?.tableMetaFile,
      DEFAULT_OUTPUT_TABLE_META_FILE
    ),
    outputAppTableMetaFile: resolveOutputPath(
      config.outputs?.appTableMetaFile,
      path.join(__dirname, "../../shared/table-meta.ts")
    ),
    outputWorkerPgSchemaFile: resolveOutputPath(
      config.outputs?.workerPgSchemaFile,
      DEFAULT_OUTPUT_WORKER_PG_SCHEMA_FILE
    ),
    outputWorkerConfigFile: resolveOutputPath(
      config.outputs?.workerConfigFile,
      DEFAULT_OUTPUT_WORKER_CONFIG_FILE
    ),
    outputWorkerEntrypointFile: resolveOptionalOutputPath(
      config.outputs?.workerEntrypointFile
    ),
    outputSyncRuntimeConfigFile: resolveOptionalOutputPath(
      config.outputs?.syncRuntimeConfigFile
    ),
    outputSqliteDrizzleConfig: resolveOptionalOutputPath(
      config.outputs?.sqliteDrizzleConfig
    ),
  };
}

function getSqliteMigrationFiles(params: {
  browserSqliteConfig: ICodegenConfigFile["browserSqlite"];
  sqliteMigrationDirectory: string | null;
}): string[] {
  if (params.browserSqliteConfig?.migrationFiles) {
    return params.browserSqliteConfig.migrationFiles;
  }
  return params.sqliteMigrationDirectory
    ? listSqliteMigrationFiles(params.sqliteMigrationDirectory)
    : [];
}

async function main(): Promise<void> /* NOSONAR - codegen orchestration is intentionally linear. */ {
  const args = parseArgs(process.argv.slice(2));
  const configPath = resolveConfigPath({ cliPath: args.configPath });
  const config = loadCodegenConfig(configPath);
  const {
    outputSchemaFile,
    outputSqliteSchemaWrapperFile,
    outputSqliteClientFile,
    outputTableMetaFile,
    outputAppTableMetaFile,
    outputWorkerPgSchemaFile,
    outputWorkerConfigFile,
    outputWorkerEntrypointFile,
    outputSyncRuntimeConfigFile,
    outputSqliteDrizzleConfig,
  } = getOutputPaths(config);

  const {
    columns,
    primaryKeys,
    uniqueConstraints,
    indexes,
    foreignKeys,
    tableComments,
    columnComments,
  } = await introspect({ databaseUrl: args.databaseUrl, schema: args.schema });

  const next = buildSchemaTs({
    schema: args.schema,
    columns,
    primaryKeys,
    uniqueConstraints,
    indexes,
    foreignKeys,
    strict: args.strict,
  });

  const allTables = getAllTableNames(columns);
  const columnsByTable = getTableColumns(columns);
  const pkByTable = getPrimaryKeyByTable(primaryKeys);
  const uniqueByTable = getUniqueConstraintsByTable(uniqueConstraints);

  const legacyWhitelist =
    Array.isArray(config.tableMeta?.syncableTables) &&
    config.tableMeta?.syncableTables.length > 0
      ? [...(config.tableMeta?.syncableTables ?? [])]
      : null;

  const excluded = new Set<string>(["schema_migrations", "drizzle_migrations"]);
  for (const t of config.tableMeta?.excludeTables ?? []) excluded.add(t);
  for (const row of tableComments) {
    const tags = parseOosyncTableTags(row.comment);
    if (tags.exclude) excluded.add(row.table_name);
  }

  const syncableTables = inferSyncableTables({
    allTables,
    pkByTable,
    legacyWhitelist,
    excluded,
  });

  const inferredRegistry = inferTableMetaCore({
    columnsByTable,
    pkByTable,
    uniqueByTable,
    overrides: config.tableMeta?.overrides ?? EMPTY_TABLE_META_OVERRIDES,
  });

  // Legacy full registry still supported as an override layer.
  const tableRegistryCore: Record<string, ITableMetaCore> = {
    ...inferredRegistry,
    ...(config.tableMeta?.tableRegistryCore ?? EMPTY_TABLE_REGISTRY_CORE),
  };

  const columnDescriptionsByTable: Record<string, Record<string, string>> = {};
  for (const row of columnComments) {
    if (!row.comment) continue;
    if (!columnDescriptionsByTable[row.table_name]) {
      columnDescriptionsByTable[row.table_name] = {};
    }
    columnDescriptionsByTable[row.table_name][row.column_name] = row.comment;
  }

  const nextTableMeta = buildTableMetaTs({
    schema: args.schema,
    columns,
    primaryKeys,
    uniqueConstraints,
    strict: args.strict,
    syncableTables,
    tableRegistryCore,
    columnDescriptionsByTable,
  });

  const changeCategoryByTable: Record<string, ChangeCategory> = {
    ...(config.tableMeta?.changeCategoryByTable ??
      EMPTY_CHANGE_CATEGORY_BY_TABLE),
  };
  for (const row of tableComments) {
    const tags = parseOosyncTableTags(row.comment);
    if (tags.changeCategory !== undefined) {
      changeCategoryByTable[row.table_name] = tags.changeCategory;
    }
  }

  const normalizeDatetimeByTable: Record<string, string[]> = {
    ...(config.tableMeta?.normalizeDatetimeByTable ??
      EMPTY_NORMALIZE_DATETIME_BY_TABLE),
  };
  for (const row of tableComments) {
    const tags = parseOosyncTableTags(row.comment);
    if (tags.normalizeDatetime && tags.normalizeDatetime.length > 0) {
      normalizeDatetimeByTable[row.table_name] = tags.normalizeDatetime;
    }
  }

  const ownerColumnOverrideByTable: Record<string, string> = {};
  for (const row of tableComments) {
    const tags = parseOosyncTableTags(row.comment);
    if (tags.ownerColumn)
      ownerColumnOverrideByTable[row.table_name] = tags.ownerColumn;
  }

  const tableSyncOrder = buildTableSyncOrder({
    tables: syncableTables,
    foreignKeys,
    overrides: config.tableMeta?.tableSyncOrderOverrides ?? EMPTY_NUMBER_RECORD,
  });

  const tableToSchemaKey = buildTableToSchemaKeyMap({
    tables: syncableTables,
    overrides:
      config.tableMeta?.tableToSchemaKeyOverrides ?? EMPTY_STRING_RECORD,
  });

  const nextAppTableMeta = buildAppTableMetaTs({
    schema: args.schema,
    generatedTableMetaImportPath: toImportPath({
      fromFile: outputAppTableMetaFile,
      toFile: outputTableMetaFile,
    }),
    syncableTables,
    changeCategoryByTable,
    normalizeDatetimeByTable,
    tableSyncOrder,
    tableToSchemaKey,
    columnDescriptionsByTable,
  });

  const nextWorkerPgSchema = buildPgSchemaTs({
    schema: args.schema,
    columns,
    primaryKeys,
    strict: args.strict,
  });

  const defaultWorkerConfig = buildDefaultWorkerConfig({
    syncableTables,
    columnsByTable,
    foreignKeys,
    tableRegistryCore,
    ownerColumnOverrideByTable,
  });
  const mergedWorkerConfig = mergeWorkerConfigs(
    defaultWorkerConfig,
    config.worker?.config ?? EMPTY_WORKER_CONFIG_RECORD
  );
  const nextWorkerConfig = buildWorkerConfigTs({
    schema: args.schema,
    config: mergedWorkerConfig,
  });

  const formattedSchema = formatWithBiome(outputSchemaFile, next);
  const formattedTableMeta = formatWithBiome(
    outputTableMetaFile,
    nextTableMeta
  );
  const formattedAppTableMeta = formatWithBiome(
    outputAppTableMetaFile,
    nextAppTableMeta
  );
  const formattedWorkerPgSchema = formatWithBiome(
    outputWorkerPgSchemaFile,
    nextWorkerPgSchema
  );
  const formattedWorkerConfig = formatWithBiome(
    outputWorkerConfigFile,
    nextWorkerConfig
  );

  const formattedSqliteSchemaWrapper = outputSqliteSchemaWrapperFile
    ? formatWithBiome(
        outputSqliteSchemaWrapperFile,
        buildSqliteSchemaWrapperTs({
          schema: args.schema,
          sqliteSchemaImportPath: toImportPath({
            fromFile: outputSqliteSchemaWrapperFile,
            toFile: outputSchemaFile,
          }),
        })
      )
    : null;

  const formattedWorkerEntrypoint = outputWorkerEntrypointFile
    ? formatWithBiome(
        outputWorkerEntrypointFile,
        buildWorkerEntrypointTs({
          schema: args.schema,
          workerSchemaImportPath: toImportPath({
            fromFile: outputWorkerEntrypointFile,
            toFile: outputWorkerPgSchemaFile,
          }),
          workerConfigImportPath: toImportPath({
            fromFile: outputWorkerEntrypointFile,
            toFile: outputWorkerConfigFile,
          }),
          tableMetaImportPath: toImportPath({
            fromFile: outputWorkerEntrypointFile,
            toFile: outputTableMetaFile,
          }),
        })
      )
    : null;

  const browserSqliteConfig = config.browserSqlite;
  const browserSqliteHooksFile = browserSqliteConfig
    ? toAbsoluteFromCwd(browserSqliteConfig.hooksModule)
    : null;
  const browserSqliteHooksExportName =
    browserSqliteConfig?.hooksExportName ?? "browserSqliteHooks";
  const sqliteMigrationDirectory = browserSqliteConfig
    ? toAbsoluteFromCwd(
        browserSqliteConfig.migrationDirectory ?? "drizzle/migrations/sqlite"
      )
    : null;
  const sqliteMigrationFiles = getSqliteMigrationFiles({
    browserSqliteConfig,
    sqliteMigrationDirectory,
  });

  if (
    (outputSqliteClientFile || outputSyncRuntimeConfigFile) &&
    (!browserSqliteConfig || !browserSqliteHooksFile)
  ) {
    throw new Error(
      "browserSqlite config is required when generating sqliteClientFile or syncRuntimeConfigFile."
    );
  }

  const formattedSqliteClient =
    outputSqliteClientFile && browserSqliteConfig && browserSqliteHooksFile
      ? formatWithBiome(
          outputSqliteClientFile,
          buildSqliteClientTs({
            schema: args.schema,
            localSchemaImportPath: toImportPath({
              fromFile: outputSqliteClientFile,
              toFile: outputSqliteSchemaWrapperFile ?? outputSchemaFile,
            }),
            appTableMetaImportPath: toImportPath({
              fromFile: outputSqliteClientFile,
              toFile: outputAppTableMetaFile,
            }),
            hooksImportPath: toImportPath({
              fromFile: outputSqliteClientFile,
              toFile: browserSqliteHooksFile,
            }),
            hooksExportName: browserSqliteHooksExportName,
            migrationFiles: sqliteMigrationFiles,
            browserConfig: browserSqliteConfig,
          })
        )
      : null;

  const formattedSyncRuntimeConfig =
    outputSyncRuntimeConfigFile && outputSqliteClientFile
      ? formatWithBiome(
          outputSyncRuntimeConfigFile,
          buildSyncRuntimeConfigTs({
            schema: args.schema,
            localSchemaImportPath: toImportPath({
              fromFile: outputSyncRuntimeConfigFile,
              toFile: outputSqliteSchemaWrapperFile ?? outputSchemaFile,
            }),
            appTableMetaImportPath: toImportPath({
              fromFile: outputSyncRuntimeConfigFile,
              toFile: outputAppTableMetaFile,
            }),
            clientImportPath: toImportPath({
              fromFile: outputSyncRuntimeConfigFile,
              toFile: outputSqliteClientFile,
            }),
          })
        )
      : null;

  if (args.check) {
    const current = fs.existsSync(outputSchemaFile)
      ? fs.readFileSync(outputSchemaFile, "utf8")
      : "";
    if (current !== formattedSchema) {
      throw new Error(
        `SQLite schema is out of date. Run: npm run codegen:schema (output: ${outputSchemaFile})`
      );
    }

    {
      const currentMeta = fs.existsSync(outputTableMetaFile)
        ? fs.readFileSync(outputTableMetaFile, "utf8")
        : "";
      if (currentMeta !== formattedTableMeta) {
        throw new Error(
          `Shared table metadata is out of date. Run: npm run codegen:schema (output: ${outputTableMetaFile})`
        );
      }
    }

    {
      const currentAppMeta = fs.existsSync(outputAppTableMetaFile)
        ? fs.readFileSync(outputAppTableMetaFile, "utf8")
        : "";
      if (currentAppMeta !== formattedAppTableMeta) {
        throw new Error(
          `App table metadata is out of date. Run: npm run codegen:schema (output: ${outputAppTableMetaFile})`
        );
      }
    }

    const currentWorkerSchema = fs.existsSync(outputWorkerPgSchemaFile)
      ? fs.readFileSync(outputWorkerPgSchemaFile, "utf8")
      : "";
    if (currentWorkerSchema !== formattedWorkerPgSchema) {
      throw new Error(
        `Worker Postgres schema is out of date. Run: npm run codegen:schema (output: ${outputWorkerPgSchemaFile})`
      );
    }

    const currentWorkerConfig = fs.existsSync(outputWorkerConfigFile)
      ? fs.readFileSync(outputWorkerConfigFile, "utf8")
      : "";
    if (currentWorkerConfig !== formattedWorkerConfig) {
      throw new Error(
        `Worker config is out of date. Run: npm run codegen:schema (output: ${outputWorkerConfigFile})`
      );
    }

    if (outputSqliteSchemaWrapperFile && formattedSqliteSchemaWrapper) {
      const currentSqliteSchemaWrapper = fs.existsSync(
        outputSqliteSchemaWrapperFile
      )
        ? fs.readFileSync(outputSqliteSchemaWrapperFile, "utf8")
        : "";
      if (currentSqliteSchemaWrapper !== formattedSqliteSchemaWrapper) {
        throw new Error(
          `SQLite schema wrapper is out of date. Run: npm run codegen:schema (output: ${outputSqliteSchemaWrapperFile})`
        );
      }
    }

    if (outputWorkerEntrypointFile && formattedWorkerEntrypoint) {
      const currentWorkerEntrypoint = fs.existsSync(outputWorkerEntrypointFile)
        ? fs.readFileSync(outputWorkerEntrypointFile, "utf8")
        : "";
      if (currentWorkerEntrypoint !== formattedWorkerEntrypoint) {
        throw new Error(
          `Worker entrypoint is out of date. Run: npm run codegen:schema (output: ${outputWorkerEntrypointFile})`
        );
      }
    }

    if (outputSqliteClientFile && formattedSqliteClient) {
      const currentSqliteClient = fs.existsSync(outputSqliteClientFile)
        ? fs.readFileSync(outputSqliteClientFile, "utf8")
        : "";
      if (currentSqliteClient !== formattedSqliteClient) {
        throw new Error(
          `SQLite client wrapper is out of date. Run: npm run codegen:schema (output: ${outputSqliteClientFile})`
        );
      }
    }

    if (outputSyncRuntimeConfigFile && formattedSyncRuntimeConfig) {
      const currentSyncRuntimeConfig = fs.existsSync(
        outputSyncRuntimeConfigFile
      )
        ? fs.readFileSync(outputSyncRuntimeConfigFile, "utf8")
        : "";
      if (currentSyncRuntimeConfig !== formattedSyncRuntimeConfig) {
        throw new Error(
          `Sync runtime config is out of date. Run: npm run codegen:schema (output: ${outputSyncRuntimeConfigFile})`
        );
      }
    }
    // eslint-disable-next-line no-console
    console.log(
      "✅ Schemas + metadata are up to date (via Postgres introspection)."
    );
    return;
  }

  fs.writeFileSync(outputSchemaFile, formattedSchema, "utf8");
  fs.mkdirSync(path.dirname(outputTableMetaFile), { recursive: true });
  fs.writeFileSync(outputTableMetaFile, formattedTableMeta, "utf8");

  fs.mkdirSync(path.dirname(outputAppTableMetaFile), { recursive: true });
  fs.writeFileSync(outputAppTableMetaFile, formattedAppTableMeta, "utf8");

  fs.mkdirSync(path.dirname(outputWorkerPgSchemaFile), { recursive: true });
  fs.writeFileSync(outputWorkerPgSchemaFile, formattedWorkerPgSchema, "utf8");

  fs.mkdirSync(path.dirname(outputWorkerConfigFile), { recursive: true });
  fs.writeFileSync(outputWorkerConfigFile, formattedWorkerConfig, "utf8");

  if (outputSqliteSchemaWrapperFile && formattedSqliteSchemaWrapper) {
    fs.mkdirSync(path.dirname(outputSqliteSchemaWrapperFile), {
      recursive: true,
    });
    fs.writeFileSync(
      outputSqliteSchemaWrapperFile,
      formattedSqliteSchemaWrapper,
      "utf8"
    );
  }

  if (outputWorkerEntrypointFile && formattedWorkerEntrypoint) {
    fs.mkdirSync(path.dirname(outputWorkerEntrypointFile), { recursive: true });
    fs.writeFileSync(
      outputWorkerEntrypointFile,
      formattedWorkerEntrypoint,
      "utf8"
    );
  }

  if (outputSqliteClientFile && formattedSqliteClient) {
    fs.mkdirSync(path.dirname(outputSqliteClientFile), { recursive: true });
    fs.writeFileSync(outputSqliteClientFile, formattedSqliteClient, "utf8");
  }

  if (outputSyncRuntimeConfigFile && formattedSyncRuntimeConfig) {
    fs.mkdirSync(path.dirname(outputSyncRuntimeConfigFile), {
      recursive: true,
    });
    fs.writeFileSync(
      outputSyncRuntimeConfigFile,
      formattedSyncRuntimeConfig,
      "utf8"
    );
  }

  if (outputSqliteDrizzleConfig) {
    runDrizzleKitGenerate(outputSqliteDrizzleConfig);
    // eslint-disable-next-line no-console
    console.log(
      `✅ drizzle-kit generate completed (config: ${path.relative(process.cwd(), outputSqliteDrizzleConfig)})`
    );
  }

  const writtenOutputs = [
    outputSchemaFile,
    outputTableMetaFile,
    outputAppTableMetaFile,
    outputWorkerPgSchemaFile,
    outputWorkerConfigFile,
    outputSqliteSchemaWrapperFile,
    outputSqliteClientFile,
    outputSyncRuntimeConfigFile,
    outputWorkerEntrypointFile,
  ].filter((value): value is string => typeof value === "string");

  // eslint-disable-next-line no-console
  console.log(
    `✅ Wrote ${writtenOutputs
      .map((value) => path.relative(process.cwd(), value))
      .join(", ")}`
  );
}

try {
  await main();
} catch (err) {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
}
