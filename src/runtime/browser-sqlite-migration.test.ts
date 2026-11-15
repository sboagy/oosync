import { sqliteTable, text } from "drizzle-orm/sqlite-core";
import { IDBFactory } from "fake-indexeddb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  BrowserSqliteMigrationBlockedError,
  createBrowserSqliteClient,
  type IBrowserSqliteClient,
  type IBrowserSqliteHooks,
} from "./browser-sqlite";
import {
  createSqliteWasmDatabase,
  initSqliteWasm,
} from "./sqlite-wasm-adapter";

const queue = sqliteTable("sync_push_queue", {
  id: text("id"),
  tableName: text("table_name"),
  rowId: text("row_id"),
  operation: text("operation"),
  status: text("status"),
  changedAt: text("changed_at"),
  syncedAt: text("synced_at"),
  attempts: text("attempts"),
  lastError: text("last_error"),
});
const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() };
const user = "test-user";
const keys = {
  db: `db-${user}`,
  version: `version-${user}`,
  backup: `backup-${user}`,
};
const versionBytes = new Uint8Array([1, 0, 0, 0]);
let local: Map<string, string>;
let original: Uint8Array;
let initialized = vi.fn();
const clients: IBrowserSqliteClient<Record<string, unknown>>[] = [];

function makeClient(
  options: {
    version?: number;
    syncableTables?: string[];
    hook?: IBrowserSqliteHooks["canDiscardLocalTable"];
  } = {}
) {
  const client = createBrowserSqliteClient({
    schema: {},
    syncSchema: {
      syncableTables: options.syncableTables ?? ["item"],
      tableRegistry: {
        draft: {
          primaryKey: "body",
          uniqueKeys: ["body"],
          timestamps: [],
          booleanColumns: [],
          supportsIncremental: false,
          hasDeletedFlag: false,
        },
        item: {
          primaryKey: "id",
          uniqueKeys: ["id"],
          timestamps: [],
          booleanColumns: [],
          supportsIncremental: false,
          hasDeletedFlag: false,
        },
      },
      tableSyncOrder: { item: 1, draft: 2 },
      tableToSchemaKey: { item: "item", draft: "draft" },
    },
    syncPushQueue: queue,
    databaseVersion: options.version ?? 2,
    schemaVersion: "new",
    migrationFiles: ["/migration.sql"],
    forceResetQueryParams: [{ key: "reset", value: "true" }],
    storage: {
      indexedDbName: "test-db",
      indexedDbStore: "blobs",
      dbKeyPrefix: "db",
      dbVersionKeyPrefix: "version",
      outboxBackupKeyPrefix: "backup",
      lastSyncTimestampKeyPrefix: "watermark",
    },
    hooks: {
      logger,
      onDatabaseReady: initialized,
      canDiscardLocalTable: options.hook,
    },
  });
  clients.push(client);
  return client;
}

async function openStore(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("test-db");
    request.onupgradeneeded = () => request.result.createObjectStore("blobs");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}
async function put(key: string, value: Uint8Array): Promise<void> {
  const db = await openStore();
  try {
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction("blobs", "readwrite");
      tx.objectStore("blobs").put(value, key);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  } finally {
    db.close();
  }
}
async function get(key: string): Promise<unknown> {
  const db = await openStore();
  try {
    return await new Promise((resolve, reject) => {
      const request = db
        .transaction("blobs", "readonly")
        .objectStore("blobs")
        .get(key);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  } finally {
    db.close();
  }
}
async function seed(extraSql = ""): Promise<void> {
  const db = createSqliteWasmDatabase(await initSqliteWasm());
  try {
    db.run("CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT)");
    db.run("INSERT INTO item VALUES ('saved', 'remote copy exists')");
    db.run("CREATE TABLE sync_push_queue (status TEXT)");
    db.run(
      "CREATE TABLE sync_trigger_control (id INTEGER PRIMARY KEY, disabled INTEGER)"
    );
    db.run("INSERT INTO sync_trigger_control VALUES (1, 0)");
    if (extraSql) db.run(extraSql);
    original = db.export();
  } finally {
    db.close();
  }
  await put(keys.db, original);
  await put(keys.version, versionBytes);
}
async function expectPreserved(): Promise<void> {
  expect(await get(keys.db)).toEqual(original);
  expect(await get(keys.version)).toEqual(versionBytes);
  expect(local.get("schema_version")).toBe("old");
  expect(local.get(`watermark_${user}`)).toBe("before-upgrade");
  expect(initialized).not.toHaveBeenCalled();
  expect(fetch).not.toHaveBeenCalled();
}

beforeEach(() => {
  vi.stubGlobal("indexedDB", new IDBFactory());
  local = new Map([
    ["schema_version", "old"],
    [`watermark_${user}`, "before-upgrade"],
  ]);
  const storage = {
    getItem: (key: string) => local.get(key) ?? null,
    setItem: (key: string, value: string) => local.set(key, value),
    removeItem: (key: string) => local.delete(key),
  };
  vi.stubGlobal("localStorage", storage);
  vi.stubGlobal("window", {
    localStorage: storage,
    location: { search: "", href: "https://example.test/" },
    history: { replaceState: vi.fn() },
  });
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(
        new Response(
          "CREATE TABLE item (id TEXT PRIMARY KEY, value TEXT); CREATE TABLE draft (body TEXT PRIMARY KEY);"
        )
      )
  );
  initialized = vi.fn();
});
afterEach(async () => {
  for (const client of clients.splice(0)) await client.closeDb();
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

describe("pre-rebuild guard (real SQLite and IndexedDB requests)", () => {
  it.each(["pending", "failed", "in_progress", "unknown"])(
    "preserves the database for %s writes",
    async (status) => {
      await seed(`INSERT INTO sync_push_queue VALUES ('${status}')`);
      const client = makeClient();
      await expect(client.initializeDb(user)).rejects.toMatchObject({
        name: "BrowserSqliteMigrationBlockedError",
        reason: "outbox",
      });
      expect(client.getDebugState().dbReady).toBe(false);
      await expectPreserved();
    }
  );

  it("guards schema-only clearing before consumer hooks or watermark changes", async () => {
    await seed("INSERT INTO sync_push_queue VALUES ('pending')");
    await expect(
      makeClient({ version: 1 }).initializeDb(user)
    ).rejects.toBeInstanceOf(BrowserSqliteMigrationBlockedError);
    await expectPreserved();
  });

  it("does not let a forced-reset URL discard an unreplayed backup", async () => {
    await seed();
    const backup = new TextEncoder().encode(
      JSON.stringify({
        version: 1,
        items: [{ rowData: { secret: "keep me" } }],
      })
    );
    await put(keys.backup, backup);
    window.location.search = "?reset=true";
    await expect(makeClient().initializeDb(user)).rejects.toMatchObject({
      reason: "backup",
    });
    expect(await get(keys.backup)).toEqual(backup);
    await expectPreserved();
  });

  it("fails closed for an unreadable backup without logging its contents", async () => {
    await seed();
    await put(keys.backup, new TextEncoder().encode("private-invalid-json"));
    await expect(makeClient().initializeDb(user)).rejects.toMatchObject({
      reason: "inspection_failed",
    });
    expect(JSON.stringify(logger.error.mock.calls)).not.toContain(
      "private-invalid-json"
    );
    await expectPreserved();
  });

  it("blocks populated local-only tables without consumer approval", async () => {
    await seed(
      "CREATE TABLE draft (body TEXT); INSERT INTO draft VALUES ('local only');"
    );
    await expect(makeClient().initializeDb(user)).rejects.toMatchObject({
      reason: "local_data",
    });
    await expectPreserved();
  });

  it("blocks promotion of populated local-only data into the new sync schema", async () => {
    await seed(
      "CREATE TABLE draft (body TEXT); INSERT INTO draft VALUES ('local only');"
    );
    const hook = vi.fn(
      ({ tableName }: { tableName: string }) => tableName === "item"
    );
    await expect(
      makeClient({ syncableTables: ["item", "draft"], hook }).initializeDb(user)
    ).rejects.toMatchObject({ reason: "local_data" });
    expect(hook).toHaveBeenCalledWith(
      expect.objectContaining({ tableName: "draft" })
    );
    await expectPreserved();
  });

  it("requires approval even for a populated table already named in sync metadata", async () => {
    await seed();
    await expect(makeClient().initializeDb(user)).rejects.toMatchObject({
      reason: "local_data",
    });
    await expectPreserved();
  });

  it("allows promoted rows only after durable preservation is explicitly confirmed", async () => {
    await seed(
      "CREATE TABLE draft (body TEXT); INSERT INTO draft VALUES ('local only');"
    );
    const preservedRows = [["local only"]]; // Represents a verified durable export.
    const hook: NonNullable<IBrowserSqliteHooks["canDiscardLocalTable"]> = ({
      tableName,
      rawDb,
    }) => {
      if (tableName === "item") return true;
      expect(tableName).toBe("draft");
      expect(rawDb.exec("SELECT body FROM draft")[0]?.values).toEqual(
        preservedRows
      );
      return true;
    };
    await expect(
      makeClient({ syncableTables: ["item", "draft"], hook }).initializeDb(user)
    ).resolves.toBeDefined();
  });

  it("blocks a removed formerly synced table rather than losing its rows", async () => {
    await seed(
      "CREATE TABLE retired (value TEXT); INSERT INTO retired VALUES ('keep');"
    );
    await expect(makeClient().initializeDb(user)).rejects.toMatchObject({
      reason: "local_data",
    });
    await expectPreserved();
  });

  it("fails closed when a consumer inspection cannot complete", async () => {
    await seed(
      "CREATE TABLE draft (body TEXT); INSERT INTO draft VALUES ('local only');"
    );
    await expect(
      makeClient({
        hook: () => {
          throw new Error("secret details");
        },
      }).initializeDb(user)
    ).rejects.toMatchObject({ reason: "inspection_failed" });
    await expectPreserved();
  });

  it("provides a read-only snapshot to consumer inspections", async () => {
    await seed(
      "CREATE TABLE draft (body TEXT); INSERT INTO draft VALUES ('local only');"
    );
    await expect(
      makeClient({
        hook: ({ rawDb }) => {
          rawDb.run("DELETE FROM draft");
          return true;
        },
      }).initializeDb(user)
    ).rejects.toMatchObject({ reason: "inspection_failed" });
    await expectPreserved();
  });

  it("rebuilds a clean database after explicit approval of reconstructible local data", async () => {
    await seed(
      "CREATE TABLE cache (value TEXT); INSERT INTO cache VALUES ('reconstructible');"
    );
    const hook = vi.fn(() => true);
    const client = makeClient({ hook });
    await client.initializeDb(user);
    expect(hook).toHaveBeenCalledWith(
      expect.objectContaining({ tableName: "cache", userId: user })
    );
    expect(
      (await client.getSqliteInstance())?.exec("SELECT * FROM item")[0]?.values
    ).toEqual([]);
    expect(local.has(`watermark_${user}`)).toBe(false);
    expect(local.get("schema_version")).toBe("new");
    expect(initialized).toHaveBeenCalledOnce();
    await client.persistDb();
    expect(await get(keys.version)).toEqual(new Uint8Array([2, 0, 0, 0]));
  });

  it("permits empty local tables and an empty backup", async () => {
    await seed("DELETE FROM item; CREATE TABLE draft (body TEXT)");
    await put(
      keys.backup,
      new TextEncoder().encode(JSON.stringify({ version: 1, items: [] }))
    );
    await expect(makeClient().initializeDb(user)).resolves.toBeDefined();
  });

  it("does not block normal reloads with pending writes", async () => {
    await seed("INSERT INTO sync_push_queue VALUES ('pending')");
    local.set("schema_version", "new");
    const client = makeClient({ version: 1 });
    // Normal startup installs outbox indexes: use the real schema for this case.
    const db = createSqliteWasmDatabase(await initSqliteWasm(), original);
    db.run("DROP TABLE sync_push_queue");
    db.run(
      "CREATE TABLE sync_push_queue (id TEXT, table_name TEXT, row_id TEXT, operation TEXT, status TEXT, changed_at TEXT, synced_at TEXT, attempts INTEGER, last_error TEXT)"
    );
    db.run(
      "INSERT INTO sync_push_queue (id, status) VALUES ('pending-id', 'pending')"
    );
    await put(keys.db, db.export());
    db.close();
    await client.initializeDb(user);
    expect(
      (await client.getSqliteInstance())?.exec(
        "SELECT id FROM sync_push_queue"
      )[0]?.values
    ).toEqual([["pending-id"]]);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("can retry safely after a failed initialization once the old database is synchronized", async () => {
    await seed("INSERT INTO sync_push_queue VALUES ('failed')");
    const client = makeClient({
      hook: ({ tableName }) => tableName === "item",
    });
    await expect(client.initializeDb(user)).rejects.toMatchObject({
      reason: "outbox",
    });
    await expectPreserved();
    // A compatible app completes the push and persists the old-version DB.
    const oldDb = createSqliteWasmDatabase(await initSqliteWasm(), original);
    oldDb.run("DELETE FROM sync_push_queue");
    await put(keys.db, oldDb.export());
    oldDb.close();
    await expect(client.initializeDb(user)).resolves.toBeDefined();
  });
});
