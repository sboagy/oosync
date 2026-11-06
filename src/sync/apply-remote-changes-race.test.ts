import { drizzle } from "drizzle-orm/sql-js";
import { sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createSqliteWasmDatabase,
  type IRawSqliteDatabase,
  initSqliteWasm,
} from "../runtime/sqlite-wasm-adapter";
import type { SyncChange } from "../shared/protocol";
import { clearAdapterCache } from "./adapters";
import { applyRemoteChangesToLocalDb } from "./apply-remote-changes";
import {
  getSyncRuntime,
  type SqliteDatabase,
  type SyncRuntime,
  setSyncRuntime,
} from "./runtime-context";

const item = sqliteTable(
  "item",
  {
    id: text("id").primaryKey(),
    owner: text("owner"),
    value: text("value"),
    lastModifiedAt: text("last_modified_at"),
  },
  (table) => [uniqueIndex("item_owner_unique").on(table.owner)]
);
const before = "2026-11-06T16:14:53.537Z";
const after = "2026-11-06T16:14:54.400Z";

// Use actual SQLite and Drizzle conflict handling: mocked inserts cannot show
// whether a delayed response overwrites a row written while the request ran.
describe("remote changes racing with local edits", () => {
  let raw: IRawSqliteDatabase;
  let db: SqliteDatabase;

  beforeEach(async () => {
    raw = createSqliteWasmDatabase(await initSqliteWasm());
    raw.run(
      "CREATE TABLE item (id TEXT PRIMARY KEY, owner TEXT UNIQUE, value TEXT, last_modified_at TEXT)"
    );
    db = drizzle(raw as unknown as Parameters<typeof drizzle>[0]);
    clearAdapterCache();
    setSyncRuntime({
      schema: {
        syncableTables: ["item"],
        tableRegistry: {
          item: {
            primaryKey: "id",
            uniqueKeys: ["owner"],
            timestamps: ["last_modified_at"],
            booleanColumns: [],
            supportsIncremental: true,
            hasDeletedFlag: false,
          },
        },
        tableSyncOrder: { item: 0 },
        tableToSchemaKey: { item: "item" },
      },
      localSchema: { item },
      syncPushQueue: {} as SyncRuntime["syncPushQueue"],
      getSqliteInstance: async () => raw,
      loadOutboxBackupForUser: async () => null,
      clearOutboxBackupForUser: async () => {},
      replayOutboxBackup: () => ({ applied: 0, skipped: 0, errors: [] }),
      suppressSyncTriggers: vi.fn(),
      enableSyncTriggers: vi.fn(),
      logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    });
  });

  afterEach(() => {
    raw.close();
    clearAdapterCache();
  });

  function response(
    timestamp = before,
    id = "local-id",
    deleted = false
  ): SyncChange {
    return {
      table: "item",
      rowId: id,
      deleted,
      lastModifiedAt: timestamp,
      data: {
        id,
        owner: "owner-a",
        value: "remote",
        lastModifiedAt: timestamp,
      },
    };
  }

  async function seedLocal(timestamp = after) {
    await db
      .insert(item)
      .values({
        id: "local-id",
        owner: "owner-a",
        value: "local edit",
        lastModifiedAt: timestamp,
      })
      .run();
  }

  it("preserves an edit made after the uploaded row was captured", async () => {
    const delayedResponse = response();
    await seedLocal();
    const result = await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [delayedResponse],
    });
    expect(result.errors).toEqual([]);
    expect(await db.select().from(item)).toEqual([
      {
        id: "local-id",
        owner: "owner-a",
        value: "local edit",
        lastModifiedAt: after,
      },
    ]);
  });

  it("preserves the newer row when remote and local IDs differ on a natural key", async () => {
    await seedLocal();
    const result = await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [response(before, "remote-id")],
    });
    expect(result.errors).toEqual([]);
    expect((await db.select().from(item))[0]?.value).toBe("local edit");
  });

  it("does not delete a newer local edit", async () => {
    await seedLocal();
    await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [response(before, "local-id", true)],
    });
    expect((await db.select().from(item))[0]?.value).toBe("local edit");
  });

  it("protects tables whose only conflict target is the primary key", async () => {
    const runtime = getSyncRuntime();
    setSyncRuntime({
      ...runtime,
      schema: {
        ...runtime.schema,
        tableRegistry: {
          item: { ...runtime.schema.tableRegistry.item, uniqueKeys: null },
        },
      },
    });
    await seedLocal();
    await applyRemoteChangesToLocalDb({ localDb: db, changes: [response()] });
    expect((await db.select().from(item))[0]?.value).toBe("local edit");
  });

  it("inserts remote rows that do not exist locally", async () => {
    const result = await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [response()],
    });
    expect(result.errors).toEqual([]);
    expect((await db.select().from(item))[0]?.value).toBe("remote");
  });

  it("applies newer remote deletes", async () => {
    await seedLocal(before);
    await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [response(after, "local-id", true)],
    });
    expect(await db.select().from(item)).toEqual([]);
  });

  it("applies a newer natural-key conflict while preserving the local ID", async () => {
    await seedLocal(before);
    const result = await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [response(after, "remote-id")],
    });
    expect(result.errors).toEqual([]);
    expect((await db.select().from(item))[0]).toMatchObject({
      id: "local-id",
      value: "remote",
    });
  });

  it("still applies changes to tables without sync timestamps", async () => {
    const plainItem = sqliteTable("item", {
      id: text("id").primaryKey(),
      owner: text("owner"),
      value: text("value"),
    });
    setSyncRuntime({ ...getSyncRuntime(), localSchema: { item: plainItem } });
    await seedLocal();
    const result = await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [response()],
    });
    expect(result.errors).toEqual([]);
    expect((await db.select().from(item))[0]?.value).toBe("remote");
  });

  it("still applies a newer remote change", async () => {
    await seedLocal(before);
    await applyRemoteChangesToLocalDb({
      localDb: db,
      changes: [response(after)],
    });
    expect((await db.select().from(item))[0]?.value).toBe("remote");
  });

  it("compares timestamp instants rather than ISO string formatting", async () => {
    await seedLocal("2026-11-06 16:14:54.400+00:00");
    await applyRemoteChangesToLocalDb({ localDb: db, changes: [response()] });
    expect((await db.select().from(item))[0]?.value).toBe("local edit");
  });

  it("accepts equal timestamps so server normalization is retained", async () => {
    await seedLocal(before);
    await applyRemoteChangesToLocalDb({ localDb: db, changes: [response()] });
    expect((await db.select().from(item))[0]?.value).toBe("remote");
  });
});
