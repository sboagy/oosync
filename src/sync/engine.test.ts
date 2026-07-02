import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SqliteDatabase, SyncRuntime } from "./runtime-context";
import { setSyncRuntime } from "./runtime-context";

const workerSyncMock = vi.fn();
const applyRemoteChangesMock = vi.fn();
const getPendingOutboxItemsMock = vi.fn();
const markOutboxCompletedMock = vi.fn();

vi.mock("./worker-client", () => ({
  WorkerClient: class {
    sync = workerSyncMock;
  },
}));

vi.mock("./apply-remote-changes", () => ({
  applyRemoteChangesToLocalDb: applyRemoteChangesMock,
}));

vi.mock("./outbox", () => ({
  backfillOutboxSince: vi.fn().mockResolvedValue(0),
  fetchLocalRowByPrimaryKey: vi.fn(),
  getOutboxStats: vi.fn(),
  getPendingOutboxItems: getPendingOutboxItemsMock,
  markOutboxCompleted: markOutboxCompletedMock,
}));

function configureTestRuntime(): void {
  const runtime = {
    schema: {
      syncableTables: ["entity_table"],
      tableRegistry: {} as Record<string, unknown>,
      tableSyncOrder: { entity_table: 1 },
      tableToSchemaKey: { entity_table: "entityTable" },
    },
    localSchema: { entityTable: { name: "entity_table" } },
    syncPushQueue: {} as SyncRuntime["syncPushQueue"],
    getSqliteInstance: async () => null,
    loadOutboxBackupForUser: async () => null,
    clearOutboxBackupForUser: async () => {},
    replayOutboxBackup: () => ({ applied: 0, skipped: 0, errors: [] }),
    enableSyncTriggers: () => {},
    suppressSyncTriggers: () => {},
    logger: {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    },
  } as unknown as SyncRuntime;

  setSyncRuntime(runtime);
}

function createSupabaseClient() {
  return {
    auth: {
      getSession: vi.fn().mockResolvedValue({
        data: {
          session: {
            access_token: "token",
          },
        },
      }),
    },
  };
}

beforeEach(() => {
  configureTestRuntime();
  workerSyncMock.mockReset();
  applyRemoteChangesMock.mockReset();
  getPendingOutboxItemsMock.mockReset();
  markOutboxCompletedMock.mockReset();
  getPendingOutboxItemsMock.mockResolvedValue([]);
  applyRemoteChangesMock.mockImplementation(async ({ changes }) => ({
    synced: changes.length,
    failed: 0,
    errors: [],
    affectedTables: changes.map((change: { table: string }) => change.table),
  }));

  vi.stubGlobal("localStorage", {
    getItem: vi.fn().mockReturnValue(null),
    setItem: vi.fn(),
    removeItem: vi.fn(),
  });
});

describe("SyncEngine initial sync paging", () => {
  it("reduces page size after a retriable initial-sync page failure", async () => {
    workerSyncMock
      .mockRejectedValueOnce(new Error("Sync failed: 503 cpu limit"))
      .mockResolvedValueOnce({
        changes: [
          {
            table: "entity_table",
            rowId: "row-1",
            data: { id: "row-1" },
            deleted: false,
            lastModifiedAt: "2024-01-01T00:00:00.000Z",
          },
        ],
        nextCursor: "cursor-2",
        syncStartedAt: "2024-01-01T00:00:00.000Z",
        syncedAt: "2024-01-01T00:00:00.000Z",
      })
      .mockResolvedValueOnce({
        changes: [
          {
            table: "entity_table",
            rowId: "row-2",
            data: { id: "row-2" },
            deleted: false,
            lastModifiedAt: "2024-01-01T00:00:01.000Z",
          },
        ],
        nextCursor: undefined,
        syncStartedAt: "2024-01-01T00:00:00.000Z",
        syncedAt: "2024-01-01T00:00:02.000Z",
      });

    const { SyncEngine } = await import("./engine");
    const engine = new SyncEngine(
      {} as SqliteDatabase,
      createSupabaseClient() as never,
      "user-1"
    );

    const result = await engine.syncWithWorker();

    expect(result.success).toBe(true);
    expect(workerSyncMock).toHaveBeenCalledTimes(3);
    expect(workerSyncMock.mock.calls.map((call) => call[2]?.pageSize)).toEqual([
      200, 100, 100,
    ]);
    expect(
      workerSyncMock.mock.calls.map((call) => call[2]?.initialPageCount)
    ).toEqual([16, 16, 16]);
    expect(localStorage.setItem).toHaveBeenCalledWith(
      "TT_LAST_SYNC_TIMESTAMP_user-1",
      "2024-01-01T00:00:00.000Z"
    );
  });

  it("does not retry smaller page sizes for non-retriable initial-sync failures", async () => {
    workerSyncMock.mockRejectedValueOnce(
      new Error("Sync failed: 400 bad request")
    );

    const { SyncEngine } = await import("./engine");
    const engine = new SyncEngine(
      {} as SqliteDatabase,
      createSupabaseClient() as never,
      "user-2"
    );

    const result = await engine.syncWithWorker();

    expect(result.success).toBe(false);
    expect(workerSyncMock).toHaveBeenCalledTimes(1);
    expect(workerSyncMock.mock.calls[0][2]?.pageSize).toBe(200);
    expect(workerSyncMock.mock.calls[0][2]?.initialPageCount).toBe(16);
    expect(result.errors[0]).toContain("Sync failed: 400");
  });
});
