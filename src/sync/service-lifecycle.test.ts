import type { SupabaseClient } from "@supabase/supabase-js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SqliteDatabase } from "./runtime-context";
import { type SyncResult, SyncService } from "./service";

const mocks = vi.hoisted(() => ({
  sync: vi.fn(),
  up: vi.fn(),
  down: vi.fn(),
  tables: vi.fn(),
  stats: vi.fn(),
  persist: vi.fn(),
  clearTimestamp: vi.fn(),
}));
vi.mock("./engine", () => ({
  SyncEngine: class {
    sync = mocks.sync;
    syncUpFromOutbox = mocks.up;
    syncDown = mocks.down;
    syncDownTables = mocks.tables;
    getOutboxStats = mocks.stats;
    clearLastSyncTimestamp = mocks.clearTimestamp;
  },
}));
vi.mock("./runtime-context", () => ({
  getSyncRuntime: () => ({
    persistDb: mocks.persist,
    schema: { syncableTables: ["entity"] },
  }),
}));
const result: SyncResult = {
  success: true,
  errors: [],
  itemsSynced: 1,
  itemsFailed: 0,
  conflicts: 0,
  affectedTables: [],
  timestamp: "2026-11-21",
};
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function service() {
  return new SyncService({} as SqliteDatabase, {
    supabase: {} as SupabaseClient,
    userId: "owner",
    syncIntervalMs: 100,
  });
}
function realtimeService() {
  let status!: (value: string) => void;
  const channel = {
    on: () => channel,
    subscribe: vi.fn((callback: (value: string) => void) => {
      status = callback;
    }),
  };
  const removeChannel = vi.fn().mockResolvedValue(undefined);
  const instance = new SyncService({} as SqliteDatabase, {
    supabase: {
      channel: () => channel,
      removeChannel,
    } as unknown as SupabaseClient,
    userId: "owner",
    realtimeEnabled: true,
  });
  return {
    instance,
    channel,
    removeChannel,
    error: () => status("CHANNEL_ERROR"),
  };
}
beforeEach(() => {
  vi.useFakeTimers();
  vi.resetAllMocks();
  const events = new EventTarget();
  vi.stubGlobal("addEventListener", events.addEventListener.bind(events));
  vi.stubGlobal("dispatchEvent", events.dispatchEvent.bind(events));
  vi.stubGlobal("navigator", { onLine: true });
  mocks.stats.mockResolvedValue({ pending: 0, inProgress: 0 });
  for (const mock of [mocks.sync, mocks.up, mocks.down, mocks.tables])
    mock.mockResolvedValue(result);
  mocks.persist.mockResolvedValue(undefined);
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("SyncService shutdown", () => {
  it.each(["sync", "syncUp", "syncDown", "syncDownTables"] as const)(
    "drains active %s before allowing the database to close",
    async (method) => {
      const gate = deferred<SyncResult>();
      for (const mock of [mocks.sync, mocks.up, mocks.down, mocks.tables])
        mock.mockReturnValue(gate.promise);
      const instance = service();
      const operation =
        method === "syncDownTables" ? instance[method]([]) : instance[method]();
      let drained = false;
      const stopping = instance.destroy().then(() => {
        drained = true;
      });
      await Promise.resolve();
      expect(drained).toBe(false);
      await expect(instance.sync()).rejects.toThrow("destroyed");
      gate.resolve(result);
      await operation;
      await stopping;
      expect(drained).toBe(true);
    }
  );
  it("waits for post-download persistence", async () => {
    const gate = deferred<void>();
    mocks.persist.mockReturnValue(gate.promise);
    const instance = service();
    const operation = instance.syncDown();
    await vi.advanceTimersByTimeAsync(0);
    expect(mocks.persist).toHaveBeenCalledOnce();
    let drained = false;
    const stopping = instance.destroy().then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);
    gate.resolve();
    await operation;
    await stopping;
    expect(drained).toBe(true);
  });
  it("does not start timers after a stopped startup download finishes", async () => {
    const gate = deferred<SyncResult>();
    mocks.down.mockReturnValue(gate.promise);
    const instance = service();
    instance.startAutoSync();
    await vi.advanceTimersByTimeAsync(0);
    const stopping = instance.destroy();
    gate.resolve(result);
    await stopping;
    await vi.advanceTimersByTimeAsync(240000);
    expect(mocks.down).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("cancels delayed startup retries", async () => {
    mocks.down.mockRejectedValue(new Error("offline"));
    const instance = service();
    instance.startAutoSync();
    await vi.advanceTimersByTimeAsync(0);
    expect(mocks.down).toHaveBeenCalledOnce();
    await instance.destroy();
    await vi.advanceTimersByTimeAsync(5000);
    expect(mocks.down).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("removes a deferred online startup and allows pause/restart", async () => {
    vi.stubGlobal("navigator", { onLine: false });
    const instance = service();
    instance.startAutoSync();
    instance.stopAutoSync();
    vi.stubGlobal("navigator", { onLine: true });
    globalThis.dispatchEvent(new Event("online"));
    await vi.advanceTimersByTimeAsync(0);
    expect(mocks.down).not.toHaveBeenCalled();
    instance.startAutoSync();
    await vi.advanceTimersByTimeAsync(0);
    expect(mocks.down).toHaveBeenCalledOnce();
    await instance.destroy();
  });
  it("drains a periodic outbox probe without starting another upload", async () => {
    const instance = service();
    instance.startAutoSync();
    await vi.advanceTimersByTimeAsync(0);
    const gate = deferred<{ pending: number; inProgress: number }>();
    mocks.stats.mockReturnValue(gate.promise);
    await vi.advanceTimersByTimeAsync(100);
    let drained = false;
    const stopping = instance.destroy().then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);
    gate.resolve({ pending: 1, inProgress: 0 });
    await stopping;
    expect(mocks.up).not.toHaveBeenCalled();
  });
  it("drains persistence after a rejected upload without hiding the error", async () => {
    const gate = deferred<void>();
    mocks.up.mockRejectedValue(new Error("upload failed"));
    mocks.persist.mockReturnValue(gate.promise);
    const instance = service();
    const operation = instance.syncUp();
    const rejected = expect(operation).rejects.toThrow("upload failed");
    await vi.advanceTimersByTimeAsync(0);
    let drained = false;
    const stopping = instance.destroy().then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);
    gate.resolve();
    await rejected;
    await stopping;
    expect(drained).toBe(true);
  });
  it("permits a new startup after exhausting initial retries", async () => {
    const instance = service();
    mocks.down.mockRejectedValue(new Error("offline"));
    instance.startAutoSync();
    await vi.advanceTimersByTimeAsync(2000);
    expect(mocks.down).toHaveBeenCalledTimes(3);
    mocks.down.mockResolvedValue(result);
    instance.startAutoSync();
    await vi.advanceTimersByTimeAsync(0);
    expect(mocks.down).toHaveBeenCalledTimes(4);
    await instance.destroy();
  });

  it("retries Realtime startup after a channel error", async () => {
    const { instance, channel, error } = realtimeService();
    await instance.startRealtime();
    expect(channel.subscribe).toHaveBeenCalledOnce();
    error();
    await instance.startRealtime();
    expect(channel.subscribe).toHaveBeenCalledTimes(2);
    await instance.destroy();
  });

  it("allows retrying failed shutdown but remains closed to new work", async () => {
    const { instance, removeChannel } = realtimeService();
    await instance.startRealtime();
    removeChannel.mockRejectedValueOnce(new Error("unsubscribe failed"));
    await expect(instance.destroy()).rejects.toThrow("unsubscribe failed");
    await expect(instance.sync()).rejects.toThrow("destroyed");
    await instance.destroy();
    expect(removeChannel).toHaveBeenCalledTimes(2);
  });

  it("is idempotent and forbids restarting or clearing the watermark after destroy", async () => {
    const instance = service();
    const stopping = instance.destroy();
    expect(instance.destroy()).toBe(stopping);
    await stopping;
    expect(() => instance.startAutoSync()).toThrow("destroyed");
    await expect(instance.forceFullSyncDown()).rejects.toThrow("destroyed");
    expect(mocks.clearTimestamp).not.toHaveBeenCalled();
  });
});
