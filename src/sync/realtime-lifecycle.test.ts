import type { SupabaseClient } from "@supabase/supabase-js";
import { afterEach, expect, it, vi } from "vitest";
import { RealtimeManager } from "./realtime";
import type { SyncService } from "./service";

afterEach(() => vi.useRealTimers());

it("cancels pending debounce and ignores late channel signals after stop", async () => {
  vi.useFakeTimers();
  const sync = vi.fn().mockResolvedValue({ success: true });
  let signal!: () => void;
  const channel = {
    on: vi.fn(
      (
        _type: string,
        _filter: unknown,
        callback: (payload: unknown) => void
      ) => {
        signal = () => callback({ eventType: "INSERT" });
        return channel;
      }
    ),
    subscribe: vi.fn(),
  };
  const supabase = {
    channel: () => channel,
    removeChannel: vi.fn().mockResolvedValue(undefined),
  };
  const manager = new RealtimeManager(
    { sync, syncing: false } as unknown as SyncService,
    {
      enabled: true,
      userId: "owner",
      tables: ["entity"],
      supabase: supabase as unknown as SupabaseClient,
    }
  );
  await manager.start();
  signal();
  await manager.stop();
  signal(); // A channel may deliver a queued callback after unsubscribe.
  await vi.advanceTimersByTimeAsync(3000);
  expect(sync).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
  await manager.start();
  signal();
  await vi.advanceTimersByTimeAsync(2000);
  expect(sync).toHaveBeenCalledOnce();
  await manager.stop();
});
