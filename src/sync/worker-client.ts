import type {
  SyncChange,
  SyncRequest,
  SyncRequestOverrides,
  SyncResponse,
} from "@oosync/shared/protocol";

const WORKER_URL = import.meta.env.VITE_WORKER_URL || "http://localhost:8787";
const SYNC_DIAGNOSTICS = import.meta.env.VITE_SYNC_DIAGNOSTICS === "true";
const DEFAULT_INITIAL_PAGE_COUNT = 16;

export class WorkerClient {
  constructor(private readonly token: string) {}

  async sync(
    changes: SyncChange[],
    lastSyncAt?: string,
    options?: {
      pullCursor?: string;
      syncStartedAt?: string;
      pageSize?: number;
      initialPageCount?: number;
      overrides?: SyncRequestOverrides | null;
    }
  ): Promise<SyncResponse> {
    const overrides = options?.overrides ?? undefined;
    const initialPageCount =
      options?.initialPageCount ??
      (lastSyncAt ? undefined : DEFAULT_INITIAL_PAGE_COUNT);
    const payload: SyncRequest = {
      changes,
      lastSyncAt,
      schemaVersion: 1,
      pullCursor: options?.pullCursor,
      syncStartedAt: options?.syncStartedAt,
      pageSize: options?.pageSize,
      initialPageCount,
      collectionsOverride: overrides?.collectionsOverride,
      rpcParamOverrides: overrides?.rpcParamOverrides,
      pullTables: overrides?.pullTables,
    };

    const response = await fetch(`${WORKER_URL}/api/sync`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.token}`,
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Sync failed: ${response.status} ${text}`);
    }

    const json = await response.json();

    if (SYNC_DIAGNOSTICS) {
      const total = Array.isArray(json.changes) ? json.changes.length : 0;
      console.log(`[WorkerClientDiag] response totalChanges=${total}`);
      if (Array.isArray(json.debug) && json.debug.length > 0) {
        for (const line of json.debug.slice(0, 50)) {
          console.log(`[WorkerClientDiag] worker: ${String(line)}`);
        }
      }
    }

    return json;
  }
}
