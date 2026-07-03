import type {
  SyncChange,
  SyncRequest,
  SyncRequestOverrides,
  SyncResponse,
} from "@oosync/shared/protocol";

const WORKER_URL = import.meta.env.VITE_WORKER_URL || "http://localhost:8787";
const SYNC_DIAGNOSTICS = import.meta.env.VITE_SYNC_DIAGNOSTICS === "true";
const DEFAULT_INITIAL_PAGE_COUNT = 16;
const DIAGNOSTICS_STORAGE_KEY = "oosync:sync-diagnostics";
const CONSUMER_DIAGNOSTICS_STORAGE_KEYS = [
  "tunetrees:sync-baseline-diagnostics",
];

function parseDiagnosticsFlag(value: string | null): boolean | null {
  if (value === null) {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  if (["", "1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return null;
}

function isRuntimeDiagnosticsEnabled(): boolean {
  if (SYNC_DIAGNOSTICS) {
    return true;
  }

  if (typeof window === "undefined") {
    return false;
  }

  try {
    const params = new URLSearchParams(window.location.search);
    const queryFlag =
      parseDiagnosticsFlag(params.get("ttSyncDiagnostics")) ??
      parseDiagnosticsFlag(params.get("syncDiagnostics"));
    if (queryFlag !== null) {
      window.localStorage.setItem(DIAGNOSTICS_STORAGE_KEY, String(queryFlag));
      return queryFlag;
    }

    const storedFlag = parseDiagnosticsFlag(
      window.localStorage.getItem(DIAGNOSTICS_STORAGE_KEY)
    );
    if (storedFlag !== null) {
      return storedFlag;
    }

    return CONSUMER_DIAGNOSTICS_STORAGE_KEYS.some(
      (key) => parseDiagnosticsFlag(window.localStorage.getItem(key)) === true
    );
  } catch {
    return false;
  }
}

export class WorkerClient {
  private readonly authToken: string;

  constructor(token: string) {
    this.authToken = token;
  }

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
    const diagnosticsEnabled = isRuntimeDiagnosticsEnabled();
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
      diagnostics: diagnosticsEnabled ? true : undefined,
    };

    const response = await fetch(`${WORKER_URL}/api/sync`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.authToken}`,
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Sync failed: ${response.status} ${text}`);
    }

    const json = await response.json();

    if (diagnosticsEnabled) {
      const total = Array.isArray(json.changes) ? json.changes.length : 0;
      console.log(`[WorkerClientDiag] response totalChanges=${total}`);
      if (Array.isArray(json.debug) && json.debug.length > 0) {
        for (const line of json.debug.slice(0, 200)) {
          console.log(`[WorkerClientDiag] worker: ${String(line)}`);
        }
      }
    }

    return json;
  }
}
