import { beforeEach, describe, expect, it, vi } from "vitest";
import { WorkerClient } from "./worker-client";

const fetchMock = vi.fn();

function createStorage(initial: Record<string, string> = {}) {
  const values = new Map(Object.entries(initial));
  return {
    getItem: vi.fn((key: string) => values.get(key) ?? null),
    setItem: vi.fn((key: string, value: string) => {
      values.set(key, value);
    }),
  };
}

beforeEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockReset();
  fetchMock.mockResolvedValue({
    ok: true,
    json: async () => ({
      changes: [],
      syncedAt: "2024-01-01T00:00:00.000Z",
    }),
  });
  vi.stubGlobal("fetch", fetchMock);
});

function getRequestPayload(): Record<string, unknown> {
  const init = fetchMock.mock.calls[0]?.[1] as RequestInit | undefined;
  expect(init?.body).toBeTypeOf("string");
  return JSON.parse(init?.body as string) as Record<string, unknown>;
}

describe("WorkerClient initial pull batching", () => {
  it("requests batched initial pages for initial sync", async () => {
    const client = new WorkerClient("token");

    await client.sync([]);

    expect(getRequestPayload().initialPageCount).toBe(16);
  });

  it("does not send initial page batching hints for incremental sync", async () => {
    const client = new WorkerClient("token");

    await client.sync([], "2024-01-01T00:00:00.000Z");

    expect(getRequestPayload()).not.toHaveProperty("initialPageCount");
  });

  it("requests diagnostics when enabled by the runtime URL", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    vi.stubGlobal("window", {
      location: {
        search: "?ttSyncDiagnostics=1",
      },
      localStorage: createStorage(),
    });
    const client = new WorkerClient("token");

    await client.sync([]);

    expect(getRequestPayload().diagnostics).toBe(true);
    logSpy.mockRestore();
  });

  it("requests diagnostics when enabled by consumer localStorage", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    vi.stubGlobal("window", {
      location: {
        search: "",
      },
      localStorage: createStorage({
        "tunetrees:sync-baseline-diagnostics": "true",
      }),
    });
    const client = new WorkerClient("token");

    await client.sync([]);

    expect(getRequestPayload().diagnostics).toBe(true);
    logSpy.mockRestore();
  });
});
