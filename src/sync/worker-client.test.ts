import { beforeEach, describe, expect, it, vi } from "vitest";
import { WorkerClient } from "./worker-client";

const fetchMock = vi.fn();

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
    });
    const client = new WorkerClient("token");

    await client.sync([]);

    expect(getRequestPayload().diagnostics).toBe(true);
    logSpy.mockRestore();
  });
});
