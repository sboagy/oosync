import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

describe("sync-core table-specific hack guard", () => {
  it("does not contain app-table-specific sync hacks in SyncEngine logic", () => {
    const candidates = [
      resolve(process.cwd(), "src/sync/engine.ts"),
      resolve(process.cwd(), "oosync/src/sync/engine.ts"),
    ];
    const enginePath = candidates.find((candidate) => existsSync(candidate));
    if (!enginePath) {
      throw new Error("Unable to locate sync engine source file for guard test");
    }

    const text = readFileSync(enginePath, "utf-8");

    expect(text).not.toContain('change.table === "entity_table"');
    expect(text).not.toContain(
      "entity_table has two distinct uniqueness constraints"
    );
    expect(text).not.toContain("backfillEntityTableOutbox");
  });
});
