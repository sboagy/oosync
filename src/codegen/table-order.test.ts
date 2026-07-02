import { describe, expect, it } from "vitest";
import { orderSyncableTablesByDependency } from "./table-order";

describe("orderSyncableTablesByDependency", () => {
  it("orders syncable tables by FK-derived dependency order", () => {
    expect(
      orderSyncableTablesByDependency({
        syncableTables: ["child", "unrelated_b", "parent", "unrelated_a"],
        tableSyncOrder: {
          parent: 1,
          child: 2,
          unrelated_a: 3,
          unrelated_b: 3,
        },
      })
    ).toEqual(["parent", "child", "unrelated_a", "unrelated_b"]);
  });

  it("keeps deterministic order for tables without dependency metadata", () => {
    expect(
      orderSyncableTablesByDependency({
        syncableTables: ["late_b", "known", "late_a"],
        tableSyncOrder: {
          known: 1,
        },
      })
    ).toEqual(["known", "late_a", "late_b"]);
  });
});
