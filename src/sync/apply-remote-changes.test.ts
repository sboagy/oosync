import { describe, expect, it } from "vitest";
import { sanitizeDataForSqliteBinding } from "./apply-remote-changes";

describe("sanitizeDataForSqliteBinding", () => {
  it("serializes object and array values before SQLite binding", () => {
    const sanitized = sanitizeDataForSqliteBinding({
      enabled: true,
      libOptions: { mode: "fsrs", queue: ["due", "new"] },
      fsrsParams: [0.4, 0.6],
      orderMode: "fsrs",
      practiceTimeLimit: 300,
      nullable: null,
    });

    expect(sanitized).toEqual({
      enabled: 1,
      libOptions: JSON.stringify({ mode: "fsrs", queue: ["due", "new"] }),
      fsrsParams: JSON.stringify([0.4, 0.6]),
      orderMode: "fsrs",
      practiceTimeLimit: 300,
      nullable: null,
    });
  });
});
