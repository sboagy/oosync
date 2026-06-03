import { describe, expect, it } from "vitest";
import {
  createSqliteWasmDatabase,
  initSqliteWasm,
} from "./sqlite-wasm-adapter";

describe("sqlite-wasm adapter", () => {
  it("supports the Drizzle-compatible statement surface", async () => {
    const sqlite3 = await initSqliteWasm();
    const db = createSqliteWasmDatabase(sqlite3);

    db.run(
      "CREATE TABLE item (id TEXT PRIMARY KEY, n INTEGER, label TEXT, data BLOB)"
    );

    const insert = db.prepare(
      "INSERT INTO item (id, n, label, data) VALUES (?, ?, ?, ?)"
    );
    insert.run(["a", 1, "alpha", new Uint8Array([1, 2, 3])]);
    insert.run(["b", 2, null, null]);
    insert.free();

    const select = db.prepare(
      "SELECT id, n, label, data FROM item ORDER BY id"
    );
    select.bind([]);
    expect(select.step()).toBe(true);
    expect(select.get()).toEqual(["a", 1, "alpha", new Uint8Array([1, 2, 3])]);
    expect(select.step()).toBe(true);
    expect(select.getAsObject()).toEqual({
      id: "b",
      n: 2,
      label: null,
      data: null,
    });
    expect(select.step()).toBe(false);
    select.free();

    expect(db.exec("SELECT COUNT(*) AS total FROM item")[0]?.values).toEqual([
      [2],
    ]);

    db.run("BEGIN");
    db.run("UPDATE item SET n = n + 1 WHERE id = ?", ["a"]);
    db.run("COMMIT");

    const exported = db.export();
    db.close();

    const reloaded = createSqliteWasmDatabase(sqlite3, exported);
    expect(
      reloaded.exec("SELECT n FROM item WHERE id = 'a'")[0]?.values
    ).toEqual([[2]]);
    reloaded.close();
  });
});
