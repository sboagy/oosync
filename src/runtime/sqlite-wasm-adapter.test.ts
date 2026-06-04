import { describe, expect, it } from "vitest";
import {
  createSqliteWasmDatabase,
  initSqliteWasm,
} from "./sqlite-wasm-adapter";

describe("sqlite-wasm adapter", () => {
  it("supports the Drizzle-compatible statement surface", async () => {
    const sqlite3 = await initSqliteWasm();
    const db = createSqliteWasmDatabase(sqlite3);

    db.run("PRAGMA foreign_keys = ON");
    db.run("CREATE TABLE parent (id TEXT PRIMARY KEY)");
    db.run(
      "CREATE TABLE item (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parent(id), n INTEGER, label TEXT, data BLOB)"
    );
    db.run("INSERT INTO parent (id) VALUES (?)", ["parent-a"]);

    const insert = db.prepare(
      "INSERT INTO item (id, parent_id, n, label, data) VALUES (?, ?, ?, ?, ?)"
    );
    insert.run(["a", "parent-a", 1, "alpha", new Uint8Array([1, 2, 3])]);
    insert.run(["b", "parent-a", 2, null, null]);
    insert.free();

    const namedInsert = db.prepare(
      "INSERT INTO item (id, parent_id, n, label, data) VALUES ($id, $parentId, $n, $label, $data)"
    );
    namedInsert.run({
      $id: "c",
      $parentId: "parent-a",
      $n: 3,
      $label: "gamma",
      $data: new Uint8Array([9, 8, 7]),
    });
    namedInsert.free();

    expect(() =>
      db.run(
        "INSERT INTO item (id, parent_id, n, label, data) VALUES (?, ?, ?, ?, ?)",
        ["orphan", "missing-parent", 4, "orphan", null]
      )
    ).toThrow();

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
    expect(select.step()).toBe(true);
    expect(select.get()).toEqual(["c", 3, "gamma", new Uint8Array([9, 8, 7])]);
    expect(select.step()).toBe(false);
    select.free();

    expect(db.exec("SELECT COUNT(*) AS total FROM item")[0]?.values).toEqual([
      [3],
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
