import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  createSqliteWasmDatabase,
  initSqliteWasm,
} from "./sqlite-wasm-adapter";

describe("sqlite-wasm adapter", () => {
  it("preserves a pre-upgrade database and its pending writes across reloads", async () => {
    // Exported by @sqlite.org/sqlite-wasm@3.53.0-build1. Keep these bytes
    // unchanged: generating them with the installed version would hide regressions.
    // The fixture records the original SQL and engine version for reproducibility.
    const fixture = JSON.parse(
      readFileSync(
        new URL("./fixtures/sqlite-3.53.0.json", import.meta.url),
        "utf8"
      )
    ) as { sqliteVersion: string; databaseBase64: string };
    expect(fixture.sqliteVersion).toBe("3.53.0");
    const sqlite3 = await initSqliteWasm();
    const db = createSqliteWasmDatabase(
      sqlite3,
      new Uint8Array(Buffer.from(fixture.databaseBase64, "base64"))
    );
    let exported: Uint8Array;
    try {
      expect(db.exec("PRAGMA integrity_check")[0]?.values).toEqual([["ok"]]);
      expect(db.exec("PRAGMA user_version")[0]?.values).toEqual([[7]]);
      expect(db.exec("SELECT * FROM item")[0]?.values).toEqual([
        ["existing", "saved before upgrade", new Uint8Array([0, 1, 254, 255])],
      ]);
      // Exceed the original allocation to exercise RESIZEABLE deserialization.
      db.run("UPDATE item SET data = ? WHERE id = ?", [
        new Uint8Array(65536).fill(42),
        "existing",
      ]);
      db.run("BEGIN");
      db.run("DELETE FROM pending");
      db.run("ROLLBACK");
      exported = db.export();
    } finally {
      db.close();
    }
    const restored = createSqliteWasmDatabase(sqlite3, exported);
    try {
      expect(restored.exec("PRAGMA integrity_check")[0]?.values).toEqual([
        ["ok"],
      ]);
      expect(restored.exec("SELECT data FROM item")[0]?.values).toEqual([
        [new Uint8Array(65536).fill(42)],
      ]);
      expect(
        restored.exec("SELECT * FROM pending ORDER BY rowid")[0]?.values
      ).toEqual([
        ["existing", "insert"],
        ["existing", "update"],
      ]);
    } finally {
      restored.close();
    }
  });

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

    const getWithoutStep = db.prepare(
      "SELECT id, n FROM item WHERE id = ? LIMIT 1"
    );
    expect(getWithoutStep.get(["a"])).toEqual(["a", 1]);
    expect(getWithoutStep.getAsObject(["c"])).toEqual({
      id: "c",
      n: 3,
    });
    expect(getWithoutStep.get(["missing"])).toEqual([]);
    getWithoutStep.free();

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
