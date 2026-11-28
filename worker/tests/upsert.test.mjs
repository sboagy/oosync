import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { after, before, test } from "node:test";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { sql } from "drizzle-orm";
import { integer, pgTable, text } from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { applyUpsert } from "../src/upsert.ts";

const url = new URL(
  process.env.OOSYNC_TEST_DATABASE_URL ??
    "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
);
before(() => {
  assert.equal(
    url.search,
    "",
    "Test database URLs must not contain connection overrides"
  );
  assert.ok(
    ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname),
    "Upsert tests require local Postgres"
  );
});
const client = postgres(url.toString(), {
  max: 1,
  prepare: false,
  connect_timeout: 5,
});
const db = drizzle(client);
after(() => client.end({ timeout: 5 }));
const table = pgTable("upsert_probe", {
  id: integer("id").primaryKey(),
  owner: text("owner").notNull(),
  event: text("event"),
  value: text("value").notNull(),
});
const primary = [{ col: table.id, prop: "id" }];
const natural = [
  { col: table.owner, prop: "owner" },
  { col: table.event, prop: "event" },
];
// Static trigger DDL, PG15+ NULLS NOT DISTINCT, and bare expression fragments
// use sql.raw: the editor SQL parser does not support these forms. These contain
// no interpolated input and are validated against real PostgreSQL below.
const options = { omitSetProps: ["id", "owner"] };
async function fixture(run) {
  await db.transaction(async (tx) => {
    await tx.execute(
      sql`create temporary table upsert_probe (id integer primary key, owner text not null, event text, value text not null check (value <> 'invalid'), unique(owner,event)) on commit drop`
    );
    await run(tx);
  });
}
const upsert = (tx, data) =>
  applyUpsert(tx, table, natural, primary, data, options);

test("reproduces the old NULL unique-key primary-key violation", async () => {
  await fixture(async (tx) => {
    const data = { id: 1, owner: "a", event: null, value: "original" };
    await tx.insert(table).values(data);
    await assert.rejects(
      tx.transaction((sp) =>
        sp
          .insert(table)
          .values(data)
          .onConflictDoUpdate({
            target: [table.owner, table.event],
            set: { value: "retry" },
          })
      ),
      (error) => error.cause?.code === "23505"
    );
  });
});

test("NULL-key uploads update the same ID repeatedly and preserve omitted update properties", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    for (let i = 0; i < 2; i++)
      await upsert(tx, { id: 1, owner: "a", event: null, value: "updated" });
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "updated" },
    ]);
  });
});

test("omitted nullable unique-key component updates a NULL placeholder", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await upsert(tx, { id: 1, owner: "a", value: "updated" });
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "updated" },
    ]);
  });
});

test("complete natural keys still deduplicate different incoming IDs", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: "first", value: "original" });
    await upsert(tx, { id: 2, owner: "a", event: "first", value: "updated" });
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: "first", value: "updated" },
    ]);
  });
});

test("distinct IDs with NULL natural keys remain distinct rows", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "one" });
    await upsert(tx, { id: 2, owner: "a", event: null, value: "two" });
    assert.equal((await tx.select().from(table)).length, 2);
  });
});

test("constraint failures propagate, with savepoint rollback and a usable transaction", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await assert.rejects(
      upsert(tx, { id: 1, owner: "a", event: null, value: "invalid" }),
      (error) => error.cause?.code === "23514"
    );
    assert.equal((await tx.select().from(table))[0].value, "original");
    await upsert(tx, { id: 1, owner: "a", value: "retry" });
    assert.equal((await tx.select().from(table))[0].value, "retry");
  });
});

test("NULL-key fallback supports composite primary keys", async () => {
  await db.transaction(async (tx) => {
    await tx.execute(
      sql`create temporary table composite_probe (owner text, id integer, event text, value text, primary key(owner,id), unique(owner,event)) on commit drop`
    );
    const composite = pgTable("composite_probe", {
      owner: text("owner"),
      id: integer("id"),
      event: text("event"),
      value: text("value"),
    });
    const pk = [
      { col: composite.owner, prop: "owner" },
      { col: composite.id, prop: "id" },
    ];
    const unique = [
      { col: composite.owner, prop: "owner" },
      { col: composite.event, prop: "event" },
    ];
    for (const owner of ["a", "b"])
      await applyUpsert(tx, composite, unique, pk, {
        owner,
        id: 1,
        event: null,
        value: "original",
      });
    await applyUpsert(tx, composite, unique, pk, {
      owner: "a",
      id: 1,
      event: null,
      value: "updated",
    });
    assert.deepEqual(
      await tx.select().from(composite).orderBy(composite.owner),
      [
        { owner: "a", id: 1, event: null, value: "updated" },
        { owner: "b", id: 1, event: null, value: "original" },
      ]
    );
  });
});

test("primary-key fallback rejects a different natural key without changing the row", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await assert.rejects(
      tx.transaction((sp) =>
        upsert(sp, { id: 1, owner: "b", event: null, value: "attack" })
      ),
      /Upsert returned no row: table=upsert_probe; conflictTarget=\(id\); primaryKeyFallback=true; naturalKeyGuard=\(owner,event\)/
    );
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "original" },
    ]);
  });
});

test("omitted defaulted natural keys still deduplicate different IDs", async () => {
  await fixture(async (tx) => {
    await tx.execute(
      sql`alter table upsert_probe alter column event set default 'first'`
    );
    const defaulted = pgTable("upsert_probe", {
      id: integer("id").primaryKey(),
      owner: text("owner").notNull(),
      event: text("event").default("first"),
      value: text("value").notNull(),
    });
    const unique = [
      { col: defaulted.owner, prop: "owner" },
      { col: defaulted.event, prop: "event" },
    ];
    const pk = [{ col: defaulted.id, prop: "id" }];
    await applyUpsert(
      tx,
      defaulted,
      unique,
      pk,
      { id: 1, owner: "a", value: "original" },
      options
    );
    await applyUpsert(
      tx,
      defaulted,
      unique,
      pk,
      { id: 2, owner: "a", value: "updated" },
      options
    );
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: "first", value: "updated" },
    ]);
  });
});

test("omitted or NULL keys cannot overwrite an existing non-NULL natural key", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: "first", value: "original" });
    for (const data of [
      { id: 1, owner: "a", value: "omitted" },
      { id: 1, owner: "a", event: null, value: "null" },
    ]) {
      await assert.rejects(
        tx.transaction((sp) => upsert(sp, data)),
        /Upsert returned no row/
      );
    }
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: "first", value: "original" },
    ]);
  });
});

test("a BEFORE INSERT trigger can skip an upsert without a key conflict", async () => {
  await fixture(async (tx) => {
    await tx.execute(sql`create or replace function pg_temp.skip_upsert_write()
      returns trigger language plpgsql as $$ begin return null; end $$`);
    await tx.execute(
      sql.raw(`create trigger skip_write before insert on upsert_probe
      for each row execute function pg_temp.skip_upsert_write()`)
    );
    await assert.rejects(
      tx.transaction((sp) =>
        upsert(sp, { id: 1, owner: "a", event: "first", value: "original" })
      ),
      (error) => {
        assert.match(error.message, /primaryKeyFallback=false/);
        assert.match(error.message, /Check BEFORE INSERT\/UPDATE triggers/);
        assert.doesNotMatch(error.message, /Natural-key changes/);
        return true;
      }
    );
    assert.deepEqual(await tx.select().from(table), []);
  });
});

test("a BEFORE UPDATE trigger can skip an upsert with a matching natural key", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await tx.execute(sql`create or replace function pg_temp.skip_upsert_write()
      returns trigger language plpgsql as $$ begin return null; end $$`);
    await tx.execute(
      sql.raw(`create trigger skip_write before update on upsert_probe
      for each row execute function pg_temp.skip_upsert_write()`)
    );
    await assert.rejects(
      tx.transaction((sp) =>
        upsert(sp, { id: 1, owner: "a", event: null, value: "updated" })
      ),
      /Upsert returned no row/
    );
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "original" },
    ]);
  });
});

test("lenient generated defaults preserve natural-key deduplication", async () => {
  const schemaName = `upsert_codegen_${randomUUID().replaceAll("-", "")}`;
  let schemaCreated = false;
  const directory = await mkdtemp(path.join(process.cwd(), ".upsert-codegen-"));
  try {
    await client.unsafe(`create schema ${schemaName}`);
    schemaCreated = true;
    await client.unsafe(
      `create table ${schemaName}.sql (id integer primary key, value text default lower('FIRST'))`
    );
    await client.unsafe(`create table ${schemaName}.probe (
      id integer primary key, owner text not null,
      event text default lower('FIRST'::text), value text not null,
      unique(owner, event))`);
    const outputs = Object.fromEntries(
      [
        "sqliteSchemaFile",
        "tableMetaFile",
        "appTableMetaFile",
        "workerPgSchemaFile",
        "workerConfigFile",
      ].map((name) => [name, path.join(directory, `${name}.ts`)])
    );
    const configPath = path.join(directory, "config.json");
    await writeFile(configPath, JSON.stringify({ outputs }));
    const args = [
      "--import",
      "tsx",
      "src/codegen-schema.ts",
      "--lenient",
      `--schema=${schemaName}`,
      `--config=${configPath}`,
    ];
    const runGenerator = () =>
      promisify(execFile)(process.execPath, args, {
        env: { ...process.env, OOSYNC_DATABASE_URL: url.toString() },
      });
    await runGenerator();
    const sqliteOutput = await readFile(outputs.sqliteSchemaFile, "utf8");
    assert.doesNotMatch(sqliteOutput, /lower\(/);
    // Exercise drift detection as well as the generated worker artifact.
    args.push("--check");
    await runGenerator();
    const { tables } = await import(
      pathToFileURL(outputs.workerPgSchemaFile).href
    );
    assert.equal(tables.sql.value.hasDefault, true);
    const generated = tables.probe;
    assert.equal(generated.event.hasDefault, true);
    const unique = [
      { col: generated.owner, prop: "owner" },
      { col: generated.event, prop: "event" },
    ];
    const pk = [{ col: generated.id, prop: "id" }];
    await db.transaction(async (tx) => {
      await applyUpsert(
        tx,
        generated,
        unique,
        pk,
        { id: 1, owner: "a", value: "original" },
        options
      );
      await applyUpsert(
        tx,
        generated,
        unique,
        pk,
        { id: 2, owner: "a", value: "updated" },
        options
      );
      await applyUpsert(
        tx,
        generated,
        unique,
        pk,
        { id: 3, owner: "a", event: null, value: "null original" },
        options
      );
      await applyUpsert(
        tx,
        generated,
        unique,
        pk,
        { id: 3, owner: "a", event: null, value: "null updated" },
        options
      );
      assert.deepEqual(
        await tx.select().from(generated).orderBy(generated.id),
        [
          { id: 1, owner: "a", event: "first", value: "updated" },
          { id: 3, owner: "a", event: null, value: "null updated" },
        ]
      );
    });
  } finally {
    try {
      if (schemaCreated)
        await client.unsafe(`drop schema ${schemaName} cascade`);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
});

test("rejects empty and undefined-only update sets with configuration diagnostics", async () => {
  await fixture(async (tx) => {
    for (const data of [
      { id: 1 },
      { id: 1, value: undefined },
      { id: 1, nonColumn: "ignored" },
    ]) {
      await assert.rejects(
        applyUpsert(tx, table, primary, primary, data, {
          omitSetProps: ["id"],
        }),
        /table=upsert_probe; no update values remain; omittedProperties=\(id\)/
      );
    }
    assert.deepEqual(await tx.select().from(table), []);
  });
});

test("rejects fallback uploads without identity even when Postgres supplies a default", async () => {
  await fixture(async (tx) => {
    await tx.execute(
      sql`alter table upsert_probe alter column id set default 42`
    );
    for (const id of [undefined, null]) {
      await assert.rejects(
        upsert(tx, { id, owner: "a", event: null, value: "original" }),
        /table=upsert_probe; missingPrimaryKey=\(id\)/
      );
    }
    assert.deepEqual(await tx.select().from(table), []);
  });
});

test("rejects a missing component of a composite fallback identity", async () => {
  await fixture(async (tx) => {
    const composite = [...primary, { col: table.owner, prop: "owner" }];
    await assert.rejects(
      applyUpsert(tx, table, natural, composite, {
        id: 1,
        event: null,
        value: "original",
      }),
      /missingPrimaryKey=\(owner\)/
    );
    assert.deepEqual(await tx.select().from(table), []);
  });
});

test("writes the trigger-normalized key checked by the fallback guard", async () => {
  await fixture(async (tx) => {
    await tx.execute(sql`create or replace function pg_temp.normalize_upsert_owner()
      returns trigger language plpgsql as $$ begin new.owner := lower(new.owner); return new; end $$`);
    await tx.execute(
      sql.raw(`create trigger normalize_owner before insert on upsert_probe
      for each row execute function pg_temp.normalize_upsert_owner()`)
    );
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await applyUpsert(
      tx,
      table,
      natural,
      primary,
      { id: 1, owner: "A", event: null, value: "updated" },
      { omitSetProps: ["id"] }
    );
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "updated" },
    ]);
  });
});

test("omitted defaults that evaluate to NULL can be uploaded repeatedly", async () => {
  await fixture(async (tx) => {
    await tx.execute(
      sql`alter table upsert_probe alter column event set default null::text`
    );
    const defaulted = pgTable("upsert_probe", {
      id: integer("id").primaryKey(),
      owner: text("owner").notNull(),
      event: text("event").default(sql.raw("null::text")),
      value: text("value").notNull(),
    });
    const keys = [
      { col: defaulted.owner, prop: "owner" },
      { col: defaulted.event, prop: "event" },
    ];
    const pk = [{ col: defaulted.id, prop: "id" }];
    await applyUpsert(
      tx,
      defaulted,
      keys,
      pk,
      { id: 1, owner: "a", value: "original" },
      options
    );
    await applyUpsert(
      tx,
      defaulted,
      keys,
      pk,
      { id: 1, owner: "a", value: "updated" },
      options
    );
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "updated" },
    ]);
  });
});

test("NULLS NOT DISTINCT deduplicates different IDs by the natural key", async () => {
  await fixture(async (tx) => {
    await tx.execute(
      sql`alter table upsert_probe drop constraint upsert_probe_owner_event_key`
    );
    await tx.execute(
      sql.raw(
        "alter table upsert_probe add unique nulls not distinct(owner,event)"
      )
    );
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await upsert(tx, { id: 2, owner: "a", event: null, value: "updated" });
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "updated" },
    ]);
  });
});

test("deferrable PK permits natural-key writes but rejects unsupported identity fallback explicitly", async () => {
  await fixture(async (tx) => {
    await tx.execute(
      sql`alter table upsert_probe drop constraint upsert_probe_pkey`
    );
    await tx.execute(
      sql`alter table upsert_probe add primary key(id) deferrable initially immediate`
    );
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await assert.rejects(
      tx.transaction((sp) =>
        upsert(sp, { id: 1, owner: "a", event: null, value: "retry" })
      ),
      /constraint=upsert_probe_pkey is DEFERRABLE/
    );
    await upsert(tx, { id: 2, owner: "a", event: "first", value: "original" });
    await upsert(tx, { id: 3, owner: "a", event: "first", value: "updated" });
    assert.deepEqual(await tx.select().from(table).orderBy(table.id), [
      { id: 1, owner: "a", event: null, value: "original" },
      { id: 2, owner: "a", event: "first", value: "updated" },
    ]);
  });
});

test("missing identity is rejected even with a complete natural key", async () => {
  await fixture(async (tx) => {
    await tx.execute(
      sql`alter table upsert_probe alter column id set default 42`
    );
    await assert.rejects(
      upsert(tx, { owner: "a", event: "first", value: "original" }),
      /missingPrimaryKey=\(id\)/
    );
    assert.deepEqual(await tx.select().from(table), []);
  });
});

test("unrelated unique constraints propagate without identity fallback", async () => {
  await fixture(async (tx) => {
    await tx.execute(sql`alter table upsert_probe add unique(value)`);
    await tx.execute(sql`create temporary sequence unrelated_attempts`);
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await tx.execute(sql`create or replace function pg_temp.count_unrelated_attempts()
      returns trigger language plpgsql as $$ begin perform nextval('pg_temp.unrelated_attempts'); return new; end $$`);
    await tx.execute(
      sql.raw(`create trigger count_attempts before insert on upsert_probe
      for each row execute function pg_temp.count_unrelated_attempts()`)
    );
    await assert.rejects(
      tx.transaction((sp) =>
        upsert(sp, { id: 2, owner: "a", event: null, value: "original" })
      ),
      (error) => error.cause?.constraint_name === "upsert_probe_value_key"
    );
    assert.equal(
      String(
        (
          await tx.execute(
            sql`select last_value from pg_temp.unrelated_attempts`
          )
        )[0].last_value
      ),
      "1"
    );
    await tx.execute(sql`drop sequence pg_temp.unrelated_attempts`);
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "original" },
    ]);
  });
});

test("primary-key violations inside insert triggers are not retried against the uploaded table", async () => {
  await fixture(async (tx) => {
    await tx.execute(
      sql`create temporary table other_probe (id integer primary key) on commit drop`
    );
    await tx.execute(sql`insert into other_probe values (1)`);
    await tx.execute(sql`create temporary sequence other_attempts`);
    await tx.execute(sql`create or replace function pg_temp.conflicting_upsert_write()
      returns trigger language plpgsql as $$ begin perform nextval('pg_temp.other_attempts'); insert into other_probe values (1); return new; end $$`);
    await tx.execute(
      sql.raw(`create trigger conflict_write before insert on upsert_probe
      for each row execute function pg_temp.conflicting_upsert_write()`)
    );
    await assert.rejects(
      tx.transaction((sp) =>
        upsert(sp, { id: 1, owner: "a", event: null, value: "original" })
      ),
      (error) =>
        error.cause?.table_name === "other_probe" &&
        error.cause?.code === "23505"
    );
    assert.equal(
      String(
        (
          await tx.execute(sql`select last_value from pg_temp.other_attempts`)
        )[0].last_value
      ),
      "1"
    );
    await tx.execute(sql`drop sequence pg_temp.other_attempts`);
    assert.deepEqual(await tx.select().from(table), []);
  });
});

test("fallback refuses an omitted key whose default differs from the stored NULL", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await tx.execute(
      sql`alter table upsert_probe alter column event set default 'private'`
    );
    await assert.rejects(
      upsert(tx, { id: 1, owner: "a", value: "retry" }),
      /Upsert returned no row/
    );
    assert.equal((await tx.select().from(table))[0].value, "original");
  });
});

test("fallback SQL failure rolls back without poisoning the caller transaction", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await tx.execute(sql`create or replace function pg_temp.invalid_upsert_update()
      returns trigger language plpgsql as $$ begin new.value := 'invalid'; return new; end $$`);
    await tx.execute(
      sql.raw(`create trigger invalid_update before update on upsert_probe
      for each row execute function pg_temp.invalid_upsert_update()`)
    );
    await assert.rejects(
      upsert(tx, { id: 1, owner: "a", event: null, value: "retry" }),
      (error) => error.cause?.code === "23514"
    );
    assert.equal((await tx.select().from(table))[0].value, "original");
  });
});

test("fallback rolls back an identity changed by the second insert trigger invocation", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await tx.execute(sql`create temporary sequence upsert_identity_sequence`);
    try {
      await tx.execute(sql`create or replace function pg_temp.change_upsert_identity()
        returns trigger language plpgsql as $$ begin new.id := nextval('pg_temp.upsert_identity_sequence'); return new; end $$`);
      await tx.execute(
        sql.raw(`create trigger change_identity before insert on upsert_probe
        for each row execute function pg_temp.change_upsert_identity()`)
      );
      await assert.rejects(
        upsert(tx, { id: 1, owner: "a", event: null, value: "retry" }),
        /Upsert identity changed during fallback/
      );
      assert.deepEqual(await tx.select().from(table), [
        { id: 1, owner: "a", event: null, value: "original" },
      ]);
    } finally {
      await tx.execute(sql`drop sequence pg_temp.upsert_identity_sequence`);
    }
  });
});

test("partition primary-key violations are verified against the parent constraint", async () => {
  await db.transaction(async (tx) => {
    await tx.execute(sql`create temporary table partition_probe
      (owner text, id integer, event text, value text, primary key(owner,id), unique(owner,event))
      partition by list(owner) on commit drop`);
    await tx.execute(
      sql`create temporary table partition_probe_a partition of partition_probe for values in ('a')`
    );
    const partition = pgTable("partition_probe", {
      owner: text("owner"),
      id: integer("id"),
      event: text("event"),
      value: text("value"),
    });
    const pk = [
      { col: partition.owner, prop: "owner" },
      { col: partition.id, prop: "id" },
    ];
    const keys = [
      { col: partition.owner, prop: "owner" },
      { col: partition.event, prop: "event" },
    ];
    await applyUpsert(
      tx,
      partition,
      keys,
      pk,
      { id: 1, owner: "a", event: null, value: "original" },
      options
    );
    await applyUpsert(
      tx,
      partition,
      keys,
      pk,
      { id: 1, owner: "a", event: null, value: "updated" },
      options
    );
    assert.deepEqual(await tx.select().from(partition), [
      { id: 1, owner: "a", event: null, value: "updated" },
    ]);
  });
});

test("initially deferred duplicate identities fail at outer commit and roll back the batch", async () => {
  // Keep the table across transactions so the rollback can be observed after commit fails.
  await client.unsafe(`create temporary table upsert_probe
    (id integer primary key deferrable initially deferred, owner text not null,
     event text, value text not null, unique(owner,event))`);
  try {
    await client.unsafe(
      "insert into upsert_probe values (1, 'a', null, 'original')"
    );
    await assert.rejects(
      db.transaction(async (tx) => {
        await upsert(tx, {
          id: 2,
          owner: "a",
          event: "other",
          value: "batch write",
        });
        await upsert(tx, { id: 1, owner: "a", event: null, value: "retry" });
      }),
      (error) => error.code === "23505"
    );
    assert.deepEqual(await db.select().from(table), [
      { id: 1, owner: "a", event: null, value: "original" },
    ]);
  } finally {
    await client.unsafe("drop table pg_temp.upsert_probe");
  }
});

test("fallback cannot redirect an update to a different existing identity", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original one" });
    await upsert(tx, { id: 2, owner: "a", event: null, value: "original two" });
    await tx.execute(sql`create temporary sequence upsert_redirect_sequence`);
    try {
      await tx.execute(sql`create or replace function pg_temp.redirect_upsert_identity()
        returns trigger language plpgsql as $$ begin new.id := nextval('pg_temp.upsert_redirect_sequence'); return new; end $$`);
      await tx.execute(
        sql.raw(`create trigger redirect_identity before insert on upsert_probe
        for each row execute function pg_temp.redirect_upsert_identity()`)
      );
      // A third submitted identity would otherwise overwrite row 2 and RETURNING
      // would appear to validate it after the UPDATE changed its ID to 3.
      await assert.rejects(
        applyUpsert(tx, table, natural, primary, {
          id: 3,
          owner: "a",
          event: null,
          value: "retry",
        }),
        /Upsert returned no row/
      );
      assert.deepEqual(await tx.select().from(table).orderBy(table.id), [
        { id: 1, owner: "a", event: null, value: "original one" },
        { id: 2, owner: "a", event: null, value: "original two" },
      ]);
    } finally {
      await tx.execute(sql`drop sequence pg_temp.upsert_redirect_sequence`);
    }
  });
});

test("a nested same-table trigger violation is not retried", async () => {
  await fixture(async (tx) => {
    await upsert(tx, { id: 1, owner: "a", event: null, value: "original" });
    await tx.execute(sql`create temporary sequence nested_attempts`);
    await tx.execute(sql`create or replace function pg_temp.nested_conflict()
      returns trigger language plpgsql as $$ begin
        if new.value = 'retry' then
          perform nextval('pg_temp.nested_attempts');
          insert into upsert_probe values (1, 'nested', null, 'nested');
        end if;
        return new;
      end $$`);
    await tx.execute(
      sql.raw(`create trigger nested_conflict before insert on upsert_probe
      for each row execute function pg_temp.nested_conflict()`)
    );
    await assert.rejects(
      upsert(tx, { id: 2, owner: "a", event: null, value: "retry" }),
      (error) => error.cause?.code === "23505" && Boolean(error.cause?.where)
    );
    assert.equal(
      String(
        (
          await tx.execute(sql`select last_value from pg_temp.nested_attempts`)
        )[0].last_value
      ),
      "1"
    );
    assert.deepEqual(await tx.select().from(table), [
      { id: 1, owner: "a", event: null, value: "original" },
    ]);
    await tx.execute(sql`drop sequence pg_temp.nested_attempts`);
  });
});

test("reports the unsupported excluded table name before executing SQL", async () => {
  const reserved = pgTable("excluded", { id: integer("id").primaryKey() });
  await assert.rejects(
    applyUpsert(
      null,
      reserved,
      [{ col: reserved.id, prop: "id" }],
      [{ col: reserved.id, prop: "id" }],
      { id: 1 }
    ),
    /name conflicts with PostgreSQL upsert references/
  );
});

for (const [before, afterValue] of [
  [null, "filled"],
  ["first", "second"],
  ["first", null],
]) {
  test(`natural-key policy rejects ${before} -> ${afterValue} for the same identity`, async () => {
    await fixture(async (tx) => {
      const original = { id: 1, owner: "a", event: before, value: "original" };
      await upsert(tx, original);
      await assert.rejects(
        upsert(tx, { ...original, event: afterValue, value: "changed" }),
        /Natural-key changes for an existing identity are not supported/
      );
      assert.deepEqual(await tx.select().from(table), [original]);
    });
  });
}

test("preserves the write and cleanup errors without retrying an unusable transaction", async () => {
  const original = Object.assign(new Error("duplicate identity"), {
    code: "23505",
    constraint_name: "upsert_probe_pkey",
  });
  const cleanup = new Error("connection closed during rollback");
  let commands = 0;
  let attempts = 0;
  const tx = {
    execute: async () => {
      if (++commands > 1) throw cleanup;
    },
    transaction: async () => {
      attempts++;
      throw original;
    },
  };
  await assert.rejects(
    upsert(tx, { id: 1, owner: "a", event: null, value: "retry" }),
    (error) => {
      assert.ok(error instanceof AggregateError);
      assert.deepEqual(error.errors, [original, cleanup]);
      return true;
    }
  );
  assert.equal(attempts, 1);
  assert.equal(commands, 2);
});

test("schema-less inserts and catalog verification share the transaction search path", async () => {
  await fixture(async (tx) => {
    // There is no permanent upsert_probe: pg_temp is intentionally resolved
    // ahead of a non-default search path, for both INSERT and to_regclass.
    await tx.execute(sql`set local search_path = pg_catalog`);
    await upsert(tx, { id: 1, owner: "a", event: null, value: "first" });
    await upsert(tx, { id: 1, owner: "a", event: null, value: "second" });
    assert.equal((await tx.select().from(table))[0].value, "second");
  });
});

test("rejects a changed natural key that would update a different existing identity", async () => {
  await fixture(async (tx) => {
    const originals = [
      { id: 1, owner: "a", event: "first", value: "original" },
      { id: 2, owner: "a", event: "second", value: "other" },
    ];
    await tx.insert(table).values(originals);
    await assert.rejects(
      upsert(tx, { ...originals[0], event: "second", value: "changed" }),
      /Natural-key changes for an existing identity are not supported/
    );
    assert.deepEqual(
      await tx.select().from(table).orderBy(table.id),
      originals
    );
  });
});

for (const event of [null, "first"]) {
  test(`rejects UPDATE-trigger natural-key changes for ${event}`, async () => {
    await fixture(async (tx) => {
      const original = { id: 1, owner: "a", event, value: "original" };
      await tx.insert(table).values(original);
      await tx.execute(sql`create or replace function pg_temp.change_event()
        returns trigger language plpgsql as $$ begin
          new.event := 'changed'; return new;
        end $$`);
      await tx.execute(
        sql.raw(`create trigger change_event before update on upsert_probe
        for each row execute function pg_temp.change_event()`)
      );
      await assert.rejects(
        upsert(tx, { ...original, value: "updated" }),
        /Natural-key changes for an existing identity are not supported/
      );
      assert.deepEqual(await tx.select().from(table), [original]);
    });
  });
}
