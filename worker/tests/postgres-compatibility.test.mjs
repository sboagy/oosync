import assert from "node:assert/strict";
import { after, test } from "node:test";
import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";

// Explicit integration suite: only temporary tables on local Postgres, never
// consumer tables. Keep the same pooling/prepare options as the sync worker.
const url = new URL(
  process.env.OOSYNC_TEST_DATABASE_URL ??
    "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
);
assert.ok(
  ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname),
  "Postgres compatibility tests require a local database"
);
const client = postgres(url.toString(), {
  max: 1,
  prepare: false,
  connect_timeout: 5,
});
const db = drizzle(client);
after(() => client.end({ timeout: 5 }));

test("failed upsert savepoint permits a minimal-payload retry and commit", async () => {
  await db.transaction(async (tx) => {
    await tx.execute(
      sql`create temporary table driver_retry (id integer primary key, value text) on commit drop`
    );
    await tx.execute(sql`insert into driver_retry values (1, 'original')`);
    await assert.rejects(
      tx.transaction(async (sp) => {
        await sp.execute(sql`insert into driver_retry values (1, 'duplicate')`);
      }),
      (error) => error.cause?.code === "23505"
    );
    await tx.transaction(async (sp) => {
      await sp.execute(
        sql`update driver_retry set value = 'retry' where id = 1`
      );
    });
    const rows = await tx.execute(sql`select value from driver_retry`);
    assert.equal(rows[0].value, "retry");
  });
});

test("RPC parameters preserve UUID arrays and timestamp instants", async () => {
  const ids = ["00000000-0000-4000-8000-000000000001"];
  const instant = "2026-11-05T12:00:00.000Z";
  const rows = await client.unsafe(
    "select $1::uuid[] as ids, $2::timestamptz as since",
    [ids, instant]
  );
  assert.deepEqual(rows[0].ids, ids);
  assert.equal(new Date(rows[0].since).toISOString(), instant);
});

test("Drizzle timestamp and JSON serializers round-trip outside UTC", async () => {
  await db.transaction(async (tx) => {
    await tx.execute(sql`set local timezone = 'America/New_York'`);
    await tx.execute(
      sql`create temporary table driver_values (happened_at timestamptz, payload jsonb) on commit drop`
    );
    const instant = "2026-11-05T12:00:00.000Z";
    await tx.execute(
      sql`insert into driver_values values (${instant}::timestamptz, ${JSON.stringify({ ok: true })}::jsonb)`
    );
    const rows = await tx.execute(sql`select * from driver_values`);
    assert.equal(new Date(rows[0].happened_at).toISOString(), instant);
    assert.deepEqual(rows[0].payload, { ok: true });
  });
});

test("transaction failure rolls back writes and leaves the connection usable", async () => {
  await client`create temporary table driver_rollback (id integer)`;
  try {
    await assert.rejects(
      db.transaction(async (tx) => {
        await tx.execute(sql`insert into driver_rollback values (1)`);
        await tx.execute(sql`select 1 / 0`);
      }),
      (error) => error.cause?.code === "22012"
    );
    const rows =
      await client`select count(*)::integer as count from driver_rollback`;
    assert.equal(rows[0].count, 0);
  } finally {
    await client`drop table driver_rollback`;
  }
});
