import assert from "node:assert/strict";
import { test } from "node:test";
import { integer, pgTable, text } from "drizzle-orm/pg-core";
import { createWorker } from "../src/index.ts";
import { pullRuleProtectedProps } from "../src/sync-schema.ts";

const table = pgTable("policy_probe", {
  id: integer("id").primaryKey(),
  ownerRef: text("owner_ref"),
  eventName: text("event_name"),
});
function artifacts(upsert, pullRule, bindAuthUserIdProps = []) {
  return {
    schemaTables: { policy_probe: table },
    syncableTables: ["policy_probe"],
    tableRegistryCore: {
      policy_probe: {
        primaryKey: "id",
        uniqueKeys: ["owner_ref", "event_name"],
        timestamps: [],
        booleanColumns: [],
        supportsIncremental: false,
        hasDeletedFlag: false,
      },
    },
    workerSyncConfig: {
      push: { tableRules: { policy_probe: { upsert, bindAuthUserIdProps } } },
      pull: { tableRules: { policy_probe: pullRule } },
    },
  };
}

for (const kind of [
  "eqUserId",
  "orNullEqUserId",
  "inCollection",
  "publicOnly",
]) {
  test(`startup protects ${kind} pull columns`, () => {
    assert.throws(
      () =>
        createWorker(
          artifacts(
            { mutableNaturalKeyProps: ["ownerRef"] },
            { kind, column: "owner_ref", collection: "owners" }
          )
        ),
      /Invalid mutable natural-key property.*property=ownerRef/
    );
  });
}
test("startup protects both columns inside nested compound pull rules", () => {
  const rule = {
    kind: "compound",
    rules: [
      {
        kind: "compound",
        rules: [
          {
            kind: "orEqUserIdOrTrue",
            column: "owner_ref",
            orColumn: "event_name",
          },
        ],
      },
    ],
  };
  assert.deepEqual(pullRuleProtectedProps(rule), ["ownerRef", "eventName"]);
  assert.throws(
    () =>
      createWorker(artifacts({ mutableNaturalKeyProps: ["eventName"] }, rule)),
    /Invalid mutable natural-key property.*property=eventName/
  );
});
for (const [name, options, bindings] of [
  ["primary key", { mutableNaturalKeyProps: ["id"] }, []],
  ["unknown property", { mutableNaturalKeyProps: ["missing"] }, []],
  [
    "insert-only property",
    { mutableNaturalKeyProps: ["eventName"], omitSetProps: ["eventName"] },
    [],
  ],
  ["auth binding", { mutableNaturalKeyProps: ["ownerRef"] }, ["ownerRef"]],
]) {
  test(`startup rejects mutable ${name}`, () => {
    assert.throws(
      () => createWorker(artifacts(options, undefined, bindings)),
      /Invalid mutable natural-key property/
    );
  });
}
test("startup accepts editable keys outside access scope using the runtime table registry", () => {
  const worker = createWorker(
    artifacts(
      { mutableNaturalKeyProps: ["eventName"], omitSetProps: ["id"] },
      { kind: "eqUserId", column: "owner_ref" }
    )
  );
  assert.equal(typeof worker.fetch, "function");
});
test("RPC rules do not invent protected columns; consumers supply explicit protection", () => {
  assert.deepEqual(
    pullRuleProtectedProps({
      kind: "rpc",
      functionName: "visible_rows",
      paramMap: {},
    }),
    []
  );
  assert.deepEqual(pullRuleProtectedProps(undefined), []);
});
test("existing configurations without editable keys remain valid", () => {
  assert.equal(
    typeof createWorker(artifacts(undefined, undefined)).fetch,
    "function"
  );
});
