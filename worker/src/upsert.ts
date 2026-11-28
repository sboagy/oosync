/**
 * Applies sync uploads by the configured natural key, retrying by identity only
 * after PostgreSQL reports a verified primary-key violation. This makes nullable
 * natural-key uploads repeatable without guessing how defaults or UNIQUE NULLS
 * NOT DISTINCT behave. A savepoint rolls back the failed first attempt.
 * Natural-key components are immutable through sync, including NULL-to-value edits.
 *
 * The fallback atomically checks every natural-key component against `excluded`
 * (including defaults and BEFORE INSERT effects) before updating. Updated key
 * properties use those same normalized values. All uploads require explicit
 * primary keys, retain configured insert-only fields, and reject skipped writes.
 * Deferrable primary keys cannot arbitrate the fallback. Deferred violations
 * remain the outer transaction owner's responsibility at commit.
 * Caller-side authorization and transaction handling remain required.
 */
import { and, eq, getTableColumns, getTableName, sql } from "drizzle-orm";
import { type AnyPgColumn, getTableConfig } from "drizzle-orm/pg-core";
import { findPostgresErrorLike } from "./db-errors";
import type {
  DynamicPgTable,
  WorkerTransaction as Transaction,
} from "./sync-schema";

type KeyColumn = { col: AnyPgColumn; prop: string };

export async function applyUpsert(
  tx: Transaction,
  table: DynamicPgTable,
  conflictKeyCols: KeyColumn[],
  primaryKeyCols: KeyColumn[],
  data: Record<string, unknown>,
  opts?: {
    /**
     * Properties to omit from the UPDATE set (still inserted on first write).
     * This is critical when upserting on a non-PK unique key.
     */
    omitSetProps?: readonly string[];
  }
): Promise<void> {
  if (getTableName(table) === "excluded") {
    throw new Error(
      'Cannot upsert table "excluded": its name conflicts with PostgreSQL upsert references. ' +
        "This Worker upsert path requires a different table name."
    );
  }
  const missingKeys = primaryKeyCols.filter(
    ({ prop }) => data[prop] === undefined || data[prop] === null
  );
  if (primaryKeyCols.length === 0 || missingKeys.length > 0) {
    throw new Error(
      `Cannot upsert: table=${getTableName(table)}; ` +
        `missingPrimaryKey=(${missingKeys.map(({ col }) => col.name).join(",")}). ` +
        "Uploads must retain every primary-key value, including minimal-payload retries."
    );
  }
  const omit = opts?.omitSetProps ?? [];
  const columns = getTableColumns(table);
  const setData: Record<string, unknown> = Object.fromEntries(
    Object.entries(data).filter(
      ([prop, value]) =>
        Object.hasOwn(columns, prop) &&
        value !== undefined &&
        !omit.includes(prop)
    )
  );

  if (Object.keys(setData).length === 0) {
    throw new Error(
      `Cannot upsert: table=${getTableName(table)}; no update values remain; ` +
        `omittedProperties=(${omit.join(",")}). ` +
        "Check the upload payload and upsert omission/retry configuration."
    );
  }

  // Use the same trigger-normalized natural-key values that the guard checks.
  // Preserve configured insert-only properties and omitted/undefined fields.
  for (const { col, prop } of conflictKeyCols) {
    if (setData[prop] !== undefined) {
      setData[prop] = sql.join([
        sql.raw("excluded."),
        sql.identifier(col.name),
      ]);
    }
  }

  const write = { table, conflictKeyCols, primaryKeyCols, data, setData };

  // Let PostgreSQL evaluate defaults, triggers and NULLS NOT DISTINCT. A failed
  // attempt must roll back before either inspecting catalogs or retrying.
  try {
    await withReleasedSavepoint(tx, (sp) => writeUpsert(sp, write, false));
  } catch (error) {
    await requirePrimaryKeyConflict(tx, table, primaryKeyCols, error);
    await withReleasedSavepoint(tx, (sp) => writeUpsert(sp, write, true));
  }
}

// Postgres.js retains nested savepoints. Releasing this enclosing boundary also
// releases the driver's savepoint, after its error handling has completed.
// Keep the driver scope: it tracks failed queries even when callers catch them.
async function withReleasedSavepoint(
  tx: Transaction,
  run: (sp: Transaction) => Promise<void>
): Promise<void> {
  const boundary = sql.identifier(
    `oosync_${globalThis.crypto.randomUUID().replaceAll("-", "")}`
  );
  await tx.execute(sql.join([sql.raw("savepoint "), boundary]));
  try {
    await tx.transaction(run);
  } catch (error) {
    try {
      // The driver normally rolled back its inner savepoint already. Restore
      // our boundary too, then release it without hiding the original failure.
      await tx.execute(sql.join([sql.raw("rollback to savepoint "), boundary]));
      await tx.execute(sql.join([sql.raw("release savepoint "), boundary]));
    } catch (cleanupError) {
      // Do not expose a retryable cause: cleanup failure means the transaction
      // is not known to be usable. Retain both errors for diagnostics.
      throw new AggregateError(
        [error, cleanupError],
        "Upsert failed and savepoint cleanup also failed; transaction must be aborted."
      );
    }
    throw error;
  }
  await tx.execute(sql.join([sql.raw("release savepoint "), boundary]));
}

async function writeUpsert(
  transaction: Transaction,
  {
    table,
    conflictKeyCols,
    primaryKeyCols,
    data,
    setData,
  }: {
    table: DynamicPgTable;
    conflictKeyCols: KeyColumn[];
    primaryKeyCols: KeyColumn[];
    data: Record<string, unknown>;
    setData: Record<string, unknown>;
  },
  usePrimaryKey: boolean
): Promise<void> {
  // Lock a known upload identity before either conflict path. Otherwise a
  // changed natural key could update a different row without ever hitting the
  // primary-key fallback. Keep the snapshot for detecting UPDATE-trigger edits.
  const [existing] = await transaction
    .select()
    .from(table)
    .where(and(...primaryKeyCols.map(({ col, prop }) => eq(col, data[prop]))))
    .for("update");
  const unchangedStoredKey = existing
    ? and(
        ...conflictKeyCols.map(({ col, prop }) =>
          sql.join([
            col,
            sql.raw(" is not distinct from "),
            sql.param(existing[prop], col),
          ])
        )
      )
    : sql.raw("true");
  const targetCols = (usePrimaryKey ? primaryKeyCols : conflictKeyCols).map(
    ({ col }) => col
  );
  const matchingNaturalKey = usePrimaryKey
    ? and(
        ...primaryKeyCols.map(({ col, prop }) => eq(col, data[prop])),
        ...conflictKeyCols.map(({ col }) =>
          sql.join([
            col,
            sql.raw(" is not distinct from excluded."),
            sql.identifier(col.name),
          ])
        )
      )
    : undefined;
  const rows = await transaction
    .insert(table)
    .values(data)
    .onConflictDoUpdate({
      target: targetCols,
      set: setData,
      setWhere: matchingNaturalKey,
    })
    .returning({
      storedKeyMatches: unchangedStoredKey ?? sql.raw("true"),
      identityMatches:
        usePrimaryKey || existing !== undefined
          ? (and(
              ...primaryKeyCols.map(({ col, prop }) => eq(col, data[prop]))
            ) ?? sql.raw("false"))
          : sql.raw("true"),
    });
  if (rows.length === 0) {
    const guardColumns = usePrimaryKey
      ? conflictKeyCols.map(({ col }) => col.name).join(",")
      : "none";
    const explanation = usePrimaryKey
      ? "Natural-key changes for an existing identity are not supported. " +
        "Check for a changed natural key or identity and BEFORE INSERT/UPDATE triggers; " +
        "the empty result does not identify which skipped the write."
      : "Check BEFORE INSERT/UPDATE triggers for a skipped write.";
    throw new Error(
      `Upsert returned no row: table=${getTableName(table)}; ` +
        `conflictTarget=(${targetCols.map((col) => col.name).join(",")}); ` +
        `primaryKeyFallback=${usePrimaryKey}; naturalKeyGuard=(${guardColumns}). ` +
        explanation
    );
  }
  if (usePrimaryKey && rows[0].identityMatches !== true) {
    throw new Error(
      `Upsert identity changed during fallback: table=${getTableName(table)}; ` +
        `primaryKey=(${primaryKeyCols.map(({ col }) => col.name).join(",")}). ` +
        "Check BEFORE INSERT/UPDATE triggers; the fallback write was rolled back."
    );
  }
  if (
    existing &&
    (rows[0].identityMatches !== true || rows[0].storedKeyMatches !== true)
  ) {
    throw new Error(
      `Natural-key changes for an existing identity are not supported: table=${getTableName(table)}. ` +
        "The returned identity or natural key changed; the write was rolled back."
    );
  }
}

async function requirePrimaryKeyConflict(
  tx: Transaction,
  table: DynamicPgTable,
  primaryKeyCols: KeyColumn[],
  error: unknown
): Promise<void> {
  const pgError = findPostgresErrorLike(error);
  if (
    pgError?.code !== "23505" ||
    typeof pgError.constraint_name !== "string" ||
    // PostgreSQL supplies execution context for nested PL/pgSQL statements.
    // A trigger's own write is not a direct conflict of the uploaded row.
    (typeof pgError.where === "string" && pgError.where.length > 0)
  )
    throw error;
  const { schema, name } = getTableConfig(table);
  const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;
  // Match the INSERT: schema-less Drizzle tables also resolve via search_path.
  const relation = schema ? `${quote(schema)}.${quote(name)}` : quote(name);
  // Verify the actual violated constraint, including its relation and columns;
  // unrelated unique violations (including ones inside triggers) must propagate.
  const constraints = await tx.execute<{
    deferred: boolean;
    schema: string;
    columns: string[];
  }>(sql`
    with recursive violated as (
      select c.* from pg_constraint c
      join pg_class t on t.oid = c.conrelid
      join pg_namespace n on n.oid = t.relnamespace
      where c.contype = 'p' and c.conname = ${pgError.constraint_name}
        and t.relname = ${pgError.table_name} and n.nspname = ${pgError.schema_name}
      union all
      select parent.* from pg_constraint parent
      join violated child on parent.oid = child.conparentid
    )
    select c.condeferrable as deferred,
      (select n.nspname from pg_class t join pg_namespace n on n.oid = t.relnamespace
        where t.oid = c.conrelid) as schema,
      array(select a.attname::text from unnest(c.conkey) with ordinality as k(num, pos)
        join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.num
        order by k.pos) as columns
    from violated c
    where c.conrelid = to_regclass(${relation}) and c.contype = 'p'
  `);
  const constraint: unknown = Array.isArray(constraints)
    ? constraints[0]
    : undefined;
  const expected = primaryKeyCols.map(({ col }) => col.name);
  if (
    !constraint ||
    typeof constraint !== "object" ||
    !("columns" in constraint) ||
    !Array.isArray(constraint.columns) ||
    !("schema" in constraint) ||
    typeof constraint.schema !== "string" ||
    !("deferred" in constraint) ||
    typeof constraint.deferred !== "boolean" ||
    constraint.columns.length !== expected.length ||
    !constraint.columns.every(
      (column: unknown) =>
        typeof column === "string" && expected.includes(column)
    )
  )
    throw error;
  if (constraint.deferred) {
    throw new Error(
      `Cannot retry upsert by primary key: table=${quote(constraint.schema)}.${quote(name)}; constraint=${pgError.constraint_name} is DEFERRABLE. ` +
        "PostgreSQL requires a non-deferrable unique arbiter for ON CONFLICT; this identity fallback is unsupported."
    );
  }
}
