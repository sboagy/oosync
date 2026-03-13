import type { SyncChange } from "@oosync/shared/protocol";
import { and, eq } from "drizzle-orm";
import type { AnySQLiteColumn, AnySQLiteTable } from "drizzle-orm/sqlite-core";
import { getAdapter, type SyncableTableName } from "./adapters";
import { toCamelCase } from "./casing";
import { getSyncRuntime, type SqliteDatabase } from "./runtime-context";

type DynamicSQLiteTable = AnySQLiteTable &
  Record<string, AnySQLiteColumn | undefined>;

function getTableColumn(
  table: DynamicSQLiteTable,
  columnKey: string,
  tableName: string
): AnySQLiteColumn | null {
  const column = table[columnKey];
  if (!column) {
    console.warn(
      `[SyncEngine] Column not found in local schema: ${tableName}.${columnKey}`
    );
    return null;
  }
  return column;
}

const isForeignKeyConstraintFailure = (e: unknown): boolean => {
  const msg = e instanceof Error ? e.message : String(e);
  return /FOREIGN KEY constraint failed/i.test(msg);
};

const sanitizeData = (data: Record<string, unknown>) => {
  const sanitized = { ...data };
  // Convert boolean to integer for SQLite
  for (const key in sanitized) {
    if (typeof sanitized[key] === "boolean") {
      sanitized[key] = sanitized[key] ? 1 : 0;
    }
  }
  return sanitized;
};

export async function applyRemoteChangesToLocalDb(params: {
  localDb: SqliteDatabase;
  changes: SyncChange[];
  deferForeignKeyFailuresTo?: SyncChange[];
  onTriggersRestored?: (triggersSuppressedAt: string) => Promise<void>;
}): Promise<{
  synced: number;
  failed: number;
  errors: string[];
  affectedTables: Set<string>;
}> {
  const { changes, deferForeignKeyFailuresTo, onTriggersRestored } = params;
  const result = {
    synced: 0,
    failed: 0,
    errors: [] as string[],
    affectedTables: new Set<string>(),
  };

  if (changes.length === 0) return result;

  const runtime = getSyncRuntime();
  const {
    schema,
    localSchema,
    getSqliteInstance,
    suppressSyncTriggers,
    enableSyncTriggers,
  } = runtime;
  const { tableSyncOrder, tableToSchemaKey } = schema;
  const logger = runtime.logger;

  const sqliteInstance = await getSqliteInstance();
  if (!sqliteInstance) return result;

  const triggersSuppressedAt = new Date().toISOString();
  suppressSyncTriggers(sqliteInstance);

  try {
    // Sort changes by dependency to avoid FK violations.
    // Inserts/updates: parent tables first. Deletes: child tables first.
    const sortedChanges = [...changes].sort((a, b) => {
      const orderA = tableSyncOrder[a.table] ?? 100;
      const orderB = tableSyncOrder[b.table] ?? 100;

      if (a.deleted && b.deleted) return orderB - orderA;
      if (!a.deleted && !b.deleted) return orderA - orderB;
      if (a.deleted) return 1;
      if (b.deleted) return -1;
      return orderA - orderB;
    });

    for (const change of sortedChanges) {
      result.affectedTables.add(change.table);
      const adapter = getAdapter(change.table as SyncableTableName);

      const schemaKey = tableToSchemaKey[change.table] || change.table;
      const table = (
        localSchema as Record<string, DynamicSQLiteTable | undefined>
      )[schemaKey];

      if (!table) {
        logger.warn(
          `[SyncEngine] Table not found in local schema: ${change.table} (key: ${schemaKey})`
        );
        continue;
      }

      try {
        if (change.deleted) {
          // Delete local
          const pk = adapter.primaryKey;
          const localKeyData = adapter.toLocal(change.data);

          if (Array.isArray(pk)) {
            const conditions = pk
              .map((k) => {
                const columnKey = toCamelCase(k);
                const value = localKeyData[columnKey];
                if (typeof value === "undefined") {
                  logger.warn(
                    `[SyncEngine] Missing PK value for ${change.table}.${k} during delete`,
                    { rowId: change.rowId }
                  );
                  return null;
                }
                const column = getTableColumn(table, columnKey, change.table);
                return column ? eq(column, value) : null;
              })
              .filter((c): c is ReturnType<typeof eq> => c !== null);

            if (conditions.length !== pk.length) {
              continue;
            }
            await params.localDb
              .delete(table)
              .where(and(...conditions))
              .run();
          } else if (typeof pk === "string") {
            const columnKey = toCamelCase(pk);
            const value = localKeyData[columnKey];
            if (typeof value === "undefined") {
              logger.warn(
                `[SyncEngine] Missing PK value for ${change.table}.${pk} during delete`,
                { rowId: change.rowId }
              );
              continue;
            }
            const column = getTableColumn(table, columnKey, change.table);
            if (!column) {
              continue;
            }
            await params.localDb.delete(table).where(eq(column, value)).run();
          }
        } else {
          // Upsert local
          const sanitizedData = sanitizeData(change.data);

          const adapterPk = adapter.primaryKey;
          const conflictTarget = Array.isArray(adapterPk)
            ? adapterPk
                .map((k) => getTableColumn(table, toCamelCase(k), change.table))
                .filter((column): column is AnySQLiteColumn => column !== null)
            : getTableColumn(
                table,
                toCamelCase(adapterPk as string),
                change.table
              );

          if (
            (Array.isArray(conflictTarget) && conflictTarget.length === 0) ||
            (!Array.isArray(conflictTarget) && conflictTarget === null)
          ) {
            result.failed += 1;
            result.errors.push(
              `${change.table}:${change.rowId}: missing conflict target columns`
            );
            continue;
          }

          // Some tables have a natural composite unique key (adapter.conflictKeys)
          // in addition to a synthetic PK (id). If we only upsert by id, an insert
          // can fail on the composite unique constraint when the same logical row
          // exists locally with a different id.
          const compositeKeys = adapter.conflictKeys;
          const isSingleIdPk =
            !Array.isArray(adapter.primaryKey) && adapter.primaryKey === "id";

          if (isSingleIdPk && compositeKeys) {
            try {
              await params.localDb
                .insert(table)
                .values(sanitizedData)
                .onConflictDoUpdate({
                  target: conflictTarget,
                  set: sanitizedData,
                })
                .run();
            } catch (e) {
              const errorMsg = e instanceof Error ? e.message : String(e);
              const isCompositeUniqueViolation =
                errorMsg.includes("UNIQUE constraint failed:") &&
                compositeKeys.every(
                  (k) =>
                    errorMsg.includes(`${change.table}.${k}`) ||
                    errorMsg.includes(k)
                );

              if (!isCompositeUniqueViolation) {
                throw e;
              }

              const compositeTarget = compositeKeys
                .map((k) => getTableColumn(table, toCamelCase(k), change.table))
                .filter((column): column is AnySQLiteColumn => column !== null);

              if (compositeTarget.length !== compositeKeys.length) {
                throw new Error(
                  `Missing composite conflict target columns for ${change.table}`
                );
              }

              await params.localDb
                .insert(table)
                .values(sanitizedData)
                .onConflictDoUpdate({
                  target: compositeTarget,
                  set:
                    compositeKeys.length === 1 && compositeKeys[0] === "id"
                      ? sanitizedData
                      : ((): Record<string, unknown> => {
                          const { id: _ignoredId, ...rest } =
                            sanitizedData as Record<string, unknown>;
                          return rest;
                        })(),
                })
                .run();
            }
          } else {
            await params.localDb
              .insert(table)
              .values(sanitizedData)
              .onConflictDoUpdate({
                target: conflictTarget,
                set: sanitizedData,
              })
              .run();
          }
        }
        result.synced += 1;
      } catch (e) {
        if (isForeignKeyConstraintFailure(e)) {
          if (deferForeignKeyFailuresTo) {
            deferForeignKeyFailuresTo.push(change);
          }
          continue;
        }

        result.failed += 1;
        const errorMsg = e instanceof Error ? e.message : "Unknown error";
        result.errors.push(`${change.table}:${change.rowId}: ${errorMsg}`);
        logger.error(
          `[SyncEngine] Failed to apply change to ${change.table} rowId=${change.rowId}:`,
          e
        );
      }
    }
  } finally {
    enableSyncTriggers(sqliteInstance);

    if (onTriggersRestored) {
      try {
        await onTriggersRestored(triggersSuppressedAt);
      } catch (e) {
        logger.warn(
          "[SyncEngine] Outbox backfill after trigger suppression failed",
          e
        );
      }
    }
  }

  return result;
}
