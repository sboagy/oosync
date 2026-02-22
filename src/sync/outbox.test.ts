import { beforeEach, describe, expect, it } from "vitest";
import {
	clearOldOutboxItems,
	fetchLocalRowByPrimaryKey,
	getDrizzleTable,
	getOutboxStats,
	getPendingOutboxItems,
	markOutboxCompleted,
	markOutboxFailed,
	markOutboxInProgress,
	markOutboxPermanentlyFailed,
	type OutboxItem,
	parseRowId,
} from "./outbox";
import {
	type SqliteDatabase,
	type SyncPushQueueTable,
	type SyncRuntime,
	setSyncRuntime,
} from "./runtime-context";

function configureTestRuntime(): void {
	const queueColumns = {
		id: { name: "id" },
		tableName: { name: "table_name" },
		rowId: { name: "row_id" },
		operation: { name: "operation" },
		status: { name: "status" },
		changedAt: { name: "changed_at" },
		syncedAt: { name: "synced_at" },
		attempts: { name: "attempts" },
		lastError: { name: "last_error" },
	} as unknown as SyncPushQueueTable;

	const runtime = {
		schema: {
			syncableTables: ["entity_table"],
			tableRegistry: {} as Record<string, unknown>,
			tableSyncOrder: {},
			tableToSchemaKey: { entity_table: "entityTable" },
		},
		syncPushQueue: queueColumns,
		localSchema: { entityTable: { name: "entity_table" } },
		getSqliteInstance: async () => null,
		loadOutboxBackupForUser: async () => null,
		clearOutboxBackupForUser: async () => {},
		replayOutboxBackup: () => ({ applied: 0, skipped: 0, errors: [] }),
		enableSyncTriggers: () => {},
		suppressSyncTriggers: () => {},
		logger: {
			debug: () => {},
			info: () => {},
			warn: () => {},
			error: () => {},
		},
	} as unknown as SyncRuntime;

	setSyncRuntime(runtime);
}

beforeEach(() => {
	configureTestRuntime();
});

function createPendingItemsDb(items: OutboxItem[]): SqliteDatabase {
	return {
		select: () => ({
			from: () => ({
				where: () => ({
					orderBy: () => ({
						limit: async (limit: number) => items.slice(0, limit),
					}),
				}),
			}),
		}),
	} as unknown as SqliteDatabase;
}

function createUpdateDb(onSet: (values: unknown) => void): SqliteDatabase {
	return {
		update: () => ({
			set: (values: unknown) => {
				onSet(values);
				return {
					where: async () => {},
				};
			},
		}),
	} as unknown as SqliteDatabase;
}

function createDeleteDb(onDelete: () => void): SqliteDatabase {
	return {
		delete: () => ({
			where: async () => {
				onDelete();
			},
		}),
	} as unknown as SqliteDatabase;
}

describe("outbox utilities", () => {
	describe("getPendingOutboxItems", () => {
		it("returns rows from pending query chain", async () => {
			const rows: OutboxItem[] = [
				{
					id: "item-1",
					tableName: "entity_table",
					rowId: "entity-1",
					operation: "INSERT",
					status: "pending",
					changedAt: "2024-01-01T00:00:00.000Z",
					syncedAt: null,
					attempts: 0,
					lastError: null,
				},
				{
					id: "item-2",
					tableName: "entity_table",
					rowId: "entity-2",
					operation: "UPDATE",
					status: "pending",
					changedAt: "2024-01-02T00:00:00.000Z",
					syncedAt: null,
					attempts: 1,
					lastError: "retry",
				},
			];

			const db = createPendingItemsDb(rows);
			const result = await getPendingOutboxItems(db, 1);

			expect(result).toHaveLength(1);
			expect(result[0].id).toBe("item-1");
		});
	});

	describe("markOutboxInProgress", () => {
		it("sets status to in_progress", async () => {
			let recorded: unknown;
			const db = createUpdateDb((values) => {
				recorded = values;
			});

			await markOutboxInProgress(db, "item-1");

			expect(recorded).toEqual({ status: "in_progress" });
		});
	});

	describe("markOutboxFailed", () => {
		it("sets status pending and increments attempts", async () => {
			let recorded: unknown;
			const db = createUpdateDb((values) => {
				recorded = values;
			});

			await markOutboxFailed(db, "item-2", "network", 2);

			expect(recorded).toEqual({
				status: "pending",
				attempts: 3,
				lastError: "network",
			});
		});
	});

	describe("markOutboxPermanentlyFailed", () => {
		it("sets failed status and syncedAt", async () => {
			let recorded: unknown;
			const db = createUpdateDb((values) => {
				recorded = values;
			});

			await markOutboxPermanentlyFailed(db, "item-3", "fatal");

			expect(recorded).toMatchObject({
				status: "failed",
				lastError: "fatal",
			});
			expect(recorded).toEqual(
				expect.objectContaining({ syncedAt: expect.any(String) }),
			);
		});
	});

	describe("markOutboxCompleted", () => {
		it("deletes the queue item", async () => {
			let deleteCalls = 0;
			const db = createDeleteDb(() => {
				deleteCalls += 1;
			});

			await markOutboxCompleted(db, "item-4");

			expect(deleteCalls).toBe(1);
		});
	});

	describe("getOutboxStats", () => {
		it("normalizes aggregate row values to numbers", async () => {
			const db = {
				all: async () => [
					{
						pending: "2",
						in_progress: 1,
						failed: "3",
						total: 6,
					},
				],
			} as unknown as SqliteDatabase;

			const stats = await getOutboxStats(db);

			expect(stats).toEqual({
				pending: 2,
				inProgress: 1,
				failed: 3,
				total: 6,
			});
		});
	});

	describe("clearOldOutboxItems", () => {
		it("deletes only rows older than cutoff", async () => {
			const oldIso = "2000-01-01T00:00:00.000Z";
			const newIso = "2999-01-01T00:00:00.000Z";

			let detailCall = 0;
			let deleteCalls = 0;

			const db = {
				select: (projection?: unknown) => {
					if (projection) {
						return {
							from: () => ({
								where: async () => [{ id: "old" }, { id: "new" }],
							}),
						};
					}

					return {
						from: () => ({
							where: () => ({
								limit: async () => {
									detailCall += 1;
									if (detailCall === 1) {
										return [{ id: "old", changedAt: oldIso }];
									}
									return [{ id: "new", changedAt: newIso }];
								},
							}),
						}),
					};
				},
				delete: () => ({
					where: async () => {
						deleteCalls += 1;
					},
				}),
			} as unknown as SqliteDatabase;

			await clearOldOutboxItems(db, 7 * 24 * 60 * 60 * 1000);

			expect(deleteCalls).toBe(1);
		});
	});

	describe("parseRowId", () => {
		it("returns plain row id unchanged", () => {
			expect(parseRowId("entity-123")).toBe("entity-123");
		});

		it("parses composite JSON key", () => {
			expect(parseRowId('{"part_a":"a","part_b":"b"}')).toEqual({
				part_a: "a",
				part_b: "b",
			});
		});

		it("returns malformed JSON unchanged", () => {
			expect(parseRowId("{bad")).toBe("{bad");
		});
	});

	describe("getDrizzleTable", () => {
		it("returns mapped table from runtime schema", () => {
			const table = getDrizzleTable("entity_table");
			expect(table).toBeDefined();
		});

		it("returns undefined for unknown table", () => {
			const table = getDrizzleTable("unknown_table");
			expect(table).toBeUndefined();
		});
	});

	describe("fetchLocalRowByPrimaryKey", () => {
		it("throws for unknown table before DB lookup", async () => {
			const db = {} as unknown as SqliteDatabase;

			await expect(
				fetchLocalRowByPrimaryKey(
					db,
					"unknown_table" as unknown as Parameters<
						typeof fetchLocalRowByPrimaryKey
					>[1],
					"id-1",
				),
			).rejects.toThrow("Unknown table: unknown_table");
		});
	});
});
