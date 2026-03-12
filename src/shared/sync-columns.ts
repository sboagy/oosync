import { integer, text, timestamp } from "drizzle-orm/pg-core";
import {
  integer as sqliteInteger,
  text as sqliteText,
} from "drizzle-orm/sqlite-core";

export const pgSyncColumns = {
  syncVersion: integer("sync_version").default(1).notNull(),
  lastModifiedAt: timestamp("last_modified_at").defaultNow().notNull(),
  deviceId: text("device_id"),
};

export const sqliteSyncColumns = {
  syncVersion: sqliteInteger("sync_version").default(1).notNull(),
  lastModifiedAt: sqliteText("last_modified_at")
    .notNull()
    .$defaultFn(() => new Date().toISOString()),
  deviceId: sqliteText("device_id"),
};

export interface SyncColumns {
  syncVersion: number;
  lastModifiedAt: Date | string;
  deviceId: string | null;
}

export const SYNC_COLUMN_NAMES = [
  "sync_version",
  "last_modified_at",
  "device_id",
] as const;
