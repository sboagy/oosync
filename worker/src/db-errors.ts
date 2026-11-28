type PostgresJsErrorLike = {
  message?: unknown;
  code?: unknown;
  detail?: unknown;
  hint?: unknown;
  where?: unknown;
  table_name?: unknown;
  schema_name?: unknown;
  column_name?: unknown;
  constraint_name?: unknown;
  cause?: unknown;
};

function isPostgresJsErrorLike(error: unknown): error is PostgresJsErrorLike {
  return typeof error === "object" && error !== null;
}

function getErrorCause(error: unknown): unknown {
  if (!isPostgresJsErrorLike(error)) return undefined;
  return error.cause;
}

export function findPostgresErrorLike(
  error: unknown
): PostgresJsErrorLike | undefined {
  // Some libraries wrap the underlying Postgres error under `cause`.
  let current: unknown = error;
  for (let depth = 0; depth < 4; depth += 1) {
    if (isPostgresJsErrorLike(current)) {
      // Heuristic: if it has any of the Postgres-ish fields, treat it as one.
      if (
        current.code !== undefined ||
        current.detail !== undefined ||
        current.constraint_name !== undefined ||
        current.table_name !== undefined ||
        current.column_name !== undefined
      ) {
        return current;
      }
    }
    const next = getErrorCause(current);
    if (next === undefined) break;
    current = next;
  }
  return undefined;
}
