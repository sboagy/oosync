export function orderSyncableTablesByDependency(params: {
  syncableTables: string[];
  tableSyncOrder: Record<string, number>;
}): string[] {
  return [...params.syncableTables].sort((a, b) => {
    const orderA = params.tableSyncOrder[a] ?? Number.MAX_SAFE_INTEGER;
    const orderB = params.tableSyncOrder[b] ?? Number.MAX_SAFE_INTEGER;
    if (orderA !== orderB) return orderA - orderB;
    return a.localeCompare(b);
  });
}
