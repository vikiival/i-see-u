import { sql } from 'drizzle-orm';

import type { StoreDatabase } from './store.ts';

/** Run in one transaction so PostgreSQL keeps the advisory lock on one session and releases it automatically. */
export async function withAdvisoryLock<T>(
  db: StoreDatabase,
  key: bigint,
  fn: () => Promise<T>,
): Promise<T | undefined> {
  return db.transaction(async (tx) => {
    const result = await tx.execute(sql<{ acquired: boolean }>`SELECT pg_try_advisory_xact_lock(${key}) AS acquired`);
    const [row] = (Array.isArray(result) ? result : result.rows) as { acquired: boolean }[];
    return row?.acquired ? fn() : undefined;
  });
}
