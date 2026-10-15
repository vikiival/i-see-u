import { createHash } from 'node:crypto';

import { sql } from 'drizzle-orm';
import type { Pool, PoolClient } from 'pg';

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

/** Stable signed 64-bit advisory-lock key for a name, e.g. `follower:ethereum`. */
export const advisoryKey = (name: string): bigint =>
  BigInt.asIntN(64, BigInt(`0x${createHash('sha256').update(name).digest('hex').slice(0, 16)}`));

/**
 * Long-lived leadership: a SESSION advisory lock (pg_try_advisory_lock) held on one connection for the leader's
 * lifetime. A transaction-scoped lock would keep one transaction open forever. If the session dies, PostgreSQL
 * releases the lock and another process can take over; the follower's cursor check prevents double application.
 */
export interface LeaderLock {
  /** Acquire if free; when already held, verify the session is still alive (false if it was lost). */
  tryAcquire(): Promise<boolean>;
  held(): boolean;
  release(): Promise<void>;
}

/** node-postgres: a dedicated pool client holds the lock. */
export function pgLeaderLock(pool: Pool, key: bigint): LeaderLock {
  let client: PoolClient | undefined;

  const drop = (c: PoolClient): void => {
    if (client !== c) return;
    client = undefined;
    c.release(true); // destroy: the session (and with it the lock) is gone
  };

  return {
    async tryAcquire() {
      if (client) {
        const c = client;

        try {
          await c.query('SELECT 1');

          return true;
        } catch {
          drop(c);

          return false;
        }
      }

      const c = await pool.connect();

      try {
        const { rows } = await c.query<{ acquired: boolean }>('SELECT pg_try_advisory_lock($1) AS acquired', [key.toString()]);

        if (!rows[0]?.acquired) {
          c.release();

          return false;
        }
      } catch (error) {
        c.release(true);
        throw error;
      }

      client = c;
      c.on('error', () => drop(c));

      return true;
    },
    held: () => client !== undefined,
    async release() {
      const c = client;

      if (!c) return;
      client = undefined;

      try {
        await c.query('SELECT pg_advisory_unlock($1)', [key.toString()]);
        c.release();
      } catch {
        c.release(true);
      }
    },
  };
}

/** A database with exactly one connection (PGlite): the session lock lives on that connection. */
export function singleConnectionLeaderLock(db: StoreDatabase, key: bigint): LeaderLock {
  let held = false;

  const query = async (statement: ReturnType<typeof sql>): Promise<boolean> => {
    const result = await db.execute(statement);
    const [row] = (Array.isArray(result) ? result : result.rows) as { ok: boolean }[];

    return row?.ok === true;
  };

  return {
    async tryAcquire() {
      held ||= await query(sql`SELECT pg_try_advisory_lock(${key}) AS ok`);

      return held;
    },
    held: () => held,
    async release() {
      if (held) await query(sql`SELECT pg_advisory_unlock(${key}) AS ok`);
      held = false;
    },
  };
}
