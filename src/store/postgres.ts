import { fileURLToPath } from 'node:url';

import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';

import { createBootstrapStore } from '../bootstrap/store.ts';
import type { BootstrapStore } from '../bootstrap/store.ts';
import type { Store } from '../core/types.ts';
import { advisoryKey, pgLeaderLock } from './advisory-lock.ts';
import type { LeaderLock } from './advisory-lock.ts';
import * as schema from './schema.ts';
import { createStore } from './store.ts';

export const DEV_DATABASE_URL = 'postgresql://iseeu:iseeu@localhost:5432/iseeu';
export const migrationsFolder = fileURLToPath(new URL('../../drizzle', import.meta.url));

export interface PostgresHandle {
  store: Store;
  bootstrap: BootstrapStore;
  /** Session advisory lock for the one follower of `network`, on its own pool connection. */
  followerLock(network: string): LeaderLock;
  /** Session advisory lock for the one bootstrap loader of `network`. */
  bootstrapLock(network: string): LeaderLock;
}

export async function openPostgresStore(
  databaseUrl: string,
  opts: { max?: number; connectionTimeoutMillis?: number; migrate?: boolean } = {},
): Promise<PostgresHandle> {
  const pool = new Pool({
    connectionString: databaseUrl,
    max: opts.max,
    connectionTimeoutMillis: opts.connectionTimeoutMillis,
  });

  const db = drizzle(pool, { schema });

  try {
    if (opts.migrate !== false) {
      await migrate(db, { migrationsFolder });
    }
  } catch (error) {
    await pool.end();
    throw error;
  }

  return {
    store: createStore(db, () => pool.end()),
    bootstrap: createBootstrapStore(db),
    followerLock: (network) => pgLeaderLock(pool, advisoryKey(`follower:${network}`)),
    bootstrapLock: (network) => pgLeaderLock(pool, advisoryKey(`bootstrap:${network}`)),
  };
}
