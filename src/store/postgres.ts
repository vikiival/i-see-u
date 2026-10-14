import { fileURLToPath } from 'node:url';

import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';

import type { Store } from '../core/types.ts';
import * as schema from './schema.ts';
import { createStore } from './store.ts';

export const DEV_DATABASE_URL = 'postgresql://iseeu:iseeu@localhost:5432/iseeu';
export const migrationsFolder = fileURLToPath(new URL('../../drizzle', import.meta.url));

export async function openPostgresStore(databaseUrl: string): Promise<Store> {
  const pool = new Pool({ connectionString: databaseUrl });
  const db = drizzle(pool, { schema });
  try {
    await migrate(db, { migrationsFolder });
  } catch (error) {
    await pool.end();
    throw error;
  }
  return createStore(db, () => pool.end());
}
