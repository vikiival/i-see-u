import { PGlite } from '@electric-sql/pglite';
import { drizzle } from 'drizzle-orm/pglite';
import { migrate } from 'drizzle-orm/pglite/migrator';

import { migrationsFolder } from '../src/store/postgres.ts';
import * as schema from '../src/store/schema.ts';
import { createStore } from '../src/store/store.ts';

export async function createTestStore() {
  const client = new PGlite();
  const db = drizzle(client, { schema });
  await migrate(db, { migrationsFolder });
  return { client, db, store: createStore(db, () => client.close()) };
}
