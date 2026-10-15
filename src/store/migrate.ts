import { DEV_DATABASE_URL, openPostgresStore } from './postgres.ts';

try {
  process.loadEnvFile(new URL('../../.env', import.meta.url));
} catch (error) {
  if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
}

const { store } = await openPostgresStore(process.env.DATABASE_URL ?? DEV_DATABASE_URL);

await store.close();
