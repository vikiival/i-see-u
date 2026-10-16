// `npm run bootstrap`: one-time global load of [START_BLOCK, finalized], resumable; rerun after any failure.
import { config } from '../config.ts';
import { openPostgresStore } from '../store/postgres.ts';
import { assertStartBlock } from './bootstrap.ts';
import { runConfiguredBootstrap } from './wire.ts';

const log = (message: string) => console.log(`[bootstrap ${new Date().toISOString().slice(11, 19)}] ${message}`);
const { store, bootstrap, bootstrapLock } = await openPostgresStore(config.databaseUrl, { max: config.bootstrap.rpcWorkers + 4 });
let exitCode = 0;

for (const network of Object.values(config.networks)) {
  const lock = bootstrapLock(network.name);

  try {
    await assertStartBlock(store, bootstrap, network.name, network.startBlock);

    if (!await lock.tryAcquire()) throw new Error(`Another bootstrap of ${network.name} is running`);
    log(`${network.name}: start block ${network.startBlock}`);
    const run = runConfiguredBootstrap(config, network, { store, bootstrap, log });

    try {
      const result = await run.done;
      log(`${network.name}: ${result.status === 'done' ? `${result.transfers} transfers through ${result.target} in ${result.seconds}s` : 'index already initialized; nothing to do'}`);
    } finally {
      log(`${network.name} source stats: ${JSON.stringify(run.stats())}`);
    }
  } catch (error) {
    exitCode = 1;
    console.error(`[bootstrap] ${network.name} failed: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    await lock.release();
  }
}

await store.close();

// The native HyperSync client may keep abandoned requests alive; do not wait for them.
process.exit(exitCode);
