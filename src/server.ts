import { serve } from '@hono/node-server';

import { createApp } from './api/routes.ts';
import { createBalanceReader } from './balances/reader.ts';
import { assertStartBlock } from './bootstrap/bootstrap.ts';
import { runConfiguredBootstrap } from './bootstrap/wire.ts';
import { createEvmAdapter } from './chains/evm/adapter.ts';
import { config } from './config.ts';
import { createLimiter } from './core/limiter.ts';
import { startBackground } from './core/runtime.ts';
import type { ConfiguredBootstrap } from './bootstrap/wire.ts';
import { createDiscoveryService } from './discovery/service.ts';
import { createFollower } from './follower/follower.ts';
import { createRpcClient } from './rpc/client.ts';
import { openPostgresStore } from './store/postgres.ts';
import { createTail } from './tail/tail.ts';

const rpc = createRpcClient(config.rpc);

const adapters = Object.fromEntries(Object.values(config.networks).map((network) =>
  [network.name, createEvmAdapter(rpc, { ...network, network: network.name, headStallMs: config.headStallMs })]));

const { store, bootstrap, followerLock, bootstrapLock } = await openPostgresStore(config.databaseUrl, { max: config.db.poolMax, connectionTimeoutMillis: config.db.connectTimeoutMs });

// A database initialized (or being bootstrapped) from another START_BLOCK cannot serve this configuration.
try {
  for (const network of Object.values(config.networks)) await assertStartBlock(store, bootstrap, network.name, network.startBlock);
} catch (error) {
  await store.close();
  throw error;
}

const requestRpc = { limiter: createLimiter(config.request.rpcConcurrency, config.request.rpcQueue), deadlineMs: config.request.deadlineMs };

const balances = Object.fromEntries(Object.values(config.networks).map((network) => [network.name, createBalanceReader({
  adapter: adapters[network.name], store, requestRpc,
  batchSize: network.multicallBatch, windowMs: config.balances.batchWindowMs, alwaysRefresh: network.alwaysRefreshTokens,
})]));

const followers = Object.fromEntries(Object.values(adapters).map((adapter) => [adapter.network, createFollower({
  adapter, store, lock: config.serveOnly ? undefined : followerLock(adapter.network), ...config.follower,
  maxNoAdvanceMs: config.maxHeadProgressAgeMs,
  onError: (message) => console.warn('Follower failed:', message),
})]));

// Leader-only (the follower's lock); every replica loads the shared tip and serves latest from the database.
const tails = config.tail.enabled ? Object.fromEntries(Object.values(adapters).map((adapter) => [adapter.network, createTail({
  adapter, store, follower: followers[adapter.network],
  maxDepth: config.tail.maxDepth, maxAheadBlocks: config.tail.maxAheadBlocks, pollOffsetsMs: config.tail.pollOffsetsMs,
  onEpoch: () => followers[adapter.network].kick(),
  onError: (message) => console.warn('Tail failed:', message),
})])) : undefined;

const discovery = createDiscoveryService({
  adapters, store, tails, maxTailTipAgeMs: config.tail.maxTipAgeMs, maxHeadAgeMs: config.maxHeadAgeMs, maxNoAdvanceMs: config.maxHeadProgressAgeMs,
});

const bootstrapRuns = new Map<string, ConfiguredBootstrap>();

console.log(`i-see-u mode: ${config.serveOnly ? 'serve-only' : 'indexer + API'}`);

await startBackground(config.serveOnly, [...Object.values(followers), ...Object.values(tails ?? {})], async () => {
  if (!config.bootstrap.onStart) return;

  for (const network of Object.values(config.networks)) {
    if (await store.indexState(network.name)) continue;
    const lock = bootstrapLock(network.name);

    if (!await lock.tryAcquire()) {
      console.warn(`Bootstrap of ${network.name} is running elsewhere`);
      continue;
    }

    const log = (message: string) => console.log(`[bootstrap ${network.name}] ${message}`);
    const run = runConfiguredBootstrap(config, network, { store, bootstrap, log });
    bootstrapRuns.set(network.name, run);
    run.done
      .then(() => followers[network.name].kick(), (error) => console.error(`[bootstrap ${network.name}] failed: ${error instanceof Error ? error.message : String(error)}`))
      .finally(() => lock.release());
  }
});

const app = createApp({
  discovery, adapters, balances, followers, tails, requestRpc,
  maxVerifyOwnershipItems: config.request.maxVerifyOwnershipItems,
  rpcStats: () => rpc.stats(), startedAt: Date.now(),
  bootstrap: (network) => bootstrap.status(network, Date.now()),
  opsSnapshot: (network) => store.opsSnapshot(network),
  hypersyncStats: (network) => bootstrapRuns.get(network)?.stats().hypersync,
  bootstrapRpcStats: () => Object.fromEntries([...bootstrapRuns].map(([network, run]) => [`bootstrap:${network}`, run.rpcStats()])),
  readyzAllow: config.readyzAllow,
}, { requestLog: process.env.LOG_LEVEL !== 'warn' && process.env.LOG_LEVEL !== 'error' });

const server = serve({ fetch: app.fetch, port: config.port, hostname: '0.0.0.0' }, (info) => {
  console.log(`i-see-u listening on http://localhost:${info.port}; bootstrap (npm run bootstrap) serves partial data until publication`);
});

let shuttingDown = false;

const shutdown = () => {
  if (shuttingDown) return;
  shuttingDown = true;
  server.close(async (error) => {
    await Promise.all(Object.values(tails ?? {}).map((tail) => tail.stop()));
    await Promise.all(Object.values(followers).map((follower) => follower.stop()));
    await store.close();

    if (error) {
      console.error('shutdown failed', error);
      process.exitCode = 1;
    }
  });
};

process.on('SIGINT', shutdown);

process.on('SIGTERM', shutdown);
