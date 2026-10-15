import { serve } from '@hono/node-server';

import { createApp } from './api/routes.ts';
import { createBalanceReader } from './balances/reader.ts';
import { createEvmAdapter } from './chains/evm/adapter.ts';
import { config } from './config.ts';
import { createLimiter } from './core/limiter.ts';
import { createDiscoveryService } from './discovery/service.ts';
import { createFollower } from './follower/follower.ts';
import { createRpcClient } from './rpc/client.ts';
import { openPostgresStore } from './store/postgres.ts';
import { createTail } from './tail/tail.ts';

const rpc = createRpcClient(config.rpc);

const adapters = Object.fromEntries(Object.values(config.networks).map((network) =>
  [network.name, createEvmAdapter(rpc, { ...network, network: network.name })]));

const { store, followerLock } = await openPostgresStore(config.databaseUrl, { max: config.db.poolMax, connectionTimeoutMillis: config.db.connectTimeoutMs });
const requestRpc = { limiter: createLimiter(config.request.rpcConcurrency, config.request.rpcQueue), deadlineMs: config.request.deadlineMs };

const balances = Object.fromEntries(Object.values(config.networks).map((network) => [network.name, createBalanceReader({
  adapter: adapters[network.name], store, requestRpc,
  batchSize: network.multicallBatch, windowMs: config.balances.batchWindowMs, alwaysRefresh: network.alwaysRefreshTokens,
})]));

const followers = Object.fromEntries(Object.values(adapters).map((adapter) => [adapter.network, createFollower({
  adapter, store, lock: followerLock(adapter.network), ...config.follower,
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

await Promise.all([...Object.values(followers), ...Object.values(tails ?? {})].map((part) => part.load()));

for (const part of [...Object.values(followers), ...Object.values(tails ?? {})]) part.start();

const app = createApp({
  discovery, adapters, balances, followers, tails, requestRpc,
  maxVerifyOwnershipItems: config.request.maxVerifyOwnershipItems,
  rpcStats: () => rpc.stats(), startedAt: Date.now(),
}, { requestLog: process.env.LOG_LEVEL !== 'warn' && process.env.LOG_LEVEL !== 'error' });

const server = serve({ fetch: app.fetch, port: config.port, hostname: '0.0.0.0' }, (info) => {
  console.log(`i-see-u listening on http://localhost:${info.port}; global bootstrap must initialize the index before it is ready`);
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
