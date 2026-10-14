import { serve } from '@hono/node-server';

import { createApp } from './api/routes.ts';
import { createBalanceWorker } from './balances/worker.ts';
import { createEvmAdapter } from './chains/evm/adapter.ts';
import { config } from './config.ts';
import { createLimiter } from './core/limiter.ts';
import { createDiscoveryService } from './discovery/service.ts';
import { createFollower } from './follower/follower.ts';
import { createRpcClient } from './rpc/client.ts';
import { withFallback } from './sources/fallback.ts';
import { createHypersyncLogSource } from './sources/hypersync.ts';
import { createRpcLogSource } from './sources/rpc-logs.ts';
import { openPostgresStore } from './store/postgres.ts';

const rpc = createRpcClient(config.rpc);
const adapters = Object.fromEntries(Object.values(config.networks).map((network) => {
  const rpcLogs = createRpcLogSource(rpc, {
    preferredMaxRange: network.logsMaxRange,
    minRange: network.logsMinRange,
  });
  const useHypersync = config.logSource === 'hypersync'
    || (config.logSource === 'auto' && config.hypersync.apiToken !== undefined);
  const accountLogs = useHypersync
    ? withFallback(createHypersyncLogSource({
        url: config.hypersync.url ?? network.hypersyncUrl,
        apiToken: config.hypersync.apiToken!,
        preferredMaxRange: config.hypersync.maxRange,
        timeoutMs: config.hypersync.timeoutMs,
      }), rpcLogs)
    : rpcLogs;
  return [network.name, createEvmAdapter(rpc, { ...network, network: network.name, accountLogs })];
}));
const { store, followerLock } = await openPostgresStore(config.databaseUrl);
const logError = (what: string) => (error: unknown) =>
  console.warn(`${what} failed:`, error instanceof Error ? error.message : String(error));

const requestRpc = {
  limiter: createLimiter(config.request.rpcConcurrency, config.request.rpcQueue),
  deadlineMs: config.request.deadlineMs,
};
const balances = Object.fromEntries(Object.values(adapters).map((adapter) => [adapter.network, createBalanceWorker({
  adapter,
  store,
  activeTtlMs: config.balances.activeAccountTtlMs,
  refreshMs: config.balances.refreshMs,
  requestRpc,
  onError: logError('balance refresh'),
})]));
const followers = Object.fromEntries(Object.values(adapters).map((adapter) => [adapter.network, createFollower({
  adapter,
  store,
  lock: followerLock(adapter.network),
  pollMs: config.follower.pollMs,
  maxLagBlocks: config.follower.maxLagBlocks,
  staleAfterMs: config.networks[adapter.network].finalizedMaxNoProgressMs,
  maxNoAdvanceMs: config.maxHeadProgressAgeMs,
  onAdvance: () => balances[adapter.network].kick(),
  onError: logError('follower tick'),
})]));
const discovery = createDiscoveryService({
  adapters,
  store,
  followers,
  requestDeadlineMs: config.request.deadlineMs,
  backfillLimiter: createLimiter(config.request.backfillConcurrency, config.request.backfillQueue),
});

await Promise.all(Object.values(followers).map((follower) => follower.load()));
for (const follower of Object.values(followers)) follower.start();
for (const worker of Object.values(balances)) worker.start();

const app = createApp(
  { discovery, adapters, balances, followers, requestRpc, rpcStats: () => rpc.stats(), startedAt: Date.now() },
  { requestLog: process.env.LOG_LEVEL !== 'warn' && process.env.LOG_LEVEL !== 'error' },
);

const server = serve({ fetch: app.fetch, port: config.port, hostname: '0.0.0.0' }, (info) => {
  const configured = Object.values(config.networks).map((network) => `${network.name} from block ${network.startBlock}`).join(', ');
  console.log(`i-see-u listening on http://localhost:${info.port} (${configured})`);
});
let shuttingDown = false;
const shutdown = () => {
  if (shuttingDown) return;
  shuttingDown = true;
  server.close(async (error) => {
    await Promise.all([...Object.values(followers), ...Object.values(balances)].map((part) => part.stop()));
    await Promise.all(Object.values(adapters).map((adapter) => adapter.accountLogs.close?.()));
    await store.close();
    if (error) {
      console.error('shutdown failed', error);
      process.exitCode = 1;
    }
  });
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
