import { serve } from '@hono/node-server';
import { mainnet } from 'viem/chains';

import { createApp } from './api/routes.ts';
import { createBalanceWorker } from './balances/worker.ts';
import { createEvmAdapter } from './chains/evm/adapter.ts';
import { config } from './config.ts';
import { createLimiter } from './core/limiter.ts';
import { createDiscoveryService } from './discovery/service.ts';
import { createFollower } from './follower/follower.ts';
import { createRpcClient } from './rpc/client.ts';
import { openPostgresStore } from './store/postgres.ts';

const rpc = createRpcClient(config.rpc);
const eth = config.ethereum;
const adapter = createEvmAdapter(rpc, { ...eth, chain: mainnet });
const adapters = { [eth.network]: adapter };
const { store, followerLock } = await openPostgresStore(config.databaseUrl);
const logError = (what: string) => (error: unknown) =>
  console.warn(`${what} failed:`, error instanceof Error ? error.message : String(error));

const requestRpc = {
  limiter: createLimiter(config.request.rpcConcurrency, config.request.rpcQueue),
  deadlineMs: config.request.deadlineMs,
};
const balances = {
  [eth.network]: createBalanceWorker({
    adapter,
    store,
    activeTtlMs: config.balances.activeAccountTtlMs,
    refreshMs: config.balances.refreshMs,
    requestRpc,
    onError: logError('balance refresh'),
  }),
};
const followers = {
  [eth.network]: createFollower({
    adapter,
    store,
    lock: followerLock(eth.network),
    pollMs: config.follower.pollMs,
    maxLagBlocks: config.follower.maxLagBlocks,
    staleAfterMs: eth.finalizedMaxNoProgressMs,
    maxNoAdvanceMs: config.maxHeadProgressAgeMs,
    onAdvance: () => balances[eth.network].kick(),
    onError: logError('follower tick'),
  }),
};
const discovery = createDiscoveryService({
  adapters,
  store,
  followers,
  requestDeadlineMs: config.request.deadlineMs,
  backfillLimiter: createLimiter(config.request.backfillConcurrency, config.request.backfillQueue),
});

for (const follower of Object.values(followers)) follower.start();
for (const worker of Object.values(balances)) worker.start();

const app = createApp(
  { discovery, adapters, balances, followers, requestRpc, rpcStats: () => rpc.stats(), startedAt: Date.now() },
  { requestLog: process.env.LOG_LEVEL !== 'warn' && process.env.LOG_LEVEL !== 'error' },
);

const server = serve({ fetch: app.fetch, port: config.port, hostname: '0.0.0.0' }, (info) => {
  console.log(`i-see-u listening on http://localhost:${info.port} (${eth.network} from block ${eth.startBlock})`);
});
let shuttingDown = false;
const shutdown = () => {
  if (shuttingDown) return;
  shuttingDown = true;
  server.close(async (error) => {
    await Promise.all([...Object.values(followers), ...Object.values(balances)].map((part) => part.stop()));
    await store.close();
    if (error) {
      console.error('shutdown failed', error);
      process.exitCode = 1;
    }
  });
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
