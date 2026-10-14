import { serve } from '@hono/node-server';
import { mainnet } from 'viem/chains';

import { createApp } from './api/routes.ts';
import { createEvmAdapter } from './chains/evm/adapter.ts';
import { config } from './config.ts';
import { createDiscoveryService } from './discovery/service.ts';
import { createRpcClient } from './rpc/client.ts';
import { openSqliteStore } from './store/sqlite.ts';

const rpc = createRpcClient(config.rpc);
const eth = config.ethereum;
const adapters = { [eth.network]: createEvmAdapter(rpc, { ...eth, chain: mainnet }) };
const store = openSqliteStore(config.dbPath);
const discovery = createDiscoveryService({
  adapters,
  store,
  maxHeadProgressAgeMs: config.maxHeadProgressAgeMs,
});

const app = createApp(
  { discovery, adapters, rpcStats: () => rpc.stats(), startedAt: Date.now() },
  { requestLog: process.env.LOG_LEVEL !== 'warn' && process.env.LOG_LEVEL !== 'error' },
);

const server = serve({ fetch: app.fetch, port: config.port, hostname: '0.0.0.0' }, (info) => {
  console.log(`i-see-u listening on http://localhost:${info.port} (${eth.network} from block ${eth.startBlock})`);
});
let shuttingDown = false;
const shutdown = () => {
  if (shuttingDown) return;
  shuttingDown = true;
  server.close((error) => {
    store.close();
    if (error) {
      console.error('shutdown failed', error);
      process.exitCode = 1;
    }
  });
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
