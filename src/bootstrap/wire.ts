import { createEvmAdapter } from '../chains/evm/adapter.ts';
import type { config as appConfig } from '../config.ts';
import type { Store } from '../core/types.ts';
import { createRpcClient } from '../rpc/client.ts';
import { nativeHypersyncClient } from '../sources/hypersync-wire.ts';
import type { BootstrapStore } from './store.ts';
import { runBootstrap } from './bootstrap.ts';
import type { BootstrapResult } from './bootstrap.ts';
import { createHypersyncBootstrapSource, createRpcBootstrapSource } from './sources.ts';
import type { BootstrapSourceStats } from './sources.ts';

type AppConfig = typeof appConfig;

/** Production wiring shared by `npm run bootstrap` and BOOTSTRAP_ON_START: its own RPC client with a bulk timeout. */
export interface ConfiguredBootstrap {
  done: Promise<BootstrapResult>;
  stats: () => { rpc: BootstrapSourceStats; hypersync?: BootstrapSourceStats; rpcMethods: Record<string, number> };
}

export function runConfiguredBootstrap(config: AppConfig, network: AppConfig['ethereum'], deps: { store: Store; bootstrap: BootstrapStore; log: (message: string) => void }): ConfiguredBootstrap {
  const b = config.bootstrap;
  const rpcClient = createRpcClient({ ...config.rpc, timeoutMs: b.rpcTimeoutMs });
  const adapter = createEvmAdapter(rpcClient, { ...network, network: network.name });
  const rpc = createRpcBootstrapSource(rpcClient, { logRange: b.rpcLogRange });

  const hypersync = b.hypersyncApiKey === undefined ? undefined : createHypersyncBootstrapSource({
    url: b.hypersyncUrl, apiToken: b.hypersyncApiKey, idleMs: b.hypersyncIdleMs,
    clientFactory: () => nativeHypersyncClient(b.hypersyncUrl, b.hypersyncApiKey!, b.hypersyncIdleMs),
    onRateLimit: (waitMs) => deps.log(`HyperSync quota exhausted; waiting ${Math.round(waitMs / 1000)} s`),
  });

  if (!hypersync) deps.log('HYPERSYNC_API_KEY is not set: RPC-only bootstrap (one eth_getBlockByNumber per header)');

  const done = runBootstrap({
    network: network.name, startBlock: network.startBlock, adapter, index: deps.store, store: deps.bootstrap, hypersync, rpc,
    segmentBlocks: b.segmentBlocks, headerChunkBlocks: b.headerChunkBlocks, rpcWorkers: b.rpcWorkers, workMem: b.workMem, maintenanceWorkMem: b.maintenanceWorkMem,
    log: deps.log,
  });

  return {
    done,
    stats: () => ({ rpc: rpc.stats(), ...(hypersync && { hypersync: hypersync.stats() }), rpcMethods: rpcClient.stats().byMethod }),
  };
}
