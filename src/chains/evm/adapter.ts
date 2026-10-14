import { isAddress } from 'viem';
import type { Chain } from 'viem';

import type { Address, BlockRange, ChainAdapter, RpcClient } from '../../core/types.ts';
import { createHeadTracker } from './head.ts';
import { scanAccountChunk } from './logs.ts';
import { readErc20Balances, readErc721Owners } from './multicall.ts';

export interface EvmAdapterOptions {
  network: string;
  chain: Chain;
  startBlock: bigint;
  confirmations: bigint;
  headCacheMs: number;
  finalizedMaxNoProgressMs: number;
  latestMaxNoProgressMs: number;
  logsMaxRange: bigint;
  logsMinRange: bigint;
  /** A getLogs response larger than this (bytes) or slower than this (ms) shrinks the account's range. */
  logsHeavyBytes: number;
  logsHeavyMs: number;
  scanConcurrency: number;
  multicall3: Address;
  multicallBatch: number;
}

/** Any EVM chain with a plain JSON-RPC endpoint: differs only by options. */
export function createEvmAdapter(rpc: RpcClient, o: EvmAdapterOptions): ChainAdapter {
  const head = createHeadTracker(rpc, {
    confirmations: o.confirmations,
    cacheMs: o.headCacheMs,
    finalizedMaxNoProgressMs: o.finalizedMaxNoProgressMs,
    latestMaxNoProgressMs: o.latestMaxNoProgressMs,
    chain: o.chain,
  });
  const mc = { multicall3: o.multicall3, batchSize: o.multicallBatch };
  return {
    network: o.network,
    startBlock: o.startBlock,
    logsMaxRange: o.logsMaxRange,
    logsMinRange: o.logsMinRange,
    scanConcurrency: o.scanConcurrency,
    normalizeAddress: (input) => isAddress(input, { strict: false }) ? input.toLowerCase() as Address : undefined,
    finalizedHead: () => head.finalizedHead(),
    safeHead: () => head.safeHead(),
    cachedLatestHead: () => head.latest(),
    scanChunk: (account: Address, range: BlockRange) =>
      scanAccountChunk(rpc, account, range, { heavyBytes: o.logsHeavyBytes, heavyMs: o.logsHeavyMs }),
    erc20Balances: (account, tokens, at) => readErc20Balances(rpc, account, tokens, at, mc),
    erc721Owners: (items, at) => readErc721Owners(rpc, items, at, mc),
  };
}
