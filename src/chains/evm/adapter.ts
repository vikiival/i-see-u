import { isAddress } from 'viem';
import type { Chain } from 'viem';

import type { Address, ChainAdapter, RpcClient } from '../../core/types.ts';
import { createHeadTracker } from './head.ts';
import { scanAccountChunk } from './logs.ts';
import { readErc20Balances, readErc721Owners } from './multicall.ts';

/** How long a finalized-head observation is shared by all requests before asking the node again. */
const HEAD_CACHE_MS = 4_000;

export interface EvmAdapterOptions {
  network: string;
  chain: Chain;
  startBlock: bigint;
  /** Mark the finalized head stale after this long without a higher block number. */
  finalizedMaxNoProgressMs: number;
  logsMaxRange: bigint;
  logsMinRange: bigint;
  scanConcurrency: number;
  multicallBatch: number;
}

/** Any EVM chain with a plain JSON-RPC endpoint: differs only by options. */
export function createEvmAdapter(rpc: RpcClient, o: EvmAdapterOptions): ChainAdapter {
  const head = createHeadTracker(rpc, { cacheMs: HEAD_CACHE_MS, maxNoProgressMs: o.finalizedMaxNoProgressMs, chain: o.chain });
  return {
    network: o.network,
    startBlock: o.startBlock,
    logsMaxRange: o.logsMaxRange,
    logsMinRange: o.logsMinRange,
    scanConcurrency: o.scanConcurrency,
    normalizeAddress: (input) => isAddress(input, { strict: false }) ? input.toLowerCase() as Address : undefined,
    finalizedHead: head.finalizedHead,
    cachedLatestBlockNumber: head.latestBlockNumber,
    scanChunk: (account, range) => scanAccountChunk(rpc, account, range),
    erc20Balances: (account, tokens, blockHash) => readErc20Balances(rpc, account, tokens, blockHash, o.multicallBatch),
    erc721Owners: (items, blockHash) => readErc721Owners(rpc, items, blockHash, o.multicallBatch),
  };
}
