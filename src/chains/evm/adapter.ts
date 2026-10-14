import { isAddress } from 'viem';
import type { Chain } from 'viem';

import type { Address, ChainAdapter, RpcClient } from '../../core/types.ts';
import { createHeadTracker } from './head.ts';
import { scanAccountChunk, scanAllTransfers } from './logs.ts';
import { readErc20BalancePairs, readErc721Owners } from './multicall.ts';

/** Default time a finalized-head observation is shared by all callers before asking the node again. */
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
  /** Max span of one unfiltered follower getLogs call. */
  followerMaxRange: bigint;
  multicallBatch: number;
  headCacheMs?: number;
}

/** Any EVM chain with a plain JSON-RPC endpoint: differs only by options. */
export function createEvmAdapter(rpc: RpcClient, o: EvmAdapterOptions): ChainAdapter {
  const head = createHeadTracker(rpc, { cacheMs: o.headCacheMs ?? HEAD_CACHE_MS, maxNoProgressMs: o.finalizedMaxNoProgressMs, chain: o.chain });
  return {
    network: o.network,
    startBlock: o.startBlock,
    logsMaxRange: o.logsMaxRange,
    logsMinRange: o.logsMinRange,
    scanConcurrency: o.scanConcurrency,
    followerMaxRange: o.followerMaxRange,
    normalizeAddress: (input) => isAddress(input, { strict: false }) ? input.toLowerCase() as Address : undefined,
    finalizedHead: head.finalizedHead,
    cachedLatestBlockNumber: head.latestBlockNumber,
    scanChunk: (account, range) => scanAccountChunk(rpc, account, range),
    blockByNumber: head.blockByNumber,
    scanAllTransfers: (range, onTransfer, accept) => scanAllTransfers(rpc, range, onTransfer, accept),
    erc20BalancePairs: (pairs, blockHash) => readErc20BalancePairs(rpc, pairs, blockHash, o.multicallBatch),
    erc721Owners: (items, blockHash) => readErc721Owners(rpc, items, blockHash, o.multicallBatch),
  };
}
