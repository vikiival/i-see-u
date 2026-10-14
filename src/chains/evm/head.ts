import { createPublicClient, custom } from 'viem';
import type { Chain } from 'viem';

import type { BlockHeader, HeadObservation, Hex, RpcClient } from '../../core/types.ts';

const hexPattern = /^0x[0-9a-f]+$/i;

const validBlock = (value: unknown): boolean => {
  if (!value || typeof value !== 'object') return false;
  const block = value as Record<string, unknown>;
  return ['number', 'hash', 'parentHash', 'timestamp'].every(key => typeof block[key] === 'string' && hexPattern.test(block[key]));
};

export interface HeadTracker {
  /** Cached for cacheMs and single-flight; on failure or a lagging node, the newer cached block is served stale. */
  finalizedHead(): Promise<HeadObservation>;
  /** Highest latest-block number seen by the background refresh in finalizedHead(); never triggers RPC. */
  latestBlockNumber(): bigint | undefined;
  latestHeader(): Promise<BlockHeader>;
  /** Uncached header of one block by number. */
  blockByNumber(number: bigint): Promise<BlockHeader>;
}

export function createHeadTracker(
  rpc: RpcClient,
  opts: { cacheMs: number; maxNoProgressMs: number; chain: Chain },
): HeadTracker {
  // viem's getBlock formats the block; RpcClient owns retries, failover and billing. getBlock cannot pass
  // RpcClient options, so the transport injects block validation (a null block fails over).
  const client = createPublicClient({
    batch: { multicall: false },
    chain: opts.chain,
    transport: custom({
      request: ({ method, params }) => rpc.request(method, params ? [...params] : [], { validate: validBlock }),
    }, { retryCount: 0 }),
  });

  let observation: HeadObservation | undefined;
  let checkedAt = 0;
  let inFlight: Promise<HeadObservation> | undefined;
  let latest: bigint | undefined;

  const fetchBlock = async (at: 'finalized' | 'latest' | bigint): Promise<BlockHeader> => {
    const block = await client.getBlock(typeof at === 'bigint' ? { blockNumber: at } : { blockTag: at });
    return {
      number: block.number,
      hash: block.hash.toLowerCase() as Hex,
      parentHash: block.parentHash.toLowerCase() as Hex,
      timestamp: block.timestamp,
    };
  };

  const refresh = async (): Promise<HeadObservation> => {
    // Also refresh the latest head so responses can report real lag behind the tip.
    // One extra call per cache window, shared by all requests; failures only cost the lag figure.
    fetchBlock('latest').then((block) => {
      if (latest === undefined || block.number > latest) latest = block.number;
    }, () => {});
    try {
      const block = await fetchBlock('finalized');
      checkedAt = Date.now();
      if (observation && block.number < observation.block.number) {
        // A lagging node: keep the newer block, but flag it.
        observation = { ...observation, stale: true };
      } else {
        const lastProgressAt = !observation || block.number > observation.block.number ? checkedAt : observation.lastProgressAt;
        observation = { block, lastProgressAt, stale: checkedAt - lastProgressAt > opts.maxNoProgressMs };
      }
    } catch (error) {
      if (!observation) throw error;
      checkedAt = Date.now();
      observation = { ...observation, stale: true };
    }
    return observation;
  };

  return {
    finalizedHead() {
      if (observation && Date.now() - checkedAt < opts.cacheMs) return Promise.resolve(observation);
      inFlight ??= refresh().finally(() => { inFlight = undefined; });
      return inFlight;
    },
    latestBlockNumber: () => latest,
    latestHeader: () => fetchBlock('latest'),
    blockByNumber: (number) => fetchBlock(number),
  };
}
