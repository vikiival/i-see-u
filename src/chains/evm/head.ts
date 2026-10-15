import { createPublicClient, custom } from 'viem';
import type { Chain } from 'viem';

import { isObject, isString, toLowerHex } from '../../core/parse.ts';
import type { BlockHeader, HeadObservation, RpcClient } from '../../core/types.ts';

const hexPattern = /^0x[0-9a-f]+$/i;

/** The header fields of an eth_getBlockByNumber result that viem's formatter needs, before formatting. */
interface WireBlockHeader {
  number: string;
  hash: string;
  parentHash: string;
  timestamp: string;
}

const isHexString = (value: unknown): value is string => isString(value) && hexPattern.test(value);

const validBlock = (value: unknown): value is WireBlockHeader =>
  isObject(value)
  && 'number' in value && isHexString(value.number)
  && 'hash' in value && isHexString(value.hash)
  && 'parentHash' in value && isHexString(value.parentHash)
  && 'timestamp' in value && isHexString(value.timestamp);

export interface HeadTracker {
  /** Cached for cacheMs and single-flight; on failure or a lagging node, the newer cached block is served stale. */
  finalizedHead(): Promise<HeadObservation>;
  /** Highest latest-block number seen by the background refresh in finalizedHead(); never triggers RPC. */
  latestBlockNumber(): bigint | undefined;
  /** Uncached `latest` header (the tail polls it). */
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
    const block = await client.getBlock(at === 'finalized' || at === 'latest' ? { blockTag: at } : { blockNumber: at });

    return {
      number: block.number,
      hash: toLowerHex(block.hash),
      parentHash: toLowerHex(block.parentHash),
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
        observation = { block: observation.block, lastProgressAt: observation.lastProgressAt, stale: true };
      } else {
        const lastProgressAt = !observation || block.number > observation.block.number ? checkedAt : observation.lastProgressAt;
        observation = { block, lastProgressAt, stale: checkedAt - lastProgressAt > opts.maxNoProgressMs };
      }
    } catch (error) {
      if (!observation) throw error;
      checkedAt = Date.now();
      observation = { ...observation, stale: true, refreshFailed: true };
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
