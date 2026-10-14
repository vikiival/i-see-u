import type { Chain } from 'viem';

import type { BlockRef, HeadObservation, Hex, RpcClient } from '../../core/types.ts';
import { createViemClient } from './viem.ts';

interface WireBlock {
  number: string;
  hash: string;
  parentHash: string;
  timestamp: string;
}

interface CacheState {
  observation?: HeadObservation;
  checkedAt: number;
  inFlight?: Promise<HeadObservation>;
}

export interface HeadTracker {
  finalizedHead(): Promise<HeadObservation>;
  safeHead(): Promise<HeadObservation>;
  /** Latest raw chain head, refreshed alongside finalizedHead() and safeHead(). */
  latest(): HeadObservation | undefined;
}

const hexPattern = /^0x[0-9a-f]+$/i;

const validBlock = (value: unknown): value is WireBlock => {
  if (!value || typeof value !== 'object') return false;
  const block = value as Partial<WireBlock>;
  return typeof block.number === 'string' && hexPattern.test(block.number)
    && typeof block.hash === 'string' && hexPattern.test(block.hash)
    && typeof block.parentHash === 'string' && hexPattern.test(block.parentHash)
    && typeof block.timestamp === 'string' && hexPattern.test(block.timestamp);
};

const DEFAULT_FINALIZED_MAX_NO_PROGRESS_MS = 1_200_000;
const DEFAULT_LATEST_MAX_NO_PROGRESS_MS = 120_000;

export function createHeadTracker(
  rpc: RpcClient,
  opts: {
    confirmations: bigint;
    cacheMs: number;
    finalizedMaxNoProgressMs?: number;
    latestMaxNoProgressMs?: number;
    chain: Chain;
  },
): HeadTracker {
  if (opts.confirmations < 0n) throw new Error('confirmations must not be negative');
  if (opts.cacheMs < 0) throw new Error('cacheMs must not be negative');
  const finalizedMaxNoProgressMs = opts.finalizedMaxNoProgressMs ?? DEFAULT_FINALIZED_MAX_NO_PROGRESS_MS;
  const latestMaxNoProgressMs = opts.latestMaxNoProgressMs ?? DEFAULT_LATEST_MAX_NO_PROGRESS_MS;
  if (!Number.isFinite(finalizedMaxNoProgressMs) || finalizedMaxNoProgressMs < 0) {
    throw new Error('finalizedMaxNoProgressMs must not be negative');
  }
  if (!Number.isFinite(latestMaxNoProgressMs) || latestMaxNoProgressMs < 0) {
    throw new Error('latestMaxNoProgressMs must not be negative');
  }

  const finalized: CacheState = { checkedAt: 0 };
  const safe: CacheState = { checkedAt: 0 };
  const latest: CacheState = { checkedAt: 0 };
  // getBlock cannot pass RpcClient validation options, so inject validation in its transport facade.
  const client = createViemClient({
    request: (method, params, requestOpts) => rpc.request(method, params, {
      ...requestOpts,
      validate: method === 'eth_getBlockByNumber' ? validBlock : requestOpts?.validate,
    }),
    stats: () => rpc.stats(),
  }, opts.chain);

  const observe = (state: CacheState, block: BlockRef, responseAt: number, maxNoProgressMs: number): HeadObservation => {
    if (state.observation && block.number < state.observation.block.number) {
      state.checkedAt = responseAt;
      state.observation = { ...state.observation, lastResponseAt: responseAt, stale: true };
      return state.observation;
    }
    const lastProgressAt = !state.observation || block.number > state.observation.block.number
      ? responseAt
      : state.observation.lastProgressAt;
    state.checkedAt = responseAt;
    state.observation = {
      block,
      lastResponseAt: responseAt,
      lastProgressAt,
      stale: responseAt - lastProgressAt > maxNoProgressMs,
    };
    return state.observation;
  };

  const staleFallback = (state: CacheState, checkedAt: number, error?: unknown): HeadObservation => {
    if (!state.observation) throw error;
    state.checkedAt = checkedAt;
    state.observation = { ...state.observation, stale: true };
    return state.observation;
  };

  const fetchBlock = async (tag: 'finalized' | 'latest' | bigint): Promise<BlockRef> => {
    const block = typeof tag === 'bigint'
      ? await client.getBlock({ blockNumber: tag })
      : await client.getBlock({ blockTag: tag });
    return {
      number: block.number,
      hash: block.hash.toLowerCase() as Hex,
      parentHash: block.parentHash.toLowerCase() as Hex,
      timestamp: block.timestamp,
    };
  };

  const cachedOrFetch = (
    state: CacheState,
    fetcher: () => Promise<HeadObservation>,
  ): Promise<HeadObservation> => {
    if (state.observation && Date.now() - state.checkedAt < opts.cacheMs) {
      return Promise.resolve(state.observation);
    }
    if (state.inFlight) return state.inFlight;
    state.inFlight = fetcher().finally(() => {
      state.inFlight = undefined;
    });
    return state.inFlight;
  };

  const finalizedHead = (): Promise<HeadObservation> => cachedOrFetch(finalized, async () => {
    // Also refresh the latest head so responses can report real lag behind the tip.
    // One extra call per cache window, shared by all requests; failures only cost the lag figure.
    void fetchBlock('latest').then(
      (block) => observe(latest, block, Date.now(), latestMaxNoProgressMs),
      () => { if (latest.observation) staleFallback(latest, Date.now()); },
    );
    try {
      const block = await fetchBlock('finalized');
      return observe(finalized, block, Date.now(), finalizedMaxNoProgressMs);
    } catch (error) {
      return staleFallback(finalized, Date.now(), error);
    }
  });

  const safeHead = (): Promise<HeadObservation> => cachedOrFetch(safe, async () => {
    try {
      const fetchedLatest = await fetchBlock('latest');
      observe(latest, fetchedLatest, Date.now(), latestMaxNoProgressMs);

      const target = fetchedLatest.number > opts.confirmations ? fetchedLatest.number - opts.confirmations : 0n;
      const block = opts.confirmations === 0n
        ? fetchedLatest
        : await fetchBlock(target);
      return observe(safe, block, Date.now(), latestMaxNoProgressMs);
    } catch (error) {
      if (latest.observation) staleFallback(latest, Date.now());
      return staleFallback(safe, Date.now(), error);
    }
  });

  return {
    finalizedHead,
    safeHead,
    latest: () => latest.observation,
  };
}
