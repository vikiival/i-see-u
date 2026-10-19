import { createApp } from '../src/api/routes.ts';
import { createBalanceReader } from '../src/balances/reader.ts';
import { createBootstrapStore } from '../src/bootstrap/store.ts';
import { createLimiter } from '../src/core/limiter.ts';
import type { Address, BlockHeader, BlockRange, TransferEvent } from '../src/core/types.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { createFollower } from '../src/follower/follower.ts';
import { createTail } from '../src/tail/tail.ts';
import type { TailOptions } from '../src/tail/tail.ts';
import { blockHash, FakeChain } from './fake-chain.ts';
import { createTestStore } from './store-fixture.ts';

export const header = (number: bigint): BlockHeader => ({ number, hash: blockHash(number), parentHash: blockHash(number - 1n), timestamp: number * 12n });
export const blockRange = (fromBlock: bigint, toBlock = fromBlock): BlockRange => ({ fromBlock, toBlock });
export const headersFor = (from: bigint, to = from): BlockHeader[] => Array.from({ length: Number(to - from + 1n) }, (_, i) => header(from + BigInt(i)));

export async function stack(chain = new FakeChain(102n), options: {
  initialize?: boolean; maxRanges?: number; range?: bigint; deadlineMs?: number;
  balances?: Partial<{ batchSize: number; windowMs: number; alwaysRefresh: Address[]; concurrency: number; queue: number }>;
  /** Wire a tail (latest view) into discovery and the API. */
  tail?: Partial<Omit<TailOptions, 'adapter' | 'store' | 'follower'>> & { maxTipAgeMs?: number };
} = {}) {
  const { store, db } = await createTestStore();
  const clock = { at: Date.now() };
  const adapter = chain.adapter({ followerMaxRange: options.range ?? 500n });

  if (options.initialize !== false) await store.initialize('ethereum', 100n, header(99n), clock.at);
  const follower = createFollower({ adapter, store, pollMs: 30_000, maxNoAdvanceMs: 1_800_000, maxRangesPerTick: options.maxRanges, now: () => clock.at });
  const { maxTipAgeMs, ...tailOptions } = options.tail ?? {};
  const tail = options.tail ? createTail({ adapter, store, follower, ...tailOptions }) : undefined;
  const tails = tail && { ethereum: tail };

  const discovery = createDiscoveryService({
    adapters: { ethereum: adapter }, store, tails, maxTailTipAgeMs: maxTipAgeMs, maxHeadAgeMs: 120_000, maxNoAdvanceMs: 1_800_000, now: () => clock.at,
  });

  const b = options.balances ?? {};
  const requestRpc = { limiter: createLimiter(b.concurrency ?? 2, b.queue ?? 8), deadlineMs: options.deadlineMs ?? 5_000 };
  const balances = createBalanceReader({ adapter, store, requestRpc, batchSize: b.batchSize ?? 1_000, windowMs: b.windowMs ?? 0, alwaysRefresh: b.alwaysRefresh ?? [] });

  const app = createApp({
    discovery, adapters: { ethereum: adapter }, balances: { ethereum: balances }, followers: { ethereum: follower }, tails, requestRpc,
    rpcStats: () => chain.rpc.stats(), startedAt: clock.at,
    bootstrap: (network) => createBootstrapStore(db).status(network, clock.at),
    opsSnapshot: (network) => store.opsSnapshot(network),
  });

  return {
    store, db, chain, adapter, follower, tail, discovery, app, balances, requestRpc, clock,
    close: async () => { await tail?.stop(); await follower.stop(); await store.close(); },
  };
}

export async function seeded(events: TransferEvent[], end = 103n) {
  const fixture = await createTestStore();
  const { store } = fixture;
  await store.initialize('ethereum', 100n, header(99n), 1000);
  await store.observeHead('ethereum', header(end), 1000);
  await store.appendRange('ethereum', blockRange(100n, end), headersFor(100n, end), events, 1000);

  return fixture;
}
