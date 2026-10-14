import { expect, test } from 'vitest';

import { createApp } from '../src/api/routes.ts';
import { createBalanceWorker } from '../src/balances/worker.ts';
import { createLimiter } from '../src/core/limiter.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import type { Address, BlockHeader, ChunkAggregate, FollowerStatus, Hex } from '../src/core/types.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { createTail, nextPollDelay } from '../src/tail/tail.ts';
import { address, blockHash, blockRef, FakeChain } from './fake-chain.ts';
import { createTestStore } from './store-fixture.ts';

const account = address(1);
const peer = address(2);
const token = address(10);
const nft = address(11);

async function stack(chain: FakeChain, maxDepth = 64n) {
  const { store } = await createTestStore();
  await store.initFollowerCursor('ethereum', blockRef(chain.finalized), 1_800_000_000_000);
  const adapter = chain.adapter({ startBlock: chain.finalized + 1n });
  const followerStatus = (): FollowerStatus => ({
    ready: true,
    leader: true,
    cursor: { ...blockRef(chain.finalized), updatedAt: 1_800_000_000_000 },
    headNumber: chain.finalized,
    lagBlocks: 0n,
    lastAdvanceAt: 1_800_000_000_000,
    stale: false,
  });
  const follower = { status: followerStatus, track() {} };
  const tail = createTail({ adapter, store, follower, maxDepth, now: () => 1_800_000_010_000 });
  await tail.load();
  const discovery = createDiscoveryService({
    adapters: { ethereum: adapter }, store, followers: { ethereum: follower }, tails: { ethereum: tail }, requestDeadlineMs: 1_000,
  });
  return { store, adapter, follower, tail, discovery, close: () => store.close() };
}

const ready = async (s: Awaited<ReturnType<typeof stack>>, finality?: 'latest' | 'finalized') => {
  const result = await s.discovery.discover('ethereum', account, finality);
  if (result.status !== 'ready') throw new Error('expected ready');
  return result;
};

test('follows the tip block by block with one header and one block-hash getLogs call per block', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  const s = await stack(chain);
  try {
    await s.tail.tick();
    expect(chain.calls).toEqual({ eth_getBlockByNumber: 1, eth_getLogs: 1 });
    expect(await s.store.getTailTip('ethereum')).toMatchObject({ number: 101n, hash: blockHash(101n) });

    chain.latest = 102n;
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
    chain.calls = {};
    await s.tail.tick();
    expect(chain.calls).toEqual({ eth_getBlockByNumber: 1, eth_getLogs: 1 });
    expect((await ready(s)).holdings[0]).toMatchObject({ receivedCount: 2, unfinalizedReceivedCount: 2, lastBlock: 102n });
  } finally {
    await s.close();
  }
});

test('depth-1 and depth-3 reorgs delete old transfers and recompute counts, bounds and NFT state', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 104n;
  for (let blockNumber = 101n; blockNumber <= 104n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }
  chain.addTransfer({ token: nft, standard: 'erc721', from: peer, to: account, value: 7n, blockNumber: 102n });
  chain.addTransfer({ token: nft, standard: 'erc721', from: account, to: peer, value: 7n, blockNumber: 104n });
  const s = await stack(chain);
  try {
    await s.tail.tick();
    expect((await ready(s)).nftItems[0]).toMatchObject({ heldBySeenLogs: false, unfinalized: true });

    chain.reorg(1, [{ token, from: account, to: peer, value: 1n, blockNumber: 104n }]);
    await s.tail.tick();
    let result = await ready(s);
    expect(result.holdings.find((h) => h.token === token)).toMatchObject({ sentCount: 1, receivedCount: 3, firstBlock: 101n, lastBlock: 104n });
    expect(result.nftItems[0]).toMatchObject({ heldBySeenLogs: true, lastBlock: 102n });

    chain.reorg(3, [
      { token, from: account, to: peer, value: 1n, blockNumber: 103n },
      { token: nft, standard: 'erc721', from: account, to: peer, value: 7n, blockNumber: 103n },
    ]);
    await s.tail.tick();
    result = await ready(s);
    expect(result.holdings.find((h) => h.token === token)).toMatchObject({ sentCount: 1, receivedCount: 1, firstBlock: 101n, lastBlock: 103n });
    expect(result.nftItems[0]).toMatchObject({ heldBySeenLogs: false, lastBlock: 103n, unfinalized: true });
    expect(s.tail.status()).toMatchObject({ reorgs: 2, lastReorgDepth: 3n, healthy: true });
  } finally {
    await s.close();
  }
});

test('a reorg to a shorter chain rewinds and re-ingests while a stale lower head is ignored', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 103n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 103n });
  const s = await stack(chain);
  try {
    await s.tail.tick();
    chain.latest = 102n;
    await s.tail.tick();
    expect((await s.store.getTailTip('ethereum'))?.number).toBe(103n); // same stored hash: lagging node

    chain.latest = 103n;
    chain.reorg(2, [{ token, from: account, to: peer, value: 1n, blockNumber: 102n }]);
    chain.latest = 102n;
    await s.tail.tick();
    expect(await s.store.getTailBlocks('ethereum')).toHaveLength(2);
    expect((await ready(s)).holdings[0]).toMatchObject({ sentCount: 1, receivedCount: 1, lastBlock: 102n });
  } finally {
    await s.close();
  }
});

test('deep reorgs wipe and degrade to finalized, then rebuild on the next tick', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 104n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 104n });
  const s = await stack(chain, 2n);
  try {
    await s.tail.tick();
    chain.reorg(3, [{ token, from: account, to: peer, value: 1n, blockNumber: 104n }]);
    await s.tail.tick();
    expect(await s.store.getTailBlocks('ethereum')).toEqual([]);
    expect(s.tail.status()).toMatchObject({ healthy: false, reorgs: 1 });
    const degraded = await ready(s);
    expect(degraded).toMatchObject({ finality: 'finalized', blockHeight: 100n });
    expect(degraded.tailError).toContain('no common ancestor');

    await s.tail.tick();
    expect(s.tail.status().healthy).toBe(true);
    expect(await ready(s)).toMatchObject({ finality: 'latest', blockHeight: 104n });
  } finally {
    await s.close();
  }
});

test('a reorg whose only common ancestor is the finalized cursor also degrades before rebuilding', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  const s = await stack(chain);
  try {
    await s.tail.tick();
    chain.reorg(2, [{ token, from: account, to: peer, value: 1n, blockNumber: 102n }]);
    await s.tail.tick();
    expect(s.tail.status()).toMatchObject({ healthy: false, lastReorgDepth: 2n });
    expect((await ready(s)).finality).toBe('finalized');
    await s.tail.tick();
    expect((await ready(s)).finality).toBe('latest');
  } finally {
    await s.close();
  }
});

test('finalization prunes matching rows without double count and wipes a mismatched overlap', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 103n;
  for (let blockNumber = 101n; blockNumber <= 103n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }
  const s = await stack(chain);
  try {
    await s.store.trackAccount('ethereum', account, 1);
    await s.tail.tick();
    const aggregate = emptyAggregate();
    for (const event of chain.transfers.filter((event) => event.blockNumber <= 102n)) addTransfer(aggregate, account, event);
    await s.store.applyFollowerRange('ethereum', { fromBlock: 101n, toBlock: 102n }, blockRef(102n), 2,
      new Map<Address, ChunkAggregate>([[account, aggregate]]), new Set([account]));
    expect((await s.store.getTailBlocks('ethereum')).map((block) => block.number)).toEqual([103n]);
    const view = await s.store.readAccount('ethereum', account);
    expect(view?.holdings[0].receivedCount).toBe(2);
    expect(view?.tailTransfers).toHaveLength(1);

    const fork: BlockHeader = { number: 104n, hash: '0xdead' as Hex, parentHash: blockHash(103n), timestamp: 1_248n };
    await s.store.appendTailBlocks('ethereum', blockHash(103n), [{ header: fork, transfers: [] }]);
    await s.store.applyFollowerRange('ethereum', { fromBlock: 103n, toBlock: 104n }, blockRef(104n), 3,
      new Map(), new Set([account]));
    expect(await s.store.getTailBlocks('ethereum')).toEqual([]);
  } finally {
    await s.close();
  }
});

test('an account first tracked mid-tail is complete and stays correct after a reorg', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  const s = await stack(chain);
  try {
    await s.tail.tick(); // no tracked account existed while ingesting
    expect((await ready(s)).holdings[0]).toMatchObject({ receivedCount: 1 });
    chain.reorg(1, [{ token, from: account, to: peer, value: 1n, blockNumber: 102n }]);
    await s.tail.tick();
    expect((await ready(s)).holdings[0]).toMatchObject({ sentCount: 1, receivedCount: 1 });
  } finally {
    await s.close();
  }
});

test('range hash mismatches are rejected, unknown block is retried next tick, and removed logs are ignored', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  chain.mismatchRangeLogHash = true;
  const s = await stack(chain);
  try {
    await s.tail.tick();
    expect(await s.store.getTailTip('ethereum')).toBeUndefined();
    expect(s.tail.status().lastError).toContain('does not match header');
    chain.mismatchRangeLogHash = false;
    await s.tail.tick();
    expect((await s.store.getTailTip('ethereum'))?.number).toBe(102n);

    chain.latest = 103n;
    const removed = chain.addRemovedTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 103n });
    chain.unknownBlockOnce.add(removed.blockHash);
    await s.tail.tick();
    expect((await s.store.getTailTip('ethereum'))?.number).toBe(102n);
    await s.tail.tick();
    expect((await s.store.getTailTip('ethereum'))?.number).toBe(103n);
    expect((await ready(s)).holdings[0].receivedCount).toBe(1);
  } finally {
    await s.close();
  }
});

test('restart re-validates a stored tip through the normal reorg path', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
  const s = await stack(chain);
  try {
    await s.tail.tick();
    chain.reorg(1, [{ token, from: account, to: peer, value: 1n, blockNumber: 102n }]);
    const restarted = createTail({ adapter: s.adapter, store: s.store, follower: s.follower });
    await restarted.load();
    await restarted.tick();
    expect(restarted.status()).toMatchObject({ reorgs: 1, healthy: true });
    expect((await ready({ ...s, tail: restarted, discovery: createDiscoveryService({
      adapters: { ethereum: s.adapter }, store: s.store, followers: { ethereum: s.follower }, tails: { ethereum: restarted },
    }) })).holdings[0]).toMatchObject({ sentCount: 1, receivedCount: 0 });
  } finally {
    await s.close();
  }
});

test('a tail tip older than maxTailTipAgeMs is served as finalized with tailError', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
  const s = await stack(chain);
  try {
    await s.tail.tick();
    const tipMs = 102 * 12 * 1000; // FakeChain timestamps are number * 12 s
    const at = (nowMs: number) => ready({ ...s, discovery: createDiscoveryService({
      adapters: { ethereum: s.adapter }, store: s.store, followers: { ethereum: s.follower }, tails: { ethereum: s.tail },
      maxTailTipAgeMs: 60_000, now: () => nowMs,
    }) });
    const old = await at(tipMs + 120_000);
    expect(old).toMatchObject({ finality: 'finalized', blockHeight: 100n });
    expect(old.tailError).toMatch(/Tail tip is \d+ s old/);
    const fresh = await at(tipMs + 30_000);
    expect(fresh).toMatchObject({ finality: 'latest', blockHeight: 102n });
    expect(fresh.tailError).toBeUndefined();
  } finally {
    await s.close();
  }
});

test('lastProgressAt is the tip time when the serving process never advanced the tail itself', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  const s = await stack(chain);
  try {
    await s.tail.tick();
    const tip = (await s.store.getTailTip('ethereum'))!;
    const nonLeader = createTail({ adapter: s.adapter, store: s.store, follower: s.follower });
    const result = await ready({ ...s, discovery: createDiscoveryService({
      adapters: { ethereum: s.adapter }, store: s.store, followers: { ethereum: s.follower }, tails: { ethereum: nonLeader },
    }) });
    expect(result).toMatchObject({ finality: 'latest', blockHeight: 102n });
    expect(result.lastProgressAt).toBe(Number(tip.timestamp) * 1000);
  } finally {
    await s.close();
  }
});

test('API finality overlays tail-only tokens and balances at the tip hash; finalized keeps the base shape', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  let balanceAt: Hex | undefined;
  chain.balanceOf = (_account, _token, at) => { balanceAt = at; return 9n; };
  const s = await stack(chain);
  const requestRpc = { limiter: createLimiter(2, 2), deadlineMs: 1_000 };
  const balances = createBalanceWorker({
    adapter: s.adapter, store: s.store, activeTtlMs: 300_000, refreshMs: 60_000, requestRpc,
    tailTip: () => s.store.getTailTip('ethereum'),
  });
  const app = createApp({
    discovery: s.discovery,
    adapters: { ethereum: s.adapter },
    balances: { ethereum: balances },
    followers: { ethereum: s.follower },
    tails: { ethereum: s.tail },
    requestRpc,
    rpcStats: () => chain.rpc.stats(),
    startedAt: 0,
  });
  try {
    await s.tail.tick();
    const latest = await app.request(`/v1/ethereum/accounts/${account}/erc20?balances=true`);
    const body = await latest.json();
    expect(body).toMatchObject({ finality: 'latest', blockHeight: '101', finalizedBlockHeight: '100' });
    expect(body.tokens[0]).toMatchObject({ contract: token, unfinalizedReceivedCount: 1, balance: '9', balanceBlockHash: blockHash(101n) });
    expect(balanceAt).toBe(blockHash(101n));

    const finalized = await (await app.request(`/v1/ethereum/accounts/${account}/erc20?finality=finalized`)).json();
    expect(finalized).toMatchObject({ finality: 'finalized', blockHeight: '100', tokens: [] });
    expect(finalized.finalizedBlockHeight).toBeUndefined();
    expect(finalized.tailError).toBeUndefined();
    const health = await (await app.request('/health')).json();
    expect(health.tails.ethereum).toMatchObject({ healthy: true, tip: { number: '101' }, lagBlocks: '0' });
  } finally {
    await balances.stop();
    await s.close();
  }
});

test('a lagging head at the cursor height is ignored, not treated as a reorg that wipes the tail', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
  const s = await stack(chain);
  try {
    await s.tail.tick();
    chain.latest = 100n;
    await s.tail.tick();
    expect((await s.store.getTailBlocks('ethereum')).map((block) => block.number)).toEqual([101n, 102n]);
    expect(s.tail.status()).toMatchObject({ reorgs: 0, healthy: true });
  } finally {
    await s.close();
  }
});

test('a header that changes mid catch-up (load-balanced nodes) is retried, not counted as a reorg', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  const s = await stack(chain);
  let flaky = true;
  const adapter = {
    ...s.adapter,
    headerByNumber: async (number: bigint) => {
      const header = await s.adapter.headerByNumber(number);
      if (number !== 102n || !flaky) return header;
      flaky = false;
      return { ...header, parentHash: '0xbad' as Hex };
    },
  };
  const tail = createTail({ adapter, store: s.store, follower: s.follower });
  try {
    await tail.tick();
    chain.latest = 104n;
    await tail.tick();
    expect(tail.status()).toMatchObject({ reorgs: 0, tipNumber: 101n });
    expect(tail.status().lastError).toContain('Header chain changed');
    await tail.tick();
    expect(tail.status()).toMatchObject({ reorgs: 0, tipNumber: 104n, lastError: undefined });
  } finally {
    await s.close();
  }
});

test('range catch-up splits a too-large getLogs and still verifies every log hash', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 104n;
  for (let blockNumber = 101n; blockNumber <= 104n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }
  let refused = false;
  chain.beforeLogs = (filter) => {
    if (!refused && filter.fromBlock < filter.toBlock) {
      refused = true;
      throw new RpcError('too_large', 'response too large');
    }
  };
  const s = await stack(chain);
  try {
    await s.tail.tick();
    expect(refused).toBe(true);
    expect(chain.calls.eth_getLogs).toBe(3);
    expect((await ready(s)).holdings[0]).toMatchObject({ receivedCount: 4, unfinalizedReceivedCount: 4 });
  } finally {
    await s.close();
  }
});

test('the tail wakes the follower at each epoch boundary, then every 4 slots until finalized moves', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  const s = await stack(chain);
  let epochs = 0;
  const tail = createTail({ adapter: s.adapter, store: s.store, follower: s.follower, onEpoch: () => { epochs += 1; } });
  try {
    await tail.load();
    await tail.tick();
    expect(epochs).toBe(0);
    chain.latest = 102n;
    await tail.tick();
    chain.latest = 101n + 32n; // fake blocks are 12 s apart, so 32 blocks always cross one epoch boundary
    await tail.tick();
    expect(epochs).toBe(1);
    const step = async (): Promise<void> => { chain.latest! += 1n; await tail.tick(); };
    for (let i = 0; i < 3; i += 1) await step();
    expect(epochs).toBe(1);
    await step(); // finalized has not moved 4 slots later: wake the follower again
    expect(epochs).toBe(2);
    chain.finalized = 101n; // the follower saw the new finalized head
    for (let i = 0; i < 8; i += 1) await step();
    expect(epochs).toBe(2);
  } finally {
    await s.close();
  }
});

test('polling skips the rest of a slot once its block is in, and retries later offsets while it is missing', () => {
  const genesis = 1_606_824_023_000;
  const slotStart = genesis + 1_000 * 12_000;
  const header = (slot: number): BlockHeader => ({ number: 1n, hash: blockHash(1n), parentHash: blockHash(0n), timestamp: BigInt((genesis + slot * 12_000) / 1_000) });
  const offsets = [2_500, 5_000, 9_000];
  expect(nextPollDelay(slotStart + 1_000, offsets, header(999))).toBe(1_500);
  expect(nextPollDelay(slotStart + 2_500, offsets, header(999))).toBe(2_500); // block late: retry at +5 s
  expect(nextPollDelay(slotStart + 9_500, offsets, header(999))).toBe(5_000); // missed slot: next slot's +2.5 s
  expect(nextPollDelay(slotStart + 2_600, offsets, header(1_000))).toBe(11_900); // have this slot's block
  expect(nextPollDelay(slotStart + 2_600, offsets)).toBe(2_400);
});

test('while the follower catches up more than an epoch behind finalized, the tail waits and wakes it', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 300n;
  const s = await stack(chain);
  let wakes = 0;
  let headNumber = 250n;
  const follower = { status: () => ({ ...s.follower.status(), headNumber }) };
  const tail = createTail({ adapter: s.adapter, store: s.store, follower, onEpoch: () => { wakes += 1; } });
  try {
    chain.calls = {};
    await tail.tick();
    expect(chain.total).toBe(0);
    expect(wakes).toBe(1);
    headNumber = 100n;
    chain.latest = 102n;
    await tail.tick();
    expect((await s.store.getTailTip('ethereum'))?.number).toBe(102n);
  } finally {
    await s.close();
  }
});
