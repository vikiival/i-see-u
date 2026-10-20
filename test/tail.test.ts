import { expect, test } from 'vitest';

import type { BlockHeader, Tail } from '../src/core/types.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import type { DiscoveryDependencies } from '../src/discovery/service.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { createTail, nextPollDelay } from '../src/tail/tail.ts';
import { address, blockHash, blockRef, FakeChain } from './fake-chain.ts';
import { stack } from './stack.ts';

const [account, peer, token, nft] = [1, 2, 10, 11].map(address);

type Stack = Awaited<ReturnType<typeof stack>>;

/** Finalized index at chain.finalized (100), tail wired into discovery and the API. */
async function tailStack(chain: FakeChain, tail: NonNullable<Parameters<typeof stack>[1]>['tail'] = {}) {
  const s = await stack(chain, { tail });
  await s.follower.tick();
  expect(s.follower.status()).toMatchObject({ ready: true, leader: true });

  return { ...s, tail: s.tail! };
}

/** A discovery service over another tail instance or clock. */
const discoveryFor = (s: Stack, tail: Tail, extra: Partial<DiscoveryDependencies> = {}) =>
  createDiscoveryService({
    adapters: { ethereum: s.adapter },
    store: s.store,
    tails: { ethereum: tail },
    maxHeadAgeMs: 120_000,
    maxNoAdvanceMs: 1_800_000,
    now: () => s.clock.at,
    ...extra,
  });

const ready = (s: Pick<Stack, 'discovery'>, finality?: 'latest' | 'finalized') =>
  s.discovery.discover('ethereum', account, { finality });

test('follows the tip block by block with one header and one block-hash getLogs call per block', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  const s = await tailStack(chain);

  try {
    chain.calls = {};
    await s.tail.tick();
    expect(chain.calls).toEqual({ eth_getBlockByNumber: 1, eth_getLogs: 1 });
    expect(await s.store.tailTip('ethereum')).toMatchObject({ number: 101n, hash: blockHash(101n) });

    chain.latest = 102n;
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
    chain.calls = {};
    await s.tail.tick();
    expect(chain.calls).toEqual({ eth_getBlockByNumber: 1, eth_getLogs: 1 });
    expect(await ready(s)).toMatchObject({
      finality: 'latest',
      blockHeight: 102n,
      finalizedBlockHeight: 100n,
      finalizedBlockHash: blockHash(100n),
    });
    expect((await ready(s)).holdings[0]).toMatchObject({
      receivedCount: 2n,
      unfinalizedReceivedCount: 2n,
      lastBlock: 102n,
    });
  } finally {
    await s.close();
  }
});

test('depth-1 and depth-3 reorgs delete old transfers and recompute counts, bounds and NFT ownership', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 104n;

  for (let blockNumber = 101n; blockNumber <= 104n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }

  chain.addTransfer({ token: nft, standard: 'erc721', from: peer, to: account, value: 7n, blockNumber: 102n });
  chain.addTransfer({ token: nft, standard: 'erc721', from: account, to: peer, value: 7n, blockNumber: 104n });
  const s = await tailStack(chain);

  try {
    await s.tail.tick();
    expect((await ready(s)).nftItems).toEqual([]);

    chain.reorg(1, [{ token, from: account, to: peer, value: 1n, blockNumber: 104n }]);
    await s.tail.tick();
    let result = await ready(s);
    expect(result.holdings.find((h) => h.token === token)).toMatchObject({
      sentCount: 1n,
      receivedCount: 3n,
      firstBlock: 101n,
      lastBlock: 104n,
    });
    expect(result.nftItems).toMatchObject([{ tokenId: 7n, heldBySeenLogs: true, lastBlock: 102n, unfinalized: true }]);

    chain.reorg(3, [
      { token, from: account, to: peer, value: 1n, blockNumber: 103n },
      { token: nft, standard: 'erc721', from: account, to: peer, value: 7n, blockNumber: 103n },
    ]);
    await s.tail.tick();
    result = await ready(s);
    expect(result.holdings.find((h) => h.token === token)).toMatchObject({
      sentCount: 1n,
      receivedCount: 1n,
      firstBlock: 101n,
      lastBlock: 103n,
    });
    expect(result.nftItems).toEqual([]);
    expect(s.tail.status()).toMatchObject({ reorgs: 2, lastReorgDepth: 3n, healthy: true });
  } finally {
    await s.close();
  }
});

test('a reorg to a shorter chain rewinds and re-ingests while a stale lower head is ignored', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 103n;

  for (let blockNumber = 101n; blockNumber <= 103n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }

  const s = await tailStack(chain);

  try {
    await s.tail.tick();
    chain.latest = 102n;
    await s.tail.tick();
    expect((await s.store.tailTip('ethereum'))?.number).toBe(103n); // same stored hash: lagging node

    chain.latest = 103n;
    chain.reorg(2, [{ token, from: account, to: peer, value: 1n, blockNumber: 102n }]);
    chain.latest = 102n;
    await s.tail.tick();
    expect(await s.store.tailBlocks('ethereum')).toHaveLength(2);
    expect((await ready(s)).holdings[0]).toMatchObject({ sentCount: 1n, receivedCount: 1n, lastBlock: 102n });
  } finally {
    await s.close();
  }
});

test('deep reorgs wipe and degrade to finalized with tailError, then rebuild on the next tick', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 104n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 104n });
  const s = await tailStack(chain, { maxDepth: 2n });

  try {
    await s.tail.tick();
    chain.reorg(3, [{ token, from: account, to: peer, value: 1n, blockNumber: 104n }]);
    await s.tail.tick();
    expect(await s.store.tailBlocks('ethereum')).toEqual([]);
    expect(s.tail.status()).toMatchObject({ healthy: false, reorgs: 1 });
    const degraded = await ready(s);
    expect(degraded).toMatchObject({ finality: 'finalized', blockHeight: 100n, holdings: [] });
    expect(degraded.tailError).toContain('no common ancestor');

    await s.tail.tick();
    expect(s.tail.status().healthy).toBe(true);
    expect(await ready(s)).toMatchObject({ finality: 'latest', blockHeight: 104n, holdings: [{ sentCount: 1n }] });
  } finally {
    await s.close();
  }
});

test('a reorg whose only common ancestor is the finalized cursor also degrades before rebuilding', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  const s = await tailStack(chain);

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

test('finalization prunes matching rows in the cursor transaction without double count and wipes a mismatched overlap', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 103n;

  for (let blockNumber = 101n; blockNumber <= 103n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }

  const s = await tailStack(chain);

  try {
    await s.tail.tick();
    chain.finalized = 102n;
    await s.follower.tick();
    expect((await s.store.tailBlocks('ethereum')).map((block) => block.number)).toEqual([103n]);
    expect((await ready(s)).holdings[0]).toMatchObject({ receivedCount: 3n, unfinalizedReceivedCount: 1n });
    expect((await ready(s, 'finalized')).holdings[0]).toMatchObject({ receivedCount: 2n });

    const fork: BlockHeader = { number: 104n, hash: blockHash(9_999n), parentHash: blockHash(103n), timestamp: 1_248n };
    await s.store.appendTailBlocks('ethereum', blockHash(103n), [{ header: fork, transfers: [] }]);
    chain.finalized = 104n;
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, cursor: { number: 104n } });
    expect(await s.store.tailBlocks('ethereum')).toEqual([]);
  } finally {
    await s.close();
  }
});

test('range hash mismatches are rejected, unknown block is retried next tick, and removed logs are ignored', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 101n });
  const s = await tailStack(chain);

  try {
    chain.mismatchRangeLogHash = true;
    await s.tail.tick();
    expect(await s.store.tailTip('ethereum')).toBeUndefined();
    expect(s.tail.status().lastError).toContain('does not match header');
    chain.mismatchRangeLogHash = false;
    await s.tail.tick();
    expect((await s.store.tailTip('ethereum'))?.number).toBe(102n);

    chain.latest = 103n;
    const removed = chain.addRemovedTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 103n });
    chain.unknownBlockOnce.add(removed.blockHash);
    await s.tail.tick();
    expect((await s.store.tailTip('ethereum'))?.number).toBe(102n);
    await s.tail.tick();
    expect((await s.store.tailTip('ethereum'))?.number).toBe(103n);
    expect((await ready(s)).holdings[0].receivedCount).toBe(1n);
  } finally {
    await s.close();
  }
});

test('restart re-validates a stored tip through the normal reorg path', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
  const s = await tailStack(chain);

  try {
    await s.tail.tick();
    chain.reorg(1, [{ token, from: account, to: peer, value: 1n, blockNumber: 102n }]);
    const restarted = createTail({ adapter: s.adapter, store: s.store, follower: s.follower });
    await restarted.load();
    await restarted.tick();
    expect(restarted.status()).toMatchObject({ reorgs: 1, healthy: true });
    expect((await ready({ discovery: discoveryFor(s, restarted) })).holdings[0]).toMatchObject({
      sentCount: 1n,
      receivedCount: 0n,
    });
  } finally {
    await s.close();
  }
});

test('a tail tip older than maxTailTipAgeMs is served as finalized with tailError', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
  const s = await tailStack(chain);

  try {
    await s.tail.tick();
    const tipMs = 102 * 12 * 1000; // FakeChain timestamps are number * 12 s

    const at = (nowMs: number) =>
      ready({ discovery: discoveryFor(s, s.tail, { maxTailTipAgeMs: 60_000, now: () => nowMs }) });

    const old = await at(tipMs + 120_000);
    expect(old).toMatchObject({ finality: 'finalized', blockHeight: 100n, holdings: [] });
    expect(old.tailError).toMatch(/Tail tip is \d+ s old/);
    const fresh = await at(tipMs + 30_000);
    expect(fresh).toMatchObject({ finality: 'latest', blockHeight: 102n });
    expect(fresh.tailError).toBeUndefined();
  } finally {
    await s.close();
  }
});

test('lastProgressAt is the shared tip time, also in a process that loaded but never advanced the tail', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  const s = await tailStack(chain);

  try {
    await s.tail.tick();
    const tip = (await s.store.tailTip('ethereum'))!;

    const nonLeader = createTail({
      adapter: s.adapter,
      store: s.store,
      follower: s.follower,
      now: () => 1_900_000_000_000,
    });

    await nonLeader.load(); // loaded like every server process, never ticked
    const result = await ready({ discovery: discoveryFor(s, nonLeader) });
    expect(result).toMatchObject({
      finality: 'latest',
      blockHeight: 102n,
      lastProgressAt: Number(tip.timestamp) * 1000,
    });
    expect(await ready(s)).toMatchObject({ lastProgressAt: Number(tip.timestamp) * 1000 }); // the leader reports the same value
  } finally {
    await s.close();
  }
});

test('the tail stops appending maxAheadBlocks above the cursor and degrades to finalized', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 150n;
  const s = await tailStack(chain);

  try {
    let wakes = 0;

    const bounded = createTail({
      adapter: s.adapter,
      store: s.store,
      follower: s.follower,
      maxAheadBlocks: 4n,
      onEpoch: () => {
        wakes += 1;
      },
    });

    for (let i = 0; i < 3; i += 1) {
      await bounded.tick();
    }

    expect((await s.store.tailTip('ethereum'))!.number - chain.finalized).toBeLessThanOrEqual(4n);
    expect(bounded.status()).toMatchObject({ healthy: false, tailError: expect.stringMatching(/finality/) });
    expect(wakes).toBeGreaterThan(0);
    expect(await ready({ discovery: discoveryFor(s, bounded) })).toMatchObject({
      finality: 'finalized',
      blockHeight: 100n,
      tailError: expect.stringMatching(/finality/),
    });
  } finally {
    await s.close();
  }
});

test('a lagging head at the cursor height is ignored, not treated as a reorg that wipes the tail', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 102n });
  const s = await tailStack(chain);

  try {
    await s.tail.tick();
    chain.latest = 100n;
    await s.tail.tick();
    expect((await s.store.tailBlocks('ethereum')).map((block) => block.number)).toEqual([101n, 102n]);
    expect(s.tail.status()).toMatchObject({ reorgs: 0, healthy: true });
  } finally {
    await s.close();
  }
});

test('a header that changes mid catch-up (load-balanced nodes) is retried, not counted as a reorg', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  const s = await tailStack(chain);
  let flaky = true;

  const adapter = {
    ...s.adapter,
    headerByNumber: async (number: bigint): Promise<BlockHeader> => {
      const header = await s.adapter.headerByNumber(number);

      if (number !== 102n || !flaky) {
        return header;
      }

      flaky = false;

      return { ...header, parentHash: blockHash(8_888n) };
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

  const s = await tailStack(chain);
  let refused = false;

  chain.beforeLogs = (filter) => {
    if (!refused && filter.fromBlock < filter.toBlock) {
      refused = true;
      throw new RpcError('too_large', 'response too large');
    }
  };

  try {
    chain.calls = {};
    await s.tail.tick();
    expect(refused).toBe(true);
    expect(chain.calls.eth_getLogs).toBe(3);
    expect((await ready(s)).holdings[0]).toMatchObject({ receivedCount: 4n, unfinalizedReceivedCount: 4n });
  } finally {
    await s.close();
  }
});

test('the tail wakes the follower at each epoch boundary, then every 4 slots until finalized moves', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  const s = await tailStack(chain);
  let epochs = 0;

  const tail = createTail({
    adapter: s.adapter,
    store: s.store,
    follower: s.follower,
    onEpoch: () => {
      epochs += 1;
    },
  });

  try {
    await tail.load();
    await tail.tick();
    expect(epochs).toBe(0);
    chain.latest = 102n;
    await tail.tick();
    chain.latest = 101n + 32n; // fake blocks are 12 s apart, so 32 blocks always cross one epoch boundary
    await tail.tick();
    expect(epochs).toBe(1);

    const step = async (): Promise<void> => {
      chain.latest! += 1n;
      await tail.tick();
    };

    for (let i = 0; i < 3; i += 1) {
      await step();
    }

    expect(epochs).toBe(1);
    await step(); // finalized has not moved 4 slots later: wake the follower again
    expect(epochs).toBe(2);
    chain.finalized = 101n;
    await s.follower.tick(); // the follower saw the new finalized head

    for (let i = 0; i < 8; i += 1) {
      await step();
    }

    expect(epochs).toBe(2);
  } finally {
    await s.close();
  }
});

test('polling skips the rest of a slot once its block is in, and retries later offsets while it is missing', () => {
  const genesis = 1_606_824_023_000;
  const slotStart = genesis + 1_000 * 12_000;

  const header = (slot: number): BlockHeader => ({
    number: 1n,
    hash: blockHash(1n),
    parentHash: blockHash(0n),
    timestamp: BigInt((genesis + slot * 12_000) / 1_000),
  });

  const offsets = [2_500, 5_000, 9_000];
  expect(nextPollDelay(slotStart + 1_000, offsets, header(999))).toBe(1_500);
  expect(nextPollDelay(slotStart + 2_500, offsets, header(999))).toBe(2_500); // block late: retry at +5 s
  expect(nextPollDelay(slotStart + 9_500, offsets, header(999))).toBe(5_000); // missed slot: next slot's +2.5 s
  expect(nextPollDelay(slotStart + 2_600, offsets, header(1_000))).toBe(11_900); // have this slot's block
  expect(nextPollDelay(slotStart + 2_600, offsets)).toBe(2_400);
});

test('while the global index is more than an epoch behind finalized, the tail waits and wakes the follower', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 300n;
  const s = await tailStack(chain);
  let wakes = 0;

  const tail = createTail({
    adapter: s.adapter,
    store: s.store,
    follower: s.follower,
    onEpoch: () => {
      wakes += 1;
    },
  });

  try {
    await s.store.observeHead('ethereum', blockRef(250n), s.clock.at);
    chain.calls = {};
    await tail.tick();
    expect(chain.total).toBe(0);
    expect(wakes).toBe(1);
    chain.finalized = 250n;
    await s.follower.tick();
    chain.latest = 252n;
    await tail.tick();
    expect((await s.store.tailTip('ethereum'))?.number).toBe(252n);
  } finally {
    await s.close();
  }
});

test('a non-leader process never polls the chain', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  const s = await tailStack(chain);

  const tail = createTail({
    adapter: s.adapter,
    store: s.store,
    follower: { status: () => ({ ...s.follower.status(), leader: false }) },
  });

  try {
    chain.calls = {};
    await tail.tick();
    expect(chain.total).toBe(0);
    expect(await s.store.tailTip('ethereum')).toBeUndefined();
  } finally {
    await s.close();
  }
});
