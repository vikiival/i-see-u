import { expect, test } from 'vitest';

import type { Hex } from '../src/core/types.ts';
import { address, blockHash, FakeChain } from './fake-chain.ts';
import { stack } from './stack.ts';

const [account, peer, tokenA, tokenB, nft] = [1, 2, 3, 4, 11].map(address);
const path = `/v1/ethereum/accounts/${account}`;

type Stack = Awaited<ReturnType<typeof stack>>;

async function latestStack(chain: FakeChain, maxTipAgeMs?: number) {
  const s = await stack(chain, { tail: { maxTipAgeMs } });
  await s.follower.tick();
  await s.tail!.tick();

  return { ...s, tail: s.tail! };
}

const get = async (s: Stack, url: string) => {
  const response = await s.app.request(url);
  expect(response.status).toBe(200);

  return response.json();
};

const byToken = (body: { tokens: { contract: string }[] }, token: string) =>
  body.tokens.find((t) => t.contract === token);

test('latest overlays tail-only tokens and NFTs and reads balances at the tip hash; finalized keeps the base view', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 101n });
  chain.addTransfer({ token: nft, standard: 'erc721', from: peer, to: account, value: 7n, blockNumber: 101n });
  let balanceAt: Hex | undefined;
  chain.balanceOf = (_account, _token, at) => {
    balanceAt = at;

    return 9n;
  };

  const s = await latestStack(chain);

  try {
    const latest = await get(s, `${path}/erc20?balances=true`);
    expect(latest).toMatchObject({
      finality: 'latest',
      blockHeight: '101',
      blockHash: blockHash(101n),
      finalizedBlockHeight: '100',
      balancesCurrent: true,
    });
    expect(latest.tailError).toBeUndefined();
    expect(latest.tokens).toMatchObject([
      {
        contract: tokenA,
        receivedCount: '1',
        unfinalizedReceivedCount: '1',
        balance: '9',
        balanceBlock: '101',
        balanceBlockHash: blockHash(101n),
        balanceValidAt: '101',
      },
    ]);
    expect(balanceAt).toBe(blockHash(101n));
    const nfts = await get(s, `${path}/erc721`);
    expect(nfts).toMatchObject({
      finality: 'latest',
      collections: [{ contract: nft, tokens: [{ tokenId: '7', heldBySeenLogs: true, unfinalized: true }] }],
    });

    const finalized = await get(s, `${path}/erc20?finality=finalized`);
    expect(finalized).toMatchObject({ finality: 'finalized', blockHeight: '100', tokens: [] });
    expect(finalized.finalizedBlockHeight).toBeUndefined();
    expect(finalized.tailError).toBeUndefined();
    expect((await get(s, '/health')).tails.ethereum).toMatchObject({
      healthy: true,
      tip: { number: '101' },
      lagBlocks: '0',
      reorgs: 0,
    });
  } finally {
    await s.close();
  }
});

test('transfer history merges unfinalized tail rows newest first and pages into the finalized index', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 5n, blockNumber: 100n });
  chain.addTransfer({ token: tokenA, from: account, to: peer, value: 1n, blockNumber: 101n });
  chain.addTransfer({ token: tokenB, from: peer, to: account, value: 2n, blockNumber: 102n });
  const s = await latestStack(chain);

  try {
    const first = await get(s, `${path}/transfers?limit=2`);
    expect(first).toMatchObject({
      finality: 'latest',
      blockHeight: '102',
      finalizedBlockHeight: '100',
      throughBlock: '102',
    });
    expect(first.transfers).toMatchObject([
      { blockNumber: '102', unfinalized: true },
      { blockNumber: '101', unfinalized: true },
    ]);
    const second = await get(s, `${path}/transfers?limit=2&before=${first.next}`);
    expect(second.transfers).toMatchObject([{ blockNumber: '100', value: '5' }]);
    expect(second.transfers[0].unfinalized).toBeUndefined();
    expect(second.next).toBeNull();

    const finalized = await get(s, `${path}/transfers?finality=finalized`);
    expect(finalized).toMatchObject({
      finality: 'finalized',
      blockHeight: '100',
      throughBlock: '100',
      transfers: [{ blockNumber: '100' }],
    });
    expect(finalized.transfers).toHaveLength(1);
    // A latest cursor above the finalized cursor cannot continue in the finalized view.
    expect((await s.app.request(`${path}/transfers?finality=finalized&before=${first.next}`)).status).toBe(409);
  } finally {
    await s.close();
  }
});

test('an unhealthy or stale tail serves every endpoint finalized with tailError', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.addTransfer({ token: tokenB, from: peer, to: account, value: 1n, blockNumber: 102n });
  const s = await latestStack(chain, 60_000); // fake block times are decades old

  try {
    for (const suffix of ['erc20', 'erc721', 'transfers']) {
      const body = await get(s, `${path}/${suffix}`);
      expect(body).toMatchObject({
        finality: 'finalized',
        blockHeight: '100',
        tailError: expect.stringMatching(/Tail tip is \d+ s old/),
      });
      expect(body.finalizedBlockHeight).toBeUndefined();
    }

    expect((await get(s, `${path}/erc20`)).tokens).toMatchObject([{ contract: tokenA }]);
    expect((await get(s, `${path}/transfers`)).transfers).toHaveLength(1);
    expect((await s.app.request(`${path}/transfers?before=102:0:102:0`)).status).toBe(409);
  } finally {
    await s.close();
  }
});

test.each([false, true])(
  'history fallback uses the second snapshot metadata and readiness (pending repair: %s)',
  async (pendingRepair) => {
    const chain = new FakeChain(100n);
    chain.latest = 102n;
    chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 101n });
    const s = await latestStack(chain, 60_000);
    const transferHistory = s.store.transferHistory;
    let reads = 0;

    try {
      s.store.transferHistory = async (...args) => {
        const page = await transferHistory(...args);

        if (++reads === 1) {
          chain.finalized = 101n;
          await s.follower.tick();

          if (pendingRepair) {
            await s.store.enqueueRepair('ethereum', { fromBlock: 101n, toBlock: 101n }, 'suspect', s.clock.at);
          }
        }

        return page;
      };

      if (pendingRepair) {
        expect(await s.discovery.history('ethereum', account, 10)).toMatchObject({
          stale: true,
          repairPending: true,
          staleReason: 'Global index has pending repairs',
          blockHeight: 101n,
          page: { throughBlock: 101n, transfers: [{ blockNumber: 101n }] },
        });
        expect(reads).toBe(2);
        expect(await s.discovery.history('ethereum', account, 10, undefined, 'finalized')).toMatchObject({
          stale: true,
          repairPending: true,
        });
      } else {
        const result = await s.discovery.history('ethereum', account, 10);
        expect(reads).toBe(2);
        expect(result).toMatchObject({
          finality: 'finalized',
          blockHeight: 101n,
          blockHash: blockHash(101n),
          tailError: expect.stringMatching(/Tail tip is \d+ s old/),
          page: {
            state: { cursor: { number: 101n, hash: blockHash(101n) } },
            throughBlock: 101n,
            transfers: [{ blockNumber: 101n }],
          },
        });
        expect(result.finalizedBlockHeight).toBeUndefined();
        expect(result.finalizedBlockHash).toBeUndefined();
      }
    } finally {
      await s.close();
    }
  },
);

test('a tail Transfer above the snapshot block invalidates reuse of only that token', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.addTransfer({ token: tokenB, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.balanceOf = () => 50n;
  const s = await latestStack(chain);
  const targets: string[][] = [];
  chain.beforeMulticall = (t) => {
    targets.push(t);
  };

  try {
    await get(s, `${path}/erc20?balances=true`);
    chain.latest = 102n;
    await s.tail.tick();
    let body = await get(s, `${path}/erc20?balances=true`);
    expect(targets).toEqual([[tokenA, tokenB]]);
    expect(body).toMatchObject({ blockHeight: '102', balancesCurrent: true });
    expect(byToken(body, tokenA)).toMatchObject({
      balanceBlock: '101',
      balanceBlockHash: blockHash(101n),
      balanceValidAt: '102',
      balanceCached: true,
    });

    chain.latest = 103n;
    chain.addTransfer({ token: tokenB, from: account, to: peer, value: 1n, blockNumber: 103n });
    await s.tail.tick();
    body = await get(s, `${path}/erc20?balances=true`);
    expect(targets).toEqual([[tokenA, tokenB], [tokenB]]);
    expect(byToken(body, tokenB)).toMatchObject({
      balanceBlock: '103',
      balanceValidAt: '103',
      balanceCached: false,
      unfinalizedSentCount: '1',
    });
    expect(byToken(body, tokenA)).toMatchObject({ balanceBlock: '101', balanceValidAt: '103', balanceCached: true });
  } finally {
    await s.close();
  }
});

test('a reorg of the snapshot block makes it non-reusable even without a Transfer', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.balanceOf = (_account, _token, at) => (at === blockHash(102n) ? 9n : 4n);
  const s = await latestStack(chain);

  try {
    expect(byToken(await get(s, `${path}/erc20?balances=true`), tokenA)).toMatchObject({
      balance: '9',
      balanceBlock: '102',
    });
    chain.reorg(1, []);
    chain.latest = 103n;
    await s.tail.tick();
    expect(s.tail.status()).toMatchObject({ reorgs: 1, tipNumber: 103n });
    expect((await s.store.balances('ethereum', account))[0]).toMatchObject({ blockNumber: 102n, canonical: false });
    const body = await get(s, `${path}/erc20?balances=true`);
    expect(byToken(body, tokenA)).toMatchObject({
      balance: '4',
      balanceBlock: '103',
      balanceValidAt: '103',
      balanceCached: false,
    });
    expect(chain.multicallSizes).toEqual([1, 1]);
  } finally {
    await s.close();
  }
});

test('after a reorg to a shorter chain, a read at the new tip replaces the orphaned newer snapshot', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 103n;
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 103n });
  chain.balanceOf = (_account, _token, at) => (at === blockHash(103n) ? 9n : 4n);
  const s = await latestStack(chain);

  try {
    expect(byToken(await get(s, `${path}/erc20?balances=true`), tokenA)).toMatchObject({
      balance: '9',
      balanceBlock: '103',
      balanceBlockHash: blockHash(103n),
    });
    chain.reorg(2, [{ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 102n }]);
    chain.latest = 102n;
    await s.tail.tick();
    const tip = (await s.store.tailTip('ethereum'))!;
    expect(tip.number).toBe(102n);
    expect(byToken(await get(s, `${path}/erc20?balances=true`), tokenA)).toMatchObject({
      balance: '4',
      balanceBlock: '102',
      balanceBlockHash: tip.hash,
      balanceCached: false,
    });
    expect(await s.store.balances('ethereum', account)).toMatchObject([
      { blockNumber: 102n, blockHash: tip.hash, canonical: true },
    ]);
    expect(byToken(await get(s, `${path}/erc20?balances=true`), tokenA)).toMatchObject({ balanceCached: true });
  } finally {
    await s.close();
  }
});

test('a read whose tail block is reorged out before it is saved is served uncached, not failed', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.balanceOf = () => 9n;
  const s = await latestStack(chain);
  let reorged = false;

  chain.beforeMulticall = async () => {
    if (reorged) {
      return;
    }

    reorged = true;
    chain.reorg(1, []);
    await s.tail.tick();
  };

  try {
    const body = await get(s, `${path}/erc20?balances=true`);
    expect(byToken(body, tokenA)).toMatchObject({
      balance: '9',
      balanceBlock: '101',
      balanceBlockHash: blockHash(101n),
      balanceCached: false,
    });
    expect(await s.store.balances('ethereum', account)).toEqual([]);
  } finally {
    await s.close();
  }
});
