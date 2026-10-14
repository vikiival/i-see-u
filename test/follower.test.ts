import { expect, test } from 'vitest';

import { createApp } from '../src/api/routes.ts';
import { createBalanceWorker } from '../src/balances/worker.ts';
import { createLimiter } from '../src/core/limiter.ts';
import type { Address, Store } from '../src/core/types.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { createFollower } from '../src/follower/follower.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { advisoryKey, singleConnectionLeaderLock } from '../src/store/advisory-lock.ts';
import { address, blockHash, FakeChain, gate } from './fake-chain.ts';
import { createTestStore } from './store-fixture.ts';

type AdapterOverrides = Parameters<FakeChain['adapter']>[0];

/** The full service (store, follower, balance worker, discovery, routes) over a FakeChain, with a test clock. */
async function stack(chain: FakeChain, opts: { adapter?: AdapterOverrides; deadlineMs?: number } = {}) {
  const { db, store } = await createTestStore();
  const clock = { t: 1_800_000_000_000 };
  const now = () => clock.t;
  const adapter = chain.adapter(opts.adapter);
  const follower = createFollower({
    adapter,
    store,
    lock: singleConnectionLeaderLock(db, advisoryKey('follower:ethereum')),
    pollMs: 60_000,
    maxLagBlocks: 96n,
    staleAfterMs: 1_200_000,
    maxNoAdvanceMs: 1_800_000,
    now,
  });
  const requestRpc = { limiter: createLimiter(8, 1_000), deadlineMs: opts.deadlineMs ?? 30_000 };
  const balances = createBalanceWorker({ adapter, store, activeTtlMs: 300_000, refreshMs: 60_000, requestRpc, now });
  const discovery = createDiscoveryService({
    adapters: { ethereum: adapter },
    store,
    followers: { ethereum: follower },
    requestDeadlineMs: opts.deadlineMs ?? 30_000,
    now,
  });
  const app = createApp({
    discovery,
    adapters: { ethereum: adapter },
    balances: { ethereum: balances },
    followers: { ethereum: follower },
    requestRpc,
    rpcStats: () => chain.rpc.stats(),
    startedAt: 0,
  });
  const get = async (path: string) => {
    const response = await app.request(path);
    return { status: response.status, headers: response.headers, body: await response.json() };
  };
  return {
    store, adapter, follower, balances, discovery, clock, get,
    close: async () => {
      await follower.stop();
      await balances.stop();
      await store.close();
    },
  };
}

/** Holdings and NFT items in the store equal a perfect fold of every transfer in [fromBlock, toBlock]. */
async function expectTruth(store: Store, chain: FakeChain, account: Address, fromBlock: bigint, toBlock: bigint) {
  const truth = chain.truth(account, fromBlock, toBlock);
  const holdings = await store.holdings('ethereum', account);
  expect(Object.fromEntries(holdings.map((h) => [h.token, {
    standard: h.standard, firstBlock: h.firstBlock, lastBlock: h.lastBlock, sentCount: h.sentCount, receivedCount: h.receivedCount,
  }]))).toEqual(Object.fromEntries(truth.tokens));
  const items = await store.nftItems('ethereum', account);
  expect(Object.fromEntries(items.map((i) => [`${i.token}:${i.tokenId}`, { held: i.heldBySeenLogs, block: i.lastBlock }])))
    .toEqual(Object.fromEntries([...truth.nfts].map(([key, n]) => [key, { held: n.held, block: n.block }])));
}

test('5k accounts x 10 tokens: one follower tick and one refresh cost O(ranges + pairs/1000) calls; 5k requests then cost zero', async () => {
  const chain = new FakeChain(1_000n);
  const s = await stack(chain, { adapter: { startBlock: 1_001n } });
  try {
    await s.follower.tick(); // initializes the cursor at the finalized head (1000)
    expect(s.follower.status()).toMatchObject({ ready: true, leader: true, lagBlocks: 0n });
    const accounts = Array.from({ length: 5_000 }, (_, i) => address(1_000_000 + i));
    const tokens = Array.from({ length: 10 }, (_, j) => address(500 + j));
    const each = async (fn: (account: Address) => Promise<void>) => {
      for (let i = 0; i < accounts.length; i += 250) await Promise.all(accounts.slice(i, i + 250).map(fn));
    };

    // First polls: every account is tracked (nothing to backfill: the cursor is below startBlock) and active.
    const before = chain.total;
    await each(async (account) => {
      const r = await s.get(`/v1/ethereum/accounts/${account}/erc20?balances=true`);
      expect(r.status).toBe(200);
      expect(r.body.tokens).toEqual([]);
    });
    expect(chain.total).toBe(before);

    // One finalized epoch with 50k transfers to tracked accounts plus noise.
    chain.finalized = 1_032n;
    accounts.forEach((account, i) => tokens.forEach((token) => chain.addTransfer({
      token, from: address(900), to: account, value: 5n, blockNumber: 1_001n + BigInt(i % 32),
    })));
    for (let i = 0; i < 1_000; i += 1) chain.addTransfer({ token: tokens[0], from: address(901), to: address(902), value: 1n, blockNumber: 1_010n });
    chain.balanceOf = (account, token) => BigInt(Number.parseInt(account.slice(-4), 16) + Number.parseInt(token.slice(-3), 16));

    const tickStart = { ...chain.calls };
    await s.follower.tick();
    expect(s.follower.status().cursor?.number).toBe(1_032n);
    expect((chain.calls.eth_getLogs ?? 0) - (tickStart.eth_getLogs ?? 0)).toBe(1); // one 32-block range
    expect((chain.calls.eth_getBlockByNumber ?? 0) - (tickStart.eth_getBlockByNumber ?? 0)).toBeLessThanOrEqual(2);

    expect(await s.balances.refresh()).toEqual({ pairs: 50_000, block: 1_032n });
    expect(chain.calls.eth_call).toBe(50); // 50k pairs / 1000 per aggregate3
    expect(chain.multicallSizes.every((size) => size === 1_000)).toBe(true);

    const served = chain.total;
    await each(async (account) => {
      const r = await s.get(`/v1/ethereum/accounts/${account}/erc20?balances=true`);
      expect(r.status).toBe(200);
      expect(r.body).toMatchObject({ blockHeight: '1032', blockHash: blockHash(1_032n), balancesAtBlockHeight: true, followerLagBlocks: '0' });
      expect(r.body.tokens).toHaveLength(10);
      expect(r.body.tokens[0]).toMatchObject({ receivedCount: 1, balanceStatus: 'ok', balanceBlock: '1032' });
      expect(r.body.tokens[0].balance).toBe(chain.balanceOf(account, r.body.tokens[0].contract, '0x')!.toString());
    });
    expect(chain.total).toBe(served); // zero RPC calls for 5k requests
  } finally {
    await s.close();
  }
}, 300_000);

/** A transfer history around the handoff boundary C = 150: counts, bounds and an NFT that moves at C and C+1. */
function historyFor(chain: FakeChain, account: Address) {
  const erc20 = address(31);
  const nft = address(32);
  const peer = address(33);
  for (const blockNumber of [100n, 149n, 150n, 151n, 175n, 200n]) {
    chain.addTransfer({ token: erc20, from: peer, to: account, value: 1n, blockNumber });
    chain.addTransfer({ token: erc20, from: account, to: peer, value: 1n, blockNumber });
  }
  chain.addTransfer({ token: erc20, from: account, to: account, value: 1n, blockNumber: 150n }); // self-transfer
  chain.addTransfer({ token: address(34), from: peer, to: account, value: 9n, blockNumber: 151n }); // first seen above C
  chain.addTransfer({ token: nft, from: peer, to: account, value: 7n, blockNumber: 150n, standard: 'erc721' });
  chain.addTransfer({ token: nft, from: account, to: peer, value: 7n, blockNumber: 151n, standard: 'erc721' });
  chain.addTransfer({ token: nft, from: peer, to: account, value: 8n, blockNumber: 120n, standard: 'erc721' });
  chain.addTransfer({ token: nft, from: account, to: peer, value: 8n, blockNumber: 150n, standard: 'erc721' });
  chain.addTransfer({ token: nft, from: peer, to: account, value: 8n, blockNumber: 199n, standard: 'erc721' });
}

test('handoff: the follower advances during a backfill without missing or double-counting across C', async () => {
  const chain = new FakeChain(150n);
  const account = address(77);
  historyFor(chain, account);
  const s = await stack(chain, { adapter: { logsMaxRange: 20n }, deadlineMs: 50 });
  const backfillGate = gate();
  chain.beforeLogs = (filter) => (filter.filtered ? backfillGate.opened : undefined);
  try {
    await s.follower.tick(); // cursor C = 150
    const first = await s.get(`/v1/ethereum/accounts/${account}/erc20`);
    expect(first.status).toBe(202);
    expect(first.headers.get('retry-after')).toBe('2');
    expect(first.body).toMatchObject({ status: 'syncing', progressBlocks: '0', targetBlocks: '51' });
    expect(await s.store.getTracking('ethereum', account)).toEqual({ target: 150n, progress: undefined });

    chain.finalized = 210n;
    await s.follower.tick(); // applies (150, 210] for the tracked account while its backfill is stuck
    expect(s.follower.status().cursor?.number).toBe(210n);
    expect((await s.get(`/v1/ethereum/accounts/${account}/erc20`)).status).toBe(202);

    backfillGate.release();
    let ready = await s.get(`/v1/ethereum/accounts/${account}/erc721`);
    while (ready.status === 202) ready = await s.get(`/v1/ethereum/accounts/${account}/erc721`);
    expect(ready.status).toBe(200);
    expect(ready.body.blockHeight).toBe('210');
    await expectTruth(s.store, chain, account, 100n, 210n);
    expect(ready.body.collections[0].tokens).toEqual([
      { tokenId: '8', heldBySeenLogs: true },
      { tokenId: '7', heldBySeenLogs: false },
    ]);
  } finally {
    await s.close();
  }
});

test('handoff: an account tracked while the follower is decoding a range is refetched, not missed', async () => {
  const chain = new FakeChain(150n);
  const account = address(78);
  historyFor(chain, account);
  const s = await stack(chain);
  try {
    await s.follower.tick(); // C = 150
    chain.finalized = 210n;
    const followerGate = gate();
    let unfiltered = 0;
    chain.beforeLogs = (filter) => {
      if (filter.filtered) return undefined;
      unfiltered += 1;
      return unfiltered === 1 ? followerGate.opened : undefined;
    };
    const ticking = s.follower.tick();
    while (unfiltered === 0) await new Promise((resolve) => setImmediate(resolve));

    // Tracked while the follower holds logs of (150, 210] decoded without it: target is still 150.
    const response = await s.get(`/v1/ethereum/accounts/${account}/erc20`);
    expect(response.status).toBe(200);
    expect(response.body.blockHeight).toBe('150');
    expect(await s.store.getTracking('ethereum', account)).toEqual({ target: 150n, progress: 150n });

    followerGate.release();
    await ticking;
    expect(unfiltered).toBe(2); // the commit saw an unknown tracked account and refetched the range
    expect(s.follower.status()).toMatchObject({ lastError: undefined, cursor: { number: 210n } });
    await expectTruth(s.store, chain, account, 100n, 210n);
  } finally {
    await s.close();
  }
});

test('a frozen or failing follower flags stale, then 503s past the thresholds, and recovers', async () => {
  const chain = new FakeChain(1_000n);
  const account = address(5);
  const s = await stack(chain, { adapter: { startBlock: 1_001n } });
  const erc20 = (async () => s.get(`/v1/ethereum/accounts/${account}/erc20`));
  try {
    await s.follower.tick();
    expect((await erc20()).body).toMatchObject({ stale: false, followerLagBlocks: '0' });

    // getLogs failing while finalized moves on: stale within the lag budget, 503 beyond it.
    const failing = () => { throw new RpcError('transient', 'HTTP 503'); };
    chain.beforeLogs = failing;
    chain.finalized = 1_050n;
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ stale: true, lagBlocks: 50n, lastError: 'HTTP 503' });
    expect((await erc20()).body).toMatchObject({ stale: true, blockHeight: '1000', followerLagBlocks: '50' });
    chain.finalized = 1_200n;
    await s.follower.tick();
    const behind = await erc20();
    expect(behind.status).toBe(503);
    expect(behind.headers.get('retry-after')).toBe('30');
    expect(behind.body.error).toBe('Follower is 200 blocks behind the finalized head');

    chain.beforeLogs = undefined;
    await s.follower.tick();
    expect((await erc20()).body).toMatchObject({ status: 'ready', stale: false, blockHeight: '1200', followerLagBlocks: '0' });

    // Finalized stops advancing (and the head RPC fails): stale after 20 min, 503 after 30 min.
    chain.failHead = true;
    s.clock.t += 1_200_001;
    await s.follower.tick();
    expect((await erc20()).body).toMatchObject({ status: 'ready', stale: true });
    s.clock.t += 600_000;
    expect((await erc20()).status).toBe(503);

    chain.failHead = false;
    chain.finalized = 1_232n;
    await s.follower.tick();
    expect((await erc20()).body).toMatchObject({ status: 'ready', stale: false, blockHeight: '1232' });
  } finally {
    await s.close();
  }
});

test('balances: concurrent first requests share one read; different accounts are batched by the refresh', async () => {
  const chain = new FakeChain(150n);
  const [a, b] = [address(41), address(42)];
  const tokens = [address(51), address(52), address(53)];
  for (const account of [a, b]) for (const token of tokens) chain.addTransfer({ token, from: address(99), to: account, value: 1n, blockNumber: 120n });
  chain.balanceOf = (account, token, at) => at === blockHash(150n) ? 10n : account === a && token === tokens[0] ? undefined : 20n;
  const s = await stack(chain);
  try {
    await s.follower.tick();
    await s.get(`/v1/ethereum/accounts/${a}/erc20`);
    await s.get(`/v1/ethereum/accounts/${b}/erc20`);
    chain.calls = {};

    const [r1, r2, r3] = await Promise.all([1, 2, 3].map(() => s.get(`/v1/ethereum/accounts/${a}/erc20?balances=true`)));
    expect(chain.calls).toEqual({ eth_call: 1 });
    expect(r1.body).toEqual(r2.body);
    expect(r3.body.tokens.map((t: { balance: string; balanceBlock: string }) => [t.balance, t.balanceBlock]))
      .toEqual([['10', '150'], ['10', '150'], ['10', '150']]);
    await s.get(`/v1/ethereum/accounts/${b}/erc20?balances=true`);
    expect(chain.calls).toEqual({ eth_call: 2 });

    // The follower advances: snapshots keep their own block until the refresh re-reads every pair at the new one.
    chain.finalized = 182n;
    await s.follower.tick();
    const old = await s.get(`/v1/ethereum/accounts/${a}/erc20?balances=true`);
    expect(old.body).toMatchObject({ blockHeight: '182', balancesAtBlockHeight: false });
    expect(old.body.tokens[0]).toMatchObject({ balance: '10', balanceBlock: '150' });

    chain.calls = {};
    chain.multicallSizes = [];
    expect(await s.balances.refresh()).toEqual({ pairs: 6, block: 182n });
    expect(chain.calls).toEqual({ eth_call: 1 });
    expect(chain.multicallSizes).toEqual([6]);
    const fresh = await s.get(`/v1/ethereum/accounts/${a}/erc20?balances=true`);
    expect(fresh.body.balancesAtBlockHeight).toBe(true);
    const byToken = Object.fromEntries(fresh.body.tokens.map((t: { contract: string }) => [t.contract, t]));
    expect(byToken[tokens[0]]).toMatchObject({ balanceStatus: 'error', balanceBlock: '182' });
    expect(byToken[tokens[1]]).toMatchObject({ balance: '20', balanceStatus: 'ok', balanceBlock: '182' });
    expect(chain.calls).toEqual({ eth_call: 1 });
  } finally {
    await s.close();
  }
});

test('a backfill that fails mid-way resumes from its last chunk and stays syncing until done', async () => {
  const chain = new FakeChain(160n);
  const account = address(66);
  historyFor(chain, account);
  const s = await stack(chain, { adapter: { logsMaxRange: 10n, scanConcurrency: 1 }, deadlineMs: 50 });
  let failing = true;
  const retryGate = gate();
  const requested: bigint[] = [];
  chain.beforeLogs = async (filter) => {
    if (!filter.filtered) return;
    requested.push(filter.fromBlock);
    if (filter.fromBlock === 130n) {
      if (failing) throw new RpcError('transient', 'HTTP 503');
      await retryGate.opened;
    }
  };
  try {
    await s.follower.tick();
    const failed = await s.get(`/v1/ethereum/accounts/${account}/erc20`);
    expect(failed.status).toBe(503);
    expect(await s.store.getTracking('ethereum', account)).toEqual({ target: 160n, progress: 129n });

    failing = false;
    requested.length = 0;
    const syncing = await s.get(`/v1/ethereum/accounts/${account}/erc20`);
    expect(syncing.status).toBe(202);
    expect(syncing.body).toMatchObject({ status: 'syncing', progressBlocks: '30', targetBlocks: '61' });
    expect(requested[0]).toBe(130n);

    retryGate.release();
    let done = await s.get(`/v1/ethereum/accounts/${account}/erc20`);
    while (done.status === 202) done = await s.get(`/v1/ethereum/accounts/${account}/erc20`);
    expect(done.status).toBe(200);
    expect(requested.filter((from) => from < 130n)).toEqual([]);
    await expectTruth(s.store, chain, account, 100n, 160n);
  } finally {
    await s.close();
  }
});

test('overloaded backfills get 503 with Retry-After instead of queueing without bound', async () => {
  const chain = new FakeChain(150n);
  const { store } = await createTestStore();
  const adapter = chain.adapter();
  const stuck = gate();
  chain.beforeLogs = () => stuck.opened;
  const follower = createFollower({ adapter, store, pollMs: 60_000, maxLagBlocks: 96n, staleAfterMs: 1e6, maxNoAdvanceMs: 1e7 });
  const discovery = createDiscoveryService({
    adapters: { ethereum: adapter },
    store,
    followers: { ethereum: follower },
    requestDeadlineMs: 20,
    backfillLimiter: createLimiter(1, 1),
  });
  try {
    await follower.tick();
    expect(await discovery.discover('ethereum', address(1))).toMatchObject({ status: 'syncing' });
    expect(await discovery.discover('ethereum', address(2))).toMatchObject({ status: 'syncing' }); // queued
    await expect(discovery.discover('ethereum', address(3))).rejects.toMatchObject({ statusCode: 503, retryAfterS: 5 });
    stuck.release();
  } finally {
    await follower.stop();
    await store.close();
  }
});

test('a restarted follower serves its persisted cursor after load(), before its first tick reaches the RPC', async () => {
  const chain = new FakeChain(1_000n);
  const s = await stack(chain);
  try {
    await s.follower.tick();
    const restarted = createFollower({
      adapter: s.adapter, store: s.store, pollMs: 60_000, maxLagBlocks: 96n,
      staleAfterMs: 1_200_000, maxNoAdvanceMs: 1_800_000, now: () => s.clock.t,
    });
    expect(restarted.status()).toMatchObject({ ready: false });
    const before = chain.total;
    await restarted.load();
    expect(chain.total).toBe(before);
    expect(restarted.status()).toMatchObject({ ready: true, unhealthy: undefined, cursor: { number: 1_000n } });
  } finally {
    await s.close();
  }
});

test('kick() wakes a follower with a long fallback poll, even when it arrives during a tick', async () => {
  const chain = new FakeChain(1_000n);
  const { store } = await createTestStore();
  const follower = createFollower({
    adapter: chain.adapter(), store, pollMs: 3_600_000, maxLagBlocks: 96n, staleAfterMs: 1e6, maxNoAdvanceMs: 1e7,
  });
  const until = async (done: () => boolean) => { for (let i = 0; i < 200 && !done(); i += 1) await new Promise((r) => setTimeout(r, 5)); };
  try {
    follower.start();
    await until(() => follower.status().cursor?.number === 1_000n);
    chain.finalized = 1_032n;
    follower.kick();
    await until(() => follower.status().cursor?.number === 1_032n);
    expect(follower.status().cursor?.number).toBe(1_032n);

    const stuck = gate();
    chain.beforeLogs = () => stuck.opened;
    chain.finalized = 1_064n;
    follower.kick();
    await until(() => follower.status().headNumber === 1_064n);
    chain.finalized = 1_096n;
    follower.kick(); // during the tick that is waiting on getLogs
    chain.beforeLogs = undefined;
    stuck.release();
    await until(() => follower.status().cursor?.number === 1_096n);
    expect(follower.status().cursor?.number).toBe(1_096n);
  } finally {
    await follower.stop();
    await store.close();
  }
});
