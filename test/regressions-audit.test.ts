import { expect, test } from 'vitest';

import { createApp } from '../src/api/routes.ts';
import { createBalanceWorker } from '../src/balances/worker.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import { createLimiter } from '../src/core/limiter.ts';
import type { Address, BlockRef, Hex, Store } from '../src/core/types.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { createFollower } from '../src/follower/follower.ts';
import { advisoryKey, singleConnectionLeaderLock } from '../src/store/advisory-lock.ts';
import { address, blockRef, FakeChain, gate } from './fake-chain.ts';
import { createTestStore } from './store-fixture.ts';

// Reproductions of three audit claims against feat/improve. Each test is expected to FAIL until the bug is fixed.

/** Same wiring as test/follower.test.ts. */
async function stack(chain: FakeChain) {
  const { db, store } = await createTestStore();
  const clock = { t: 1_800_000_000_000 };
  const now = () => clock.t;
  const adapter = chain.adapter();
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
  const requestRpc = { limiter: createLimiter(8, 1_000), deadlineMs: 30_000 };
  const balances = createBalanceWorker({ adapter, store, activeTtlMs: 300_000, refreshMs: 60_000, requestRpc, now });
  const discovery = createDiscoveryService({
    adapters: { ethereum: adapter }, store, followers: { ethereum: follower }, requestDeadlineMs: 30_000, now,
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
    return { status: response.status, body: await response.json() };
  };
  return {
    store, follower, clock, get,
    close: async () => { await follower.stop(); await balances.stop(); await store.close(); },
  };
}

async function expectTruth(store: Store, chain: FakeChain, account: Address, fromBlock: bigint, toBlock: bigint) {
  const truth = chain.truth(account, fromBlock, toBlock);
  const holdings = await store.holdings('ethereum', account);
  expect(Object.fromEntries(holdings.map((h) => [h.token, {
    standard: h.standard, firstBlock: h.firstBlock, lastBlock: h.lastBlock, sentCount: h.sentCount, receivedCount: h.receivedCount,
  }]))).toEqual(Object.fromEntries(truth.tokens));
}

test('claim 1: a transfer from a tracked account to an account tracked mid-range is folded for both', async () => {
  const chain = new FakeChain(150n);
  const a = address(80);
  const b = address(81);
  const token = address(35);
  chain.addTransfer({ token: address(36), from: address(33), to: a, value: 1n, blockNumber: 120n });
  chain.addTransfer({ token, from: a, to: b, value: 5n, blockNumber: 180n }); // A -> B, above B's target
  const s = await stack(chain);
  try {
    await s.follower.tick(); // C = 150
    expect((await s.get(`/v1/ethereum/accounts/${a}/erc20`)).status).toBe(200); // A tracked before the range
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

    // B is tracked while (150, 210] is decoded with known = {A}: the A -> B log is folded for A only, not skipped.
    expect((await s.get(`/v1/ethereum/accounts/${b}/erc20`)).status).toBe(200);
    expect(await s.store.getTracking('ethereum', b)).toEqual({ target: 150n, progress: 150n });

    followerGate.release();
    await ticking;
    expect(s.follower.status()).toMatchObject({ lastError: undefined, cursor: { number: 210n } });
    await expectTruth(s.store, chain, a, 100n, 210n);
    await expectTruth(s.store, chain, b, 100n, 210n); // fails: B never gets the 180 transfer
  } finally {
    await s.close();
  }
});

test('claim 2: a delayed old-fork request-path read does not overwrite the canonical snapshot at the same height', async () => {
  const chain = new FakeChain(150n);
  const { store } = await createTestStore();
  const account = address(90);
  const token = address(37);
  const orphan: BlockRef = { number: 160n, hash: `0x${'d'.repeat(64)}` as Hex, timestamp: 1920n };
  const canonical: BlockRef = { number: 160n, hash: `0x${'e'.repeat(64)}` as Hex, timestamp: 1920n };
  chain.balanceOf = (_account, _token, at) => (at === orphan.hash ? 111n : 222n);
  try {
    await store.initFollowerCursor('ethereum', blockRef(150n), 0);
    const aggregate = emptyAggregate();
    addTransfer(aggregate, account, {
      standard: 'erc20', token, from: address(33), to: account, value: 1n,
      blockNumber: 120n, blockHash: blockRef(120n).hash, transactionHash: blockRef(1n).hash, logIndex: 0,
    });
    await store.applyScanChunk('ethereum', account, { fromBlock: 0n, toBlock: 150n }, aggregate);

    const worker = createBalanceWorker({
      adapter: chain.adapter(),
      store,
      activeTtlMs: 300_000,
      refreshMs: 60_000,
      requestRpc: { limiter: createLimiter(8, 1_000), deadlineMs: 30_000 },
      tailTip: async () => canonical,
    });
    const readGate = gate();
    let multicalls = 0;
    chain.beforeMulticall = () => (++multicalls === 1 ? readGate.opened : undefined);

    // A request served at the tail tip just before it was reorged out: first read for this token, held in flight.
    const request = worker.forAccount(account, [token], orphan);
    while (multicalls === 0) await new Promise((resolve) => setImmediate(resolve));

    // The reorg refresh reads the canonical block at the same height and commits first.
    await worker.refresh();
    expect((await store.balances('ethereum', account))[0]).toMatchObject({ blockHash: canonical.hash });

    readGate.release();
    await request;
    // fails: the orphaned read (111 @ 0xddd..) replaced the canonical snapshot (222 @ 0xeee..)
    expect((await store.balances('ethereum', account))[0]).toMatchObject({
      result: { status: 'ok', balance: 222n }, blockNumber: 160n, blockHash: canonical.hash,
    });
  } finally {
    await store.close();
  }
});

test('claim 3: a failed finalized-head poll is reported as stale', async () => {
  const chain = new FakeChain(150n);
  const s = await stack(chain);
  try {
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ stale: false, cursor: { number: 150n } });
    chain.failHead = true;
    s.clock.t += 60_000;
    await s.follower.tick();
    // fails: head.ts serves the cached head with stale: true, but the follower clears lastError and ignores it
    expect(s.follower.status().stale).toBe(true);
  } finally {
    await s.close();
  }
});
