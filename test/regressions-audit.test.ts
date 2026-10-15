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

// Regression tests for three audit findings on feat/improve (follower refold, same-height balances, stale head).

/** Same wiring as test/follower.test.ts. */
async function stack(chain: FakeChain, opts: { followerStore?: (store: Store) => Store } = {}) {
  const { db, store } = await createTestStore();
  const clock = { t: 1_800_000_000_000 };
  const now = () => clock.t;
  const adapter = chain.adapter();
  const follower = createFollower({
    adapter,
    store: opts.followerStore?.(store) ?? store,
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
    await expectTruth(s.store, chain, b, 100n, 210n);
  } finally {
    await s.close();
  }
});

/** Gate the first unfiltered follower getLogs; resolves once the follower is waiting on it. */
async function holdFollowerLogs(chain: FakeChain) {
  const followerGate = gate();
  const counter = { unfiltered: 0 };
  chain.beforeLogs = (filter) => {
    if (filter.filtered) return undefined;
    counter.unfiltered += 1;
    return counter.unfiltered === 1 ? followerGate.opened : undefined;
  };
  return { followerGate, counter };
}

test('claim 1: an account tracked mid-range that both receives from and sends to a tracked account is folded fully', async () => {
  const chain = new FakeChain(150n);
  const a = address(82);
  const b = address(83);
  const token = address(38);
  chain.addTransfer({ token: address(36), from: address(33), to: a, value: 1n, blockNumber: 120n });
  chain.addTransfer({ token, from: a, to: b, value: 5n, blockNumber: 170n });
  chain.addTransfer({ token, from: b, to: a, value: 2n, blockNumber: 180n });
  chain.addTransfer({ token, from: b, to: b, value: 1n, blockNumber: 190n }); // self-transfer, skipped at scan
  const s = await stack(chain);
  try {
    await s.follower.tick();
    expect((await s.get(`/v1/ethereum/accounts/${a}/erc20`)).status).toBe(200);
    chain.finalized = 210n;
    const { followerGate, counter } = await holdFollowerLogs(chain);
    const ticking = s.follower.tick();
    while (counter.unfiltered === 0) await new Promise((resolve) => setImmediate(resolve));
    expect((await s.get(`/v1/ethereum/accounts/${b}/erc20`)).status).toBe(200);

    followerGate.release();
    await ticking;
    expect(counter.unfiltered).toBe(1);
    expect(s.follower.status()).toMatchObject({ lastError: undefined, cursor: { number: 210n } });
    await expectTruth(s.store, chain, a, 100n, 210n);
    await expectTruth(s.store, chain, b, 100n, 210n);
  } finally {
    await s.close();
  }
});

test('claim 1: accounts tracked in two different refold rounds each get every log of the range', async () => {
  const chain = new FakeChain(150n);
  const a = address(84);
  const b = address(85);
  const c = address(86);
  const token = address(39);
  chain.addTransfer({ token: address(36), from: address(33), to: a, value: 1n, blockNumber: 120n });
  chain.addTransfer({ token, from: a, to: b, value: 5n, blockNumber: 160n }); // folded for A at scan
  chain.addTransfer({ token, from: b, to: c, value: 4n, blockNumber: 170n }); // skipped, refolded in round 1
  chain.addTransfer({ token, from: c, to: a, value: 3n, blockNumber: 180n }); // folded for A at scan
  chain.addTransfer({ token, from: address(33), to: c, value: 2n, blockNumber: 190n }); // skipped until round 2
  let commits = 0;
  let trackC: (() => Promise<void>) | undefined;
  const s = await stack(chain, {
    followerStore: (store) => ({
      ...store,
      async applyFollowerRange(...args: Parameters<Store['applyFollowerRange']>) {
        const result = await store.applyFollowerRange(...args);
        commits += 1;
        if (!result.applied && trackC) {
          const track = trackC;
          trackC = undefined;
          await track(); // C is tracked between the first and the second commit attempt
        }
        return result;
      },
    }),
  });
  try {
    await s.follower.tick();
    expect((await s.get(`/v1/ethereum/accounts/${a}/erc20`)).status).toBe(200);
    chain.finalized = 210n;
    commits = 0;
    const { followerGate, counter } = await holdFollowerLogs(chain);
    const ticking = s.follower.tick();
    while (counter.unfiltered === 0) await new Promise((resolve) => setImmediate(resolve));
    expect((await s.get(`/v1/ethereum/accounts/${b}/erc20`)).status).toBe(200);
    trackC = async () => { expect((await s.get(`/v1/ethereum/accounts/${c}/erc20`)).status).toBe(200); };

    followerGate.release();
    await ticking;
    expect(counter.unfiltered).toBe(1);
    expect(commits).toBe(3); // missing [B], missing [C], applied
    expect(s.follower.status()).toMatchObject({ lastError: undefined, cursor: { number: 210n } });
    await expectTruth(s.store, chain, a, 100n, 210n);
    await expectTruth(s.store, chain, b, 100n, 210n);
    await expectTruth(s.store, chain, c, 100n, 210n);
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
    // The orphaned read (111 @ 0xddd..) must not replace the canonical snapshot (222 @ 0xeee..).
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
    // head.ts serves the cached head flagged stale; the follower must surface it.
    expect(s.follower.status().stale).toBe(true);
  } finally {
    await s.close();
  }
});
