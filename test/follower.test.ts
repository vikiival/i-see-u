import { expect, test } from 'vitest';

import { createFollower } from '../src/follower/follower.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { address, blockHash, FakeChain, gate } from './fake-chain.ts';
import { blockRange, header, headersFor, stack } from './stack.ts';

const [account, peer, token] = [1, 2, 3].map(address);

test('follower awaits bootstrap instead of initializing a cursor at the head', async () => {
  const s = await stack(new FakeChain(102n), { initialize: false });

  try {
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({
      ready: false,
      lastError: 'Global index awaits bootstrap initialization',
    });
    expect(await s.store.indexState('ethereum')).toBeUndefined();
    expect(s.chain.total).toBe(0);
  } finally {
    await s.close();
  }
});

test('a non-leader clears its bootstrap error once the shared index exists', async () => {
  const s = await stack(new FakeChain(102n), { initialize: false });

  const follower = createFollower({
    adapter: s.adapter,
    store: s.store,
    pollMs: 1000,
    maxNoAdvanceMs: 1_800_000,
    lock: { tryAcquire: async () => false, held: () => false, release: async () => {} },
  });

  try {
    await follower.tick();
    expect(follower.status()).toMatchObject({
      ready: false,
      leader: false,
      lastError: 'Global index awaits bootstrap initialization',
    });
    await s.store.initialize('ethereum', 100n, header(99n), s.clock.at);
    await s.follower.tick();
    const calls = s.chain.total;
    await follower.tick();
    expect(follower.status()).toMatchObject({
      ready: true,
      stale: false,
      leader: false,
      lastError: undefined,
      unhealthy: undefined,
    });
    expect(s.chain.total).toBe(calls);
  } finally {
    await s.close();
  }
});

test('global ranges index all accounts before their first request; RPC cost is independent of accounts', async () => {
  const chain = new FakeChain(102n);

  for (let i = 1; i <= 1000; i += 1) {
    chain.addTransfer({ token, from: peer, to: address(1000 + i), value: 1n, blockNumber: 100n + BigInt(i % 3) });
  }

  const s = await stack(chain);

  try {
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, cursor: { number: 102n } });
    expect(chain.calls.eth_getLogs).toBe(1);
    const calls = chain.total;

    for (const who of [address(1001), address(1500), address(2000)]) {
      expect((await s.discovery.discover('ethereum', who)).holdings).toMatchObject([{ token, receivedCount: 1n }]);
    }

    expect(chain.total).toBe(calls);
    expect((await s.store.holdings('ethereum', peer))[0].sentCount).toBe(1000n);
  } finally {
    await s.close();
  }
});

test('failed ranges preserve progress and resume without repeating committed transfers', async () => {
  const chain = new FakeChain(105n);

  for (let blockNumber = 100n; blockNumber <= 105n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }

  const s = await stack(chain, { range: 2n });
  let fail = true;
  chain.beforeLogs = ({ fromBlock }) => {
    if (fail && fromBlock === 102n) {
      throw new RpcError('transient', 'unavailable');
    }
  };

  try {
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: false, cursor: { number: 101n }, lagBlocks: 4n });
    expect((await s.store.holdings('ethereum', account))[0].receivedCount).toBe(2n);
    fail = false;
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, cursor: { number: 105n } });
    expect((await s.store.holdings('ethereum', account))[0].receivedCount).toBe(6n);
    expect(chain.calls.eth_getLogs).toBe(4);
  } finally {
    await s.close();
  }
});

test('range budget bounds catch-up; service readiness stays false until the observed head is reached', async () => {
  const s = await stack(new FakeChain(105n), { range: 2n, maxRanges: 1 });

  try {
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: false, cursor: { number: 101n } });
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: false, cursor: { number: 103n } });
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, cursor: { number: 105n } });
  } finally {
    await s.close();
  }
});

test('parent hash mismatch enqueues a durable repair and never advances the cursor', async () => {
  const s = await stack();

  try {
    await s.follower.tick();
    s.chain.finalized = 103n;

    const follower = createFollower({
      adapter: { ...s.adapter, headerByNumber: async (n) => ({ ...header(n), parentHash: blockHash(900n) }) },
      store: s.store,
      pollMs: 1000,
      maxNoAdvanceMs: 1_800_000,
    });

    await follower.tick();
    expect(follower.status()).toMatchObject({
      ready: false,
      cursor: { number: 102n },
      lastError: 'Next range parent hash mismatch',
    });
    expect(await s.store.repairs('ethereum')).toMatchObject([{ fromBlock: 102n, toBlock: 102n }]);
    await follower.tick();
    expect(await s.store.repairs('ethereum')).toHaveLength(1);
  } finally {
    await s.close();
  }
});

test('same-height finalized hash disagreement is queued, then a canonical replacement restores the tip', async () => {
  const s = await stack();

  try {
    await s.follower.tick();
    s.chain.latest = s.chain.finalized;
    s.chain.reorg(1, []);
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: false, lastError: 'Finalized head hash changed' });
    const canonical = await s.adapter.headerByNumber(102n);
    await s.store.replaceRange('ethereum', blockRange(102n), [canonical], []);
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, cursor: { hash: canonical.hash, revision: 1n } });
  } finally {
    await s.close();
  }
});

test('inconsistent log/header hashes fail the complete range and retry cleanly', async () => {
  const chain = new FakeChain(102n);
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.mismatchRangeLogHash = true;
  const s = await stack(chain);

  try {
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: false, cursor: { number: 99n } });
    expect(await s.store.holdings('ethereum', account)).toEqual([]);
    chain.mismatchRangeLogHash = false;
    await s.follower.tick();
    expect(s.follower.status().ready).toBe(true);
    expect((await s.store.holdings('ethereum', account))[0].receivedCount).toBe(1n);
  } finally {
    await s.close();
  }
});

test('splits provider-limited ranges and ignores removed logs while retaining the complete header chain', async () => {
  const chain = new FakeChain(103n);
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.addRemovedTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 103n });
  chain.beforeLogs = ({ fromBlock, toBlock }) => {
    if (toBlock - fromBlock > 1n) {
      throw new RpcError('too_large', 'response too large');
    }
  };

  const s = await stack(chain);

  try {
    await s.follower.tick();
    expect(s.follower.status().ready).toBe(true);
    expect(chain.calls.eth_getLogs).toBe(3);
    expect((await s.store.holdings('ethereum', account))[0].receivedCount).toBe(1n);
  } finally {
    await s.close();
  }
});

test('read-only replicas load shared progress; a lost or failed lock clears leader status', async () => {
  const s = await stack();
  let held = true;
  let fail = false;

  const follower = createFollower({
    adapter: s.adapter,
    store: s.store,
    pollMs: 1000,
    maxNoAdvanceMs: 1_800_000,
    lock: {
      tryAcquire: async () => {
        if (fail) {
          throw new Error('lock lost');
        }

        return held;
      },
      held: () => held,
      release: async () => {},
    },
  });

  try {
    await s.follower.tick();
    const calls = s.chain.total;
    await follower.load();
    expect(s.chain.total).toBe(calls);
    expect(follower.status()).toMatchObject({ ready: true, leader: false });
    await follower.tick();
    expect(follower.status().leader).toBe(true);
    held = false;
    expect(follower.status().leader).toBe(false);
    fail = true;
    await follower.tick();
    expect(follower.status()).toMatchObject({ leader: false, lastError: 'lock lost' });
  } finally {
    await s.close();
  }
});

test('stale head observations never renew shared freshness and recover after a successful poll', async () => {
  const s = await stack();

  try {
    await s.follower.tick();
    const checkedAt = (await s.store.indexState('ethereum'))?.checkedAt;
    s.chain.failHead = true;
    s.clock.at += 60_000;
    await s.follower.tick();
    // A failed finalized-head poll is served cached with refreshFailed; the follower must surface it as stale.
    expect(s.follower.status()).toMatchObject({
      ready: false,
      stale: true,
      lastError: 'Finalized head observation is stale',
    });
    expect((await (await s.app.request('/health')).json()).followers.ethereum).toMatchObject({
      stale: true,
      lastError: 'Finalized head observation is stale',
    });
    expect((await s.store.indexState('ethereum'))?.checkedAt).toBe(checkedAt);
    s.chain.failHead = false;
    s.chain.finalized = 103n;
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, stale: false, lastError: undefined });
  } finally {
    await s.close();
  }
});

test('concurrent ticks share work and a request during ingestion sees the committed stale snapshot', async () => {
  const s = await stack();
  const pause = gate();
  let entered = false;
  s.chain.beforeLogs = async () => {
    entered = true;
    await pause.opened;
  };

  try {
    const a = s.follower.tick();
    const b = s.follower.tick();

    while (!entered) {
      await new Promise((resolve) => setImmediate(resolve));
    }

    expect(await s.discovery.discover('ethereum', account)).toMatchObject({
      status: 'ready',
      stale: true,
      blockHeight: 99n,
      holdings: [],
    });
    pause.release();
    await Promise.all([a, b]);
    expect(s.chain.calls.eth_getLogs).toBe(1);
  } finally {
    pause.release();
    await s.close();
  }
});

test('bootstrap append uses exactly the same cursor and the follower continues from it', async () => {
  const s = await stack(new FakeChain(103n));

  try {
    await s.store.observeHead('ethereum', header(103n), s.clock.at);
    await s.store.appendRange('ethereum', blockRange(100n, 101n), headersFor(100n, 101n), [], s.clock.at);
    const ranges: bigint[] = [];
    s.chain.beforeLogs = ({ fromBlock }) => {
      ranges.push(fromBlock);
    };

    await s.follower.tick();
    expect(ranges).toEqual([102n]);
    expect(s.follower.status().ready).toBe(true);
  } finally {
    await s.close();
  }
});
