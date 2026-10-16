import { expect, test, vi } from 'vitest';

import { createBalanceReader } from '../src/balances/reader.ts';
import { createLimiter } from '../src/core/limiter.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { address, blockHash, FakeChain, gate } from './fake-chain.ts';
import { blockRange, stack } from './stack.ts';

const [account, peer, tokenA, tokenB, other] = [1, 2, 3, 4, 5].map(address);

type Stack = Awaited<ReturnType<typeof stack>>;

type Options = NonNullable<Parameters<typeof stack>[1]>['balances'];

async function fixture(balances?: Options) {
  const chain = new FakeChain(102n);
  chain.addTransfer({ token: tokenA, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.addTransfer({ token: tokenB, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.addTransfer({ token: tokenA, from: peer, to: other, value: 1n, blockNumber: 101n });
  chain.balanceOf = () => 50n;
  const s = await stack(chain, { balances });
  await s.follower.tick();

  return s;
}

const ethCalls = (s: Stack) => s.chain.calls.eth_call ?? 0;

const read = async (s: Stack, who = account) => {
  const response = await s.app.request(`/v1/ethereum/accounts/${who}/erc20?balances=true`);
  expect(response.status).toBe(200);

  return response.json();
};

const byToken = (body: { tokens: { contract: string }[] }, token: string) => body.tokens.find((t) => t.contract === token);

/** Advance the finalized index; transfers land in the new block. */
const advance = async (s: Stack, to: bigint, transfers: { token: typeof tokenA; from: typeof account; to: typeof account }[] = []) => {
  for (const t of transfers) s.chain.addTransfer({ ...t, value: 1n, blockNumber: to });
  s.chain.finalized = to;
  await s.follower.tick();
  expect(s.follower.status()).toMatchObject({ ready: true, cursor: { number: to } });
};

test('first request reads and caches; the same cursor is then served with zero RPC', async () => {
  const s = await fixture();

  try {
    const first = await read(s);
    expect(ethCalls(s)).toBe(1);
    expect(byToken(first, tokenA)).toMatchObject({ balance: '50', balanceBlock: '102', balanceValidAt: '102', balanceCached: false });
    expect(await s.store.balances('ethereum', account)).toHaveLength(2);
    const second = await read(s);
    expect(ethCalls(s)).toBe(1);
    expect(second).toMatchObject({ balancesCurrent: true });
    expect(byToken(second, tokenB)).toMatchObject({ balance: '50', balanceBlock: '102', balanceValidAt: '102', balanceCached: true });
  } finally { await s.close(); }
});

test('an advanced cursor without a relevant transfer reuses the read, labelled with read and valid-at blocks', async () => {
  const s = await fixture();

  try {
    await read(s);
    await advance(s, 103n, [{ token: tokenA, from: peer, to: other }]);
    const body = await read(s);
    expect(ethCalls(s)).toBe(1);
    expect(body).toMatchObject({ blockHeight: '103', balancesCurrent: true });
    expect(byToken(body, tokenA)).toMatchObject({ balanceBlock: '102', balanceBlockHash: blockHash(102n), balanceValidAt: '103', balanceCached: true });
  } finally { await s.close(); }
});

test('a transfer touching (account, token) re-reads only that token', async () => {
  const s = await fixture();
  const targets: string[][] = [];
  s.chain.beforeMulticall = (t) => { targets.push(t); };

  try {
    await read(s);
    await advance(s, 103n, [{ token: tokenB, from: account, to: peer }]);
    const body = await read(s);
    expect(targets.at(-1)).toEqual([tokenB]);
    expect(byToken(body, tokenB)).toMatchObject({ balanceBlock: '103', balanceValidAt: '103', balanceCached: false });
    expect(byToken(body, tokenA)).toMatchObject({ balanceBlock: '102', balanceValidAt: '103', balanceCached: true });
  } finally { await s.close(); }
});

test('an always-refresh (rebasing) token is re-read at every new cursor, but not twice at the same one', async () => {
  const s = await fixture({ alwaysRefresh: [tokenB] });
  const targets: string[][] = [];
  s.chain.beforeMulticall = (t) => { targets.push(t); };

  try {
    await read(s);
    await advance(s, 103n);
    await read(s);
    await read(s);
    expect(targets).toEqual([[tokenA, tokenB], [tokenB]]);
  } finally { await s.close(); }
});

test('a repair revision bump invalidates snapshots of untouched accounts', async () => {
  const s = await fixture();

  try {
    await read(s);
    await s.store.rebuildRange('ethereum', blockRange(101n));
    expect(await s.store.balances('ethereum', account)).toHaveLength(2);
    const body = await read(s);
    expect(s.chain.multicallSizes).toEqual([2, 2]);
    expect(byToken(body, tokenA)).toMatchObject({ balanceCached: false, balanceValidAt: '102' });
    expect((await s.store.balances('ethereum', account))[0].revision).toBe(1n);
  } finally { await s.close(); }
});

test('100 concurrent requests for different accounts share ceil(pairs / batch) Multicalls', async () => {
  const chain = new FakeChain(102n);
  const accounts = Array.from({ length: 100 }, (_, i) => address(1000 + i));

  for (const who of accounts) for (const token of [tokenA, tokenB]) chain.addTransfer({ token, from: peer, to: who, value: 1n, blockNumber: 100n });
  chain.balanceOf = () => 9n;
  const s = await stack(chain, { balances: { batchSize: 64, windowMs: 1_000 } });

  try {
    await s.follower.tick();
    const bodies = await Promise.all(accounts.map((who) => read(s, who)));
    expect(bodies.every((body) => body.balancesCurrent === true)).toBe(true);
    expect(s.chain.calls.eth_call).toBe(Math.ceil(200 / 64));
    expect(s.chain.multicallSizes.reduce((a, b) => a + b, 0)).toBe(200);
  } finally { await s.close(); }
});

test('concurrent requests for the same account share one read', async () => {
  const s = await fixture({ windowMs: 50 });

  try {
    await Promise.all(Array.from({ length: 5 }, () => read(s)));
    expect(s.chain.multicallSizes).toEqual([2]);
  } finally { await s.close(); }
});

test('a reverted balanceOf is stored as an error at its block and is not an outage', async () => {
  const s = await fixture();
  s.chain.balanceOf = (_account, token) => token === tokenB ? undefined : 50n;

  try {
    const body = await read(s);
    expect(body).toMatchObject({ balancesCurrent: false });
    expect(byToken(body, tokenB)).toMatchObject({ balanceStatus: 'error', balanceError: 'inner call failed', balanceBlock: '102' });
    expect(byToken(body, tokenB)).not.toHaveProperty('balanceValidAt');
    expect(await s.store.balances('ethereum', account)).toContainEqual(expect.objectContaining({ token: tokenB, blockNumber: 102n, result: { status: 'error', error: 'inner call failed' } }));
    await advance(s, 103n);
    const later = await read(s);
    expect(ethCalls(s)).toBe(1);
    expect(byToken(later, tokenB)).toMatchObject({ balanceStatus: 'error', balanceBlock: '102', balanceCached: true });
    expect(byToken(later, tokenB)).not.toHaveProperty('balanceValidAt');
  } finally { await s.close(); }
});

for (const [kind, message] of [['transient', 'HTTP 503'], ['fatal', 'HTTP 401']] as const) {
  test(`an RPC outage (${message}) fails the request with 503 and caches nothing`, async () => {
    const s = await fixture();
    s.chain.beforeMulticall = () => { throw new RpcError(kind, message); };

    try {
      const response = await s.app.request(`/v1/ethereum/accounts/${account}/erc20?balances=true`);
      expect(response.status).toBe(503);
      expect(response.headers.get('retry-after')).toBe('5');
      expect(await s.store.balances('ethereum', account)).toEqual([]);
    } finally { await s.close(); }
  });
}

test('a delayed read from an older block never overwrites a newer snapshot', async () => {
  const s = await fixture();
  const pause = gate();
  let gated = false;
  let entered = false;
  s.chain.balanceOf = (_account, _token, at) => at === blockHash(102n) ? 5n : 7n;
  s.chain.beforeMulticall = async () => { if (!gated) { gated = true; entered = true; await pause.opened; } };

  try {
    const old = read(s);

    while (!entered) await new Promise((resolve) => setImmediate(resolve));
    await advance(s, 103n);
    expect(byToken(await read(s), tokenA)).toMatchObject({ balance: '7', balanceBlock: '103' });
    pause.release();
    expect(byToken(await old, tokenA)).toMatchObject({ balance: '5', balanceBlock: '102', balanceValidAt: '102' });
    expect(await s.store.balances('ethereum', account)).toEqual([
      expect.objectContaining({ blockNumber: 103n, result: { status: 'ok', balance: 7n } }),
      expect.objectContaining({ blockNumber: 103n, result: { status: 'ok', balance: 7n } }),
    ]);
  } finally { pause.release(); await s.close(); }
});

test('without balances=true no balance RPC or cache write happens', async () => {
  const s = await fixture();

  try {
    expect((await s.app.request(`/v1/ethereum/accounts/${account}/erc20`)).status).toBe(200);
    expect(ethCalls(s)).toBe(0);
    expect(await s.store.balances('ethereum', account)).toEqual([]);
  } finally { await s.close(); }
});

test('a shared balance batch stops its RPC only once every waiting request gave up', async () => {
  const s = await fixture();
  const signals: AbortSignal[] = [];

  const reader = createBalanceReader({
    adapter: {
      ...s.adapter,
      erc20BalancePairs: (_pairs, _hash, signal) => {
        if (signal) signals.push(signal);

        return new Promise((_resolve, reject) => signal?.addEventListener('abort', () => reject(signal.reason)));
      },
    },
    store: s.store,
    requestRpc: { limiter: createLimiter(2, 8), deadlineMs: 60_000 },
    batchSize: 1_000,
    windowMs: 0,
    alwaysRefresh: [],
  });

  const at = { number: 102n, hash: blockHash(102n), timestamp: 0n };
  const holdings = [{ token: tokenA, lastBlock: 100n }];
  const [a, b, c] = [new AbortController(), new AbortController(), new AbortController()];

  try {
    const first = reader.forAccount(account, holdings, at, 0n, [], a.signal);
    const second = reader.forAccount(other, holdings, at, 0n, [], b.signal);
    await vi.waitFor(() => expect(signals).toHaveLength(1));

    a.abort();
    await expect(first).rejects.toMatchObject({ statusCode: 503, message: 'Request cancelled' });
    expect(signals[0].aborted).toBe(false);

    b.abort();
    await expect(second).rejects.toMatchObject({ statusCode: 503, message: 'Request cancelled' });
    expect(signals[0].aborted).toBe(true);

    // A later request for the same pair starts a fresh read instead of joining the abandoned one.
    const third = reader.forAccount(account, holdings, at, 0n, [], c.signal);
    await vi.waitFor(() => expect(signals).toHaveLength(2));
    expect(signals[1].aborted).toBe(false);
    c.abort();
    await expect(third).rejects.toMatchObject({ statusCode: 503 });
  } finally { await s.close(); }
});
