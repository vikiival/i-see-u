import { expect, test } from 'vitest';

import { createApp } from '../src/api/routes.ts';
import { createLimiter } from '../src/core/limiter.ts';
import type { OwnerResult } from '../src/core/types.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { address, blockHash, FakeChain, gate } from './fake-chain.ts';
import { blockRange, headersFor, stack } from './stack.ts';

const [account, peer, token] = [1, 2, 3].map(address);
const path = `/v1/ethereum/accounts/${account}`;

async function fixture() {
  const chain = new FakeChain(102n);
  chain.addTransfer({ token, from: peer, to: account, value: 100n, blockNumber: 100n });
  chain.addTransfer({ token, from: account, to: account, value: 1n, blockNumber: 101n });
  chain.balanceOf = () => 101n;
  const s = await stack(chain);
  await s.follower.tick();

  return s;
}

test('discovery serializes exact quantities and on-request balances are pinned and cached with read time', async () => {
  const s = await fixture();

  try {
    const response = await s.app.request(`${path}/erc20?balances=true`);
    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body).toMatchObject({ status: 'ready', finality: 'finalized', blockHeight: '102', balancesCurrent: true });
    expect(body.tokens).toMatchObject([{
      contract: token, sentCount: '1', receivedCount: '2', balance: '101', balanceBlock: '102', balanceBlockHash: blockHash(102n),
      balanceValidAt: '102', balanceCached: false, balanceStatus: 'ok',
    }]);
    expect(body.tokens[0].balanceReadAt).toBeTypeOf('string');
    expect(body.scan).toBeUndefined();
    expect(await s.store.balances('ethereum', account)).toMatchObject([{ result: { status: 'ok', balance: 101n }, blockNumber: 102n, readAt: expect.any(Number) }]);
    const calls = s.chain.total;
    await s.app.request(`${path}/erc20`);
    expect(s.chain.total).toBe(calls);
  } finally { await s.close(); }
});

test('not-ready is a service-level 503, never an account 202; health uses the same readiness', async () => {
  const s = await stack();

  try {
    for (const suffix of ['erc20', 'erc721', 'transfers']) {
      const response = await s.app.request(`${path}/${suffix}`);
      expect(response.status).toBe(503);
      expect(response.headers.get('retry-after')).toBe('5');
      expect(await response.json()).toMatchObject({ status: 'not_ready' });
    }

    expect((await s.app.request('/health')).status).toBe(503);
    await s.follower.tick();
    expect((await s.app.request('/health')).status).toBe(200);
  } finally { await s.close(); }
});

test('history endpoint uses descending keysets and a self-transfer appears only once', async () => {
  const s = await fixture();

  try {
    const first = await (await s.app.request(`${path}/transfers?limit=1`)).json();
    expect(first.transfers).toMatchObject([{ blockNumber: '101', from: account, to: account }]);
    expect(first.next).toBeTypeOf('string');
    const second = await (await s.app.request(`${path}/transfers?limit=1&before=${first.next}`)).json();
    expect(second.transfers).toMatchObject([{ blockNumber: '100', value: '100' }]);
    expect(second.next).toBeNull();
    await s.store.replaceRange('ethereum', blockRange(100n), headersFor(100n), []);
    expect((await s.app.request(`${path}/transfers?before=${first.next}`)).status).toBe(409);
  } finally { await s.close(); }
});

test('invalid flags, finality, cursor, limits, address and network are rejected', async () => {
  const s = await fixture();

  try {
    for (const query of ['limit=0', 'limit=201', 'limit=1.5', 'before=no', 'before=1:0:2:0', 'before=1:0:1:9999999999']) {
      expect((await s.app.request(`${path}/transfers?${query}`)).status).toBe(400);
    }

    expect((await s.app.request(`${path}/erc20?balances=maybe`)).status).toBe(400);
    expect((await s.app.request(`${path}/erc20?finality=pending`)).status).toBe(400);
    expect((await s.app.request(`${path}/transfers?finality=safe`)).status).toBe(400);
    expect((await s.app.request('/v1/ethereum/accounts/bad/erc20')).status).toBe(400);
    expect((await s.app.request(`/v1/constructor/accounts/${account}/erc20`)).status).toBe(404);
    expect((await s.app.request('/unknown')).status).toBe(404);
  } finally { await s.close(); }
});

test('failed balance reads at the served block are not reported as current', async () => {
  const s = await fixture();
  s.chain.balanceOf = () => undefined;

  try {
    const body = await (await s.app.request(`${path}/erc20?balances=true`)).json();
    expect(body).toMatchObject({ balancesCurrent: false });
    expect(body.tokens[0]).toMatchObject({ balanceStatus: 'error', balanceBlock: '102' });
    expect(body.tokens[0].balanceValidAt).toBeUndefined();
  } finally { await s.close(); }
});

test('concurrent balance requests share one pinned Multicall', async () => {
  const s = await fixture();
  const pause = gate();
  let entered = false;
  s.chain.beforeMulticall = async () => { entered = true; await pause.opened; };

  try {
    const a = s.app.request(`${path}/erc20?balances=true`);
    const b = s.app.request(`${path}/erc20?balances=true`);

    while (!entered) await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setTimeout(resolve, 10));
    pause.release();
    expect((await a).status).toBe(200);
    expect((await b).status).toBe(200);
    expect(s.chain.calls.eth_call).toBe(1);
  } finally { pause.release(); await s.close(); }
});

test('a balance read crossing repair cannot restore an invalidated cache entry', async () => {
  const s = await fixture();
  const pause = gate();
  let entered = false;
  s.chain.beforeMulticall = async () => { entered = true; await pause.opened; };

  try {
    const response = s.app.request(`${path}/erc20?balances=true`);

    while (!entered) await new Promise((resolve) => setImmediate(resolve));
    await s.store.replaceRange('ethereum', blockRange(100n), headersFor(100n), []);
    pause.release();
    expect((await response).status).toBe(503);
    expect(await s.store.balances('ethereum', account)).toEqual([]);
  } finally { pause.release(); await s.close(); }
});

test('ERC-721 returns current ownership and optionally verifies it at the finalized hash', async () => {
  const chain = new FakeChain(102n);
  chain.addTransfer({ token, standard: 'erc721', from: peer, to: account, value: 7n, blockNumber: 100n });
  chain.addTransfer({ token, standard: 'erc721', from: account, to: peer, value: 8n, blockNumber: 101n });
  const s = await stack(chain);
  let calls = 0;

  const adapter = { ...s.adapter, erc721Owners: async () => { calls += 1;

 return [{ status: 'ok' as const, owner: account }]; } };

  const app = createApp({
    discovery: s.discovery, adapters: { ethereum: adapter }, balances: { ethereum: s.balances }, followers: { ethereum: s.follower },
    requestRpc: { limiter: createLimiter(2, 2), deadlineMs: 1000 }, rpcStats: () => chain.rpc.stats(), startedAt: 0,
  });

  try {
    await s.follower.tick();
    const response = await app.request(`${path}/erc721?verifyOwnership=true`);
    expect(response.status).toBe(200);
    expect((await response.json()).collections[0].tokens).toEqual([{ tokenId: '7', heldBySeenLogs: true, owner: account, ownedByAccount: true, ownerStatus: 'ok' }]);
    await app.request(`${path}/erc721?verifyOwnership=true`);
    expect(calls).toBe(1);
  } finally { await s.close(); }
});

test('on-request RPC deadlines release the HTTP request while bounded work finishes', async () => {
  const chain = new FakeChain(100n);
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 100n });
  const s = await stack(chain, { deadlineMs: 10 });
  const pause = gate();
  chain.beforeMulticall = () => pause.opened;

  try {
    await s.follower.tick();
    const response = await s.app.request(`${path}/erc20?balances=true`);
    expect(response.status).toBe(503);
    expect(response.headers.get('retry-after')).toBe('2');
    pause.release();
    await s.balances.forAccount(account, [{ token, lastBlock: 100n }], await s.adapter.headerByNumber(100n), 0n);
  } finally { pause.release(); await s.close(); }
});

/** An ERC-721 stack whose ownership sweep is `owners`, served by its own app. */
async function ownershipFixture(owners: () => Promise<OwnerResult[]>) {
  const chain = new FakeChain(102n);
  chain.addTransfer({ token, standard: 'erc721', from: peer, to: account, value: 7n, blockNumber: 100n });
  const s = await stack(chain);

  const app = createApp({
    discovery: s.discovery, adapters: { ethereum: { ...s.adapter, erc721Owners: owners } }, balances: { ethereum: s.balances },
    followers: { ethereum: s.follower }, requestRpc: { limiter: createLimiter(2, 2), deadlineMs: 1000 }, rpcStats: () => chain.rpc.stats(), startedAt: 0,
  });

  await s.follower.tick();

  return { s, app };
}

test('an ownership RPC failure, even a fatal one such as HTTP 401, is a 503 and is not cached', async () => {
  let calls = 0;

  const { s, app } = await ownershipFixture(async () => {
    calls += 1;

    throw new RpcError('fatal', 'HTTP 401');
  });

  try {
    const response = await app.request(`${path}/erc721?verifyOwnership=true`);
    expect(response.status).toBe(503);
    expect(response.headers.get('retry-after')).toBe('5');
    expect(await response.json()).toEqual({ error: 'Ownership RPC unavailable' });
    await app.request(`${path}/erc721?verifyOwnership=true`);
    expect(calls).toBe(2);
  } finally { await s.close(); }
});

test('a transient per-item ownership error is retried by the next request at the same block', async () => {
  const answers: OwnerResult[][] = [
    [{ status: 'error', error: 'HTTP 503', transient: true }],
    [{ status: 'ok', owner: account }],
  ];

  let calls = 0;
  const { s, app } = await ownershipFixture(async () => answers[Math.min(calls++, answers.length - 1)]);
  const tokensOf = async () => (await (await app.request(`${path}/erc721?verifyOwnership=true`)).json()).collections[0].tokens;

  try {
    expect(await tokensOf()).toEqual([expect.objectContaining({ ownerStatus: 'error' })]);
    expect(await tokensOf()).toEqual([expect.objectContaining({ ownerStatus: 'ok', owner: account })]);
    // The final answer is cached.
    await tokensOf();
    expect(calls).toBe(2);
  } finally { await s.close(); }
});
