import { expect, test, vi } from 'vitest';

import { createApp } from '../src/api/routes.ts';
import type { BalanceWorker } from '../src/balances/worker.ts';
import { createLimiter } from '../src/core/limiter.ts';
import { RpcError } from '../src/rpc/errors.ts';
import type {
  Address,
  BalanceSnapshot,
  BlockRef,
  ChainAdapter,
  DiscoveryResult,
  DiscoveryService,
  Hex,
  RpcStats,
} from '../src/core/types.ts';

const address = (n: number): Address => `0x${n.toString(16).padStart(40, '0')}` as Address;
const hash = (n: number): Hex => `0x${n.toString(16).padStart(64, '0')}` as Hex;

const account = address(1);
const erc20 = address(2);
const erc721 = address(3);

const resultFor = (): DiscoveryResult => ({
  status: 'ready',
  network: 'ethereum',
  account,
  fromBlock: 25_933_634n,
  blockHeight: 26_000_123n,
  blockHash: hash(9),
  blockTimestamp: 1_760_441_940n,
  finality: 'finalized',
  lastProgressAt: Date.parse('2026-10-14T12:00:00.000Z'),
  stale: false,
  lagBlocks: 64n,
  followerLagBlocks: 0n,
  holdings: [
    { token: erc721, standard: 'erc721', firstBlock: 25_933_650n, lastBlock: 26_000_110n, sentCount: 1, receivedCount: 2 },
    { token: erc20, standard: 'erc20', firstBlock: 25_933_640n, lastBlock: 26_000_100n, sentCount: 2, receivedCount: 4 },
  ],
  nftItems: [
    { token: erc721, tokenId: 7n, heldBySeenLogs: true, lastBlock: 26_000_110n },
    { token: erc721, tokenId: 8n, heldBySeenLogs: false, lastBlock: 26_000_105n },
  ],
  scan: { newBlocks: 456n, rpcCalls: 2, ms: 17 },
});

const stats = (): RpcStats => ({
  requests: 4,
  attempts: 5,
  byMethod: { eth_call: 2 },
  errors: {},
  endpoints: [{ url: 'https://rpc.invalid', state: 'closed', failures: 0 }],
});

const adapterWith = (overrides: Partial<ChainAdapter> = {}): ChainAdapter => ({
  network: 'ethereum',
  startBlock: 25_933_634n,
  accountLogs: {
    name: 'rpc',
    preferredMaxRange: 10_000n,
    adaptiveRange: { minRange: 500n },
    async scanAccount() { return { aggregate: { tokens: new Map(), nfts: new Map(), selfSeen: new Set() }, calls: 0, logs: 0, heavy: false }; },
    stats: () => ({ rpc: { requests: 0, chunks: 0, logs: 0, failures: 0, fallbacks: 0 } }),
  },
  scanConcurrency: 4,
  followerMaxRange: 500n,
  normalizeAddress(input) {
    return /^0x[0-9a-f]{40}$/i.test(input) ? input.toLowerCase() as Address : undefined;
  },
  async finalizedHead() {
    const now = Date.now();
    return {
      block: { number: 1n, hash: hash(1), timestamp: BigInt(Math.floor(now / 1_000)) },
      lastProgressAt: now,
      stale: false,
    };
  },
  async blockByNumber(number) { return { number, hash: hash(Number(number)), timestamp: 0n }; },
  async scanAllTransfers() { return { calls: 0 }; },
  async erc20BalancePairs(pairs) {
    return pairs.map(() => ({ status: 'ok', balance: 0n }));
  },
  async erc721Owners(items) {
    return items.map(() => ({ status: 'ok', owner: account }));
  },
  ...overrides,
});

const balancesWith = (forAccount: BalanceWorker['forAccount']): BalanceWorker => ({
  forAccount,
  refresh: async () => ({ pairs: 0 }),
  kick() {},
  start() {},
  async stop() {},
  stats: () => ({ activeAccounts: 0 }),
});

const appWith = (
  discovery: DiscoveryService,
  adapter = adapterWith(),
  balances = balancesWith(async () => []),
) => createApp({
  discovery,
  adapters: { ethereum: adapter },
  balances: { ethereum: balances },
  followers: { ethereum: { status: () => ({ ready: true, leader: true, lagBlocks: 0n, lastAdvanceAt: 0, stale: false }) } },
  requestRpc: { limiter: createLimiter(2, 2), deadlineMs: 1_000 },
  rpcStats: stats,
  startedAt: Date.now() - 2_500,
});

test('serializes bigint fields and labels each balance with the block it was read at', async () => {
  let receivedAt: BlockRef | undefined;
  let receivedAccount: Address | undefined;
  let balanceCalls = 0;
  const discovery: DiscoveryService = {
    async discover(network) {
      expect(network).toBe('ethereum');
      return resultFor();
    },
  };
  const balances = balancesWith(async (normalizedAccount, tokens, at): Promise<BalanceSnapshot[]> => {
    balanceCalls += 1;
    receivedAccount = normalizedAccount;
    receivedAt = at;
    expect(tokens).toEqual([erc20]);
    return [{ token: erc20, result: { status: 'ok', balance: 987_654_321n }, blockNumber: 26_000_091n, blockHash: hash(8) }];
  });
  const app = appWith(discovery, adapterWith(), balances);

  const response = await app.request(`/v1/ethereum/accounts/${account}/erc20?balances=true`);
  expect(response.status).toBe(200);
  expect(receivedAccount).toBe(account);
  expect(receivedAt).toEqual({ number: 26_000_123n, hash: hash(9), timestamp: 1_760_441_940n });
  const body = await response.json();
  expect(body.status).toBe('ready');
  expect(body.followerLagBlocks).toBe('0');
  expect(body.balancesAtBlockHeight).toBe(false);
  expect(body.fromBlock).toBe('25933634');
  expect(body.blockHeight).toBe('26000123');
  expect(body.finality).toBe('finalized');
  expect(body.lastProgressAt).toBe('2026-10-14T12:00:00.000Z');
  expect(body.blockTimestamp).toBe('2025-10-14T11:39:00.000Z');
  expect(body.dataAgeS).toBeTypeOf('number');
  expect(body.stale).toBe(false);
  expect(body.lagBlocks).toBe('64');
  expect(body.scan.newBlocks).toBe('456');
  expect(body.tokens).toEqual([{
    contract: erc20,
    firstSeenBlock: '25933640',
    lastSeenBlock: '26000100',
    sentCount: 2,
    receivedCount: 4,
    balance: '987654321',
    balanceStatus: 'ok',
    balanceBlock: '26000091',
  }]);

  const withoutBalances = await app.request(`/v1/ethereum/accounts/${account}/erc20?balances=false`);
  expect(withoutBalances.status).toBe(200);
  expect(balanceCalls).toBe(1);
  const plain = await withoutBalances.json();
  expect(plain.tokens[0].balanceStatus).toBeUndefined();
  expect(plain.balancesAtBlockHeight).toBeUndefined();
});

test('a syncing account answers 202 with progress; overload answers 503 with Retry-After', async () => {
  const syncing: DiscoveryService = {
    async discover() {
      return { status: 'syncing', network: 'ethereum', account, fromBlock: 100n, progressBlocks: 40n, targetBlocks: 120n, followerLagBlocks: 3n };
    },
  };
  const response = await appWith(syncing).request(`/v1/ethereum/accounts/${account}/erc721`);
  expect(response.status).toBe(202);
  expect(response.headers.get('retry-after')).toBe('2');
  expect(await response.json()).toEqual({
    status: 'syncing', network: 'ethereum', account, fromBlock: '100', progressBlocks: '40', targetBlocks: '120', followerLagBlocks: '3',
  });

  const busy: DiscoveryService = {
    async discover() { throw Object.assign(new Error('Server busy'), { statusCode: 503, retryAfterS: 7 }); },
  };
  const overloaded = await appWith(busy).request(`/v1/ethereum/accounts/${account}/erc20`);
  expect(overloaded.status).toBe(503);
  expect(overloaded.headers.get('retry-after')).toBe('7');
});

test('verifies only ERC-721 items currently held according to seen logs', async () => {
  let receivedItems: { token: Address; tokenId: bigint }[] | undefined;
  let receivedBlockHash: Hex | undefined;
  const discovery: DiscoveryService = { async discover() { return resultFor(); } };
  const adapter = adapterWith({
    async erc721Owners(items, blockHash) {
      receivedItems = items.map(({ token, tokenId }) => ({ token, tokenId }));
      receivedBlockHash = blockHash;
      return [{ status: 'ok', owner: account }];
    },
  });

  const response = await appWith(discovery, adapter)
    .request(`/v1/ethereum/accounts/${account}/erc721?verifyOwnership=true`);
  expect(response.status).toBe(200);
  expect(receivedItems).toEqual([{ token: erc721, tokenId: 7n }]);
  expect(receivedBlockHash).toBe(hash(9));
  expect((await response.json()).collections[0].tokens).toEqual([
    { tokenId: '7', heldBySeenLogs: true, owner: account, ownedByAccount: true, ownerStatus: 'ok' },
    { tokenId: '8', heldBySeenLogs: false },
  ]);
});

test('maps transient RPC failures to 503 with Retry-After', async () => {
  const discovery: DiscoveryService = {
    async discover() {
      throw new Error('viem wrapper', { cause: new RpcError('transient', 'upstream details must stay private') });
    },
  };

  const response = await appWith(discovery).request(`/v1/ethereum/accounts/${account}/erc20`);
  expect(response.status).toBe(503);
  expect(response.headers.get('retry-after')).toBe('5');
  expect(await response.json()).toEqual({ error: 'RPC temporarily unavailable' });
});

test('returns normalized JSON for unknown routes', async () => {
  const response = await appWith({ async discover() { return resultFor(); } }).request('/unknown');

  expect(response.status).toBe(404);
  expect(await response.json()).toEqual({ error: 'Not found' });
});

test('rejects an invalid boolean query before discovery', async () => {
  const discover = vi.fn<DiscoveryService['discover']>();
  const response = await appWith({ discover })
    .request(`/v1/ethereum/accounts/${account}/erc20?balances=maybe`);

  expect(response.status).toBe(400);
  expect(await response.json()).toEqual({ error: 'Invalid boolean query value for balances' });
  expect(discover).not.toHaveBeenCalled();
});

test('/health exposes per-source backfill stats', async () => {
  const response = await appWith({ async discover() { return resultFor(); } }).request('/health');
  expect(response.status).toBe(200);
  expect((await response.json()).logSources).toEqual({
    ethereum: { rpc: { requests: 0, chunks: 0, logs: 0, failures: 0, fallbacks: 0 } },
  });
});
