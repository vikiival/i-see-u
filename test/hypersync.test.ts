import { expect, test } from 'vitest';
import { pad } from 'viem';

import { TRANSFER_TOPIC } from '../src/chains/evm/logs.ts';
import { emptyAggregate } from '../src/core/aggregate.ts';
import type {
  AccountLogSource,
  AccountLogSourceStats,
  Address,
  BalanceResult,
  BlockRange,
  ChainAdapter,
  ChunkAggregate,
  ChunkScan,
  FollowerStatus,
  Hex,
  OwnerResult,
  RpcClient,
  RpcStats,
} from '../src/core/types.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { withFallback } from '../src/sources/fallback.ts';
import {
  createHypersyncLogSource,
} from '../src/sources/hypersync.ts';
import type {
  HypersyncClientLike,
  HypersyncFactoryResult,
  HypersyncStream,
} from '../src/sources/hypersync.ts';
import { createRpcLogSource } from '../src/sources/rpc-logs.ts';
import { lowerAddress, rpcParam, staged } from './helpers.ts';
import { createTestStore } from './store-fixture.ts';
import type { Log, Query, QueryResponse, StreamConfig } from '@envio-dev/hypersync-client';

const ACCOUNT: Address = '0x1111111111111111111111111111111111111111';
const PEER: Address = '0x2222222222222222222222222222222222222222';
const ERC20: Address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
const ERC721: Address = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
const topic = (address: Address): Hex => pad(address, { size: 32 });
const hash = (n: number): Hex => `0x${n.toString(16).padStart(64, '0')}`;
const quantity = (n: number): Hex => `0x${n.toString(16)}`;
const rpcStats = (): RpcStats => ({ requests: 0, attempts: 0, byMethod: {}, errors: {}, endpoints: [] });

const log = (options: {
  block: number;
  index: number;
  from: Address;
  to: Address;
  token?: Address;
  tokenId?: bigint;
}): Log => ({
  address: options.token ?? ERC20,
  blockNumber: options.block,
  blockHash: hash(options.block),
  logIndex: options.index,
  transactionHash: hash(options.block * 10 + options.index),
  topics: [
    TRANSFER_TOPIC,
    topic(options.from),
    topic(options.to),
    ...(options.tokenId === undefined ? [] : [pad(quantity(Number(options.tokenId)), { size: 32 })]),
  ],
  data: options.tokenId === undefined ? pad('0x1', { size: 32 }) : '0x',
});

const response = (logs: Log[], nextBlock: number, archiveHeight = 100): QueryResponse => ({
  archiveHeight,
  nextBlock,
  totalExecutionTime: 1,
  data: { logs, blocks: [], transactions: [], traces: [] },
});

class Pages implements HypersyncStream {
  readonly values: Array<QueryResponse | Error | null>;
  closed = 0;

  constructor(values: Array<QueryResponse | Error | null>) {
    this.values = [...values];
  }

  async recv(): Promise<QueryResponse | null> {
    const value = this.values.shift() ?? null;

    if (value instanceof Error) throw value;

    return value;
  }

  async close(): Promise<void> {
    this.closed += 1;
  }
}

const clientFactory = (client: HypersyncClientLike): (() => Promise<HypersyncFactoryResult>) =>
  async () => ({ client, joinNothing: 2 });

const canonical = (aggregate: ChunkAggregate) => ({
  tokens: [...aggregate.tokens].sort().map(([token, value]) => [token, { ...value, firstBlock: value.firstBlock.toString(), lastBlock: value.lastBlock.toString() }]),
  nfts: [...aggregate.nfts].sort().map(([key, value]) => [key, { ...value, tokenId: value.tokenId.toString(), block: value.block.toString() }]),
  selfSeen: [...aggregate.selfSeen].sort(),
});

const fixedSource = (name: string, result: ChunkScan, handler?: () => Promise<ChunkScan>): AccountLogSource => {
  const state: AccountLogSourceStats = { requests: 0, chunks: 0, logs: 0, failures: 0, fallbacks: 0 };

  return {
    name,
    preferredMaxRange: 100n,
    async scanAccount() {
      state.chunks += 1;
      state.requests += 1;

      try {
        const value = handler ? await handler() : result;
        state.logs += value.logs;

        return value;
      } catch (error) {
        state.failures += 1;
        state.lastError = error instanceof Error ? error.message : String(error);
        throw error;
      }
    },
    stats: () => ({ [name]: { ...state } }),
  };
};

test('HyperSync multi-page streaming exactly matches RPC aggregation, including self-transfers and ERC-721 latest-wins', async () => {
  const logs = [
    log({ block: 10, index: 0, from: ACCOUNT, to: PEER }),
    log({ block: 11, index: 0, from: ACCOUNT, to: ACCOUNT }),
    log({ block: 12, index: 1, from: PEER, to: ACCOUNT, token: ERC721, tokenId: 7n }),
    log({ block: 13, index: 2, from: ACCOUNT, to: PEER, token: ERC721, tokenId: 7n }),
  ];

  const streams: Pages[] = [];
  let query: Query | undefined;

  const hsClient: HypersyncClientLike = {
    async stream(received) {
      query = received;
      const stream = new Pages([response(logs.slice(0, 2), 12), response(logs.slice(2), 21), null]);
      streams.push(stream);

      return stream;
    },
  };

  const hs = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100_000n, timeoutMs: 1_000,
    clientFactory: clientFactory(hsClient),
  });

  const rpc: RpcClient = {
    async request<T>(_method: string, params: unknown[]): Promise<T> {
      const filter = rpcParam<{ topics: Array<Hex | null> }>(params, 0);
      const sent = filter.topics[1] !== null;

      return staged<T>(logs.flatMap((item) => (sent ? item.topics[1] === topic(ACCOUNT) : item.topics[2] === topic(ACCOUNT))
        ? [{ ...item, blockNumber: quantity(item.blockNumber!), logIndex: quantity(item.logIndex!) }]
        : []));
    },
    stats: rpcStats,
  };

  const rpcSource = createRpcLogSource(rpc, { preferredMaxRange: 3_000n, minRange: 500n });

  try {
    const range = { fromBlock: 10n, toBlock: 20n };
    const [fromHs, fromRpc] = await Promise.all([hs.scanAccount(ACCOUNT, range), rpcSource.scanAccount(ACCOUNT, range)]);
    expect(canonical(fromHs.aggregate)).toEqual(canonical(fromRpc.aggregate));
    expect(fromHs.logs).toBe(4);
    expect(query).toMatchObject({ fromBlock: 10, toBlock: 21, joinMode: 2 });
    expect(query?.logs).toEqual([
      { include: { topics: [[TRANSFER_TOPIC], [topic(ACCOUNT)]] } },
      { include: { topics: [[TRANSFER_TOPIC], [], [topic(ACCOUNT)]] } },
    ]);
    expect(streams[0].closed).toBe(1);
    expect(hs.stats().hypersync).toMatchObject({ requests: 1, chunks: 1, logs: 4, failures: 0 });
  } finally {
    await hs.close?.();
  }
});

test('height below the inclusive range fails the primary and serves the identical RPC fallback chunk', async () => {
  const aggregate = emptyAggregate();
  const expected = { aggregate, calls: 2, logs: 0, heavy: false };

  const hs = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100n, timeoutMs: 1_000,
    clientFactory: clientFactory({ async stream() { return new Pages([response([], 21, 19)]); } }),
  });

  const fallback = fixedSource('rpc', expected);
  const source = withFallback(hs, fallback, { failureThreshold: 1, cooldownMs: 1_000 });

  try {
    const result = await source.scanAccount(ACCOUNT, { fromBlock: 10n, toBlock: 20n });
    expect(result).toMatchObject({ aggregate, calls: 2 });
    expect(source.stats()).toMatchObject({
      hypersync: { failures: 1, fallbacks: 1 },
      rpc: { chunks: 1, requests: 1 },
    });
  } finally {
    await source.close?.();
  }
});

test.each([
  ['401', () => ({ async stream() { throw new Error('HTTP 401 apiToken=VERY_SECRET'); } })],
  ['429', () => ({ async stream() { throw new Error('HTTP 429'); } })],
  ['503', () => ({ async stream() { throw new Error('HTTP 503'); } })],
  ['malformed', () => ({
    async stream() { return new Pages([staged<QueryResponse>({ ...response([], 21), data: { logs: null } })]); },
  })],
] as const)('%s primary failure falls back and redacts source stats', async (_label, makeClient) => {
  const expected = { aggregate: emptyAggregate(), calls: 2, logs: 0, heavy: false };

  const hs = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'VERY_SECRET', preferredMaxRange: 100n, timeoutMs: 1_000,
    clientFactory: clientFactory(makeClient()),
  });

  const source = withFallback(hs, fixedSource('rpc', expected), { failureThreshold: 99 });

  try {
    expect(await source.scanAccount(ACCOUNT, { fromBlock: 10n, toBlock: 20n })).toMatchObject({ aggregate: expected.aggregate });
    expect(JSON.stringify(source.stats())).not.toContain('VERY_SECRET');
    expect(source.stats().hypersync).toMatchObject({ failures: 1, fallbacks: 1 });
  } finally {
    await source.close?.();
  }
});

test('a timed-out stream is closed and the complete fallback result is used', async () => {
  let release = (_value: QueryResponse | null): void => {};

  const stream: HypersyncStream = {
    recv: () => new Promise<QueryResponse | null>((resolve) => { release = resolve; }),
    async close() { release(null); },
  };

  const hs = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100n, timeoutMs: 10,
    clientFactory: clientFactory({ async stream(_query: Query, _config: StreamConfig) { return stream; } }),
  });

  const expected = { aggregate: emptyAggregate(), calls: 2, logs: 0, heavy: false };
  const source = withFallback(hs, fixedSource('rpc', expected), { failureThreshold: 1 });

  try {
    expect(await source.scanAccount(ACCOUNT, { fromBlock: 10n, toBlock: 20n })).toMatchObject({ aggregate: expected.aggregate });
    expect(source.stats().hypersync.lastError).toContain('timed out');
  } finally {
    await source.close?.();
  }
});

test('fallback breaker skips a dead primary during cooldown and retries it afterwards', async () => {
  const result = { aggregate: emptyAggregate(), calls: 1, logs: 0, heavy: false };
  let attempts = 0;
  let clock = 1_000;

  const primary = fixedSource('primary', result, async () => {
    attempts += 1;

    if (attempts <= 2) throw new Error('down');

    return result;
  });

  const source = withFallback(primary, fixedSource('rpc', result), {
    failureThreshold: 2, cooldownMs: 500, now: () => clock,
  });

  const range: BlockRange = { fromBlock: 1n, toBlock: 1n };
  await source.scanAccount(ACCOUNT, range);
  await source.scanAccount(ACCOUNT, range);
  await source.scanAccount(ACCOUNT, range);
  expect(attempts).toBe(2);
  expect(source.stats().primary).toMatchObject({ fallbacks: 3, cooldownUntil: 1_500 });
  clock = 1_501;
  await source.scanAccount(ACCOUNT, range);
  expect(attempts).toBe(3);
  expect(source.stats().primary.cooldownUntil).toBeUndefined();
});

test('a mid-stream failure commits nothing; the next discovery resumes the same chunk', async () => {
  const transfer = log({ block: 15, index: 0, from: PEER, to: ACCOUNT });
  let fail = true;

  const hsClient: HypersyncClientLike = {
    async stream() {
      return fail
        ? new Pages([response([transfer], 16, 20), new Error('stream broke')])
        : new Pages([response([transfer], 21, 20), null]);
    },
  };

  const source = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100n, timeoutMs: 1_000,
    clientFactory: clientFactory(hsClient),
  });

  const adapter: ChainAdapter = {
    network: 'ethereum', startBlock: 10n, accountLogs: source, scanConcurrency: 1, followerMaxRange: 100n,
    normalizeAddress: (value) => /^0x[0-9a-f]{40}$/i.test(value) ? lowerAddress(value) : undefined,
    async finalizedHead() { return { block: { number: 20n, hash: hash(20), timestamp: 200n }, lastProgressAt: 1, stale: false }; },
    async latestHeader() { return { number: 20n, hash: hash(20), parentHash: hash(19), timestamp: 200n }; },
    async blockByNumber(number) { return { number, hash: hash(Number(number)), timestamp: number * 10n }; },
    async headerByNumber(number) { return { number, hash: hash(Number(number)), parentHash: hash(Number(number - 1n)), timestamp: number * 10n }; },
    async transfersInBlock() { return []; },
    async transfersInRange() { return []; },
    async scanAllTransfers() { return { calls: 0 }; },
    async erc20BalancePairs(): Promise<BalanceResult[]> { return []; },
    async erc721Owners(): Promise<OwnerResult[]> { return []; },
  };

  const { store } = await createTestStore();
  await store.initFollowerCursor('ethereum', { number: 20n, hash: hash(20), timestamp: 200n }, 1);
  const status = (): FollowerStatus => ({ ready: true, leader: true, lagBlocks: 0n, lastAdvanceAt: 1, stale: false });

  const discovery = createDiscoveryService({
    adapters: { ethereum: adapter }, store, followers: { ethereum: { status, track() {} } }, requestDeadlineMs: 1_000,
  });

  try {
    await expect(discovery.discover('ethereum', ACCOUNT)).rejects.toThrow('stream broke');
    expect(await store.getProgress('ethereum', ACCOUNT)).toBeUndefined();
    expect(await store.holdings('ethereum', ACCOUNT)).toEqual([]);
    fail = false;
    await expect(discovery.discover('ethereum', ACCOUNT)).resolves.toMatchObject({ status: 'ready' });
    expect(await store.getProgress('ethereum', ACCOUNT)).toBe(20n);
    expect(await store.holdings('ethereum', ACCOUNT)).toMatchObject([{ token: ERC20, receivedCount: 1 }]);
  } finally {
    await source.close?.();
    await store.close();
  }
});
