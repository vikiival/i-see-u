import { expect, test } from 'vitest';
import { pad } from 'viem';

import { TRANSFER_TOPIC } from '../src/chains/evm/logs.ts';
import { emptyAggregate } from '../src/core/aggregate.ts';
import type {
  AccountLogSource,
  AccountLogSourceStats,
  Address,
  BlockRange,
  ChunkAggregate,
  ChunkScan,
  Hex,
  RpcClient,
  RpcStats,
} from '../src/core/types.ts';
import { withFallback } from '../src/sources/fallback.ts';
import { createHypersyncLogSource } from '../src/sources/hypersync.ts';
import type { HypersyncFactoryResult } from '../src/sources/hypersync.ts';
import type { HypersyncPager } from '../src/sources/hypersync-pages.ts';
import { createRpcLogSource } from '../src/sources/rpc-logs.ts';
import { rpcParam, staged } from './helpers.ts';
import type { Log, Query, QueryResponse } from '@envio-dev/hypersync-client';

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

/** Answers getWithRateLimit from a script: a page, a thrown error, a 429, or (exhausted) an error. */
const pager = (values: Array<QueryResponse | Error | 'limited'>, seen: Query[] = []): HypersyncPager & { seen: Query[] } => ({
  seen,
  async getWithRateLimit(query) {
    seen.push(query);
    const value = values.shift();

    if (value instanceof Error) throw value;

    if (value === 'limited') return { rateLimit: { resetSecs: 0, remaining: 0, cost: 1000 } };

    if (!value) throw new Error('no more pages');

    return { response: value, rateLimit: { remaining: 14_000, cost: 1000 } };
  },
});

const clientFactory = (client: HypersyncPager): (() => Promise<HypersyncFactoryResult>) =>
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

  const hsClient = pager([response(logs.slice(0, 2), 12), 'limited', response(logs.slice(2), 21)]);
  const waits: number[] = [];

  const hs = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100_000n, timeoutMs: 1_000,
    clientFactory: clientFactory(hsClient), sleep: async (ms) => { waits.push(ms); },
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

  const rpcSource = createRpcLogSource(rpc, { preferredMaxRange: 3_000n });

  try {
    const range = { fromBlock: 10n, toBlock: 20n };
    const [fromHs, fromRpc] = await Promise.all([hs.scanAccount(ACCOUNT, range), rpcSource.scanAccount(ACCOUNT, range)]);
    expect(canonical(fromHs.aggregate)).toEqual(canonical(fromRpc.aggregate));
    expect(fromHs.logs).toBe(4);
    // Page two continues at nextBlock; the 429 sleeps for the quota window and repeats the same request.
    expect(hsClient.seen.map((query) => [query.fromBlock, query.toBlock])).toEqual([[10, 21], [12, 21], [12, 21]]);
    expect(waits).toEqual([1_000]);
    expect(hsClient.seen[0]).toMatchObject({ joinMode: 2 });
    expect(hsClient.seen[0].logs).toEqual([
      { include: { topics: [[TRANSFER_TOPIC], [topic(ACCOUNT)]] } },
      { include: { topics: [[TRANSFER_TOPIC], [], [topic(ACCOUNT)]] } },
    ]);
    expect(hs.stats().hypersync).toMatchObject({ requests: 3, chunks: 1, logs: 4, failures: 0 });
  } finally {
    await hs.close?.();
  }
});

test('height below the inclusive range fails the primary and serves the identical RPC fallback chunk', async () => {
  const aggregate = emptyAggregate();
  const expected = { aggregate, calls: 2, logs: 0, heavy: false };

  const hs = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100n, timeoutMs: 1_000,
    clientFactory: clientFactory(pager([response([], 21, 19)])),
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
  ['401', () => pager([new Error('HTTP 401 apiToken=VERY_SECRET')])],
  ['503', () => pager([new Error('HTTP 503')])],
  ['malformed', () => pager([staged<QueryResponse>({ ...response([], 21), data: { logs: null } })])],
  ['no progress', () => pager([response([], 10)])],
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

test('a request that never answers times out without being awaited again, and the fallback result is used', async () => {
  let calls = 0;

  const hs = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100n, timeoutMs: 10,
    clientFactory: clientFactory({
      getWithRateLimit: () => {
        calls += 1;

        return new Promise(() => {});
      },
    }),
  });

  const expected = { aggregate: emptyAggregate(), calls: 2, logs: 0, heavy: false };
  const source = withFallback(hs, fixedSource('rpc', expected), { failureThreshold: 1 });

  try {
    expect(await source.scanAccount(ACCOUNT, { fromBlock: 10n, toBlock: 20n })).toMatchObject({ aggregate: expected.aggregate });
    expect(source.stats().hypersync.lastError).toContain('timed out');
    expect(calls).toBe(1);
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

test('a mid-range failure exposes no aggregate and a retry returns the complete range', async () => {
  let fail = true;
  const transfer = log({ block: 15, index: 0, from: PEER, to: ACCOUNT });

  const source = createHypersyncLogSource({
    url: 'https://eth.hypersync.xyz', apiToken: 'token', preferredMaxRange: 100n, timeoutMs: 1_000,
    clientFactory: clientFactory({
      async getWithRateLimit(query) {
        if (fail && query.fromBlock !== 10) throw new Error('stream broke');

        return { response: fail ? response([transfer], 16, 20) : response([transfer], 21, 20), rateLimit: {} };
      },
    }),
  });

  try {
    await expect(source.scanAccount(ACCOUNT, { fromBlock: 10n, toBlock: 20n })).rejects.toThrow('stream broke');
    fail = false;
    expect((await source.scanAccount(ACCOUNT, { fromBlock: 10n, toBlock: 20n })).logs).toBe(1);
  } finally {
    await source.close?.();
  }
});
