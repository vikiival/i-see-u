import type { Query, QueryResponse } from '@envio-dev/hypersync-client';
import { pad } from 'viem';
import { expect, test } from 'vitest';

import {
  bloomMayHaveTransfer,
  createHypersyncBootstrapSource,
  createRpcBootstrapSource,
} from '../src/bootstrap/sources.ts';
import { decodeTransferLog, decodeTransferLogFast, TRANSFER_TOPIC } from '../src/chains/evm/logs.ts';
import type { WireLog } from '../src/chains/evm/logs.ts';
import type { Hex, TransferEvent } from '../src/core/types.ts';
import { address, blockHash, FakeChain } from './fake-chain.ts';

/** Deterministic PRNG (mulberry32) so the fuzz corpus is the same on every run. */
const random = (seed: number) => () => {
  let t = (seed += 0x6d2b79f5);
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);

  return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
};

test('the bulk decoder agrees with viem strict decoding on canonical and malformed corpus', () => {
  const next = random(42);
  const pick = <T>(values: T[]): T => values[Math.floor(next() * values.length)];

  const digits = (n: number) =>
    Array.from({ length: n }, () => '0123456789abcdefABCDEF'[Math.floor(next() * 22)]).join('');

  const word = (n: number): Hex => `0x${digits(n).padStart(64, '0')}`;
  const corpus: WireLog[] = [];

  // Each sample is a canonical log (mixed-case hex included) with at most one field broken.
  const mutations: Array<(log: WireLog & { topics: unknown[] }) => void> = [
    (log) => {
      log.topics[0] = TRANSFER_TOPIC.toUpperCase().replace('0X', '0x');
    },
    (log) => {
      log.topics[0] = word(4);
    },
    (log) => {
      log.topics.length = 2;
    },
    (log) => {
      log.topics.push(word(3));
      log.topics.push(word(3));
    },
    (log) => {
      log.topics[1] = `0x${digits(66)}`;
    },
    (log) => {
      log.topics[2] = `0x${digits(62)}`;
    },
    (log) => {
      log.topics[1] = `0x${digits(64)}`;
    }, // dirty high bytes
    (log) => {
      log.topics[2] = null;
    },
    (log) => {
      log.data = `0x${digits(128)}`;
    },
    (log) => {
      log.data = '0x00';
    },
    (log) => {
      log.data = `0x${digits(62)}`;
    },
    (log) => {
      log.address = `0x${digits(39)}`;
    },
    (log) => {
      log.address = 42;
    },
    (log) => {
      log.blockNumber = '16';
    },
    (log) => {
      log.blockNumber = `0x${'f'.repeat(14)}`;
    },
    (log) => {
      log.logIndex = '0x';
    },
    (log) => {
      log.logIndex = `0x${'f'.repeat(16)}`;
    },
    (log) => {
      log.blockHash = 'nope';
    },
    (log) => {
      log.removed = true;
    },
  ];

  for (let i = 0; i < 5_000; i += 1) {
    const erc721 = next() < 0.3;
    const topics: unknown[] = [TRANSFER_TOPIC, word(40), word(40), ...(erc721 ? [word(8)] : [])];

    const log: WireLog & { topics: unknown[] } = {
      address: `0x${digits(40)}`,
      topics,
      data: erc721 ? '0x' : word(12),
      blockNumber: `0x${digits(6)}`.replace('0x0', '0x1'),
      blockHash: word(64),
      transactionHash: word(64),
      logIndex: `0x${digits(2)}`.replace('0x0', '0x1'),
      removed: false,
    };

    if (next() < 0.6) {
      pick(mutations)(log);
    }

    corpus.push(log);
  }

  const text = (event: TransferEvent | undefined) =>
    JSON.stringify(event && { ...event, value: event.value.toString(), blockNumber: event.blockNumber.toString() });

  const decoded = corpus.filter((sample) => decodeTransferLog(sample)).length;
  expect(decoded).toBeGreaterThan(500);
  expect(corpus.filter((sample) => text(decodeTransferLog(sample)) !== text(decodeTransferLogFast(sample)))).toEqual(
    [],
  );
});

test('the Transfer bloom check: an empty bloom proves no Transfer, a full or unknown bloom never does', () => {
  expect(bloomMayHaveTransfer(`0x${'0'.repeat(512)}`)).toBe(false);
  expect(bloomMayHaveTransfer(`0x${'f'.repeat(512)}`)).toBe(true);
  expect(bloomMayHaveTransfer('0x')).toBe(true);
});

test('the RPC source reads headers with their bloom and fetches logs in fixed sub-ranges', async () => {
  const chain = new FakeChain(130n);
  chain.addTransfer({ token: address(4), from: address(1), to: address(2), value: 1n, blockNumber: 105n });
  const source = createRpcBootstrapSource(chain.rpc, { logRange: 4n });
  const pages: number[] = [];

  expect(
    await source.transfers({ fromBlock: 100n, toBlock: 109n }, async (events) => {
      pages.push(events.length);
    }),
  ).toEqual({ requests: 3 });
  expect(pages).toEqual([0, 1, 0]);
  const headers = await source.headers({ fromBlock: 104n, toBlock: 106n });
  expect(headers.map((header) => [header.number, header.hash, header.transferBloom])).toEqual([
    [104n, blockHash(104n), false],
    [105n, blockHash(105n), true],
    [106n, blockHash(106n), false],
  ]);
  expect(source.stats()).toMatchObject({ requests: 6, logs: 1, failures: 0 });
});

test('the HyperSync source pages headers and topic0-only logs, and waits out a 429', async () => {
  const seen: Query[] = [];
  const waits: number[] = [];
  const bloom = `0x${'f'.repeat(512)}`;

  const block = (n: number) => ({
    number: n,
    hash: blockHash(BigInt(n)),
    parentHash: blockHash(BigInt(n - 1)),
    timestamp: n * 12,
    logsBloom: bloom,
  });

  const log = (n: number, index: number) => ({
    blockNumber: n,
    logIndex: index,
    blockHash: blockHash(BigInt(n)),
    transactionHash: blockHash(BigInt(n * 100 + index)),
    address: address(4),
    data: pad('0x5', { size: 32 }),
    topics: [TRANSFER_TOPIC, pad(address(1), { size: 32 }), pad(address(2), { size: 32 }), null],
  });

  const page = (nextBlock: number, data: Partial<QueryResponse['data']>): QueryResponse => ({
    archiveHeight: 1_000,
    nextBlock,
    totalExecutionTime: 1,
    data: { blocks: [], logs: [], transactions: [], traces: [], ...data },
  });

  const script: Array<QueryResponse | 'limited'> = [
    page(12, { blocks: [block(10), block(11)] }),
    'limited',
    page(13, { blocks: [block(12)] }),
    page(12, { logs: [log(10, 0), log(11, 3)] }),
    page(13, { logs: [log(12, 1)] }),
  ];

  const source = createHypersyncBootstrapSource({
    url: 'https://eth.hypersync.xyz',
    apiToken: 'SECRET',
    idleMs: 1_000,
    sleep: async (ms) => {
      waits.push(ms);
    },
    clientFactory: async () => ({
      joinNothing: 2,
      client: {
        async getWithRateLimit(query) {
          seen.push(query);
          const next = script.shift();

          return next === 'limited' ? { rateLimit: { resetSecs: 2 } } : { response: next, rateLimit: {} };
        },
      },
    }),
  });

  const headers = await source.headers({ fromBlock: 10n, toBlock: 12n });
  expect(headers.map((header) => [header.number, header.transferBloom])).toEqual([
    [10n, true],
    [11n, true],
    [12n, true],
  ]);
  expect(seen[0]).toMatchObject({ fromBlock: 10, toBlock: 13, includeAllBlocks: true });
  expect(waits).toEqual([3_000]);
  const events: TransferEvent[] = [];
  expect(
    await source.transfers({ fromBlock: 10n, toBlock: 12n }, async (pageEvents) => {
      events.push(...pageEvents);
    }),
  ).toEqual({ requests: 2 });
  expect(events.map((event) => [event.blockNumber, event.logIndex, event.value])).toEqual([
    [10n, 0, 5n],
    [11n, 3, 5n],
    [12n, 1, 5n],
  ]);
  expect(seen.at(-2)?.logs).toEqual([{ include: { topics: [[TRANSFER_TOPIC]] } }]);
  expect(source.stats()).toMatchObject({ requests: 5, logs: 3, rateLimitWaits: 1, failures: 0 });
});

test.each(['request', 'rate-limit', 'quota'])(
  'the HyperSync bootstrap source cancels a waiting %s and queued work',
  async (wait) => {
    const controller = new AbortController();
    const waiting = Promise.withResolvers<void>();
    let requests = 0;
    let pages = 0;

    const source = createHypersyncBootstrapSource({
      url: 'https://hypersync.invalid',
      apiToken: 'token',
      idleMs: 120_000,
      concurrency: 1,
      signal: controller.signal,
      onRateLimit: () => waiting.resolve(),
      clientFactory: async () => ({
        joinNothing: 2,
        client: {
          getWithRateLimit() {
            requests += 1;

            if (wait === 'request') {
              waiting.resolve();

              return new Promise(() => {});
            }

            const response: QueryResponse = {
              archiveHeight: 1000,
              nextBlock: 11,
              totalExecutionTime: 1,
              data: { blocks: [], logs: [], transactions: [], traces: [] },
            };

            return Promise.resolve({
              ...(wait === 'quota' && { response }),
              rateLimit: { resetSecs: 120, remaining: 0, cost: 1 },
            });
          },
        },
      }),
    });

    const running = source.transfers({ fromBlock: 10n, toBlock: 12n }, async () => {
      pages += 1;
    });

    const queued = source.headers({ fromBlock: 10n, toBlock: 12n });
    const settled = Promise.allSettled([running, queued]);
    await waiting.promise;
    controller.abort(new Error('Bootstrap stopped'));
    expect((await settled).map((result) => result.status)).toEqual(['rejected', 'rejected']);
    expect(requests).toBe(1);
    expect(pages).toBe(wait === 'quota' ? 1 : 0);
  },
);
