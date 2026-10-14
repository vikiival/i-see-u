import { afterEach, expect, test, vi } from 'vitest';
import { pad } from 'viem';
import type { Address, Hex, RpcClient, RpcRequestOptions, RpcStats, TransferEvent } from '../src/core/types.ts';
import { decodeTransferLog, scanAccountChunk, scanTransfers, TRANSFER_TOPIC } from '../src/chains/evm/logs.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { createRpcClient } from '../src/rpc/client.ts';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

const ACCOUNT = '0x1111111111111111111111111111111111111111' as Address;
const OTHER = '0x2222222222222222222222222222222222222222' as Address;
const TOKEN = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as Address;
const hash = (value: bigint): Hex => `0x${value.toString(16).padStart(64, '0')}`;
const topic = (value: Address): Hex => pad(value, { size: 32 });
const stats = (): RpcStats => ({ requests: 0, attempts: 0, byMethod: {}, errors: {}, endpoints: [] });

const wireLog = (block: bigint, topics = [TRANSFER_TOPIC, topic(OTHER), topic(ACCOUNT)], data = `0x${123n.toString(16).padStart(64, '0')}`) => ({
  address: TOKEN.toUpperCase().replace('0X', '0x'),
  topics,
  data,
  blockNumber: `0x${block.toString(16)}`,
  blockHash: hash(block),
  transactionHash: hash(block + 100n),
  logIndex: '0x1',
});

test('decodeEventLog-based decoding handles ERC-20 and ERC-721 wire logs', () => {
  const erc20 = decodeTransferLog(wireLog(12n));
  expect(erc20).toEqual({
    standard: 'erc20',
    token: TOKEN,
    from: OTHER,
    to: ACCOUNT,
    value: 123n,
    blockNumber: 12n,
    blockHash: hash(12n),
    logIndex: 1,
    transactionHash: hash(112n),
  });

  const erc721 = decodeTransferLog(wireLog(13n, [TRANSFER_TOPIC, topic(OTHER), topic(ACCOUNT), hash(999n)], '0x'));
  expect(erc721?.standard).toBe('erc721');
  expect(erc721?.value).toBe(999n);
  expect(decodeTransferLog(wireLog(14n, [TRANSFER_TOPIC, topic(OTHER)], '0x'))).toBeUndefined();
  // Strict decoding: a three-topic log with empty data is malformed and skipped.
  expect(decodeTransferLog(wireLog(12n, undefined, '0x'))).toBeUndefined();
});

test('a wrapped too_large RpcError drives suggested range splitting through the viem transport', async () => {
  const requested: Array<[bigint, bigint]> = [];
  const rpc: RpcClient = {
    async request<T>(method: string, params: unknown[]): Promise<T> {
      expect(method).toBe('eth_getLogs');
      const filter = params[0] as { fromBlock: string; toBlock: string; topics: unknown[] };
      expect(filter.topics).toEqual([TRANSFER_TOPIC, null, topic(ACCOUNT)]);
      const from = BigInt(filter.fromBlock);
      const to = BigInt(filter.toBlock);
      requested.push([from, to]);
      if (to - from + 1n > 2n) {
        throw new RpcError('too_large', `query returned more than 10000 results. Try with this block range [0x${from.toString(16)}, 0x${(from + 1n).toString(16)}]`);
      }
      const logs = [];
      for (let block = from; block <= to; block += 1n) logs.push(wireLog(block));
      return logs as T;
    },
    stats,
  };

  const events: TransferEvent[] = [];
  const result = await scanTransfers(rpc, ACCOUNT, 'to', { fromBlock: 1n, toBlock: 5n }, event => events.push(event));
  expect(result).toMatchObject({ calls: 5, split: true });
  expect(requested).toEqual([[1n, 5n], [1n, 2n], [3n, 5n], [3n, 4n], [5n, 5n]]);
  expect(events.map(event => event.blockNumber)).toEqual([1n, 2n, 3n, 4n, 5n]);
});

test('fetchTransfers halves without a usable suggestion and stops at one block', async () => {
  let calls = 0;
  const rpc: RpcClient = {
    async request<T>(_method: string, params: unknown[]): Promise<T> {
      calls += 1;
      const filter = params[0] as { fromBlock: string; toBlock: string; topics: unknown[] };
      expect(filter.topics).toEqual([TRANSFER_TOPIC, topic(ACCOUNT), null]);
      const from = BigInt(filter.fromBlock);
      const to = BigInt(filter.toBlock);
      if (from === to) throw new RpcError('too_large', 'response size exceeded');
      throw new RpcError('too_large', 'response size exceeded');
    },
    stats,
  };
  await expect(
    scanTransfers(rpc, ACCOUNT, 'from', { fromBlock: 10n, toBlock: 12n }, () => {}),
  ).rejects.toMatchObject({ kind: 'too_large' });
  expect(calls, '3-block range halves to 2 blocks, then reaches the failing single block').toBe(3);
});

test('a timeout on a multi-block range splits it in half; a single block retries normally', async () => {
  const requested: Array<[bigint, bigint, boolean | undefined]> = [];
  const rpc: RpcClient = {
    async request<T>(_method: string, params: unknown[], opts?: RpcRequestOptions): Promise<T> {
      const filter = params[0] as { fromBlock: string; toBlock: string };
      const from = BigInt(filter.fromBlock);
      const to = BigInt(filter.toBlock);
      requested.push([from, to, opts?.failFastOnTimeout]);
      if (to - from + 1n > 2n) throw new RpcError('transient', 'RPC request timed out', { timeout: true });
      return [wireLog(from)] as T;
    },
    stats,
  };
  const events: TransferEvent[] = [];
  const result = await scanTransfers(rpc, ACCOUNT, 'to', { fromBlock: 1n, toBlock: 8n }, event => events.push(event));
  expect(result).toMatchObject({ calls: 7, split: true });
  expect(requested).toEqual([
    [1n, 8n, true], [1n, 4n, true], [1n, 2n, true], [3n, 4n, true], [5n, 8n, true], [5n, 6n, true], [7n, 8n, true],
  ]);
  expect(events.map(event => event.blockNumber)).toEqual([1n, 3n, 5n, 7n]);

  requested.length = 0;
  await scanTransfers(rpc, ACCOUNT, 'to', { fromBlock: 9n, toBlock: 9n }, () => {});
  expect(requested).toEqual([[9n, 9n, false]]);
});

test('scanAccountChunk folds both directions into one aggregate and flags heavy chunks', async () => {
  const self = (block: bigint, logIndex: string) => ({ ...wireLog(block, [TRANSFER_TOPIC, topic(ACCOUNT), topic(ACCOUNT)]), logIndex });
  const responses = {
    from: [self(5n, '0x1'), self(5n, '0x2'), wireLog(4n, [TRANSFER_TOPIC, topic(ACCOUNT), topic(OTHER)])],
    to: [self(5n, '0x1'), self(5n, '0x2'), wireLog(6n)],
  };
  let bytes: number | undefined = 100;
  let durationMs = 10;
  const rpc: RpcClient = {
    async request<T>(_method: string, params: unknown[], opts?: RpcRequestOptions): Promise<T> {
      const filter = params[0] as { topics: unknown[] };
      opts?.onAttempt?.({ ok: true, durationMs, bytes });
      return (filter.topics[1] === null ? responses.to : responses.from) as T;
    },
    stats,
  };
  const range = { fromBlock: 1n, toBlock: 10n };
  const light = await scanAccountChunk(rpc, ACCOUNT, range);
  expect(light.calls).toBe(2);
  expect(light.heavy).toBe(false);
  expect(light.aggregate.tokens.get(TOKEN)).toEqual({ standard: 'erc20', firstBlock: 4n, lastBlock: 6n, sentCount: 3, receivedCount: 3 });

  bytes = 60_000_000; // above the 50 MB threshold
  expect((await scanAccountChunk(rpc, ACCOUNT, range)).heavy).toBe(true);
  bytes = undefined; // compressed response: estimated from the log count (3 logs, well under 1 kB each)
  expect((await scanAccountChunk(rpc, ACCOUNT, range)).heavy).toBe(false);
  durationMs = 4_000; // slow (> 3 s): heavy only when the response is not trivial (> 5 MB)
  bytes = 1_000_000;
  expect((await scanAccountChunk(rpc, ACCOUNT, range)).heavy).toBe(false);
  bytes = 6_000_000;
  expect((await scanAccountChunk(rpc, ACCOUNT, range)).heavy).toBe(true);
});

test('scanAccountChunk rejects with the failing direction error', async () => {
  const rpc: RpcClient = {
    async request<T>(_method: string, params: unknown[]): Promise<T> {
      const filter = params[0] as { topics: unknown[] };
      if (filter.topics[1] === null) throw new RpcError('fatal', 'received side failed');
      return [] as T;
    },
    stats,
  };
  await expect(scanAccountChunk(rpc, ACCOUNT, { fromBlock: 1n, toBlock: 2n }))
    .rejects.toThrow('received side failed');
});

test('a non-array eth_getLogs result fails over through RpcClient', async () => {
  const urls: string[] = [];
  vi.spyOn(Math, 'random').mockReturnValue(0);
  vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
    urls.push(String(input));
    const result = String(input).startsWith('https://primary.example') ? null : [wireLog(7n)];
    return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  }));
  const rpc = createRpcClient({
    urls: ['https://primary.example/key', 'https://fallback.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 2,
    maxRps: 1_000,
  });

  const events: TransferEvent[] = [];
  await scanTransfers(rpc, ACCOUNT, 'to', { fromBlock: 7n, toBlock: 7n }, event => events.push(event));
  expect(events.map(event => event.blockNumber)).toEqual([7n]);
  expect(urls.map(url => new URL(url).host)).toEqual(['primary.example', 'fallback.example']);
  expect(rpc.stats().errors).toEqual({ transient: 1 });
});
