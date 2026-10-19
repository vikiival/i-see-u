import type { Query } from '@envio-dev/hypersync-client';
import { hexToBytes, keccak256 } from 'viem';

import { decodeTransferLogFast, scanAllTransfers, transfersInBlock, TRANSFER_TOPIC } from '../chains/evm/logs.ts';
import { createLimiter } from '../core/limiter.ts';
import { abortable } from '../core/abort.ts';
import { isObject, isString, thrownMessage, toLowerHex } from '../core/parse.ts';
import type { BlockHeader, BlockRange, Hex, RpcClient, RpcRequestOptions, TransferEvent } from '../core/types.ts';
import { hypersyncPages, redactSecrets } from '../sources/hypersync-pages.ts';
import { LOG_FIELDS, wireLog } from '../sources/hypersync-wire.ts';
import type { HypersyncFactoryResult } from '../sources/hypersync-wire.ts';

/** A header plus whether its logsBloom may contain the Transfer topic (false proves the block has no Transfer). */
export interface BootstrapHeader extends BlockHeader {
  transferBloom: boolean;
}

export interface BootstrapSourceStats {
  requests: number;
  logs: number;
  failures: number;
  rateLimitWaits: number;
  lastError?: string;
}

/** One bulk data source. Every method is complete-or-throw; pages are handed over without collecting the range. */
export interface BootstrapSource {
  readonly name: string;
  /** Every header of the range in order, including empty blocks. */
  headers(range: BlockRange): Promise<BootstrapHeader[]>;
  /** Decoded Transfers of the range, ascending, in pages; returns the requests it used. */
  transfers(range: BlockRange, onPage: (events: TransferEvent[]) => Promise<void>): Promise<{ requests: number }>;
  /** The Transfers of one block, for the zero-log / logsBloom cross-check. */
  blockTransfers(block: { number: bigint; hash: Hex }): Promise<TransferEvent[]>;
  stats(): BootstrapSourceStats;
}

// EIP-2159 bloom: three 11-bit indices from keccak256(topic).
const bloomBits = (() => {
  const digest = hexToBytes(keccak256(TRANSFER_TOPIC));

  return [0, 2, 4].map((i) => ((digest[i] << 8) | digest[i + 1]) & 2047);
})();

/** False only when the bloom proves the block has no log with the Transfer topic. */
export function bloomMayHaveTransfer(logsBloom: string): boolean {
  if (!/^0x[0-9a-fA-F]{512}$/.test(logsBloom)) return true; // unknown bloom: always cross-check
  const bytes = hexToBytes(`0x${logsBloom.slice(2)}`);

  return bloomBits.every((bit) => (bytes[255 - (bit >> 3)] >> (bit & 7)) & 1);
}

const hex = (value: bigint): Hex => `0x${value.toString(16)}`;

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: JSON-RPC and HyperSync fields are untyped
const isHexString = (value: unknown): value is Hex => isString(value) && /^0x[0-9a-fA-F]+$/.test(value);

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: validates the raw eth_getBlockByNumber result
const validRpcBlock = (value: unknown): boolean => isObject(value)
  && 'number' in value && isHexString(value.number) && 'hash' in value && isHexString(value.hash)
  && 'parentHash' in value && isHexString(value.parentHash) && 'timestamp' in value && isHexString(value.timestamp)
  && 'logsBloom' in value && isHexString(value.logsBloom);

interface RpcBlock {
  number: Hex;
  hash: Hex;
  parentHash: Hex;
  timestamp: Hex;
  logsBloom: Hex;
}

/**
 * Quicknode (any JSON-RPC) source: unfiltered topic0 `eth_getLogs` over `logRange`-block calls (500 blocks reached
 * ~370 MB of JSON), split in half on too-large/timeout. Headers cost one call per block, so they are the fallback only.
 */
export function createRpcBootstrapSource(rpc: RpcClient, o: { logRange: bigint; headerConcurrency?: number; name?: string }): BootstrapSource {
  const state: BootstrapSourceStats = { requests: 0, logs: 0, failures: 0, rateLimitWaits: 0 };
  const concurrency = o.headerConcurrency ?? 8;

  const counted = (calls: { n: number }): RpcClient => ({
    request<T>(method: string, params: unknown[], options?: RpcRequestOptions) {
      calls.n += 1;
      state.requests += 1;

      return rpc.request<T>(method, params, options);
    },
    stats: () => rpc.stats(),
  });

  const tracked = async <T>(work: () => Promise<T>): Promise<T> => {
    try {
      return await work();
    } catch (error) {
      state.failures += 1;
      state.lastError = thrownMessage(error).slice(0, 200);
      throw error;
    }
  };

  const header = async (number: bigint): Promise<BootstrapHeader> => {
    const block = await counted({ n: 0 }).request<RpcBlock>('eth_getBlockByNumber', [hex(number), false], { validate: validRpcBlock });

    return {
      number: BigInt(block.number), hash: toLowerHex(block.hash), parentHash: toLowerHex(block.parentHash),
      timestamp: BigInt(block.timestamp), transferBloom: bloomMayHaveTransfer(block.logsBloom),
    };
  };

  return {
    name: o.name ?? 'rpc',
    headers: (range) => tracked(async () => {
      const headers: BootstrapHeader[] = [];

      for (let start = range.fromBlock; start <= range.toBlock; start += BigInt(concurrency)) {
        const count = Number(range.toBlock - start + 1n < BigInt(concurrency) ? range.toBlock - start + 1n : BigInt(concurrency));
        headers.push(...await Promise.all(Array.from({ length: count }, (_, i) => header(start + BigInt(i)))));
      }

      return headers;
    }),
    transfers: (range, onPage) => tracked(async () => {
      const calls = { n: 0 };

      for (let from = range.fromBlock; from <= range.toBlock; from += o.logRange) {
        const to = from + o.logRange - 1n < range.toBlock ? from + o.logRange - 1n : range.toBlock;
        const events: TransferEvent[] = [];
        await scanAllTransfers(counted(calls), { fromBlock: from, toBlock: to }, (event) => { events.push(event); });
        state.logs += events.length;
        await onPage(events);
      }

      return { requests: calls.n };
    }),
    blockTransfers: (block) => tracked(() => transfersInBlock(counted({ n: 0 }), block.hash)),
    stats: () => ({ ...state }),
  };
}

export interface HypersyncBootstrapOptions {
  url: string;
  apiToken: string;
  /** Per-request idle timeout; the native request is abandoned (never awaited, never `close()`d) after it. */
  idleMs: number;
  /** Requests in flight (default 2: one segment plus one single-block cross-check; more only burn the 15/min quota). */
  concurrency?: number;
  clientFactory: () => Promise<HypersyncFactoryResult>;
  sleep?: (ms: number) => Promise<void>;
  onRateLimit?: (waitMs: number) => void;
  signal?: AbortSignal;
}

/**
 * Envio HyperSync source: paginated `getWithRateLimit` (never `stream()`/`collect()`), headers for ~5,000 blocks per
 * request via `includeAllBlocks`, and topic0-only log pages (server-truncated at ~28k-150k logs).
 */
export function createHypersyncBootstrapSource(o: HypersyncBootstrapOptions): BootstrapSource {
  const state: BootstrapSourceStats = { requests: 0, logs: 0, failures: 0, rateLimitWaits: 0 };
  const limiter = createLimiter(o.concurrency ?? 2, 1_000_000);
  let client: Promise<HypersyncFactoryResult> | undefined;
  const secrets = [o.apiToken, o.url];

  const run = <T>(work: (hs: HypersyncFactoryResult) => Promise<T>): Promise<T> => limiter.run(async () => {
    try {
      client ??= o.clientFactory();

      return await work(await abortable(client, o.signal));
    } catch (error) {
      state.failures += 1;
      state.lastError = redactSecrets(thrownMessage(error), secrets);
      throw new Error(state.lastError);
    }
  }, { signal: o.signal });

  const pages = (hs: HypersyncFactoryResult, query: Query, requests: { n: number }) => hypersyncPages(hs.client, query, {
    idleMs: o.idleMs,
    sleep: o.sleep,
    signal: o.signal,
    onRequest: () => { requests.n += 1; state.requests += 1; },
    onRateLimit: (waitMs) => { state.rateLimitWaits += 1; o.onRateLimit?.(waitMs); },
  });

  const bounds = (range: BlockRange) => {
    if (range.fromBlock < 0n || range.toBlock + 1n > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('HyperSync block range is not safely representable');

    return { fromBlock: Number(range.fromBlock), toBlock: Number(range.toBlock + 1n) };
  };

  const transfers = (range: BlockRange, onPage: (events: TransferEvent[]) => Promise<void>) => run(async (hs) => {
    const requests = { n: 0 };
    const query: Query = { ...bounds(range), joinMode: hs.joinNothing, logs: [{ include: { topics: [[TRANSFER_TOPIC]] } }], fieldSelection: { log: LOG_FIELDS } };

    for await (const response of pages(hs, query, requests)) {
      const events: TransferEvent[] = [];

      for (const raw of response.data.logs) {
        if (!isObject(raw) || !Array.isArray(raw.topics)) throw new Error('HyperSync returned a malformed log');
        const event = decodeTransferLogFast(wireLog(raw));

        if (event) events.push(event);
      }

      state.logs += events.length;
      await onPage(events);
    }

    return { requests: requests.n };
  });

  return {
    name: 'hypersync',
    headers: (range) => run(async (hs) => {
      const query: Query = {
        ...bounds(range), joinMode: hs.joinNothing, includeAllBlocks: true,
        fieldSelection: { block: ['Number', 'Hash', 'ParentHash', 'Timestamp', 'LogsBloom'] },
      };

      const headers: BootstrapHeader[] = [];

      for await (const response of pages(hs, query, { n: 0 })) {
        for (const block of response.data.blocks) {
          const { number, hash, parentHash, timestamp, logsBloom } = block;

          if (number === undefined || !isHexString(hash) || !isHexString(parentHash) || timestamp === undefined || !isString(logsBloom)) {
            throw new Error('HyperSync returned a malformed block');
          }

          headers.push({ number: BigInt(number), hash: toLowerHex(hash), parentHash: toLowerHex(parentHash), timestamp: BigInt(timestamp), transferBloom: bloomMayHaveTransfer(logsBloom) });
        }
      }

      return headers;
    }),
    transfers,
    async blockTransfers(block) {
      const events: TransferEvent[] = [];
      await transfers({ fromBlock: block.number, toBlock: block.number }, async (page) => { events.push(...page); });

      return events;
    },
    stats: () => ({ ...state }),
  };
}
