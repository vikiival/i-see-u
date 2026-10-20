import { decodeEventLog, encodeEventTopics, isAddress, isHex, pad, parseAbiItem } from 'viem';

import { ChainGapError } from '../../core/errors.ts';
import { addTransfer, aggregateLogCount, emptyAggregate } from '../../core/aggregate.ts';
import type {
  Address,
  BlockRange,
  ChunkScan,
  Hex,
  RpcClient,
  TransferDirection,
  TransferEvent,
} from '../../core/types.ts';
import { RpcError, rpcErrorOf } from '../../rpc/errors.ts';
import { isString, toLowerHex } from '../../core/parse.ts';

const erc20Transfer = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)');

const erc721Transfer = parseAbiItem(
  'event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)',
);

export const TRANSFER_TOPIC = encodeEventTopics({ abi: [erc20Transfer] })[0];

/** A response above this many bytes marks the chunk heavy. */
const HEAVY_BYTES = 50_000_000;

/** A successful attempt slower than this marks the chunk heavy, if its response exceeds HEAVY_BYTES / 10. */
const HEAVY_MS = 3_000;

/** Rough JSON size of one address-filtered Transfer log; used when the provider compresses (no Content-Length). */
const APPROX_LOG_BYTES = 640;

/** eth_getLogs entry as it comes off the wire: unvalidated. */
export interface WireLog {
  address?: unknown;
  topics?: unknown;
  data?: unknown;
  blockNumber?: unknown;
  blockHash?: unknown;
  transactionHash?: unknown;
  logIndex?: unknown;
  removed?: unknown;
}

/** A hex quantity field of a wire log; throws unless it is a hex string BigInt accepts. */
const quantity = (log: WireLog, field: 'blockNumber' | 'logIndex'): bigint => {
  const value = log[field];

  if (!isHex(value)) {
    throw new Error('not a hex quantity');
  }

  return BigInt(value);
};

/** Decode and normalize an eth_getLogs Transfer entry strictly; malformed/non-Transfer logs are skipped. */
export function decodeTransferLog(log: WireLog): TransferEvent | undefined {
  if (log.removed === true) {
    return undefined;
  }

  const { topics } = log;

  if (!Array.isArray(topics) || (topics.length !== 3 && topics.length !== 4)) {
    return undefined;
  }

  if (!isString(log.address) || !isAddress(log.address, { strict: false })) {
    return undefined;
  }

  if (!isHex(log.transactionHash) || !isHex(log.blockHash) || !isHex(log.data)) {
    return undefined;
  }

  try {
    const standard = topics.length === 3 ? 'erc20' : 'erc721';

    const { args } = decodeEventLog({
      abi: [standard === 'erc20' ? erc20Transfer : erc721Transfer],
      data: log.data,
      strict: true,
      // SAFETY: only satisfies decodeEventLog's static signature; with strict: true it checks every topic against the
      // ABI and throws (caught below) on a malformed one.
      topics: topics as [Hex, ...Hex[]],
    });

    const { from, to } = args;
    const value = 'value' in args ? args.value : args.tokenId;
    const logIndex = quantity(log, 'logIndex');

    if (logIndex > BigInt(Number.MAX_SAFE_INTEGER)) {
      return undefined;
    }

    return {
      standard,
      token: toLowerHex(log.address),
      from: toLowerHex(from),
      to: toLowerHex(to),
      value,
      blockNumber: quantity(log, 'blockNumber'),
      blockHash: toLowerHex(log.blockHash),
      logIndex: Number(logIndex),
      transactionHash: toLowerHex(log.transactionHash),
    };
  } catch {
    return undefined;
  }
}

const HEX = /^0x[0-9a-fA-F]*$/;
const WORD = /^0x[0-9a-fA-F]{64}$/;
const ADDRESS = /^0x[0-9a-fA-F]{40}$/;
const QUANTITY = /^0x[0-9a-fA-F]{1,13}$/;

/**
 * Bulk decoder (~15x faster than viem): the canonical wire shape (exact Transfer topic0, 32-byte topics, 32-byte
 * ERC-20 data or empty ERC-721 data) is decoded by hand; every other shape goes through decodeTransferLog, so both
 * always agree (a golden test fuzzes them against each other).
 */
export function decodeTransferLogFast(log: WireLog): TransferEvent | undefined {
  const { topics, data } = log;

  if (
    log.removed === true ||
    !Array.isArray(topics) ||
    (topics.length !== 3 && topics.length !== 4) ||
    topics[0] !== TRANSFER_TOPIC
  ) {
    return decodeTransferLog(log);
  }

  const erc20 = topics.length === 3;
  const [, from, to, id] = topics;

  const { address, transactionHash, blockHash, blockNumber, logIndex } = log;

  if (
    !isString(address) ||
    !ADDRESS.test(address) ||
    !isString(transactionHash) ||
    !HEX.test(transactionHash) ||
    !isString(blockHash) ||
    !HEX.test(blockHash) ||
    !isString(blockNumber) ||
    !QUANTITY.test(blockNumber) ||
    !isString(logIndex) ||
    !QUANTITY.test(logIndex) ||
    !isString(from) ||
    !WORD.test(from) ||
    !isString(to) ||
    !WORD.test(to) ||
    (erc20 ? !isString(data) || !WORD.test(data) : data !== '0x' || !isString(id) || !WORD.test(id))
  ) {
    return decodeTransferLog(log);
  }

  return {
    standard: erc20 ? 'erc20' : 'erc721',
    token: toLowerHex(`0x${address.slice(2)}`),
    from: `0x${from.slice(26).toLowerCase()}`,
    to: `0x${to.slice(26).toLowerCase()}`,
    value: BigInt(erc20 ? data : id),
    blockNumber: BigInt(blockNumber),
    blockHash: toLowerHex(`0x${blockHash.slice(2)}`),
    logIndex: Number(logIndex),
    transactionHash: toLowerHex(`0x${transactionHash.slice(2)}`),
  };
}

const getLogs = (rpc: RpcClient, filter: { blockHash: Hex; topics: Hex[] }): Promise<WireLog[]> =>
  rpc.request('eth_getLogs', [filter], {
    validate: Array.isArray,
  });

/** EIP-234 block-hash query: one canonical block, never a same-height block from another backend. */
export async function transfersInBlock(rpc: RpcClient, blockHash: Hex): Promise<TransferEvent[]> {
  const raw = await getLogs(rpc, { blockHash, topics: [TRANSFER_TOPIC] });
  const events: TransferEvent[] = [];

  for (const log of raw) {
    if (log.removed === true) {
      continue;
    }

    if (!isHex(log.blockHash) || log.blockHash.toLowerCase() !== blockHash) {
      throw new RpcError('transient', 'Log block hash does not match requested block');
    }

    const event = decodeTransferLog(log);

    if (event) {
      events.push(event);
    }
  }

  return events;
}

/**
 * Catch-up by number (splits on too_large/timeout like the follower), accepted only when every log belongs to the
 * supplied canonical header: a load-balanced node may answer from another fork.
 */
export async function transfersInRange(
  rpc: RpcClient,
  range: BlockRange,
  headers: { number: bigint; hash: Hex }[],
): Promise<TransferEvent[]> {
  const expected = new Map(headers.map((header) => [header.number, header.hash]));
  const events: TransferEvent[] = [];
  await scanLogs(rpc, [TRANSFER_TOPIC], range, (log) => {
    if (log.removed === true) {
      return;
    }

    let number: bigint;

    try {
      number = quantity(log, 'blockNumber');
    } catch {
      throw new RpcError('transient', 'Log has no valid block number');
    }

    if (!isHex(log.blockHash) || expected.get(number) !== log.blockHash.toLowerCase()) {
      throw new ChainGapError(`Log block hash does not match header at ${number}`);
    }

    const event = decodeTransferLog(log);

    if (event) {
      events.push(event);
    }
  });

  return events;
}

interface LogsScanStats {
  calls: number;
  /** Largest single response, in bytes (Content-Length, else estimated from the log count). */
  maxBytes: number;
  /** Slowest successful attempt, in ms. */
  maxMs: number;
  /** A range had to be split (too_large or timeout). */
  split: boolean;
}

/**
 * Fetch one topic filter over `range` and hand every raw log to `onLog`, without collecting arrays.
 * A too_large error, or a timeout on a multi-block range, splits the range in half (or at the provider's suggestion).
 * An aborted signal stops the in-flight request and every remaining range; the scan rejects with signal.reason.
 */
async function scanLogs(
  rpc: RpcClient,
  topics: (Hex | null)[],
  range: BlockRange,
  onLog: (log: WireLog) => void,
  signal?: AbortSignal,
): Promise<LogsScanStats> {
  const stats: LogsScanStats = { calls: 0, maxBytes: 0, maxMs: 0, split: false };

  const fetchRange = async (part: BlockRange): Promise<void> => {
    signal?.throwIfAborted();
    stats.calls += 1;
    let bytes: number | undefined;
    let ms = 0;
    let raw: WireLog[];

    try {
      raw = await rpc.request<WireLog[]>(
        'eth_getLogs',
        [
          {
            fromBlock: `0x${part.fromBlock.toString(16)}`,
            toBlock: `0x${part.toBlock.toString(16)}`,
            topics,
          },
        ],
        {
          validate: Array.isArray,
          failFastOnTimeout: part.fromBlock < part.toBlock,
          onAttempt: (attempt) => {
            if (!attempt.ok) {
              return;
            }

            bytes = attempt.bytes;
            ms = attempt.durationMs;
          },
          signal,
        },
      );
    } catch (error) {
      const rpcError = rpcErrorOf(error);

      const splittable =
        rpcError?.kind === 'too_large' || (rpcError?.kind === 'transient' && rpcError.timeout === true);

      if (!splittable) {
        throw rpcError ?? error;
      }

      if (part.fromBlock === part.toBlock) {
        throw rpcError;
      }

      stats.split = true;
      const suggested = rpcError.suggestedRange;

      const splitAt =
        suggested && suggested.fromBlock === part.fromBlock && suggested.toBlock < part.toBlock
          ? suggested.toBlock
          : part.fromBlock + (part.toBlock - part.fromBlock) / 2n;

      await fetchRange({ fromBlock: part.fromBlock, toBlock: splitAt });
      await fetchRange({ fromBlock: splitAt + 1n, toBlock: part.toBlock });

      return;
    }

    stats.maxBytes = Math.max(stats.maxBytes, bytes ?? raw.length * APPROX_LOG_BYTES);
    stats.maxMs = Math.max(stats.maxMs, ms);

    for (const log of raw) {
      if (log.removed === true) {
        continue;
      }

      let number: bigint;

      try {
        number = quantity(log, 'blockNumber');
      } catch {
        throw new RpcError('transient', 'Log has no valid block number');
      }

      if (number < part.fromBlock || number > part.toBlock) {
        throw new RpcError(
          'transient',
          `Log block ${number} outside requested range ${part.fromBlock}-${part.toBlock}`,
        );
      }
    }

    for (const log of raw) {
      onLog(log);
    }
  };

  await fetchRange(range);

  return stats;
}

/** Stream every decoded Transfer with `account` on `direction` side into `onTransfer`. */
export async function scanTransfers(
  rpc: RpcClient,
  account: Address,
  direction: TransferDirection,
  range: BlockRange,
  onTransfer: (event: TransferEvent) => void,
  signal?: AbortSignal,
): Promise<LogsScanStats> {
  const accountTopic = pad(account, { size: 32 });

  const topics = direction === 'from' ? [TRANSFER_TOPIC, accountTopic, null] : [TRANSFER_TOPIC, null, accountTopic];

  return scanLogs(
    rpc,
    topics,
    range,
    (log) => {
      const event = decodeTransferLog(log);

      if (event) {
        onTransfer(event);
      }
    },
    signal,
  );
}

/** All decoded Transfer logs, unfiltered by account (the bootstrap's RPC source; it verifies them against its headers). */
export async function scanAllTransfers(
  rpc: RpcClient,
  range: BlockRange,
  onTransfer: (event: TransferEvent) => void,
): Promise<{ calls: number }> {
  const { calls } = await scanLogs(rpc, [TRANSFER_TOPIC], range, (log) => {
    const event = decodeTransferLogFast(log);

    if (event) {
      onTransfer(event);
    }
  });

  return { calls };
}

/** Both directions of one chunk in parallel, folded into one aggregate while decoding. */
export async function scanAccountChunk(rpc: RpcClient, account: Address, range: BlockRange): Promise<ChunkScan> {
  const aggregate = emptyAggregate();
  // The first failing direction aborts the other (in flight too): don't spend more calls on a chunk that already failed.
  const failed = new AbortController();
  const onTransfer = (event: TransferEvent): void => addTransfer(aggregate, account, event);

  const run = (direction: TransferDirection) =>
    scanTransfers(rpc, account, direction, range, onTransfer, failed.signal).catch((error) => {
      failed.abort(error); // no-op after the first
      throw error;
    });

  const [sent, received] = await Promise.allSettled([run('from'), run('to')]);

  if (sent.status === 'rejected' || received.status === 'rejected') {
    throw failed.signal.reason;
  }

  const a = sent.value;
  const b = received.value;
  // Slowness only counts for a non-trivial response: provider latency is mostly per-request overhead,
  // and shrinking a slow small request would only add requests.
  const bytes = Math.max(a.maxBytes, b.maxBytes);

  const heavy =
    a.split || b.split || bytes > HEAVY_BYTES || (bytes > HEAVY_BYTES / 10 && Math.max(a.maxMs, b.maxMs) > HEAVY_MS);

  return { aggregate, calls: a.calls + b.calls, logs: aggregateLogCount(aggregate), heavy };
}
