import { decodeEventLog, encodeEventTopics, isAddress, isHex, pad, parseAbiItem } from 'viem';

import { addTransfer, emptyAggregate } from '../../core/aggregate.ts';
import type {
  Address,
  BlockRange,
  ChunkScan,
  Hex,
  RawLog,
  RpcClient,
  TransferDirection,
  TransferEvent,
} from '../../core/types.ts';
import { rpcErrorOf } from './viem.ts';

export const erc20Transfer = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)');
export const erc721Transfer = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)');
export const TRANSFER_TOPIC = encodeEventTopics({ abi: [erc20Transfer] })[0];

interface WireLog {
  address?: unknown;
  topics?: unknown;
  data?: unknown;
  blockNumber?: unknown;
  blockHash?: unknown;
  transactionHash?: unknown;
  logIndex?: unknown;
}

const asBigInt = (value: unknown): bigint | undefined => {
  try {
    if (typeof value === 'bigint') return value;
    if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
    if (typeof value === 'string' && /^(?:0x[0-9a-f]+|[0-9]+)$/i.test(value)) return BigInt(value);
  } catch {
    // Invalid wire value.
  }
  return undefined;
};

/** Decode and normalize an eth_getLogs Transfer entry; malformed/non-Transfer logs are skipped. */
export function decodeTransferLog(raw: RawLog | WireLog): TransferEvent | undefined {
  const wire = raw as WireLog;
  if (!Array.isArray(wire.topics) || (wire.topics.length !== 3 && wire.topics.length !== 4)) return undefined;
  if (!wire.topics.every(topic => typeof topic === 'string' && isHex(topic))) return undefined;
  const topics = wire.topics as [Hex, ...Hex[]];
  const blockNumber = asBigInt(wire.blockNumber);
  const logIndexValue = asBigInt(wire.logIndex);
  if (blockNumber === undefined || logIndexValue === undefined || logIndexValue > BigInt(Number.MAX_SAFE_INTEGER)) {
    return undefined;
  }
  if (typeof wire.address !== 'string' || !isAddress(wire.address, { strict: false })) return undefined;
  if (typeof wire.transactionHash !== 'string' || !isHex(wire.transactionHash)) return undefined;
  if (typeof wire.data !== 'string' || !isHex(wire.data)) return undefined;

  try {
    const standard = topics.length === 3 ? 'erc20' : 'erc721';
    const decoded = decodeEventLog({
      abi: [standard === 'erc20' ? erc20Transfer : erc721Transfer],
      data: wire.data,
      strict: true,
      topics,
    });
    const args = decoded.args as { from: Address; to: Address; value?: bigint; tokenId?: bigint };

    return {
      standard,
      token: wire.address.toLowerCase() as Address,
      from: args.from.toLowerCase() as Address,
      to: args.to.toLowerCase() as Address,
      value: standard === 'erc20' ? args.value! : args.tokenId!,
      blockNumber,
      logIndex: Number(logIndexValue),
      transactionHash: wire.transactionHash.toLowerCase() as Hex,
    };
  } catch {
    return undefined;
  }
}

/** Rough JSON size of one address-filtered Transfer log; used when the provider compresses (no Content-Length). */
const APPROX_LOG_BYTES = 640;

export interface LogsScanStats {
  calls: number;
  /** Largest single response, in bytes (Content-Length, else estimated from the log count). */
  maxBytes: number;
  /** Slowest successful attempt, in ms. */
  maxMs: number;
  /** A range had to be split (too_large or timeout). */
  split: boolean;
}

/**
 * Stream every decoded Transfer with `account` on `direction` side into `onTransfer`, without collecting arrays.
 * A too_large error, or a timeout on a multi-block range, splits the range in half (or at the provider's suggestion).
 */
export async function scanTransfers(
  rpc: RpcClient,
  account: Address,
  direction: TransferDirection,
  range: BlockRange,
  onTransfer: (event: TransferEvent) => void,
  shouldStop: () => boolean = () => false,
): Promise<LogsScanStats> {
  if (!isAddress(account, { strict: false })) throw new Error(`Invalid EVM address: ${account}`);
  const stats: LogsScanStats = { calls: 0, maxBytes: 0, maxMs: 0, split: false };
  if (range.fromBlock > range.toBlock) return stats;
  const accountTopic = pad(account, { size: 32 });
  const topics = direction === 'from'
    ? [TRANSFER_TOPIC, accountTopic, null]
    : [TRANSFER_TOPIC, null, accountTopic];

  const fetchRange = async (part: BlockRange): Promise<void> => {
    if (shouldStop()) throw new Error('Chunk scan aborted');
    stats.calls += 1;
    let bytes: number | undefined;
    let ms = 0;
    let raw: unknown[];
    try {
      raw = await rpc.request<unknown[]>('eth_getLogs', [{
          fromBlock: `0x${part.fromBlock.toString(16)}`,
          toBlock: `0x${part.toBlock.toString(16)}`,
          topics,
        }], {
        validate: Array.isArray,
        failFastOnTimeout: part.fromBlock < part.toBlock,
        onAttempt: (attempt) => {
          if (!attempt.ok) return;
          bytes = attempt.bytes;
          ms = attempt.durationMs;
        },
      });
    } catch (error) {
      const rpcError = rpcErrorOf(error);
      const splittable = rpcError?.kind === 'too_large' || (rpcError?.kind === 'transient' && rpcError.timeout === true);
      if (!splittable) throw rpcError ?? error;
      if (part.fromBlock === part.toBlock) throw rpcError;

      stats.split = true;
      const suggested = rpcError.suggestedRange;
      const splitAt = suggested && suggested.fromBlock === part.fromBlock && suggested.toBlock >= part.fromBlock && suggested.toBlock < part.toBlock
        ? suggested.toBlock
        : part.fromBlock + (part.toBlock - part.fromBlock) / 2n;
      await fetchRange({ fromBlock: part.fromBlock, toBlock: splitAt });
      await fetchRange({ fromBlock: splitAt + 1n, toBlock: part.toBlock });
      return;
    }
    const size = bytes ?? raw.length * APPROX_LOG_BYTES;
    if (size > stats.maxBytes) stats.maxBytes = size;
    if (ms > stats.maxMs) stats.maxMs = ms;
    for (const value of raw) {
      const event = decodeTransferLog(value as WireLog);
      if (event) onTransfer(event);
    }
  };
  await fetchRange(range);
  return stats;
}

export interface ChunkScanOptions {
  /** A response above this many bytes marks the chunk heavy. */
  heavyBytes: number;
  /** A successful attempt slower than this marks the chunk heavy, if its response exceeds heavyBytes / 10. */
  heavyMs: number;
}

/** Both directions of one chunk in parallel, folded into one aggregate while decoding. */
export async function scanAccountChunk(
  rpc: RpcClient,
  account: Address,
  range: BlockRange,
  opts: ChunkScanOptions,
): Promise<ChunkScan> {
  const aggregate = emptyAggregate();
  let failed = false;
  let firstError: unknown;
  const onTransfer = (event: TransferEvent): void => addTransfer(aggregate, account, event);
  const run = (direction: TransferDirection) => scanTransfers(rpc, account, direction, range, onTransfer, () => failed)
    .catch((error: unknown) => {
      if (!failed) firstError = error;
      failed = true; // don't spend more calls on a chunk that already failed
      throw error;
    });
  const [sent, received] = await Promise.allSettled([run('from'), run('to')]);
  if (sent.status === 'rejected' || received.status === 'rejected') throw firstError;
  const a = sent.value;
  const b = received.value;
  // Slowness only counts for a non-trivial response: provider latency is mostly per-request overhead,
  // and shrinking a slow small request would only add requests.
  const bytes = Math.max(a.maxBytes, b.maxBytes);
  const heavy = a.split || b.split || bytes > opts.heavyBytes || (bytes > opts.heavyBytes / 10 && Math.max(a.maxMs, b.maxMs) > opts.heavyMs);
  return { aggregate, calls: a.calls + b.calls, heavy };
}
