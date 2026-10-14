import { decodeEventLog, encodeEventTopics, isAddress, isHex, pad, parseAbiItem } from 'viem';

import { addTransfer, emptyAggregate } from '../../core/aggregate.ts';
import type { Address, BlockRange, ChunkScan, Hex, RpcClient, TransferDirection, TransferEvent } from '../../core/types.ts';
import { rpcErrorOf } from '../../rpc/errors.ts';

const erc20Transfer = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)');
const erc721Transfer = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)');
export const TRANSFER_TOPIC = encodeEventTopics({ abi: [erc20Transfer] })[0];

/** A response above this many bytes marks the chunk heavy. */
const HEAVY_BYTES = 50_000_000;
/** A successful attempt slower than this marks the chunk heavy, if its response exceeds HEAVY_BYTES / 10. */
const HEAVY_MS = 3_000;
/** Rough JSON size of one address-filtered Transfer log; used when the provider compresses (no Content-Length). */
const APPROX_LOG_BYTES = 640;

/** eth_getLogs entry as it comes off the wire: unvalidated. */
interface WireLog {
  address?: unknown;
  topics?: unknown;
  data?: unknown;
  blockNumber?: unknown;
  transactionHash?: unknown;
  logIndex?: unknown;
}

const quantity = (value: unknown): bigint => {
  if (!isHex(value)) throw new Error('not a hex quantity');
  return BigInt(value);
};

/** Decode and normalize an eth_getLogs Transfer entry strictly; malformed/non-Transfer logs are skipped. */
export function decodeTransferLog(log: WireLog): TransferEvent | undefined {
  const { topics } = log;
  if (!Array.isArray(topics) || (topics.length !== 3 && topics.length !== 4)) return undefined;
  if (typeof log.address !== 'string' || !isAddress(log.address, { strict: false })) return undefined;
  if (!isHex(log.transactionHash) || !isHex(log.data)) return undefined;
  try {
    const standard = topics.length === 3 ? 'erc20' : 'erc721';
    const { args } = decodeEventLog({
      abi: [standard === 'erc20' ? erc20Transfer : erc721Transfer],
      data: log.data,
      strict: true,
      topics: topics as [Hex, ...Hex[]],
    });
    const { from, to, value, tokenId } = args as { from: Address; to: Address; value?: bigint; tokenId?: bigint };
    const logIndex = quantity(log.logIndex);
    if (logIndex > BigInt(Number.MAX_SAFE_INTEGER)) return undefined;
    return {
      standard,
      token: log.address.toLowerCase() as Address,
      from: from.toLowerCase() as Address,
      to: to.toLowerCase() as Address,
      value: (value ?? tokenId)!,
      blockNumber: quantity(log.blockNumber),
      logIndex: Number(logIndex),
      transactionHash: log.transactionHash.toLowerCase() as Hex,
    };
  } catch {
    return undefined;
  }
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
  const stats: LogsScanStats = { calls: 0, maxBytes: 0, maxMs: 0, split: false };
  const accountTopic = pad(account, { size: 32 });
  const topics = direction === 'from'
    ? [TRANSFER_TOPIC, accountTopic, null]
    : [TRANSFER_TOPIC, null, accountTopic];

  const fetchRange = async (part: BlockRange): Promise<void> => {
    if (shouldStop()) throw new Error('Chunk scan aborted');
    stats.calls += 1;
    let bytes: number | undefined;
    let ms = 0;
    let raw: WireLog[];
    try {
      raw = await rpc.request<WireLog[]>('eth_getLogs', [{
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
      const splitAt = suggested && suggested.fromBlock === part.fromBlock && suggested.toBlock < part.toBlock
        ? suggested.toBlock
        : part.fromBlock + (part.toBlock - part.fromBlock) / 2n;
      await fetchRange({ fromBlock: part.fromBlock, toBlock: splitAt });
      await fetchRange({ fromBlock: splitAt + 1n, toBlock: part.toBlock });
      return;
    }
    stats.maxBytes = Math.max(stats.maxBytes, bytes ?? raw.length * APPROX_LOG_BYTES);
    stats.maxMs = Math.max(stats.maxMs, ms);
    for (const log of raw) {
      const event = decodeTransferLog(log);
      if (event) onTransfer(event);
    }
  };
  await fetchRange(range);
  return stats;
}

/** Both directions of one chunk in parallel, folded into one aggregate while decoding. */
export async function scanAccountChunk(rpc: RpcClient, account: Address, range: BlockRange): Promise<ChunkScan> {
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
  const heavy = a.split || b.split || bytes > HEAVY_BYTES || (bytes > HEAVY_BYTES / 10 && Math.max(a.maxMs, b.maxMs) > HEAVY_MS);
  return { aggregate, calls: a.calls + b.calls, heavy };
}
