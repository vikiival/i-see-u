import { pad } from 'viem';
import type { Log, Query, QueryResponse, StreamConfig } from '@envio-dev/hypersync-client';

import { addTransfer, aggregateLogCount, emptyAggregate } from '../core/aggregate.ts';
import type { AccountLogSource, AccountLogSourceStats, Address, BlockRange } from '../core/types.ts';
import { decodeTransferLog } from '../chains/evm/logs.ts';
import type { WireLog } from '../chains/evm/logs.ts';
import { TRANSFER_TOPIC } from '../chains/evm/logs.ts';

export interface HypersyncStream {
  recv(): Promise<QueryResponse | null>;
  close(): Promise<void>;
}

export interface HypersyncClientLike {
  stream(query: Query, config: StreamConfig): Promise<HypersyncStream>;
}

export interface HypersyncFactoryResult {
  client: HypersyncClientLike;
  joinNothing: number;
}

export interface HypersyncSourceOptions {
  url: string;
  apiToken: string;
  preferredMaxRange: bigint;
  timeoutMs: number;
  name?: string;
  /** Tests inject a fake; production dynamically imports the native addon on first use. */
  clientFactory?: () => Promise<HypersyncFactoryResult>;
}

const integer = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) >= 0;
const quantity = (value: number): `0x${string}` => `0x${value.toString(16)}`;

const redact = (value: unknown, secrets: string[]): string => {
  let message = value instanceof Error ? value.message : String(value);
  for (const secret of secrets) if (secret) message = message.split(secret).join('[redacted]');
  message = message
    .replace(/(api[-_ ]?key|api[-_ ]?token|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
    .replace(/(https?:\/\/[^\s/]+)\/\S+/gi, '$1/***');
  return (message.trim().replace(/\s+/g, ' ') || 'HyperSync request failed').slice(0, 200);
};

const wireLog = (log: Log): WireLog => {
  const topics = [...log.topics];
  while (topics.at(-1) == null) topics.pop();
  return {
    address: log.address,
    topics,
    data: log.data,
    blockNumber: integer(log.blockNumber) ? quantity(log.blockNumber) : log.blockNumber,
    transactionHash: log.transactionHash,
    logIndex: integer(log.logIndex) ? quantity(log.logIndex) : log.logIndex,
  };
};

/** Streaming Envio HyperSync source. Pages are folded immediately; collect() is intentionally never used. */
export function createHypersyncLogSource(options: HypersyncSourceOptions): AccountLogSource {
  const name = options.name ?? 'hypersync';
  const state: AccountLogSourceStats = { requests: 0, chunks: 0, logs: 0, failures: 0, fallbacks: 0 };
  const active = new Set<HypersyncStream>();
  let client: Promise<HypersyncFactoryResult> | undefined;
  let closed = false;
  const secrets = [options.apiToken, options.url];

  const factory = options.clientFactory ?? (async () => {
    const { HypersyncClient, JoinMode } = await import('@envio-dev/hypersync-client');
    return {
      client: new HypersyncClient({
        url: options.url,
        apiToken: options.apiToken,
        httpReqTimeoutMillis: options.timeoutMs,
      }),
      joinNothing: JoinMode.JoinNothing,
    };
  });
  const getClient = (): Promise<HypersyncFactoryResult> => {
    if (closed) throw new Error('HyperSync source is closed');
    return client ??= factory();
  };

  const scan = async (
    account: Address,
    range: BlockRange,
    timedOut: () => boolean,
    setOwnedStream: (stream: HypersyncStream | undefined) => void,
  ) => {
    const exclusive = range.toBlock + 1n;
    if (range.fromBlock < 0n || exclusive > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('HyperSync block range is not safely representable');
    const { client: hs, joinNothing } = await getClient();
    const paddedAccount = pad(account, { size: 32 }).toLowerCase();
    const query: Query = {
      fromBlock: Number(range.fromBlock),
      toBlock: Number(exclusive),
      joinMode: joinNothing,
      logs: [
        { include: { topics: [[TRANSFER_TOPIC], [paddedAccount]] } },
        { include: { topics: [[TRANSFER_TOPIC], [], [paddedAccount]] } },
      ],
      fieldSelection: { log: ['BlockNumber', 'LogIndex', 'TransactionHash', 'Address', 'Topic0', 'Topic1', 'Topic2', 'Topic3', 'Data'] },
    };
    const aggregate = emptyAggregate();
    let stream: HypersyncStream | undefined;
    let pages = 0;
    let nextBlock = Number(range.fromBlock);
    try {
      stream = await hs.stream(query, {});
      if (timedOut()) throw new Error('HyperSync request timed out');
      setOwnedStream(stream);
      active.add(stream);
      for (;;) {
        const response = await stream.recv();
        if (!response) break;
        pages += 1;
        if (!integer(response.archiveHeight) || BigInt(response.archiveHeight) < range.toBlock) {
          throw new Error('HyperSync response archive height is below the requested range');
        }
        if (!integer(response.nextBlock) || response.nextBlock < nextBlock || response.nextBlock > Number(exclusive)) {
          throw new Error('HyperSync returned a malformed next block');
        }
        if (!response.data || !Array.isArray(response.data.logs)) throw new Error('HyperSync returned malformed log data');
        nextBlock = response.nextBlock;
        for (const raw of response.data.logs) {
          if (!raw || typeof raw !== 'object' || !Array.isArray(raw.topics)) throw new Error('HyperSync returned a malformed log');
          const event = decodeTransferLog(wireLog(raw));
          if (event) addTransfer(aggregate, account, event);
        }
      }
    } finally {
      if (stream) {
        setOwnedStream(undefined);
        active.delete(stream);
        await stream.close().catch(() => {});
      }
    }
    if (pages === 0 || nextBlock < Number(exclusive)) throw new Error('HyperSync stream ended before the requested range completed');
    return { aggregate, calls: 1, logs: aggregateLogCount(aggregate), heavy: false };
  };

  return {
    name,
    preferredMaxRange: options.preferredMaxRange,
    async scanAccount(account, range) {
      state.chunks += 1;
      state.requests += 1;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let expired = false;
      let ownedStream: HypersyncStream | undefined;
      const work = scan(account, range, () => expired, (stream) => { ownedStream = stream; });
      const timeout = new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          expired = true;
          if (ownedStream) void ownedStream.close().catch(() => {});
          reject(new Error(`HyperSync request timed out after ${options.timeoutMs} ms`));
        }, options.timeoutMs);
      });
      try {
        const result = await Promise.race([work, timeout]);
        state.logs += result.logs;
        return result;
      } catch (error) {
        state.failures += 1;
        state.lastError = redact(error, secrets);
        throw new Error(state.lastError);
      } finally {
        clearTimeout(timer);
      }
    },
    stats: () => ({ [name]: { ...state } }),
    async close() {
      closed = true;
      await Promise.all([...active].map((stream) => stream.close().catch(() => {})));
      active.clear();
      client = undefined; // HypersyncClient 1.4.1 has no close(); dropping it releases the native handle.
    },
  };
}
