import { pad } from 'viem';
import type { Log, Query } from '@envio-dev/hypersync-client';

import { addTransfer, aggregateLogCount, emptyAggregate } from '../core/aggregate.ts';
import type { AccountLogSource, AccountLogSourceStats } from '../core/types.ts';
import { isNumber, isObject, thrownMessage } from '../core/parse.ts';
import { decodeTransferLog } from '../chains/evm/logs.ts';
import type { WireLog } from '../chains/evm/logs.ts';
import { TRANSFER_TOPIC } from '../chains/evm/logs.ts';
import { hypersyncPages, redactSecrets } from './hypersync-pages.ts';
import type { HypersyncPager } from './hypersync-pages.ts';

export interface HypersyncFactoryResult {
  client: HypersyncPager;
  joinNothing: number;
}

export interface HypersyncSourceOptions {
  url: string;
  apiToken: string;
  preferredMaxRange: bigint;
  /** Per-request idle timeout (not a whole-chunk timeout: a chunk may legitimately wait for the quota window). */
  timeoutMs: number;
  name?: string;
  /** Tests inject a fake; production dynamically imports the native addon on first use. */
  clientFactory?: () => Promise<HypersyncFactoryResult>;
  sleep?: (ms: number) => Promise<void>;
}

const integer = (value: unknown): value is number => isNumber(value) && Number.isSafeInteger(value) && value >= 0;
const quantity = (value: number): `0x${string}` => `0x${value.toString(16)}`;

/** The native client, configured to report 429s to us instead of sleeping silently inside a request. */
export async function nativeHypersyncClient(url: string, apiToken: string, timeoutMs: number): Promise<HypersyncFactoryResult> {
  const { HypersyncClient, JoinMode } = await import('@envio-dev/hypersync-client');

  return {
    client: new HypersyncClient({ url, apiToken, httpReqTimeoutMillis: timeoutMs, proactiveRateLimitSleep: false, maxNumRetries: 3 }),
    joinNothing: JoinMode.JoinNothing,
  };
}

export const wireLog = (log: Log): WireLog => {
  const topics = [...log.topics];

  while (topics.at(-1) == null) topics.pop();

  return {
    address: log.address,
    topics,
    data: log.data,
    blockNumber: integer(log.blockNumber) ? quantity(log.blockNumber) : log.blockNumber,
    blockHash: log.blockHash,
    transactionHash: log.transactionHash,
    logIndex: integer(log.logIndex) ? quantity(log.logIndex) : log.logIndex,
  };
};

/** Paginated Envio HyperSync diagnostic source (sequential `getWithRateLimit`, never `stream()` or `collect()`). */
export function createHypersyncLogSource(options: HypersyncSourceOptions): AccountLogSource {
  const name = options.name ?? 'hypersync';
  const state: AccountLogSourceStats = { requests: 0, chunks: 0, logs: 0, failures: 0, fallbacks: 0 };
  let client: Promise<HypersyncFactoryResult> | undefined;
  let closed = false;
  const secrets = [options.apiToken, options.url];
  const factory = options.clientFactory ?? (() => nativeHypersyncClient(options.url, options.apiToken, options.timeoutMs));

  const getClient = (): Promise<HypersyncFactoryResult> => {
    if (closed) throw new Error('HyperSync source is closed');

    return client ??= factory();
  };

  return {
    name,
    preferredMaxRange: options.preferredMaxRange,
    async scanAccount(account, range) {
      state.chunks += 1;

      try {
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
          fieldSelection: { log: ['BlockNumber', 'BlockHash', 'LogIndex', 'TransactionHash', 'Address', 'Topic0', 'Topic1', 'Topic2', 'Topic3', 'Data'] },
        };

        const aggregate = emptyAggregate();

        const pages = hypersyncPages(hs, query, {
          idleMs: options.timeoutMs, sleep: options.sleep, aborted: () => closed, onRequest: () => { state.requests += 1; },
        });

        for await (const response of pages) {
          for (const raw of response.data.logs) {
            if (!isObject(raw) || !Array.isArray(raw.topics)) throw new Error('HyperSync returned a malformed log');
            const event = decodeTransferLog(wireLog(raw));

            if (event) addTransfer(aggregate, account, event);
          }
        }

        const result = { aggregate, calls: 1, logs: aggregateLogCount(aggregate), heavy: false };
        state.logs += result.logs;

        return result;
      } catch (error) {
        state.failures += 1;
        state.lastError = redactSecrets(thrownMessage(error), secrets);
        throw new Error(state.lastError);
      }
    },
    stats: () => ({ [name]: { ...state } }),
    async close() {
      closed = true;
      client = undefined; // HypersyncClient 1.4.1 has no close(); dropping it releases the native handle.
    },
  };
}
