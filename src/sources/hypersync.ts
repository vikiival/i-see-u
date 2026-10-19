import { pad } from 'viem';
import type { Query } from '@envio-dev/hypersync-client';

import { addTransfer, aggregateLogCount, emptyAggregate } from '../core/aggregate.ts';
import type { AccountLogSource, AccountLogSourceStats } from '../core/types.ts';
import { isObject, thrownMessage } from '../core/parse.ts';
import { decodeTransferLog } from '../chains/evm/logs.ts';
import { TRANSFER_TOPIC } from '../chains/evm/logs.ts';
import { hypersyncPages, redactSecrets } from './hypersync-pages.ts';
import { LOG_FIELDS, nativeHypersyncClient, wireLog } from './hypersync-wire.ts';
import type { HypersyncFactoryResult } from './hypersync-wire.ts';

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
          fieldSelection: { log: LOG_FIELDS },
        };

        const aggregate = emptyAggregate();
        let calls = 0;

        const pages = hypersyncPages(hs, query, {
          idleMs: options.timeoutMs, sleep: options.sleep, aborted: () => closed, onRequest: () => { calls += 1; state.requests += 1; },
        });

        for await (const response of pages) {
          for (const raw of response.data.logs) {
            if (!isObject(raw) || !Array.isArray(raw.topics)) throw new Error('HyperSync returned a malformed log');
            const event = decodeTransferLog(wireLog(raw));

            if (event) addTransfer(aggregate, account, event);
          }
        }

        const result = { aggregate, calls, logs: aggregateLogCount(aggregate), heavy: false };
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
