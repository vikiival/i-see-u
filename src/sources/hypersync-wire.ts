import type { Log, Query } from '@envio-dev/hypersync-client';

import { isNumber } from '../core/parse.ts';
import type { WireLog } from '../chains/evm/logs.ts';
import type { HypersyncPager } from './hypersync-pages.ts';

export interface HypersyncFactoryResult {
  client: HypersyncPager;
  joinNothing: number;
}

/** The log fields every HyperSync query selects. */
export const LOG_FIELDS: NonNullable<NonNullable<Query['fieldSelection']>['log']> = [
  'BlockNumber',
  'BlockHash',
  'LogIndex',
  'TransactionHash',
  'Address',
  'Topic0',
  'Topic1',
  'Topic2',
  'Topic3',
  'Data',
];

const integer = (value: unknown): value is number => isNumber(value) && Number.isSafeInteger(value) && value >= 0;
const quantity = (value: number): `0x${string}` => `0x${value.toString(16)}`;

/** The native client, configured to report 429s to us instead of sleeping silently inside a request. */
export async function nativeHypersyncClient(
  url: string,
  apiToken: string,
  timeoutMs: number,
): Promise<HypersyncFactoryResult> {
  const { HypersyncClient, JoinMode } = await import('@envio-dev/hypersync-client');

  return {
    client: new HypersyncClient({
      url,
      apiToken,
      httpReqTimeoutMillis: timeoutMs,
      proactiveRateLimitSleep: false,
      maxNumRetries: 3,
    }),
    joinNothing: JoinMode.JoinNothing,
  };
}

/** A HyperSync log in eth_getLogs wire shape: hex quantities, trailing null topics dropped. */
export const wireLog = (log: Log): WireLog => {
  const topics = [...log.topics];

  while (topics.length > 0 && topics.at(-1) == null) {
    topics.pop();
  }

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
