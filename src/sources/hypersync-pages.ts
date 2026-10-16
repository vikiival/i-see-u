import type { Query, QueryResponse, QueryResponseWithRateLimit } from '@envio-dev/hypersync-client';

import { isNumber } from '../core/parse.ts';

/** The one HypersyncClient method the repo uses. Never `stream()` (see README): it burns the free-tier quota. */
export interface HypersyncPager {
  getWithRateLimit(query: Query): Promise<QueryResponseWithRateLimit>;
}

export interface PageOptions {
  /** Give up on one request after this long. The native request cannot be cancelled; it is abandoned, never awaited. */
  idleMs: number;
  /** Called before sleeping for the quota window (free tier: 15 requests per 60 s). */
  onRateLimit?: (waitMs: number) => void;
  /** Called after every HTTP request (including 429s). */
  onRequest?: () => void;
  sleep?: (ms: number) => Promise<void>;
  /** Stop paginating early (e.g. shutdown); the generator then throws. */
  aborted?: () => boolean;
}

const defaultSleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const integer = (value: unknown): value is number => isNumber(value) && Number.isSafeInteger(value) && value >= 0;

/**
 * Sequential paginated `getWithRateLimit` over [query.fromBlock, query.toBlock): each response is truncated by the
 * server (~28k-150k logs), so the next request continues at `nextBlock`. A 429 sleeps until the quota window resets
 * instead of parking silently inside the native client. Completes only when `nextBlock` reaches `toBlock`, so an early
 * end can never look complete.
 */
export async function* hypersyncPages(client: HypersyncPager, query: Query, o: PageOptions): AsyncGenerator<QueryResponse> {
  const sleep = o.sleep ?? defaultSleep;
  const end = query.toBlock;

  if (end === undefined || !integer(query.fromBlock) || !integer(end) || end <= query.fromBlock) throw new Error('HyperSync query needs a bounded block range');
  let next = query.fromBlock;

  while (next < end) {
    if (o.aborted?.()) throw new Error('HyperSync pagination aborted');
    let timer: ReturnType<typeof setTimeout> | undefined;
    const request = client.getWithRateLimit({ ...query, fromBlock: next });
    request.catch(() => {}); // abandoned after a timeout: never awaited again, never an unhandled rejection
    o.onRequest?.();

    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`HyperSync request timed out after ${o.idleMs} ms`)), o.idleMs);
    });

    let result: QueryResponseWithRateLimit;

    try {
      result = await Promise.race([request, timeout]);
    } finally {
      clearTimeout(timer);
    }

    const { response, rateLimit } = result;

    if (!response) {
      const waitMs = ((integer(rateLimit?.resetSecs) ? rateLimit.resetSecs : 30) + 1) * 1_000;
      o.onRateLimit?.(waitMs);
      await sleep(waitMs);
      continue;
    }

    if (!integer(response.archiveHeight) || response.archiveHeight < end - 1) throw new Error('HyperSync archive height is below the requested range');

    if (!integer(response.nextBlock) || response.nextBlock <= next || response.nextBlock > end) throw new Error('HyperSync page did not advance');

    if (!response.data || !Array.isArray(response.data.logs) || !Array.isArray(response.data.blocks)) throw new Error('HyperSync returned malformed data');
    next = response.nextBlock;

    yield response;

    // Out of quota for this window: wait now rather than spend a request on a 429.
    if (next < end && integer(rateLimit?.remaining) && integer(rateLimit.cost) && rateLimit.remaining < rateLimit.cost) {
      const waitMs = ((integer(rateLimit.resetSecs) ? rateLimit.resetSecs : 30) + 1) * 1_000;
      o.onRateLimit?.(waitMs);
      await sleep(waitMs);
    }
  }
}

/** Never put an API token or full URL into logs, health output or errors. */
export const redactSecrets = (raw: string, secrets: string[]): string => {
  let message = raw;

  for (const secret of secrets) if (secret) message = message.split(secret).join('[redacted]');
  message = message
    .replace(/(api[-_ ]?key|api[-_ ]?token|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
    .replace(/(https?:\/\/[^\s/]+)\/\S+/gi, '$1/***');

  return (message.trim().replace(/\s+/g, ' ') || 'HyperSync request failed').slice(0, 200);
};
