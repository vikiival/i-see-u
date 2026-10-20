import { afterEach, assert, expect, test, vi } from 'vitest';
import { mainnet } from 'viem/chains';
import type { RpcClient, RpcStats } from '../src/core/types.ts';
import { createHeadTracker } from '../src/chains/evm/head.ts';
import { createRpcClient } from '../src/rpc/client.ts';
import { staged } from './helpers.ts';
import { RpcError, classify } from '../src/rpc/errors.ts';

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

test('classify recognizes provider, HTTP, and network failures', () => {
  const cases: Array<[number | undefined, unknown, string]> = [
    [undefined, { code: -32010, message: 'log requests limited to 1/sec' }, 'rate_limited'],
    [undefined, { code: -32007, message: '5/second request limit reached' }, 'rate_limited'],
    [429, { message: 'anything' }, 'rate_limited'],
    [413, { message: 'anything' }, 'too_large'],
    [401, { message: 'anything' }, 'fatal'],
    [403, { message: 'anything' }, 'fatal'],
    [
      undefined,
      {
        code: -32005,
        message: 'query returned more than 10000 results. Try with this block range [0x18E2D98, 0x18E4B00]',
      },
      'too_large',
    ],
    [
      undefined,
      {
        code: -32602,
        message: 'Query returned more than 20000 results. Try with this block range [0x18e2d98, 0x18e411f]',
      },
      'too_large',
    ],
    [undefined, { message: 'ranges over 10000 blocks are not supported' }, 'too_large'],
    [undefined, { message: 'Max range: 800' }, 'too_large'],
    [undefined, { message: 'block range too large' }, 'too_large'],
    [undefined, { message: 'response size exceeded' }, 'too_large'],
    [undefined, { message: 'method not available' }, 'unsupported'],
    [undefined, { message: 'Archive requests require a personal token' }, 'unsupported'],
    [undefined, { message: 'Please specify an address in your request' }, 'unsupported'],
    [undefined, { message: 'Method not found' }, 'unsupported'],
    [500, { message: 'server broke' }, 'transient'],
    [525, { message: 'TLS failed' }, 'transient'],
    [undefined, new Error('request timeout'), 'transient'],
    [undefined, { code: 'ECONNRESET' }, 'transient'],
    [undefined, { message: 'header not found' }, 'transient'],
    [undefined, { message: 'unknown block' }, 'transient'],
    [undefined, { message: 'header for hash not found' }, 'transient'],
    [undefined, { message: 'execution reverted' }, 'fatal'],
    [undefined, { code: -32602, message: 'invalid params' }, 'fatal'],
  ];

  for (const [status, error, expected] of cases) {
    expect(classify(status, error), String(error)).toBe(expected);
  }

  const parsed = new RpcError('too_large', 'Try with this block range [0x18E2D98, 0x18E4B00]');
  expect(parsed.suggestedRange).toEqual({ fromBlock: 0x18e2d98n, toBlock: 0x18e4b00n });
});

const rpcStats = (): RpcStats => ({ requests: 0, attempts: 0, byMethod: {}, errors: {}, endpoints: [] });

test('client retries on a fallback and opens the failing endpoint breaker', async () => {
  let failedCalls = 0;
  let goodCalls = 0;
  vi.spyOn(Math, 'random').mockReturnValue(0);
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string | URL | Request) => {
      if (String(input).startsWith('https://primary.example')) {
        failedCalls += 1;

        return new Response(
          JSON.stringify({ jsonrpc: '2.0', id: 1, error: { code: -32000, message: 'temporarily unavailable' } }),
          {
            status: 503,
            headers: { 'content-type': 'application/json' },
          },
        );
      }

      goodCalls += 1;

      return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: '0x2a' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }),
  );

  const client = createRpcClient({
    urls: ['https://primary.example/api-key?secret=yes', 'https://fallback.example/other-key'],
    timeoutMs: 1_000,
    maxAttempts: 2,
    maxRps: 1_000,
  });

  for (let i = 0; i < 5; i += 1) {
    expect(await client.request('eth_blockNumber', [])).toBe('0x2a');
  }

  expect(failedCalls).toBe(5);
  expect(goodCalls).toBe(5);
  expect(client.stats().endpoints[0].state).toBe('open');

  expect(await client.request('eth_blockNumber', [])).toBe('0x2a');
  expect(failedCalls, 'open primary is skipped').toBe(5);
  expect(goodCalls).toBe(6);
  expect(client.stats().endpoints.map((endpoint) => endpoint.url)).toEqual([
    'https://primary.example/***',
    'https://fallback.example/***',
  ]);
  expect(client.stats().attempts).toBe(11);
  expect(client.stats().errors.transient).toBe(5);
});

test('finalized head is single-flight, cached, records the latest number, and serves stale on failure', async () => {
  let calls = 0;
  let fail = false;

  const rpc: RpcClient = {
    async request<T>(_method: string, params: unknown[]): Promise<T> {
      calls += 1;

      if (fail) {
        throw new Error('offline');
      }

      await Promise.resolve();
      const tag = params[0];
      const number = tag === 'latest' ? '0xa' : '0x8';

      return staged<T>({
        number,
        hash: `0x${number.slice(2).padStart(64, 'a')}`,
        parentHash: `0x${number.slice(2).padStart(64, 'b')}`,
        timestamp: '0x64',
      });
    },
    stats: rpcStats,
  };

  const tracker = createHeadTracker(rpc, { cacheMs: 60_000, maxNoProgressMs: 60_000, chain: mainnet });
  const [first, concurrent] = await Promise.all([tracker.finalizedHead(), tracker.finalizedHead()]);
  expect(first.block.number).toBe(8n);
  expect(concurrent).toEqual(first);
  expect(calls, 'one finalized + one background latest').toBe(2);
  expect(await tracker.finalizedHead()).toEqual(first);
  expect(calls).toBe(2);
  expect(tracker.latestBlockNumber()).toBe(10n);
  expect(first.stale).toBe(false);

  const uncached = createHeadTracker(rpc, { cacheMs: 0, maxNoProgressMs: 60_000, chain: mainnet });
  const prior = await uncached.finalizedHead();
  fail = true;
  const fallback = await uncached.finalizedHead();
  expect(fallback.block).toEqual(prior.block);
  expect(fallback.lastProgressAt).toBe(prior.lastProgressAt);
  expect(fallback.stale).toBe(true);
});

test('finalized head never moves backwards and marks a lagging observation stale', async () => {
  let number = 10n;
  const tags: unknown[] = [];

  const block = (value: bigint) => ({
    number: `0x${value.toString(16)}`,
    hash: `0x${value.toString(16).padStart(64, 'a')}`,
    parentHash: `0x${(value - 1n).toString(16).padStart(64, 'b')}`,
    timestamp: `0x${(1_000n + value).toString(16)}`,
  });

  const rpc: RpcClient = {
    async request<T>(_method: string, params: unknown[]): Promise<T> {
      tags.push(params[0]);

      return staged<T>(block(number));
    },
    stats: rpcStats,
  };

  const tracker = createHeadTracker(rpc, { cacheMs: 0, maxNoProgressMs: 60_000, chain: mainnet });
  const first = await tracker.finalizedHead();
  number = 9n;
  const lagging = await tracker.finalizedHead();
  expect(lagging.block).toEqual(first.block);
  expect(lagging.lastProgressAt).toBe(first.lastProgressAt);
  expect(lagging.stale).toBe(true);
  // Recorded rather than asserted in the mock: a throw there would be swallowed by the background 'latest' refresh.
  expect(tags).toContain('finalized');
  expect(
    tags.every((tag) => tag === 'finalized' || tag === 'latest'),
    String(tags),
  ).toBe(true);
});

test('result validation fails over from a null head response', async () => {
  const called: string[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      // finalizedHead() also refreshes 'latest' in the background; track only the finalized lookups.
      const [tag] = JSON.parse(String(init?.body)).params;

      if (tag === 'finalized') {
        called.push(String(input));
      }

      const result = String(input).startsWith('https://primary.example')
        ? null
        : { number: '0xa', hash: '0xabc', parentHash: '0xdef', timestamp: '0x64' };

      return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }),
  );
  vi.spyOn(Math, 'random').mockReturnValue(0);

  const client = createRpcClient({
    urls: ['https://primary.example/key', 'https://fallback.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 2,
    maxRps: 1_000,
  });

  const tracker = createHeadTracker(client, { cacheMs: 0, maxNoProgressMs: 60_000, chain: mainnet });
  const observation = await tracker.finalizedHead();
  expect(observation.block.number).toBe(10n);
  expect(called.length).toBe(2);
  expect(called[1]).toMatch(/^https:\/\/fallback\.example/);
  // >= 1: the background 'latest' refresh may or may not have hit the primary yet.
  expect(client.stats().errors.transient ?? 0).toBeGreaterThanOrEqual(1);
});

test('an unchanged finalized block becomes stale and progress resets on advance', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000);
  let number = 10n;

  const rpc: RpcClient = {
    async request<T>(): Promise<T> {
      return staged<T>({
        number: `0x${number.toString(16)}`,
        hash: `0x${number.toString(16).padStart(64, 'a')}`,
        parentHash: `0x${(number - 1n).toString(16).padStart(64, 'b')}`,
        timestamp: '0x64',
      });
    },
    stats: rpcStats,
  };

  const tracker = createHeadTracker(rpc, { cacheMs: 0, maxNoProgressMs: 100, chain: mainnet });
  const first = await tracker.finalizedHead();
  await vi.advanceTimersByTimeAsync(101);
  const stuck = await tracker.finalizedHead();
  expect(stuck.stale).toBe(true);
  expect(stuck.lastProgressAt).toBe(first.lastProgressAt);

  number = 11n;
  await vi.advanceTimersByTimeAsync(1);
  const advanced = await tracker.finalizedHead();
  expect(advanced.stale).toBe(false);
  expect(advanced.lastProgressAt).toBe(1_102);
});

interface RpcEnvelope {
  jsonrpc: '2.0';
  id: number;
  result?: string | string[];
  error?: { code: number; message: string };
}

const json = (body: RpcEnvelope, status = 200, headers: Record<string, string> = {}): Response =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...headers } });

/** The next link of an error's cause chain, if any. */
const causeOf = (value: NonNullable<unknown>): NonNullable<unknown> | undefined => {
  if (!(value instanceof Object) || !('cause' in value)) {
    return undefined;
  }

  const { cause } = value;

  return cause === null || cause === undefined ? undefined : cause;
};

const stubFetch = (respond: (url: string, call: number) => Response) => {
  const urls: string[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string | URL | Request) => {
      urls.push(String(input));

      return respond(String(input), urls.length);
    }),
  );

  return urls;
};

test('-32005 with "more than 10000 results" is too_large with the suggested range, not retried', async () => {
  const urls = stubFetch(() =>
    json({
      jsonrpc: '2.0',
      id: 1,
      error: { code: -32005, message: 'query returned more than 10000 results. Try with this block range [0x1, 0x2].' },
    }),
  );

  const client = createRpcClient({
    urls: ['https://a.example/key', 'https://b.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 4,
    maxRps: 1_000,
  });

  const error = await client.request('eth_getLogs', []).catch((e: Error) => e);
  assert.instanceOf(error, RpcError);
  expect(error).toMatchObject({ kind: 'too_large', code: -32005, suggestedRange: { fromBlock: 1n, toBlock: 2n } });
  expect(error.cause).toMatchObject({ code: -32005 });
  expect(urls.length).toBe(1);
  expect(client.stats().attempts).toBe(1);
});

test('Quicknode -32010 is retried as rate_limited', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0);
  stubFetch((_url, call) =>
    call === 1
      ? json({ jsonrpc: '2.0', id: 1, error: { code: -32010, message: 'eth_getLogs is limited to 1/sec' } })
      : json({ jsonrpc: '2.0', id: 1, result: [] }),
  );

  const client = createRpcClient({
    urls: ['https://only.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 3,
    maxRps: 1_000,
  });

  expect(await client.request('eth_getLogs', [])).toEqual([]);
  expect(client.stats()).toMatchObject({ attempts: 2, errors: { rate_limited: 1 } });
});

test('HTTP 429 Retry-After is honoured only up to the cap', async () => {
  vi.useFakeTimers();
  vi.spyOn(Math, 'random').mockReturnValue(0);

  const urls = stubFetch((_url, call) =>
    call === 1
      ? new Response('Too Many Requests', { status: 429, headers: { 'retry-after': '3600' } })
      : json({ jsonrpc: '2.0', id: 1, result: '0x1' }),
  );

  const client = createRpcClient({
    urls: ['https://only.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 2,
    maxRps: 1_000,
  });

  let settled = false;

  const pending = client.request('eth_blockNumber', []).finally(() => {
    settled = true;
  });

  await vi.advanceTimersByTimeAsync(9_999);
  expect(settled).toBe(false);
  expect(urls.length).toBe(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(await pending).toBe('0x1');
  expect(client.stats().errors).toEqual({ rate_limited: 1 });
});

test('HTTP 401 is fatal: one attempt, no failover', async () => {
  const urls = stubFetch(() => new Response('unauthorized', { status: 401 }));

  const client = createRpcClient({
    urls: ['https://a.example/key', 'https://b.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 4,
    maxRps: 1_000,
  });

  await expect(client.request('eth_blockNumber', [])).rejects.toMatchObject({
    name: 'RpcError',
    kind: 'fatal',
    message: 'HTTP 401',
  });
  expect(urls).toEqual(['https://a.example/key']);
});

test('endpoint path and key never leak into thrown errors or stats', async () => {
  const secret = 'SECRET-KEY-123';
  vi.spyOn(Math, 'random').mockReturnValue(0);

  const responses = [
    () => new Response('bad gateway', { status: 502 }),
    () => json({ jsonrpc: '2.0', id: 1, error: { code: -32000, message: 'execution reverted' } }),
    () => {
      throw new TypeError('fetch failed', { cause: { code: 'ECONNRESET' } });
    },
    () => new Response('not json', { status: 200 }),
  ];

  const errors: RpcError[] = [];

  for (const respond of responses) {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => respond()),
    );

    const client = createRpcClient({
      urls: [`https://node.example/${secret}?token=${secret}`],
      timeoutMs: 1_000,
      maxAttempts: 1,
      maxRps: 1_000,
    });

    const error = await client.request('eth_call', []).catch((e: Error) => e);
    assert.instanceOf(error, RpcError);
    errors.push(error);
    expect(JSON.stringify(client.stats())).not.toContain(secret);
  }

  expect(errors.map((error) => error.kind)).toEqual(['transient', 'fatal', 'transient', 'fatal']);

  // Walk the whole cause chain (what a logger would print), not just the top-level message.
  for (const error of errors) {
    for (let current: NonNullable<unknown> | undefined = error; current; current = causeOf(current)) {
      const text = current instanceof Error ? `${current.message} ${current.stack}` : JSON.stringify(current);
      expect(text).not.toContain(secret);
    }
  }
});

test('a viem transport timeout is transient and fails over', async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0);
  const urls: string[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: string, init?: RequestInit) => {
      urls.push(input);

      if (!input.startsWith('https://slow.example')) {
        return Promise.resolve(json({ jsonrpc: '2.0', id: 1, result: '0x1' }));
      }

      return new Promise((_resolve, reject) =>
        init?.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError'))),
      );
    }),
  );

  const client = createRpcClient({
    urls: ['https://slow.example/key', 'https://fast.example/key'],
    timeoutMs: 20,
    maxAttempts: 2,
    maxRps: 1_000,
  });

  expect(await client.request('eth_blockNumber', [])).toBe('0x1');
  expect(urls.map((url) => new URL(url).host)).toEqual(['slow.example', 'fast.example']);
  expect(client.stats().errors).toEqual({ transient: 1 });
});

test('failFastOnTimeout throws a flagged timeout after one attempt without tripping the breaker', async () => {
  let calls = 0;
  vi.stubGlobal(
    'fetch',
    vi.fn((_input: string, init?: RequestInit) => {
      calls += 1;

      return new Promise((_resolve, reject) =>
        init?.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError'))),
      );
    }),
  );
  const client = createRpcClient({ urls: ['https://slow.example/key'], timeoutMs: 10, maxAttempts: 3, maxRps: 1_000 });

  for (let i = 0; i < 6; i += 1) {
    await expect(client.request('eth_getLogs', [], { failFastOnTimeout: true })).rejects.toMatchObject({
      kind: 'transient',
      timeout: true,
    });
  }

  expect(calls).toBe(6);
  expect(client.stats().errors).toEqual({ transient: 6 });
  expect(client.stats().endpoints[0]).toMatchObject({ state: 'closed', failures: 0 });
});

test('onAttempt reports duration and the uncompressed Content-Length', async () => {
  const body = JSON.stringify({ jsonrpc: '2.0', id: 1, result: [] });
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => json({ jsonrpc: '2.0', id: 1, result: [] }, 200, { 'content-length': String(body.length) })),
  );

  const client = createRpcClient({
    urls: ['https://only.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 1,
    maxRps: 1_000,
  });

  const attempts: unknown[] = [];
  await client.request('eth_getLogs', [], { onAttempt: (attempt) => attempts.push(attempt) });
  expect(attempts).toEqual([{ ok: true, durationMs: expect.any(Number), bytes: body.length }]);
});

/** fetch that never answers until its (viem-merged) signal aborts, like a slow provider. */
const hangingFetch = () => {
  const calls: (AbortSignal | undefined)[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((_input: string, init?: RequestInit) => {
      calls.push(init?.signal ?? undefined);

      return new Promise((_resolve, reject) =>
        init?.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError'))),
      );
    }),
  );

  return calls;
};

test('aborting the signal cancels the in-flight fetch and makes no further attempt', async () => {
  const calls = hangingFetch();

  const client = createRpcClient({
    urls: ['https://a.example/key', 'https://b.example/key'],
    timeoutMs: 60_000,
    maxAttempts: 4,
    maxRps: 1_000,
  });

  const controller = new AbortController();
  const pending = client.request('eth_getLogs', [], { signal: controller.signal });
  await vi.waitFor(() => expect(calls.length).toBe(1));

  const reason = new Error('caller gave up');
  controller.abort(reason);
  await expect(pending).rejects.toBe(reason);
  expect(calls[0]?.aborted).toBe(true);
  expect(calls.length).toBe(1);
  // A cancellation is not an endpoint failure.
  expect(client.stats()).toMatchObject({ attempts: 1, errors: {} });
  expect(client.stats().endpoints.map((endpoint) => endpoint.failures)).toEqual([0, 0]);
});

test('aborting during the retry backoff stops at once without another attempt', async () => {
  const urls = stubFetch(() => new Response('Too Many Requests', { status: 429, headers: { 'retry-after': '5' } }));

  const client = createRpcClient({
    urls: ['https://only.example/key'],
    timeoutMs: 1_000,
    maxAttempts: 3,
    maxRps: 1_000,
  });

  const controller = new AbortController();
  const pending = client.request('eth_blockNumber', [], { signal: controller.signal });
  await vi.waitFor(() => expect(client.stats().errors).toEqual({ rate_limited: 1 }));

  const startedAt = Date.now();
  controller.abort();
  await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
  expect(Date.now() - startedAt).toBeLessThan(1_000);
  expect(urls.length).toBe(1);
});

test('an already aborted signal makes no attempt, and the token-bucket wait is abortable', async () => {
  const urls = stubFetch(() => json({ jsonrpc: '2.0', id: 1, result: '0x1' }));
  const client = createRpcClient({ urls: ['https://only.example/key'], timeoutMs: 1_000, maxAttempts: 1, maxRps: 1 });
  await expect(client.request('eth_blockNumber', [], { signal: AbortSignal.abort() })).rejects.toMatchObject({
    name: 'AbortError',
  });
  expect(urls.length).toBe(0);

  // maxRps 1: the first request takes the only token, the second waits ~1 s for the next one.
  expect(await client.request('eth_blockNumber', [])).toBe('0x1');
  const controller = new AbortController();
  const waiting = client.request('eth_blockNumber', [], { signal: controller.signal });
  controller.abort();
  await expect(waiting).rejects.toMatchObject({ name: 'AbortError' });
  expect(urls.length).toBe(1);
  expect(client.stats().attempts).toBe(1);
});
