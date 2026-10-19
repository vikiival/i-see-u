import { afterEach, expect, test, vi } from 'vitest';
import { mainnet } from 'viem/chains';

import { createApp } from '../src/api/routes.ts';
import type { RouteDeps } from '../src/api/routes.ts';
import { readyMode } from '../src/api/ops.ts';
import { createBootstrapStore } from '../src/bootstrap/store.ts';
import { createHeadTracker } from '../src/chains/evm/head.ts';
import { createMetrics } from '../src/core/metrics.ts';
import { startBackground } from '../src/core/runtime.ts';
import { createFollower } from '../src/follower/follower.ts';
import { createRpcClient } from '../src/rpc/client.ts';
import { address, FakeChain, gate } from './fake-chain.ts';
import { header, stack } from './stack.ts';

afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

const opsApp = (s: Awaited<ReturnType<typeof stack>>, overrides: Partial<RouteDeps> = {}, requestLog = false) => createApp({
  adapters: { ethereum: s.adapter }, discovery: s.discovery, followers: { ethereum: s.follower },
  balances: { ethereum: s.balances }, tails: s.tail ? { ethereum: s.tail } : undefined, requestRpc: s.requestRpc,
  startedAt: s.clock.at, rpcStats: () => s.chain.rpc.stats(), opsSnapshot: (network) => s.store.opsSnapshot(network), ...overrides,
}, { requestLog });

/** Parse the complete 0.0.4 grammar used here, and check metadata and unique series. */
const parseExposition = (body: string): Map<string, number> => {
  const samples = new Map<string, number>();
  const help = new Set<string>();
  const types = new Map<string, string>();

  for (const line of body.trim().split('\n')) {
    if (!line) continue;
    const description = /^# HELP ([a-zA-Z_:][a-zA-Z0-9_:]*) .+$/.exec(line);
    const type = /^# TYPE ([a-zA-Z_:][a-zA-Z0-9_:]*) (counter|gauge|histogram)$/.exec(line);

    if (description) { help.add(description[1]); continue; }

    if (type) { types.set(type[1], type[2]); continue; }

    const sample = /^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\\n]|\\[\\"n])*"(?:,[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\\n]|\\[\\"n])*")*\})?) (-?(?:\d+(?:\.\d+)?(?:e[+-]?\d+)?|Inf)|NaN)$/.exec(line);
    expect(sample, line).not.toBeNull();
    const key = sample![1];
    const name = key.split('{')[0];
    const family = types.has(name) ? name : name.replace(/_(bucket|sum|count)$/, '');
    expect(help.has(family), line).toBe(true);
    expect(types.has(family), line).toBe(true);
    expect(samples.has(key), line).toBe(false);
    samples.set(key, Number(sample![2]));
  }

  return samples;
};

test('metrics escape labels and emit HELP and TYPE in valid exposition', () => {
  const m = createMetrics();
  m.sample('example_total', 'An example counter.', 'counter', 2, { label: 'quote" slash\\ newline\n' });
  expect(parseExposition(m.text()).size).toBe(1);
});

test('ops routes retain their histogram patterns with production request logging enabled', async () => {
  const s = await stack();
  vi.spyOn(console, 'log').mockImplementation(() => {});

  try {
    const app = opsApp(s, {}, true);
    await app.request('/livez');
    const m = parseExposition(await (await app.request('/metrics')).text());
    expect(m.get('http_request_duration_seconds_count{route="/livez",status="200"}')).toBe(1);
  } finally { await s.close(); }
});

test('metrics use the shared cursor, head and tail, not stale local follower state, without RPC', async () => {
  const chain = new FakeChain(102n);
  chain.latest = 104n;
  const s = await stack(chain, { tail: {} });

  try {
    await s.follower.tick();
    await s.tail!.tick();
    await s.store.observeHead('ethereum', header(103n), s.clock.at);
    const queries = vi.spyOn(s.store, 'opsSnapshot');
    const status = vi.spyOn(s.discovery, 'status');
    const calls = chain.total;
    await s.app.request(`/v1/ethereum/accounts/${address(1)}/erc20`);
    await s.app.request('/unknown/secret-account');
    const response = await s.app.request('/metrics');
    const body = await response.text();
    expect(response.headers.get('content-type')).toBe('text/plain; version=0.0.4; charset=utf-8');
    const m = parseExposition(body);
    expect(m.get('finalized_cursor_block{network="ethereum"}')).toBe(102);
    expect(m.get('finalized_head_block{network="ethereum"}')).toBe(103);
    expect(m.get('follower_lag_blocks{network="ethereum"}')).toBe(1);
    expect(m.get('tail_tip_block{network="ethereum"}')).toBe(104);
    expect(m.get('tail_tip_age_seconds{network="ethereum"}')).toBeGreaterThan(0);
    expect(m.get('cursor_age_seconds{network="ethereum"}')).toBeGreaterThanOrEqual(0);
    expect(m.get('serving{network="ethereum",mode="stale"}')).toBe(1);
    expect(m.get('serving{network="ethereum",mode="full"}')).toBe(0);
    expect(m.get('http_request_duration_seconds_count{route="/v1/:network/accounts/:address/erc20",status="200"}')).toBe(1);
    expect(m.get('http_request_duration_seconds_count{route="unmatched",status="404"}')).toBe(1);
    expect(m.get('http_request_duration_seconds_bucket{route="unmatched",status="404",le="+Inf"}')).toBe(1);
    expect(body).not.toContain(address(1));
    expect(body).not.toContain('secret-account');
    expect(queries).toHaveBeenCalledTimes(1);
    expect(status).toHaveBeenCalledWith('ethereum', expect.objectContaining({ state: expect.any(Object) }));
    expect(chain.total).toBe(calls);
  } finally { await s.close(); }
});

test('metrics expose bootstrap applied/done/total before publication and local HyperSync quota stats', async () => {
  const s = await stack(new FakeChain(103n), { initialize: false });

  try {
    const bootstrap = createBootstrapStore(s.db);
    await bootstrap.createRun('ethereum', { startBlock: 100n, target: header(103n), anchor: header(99n), segmentBlocks: 2n }, s.clock.at);
    await bootstrap.commitSegment('ethereum', { fromBlock: 102n, toBlock: 103n }, 'rpc', async () => ({ logs: 0, requests: 1 }), s.clock.at);
    const app = opsApp(s, { hypersyncStats: () => ({ requests: 7, rateLimitWaits: 2 }) });
    const m = parseExposition(await (await app.request('/metrics')).text());
    expect(m.get('bootstrap_segments_total{network="ethereum"}')).toBe(2);
    expect(m.get('bootstrap_segments_done{network="ethereum"}')).toBe(1);
    expect(m.get('bootstrap_segments_applied{network="ethereum"}')).toBe(1);
    expect(m.get('serving{network="ethereum",mode="partial"}')).toBe(1);
    expect(m.has('finalized_cursor_block{network="ethereum"}')).toBe(false);
    expect(m.get('hypersync_requests_total{network="ethereum"}')).toBe(7);
    expect(m.get('hypersync_rate_limit_waits_total{network="ethereum"}')).toBe(2);
  } finally { await s.close(); }
});

test('metrics reuse RPC outcome stats with redacted credentials, retries, 429s and breaker state', async () => {
  const s = await stack();
  const secret = 'SECRET-KEY-123';
  let requests = 0;
  vi.spyOn(Math, 'random').mockReturnValue(0);
  vi.stubGlobal('fetch', vi.fn(async () => {
    requests += 1;

    return requests === 1 ? new Response('limited', { status: 429 })
      : new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: '0x1' }), { headers: { 'content-type': 'application/json' } });
  }));
  const rpc = createRpcClient({ urls: [`https://user:${secret}@primary.example/${secret}?key=${secret}`, `https://fallback.example/${secret}`], timeoutMs: 1000, maxAttempts: 2, maxRps: 1000 });

  try {
    await rpc.request('eth_blockNumber', []);
    const app = opsApp(s, { rpcStats: rpc.stats });
    const body = await (await app.request('/metrics')).text();
    const m = parseExposition(body);
    expect(body).not.toContain(secret);
    expect(body).not.toContain('user:');
    expect(m.get('rpc_retries_total{client="runtime"}')).toBe(1);
    expect(m.get('rpc_429_total{client="runtime"}')).toBe(1);
    expect(m.get('rpc_errors_total{client="runtime",endpoint="0:https://primary.example/***",method="eth_blockNumber",outcome="rate_limited"}')).toBe(1);
    expect(m.get('rpc_attempts_total{client="runtime",endpoint="1:https://fallback.example/***",method="eth_blockNumber",outcome="success"}')).toBe(1);
    expect(m.get('rpc_breaker_open{client="runtime",endpoint="0:https://primary.example/***"}')).toBe(0);
  } finally { await s.close(); }
});

test('metrics reuse limiter queue depth and rejection counter', async () => {
  const s = await stack(new FakeChain(102n), { balances: { concurrency: 1, queue: 1 } });
  const pause = gate();
  const limiter = s.requestRpc.limiter;
  const active = limiter.run(() => pause.opened);
  const queued = limiter.run(async () => {});

  try {
    await expect(limiter.run(async () => {})).rejects.toMatchObject({ statusCode: 503 });
    const m = parseExposition(await (await s.app.request('/metrics')).text());
    expect(m.get('limiter_queue_depth')).toBe(1);
    expect(m.get('limiter_rejections_total')).toBe(1);
  } finally { pause.release(); await Promise.all([active, queued]); await s.close(); }
});

for (const serving of ['full', 'stale', 'partial'] as const) {
  test(`readyz honors READYZ_ALLOW thresholds for ${serving}, with reasons and unchanged health`, async () => {
    const s = await stack(new FakeChain(102n), { initialize: serving !== 'partial' });

    try {
      if (serving === 'full') await s.follower.tick();
      const order = ['full', 'stale', 'partial'];

      for (const allow of ['full', 'stale', 'partial'] as const) {
        const response = await opsApp(s, { readyzAllow: readyMode(allow) }).request('/readyz');
        expect(response.status).toBe(order.indexOf(serving) <= order.indexOf(allow) ? 200 : 503);
        const json = await response.json();
        expect(json.networks[0].serving).toBe(serving);

        if (serving !== 'full') expect(json.networks[0].reason).toEqual(expect.any(String));
      }

      expect((await s.app.request('/health')).status).toBe(200);
      expect((await s.app.request('/readyz')).status).toBe(serving === 'full' ? 200 : 503);
    } finally { await s.close(); }
  });
}

test('livez never probes the database; readyz requires every network and fails closed on a DB error', async () => {
  const s = await stack();

  try {
    const status = vi.spyOn(s.discovery, 'status').mockRejectedValue(new Error('DB unavailable'));
    expect(await (await s.app.request('/livez')).json()).toEqual({ status: 'ok' });
    expect(status).not.toHaveBeenCalled();
    expect((await opsApp(s, { readyzAllow: 'partial' }).request('/readyz')).status).toBe(503);
    status.mockImplementation(async (network) => ({ ready: network === 'ethereum', serving: network === 'ethereum' ? 'full' : 'stale', reason: network === 'ethereum' ? undefined : 'Old observation' }));
    const app = opsApp(s, { adapters: { ethereum: s.adapter, other: s.adapter } });
    const response = await app.request('/readyz');
    expect(response.status).toBe(503);
    expect((await response.json()).networks).toHaveLength(2);
    expect(() => readyMode('invalid')).toThrow('READYZ_ALLOW');
    expect(readyMode()).toBe('full');
  } finally { await s.close(); }
});

test('SERVE_ONLY loads shared read state but starts no workers or bootstrap and acquires no leadership', async () => {
  const s = await stack(new FakeChain(102n), { tail: {} });
  const acquire = vi.fn(async () => true);

  const lockedFollower = createFollower({
    adapter: s.adapter, store: s.store, pollMs: 30_000, maxNoAdvanceMs: 1_800_000,
    lock: { tryAcquire: acquire, held: () => false, release: async () => {} },
  });

  try {
    const follower = vi.spyOn(s.follower, 'start');
    const tail = vi.spyOn(s.tail!, 'start');
    const bootstrap = vi.fn(async () => {});
    const tick = vi.spyOn(s.follower, 'tick');
    await startBackground(true, [s.follower, s.tail!, lockedFollower], bootstrap);
    expect(follower).not.toHaveBeenCalled();
    expect(tail).not.toHaveBeenCalled();
    expect(tick).not.toHaveBeenCalled();
    expect(bootstrap).not.toHaveBeenCalled();
    expect(acquire).not.toHaveBeenCalled();
    expect(s.follower.status().leader).toBe(false);
    expect(s.chain.total).toBe(0);
    expect((await s.app.request(`/v1/ethereum/accounts/${address(1)}/erc20`)).status).toBe(200);
    await startBackground(false, [], bootstrap);
    expect(bootstrap).toHaveBeenCalledTimes(1);
  } finally { await s.close(); }
});

test('metrics report reorgs and deep-reorg wipes from the existing tail status', async () => {
  const chain = new FakeChain(102n);
  chain.latest = 106n;
  const s = await stack(chain, { tail: { maxDepth: 1n } });

  try {
    await s.follower.tick();
    await s.tail!.tick();
    chain.reorg(3, []);
    await s.tail!.tick();
    const m = parseExposition(await (await s.app.request('/metrics')).text());
    expect(m.get('tail_reorgs_total{network="ethereum"}')).toBe(1);
    expect(m.get('tail_wipes_total{network="ethereum"}')).toBe(1);
    expect(m.has('tail_tip_block{network="ethereum"}')).toBe(false);
  } finally { await s.close(); }
});

test('the follower feeds the watchdog through its existing latest refresh when the tail is disabled', async () => {
  const chain = new FakeChain(102n);
  chain.latest = 104n;
  const s = await stack(chain);

  try {
    await s.follower.tick();
    vi.useFakeTimers();
    await vi.advanceTimersByTimeAsync(60_000);
    await s.follower.tick();
    expect(s.follower.status().headStalled).toBe(true);
    expect((await (await s.app.request('/health')).json()).followers.ethereum.headStalled).toBe(true);
    chain.latest = 105n;
    await s.follower.tick();
    expect(s.follower.status().headStalled).toBe(false);
  } finally { vi.useRealTimers(); await s.close(); }
});

test('a stalled single endpoint keeps serving RPC and clears the flag on progress', async () => {
  vi.useFakeTimers();
  let number = 104n;
  vi.stubGlobal('fetch', vi.fn(async (_input: string, init: RequestInit) => {
    const { method } = JSON.parse(String(init.body));
    const block = header(number);

    const result = method === 'eth_getBlockByNumber'
      ? { number: `0x${number.toString(16)}`, hash: block.hash, parentHash: block.parentHash, timestamp: '0x64' }
      : method === 'eth_getLogs' ? [] : '0x1';

    return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result }), { headers: { 'content-type': 'application/json' } });
  }));
  const rpc = createRpcClient({ urls: ['https://primary.example/key'], timeoutMs: 1000, maxAttempts: 1, maxRps: 1000 });
  const tracker = createHeadTracker(rpc, { cacheMs: 0, maxNoProgressMs: 100_000, headStallMs: 60_000, chain: mainnet });
  await tracker.latestHeader();
  await vi.advanceTimersByTimeAsync(60_000);

  for (let i = 0; i < 3; i += 1) {
    expect((await tracker.latestHeader()).number).toBe(104n);
    expect(tracker.headStalled()).toBe(true);
    expect(rpc.stats().endpoints[0].state).toBe('closed');
    expect((await tracker.finalizedHead()).block.number).toBe(104n);
    expect(await rpc.request('eth_call', [{ to: address(1), data: '0x70a08231' }, 'latest'])).toBe('0x1');
    expect(await rpc.request('eth_call', [{ to: address(1), data: '0x6352211e' }, 'latest'])).toBe('0x1');
    expect(await rpc.request('eth_getLogs', [{}])).toEqual([]);
    await vi.advanceTimersByTimeAsync(15_000);
  }

  number = 105n;
  await tracker.latestHeader();
  expect(tracker.headStalled()).toBe(false);
});

test.each(['open', 'unsupported'])('a stalled endpoint stays usable when the fallback is %s', async (state) => {
  vi.useFakeTimers({ toFake: ['Date'] });
  let seed = true;
  vi.spyOn(Math, 'random').mockReturnValue(0);
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    const block = header(104n);

    const error = seed && input.includes('primary')
      ? { code: -32000, message: 'temporarily unavailable' }
      : seed && state === 'unsupported' ? { code: -32601, message: 'Method not found' } : undefined;

    return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, ...(error ? { error } : { result: {
      number: '0x68', hash: block.hash, parentHash: block.parentHash, timestamp: '0x64',
    } }) }), { status: seed && input.includes('primary') ? 503 : 200, headers: { 'content-type': 'application/json' } });
  }));
  const rpc = createRpcClient({ urls: ['https://primary.example/key', 'https://fallback.example/key'], timeoutMs: 1000, maxAttempts: 2, maxRps: 1000 });
  let penalize = () => {};
  const request = rpc.request('eth_getBlockByNumber', ['latest', false], { onEndpoint: (trip) => { penalize = trip; } });

  if (state === 'unsupported') await expect(request).rejects.toMatchObject({ kind: 'unsupported' });
  else { await request; penalize(); }

  seed = false;
  const tracker = createHeadTracker(rpc, { cacheMs: 0, maxNoProgressMs: 100_000, headStallMs: 1000, chain: mainnet });
  await tracker.latestHeader();
  vi.setSystemTime(Date.now() + 1000);
  await tracker.latestHeader();
  expect(tracker.headStalled()).toBe(true);
  expect(rpc.stats().endpoints[0].state).toBe('closed');
  expect((await tracker.latestHeader()).number).toBe(104n);
});

test('watchdog trips the exact latest endpoint after no higher head, fails over, and clears on progress', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1000);
  let primary = 104n;
  const called: string[] = [];
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    called.push(input);
    const number = input.includes('primary') ? primary : 105n;
    const block = header(number);

    return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: {
      number: `0x${number.toString(16)}`, hash: block.hash, parentHash: block.parentHash, timestamp: '0x64',
    } }), { headers: { 'content-type': 'application/json' } });
  }));
  const rpc = createRpcClient({ urls: ['https://primary.example/secret', 'https://fallback.example/secret'], timeoutMs: 1000, maxAttempts: 1, maxRps: 1000 });
  const tracker = createHeadTracker(rpc, { cacheMs: 0, maxNoProgressMs: 100_000, headStallMs: 60_000, chain: mainnet });
  await tracker.latestHeader();
  await vi.advanceTimersByTimeAsync(59_999);
  primary = 103n;
  await tracker.latestHeader();
  expect(tracker.headStalled()).toBe(false);
  await vi.advanceTimersByTimeAsync(1);
  await tracker.latestHeader();
  expect(tracker.headStalled()).toBe(true);
  expect(rpc.stats().endpoints[0].state).toBe('open');
  expect((await tracker.latestHeader()).number).toBe(105n);
  expect(called.at(-1)).toContain('fallback.example');
  expect(tracker.headStalled()).toBe(false);
});

test('a concurrent successful response cannot clear a watchdog penalty', async () => {
  vi.useFakeTimers();
  const pause = gate();
  let requests = 0;
  vi.stubGlobal('fetch', vi.fn(async () => {
    requests += 1;

    if (requests === 2) await pause.opened;

    return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: '0x1' }), { headers: { 'content-type': 'application/json' } });
  }));
  const rpc = createRpcClient({ urls: ['https://primary.example/key', 'https://fallback.example/key'], timeoutMs: 1000, maxAttempts: 1, maxRps: 1000 });
  let penalize = () => {};

  await rpc.request('eth_blockNumber', [], { onEndpoint: (trip) => { penalize = trip; } });
  const inFlight = rpc.request('eth_chainId', []);

  try {
    await vi.waitFor(() => expect(requests).toBe(2));
    penalize();
    pause.release();
    await inFlight;
    expect(rpc.stats().endpoints[0].state).toBe('open');
    await vi.advanceTimersByTimeAsync(10_000);
    penalize();
    await vi.advanceTimersByTimeAsync(5000);
    expect(rpc.stats().endpoints[0].state).toBe('half-open');
    await rpc.request('eth_blockNumber', []);
    expect(rpc.stats().endpoints[0].state).toBe('closed');
    penalize();
    expect(rpc.stats().endpoints[0].state).toBe('open');
  } finally { pause.release(); await inFlight; }
});

test('stalled head surfaces in tail, health and metrics, then clears after progress', async () => {
  const chain = new FakeChain(102n);
  chain.latest = 104n;
  const s = await stack(chain, { tail: {} });

  try {
    await s.follower.tick();
    await s.tail!.tick();
    vi.useFakeTimers();
    await vi.advanceTimersByTimeAsync(60_000);
    chain.latest = 103n;
    await s.tail!.tick();
    expect(s.tail!.status()).toMatchObject({ headStalled: true, healthy: false });
    const health = await (await s.app.request('/health')).json();
    expect(health.tails.ethereum.headStalled).toBe(true);
    expect(parseExposition(await (await s.app.request('/metrics')).text()).get('head_stalled{network="ethereum"}')).toBe(1);
    chain.latest = 105n;
    await s.tail!.tick();
    expect(s.tail!.status()).toMatchObject({ headStalled: false, healthy: true });
  } finally { vi.useRealTimers(); await s.close(); }
});
