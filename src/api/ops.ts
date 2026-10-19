import type { Hono } from 'hono';
import { routePath } from 'hono/route';

import { createHttpMetrics, createMetrics } from '../core/metrics.ts';
import type { RouteDeps } from './routes.ts';

export type ReadyMode = 'full' | 'stale' | 'partial';

export function readyMode(value = 'full'): ReadyMode {
  if (value === 'full' || value === 'stale' || value === 'partial') return value;
  throw new Error('READYZ_ALLOW must be full, stale or partial');
}

export function installOps(app: Hono, deps: RouteDeps): void {
  const http = createHttpMetrics();
  const allowed = readyMode(deps.readyzAllow);
  const modes = ['full', 'stale', 'partial'];

  app.use('*', async (c, next) => {
    const started = performance.now();
    await next();
    const pattern = routePath(c, -1);
    http.observe(pattern === '*' || pattern === '/*' || !pattern ? 'unmatched' : pattern, c.res.status, (performance.now() - started) / 1000);
  });

  app.get('/livez', (c) => c.json({ status: 'ok' }));
  app.get('/readyz', async (c) => {
    const networks = await Promise.all(Object.keys(deps.adapters).map(async (network) => {
      try {
        return { network, ...await deps.discovery.status(network) };
      } catch {
        return { network, ready: false, serving: undefined, reason: 'Database status read failed' };
      }
    }));

    const ready = networks.every((n) => n.serving !== undefined && modes.indexOf(n.serving) <= modes.indexOf(allowed));

    return c.json({ status: ready ? 'ready' : 'not_ready', networks }, ready ? 200 : 503);
  });

  app.get('/metrics', async (c) => {
    const registry = createMetrics();
    const { sample } = registry;
    const secondsSince = (at: number) => Math.max(0, (Date.now() - at) / 1000);

    for (const network of Object.keys(deps.adapters)) {
      const local = deps.followers[network]?.status();
      const tail = deps.tails?.[network]?.status();
      const snapshot = await deps.opsSnapshot?.(network);
      const serving = await deps.discovery.status(network, snapshot);
      const state = snapshot?.state;
      const cursor = snapshot ? state?.cursor : local?.cursor;
      const head = snapshot ? state?.head?.number : local?.headNumber;
      const labels = { network };

      if (cursor) {
        sample('finalized_cursor_block', 'Committed finalized cursor block.', 'gauge', cursor.number, labels);
        sample('cursor_age_seconds', 'Seconds since the finalized cursor advanced.', 'gauge', secondsSince(cursor.updatedAt), labels);
      }

      if (head !== undefined) sample('finalized_head_block', 'Observed finalized head block.', 'gauge', head, labels);
      sample('follower_lag_blocks', 'Blocks behind the observed finalized head.', 'gauge', cursor && head !== undefined && head > cursor.number ? head - cursor.number : 0n, labels);
      const tip = snapshot ? snapshot.tipNumber : tail?.tipNumber;

      if (tip !== undefined) sample('tail_tip_block', 'Shared unfinalized tail tip block.', 'gauge', tip, labels);

      if (snapshot?.tipTimestamp !== undefined) sample('tail_tip_age_seconds', 'Age of the shared tail tip block.', 'gauge', secondsSince(Number(snapshot.tipTimestamp) * 1000), labels);
      sample('tail_reorgs_total', 'Tail reorgs observed by this process.', 'counter', tail?.reorgs ?? 0, labels);
      sample('tail_wipes_total', 'Deep-reorg tail wipes by this process.', 'counter', tail?.wipes ?? 0, labels);
      sample('head_stalled', 'Leader latest-head watchdog is stalled.', 'gauge', Number(tail?.headStalled ?? local?.headStalled ?? false), labels);

      for (const mode of modes) sample('serving', 'Shared database serving mode.', 'gauge', Number(serving.serving === mode), { ...labels, mode });

      if (snapshot?.bootstrap) {
        const b = snapshot.bootstrap;

        for (const [kind, value] of [['done', b.segmentsDone], ['applied', b.segmentsApplied], ['total', b.segmentsTotal]] as const) {
          sample(`bootstrap_segments_${kind}`, `Bootstrap segments ${kind}.`, 'gauge', value, labels);
        }
      }

      const hs = deps.hypersyncStats?.(network);

      if (hs) {
        sample('hypersync_requests_total', 'HyperSync requests by this process.', 'counter', hs.requests, labels);
        sample('hypersync_rate_limit_waits_total', 'HyperSync quota waits including 429s.', 'counter', hs.rateLimitWaits, labels);
      }
    }

    for (const [client, rpc] of Object.entries({ runtime: deps.rpcStats(), ...deps.bootstrapRpcStats?.() })) {
      sample('rpc_requests_total', 'Logical RPC requests.', 'counter', rpc.requests, { client });
      sample('rpc_retries_total', 'Additional RPC attempts.', 'counter', rpc.retries ?? 0, { client });
      sample('rpc_429_total', 'RPC HTTP 429 responses.', 'counter', rpc.rateLimited ?? 0, { client });

      for (const row of rpc.outcomes ?? []) {
        const labels = { client, endpoint: row.endpoint, method: row.method, outcome: row.outcome };
        sample('rpc_attempts_total', 'RPC attempts by redacted endpoint, method and outcome.', 'counter', row.count, labels);
        sample('rpc_errors_total', 'RPC errors by redacted endpoint, method and outcome.', 'counter', row.outcome === 'success' ? 0 : row.count, labels);
      }

      for (const [i, endpoint] of rpc.endpoints.entries()) {
        sample('rpc_breaker_open', 'RPC breaker is open or probing.', 'gauge', Number(endpoint.state !== 'closed'), { client, endpoint: `${i}:${endpoint.url}` });
      }
    }

    sample('limiter_queue_depth', 'Queued request-path RPC jobs.', 'gauge', deps.requestRpc.limiter.queued);
    sample('limiter_rejections_total', 'Request-path RPC jobs rejected at queue capacity.', 'counter', deps.requestRpc.limiter.rejections);
    http.write(registry);
    c.header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');

    return c.body(registry.text());
  });
}
