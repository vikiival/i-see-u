import { Hono } from 'hono';
import { logger } from 'hono/logger';
import type { ContentfulStatusCode } from 'hono/utils/http-status';

import type { ChainAdapter, DiscoveryResult, DiscoveryService, RpcStats } from '../core/types.ts';
import { rpcErrorOf } from '../rpc/errors.ts';

export interface RouteDeps {
  discovery: DiscoveryService;
  adapters: Record<string, ChainAdapter>;
  rpcStats: () => RpcStats;
  startedAt: number;
}

const booleanQuery = (value: string | undefined, name: string): boolean => {
  if (value === undefined || value === 'false' || value === '0') return false;
  if (value === 'true' || value === '1') return true;
  throw Object.assign(new Error(`Invalid boolean query value for ${name}`), { statusCode: 400 });
};

const scanJson = (result: DiscoveryResult) => ({
  newBlocks: result.scan.newBlocks.toString(),
  rpcCalls: result.scan.rpcCalls,
  ms: result.scan.ms,
});

const baseJson = (result: DiscoveryResult) => ({
  network: result.network,
  account: result.account,
  fromBlock: result.fromBlock.toString(),
  blockHeight: result.blockHeight.toString(),
  blockHash: result.blockHash,
  finality: result.finality,
  lastProgressAt: new Date(result.lastProgressAt).toISOString(),
  blockTimestamp: new Date(Number(result.blockTimestamp) * 1_000).toISOString(),
  dataAgeS: Math.max(0, Math.floor(Date.now() / 1_000) - Number(result.blockTimestamp)),
  stale: result.stale,
  lagBlocks: result.lagBlocks.toString(),
});

const statusCodeOf = (error: unknown): number | undefined => {
  const statusCode = (error as { statusCode?: unknown } | null)?.statusCode;
  return typeof statusCode === 'number' && statusCode >= 400 && statusCode <= 599 ? statusCode : undefined;
};

export function createApp(deps: RouteDeps, opts: { requestLog?: boolean } = {}): Hono {
  const app = new Hono();
  // Middleware lives on this app (not a parent) so its notFound/onError handlers stay in effect.
  if (opts.requestLog) app.use('*', logger());

  app.onError((error, c) => {
    const statusCode = statusCodeOf(error);
    if (statusCode !== undefined) return c.json({ error: error.message || 'Request failed' }, statusCode as ContentfulStatusCode);

    const kind = rpcErrorOf(error)?.kind;
    if (kind === 'rate_limited' || kind === 'transient') {
      c.header('Retry-After', '5');
      return c.json({ error: 'RPC temporarily unavailable' }, 503);
    }

    console.error('request failed', error);
    return c.json({ error: 'Internal server error' }, 500);
  });

  app.notFound((c) => c.json({ error: 'Not found' }, 404));

  // Discovery validates the network (404) and address (400); query flags are checked before it runs.
  app.get('/v1/:network/accounts/:address/erc20', async (c) => {
    const includeBalances = booleanQuery(c.req.query('balances'), 'balances');
    const result = await deps.discovery.discover(c.req.param('network'), c.req.param('address'));
    const holdings = result.holdings.filter((holding) => holding.standard === 'erc20');
    const balances = includeBalances
      ? await deps.adapters[result.network].erc20Balances(result.account, holdings.map((h) => h.token), result.blockHash)
      : undefined;

    const tokens = holdings.map((holding, i) => {
      const token = {
        contract: holding.token,
        firstSeenBlock: holding.firstBlock.toString(),
        lastSeenBlock: holding.lastBlock.toString(),
        sentCount: holding.sentCount,
        receivedCount: holding.receivedCount,
      };
      const balance = balances?.[i];
      if (!balance) return token;
      return balance.status === 'ok'
        ? { ...token, balance: balance.balance.toString(), balanceStatus: balance.status }
        : { ...token, balanceStatus: balance.status, balanceError: balance.error };
    });

    return c.json({ ...baseJson(result), tokens, scan: scanJson(result) });
  });

  app.get('/v1/:network/accounts/:address/erc721', async (c) => {
    const verifyOwnership = booleanQuery(c.req.query('verifyOwnership'), 'verifyOwnership');
    const result = await deps.discovery.discover(c.req.param('network'), c.req.param('address'));
    const heldItems = result.nftItems.filter((item) => item.heldBySeenLogs);
    const owners = verifyOwnership
      ? await deps.adapters[result.network].erc721Owners(heldItems, result.blockHash)
      : [];
    const ownerOf = new Map(heldItems.map((item, i) => [item, owners[i]]));

    // Collections in holdings order; bounds come from the holding, widened by any item seen later.
    const bounds = new Map<string, { first: bigint; last: bigint }>();
    for (const holding of result.holdings) {
      if (holding.standard === 'erc721') bounds.set(holding.token, { first: holding.firstBlock, last: holding.lastBlock });
    }
    for (const item of result.nftItems) {
      const b = bounds.get(item.token);
      if (!b) bounds.set(item.token, { first: item.lastBlock, last: item.lastBlock });
      else if (item.lastBlock > b.last) b.last = item.lastBlock;
    }

    const collections = [...bounds].map(([contract, { first, last }]) => ({
      contract,
      firstSeenBlock: first.toString(),
      lastSeenBlock: last.toString(),
      tokens: result.nftItems.filter((item) => item.token === contract).map((item) => {
        const token = { tokenId: item.tokenId.toString(), heldBySeenLogs: item.heldBySeenLogs };
        if (!verifyOwnership || !item.heldBySeenLogs) return token;
        const owner = ownerOf.get(item);
        if (owner?.status !== 'ok') return { ...token, ownerStatus: 'error' };
        return { ...token, owner: owner.owner, ownedByAccount: owner.owner === result.account, ownerStatus: owner.status };
      }),
    }));

    return c.json({ ...baseJson(result), collections, scan: scanJson(result) });
  });

  app.get('/health', (c) => c.json({
    status: 'ok',
    uptimeS: Math.max(0, Math.floor((Date.now() - deps.startedAt) / 1_000)),
    rpc: deps.rpcStats(),
  }));

  return app;
}
