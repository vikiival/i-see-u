import { Hono } from 'hono';
import { logger } from 'hono/logger';
import type { ContentfulStatusCode } from 'hono/utils/http-status';

import type {
  Address,
  BalanceResult,
  ChainAdapter,
  DiscoveryResult,
  DiscoveryService,
  OwnerResult,
  RpcStats,
} from '../core/types.ts';
import { rpcErrorOf } from '../chains/evm/viem.ts';

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
  if (typeof error !== 'object' || error === null || !('statusCode' in error)) return undefined;
  const statusCode = (error as { statusCode?: unknown }).statusCode;
  return typeof statusCode === 'number' && statusCode >= 400 && statusCode <= 599 ? statusCode : undefined;
};

const rpcKindOf = (error: unknown): string | undefined => {
  return rpcErrorOf(error)?.kind;
};

const messageOf = (error: unknown): string =>
  error instanceof Error && error.message ? error.message : 'Request failed';

const missingAdapterError = (network: string): Error & { statusCode: number } =>
  Object.assign(new Error(`Unsupported network: ${network}`), { statusCode: 404 });

export function createApp(deps: RouteDeps, opts: { requestLog?: boolean } = {}): Hono {
  const app = new Hono();
  // Middleware lives on this app (not a parent) so its notFound/onError handlers stay in effect.
  if (opts.requestLog) app.use('*', logger());

  app.onError((error, c) => {
    const statusCode = statusCodeOf(error);
    if (statusCode !== undefined) return c.json({ error: messageOf(error) }, statusCode as ContentfulStatusCode);

    const kind = rpcKindOf(error);
    if (kind === 'rate_limited' || kind === 'transient') {
      c.header('Retry-After', '5');
      return c.json({ error: 'RPC temporarily unavailable' }, 503);
    }

    console.error('request failed', error);
    return c.json({ error: 'Internal server error' }, 500);
  });

  app.notFound((c) => c.json({ error: 'Not found' }, 404));

  app.get('/v1/:network/accounts/:address/erc20', async (c) => {
    const network = c.req.param('network');
    const routeAdapter = deps.adapters[network];
    if (!routeAdapter) throw missingAdapterError(network);
    const account = routeAdapter.normalizeAddress(c.req.param('address'));
    if (!account) return c.json({ error: 'Invalid address' }, 400);
    const includeBalances = booleanQuery(c.req.query('balances'), 'balances');

    const result = await deps.discovery.discover(network, account);
    const holdings = result.holdings.filter((holding) => holding.standard === 'erc20');
    let balances = new Map<Address, BalanceResult>();
    if (includeBalances) {
      const adapter = deps.adapters[result.network] ?? deps.adapters[network];
      if (!adapter) throw missingAdapterError(network);
      const values = await adapter.erc20Balances(
        result.account,
        holdings.map((holding) => holding.token),
        {
          number: result.blockHeight,
          hash: result.blockHash,
          parentHash: result.blockParentHash,
          timestamp: result.blockTimestamp,
        },
      );
      balances = new Map(values.map((value) => [value.token, value]));
    }

    const tokens = holdings.map((holding) => {
      const token = {
        contract: holding.token,
        firstSeenBlock: holding.firstBlock.toString(),
        lastSeenBlock: holding.lastBlock.toString(),
        sentCount: holding.sentCount,
        receivedCount: holding.receivedCount,
      };
      if (!includeBalances) return token;

      const balance = balances.get(holding.token);
      if (!balance) return { ...token, balanceStatus: 'error', balanceError: 'missing balance result' };
      if (balance.status === 'error') {
        return { ...token, balanceStatus: balance.status, balanceError: balance.error ?? 'balance call failed' };
      }
      return { ...token, balance: balance.balance?.toString(), balanceStatus: balance.status };
    });

    return c.json({ ...baseJson(result), tokens, scan: scanJson(result) });
  });

  app.get('/v1/:network/accounts/:address/erc721', async (c) => {
    const network = c.req.param('network');
    const routeAdapter = deps.adapters[network];
    if (!routeAdapter) throw missingAdapterError(network);
    const account = routeAdapter.normalizeAddress(c.req.param('address'));
    if (!account) return c.json({ error: 'Invalid address' }, 400);
    const verifyOwnership = booleanQuery(c.req.query('verifyOwnership'), 'verifyOwnership');

    const result = await deps.discovery.discover(network, account);
    const nftHoldings = result.holdings.filter((holding) => holding.standard === 'erc721');
    const heldItems = result.nftItems.filter((item) => item.heldBySeenLogs);
    let owners = new Map<string, OwnerResult>();
    if (verifyOwnership) {
      const adapter = deps.adapters[result.network] ?? deps.adapters[network];
      if (!adapter) throw missingAdapterError(network);
      const values = await adapter.erc721Owners(
        heldItems.map((item) => ({ token: item.token, tokenId: item.tokenId })),
        {
          number: result.blockHeight,
          hash: result.blockHash,
          parentHash: result.blockParentHash,
          timestamp: result.blockTimestamp,
        },
      );
      owners = new Map(values.map((value) => [`${value.token}:${value.tokenId}`, value]));
    }

    const collectionOrder: Address[] = [];
    const collectionBounds = new Map<Address, { first: bigint; last: bigint }>();
    for (const holding of nftHoldings) {
      collectionOrder.push(holding.token);
      collectionBounds.set(holding.token, { first: holding.firstBlock, last: holding.lastBlock });
    }
    for (const item of result.nftItems) {
      const bounds = collectionBounds.get(item.token);
      if (bounds) {
        if (item.lastBlock > bounds.last) bounds.last = item.lastBlock;
      } else {
        collectionOrder.push(item.token);
        collectionBounds.set(item.token, { first: item.lastBlock, last: item.lastBlock });
      }
    }

    const collections = collectionOrder.map((contract) => {
      const bounds = collectionBounds.get(contract)!;
      const tokens = result.nftItems
        .filter((item) => item.token === contract)
        .map((item) => {
          const token = { tokenId: item.tokenId.toString(), heldBySeenLogs: item.heldBySeenLogs };
          if (!verifyOwnership || !item.heldBySeenLogs) return token;

          const owner = owners.get(`${item.token}:${item.tokenId}`);
          if (!owner) return { ...token, ownerStatus: 'error' };
          if (owner.status === 'error') return { ...token, ownerStatus: owner.status };
          return {
            ...token,
            owner: owner.owner,
            ownedByAccount: owner.owner === account,
            ownerStatus: owner.status,
          };
        });
      return {
        contract,
        firstSeenBlock: bounds.first.toString(),
        lastSeenBlock: bounds.last.toString(),
        tokens,
      };
    });

    return c.json({ ...baseJson(result), collections, scan: scanJson(result) });
  });

  app.get('/health', (c) => c.json({
    status: 'ok',
    uptimeS: Math.max(0, Math.floor((Date.now() - deps.startedAt) / 1_000)),
    rpc: deps.rpcStats(),
  }));

  return app;
}
