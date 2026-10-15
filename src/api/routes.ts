import { Hono } from 'hono';
import { logger } from 'hono/logger';
import type { ContentfulStatusCode } from 'hono/utils/http-status';

import type { BalanceWorker, RequestRpc } from '../balances/worker.ts';
import { unavailable, withDeadline } from '../core/limiter.ts';
import type {
  Address,
  ChainAdapter,
  DiscoveryResult,
  DiscoveryService,
  FollowerStatus,
  Hex,
  OwnerResult,
  RpcStats,
  SyncingResult,
  TailStatus,
} from '../core/types.ts';
import { rpcErrorOf } from '../rpc/errors.ts';

export interface RouteDeps {
  discovery: DiscoveryService;
  adapters: Record<string, ChainAdapter>;
  balances: Record<string, BalanceWorker>;
  followers: Record<string, { status(): FollowerStatus }>;
  tails?: Record<string, { status(): TailStatus }>;
  /** Bounds request-path RPC that remains (ownership verification). */
  requestRpc: RequestRpc;
  /** Held-item cap for verifyOwnership (default 2000). */
  maxVerifyOwnershipItems?: number;
  rpcStats: () => RpcStats;
  startedAt: number;
  /** Tokens whose balance changes without Transfer: only current when read at the served block (default none). */
  alwaysRefreshTokens?: readonly Address[];
}

const booleanQuery = (value: string | undefined, name: string): boolean => {
  if (value === undefined || value === 'false' || value === '0') return false;

  if (value === 'true' || value === '1') return true;
  throw Object.assign(new Error(`Invalid boolean query value for ${name}`), { statusCode: 400 });
};

const finalityQuery = (value: string | undefined): 'latest' | 'finalized' | undefined => {
  if (value === undefined) return undefined;

  if (value === 'latest' || value === 'finalized') return value;
  throw Object.assign(new Error('Invalid finality; expected latest or finalized'), { statusCode: 400 });
};

const scanJson = (result: DiscoveryResult) => ({
  newBlocks: result.scan.newBlocks.toString(),
  rpcCalls: result.scan.rpcCalls,
  ms: result.scan.ms,
});

const syncingJson = (result: SyncingResult) => ({
  status: result.status,
  network: result.network,
  account: result.account,
  fromBlock: result.fromBlock.toString(),
  progressBlocks: result.progressBlocks.toString(),
  targetBlocks: result.targetBlocks.toString(),
  followerLagBlocks: result.followerLagBlocks.toString(),
});

const baseJson = (result: DiscoveryResult) => ({
  status: result.status,
  network: result.network,
  account: result.account,
  fromBlock: result.fromBlock.toString(),
  blockHeight: result.blockHeight.toString(),
  blockHash: result.blockHash,
  finality: result.finality,
  ...(result.finalizedBlockHeight !== undefined && { finalizedBlockHeight: result.finalizedBlockHeight.toString() }),
  ...(result.finalizedBlockHash !== undefined && { finalizedBlockHash: result.finalizedBlockHash }),
  ...(result.tailError !== undefined && { tailError: result.tailError }),
  lastProgressAt: new Date(result.lastProgressAt).toISOString(),
  blockTimestamp: new Date(Number(result.blockTimestamp) * 1_000).toISOString(),
  dataAgeS: Math.max(0, Math.floor(Date.now() / 1_000) - Number(result.blockTimestamp)),
  stale: result.stale,
  lagBlocks: result.lagBlocks.toString(),
  followerLagBlocks: result.followerLagBlocks.toString(),
});

const statusJson = (status: FollowerStatus) => ({
  ready: status.ready,
  leader: status.leader,
  cursor: status.cursor && { number: status.cursor.number.toString(), hash: status.cursor.hash },
  headNumber: status.headNumber?.toString(),
  lagBlocks: status.lagBlocks.toString(),
  lastAdvanceAt: status.lastAdvanceAt ? new Date(status.lastAdvanceAt).toISOString() : undefined,
  lastError: status.lastError,
  stale: status.stale,
  unhealthy: status.unhealthy,
});

const statusCodeOf = (error: unknown): number | undefined => {
  const statusCode = (error as { statusCode?: unknown } | null)?.statusCode;

  return typeof statusCode === 'number' && statusCode >= 400 && statusCode <= 599 ? statusCode : undefined;
};

export function createApp(deps: RouteDeps, opts: { requestLog?: boolean } = {}): Hono {
  const app = new Hono();

  // Middleware lives on this app (not a parent) so its notFound/onError handlers stay in effect.
  if (opts.requestLog) app.use('*', logger());

  // Single-flight + short cache per account and block, so polling clients share one ownerOf sweep.
  const ownerCache = new Map<string, Promise<OwnerResult[]>>();

  const ownersFor = (key: string, adapter: ChainAdapter, items: { token: Address; tokenId: bigint }[], blockHash: Hex) => {
    const hit = ownerCache.get(key);

    if (hit) return hit;

    const promise = deps.requestRpc.limiter.run(
      () => adapter.erc721Owners(items, blockHash),
      { deadlineAt: Date.now() + deps.requestRpc.deadlineMs },
    );

    ownerCache.set(key, promise);
    promise.catch(() => { if (ownerCache.get(key) === promise) ownerCache.delete(key); });

    while (ownerCache.size > 1000) ownerCache.delete(ownerCache.keys().next().value as string);

    return promise;
  };

  app.onError((error, c) => {
    const statusCode = statusCodeOf(error);

    if (statusCode !== undefined) {
      const retryAfterS = (error as { retryAfterS?: unknown }).retryAfterS;

      if (typeof retryAfterS === 'number') c.header('Retry-After', String(retryAfterS));

      return c.json({ error: error.message || 'Request failed' }, statusCode as ContentfulStatusCode);
    }

    const kind = rpcErrorOf(error)?.kind;

    if (kind === 'rate_limited' || kind === 'transient') {
      c.header('Retry-After', '5');

      return c.json({ error: 'RPC temporarily unavailable' }, 503);
    }

    if (/timeout exceeded when trying to connect/i.test(error.message)) {
      c.header('Retry-After', '2');

      return c.json({ error: 'Database busy' }, 503);
    }

    console.error('request failed', error);

    return c.json({ error: 'Internal server error' }, 500);
  });

  app.notFound((c) => c.json({ error: 'Not found' }, 404));

  // Discovery validates the network (404) and address (400); query flags are checked before it runs.
  app.get('/v1/:network/accounts/:address/erc20', async (c) => {
    const includeBalances = booleanQuery(c.req.query('balances'), 'balances');
    const finality = finalityQuery(c.req.query('finality'));
    const result = await deps.discovery.discover(c.req.param('network'), c.req.param('address'), finality);

    if (result.status === 'syncing') {
      c.header('Retry-After', '2');

      return c.json(syncingJson(result), 202);
    }

    const holdings = result.holdings.filter((holding) => holding.standard === 'erc20');
    const at = { number: result.blockHeight, hash: result.blockHash, timestamp: result.blockTimestamp };

    const balances = includeBalances
      ? await deps.balances[result.network].forAccount(result.account, holdings.map((h) => h.token), at)
      : undefined;

    const withTail = deps.tails?.[result.network] !== undefined;
    const always = new Set<string>(deps.alwaysRefreshTokens ?? []);
    const current: boolean[] = [];

    const tokens = holdings.map((holding, i) => {
      const token = {
        contract: holding.token,
        firstSeenBlock: holding.firstBlock.toString(),
        lastSeenBlock: holding.lastBlock.toString(),
        sentCount: holding.sentCount,
        receivedCount: holding.receivedCount,
        ...(holding.unfinalizedSentCount && { unfinalizedSentCount: holding.unfinalizedSentCount }),
        ...(holding.unfinalizedReceivedCount && { unfinalizedReceivedCount: holding.unfinalizedReceivedCount }),
      };

      const snapshot = balances?.[i];

      if (!snapshot) return token;
      const { result: balance } = snapshot;

      // A tail-tip block can be reorged out, so with the tail the balance is also pinned by hash.
      // Current: read at the served block, or no Transfer of this token touched the account between the read and it.
      // A read above blockHeight (finalized view, tail on) may postdate a Transfer the finalized holdings do not show.
      const balanceCurrent = (snapshot.blockNumber === result.blockHeight && snapshot.blockHash === result.blockHash)
        || (balance.status === 'ok' && holding.lastBlock <= snapshot.blockNumber
          && snapshot.blockNumber <= result.blockHeight && !always.has(holding.token));

      current.push(balanceCurrent);

      const at = {
        balanceBlock: snapshot.blockNumber.toString(),
        ...(withTail && { balanceBlockHash: snapshot.blockHash }),
        balanceCurrent,
      };

      return balance.status === 'ok'
        ? { ...token, balance: balance.balance.toString(), balanceStatus: balance.status, ...at }
        : { ...token, balanceStatus: balance.status, balanceError: balance.error, ...at };
    });

    // Balances are labelled with the block they were read at; true when every one was read at blockHeight.
    const balancesAtBlockHeight = balances && balances.every((snapshot) =>
      snapshot.blockNumber === result.blockHeight && snapshot.blockHash === result.blockHash);

    const balancesCurrent = balances && current.every(Boolean);

    return c.json({ ...baseJson(result), balancesAtBlockHeight, balancesCurrent, tokens, scan: scanJson(result) });
  });

  app.get('/v1/:network/accounts/:address/erc721', async (c) => {
    const verifyOwnership = booleanQuery(c.req.query('verifyOwnership'), 'verifyOwnership');
    const finality = finalityQuery(c.req.query('finality'));
    const result = await deps.discovery.discover(c.req.param('network'), c.req.param('address'), finality);

    if (result.status === 'syncing') {
      c.header('Retry-After', '2');

      return c.json(syncingJson(result), 202);
    }

    const heldItems = result.nftItems.filter((item) => item.heldBySeenLogs);
    const adapter = deps.adapters[result.network];
    const cap = deps.maxVerifyOwnershipItems ?? 2000;
    const skipped = verifyOwnership && heldItems.length > cap;

    const owners = verifyOwnership && !skipped && heldItems.length > 0
      ? await withDeadline(
        ownersFor(`${result.network}:${result.account}:${result.blockHash}`, adapter, heldItems, result.blockHash),
        deps.requestRpc.deadlineMs,
        () => { throw unavailable('Ownership verification did not finish within the request deadline', 2); },
      )
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

    const byToken = new Map<string, typeof result.nftItems>();

    for (const item of result.nftItems) {
      const list = byToken.get(item.token);

      if (list) list.push(item);
      else byToken.set(item.token, [item]);
    }

    const collections = [...bounds].map(([contract, { first, last }]) => ({
      contract,
      firstSeenBlock: first.toString(),
      lastSeenBlock: last.toString(),
      tokens: (byToken.get(contract) ?? []).map((item) => {
        const token = {
          tokenId: item.tokenId.toString(),
          heldBySeenLogs: item.heldBySeenLogs,
          ...(item.unfinalized && { unfinalized: true }),
        };

        if (!verifyOwnership || !item.heldBySeenLogs) return token;

        if (skipped) return { ...token, ownerStatus: 'skipped' };
        const owner = ownerOf.get(item);

        if (owner?.status !== 'ok') return { ...token, ownerStatus: 'error' };

        return { ...token, owner: owner.owner, ownedByAccount: owner.owner === result.account, ownerStatus: owner.status };
      }),
    }));

    return c.json({
      ...baseJson(result),
      ...(skipped && { ownershipError: `Too many held items to verify (${heldItems.length} > ${cap})` }),
      collections,
      scan: scanJson(result),
    });
  });

  app.get('/health', (c) => c.json({
    status: 'ok',
    uptimeS: Math.max(0, Math.floor((Date.now() - deps.startedAt) / 1_000)),
    followers: Object.fromEntries(Object.entries(deps.followers).map(([network, f]) => [network, statusJson(f.status())])),
    ...(deps.tails && Object.keys(deps.tails).length > 0 && { tails: Object.fromEntries(Object.entries(deps.tails).map(([network, tail]) => {
      const status = tail.status();

      return [network, {
        enabled: status.enabled,
        tip: status.tipNumber === undefined ? undefined : { number: status.tipNumber.toString(), hash: status.tipHash },
        latestNumber: status.latestNumber?.toString(),
        lagBlocks: status.lagBlocks.toString(),
        reorgs: status.reorgs,
        lastReorgDepth: status.lastReorgDepth?.toString(),
        healthy: status.healthy,
        lastError: status.lastError,
        tailError: status.tailError,
      }];
    })) }),
    balances: Object.fromEntries(Object.entries(deps.balances).map(([network, b]) => {
      const { activeAccounts, lastRefresh } = b.stats();

      return [network, {
        activeAccounts,
        lastRefresh: lastRefresh && {
          at: new Date(lastRefresh.at).toISOString(),
          block: lastRefresh.block?.toString(),
          pairs: lastRefresh.pairs,
          candidates: lastRefresh.candidates,
          read: lastRefresh.read,
          fullSweep: lastRefresh.fullSweep,
        },
      }];
    })),
    logSources: Object.fromEntries(Object.entries(deps.adapters)
      .map(([network, adapter]) => [network, adapter.accountLogs.stats()])),
    rpc: deps.rpcStats(),
  }));

  return app;
}
