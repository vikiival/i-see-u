import { Hono } from 'hono';
import { logger } from 'hono/logger';
import type { ContentfulStatusCode } from 'hono/utils/http-status';

import type { BalanceReader, RequestRpc } from '../balances/reader.ts';
import { unavailable, withDeadline } from '../core/limiter.ts';
import { isNumber } from '../core/parse.ts';
import type {
  Address,
  ChainAdapter,
  DiscoveryResult,
  DiscoveryService,
  Finality,
  FollowerStatus,
  Hex,
  TransferCursor,
  OwnerResult,
  RpcStats,
  TailStatus,
} from '../core/types.ts';
import { rpcErrorOf } from '../rpc/errors.ts';

export interface RouteDeps {
  discovery: DiscoveryService;
  adapters: Record<string, ChainAdapter>;
  balances: Record<string, BalanceReader>;
  followers: Record<string, { status(): FollowerStatus }>;
  tails?: Record<string, { status(): TailStatus }>;
  /** Bounds request-path RPC that remains (ownership verification). */
  requestRpc: RequestRpc;
  /** Held-item cap for verifyOwnership (default 2000). */
  maxVerifyOwnershipItems?: number;
  rpcStats: () => RpcStats;
  startedAt: number;
}

const booleanQuery = (value: string | undefined, name: string): boolean => {
  if (value === undefined || value === 'false' || value === '0') return false;

  if (value === 'true' || value === '1') return true;
  throw Object.assign(new Error(`Invalid boolean query value for ${name}`), { statusCode: 400 });
};

const finalityQuery = (value: string | undefined): Finality | undefined => {
  if (value === undefined || value === 'latest' || value === 'finalized') return value;
  throw Object.assign(new Error('Invalid finality; expected latest or finalized'), { statusCode: 400 });
};

/** Latest responses also name the finalized block below the tail tip; a degraded latest request says why. */
const finalityJson = (result: { finality: Finality; finalizedBlockHeight?: bigint; finalizedBlockHash?: Hex; tailError?: string }) => ({
  finality: result.finality,
  ...(result.finalizedBlockHeight !== undefined && { finalizedBlockHeight: result.finalizedBlockHeight.toString(), finalizedBlockHash: result.finalizedBlockHash }),
  ...(result.tailError !== undefined && { tailError: result.tailError }),
});

const historyCursor = (value: string | undefined): TransferCursor | undefined => {
  if (value === undefined) return undefined;

  if (!/^\d{1,19}:\d{1,19}:\d{1,19}:\d{1,10}$/.test(value)) throw Object.assign(new Error('Invalid history cursor'), { statusCode: 400 });
  const [throughBlock, revision, blockNumber, index] = value.split(':').map(BigInt);

  if (blockNumber > throughBlock || throughBlock > 9223372036854775807n || revision > 9223372036854775807n || index > 2147483647n) {
    throw Object.assign(new Error('Invalid history cursor'), { statusCode: 400 });
  }

  return { throughBlock, revision, blockNumber, logIndex: Number(index) };
};

const baseJson = (result: DiscoveryResult) => ({
  status: result.status,
  network: result.network,
  account: result.account,
  fromBlock: result.fromBlock.toString(),
  blockHeight: result.blockHeight.toString(),
  blockHash: result.blockHash,
  ...finalityJson(result),
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

const tailJson = (status: TailStatus) => ({
  enabled: status.enabled,
  tip: status.tipNumber === undefined ? undefined : { number: status.tipNumber.toString(), hash: status.tipHash },
  latestNumber: status.latestNumber?.toString(),
  lagBlocks: status.lagBlocks.toString(),
  reorgs: status.reorgs,
  lastReorgDepth: status.lastReorgDepth?.toString(),
  lastAdvanceAt: status.lastAdvanceAt ? new Date(status.lastAdvanceAt).toISOString() : undefined,
  healthy: status.healthy,
  lastError: status.lastError,
  tailError: status.tailError,
});

/** The 4xx/5xx `statusCode` (and `retryAfterS`) attached to an error with Object.assign, e.g. by unavailable(). */
const statusOf = (error: Error): { statusCode: ContentfulStatusCode; retryAfterS: number | undefined } | undefined => {
  const statusCode = 'statusCode' in error ? error.statusCode : undefined;

  if (!isNumber(statusCode) || statusCode < 400 || statusCode > 599) return undefined;
  const retryAfterS = 'retryAfterS' in error ? error.retryAfterS : undefined;

  return {
    // SAFETY: checked to be 400-599 above; every such status carries a body, and Hono sends the number unchanged.
    statusCode: statusCode as ContentfulStatusCode,
    retryAfterS: isNumber(retryAfterS) ? retryAfterS : undefined,
  };
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

    // Evict oldest first (Map iterates in insertion order; deleting while iterating is safe).
    for (const oldest of ownerCache.keys()) {
      if (ownerCache.size <= 1000) break;
      ownerCache.delete(oldest);
    }

    return promise;
  };

  app.onError((error, c) => {
    const status = statusOf(error);

    if (status !== undefined) {
      if (status.retryAfterS !== undefined) c.header('Retry-After', String(status.retryAfterS));

      return c.json({ ...('serviceStatus' in error && { status: error.serviceStatus }), error: error.message || 'Request failed' }, status.statusCode);
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
    const result = await deps.discovery.discover(c.req.param('network'), c.req.param('address'), { finality, balances: includeBalances });

    const holdings = result.holdings.filter((holding) => holding.standard === 'erc20');
    const at = { number: result.blockHeight, hash: result.blockHash, timestamp: result.blockTimestamp };

    // Pinned to the served block hash: the tail tip for latest, the finalized cursor otherwise.
    const balances = includeBalances ? await deps.balances[result.network].forAccount(result.account, holdings, at, result.revision, result.balances) : undefined;

    const tokens = holdings.map((holding, i) => {
      const token = {
        contract: holding.token,
        firstSeenBlock: holding.firstBlock.toString(),
        lastSeenBlock: holding.lastBlock.toString(),
        sentCount: holding.sentCount.toString(),
        receivedCount: holding.receivedCount.toString(),
        ...(holding.unfinalizedSentCount !== undefined && { unfinalizedSentCount: holding.unfinalizedSentCount.toString() }),
        ...(holding.unfinalizedReceivedCount !== undefined && { unfinalizedReceivedCount: holding.unfinalizedReceivedCount.toString() }),
      };

      const snapshot = balances?.[i];

      if (!snapshot) return token;
      const { result: balance } = snapshot;

      // balanceBlock is always the block actually read; balanceValidAt (= blockHeight) only when the value provably holds there.
      const at = {
        balanceBlock: snapshot.blockNumber.toString(),
        balanceBlockHash: snapshot.blockHash,
        balanceReadAt: new Date(snapshot.readAt).toISOString(),
        balanceValidAt: snapshot.validAt?.toString(),
        balanceCached: snapshot.cached,
      };

      return balance.status === 'ok'
        ? { ...token, balance: balance.balance.toString(), balanceStatus: balance.status, ...at }
        : { ...token, balanceStatus: balance.status, balanceError: balance.error, ...at };
    });

    const balancesCurrent = balances?.every((snapshot) => snapshot.validAt === result.blockHeight);

    return c.json({ ...baseJson(result), balancesCurrent, tokens });
  });

  app.get('/v1/:network/accounts/:address/erc721', async (c) => {
    const verifyOwnership = booleanQuery(c.req.query('verifyOwnership'), 'verifyOwnership');
    const finality = finalityQuery(c.req.query('finality'));
    const result = await deps.discovery.discover(c.req.param('network'), c.req.param('address'), { finality });

    const heldItems = result.nftItems.filter((item) => item.heldBySeenLogs);
    const adapter = deps.adapters[result.network];
    const cap = deps.maxVerifyOwnershipItems ?? 2000;
    const skipped = verifyOwnership && heldItems.length > cap;

    const owners = verifyOwnership && !skipped && heldItems.length > 0
      ? await withDeadline(
        ownersFor(`${result.network}:${result.account}:${result.blockHash}:${result.revision}`, adapter, heldItems, result.blockHash),
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
    });
  });

  app.get('/v1/:network/accounts/:address/transfers', async (c) => {
    const requested = c.req.query('limit') ?? '50';
    const limit = Number(requested);

    if (!/^\d+$/.test(requested) || !Number.isInteger(limit) || limit < 1 || limit > 200) {
      throw Object.assign(new Error('Limit must be between 1 and 200'), { statusCode: 400 });
    }

    const before = historyCursor(c.req.query('before'));
    const finality = finalityQuery(c.req.query('finality'));
    const result = await deps.discovery.history(c.req.param('network'), c.req.param('address'), limit, before, finality);
    const { network, account, page } = result;
    const next = page.next;

    // Rows above finalizedBlockHeight carry `unfinalized: true` and can still be reorged out.
    return c.json({
      status: 'ready', network, account, ...finalityJson(result), blockHeight: result.blockHeight.toString(), blockHash: result.blockHash,
      throughBlock: page.throughBlock.toString(),
      transfers: page.transfers.map((event) => ({ ...event, value: event.value.toString(), blockNumber: event.blockNumber.toString() })),
      next: next ? `${next.throughBlock}:${next.revision}:${next.blockNumber}:${next.logIndex}` : null,
    });
  });

  app.get('/health', async (c) => {
    // Probe the same DB readiness rules as account reads; no RPC and no account-specific state.
    const networks = await Promise.all(Object.keys(deps.adapters).map(async (network) => {
      try {
        return { network, ...await deps.discovery.status(network) };
      } catch {
        return { network, ready: false };
      }
    }));

    const ready = networks.every((network) => network.ready);

    return c.json({
      status: ready ? 'ready' : 'not_ready',
      networks,
      uptimeS: Math.max(0, Math.floor((Date.now() - deps.startedAt) / 1_000)),
      followers: Object.fromEntries(Object.entries(deps.followers).map(([network, f]) => [network, statusJson(f.status())])),
      ...(deps.tails && { tails: Object.fromEntries(Object.entries(deps.tails).map(([network, tail]) => [network, tailJson(tail.status())])) }),
      rpc: deps.rpcStats(),
    }, ready ? 200 : 503);
  });

  return app;
}
