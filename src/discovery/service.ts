import { addTransfer, emptyAggregate } from '../core/aggregate.ts';
import { unavailable } from '../core/limiter.ts';
import type { AccountView, Address, BlockHeader, ChainAdapter, DiscoveryService, IndexState, TailStatus } from '../core/types.ts';
import type { Store } from '../core/types.ts';

export interface DiscoveryDependencies {
  adapters: Record<string, ChainAdapter>;
  store: Store;
  maxHeadAgeMs: number;
  maxNoAdvanceMs: number;
  /** Per-network tail; without one every read is finalized. */
  tails?: Record<string, { status(): TailStatus }>;
  /**
   * Serve `latest` only while the shared tail tip's own block timestamp is at most this old (undefined: no check).
   * Every process reads the same tip, so a stalled leader degrades all of them, not just itself.
   */
  maxTailTipAgeMs?: number;
  now?: () => number;
}

const newestFirst = (a: { lastBlock: bigint }, b: { lastBlock: bigint }, tie: number) => a.lastBlock === b.lastBlock ? tie : a.lastBlock > b.lastBlock ? -1 : 1;

/** Fold the account's tail Transfers over the finalized summaries and current owners (latest view). */
function overlay(view: AccountView, account: Address): Pick<AccountView, 'holdings' | 'nftItems'> {
  const aggregate = emptyAggregate();

  for (const event of view.tailTransfers) addTransfer(aggregate, account, event);
  const byToken = new Map(view.holdings.map((holding) => [holding.token, { ...holding }]));

  for (const [token, tail] of aggregate.tokens) {
    const sent = BigInt(tail.sentCount);
    const received = BigInt(tail.receivedCount);
    const base = byToken.get(token);
    const unfinalized = { ...(sent > 0n && { unfinalizedSentCount: sent }), ...(received > 0n && { unfinalizedReceivedCount: received }) };

    byToken.set(token, base ? {
      ...base, ...unfinalized,
      standard: base.standard === 'erc721' ? base.standard : tail.standard,
      lastBlock: tail.lastBlock > base.lastBlock ? tail.lastBlock : base.lastBlock,
      sentCount: base.sentCount + sent, receivedCount: base.receivedCount + received,
    } : { token, standard: tail.standard, firstBlock: tail.firstBlock, lastBlock: tail.lastBlock, sentCount: sent, receivedCount: received, ...unfinalized });
  }

  // The account's latest tail Transfer of an item decides whether it still holds it; sent items leave the list.
  const byItem = new Map(view.nftItems.map((item) => [`${item.token}:${item.tokenId}`, item]));

  for (const [key, item] of aggregate.nfts) {
    if (item.held) byItem.set(key, { token: item.token, tokenId: item.tokenId, heldBySeenLogs: true, lastBlock: item.block, unfinalized: true });
    else byItem.delete(key);
  }

  return {
    holdings: [...byToken.values()].sort((a, b) => newestFirst(a, b, a.token.localeCompare(b.token))),
    nftItems: [...byItem.values()].sort((a, b) => a.token === b.token ? (a.tokenId < b.tokenId ? -1 : 1) : a.token.localeCompare(b.token)),
  };
}

/** Shared database state governs every replica. No RPC, tracking, or work scheduled by account requests. */
export function createDiscoveryService(deps: DiscoveryDependencies): DiscoveryService {
  const now = deps.now ?? Date.now;

  const validate = (network: string, input: string) => {
    const adapter = Object.hasOwn(deps.adapters, network) ? deps.adapters[network] : undefined;

    if (!adapter) throw Object.assign(new Error(`Unknown network: ${network}`), { statusCode: 404 });
    const account = adapter.normalizeAddress(input);

    if (!account) throw Object.assign(new Error('Invalid account address'), { statusCode: 400 });

    return { adapter, account };
  };

  const requireReady = (state: IndexState | undefined): IndexState => {
    let reason: string | undefined;

    if (!state?.head || state.checkedAt === undefined) reason = 'Global index awaits bootstrap or finalized head';
    else if (state.pendingRepair) reason = 'Global index has pending repairs';
    else if (state.cursor.number < state.head.number) reason = 'Global index has not reached the finalized head';
    else if (state.cursor.number === state.head.number && state.cursor.hash !== state.head.hash) reason = 'Global index head hash mismatch';
    else if (now() - state.checkedAt > deps.maxHeadAgeMs) reason = 'Finalized head observation is too old';
    else if (now() - state.cursor.updatedAt > deps.maxNoAdvanceMs) reason = 'Global index has not advanced recently enough';

    if (reason) throw Object.assign(unavailable(reason, 5), { serviceStatus: 'not_ready' });

    return state!;
  };

  const tailOf = (network: string) => deps.tails && Object.hasOwn(deps.tails, network) ? deps.tails[network] : undefined;

  /** Why the latest view cannot be served from this tip (undefined: serve it). */
  const tailProblem = (status: TailStatus, tip: BlockHeader | undefined, cursor: bigint): string | undefined => {
    if (!status.healthy) return status.tailError ?? 'Tail is unhealthy';

    if (!tip || tip.number <= cursor) return 'Tail has no complete snapshot yet';
    const ageMs = now() - Number(tip.timestamp) * 1000;

    if (deps.maxTailTipAgeMs !== undefined && ageMs > deps.maxTailTipAgeMs) return `Tail tip is ${Math.round(ageMs / 1000)} s old`;

    return undefined;
  };

  return {
    async status(network) {
      if (!Object.hasOwn(deps.adapters, network)) throw Object.assign(new Error(`Unknown network: ${network}`), { statusCode: 404 });

      try {
        requireReady(await deps.store.indexState(network));

        return { ready: true };
      } catch (error) {
        if (error instanceof Error && 'serviceStatus' in error) return { ready: false, reason: error.message };
        throw error;
      }
    },
    async discover(network, input, options = {}) {
      const { adapter, account } = validate(network, input);
      const tail = options.finality === 'finalized' ? undefined : tailOf(network)?.status();
      const view = await deps.store.readAccount(network, account, { tail: tail !== undefined, balances: options.balances });
      const { cursor, startBlock, head } = requireReady(view?.state);
      const tailError = tail && tailProblem(tail, view!.tailTip, cursor.number);
      const tip = tail && !tailError ? view!.tailTip : undefined;
      const served = tip ?? cursor;
      const latest = tail?.latestNumber ?? adapter.cachedLatestBlockNumber?.() ?? served.number;

      return {
        status: 'ready', network, account, fromBlock: startBlock, blockHeight: served.number, blockHash: served.hash,
        blockTimestamp: served.timestamp, revision: cursor.revision, finality: tip ? 'latest' : 'finalized',
        ...(tip && { finalizedBlockHeight: cursor.number, finalizedBlockHash: cursor.hash }),
        ...(tailError && { tailError }),
        // The shared tip's block time, not process memory: identical in every process, leader or not.
        lastProgressAt: tip ? Number(tip.timestamp) * 1000 : cursor.updatedAt,
        stale: false, lagBlocks: latest > served.number ? latest - served.number : 0n,
        followerLagBlocks: head!.number > cursor.number ? head!.number - cursor.number : 0n,
        ...(tip ? overlay(view!, account) : { holdings: view!.holdings, nftItems: view!.nftItems }),
        ...(view!.balances && { balances: view!.balances }),
      };
    },
    async history(network, input, limit, before, finality) {
      const { account } = validate(network, input);
      const tail = finality === 'finalized' ? undefined : tailOf(network)?.status();
      let page = await deps.store.transferHistory(network, account, limit, before, tail !== undefined);
      const { cursor } = requireReady(page?.state);
      const tailError = tail && tailProblem(tail, page!.tailTip, cursor.number);

      // Rare: the tail was read but cannot be served; read the finalized view (a latest cursor then gets 409).
      if (tailError && page!.throughBlock > cursor.number) page = await deps.store.transferHistory(network, account, limit, before);
      const tip = tail && !tailError ? page!.tailTip : undefined;
      const served = tip ?? cursor;

      return {
        network, account, page: page!, finality: tip ? 'latest' : 'finalized', blockHeight: served.number, blockHash: served.hash,
        ...(tip && { finalizedBlockHeight: cursor.number, finalizedBlockHash: cursor.hash }),
        ...(tailError && { tailError }),
      };
    },
  };
}
