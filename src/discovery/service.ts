import { addTransfer, emptyAggregate } from '../core/aggregate.ts';
import { unavailable } from '../core/limiter.ts';
import type { AccountView, Address, BlockHeader, ChainAdapter, DiscoveryService, IndexSnapshot, IndexState, ServingMetadata, ServingStatus, TailStatus } from '../core/types.ts';
import type { Store } from '../core/types.ts';

export interface DiscoveryDependencies {
  adapters: Record<string, ChainAdapter>;
  store: Store;
  maxHeadAgeMs: number;
  maxNoAdvanceMs: number;
  /** Hard observed finalized-head distance; an old observation alone remains servable. */
  maxServeLagBlocks?: bigint;
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

  const statusOf = (snapshot: IndexSnapshot | undefined): ServingStatus => {
    const state = snapshot?.state;
    let reason: string | undefined;

    if (snapshot?.bootstrap) return { ready: false, serving: 'partial', reason: 'Global index bootstrap is incomplete' };

    if (!state?.head || state.checkedAt === undefined) reason = 'Global index awaits bootstrap or finalized head';
    else if (state.pendingRepair) reason = 'Global index has pending repairs';
    else if (state.cursor.number === state.head.number && state.cursor.hash !== state.head.hash) reason = 'Global index head hash mismatch';
    else if (state.head.number - state.cursor.number > (deps.maxServeLagBlocks ?? 7200n)) reason = 'Global index has not reached the finalized head';
    else {
      if (state.cursor.number < state.head.number) reason = 'Global index has not reached the finalized head';
      else if (now() - state.checkedAt > deps.maxHeadAgeMs) reason = 'Finalized head observation is too old';
      else if (now() - state.cursor.updatedAt > deps.maxNoAdvanceMs) reason = 'Global index has not advanced recently enough';

      return { ready: reason === undefined, serving: reason ? 'stale' : 'full', ...(reason && { reason }) };
    }

    return { ready: false, serving: 'unavailable', reason };
  };

  const requireAvailable = <T extends IndexSnapshot>(snapshot: T | undefined): T => {
    const status = statusOf(snapshot);

    if (status.serving === 'unavailable') throw Object.assign(unavailable(status.reason!, 5), { serviceStatus: 'not_ready' });

    return snapshot!;
  };

  const metadata = (snapshot: IndexSnapshot, served?: BlockHeader | IndexState['cursor'], latest?: bigint): ServingMetadata => {
    if (snapshot.bootstrap) return {
      ...snapshot.bootstrap, status: 'partial', complete: false,
      note: 'Applied ranges only. Counts and last seen blocks are lower bounds; first seen blocks may move earlier. NFT owners may be outdated where later ranges are missing.',
    };
    const { cursor, head } = snapshot.state;
    const block = served ?? cursor;
    const status = statusOf(snapshot);

    return {
      status: 'ready', complete: true, blockHeight: block.number, blockHash: block.hash, blockTimestamp: block.timestamp, revision: cursor.revision,
      lastProgressAt: block === cursor ? cursor.updatedAt : Number(block.timestamp) * 1000,
      stale: status.serving === 'stale', ...(status.reason && { staleReason: status.reason }),
      lagBlocks: latest !== undefined && latest > block.number ? latest - block.number : 0n,
      followerLagBlocks: head!.number > cursor.number ? head!.number - cursor.number : 0n,
    };
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

      return statusOf(await deps.store.indexSnapshot(network));
    },
    async discover(network, input, options = {}) {
      const { adapter, account } = validate(network, input);
      const tail = options.finality === 'finalized' ? undefined : tailOf(network)?.status();
      const view = await deps.store.readAccount(network, account, { tail: tail !== undefined, balances: options.balances });
      const snapshot = requireAvailable(view);

      if (snapshot.bootstrap) return {
        ...metadata(snapshot), network, account, fromBlock: snapshot.bootstrap.startBlock, finality: 'finalized',
        ...(options.finality !== 'finalized' && { tailError: 'Global index bootstrap is incomplete' }),
        holdings: snapshot.holdings, nftItems: snapshot.nftItems,
      };

      const { cursor, startBlock } = snapshot.state;
      const staleReason = statusOf(snapshot).reason;
      const tailError = options.finality !== 'finalized' ? staleReason ?? (tail && tailProblem(tail, snapshot.tailTip, cursor.number)) : undefined;
      const tip = tail && !tailError ? snapshot.tailTip : undefined;
      const latest = tail?.latestNumber ?? adapter.cachedLatestBlockNumber?.() ?? cursor.number;

      return {
        ...metadata(snapshot, tip ?? cursor, latest), network, account, fromBlock: startBlock, finality: tip ? 'latest' : 'finalized',
        ...(tip && { finalizedBlockHeight: cursor.number, finalizedBlockHash: cursor.hash }),
        ...(tailError && { tailError }),
        ...(tip ? overlay(snapshot, account) : { holdings: snapshot.holdings, nftItems: snapshot.nftItems }),
        ...(snapshot.balances && { balances: snapshot.balances }),
      };
    },
    async history(network, input, limit, before, finality) {
      const { adapter, account } = validate(network, input);
      const tail = finality === 'finalized' ? undefined : tailOf(network)?.status();
      let page = requireAvailable(await deps.store.transferHistory(network, account, limit, before, tail !== undefined));

      let tailError = finality !== 'finalized'
        ? statusOf(page).reason ?? (tail && tailProblem(tail, page.tailTip, page.state!.cursor.number)) : undefined;

      // Recheck availability and metadata in the second snapshot; a latest cursor may now get 409.
      if (tailError && page.state && page.throughBlock > page.state.cursor.number) {
        page = requireAvailable(await deps.store.transferHistory(network, account, limit, before));
        tailError = statusOf(page).reason ?? tailError;
      }

      const cursor = page.state?.cursor;
      const tip = tail && !tailError ? page.tailTip : undefined;
      const latest = tail?.latestNumber ?? adapter.cachedLatestBlockNumber?.() ?? cursor?.number;

      return {
        ...metadata(page, tip ?? cursor, latest), network, account, page,
        fromBlock: page.state?.startBlock ?? page.bootstrap!.startBlock, finality: tip ? 'latest' : 'finalized',
        ...(tip && cursor && { finalizedBlockHeight: cursor.number, finalizedBlockHash: cursor.hash }),
        ...(tailError && { tailError }),
      };
    },
  };
}
