import { createLimiter, unavailable, withDeadline } from '../core/limiter.ts';
import type { Limiter } from '../core/limiter.ts';
import { addTransfer, emptyAggregate } from '../core/aggregate.ts';
import type {
  Address,
  BlockRange,
  ChainAdapter,
  ChunkScan,
  DiscoveryResult,
  DiscoveryService,
  Follower,
  Store,
  SyncingResult,
  TailStatus,
  Tracking,
} from '../core/types.ts';

interface DiscoveryDependencies {
  adapters: Record<string, ChainAdapter>;
  store: Store;
  followers: Record<string, Pick<Follower, 'status' | 'track'>>;
  tails?: Record<string, { status(): TailStatus }>;
  /** How long a request waits for a cold backfill before answering `syncing`; the backfill continues. */
  requestDeadlineMs?: number;
  /** Bounds concurrent (and queued) backfills; beyond it requests get 503 with Retry-After. */
  backfillLimiter?: Limiter;
  /**
   * Serve `latest` only while the shared tail tip's own block timestamp is at most this old (undefined: no check).
   * Every process reads the same tip, so a stalled leader degrades all of them, not just itself.
   */
  maxTailTipAgeMs?: number;
  now?: () => number;
}

interface BackfillStats {
  newBlocks: bigint;
  rpcCalls: number;
  ms: number;
}

type ChunkOutcome = ({ ok: true } & ChunkScan) | { ok: false; error: unknown };

interface ChunkSlot {
  range: BlockRange;
  outcome?: ChunkOutcome;
}

const DEFAULT_REQUEST_DEADLINE_MS = 10_000;

/** Consecutive light chunks (at the current range) before the range doubles again. */
const GROW_AFTER = 3;

const backfilled = (adapter: ChainAdapter, tracking: Tracking): boolean =>
  tracking.target < adapter.startBlock || (tracking.progress !== undefined && tracking.progress >= tracking.target);

export function createDiscoveryService(deps: DiscoveryDependencies): DiscoveryService {
  const now = deps.now ?? Date.now;
  const requestDeadlineMs = deps.requestDeadlineMs ?? DEFAULT_REQUEST_DEADLINE_MS;
  const limiter = deps.backfillLimiter ?? createLimiter(4, 64);
  const inFlight = new Map<string, Promise<BackfillStats>>();

  /** Resumable chunked scan of [startBlock, target]: the follower owns every block above target. */
  const backfill = async (adapter: ChainAdapter, account: Address): Promise<BackfillStats> => {
    const startedAt = Date.now();
    const tracking = await deps.store.getTracking(adapter.network, account);

    if (!tracking) throw new Error(`Account ${account} is not tracked`);
    const target = tracking.target;
    const appliedTo = tracking.progress;
    const fromBlock = appliedTo !== undefined && appliedTo >= adapter.startBlock ? appliedTo + 1n : adapter.startBlock;

    let newBlocks = 0n;
    let rpcCalls = 0;

    if (fromBlock <= target) {
      const source = adapter.accountLogs;
      const maxRange = source.preferredMaxRange;
      const minRange = source.adaptiveRange?.minRange ?? maxRange;
      const concurrency = adapter.scanConcurrency;
      // Per-account adaptive persistence belongs to the source that understands `heavy` (RPC). A fixed-range
      // HyperSync source deliberately ignores an old RPC range and keeps its large independently committed chunks.
      const persisted = source.adaptiveRange ? await deps.store.getLogsRange(adapter.network, account) : undefined;
      let range = persisted === undefined || persisted > maxRange ? maxRange : persisted < minRange ? minRange : persisted;
      let lightStreak = 0;

      // Called in completion order: shrink below a heavy chunk's own size, grow after a streak of light ones.
      const adapt = (chunk: BlockRange, heavy: boolean): void => {
        const size = chunk.toBlock - chunk.fromBlock + 1n;

        if (heavy) {
          const half = size / 2n > minRange ? size / 2n : minRange;

          if (half < range) range = half;
          lightStreak = 0;
        } else if (size >= range && ++lightStreak >= GROW_AFTER) {
          range = range * 2n < maxRange ? range * 2n : maxRange;
          lightStreak = 0;
        }
      };

      // Uncommitted chunks in block order. A new chunk starts whenever any in-flight one finishes;
      // completed chunks wait here until every earlier chunk is committed, bounded at 2x concurrency.
      const window: ChunkSlot[] = [];
      let active = 0;
      let nextFrom = fromBlock;
      let stopped = false;
      let wake = (): void => {};
      const settled = (): Promise<void> => new Promise<void>((resolve) => { wake = resolve; });

      const launch = (): void => {
        while (!stopped && active < concurrency && window.length < 2 * concurrency && nextFrom <= target) {
          const end = nextFrom + range - 1n;
          const slot: ChunkSlot = { range: { fromBlock: nextFrom, toBlock: end < target ? end : target } };
          nextFrom = slot.range.toBlock + 1n;
          window.push(slot);
          active += 1;
          source.scanAccount(account, slot.range).then(
            (value): ChunkOutcome => ({ ok: true, ...value }),
            (error: unknown): ChunkOutcome => ({ ok: false, error }),
          ).then((outcome) => {
            slot.outcome = outcome;
            active -= 1;

            if (outcome.ok) adapt(slot.range, outcome.heavy);
            else stopped = true; // commit what precedes the failure, start nothing new
            launch();
            wake();
          });
        }
      };

      try {
        launch();

        while (window.length > 0) {
          const outcome = window[0].outcome;

          if (!outcome) {
            await settled();
            continue;
          }

          if (!outcome.ok) throw outcome.error;
          const { range: done } = window.shift()!;
          await deps.store.applyScanChunk(adapter.network, account, done, outcome.aggregate);
          rpcCalls += outcome.calls;
          newBlocks += done.toBlock - done.fromBlock + 1n;
          launch();
        }
      } finally {
        stopped = true;

        while (active > 0) await settled();

        if (source.adaptiveRange && range !== persisted) await deps.store.setLogsRange(adapter.network, account, range);
      }
    }

    return { newBlocks, rpcCalls, ms: Date.now() - startedAt };
  };

  const startBackfill = (adapter: ChainAdapter, account: Address): Promise<BackfillStats> => {
    const key = `${adapter.network}:${account}`;
    let pending = inFlight.get(key);

    if (!pending) {
      pending = limiter.run(() => backfill(adapter, account)).finally(() => inFlight.delete(key));
      inFlight.set(key, pending);
    }

    return pending;
  };

  return {
    async discover(network: string, input: string, requestedFinality?: 'latest' | 'finalized'): Promise<DiscoveryResult | SyncingResult> {
      // Own-property lookup: names like `constructor` must 404, not hit Object.prototype.
      const adapter = Object.hasOwn(deps.adapters, network) ? deps.adapters[network] : undefined;
      const follower = Object.hasOwn(deps.followers, network) ? deps.followers[network] : undefined;

      if (!adapter || !follower) {
        throw Object.assign(new Error(`Unknown network: ${network}`), { statusCode: 404 });
      }

      const account = adapter.normalizeAddress(input);

      if (!account) throw Object.assign(new Error('Invalid account address'), { statusCode: 400 });

      const status = follower.status();

      if (status.unhealthy) throw unavailable(status.unhealthy, 30);
      // Tracked first (target = follower cursor, atomically), then backfilled up to that target.
      let tracking = await deps.store.getTracking(network, account) ?? await deps.store.trackAccount(network, account, now());
      follower.track(account);

      let scan: BackfillStats = { newBlocks: 0n, rpcCalls: 0, ms: 0 };

      if (!backfilled(adapter, tracking)) {
        const done = await withDeadline(startBackfill(adapter, account), requestDeadlineMs, () => undefined);

        if (!done) {
          tracking = await deps.store.getTracking(network, account) ?? tracking;
          const blocks = (to: bigint | undefined): bigint => to !== undefined && to >= adapter.startBlock ? to - adapter.startBlock + 1n : 0n;

          return {
            status: 'syncing',
            network,
            account,
            fromBlock: adapter.startBlock,
            progressBlocks: blocks(tracking.progress),
            targetBlocks: blocks(tracking.target),
            followerLagBlocks: status.lagBlocks,
          };
        }

        scan = done;
      }

      // Pure database read from here on: no RPC on the request path once the account is backfilled.
      const view = await deps.store.readAccount(network, account);

      if (!view) throw unavailable('Follower has no cursor yet', 5);
      const { cursor } = view;
      const tailStatus = (deps.tails && Object.hasOwn(deps.tails, network) ? deps.tails[network] : undefined)?.status();
      const wantsLatest = requestedFinality !== 'finalized' && tailStatus !== undefined;
      const tipAgeMs = view.tailTip && now() - Number(view.tailTip.timestamp) * 1000;

      const tipTooOld = wantsLatest && deps.maxTailTipAgeMs !== undefined && tipAgeMs !== undefined
        && tipAgeMs > deps.maxTailTipAgeMs;

      const serveLatest = wantsLatest && tailStatus.healthy && view.tailTip !== undefined && !tipTooOld;
      let holdings = view.holdings;
      let nftItems = view.nftItems;

      if (serveLatest) {
        const aggregate = emptyAggregate();

        for (const event of view.tailTransfers) addTransfer(aggregate, account, event);
        const byToken = new Map(holdings.map((holding) => [holding.token, { ...holding }]));

        for (const [token, tail] of aggregate.tokens) {
          const base = byToken.get(token);

          if (base) {
            base.firstBlock = tail.firstBlock < base.firstBlock ? tail.firstBlock : base.firstBlock;
            base.lastBlock = tail.lastBlock > base.lastBlock ? tail.lastBlock : base.lastBlock;
            base.sentCount += tail.sentCount;
            base.receivedCount += tail.receivedCount;

            if (tail.sentCount > 0) base.unfinalizedSentCount = tail.sentCount;

            if (tail.receivedCount > 0) base.unfinalizedReceivedCount = tail.receivedCount;
          } else {
            byToken.set(token, {
              token,
              ...tail,
              ...(tail.sentCount > 0 && { unfinalizedSentCount: tail.sentCount }),
              ...(tail.receivedCount > 0 && { unfinalizedReceivedCount: tail.receivedCount }),
            });
          }
        }

        holdings = [...byToken.values()].sort((a, b) => a.lastBlock === b.lastBlock
          ? a.token.localeCompare(b.token) : a.lastBlock > b.lastBlock ? -1 : 1);
        const byItem = new Map(nftItems.map((item) => [`${item.token}:${item.tokenId}`, { ...item }]));

        for (const [key, item] of aggregate.nfts) byItem.set(key, {
          token: item.token,
          tokenId: item.tokenId,
          heldBySeenLogs: item.held,
          lastBlock: item.block,
          unfinalized: true,
        });
        nftItems = [...byItem.values()].sort((a, b) => a.lastBlock === b.lastBlock
          ? `${a.token}:${a.tokenId}`.localeCompare(`${b.token}:${b.tokenId}`) : a.lastBlock > b.lastBlock ? -1 : 1);
      }

      const served = serveLatest ? view.tailTip! : cursor;
      const latest = tailStatus?.latestNumber ?? adapter.cachedLatestBlockNumber?.() ?? served.number;
      const head = status.headNumber ?? cursor.number;

      return {
        status: 'ready',
        network,
        account,
        fromBlock: adapter.startBlock,
        blockHeight: served.number,
        blockHash: served.hash,
        blockTimestamp: served.timestamp,
        finality: serveLatest ? 'latest' : 'finalized',
        ...(serveLatest && { finalizedBlockHeight: cursor.number, finalizedBlockHash: cursor.hash }),
        ...(wantsLatest && !serveLatest && {
          tailError: tailStatus?.tailError
            ?? (tipTooOld ? `Tail tip is ${Math.round(tipAgeMs! / 1000)} s old` : 'Tail has no complete snapshot yet'),
        }),
        // The shared tip's block time, not process memory: identical in every process, leader or not.
        lastProgressAt: serveLatest ? Number(view.tailTip!.timestamp) * 1000 : cursor.updatedAt,
        stale: serveLatest ? false : status.stale,
        lagBlocks: latest > served.number ? latest - served.number : 0n,
        followerLagBlocks: head > cursor.number ? head - cursor.number : 0n,
        holdings,
        nftItems,
        scan,
      };
    },
  };
}
