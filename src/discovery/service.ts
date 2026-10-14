import { createLimiter, unavailable, withDeadline } from '../core/limiter.ts';
import type { Limiter } from '../core/limiter.ts';
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
  Tracking,
} from '../core/types.ts';

interface DiscoveryDependencies {
  adapters: Record<string, ChainAdapter>;
  store: Store;
  followers: Record<string, Pick<Follower, 'status' | 'track'>>;
  /** How long a request waits for a cold backfill before answering `syncing`; the backfill continues. */
  requestDeadlineMs?: number;
  /** Bounds concurrent (and queued) backfills; beyond it requests get 503 with Retry-After. */
  backfillLimiter?: Limiter;
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
      const { logsMinRange: minRange, logsMaxRange: maxRange, scanConcurrency: concurrency } = adapter;
      const persisted = await deps.store.getLogsRange(adapter.network, account);
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
          adapter.scanChunk(account, slot.range).then(
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
        if (range !== persisted) await deps.store.setLogsRange(adapter.network, account, range);
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
    async discover(network: string, input: string): Promise<DiscoveryResult | SyncingResult> {
      const adapter = deps.adapters[network];
      const follower = deps.followers[network];
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
      const latest = adapter.cachedLatestBlockNumber?.() ?? cursor.number;
      const head = status.headNumber ?? cursor.number;
      return {
        status: 'ready',
        network,
        account,
        fromBlock: adapter.startBlock,
        blockHeight: cursor.number,
        blockHash: cursor.hash,
        blockTimestamp: cursor.timestamp,
        finality: 'finalized',
        lastProgressAt: cursor.updatedAt,
        stale: status.stale,
        lagBlocks: latest > cursor.number ? latest - cursor.number : 0n,
        followerLagBlocks: head > cursor.number ? head - cursor.number : 0n,
        holdings: view.holdings,
        nftItems: view.nftItems,
        scan,
      };
    },
  };
}
