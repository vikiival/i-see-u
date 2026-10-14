import { addTransfer, emptyAggregate } from '../core/aggregate.ts';
import type {
  Address,
  BlockRange,
  BlockRef,
  ChainAdapter,
  ChunkAggregate,
  Follower,
  FollowerCursor,
  FollowerStatus,
  Store,
  TransferEvent,
} from '../core/types.ts';
import type { LeaderLock } from '../store/advisory-lock.ts';

export interface FollowerOptions {
  adapter: ChainAdapter;
  store: Store;
  /** Leadership across processes; without one this follower always leads (tests, single process). */
  lock?: LeaderLock;
  pollMs: number;
  /** Discovery refuses (503) when the cursor is more than this many blocks behind the finalized head. */
  maxLagBlocks: bigint;
  /** Responses are flagged stale after this long without an advance. */
  staleAfterMs: number;
  /** Discovery refuses (503) after this long without an advance. */
  maxNoAdvanceMs: number;
  now?: () => number;
  /** Called once per tick that advanced the cursor (e.g. to refresh balances at the new block). */
  onAdvance?: (cursor: FollowerCursor) => void;
  onError?: (error: unknown) => void;
}

/** More than one epoch behind means the follower is not keeping up, even if still within maxLagBlocks. */
const STALE_LAG_BLOCKS = 32n;

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error)).slice(0, 200);

/**
 * One finalized follower per network. Each tick reads the finalized head and, when it is ahead of the global cursor,
 * fetches every Transfer log of (cursor, head] in ranges of at most adapter.followerMaxRange, folds the logs of
 * tracked accounts into per-account aggregates, and commits each range together with the cursor advance.
 */
export function createFollower(o: FollowerOptions): Follower {
  const now = o.now ?? Date.now;
  const { adapter, store } = o;
  const network = adapter.network;
  let tracked: Set<Address> | undefined; // loaded when this process becomes leader
  let cursor: FollowerCursor | undefined;
  let headNumber: bigint | undefined;
  let lastError: string | undefined;
  let leader = false;
  let stopped = false;
  let ticking: Promise<void> | undefined;
  let loop: Promise<void> | undefined;
  let wake = (): void => {};
  let kicked = false;

  /** Fetch, fold and commit one range; false when it must be refetched because the tracked set grew. */
  const applyRange = async (range: BlockRange, head: BlockRef): Promise<boolean> => {
    const known = new Set(tracked);
    const aggregates = new Map<Address, ChunkAggregate>();
    const add = (account: Address, event: TransferEvent): void => {
      let aggregate = aggregates.get(account);
      if (!aggregate) aggregates.set(account, aggregate = emptyAggregate());
      addTransfer(aggregate, account, event);
    };
    await adapter.scanAllTransfers(range, (event) => {
      if (known.has(event.from)) add(event.from, event);
      if (event.to !== event.from && known.has(event.to)) add(event.to, event);
    }, (address) => known.has(address));
    const to = range.toBlock === head.number ? head : await adapter.blockByNumber(range.toBlock);
    const at = now();
    const result = await store.applyFollowerRange(network, range, to, at, aggregates, known);
    if (!result.applied) {
      // Tracked (by another request or process) while this range was decoded: refetch it with them included.
      for (const account of result.missing) tracked!.add(account);
      return false;
    }
    cursor = { number: to.number, hash: to.hash, timestamp: to.timestamp, updatedAt: at };
    return true;
  };

  const run = async (): Promise<void> => {
    try {
      leader = o.lock ? await o.lock.tryAcquire() : true;
      if (!leader) tracked = undefined;
      const head = await adapter.finalizedHead();
      if (headNumber === undefined || head.block.number > headNumber) headNumber = head.block.number;
      cursor = await store.getFollowerCursor(network);
      if (leader) {
        cursor ??= await store.initFollowerCursor(network, head.block, now());
        tracked ??= new Set(await store.trackedAccounts(network));
        const startedAt = cursor.number;
        while (!stopped && cursor.number < head.block.number) {
          const end = cursor.number + adapter.followerMaxRange;
          await applyRange({
            fromBlock: cursor.number + 1n,
            toBlock: end < head.block.number ? end : head.block.number,
          }, head.block);
        }
        if (cursor.number > startedAt) o.onAdvance?.(cursor);
      }
      lastError = undefined;
    } catch (error) {
      lastError = messageOf(error);
      o.onError?.(error);
    }
  };

  const tick = (): Promise<void> => {
    ticking ??= run().finally(() => { ticking = undefined; });
    return ticking;
  };

  return {
    tick,

    async load() {
      cursor ??= await store.getFollowerCursor(network);
    },

    start() {
      if (loop) return;
      stopped = false;
      loop = (async () => {
        while (!stopped) {
          kicked = false;
          await tick();
          if (stopped) break;
          if (kicked) continue;
          await new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, o.pollMs);
            wake = () => {
              clearTimeout(timer);
              resolve();
            };
          });
        }
      })();
    },

    async stop() {
      stopped = true;
      wake();
      await loop;
      await ticking;
      loop = undefined;
      leader = false;
      tracked = undefined;
      await o.lock?.release();
    },

    status(): FollowerStatus {
      const lagBlocks = cursor && headNumber !== undefined && headNumber > cursor.number ? headNumber - cursor.number : 0n;
      const lastAdvanceAt = cursor?.updatedAt ?? 0;
      const idleMs = now() - lastAdvanceAt;
      const unhealthy = !cursor
        ? 'Follower has not initialized its cursor yet'
        : lagBlocks > o.maxLagBlocks
          ? `Follower is ${lagBlocks} blocks behind the finalized head`
          : idleMs > o.maxNoAdvanceMs ? 'Follower has not advanced recently enough' : undefined;
      return {
        ready: cursor !== undefined,
        leader,
        cursor,
        headNumber,
        lagBlocks,
        lastAdvanceAt,
        lastError,
        stale: lastError !== undefined || lagBlocks > STALE_LAG_BLOCKS || idleMs > o.staleAfterMs,
        unhealthy,
      };
    },

    kick() {
      kicked = true;
      wake();
    },

    track(account) {
      tracked?.add(account);
    },
  };
}
