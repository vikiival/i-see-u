import { thrownMessage } from '../core/parse.ts';
import type { BlockHeader, ChainAdapter, Follower, FollowerStatus, IndexState, Store } from '../core/types.ts';
import type { LeaderLock } from '../store/advisory-lock.ts';
import { ChainGapError } from '../core/errors.ts';

export interface FollowerOptions {
  adapter: ChainAdapter;
  store: Store;
  lock?: LeaderLock;
  pollMs: number;
  maxNoAdvanceMs: number;
  /** Bound catch-up work per tick and header concurrency independently of user count. */
  maxRangesPerTick?: number;
  headerConcurrency?: number;
  now?: () => number;
  onError?: (message: string) => void;
}

export function createFollower(o: FollowerOptions): Follower {
  const now = o.now ?? Date.now;
  const { adapter, store } = o;
  let state: IndexState | undefined;
  let leader = false;
  let lastError: string | undefined;
  let stopped = false;
  let ticking: Promise<void> | undefined;
  let loop: Promise<void> | undefined;
  let wake = (): void => {};
  let kicked = false;
  const concurrency = o.headerConcurrency ?? 8;

  const fetchHeaders = async (from: bigint, to: bigint): Promise<BlockHeader[]> => {
    const headers: BlockHeader[] = [];

    for (let start = from; start <= to; start += BigInt(concurrency)) {
      const count = Number(to - start + 1n < BigInt(concurrency) ? to - start + 1n : BigInt(concurrency));
      headers.push(
        ...(await Promise.all(Array.from({ length: count }, (_, i) => adapter.headerByNumber(start + BigInt(i))))),
      );
    }

    return headers;
  };

  const run = async () => {
    try {
      leader = false;
      leader = o.lock ? await o.lock.tryAcquire() : true;
      state = await store.indexState(adapter.network);

      if (!state) {
        lastError = 'Global index awaits bootstrap initialization';

        return;
      }

      if (!leader) {
        lastError = undefined;

        return;
      }

      const head = await adapter.finalizedHead();

      if (head.stale || head.refreshFailed) {
        throw new Error('Finalized head observation is stale');
      }

      await store.observeHead(adapter.network, head.block, now());
      state = (await store.indexState(adapter.network))!;
      const target = state.head!;

      if (target.number === state.cursor.number && target.hash !== state.cursor.hash) {
        await store.enqueueRepair(
          adapter.network,
          { fromBlock: target.number, toBlock: target.number },
          'Finalized head hash changed',
          now(),
        );
        throw new ChainGapError('Finalized head hash changed');
      }

      for (
        let ranges = 0;
        !stopped && state.cursor.number < target.number && ranges < (o.maxRangesPerTick ?? 8);
        ranges += 1
      ) {
        if (o.lock && !o.lock.held()) {
          throw new Error('Follower leadership lost');
        }

        const end = state.cursor.number + adapter.followerMaxRange;
        const range = { fromBlock: state.cursor.number + 1n, toBlock: end < target.number ? end : target.number };
        const blocks = await fetchHeaders(range.fromBlock, range.toBlock);

        if (blocks[0].parentHash !== state.cursor.hash) {
          const suspect = state.cursor.number < state.startBlock ? state.startBlock : state.cursor.number;
          await store.enqueueRepair(
            adapter.network,
            { fromBlock: suspect, toBlock: suspect },
            'Next range parent hash mismatch',
            now(),
          );
          throw new ChainGapError('Next range parent hash mismatch');
        }

        try {
          for (const [i, block] of blocks.entries()) {
            if (block.number !== range.fromBlock + BigInt(i) || (i > 0 && block.parentHash !== blocks[i - 1].hash)) {
              throw new ChainGapError(`Header chain gap at ${block.number}`);
            }
          }

          const transfers = await adapter.transfersInRange(range, blocks);
          await store.appendRange(adapter.network, range, blocks, transfers, now());
        } catch (error) {
          if (error instanceof ChainGapError) {
            await store.enqueueRepair(adapter.network, range, error.message, now());
          }

          throw error;
        }

        state = (await store.indexState(adapter.network))!;
      }

      lastError = undefined;
    } catch (error) {
      lastError = thrownMessage(error).slice(0, 200);
      state = await store.indexState(adapter.network).catch(() => state);
      o.onError?.(lastError);
    }
  };

  const tick = (): Promise<void> => {
    ticking ??= run().finally(() => {
      ticking = undefined;
    });

    return ticking;
  };

  return {
    tick,
    async load() {
      state = await store.indexState(adapter.network);
    },
    start() {
      if (loop) {
        return;
      }

      stopped = false;
      loop = (async () => {
        while (!stopped) {
          kicked = false;
          await tick();

          if (stopped) {
            break;
          }

          if (kicked) {
            continue;
          }

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
      await o.lock?.release();
    },
    kick() {
      kicked = true;
      wake();
    },
    status(): FollowerStatus {
      const cursor = state?.cursor;
      const headNumber = state?.head?.number;

      const lagBlocks =
        cursor && headNumber !== undefined && headNumber > cursor.number ? headNumber - cursor.number : 0n;

      const lastAdvanceAt = cursor?.updatedAt ?? 0;

      const unhealthy = !state?.head
        ? 'Global index awaits bootstrap or finalized head'
        : state.pendingRepair
          ? 'Global index has pending repairs'
          : lagBlocks > 0n
            ? `Global index is ${lagBlocks} blocks behind finalized`
            : (lastError ??
              (now() - lastAdvanceAt > o.maxNoAdvanceMs ? 'Global index has not advanced recently enough' : undefined));

      return {
        headStalled: adapter.headStalled?.() ?? false,
        ready: unhealthy === undefined,
        leader: leader && (o.lock?.held() ?? true),
        cursor,
        headNumber,
        lagBlocks,
        lastAdvanceAt,
        lastError,
        stale: unhealthy !== undefined,
        unhealthy,
      };
    },
  };
}
