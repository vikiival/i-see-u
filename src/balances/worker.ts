import { unavailable, withDeadline } from '../core/limiter.ts';
import type { Limiter } from '../core/limiter.ts';
import type { Address, BalanceSnapshot, BlockRef, ChainAdapter, Hex, Store } from '../core/types.ts';

/** Bounded concurrency and a per-request deadline for RPC work left on the request path. */
export interface RequestRpc {
  limiter: Limiter;
  deadlineMs: number;
}

export interface BalanceWorkerOptions {
  adapter: ChainAdapter;
  store: Store;
  /** An account is active (refreshed by the worker) for this long after its last balances request. */
  activeTtlMs: number;
  refreshMs: number;
  requestRpc: RequestRpc;
  /** When enabled, balances refresh at the persisted tail tip instead of the finalized cursor. */
  tailTip?: () => Promise<BlockRef | undefined>;
  now?: () => number;
  onError?: (error: unknown) => void;
}

export interface BalanceWorker {
  /**
   * Snapshots for `tokens` in input order, each labelled with the block it was read at. Marks the account active.
   * Only when a token has no snapshot at all is the account read immediately, at `at` (single-flight per account+block).
   */
  forAccount(account: Address, tokens: Address[], at: BlockRef): Promise<BalanceSnapshot[]>;
  /** Read every (active account, ERC-20 token) pair at the follower cursor, batched across accounts. */
  refresh(): Promise<{ pairs: number; block?: bigint }>;
  /** Refresh now (the follower advanced) and restart the interval. */
  kick(): void;
  start(): void;
  stop(): Promise<void>;
  stats(): { activeAccounts: number; lastRefresh?: { at: number; block?: bigint; pairs: number } };
}

export function createBalanceWorker(o: BalanceWorkerOptions): BalanceWorker {
  const now = o.now ?? Date.now;
  const { adapter, store } = o;
  const network = adapter.network;
  const active = new Map<Address, number>();
  /** First-request reads at block readsAt, per account. */
  const reads = new Map<Address, Promise<void>>();
  let readsAt: Hex | undefined;
  let refreshing: Promise<{ pairs: number; block?: bigint }> | undefined;
  let kicked = false;
  let stopped = true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let lastRefresh: { at: number; block?: bigint; pairs: number } | undefined;

  const readNow = async (account: Address, tokens: Address[], at: BlockRef): Promise<void> => {
    const results = await adapter.erc20BalancePairs(tokens.map((token) => ({ account, token })), at.hash);
    await store.saveBalances(network, at, tokens.map((token, i) => ({ account, token, result: results[i] })));
  };

  const refresh = (): Promise<{ pairs: number; block?: bigint }> => {
    refreshing ??= (async () => {
      const cutoff = now() - o.activeTtlMs;
      for (const [account, seenAt] of active) if (seenAt < cutoff) active.delete(account);
      const cursor = await store.getFollowerCursor(network);
      if (!cursor || active.size === 0) return { pairs: 0 };
      const at = await o.tailTip?.() ?? cursor;
      const pairs = await store.erc20Pairs(network, [...active.keys()]);
      if (pairs.length === 0) return { pairs: 0, block: at.number };
      // One block for every pair, pinned by hash; a balance read at N is never reported for a later block.
      const results = await adapter.erc20BalancePairs(pairs, at.hash);
      await store.saveBalances(network, at, pairs.map((pair, i) => ({ ...pair, result: results[i] })));
      return { pairs: pairs.length, block: at.number };
    })().then((result) => {
      lastRefresh = { at: now(), ...result };
      return result;
    }).finally(() => { refreshing = undefined; });
    return refreshing;
  };

  const schedule = (): void => {
    clearTimeout(timer);
    if (!stopped) timer = setTimeout(() => void cycle(), o.refreshMs);
  };
  const cycle = async (): Promise<void> => {
    do {
      kicked = false;
      try {
        await refresh();
      } catch (error) {
        o.onError?.(error);
      }
    } while (kicked && !stopped);
    schedule();
  };

  return {
    async forAccount(account, tokens, at) {
      active.set(account, now());
      if (tokens.length === 0) return [];
      const snapshot = async () => new Map((await store.balances(network, account)).map((row) => [row.token, row]));
      let byToken = await snapshot();
      if (tokens.some((token) => !byToken.has(token))) {
        // Kept until the block changes (not just while in flight), so a request that read the snapshot before
        // another request's read committed still joins that read instead of starting a second one.
        if (at.hash !== readsAt) {
          reads.clear();
          readsAt = at.hash;
        }
        let read = reads.get(account);
        if (!read) {
          read = o.requestRpc.limiter.run(() => readNow(account, tokens, at));
          read.catch(() => { if (reads.get(account) === read) reads.delete(account); });
          reads.set(account, read);
        }
        await withDeadline(read, o.requestRpc.deadlineMs, () => {
          throw unavailable('Balance read did not finish within the request deadline', 2);
        });
        byToken = await snapshot();
      }
      return tokens.map((token) => byToken.get(token) ?? {
        token,
        result: { status: 'error', error: 'no balance snapshot' },
        blockNumber: at.number,
        blockHash: at.hash,
      });
    },

    refresh,

    kick() {
      if (stopped) return;
      if (refreshing) kicked = true;
      else void cycle();
    },

    start() {
      if (!stopped) return;
      stopped = false;
      schedule();
    },

    async stop() {
      stopped = true;
      clearTimeout(timer);
      await refreshing?.catch(() => {});
    },

    stats: () => ({ activeAccounts: active.size, lastRefresh }),
  };
}
