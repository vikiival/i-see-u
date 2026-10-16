import { unavailable, withDeadline } from '../core/limiter.ts';
import type { Limiter } from '../core/limiter.ts';
import type { Address, BalancePair, BalanceResult, BalanceSnapshot, BlockRef, ChainAdapter, Holding, Store, StoredBalance } from '../core/types.ts';
import { rpcErrorOf } from '../rpc/errors.ts';

export interface RequestRpc {
  limiter: Limiter;
  deadlineMs: number;
}

/** A balance as served. blockNumber is where it was read; validAt is the served block when the value provably holds there. */
export interface ServedBalance extends BalanceSnapshot {
  validAt?: bigint;
  /** Served from balance_snapshots without RPC in this request. */
  cached: boolean;
}

export interface BalanceReader {
  /**
   * ERC-20 balances of the account's holdings at the served block (tail tip or finalized cursor), read or reused.
   * `holdings.lastBlock` must include tail Transfers when `at` is the tail tip. `stored` are the account's snapshots
   * read in the same database snapshot as the holdings (fetched here when omitted). Never refreshes in the background.
   */
  forAccount(account: Address, holdings: Pick<Holding, 'token' | 'lastBlock'>[], at: BlockRef, revision: bigint, stored?: StoredBalance[]): Promise<ServedBalance[]>;
}

export interface BalanceReaderOptions {
  adapter: ChainAdapter;
  store: Store;
  requestRpc: RequestRpc;
  /** Pairs per shared Multicall; a full batch is sent at once. */
  batchSize: number;
  /** How long the first uncached pair of a batch waits for pairs of other requests. */
  windowMs: number;
  /** Balances that change without a Transfer: never reused at a newer block. */
  alwaysRefresh: readonly Address[];
}

interface Read {
  result: BalanceResult;
  readAt: number;
}

interface Batch {
  at: BlockRef;
  revision: bigint;
  pairs: BalancePair[];
  settle: { resolve: (read: Read) => void; reject: (error: Error) => void }[];
  deadlineAt: number;
  timer?: ReturnType<typeof setTimeout>;
}

/**
 * A snapshot read at R serves block S (same repair revision) when R == S with the same hash, or when R < S, R's hash is
 * still canonical (finalized header or current tail block), the token is not always-refresh, and the holding's last
 * Transfer block, finalized or tail, is <= R (no Transfer touched the pair in (R, S]). A reorg of R's block, or a tail
 * Transfer above R, therefore forces a new read.
 */
export const reusable = (snapshot: StoredBalance, lastBlock: bigint, at: BlockRef, revision: bigint, always: ReadonlySet<Address>): boolean =>
  snapshot.revision === revision && (snapshot.blockNumber === at.number
    ? snapshot.blockHash === at.hash
    : snapshot.blockNumber < at.number && snapshot.canonical && lastBlock <= snapshot.blockNumber && !always.has(snapshot.token));

export function createBalanceReader(o: BalanceReaderOptions): BalanceReader {
  const network = o.adapter.network;
  const always = new Set(o.alwaysRefresh);
  /** Single flight per (block hash, revision, account, token) while a read is queued or running. */
  const inflight = new Map<string, Promise<Read>>();
  /** At most one open (unsent) batch per served block and revision. */
  const open = new Map<string, Batch>();

  const flush = (key: string): void => {
    const batch = open.get(key);

    if (!batch) return;
    open.delete(key);
    clearTimeout(batch.timer);
    const { at, revision, pairs } = batch;

    o.requestRpc.limiter.run(async () => {
      let results: BalanceResult[];

      try {
        results = await o.adapter.erc20BalancePairs(pairs, at.hash);
      } catch (error) {
        // Outage, auth or rate limit: fail every waiter fast and cache nothing. Per-token reverts arrive as results.
        throw rpcErrorOf(error) ? Object.assign(unavailable('Balance RPC unavailable', 5), { cause: error }) : error;
      }

      const readAt = Date.now();
      const rows = pairs.map((pair, i) => ({ ...pair, result: results[i] }));
      // Transient per-call failures are served but never cached; reverts and bad return data are stored as errors.
      const persistent = rows.filter((row) => row.result.status !== 'error' || !row.result.transient);

      // A repair fails the read; a reorg of the served tail block only skips caching (the values stay exact at their block).
      if (!await o.store.saveBalances(network, at, persistent, readAt, revision)
        && (await o.store.indexState(network))?.cursor.revision !== revision) throw unavailable('Index repaired during balance read; retry', 1);

      return rows.map((row) => ({ result: row.result, readAt }));
    }, { deadlineAt: batch.deadlineAt }).then(
      (reads) => reads.forEach((read, i) => batch.settle[i].resolve(read)),
      (error) => batch.settle.forEach((s) => s.reject(error instanceof Error ? error : new Error(String(error)))),
    );
  };

  const read = (pair: BalancePair, at: BlockRef, revision: bigint, deadlineAt: number): Promise<Read> => {
    const key = `${at.hash}:${revision}:${pair.account}:${pair.token}`;
    const existing = inflight.get(key);

    if (existing) return existing;
    const batchKey = `${at.hash}:${revision}`;
    let batch = open.get(batchKey);

    if (!batch) {
      const created: Batch = { at, revision, pairs: [], settle: [], deadlineAt };
      created.timer = setTimeout(() => flush(batchKey), o.windowMs);
      open.set(batchKey, batch = created);
    }

    const target = batch;

    const promise = new Promise<Read>((resolve, reject) => {
      target.pairs.push(pair);
      target.settle.push({ resolve, reject });
    });

    if (deadlineAt > target.deadlineAt) target.deadlineAt = deadlineAt;
    inflight.set(key, promise);
    promise.then(() => inflight.delete(key), () => inflight.delete(key));

    if (target.pairs.length >= o.batchSize) flush(batchKey);

    return promise;
  };

  return {
    async forAccount(account, holdings, at, revision, stored) {
      if (holdings.length === 0) return [];
      const deadlineAt = Date.now() + o.requestRpc.deadlineMs;
      const snapshots = new Map((stored ?? await o.store.balances(network, account)).map((snapshot) => [snapshot.token, snapshot]));
      let misses = 0;

      const served = holdings.map(({ token, lastBlock }): ServedBalance | Promise<ServedBalance> => {
        const snapshot = snapshots.get(token);

        if (snapshot && reusable(snapshot, lastBlock, at, revision, always)) {
          // A cached error keeps its read block and is never claimed valid at the served block.
          const { canonical: _canonical, ...read } = snapshot;

          return { ...read, cached: true, ...(snapshot.result.status === 'ok' && { validAt: at.number }) };
        }

        misses += 1;

        return read({ account, token }, at, revision, deadlineAt).then(({ result, readAt }) => ({
          token, result, blockNumber: at.number, blockHash: at.hash, readAt, revision, cached: false,
          ...(result.status === 'ok' && { validAt: at.number }),
        }));
      });

      const all = Promise.all(served);

      return misses === 0 ? all : withDeadline(all, o.requestRpc.deadlineMs, () => { throw unavailable('Balance read exceeded request deadline', 2); });
    },
  };
}
