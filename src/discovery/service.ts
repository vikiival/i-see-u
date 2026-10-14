import type {
  Address,
  BlockRange,
  ChainAdapter,
  ChunkScan,
  DiscoveryResult,
  DiscoveryService,
  Store,
} from '../core/types.ts';

interface DiscoveryDependencies {
  adapters: Record<string, ChainAdapter>;
  store: Store;
  /** Refuse discovery when finalized progress is older than this. */
  maxHeadProgressAgeMs?: number;
}

type ChunkOutcome = ({ ok: true } & ChunkScan) | { ok: false; error: unknown };

interface ChunkSlot {
  range: BlockRange;
  outcome?: ChunkOutcome;
}

const DEFAULT_MAX_HEAD_PROGRESS_AGE_MS = 1_800_000;
/** Consecutive light chunks (at the current range) before the range doubles again. */
const GROW_AFTER = 3;

export function createDiscoveryService(deps: DiscoveryDependencies): DiscoveryService {
  const maxHeadProgressAgeMs = deps.maxHeadProgressAgeMs ?? DEFAULT_MAX_HEAD_PROGRESS_AGE_MS;
  const inFlight = new Map<string, Promise<DiscoveryResult>>();

  const scan = async (adapter: ChainAdapter, account: Address): Promise<DiscoveryResult> => {
    const startedAt = Date.now();
    const head = await adapter.finalizedHead();
    if (Date.now() - head.lastProgressAt > maxHeadProgressAgeMs) {
      throw Object.assign(new Error('Finalized chain head has not progressed recently enough'), { statusCode: 503 });
    }

    const previousCursor = await deps.store.getCursor(adapter.network, account);
    // Progress is never below the cursor (finalizeScan advances both); above it means uncommitted chunks.
    const appliedTo = await deps.store.getProgress(adapter.network, account);
    if (appliedTo !== undefined && appliedTo > (previousCursor?.number ?? -1n) && appliedTo > head.block.number) {
      throw Object.assign(new Error('Finalized chain head is behind persisted scan progress'), { statusCode: 503 });
    }
    const fromBlock = appliedTo !== undefined && appliedTo >= adapter.startBlock ? appliedTo + 1n : adapter.startBlock;

    let newBlocks = 0n;
    let rpcCalls = 0;
    if (fromBlock <= head.block.number) {
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
        while (!stopped && active < concurrency && window.length < 2 * concurrency && nextFrom <= head.block.number) {
          const end = nextFrom + range - 1n;
          const slot: ChunkSlot = { range: { fromBlock: nextFrom, toBlock: end < head.block.number ? end : head.block.number } };
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

    // A lagging head keeps the newer published pair.
    if (!previousCursor || head.block.number > previousCursor.number) {
      await deps.store.finalizeScan(adapter.network, account, head.block);
    }
    const cursor = (await deps.store.getCursor(adapter.network, account))!;
    const latest = adapter.cachedLatestBlockNumber?.() ?? head.block.number;
    const [holdings, nftItems] = await Promise.all([
      deps.store.holdings(adapter.network, account),
      deps.store.nftItems(adapter.network, account),
    ]);
    return {
      network: adapter.network,
      account,
      fromBlock: adapter.startBlock,
      blockHeight: cursor.number,
      blockHash: cursor.hash,
      blockTimestamp: cursor.timestamp,
      finality: 'finalized',
      lastProgressAt: head.lastProgressAt,
      stale: head.stale,
      lagBlocks: latest > cursor.number ? latest - cursor.number : 0n,
      holdings,
      nftItems,
      scan: { newBlocks, rpcCalls, ms: Date.now() - startedAt },
    };
  };

  return {
    async discover(network: string, input: string): Promise<DiscoveryResult> {
      const adapter = deps.adapters[network];
      if (!adapter) {
        throw Object.assign(new Error(`Unknown network: ${network}`), { statusCode: 404 });
      }
      const account = adapter.normalizeAddress(input);
      if (!account) throw Object.assign(new Error('Invalid account address'), { statusCode: 400 });
      const key = `${network}:${account}`;
      const existing = inFlight.get(key);
      if (existing) return existing;

      const pending = scan(adapter, account).finally(() => {
        inFlight.delete(key);
      });
      inFlight.set(key, pending);
      return pending;
    },
  };
}
