import { thrownMessage } from '../core/parse.ts';
import type { BlockRange, ChainAdapter, Hex, Store } from '../core/types.ts';
import type { BootstrapHeader, BootstrapSource } from './sources.ts';
import type { BootstrapStatus, BootstrapStore, Segment } from './store.ts';

/** A source answered, but its data contradicts the header chain or the other source. */
export class VerificationError extends Error {}

export interface BootstrapOptions {
  network: string;
  /** Configured start block S (START_BLOCK); must equal any stored run or index. */
  startBlock: bigint;
  adapter: Pick<ChainAdapter, 'finalizedHead' | 'headerByNumber'>;
  index: Pick<Store, 'indexState'>;
  store: BootstrapStore;
  /** Preferred for headers and for the low end of the segment queue; optional (RPC-only works, at 1 call per header). */
  hypersync?: BootstrapSource;
  rpc: BootstrapSource;
  /** Blocks per segment: the unit of commit, retry and resume (default 1,000). */
  segmentBlocks?: bigint;
  /** Blocks per header request (HyperSync returns ~5,000 per request). */
  headerChunkBlocks?: bigint;
  /** Concurrent RPC segment workers (default 4); HyperSync always has exactly one. */
  rpcWorkers?: number;
  maintenanceWorkMem?: string;
  /** VACUUM (ANALYZE) after finalize (default true). */
  analyze?: boolean;
  progressMs?: number;
  now?: () => number;
  log?: (message: string) => void;
}

export type BootstrapResult =
  | { status: 'already-initialized' }
  | { status: 'done'; target: bigint; transfers: number; headers: number; failedAttempts: number; seconds: number };

export class StartBlockMismatchError extends Error {}

const mismatch = (stored: bigint, configured: bigint, what: string) => new StartBlockMismatchError(
  `START_BLOCK is ${configured} but the ${what} starts at ${stored}; a different start block needs a fresh database`,
);

/** Startup guard: the configured start block must match the initialized index and any bootstrap in progress. */
export async function assertStartBlock(index: Pick<Store, 'indexState'>, store: BootstrapStore, network: string, startBlock: bigint): Promise<void> {
  const state = await index.indexState(network);

  if (state && state.startBlock !== startBlock) throw mismatch(state.startBlock, startBlock, `${network} index`);
  const run = await store.run(network);

  if (run && run.startBlock !== startBlock) throw mismatch(run.startBlock, startBlock, `${network} bootstrap run`);
}

/** Contiguous numbers, linked parents, joined to `parentHash`, optionally ending at `lastHash`. */
function verifyHeaders(range: BlockRange, headers: BootstrapHeader[], parentHash: Hex, lastHash?: Hex): void {
  if (BigInt(headers.length) !== range.toBlock - range.fromBlock + 1n) throw new VerificationError(`Expected ${range.toBlock - range.fromBlock + 1n} headers, got ${headers.length}`);
  let parent = parentHash;

  for (const [i, header] of headers.entries()) {
    if (header.number !== range.fromBlock + BigInt(i)) throw new VerificationError(`Header ${header.number} out of order`);

    if (header.parentHash !== parent) throw new VerificationError(`Parent-hash chain breaks at ${header.number}`);
    parent = header.hash;
  }

  if (lastHash !== undefined && parent !== lastHash) throw new VerificationError('Header chain does not end at the pinned finalized block');
}

const describe = (status: BootstrapStatus): string => {
  const eta = status.etaS === undefined ? '' : `, ETA ${Math.floor(status.etaS / 60)}m${status.etaS % 60}s`;
  const rate = status.logsPerS === undefined ? '' : `, ${status.logsPerS} logs/s`;

  return `${status.status}: headers ${status.headersLoaded}/${status.headersTotal}, segments ${status.segmentsDone}/${status.segmentsTotal}`
    + `${status.segmentsFailed ? ` (${status.segmentsFailed} failed)` : ''}, ${status.logs} logs${rate}${eta}`;
};

/**
 * One-time global load of [S, B] (B = the finalized block pinned when the run is created), resumable per segment:
 * headers (HyperSync, RPC fallback) -> verified segments from HyperSync (low end) and RPC workers (high end), each
 * refetched from the other source on any failure -> one finalize transaction that publishes transfers, headers,
 * holdings and nft_ownership with ingestion_state at B. The follower then continues at B + 1.
 */
export async function runBootstrap(o: BootstrapOptions): Promise<BootstrapResult> {
  const now = o.now ?? Date.now;
  const log = o.log ?? (() => {});
  const { network, store } = o;
  const started = now();
  const state = await o.index.indexState(network);

  if (state) {
    if (state.startBlock !== o.startBlock) throw mismatch(state.startBlock, o.startBlock, `${network} index`);

    return { status: 'already-initialized' };
  }

  if (o.startBlock < 1n) throw new Error('START_BLOCK must be at least 1');
  let run = await store.run(network);

  if (run && run.startBlock !== o.startBlock) throw mismatch(run.startBlock, o.startBlock, `${network} bootstrap run`);

  if (!run) {
    const head = await o.adapter.finalizedHead();

    if (head.stale || head.refreshFailed) throw new Error('Finalized head observation is stale');

    if (head.block.number < o.startBlock) throw new Error(`START_BLOCK ${o.startBlock} is above the finalized head ${head.block.number}`);
    const [target, anchor] = await Promise.all([o.adapter.headerByNumber(head.block.number), o.adapter.headerByNumber(o.startBlock - 1n)]);

    if (target.hash !== head.block.hash) throw new Error('Finalized header changed while pinning the target');
    run = await store.createRun(network, { startBlock: o.startBlock, target, anchor, segmentBlocks: o.segmentBlocks ?? 1_000n }, now());
    log(`pinned finalized target ${run.targetBlock} (${run.targetBlock - run.startBlock + 1n} blocks from ${run.startBlock})`);
  }

  if (run.status === 'done') throw new Error('Bootstrap run is done but the index is missing');
  const { reset } = await store.resume(network, now());

  if (reset) log('UNLOGGED staging rows were lost (PostgreSQL crash?); reloading every segment');
  const target = run.targetBlock;
  const sources = [o.hypersync, o.rpc].filter((source) => source !== undefined);
  const other = (source: BootstrapSource) => sources.find((candidate) => candidate !== source);

  const timer = setInterval(() => {
    store.status(network, now()).then((status) => { if (status) log(describe(status)); }, () => {});
  }, o.progressMs ?? 10_000);

  try {
    // 1. Header chain S..B in order: each chunk must join the previous one (or the anchor S-1) and end at B.
    await store.setPhase(network, 'headers', now());
    const chunk = o.headerChunkBlocks ?? 5_000n;

    for (let from = run.startBlock; from <= target; from += chunk) {
      const range = { fromBlock: from, toBlock: from + chunk - 1n < target ? from + chunk - 1n : target };

      if (await store.headerCount(network, range) === Number(range.toBlock - range.fromBlock + 1n)) continue;
      const parent = await store.headerAt(network, from - 1n);

      if (!parent) throw new Error(`Missing staged header ${from - 1n}`);
      let saved = false;
      const errors: string[] = [];

      for (const source of sources) {
        try {
          const headers = await source.headers(range);
          verifyHeaders(range, headers, parent.hash, range.toBlock === target ? run.targetHash : undefined);
          await store.saveHeaders(network, range, headers);
          saved = true;
          break;
        } catch (error) {
          errors.push(`${source.name}: ${thrownMessage(error)}`);
          log(`headers ${range.fromBlock}-${range.toBlock} from ${source.name} rejected: ${thrownMessage(error)}`);
        }
      }

      if (!saved) throw new Error(`Headers ${range.fromBlock}-${range.toBlock} failed on every source: ${errors.join('; ')}`);
    }

    // 2. Segments: HyperSync takes the low end, RPC workers the high end; each verified segment commits atomically.
    await store.setPhase(network, 'logs', now());
    const queue: Segment[] = await store.pendingSegments(network);
    let failedAttempts = 0;
    let failedSegments = 0;

    const load = async (segment: Segment, first: BootstrapSource): Promise<void> => {
      const headers = await store.headers(network, segment);

      if (BigInt(headers.length) !== segment.toBlock - segment.fromBlock + 1n) throw new Error(`Missing staged headers for ${segment.fromBlock}-${segment.toBlock}`);
      const byNumber = new Map(headers.map((header) => [header.number, header]));
      const order = [first, ...sources.filter((source) => source !== first)];
      let lastError = '';

      for (const source of order) {
        try {
          await store.commitSegment(network, segment, source.name, async (insert) => {
            const counts = new Map<bigint, number>();
            let previous = { block: -1n, index: -1 };
            let logs = 0;

            const { requests } = await source.transfers({ fromBlock: segment.fromBlock, toBlock: segment.toBlock }, async (events) => {
              for (const event of events) {
                const header = byNumber.get(event.blockNumber);

                if (!header) throw new VerificationError(`Log at block ${event.blockNumber} is outside the segment`);

                if (header.hash !== event.blockHash) throw new VerificationError(`Log block hash does not match header at ${event.blockNumber}`);

                if (event.blockNumber < previous.block || (event.blockNumber === previous.block && event.logIndex <= previous.index)) {
                  throw new VerificationError(`Logs out of order or duplicated at ${event.blockNumber}:${event.logIndex}`);
                }

                previous = { block: event.blockNumber, index: event.logIndex };
                counts.set(event.blockNumber, (counts.get(event.blockNumber) ?? 0) + 1);
              }

              logs += events.length;
              await insert(events);
            });

            // A block without decoded Transfers whose bloom may contain the topic is re-asked (other source first).
            const probe = other(source) ?? source;

            for (const header of headers) {
              if (counts.has(header.number) || !header.transferBloom) continue;
              const again = await probe.blockTransfers(header);

              if (again.some((event) => event.blockHash === header.hash)) {
                throw new VerificationError(`Block ${header.number}: logsBloom has Transfer and ${probe.name} returned ${again.length} logs, ${source.name} none`);
              }
            }

            return { logs, requests };
          }, now());

          return;
        } catch (error) {
          failedAttempts += 1;
          lastError = `${source.name}: ${thrownMessage(error)}`;
          log(`segment ${segment.fromBlock}-${segment.toBlock} from ${source.name} rejected: ${thrownMessage(error)}`);
        }
      }

      failedSegments += 1;
      await store.failSegment(network, segment, lastError, now());
    };

    const worker = async (source: BootstrapSource, fromLowEnd: boolean) => {
      for (let segment = fromLowEnd ? queue.shift() : queue.pop(); segment; segment = fromLowEnd ? queue.shift() : queue.pop()) {
        await load(segment, source);
      }
    };

    await Promise.all([
      ...(o.hypersync ? [worker(o.hypersync, true)] : []),
      ...Array.from({ length: o.rpcWorkers ?? 4 }, () => worker(o.rpc, false)),
    ]);

    if (failedSegments > 0) throw new Error(`${failedSegments} segments failed on every source; rerun to resume the remaining segments`);

    // 3. Publish in one transaction.
    await store.setPhase(network, 'finalizing', now());
    log('all segments loaded and verified; finalizing (insert, derived tables, index builds)');
    const finalized = await store.finalize(network, { at: now(), maintenanceWorkMem: o.maintenanceWorkMem });

    if (o.analyze ?? true) await store.analyze();
    const seconds = Math.round((now() - started) / 1_000);
    log(`done: ${finalized.transfers} transfers, ${finalized.headers} headers, cursor at ${target} in ${seconds}s`);

    return { status: 'done', target, ...finalized, failedAttempts, seconds };
  } catch (error) {
    await store.setPhase(network, (await store.run(network))?.status ?? 'headers', now(), thrownMessage(error).slice(0, 500)).catch(() => {});
    throw error;
  } finally {
    clearInterval(timer);
  }
}

