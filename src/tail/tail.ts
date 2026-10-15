import type {
  BlockHeader,
  ChainAdapter,
  Follower,
  Store,
  Tail,
  TailBlock,
  TailStatus,
} from '../core/types.ts';

export interface TailOptions {
  adapter: ChainAdapter;
  store: Store;
  follower: Pick<Follower, 'status'>;
  maxDepth?: bigint;
  /** Stop appending this many blocks above the finalized cursor (finality stalled): bounds rows and overlay cost. */
  maxAheadBlocks?: bigint;
  pollOffsetsMs?: number[];
  staleAfterMs?: number;
  errorThreshold?: number;
  now?: () => number;
  onAdvance?: (tip: BlockHeader, reorg: boolean) => void;
  /**
   * The finalized checkpoint may have moved (wake the follower): when the tip enters a new beacon epoch, then every
   * `finalityRetrySlots` slots of that epoch until the follower's finalized head advances.
   */
  onEpoch?: () => void;
  finalityRetrySlots?: number;
  onError?: (error: unknown) => void;
}

const SLOT_MS = 12_000;
const EPOCH_BLOCKS = 32n;
const GENESIS_MS = 1_606_824_023_000;
const slotOf = (header: BlockHeader): number => Math.floor((Number(header.timestamp) * 1_000 - GENESIS_MS) / SLOT_MS);
const epochOf = (header: BlockHeader): number => Math.floor(slotOf(header) / 32);

/**
 * Delay until the next poll: the next offset of the current slot, unless the tip already is this slot's block
 * (then the first offset of the next slot). A missed or late block is retried at the later offsets.
 */
export function nextPollDelay(at: number, offsets: number[], tip?: BlockHeader): number {
  const slot = Math.floor((at - GENESIS_MS) / SLOT_MS);
  const start = GENESIS_MS + slot * SLOT_MS;

  if (!tip || slotOf(tip) < slot) for (const offset of offsets) if (start + offset > at) return start + offset - at;

  return start + SLOT_MS + offsets[0] - at;
}

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error)).slice(0, 200);

/** Persistent unfinalized journal. Reorgs only replace these rows; finalized aggregates are never rolled back. */
export function createTail(o: TailOptions): Tail {
  const now = o.now ?? Date.now;
  const maxDepth = o.maxDepth ?? 64n;
  const maxAheadBlocks = o.maxAheadBlocks ?? 1024n;
  const offsets = [...(o.pollOffsetsMs ?? [2_500, 5_000, 9_000])].sort((a, b) => a - b);
  const staleAfterMs = o.staleAfterMs ?? 3 * SLOT_MS;
  const errorThreshold = o.errorThreshold ?? 3;
  const { adapter, store } = o;
  const network = adapter.network;
  let tip: BlockHeader | undefined;
  let latestNumber: bigint | undefined;
  let reorgs = 0;
  let lastReorgDepth: bigint | undefined;
  let lastAdvanceAt = 0;
  let lastError: string | undefined;
  let tailError: string | undefined;
  let consecutiveErrors = 0;
  let stopped = true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let ticking: Promise<void> | undefined;
  let epoch: number | undefined;
  /** Waiting for finalized to move this epoch: the follower head seen at the last wake, and that slot. */
  let finality: { head?: bigint; slot: number } | undefined;
  const retrySlots = o.finalityRetrySlots ?? 4;

  const wakeFollower = (slot: number): void => {
    finality = { head: o.follower.status().headNumber, slot };
    o.onEpoch?.();
  };

  const append = async (anchor: BlockHeader, head: BlockHeader, cursorNumber: bigint, reorg = false): Promise<void> => {
    let end = anchor.number + adapter.followerMaxRange < head.number
      ? anchor.number + adapter.followerMaxRange
      : head.number;

    if (end > cursorNumber + maxAheadBlocks) end = cursorNumber + maxAheadBlocks; // never past the finality-stall bound
    const headers: BlockHeader[] = [];
    let parent = anchor;

    for (let number = anchor.number + 1n; number <= end; number += 1n) {
      const header = number === head.number ? head : await adapter.headerByNumber(number);

      if (header.number !== number || header.parentHash !== parent.hash) {
        throw Object.assign(new Error(`Header chain does not link at ${number}`), { chainMismatch: true });
      }

      headers.push(header);
      parent = header;
    }

    if (headers.length === 0) return;
    let blocks: TailBlock[];

    if (headers.length === 1) {
      blocks = [{ header: headers[0], transfers: await adapter.transfersInBlock(headers[0].hash) }];
    } else {
      const range = { fromBlock: headers[0].number, toBlock: headers.at(-1)!.number };
      const transfers = await adapter.transfersInRange(range, headers);
      const byBlock = new Map<bigint, typeof transfers>();

      for (const transfer of transfers) {
        let rows = byBlock.get(transfer.blockNumber);

        if (!rows) byBlock.set(transfer.blockNumber, rows = []);
        rows.push(transfer);
      }

      blocks = headers.map((header) => ({ header, transfers: byBlock.get(header.number) ?? [] }));
    }

    await store.appendTailBlocks(network, anchor.hash, blocks);
    tip = headers.at(-1)!;
    lastAdvanceAt = now();
    tailError = undefined;
    o.onAdvance?.(tip, reorg);
    const slot = slotOf(tip);

    if (epoch !== undefined && epochOf(tip) > epoch) wakeFollower(slot);
    else if (finality) {
      const head = o.follower.status().headNumber;

      if (head !== undefined && (finality.head === undefined || head > finality.head)) finality = undefined;
      else if (slot - finality.slot >= retrySlots) wakeFollower(slot);
    }

    epoch = epochOf(tip);
  };

  const handleReorg = async (head: BlockHeader, cursor: BlockHeader): Promise<void> => {
    const oldTip = tip!;
    const stored = new Map((await store.getTailBlocks(network)).map((block) => [block.number, block]));
    let number = head.number < oldTip.number ? head.number : oldTip.number;
    let ancestor: BlockHeader | undefined;

    while (number > cursor.number && oldTip.number - number <= maxDepth) {
      const canonical = number === head.number ? head : await adapter.headerByNumber(number);
      const local = stored.get(number);

      if (local?.hash === canonical.hash) {
        ancestor = local;
        break;
      }

      number -= 1n;
    }

    // The old tip is still canonical: a header changed mid catch-up (load-balanced nodes), not a reorg of ours.
    if (ancestor?.number === oldTip.number) throw new Error(`Header chain changed above ${oldTip.number}; retrying`);
    reorgs += 1;
    lastReorgDepth = oldTip.number - (ancestor?.number ?? cursor.number);

    if (!ancestor) {
      await store.wipeTail(network, oldTip.hash);
      tip = undefined;
      tailError = `Tail reorg has no common ancestor within ${maxDepth} blocks above the finalized cursor`;
      lastError = tailError;
      o.onAdvance?.(cursor, true);

      return;
    }

    await store.rewindTail(network, ancestor.number, oldTip.hash);
    tip = ancestor;
    lastAdvanceAt = now();

    if (head.number > ancestor.number) await append(ancestor, head, cursor.number, true);
    else if (tip) o.onAdvance?.(tip, true);
  };

  const run = async (): Promise<void> => {
    const follower = o.follower.status();

    if (!follower.leader) return;

    try {
      const cursor = await store.getFollowerCursor(network);

      if (!cursor) throw new Error('Follower has no cursor for tail anchor');

      if (follower.headNumber !== undefined && follower.headNumber > cursor.number + EPOCH_BLOCKS) {
        // The follower is catching up (startup, outage): its commits prune the tail, so wait instead of fetching
        // hundreds of headers it would make redundant.
        o.onEpoch?.();

        return;
      }

      const anchor: BlockHeader = { ...cursor, parentHash: cursor.hash };
      tip = await store.getTailTip(network);
      const ahead = (tip?.number ?? cursor.number) - cursor.number;

      if (ahead >= maxAheadBlocks) {
        // Finality is not moving while latest is: degrade to finalized instead of growing the tail without bound.
        tailError = `Tail is ${ahead} blocks above the finalized cursor; finality may be stalled`;
        o.onEpoch?.();

        return;
      }

      const head = await adapter.latestHeader();

      if (latestNumber === undefined || head.number > latestNumber) latestNumber = head.number;

      if (head.number <= cursor.number) return; // lagging fallback endpoint

      if (!tip) {
        if (head.number > cursor.number) await append(anchor, head, cursor.number);
      } else if (head.number < tip.number) {
        const stored = (await store.getTailBlocks(network)).find((block) => block.number === head.number);

        if (stored?.hash !== head.hash) await handleReorg(head, anchor);
      } else if (head.number === tip.number) {
        if (head.hash !== tip.hash) await handleReorg(head, anchor);
      } else {
        try {
          await append(tip, head, cursor.number);
        } catch (error) {
          if ((error as { chainMismatch?: unknown }).chainMismatch === true) await handleReorg(head, anchor);
          else throw error;
        }
      }

      consecutiveErrors = 0;
      lastError = undefined;
    } catch (error) {
      consecutiveErrors += 1;
      lastError = messageOf(error);
      o.onError?.(error);
    }
  };

  const tick = (): Promise<void> => {
    ticking ??= run().finally(() => { ticking = undefined; });

    return ticking;
  };

  const schedule = (): void => {
    clearTimeout(timer);

    if (!stopped) timer = setTimeout(() => void cycle(), nextPollDelay(now(), offsets, tip));
  };

  const cycle = async (): Promise<void> => {
    await tick();
    schedule();
  };

  return {
    async load() {
      tip = await store.getTailTip(network);

      if (tip) {
        lastAdvanceAt = now();
        epoch = epochOf(tip);
      }
      // No re-validation here: leadership is not known yet at startup; the first leader tick checks the stored tip.
    },
    start() {
      if (!stopped) return;
      stopped = false;
      schedule();
    },
    async stop() {
      stopped = true;
      clearTimeout(timer);
      await ticking;
    },
    tick,
    status(): TailStatus {
      const lagBlocks = tip && latestNumber && latestNumber > tip.number ? latestNumber - tip.number : 0n;
      const stale = lagBlocks > 0n && lastAdvanceAt > 0 && now() - lastAdvanceAt > staleAfterMs;
      const healthy = tailError === undefined && consecutiveErrors < errorThreshold && !stale;

      return {
        enabled: true,
        tipNumber: tip?.number,
        tipHash: tip?.hash,
        latestNumber,
        lagBlocks,
        reorgs,
        lastReorgDepth,
        lastAdvanceAt,
        lastError,
        healthy,
        tailError: healthy ? undefined : tailError ?? lastError ?? 'Tail has not advanced recently enough',
      };
    },
  };
}
