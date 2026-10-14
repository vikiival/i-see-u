import { expect, test } from 'vitest';

import type {
  Address,
  AccountLogSource,
  BalanceResult,
  BlockRange,
  BlockRef,
  ChainAdapter,
  ChunkScan,
  DiscoveryResult,
  FollowerStatus,
  Hex,
  HeadObservation,
  OwnerResult,
  SyncingResult,
  TransferDirection,
  TransferEvent,
} from '../src/core/types.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { createTestStore } from './store-fixture.ts';

const account = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as Address;
const uppercaseAccount = `0x${account.slice(2).toUpperCase()}` as Address;

class FakeAdapter implements ChainAdapter {
  readonly network = 'ethereum';
  readonly startBlock = 100n;
  readonly logsMaxRange: bigint = 100n;
  readonly logsMinRange: bigint = 1n;
  readonly scanConcurrency: number = 2;
  readonly followerMaxRange: bigint = 500n;
  readonly accountLogs: AccountLogSource;
  head: BlockRef = {
    number: 110n,
    hash: '0x110' as Hex,
    timestamp: 1_000n,
  };
  calls: { account: Address; direction: TransferDirection; range: BlockRange }[] = [];
  gate?: Promise<void>;
  lastProgressAt = Date.now();
  stale = false;
  handler?: (direction: TransferDirection, range: BlockRange) => Promise<{ events: TransferEvent[]; calls: number }>;
  heavy?: (range: BlockRange) => boolean;

  constructor() {
    const adapter = this;
    this.accountLogs = {
      name: 'fake',
      get preferredMaxRange() { return adapter.logsMaxRange; },
      get adaptiveRange() { return { minRange: adapter.logsMinRange }; },
      scanAccount: (requestedAccount, range) => adapter.scanChunk(requestedAccount, range),
      stats: () => ({ fake: { requests: 0, chunks: 0, logs: 0, failures: 0, fallbacks: 0 } }),
    };
  }

  normalizeAddress(input: string): Address | undefined {
    return /^0x[0-9a-f]{40}$/i.test(input) ? input.toLowerCase() as Address : undefined;
  }

  async finalizedHead(): Promise<HeadObservation> {
    return {
      block: this.head,
      lastProgressAt: this.lastProgressAt,
      stale: this.stale,
    };
  }

  async latestHeader() {
    return { ...this.head, parentHash: `0x${this.head.number - 1n}` as Hex };
  }

  async scanChunk(requestedAccount: Address, range: BlockRange): Promise<ChunkScan> {
    const one = async (direction: TransferDirection) => {
      this.calls.push({ account: requestedAccount, direction, range });
      await this.gate;
      return this.handler ? this.handler(direction, range) : { events: [], calls: 1 };
    };
    const [sent, received] = await Promise.allSettled([one('from'), one('to')]);
    if (sent.status === 'rejected') throw sent.reason;
    if (received.status === 'rejected') throw received.reason;
    const aggregate = emptyAggregate();
    for (const event of [...sent.value.events, ...received.value.events]) addTransfer(aggregate, requestedAccount, event);
    return { aggregate, calls: sent.value.calls + received.value.calls, logs: 0, heavy: this.heavy?.(range) ?? false };
  }

  async blockByNumber(number: bigint): Promise<BlockRef> {
    return { number, hash: `0x${number}` as Hex, timestamp: number * 10n };
  }

  async headerByNumber(number: bigint) {
    return { ...(await this.blockByNumber(number)), parentHash: `0x${number - 1n}` as Hex };
  }

  async transfersInBlock(): Promise<TransferEvent[]> { return []; }
  async transfersInRange(): Promise<TransferEvent[]> { return []; }

  async scanAllTransfers(): Promise<{ calls: number }> {
    return { calls: 0 };
  }

  async erc20BalancePairs(): Promise<BalanceResult[]> {
    return [];
  }

  async erc721Owners(): Promise<OwnerResult[]> {
    return [];
  }
}

/** Follower stub: ready and healthy unless overridden; the store's follower cursor is set to adapter.head. */
async function setup(adapter: FakeAdapter, status: Partial<FollowerStatus> = {}) {
  const { store } = await createTestStore();
  await store.initFollowerCursor('ethereum', adapter.head, Date.now());
  const tracked: Address[] = [];
  const follower = {
    status: (): FollowerStatus => ({ ready: true, leader: true, lagBlocks: 0n, lastAdvanceAt: Date.now(), stale: false, ...status }),
    track: (who: Address) => { tracked.push(who); },
  };
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store, followers: { ethereum: follower }, requestDeadlineMs: 60_000 });
  return { store, service, tracked };
}

const ready = (result: DiscoveryResult | SyncingResult): DiscoveryResult => {
  if (result.status !== 'ready') throw new Error('expected a ready result');
  return result;
};

test('first discovery tracks the account and backfills [startBlock, follower cursor]; later ones read only the store', async () => {
  const adapter = new FakeAdapter();
  const { store, service, tracked } = await setup(adapter);
  try {
    const first = ready(await service.discover('ethereum', uppercaseAccount));
    expect(first.account).toBe(account);
    expect(tracked).toEqual([account]);
    expect(await store.getTracking('ethereum', account)).toEqual({ target: 110n, progress: 110n });
    expect(first.blockHeight).toBe(110n);
    expect(first.blockHash).toBe(adapter.head.hash);
    expect(first.finality).toBe('finalized');
    expect(first.scan.newBlocks).toBe(11n);
    expect(first.scan.rpcCalls).toBe(2);
    expect(adapter.calls.map((call) => call.range)).toEqual([
      { fromBlock: 100n, toBlock: 110n },
      { fromBlock: 100n, toBlock: 110n },
    ]);

    // The follower advanced the global cursor: the account is served from the store, without scanning.
    await store.applyFollowerRange('ethereum', { fromBlock: 111n, toBlock: 115n },
      { number: 115n, hash: '0x115' as Hex, timestamp: 1_100n }, Date.now(), new Map(), new Set([account]));
    const second = ready(await service.discover('ethereum', account));
    expect(second.blockHeight).toBe(115n);
    expect(second.blockHash).toBe('0x115');
    expect(second.scan).toEqual({ newBlocks: 0n, rpcCalls: 0, ms: 0 });
    expect(adapter.calls.length).toBe(2);
  } finally {
    await store.close();
  }
});

test('concurrent discoveries share one directional scan', async () => {
  const adapter = new FakeAdapter();
  let release = (): void => {};
  adapter.gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const { store, service } = await setup(adapter);
  try {
    const first = service.discover('ethereum', account);
    const second = service.discover('ethereum', account);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(adapter.calls.length).toBe(2);
    release();
    const [a, b] = await Promise.all([first, second]);
    expect(a).toEqual(b);
    expect(adapter.calls.map((call) => call.direction).sort()).toEqual(['from', 'to']);
  } finally {
    await store.close();
  }
});

test('unknown network rejects with 404, a malformed address with 400', async () => {
  const { store, service } = await setup(new FakeAdapter());
  try {
    await expect(service.discover('missing', account)).rejects.toMatchObject({ statusCode: 404 });
    await expect(service.discover('ethereum', 'not-an-address')).rejects.toMatchObject({ statusCode: 400 });
  } finally {
    await store.close();
  }
});

test('legacy scan progress above the follower cursor is discarded and rebuilt, not double counted', async () => {
  const adapter = new FakeAdapter();
  const { store, service } = await setup(adapter);
  const token = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' as Address;
  const transfer = {
    standard: 'erc20', token, from: account, to: token, value: 1n, blockNumber: 105n,
    blockHash: '0x105', logIndex: 0, transactionHash: '0x01',
  } as const;
  // Pre-follower data scanned through 120, above the follower cursor (110).
  const legacy = emptyAggregate();
  addTransfer(legacy, account, transfer);
  await store.applyScanChunk('ethereum', account, { fromBlock: 100n, toBlock: 120n }, legacy);
  adapter.handler = async (direction) => ({ events: direction === 'from' ? [transfer] : [], calls: 1 });
  try {
    const result = ready(await service.discover('ethereum', account));
    expect(result.blockHeight).toBe(110n);
    expect(await store.getTracking('ethereum', account)).toEqual({ target: 110n, progress: 110n });
    expect(result.holdings).toMatchObject([{ token, sentCount: 1, receivedCount: 0 }]);
  } finally {
    await store.close();
  }
});

test('an unhealthy follower rejects with 503 before tracking or scanning', async () => {
  const adapter = new FakeAdapter();
  const { store, service } = await setup(adapter, { unhealthy: 'Follower has not advanced recently enough' });
  try {
    await expect(service.discover('ethereum', account)).rejects.toMatchObject({ statusCode: 503, retryAfterS: 30 });
    expect(adapter.calls.length).toBe(0);
    expect(await store.getTracking('ethereum', account)).toBeUndefined();
  } finally {
    await store.close();
  }
});

test('a failed third chunk preserves two committed chunks and the next call resumes there', async () => {
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 2n });
  adapter.head = { number: 109n, hash: '0x109' as Hex, timestamp: 1_090n };
  const { store, service } = await setup(adapter);
  let fail = true;
  adapter.handler = async (direction, range) => {
    if (fail && direction === 'to' && range.fromBlock === 104n) throw new Error('chunk 3 failed');
    return { events: [], calls: 1 };
  };
  try {
    await expect(service.discover('ethereum', account)).rejects.toThrow('chunk 3 failed');
    expect(await store.getTracking('ethereum', account)).toEqual({ target: 109n, progress: 103n });

    fail = false;
    adapter.calls.length = 0;
    const result = ready(await service.discover('ethereum', account));
    expect(adapter.calls[0]?.range).toEqual({ fromBlock: 104n, toBlock: 105n });
    expect(result.blockHeight).toBe(109n);
    expect(result.blockHash).toBe('0x109');
    expect(await store.getProgress('ethereum', account)).toBe(109n);
  } finally {
    await store.close();
  }
});

test('a chunk is not committed when either direction fails', async () => {
  const adapter = new FakeAdapter();
  adapter.head = { number: 100n, hash: '0x100' as Hex, timestamp: 1_000n };
  adapter.handler = async (direction) => {
    if (direction === 'to') throw new Error('received direction failed');
    return {
      calls: 1,
      events: [{
        standard: 'erc20',
        token: '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' as Address,
        from: account,
        to: '0xcccccccccccccccccccccccccccccccccccccccc' as Address,
        value: 1n,
        blockNumber: 100n,
        blockHash: '0x100' as Hex,
        logIndex: 0,
        transactionHash: '0x01' as Hex,
      }],
    };
  };
  const { store, service } = await setup(adapter);
  try {
    await expect(service.discover('ethereum', account)).rejects.toThrow('received direction failed');
    expect(await store.getProgress('ethereum', account)).toBeUndefined();
    expect(await store.holdings('ethereum', account)).toEqual([]);
  } finally {
    await store.close();
  }
});

test('a self-transfer returned by both directions is counted once per side', async () => {
  const adapter = new FakeAdapter();
  adapter.head = { number: 100n, hash: '0x100' as Hex, timestamp: 1_000n };
  const transfer: TransferEvent = {
    standard: 'erc20',
    token: '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' as Address,
    from: account,
    to: account,
    value: 1n,
    blockNumber: 100n,
    blockHash: '0x100' as Hex,
    logIndex: 0,
    transactionHash: '0x01' as Hex,
  };
  adapter.handler = async () => ({ events: [transfer], calls: 1 });
  const { store, service } = await setup(adapter);
  try {
    await service.discover('ethereum', account);
    expect((await store.holdings('ethereum', account))[0]).toMatchObject({ sentCount: 1, receivedCount: 1 });
  } finally {
    await store.close();
  }
});

const tick = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

test('a slow first chunk does not idle the other slots; commits stay in order and the buffer is bounded', async () => {
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 2n });
  adapter.head = { number: 113n, hash: '0x113' as Hex, timestamp: 1_130n }; // 7 chunks
  const { store, service } = await setup(adapter);
  let releaseFirst = (): void => {};
  const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
  let active = 0;
  let peakActive = 0;
  adapter.handler = async (direction, range) => {
    if (direction === 'from') active += 1;
    peakActive = Math.max(peakActive, active);
    if (range.fromBlock === 100n) await firstGate;
    else await tick();
    if (direction === 'to') active -= 1;
    return { events: [], calls: 1 };
  };
  const committed: bigint[] = [];
  const apply = store.applyScanChunk.bind(store);
  store.applyScanChunk = async (network, who, range, chunk) => {
    committed.push(range.fromBlock);
    await apply(network, who, range, chunk);
  };
  const started = () => [...new Set(adapter.calls.map((call) => call.range.fromBlock))];
  try {
    const pending = service.discover('ethereum', account);
    for (let i = 0; i < 20; i += 1) await tick();
    // Concurrency 2: chunks 1..3 ran while chunk 0 was stuck; the buffer (2 x 2 = 4 chunks) stops chunk 4.
    expect(started()).toEqual([100n, 102n, 104n, 106n]);
    expect(committed).toEqual([]);
    releaseFirst();
    const result = ready(await pending);
    expect(committed).toEqual([100n, 102n, 104n, 106n, 108n, 110n, 112n]);
    expect(started()).toEqual(committed);
    expect(peakActive).toBeLessThanOrEqual(2);
    expect(result.scan).toMatchObject({ newBlocks: 14n, rpcCalls: 14 });
    expect(await store.getProgress('ethereum', account)).toBe(113n);
  } finally {
    await store.close();
  }
});

test('a failed later chunk still commits the earlier ones, then rejects', async () => {
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 2n });
  adapter.head = { number: 109n, hash: '0x109' as Hex, timestamp: 1_090n };
  let releaseFirst = (): void => {};
  const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
  adapter.handler = async (_direction, range) => {
    if (range.fromBlock === 100n) await firstGate;
    if (range.fromBlock === 102n) throw new Error('chunk 1 failed');
    return { events: [], calls: 1 };
  };
  const { store, service } = await setup(adapter);
  try {
    const pending = service.discover('ethereum', account);
    const assertion = expect(pending).rejects.toThrow('chunk 1 failed');
    for (let i = 0; i < 5; i += 1) await tick();
    const startedBeforeRelease = new Set(adapter.calls.map((call) => call.range.fromBlock)).size;
    releaseFirst();
    await assertion;
    expect(await store.getProgress('ethereum', account)).toBe(101n);
    // Nothing new is started once a chunk has failed.
    expect(new Set(adapter.calls.map((call) => call.range.fromBlock)).size).toBe(startedBeforeRelease);
  } finally {
    await store.close();
  }
});

test('the range halves after a heavy chunk, doubles after three light ones, and is persisted per account', async () => {
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 8n });
  Object.defineProperty(adapter, 'logsMinRange', { value: 2n });
  Object.defineProperty(adapter, 'scanConcurrency', { value: 1 });
  adapter.head = { number: 135n, hash: '0x135' as Hex, timestamp: 1_350n };
  adapter.heavy = (range) => range.fromBlock === 100n;
  const heavyAccount = '0xdddddddddddddddddddddddddddddddddddddddd' as Address;
  const resumedAccount = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as Address;
  const sizes = () => adapter.calls
    .filter((call) => call.direction === 'from')
    .map((call) => call.range.toBlock - call.range.fromBlock + 1n);
  const { store, service } = await setup(adapter);
  try {
    await service.discover('ethereum', account);
    expect(sizes()).toEqual([8n, 4n, 4n, 4n, 8n, 8n]);
    expect(await store.getLogsRange('ethereum', account)).toBe(8n);

    // A heavy account shrinks to the floor, and the range is persisted for its next scan.
    adapter.calls.length = 0;
    adapter.heavy = (range) => range.fromBlock < 116n;
    await expect(service.discover('ethereum', heavyAccount)).resolves.toMatchObject({ status: 'ready' });
    expect(sizes().slice(0, 4)).toEqual([8n, 4n, 2n, 2n]);

    // A resumed scan starts at the persisted range.
    adapter.calls.length = 0;
    adapter.heavy = () => false;
    await store.setLogsRange('ethereum', resumedAccount, 2n);
    await store.applyScanChunk('ethereum', resumedAccount, { fromBlock: 100n, toBlock: 131n }, emptyAggregate());
    await service.discover('ethereum', resumedAccount);
    expect(sizes()).toEqual([2n, 2n]);
  } finally {
    await store.close();
  }
});

test('concurrent heavy results shrink once from their own size, not repeatedly', async () => {
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 8n });
  Object.defineProperty(adapter, 'logsMinRange', { value: 1n });
  Object.defineProperty(adapter, 'scanConcurrency', { value: 4 });
  adapter.head = { number: 131n, hash: '0x131' as Hex, timestamp: 1_310n };
  adapter.heavy = (range) => range.toBlock - range.fromBlock + 1n === 8n;
  const { store, service } = await setup(adapter);
  try {
    await service.discover('ethereum', account);
    const sizes = adapter.calls
      .filter((call) => call.direction === 'from')
      .map((call) => call.range.toBlock - call.range.fromBlock + 1n);
    expect(sizes.slice(0, 4)).toEqual([8n, 8n, 8n, 8n]);
    expect(await store.getProgress('ethereum', account)).toBe(131n);
    expect(await store.getLogsRange('ethereum', account)).toBe(4n);
  } finally {
    await store.close();
  }
});
