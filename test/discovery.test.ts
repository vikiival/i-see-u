import { expect, test } from 'vitest';

import type {
  Address,
  BalanceResult,
  BlockRange,
  BlockRef,
  ChainAdapter,
  ChunkScan,
  Hex,
  HeadObservation,
  OwnerResult,
  TransferDirection,
  TransferEvent,
} from '../src/core/types.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { openSqliteStore } from '../src/store/sqlite.ts';

const account = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as Address;
const uppercaseAccount = `0x${account.slice(2).toUpperCase()}` as Address;

class FakeAdapter implements ChainAdapter {
  readonly network = 'ethereum';
  readonly startBlock = 100n;
  readonly logsMaxRange: bigint = 100n;
  readonly logsMinRange: bigint = 1n;
  readonly scanConcurrency: number = 2;
  head: BlockRef = {
    number: 110n,
    hash: '0x110' as Hex,
    parentHash: '0x109' as Hex,
    timestamp: 1_000n,
  };
  calls: { account: Address; direction: TransferDirection; range: BlockRange }[] = [];
  gate?: Promise<void>;
  lastResponseAt = Date.now();
  lastProgressAt = Date.now();
  stale = false;
  handler?: (direction: TransferDirection, range: BlockRange) => Promise<{ events: TransferEvent[]; calls: number }>;
  heavy?: (range: BlockRange) => boolean;

  normalizeAddress(input: string): Address | undefined {
    return /^0x[0-9a-f]{40}$/i.test(input) ? input.toLowerCase() as Address : undefined;
  }

  async finalizedHead(): Promise<HeadObservation> {
    return {
      block: this.head,
      lastResponseAt: this.lastResponseAt,
      lastProgressAt: this.lastProgressAt,
      stale: this.stale,
    };
  }

  async safeHead(): Promise<HeadObservation> {
    return this.finalizedHead();
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
    return { aggregate, calls: sent.value.calls + received.value.calls, heavy: this.heavy?.(range) ?? false };
  }

  async erc20Balances(): Promise<BalanceResult[]> {
    return [];
  }

  async erc721Owners(): Promise<OwnerResult[]> {
    return [];
  }
}

test('first discovery scans from startBlock and later discovery scans only the gap', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    const first = await service.discover('ethereum', uppercaseAccount);
    expect(first.account).toBe(account);
    expect(first.blockHeight).toBe(110n);
    expect(first.blockHash).toBe(adapter.head.hash);
    expect(store.getCursor('ethereum', account)?.scannedToHash).toBe(adapter.head.hash);
    expect(first.finality).toBe('finalized');
    expect(first.scan.newBlocks).toBe(11n);
    expect(first.scan.rpcCalls).toBe(2);
    expect(adapter.calls.map((call) => call.range)).toEqual([
      { fromBlock: 100n, toBlock: 110n },
      { fromBlock: 100n, toBlock: 110n },
    ]);

    adapter.head = { number: 115n, hash: '0x115' as Hex, parentHash: '0x114' as Hex, timestamp: 1_100n };
    const second = await service.discover('ethereum', account);
    expect(second.scan.newBlocks).toBe(5n);
    expect(adapter.calls.slice(2).map((call) => call.range)).toEqual([
      { fromBlock: 111n, toBlock: 115n },
      { fromBlock: 111n, toBlock: 115n },
    ]);
  } finally {
    store.close();
  }
});

test('concurrent discoveries share one directional scan', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  let release = (): void => {};
  adapter.gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
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
    store.close();
  }
});

test('unknown network rejects with statusCode 404', async () => {
  const store = openSqliteStore(':memory:');
  const service = createDiscoveryService({ adapters: {}, store });
  try {
    await expect(service.discover('missing', account)).rejects.toMatchObject({ statusCode: 404 });
  } finally {
    store.close();
  }
});

test('a cursor ahead of a lagging head is retained without a scan', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  const stored = { number: 120n, hash: '0x120' as Hex, parentHash: '0x119' as Hex, timestamp: 1_200n };
  store.finalizeScan('ethereum', account, stored);
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    const result = await service.discover('ethereum', account);
    expect(result.blockHeight).toBe(120n);
    expect(result.blockHash).toBe(stored.hash);
    expect(result.scan).toEqual({ newBlocks: 0n, rpcCalls: 0, ms: result.scan.ms });
    expect(adapter.calls.length).toBe(0);
  } finally {
    store.close();
  }
});

test('an excessively old stale finalized head rejects with statusCode 503', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  adapter.stale = true;
  adapter.lastProgressAt = Date.now() - 121_000;
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store, maxHeadProgressAgeMs: 120_000 });
  try {
    await expect(service.discover('ethereum', account)).rejects.toMatchObject({ statusCode: 503 });
    expect(adapter.calls.length).toBe(0);
  } finally {
    store.close();
  }
});

test('a failed third chunk preserves two committed chunks and the next call resumes there', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 2n });
  adapter.head = { number: 109n, hash: '0x109' as Hex, parentHash: '0x108' as Hex, timestamp: 1_090n };
  store.finalizeScan('ethereum', account, {
    number: 99n,
    hash: '0x99' as Hex,
    parentHash: '0x98' as Hex,
    timestamp: 990n,
  });
  let fail = true;
  adapter.handler = async (direction, range) => {
    if (fail && direction === 'to' && range.fromBlock === 104n) throw new Error('chunk 3 failed');
    return { events: [], calls: 1 };
  };
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    await expect(service.discover('ethereum', account)).rejects.toThrow('chunk 3 failed');
    expect(store.getProgress('ethereum', account)).toBe(103n);
    expect(store.getCursor('ethereum', account)?.scannedTo).toBe(99n);

    fail = false;
    adapter.calls.length = 0;
    const result = await service.discover('ethereum', account);
    expect(adapter.calls[0]?.range).toEqual({ fromBlock: 104n, toBlock: 105n });
    expect(result.blockHeight).toBe(109n);
    expect(store.getProgress('ethereum', account)).toBe(109n);
    expect(store.getCursor('ethereum', account)?.scannedToHash).toBe('0x109');
  } finally {
    store.close();
  }
});

test('a chunk is not committed when either direction fails', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  adapter.head = { number: 100n, hash: '0x100' as Hex, parentHash: '0x99' as Hex, timestamp: 1_000n };
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
        logIndex: 0,
        transactionHash: '0x01' as Hex,
      }],
    };
  };
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    await expect(service.discover('ethereum', account)).rejects.toThrow('received direction failed');
    expect(store.getProgress('ethereum', account)).toBeUndefined();
    expect(store.holdings('ethereum', account, 'erc20')).toEqual([]);
  } finally {
    store.close();
  }
});

test('a self-transfer returned by both directions is counted once per side', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  adapter.head = { number: 100n, hash: '0x100' as Hex, parentHash: '0x99' as Hex, timestamp: 1_000n };
  const transfer: TransferEvent = {
    standard: 'erc20',
    token: '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' as Address,
    from: account,
    to: account,
    value: 1n,
    blockNumber: 100n,
    logIndex: 0,
    transactionHash: '0x01' as Hex,
  };
  adapter.handler = async () => ({ events: [transfer], calls: 1 });
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    await service.discover('ethereum', account);
    expect(store.holdings('ethereum', account, 'erc20')[0]).toMatchObject({ sentCount: 1, receivedCount: 1 });
  } finally {
    store.close();
  }
});

const tick = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

test('a slow first chunk does not idle the other slots; commits stay in order and the buffer is bounded', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 2n });
  adapter.head = { number: 113n, hash: '0x113' as Hex, parentHash: '0x112' as Hex, timestamp: 1_130n }; // 7 chunks
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
  store.applyScanChunk = (network, who, range, chunk) => {
    committed.push(range.fromBlock);
    apply(network, who, range, chunk);
  };
  const started = () => [...new Set(adapter.calls.map((call) => call.range.fromBlock))];
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    const pending = service.discover('ethereum', account);
    for (let i = 0; i < 20; i += 1) await tick();
    // Concurrency 2: chunks 1..3 ran while chunk 0 was stuck; the buffer (2 x 2 = 4 chunks) stops chunk 4.
    expect(started()).toEqual([100n, 102n, 104n, 106n]);
    expect(committed).toEqual([]);
    releaseFirst();
    const result = await pending;
    expect(committed).toEqual([100n, 102n, 104n, 106n, 108n, 110n, 112n]);
    expect(started()).toEqual(committed);
    expect(peakActive).toBeLessThanOrEqual(2);
    expect(result.scan).toMatchObject({ newBlocks: 14n, rpcCalls: 14 });
    expect(store.getProgress('ethereum', account)).toBe(113n);
  } finally {
    store.close();
  }
});

test('a failed later chunk still commits the earlier ones, then rejects', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 2n });
  adapter.head = { number: 109n, hash: '0x109' as Hex, parentHash: '0x108' as Hex, timestamp: 1_090n };
  let releaseFirst = (): void => {};
  const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
  adapter.handler = async (_direction, range) => {
    if (range.fromBlock === 100n) await firstGate;
    if (range.fromBlock === 102n) throw new Error('chunk 1 failed');
    return { events: [], calls: 1 };
  };
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    const pending = service.discover('ethereum', account);
    const assertion = expect(pending).rejects.toThrow('chunk 1 failed');
    for (let i = 0; i < 5; i += 1) await tick();
    const startedBeforeRelease = new Set(adapter.calls.map((call) => call.range.fromBlock)).size;
    releaseFirst();
    await assertion;
    expect(store.getProgress('ethereum', account)).toBe(101n);
    // Nothing new is started once a chunk has failed.
    expect(new Set(adapter.calls.map((call) => call.range.fromBlock)).size).toBe(startedBeforeRelease);
  } finally {
    store.close();
  }
});

test('the range halves after a heavy chunk, doubles after three light ones, and is persisted per account', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 8n });
  Object.defineProperty(adapter, 'logsMinRange', { value: 2n });
  Object.defineProperty(adapter, 'scanConcurrency', { value: 1 });
  adapter.head = { number: 135n, hash: '0x135' as Hex, parentHash: '0x134' as Hex, timestamp: 1_350n };
  adapter.heavy = (range) => range.fromBlock === 100n;
  const sizes = () => adapter.calls
    .filter((call) => call.direction === 'from')
    .map((call) => call.range.toBlock - call.range.fromBlock + 1n);
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    await service.discover('ethereum', account);
    expect(sizes()).toEqual([8n, 4n, 4n, 4n, 8n, 8n]);
    expect(store.getLogsRange('ethereum', account)).toBe(8n);

    // A heavy account shrinks to the floor, and its next scan starts there.
    adapter.calls.length = 0;
    adapter.heavy = () => true;
    adapter.head = { number: 151n, hash: '0x151' as Hex, parentHash: '0x150' as Hex, timestamp: 1_510n };
    await service.discover('ethereum', account);
    expect(sizes()).toEqual([8n, 4n, 2n, 2n]);
    expect(store.getLogsRange('ethereum', account)).toBe(2n);

    adapter.calls.length = 0;
    adapter.heavy = () => false;
    adapter.head = { number: 155n, hash: '0x155' as Hex, parentHash: '0x154' as Hex, timestamp: 1_550n };
    await service.discover('ethereum', account);
    expect(sizes()).toEqual([2n, 2n]);
  } finally {
    store.close();
  }
});

test('concurrent heavy results shrink once from their own size, not repeatedly', async () => {
  const store = openSqliteStore(':memory:');
  const adapter = new FakeAdapter();
  Object.defineProperty(adapter, 'logsMaxRange', { value: 8n });
  Object.defineProperty(adapter, 'logsMinRange', { value: 1n });
  Object.defineProperty(adapter, 'scanConcurrency', { value: 4 });
  adapter.head = { number: 131n, hash: '0x131' as Hex, parentHash: '0x130' as Hex, timestamp: 1_310n };
  adapter.heavy = (range) => range.toBlock - range.fromBlock + 1n === 8n;
  const service = createDiscoveryService({ adapters: { ethereum: adapter }, store });
  try {
    await service.discover('ethereum', account);
    const sizes = adapter.calls
      .filter((call) => call.direction === 'from')
      .map((call) => call.range.toBlock - call.range.fromBlock + 1n);
    expect(sizes.slice(0, 4)).toEqual([8n, 8n, 8n, 8n]);
    expect(store.getProgress('ethereum', account)).toBe(131n);
    expect(store.getLogsRange('ethereum', account)).toBe(4n);
  } finally {
    store.close();
  }
});
