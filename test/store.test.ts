import { migrate } from 'drizzle-orm/pglite/migrator';
import { expect, test } from 'vitest';

import type { Address, BlockRef, Hex, Store, TransferEvent } from '../src/core/types.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import { advisoryKey, singleConnectionLeaderLock, withAdvisoryLock } from '../src/store/advisory-lock.ts';
import { migrationsFolder } from '../src/store/postgres.ts';
import { TailConflictError } from '../src/store/store.ts';
import { createTestStore } from './store-fixture.ts';

const account = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as Address;
const other = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' as Address;
const token = '0xcccccccccccccccccccccccccccccccccccccccc' as Address;

const block = (number: bigint): BlockRef => ({
  number,
  hash: `0x${number.toString(16)}` as Hex,
  timestamp: number * 10n,
});

const aggregateOf = (events: TransferEvent[]) => {
  const aggregate = emptyAggregate();

  for (const e of events) addTransfer(aggregate, account, e);

  return aggregate;
};

const applyScan = async (store: Store, events: TransferEvent[], at: BlockRef): Promise<void> => {
  const fromBlock = (await store.getProgress('ethereum', account) ?? -1n) + 1n;
  await store.applyScanChunk('ethereum', account, { fromBlock: fromBlock < at.number ? fromBlock : at.number, toBlock: at.number }, aggregateOf(events));
};

function event(overrides: Partial<TransferEvent> = {}): TransferEvent {
  return {
    standard: 'erc20',
    token,
    from: account,
    to: other,
    value: 1n,
    blockNumber: 10n,
    blockHash: block(10n).hash,
    logIndex: 0,
    transactionHash: '0x01' as Hex,
    ...overrides,
  };
}

test('applyScan is idempotent and progress is monotonic', async () => {
  const { store } = await createTestStore();

  try {
    const events = [event()];
    await applyScan(store, events, block(12n));
    await applyScan(store, events, block(11n));

    const [holding] = await store.holdings('ethereum', account);
    expect(holding.sentCount).toBe(1);
    expect(holding.receivedCount).toBe(0);
    expect(holding.firstBlock).toBe(10n);
    expect(holding.lastBlock).toBe(10n);
    expect(await store.getProgress('ethereum', account)).toBe(12n);
  } finally {
    await store.close();
  }
});

test('NFT held flag only follows the newest block and log index', async () => {
  const { store } = await createTestStore();

  try {
    const incoming = event({
      standard: 'erc721',
      from: other,
      to: account,
      value: 42n,
      blockNumber: 20n,
      logIndex: 3,
      transactionHash: '0x02' as Hex,
    });

    const olderOutgoing = event({
      standard: 'erc721',
      from: account,
      to: other,
      value: 42n,
      blockNumber: 20n,
      logIndex: 2,
      transactionHash: '0x03' as Hex,
    });

    const newerOutgoing = event({
      standard: 'erc721',
      from: account,
      to: other,
      value: 42n,
      blockNumber: 21n,
      logIndex: 0,
      transactionHash: '0x04' as Hex,
    });

    await applyScan(store, [incoming, olderOutgoing], block(20n));
    expect((await store.nftItems('ethereum', account))[0]?.heldBySeenLogs).toBe(true);

    await applyScan(store, [newerOutgoing], block(21n));
    const [item] = await store.nftItems('ethereum', account);
    expect(item.heldBySeenLogs).toBe(false);
    expect(item.tokenId).toBe(42n);
    expect(item.lastBlock).toBe(21n);
  } finally {
    await store.close();
  }
});

test('running the Drizzle migrator twice is a no-op', async () => {
  const { db, store } = await createTestStore();

  try {
    await migrate(db, { migrationsFolder });
    await applyScan(store, [], block(15n));
    expect(await store.getProgress('ethereum', account)).toBe(15n);
    expect((await store.initFollowerCursor('ethereum', block(15n), 1))?.hash).toBe(block(15n).hash);
  } finally {
    await store.close();
  }
});

test('a chunk straddling applied progress is rejected and the logs range round-trips', async () => {
  const { store } = await createTestStore();

  try {
    await store.applyScanChunk('ethereum', account, { fromBlock: 0n, toBlock: 10n }, aggregateOf([event()]));
    await store.applyScanChunk('ethereum', account, { fromBlock: 0n, toBlock: 10n }, aggregateOf([event()]));
    expect((await store.holdings('ethereum', account))[0]?.sentCount).toBe(1);
    await expect(store.applyScanChunk('ethereum', account, { fromBlock: 5n, toBlock: 20n }, aggregateOf([])))
      .rejects.toThrow('overlaps applied progress');
    expect(await store.getProgress('ethereum', account)).toBe(10n);

    expect(await store.getLogsRange('ethereum', account)).toBeUndefined();
    await store.setLogsRange('ethereum', account, 750n);
    await store.setLogsRange('ethereum', account, 1500n);
    expect(await store.getLogsRange('ethereum', account)).toBe(1500n);
  } finally {
    await store.close();
  }
});

test('the advisory-lock helper runs its callback while holding the transaction lock', async () => {
  const { db, store } = await createTestStore();

  try {
    let called = 0;
    expect(await withAdvisoryLock(db, 42n, async () => ++called)).toBe(1);
    expect(called).toBe(1);
  } finally {
    await store.close();
  }
});

test('the chunk aggregate dedupes only self-transfers and keeps the latest ERC-721 transfer', () => {
  const self = event({ to: account, blockNumber: 12n, logIndex: 1 });

  const nft = (blockNumber: bigint, logIndex: number, incoming: boolean) => event({
    standard: 'erc721',
    token: other,
    from: incoming ? token : account,
    to: incoming ? account : token,
    value: 7n,
    blockNumber,
    logIndex,
    transactionHash: `0x${blockNumber}${logIndex}` as Hex,
  });

  // Both directions return the self-transfer; insertion order of the NFT logs is arbitrary.
  const aggregate = aggregateOf([
    self, event({ blockNumber: 15n, logIndex: 2 }), self, event({ to: account, blockNumber: 12n, logIndex: 3 }),
    nft(30n, 5, false), nft(30n, 4, true), nft(29n, 9, true),
  ]);

  expect(aggregate.tokens.get(token)).toEqual({ standard: 'erc20', firstBlock: 12n, lastBlock: 15n, sentCount: 3, receivedCount: 2 });
  expect(aggregate.tokens.get(other)).toMatchObject({ standard: 'erc721', firstBlock: 29n, lastBlock: 30n, sentCount: 1, receivedCount: 2 });
  expect([...aggregate.nfts.values()]).toEqual([{ token: other, tokenId: 7n, held: false, block: 30n, logIndex: 5 }]);
  expect(aggregate.selfSeen.size).toBe(2);
});

test('concurrent first trackAccount calls for one account both resolve to the same target', async () => {
  const { store } = await createTestStore();

  try {
    await store.initFollowerCursor('ethereum', block(20n), 1);
    const results = await Promise.all([store.trackAccount('ethereum', account, 1), store.trackAccount('ethereum', account, 2)]);
    expect(results).toEqual([{ target: 20n, progress: undefined }, { target: 20n, progress: undefined }]);
    expect(await store.trackedAccounts('ethereum')).toEqual([account]);
  } finally {
    await store.close();
  }
});

test('trackAccount pins the backfill target to the follower cursor; the backfill cannot write above it', async () => {
  const { store } = await createTestStore();

  try {
    await expect(store.trackAccount('ethereum', account, 1)).rejects.toMatchObject({ statusCode: 503 });
    await store.initFollowerCursor('ethereum', block(20n), 1);
    expect(await store.initFollowerCursor('ethereum', block(30n), 2)).toEqual({ ...block(20n), updatedAt: 1 });
    expect(await store.trackAccount('ethereum', account, 1)).toEqual({ target: 20n, progress: undefined });
    await store.applyFollowerRange('ethereum', { fromBlock: 21n, toBlock: 25n }, block(25n), 2, new Map(), new Set([account]));
    expect(await store.trackAccount('ethereum', account, 3)).toEqual({ target: 20n, progress: undefined }); // idempotent
    expect(await store.trackAccount('ethereum', other, 3)).toEqual({ target: 25n, progress: undefined });
    expect(await store.trackedAccounts('ethereum')).toEqual([account, other]);

    await store.applyScanChunk('ethereum', account, { fromBlock: 0n, toBlock: 20n }, aggregateOf([event()]));
    await expect(store.applyScanChunk('ethereum', account, { fromBlock: 21n, toBlock: 22n }, aggregateOf([])))
      .rejects.toThrow('beyond backfill target 20');
    expect(await store.getTracking('ethereum', account)).toEqual({ target: 20n, progress: 20n });
  } finally {
    await store.close();
  }
});

test('applyFollowerRange continues the cursor only, and applies nothing when a tracked account was not decoded', async () => {
  const { store } = await createTestStore();

  try {
    await store.initFollowerCursor('ethereum', block(20n), 1);
    await store.trackAccount('ethereum', account, 1);
    const aggregates = new Map([[account, aggregateOf([event({ blockNumber: 22n })])]]);
    await expect(store.applyFollowerRange('ethereum', { fromBlock: 22n, toBlock: 25n }, block(25n), 2, aggregates, new Set([account])))
      .rejects.toThrow('does not continue cursor 20');
    expect(await store.applyFollowerRange('ethereum', { fromBlock: 21n, toBlock: 25n }, block(25n), 2, aggregates, new Set()))
      .toEqual({ applied: false, missing: [account] });
    expect(await store.holdings('ethereum', account)).toEqual([]);
    expect((await store.getFollowerCursor('ethereum'))?.number).toBe(20n);

    expect(await store.applyFollowerRange('ethereum', { fromBlock: 21n, toBlock: 25n }, block(25n), 2, aggregates, new Set([account])))
      .toEqual({ applied: true });
    expect(await store.getFollowerCursor('ethereum')).toEqual({ ...block(25n), updatedAt: 2 });
    expect((await store.readAccount('ethereum', account))?.holdings).toMatchObject([{ token, sentCount: 1, firstBlock: 22n }]);
  } finally {
    await store.close();
  }
});

test('rewindTail/wipeTail with a stale expected tip throw TailConflictError and delete nothing', async () => {
  const { store } = await createTestStore();

  try {
    await store.initFollowerCursor('ethereum', block(20n), 1);
    const header = (number: bigint) => ({ ...block(number), parentHash: block(number - 1n).hash });
    await store.appendTailBlocks('ethereum', block(20n).hash, [21n, 22n, 23n].map((n) => ({ header: header(n), transfers: [] })));
    const stale = block(22n).hash; // another writer already moved the tip past what this one observed
    await expect(store.rewindTail('ethereum', 21n, stale)).rejects.toBeInstanceOf(TailConflictError);
    await expect(store.wipeTail('ethereum', stale)).rejects.toBeInstanceOf(TailConflictError);
    expect((await store.getTailBlocks('ethereum')).map((b) => b.number)).toEqual([21n, 22n, 23n]);

    await store.rewindTail('ethereum', 21n, block(23n).hash);
    expect((await store.getTailBlocks('ethereum')).map((b) => b.number)).toEqual([21n]);
    await store.wipeTail('ethereum', block(21n).hash);
    expect(await store.getTailBlocks('ethereum')).toEqual([]);
    await expect(store.rewindTail('ethereum', 20n, block(21n).hash)).rejects.toBeInstanceOf(TailConflictError);
  } finally {
    await store.close();
  }
});

test('balance snapshots keep the block they were read at and never move to an older block', async () => {
  const { store } = await createTestStore();

  try {
    const row = (balance: bigint) => ({ account, token, result: { status: 'ok' as const, balance } });
    await store.saveBalances('ethereum', block(30n), [row(5n), { account, token: other, result: { status: 'error', error: 'reverted' } }]);
    await store.saveBalances('ethereum', block(29n), [row(4n)]);
    expect(await store.balances('ethereum', account)).toEqual(expect.arrayContaining([
      { token, result: { status: 'ok', balance: 5n }, blockNumber: 30n, blockHash: block(30n).hash },
      { token: other, result: { status: 'error', error: 'reverted' }, blockNumber: 30n, blockHash: block(30n).hash },
    ]));
    await store.saveBalances('ethereum', block(31n), [row(6n)]);
    expect((await store.balances('ethereum', account)).find((b) => b.token === token)).toMatchObject({ blockNumber: 31n });
  } finally {
    await store.close();
  }
});

test('erc20RefreshState: latest Transfer block takes the tail max over holdings; snapshot zero/ok are reported', async () => {
  const { store } = await createTestStore();

  try {
    const tailOnly = '0xdddddddddddddddddddddddddddddddddddddddd' as Address;
    const failing = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as Address;
    await applyScan(store, [event(), event({ token: failing, blockNumber: 12n, logIndex: 1 })], block(15n));
    await store.initFollowerCursor('ethereum', block(20n), 1);
    const header = (number: bigint) => ({ ...block(number), parentHash: block(number - 1n).hash });

    const inTail = (n: bigint, overrides: Partial<TransferEvent>) =>
      event({ from: other, to: account, blockNumber: n, blockHash: block(n).hash, ...overrides });

    await store.appendTailBlocks('ethereum', block(20n).hash, [
      { header: header(21n), transfers: [inTail(21n, { token: tailOnly }), inTail(21n, { logIndex: 1 })] },
      { header: header(22n), transfers: [inTail(22n, {})] },
    ]);
    await store.saveBalances('ethereum', block(21n), [
      { account, token, result: { status: 'ok', balance: 0n } },
      { account, token: tailOnly, result: { status: 'ok', balance: 5n } },
      { account, token: failing, result: { status: 'error', error: 'reverted' } },
    ]);
    const orphaned = '0x9999999999999999999999999999999999999999' as Address;
    const finalized = '0x8888888888888888888888888888888888888888' as Address;
    const later = (overrides: Partial<TransferEvent>) => event({ blockNumber: 16n, blockHash: block(16n).hash, ...overrides });
    await applyScan(store, [later({ token: orphaned, logIndex: 2 }), later({ token: finalized, logIndex: 3 })], block(18n));
    // A read at another hash of tail height 22 (orphaned), and one at or below the cursor (finalized since).
    await store.saveBalances('ethereum', { ...block(22n), hash: '0xdead' as Hex }, [
      { account, token: orphaned, result: { status: 'ok', balance: 1n } },
    ]);
    await store.saveBalances('ethereum', block(20n), [{ account, token: finalized, result: { status: 'ok', balance: 1n } }]);

    const states = await store.erc20RefreshState('ethereum', [account]);
    expect(states).toHaveLength(5);
    expect(states).toEqual(expect.arrayContaining([
      { account, token, lastTransferBlock: 22n, snapshot: { blockNumber: 21n, ok: true, zero: true, canonical: true } },
      { account, token: tailOnly, lastTransferBlock: 21n, snapshot: { blockNumber: 21n, ok: true, zero: false, canonical: true } },
      { account, token: failing, lastTransferBlock: 12n, snapshot: { blockNumber: 21n, ok: false, zero: false, canonical: true } },
      { account, token: orphaned, lastTransferBlock: 16n, snapshot: { blockNumber: 22n, ok: true, zero: false, canonical: false } },
      { account, token: finalized, lastTransferBlock: 16n, snapshot: { blockNumber: 20n, ok: true, zero: false, canonical: true } },
    ]));
    // The counterparty is not a tracked holder here, so only its tail pairs appear, without snapshots.
    expect(await store.erc20RefreshState('ethereum', [other])).toEqual(expect.arrayContaining([
      { account: other, token, lastTransferBlock: 22n },
      { account: other, token: tailOnly, lastTransferBlock: 21n },
    ]));
    expect((await store.erc20Pairs('ethereum', [account])).length).toBe(5);
  } finally {
    await store.close();
  }
});

test('a reorg refresh can replace a balance snapshot from a newer orphaned block', async () => {
  const { store } = await createTestStore();

  try {
    const row = (balance: bigint) => ({ account, token, result: { status: 'ok' as const, balance } });
    await store.saveBalances('ethereum', block(100n), [row(5n)]);
    await store.saveBalances('ethereum', block(99n), [row(4n)]);
    expect((await store.balances('ethereum', account))[0]).toMatchObject({
      result: { status: 'ok', balance: 5n }, blockNumber: 100n, blockHash: block(100n).hash,
    });

    await store.saveBalances('ethereum', block(99n), [row(4n)], { replaceNewer: true });
    expect((await store.balances('ethereum', account))[0]).toMatchObject({
      result: { status: 'ok', balance: 4n }, blockNumber: 99n, blockHash: block(99n).hash,
    });
  } finally {
    await store.close();
  }
});

test('the session leader lock is acquired, re-verified and released', async () => {
  const { db, store } = await createTestStore();

  try {
    const lock = singleConnectionLeaderLock(db, advisoryKey('follower:ethereum'));
    expect(lock.held()).toBe(false);
    expect(await lock.tryAcquire()).toBe(true);
    expect(await lock.tryAcquire()).toBe(true);
    expect(lock.held()).toBe(true);
    await lock.release();
    expect(lock.held()).toBe(false);
    expect(advisoryKey('follower:ethereum')).not.toBe(advisoryKey('follower:base'));
  } finally {
    await store.close();
  }
});
