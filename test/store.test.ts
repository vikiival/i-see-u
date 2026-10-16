import { migrate } from 'drizzle-orm/pglite/migrator';
import { eq, sql } from 'drizzle-orm';
import { expect, test } from 'vitest';

import type { TransferEvent } from '../src/core/types.ts';
import { advisoryKey, singleConnectionLeaderLock, withAdvisoryLock } from '../src/store/advisory-lock.ts';
import { migrationsFolder } from '../src/store/postgres.ts';
import { createStore } from '../src/store/store.ts';
import * as schema from '../src/store/schema.ts';
import { address, blockHash } from './fake-chain.ts';
import { createTestStore } from './store-fixture.ts';
import { blockRange, header, headersFor, seeded } from './stack.ts';

const [account, peer, token, other] = [1, 2, 3, 4].map(address);

const event = (overrides: Partial<TransferEvent> = {}): TransferEvent => ({
  standard: 'erc20', token, from: account, to: peer, value: 1n,
  blockNumber: 100n, blockHash: blockHash(100n), logIndex: 0, transactionHash: blockHash(1000n), ...overrides,
});

const at = (block: bigint, overrides: Partial<TransferEvent> = {}) => event({ blockNumber: block, blockHash: blockHash(block), ...overrides });

test('atomic append stores every account once, deduplicates identities and counts self-transfers once per side', async () => {
  const self = event({ to: account });
  const { store } = await seeded([self, self, event({ logIndex: 1 })]);

  try {
    expect(await store.holdings('ethereum', account)).toMatchObject([{ firstBlock: 100n, lastBlock: 100n, sentCount: 2n, receivedCount: 1n }]);
    expect(await store.holdings('ethereum', peer)).toMatchObject([{ sentCount: 0n, receivedCount: 1n }]);
    expect((await store.transferHistory('ethereum', account, 50))?.transfers).toHaveLength(2);
    expect(await store.appendRange('ethereum', blockRange(100n, 103n), headersFor(100n, 103n), [self], 2000)).toBe(false);
    expect((await store.holdings('ethereum', account))[0].sentCount).toBe(2n);
    expect((await store.indexState('ethereum'))?.cursor.updatedAt).toBe(1000);
  } finally { await store.close(); }
});

test('NFT ownership is one global latest (block, logIndex) winner, regardless of insertion order; burns remove holders', async () => {
  const { store } = await seeded([
    at(102n, { standard: 'erc721', value: 42n, from: peer, to: other, logIndex: 3 }),
    event({ standard: 'erc721', value: 42n, from: account, to: peer }),
    at(102n, { standard: 'erc721', value: 42n, from: peer, to: account, logIndex: 2 }),
  ]);

  try {
    expect(await store.nftItems('ethereum', account)).toEqual([]);
    expect(await store.nftItems('ethereum', other)).toMatchObject([{ tokenId: 42n, lastBlock: 102n, heldBySeenLogs: true }]);
    await store.observeHead('ethereum', header(104n), 2000);
    await store.appendRange('ethereum', blockRange(104n), headersFor(104n), [at(104n, { standard: 'erc721', value: 42n, from: other, to: address(0) })], 2000);
    expect(await store.nftItems('ethereum', other)).toEqual([]);
    expect(await store.nftItems('ethereum', address(0))).toEqual([]);
  } finally { await store.close(); }
});

test('empty ranges still commit headers and advance a contiguous cursor', async () => {
  const { store } = await seeded([]);

  try {
    expect((await store.indexState('ethereum'))?.cursor.number).toBe(103n);
    expect(await store.transferHistory('ethereum', account, 10)).toMatchObject({ transfers: [] });
  } finally { await store.close(); }
});

test('initialization cannot skip history, change the start, or replace the anchor', async () => {
  const { store } = await createTestStore();

  try {
    await expect(store.initialize('ethereum', 100n, header(103n), 1)).rejects.toThrow('immediately before');
    await store.initialize('ethereum', 100n, header(99n), 1);
    await expect(store.initialize('ethereum', 101n, header(100n), 1)).rejects.toThrow('start block changed');
    await expect(store.initialize('ethereum', 100n, { ...header(99n), hash: blockHash(999n) }, 1)).rejects.toThrow('Anchor hash changed');
    expect((await store.indexState('ethereum'))?.cursor.number).toBe(99n);
  } finally { await store.close(); }
});

test('append rejects gaps, partial overlap, incomplete or broken chains, and a range beyond finalized', async () => {
  const { store } = await seeded([]);

  try {
    await store.observeHead('ethereum', header(106n), 2000);
    await expect(store.appendRange('ethereum', blockRange(105n), headersFor(105n), [], 2)).rejects.toThrow('continue cursor');
    await expect(store.appendRange('ethereum', blockRange(103n, 104n), headersFor(103n, 104n), [], 2)).rejects.toThrow('continue cursor');
    await expect(store.appendRange('ethereum', blockRange(104n, 105n), headersFor(104n), [], 2)).rejects.toThrow('every block');
    await expect(store.appendRange('ethereum', blockRange(104n), [{ ...header(104n), parentHash: blockHash(9n) }], [], 2)).rejects.toThrow('parent hash');
    await expect(store.appendRange('ethereum', blockRange(104n, 105n), [header(104n), { ...header(105n), parentHash: blockHash(9n) }], [], 2)).rejects.toThrow('Header chain gap');
    await expect(store.appendRange('ethereum', blockRange(104n, 107n), headersFor(104n, 107n), [], 2)).rejects.toThrow('finalized head');
    expect((await store.indexState('ethereum'))?.cursor.number).toBe(103n);
  } finally { await store.close(); }
});

test('bad transfer identity, header hash and SQL constraints cannot partially advance a range', async () => {
  const { store } = await seeded([]);

  try {
    await store.observeHead('ethereum', header(104n), 2);
    const append = (events: TransferEvent[]) => store.appendRange('ethereum', blockRange(104n), headersFor(104n), events, 2);
    await expect(append([at(104n), at(104n, { value: 2n })])).rejects.toThrow('Conflicting transfer');
    await expect(append([at(103n)])).rejects.toThrow('does not match');
    await expect(append([at(104n, { logIndex: -1 })])).rejects.toThrow();
    await expect(append([at(104n, { value: 1n << 256n })])).rejects.toThrow();
    expect((await store.indexState('ethereum'))?.cursor.number).toBe(103n);
    expect(await store.holdings('ethereum', account)).toEqual([]);
    expect(await append([at(104n)])).toBe(true);
  } finally { await store.close(); }
});

test('concurrent writers of the same range apply counts once', async () => {
  const { store } = await seeded([]);

  try {
    await store.observeHead('ethereum', header(104n), 2);
    const run = () => store.appendRange('ethereum', blockRange(104n), headersFor(104n), [at(104n)], 2);
    expect((await Promise.all([run(), run()])).sort()).toEqual([false, true]);
    expect((await store.holdings('ethereum', account))[0].sentCount).toBe(1n);
  } finally { await store.close(); }
});

test('replaceRange restores bounds/counts from full history and removes obsolete pairs including old-only accounts', async () => {
  const { store } = await seeded([event(), at(101n), at(102n), at(103n, { token: other })]);

  try {
    await store.enqueueRepair('ethereum', blockRange(100n, 101n), 'suspect logs', 1);
    await store.enqueueRepair('ethereum', blockRange(100n, 101n), 'again', 2);
    expect(await store.repairs('ethereum')).toHaveLength(1);
    await store.replaceRange('ethereum', blockRange(100n, 101n), headersFor(100n, 101n), [at(101n, { from: other, to: other })]);
    expect(await store.holdings('ethereum', account)).toMatchObject([
      { token: other, firstBlock: 103n, sentCount: 1n }, { token, firstBlock: 102n, lastBlock: 102n, sentCount: 1n },
    ]);
    expect((await store.holdings('ethereum', other))[0]).toMatchObject({ token, sentCount: 1n, receivedCount: 1n });
    await store.replaceRange('ethereum', blockRange(102n), headersFor(102n), []);
    expect(await store.holdings('ethereum', peer)).toMatchObject([{ token: other }]);
    expect((await store.indexState('ethereum'))?.cursor).toMatchObject({ number: 103n, revision: 2n });
    expect(await store.repairs('ethereum')).toEqual([]);
  } finally { await store.close(); }
});

test('repairing an NFT transfer restores the earlier owner and respects a later transfer outside the repaired range', async () => {
  const n = { standard: 'erc721' as const, value: 7n };
  const { store } = await seeded([event({ ...n, from: peer, to: account }), at(101n, { ...n, from: account, to: other }), at(103n, { ...n, from: other, to: peer })]);

  try {
    await store.replaceRange('ethereum', blockRange(101n), headersFor(101n), []);
    expect(await store.nftItems('ethereum', peer)).toMatchObject([{ tokenId: 7n, lastBlock: 103n }]);
    await store.replaceRange('ethereum', blockRange(103n), headersFor(103n), []);
    expect(await store.nftItems('ethereum', account)).toMatchObject([{ tokenId: 7n, lastBlock: 100n }]);
    expect(await store.nftItems('ethereum', peer)).toEqual([]);
    await store.replaceRange('ethereum', blockRange(100n), headersFor(100n), []);
    expect(await store.nftItems('ethereum', account)).toEqual([]);
  } finally { await store.close(); }
});

test('a replacement NFT ID rebuilds both removed and new items, and repeated repairs do not add counts', async () => {
  const { store } = await seeded([event({ standard: 'erc721', value: 1n })]);

  try {
    for (let i = 0; i < 2; i += 1) await store.replaceRange('ethereum', blockRange(100n), headersFor(100n), [event({ standard: 'erc721', value: 2n, to: other })]);
    expect(await store.nftItems('ethereum', peer)).toEqual([]);
    expect(await store.nftItems('ethereum', other)).toMatchObject([{ tokenId: 2n }]);
    expect((await store.holdings('ethereum', account))[0].sentCount).toBe(1n);
  } finally { await store.close(); }
});

test('repair failure rolls back raw deletion, summaries, cursor revision and queue resolution', async () => {
  const { store } = await seeded([event()]);

  try {
    await store.enqueueRepair('ethereum', blockRange(100n), 'bad source', 1);
    await expect(store.replaceRange('ethereum', blockRange(100n), headersFor(100n), [event({ value: -1n })])).rejects.toThrow();
    expect((await store.transferHistory('ethereum', account, 10))?.transfers).toEqual([event()]);
    expect((await store.holdings('ethereum', account))[0].sentCount).toBe(1n);
    expect((await store.indexState('ethereum'))?.cursor.revision).toBe(0n);
    expect(await store.repairs('ethereum')).toHaveLength(1);
  } finally { await store.close(); }
});

test('repair must reconnect to both neighbors and cannot replace uncommitted blocks', async () => {
  const { store } = await seeded([event()]);

  try {
    await expect(store.replaceRange('ethereum', blockRange(104n), headersFor(104n), [])).rejects.toThrow('committed index');
    await expect(store.replaceRange('ethereum', blockRange(99n), headersFor(99n), [])).rejects.toThrow('committed index');
    await expect(store.replaceRange('ethereum', blockRange(101n), [{ ...header(101n), hash: blockHash(999n) }], [])).rejects.toThrow('surrounding');
    await expect(store.replaceRange('ethereum', blockRange(101n), [{ ...header(101n), parentHash: blockHash(999n) }], [])).rejects.toThrow('surrounding');
  } finally { await store.close(); }
});

test('history keysets merge both directions, deduplicate self-transfers and stay bounded during appends', async () => {
  const { store } = await seeded([
    event(), at(101n, { from: peer, to: account }), at(102n, { to: account }), at(102n, { logIndex: 1 }), at(103n),
  ]);

  try {
    const first = (await store.transferHistory('ethereum', account, 2))!;
    expect(first.transfers.map((e) => [e.blockNumber, e.logIndex])).toEqual([[103n, 0], [102n, 1]]);
    await store.observeHead('ethereum', header(104n), 2);
    await store.appendRange('ethereum', blockRange(104n), headersFor(104n), [at(104n)], 2);
    const second = (await store.transferHistory('ethereum', account, 2, first.next))!;
    const third = (await store.transferHistory('ethereum', account, 2, second.next))!;
    expect(second.transfers.map((e) => e.blockNumber)).toEqual([102n, 101n]);
    expect(third.transfers.map((e) => e.blockNumber)).toEqual([100n]);
    expect(third.next).toBeUndefined();
    await store.replaceRange('ethereum', blockRange(100n), headersFor(100n), []);
    await expect(store.transferHistory('ethereum', account, 2, first.next)).rejects.toMatchObject({ statusCode: 409 });
  } finally { await store.close(); }
});

test('token holder keysets are backed by the reverse summary index', async () => {
  const { store } = await seeded([event(), event({ logIndex: 1, from: peer, to: other })]);

  try {
    expect(await store.tokenHolders('ethereum', token, 2)).toEqual([account, peer]);
    expect(await store.tokenHolders('ethereum', token, 2, peer)).toEqual([other]);
  } finally { await store.close(); }
});

test('binary addresses/hashes, bigint counts and uint256 maximum round-trip exactly', async () => {
  const maximum = (1n << 256n) - 1n;
  const { store, db } = await seeded([event({ value: maximum })]);

  try {
    expect((await store.transferHistory('ethereum', account, 10))?.transfers[0].value).toBe(maximum);
    await db.update(schema.holdings).set({ sentCount: 3_000_000_000n }).where(eq(schema.holdings.holder, account));
    expect((await store.holdings('ethereum', account))[0].sentCount).toBe(3_000_000_000n);
    const rows = await db.select({ addressBytes: sql<number>`octet_length(${schema.transfers.from})`, hashBytes: sql<number>`octet_length(${schema.transfers.blockHash})` }).from(schema.transfers);
    expect(rows).toEqual([{ addressBytes: 20, hashBytes: 32 }]);
  } finally { await store.close(); }
});

test('balance cache labels exact block/read time, stores errors and rejects older or noncanonical writes', async () => {
  const { store } = await seeded([event()]);
  const rows = [{ account, token, result: { status: 'ok' as const, balance: (1n << 256n) - 1n } }];

  try {
    expect(await store.saveBalances('ethereum', header(102n), rows, 30, 0n)).toBe(true);
    await store.saveBalances('ethereum', header(101n), [{ ...rows[0], result: { status: 'ok', balance: 1n } }], 40, 0n);
    expect(await store.saveBalances('ethereum', { ...header(102n), hash: blockHash(9n) }, rows, 50, 0n)).toBe(false);
    expect(await store.balances('ethereum', account)).toEqual([{ token, result: rows[0].result, blockNumber: 102n, blockHash: header(102n).hash, readAt: 30, revision: 0n, canonical: true }]);
    await store.saveBalances('ethereum', header(103n), [{ account, token, result: { status: 'error', error: 'reverted' } }], 60, 0n);
    expect((await store.balances('ethereum', account))[0]).toMatchObject({ result: { status: 'error', error: 'reverted' }, readAt: 60 });
  } finally { await store.close(); }
});

test('repair invalidates affected cache entries and fences delayed cache writes with the old revision', async () => {
  const { store } = await seeded([event()]);
  const rows = [{ account, token, result: { status: 'ok' as const, balance: 5n } }];

  try {
    await store.saveBalances('ethereum', header(103n), rows, 1, 0n);
    await store.replaceRange('ethereum', blockRange(100n), headersFor(100n), []);
    expect(await store.balances('ethereum', account)).toEqual([]);
    expect(await store.saveBalances('ethereum', header(103n), rows, 2, 0n)).toBe(false);
    expect(await store.saveBalances('ethereum', header(103n), rows, 3, 1n)).toBe(true);
  } finally { await store.close(); }
});

test('every table is isolated by registry network, including repair, balances, history and ownership', async () => {
  const { db, store: original } = await createTestStore();
  const store = createStore(db, () => original.close(), new Map([['ethereum', 1], ['second', 2]]));

  try {
    for (const name of ['ethereum', 'second']) {
      await store.initialize(name, 100n, header(99n), 1);
      await store.observeHead(name, header(100n), 1);
      await store.appendRange(name, blockRange(100n), headersFor(100n), [event({ standard: 'erc721', value: name === 'ethereum' ? 1n : 2n })], 1);
      await store.saveBalances(name, header(100n), [{ account, token, result: { status: 'ok', balance: name === 'ethereum' ? 1n : 2n } }], 1, 0n);
    }

    await store.enqueueRepair('ethereum', blockRange(100n), 'test', 1);
    await store.replaceRange('ethereum', blockRange(100n), headersFor(100n), []);
    expect(await store.holdings('ethereum', account)).toEqual([]);
    expect(await store.holdings('second', account)).toHaveLength(1);
    expect((await store.transferHistory('second', account, 10))?.transfers[0].value).toBe(2n);
    expect((await store.nftItems('second', peer))[0].tokenId).toBe(2n);
    expect((await store.balances('second', account))[0].result).toEqual({ status: 'ok', balance: 2n });
    expect((await store.indexState('second'))?.cursor.revision).toBe(0n);
  } finally { await store.close(); }
});

test('migrations are repeatable and session/transaction advisory locks still work', async () => {
  const { db, store } = await createTestStore();

  try {
    await migrate(db, { migrationsFolder });
    expect(await withAdvisoryLock(db, 42n, async () => 'held')).toBe('held');
    const lock = singleConnectionLeaderLock(db, advisoryKey('follower:ethereum'));
    expect(await lock.tryAcquire()).toBe(true);
    expect(await lock.tryAcquire()).toBe(true);
    await lock.release();
    expect(lock.held()).toBe(false);
    expect(advisoryKey('follower:ethereum')).not.toBe(advisoryKey('follower:second'));
  } finally { await store.close(); }
});

test('derived rows rebuild from raw range without a source fetch or raw mutation', async () => {
  const { store, db } = await seeded([event({ standard: 'erc721', value: 7n }), at(102n)]);

  try {
    await db.delete(schema.holdings);
    await db.delete(schema.nftOwnership);
    await store.enqueueRepair('ethereum', blockRange(100n), 'needs alternate source', 1);
    await store.rebuildRange('ethereum', blockRange(100n));
    expect((await store.holdings('ethereum', account))[0]).toMatchObject({ firstBlock: 100n, lastBlock: 102n, sentCount: 2n });
    expect(await store.nftItems('ethereum', peer)).toMatchObject([{ tokenId: 7n }]);
    expect((await store.transferHistory('ethereum', account, 10))?.transfers).toHaveLength(2);
    expect(await store.repairs('ethereum')).toHaveLength(1);
  } finally { await store.close(); }
});

test('repair invalidates snapshots at repaired block hashes even for accounts absent from the range logs', async () => {
  const { store } = await seeded([]);

  try {
    await store.saveBalances('ethereum', header(103n), [{ account, token, result: { status: 'ok', balance: 5n } }], 1, 0n);
    const replacement = { ...header(103n), hash: blockHash(999n) };
    await store.observeHead('ethereum', replacement, 2);
    await store.replaceRange('ethereum', blockRange(103n), [replacement], []);
    expect(await store.balances('ethereum', account)).toEqual([]);
    expect((await store.indexState('ethereum'))?.cursor.hash).toBe(replacement.hash);
  } finally { await store.close(); }
});

test('tail journal appends are fenced, balances save only at canonical tail blocks, and repair at the cursor wipes the tail', async () => {
  const { store } = await seeded([]);
  const rows = [{ account, token, result: { status: 'ok' as const, balance: 5n } }];

  try {
    await expect(store.appendTailBlocks('ethereum', blockHash(9n), [{ header: header(104n), transfers: [] }])).rejects.toThrow('Tail parent changed');
    await expect(store.appendTailBlocks('ethereum', blockHash(103n), [{ header: header(105n), transfers: [] }])).rejects.toThrow('Non-contiguous');
    await expect(store.appendTailBlocks('ethereum', blockHash(103n), [{ header: header(104n), transfers: [at(105n)] }])).rejects.toThrow('does not belong');
    await store.appendTailBlocks('ethereum', blockHash(103n), [{ header: header(104n), transfers: [at(104n)] }, { header: header(105n), transfers: [] }]);
    expect(await store.tailTip('ethereum')).toEqual(header(105n));
    expect(await store.saveBalances('ethereum', header(105n), rows, 1, 0n)).toBe(true);
    expect(await store.saveBalances('ethereum', { ...header(105n), hash: blockHash(9n) }, rows, 2, 0n)).toBe(false);
    expect(await store.saveBalances('ethereum', header(106n), rows, 3, 0n)).toBe(false);
    await expect(store.rewindTail('ethereum', 104n, blockHash(9n))).rejects.toThrow('Tail tip changed');
    await store.rewindTail('ethereum', 104n, blockHash(105n));
    expect((await store.balances('ethereum', account))[0]).toMatchObject({ blockNumber: 105n, canonical: false });
    expect((await store.readAccount('ethereum', account, { tail: true }))?.tailTransfers).toMatchObject([{ blockNumber: 104n, to: peer }]);
    const replacement = { ...header(103n), hash: blockHash(999n) };
    await store.observeHead('ethereum', replacement, 2);
    await store.replaceRange('ethereum', blockRange(103n), [replacement], []);
    expect(await store.tailBlocks('ethereum')).toEqual([]);
  } finally { await store.close(); }
});
