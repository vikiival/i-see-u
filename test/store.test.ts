import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { expect, test } from 'vitest';

import type { Address, BlockRef, Hex, Store, TransferEvent } from '../src/core/types.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import { openSqliteStore } from '../src/store/sqlite.ts';

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

const applyScan = (store: Store, events: TransferEvent[], at: BlockRef): void => {
  const fromBlock = (store.getProgress('ethereum', account) ?? -1n) + 1n;
  store.applyScanChunk('ethereum', account, { fromBlock: fromBlock < at.number ? fromBlock : at.number, toBlock: at.number }, aggregateOf(events));
  store.finalizeScan('ethereum', account, at);
};

function event(overrides: Partial<TransferEvent> = {}): TransferEvent {
  return {
    standard: 'erc20',
    token,
    from: account,
    to: other,
    value: 1n,
    blockNumber: 10n,
    logIndex: 0,
    transactionHash: '0x01' as Hex,
    ...overrides,
  };
}

test('applyScan is idempotent and the cursor is monotonic', () => {
  const store = openSqliteStore(':memory:');
  try {
    const events = [event()];
    applyScan(store, events, block(12n));
    applyScan(store, events, block(11n));

    const [holding] = store.holdings('ethereum', account);
    expect(holding.sentCount).toBe(1);
    expect(holding.receivedCount).toBe(0);
    expect(holding.firstBlock).toBe(10n);
    expect(holding.lastBlock).toBe(10n);
    expect(store.getCursor('ethereum', account)?.number).toBe(12n);
    expect(store.getCursor('ethereum', account)?.hash).toBe(block(12n).hash);
  } finally {
    store.close();
  }
});

test('NFT held flag only follows the newest block and log index', () => {
  const store = openSqliteStore(':memory:');
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

    applyScan(store, [incoming, olderOutgoing], block(20n));
    expect(store.nftItems('ethereum', account)[0]?.heldBySeenLogs).toBe(true);

    applyScan(store, [newerOutgoing], block(21n));
    const [item] = store.nftItems('ethereum', account);
    expect(item.heldBySeenLogs).toBe(false);
    expect(item.tokenId).toBe(42n);
    expect(item.lastBlock).toBe(21n);
  } finally {
    store.close();
  }
});

test('a database from another schema version is dropped and rebuilt', () => {
  const directory = mkdtempSync(join(tmpdir(), 'i-see-u-store-'));
  const path = join(directory, 'old.sqlite');
  const old = new DatabaseSync(path);
  old.exec(`CREATE TABLE account_cursor (network TEXT, account TEXT, scanned_to INTEGER); INSERT INTO account_cursor VALUES ('ethereum', '${account}', 12);`);
  old.close();

  try {
    let store = openSqliteStore(path);
    expect(store.getCursor('ethereum', account)).toBeUndefined();
    applyScan(store, [], block(15n));
    store.close();
    store = openSqliteStore(path); // same version: kept
    expect(store.getCursor('ethereum', account)?.hash).toBe(block(15n).hash);
    store.close();
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('a chunk straddling applied progress is rejected and the logs range round-trips', () => {
  const store = openSqliteStore(':memory:');
  try {
    store.applyScanChunk('ethereum', account, { fromBlock: 0n, toBlock: 10n }, aggregateOf([event()]));
    store.applyScanChunk('ethereum', account, { fromBlock: 0n, toBlock: 10n }, aggregateOf([event()]));
    expect(store.holdings('ethereum', account)[0]?.sentCount).toBe(1);
    expect(() => store.applyScanChunk('ethereum', account, { fromBlock: 5n, toBlock: 20n }, aggregateOf([])))
      .toThrow('overlaps applied progress');
    expect(store.getProgress('ethereum', account)).toBe(10n);

    expect(store.getLogsRange('ethereum', account)).toBeUndefined();
    store.setLogsRange('ethereum', account, 750n);
    store.setLogsRange('ethereum', account, 1500n);
    expect(store.getLogsRange('ethereum', account)).toBe(1500n);
  } finally {
    store.close();
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
