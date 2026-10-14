import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import type {
  AccountCursor,
  Address,
  BlockRange,
  BlockRef,
  ChunkAggregate,
  Holding,
  Hex,
  NftItem,
  Store,
  TokenStandard,
} from '../core/types.ts';

interface CursorRow {
  network: string;
  account: string;
  scanned_to: bigint;
  scanned_to_hash: string;
  scanned_to_parent_hash: string;
  scanned_to_timestamp: bigint;
  updated_at: bigint;
}

interface ProgressRow {
  progress_to: bigint;
}

interface HoldingRow {
  network: string;
  holder: string;
  token: string;
  standard: string;
  first_block: bigint;
  last_block: bigint;
  sent_count: bigint;
  received_count: bigint;
}

interface NftRow {
  network: string;
  holder: string;
  token: string;
  token_id: string;
  held: bigint;
  last_block: bigint;
}

export function openSqliteStore(path: string): Store {
  if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });

  const db = new DatabaseSync(path, { readBigInts: true, timeout: 5_000 });
  db.exec(`
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS account_cursor (
      network TEXT NOT NULL,
      account TEXT NOT NULL,
      scanned_to INTEGER NOT NULL,
      scanned_to_hash TEXT NOT NULL,
      scanned_to_parent_hash TEXT NOT NULL,
      scanned_to_timestamp INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (network, account)
    );
    CREATE TABLE IF NOT EXISTS account_scan_progress (
      network TEXT NOT NULL,
      account TEXT NOT NULL,
      progress_to INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (network, account)
    );
    CREATE TABLE IF NOT EXISTS account_scan_range (
      network TEXT NOT NULL,
      account TEXT NOT NULL,
      logs_range INTEGER NOT NULL,
      PRIMARY KEY (network, account)
    );
    CREATE TABLE IF NOT EXISTS holdings (
      network TEXT NOT NULL,
      holder TEXT NOT NULL,
      token TEXT NOT NULL,
      standard TEXT NOT NULL,
      first_block INTEGER NOT NULL,
      last_block INTEGER NOT NULL,
      sent_count INTEGER NOT NULL,
      received_count INTEGER NOT NULL,
      PRIMARY KEY (network, holder, token)
    );
    CREATE TABLE IF NOT EXISTS nft_items (
      network TEXT NOT NULL,
      holder TEXT NOT NULL,
      token TEXT NOT NULL,
      token_id TEXT NOT NULL,
      held INTEGER NOT NULL,
      last_block INTEGER NOT NULL,
      last_log_index INTEGER NOT NULL,
      PRIMARY KEY (network, holder, token, token_id)
    );
  `);

  const cursorColumns = db.prepare('PRAGMA table_info(account_cursor)').all() as unknown as Array<{ name: string }>;
  const missingCursorColumns = ['scanned_to_hash', 'scanned_to_parent_hash', 'scanned_to_timestamp']
    .filter((name) => !cursorColumns.some((column) => column.name === name));
  if (missingCursorColumns.length > 0) {
    // Legacy summaries lack a complete BlockRef, so rebuild this derived cache safely.
    db.exec(`
      BEGIN IMMEDIATE;
      ${missingCursorColumns.map((name) => `ALTER TABLE account_cursor ADD COLUMN ${name} ${name === 'scanned_to_timestamp' ? 'INTEGER' : 'TEXT'};`).join('\n')}
      DELETE FROM account_cursor;
      DELETE FROM account_scan_progress;
      DELETE FROM holdings;
      DELETE FROM nft_items;
      COMMIT;
    `);
  }

  const getCursorStatement = db.prepare(`
    SELECT network, account, scanned_to, scanned_to_hash, scanned_to_parent_hash,
           scanned_to_timestamp, updated_at
    FROM account_cursor
    WHERE network = ? AND account = ?
  `);
  const getProgressStatement = db.prepare(`
    SELECT progress_to FROM account_scan_progress WHERE network = ? AND account = ?
  `);
  const upsertHolding = db.prepare(`
    INSERT INTO holdings (
      network, holder, token, standard, first_block, last_block, sent_count, received_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (network, holder, token) DO UPDATE SET
      standard = excluded.standard,
      first_block = MIN(first_block, excluded.first_block),
      last_block = MAX(last_block, excluded.last_block),
      sent_count = sent_count + excluded.sent_count,
      received_count = received_count + excluded.received_count
  `);
  const upsertNft = db.prepare(`
    INSERT INTO nft_items (
      network, holder, token, token_id, held, last_block, last_log_index
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (network, holder, token, token_id) DO UPDATE SET
      held = excluded.held,
      last_block = excluded.last_block,
      last_log_index = excluded.last_log_index
    WHERE excluded.last_block > last_block
       OR (excluded.last_block = last_block AND excluded.last_log_index > last_log_index)
  `);
  const upsertProgress = db.prepare(`
    INSERT INTO account_scan_progress (network, account, progress_to, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT (network, account) DO UPDATE SET
      progress_to = MAX(account_scan_progress.progress_to, excluded.progress_to),
      updated_at = excluded.updated_at
  `);
  const upsertCursor = db.prepare(`
    INSERT INTO account_cursor (
      network, account, scanned_to, scanned_to_hash, scanned_to_parent_hash,
      scanned_to_timestamp, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (network, account) DO UPDATE SET
      scanned_to = MAX(account_cursor.scanned_to, excluded.scanned_to),
      scanned_to_hash = CASE
        WHEN excluded.scanned_to > account_cursor.scanned_to THEN excluded.scanned_to_hash
        ELSE account_cursor.scanned_to_hash
      END,
      scanned_to_parent_hash = CASE
        WHEN excluded.scanned_to > account_cursor.scanned_to THEN excluded.scanned_to_parent_hash
        ELSE account_cursor.scanned_to_parent_hash
      END,
      scanned_to_timestamp = CASE
        WHEN excluded.scanned_to > account_cursor.scanned_to THEN excluded.scanned_to_timestamp
        ELSE account_cursor.scanned_to_timestamp
      END,
      updated_at = excluded.updated_at
  `);
  const getRangeStatement = db.prepare('SELECT logs_range FROM account_scan_range WHERE network = ? AND account = ?');
  const upsertRange = db.prepare(`
    INSERT INTO account_scan_range (network, account, logs_range) VALUES (?, ?, ?)
    ON CONFLICT (network, account) DO UPDATE SET logs_range = excluded.logs_range
  `);
  const selectHoldings = db.prepare(`
    SELECT network, holder, token, standard, first_block, last_block, sent_count, received_count
    FROM holdings
    WHERE network = ? AND holder = ? AND standard = ?
    ORDER BY last_block DESC, token ASC
  `);
  const selectNfts = db.prepare(`
    SELECT network, holder, token, token_id, held, last_block
    FROM nft_items
    WHERE network = ? AND holder = ?
    ORDER BY last_block DESC, token ASC, token_id ASC
  `);

  const getCursor = (network: string, account: Address): AccountCursor | undefined => {
    const row = getCursorStatement.get(network, account) as CursorRow | undefined;
    if (!row) return undefined;
    return {
      network: row.network,
      account: row.account as Address,
      scannedTo: row.scanned_to,
      scannedToHash: row.scanned_to_hash as Hex,
      scannedToParentHash: row.scanned_to_parent_hash as Hex,
      scannedToTimestamp: row.scanned_to_timestamp,
      updatedAt: Number(row.updated_at),
    };
  };

  const getProgress = (network: string, account: Address): bigint | undefined => {
    const row = getProgressStatement.get(network, account) as ProgressRow | undefined;
    return row?.progress_to;
  };

  return {
    getCursor,
    getProgress,

    applyScanChunk(network: string, account: Address, range: BlockRange, chunk: ChunkAggregate): void {
      // Idempotency without a per-log journal: chunks at or below progress were already applied.
      // The aggregate keeps writes proportional to distinct tokens, not to transfers.
      db.exec('BEGIN IMMEDIATE');
      try {
        const appliedTo = getProgress(network, account) ?? getCursor(network, account)?.scannedTo ?? -1n;
        if (range.toBlock > appliedTo) {
          if (range.fromBlock <= appliedTo) {
            throw new Error(`Chunk ${range.fromBlock}-${range.toBlock} overlaps applied progress ${appliedTo}`);
          }
          for (const [token, h] of chunk.tokens) {
            upsertHolding.run(network, account, token, h.standard, h.firstBlock, h.lastBlock, h.sentCount, h.receivedCount);
          }
          for (const n of chunk.nfts.values()) {
            upsertNft.run(network, account, n.token, n.tokenId.toString(), n.held ? 1 : 0, n.block, n.logIndex);
          }
          upsertProgress.run(network, account, range.toBlock, Date.now());
        }
        db.exec('COMMIT');
      } catch (error) {
        db.exec('ROLLBACK');
        throw error;
      }
    },

    getLogsRange(network: string, account: Address): bigint | undefined {
      return (getRangeStatement.get(network, account) as { logs_range: bigint } | undefined)?.logs_range;
    },

    setLogsRange(network: string, account: Address, range: bigint): void {
      upsertRange.run(network, account, range);
    },

    finalizeScan(network: string, account: Address, scannedTo: BlockRef): void {
      db.exec('BEGIN IMMEDIATE');
      try {
        const now = Date.now();
        upsertProgress.run(network, account, scannedTo.number, now);
        upsertCursor.run(
          network,
          account,
          scannedTo.number,
          scannedTo.hash,
          scannedTo.parentHash,
          scannedTo.timestamp,
          now,
        );
        db.exec('COMMIT');
      } catch (error) {
        db.exec('ROLLBACK');
        throw error;
      }
    },

    holdings(network: string, account: Address, standard: TokenStandard): Holding[] {
      return (selectHoldings.all(network, account, standard) as unknown as HoldingRow[]).map((row) => ({
        network: row.network,
        holder: row.holder as Address,
        token: row.token as Address,
        standard: row.standard as TokenStandard,
        firstBlock: row.first_block,
        lastBlock: row.last_block,
        sentCount: Number(row.sent_count),
        receivedCount: Number(row.received_count),
      }));
    },

    nftItems(network: string, account: Address): NftItem[] {
      return (selectNfts.all(network, account) as unknown as NftRow[]).map((row) => ({
        network: row.network,
        holder: row.holder as Address,
        token: row.token as Address,
        tokenId: BigInt(row.token_id),
        heldBySeenLogs: row.held !== 0n,
        lastBlock: row.last_block,
      }));
    },

    close(): void {
      db.close();
    },
  };
}
