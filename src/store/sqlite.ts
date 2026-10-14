import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import type { Address, BlockRange, BlockRef, ChunkAggregate, Hex, Store, TokenStandard } from '../core/types.ts';

/** Bump on any schema change: the database is a derived cache, so a mismatch drops and rebuilds it. */
const SCHEMA_VERSION = 2;

export function openSqliteStore(path: string): Store {
  if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });

  const db = new DatabaseSync(path, { readBigInts: true, timeout: 5_000 });
  db.exec('PRAGMA journal_mode = WAL');
  const { user_version: version } = db.prepare('PRAGMA user_version').get() as { user_version: bigint };
  if (Number(version) !== SCHEMA_VERSION) {
    db.exec(`
      BEGIN IMMEDIATE;
      DROP TABLE IF EXISTS account_cursor;
      DROP TABLE IF EXISTS account_scan_progress;
      DROP TABLE IF EXISTS account_scan_range;
      DROP TABLE IF EXISTS holdings;
      DROP TABLE IF EXISTS nft_items;
      CREATE TABLE account_cursor (
        network TEXT NOT NULL,
        account TEXT NOT NULL,
        scanned_to INTEGER NOT NULL,
        scanned_to_hash TEXT NOT NULL,
        scanned_to_timestamp INTEGER NOT NULL,
        PRIMARY KEY (network, account)
      );
      CREATE TABLE account_scan_progress (
        network TEXT NOT NULL,
        account TEXT NOT NULL,
        progress_to INTEGER NOT NULL,
        PRIMARY KEY (network, account)
      );
      CREATE TABLE account_scan_range (
        network TEXT NOT NULL,
        account TEXT NOT NULL,
        logs_range INTEGER NOT NULL,
        PRIMARY KEY (network, account)
      );
      CREATE TABLE holdings (
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
      CREATE TABLE nft_items (
        network TEXT NOT NULL,
        holder TEXT NOT NULL,
        token TEXT NOT NULL,
        token_id TEXT NOT NULL,
        held INTEGER NOT NULL,
        last_block INTEGER NOT NULL,
        last_log_index INTEGER NOT NULL,
        PRIMARY KEY (network, holder, token, token_id)
      );
      PRAGMA user_version = ${SCHEMA_VERSION};
      COMMIT;
    `);
  }

  const getCursorStatement = db.prepare(`
    SELECT scanned_to, scanned_to_hash, scanned_to_timestamp FROM account_cursor WHERE network = ? AND account = ?
  `);
  const getProgressStatement = db.prepare('SELECT progress_to FROM account_scan_progress WHERE network = ? AND account = ?');
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
  // Both cursors only ever move forward.
  const upsertProgress = db.prepare(`
    INSERT INTO account_scan_progress (network, account, progress_to) VALUES (?, ?, ?)
    ON CONFLICT (network, account) DO UPDATE SET progress_to = excluded.progress_to
    WHERE excluded.progress_to > progress_to
  `);
  const upsertCursor = db.prepare(`
    INSERT INTO account_cursor (network, account, scanned_to, scanned_to_hash, scanned_to_timestamp) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (network, account) DO UPDATE SET
      scanned_to = excluded.scanned_to,
      scanned_to_hash = excluded.scanned_to_hash,
      scanned_to_timestamp = excluded.scanned_to_timestamp
    WHERE excluded.scanned_to > scanned_to
  `);
  const getRangeStatement = db.prepare('SELECT logs_range FROM account_scan_range WHERE network = ? AND account = ?');
  const upsertRange = db.prepare(`
    INSERT INTO account_scan_range (network, account, logs_range) VALUES (?, ?, ?)
    ON CONFLICT (network, account) DO UPDATE SET logs_range = excluded.logs_range
  `);
  const selectHoldings = db.prepare(`
    SELECT token, standard, first_block, last_block, sent_count, received_count
    FROM holdings
    WHERE network = ? AND holder = ?
    ORDER BY last_block DESC, token ASC
  `);
  const selectNfts = db.prepare(`
    SELECT token, token_id, held, last_block
    FROM nft_items
    WHERE network = ? AND holder = ?
    ORDER BY last_block DESC, token ASC, token_id ASC
  `);

  const transaction = (fn: () => void): void => {
    db.exec('BEGIN IMMEDIATE');
    try {
      fn();
      db.exec('COMMIT');
    } catch (error) {
      db.exec('ROLLBACK');
      throw error;
    }
  };

  const getProgress = (network: string, account: Address): bigint | undefined =>
    (getProgressStatement.get(network, account) as { progress_to: bigint } | undefined)?.progress_to;

  return {
    getCursor(network, account) {
      const row = getCursorStatement.get(network, account) as
        { scanned_to: bigint; scanned_to_hash: Hex; scanned_to_timestamp: bigint } | undefined;
      return row && { number: row.scanned_to, hash: row.scanned_to_hash, timestamp: row.scanned_to_timestamp };
    },

    getProgress,

    applyScanChunk(network: string, account: Address, range: BlockRange, chunk: ChunkAggregate): void {
      // Idempotency without a per-log journal: chunks at or below progress were already applied.
      // The aggregate keeps writes proportional to distinct tokens, not to transfers.
      transaction(() => {
        const appliedTo = getProgress(network, account) ?? -1n;
        if (range.toBlock <= appliedTo) return;
        if (range.fromBlock <= appliedTo) {
          throw new Error(`Chunk ${range.fromBlock}-${range.toBlock} overlaps applied progress ${appliedTo}`);
        }
        for (const [token, h] of chunk.tokens) {
          upsertHolding.run(network, account, token, h.standard, h.firstBlock, h.lastBlock, h.sentCount, h.receivedCount);
        }
        for (const n of chunk.nfts.values()) {
          upsertNft.run(network, account, n.token, n.tokenId.toString(), n.held ? 1 : 0, n.block, n.logIndex);
        }
        upsertProgress.run(network, account, range.toBlock);
      });
    },

    getLogsRange(network, account) {
      return (getRangeStatement.get(network, account) as { logs_range: bigint } | undefined)?.logs_range;
    },

    setLogsRange(network, account, range) {
      upsertRange.run(network, account, range);
    },

    finalizeScan(network: string, account: Address, scannedTo: BlockRef): void {
      transaction(() => {
        upsertProgress.run(network, account, scannedTo.number);
        upsertCursor.run(network, account, scannedTo.number, scannedTo.hash, scannedTo.timestamp);
      });
    },

    holdings(network, account) {
      const rows = selectHoldings.all(network, account) as {
        token: Address; standard: TokenStandard; first_block: bigint; last_block: bigint; sent_count: bigint; received_count: bigint;
      }[];
      return rows.map((row) => ({
        token: row.token,
        standard: row.standard,
        firstBlock: row.first_block,
        lastBlock: row.last_block,
        sentCount: Number(row.sent_count),
        receivedCount: Number(row.received_count),
      }));
    },

    nftItems(network, account) {
      const rows = selectNfts.all(network, account) as { token: Address; token_id: string; held: bigint; last_block: bigint }[];
      return rows.map((row) => ({
        token: row.token,
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
