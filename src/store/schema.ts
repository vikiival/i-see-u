import { bigint, boolean, index, integer, pgTable, primaryKey, text } from 'drizzle-orm/pg-core';

import type { TokenStandard } from '../core/types.ts';

/** Global finalized follower cursor: every Transfer through block_number was applied for every tracked account. */
export const followerCursor = pgTable('follower_cursor', {
  network: text().primaryKey(),
  blockNumber: bigint('block_number', { mode: 'bigint' }).notNull(),
  blockHash: text('block_hash').notNull(),
  blockTimestamp: bigint('block_timestamp', { mode: 'bigint' }).notNull(),
  /** Epoch milliseconds of the last advance (written by the follower's clock). */
  updatedAt: bigint('updated_at', { mode: 'number' }).notNull(),
});

/** Reorgable block journal above follower_cursor. Finalized aggregates never depend on these rows. */
export const tailBlocks = pgTable('tail_blocks', {
  network: text().notNull(),
  number: bigint({ mode: 'bigint' }).notNull(),
  hash: text().notNull(),
  parentHash: text('parent_hash').notNull(),
  timestamp: bigint({ mode: 'bigint' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.number] })]);

export const tailTransfers = pgTable('tail_transfers', {
  network: text().notNull(),
  blockNumber: bigint('block_number', { mode: 'bigint' }).notNull(),
  logIndex: integer('log_index').notNull(),
  blockHash: text('block_hash').notNull(),
  token: text().notNull(),
  standard: text().$type<TokenStandard>().notNull(),
  from: text().notNull(),
  to: text().notNull(),
  value: text().notNull(),
  tokenId: text('token_id'),
  txHash: text('tx_hash').notNull(),
}, (table) => [
  primaryKey({ columns: [table.network, table.blockNumber, table.logIndex] }),
  index('tail_transfers_network_from_idx').on(table.network, table.from),
  index('tail_transfers_network_to_idx').on(table.network, table.to),
]);

/**
 * Accounts the follower applies Transfers for. backfill_target is the follower cursor read in the same transaction
 * that inserted the row: the backfill owns blocks <= target (progress in account_scan_progress), the follower > target.
 */
export const trackedAccounts = pgTable('tracked_accounts', {
  network: text().notNull(),
  account: text().notNull(),
  backfillTarget: bigint('backfill_target', { mode: 'bigint' }).notNull(),
  trackedAt: bigint('tracked_at', { mode: 'number' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account] })]);

/** Latest balanceOf read per pair, always labelled with the block it was read at (never relabelled). */
export const balanceSnapshots = pgTable('balance_snapshots', {
  network: text().notNull(),
  account: text().notNull(),
  token: text().notNull(),
  /** Decimal uint256, null when the read failed. */
  balance: text(),
  error: text(),
  blockNumber: bigint('block_number', { mode: 'bigint' }).notNull(),
  blockHash: text('block_hash').notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account, table.token] })]);

export const accountScanProgress = pgTable('account_scan_progress', {
  network: text().notNull(),
  account: text().notNull(),
  progressTo: bigint('progress_to', { mode: 'bigint' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account] })]);

export const accountScanRange = pgTable('account_scan_range', {
  network: text().notNull(),
  account: text().notNull(),
  logsRange: bigint('logs_range', { mode: 'bigint' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account] })]);

export const holdings = pgTable('holdings', {
  network: text().notNull(),
  holder: text().notNull(),
  token: text().notNull(),
  standard: text().$type<TokenStandard>().notNull(),
  firstBlock: bigint('first_block', { mode: 'bigint' }).notNull(),
  lastBlock: bigint('last_block', { mode: 'bigint' }).notNull(),
  sentCount: integer('sent_count').notNull(),
  receivedCount: integer('received_count').notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.holder, table.token] })]);

export const nftItems = pgTable('nft_items', {
  network: text().notNull(),
  holder: text().notNull(),
  token: text().notNull(),
  tokenId: text('token_id').notNull(),
  held: boolean().notNull(),
  lastBlock: bigint('last_block', { mode: 'bigint' }).notNull(),
  lastLogIndex: integer('last_log_index').notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.holder, table.token, table.tokenId] })]);
