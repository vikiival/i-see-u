import { bigint, boolean, index, integer, pgTable, primaryKey, text } from 'drizzle-orm/pg-core';

import type { Address, Hex, TokenStandard } from '../core/types.ts';

// Address and Hex columns ($type) are typed at the schema: every insert passes an Address/Hex value the code already
// validated and lowercased (decoded logs, normalized accounts, block headers), so reads return those domain types.

/** Global finalized follower cursor: every Transfer through block_number was applied for every tracked account. */
export const followerCursor = pgTable('follower_cursor', {
  network: text().primaryKey(),
  blockNumber: bigint('block_number', { mode: 'bigint' }).notNull(),
  blockHash: text('block_hash').$type<Hex>().notNull(),
  blockTimestamp: bigint('block_timestamp', { mode: 'bigint' }).notNull(),
  /** Epoch milliseconds of the last advance (written by the follower's clock). */
  updatedAt: bigint('updated_at', { mode: 'number' }).notNull(),
});

/** Reorgable block journal above follower_cursor. Finalized aggregates never depend on these rows. */
export const tailBlocks = pgTable('tail_blocks', {
  network: text().notNull(),
  number: bigint({ mode: 'bigint' }).notNull(),
  hash: text().$type<Hex>().notNull(),
  parentHash: text('parent_hash').$type<Hex>().notNull(),
  timestamp: bigint({ mode: 'bigint' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.number] })]);

export const tailTransfers = pgTable('tail_transfers', {
  network: text().notNull(),
  blockNumber: bigint('block_number', { mode: 'bigint' }).notNull(),
  logIndex: integer('log_index').notNull(),
  blockHash: text('block_hash').$type<Hex>().notNull(),
  token: text().$type<Address>().notNull(),
  standard: text().$type<TokenStandard>().notNull(),
  from: text().$type<Address>().notNull(),
  to: text().$type<Address>().notNull(),
  value: text().notNull(),
  tokenId: text('token_id'),
  txHash: text('tx_hash').$type<Hex>().notNull(),
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
  account: text().$type<Address>().notNull(),
  backfillTarget: bigint('backfill_target', { mode: 'bigint' }).notNull(),
  trackedAt: bigint('tracked_at', { mode: 'number' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account] })]);

/** Latest balanceOf read per pair, always labelled with the block it was read at (never relabelled). */
export const balanceSnapshots = pgTable('balance_snapshots', {
  network: text().notNull(),
  account: text().$type<Address>().notNull(),
  token: text().$type<Address>().notNull(),
  /** Decimal uint256, null when the read failed. */
  balance: text(),
  error: text(),
  blockNumber: bigint('block_number', { mode: 'bigint' }).notNull(),
  blockHash: text('block_hash').$type<Hex>().notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account, table.token] })]);

export const accountScanProgress = pgTable('account_scan_progress', {
  network: text().notNull(),
  account: text().$type<Address>().notNull(),
  progressTo: bigint('progress_to', { mode: 'bigint' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account] })]);

export const accountScanRange = pgTable('account_scan_range', {
  network: text().notNull(),
  account: text().$type<Address>().notNull(),
  logsRange: bigint('logs_range', { mode: 'bigint' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account] })]);

export const holdings = pgTable('holdings', {
  network: text().notNull(),
  holder: text().$type<Address>().notNull(),
  token: text().$type<Address>().notNull(),
  standard: text().$type<TokenStandard>().notNull(),
  firstBlock: bigint('first_block', { mode: 'bigint' }).notNull(),
  lastBlock: bigint('last_block', { mode: 'bigint' }).notNull(),
  sentCount: integer('sent_count').notNull(),
  receivedCount: integer('received_count').notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.holder, table.token] })]);

export const nftItems = pgTable('nft_items', {
  network: text().notNull(),
  holder: text().$type<Address>().notNull(),
  token: text().$type<Address>().notNull(),
  tokenId: text('token_id').notNull(),
  held: boolean().notNull(),
  lastBlock: bigint('last_block', { mode: 'bigint' }).notNull(),
  lastLogIndex: integer('last_log_index').notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.holder, table.token, table.tokenId] })]);
