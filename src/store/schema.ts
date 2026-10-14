import { bigint, boolean, integer, pgTable, primaryKey, text } from 'drizzle-orm/pg-core';

import type { TokenStandard } from '../core/types.ts';

export const accountCursor = pgTable('account_cursor', {
  network: text().notNull(),
  account: text().notNull(),
  scannedTo: bigint('scanned_to', { mode: 'bigint' }).notNull(),
  scannedToHash: text('scanned_to_hash').notNull(),
  scannedToTimestamp: bigint('scanned_to_timestamp', { mode: 'bigint' }).notNull(),
}, (table) => [primaryKey({ columns: [table.network, table.account] })]);

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
