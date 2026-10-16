import { sql } from 'drizzle-orm';
import { bigint, boolean, check, index, integer, pgTable, primaryKey, smallint, text } from 'drizzle-orm/pg-core';

import { binaryHex, standard, uint256 } from './columns.ts';

const block = (name: string) => bigint(name, { mode: 'bigint' });
const time = (name: string) => bigint(name, { mode: 'number' });
const address = (name: string) => binaryHex(name, { bytes: 20 });
const hash = (name: string) => binaryHex(name, { bytes: 32 });
const maxUint = sql`115792089237316195423570985008687907853269984665640564039457584007913129639935`;

/** One row per registry network. The anchor is start_block - 1, not an assertion that history was scanned. */
export const ingestionState = pgTable('ingestion_state', {
  network: smallint().primaryKey(),
  startBlock: block('start_block').notNull(),
  blockNumber: block('block_number').notNull(),
  blockHash: hash('block_hash').notNull(),
  blockTimestamp: block('block_timestamp').notNull(),
  updatedAt: time('updated_at').notNull(),
  revision: block('revision').notNull().default(sql`0`),
  headNumber: block('head_number'),
  headHash: hash('head_hash'),
  headTimestamp: block('head_timestamp'),
  checkedAt: time('checked_at'),
}, (t) => [
  check('state_hash_width', sql`octet_length(${t.blockHash}) = 32 AND (${t.headHash} IS NULL OR octet_length(${t.headHash}) = 32)`),
  check('state_progress', sql`${t.startBlock} >= 0 AND ${t.blockNumber} >= ${t.startBlock} - 1 AND ${t.revision} >= 0`),
]);

/** Complete header chain, including empty blocks and the initial anchor. Small relative to transfer volume. */
export const blockHeaders = pgTable('block_headers', {
  network: smallint().notNull(),
  number: block('number').notNull(),
  hash: hash('hash').notNull(),
  parentHash: hash('parent_hash').notNull(),
  timestamp: block('timestamp').notNull(),
}, (t) => [
  primaryKey({ columns: [t.network, t.number] }),
  check('header_hash_width', sql`octet_length(${t.hash}) = 32 AND octet_length(${t.parentHash}) = 32`),
]);

export const transfers = pgTable('transfers', {
  network: smallint().notNull(),
  blockNumber: block('block_number').notNull(),
  logIndex: integer('log_index').notNull(),
  blockHash: hash('block_hash').notNull(),
  transactionHash: hash('transaction_hash').notNull(),
  token: address('token').notNull(),
  standard: standard('standard').notNull(),
  from: address('from').notNull(),
  to: address('to').notNull(),
  /** Amount for ERC-20; token ID for ERC-721. No duplicate token_id column. */
  value: uint256('value').notNull(),
}, (t) => [
  primaryKey({ columns: [t.network, t.blockNumber, t.logIndex] }),
  index('transfers_from_history').on(t.network, t.from, t.blockNumber.desc(), t.logIndex.desc()),
  index('transfers_to_history').on(t.network, t.to, t.blockNumber.desc(), t.logIndex.desc()),
  index('transfers_nft_history').on(t.network, t.token, t.value, t.blockNumber.desc(), t.logIndex.desc()).where(sql`${t.standard} = 721`),
  check('transfer_addresses', sql`octet_length(${t.token}) = 20 AND octet_length(${t.from}) = 20 AND octet_length(${t.to}) = 20`),
  check('transfer_hashes', sql`octet_length(${t.blockHash}) = 32 AND octet_length(${t.transactionHash}) = 32`),
  check('transfer_quantities', sql`${t.blockNumber} >= 0 AND ${t.logIndex} >= 0 AND ${t.value} >= 0 AND ${t.value} <= ${maxUint}`),
  check('transfer_standard', sql`${t.standard} IN (20, 721)`),
]);

/**
 * Unfinalized journal above the cursor: a contiguous parent-hash chain, replaced on reorg and pruned in the same
 * transaction that advances the cursor. Never folded into holdings or nft_ownership.
 */
export const tailBlocks = pgTable('tail_blocks', {
  network: smallint().notNull(),
  number: block('number').notNull(),
  hash: hash('hash').notNull(),
  parentHash: hash('parent_hash').notNull(),
  timestamp: block('timestamp').notNull(),
}, (t) => [
  primaryKey({ columns: [t.network, t.number] }),
  check('tail_block_hash_width', sql`octet_length(${t.hash}) = 32 AND octet_length(${t.parentHash}) = 32`),
]);

/** Every Transfer of the tail blocks, same shape as transfers; bounded by TAIL_MAX_AHEAD_BLOCKS. */
export const tailTransfers = pgTable('tail_transfers', {
  network: smallint().notNull(),
  blockNumber: block('block_number').notNull(),
  logIndex: integer('log_index').notNull(),
  blockHash: hash('block_hash').notNull(),
  transactionHash: hash('transaction_hash').notNull(),
  token: address('token').notNull(),
  standard: standard('standard').notNull(),
  from: address('from').notNull(),
  to: address('to').notNull(),
  value: uint256('value').notNull(),
}, (t) => [
  primaryKey({ columns: [t.network, t.blockNumber, t.logIndex] }),
  index('tail_transfers_from_history').on(t.network, t.from, t.blockNumber.desc(), t.logIndex.desc()),
  index('tail_transfers_to_history').on(t.network, t.to, t.blockNumber.desc(), t.logIndex.desc()),
  check('tail_transfer_addresses', sql`octet_length(${t.token}) = 20 AND octet_length(${t.from}) = 20 AND octet_length(${t.to}) = 20`),
  check('tail_transfer_hashes', sql`octet_length(${t.blockHash}) = 32 AND octet_length(${t.transactionHash}) = 32`),
  check('tail_transfer_quantities', sql`${t.blockNumber} >= 0 AND ${t.logIndex} >= 0 AND ${t.value} >= 0 AND ${t.value} <= ${maxUint}`),
  check('tail_transfer_standard', sql`${t.standard} IN (20, 721)`),
]);

export const holdings = pgTable('holdings', {
  network: smallint().notNull(),
  holder: address('holder').notNull(),
  token: address('token').notNull(),
  standard: standard('standard').notNull(),
  firstBlock: block('first_block').notNull(),
  lastBlock: block('last_block').notNull(),
  sentCount: block('sent_count').notNull(),
  receivedCount: block('received_count').notNull(),
}, (t) => [
  primaryKey({ columns: [t.network, t.holder, t.token] }),
  index('holdings_token_holders').on(t.network, t.token, t.holder),
  check('holding_addresses', sql`octet_length(${t.holder}) = 20 AND octet_length(${t.token}) = 20`),
  check('holding_counts', sql`${t.sentCount} >= 0 AND ${t.receivedCount} >= 0 AND ${t.firstBlock} <= ${t.lastBlock}`),
  check('holding_standard', sql`${t.standard} IN (20, 721)`),
]);

/** One winner per NFT, not one historical held flag per account. */
export const nftOwnership = pgTable('nft_ownership', {
  network: smallint().notNull(),
  token: address('token').notNull(),
  tokenId: uint256('token_id').notNull(),
  owner: address('owner').notNull(),
  lastBlock: block('last_block').notNull(),
  lastLogIndex: integer('last_log_index').notNull(),
}, (t) => [
  primaryKey({ columns: [t.network, t.token, t.tokenId] }),
  index('nft_owner_items').on(t.network, t.owner, t.token, t.tokenId),
  check('nft_addresses', sql`octet_length(${t.owner}) = 20 AND octet_length(${t.token}) = 20`),
  check('nft_id', sql`${t.tokenId} >= 0 AND ${t.tokenId} <= ${maxUint}`),
]);

export const balanceSnapshots = pgTable('balance_snapshots', {
  network: smallint().notNull(),
  account: address('account').notNull(),
  token: address('token').notNull(),
  balance: uint256('balance'),
  error: text(),
  blockNumber: block('block_number').notNull(),
  blockHash: hash('block_hash').notNull(),
  readAt: time('read_at').notNull(),
  /** ingestion_state.revision at write time; reuse requires the current revision. */
  revision: block('revision').notNull().default(sql`0`),
}, (t) => [
  primaryKey({ columns: [t.network, t.account, t.token] }),
  index('balance_blocks').on(t.network, t.blockNumber),
  check('balance_addresses', sql`octet_length(${t.account}) = 20 AND octet_length(${t.token}) = 20 AND octet_length(${t.blockHash}) = 32`),
  check('balance_result', sql`(${t.balance} IS NULL) <> (${t.error} IS NULL)`),
  check('balance_uint', sql`${t.balance} >= 0 AND ${t.balance} <= ${maxUint}`),
]);

/** Pending work only; exact ranges deduplicate. Successful replacement removes fully covered requests. */
export const repairQueue = pgTable('repair_queue', {
  network: smallint().notNull(),
  fromBlock: block('from_block').notNull(),
  toBlock: block('to_block').notNull(),
  reason: text().notNull(),
  createdAt: time('created_at').notNull(),
}, (t) => [
  primaryKey({ columns: [t.network, t.fromBlock, t.toBlock] }),
  check('repair_range', sql`${t.fromBlock} >= 0 AND ${t.toBlock} >= ${t.fromBlock}`),
]);

// ---------------------------------------------------------------------------
// One-time bulk load (src/bootstrap). New segments write directly to the index;
// bootstrap_transfers remains for runs created by the earlier staged loader.
// ---------------------------------------------------------------------------

/** One bootstrap per network: the pinned finalized target B, never moved during the run. */
export const bootstrapRuns = pgTable('bootstrap_runs', {
  network: smallint().primaryKey(),
  startBlock: block('start_block').notNull(),
  targetBlock: block('target_block').notNull(),
  targetHash: hash('target_hash').notNull(),
  /** headers | logs | materializing | publishing | done */
  status: text().notNull(),
  startedAt: time('started_at').notNull(),
  /** Last (re)start of a loader process; progress rates are measured from here. */
  resumedAt: time('resumed_at').notNull(),
  updatedAt: time('updated_at').notNull(),
  error: text(),
}, (t) => [
  check('bootstrap_run_hash_width', sql`octet_length(${t.targetHash}) = 32`),
  check('bootstrap_run_range', sql`${t.startBlock} >= 1 AND ${t.targetBlock} >= ${t.startBlock}`),
]);

/** Unit of fetch, verification, commit and resume. Rows of a segment exist in bootstrap_transfers iff it is done. */
export const bootstrapSegments = pgTable('bootstrap_segments', {
  network: smallint().notNull(),
  fromBlock: block('from_block').notNull(),
  toBlock: block('to_block').notNull(),
  done: boolean().notNull().default(false),
  /** True iff this segment's raw and derived rows committed together. Old staged segments default false. */
  applied: boolean().notNull().default(false),
  source: text(),
  logs: bigint('logs', { mode: 'number' }).notNull().default(0),
  calls: integer().notNull().default(0),
  attempts: integer().notNull().default(0),
  error: text(),
  updatedAt: time('updated_at'),
}, (t) => [
  primaryKey({ columns: [t.network, t.fromBlock] }),
  check('bootstrap_segment_range', sql`${t.toBlock} >= ${t.fromBlock}`),
]);

/** Complete header chain S-1..B; `transfer_bloom` records whether logsBloom may contain the Transfer topic. */
export const bootstrapHeaders = pgTable('bootstrap_headers', {
  network: smallint().notNull(),
  number: block('number').notNull(),
  hash: hash('hash').notNull(),
  parentHash: hash('parent_hash').notNull(),
  timestamp: block('timestamp').notNull(),
  transferBloom: boolean('transfer_bloom').notNull(),
}, (t) => [primaryKey({ columns: [t.network, t.number] })]);

/** Same columns as transfers; no keys, indexes or checks (UNLOGGED, see the custom migration). Finalize enforces them. */
export const bootstrapTransfers = pgTable('bootstrap_transfers', {
  network: smallint().notNull(),
  blockNumber: block('block_number').notNull(),
  logIndex: integer('log_index').notNull(),
  blockHash: hash('block_hash').notNull(),
  transactionHash: hash('transaction_hash').notNull(),
  token: address('token').notNull(),
  standard: standard('standard').notNull(),
  from: address('from').notNull(),
  to: address('to').notNull(),
  value: uint256('value').notNull(),
});
