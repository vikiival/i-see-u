import { and, asc, desc, eq, gt, inArray, lt, lte, or, sql } from 'drizzle-orm';
import type { PgDatabase } from 'drizzle-orm/pg-core';

import type {
  AccountView,
  Address,
  BalancePair,
  BlockRange,
  ChunkAggregate,
  FollowerCursor,
  Hex,
  Store,
  TailBlock,
  BlockHeader,
  TransferEvent,
  Tracking,
} from '../core/types.ts';
import { unavailable } from '../core/limiter.ts';
import * as schema from './schema.ts';

export type StoreDatabase = PgDatabase<any, typeof schema>;
type Executor = Pick<StoreDatabase, 'select' | 'insert' | 'update' | 'delete' | 'execute'>;

/** Rows per multi-row INSERT, far below PostgreSQL's 65,535 bind-parameter limit. */
const INSERT_BATCH = 1_000;

const inBatches = async <T>(rows: T[], fn: (batch: T[]) => Promise<unknown>): Promise<void> => {
  for (let i = 0; i < rows.length; i += INSERT_BATCH) await fn(rows.slice(i, i + INSERT_BATCH));
};

const cursorColumns = {
  number: schema.followerCursor.blockNumber,
  hash: schema.followerCursor.blockHash,
  timestamp: schema.followerCursor.blockTimestamp,
  updatedAt: schema.followerCursor.updatedAt,
};
const toCursor = (row: { number: bigint; hash: string; timestamp: bigint; updatedAt: number }): FollowerCursor =>
  ({ ...row, hash: row.hash as Hex });

const tailColumns = {
  number: schema.tailBlocks.number,
  hash: schema.tailBlocks.hash,
  parentHash: schema.tailBlocks.parentHash,
  timestamp: schema.tailBlocks.timestamp,
};
const toHeader = (row: { number: bigint; hash: string; parentHash: string; timestamp: bigint }): BlockHeader => ({
  ...row,
  hash: row.hash as Hex,
  parentHash: row.parentHash as Hex,
});

export class TailConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TailConflictError';
  }
}

/** Fold per-account aggregates into holdings and NFT items: counts add, bounds widen, the newest NFT transfer wins. */
async function upsertAggregates(tx: Executor, network: string, aggregates: Iterable<[Address, ChunkAggregate]>): Promise<void> {
  const holdingValues = [];
  const nftValues = [];
  for (const [account, chunk] of aggregates) {
    for (const [token, value] of chunk.tokens) {
      holdingValues.push({
        network,
        holder: account,
        token,
        standard: value.standard,
        firstBlock: value.firstBlock,
        lastBlock: value.lastBlock,
        sentCount: value.sentCount,
        receivedCount: value.receivedCount,
      });
    }
    for (const value of chunk.nfts.values()) {
      nftValues.push({
        network,
        holder: account,
        token: value.token,
        tokenId: value.tokenId.toString(),
        held: value.held,
        lastBlock: value.block,
        lastLogIndex: value.logIndex,
      });
    }
  }
  await inBatches(holdingValues, (values) => tx.insert(schema.holdings).values(values).onConflictDoUpdate({
    target: [schema.holdings.network, schema.holdings.holder, schema.holdings.token],
    set: {
      standard: sql`excluded.standard`,
      firstBlock: sql`LEAST(${schema.holdings.firstBlock}, excluded.first_block)`,
      lastBlock: sql`GREATEST(${schema.holdings.lastBlock}, excluded.last_block)`,
      sentCount: sql`${schema.holdings.sentCount} + excluded.sent_count`,
      receivedCount: sql`${schema.holdings.receivedCount} + excluded.received_count`,
    },
  }));
  await inBatches(nftValues, (values) => tx.insert(schema.nftItems).values(values).onConflictDoUpdate({
    target: [schema.nftItems.network, schema.nftItems.holder, schema.nftItems.token, schema.nftItems.tokenId],
    set: {
      held: sql`excluded.held`,
      lastBlock: sql`excluded.last_block`,
      lastLogIndex: sql`excluded.last_log_index`,
    },
    setWhere: sql`excluded.last_block > ${schema.nftItems.lastBlock}
      OR (excluded.last_block = ${schema.nftItems.lastBlock}
        AND excluded.last_log_index > ${schema.nftItems.lastLogIndex})`,
  }));
}

export function createStore(db: StoreDatabase, close: () => Promise<void> = async () => {}): Store {
  const getProgress = async (tx: Executor, network: string, account: Address): Promise<bigint | undefined> => {
    const [row] = await tx.select({ progressTo: schema.accountScanProgress.progressTo })
      .from(schema.accountScanProgress).where(and(
        eq(schema.accountScanProgress.network, network),
        eq(schema.accountScanProgress.account, account),
      )).limit(1);
    return row?.progressTo;
  };

  const getTarget = async (tx: Executor, network: string, account: Address): Promise<bigint | undefined> => {
    const [row] = await tx.select({ target: schema.trackedAccounts.backfillTarget })
      .from(schema.trackedAccounts).where(and(
        eq(schema.trackedAccounts.network, network),
        eq(schema.trackedAccounts.account, account),
      )).limit(1);
    return row?.target;
  };

  const holdings = async (tx: Executor, network: string, account: Address) => {
    const rows = await tx.select({
      token: schema.holdings.token,
      standard: schema.holdings.standard,
      firstBlock: schema.holdings.firstBlock,
      lastBlock: schema.holdings.lastBlock,
      sentCount: schema.holdings.sentCount,
      receivedCount: schema.holdings.receivedCount,
    }).from(schema.holdings).where(and(
      eq(schema.holdings.network, network),
      eq(schema.holdings.holder, account),
    )).orderBy(desc(schema.holdings.lastBlock), asc(schema.holdings.token));
    return rows.map((row) => ({ ...row, token: row.token as Address }));
  };

  const nftItems = async (tx: Executor, network: string, account: Address) => {
    const rows = await tx.select({
      token: schema.nftItems.token,
      tokenId: schema.nftItems.tokenId,
      held: schema.nftItems.held,
      lastBlock: schema.nftItems.lastBlock,
    }).from(schema.nftItems).where(and(
      eq(schema.nftItems.network, network),
      eq(schema.nftItems.holder, account),
    )).orderBy(desc(schema.nftItems.lastBlock), asc(schema.nftItems.token), asc(schema.nftItems.tokenId));
    return rows.map((row) => ({
      token: row.token as Address,
      tokenId: BigInt(row.tokenId),
      heldBySeenLogs: row.held,
      lastBlock: row.lastBlock,
    }));
  };

  const getFollowerCursor = async (tx: Executor, network: string, lock?: 'share' | 'update') => {
    const query = tx.select(cursorColumns).from(schema.followerCursor).where(eq(schema.followerCursor.network, network));
    const [row] = await (lock === 'update' ? query.for('update') : lock === 'share' ? query.for('share') : query);
    return row && toCursor(row);
  };

  const getTailTip = async (tx: Executor, network: string, lock?: 'update') => {
    const query = tx.select(tailColumns).from(schema.tailBlocks)
      .where(eq(schema.tailBlocks.network, network)).orderBy(desc(schema.tailBlocks.number)).limit(1);
    const [row] = await (lock === 'update' ? query.for('update') : query);
    return row && toHeader(row);
  };

  const wipeTail = async (tx: Executor, network: string): Promise<void> => {
    await tx.delete(schema.tailTransfers).where(eq(schema.tailTransfers.network, network));
    await tx.delete(schema.tailBlocks).where(eq(schema.tailBlocks.network, network));
  };

  return {
    getFollowerCursor: (network) => getFollowerCursor(db, network),

    async initFollowerCursor(network, block, at) {
      await db.insert(schema.followerCursor).values({
        network,
        blockNumber: block.number,
        blockHash: block.hash,
        blockTimestamp: block.timestamp,
        updatedAt: at,
      }).onConflictDoNothing();
      return (await getFollowerCursor(db, network))!;
    },

    async trackAccount(network, account, at) {
      return db.transaction(async (tx): Promise<Tracking> => {
        // FOR SHARE conflicts with the follower's FOR UPDATE: its commit is entirely before or after this one.
        const cursor = await getFollowerCursor(tx, network, 'share');
        if (!cursor) throw Object.assign(new Error('Follower has no cursor yet'), { statusCode: 503, retryAfterS: 5 });
        const existing = await getTarget(tx, network, account);
        if (existing !== undefined) return { target: existing, progress: await getProgress(tx, network, account) };

        let progress = await getProgress(tx, network, account);
        if (progress !== undefined && progress > cursor.number) {
          // Legacy scan data above the cursor would overlap the follower's blocks: rebuild the account from scratch.
          const { holdings: h, nftItems: n, accountScanProgress: p } = schema;
          await tx.delete(h).where(and(eq(h.network, network), eq(h.holder, account)));
          await tx.delete(n).where(and(eq(n.network, network), eq(n.holder, account)));
          await tx.delete(p).where(and(eq(p.network, network), eq(p.account, account)));
          progress = undefined;
        }
        const inserted = await tx.insert(schema.trackedAccounts)
          .values({ network, account, backfillTarget: cursor.number, trackedAt: at })
          .onConflictDoNothing().returning({ target: schema.trackedAccounts.backfillTarget });
        if (inserted.length > 0) return { target: cursor.number, progress };
        // A concurrent first request won the insert: serve its target.
        const winner = await getTarget(tx, network, account);
        if (winner === undefined) throw unavailable('Account tracking raced; retry', 1);
        return { target: winner, progress: await getProgress(tx, network, account) };
      });
    },

    async getTracking(network, account) {
      const target = await getTarget(db, network, account);
      return target === undefined ? undefined : { target, progress: await getProgress(db, network, account) };
    },

    async trackedAccounts(network) {
      const rows = await db.select({ account: schema.trackedAccounts.account })
        .from(schema.trackedAccounts).where(eq(schema.trackedAccounts.network, network))
        .orderBy(asc(schema.trackedAccounts.account));
      return rows.map((row) => row.account as Address);
    },

    async applyFollowerRange(network, range, to, at, aggregates, known) {
      return db.transaction(async (tx) => {
        const cursor = await getFollowerCursor(tx, network, 'update');
        if (cursor?.number !== range.fromBlock - 1n || to.number !== range.toBlock) {
          throw new Error(`Follower range ${range.fromBlock}-${range.toBlock} does not continue cursor ${cursor?.number}`);
        }
        // Accounts tracked while the range was being decoded are not in `known`: the follower re-folds its retained logs for them and retries.
        const tracked = await tx.select({ account: schema.trackedAccounts.account })
          .from(schema.trackedAccounts).where(eq(schema.trackedAccounts.network, network));
        const missing = tracked.map((row) => row.account as Address).filter((account) => !known.has(account));
        if (missing.length > 0) return { applied: false as const, missing };

        await upsertAggregates(tx, network, aggregates);
        const [overlap] = await tx.select({ hash: schema.tailBlocks.hash }).from(schema.tailBlocks).where(and(
          eq(schema.tailBlocks.network, network), eq(schema.tailBlocks.number, to.number),
        )).limit(1);
        if (overlap && overlap.hash !== to.hash) await wipeTail(tx, network);
        else {
          await tx.delete(schema.tailTransfers).where(and(
            eq(schema.tailTransfers.network, network), lte(schema.tailTransfers.blockNumber, to.number),
          ));
          await tx.delete(schema.tailBlocks).where(and(
            eq(schema.tailBlocks.network, network), lte(schema.tailBlocks.number, to.number),
          ));
        }
        await tx.update(schema.followerCursor).set({
          blockNumber: to.number,
          blockHash: to.hash,
          blockTimestamp: to.timestamp,
          updatedAt: at,
        }).where(eq(schema.followerCursor.network, network));
        return { applied: true as const };
      });
    },

    async getTailBlocks(network) {
      const rows = await db.select(tailColumns).from(schema.tailBlocks)
        .where(eq(schema.tailBlocks.network, network)).orderBy(asc(schema.tailBlocks.number));
      return rows.map(toHeader);
    },

    getTailTip: (network) => getTailTip(db, network),

    async appendTailBlocks(network, expectedParentHash, blocks) {
      if (blocks.length === 0) return;
      await db.transaction(async (tx) => {
        // The cursor lock serializes an empty-tail anchor against finalization/pruning.
        const cursor = await getFollowerCursor(tx, network, 'share');
        if (!cursor) throw new TailConflictError('Follower has no cursor for tail anchor');
        const tip = await getTailTip(tx, network, 'update');
        const anchorHash = tip?.hash ?? cursor.hash;
        const anchorNumber = tip?.number ?? cursor.number;
        if (anchorHash !== expectedParentHash) {
          throw new TailConflictError(`Tail parent changed from ${expectedParentHash} to ${anchorHash}`);
        }
        let parentHash = anchorHash;
        let number = anchorNumber + 1n;
        for (const block of blocks) {
          if (block.header.number !== number || block.header.parentHash !== parentHash) {
            throw new TailConflictError(`Non-contiguous tail block ${block.header.number}`);
          }
          for (const transfer of block.transfers) {
            if (transfer.blockNumber !== block.header.number || transfer.blockHash !== block.header.hash) {
              throw new TailConflictError(`Transfer does not belong to tail block ${block.header.number}`);
            }
          }
          parentHash = block.header.hash;
          number += 1n;
        }
        await inBatches(blocks, (values) => tx.insert(schema.tailBlocks).values(values.map(({ header }) => ({
          network,
          number: header.number,
          hash: header.hash,
          parentHash: header.parentHash,
          timestamp: header.timestamp,
        }))));
        const transfers = blocks.flatMap((block) => block.transfers.map((event) => ({
          network,
          blockNumber: event.blockNumber,
          logIndex: event.logIndex,
          blockHash: event.blockHash,
          token: event.token,
          standard: event.standard,
          from: event.from,
          to: event.to,
          value: event.value.toString(),
          tokenId: event.standard === 'erc721' ? event.value.toString() : null,
          txHash: event.transactionHash,
        })));
        await inBatches(transfers, (values) => tx.insert(schema.tailTransfers).values(values));
      });
    },

    async rewindTail(network, aboveNumber, expectedTipHash) {
      await db.transaction(async (tx) => {
        // Fenced like appendTailBlocks: a concurrent tail writer (failover) must not lose rows it just appended.
        const tip = await getTailTip(tx, network, 'update');
        if (tip?.hash !== expectedTipHash) {
          throw new TailConflictError(`Tail tip changed from ${expectedTipHash} to ${tip?.hash}`);
        }
        await tx.delete(schema.tailTransfers).where(and(
          eq(schema.tailTransfers.network, network), gt(schema.tailTransfers.blockNumber, aboveNumber),
        ));
        await tx.delete(schema.tailBlocks).where(and(
          eq(schema.tailBlocks.network, network), gt(schema.tailBlocks.number, aboveNumber),
        ));
      });
    },

    async wipeTail(network, expectedTipHash) {
      await db.transaction(async (tx) => {
        const tip = await getTailTip(tx, network, 'update');
        if (expectedTipHash !== undefined && tip?.hash !== expectedTipHash) {
          throw new TailConflictError(`Tail tip changed from ${expectedTipHash} to ${tip?.hash}`);
        }
        await wipeTail(tx, network);
      });
    },

    getProgress: (network, account) => getProgress(db, network, account),

    async applyScanChunk(network, account, range, chunk) {
      await db.transaction(async (tx) => {
        const target = await getTarget(tx, network, account);
        if (target !== undefined && range.toBlock > target) {
          throw new Error(`Chunk ${range.fromBlock}-${range.toBlock} is beyond backfill target ${target}`);
        }
        const claimed = await tx.insert(schema.accountScanProgress).values({
          network,
          account,
          progressTo: range.toBlock,
        }).onConflictDoUpdate({
          target: [schema.accountScanProgress.network, schema.accountScanProgress.account],
          set: { progressTo: range.toBlock },
          setWhere: lt(schema.accountScanProgress.progressTo, range.fromBlock),
        }).returning({ progressTo: schema.accountScanProgress.progressTo });

        if (claimed.length === 0) {
          const appliedTo = await getProgress(tx, network, account) ?? -1n;
          if (range.toBlock <= appliedTo) return;
          throw new Error(`Chunk ${range.fromBlock}-${range.toBlock} overlaps applied progress ${appliedTo}`);
        }
        await upsertAggregates(tx, network, [[account, chunk]]);
      });
    },

    async getLogsRange(network, account) {
      const [row] = await db.select({ logsRange: schema.accountScanRange.logsRange })
        .from(schema.accountScanRange).where(and(
          eq(schema.accountScanRange.network, network),
          eq(schema.accountScanRange.account, account),
        )).limit(1);
      return row?.logsRange;
    },

    async setLogsRange(network, account, range) {
      await db.insert(schema.accountScanRange).values({ network, account, logsRange: range }).onConflictDoUpdate({
        target: [schema.accountScanRange.network, schema.accountScanRange.account],
        set: { logsRange: range },
      });
    },

    async readAccount(network, account) {
      return db.transaction(async (tx): Promise<AccountView | undefined> => {
        const cursor = await getFollowerCursor(tx, network);
        if (!cursor) return undefined;
        const rows = await tx.select({
          standard: schema.tailTransfers.standard,
          token: schema.tailTransfers.token,
          from: schema.tailTransfers.from,
          to: schema.tailTransfers.to,
          value: schema.tailTransfers.value,
          blockNumber: schema.tailTransfers.blockNumber,
          blockHash: schema.tailTransfers.blockHash,
          logIndex: schema.tailTransfers.logIndex,
          transactionHash: schema.tailTransfers.txHash,
        }).from(schema.tailTransfers).where(and(
          eq(schema.tailTransfers.network, network),
          gt(schema.tailTransfers.blockNumber, cursor.number),
          or(eq(schema.tailTransfers.from, account), eq(schema.tailTransfers.to, account)),
        )).orderBy(asc(schema.tailTransfers.blockNumber), asc(schema.tailTransfers.logIndex));
        const tailTransfers: TransferEvent[] = rows.map((row) => ({
          ...row,
          token: row.token as Address,
          from: row.from as Address,
          to: row.to as Address,
          value: BigInt(row.value),
          blockHash: row.blockHash as Hex,
          transactionHash: row.transactionHash as Hex,
        }));
        return {
          cursor,
          holdings: await holdings(tx, network, account),
          nftItems: await nftItems(tx, network, account),
          tailTransfers,
          tailTip: await getTailTip(tx, network),
        };
      }, { isolationLevel: 'repeatable read', accessMode: 'read only' });
    },

    holdings: (network, account) => holdings(db, network, account),
    nftItems: (network, account) => nftItems(db, network, account),

    async erc20Pairs(network, accounts) {
      const pairs: BalancePair[] = [];
      const seen = new Set<string>();
      await inBatches(accounts, async (batch) => {
        const members = new Set<string>(batch);
        const rows = await db.select({ account: schema.holdings.holder, token: schema.holdings.token })
          .from(schema.holdings).where(and(
            eq(schema.holdings.network, network),
            eq(schema.holdings.standard, 'erc20'),
            inArray(schema.holdings.holder, batch),
          )).orderBy(asc(schema.holdings.holder), asc(schema.holdings.token));
        for (const row of rows) {
          const key = `${row.account}:${row.token}`;
          if (!seen.has(key)) pairs.push({ account: row.account as Address, token: row.token as Address });
          seen.add(key);
        }
        const tail = await db.select({
          from: schema.tailTransfers.from,
          to: schema.tailTransfers.to,
          token: schema.tailTransfers.token,
        }).from(schema.tailTransfers).where(and(
          eq(schema.tailTransfers.network, network),
          eq(schema.tailTransfers.standard, 'erc20'),
          or(inArray(schema.tailTransfers.from, batch), inArray(schema.tailTransfers.to, batch)),
        ));
        for (const row of tail) for (const account of [row.from, row.to]) {
          if (!members.has(account)) continue;
          const key = `${account}:${row.token}`;
          if (!seen.has(key)) pairs.push({ account: account as Address, token: row.token as Address });
          seen.add(key);
        }
      });
      return pairs;
    },

    async saveBalances(network, block, rows, opts) {
      await inBatches(rows, (batch) => db.insert(schema.balanceSnapshots).values(batch.map((row) => ({
        network,
        account: row.account,
        token: row.token,
        balance: row.result.status === 'ok' ? row.result.balance.toString() : null,
        error: row.result.status === 'error' ? row.result.error : null,
        blockNumber: block.number,
        blockHash: block.hash,
      }))).onConflictDoUpdate({
        target: [schema.balanceSnapshots.network, schema.balanceSnapshots.account, schema.balanceSnapshots.token],
        set: {
          balance: sql`excluded.balance`,
          error: sql`excluded.error`,
          blockNumber: sql`excluded.block_number`,
          blockHash: sql`excluded.block_hash`,
        },
        // replaceNewer (only the reorg-kicked refresh at the canonical tip) overwrites unconditionally, so snapshots of
        // orphaned blocks, newer or at the same height, are replaced. Otherwise a snapshot only moves to a later block,
        // or is re-read at the same block: a same-height read of another (orphaned) hash never replaces it.
        ...(!opts?.replaceNewer && {
          setWhere: sql`excluded.block_number > ${schema.balanceSnapshots.blockNumber}
            OR (excluded.block_number = ${schema.balanceSnapshots.blockNumber}
              AND excluded.block_hash = ${schema.balanceSnapshots.blockHash})`,
        }),
      }));
    },

    async balances(network, account) {
      const rows = await db.select({
        token: schema.balanceSnapshots.token,
        balance: schema.balanceSnapshots.balance,
        error: schema.balanceSnapshots.error,
        blockNumber: schema.balanceSnapshots.blockNumber,
        blockHash: schema.balanceSnapshots.blockHash,
      }).from(schema.balanceSnapshots).where(and(
        eq(schema.balanceSnapshots.network, network),
        eq(schema.balanceSnapshots.account, account),
      ));
      return rows.map((row) => ({
        token: row.token as Address,
        result: row.balance !== null
          ? { status: 'ok' as const, balance: BigInt(row.balance) }
          : { status: 'error' as const, error: row.error ?? 'unknown error' },
        blockNumber: row.blockNumber,
        blockHash: row.blockHash as Hex,
      }));
    },

    close,
  };
}
