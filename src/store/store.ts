import { and, asc, desc, eq, inArray, lt, sql } from 'drizzle-orm';
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
  Tracking,
} from '../core/types.ts';
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
        await tx.insert(schema.trackedAccounts).values({ network, account, backfillTarget: cursor.number, trackedAt: at });
        return { target: cursor.number, progress };
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
        // Accounts tracked while the range was being decoded are not in `known`: refetch rather than miss their logs.
        const tracked = await tx.select({ account: schema.trackedAccounts.account })
          .from(schema.trackedAccounts).where(eq(schema.trackedAccounts.network, network));
        const missing = tracked.map((row) => row.account as Address).filter((account) => !known.has(account));
        if (missing.length > 0) return { applied: false as const, missing };

        await upsertAggregates(tx, network, aggregates);
        await tx.update(schema.followerCursor).set({
          blockNumber: to.number,
          blockHash: to.hash,
          blockTimestamp: to.timestamp,
          updatedAt: at,
        }).where(eq(schema.followerCursor.network, network));
        return { applied: true as const };
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
        return { cursor, holdings: await holdings(tx, network, account), nftItems: await nftItems(tx, network, account) };
      }, { isolationLevel: 'repeatable read', accessMode: 'read only' });
    },

    holdings: (network, account) => holdings(db, network, account),
    nftItems: (network, account) => nftItems(db, network, account),

    async erc20Pairs(network, accounts) {
      const pairs: BalancePair[] = [];
      await inBatches(accounts, async (batch) => {
        const rows = await db.select({ account: schema.holdings.holder, token: schema.holdings.token })
          .from(schema.holdings).where(and(
            eq(schema.holdings.network, network),
            eq(schema.holdings.standard, 'erc20'),
            inArray(schema.holdings.holder, batch),
          )).orderBy(asc(schema.holdings.holder), asc(schema.holdings.token));
        for (const row of rows) pairs.push({ account: row.account as Address, token: row.token as Address });
      });
      return pairs;
    },

    async saveBalances(network, block, rows) {
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
        setWhere: sql`excluded.block_number >= ${schema.balanceSnapshots.blockNumber}`,
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
