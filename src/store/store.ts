import { and, asc, desc, eq, lt, sql } from 'drizzle-orm';
import type { PgDatabase } from 'drizzle-orm/pg-core';

import type { Address, BlockRange, BlockRef, ChunkAggregate, Hex, Store } from '../core/types.ts';
import * as schema from './schema.ts';

export type StoreDatabase = PgDatabase<any, typeof schema>;

export function createStore(db: StoreDatabase, close: () => Promise<void> = async () => {}): Store {
  return {
    async getCursor(network, account) {
      const [row] = await db.select({
        number: schema.accountCursor.scannedTo,
        hash: schema.accountCursor.scannedToHash,
        timestamp: schema.accountCursor.scannedToTimestamp,
      }).from(schema.accountCursor).where(and(
        eq(schema.accountCursor.network, network),
        eq(schema.accountCursor.account, account),
      )).limit(1);
      return row && { number: row.number, hash: row.hash as Hex, timestamp: row.timestamp };
    },

    async getProgress(network, account) {
      const [row] = await db.select({ progressTo: schema.accountScanProgress.progressTo })
        .from(schema.accountScanProgress).where(and(
          eq(schema.accountScanProgress.network, network),
          eq(schema.accountScanProgress.account, account),
        )).limit(1);
      return row?.progressTo;
    },

    async applyScanChunk(network: string, account: Address, range: BlockRange, chunk: ChunkAggregate): Promise<void> {
      await db.transaction(async (tx) => {
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
          const [row] = await tx.select({ progressTo: schema.accountScanProgress.progressTo })
            .from(schema.accountScanProgress).where(and(
              eq(schema.accountScanProgress.network, network),
              eq(schema.accountScanProgress.account, account),
            )).limit(1);
          const appliedTo = row?.progressTo ?? -1n;
          if (range.toBlock <= appliedTo) return;
          throw new Error(`Chunk ${range.fromBlock}-${range.toBlock} overlaps applied progress ${appliedTo}`);
        }

        const holdingValues = [...chunk.tokens].map(([token, value]) => ({
          network,
          holder: account,
          token,
          standard: value.standard,
          firstBlock: value.firstBlock,
          lastBlock: value.lastBlock,
          sentCount: value.sentCount,
          receivedCount: value.receivedCount,
        }));
        if (holdingValues.length > 0) {
          await tx.insert(schema.holdings).values(holdingValues).onConflictDoUpdate({
            target: [schema.holdings.network, schema.holdings.holder, schema.holdings.token],
            set: {
              standard: sql`excluded.standard`,
              firstBlock: sql`LEAST(${schema.holdings.firstBlock}, excluded.first_block)`,
              lastBlock: sql`GREATEST(${schema.holdings.lastBlock}, excluded.last_block)`,
              sentCount: sql`${schema.holdings.sentCount} + excluded.sent_count`,
              receivedCount: sql`${schema.holdings.receivedCount} + excluded.received_count`,
            },
          });
        }

        const nftValues = [...chunk.nfts.values()].map((value) => ({
          network,
          holder: account,
          token: value.token,
          tokenId: value.tokenId.toString(),
          held: value.held,
          lastBlock: value.block,
          lastLogIndex: value.logIndex,
        }));
        if (nftValues.length > 0) {
          await tx.insert(schema.nftItems).values(nftValues).onConflictDoUpdate({
            target: [schema.nftItems.network, schema.nftItems.holder, schema.nftItems.token, schema.nftItems.tokenId],
            set: {
              held: sql`excluded.held`,
              lastBlock: sql`excluded.last_block`,
              lastLogIndex: sql`excluded.last_log_index`,
            },
            setWhere: sql`excluded.last_block > ${schema.nftItems.lastBlock}
              OR (excluded.last_block = ${schema.nftItems.lastBlock}
                AND excluded.last_log_index > ${schema.nftItems.lastLogIndex})`,
          });
        }
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

    async finalizeScan(network: string, account: Address, scannedTo: BlockRef): Promise<void> {
      await db.transaction(async (tx) => {
        await tx.insert(schema.accountScanProgress).values({
          network,
          account,
          progressTo: scannedTo.number,
        }).onConflictDoUpdate({
          target: [schema.accountScanProgress.network, schema.accountScanProgress.account],
          set: { progressTo: scannedTo.number },
          setWhere: lt(schema.accountScanProgress.progressTo, scannedTo.number),
        });
        await tx.insert(schema.accountCursor).values({
          network,
          account,
          scannedTo: scannedTo.number,
          scannedToHash: scannedTo.hash,
          scannedToTimestamp: scannedTo.timestamp,
        }).onConflictDoUpdate({
          target: [schema.accountCursor.network, schema.accountCursor.account],
          set: {
            scannedTo: scannedTo.number,
            scannedToHash: scannedTo.hash,
            scannedToTimestamp: scannedTo.timestamp,
          },
          setWhere: lt(schema.accountCursor.scannedTo, scannedTo.number),
        });
      });
    },

    async holdings(network, account) {
      const rows = await db.select({
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
    },

    async nftItems(network, account) {
      const rows = await db.select({
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
    },

    close,
  };
}
