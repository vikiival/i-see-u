import { and, asc, desc, eq, getTableColumns, gt, gte, lte, or, sql } from 'drizzle-orm';
import type { SQL } from 'drizzle-orm';
import type { PgDatabase } from 'drizzle-orm/pg-core';

import type { Address, BlockHeader, BlockRange, HistoryTransfer, IndexState, Store, TransferEvent } from '../core/types.ts';
import { ChainGapError, TailConflictError } from '../core/errors.ts';
import { networkRegistry } from '../networks.ts';
import * as schema from './schema.ts';

export type StoreDatabase = PgDatabase<any, typeof schema>;

export type Executor = Pick<StoreDatabase, 'select' | 'insert' | 'update' | 'delete' | 'execute'>;

const {
  ingestionState: state, blockHeaders: headers, transfers: raw, holdings: h, nftOwnership: nft, balanceSnapshots: cache, repairQueue: repairs,
  tailBlocks: tailHeaders, tailTransfers: tailRaw,
} = schema;

const INSERT_BATCH = 1_000;
const ZERO: Address = `0x${'0'.repeat(40)}`;

const batches = async <T, R>(rows: T[], insert: (batch: T[]) => PromiseLike<R>): Promise<void> => {
  for (let i = 0; i < rows.length; i += INSERT_BATCH) await insert(rows.slice(i, i + INSERT_BATCH));
};

/** Checks the complete range even when it contains no transfers. Duplicate identities must carry identical data. */
function validateRange(range: BlockRange, blocks: BlockHeader[], events: TransferEvent[]): TransferEvent[] {
  if (range.fromBlock < 0n || range.toBlock < range.fromBlock || BigInt(blocks.length) !== range.toBlock - range.fromBlock + 1n) {
    throw new ChainGapError('Range must contain every block header');
  }

  const byBlock = new Map<bigint, BlockHeader>();

  for (const [i, block] of blocks.entries()) {
    if (block.number !== range.fromBlock + BigInt(i) || (i > 0 && block.parentHash !== blocks[i - 1].hash)) {
      throw new ChainGapError(`Header chain gap at ${block.number}`);
    }

    byBlock.set(block.number, block);
  }

  const unique = new Map<string, TransferEvent>();

  for (const event of events) {
    if (byBlock.get(event.blockNumber)?.hash !== event.blockHash) throw new ChainGapError('Transfer does not match range header');
    const key = `${event.blockNumber}:${event.logIndex}`;
    const previous = unique.get(key);

    if (previous && (previous.blockHash !== event.blockHash || previous.transactionHash !== event.transactionHash
      || previous.token !== event.token || previous.from !== event.from || previous.to !== event.to
      || previous.standard !== event.standard || previous.value !== event.value)) throw new Error(`Conflicting transfer ${key}`);
    unique.set(key, event);
  }

  return [...unique.values()];
}

const newestFirst = (a: TransferEvent, b: TransferEvent) => a.blockNumber === b.blockNumber ? b.logIndex - a.logIndex : a.blockNumber > b.blockNumber ? -1 : 1;

/** A snapshot's block is canonical while it is the finalized header or the current tail block at its height. */
const canonicalSnapshot = sql`(EXISTS (SELECT 1 FROM block_headers c WHERE c.network = ${cache.network} AND c.number = ${cache.blockNumber} AND c.hash = ${cache.blockHash})
  OR EXISTS (SELECT 1 FROM tail_blocks c WHERE c.network = ${cache.network} AND c.number = ${cache.blockNumber} AND c.hash = ${cache.blockHash}))`;

const summarySelect = (source: SQL) => sql`
  SELECT network, holder, token, max(standard), min(block_number), max(block_number),
    sum(sent)::bigint, sum(received)::bigint
  FROM (${source}) movements GROUP BY network, holder, token`;

export const applySummaries = (tx: Executor, source: SQL) => tx.execute(sql`
  INSERT INTO holdings (network, holder, token, standard, first_block, last_block, sent_count, received_count)
  ${summarySelect(source)}
  ON CONFLICT (network, holder, token) DO UPDATE SET
    standard = greatest(holdings.standard, excluded.standard),
    first_block = least(holdings.first_block, excluded.first_block),
    last_block = greatest(holdings.last_block, excluded.last_block),
    sent_count = holdings.sent_count + excluded.sent_count,
    received_count = holdings.received_count + excluded.received_count`);

export const applyNfts = (tx: Executor, source: SQL) => tx.execute(sql`
  INSERT INTO nft_ownership (network, token, token_id, owner, last_block, last_log_index)
  SELECT DISTINCT ON (network, token, value) network, token, value, "to", block_number, log_index
  FROM (${source}) events ORDER BY network, token, value, block_number DESC, log_index DESC
  ON CONFLICT (network, token, token_id) DO UPDATE SET
    owner = excluded.owner, last_block = excluded.last_block, last_log_index = excluded.last_log_index
  WHERE (excluded.last_block, excluded.last_log_index) > (nft_ownership.last_block, nft_ownership.last_log_index)`);

/** Registry names are converted once here; every SQL key uses a stable smallint, never the EVM chain ID. */
export function createStore(
  db: StoreDatabase,
  close: () => Promise<void> = async () => {},
  networks: ReadonlyMap<string, number> = new Map(Object.values(networkRegistry).map((n) => [n.name, n.id])),
): Store {
  const id = (network: string): number => {
    const value = networks.get(network);

    if (value === undefined || !Number.isInteger(value) || value < 1 || value > 32767) throw new Error(`Unknown database network: ${network}`);

    return value;
  };

  if (new Set(networks.values()).size !== networks.size) throw new Error('Database network IDs must be unique');

  const readState = async (tx: Executor, network: number, lock?: 'update' | 'share'): Promise<IndexState | undefined> => {
    const query = tx.select().from(state).where(eq(state.network, network));
    const [row] = await (lock ? query.for(lock) : query);

    if (!row) return undefined;
    const [repair] = await tx.select({ from: repairs.fromBlock }).from(repairs).where(eq(repairs.network, network)).limit(1);

    return {
      startBlock: row.startBlock,
      cursor: { number: row.blockNumber, hash: row.blockHash, timestamp: row.blockTimestamp, updatedAt: row.updatedAt, revision: row.revision },
      head: row.headNumber === null ? undefined : { number: row.headNumber, hash: row.headHash!, timestamp: row.headTimestamp! },
      checkedAt: row.checkedAt ?? undefined,
      pendingRepair: repair !== undefined,
    };
  };

  const headerAt = async (tx: Executor, network: number, number: bigint) => {
    const [row] = await tx.select().from(headers).where(and(eq(headers.network, network), eq(headers.number, number)));

    return row;
  };

  const holdings = (tx: Executor, network: number, account: Address) => tx.select({
    token: h.token, standard: h.standard, firstBlock: h.firstBlock, lastBlock: h.lastBlock, sentCount: h.sentCount, receivedCount: h.receivedCount,
  }).from(h).where(and(eq(h.network, network), eq(h.holder, account))).orderBy(desc(h.lastBlock), asc(h.token));

  const nftItems = async (tx: Executor, network: number, account: Address) => {
    if (account === ZERO) return [];

    const rows = await tx.select({ token: nft.token, tokenId: nft.tokenId, lastBlock: nft.lastBlock })
      .from(nft).where(and(eq(nft.network, network), eq(nft.owner, account))).orderBy(asc(nft.token), asc(nft.tokenId));

    return rows.map((row) => ({ ...row, heldBySeenLogs: true }));
  };

  const balances = async (tx: Executor, network: number, account: Address) => {
    const rows = await tx.select({ ...getTableColumns(cache), canonical: canonicalSnapshot.mapWith(Boolean) })
      .from(cache).where(and(eq(cache.network, network), eq(cache.account, account)));

    return rows.map((row) => ({
      token: row.token,
      result: row.balance === null ? { status: 'error' as const, error: row.error! } : { status: 'ok' as const, balance: row.balance },
      blockNumber: row.blockNumber, blockHash: row.blockHash, readAt: row.readAt, revision: row.revision, canonical: row.canonical,
    }));
  };

  const tailTip = async (tx: Executor, network: number, lock?: 'update'): Promise<BlockHeader | undefined> => {
    const query = tx.select({ number: tailHeaders.number, hash: tailHeaders.hash, parentHash: tailHeaders.parentHash, timestamp: tailHeaders.timestamp })
      .from(tailHeaders).where(eq(tailHeaders.network, network)).orderBy(desc(tailHeaders.number)).limit(1);

    const [row] = await (lock ? query.for(lock) : query);

    return row;
  };

  const wipeTail = async (tx: Executor, network: number) => {
    await tx.delete(tailRaw).where(eq(tailRaw.network, network));
    await tx.delete(tailHeaders).where(eq(tailHeaders.network, network));
  };

  /** Fence a tail rewind/wipe on the tip the caller observed: a concurrent writer (failover) must not lose rows. */
  const fenceTip = async (tx: Executor, network: number, expected: string | undefined) => {
    const tip = await tailTip(tx, network, 'update');

    if (expected !== undefined && tip?.hash !== expected) throw new TailConflictError(`Tail tip changed from ${expected} to ${tip?.hash}`);
  };

  const insertRange = async (tx: Executor, network: number, blocks: BlockHeader[], events: TransferEvent[]) => {
    await batches(blocks, (batch) => tx.insert(headers).values(batch.map((block) => ({ network, ...block }))));
    await batches(events, (batch) => tx.insert(raw).values(batch.map((event) => ({ network, ...event }))));
  };

  const rangeCondition = (network: number, range: BlockRange) => and(eq(raw.network, network), gte(raw.blockNumber, range.fromBlock), lte(raw.blockNumber, range.toBlock))!;
  const rangeSql = (network: number, range: BlockRange) => sql`network = ${network} AND block_number BETWEEN ${range.fromBlock} AND ${range.toBlock}`;

  const checkLimit = (limit: number) => {
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) throw new Error('Limit must be between 1 and 200');
  };

  // Every affected account is scanned once per direction, not once per (account, token) pair.
  const rebuildTouched = async (tx: Executor, n: number) => {
    await tx.execute(sql`DELETE FROM holdings h USING repair_accounts a WHERE h.network = ${n} AND h.holder = a.holder`);
    // Indexed lateral lookups avoid scanning the global history for a small repair.
    await applySummaries(tx, sql`
      SELECT t.network, p.holder, t.token, t.standard, t.block_number, 1 AS sent, 0 AS received
      FROM repair_accounts p CROSS JOIN LATERAL (SELECT * FROM transfers WHERE network = ${n} AND "from" = p.holder) t
      UNION ALL
      SELECT t.network, p.holder, t.token, t.standard, t.block_number, 0 AS sent, 1 AS received
      FROM repair_accounts p CROSS JOIN LATERAL (SELECT * FROM transfers WHERE network = ${n} AND "to" = p.holder) t`);
    await tx.execute(sql`DELETE FROM nft_ownership o USING repair_nfts p WHERE o.network = ${n} AND o.token = p.token AND o.token_id = p.token_id`);
    await applyNfts(tx, sql`SELECT t.* FROM repair_nfts p CROSS JOIN LATERAL (
      SELECT * FROM transfers WHERE network = ${n} AND standard = 721 AND token = p.token AND value = p.token_id
      ORDER BY block_number DESC, log_index DESC LIMIT 1) t`);
    await tx.execute(sql`DELETE FROM balance_snapshots b USING repair_accounts p WHERE b.network = ${n} AND b.account = p.holder`);
  };

  return {
    async initialize(network, startBlock, anchor, at) {
      if (anchor.number !== startBlock - 1n) throw new Error('Anchor must be immediately before startBlock');
      const n = id(network);

      return db.transaction(async (tx) => {
        await tx.insert(state).values({ network: n, startBlock, blockNumber: anchor.number, blockHash: anchor.hash, blockTimestamp: anchor.timestamp, updatedAt: at }).onConflictDoNothing();
        const current = (await readState(tx, n, 'update'))!;

        if (current.startBlock !== startBlock) throw new Error('Network start block changed');
        const existing = await headerAt(tx, n, anchor.number);

        if (existing && existing.hash !== anchor.hash) throw new ChainGapError('Anchor hash changed');
        await tx.insert(headers).values({ network: n, ...anchor }).onConflictDoNothing();

        return current;
      });
    },

    indexState: (network) => db.transaction((tx) => readState(tx, id(network)), { isolationLevel: 'repeatable read', accessMode: 'read only' }),

    async observeHead(network, head, at) {
      const n = id(network);
      await db.transaction(async (tx) => {
        const current = await readState(tx, n, 'update');

        if (!current) throw new Error('Index is not initialized');
        const previous = current.head;

        if (previous && head.number < previous.number) return;
        await tx.update(state).set({ headNumber: head.number, headHash: head.hash, headTimestamp: head.timestamp, checkedAt: at }).where(eq(state.network, n));
      });
    },

    async appendRange(network, range, blocks, events, at) {
      const unique = validateRange(range, blocks, events);
      const n = id(network);

      return db.transaction(async (tx) => {
        const current = await readState(tx, n, 'update');

        if (!current) throw new Error('Index is not initialized');

        if (range.toBlock <= current.cursor.number) {
          const stored = await tx.select().from(headers).where(and(eq(headers.network, n), gte(headers.number, range.fromBlock), lte(headers.number, range.toBlock))).orderBy(asc(headers.number));

          if (stored.length !== blocks.length || stored.some((block, i) => block.hash !== blocks[i].hash || block.parentHash !== blocks[i].parentHash)) {
            throw new ChainGapError('Committed range differs; use replaceRange');
          }

          return false;
        }

        if (range.fromBlock !== current.cursor.number + 1n) throw new ChainGapError('Range does not continue cursor');

        if (blocks[0].parentHash !== current.cursor.hash) throw new ChainGapError('Next range parent hash does not match cursor');

        if (!current.head || range.toBlock > current.head.number) throw new Error('Range exceeds observed finalized head');
        const last = blocks.at(-1)!;

        if (last.number === current.head.number && last.hash !== current.head.hash) throw new ChainGapError('Range does not match finalized head');
        await insertRange(tx, n, blocks, unique);
        const condition = rangeSql(n, range);
        await applySummaries(tx, sql`
          SELECT network, "from" AS holder, token, standard, block_number, 1 AS sent, 0 AS received FROM transfers WHERE ${condition}
          UNION ALL
          SELECT network, "to" AS holder, token, standard, block_number, 0 AS sent, 1 AS received FROM transfers WHERE ${condition}`);
        await applyNfts(tx, sql`SELECT * FROM transfers WHERE ${condition} AND standard = 721`);
        await tx.update(state).set({ blockNumber: last.number, blockHash: last.hash, blockTimestamp: last.timestamp, updatedAt: at }).where(eq(state.network, n));
        // Finalized rows leave the tail in the same transaction; a different tail hash at the new cursor was a fork.
        const [overlap] = await tx.select({ hash: tailHeaders.hash }).from(tailHeaders).where(and(eq(tailHeaders.network, n), eq(tailHeaders.number, last.number)));

        if (overlap && overlap.hash !== last.hash) await wipeTail(tx, n);
        else {
          await tx.delete(tailRaw).where(and(eq(tailRaw.network, n), lte(tailRaw.blockNumber, last.number)));
          await tx.delete(tailHeaders).where(and(eq(tailHeaders.network, n), lte(tailHeaders.number, last.number)));
        }

        await tx.delete(repairs).where(and(eq(repairs.network, n), gte(repairs.fromBlock, range.fromBlock), lte(repairs.toBlock, range.toBlock)));

        return true;
      });
    },

    async replaceRange(network, range, blocks, events) {
      const unique = validateRange(range, blocks, events);
      const n = id(network);
      await db.transaction(async (tx) => {
        const current = await readState(tx, n, 'update');

        if (!current || range.fromBlock < current.startBlock || range.toBlock > current.cursor.number) throw new Error('Repair must be inside the committed index');
        const previous = await headerAt(tx, n, range.fromBlock - 1n);
        const next = await headerAt(tx, n, range.toBlock + 1n);
        const last = blocks.at(-1)!;

        if (previous?.hash !== blocks[0].parentHash || (next && next.parentHash !== last.hash)) throw new ChainGapError('Repair does not join surrounding header chain');

        if (last.number === current.head?.number && last.hash !== current.head.hash) throw new ChainGapError('Repair does not match finalized head');

        // Capture both removed and replacement endpoints. An account/token that disappears must also be rebuilt.
        await tx.execute(sql`CREATE TEMP TABLE repair_pairs (holder bytea, token bytea, PRIMARY KEY(holder, token)) ON COMMIT DROP`);
        await tx.execute(sql`CREATE TEMP TABLE repair_nfts (token bytea, token_id numeric(78,0), PRIMARY KEY(token, token_id)) ON COMMIT DROP`);
        const condition = rangeSql(n, range);

        const touch = async () => {
          await tx.execute(sql`INSERT INTO repair_pairs
            SELECT "from", token FROM transfers WHERE ${condition} UNION SELECT "to", token FROM transfers WHERE ${condition}
            ON CONFLICT DO NOTHING`);
          await tx.execute(sql`INSERT INTO repair_nfts SELECT DISTINCT token, value FROM transfers WHERE ${condition} AND standard = 721 ON CONFLICT DO NOTHING`);
        };

        await touch();
        await tx.delete(raw).where(rangeCondition(n, range));
        await tx.delete(headers).where(and(eq(headers.network, n), gte(headers.number, range.fromBlock), lte(headers.number, range.toBlock)));
        await insertRange(tx, n, blocks, unique);
        await touch();
        await tx.execute(sql`CREATE TEMP TABLE repair_accounts ON COMMIT DROP AS SELECT DISTINCT holder FROM repair_pairs`);
        await rebuildTouched(tx, n);
        await tx.delete(cache).where(and(eq(cache.network, n), gte(cache.blockNumber, range.fromBlock), lte(cache.blockNumber, range.toBlock)));
        await tx.update(state).set({
          revision: sql`${state.revision} + 1`,
          ...(range.toBlock === current.cursor.number && { blockHash: last.hash, blockTimestamp: last.timestamp }),
        }).where(eq(state.network, n));

        // The tail was anchored on the replaced cursor hash; the tail rebuilds from the new one.
        if (range.toBlock === current.cursor.number && last.hash !== current.cursor.hash) await wipeTail(tx, n);
        await tx.delete(repairs).where(and(eq(repairs.network, n), gte(repairs.fromBlock, range.fromBlock), lte(repairs.toBlock, range.toBlock)));
      });
    },

    async rebuildRange(network, range) {
      const n = id(network);
      await db.transaction(async (tx) => {
        const current = await readState(tx, n, 'update');

        if (!current || range.fromBlock < current.startBlock || range.toBlock > current.cursor.number || range.toBlock < range.fromBlock) {
          throw new Error('Rebuild must be inside the committed index');
        }

        const condition = rangeSql(n, range);
        await tx.execute(sql`CREATE TEMP TABLE repair_accounts ON COMMIT DROP AS
          SELECT "from" AS holder FROM transfers WHERE ${condition} UNION SELECT "to" FROM transfers WHERE ${condition}`);
        await tx.execute(sql`CREATE TEMP TABLE repair_nfts ON COMMIT DROP AS
          SELECT DISTINCT token, value AS token_id FROM transfers WHERE ${condition} AND standard = 721`);
        await rebuildTouched(tx, n);
        await tx.update(state).set({ revision: sql`${state.revision} + 1` }).where(eq(state.network, n));
        // Rebuilding derived state does not certify suspect raw data, so repair queue entries remain pending.
      });
    },

    async enqueueRepair(network, range, reason, at) {
      const n = id(network);
      await db.transaction(async (tx) => {
        if (!await readState(tx, n, 'update')) throw new Error('Index is not initialized');
        await tx.insert(repairs).values({ network: n, ...range, reason, createdAt: at }).onConflictDoNothing();
      });
    },

    async repairs(network) {
      return db.select({ fromBlock: repairs.fromBlock, toBlock: repairs.toBlock, reason: repairs.reason, createdAt: repairs.createdAt })
        .from(repairs).where(eq(repairs.network, id(network))).orderBy(asc(repairs.fromBlock));
    },

    async readAccount(network, account, options = {}) {
      const n = id(network);

      return db.transaction(async (tx) => {
        const current = await readState(tx, n);

        if (!current) return undefined;
        const tip = options.tail ? await tailTip(tx, n) : undefined;

        const tail = tip ? await tx.select().from(tailRaw).where(and(
          eq(tailRaw.network, n), gt(tailRaw.blockNumber, current.cursor.number), or(eq(tailRaw.from, account), eq(tailRaw.to, account)),
        )).orderBy(asc(tailRaw.blockNumber), asc(tailRaw.logIndex)) : [];

        return {
          state: current, holdings: await holdings(tx, n, account), nftItems: await nftItems(tx, n, account),
          tailTransfers: tail.map(({ network: _network, ...event }) => event), tailTip: tip,
          ...(options.balances && { balances: await balances(tx, n, account) }),
        };
      }, { isolationLevel: 'repeatable read', accessMode: 'read only' });
    },

    holdings: (network, account) => holdings(db, id(network), account),
    nftItems: (network, account) => nftItems(db, id(network), account),
    balances: (network, account) => balances(db, id(network), account),

    async tokenHolders(network, token, limit, after) {
      checkLimit(limit);

      const rows = await db.select({ holder: h.holder }).from(h).where(and(
        eq(h.network, id(network)), eq(h.token, token), after === undefined ? undefined : gt(h.holder, after),
      )).orderBy(asc(h.holder)).limit(limit);

      return rows.map((row) => row.holder);
    },

    async transferHistory(network, account, limit, before, tail = false) {
      checkLimit(limit);
      const n = id(network);

      return db.transaction(async (tx) => {
        const current = await readState(tx, n);

        if (!current) return undefined;
        const { cursor } = current;
        const tip = tail ? await tailTip(tx, n) : undefined;
        const top = tip && tip.number > cursor.number ? tip.number : cursor.number;

        // A latest cursor above the finalized cursor stays valid while the tail still reaches its bound.
        if (before && (before.revision !== cursor.revision || before.throughBlock > top)) {
          throw Object.assign(new Error('History changed; restart pagination'), { statusCode: 409 });
        }

        const through = before?.throughBlock ?? top;
        const keyset = (t: typeof raw | typeof tailRaw) => and(eq(t.network, n), lte(t.blockNumber, through), before && sql`(${t.blockNumber}, ${t.logIndex}) < (${before.blockNumber}, ${before.logIndex})`);
        const notSelf = (t: typeof raw | typeof tailRaw) => sql`${t.from} <> ${sql.param(account, t.from)}`;
        // Two ordered index scans per table with bounded output; exclude the self-transfer from the received branch.
        const sent = tx.select().from(raw).where(and(keyset(raw), eq(raw.from, account))).orderBy(desc(raw.blockNumber), desc(raw.logIndex)).limit(limit + 1);
        const received = tx.select().from(raw).where(and(keyset(raw), eq(raw.to, account), notSelf(raw))).orderBy(desc(raw.blockNumber), desc(raw.logIndex)).limit(limit + 1);
        const rows: HistoryTransfer[] = [...await sent, ...await received].map(({ network: _network, ...event }) => event);

        if (tip && through > cursor.number) {
          const above = and(keyset(tailRaw), gt(tailRaw.blockNumber, cursor.number));
          const tailSent = tx.select().from(tailRaw).where(and(above, eq(tailRaw.from, account))).orderBy(desc(tailRaw.blockNumber), desc(tailRaw.logIndex)).limit(limit + 1);
          const tailReceived = tx.select().from(tailRaw).where(and(above, eq(tailRaw.to, account), notSelf(tailRaw))).orderBy(desc(tailRaw.blockNumber), desc(tailRaw.logIndex)).limit(limit + 1);
          rows.push(...[...await tailSent, ...await tailReceived].map(({ network: _network, ...event }) => ({ ...event, unfinalized: true as const })));
        }

        rows.sort(newestFirst);
        const page = rows.slice(0, limit);
        const last = page.at(-1);

        return {
          state: current,
          throughBlock: through,
          transfers: page,
          next: rows.length > limit && last ? { blockNumber: last.blockNumber, logIndex: last.logIndex, throughBlock: through, revision: cursor.revision } : undefined,
          tailTip: tip,
        };
      }, { isolationLevel: 'repeatable read', accessMode: 'read only' });
    },

    async saveBalances(network, block, rows, readAt, revision) {
      const n = id(network);

      return db.transaction(async (tx) => {
        const current = await readState(tx, n, 'share');

        if (!current || current.cursor.revision !== revision) return false;

        const [canonical] = block.number > current.cursor.number
          ? await tx.select({ hash: tailHeaders.hash }).from(tailHeaders).where(and(eq(tailHeaders.network, n), eq(tailHeaders.number, block.number)))
          : [await headerAt(tx, n, block.number)];

        if (canonical?.hash !== block.hash) return false;
        await batches(rows, (batch) => tx.insert(cache).values(batch.map((row) => ({
          network: n, account: row.account, token: row.token,
          balance: row.result.status === 'ok' ? row.result.balance : null,
          error: row.result.status === 'error' ? row.result.error : null,
          blockNumber: block.number, blockHash: block.hash, readAt, revision,
        }))).onConflictDoUpdate({
          target: [cache.network, cache.account, cache.token],
          set: {
            balance: sql`excluded.balance`, error: sql`excluded.error`, blockNumber: sql`excluded.block_number`,
            blockHash: sql`excluded.block_hash`, readAt: sql`excluded.read_at`, revision: sql`excluded.revision`,
          },
          // A newer read never loses to an older one, unless the newer one's block was reorged out.
          setWhere: sql`excluded.block_number > ${cache.blockNumber} OR
            (excluded.block_number = ${cache.blockNumber} AND excluded.block_hash = ${cache.blockHash} AND excluded.read_at >= ${cache.readAt})
            OR NOT ${canonicalSnapshot}`,
        }));

        return true;
      });
    },

    tailTip: (network) => tailTip(db, id(network)),

    async tailBlocks(network) {
      return db.select({ number: tailHeaders.number, hash: tailHeaders.hash, parentHash: tailHeaders.parentHash, timestamp: tailHeaders.timestamp })
        .from(tailHeaders).where(eq(tailHeaders.network, id(network))).orderBy(asc(tailHeaders.number));
    },

    async appendTailBlocks(network, expectedParentHash, blocks) {
      if (blocks.length === 0) return;
      const n = id(network);
      await db.transaction(async (tx) => {
        // The cursor share lock serializes an empty-tail anchor against finalization and pruning.
        const current = await readState(tx, n, 'share');

        if (!current) throw new TailConflictError('Index is not initialized');
        const tip = await tailTip(tx, n, 'update');
        const anchor = tip ?? current.cursor;

        if (anchor.hash !== expectedParentHash) throw new TailConflictError(`Tail parent changed from ${expectedParentHash} to ${anchor.hash}`);
        let parent = anchor.hash;
        let number = anchor.number + 1n;

        for (const block of blocks) {
          if (block.header.number !== number || block.header.parentHash !== parent) throw new TailConflictError(`Non-contiguous tail block ${block.header.number}`);

          if (block.transfers.some((event) => event.blockNumber !== number || event.blockHash !== block.header.hash)) {
            throw new TailConflictError(`Transfer does not belong to tail block ${number}`);
          }

          parent = block.header.hash;
          number += 1n;
        }

        await batches(blocks, (batch) => tx.insert(tailHeaders).values(batch.map(({ header }) => ({ network: n, ...header }))));
        const events = [...new Map(blocks.flatMap((block) => block.transfers).map((event) => [`${event.blockNumber}:${event.logIndex}`, event])).values()];
        await batches(events, (batch) => tx.insert(tailRaw).values(batch.map((event) => ({ network: n, ...event }))));
      });
    },

    async rewindTail(network, aboveNumber, expectedTipHash) {
      const n = id(network);
      await db.transaction(async (tx) => {
        await fenceTip(tx, n, expectedTipHash);
        await tx.delete(tailRaw).where(and(eq(tailRaw.network, n), gt(tailRaw.blockNumber, aboveNumber)));
        await tx.delete(tailHeaders).where(and(eq(tailHeaders.network, n), gt(tailHeaders.number, aboveNumber)));
      });
    },

    async wipeTail(network, expectedTipHash) {
      const n = id(network);
      await db.transaction(async (tx) => {
        await fenceTip(tx, n, expectedTipHash);
        await wipeTail(tx, n);
      });
    },

    close,
  };
}
