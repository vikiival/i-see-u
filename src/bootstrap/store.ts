import { and, asc, eq, gte, lte, sql } from 'drizzle-orm';
import type { SQL } from 'drizzle-orm';

import type { BlockHeader, BlockRange, Hex, TransferEvent } from '../core/types.ts';
import { networkRegistry } from '../networks.ts';
import * as schema from '../store/schema.ts';
import { applyNfts, applySummaries } from '../store/store.ts';
import type { Executor, StoreDatabase } from '../store/store.ts';
import type { BootstrapHeader } from './sources.ts';

const { bootstrapRuns: runs, bootstrapSegments: segments, bootstrapHeaders: staged, ingestionState: state } = schema;

export type BootstrapPhase = 'headers' | 'logs' | 'finalizing' | 'done';

export interface BootstrapRun {
  startBlock: bigint;
  targetBlock: bigint;
  targetHash: Hex;
  status: BootstrapPhase;
  startedAt: number;
  resumedAt: number;
  error?: string;
}

export interface BootstrapStatus extends BootstrapRun {
  segmentsTotal: number;
  segmentsDone: number;
  /** Segments whose last attempt failed on every source (they stay pending for the next run). */
  segmentsFailed: number;
  headersLoaded: number;
  headersTotal: number;
  logs: number;
  /** Rows per second since the last (re)start of the loader. */
  logsPerS?: number;
  etaS?: number;
}

export interface Segment extends BlockRange {
  attempts: number;
}

export interface FinalizeOptions {
  at: number;
  /** SET LOCAL maintenance_work_mem for the index builds, e.g. '1GB' (Postgres only). */
  maintenanceWorkMem?: string;
}

/** Row width of one staged insert statement (arrays of text, decoded in SQL). */
const STAGE_BATCH = 5_000;

/**
 * Secondary indexes rebuilt after the bulk insert (loading into an indexed table measured 1.8-2.5x slower end to end).
 * Must match the generated migrations exactly; primary keys stay.
 */
const SECONDARY_INDEXES = [
  ['transfers_from_history', sql`CREATE INDEX "transfers_from_history" ON "transfers" USING btree ("network","from","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST)`],
  ['transfers_to_history', sql`CREATE INDEX "transfers_to_history" ON "transfers" USING btree ("network","to","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST)`],
  ['transfers_nft_history', sql`CREATE INDEX "transfers_nft_history" ON "transfers" USING btree ("network","token","value","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST) WHERE "transfers"."standard" = 721`],
  ['holdings_token_holders', sql`CREATE INDEX "holdings_token_holders" ON "holdings" USING btree ("network","token","holder")`],
  ['nft_owner_items', sql`CREATE INDEX "nft_owner_items" ON "nft_ownership" USING btree ("network","owner","token","token_id")`],
] as const;

const count = sql<number>`count(*)`.mapWith(Number);

export interface BootstrapStore {
  run(network: string): Promise<BootstrapRun | undefined>;
  /** Pins the run (target B) and its segments, and stages the anchor header S-1. Idempotent: an existing run wins. */
  createRun(network: string, run: { startBlock: bigint; target: BlockHeader; anchor: BlockHeader; segmentBlocks: bigint }, at: number): Promise<BootstrapRun>;
  /** Marks a loader (re)start; resets done segments if PostgreSQL lost the UNLOGGED staging rows (crash recovery). */
  resume(network: string, at: number): Promise<{ reset: boolean }>;
  setPhase(network: string, phase: BootstrapPhase, at: number, error?: string): Promise<void>;
  headerCount(network: string, range: BlockRange): Promise<number>;
  headerAt(network: string, number: bigint): Promise<BootstrapHeader | undefined>;
  headers(network: string, range: BlockRange): Promise<BootstrapHeader[]>;
  /** Replaces the staged headers of the range atomically. */
  saveHeaders(network: string, range: BlockRange, headers: BootstrapHeader[]): Promise<void>;
  pendingSegments(network: string): Promise<Segment[]>;
  /**
   * One transaction per segment: `fill` streams verified rows through `insert`; the segment is marked done with its
   * counts only if `fill` resolves, so a crash or verification failure leaves no rows and no marker behind.
   */
  commitSegment(network: string, segment: BlockRange, source: string, fill: (insert: (rows: TransferEvent[]) => Promise<void>) => Promise<{ logs: number; requests: number }>, at: number): Promise<void>;
  failSegment(network: string, segment: BlockRange, error: string, at: number): Promise<void>;
  /** Verify the whole run and publish it into the index in one transaction; the follower continues from B. */
  finalize(network: string, options: FinalizeOptions): Promise<{ transfers: number; headers: number }>;
  /** VACUUM (ANALYZE) the published tables (outside any transaction). */
  analyze(): Promise<void>;
  status(network: string, now: number): Promise<BootstrapStatus | undefined>;
}

export function createBootstrapStore(
  db: StoreDatabase,
  networks: ReadonlyMap<string, number> = new Map(Object.values(networkRegistry).map((n) => [n.name, n.id])),
): BootstrapStore {
  const id = (network: string): number => {
    const value = networks.get(network);

    if (value === undefined) throw new Error(`Unknown database network: ${network}`);

    return value;
  };

  const readRun = async (tx: Executor, n: number, lock = false): Promise<BootstrapRun | undefined> => {
    const query = tx.select().from(runs).where(eq(runs.network, n));
    const [row] = await (lock ? query.for('update') : query);

    if (!row) return undefined;

    return {
      startBlock: row.startBlock, targetBlock: row.targetBlock, targetHash: row.targetHash,
      status: phaseOf(row.status), startedAt: row.startedAt, resumedAt: row.resumedAt, error: row.error ?? undefined,
    };
  };

  const headerRange = (n: number, range: BlockRange) => and(eq(staged.network, n), gte(staged.number, range.fromBlock), lte(staged.number, range.toBlock));

  const headerOf = (row: typeof staged.$inferSelect): BootstrapHeader => ({
    number: row.number, hash: row.hash, parentHash: row.parentHash, timestamp: row.timestamp, transferBloom: row.transferBloom,
  });

  const stageRows = async (tx: Executor, n: number, rows: TransferEvent[]) => {
    for (let i = 0; i < rows.length; i += STAGE_BATCH) {
      const batch = rows.slice(i, i + STAGE_BATCH);
      const column = (pick: (row: TransferEvent) => string): SQL => sql`${sql.param(batch.map(pick))}::text[]`;
      const bytes = (pick: (row: TransferEvent) => Hex) => column((row) => pick(row).slice(2));
      // Text arrays + unnest: one statement per batch, identical on node-postgres and PGlite, no COPY dependency.
      await tx.execute(sql`INSERT INTO bootstrap_transfers (network, block_number, log_index, block_hash, transaction_hash, token, standard, "from", "to", value)
        SELECT ${n}::smallint, b::bigint, i::integer, decode(bh, 'hex'), decode(th, 'hex'), decode(tk, 'hex'), s::smallint, decode(f, 'hex'), decode(t, 'hex'), v::numeric
        FROM unnest(${column((row) => row.blockNumber.toString())}, ${column((row) => String(row.logIndex))}, ${bytes((row) => row.blockHash)},
          ${bytes((row) => row.transactionHash)}, ${bytes((row) => row.token)}, ${column((row) => row.standard === 'erc20' ? '20' : '721')},
          ${bytes((row) => row.from)}, ${bytes((row) => row.to)}, ${column((row) => row.value.toString())}) AS u(b, i, bh, th, tk, s, f, t, v)`);
    }
  };

  return {
    run: (network) => readRun(db, id(network)),

    async createRun(network, o, at) {
      const n = id(network);

      if (o.anchor.number !== o.startBlock - 1n) throw new Error('Anchor must be the block before the start block');

      return db.transaction(async (tx) => {
        const inserted = await tx.insert(runs).values({
          network: n, startBlock: o.startBlock, targetBlock: o.target.number, targetHash: o.target.hash,
          status: 'headers', startedAt: at, resumedAt: at, updatedAt: at,
        }).onConflictDoNothing().returning({ network: runs.network });

        if (inserted.length > 0) {
          const rows: (typeof segments.$inferInsert)[] = [];

          for (let from = o.startBlock; from <= o.target.number; from += o.segmentBlocks) {
            const to = from + o.segmentBlocks - 1n;
            rows.push({ network: n, fromBlock: from, toBlock: to < o.target.number ? to : o.target.number });
          }

          for (let i = 0; i < rows.length; i += 1_000) await tx.insert(segments).values(rows.slice(i, i + 1_000));
          await tx.insert(staged).values({ network: n, ...o.anchor, transferBloom: false }).onConflictDoNothing();
        }

        return (await readRun(tx, n))!;
      });
    },

    async resume(network, at) {
      const n = id(network);

      return db.transaction(async (tx) => {
        await tx.update(runs).set({ resumedAt: at, updatedAt: at, error: null }).where(eq(runs.network, n));
        const [loaded] = await tx.select({ count }).from(segments).where(and(eq(segments.network, n), eq(segments.done, true), sql`${segments.logs} > 0`));
        const lost = loaded.count > 0 && (await tx.execute(sql`SELECT 1 FROM bootstrap_transfers WHERE network = ${n} LIMIT 1`)).rows.length === 0;

        if (lost) await tx.update(segments).set({ done: false, source: null, logs: 0, calls: 0 }).where(eq(segments.network, n));

        return { reset: lost };
      });
    },

    async setPhase(network, phase, at, error) {
      await db.update(runs).set({ status: phase, updatedAt: at, error: error ?? null }).where(eq(runs.network, id(network)));
    },

    async headerCount(network, range) {
      const [row] = await db.select({ count }).from(staged).where(headerRange(id(network), range));

      return row.count;
    },

    async headerAt(network, number) {
      const [row] = await db.select().from(staged).where(and(eq(staged.network, id(network)), eq(staged.number, number)));

      return row && headerOf(row);
    },

    async headers(network, range) {
      return (await db.select().from(staged).where(headerRange(id(network), range)).orderBy(asc(staged.number))).map(headerOf);
    },

    async saveHeaders(network, range, headers) {
      const n = id(network);
      await db.transaction(async (tx) => {
        await tx.delete(staged).where(headerRange(n, range));

        for (let i = 0; i < headers.length; i += 1_000) {
          await tx.insert(staged).values(headers.slice(i, i + 1_000).map((header) => ({ network: n, ...header })));
        }
      });
    },

    async pendingSegments(network) {
      const rows = await db.select({ fromBlock: segments.fromBlock, toBlock: segments.toBlock, attempts: segments.attempts })
        .from(segments).where(and(eq(segments.network, id(network)), eq(segments.done, false))).orderBy(asc(segments.fromBlock));

      return rows;
    },

    async commitSegment(network, segment, source, fill, at) {
      const n = id(network);
      await db.transaction(async (tx) => {
        const [row] = await tx.select({ done: segments.done }).from(segments)
          .where(and(eq(segments.network, n), eq(segments.fromBlock, segment.fromBlock))).for('update');

        if (!row) throw new Error(`Unknown bootstrap segment ${segment.fromBlock}`);

        if (row.done) return; // another loader committed it meanwhile
        const result = await fill((rows) => stageRows(tx, n, rows));
        await tx.update(segments).set({
          done: true, source, logs: result.logs, calls: result.requests, attempts: sql`${segments.attempts} + 1`, error: null, updatedAt: at,
        }).where(and(eq(segments.network, n), eq(segments.fromBlock, segment.fromBlock)));
      });
    },

    async failSegment(network, segment, error, at) {
      await db.update(segments).set({ attempts: sql`${segments.attempts} + 1`, error: error.slice(0, 500), updatedAt: at })
        .where(and(eq(segments.network, id(network)), eq(segments.fromBlock, segment.fromBlock)));
    },

    async finalize(network, o) {
      const n = id(network);

      return db.transaction(async (tx) => {
        const run = await readRun(tx, n, true);

        if (!run) throw new Error('No bootstrap run to finalize');

        if (run.status === 'done') throw new Error('Bootstrap is already finalized');

        if ((await tx.select({ count }).from(state).where(eq(state.network, n)))[0].count > 0) throw new Error('Index is already initialized');
        const [pending] = await tx.select({ count }).from(segments).where(and(eq(segments.network, n), eq(segments.done, false)));

        if (pending.count > 0) throw new Error(`${pending.count} bootstrap segments are not loaded`);

        // The staged chain must cover S-1..B, link every parent, and end at the pinned finalized hash.
        const headerRows = (await tx.select({ count }).from(staged).where(eq(staged.network, n)))[0].count;

        if (BigInt(headerRows) !== run.targetBlock - run.startBlock + 2n) throw new Error(`Staged header chain is incomplete (${headerRows} headers)`);

        const [breaks] = await tx.select({ count }).from(staged)
          .innerJoin(sql`bootstrap_headers p`, sql`p.network = ${staged.network} AND p.number = ${staged.number} - 1`)
          .where(and(eq(staged.network, n), sql`p.hash <> ${staged.parentHash}`));

        if (breaks.count > 0) throw new Error(`Staged header chain has ${breaks.count} parent-hash breaks`);
        const [target] = await tx.select().from(staged).where(and(eq(staged.network, n), eq(staged.number, run.targetBlock)));

        if (target?.hash !== run.targetHash) throw new Error('Staged header chain does not end at the pinned finalized block');

        const [expected] = await tx.select({ logs: sql<number>`coalesce(sum(${segments.logs}), 0)`.mapWith(Number) }).from(segments).where(eq(segments.network, n));
        const [stagedRows] = await tx.select({ count }).from(schema.bootstrapTransfers).where(eq(schema.bootstrapTransfers.network, n));

        if (stagedRows.count !== expected.logs) throw new Error(`Staged rows (${stagedRows.count}) differ from segment counts (${expected.logs}); rerun to reload`);

        const [existing] = await tx.select({ count }).from(schema.transfers).where(eq(schema.transfers.network, n));

        if (existing.count > 0) throw new Error('transfers already holds rows for this network; bootstrap needs a fresh database');

        if (o.maintenanceWorkMem) await tx.execute(sql.raw(`SET LOCAL maintenance_work_mem = '${o.maintenanceWorkMem.replace(/[^0-9A-Za-z]/g, '')}'`));

        for (const [name] of SECONDARY_INDEXES) await tx.execute(sql.raw(`DROP INDEX IF EXISTS "${name}"`));
        await tx.execute(sql`INSERT INTO block_headers (network, number, hash, parent_hash, timestamp)
          SELECT network, number, hash, parent_hash, timestamp FROM bootstrap_headers WHERE network = ${n} ORDER BY number`);
        // The primary key rejects any duplicate (block, log index) that slipped through verification.
        await tx.execute(sql`INSERT INTO transfers (network, block_number, log_index, block_hash, transaction_hash, token, standard, "from", "to", value)
          SELECT network, block_number, log_index, block_hash, transaction_hash, token, standard, "from", "to", value
          FROM bootstrap_transfers WHERE network = ${n} ORDER BY block_number, log_index`);
        // Same SQL as the follower's appendRange, as one set-based pass over the whole range.
        await applySummaries(tx, sql`
          SELECT network, "from" AS holder, token, standard, block_number, 1 AS sent, 0 AS received FROM transfers WHERE network = ${n}
          UNION ALL
          SELECT network, "to" AS holder, token, standard, block_number, 0 AS sent, 1 AS received FROM transfers WHERE network = ${n}`);
        await applyNfts(tx, sql`SELECT * FROM transfers WHERE network = ${n} AND standard = 721`);

        for (const [, create] of SECONDARY_INDEXES) await tx.execute(create);
        await tx.insert(state).values({
          network: n, startBlock: run.startBlock, blockNumber: target.number, blockHash: target.hash, blockTimestamp: target.timestamp, updatedAt: o.at,
          headNumber: target.number, headHash: target.hash, headTimestamp: target.timestamp, checkedAt: o.at,
        });
        await tx.update(runs).set({ status: 'done', updatedAt: o.at, error: null }).where(eq(runs.network, n));
        const others = await tx.execute(sql`SELECT 1 FROM bootstrap_transfers WHERE network <> ${n} LIMIT 1`);

        if (others.rows.length === 0) await tx.execute(sql`TRUNCATE bootstrap_transfers`);
        else await tx.execute(sql`DELETE FROM bootstrap_transfers WHERE network = ${n}`);
        await tx.delete(staged).where(eq(staged.network, n));

        return { transfers: stagedRows.count, headers: headerRows };
      });
    },

    async analyze() {
      for (const table of ['transfers', 'holdings', 'nft_ownership', 'block_headers']) await db.execute(sql.raw(`VACUUM (ANALYZE) ${table}`));
    },

    async status(network, now) {
      const n = id(network);
      const run = await readRun(db, n);

      if (!run) return undefined;
      const since = run.resumedAt;

      const [row] = await db.select({
        total: count,
        done: sql<number>`count(*) FILTER (WHERE ${segments.done})`.mapWith(Number),
        failed: sql<number>`count(*) FILTER (WHERE NOT ${segments.done} AND ${segments.error} IS NOT NULL)`.mapWith(Number),
        logs: sql<number>`coalesce(sum(${segments.logs}) FILTER (WHERE ${segments.done}), 0)`.mapWith(Number),
        doneSince: sql<number>`count(*) FILTER (WHERE ${segments.done} AND ${segments.updatedAt} >= ${since})`.mapWith(Number),
        logsSince: sql<number>`coalesce(sum(${segments.logs}) FILTER (WHERE ${segments.done} AND ${segments.updatedAt} >= ${since}), 0)`.mapWith(Number),
      }).from(segments).where(eq(segments.network, n));

      const headersLoaded = run.status === 'done' ? 0 : (await db.select({ count }).from(staged).where(eq(staged.network, n)))[0].count;
      const elapsedS = Math.max(1, (now - since) / 1_000);
      const remaining = row.total - row.done;

      return {
        ...run,
        segmentsTotal: row.total, segmentsDone: row.done, segmentsFailed: row.failed, logs: row.logs,
        headersLoaded, headersTotal: Number(run.targetBlock - run.startBlock + 2n),
        ...(row.doneSince > 0 && { logsPerS: Math.round(row.logsSince / elapsedS), etaS: Math.round(remaining * elapsedS / row.doneSince) }),
      };
    },
  };
}

const phaseOf = (value: string): BootstrapPhase => {
  if (value === 'headers' || value === 'logs' || value === 'finalizing' || value === 'done') return value;
  throw new Error(`Unknown bootstrap phase ${value}`);
};
