import { sql } from 'drizzle-orm';
import { expect, test, vi } from 'vitest';

import { assertStartBlock, runBootstrap, StartBlockMismatchError } from '../src/bootstrap/bootstrap.ts';
import type { BootstrapOptions } from '../src/bootstrap/bootstrap.ts';
import { createBootstrapStore } from '../src/bootstrap/store.ts';
import type { BootstrapHeader, BootstrapSource } from '../src/bootstrap/sources.ts';
import type { BlockRange, TransferEvent } from '../src/core/types.ts';
import { createFollower } from '../src/follower/follower.ts';
import { address, blockHash, FakeChain } from './fake-chain.ts';
import { headersFor, stack } from './stack.ts';
import { createTestStore } from './store-fixture.ts';

// Each test builds two PGlite databases (bootstrap and follower paths); slow under a parallel full run.
vi.setConfig({ testTimeout: 30_000 });

const [alice, bob, carol, token, nft] = [1, 2, 3, 4, 5].map(address);
const ZERO = address(0);

/** Mints, transfers, a self-transfer and an NFT that changes hands; several blocks stay empty. */
function populated(finalized = 130n): FakeChain {
  const chain = new FakeChain(finalized);

  for (let block = 100n; block <= finalized; block += 1n) {
    if (block % 4n === 3n) continue; // empty block: bloom says no Transfer
    chain.addTransfer({ token, from: ZERO, to: alice, value: 100n, blockNumber: block });
    chain.addTransfer({ token, from: alice, to: block % 2n ? bob : carol, value: block, blockNumber: block });

    if (block % 5n === 0n) chain.addTransfer({ token: nft, standard: 'erc721', from: block === 100n ? ZERO : alice, to: block % 10n ? alice : bob, value: 7n, blockNumber: block });
  }

  if (finalized >= 118n) chain.addTransfer({ token, from: carol, to: carol, value: 1n, blockNumber: 118n });

  return chain;
}

interface FakeSourceOptions {
  pageBlocks?: bigint;
  headers?: (headers: BootstrapHeader[]) => BootstrapHeader[];
  events?: (events: TransferEvent[], range: BlockRange) => TransferEvent[];
  /** Throw after the first page of a range (a crash in the middle of a segment). */
  crash?: (range: BlockRange) => boolean;
}

interface SourceCalls {
  headers: BlockRange[];
  transfers: BlockRange[];
  blocks: bigint[];
}

/** A source over the fake chain that records every range it was asked for. */
function fakeSource(name: string, chain: FakeChain, o: FakeSourceOptions = {}) {
  const calls: SourceCalls = { headers: [], transfers: [], blocks: [] };
  const page = o.pageBlocks ?? 3n;

  const source: BootstrapSource = {
    name,
    async headers(range) {
      calls.headers.push(range);
      const headers = headersFor(range.fromBlock, range.toBlock).map((header) => ({ ...header, transferBloom: chain.transfers.some((event) => event.blockNumber === header.number) }));

      return o.headers ? o.headers(headers) : headers;
    },
    async transfers(range, onPage) {
      calls.transfers.push(range);

      let events = chain.transfers.filter((event) => event.blockNumber >= range.fromBlock && event.blockNumber <= range.toBlock)
        .sort((a, b) => a.blockNumber === b.blockNumber ? a.logIndex - b.logIndex : a.blockNumber < b.blockNumber ? -1 : 1);

      if (o.events) events = o.events(events, range);
      let requests = 0;

      for (let from = range.fromBlock; from <= range.toBlock; from += page) {
        requests += 1;
        await onPage(events.filter((event) => event.blockNumber >= from && event.blockNumber < from + page));

        if (o.crash?.(range)) throw new Error('process crashed');
      }

      return { requests };
    },
    async blockTransfers(block) {
      calls.blocks.push(block.number);

      return chain.transfers.filter((event) => event.blockHash === block.hash);
    },
    stats: () => ({ requests: 0, logs: 0, failures: 0, rateLimitWaits: 0 }),
  };

  return { source, calls };
}

async function fixture() {
  const db = await createTestStore();

  return { ...db, bootstrap: createBootstrapStore(db.db) };
}

type Fixture = Awaited<ReturnType<typeof fixture>>;

const options = (f: Pick<Fixture, 'store' | 'bootstrap'>, chain: FakeChain, overrides: Partial<BootstrapOptions> = {}): BootstrapOptions => ({
  network: 'ethereum', startBlock: 100n, adapter: chain.adapter(), index: f.store, store: f.bootstrap,
  rpc: fakeSource('rpc', chain).source, segmentBlocks: 7n, headerChunkBlocks: 10n, rpcWorkers: 2, analyze: false, ...overrides,
});

/** Derived state as comparable text rows. */
async function dump(f: { db: Fixture['db'] }) {
  const rows = async (query: ReturnType<typeof sql>) => (await f.db.execute(query)).rows;

  return {
    holdings: await rows(sql`SELECT encode(holder, 'hex') h, encode(token, 'hex') t, standard, first_block::text f, last_block::text l, sent_count::text s, received_count::text r FROM holdings ORDER BY 1, 2`),
    nfts: await rows(sql`SELECT encode(token, 'hex') t, token_id::text i, encode(owner, 'hex') o, last_block::text b, last_log_index FROM nft_ownership ORDER BY 1, 2`),
    transfers: await rows(sql`SELECT block_number::text b, log_index, encode(block_hash, 'hex') h, encode("from", 'hex') f, encode("to", 'hex') t, value::text v FROM transfers ORDER BY block_number, log_index`),
    headers: await rows(sql`SELECT number::text n, encode(hash, 'hex') h, encode(parent_hash, 'hex') p FROM block_headers ORDER BY number`),
  };
}

async function followerDump(chain: FakeChain) {
  const s = await stack(chain, { range: 5n, maxRanges: 100 });

  try {
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, cursor: { number: chain.finalized } });

    return await dump(s);
  } finally { await s.close(); }
}

test('bootstrap publishes exactly the index the follower builds from the same logs, using both sources', async () => {
  const chain = populated();
  const f = await fixture();
  const hypersync = fakeSource('hypersync', chain);
  const rpc = fakeSource('rpc', chain);

  try {
    const result = await runBootstrap(options(f, chain, { hypersync: hypersync.source, rpc: rpc.source }));
    expect(result).toMatchObject({ status: 'done', target: 130n, transfers: chain.transfers.length, headers: 32, failedAttempts: 0 });
    expect(await dump(f)).toEqual(await followerDump(populated()));
    expect((await f.store.indexState('ethereum'))?.cursor).toMatchObject({ number: 130n, hash: blockHash(130n) });
    // Headers come from the preferred source in 10-block chunks; segments are shared from both ends.
    expect(hypersync.calls.headers).toHaveLength(4);
    expect(rpc.calls.headers).toHaveLength(0);
    expect(hypersync.calls.transfers.length + rpc.calls.transfers.length).toBe(5);
    expect(hypersync.calls.transfers[0]).toEqual({ fromBlock: 100n, toBlock: 106n });
    expect(rpc.calls.transfers[0]).toEqual({ fromBlock: 128n, toBlock: 130n });
    // Staging is emptied; the run stays as a record.
    expect((await f.db.execute(sql`SELECT count(*)::int c FROM bootstrap_transfers`)).rows).toEqual([{ c: 0 }]);
    expect(await f.bootstrap.status('ethereum', Date.now())).toMatchObject({ status: 'done', segmentsDone: 5, segmentsTotal: 5 });
  } finally { await f.store.close(); }
});

test('the follower continues at B + 1 without a gap and the service becomes ready', async () => {
  const chain = populated();
  const f = await fixture();

  try {
    await runBootstrap(options(f, chain));
    chain.finalized = 140n;
    chain.addTransfer({ token, from: alice, to: bob, value: 5n, blockNumber: 131n });
    chain.addTransfer({ token, from: bob, to: carol, value: 5n, blockNumber: 140n });
    const ranges: bigint[] = [];

    chain.beforeLogs = ({ fromBlock }) => { ranges.push(fromBlock); };

    const follower = createFollower({ adapter: chain.adapter(), store: f.store, pollMs: 30_000, maxNoAdvanceMs: 1_800_000 });
    await follower.tick();
    expect(follower.status()).toMatchObject({ ready: true, cursor: { number: 140n } });
    expect(ranges).toEqual([131n]);
    expect(await dump(f)).toEqual(await followerDump(chain));
  } finally { await f.store.close(); }
});

test('a crash mid-segment leaves no rows; the rerun loads only that segment, a further rerun is a no-op', async () => {
  const chain = populated();
  const f = await fixture();
  let crash = true;
  const first = fakeSource('rpc', chain, { crash: (range) => crash && range.fromBlock === 107n });

  try {
    await expect(runBootstrap(options(f, chain, { rpc: first.source }))).rejects.toThrow('1 segments failed on every source');
    expect(await f.bootstrap.status('ethereum', Date.now())).toMatchObject({ status: 'logs', segmentsDone: 4, segmentsTotal: 5, segmentsFailed: 1 });
    expect(await f.store.indexState('ethereum')).toBeUndefined();
    // The failed segment's raw rows and additive summaries both rolled back.
    const committed = await f.db.execute(sql`SELECT count(*)::int c, count(*) FILTER (WHERE block_number BETWEEN 107 AND 113)::int crashed FROM transfers`);
    expect(committed.rows).toEqual([{ c: chain.transfers.filter((event) => event.blockNumber < 107n || event.blockNumber > 113n).length, crashed: 0 }]);
    expect((await f.db.execute(sql`SELECT count(*)::int c FROM bootstrap_segments WHERE applied`)).rows).toEqual([{ c: 4 }]);

    crash = false;
    const second = fakeSource('rpc', chain);
    expect(await runBootstrap(options(f, chain, { rpc: second.source }))).toMatchObject({ status: 'done', transfers: chain.transfers.length });
    expect(second.calls.transfers).toEqual([{ fromBlock: 107n, toBlock: 113n }]);
    expect(second.calls.headers).toEqual([]);
    expect(await dump(f)).toEqual(await followerDump(populated()));

    const third = fakeSource('rpc', chain);
    expect(await runBootstrap(options(f, chain, { rpc: third.source }))).toEqual({ status: 'already-initialized' });
    expect(third.calls).toEqual({ headers: [], transfers: [], blocks: [] });
  } finally { await f.store.close(); }
});

test('loss of old UNLOGGED staging leaves newly committed segments intact', async () => {
  const chain = populated();
  const f = await fixture();

  try {
    await expect(runBootstrap(options(f, chain, { rpc: fakeSource('rpc', chain, { crash: (range) => range.fromBlock === 107n }).source }))).rejects.toThrow();
    await f.db.execute(sql`TRUNCATE bootstrap_transfers`);
    const rerun = fakeSource('rpc', chain);
    await runBootstrap(options(f, chain, { rpc: rerun.source }));
    expect(rerun.calls.transfers).toEqual([{ fromBlock: 107n, toBlock: 113n }]);
    expect(await dump(f)).toEqual(await followerDump(populated()));
  } finally { await f.store.close(); }
});

test('legacy staged segments replay independently; a failed later segment resumes without double counting', async () => {
  const chain = populated(113n);
  const f = await fixture();
  const headers = headersFor(99n, 113n);
  const first = chain.transfers.filter((event) => event.blockNumber <= 106n);
  const second = chain.transfers.filter((event) => event.blockNumber >= 107n);

  const stage = async (events: TransferEvent[]) => {
    for (const event of events) await f.db.execute(sql`INSERT INTO bootstrap_transfers
      (network, block_number, log_index, block_hash, transaction_hash, token, standard, "from", "to", value)
      VALUES (1, ${event.blockNumber}, ${event.logIndex}, decode(${event.blockHash.slice(2)}, 'hex'),
        decode(${event.transactionHash.slice(2)}, 'hex'), decode(${event.token.slice(2)}, 'hex'),
        ${event.standard === 'erc20' ? 20 : 721}, decode(${event.from.slice(2)}, 'hex'), decode(${event.to.slice(2)}, 'hex'), ${event.value})`);
  };

  try {
    await f.bootstrap.createRun('ethereum', { startBlock: 100n, target: headers.at(-1)!, anchor: headers[0], segmentBlocks: 7n }, Date.now());
    await f.bootstrap.saveHeaders('ethereum', { fromBlock: 100n, toBlock: 113n }, headers.slice(1).map((header) => ({ ...header, transferBloom: true })));
    await stage(first);
    await f.db.execute(sql`UPDATE bootstrap_segments SET done = true, logs = CASE from_block WHEN 100 THEN ${first.length}::bigint ELSE ${second.length}::bigint END`);
    await expect(f.bootstrap.finalize('ethereum', { at: Date.now(), maintenanceWorkMem: 'invalid' })).rejects.toThrow();
    expect(await f.bootstrap.status('ethereum', Date.now())).toMatchObject({ status: 'indexing', segmentsApplied: 0, indexesDone: 0, indexesTotal: 1 });
    await expect(f.bootstrap.finalize('ethereum', { at: Date.now() })).rejects.toThrow('staged rows');
    expect((await f.db.execute(sql`SELECT from_block::text b, applied FROM bootstrap_segments ORDER BY from_block`)).rows)
      .toEqual([{ b: '100', applied: true }, { b: '107', applied: false }]);
    expect(await f.bootstrap.status('ethereum', Date.now())).toMatchObject({ status: 'materializing', segmentsApplied: 1 });
    expect(await f.store.indexState('ethereum')).toBeUndefined();
    await stage(second);
    await f.bootstrap.finalize('ethereum', { at: Date.now() });
    expect(await dump(f)).toEqual(await followerDump(chain));
    expect((await f.db.execute(sql`SELECT sent_count::text s FROM holdings WHERE holder = decode(${alice.slice(2)}, 'hex') AND token = decode(${token.slice(2)}, 'hex')`)).rows).toHaveLength(1);
  } finally { await f.store.close(); }
});

test('publish refuses an incomplete staged header chain after all segments are applied', async () => {
  const chain = populated(106n);
  const f = await fixture();
  const headers = headersFor(99n, 106n);

  try {
    await f.bootstrap.createRun('ethereum', { startBlock: 100n, target: headers.at(-1)!, anchor: headers[0], segmentBlocks: 7n }, Date.now());
    await f.bootstrap.commitSegment('ethereum', { fromBlock: 100n, toBlock: 106n }, 'test', async (insert) => {
      await insert(chain.transfers);

      return { logs: chain.transfers.length, requests: 1 };
    }, Date.now());
    await expect(f.bootstrap.finalize('ethereum', { at: Date.now() })).rejects.toThrow('incomplete');
    expect(await f.store.indexState('ethereum')).toBeUndefined();
    await f.bootstrap.saveHeaders('ethereum', { fromBlock: 100n, toBlock: 106n }, headers.slice(1).map((header) => ({ ...header, transferBloom: true })));
    await f.bootstrap.finalize('ethereum', { at: Date.now() });
    expect((await f.store.indexState('ethereum'))?.cursor.number).toBe(106n);
  } finally { await f.store.close(); }
});

test('out-of-order and parallel segment commits converge on follower holdings and NFT winners', async () => {
  const chain = populated(113n);
  const ranges = [{ fromBlock: 100n, toBlock: 106n }, { fromBlock: 107n, toBlock: 113n }];
  const expected = await followerDump(chain);

  for (const parallel of [false, true]) {
    const f = await fixture();
    const headers = headersFor(99n, 113n);

    try {
      await f.bootstrap.createRun('ethereum', { startBlock: 100n, target: headers.at(-1)!, anchor: headers[0], segmentBlocks: 7n }, Date.now());
      await f.bootstrap.saveHeaders('ethereum', { fromBlock: 100n, toBlock: 113n }, headers.slice(1).map((header) => ({ ...header, transferBloom: true })));

      const commit = (range: BlockRange) => {
        const events = chain.transfers.filter((event) => event.blockNumber >= range.fromBlock && event.blockNumber <= range.toBlock);

        return f.bootstrap.commitSegment('ethereum', range, 'test', async (insert) => {
          await insert(events);

          return { logs: events.length, requests: 1 };
        }, Date.now());
      };

      if (parallel) await Promise.all(ranges.map(commit));
      else for (const range of ranges.toReversed()) await commit(range);
      await f.bootstrap.finalize('ethereum', { at: Date.now() });
      expect(await dump(f)).toEqual(expected);
    } finally { await f.store.close(); }
  }
});

test('a log hash that does not match the header chain refetches the segment from the other source', async () => {
  const chain = populated();
  const f = await fixture();

  const hypersync = fakeSource('hypersync', chain, {
    events: (events) => events.map((event) => event.blockNumber === 102n ? { ...event, blockHash: blockHash(9_999n) } : event),
  });

  const rpc = fakeSource('rpc', chain);

  try {
    await runBootstrap(options(f, chain, { hypersync: hypersync.source, rpc: rpc.source, rpcWorkers: 1 }));
    expect(rpc.calls.transfers).toContainEqual({ fromBlock: 100n, toBlock: 106n });
    const sources = (await f.db.execute(sql`SELECT from_block::text f, source FROM bootstrap_segments ORDER BY from_block`)).rows;
    expect(sources[0]).toEqual({ f: '100', source: 'rpc' });
    expect(await dump(f)).toEqual(await followerDump(populated()));
  } finally { await f.store.close(); }
});

test('a broken parent chain from HyperSync refetches that header chunk from RPC', async () => {
  const chain = populated();
  const f = await fixture();
  const hypersync = fakeSource('hypersync', chain, { headers: (headers) => headers.map((header) => header.number === 115n ? { ...header, parentHash: blockHash(1n) } : header) });
  const rpc = fakeSource('rpc', chain);

  try {
    await runBootstrap(options(f, chain, { hypersync: hypersync.source, rpc: rpc.source }));
    expect(rpc.calls.headers).toEqual([{ fromBlock: 110n, toBlock: 119n }]);
    expect(await dump(f)).toEqual(await followerDump(populated()));
  } finally { await f.store.close(); }
});

test('a block whose bloom has Transfer but returned no logs is re-asked on the other source, which then loads the segment', async () => {
  const chain = populated();
  const f = await fixture();
  const hypersync = fakeSource('hypersync', chain, { events: (events) => events.filter((event) => event.blockNumber !== 105n) });
  const rpc = fakeSource('rpc', chain);

  try {
    await runBootstrap(options(f, chain, { hypersync: hypersync.source, rpc: rpc.source, rpcWorkers: 1 }));
    // Empty blocks with a clean bloom (103, 107, ...) are never re-asked; block 105 is, once.
    expect(rpc.calls.blocks).toEqual([105n]);
    expect(rpc.calls.transfers).toContainEqual({ fromBlock: 100n, toBlock: 106n });
    expect(await dump(f)).toEqual(await followerDump(populated()));
  } finally { await f.store.close(); }
});

test('START_BLOCK must match the stored index and any bootstrap in progress', async () => {
  const chain = populated();
  const f = await fixture();

  try {
    await expect(runBootstrap(options(f, chain, { rpc: fakeSource('rpc', chain, { crash: () => true }).source }))).rejects.toThrow();
    await expect(assertStartBlock(f.store, f.bootstrap, 'ethereum', 101n)).rejects.toThrow(StartBlockMismatchError);
    await expect(runBootstrap(options(f, chain, { startBlock: 101n }))).rejects.toThrow('START_BLOCK is 101 but the ethereum bootstrap run starts at 100');
    await runBootstrap(options(f, chain));
    await expect(assertStartBlock(f.store, f.bootstrap, 'ethereum', 99n)).rejects.toThrow('a different start block needs a fresh database');
    await expect(runBootstrap(options(f, chain, { startBlock: 99n }))).rejects.toThrow(StartBlockMismatchError);
    await assertStartBlock(f.store, f.bootstrap, 'ethereum', 100n);
  } finally { await f.store.close(); }
});

test('/health reports bootstrap progress while not ready, and is ready once the run is published', async () => {
  const chain = populated();
  const s = await stack(chain, { initialize: false });
  const f = { store: s.store, bootstrap: createBootstrapStore(s.db) };

  try {
    await expect(runBootstrap(options(f, chain, { rpc: fakeSource('rpc', chain, { crash: (range) => range.fromBlock === 107n }).source }))).rejects.toThrow();
    const pending = await s.app.request('/health');
    expect(pending.status).toBe(200);
    expect(await pending.json()).toMatchObject({
      status: 'not_ready', networks: [{ network: 'ethereum', ready: false, serving: 'partial' }],
      bootstrap: { ethereum: { status: 'logs', startBlock: '100', targetBlock: '130', headers: '32/32', segmentsDone: 4, segmentsTotal: 5, segmentsFailed: 1 } },
    });
    await runBootstrap(options(f, chain));
    const ready = await s.app.request('/health');
    expect(ready.status).toBe(200);
    expect(await ready.json()).not.toHaveProperty('bootstrap');
    const account = await (await s.app.request(`/v1/ethereum/accounts/${bob}/erc20`)).json();
    expect(account).toMatchObject({ status: 'ready', blockHeight: '130', fromBlock: '100', tokens: [{ contract: token }] });
  } finally { await s.close(); }
});
