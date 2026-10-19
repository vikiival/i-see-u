import { sql } from 'drizzle-orm';
import { expect, it } from 'vitest';

import { createBootstrapStore } from '../src/bootstrap/store.ts';
import type { Address, BlockRange, Finality, TransferEvent } from '../src/core/types.ts';
import { address, FakeChain } from './fake-chain.ts';
import { stack } from './stack.ts';

const SEEDS = [1, 42, 2026, 0xdecafbad, 0xffffffff];
const START = 100n;
const END = 339n; // 240 blocks, including empty blocks.
const AT = 1_900_000_000_000;
const ZERO = address(0);
const ACCOUNTS = [1, 2, 3, 4, 5, 6].map(address);
const READERS = [ZERO, ...ACCOUNTS, address(999)]; // Include burns/mints and an untouched account.
const ERC20 = [20, 21, 22].map(address);
const ERC721 = [721, 722].map(address);

type Stack = Awaited<ReturnType<typeof stack>>;

type Random = () => number;

type Input = Parameters<FakeChain['addTransfer']>[0];

/** mulberry32: all chain contents, segment orders, page sizes and fork schedules derive from the seed. */
function random(seed: number): Random {
  let state = seed >>> 0;

  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = Math.imul(state ^ (state >>> 15), 1 | state);
    value ^= value + Math.imul(value ^ (value >>> 7), 61 | value);

    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

const pick = <T>(rng: Random, values: readonly T[]): T => values[Math.floor(rng() * values.length)];

const ordered = (events: TransferEvent[]) => events.toSorted((a, b) => a.blockNumber === b.blockNumber
  ? a.logIndex - b.logIndex : a.blockNumber < b.blockNumber ? -1 : 1);

const inRange = (chain: FakeChain, range: BlockRange) => ordered(chain.transfers.filter((event) => event.blockNumber >= range.fromBlock && event.blockNumber <= range.toBlock));

function shuffled<T>(rng: Random, values: T[]): T[] {
  const result = [...values];

  for (let i = result.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rng() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }

  return result;
}

/** Generate valid NFT movements from the surviving prefix, including repeated same-block movements. */
function generate(chain: FakeChain, rng: Random, from: bigint, to: bigint): Input[] {
  const owners = new Map<string, Address>();

  for (const event of ordered(chain.transfers)) {
    if (event.blockNumber < from && event.standard === 'erc721') owners.set(`${event.token}:${event.value}`, event.to);
  }

  const events: Input[] = [];

  const nft = (blockNumber: bigint, token: Address, value: bigint, recipient: Address) => {
    const key = `${token}:${value}`;
    events.push({ standard: 'erc721', token, value, from: owners.get(key) ?? ZERO, to: recipient, blockNumber });
    owners.set(key, recipient);
  };

  for (let block = from; block <= to; block += 1n) {
    if (block % 11n === 0n || rng() < 0.2) continue;
    const count = 1 + Math.floor(rng() * 4);

    for (let i = 0; i < count; i += 1) {
      if (rng() < 0.4) {
        const token = pick(rng, ERC721);
        const value = pick(rng, [0n, 1n, 2n, 3n, 2n ** 200n]);
        const owner = owners.get(`${token}:${value}`) ?? ZERO;
        nft(block, token, value, pick(rng, [ZERO, owner, ...ACCOUNTS]));
      } else {
        const sender = pick(rng, [ZERO, ...ACCOUNTS]);
        events.push({ token: pick(rng, ERC20), from: sender, to: pick(rng, [ZERO, sender, ...ACCOUNTS]),
          value: pick(rng, [0n, 1n, 17n, 2n ** 255n]), blockNumber: block });
      }
    }

    // Guarantee mints, burns, self-transfers and an NFT log-index winner in every seed.
    if (block % 17n === 0n) {
      events.push({ token: ERC20[0], from: ZERO, to: ACCOUNTS[0], value: 10n, blockNumber: block });
      events.push({ token: ERC20[0], from: ACCOUNTS[0], to: ACCOUNTS[0], value: 0n, blockNumber: block });
      events.push({ token: ERC20[0], from: ACCOUNTS[0], to: ZERO, value: 1n, blockNumber: block });
      nft(block, ERC721[0], 1n, ACCOUNTS[0]);
      nft(block, ERC721[0], 1n, ACCOUNTS[1]);
      nft(block, ERC721[0], 1n, ZERO);
    }
  }

  return events;
}

async function fixture(chain: FakeChain, tail = false): Promise<Stack> {
  const options: NonNullable<Parameters<typeof stack>[1]> = { initialize: false, range: 37n, maxRanges: 100 };

  if (tail) options.tail = { maxDepth: 8n };
  const s = await stack(chain, options);
  s.clock.at = AT;

  return s;
}

/** Ordered canonical reference: one normal append, with no bootstrap aggregates or tail overlay. */
async function linear(s: Stack, end: bigint): Promise<void> {
  const headers = [];

  for (let number = START - 1n; number <= end; number += 1n) headers.push(await s.adapter.headerByNumber(number));
  await s.store.initialize('ethereum', START, headers[0], AT);
  await s.store.observeHead('ethereum', headers.at(-1)!, AT);
  await s.store.appendRange('ethereum', { fromBlock: START, toBlock: end }, headers.slice(1), inRange(s.chain, { fromBlock: START, toBlock: end }), AT);
}

async function dump(s: Stack) {
  return {
    holdings: (await s.db.execute(sql`SELECT network, encode(holder, 'hex') holder, encode(token, 'hex') token,
      standard, first_block::text first, last_block::text last, sent_count::text sent, received_count::text received
      FROM holdings ORDER BY network, holder, token`)).rows,
    nfts: (await s.db.execute(sql`SELECT network, encode(token, 'hex') token, token_id::text id, encode(owner, 'hex') owner,
      last_block::text block, last_log_index FROM nft_ownership ORDER BY network, token, token_id`)).rows,
    transfers: await transferRows(s),
    headers: (await s.db.execute(sql`SELECT network, number::text number, encode(hash, 'hex') hash,
      encode(parent_hash, 'hex') parent, timestamp::text timestamp FROM block_headers ORDER BY network, block_headers.number`)).rows,
  };
}

/** UNION ALL intentionally retains duplicates, detecting overlap across the finalized/tail handoff. */
async function transferRows(s: Stack, tail = false) {
  const source = tail ? sql`(SELECT * FROM transfers UNION ALL SELECT * FROM tail_transfers)` : sql`transfers`;

  return (await s.db.execute(sql`SELECT network, block_number::text block, log_index, encode(block_hash, 'hex') hash,
    encode(transaction_hash, 'hex') tx, encode(token, 'hex') token, standard, encode("from", 'hex') sender,
    encode("to", 'hex') recipient, value::text value FROM ${source} t ORDER BY network, block_number, log_index`)).rows;
}

const expectedRows = (chain: FakeChain, through: bigint) => inRange(chain, { fromBlock: START, toBlock: through }).map((event) => ({
  network: 1, block: String(event.blockNumber), log_index: event.logIndex, hash: event.blockHash.slice(2),
  tx: event.transactionHash.slice(2), token: event.token.slice(2), standard: event.standard === 'erc20' ? 20 : 721,
  sender: event.from.slice(2), recipient: event.to.slice(2), value: String(event.value),
}));

interface ApiTransfer extends Omit<TransferEvent, 'value' | 'blockNumber'> {
  value: string;
  blockNumber: string;
  unfinalized?: boolean;
}

interface ApiBody {
  status: string;
  complete: boolean;
  finality: Finality;
  blockHeight: string;
  blockHash: string;
  lastProgressAt: string;
  dataAgeS: number;
  transfers?: ApiTransfer[];
  next?: string | null;
}

async function response(s: Stack, path: string): Promise<ApiBody> {
  const res = await s.app.request(path);
  expect(res.status, path).toBe(200);
  const body: ApiBody = await res.json();
  expect(body, path).toMatchObject({ status: 'ready', complete: true });

  return body;
}

// Commit times and wall-clock age are operational metadata, not ordering-dependent index contents.
const stable = ({ lastProgressAt: _progress, dataAgeS: _age, ...body }: ApiBody) => body;

async function history(s: Stack, account: Address, finality: Finality, limit: number) {
  const path = `/v1/ethereum/accounts/${account}/transfers?finality=${finality}&limit=${limit}`;
  const pages = [];
  const cursors = new Set<string>();
  let before: string | undefined;

  do {
    const body = await response(s, path + (before ? `&before=${encodeURIComponent(before)}` : ''));
    expect(body.finality).toBe(finality);
    pages.push(stable(body));
    before = body.next ?? undefined;

    if (before) {
      expect(cursors.has(before), 'pagination must advance').toBe(false);
      cursors.add(before);
    }
  } while (before);

  return pages;
}

async function compareApis(actual: Stack, reference: Stack, rng: Random): Promise<void> {
  for (const account of READERS) {
    for (const endpoint of ['erc20', 'erc721']) {
      const path = `/v1/ethereum/accounts/${account}/${endpoint}?finality=finalized`;
      expect(stable(await response(actual, path)), `${account}/${endpoint}`).toEqual(stable(await response(reference, path)));
    }

    const limit = pick(rng, [19, 37, 61]);
    const actualPages = await history(actual, account, 'finalized', limit);
    expect(actualPages, `${account}/transfers`).toEqual(await history(reference, account, 'finalized', limit));
    const transfers = actualPages.flatMap((page) => page.transfers ?? []);

    const expected = inRange(actual.chain, { fromBlock: START, toBlock: END }).toReversed()
      .filter((event) => event.from === account || event.to === account)
      .map((event) => ({ ...event, blockNumber: String(event.blockNumber), value: String(event.value) }));

    expect(transfers, 'all pages contain each matching log once, including self-transfers').toEqual(expected);
  }
}

async function publish(s: Stack, rng: Random, end: bigint, injectFailures: boolean): Promise<void> {
  const bootstrap = createBootstrapStore(s.db);
  const target = await s.adapter.headerByNumber(end);
  const anchor = await s.adapter.headerByNumber(START - 1n);
  await bootstrap.createRun('ethereum', { startBlock: START, target, anchor, segmentBlocks: 19n }, AT);
  const headers = [];

  for (let number = START; number <= end; number += 1n) {
    const header = await s.adapter.headerByNumber(number);
    headers.push({ ...header, transferBloom: s.chain.transfers.some((event) => event.blockNumber === number) });
  }

  await bootstrap.saveHeaders('ethereum', { fromBlock: START, toBlock: end }, headers);
  const segments = shuffled(rng, await bootstrap.pendingSegments('ethereum'));
  let failures = 0;

  for (const [index, segment] of segments.entries()) {
    const events = inRange(s.chain, segment);
    const pageSize = 1 + Math.floor(rng() * 11);

    const commit = (crash: boolean) => bootstrap.commitSegment('ethereum', segment, 'seeded-replay', async (insert) => {
      for (let i = 0; i < events.length; i += pageSize) {
        await insert(events.slice(i, i + pageSize));

        if (crash) throw new Error('injected mid-segment failure');
      }

      return { logs: events.length, requests: Math.ceil(events.length / pageSize) };
    }, AT);

    if (injectFailures && index % 3 === 0 && events.length > pageSize) {
      const before = await dump(s);
      const status = await bootstrap.status('ethereum', AT);
      await expect(commit(true)).rejects.toThrow('injected mid-segment failure');
      expect(await dump(s), `rollback segment ${segment.fromBlock}`).toEqual(before);
      expect(await bootstrap.status('ethereum', AT), 'failed transaction must not publish coverage/counts').toEqual(status);
      failures += 1;
    }

    await commit(false);
    // A committed segment can be retried without invoking fill or adding counts again.
    await bootstrap.commitSegment('ethereum', segment, 'retry', async () => { throw new Error('committed segment replayed'); }, AT);
  }

  if (injectFailures) expect(failures).toBeGreaterThanOrEqual(3);
  expect(await s.store.indexState('ethereum')).toBeUndefined();
  expect(await bootstrap.finalize('ethereum', { at: AT })).toMatchObject({ transfers: inRange(s.chain, { fromBlock: START, toBlock: end }).length });
  expect(await bootstrap.status('ethereum', AT)).toMatchObject({ status: 'done', segmentsApplied: segments.length });
}

for (const seed of SEEDS) {
  it(`bootstrap shuffled segments, rollback and retries equal linear replay (seed=${seed})`, async () => {
    const rng = random(seed);
    const chain = new FakeChain(END);

    for (const event of generate(chain, rng, START, END)) chain.addTransfer(event);
    const actual = await fixture(chain);
    let reference: Stack | undefined;

    try {
      await publish(actual, rng, END, true);
      reference = await fixture(chain);
      await linear(reference, END);
      expect(await dump(actual)).toEqual(await dump(reference));
      expect(await transferRows(actual)).toEqual(expectedRows(chain, END));
      await compareApis(actual, reference, rng);
    } finally { await reference?.close(); await actual.close(); }
  }, 30_000);

  it(`follower and reorging tail converge through finalization (seed=${seed})`, async () => {
    const rng = random(seed);
    const chain = new FakeChain(119n);
    let latest = chain.finalized;
    chain.latest = latest;

    for (const event of generate(chain, rng, START, chain.finalized)) chain.addTransfer(event);
    const actual = await fixture(chain, true);
    let reference: Stack | undefined;

    try {
      await publish(actual, rng, chain.finalized, false);
      await actual.follower.tick(); // Acquire leadership over the published index.
      let rounds = 0;

      while (latest < END) {
        const from = latest + 1n;
        const next = latest + BigInt(22 + Math.floor(rng() * 13));
        latest = next < END ? next : END;
        chain.latest = latest;

        for (const event of generate(chain, rng, from, latest)) chain.addTransfer(event);

        // Bounded catch-up may take two ticks; duplicate ticks must be harmless.
        for (let i = 0; i < 3; i += 1) await actual.tail!.tick();
        expect((await actual.store.tailTip('ethereum'))?.number).toBe(latest);

        const forks = 1 + Math.floor(rng() * 2);

        for (let fork = 0; fork < forks; fork += 1) {
          const depth = 1 + Math.floor(rng() * 8);
          const replacements = generate(chain, rng, latest - BigInt(depth) + 1n, latest);
          // Ensure every fork has different Transfers, even if the random suffix was empty.
          replacements.push({ token: ERC20[2], from: ACCOUNTS[5], to: ACCOUNTS[4], value: BigInt(1000 + rounds * 10 + fork), blockNumber: latest });
          chain.reorg(depth, replacements);

          // Sometimes the follower prunes the surviving prefix before the tail notices the fork.
          if (fork === 0 && rng() < 0.5) {
            chain.finalized = latest - BigInt(11 + Math.floor(rng() * 4));
            await actual.follower.tick();
            expect(actual.follower.status()).toMatchObject({ ready: true, lastError: undefined });
          }

          await actual.tail!.tick();
          expect(actual.tail!.status()).toMatchObject({ healthy: true, lastReorgDepth: BigInt(depth), lastError: undefined });
          expect(await transferRows(actual, true)).toEqual(expectedRows(chain, latest));
        }

        // Finalize a random prefix, retaining a common ancestor above the cursor for the next reorg.
        chain.finalized = latest - BigInt(9 + Math.floor(rng() * 2));
        const account = ACCOUNTS[rounds % ACCOUNTS.length];
        const before = await actual.discovery.discover('ethereum', account);
        expect(before.finality).toBe('latest');
        const beforePages = await history(actual, account, 'latest', 37);
        await actual.follower.tick();
        expect(actual.follower.status()).toMatchObject({ ready: true, lastError: undefined, cursor: { number: chain.finalized } });
        expect(await transferRows(actual, true), 'handoff preserves every canonical row exactly once').toEqual(expectedRows(chain, latest));
        expect((await actual.store.tailBlocks('ethereum')).every((block) => block.number > chain.finalized)).toBe(true);
        const after = await actual.discovery.discover('ethereum', account);
        const counts = (view: typeof before) => view.holdings.map(({ unfinalizedSentCount: _sent, unfinalizedReceivedCount: _received, ...holding }) => holding);
        expect(counts(after), 'handoff preserves discovery totals').toEqual(counts(before));
        const items = (view: typeof before) => view.nftItems.map(({ unfinalized: _tail, ...item }) => item);

        expect(items(after), 'handoff preserves NFT ownership').toEqual(items(before));

        const flatten = (pages: Awaited<ReturnType<typeof history>>) => pages.flatMap((page) => page.transfers ?? [])
          .map(({ unfinalized: _unfinalized, ...event }) => event);

        expect(flatten(await history(actual, account, 'latest', 37)), 'handoff preserves paginated history').toEqual(flatten(beforePages));
        rounds += 1;
      }

      expect(rounds).toBeGreaterThanOrEqual(6);
      expect(actual.tail!.status().reorgs).toBeGreaterThanOrEqual(rounds);
      chain.finalized = END;
      await actual.follower.tick();
      await actual.follower.tick(); // Retry after complete finalization.
      expect(actual.follower.status()).toMatchObject({ ready: true, cursor: { number: END } });
      expect(await actual.store.tailBlocks('ethereum')).toEqual([]);
      expect((await actual.db.execute(sql`SELECT count(*)::int count FROM tail_transfers`)).rows).toEqual([{ count: 0 }]);
      reference = await fixture(chain);
      await linear(reference, END); // Only the surviving canonical chain; no orphaned events.
      expect(await dump(actual)).toEqual(await dump(reference));
      await compareApis(actual, reference, rng);
    } finally { await reference?.close(); await actual.close(); }
  }, 30_000);
}

it('tail NFT burns must not become zero-address ownership before finalization', async () => {
  const chain = new FakeChain(100n);
  chain.latest = 101n;
  chain.addTransfer({ token: ERC721[0], standard: 'erc721', from: ZERO, to: ACCOUNTS[0], value: 7n, blockNumber: 100n });
  chain.addTransfer({ token: ERC721[0], standard: 'erc721', from: ACCOUNTS[0], to: ZERO, value: 7n, blockNumber: 101n });
  const s = await stack(chain, { tail: { maxDepth: 8n } });
  s.clock.at = AT;

  try {
    await s.follower.tick();
    await s.tail!.tick();
    const before = await s.discovery.discover('ethereum', ZERO);
    expect(before.finality).toBe('latest');
    chain.finalized = 101n;
    await s.follower.tick();
    expect(s.follower.status()).toMatchObject({ ready: true, cursor: { number: 101n } });
    const after = await s.discovery.discover('ethereum', ZERO, { finality: 'finalized' });
    expect(after.nftItems).toEqual([]);
    expect(before.nftItems, 'burned NFTs have no current holder, including in the tail').toEqual(after.nftItems);
  } finally { await s.close(); }
}, 30_000);
