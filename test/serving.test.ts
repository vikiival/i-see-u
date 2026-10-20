import { sql } from 'drizzle-orm';
import { expect, test } from 'vitest';

import { createBootstrapStore } from '../src/bootstrap/store.ts';
import type { BlockRange } from '../src/core/types.ts';
import { address, blockHash, FakeChain } from './fake-chain.ts';
import { blockRange, header, headersFor, stack } from './stack.ts';

const [account, peer, token, missingToken, nft] = [1, 2, 3, 4, 5].map(address);
const path = `/v1/ethereum/accounts/${account}`;
const behind = 'Global index has not reached the finalized head';

async function lagging() {
  const chain = new FakeChain(100n);
  chain.latest = 102n;
  chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber: 100n });
  chain.addTransfer({ token: missingToken, from: peer, to: account, value: 2n, blockNumber: 102n });
  const s = await stack(chain, { tail: {} });
  await s.follower.tick();
  await s.tail!.tick();
  await s.store.observeHead('ethereum', header(101n), s.clock.at);

  return s;
}

test('lagging discovery and history return 200 at the cursor, excluding a healthy tail', async () => {
  const s = await lagging();

  try {
    const calls = s.chain.total;

    for (const suffix of ['erc20', 'erc721', 'transfers']) {
      const response = await s.app.request(`${path}/${suffix}`);
      expect(response.status).toBe(200);
      expect(response.headers.get('retry-after')).toBeNull();
      const body = await response.json();
      expect(body).toMatchObject({
        status: 'ready',
        complete: true,
        stale: true,
        staleReason: behind,
        finality: 'finalized',
        blockHeight: '100',
        blockHash: blockHash(100n),
        followerLagBlocks: '1',
        lagBlocks: '2',
        tailError: behind,
      });

      if (suffix === 'erc20') {
        expect(body.tokens).toMatchObject([{ contract: token, receivedCount: '1' }]);
      }

      if (suffix === 'transfers') {
        expect(body.transfers).toMatchObject([{ blockNumber: '100' }]);
      }
    }

    const explicit = await (await s.app.request(`${path}/transfers?finality=finalized`)).json();
    expect(explicit).toMatchObject({ stale: true, blockHeight: '100' });
    expect(explicit.tailError).toBeUndefined();
    const health = await s.app.request('/health');
    expect(health.status).toBe(200);
    expect(await health.json()).toMatchObject({
      status: 'not_ready',
      networks: [{ ready: false, serving: 'stale', reason: behind }],
    });
    expect(s.chain.total).toBe(calls);
  } finally {
    await s.close();
  }
});

test('arbitrarily large observed lag serves the cursor and health stays 200', async () => {
  const s = await lagging();

  try {
    await s.store.observeHead('ethereum', header(100_000n), s.clock.at);

    for (const suffix of ['erc20', 'erc721', 'transfers']) {
      const response = await s.app.request(`${path}/${suffix}`);
      expect(response.status).toBe(200);
      expect(response.headers.get('retry-after')).toBeNull();
      expect(await response.json()).toMatchObject({
        complete: true,
        stale: true,
        staleReason: behind,
        blockHeight: '100',
        followerLagBlocks: '99900',
      });
    }

    const health = await s.app.request('/health');
    expect(health.status).toBe(200);
    expect(health.headers.get('retry-after')).toBeNull();
    expect(await health.json()).toMatchObject({
      status: 'not_ready',
      networks: [{ serving: 'stale', reason: behind }],
    });
  } finally {
    await s.close();
  }
});

test.each(['repair', 'hash', 'both'])(
  '%s inconsistency serves stale with an explicit flag even with an expired observation',
  async (problem) => {
    const s = await lagging();

    try {
      const repair = problem !== 'hash';

      if (repair) {
        await s.store.enqueueRepair('ethereum', blockRange(100n), 'suspect', s.clock.at);
      }

      if (problem !== 'repair') {
        // Same-height mismatch without a repair queue entry, as seen before the follower queues it.
        await s.db.execute(
          sql`UPDATE ingestion_state SET head_number = 100, head_hash = decode(${blockHash(999n).slice(2)}, 'hex')`,
        );
      }

      s.clock.at += 2_000_000;

      for (const suffix of ['erc20', 'erc721', 'transfers']) {
        const response = await s.app.request(`${path}/${suffix}`);
        expect(response.status).toBe(200);
        expect(response.headers.get('retry-after')).toBeNull();
        expect(await response.json()).toMatchObject({
          status: 'ready',
          complete: true,
          stale: true,
          blockHeight: '100',
          finality: 'finalized',
          staleReason: expect.stringMatching(repair ? /pending repairs/ : /hash mismatch/),
          ...(repair && { repairPending: true, note: expect.stringContaining('may change') }),
          ...(problem !== 'repair' && { headHashMismatch: true }),
        });
      }

      expect(await s.discovery.discover('ethereum', account)).toMatchObject({
        holdings: [{ token, receivedCount: 1n }],
      });

      if (problem === 'repair') {
        await s.store.replaceRange('ethereum', blockRange(100n), headersFor(100n), []);
        const repaired = await s.discovery.discover('ethereum', account);
        expect(repaired).toMatchObject({ holdings: [], revision: 1n });
        expect(repaired).not.toHaveProperty('repairPending');
        expect((await s.app.request(`${path}/transfers?before=100:0:100:0`)).status).toBe(409);
      }

      const health = await s.app.request('/health');
      expect(health.status).toBe(200);
      expect(await health.json()).toMatchObject({ status: 'not_ready', networks: [{ serving: 'stale' }] });
    } finally {
      await s.close();
    }
  },
);

test.each(['observation', 'progress'])(
  'old %s alone serves stale discovery and history with the original reason',
  async (reason) => {
    const s = await stack();

    try {
      await s.follower.tick();
      s.clock.at += reason === 'observation' ? 120_001 : 1_800_001;

      if (reason === 'progress') {
        await s.store.observeHead('ethereum', header(102n), s.clock.at);
      }

      for (const suffix of ['erc20', 'transfers']) {
        const response = await s.app.request(`${path}/${suffix}`);
        expect(response.status).toBe(200);
        expect(await response.json()).toMatchObject({
          stale: true,
          staleReason:
            reason === 'observation'
              ? 'Finalized head observation is too old'
              : 'Global index has not advanced recently enough',
          blockHeight: '102',
          followerLagBlocks: '0',
        });
      }
    } finally {
      await s.close();
    }
  },
);

async function partialFixture() {
  const chain = new FakeChain(111n);

  for (let blockNumber = 100n; blockNumber <= 111n; blockNumber += 1n) {
    chain.addTransfer({ token, from: peer, to: account, value: 1n, blockNumber });
  }

  chain.addTransfer({ token: missingToken, from: peer, to: account, value: 1n, blockNumber: 104n });
  chain.addTransfer({ token: nft, standard: 'erc721', from: peer, to: account, value: 7n, blockNumber: 100n });
  chain.addTransfer({ token: nft, standard: 'erc721', from: account, to: peer, value: 7n, blockNumber: 104n });
  const s = await stack(chain, { initialize: false, tail: {} });
  const bootstrap = createBootstrapStore(s.db);
  await bootstrap.createRun(
    'ethereum',
    { startBlock: 100n, target: header(111n), anchor: header(99n), segmentBlocks: 2n },
    s.clock.at,
  );
  await bootstrap.saveHeaders(
    'ethereum',
    blockRange(100n, 111n),
    headersFor(100n, 111n).map((h) => ({ ...h, transferBloom: true })),
  );

  const commit = async (range: BlockRange) => {
    const events = chain.transfers.filter((e) => e.blockNumber >= range.fromBlock && e.blockNumber <= range.toBlock);
    await bootstrap.commitSegment(
      'ethereum',
      range,
      'test',
      async (insert) => {
        await insert(events);

        return { logs: events.length, requests: 1 };
      },
      s.clock.at,
    );
  };

  return { ...s, bootstrap, commit };
}

test('out-of-order applied segments serve partial holdings, NFTs and paged history with merged coverage and no RPC', async () => {
  const s = await partialFixture();

  try {
    for (const start of [110n, 108n, 100n]) {
      await s.commit(blockRange(start, start + 1n));
    }

    // A failed segment's raw and summary writes roll back and cannot appear in coverage or account rows.
    await expect(
      s.bootstrap.commitSegment(
        'ethereum',
        blockRange(102n, 103n),
        'test',
        async (insert) => {
          await insert(s.chain.transfers.filter((e) => e.blockNumber === 102n));
          throw new Error('failed segment');
        },
        s.clock.at,
      ),
    ).rejects.toThrow('failed segment');
    // Legacy done-but-unapplied segments do not count as coverage.
    await s.db.execute(sql`UPDATE bootstrap_segments SET done = true WHERE from_block = 106`);

    const expected = {
      status: 'partial',
      complete: false,
      startBlock: '100',
      fromBlock: '100',
      targetBlock: '111',
      targetHash: blockHash(111n),
      coveredRanges: [
        { fromBlock: '100', toBlock: '101' },
        { fromBlock: '108', toBlock: '111' },
      ],
      coveredBlocks: '6',
      totalBlocks: '12',
      finality: 'finalized',
      note: expect.stringContaining('lower bounds'),
      tailError: 'Global index bootstrap is incomplete',
    };

    for (const suffix of ['erc20?balances=true', 'erc721?verifyOwnership=true', 'transfers']) {
      const response = await s.app.request(`${path}/${suffix}`);
      expect(response.status).toBe(200);
      expect(response.headers.get('retry-after')).toBeNull();
      const body = await response.json();
      expect(body).toMatchObject(expected);
      expect(body.blockHeight).toBeUndefined();
      expect(body.blockHash).toBeUndefined();

      if (suffix.startsWith('erc20')) {
        expect(body).toMatchObject({
          balancesCurrent: false,
          balancesStatus: 'omitted',
          balancesReason: expect.stringContaining('incomplete'),
        });
        expect(body.tokens).toEqual([
          { contract: token, firstSeenBlock: '100', lastSeenBlock: '111', sentCount: '0', receivedCount: '6' },
        ]);
      }

      if (suffix.startsWith('erc721')) {
        expect(body).toMatchObject({
          collections: [{ tokens: [{ tokenId: '7', ownerStatus: 'skipped' }] }],
          ownershipError: expect.stringContaining('complete snapshot'),
        });
      }

      if (suffix === 'transfers') {
        expect(body.transfers.map((e: { blockNumber: string }) => e.blockNumber)).toEqual([
          '111',
          '110',
          '109',
          '108',
          '101',
          '100',
          '100',
        ]);
      }
    }

    const first = await (await s.app.request(`${path}/transfers?limit=2`)).json();
    const second = await (await s.app.request(`${path}/transfers?limit=2&before=${first.next}`)).json();
    expect(second).toMatchObject({
      ...expected,
      throughBlock: '111',
      transfers: [{ blockNumber: '109' }, { blockNumber: '108' }],
    });
    await s.commit(blockRange(102n, 103n));
    expect((await s.app.request(`${path}/transfers?before=${first.next}`)).status).toBe(409);
    expect(s.chain.total).toBe(0);
    expect(await s.discovery.status('ethereum')).toMatchObject({ ready: false, serving: 'partial' });
  } finally {
    await s.close();
  }
});

test('no run and a created run with zero applied blocks both serve empty partial data', async () => {
  const s = await stack(new FakeChain(102n), { initialize: false });

  try {
    const calls = s.chain.total;

    for (const suffix of ['erc20?balances=true', 'erc721?verifyOwnership=true', 'transfers']) {
      const response = await s.app.request(`${path}/${suffix}`);
      expect(response.status).toBe(200);
      expect(response.headers.get('retry-after')).toBeNull();
      const body = await response.json();
      expect(body).toMatchObject({
        status: 'partial',
        complete: false,
        startBlock: '100',
        coveredRanges: [],
        coveredBlocks: '0',
      });
      expect(body.targetBlock).toBeUndefined();
      expect(body.targetHash).toBeUndefined();
      expect(body.totalBlocks).toBeUndefined();
      expect(body.lastProgressAt).toBeUndefined();

      if (suffix.startsWith('erc20')) {
        expect(body).toMatchObject({ tokens: [], balancesStatus: 'omitted' });
      }

      if (suffix.startsWith('erc721')) {
        expect(body).toMatchObject({ collections: [], ownershipError: expect.stringContaining('complete snapshot') });
      }

      if (suffix === 'transfers') {
        expect(body).toMatchObject({ transfers: [], next: null, throughBlock: '0' });
      }
    }

    expect(await s.discovery.discover('ethereum', account)).toMatchObject({ holdings: [], nftItems: [] });
    expect(s.chain.total).toBe(calls);
    const emptyHealth = await s.app.request('/health');
    expect(emptyHealth.status).toBe(200);
    expect(await emptyHealth.json()).toMatchObject({
      status: 'not_ready',
      networks: [{ ready: false, serving: 'partial' }],
    });
    expect(await s.discovery.status('ethereum')).toMatchObject({ serving: 'partial' });
    expect((await s.app.request(`${path}/transfers?before=100:0:100:0:partial`)).status).toBe(409);
    await createBootstrapStore(s.db).createRun(
      'ethereum',
      { startBlock: 100n, target: header(102n), anchor: header(99n), segmentBlocks: 2n },
      s.clock.at,
    );

    for (const suffix of ['erc20', 'transfers']) {
      const response = await s.app.request(`${path}/${suffix}?finality=finalized`);
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body).toMatchObject({ status: 'partial', coveredRanges: [], coveredBlocks: '0', totalBlocks: '3' });
      expect(body.tailError).toBeUndefined();
    }

    const health = await s.app.request('/health');
    expect(health.status).toBe(200);
    expect(await health.json()).toMatchObject({
      status: 'not_ready',
      networks: [{ ready: false, serving: 'partial' }],
    });
  } finally {
    await s.close();
  }
});

test('publication switches partial to full atomically and invalidates partial pagination', async () => {
  const s = await partialFixture();

  try {
    for (let start = 100n; start <= 111n; start += 2n) {
      await s.commit(blockRange(start, start + 1n));
    }

    const first = await (await s.app.request(`${path}/transfers?limit=1`)).json();
    expect(first).toMatchObject({ status: 'partial', complete: false, coveredBlocks: '12' });
    await s.bootstrap.finalize('ethereum', { at: s.clock.at });
    expect((await s.app.request(`${path}/transfers?before=${first.next}`)).status).toBe(409);
    const response = await s.app.request(`${path}/erc20`);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      status: 'ready',
      complete: true,
      stale: false,
      blockHeight: '111',
      tokens: [expect.any(Object), expect.any(Object)],
    });
    expect(await s.discovery.status('ethereum')).toEqual({ ready: true, serving: 'full' });
    const health = await s.app.request('/health');
    expect(health.status).toBe(200);
    expect(health.headers.get('retry-after')).toBeNull();
    expect(await health.json()).toMatchObject({ status: 'ready', networks: [{ ready: true, serving: 'full' }] });
    expect(await s.store.nftItems('ethereum', account)).toEqual([]);
  } finally {
    await s.close();
  }
});

test('published index without a head observation serves stale at the cursor', async () => {
  const s = await stack();

  try {
    await s.follower.tick();
    await s.db.execute(
      sql`UPDATE ingestion_state SET head_number = NULL, head_hash = NULL, head_timestamp = NULL, checked_at = NULL`,
    );

    for (const suffix of ['erc20', 'erc721', 'transfers']) {
      const response = await s.app.request(`${path}/${suffix}`);
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({
        stale: true,
        staleReason: 'Global index awaits finalized head observation',
        blockHeight: '102',
        followerLagBlocks: '0',
      });
    }

    const health = await s.app.request('/health');
    expect(health.status).toBe(200);
    expect(await health.json()).toMatchObject({ status: 'not_ready', networks: [{ serving: 'stale' }] });
  } finally {
    await s.close();
  }
});

test('database acquisition failures still return 503 with Retry-After for accounts and health', async () => {
  const s = await stack();

  const fail = async () => {
    throw new Error('timeout exceeded when trying to connect');
  };

  s.store.readAccount = fail;
  s.store.transferHistory = fail;
  s.store.indexSnapshot = fail;

  try {
    for (const url of [`${path}/erc20`, `${path}/erc721`, `${path}/transfers`, '/health']) {
      const response = await s.app.request(url);
      expect(response.status).toBe(503);
      expect(response.headers.get('retry-after')).not.toBeNull();
    }
  } finally {
    await s.close();
  }
});
