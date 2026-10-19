import { expect, test } from 'vitest';

import { address, FakeChain } from './fake-chain.ts';
import { blockRange, header, headersFor, stack } from './stack.ts';

const account = address(1);

test('first and repeated discoveries are pure DB reads for any account, including an empty account', async () => {
  const chain = new FakeChain(102n);
  chain.addTransfer({ token: address(3), from: address(2), to: account, value: 7n, blockNumber: 101n });
  const s = await stack(chain);

  try {
    await s.follower.tick();
    const calls = chain.total;
    expect(await s.discovery.discover('ethereum', `0x${account.slice(2).toUpperCase()}`)).toMatchObject({
      status: 'ready', account, blockHeight: 102n, holdings: [{ token: address(3), receivedCount: 1n }],
    });
    expect((await s.discovery.discover('ethereum', address(99))).holdings).toEqual([]);
    await s.discovery.discover('ethereum', account);
    expect(chain.total).toBe(calls);
  } finally { await s.close(); }
});

test('one network serving mode applies to all accounts during catch-up', async () => {
  const s = await stack(new FakeChain(103n), { range: 2n, maxRanges: 1 });

  try {
    for (const who of [account, address(2)]) await expect(s.discovery.discover('ethereum', who)).rejects.toMatchObject({ statusCode: 503, serviceStatus: 'not_ready' });
    await s.follower.tick();
    expect(await s.discovery.discover('ethereum', account)).toMatchObject({ status: 'ready', stale: true, staleReason: 'Global index has not reached the finalized head' });
    await s.follower.tick();
    expect((await s.discovery.discover('ethereum', account)).status).toBe('ready');
  } finally { await s.close(); }
});

test('unobserved heads are unavailable; expired observations and stalled progress serve stale snapshots', async () => {
  const s = await stack();

  try {
    await expect(s.discovery.discover('ethereum', account)).rejects.toThrow('finalized head');
    await s.follower.tick();
    s.clock.at += 120_001;
    expect(await s.discovery.discover('ethereum', account)).toMatchObject({ stale: true, staleReason: 'Finalized head observation is too old' });
    await s.follower.tick();
    expect((await s.discovery.discover('ethereum', account)).status).toBe('ready');
    s.clock.at += 1_800_001;
    await s.follower.tick();
    expect(await s.discovery.discover('ethereum', account)).toMatchObject({ stale: true, staleReason: 'Global index has not advanced recently enough' });
  } finally { await s.close(); }
});

test('pending repairs block discovery until replacement resolves the suspect range', async () => {
  const s = await stack();

  try {
    await s.follower.tick();
    await s.store.enqueueRepair('ethereum', blockRange(101n), 'suspect', s.clock.at);
    await expect(s.discovery.discover('ethereum', account)).rejects.toThrow('pending repairs');
    await s.store.replaceRange('ethereum', blockRange(101n), headersFor(101n), []);
    expect(await s.discovery.discover('ethereum', account)).toMatchObject({ revision: 1n });
  } finally { await s.close(); }
});

test('network and account validation precede database reads, including prototype names', async () => {
  const s = await stack();

  try {
    for (const name of ['missing', 'constructor', '__proto__', 'toString']) await expect(s.discovery.discover(name, account)).rejects.toMatchObject({ statusCode: 404 });
    await expect(s.discovery.discover('ethereum', 'bad')).rejects.toMatchObject({ statusCode: 400 });
    expect(s.chain.total).toBe(0);
  } finally { await s.close(); }
});

test('a newly observed target makes all API replicas stale, without waiting for their follower poll', async () => {
  const s = await stack();

  try {
    await s.follower.tick();
    expect(s.follower.status().ready).toBe(true);
    await s.store.observeHead('ethereum', header(103n), s.clock.at);
    expect(await s.discovery.discover('ethereum', account)).toMatchObject({ status: 'ready', stale: true, staleReason: 'Global index has not reached the finalized head' });
    s.chain.finalized = 103n;
    await s.follower.tick();
    expect(await s.discovery.discover('ethereum', account)).toMatchObject({ blockHeight: 103n });
  } finally { await s.close(); }
});
