import { expect, test } from 'vitest';
import type { Address, BlockRef, Hex, RpcClient, RpcErrorKind } from '../src/core/types.ts';
import { scanTransfers } from '../src/chains/evm/logs.ts';
import { readErc20Balances } from '../src/chains/evm/multicall.ts';
import { RpcError } from '../src/rpc/errors.ts';

const failingRpc = (kind: RpcErrorKind) => {
  let calls = 0;
  const rpc: RpcClient = {
    async request() {
      calls += 1;
      throw new RpcError(kind, kind);
    },
    stats: () => ({ requests: calls, attempts: calls, byMethod: {}, errors: {}, endpoints: [] }),
  };
  return { rpc, calls: () => calls };
};

const account = '0x675265996dfa20c9a21c31bcdf32ddbbf865f68a' as Address;
const at: BlockRef = {
  number: 1n,
  hash: '0x01' as Hex,
  parentHash: '0x00' as Hex,
  timestamp: 1n,
};

for (const kind of ['fatal', 'transient', 'rate_limited'] as const) {
  test(`getLogs does not split the range on non-timeout ${kind} errors`, async () => {
    const { rpc, calls } = failingRpc(kind);
    await expect(
      scanTransfers(rpc, account, 'to', { fromBlock: 0n, toBlock: 9_999n }, () => {}),
    ).rejects.toMatchObject({ kind });
    expect(calls()).toBe(1);
  });
}

for (const kind of ['transient', 'rate_limited'] as const) {
  test(`multicall does not halve the batch on ${kind} errors`, async () => {
    const { rpc, calls } = failingRpc(kind);
    const tokens = Array.from({ length: 1000 }, (_, i) => `0x${(i + 1).toString(16).padStart(40, '0')}` as Address);
    await expect(
      readErc20Balances(rpc, account, tokens, at, {
        multicall3: '0xca11bde05977b3631167028862be2a173976ca11',
        batchSize: 1000,
      }),
    ).rejects.toMatchObject({ kind });
    expect(calls()).toBe(1);
  });
}
