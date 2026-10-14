import { expect, test } from 'vitest';
import type { Address, RpcClient, RpcErrorKind } from '../src/core/types.ts';
import { scanTransfers } from '../src/chains/evm/logs.ts';
import { readErc20Balances } from '../src/chains/evm/multicall.ts';
import { RpcError } from '../src/rpc/errors.ts';

const failingRpc = (kind: RpcErrorKind, message: string = kind) => {
  let calls = 0;
  const rpc: RpcClient = {
    async request() {
      calls += 1;
      throw new RpcError(kind, message);
    },
    stats: () => ({ requests: calls, attempts: calls, byMethod: {}, errors: {}, endpoints: [] }),
  };
  return { rpc, calls: () => calls };
};

const account = '0x675265996dfa20c9a21c31bcdf32ddbbf865f68a' as Address;

for (const kind of ['fatal', 'transient', 'rate_limited'] as const) {
  test(`getLogs does not split the range on non-timeout ${kind} errors`, async () => {
    const { rpc, calls } = failingRpc(kind);
    await expect(
      scanTransfers(rpc, account, 'to', { fromBlock: 0n, toBlock: 9_999n }, () => {}),
    ).rejects.toMatchObject({ kind });
    expect(calls()).toBe(1);
  });
}

for (const [kind, message] of [['fatal', 'HTTP 401'], ['transient', 'HTTP 503'], ['rate_limited', 'HTTP 429']] as const) {
  test(`multicall does not halve the batch on ${kind} errors (${message})`, async () => {
    const { rpc, calls } = failingRpc(kind, message);
    const tokens = Array.from({ length: 1000 }, (_, i) => `0x${(i + 1).toString(16).padStart(40, '0')}` as Address);
    await expect(readErc20Balances(rpc, account, tokens, '0x01', 1000)).rejects.toMatchObject({ kind, message });
    expect(calls()).toBe(1);
  });
}
