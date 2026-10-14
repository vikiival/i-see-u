import { expect, test } from 'vitest';

import { decodeFunctionData, encodeFunctionResult } from 'viem';

import { MULTICALL3, readErc20Balances, readErc721Owners } from '../src/chains/evm/multicall.ts';
import type { Address, Hex, RpcClient, RpcStats } from '../src/core/types.ts';
import { RpcError } from '../src/rpc/errors.ts';

const aggregate3Abi = [
  {
    type: 'function',
    name: 'aggregate3',
    stateMutability: 'payable',
    inputs: [{
      name: 'calls',
      type: 'tuple[]',
      components: [
        { name: 'target', type: 'address' },
        { name: 'allowFailure', type: 'bool' },
        { name: 'callData', type: 'bytes' },
      ],
    }],
    outputs: [{
      name: 'returnData',
      type: 'tuple[]',
      components: [
        { name: 'success', type: 'bool' },
        { name: 'returnData', type: 'bytes' },
      ],
    }],
  },
] as const;

const balanceOfAbi = [{
  type: 'function',
  name: 'balanceOf',
  stateMutability: 'view',
  inputs: [{ name: 'account', type: 'address' }],
  outputs: [{ name: 'balance', type: 'uint256' }],
}] as const;

const ownerOfAbi = [{
  type: 'function',
  name: 'ownerOf',
  stateMutability: 'view',
  inputs: [{ name: 'tokenId', type: 'uint256' }],
  outputs: [{ name: 'owner', type: 'address' }],
}] as const;

const address = (n: number): Address => `0x${n.toString(16).padStart(40, '0')}` as Address;
const blockHash = (number: bigint): Hex => `0x${number.toString(16).padStart(64, 'a')}`;

const emptyStats = (): RpcStats => ({ requests: 0, attempts: 0, byMethod: {}, errors: {}, endpoints: [] });

class FakeRpc implements RpcClient {
  readonly handler: (method: string, params: unknown[]) => unknown | Promise<unknown>;

  constructor(handler: (method: string, params: unknown[]) => unknown | Promise<unknown>) {
    this.handler = handler;
  }

  async request<T>(method: string, params: unknown[]): Promise<T> {
    return await this.handler(method, params) as T;
  }

  stats(): RpcStats {
    return emptyStats();
  }
}

const callsFrom = (params: unknown[]) => {
  const transaction = params[0] as { to: Address; data: Hex };
  expect(transaction.to).toBe(MULTICALL3);
  const decoded = decodeFunctionData({ abi: aggregate3Abi, data: transaction.data });
  return decoded.args[0];
};

test('ERC-20 calls round-trip through aggregate3 and are pinned to the requested block', async () => {
  const account = address(42);
  const tokens = [address(1), address(2), address(3)];
  let receivedMethod: string | undefined;
  let receivedParams: unknown[] | undefined;
  let receivedCalls: ReturnType<typeof callsFrom> | undefined;
  const rpc = new FakeRpc((method, params) => {
    receivedMethod = method;
    receivedParams = params;
    const calls = callsFrom(params);
    receivedCalls = calls;
    return encodeFunctionResult({
      abi: aggregate3Abi,
      functionName: 'aggregate3',
      result: [
        { success: true, returnData: encodeFunctionResult({ abi: balanceOfAbi, functionName: 'balanceOf', result: 11n }) },
        { success: false, returnData: '0x' },
        { success: true, returnData: '0x12' },
      ],
    });
  });

  const result = await readErc20Balances(rpc, account, tokens, blockHash(123n), 10);
  expect(receivedMethod).toBe('eth_call');
  expect(receivedParams?.[1]).toEqual({ blockHash: blockHash(123n) });
  expect(receivedCalls?.map((call) => call.target.toLowerCase())).toEqual(tokens);
  for (const call of receivedCalls ?? []) {
    expect(call.allowFailure).toBe(true);
    const inner = decodeFunctionData({ abi: balanceOfAbi, data: call.callData });
    expect(inner.args[0].toLowerCase()).toBe(account);
  }
  expect(result).toEqual([
    { status: 'ok', balance: 11n },
    { status: 'error', error: 'inner call failed' },
    { status: 'error', error: 'invalid balanceOf return data' },
  ]);
});

test('out-of-gas failures recursively halve and preserve input order', async () => {
  const tokens = [address(10), address(11), address(12), address(13)];
  const seenSizes: number[] = [];
  const rpc = new FakeRpc((_method, params) => {
    const calls = callsFrom(params);
    seenSizes.push(calls.length);
    if (calls.length > 1) throw new RpcError('fatal', 'execution ran out of gas');
    const value = BigInt(tokens.indexOf(calls[0].target.toLowerCase() as Address) + 100);
    return encodeFunctionResult({
      abi: aggregate3Abi,
      functionName: 'aggregate3',
      result: [{
        success: true,
        returnData: encodeFunctionResult({ abi: balanceOfAbi, functionName: 'balanceOf', result: value }),
      }],
    });
  });

  const result = await readErc20Balances(rpc, address(50), tokens, blockHash(77n), 4);
  expect(seenSizes).toEqual([4, 2, 1, 1, 2, 1, 1]);
  expect(result).toEqual([100n, 101n, 102n, 103n].map((balance) => ({ status: 'ok', balance })));
});

test('ERC-721 ownerOf results decode and normalize owner addresses', async () => {
  const token = address(70);
  const items = [{ token, tokenId: 5n }, { token, tokenId: 9n }];
  const owner = address(88);
  const rpc = new FakeRpc((_method, params) => {
    expect(params[1]).toEqual({ blockHash: blockHash(500n) });
    const calls = callsFrom(params);
    expect(calls.map((call) => decodeFunctionData({ abi: ownerOfAbi, data: call.callData }).args[0])).toEqual([5n, 9n]);
    return encodeFunctionResult({
      abi: aggregate3Abi,
      functionName: 'aggregate3',
      result: calls.map(() => ({
        success: true,
        returnData: encodeFunctionResult({ abi: ownerOfAbi, functionName: 'ownerOf', result: owner }),
      })),
    });
  });

  expect(await readErc721Owners(rpc, items, blockHash(500n), 100))
    .toEqual(items.map(() => ({ status: 'ok', owner })));
});
