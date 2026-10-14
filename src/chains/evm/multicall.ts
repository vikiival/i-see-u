import {
  decodeFunctionResult,
  encodeFunctionData,
  erc20Abi,
  erc721Abi,
  isHex,
  multicall3Abi,
} from 'viem';

import type { Address, BalanceResult, BlockRef, Hex, OwnerResult, RpcClient } from '../../core/types.ts';
import { rpcErrorOf } from './viem.ts';

interface EncodedCall {
  target: Address;
  callData: Hex;
}

type CallOutcome =
  | { status: 'ok'; returnData: Hex }
  | { status: 'error'; error: string };

interface MulticallOptions {
  multicall3: Address;
  batchSize: number;
}

const shortReason = (error: unknown): string => {
  const source = rpcErrorOf(error) ?? error;
  const message = source instanceof Error ? source.message : String(source);
  return (message.trim().replace(/\s+/g, ' ') || 'multicall failed').slice(0, 160);
};

const isWord = (data: Hex): boolean => /^0x[0-9a-fA-F]{64}$/.test(data);

const splittableFailure = (error: unknown): boolean => {
  const rpcError = rpcErrorOf(error);
  const kind = rpcError?.kind;
  if (kind === 'too_large') return true;
  const message = rpcError?.message ?? (error instanceof Error ? error.message : String(error));
  return kind === 'fatal' && /revert|out of gas|gas required|exceeds|too large|entity too large/i.test(message);
};

async function aggregateChunk(
  rpc: RpcClient,
  calls: EncodedCall[],
  at: BlockRef,
  multicall3: Address,
): Promise<CallOutcome[]> {
  try {
    const data = encodeFunctionData({
      abi: multicall3Abi,
      functionName: 'aggregate3',
      args: [calls.map((call) => ({ ...call, allowFailure: true }))],
    });
    const response = await rpc.request<Hex>(
      'eth_call',
      [{ to: multicall3, data }, { blockHash: at.hash }],
      { validate: (value) => typeof value === 'string' && isHex(value) },
    );
    const decoded = decodeFunctionResult({
      abi: multicall3Abi,
      functionName: 'aggregate3',
      data: response,
    });
    if (decoded.length !== calls.length) throw new Error('multicall result length mismatch');

    return decoded.map((result) =>
      result.success
        ? { status: 'ok', returnData: result.returnData }
        : { status: 'error', error: 'inner call failed' },
    );
  } catch (error) {
    const rpcError = rpcErrorOf(error);
    if (!splittableFailure(error)) throw rpcError ?? error;
    if (calls.length === 1) return [{ status: 'error', error: shortReason(rpcError ?? error) }];
    const middle = Math.floor(calls.length / 2);
    const left = await aggregateChunk(rpc, calls.slice(0, middle), at, multicall3);
    const right = await aggregateChunk(rpc, calls.slice(middle), at, multicall3);
    return [...left, ...right];
  }
}

async function executeCalls(
  rpc: RpcClient,
  calls: EncodedCall[],
  at: BlockRef,
  opts: MulticallOptions,
): Promise<CallOutcome[]> {
  if (!Number.isSafeInteger(opts.batchSize) || opts.batchSize < 1) {
    throw new RangeError('batchSize must be a positive safe integer');
  }
  if (calls.length === 0) return [];

  const chunks: EncodedCall[][] = [];
  for (let i = 0; i < calls.length; i += opts.batchSize) chunks.push(calls.slice(i, i + opts.batchSize));

  const results = new Array<CallOutcome[]>(chunks.length);
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < chunks.length) {
      const index = next++;
      results[index] = await aggregateChunk(rpc, chunks[index], at, opts.multicall3);
    }
  };
  await Promise.all(Array.from({ length: Math.min(2, chunks.length) }, worker));
  return results.flat();
}

export async function readErc20Balances(
  rpc: RpcClient,
  account: Address,
  tokens: Address[],
  at: BlockRef,
  opts: MulticallOptions,
): Promise<BalanceResult[]> {
  const calls = tokens.map((target) => ({
    target,
    callData: encodeFunctionData({ abi: erc20Abi, functionName: 'balanceOf', args: [account] }),
  }));
  const outcomes = await executeCalls(rpc, calls, at, opts);

  return outcomes.map((outcome, index) => {
    const token = tokens[index];
    if (outcome.status === 'error') return { token, status: 'error', error: outcome.error };
    if (!isWord(outcome.returnData)) return { token, status: 'error', error: 'invalid balanceOf return data' };
    try {
      const balance = decodeFunctionResult({ abi: erc20Abi, functionName: 'balanceOf', data: outcome.returnData });
      return { token, status: 'ok', balance };
    } catch (error) {
      return { token, status: 'error', error: `decode failed: ${shortReason(error)}` };
    }
  });
}

export async function readErc721Owners(
  rpc: RpcClient,
  items: { token: Address; tokenId: bigint }[],
  at: BlockRef,
  opts: MulticallOptions,
): Promise<OwnerResult[]> {
  const calls = items.map((item) => ({
    target: item.token,
    callData: encodeFunctionData({ abi: erc721Abi, functionName: 'ownerOf', args: [item.tokenId] }),
  }));
  const outcomes = await executeCalls(rpc, calls, at, opts);

  return outcomes.map((outcome, index) => {
    const item = items[index];
    const identity = { token: item.token, tokenId: item.tokenId };
    if (outcome.status === 'error') return { ...identity, status: 'error', error: outcome.error };
    if (!isWord(outcome.returnData)) return { ...identity, status: 'error', error: 'invalid ownerOf return data' };
    try {
      const owner = decodeFunctionResult({
        abi: erc721Abi,
        functionName: 'ownerOf',
        data: outcome.returnData,
      }).toLowerCase() as Address;
      return { ...identity, status: 'ok', owner };
    } catch (error) {
      return { ...identity, status: 'error', error: `decode failed: ${shortReason(error)}` };
    }
  });
}
