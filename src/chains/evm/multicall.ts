import { decodeFunctionResult, encodeFunctionData, erc20Abi, erc721Abi, isHex, multicall3Abi } from 'viem';

import { isString, thrownMessage, toLowerHex } from '../../core/parse.ts';
import type { Address, BalancePair, BalanceResult, Hex, OwnerResult, RpcClient } from '../../core/types.ts';
import { RpcError, rpcErrorOf } from '../../rpc/errors.ts';

/** Multicall3 has the same deterministic address on Ethereum and most EVM chains. */
export const MULTICALL3: Address = '0xca11bde05977b3631167028862be2a173976ca11';

interface EncodedCall {
  target: Address;
  callData: Hex;
}

type CallOutcome = { ok: true; returnData: Hex } | { ok: false; error: string; transient?: true };

const shortReason = (message: string): string =>
  (message.trim().replace(/\s+/g, ' ') || 'multicall failed').slice(0, 160);

/** Only size/gas/revert failures shrink the batch; auth, rate limits, outages and malformed responses propagate. */
const splittable = (error: RpcError | undefined): error is RpcError =>
  error?.kind === 'too_large'
  || (error?.kind === 'fatal' && /revert|out of gas|gas required|exceeds|too large|entity too large/i.test(error.message));

async function aggregateChunk(rpc: RpcClient, calls: EncodedCall[], blockHash: Hex, multicall3: Address, signal?: AbortSignal): Promise<CallOutcome[]> {
  try {
    const data = encodeFunctionData({
      abi: multicall3Abi,
      functionName: 'aggregate3',
      args: [calls.map((call) => ({ ...call, allowFailure: true }))],
    });

    // EIP-1898: pinned to the exact block hash, never re-resolved against another block at the same height.
    const response = await rpc.request<Hex>('eth_call', [{ to: multicall3, data }, { blockHash }], {
      validate: (value) => {
        if (!isString(value) || !isHex(value)) return false;

        try {
          const decoded = decodeFunctionResult({ abi: multicall3Abi, functionName: 'aggregate3', data: value });

          return decoded.length === calls.length;
        } catch {
          return false;
        }
      },
      signal,
    });

    const decoded = decodeFunctionResult({ abi: multicall3Abi, functionName: 'aggregate3', data: response });

    return decoded.map((result) =>
      result.success ? { ok: true, returnData: result.returnData } : { ok: false, error: 'inner call failed' });
  } catch (error) {
    const rpcError = rpcErrorOf(error);

    if (!splittable(rpcError)) throw rpcError ?? error;

    if (calls.length === 1) return [{ ok: false, error: shortReason(rpcError.message) }];
    const middle = Math.floor(calls.length / 2);
    const left = await aggregateChunk(rpc, calls.slice(0, middle), blockHash, multicall3, signal);
    const right = await aggregateChunk(rpc, calls.slice(middle), blockHash, multicall3, signal);

    return [...left, ...right];
  }
}

/** Batches of batchSize calls, two in flight; every call is decoded strictly from exactly one ABI word. */
async function readCalls<T>(
  rpc: RpcClient,
  calls: EncodedCall[],
  blockHash: Hex,
  batchSize: number,
  label: string,
  decode: (data: Hex) => T,
  multicall3: Address,
  signal?: AbortSignal,
): Promise<({ status: 'ok'; value: T } | { status: 'error'; error: string; transient?: true })[]> {
  const chunks: EncodedCall[][] = [];

  for (let i = 0; i < calls.length; i += batchSize) chunks.push(calls.slice(i, i + batchSize));
  const chunksResults: ({ outcomes: CallOutcome[] } | { error: unknown })[] = Array.from({ length: chunks.length });
  let next = 0;

  const worker = async (): Promise<void> => {
    while (next < chunks.length && !signal?.aborted) {
      const index = next++;

      try {
        chunksResults[index] = { outcomes: await aggregateChunk(rpc, chunks[index], blockHash, multicall3, signal) };
      } catch (error) {
        chunksResults[index] = { error };
      }
    }
  };

  await Promise.all(Array.from({ length: Math.min(2, chunks.length) }, worker));
  // Aborted chunks were caught as failures above; never pass them off as per-call transient errors.
  signal?.throwIfAborted();
  const failures = chunksResults.filter((result): result is { error: unknown } => 'error' in result);

  if (failures.length > 0 && failures.length === chunks.length) throw failures[0].error;

  const results = chunksResults.flatMap((result, index) => 'outcomes' in result
    ? result.outcomes
    : chunks[index].map((): CallOutcome => ({ ok: false, error: shortReason(thrownMessage(result.error)), transient: true })));

  return results.map((outcome) => {
    if (!outcome.ok) return outcome.transient
      ? { status: 'error', error: outcome.error, transient: true }
      : { status: 'error', error: outcome.error };

    if (!/^0x[0-9a-fA-F]{64}$/.test(outcome.returnData)) return { status: 'error', error: `invalid ${label} return data` };

    try {
      return { status: 'ok', value: decode(outcome.returnData) };
    } catch (error) {
      return { status: 'error', error: `decode failed: ${shortReason(thrownMessage(error))}` };
    }
  });
}

/** balanceOf for (account, token) pairs of any number of accounts, packed into shared aggregate3 batches. */
export async function readErc20BalancePairs(
  rpc: RpcClient,
  pairs: BalancePair[],
  blockHash: Hex,
  batchSize: number,
  multicall3: Address = MULTICALL3,
  signal?: AbortSignal,
): Promise<BalanceResult[]> {
  const callData = new Map<Address, Hex>();

  const calls = pairs.map(({ account, token }) => {
    let data = callData.get(account);

    if (!data) callData.set(account, data = encodeFunctionData({ abi: erc20Abi, functionName: 'balanceOf', args: [account] }));

    return { target: token, callData: data };
  });

  const results = await readCalls(rpc, calls, blockHash, batchSize, 'balanceOf',
    (data) => decodeFunctionResult({ abi: erc20Abi, functionName: 'balanceOf', data }), multicall3, signal);

  return results.map((r) => r.status === 'ok' ? { status: 'ok', balance: r.value } : r);
}

export const readErc20Balances = (
  rpc: RpcClient,
  account: Address,
  tokens: Address[],
  blockHash: Hex,
  batchSize: number,
): Promise<BalanceResult[]> => readErc20BalancePairs(rpc, tokens.map((token) => ({ account, token })), blockHash, batchSize);

export async function readErc721Owners(
  rpc: RpcClient,
  items: { token: Address; tokenId: bigint }[],
  blockHash: Hex,
  batchSize: number,
  multicall3: Address = MULTICALL3,
  signal?: AbortSignal,
): Promise<OwnerResult[]> {
  const calls = items.map((item) => ({
    target: item.token,
    callData: encodeFunctionData({ abi: erc721Abi, functionName: 'ownerOf', args: [item.tokenId] }),
  }));

  const results = await readCalls(rpc, calls, blockHash, batchSize, 'ownerOf',
    (data) => toLowerHex(decodeFunctionResult({ abi: erc721Abi, functionName: 'ownerOf', data })), multicall3, signal);

  return results.map((r) => r.status === 'ok' ? { status: 'ok', owner: r.value } : r);
}
