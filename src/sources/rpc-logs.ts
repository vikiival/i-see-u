import { scanAccountChunk } from '../chains/evm/logs.ts';
import type { AccountLogSource, AccountLogSourceStats, Address, BlockRange, RpcClient, RpcRequestOptions } from '../core/types.ts';

const shortError = (error: unknown): string =>
  (error instanceof Error ? error.message : String(error)).trim().replace(/\s+/g, ' ').slice(0, 200);

/** Existing two-direction eth_getLogs backfill, exposed through the chain-agnostic source contract. */
export function createRpcLogSource(
  rpc: RpcClient,
  options: { preferredMaxRange: bigint; minRange: bigint; name?: string },
): AccountLogSource {
  const name = options.name ?? 'rpc';
  const state: AccountLogSourceStats = { requests: 0, chunks: 0, logs: 0, failures: 0, fallbacks: 0 };

  return {
    name,
    preferredMaxRange: options.preferredMaxRange,
    adaptiveRange: { minRange: options.minRange },
    async scanAccount(account: Address, range: BlockRange) {
      state.chunks += 1;
      let calls = 0;

      const counted: RpcClient = {
        request<T>(method: string, params: unknown[], requestOptions?: RpcRequestOptions) {
          calls += 1;

          return rpc.request<T>(method, params, requestOptions);
        },
        stats: () => rpc.stats(),
      };

      try {
        const result = await scanAccountChunk(counted, account, range);
        state.logs += result.logs;

        return result;
      } catch (error) {
        state.failures += 1;
        state.lastError = shortError(error);
        throw error;
      } finally {
        state.requests += calls;
      }
    },
    stats: () => ({ [name]: { ...state } }),
  };
}
