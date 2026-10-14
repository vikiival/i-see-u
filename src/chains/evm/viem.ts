import { createPublicClient, custom } from 'viem';
import type { Chain } from 'viem';

import type { RpcClient, RpcErrorKind, RpcErrorShape } from '../../core/types.ts';

const rpcErrorKinds = new Set<RpcErrorKind>([
  'rate_limited',
  'transient',
  'too_large',
  'unsupported',
  'fatal',
]);

export function createViemClient(rpc: RpcClient, chain: Chain) {
  return createPublicClient({
    batch: { multicall: false },
    chain,
    // RpcClient owns retries, failover, billing, and batching policy.
    transport: custom({
      request: ({ method, params }) => rpc.request(method, params ? [...params] : []),
    }, { retryCount: 0 }),
  });
}

export type ViemClient = ReturnType<typeof createViemClient>;

export function rpcErrorOf(error: unknown): RpcErrorShape | undefined {
  const seen = new Set<unknown>();
  let current = error;
  while (current && typeof current === 'object' && !seen.has(current)) {
    seen.add(current);
    const candidate = current as Partial<RpcErrorShape>;
    if (candidate instanceof Error && rpcErrorKinds.has((candidate as { kind?: RpcErrorKind }).kind!)) {
      return candidate as RpcErrorShape;
    }
    current = (current as { cause?: unknown }).cause;
  }
  return undefined;
}
