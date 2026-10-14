import type { Chain } from 'viem';
import { mainnet } from 'viem/chains';

import { MULTICALL3 } from './chains/evm/multicall.ts';
import type { Address } from './core/types.ts';

export interface EvmNetworkDefinition {
  name: string;
  chainId: number;
  chain: Chain;
  startBlock: bigint;
  hypersyncUrl: string;
  multicall3: Address;
}

/** Static per-network facts. Adding an EVM network starts with one entry here. */
export const networkRegistry: Record<string, EvmNetworkDefinition> = {
  ethereum: {
    name: 'ethereum',
    chainId: mainnet.id,
    chain: mainnet,
    startBlock: 25_933_634n,
    hypersyncUrl: 'https://eth.hypersync.xyz',
    multicall3: MULTICALL3,
  },
};
