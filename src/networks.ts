import type { Chain } from 'viem';
import { mainnet } from 'viem/chains';

import { MULTICALL3 } from './chains/evm/multicall.ts';
import type { Address } from './core/types.ts';

export interface EvmNetworkDefinition {
  name: string;
  /** Stable database ID, independent of the chain ID; never renumber. */
  id: number;
  chainId: number;
  chain: Chain;
  startBlock: bigint;
  hypersyncUrl: string;
  multicall3: Address;
  /** Tokens whose balances change without a Transfer log: a cached balance is never reused at a newer block. */
  alwaysRefreshTokens: readonly Address[];
}

/**
 * Ethereum mainnet starter list (not exhaustive; unknown ones are an accepted risk, extra entries only cost reads):
 * WETH9 (Deposit/Withdrawal mint and burn without Transfer), Lido stETH and Ampleforth (rebasing),
 * Aave v2 aWETH/aUSDC/aDAI/aUSDT and v3 aEthWETH/aEthUSDC/aEthUSDT/aEthwstETH/aEthWBTC (interest accrues without Transfer).
 */
const ethereumAlwaysRefresh: readonly Address[] = [
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
  '0xae7ab96520de3a18e5e111b5eaab095312d7fe84',
  '0xd46ba6d942050d489dbd938a2c909a5d5039a161',
  '0x030ba81f1c18d280636f32af80b9aad02cf0854e',
  '0xbcca60bb61934080951369a648fb03df4f96263c',
  '0x028171bca77440897b824ca71d1c56cac55b68a3',
  '0x3ed3b47dd13ec9a98b44e6204a523e766b225811',
  '0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8',
  '0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c',
  '0x23878914efe38d27c4d67ab83ed1b93a74d4086a',
  '0x0b925ed163218f6662a35e0f0371ac234f9e9371',
  '0x5ee5bf7ae06d1be5997a1a72006fe6c607ec6de8',
];

/** Static per-network facts. Adding an EVM network starts with one entry here. */
export const networkRegistry = {
  ethereum: {
    name: 'ethereum',
    id: 1,
    chainId: mainnet.id,
    chain: mainnet,
    startBlock: 25_933_634n,
    hypersyncUrl: 'https://eth.hypersync.xyz',
    multicall3: MULTICALL3,
    alwaysRefreshTokens: ethereumAlwaysRefresh,
  },
} satisfies Record<string, EvmNetworkDefinition>;
