import { decodeFunctionData, encodeFunctionResult, erc20Abi, multicall3Abi, pad } from 'viem';
import { mainnet } from 'viem/chains';

import { createEvmAdapter } from '../src/chains/evm/adapter.ts';
import { TRANSFER_TOPIC } from '../src/chains/evm/logs.ts';
import { MULTICALL3 } from '../src/chains/evm/multicall.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import type { Address, BlockRef, ChainAdapter, ChunkAggregate, Hex, RpcClient, TransferEvent } from '../src/core/types.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { createRpcLogSource } from '../src/sources/rpc-logs.ts';

export const address = (n: number): Address => `0x${n.toString(16).padStart(40, '0')}` as Address;
export const blockHash = (n: bigint): Hex => `0x${n.toString(16).padStart(64, 'b')}`;
export const blockRef = (n: bigint): BlockRef => ({ number: n, hash: blockHash(n), timestamp: n * 12n });
const hex = (n: bigint | number): Hex => `0x${n.toString(16)}`;

interface LogFilter {
  fromBlock: bigint;
  toBlock: bigint;
  /** Address-filtered (per-account backfill) rather than the follower's unfiltered query. */
  filtered: boolean;
}

/**
 * An in-memory chain behind a fake RpcClient: blocks, Transfer logs and ERC-20 balances. Counts every call by method,
 * and lets tests gate or fail getLogs to stage races. Used with the real EVM adapter.
 */
export class FakeChain {
  finalized: bigint;
  latest?: bigint;
  transfers: TransferEvent[] = [];
  calls: Record<string, number> = {};
  /** balanceOf(account) on token at a block hash; undefined makes the inner call fail. */
  balanceOf: (account: Address, token: Address, at: Hex) => bigint | undefined = () => 0n;
  /** Awaited before answering eth_getLogs (e.g. a gate the test releases). */
  beforeLogs?: (filter: LogFilter) => Promise<void> | void;
  failHead = false;
  /** Every eth_call received, as the decoded aggregate3 call count. */
  multicallSizes: number[] = [];
  private logsPerBlock = new Map<bigint, number>();

  constructor(finalized: bigint) {
    this.finalized = finalized;
  }

  get total(): number {
    return Object.values(this.calls).reduce((a, b) => a + b, 0);
  }

  addTransfer(t: Omit<TransferEvent, 'transactionHash' | 'logIndex' | 'standard'> & Partial<TransferEvent>): TransferEvent {
    const event: TransferEvent = {
      standard: 'erc20',
      logIndex: this.logsPerBlock.get(t.blockNumber) ?? 0,
      transactionHash: blockHash(BigInt(this.transfers.length + 1) * 1_000_000n),
      ...t,
    };
    this.logsPerBlock.set(event.blockNumber, Math.max(this.logsPerBlock.get(event.blockNumber) ?? 0, event.logIndex) + 1);
    this.transfers.push(event);
    return event;
  }

  /** What a perfect indexer would hold for `account` from `fromBlock` through `toBlock`. */
  truth(account: Address, fromBlock: bigint, toBlock: bigint): ChunkAggregate {
    const aggregate = emptyAggregate();
    for (const e of this.transfers) if (e.blockNumber >= fromBlock && e.blockNumber <= toBlock) addTransfer(aggregate, account, e);
    return aggregate;
  }

  private block(n: bigint) {
    return { number: hex(n), hash: blockHash(n), parentHash: blockHash(n - 1n), timestamp: hex(n * 12n), transactions: [] };
  }

  private wireLog(e: TransferEvent) {
    const topics = [TRANSFER_TOPIC, pad(e.from, { size: 32 }), pad(e.to, { size: 32 })];
    if (e.standard === 'erc721') topics.push(pad(hex(e.value), { size: 32 }));
    return {
      address: e.token,
      topics,
      data: e.standard === 'erc721' ? '0x' : pad(hex(e.value), { size: 32 }),
      blockNumber: hex(e.blockNumber),
      blockHash: blockHash(e.blockNumber),
      transactionHash: e.transactionHash,
      logIndex: hex(e.logIndex),
    };
  }

  readonly rpc: RpcClient = {
    request: async <T>(method: string, params: unknown[]): Promise<T> => {
      this.calls[method] = (this.calls[method] ?? 0) + 1;
      if (method === 'eth_getBlockByNumber') {
        if (this.failHead) throw new RpcError('transient', 'HTTP 503');
        const tag = params[0];
        const n = tag === 'finalized' ? this.finalized : tag === 'latest' ? this.latest ?? this.finalized + 64n : BigInt(tag as string);
        return this.block(n) as T;
      }
      if (method === 'eth_getLogs') {
        const f = params[0] as { fromBlock: Hex; toBlock: Hex; topics: (Hex | null)[] };
        const filter = { fromBlock: BigInt(f.fromBlock), toBlock: BigInt(f.toBlock), filtered: f.topics.length > 1 };
        await this.beforeLogs?.(filter);
        const topicOf = (a: Address) => pad(a, { size: 32 });
        return this.transfers.filter((e) => e.blockNumber >= filter.fromBlock && e.blockNumber <= filter.toBlock
          && (!f.topics[1] || f.topics[1] === topicOf(e.from))
          && (!f.topics[2] || f.topics[2] === topicOf(e.to))).map((e) => this.wireLog(e)) as T;
      }
      if (method === 'eth_call') {
        const [{ data }, { blockHash: at }] = params as [{ data: Hex }, { blockHash: Hex }];
        const { args } = decodeFunctionData({ abi: multicall3Abi, data });
        const calls = (args as unknown as [{ target: Address; callData: Hex }[]])[0];
        this.multicallSizes.push(calls.length);
        return encodeFunctionResult({
          abi: multicall3Abi,
          functionName: 'aggregate3',
          result: calls.map((call) => {
            const [account] = decodeFunctionData({ abi: erc20Abi, data: call.callData }).args as [Address];
            const balance = this.balanceOf(account.toLowerCase() as Address, call.target.toLowerCase() as Address, at);
            return balance === undefined
              ? { success: false, returnData: '0x' as Hex }
              : { success: true, returnData: encodeFunctionResult({ abi: erc20Abi, functionName: 'balanceOf', result: balance }) };
          }),
        }) as T;
      }
      throw new RpcError('fatal', `unexpected method ${method}`);
    },
    stats: () => ({ requests: this.total, attempts: this.total, byMethod: { ...this.calls }, errors: {}, endpoints: [] }),
  };

  adapter(overrides: Partial<{ startBlock: bigint; logsMaxRange: bigint; followerMaxRange: bigint; scanConcurrency: number }> = {}): ChainAdapter {
    const logsMaxRange = overrides.logsMaxRange ?? 1_000n;
    return createEvmAdapter(this.rpc, {
      network: 'ethereum',
      chain: mainnet,
      startBlock: 100n,
      accountLogs: createRpcLogSource(this.rpc, { preferredMaxRange: logsMaxRange, minRange: 1n }),
      finalizedMaxNoProgressMs: 1_200_000,
      scanConcurrency: 2,
      followerMaxRange: 500n,
      multicallBatch: 1_000,
      multicall3: MULTICALL3,
      headCacheMs: 0,
      ...overrides,
    });
  }
}

/** A gate a fake RPC can await; release() opens it. */
export const gate = () => {
  let release = (): void => {};
  const opened = new Promise<void>((resolve) => { release = resolve; });
  return { opened, release };
};
