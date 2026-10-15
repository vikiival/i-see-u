import { decodeFunctionData, encodeFunctionResult, erc20Abi, multicall3Abi, pad } from 'viem';
import { mainnet } from 'viem/chains';

import { createEvmAdapter } from '../src/chains/evm/adapter.ts';
import { TRANSFER_TOPIC } from '../src/chains/evm/logs.ts';
import { MULTICALL3 } from '../src/chains/evm/multicall.ts';
import { addTransfer, emptyAggregate } from '../src/core/aggregate.ts';
import type { Address, BlockHeader, BlockRef, ChainAdapter, ChunkAggregate, Hex, RpcClient, TransferEvent } from '../src/core/types.ts';
import { RpcError } from '../src/rpc/errors.ts';
import { lowerAddress, rpcParam, staged } from './helpers.ts';

export const address = (n: number): Address => `0x${n.toString(16).padStart(40, '0')}`;
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
  /** Awaited before answering one decoded Multicall3 aggregate. */
  beforeMulticall?: (targets: Address[]) => Promise<void> | void;
  failHead = false;
  mismatchRangeLogHash = false;
  readonly unknownBlockOnce = new Set<Hex>();
  /** Every eth_call received, as the decoded aggregate3 call count. */
  multicallSizes: number[] = [];
  private logsPerBlock = new Map<bigint, number>();
  private headers = new Map<bigint, BlockHeader>();
  private fork = 0n;
  private removed = new Set<TransferEvent>();

  constructor(finalized: bigint) {
    this.finalized = finalized;
  }

  get total(): number {
    return Object.values(this.calls).reduce((a, b) => a + b, 0);
  }

  private ensureHeaders(to: bigint): void {
    for (let number = 0n; number <= to; number += 1n) {
      if (this.headers.has(number)) continue;
      this.headers.set(number, {
        number,
        hash: blockHash(number),
        parentHash: number === 0n ? blockHash(0n) : this.headers.get(number - 1n)!.hash,
        timestamp: number * 12n,
      });
    }
  }

  addTransfer(t: Omit<TransferEvent, 'transactionHash' | 'logIndex' | 'standard' | 'blockHash'> & Partial<TransferEvent>): TransferEvent {
    this.ensureHeaders(t.blockNumber);

    const event: TransferEvent = {
      standard: 'erc20',
      logIndex: this.logsPerBlock.get(t.blockNumber) ?? 0,
      transactionHash: blockHash(BigInt(this.transfers.length + 1) * 1_000_000n),
      blockHash: this.headers.get(t.blockNumber)!.hash,
      ...t,
    };

    this.logsPerBlock.set(event.blockNumber, Math.max(this.logsPerBlock.get(event.blockNumber) ?? 0, event.logIndex) + 1);
    this.transfers.push(event);

    return event;
  }

  addRemovedTransfer(t: Parameters<FakeChain['addTransfer']>[0]): TransferEvent {
    const event = this.addTransfer(t);
    this.removed.add(event);

    return event;
  }

  /** Replace the last depth blocks at latest (or finalized), including their logs, with a new hash chain. */
  reorg(depth: number, newLogs: Parameters<FakeChain['addTransfer']>[0][]): void {
    const head = this.latest ?? this.finalized;
    const ancestor = head - BigInt(depth);
    this.ensureHeaders(head);
    this.transfers = this.transfers.filter((event) => event.blockNumber <= ancestor);
    this.fork += 1n;
    let parentHash = this.headers.get(ancestor)!.hash;

    for (let number = ancestor + 1n; number <= head; number += 1n) {
      const hash: Hex = `0x${(this.fork * 10n ** 12n + number).toString(16).padStart(64, 'c')}`;
      this.headers.set(number, { number, hash, parentHash, timestamp: number * 12n });
      parentHash = hash;
      this.logsPerBlock.delete(number);
    }

    for (const event of newLogs) this.addTransfer(event);
  }

  /** What a perfect indexer would hold for `account` from `fromBlock` through `toBlock`. */
  truth(account: Address, fromBlock: bigint, toBlock: bigint): ChunkAggregate {
    const aggregate = emptyAggregate();

    for (const e of this.transfers) if (e.blockNumber >= fromBlock && e.blockNumber <= toBlock) addTransfer(aggregate, account, e);

    return aggregate;
  }

  private block(n: bigint) {
    this.ensureHeaders(n);
    const header = this.headers.get(n)!;

    return { number: hex(n), hash: header.hash, parentHash: header.parentHash, timestamp: hex(header.timestamp), transactions: [] };
  }

  private wireLog(e: TransferEvent) {
    const topics = [TRANSFER_TOPIC, pad(e.from, { size: 32 }), pad(e.to, { size: 32 })];

    if (e.standard === 'erc721') topics.push(pad(hex(e.value), { size: 32 }));

    return {
      address: e.token,
      topics,
      data: e.standard === 'erc721' ? '0x' : pad(hex(e.value), { size: 32 }),
      blockNumber: hex(e.blockNumber),
      blockHash: e.blockHash,
      transactionHash: e.transactionHash,
      logIndex: hex(e.logIndex),
      removed: this.removed.has(e),
    };
  }

  readonly rpc: RpcClient = {
    request: async <T>(method: string, params: unknown[]): Promise<T> => {
      this.calls[method] = (this.calls[method] ?? 0) + 1;

      if (method === 'eth_getBlockByNumber') {
        if (this.failHead) throw new RpcError('transient', 'HTTP 503');
        const tag = params[0];
        const n = tag === 'finalized' ? this.finalized : tag === 'latest' ? this.latest ?? this.finalized + 64n : BigInt(String(tag));

        return staged<T>(this.block(n));
      }

      if (method === 'eth_getLogs') {
        const f = rpcParam<{ fromBlock?: Hex; toBlock?: Hex; blockHash?: Hex; topics: (Hex | null)[] }>(params, 0);
        let fromBlock: bigint;
        let toBlock: bigint;

        if (f.blockHash) {
          if (this.unknownBlockOnce.delete(f.blockHash)) throw new RpcError('transient', 'unknown block');
          const header = [...this.headers.values()].find((block) => block.hash === f.blockHash);

          if (!header) throw new RpcError('transient', 'unknown block');
          fromBlock = toBlock = header.number;
        } else {
          fromBlock = BigInt(f.fromBlock!);
          toBlock = BigInt(f.toBlock!);
        }

        const filter = { fromBlock, toBlock, filtered: f.topics.length > 1 };
        await this.beforeLogs?.(filter);
        const topicOf = (a: Address) => pad(a, { size: 32 });

        return staged<T>(this.transfers.filter((e) => e.blockNumber >= filter.fromBlock && e.blockNumber <= filter.toBlock
          && (!f.blockHash || e.blockHash === f.blockHash)
          && (!f.topics[1] || f.topics[1] === topicOf(e.from))
          && (!f.topics[2] || f.topics[2] === topicOf(e.to))).map((e, index) => {
            const log = this.wireLog(e);

            return this.mismatchRangeLogHash && !f.blockHash && index === 0 ? { ...log, blockHash: blockHash(9_999_999n) } : log;
          }));
      }

      if (method === 'eth_call') {
        const { data } = rpcParam<{ data: Hex }>(params, 0);
        const { blockHash: at } = rpcParam<{ blockHash: Hex }>(params, 1);
        const decoded = decodeFunctionData({ abi: multicall3Abi, data });

        if (decoded.functionName !== 'aggregate3') throw new RpcError('fatal', `unexpected call ${decoded.functionName}`);
        const [calls] = decoded.args;
        this.multicallSizes.push(calls.length);
        await this.beforeMulticall?.(calls.map((call) => lowerAddress(call.target)));

        return staged<T>(encodeFunctionResult({
          abi: multicall3Abi,
          functionName: 'aggregate3',
          result: calls.map((call) => {
            const inner = decodeFunctionData({ abi: erc20Abi, data: call.callData });

            if (inner.functionName !== 'balanceOf') throw new RpcError('fatal', `unexpected inner call ${inner.functionName}`);
            const [account] = inner.args;
            const balance = this.balanceOf(lowerAddress(account), lowerAddress(call.target), at);

            return balance === undefined
              ? { success: false, returnData: '0x' as const }
              : { success: true, returnData: encodeFunctionResult({ abi: erc20Abi, functionName: 'balanceOf', result: balance }) };
          }),
        }));
      }

      throw new RpcError('fatal', `unexpected method ${method}`);
    },
    stats: () => ({ requests: this.total, attempts: this.total, byMethod: { ...this.calls }, errors: {}, endpoints: [] }),
  };

  adapter(overrides: Partial<{
    startBlock: bigint;
    followerMaxRange: bigint;
    multicallBatch: number;
  }> = {}): ChainAdapter {
    return createEvmAdapter(this.rpc, {
      network: 'ethereum',
      chain: mainnet,
      startBlock: 100n,
      finalizedMaxNoProgressMs: 1_200_000,
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
