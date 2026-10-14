// Shared contracts. Runs on Node's native type stripping: no enums, no parameter properties, `import type` for types.

export type Hex = `0x${string}`;
/** Always lowercase 0x-prefixed 20-byte address. */
export type Address = `0x${string}`;

export type TokenStandard = 'erc20' | 'erc721';

export interface BlockRef {
  number: bigint;
  hash: Hex;
  /** Block header timestamp, in Unix seconds. */
  timestamp: bigint;
}

export interface HeadObservation {
  block: BlockRef;
  /** Epoch milliseconds when the head last advanced to a higher block number. */
  lastProgressAt: number;
  stale: boolean;
}

export interface BlockRange {
  fromBlock: bigint;
  toBlock: bigint; // inclusive
}

/** A decoded Transfer(address,address,uint256) log. */
export interface TransferEvent {
  standard: TokenStandard; // 3 topics = erc20, 4 topics = erc721
  token: Address;
  from: Address;
  to: Address;
  /** erc20: amount; erc721: tokenId */
  value: bigint;
  blockNumber: bigint;
  logIndex: number;
  transactionHash: Hex;
}

/** Which side of the Transfer the account must be on. */
export type TransferDirection = 'from' | 'to';

/** Per-token summary of one scanned chunk, built while decoding (no per-transfer arrays). */
export interface TokenAggregate {
  standard: TokenStandard;
  firstBlock: bigint;
  lastBlock: bigint;
  sentCount: number;
  receivedCount: number;
}

/** Latest seen Transfer of one ERC-721 tokenId within a chunk, by (block, logIndex). */
export interface NftAggregate {
  token: Address;
  tokenId: bigint;
  held: boolean;
  block: bigint;
  logIndex: number;
}

export interface ChunkAggregate {
  tokens: Map<Address, TokenAggregate>;
  /** Keyed by `${token}:${tokenId}`. */
  nfts: Map<string, NftAggregate>;
  /** `${txHash}:${logIndex}` of self-transfers (from == to == account), which both directions return. */
  selfSeen: Set<string>;
}

export interface ChunkScan {
  aggregate: ChunkAggregate;
  calls: number;
  /** The chunk had to be split, or a response was large or slow: use a smaller range next. */
  heavy: boolean;
}

// ---------------------------------------------------------------------------
// Chain adapter boundary (one implementation per network family; EVM first).
// ---------------------------------------------------------------------------

export interface ChainAdapter {
  readonly network: string; // e.g. "ethereum"
  readonly startBlock: bigint;
  readonly logsMaxRange: bigint;
  /** Floor for the adaptive per-account chunk range. */
  readonly logsMinRange: bigint;
  readonly scanConcurrency: number;
  normalizeAddress(input: string): Address | undefined;
  /** Protocol-finalized head used for durable discovery. */
  finalizedHead(): Promise<HeadObservation>;
  /** Highest latest-block number seen by finalizedHead()'s background refresh; never triggers RPC. */
  cachedLatestBlockNumber?(): bigint | undefined;
  /**
   * Aggregate every Transfer with `account` on either side within range (both directions).
   * Must handle provider range/result limits internally (adaptive splitting); rejects if either direction fails.
   */
  scanChunk(account: Address, range: BlockRange): Promise<ChunkScan>;
  /** balanceOf(account) for each token, in input order, pinned to one block hash. Never throws per token. */
  erc20Balances(account: Address, tokens: Address[], blockHash: Hex): Promise<BalanceResult[]>;
  /** ownerOf(tokenId) for each item, in input order, pinned to one block hash. Never throws per item. */
  erc721Owners(items: { token: Address; tokenId: bigint }[], blockHash: Hex): Promise<OwnerResult[]>;
}

export type BalanceResult = { status: 'ok'; balance: bigint } | { status: 'error'; error: string };
export type OwnerResult = { status: 'ok'; owner: Address } | { status: 'error'; error: string };

// ---------------------------------------------------------------------------
// Low-level JSON-RPC client (src/rpc). Resilience lives here.
// ---------------------------------------------------------------------------

export interface RpcClient {
  /** Single JSON-RPC call with retries, backoff, endpoint failover and circuit breaking. */
  request<T>(method: string, params: unknown[], opts?: RpcRequestOptions): Promise<T>;
  stats(): RpcStats;
}

export interface RpcRequestOptions {
  /** Treat a syntactically successful but unusable result as a transient endpoint failure. */
  validate?: (result: unknown) => boolean;
  /** Throw a timeout (error.timeout) at once instead of retrying it, e.g. so the caller can split a log range. */
  failFastOnTimeout?: boolean;
  /** Called after every HTTP attempt; bytes is the uncompressed Content-Length when the provider sends one. */
  onAttempt?: (attempt: RpcAttemptInfo) => void;
}

export interface RpcAttemptInfo {
  ok: boolean;
  durationMs: number;
  bytes?: number;
}

export interface RpcStats {
  requests: number; // logical requests
  attempts: number; // HTTP attempts (= billed calls, roughly)
  byMethod: Record<string, number>; // attempts per method
  errors: Record<string, number>; // by error class
  endpoints: { url: string; state: 'closed' | 'open' | 'half-open'; failures: number }[];
}

/** Error classes the RPC layer distinguishes. */
export type RpcErrorKind =
  | 'rate_limited' // 429, -32005 "limit", -32007, -32010 "requests limited"
  | 'transient' // 5xx, network, timeout, header not found
  | 'too_large' // result/range too large; caller should split the range
  | 'unsupported' // method not available / archive refused on this endpoint
  | 'fatal'; // anything else (bad params, execution reverted)

// ---------------------------------------------------------------------------
// Storage (src/store). SQLite (node:sqlite) for the PoC; Postgres later behind the same interface.
// ---------------------------------------------------------------------------

export interface Holding {
  token: Address;
  standard: TokenStandard;
  firstBlock: bigint;
  lastBlock: bigint;
  sentCount: number;
  receivedCount: number;
}

export interface NftItem {
  token: Address;
  tokenId: bigint;
  /** true if the latest seen Transfer of this tokenId (by block, logIndex) was TO the account. */
  heldBySeenLogs: boolean;
  lastBlock: bigint;
}

export interface Store {
  /** Published finalized cursor: every chunk through this block was applied. */
  getCursor(network: string, account: Address): BlockRef | undefined;
  /** Highest block whose events were applied, even if the finalized cursor is not complete yet. */
  getProgress(network: string, account: Address): bigint | undefined;
  /**
   * Atomically apply one completed chunk's aggregate and advance progress to range.toBlock.
   * Idempotent: a chunk at or below applied progress is a no-op; one straddling it is rejected.
   */
  applyScanChunk(network: string, account: Address, range: BlockRange, chunk: ChunkAggregate): void;
  /** Last adaptive getLogs range used for this account, so the next scan of a heavy account starts small. */
  getLogsRange(network: string, account: Address): bigint | undefined;
  setLogsRange(network: string, account: Address, range: bigint): void;
  /** Publish a consistent finalized height/hash pair after every chunk through it was applied. Never moves back. */
  finalizeScan(network: string, account: Address, scannedTo: BlockRef): void;
  /** ERC-20 and ERC-721 summaries, newest first. */
  holdings(network: string, account: Address): Holding[];
  nftItems(network: string, account: Address): NftItem[];
  close(): void;
}

// ---------------------------------------------------------------------------
// Discovery service (src/discovery).
// ---------------------------------------------------------------------------

export interface DiscoveryResult {
  network: string;
  account: Address;
  fromBlock: bigint; // network start block
  blockHeight: bigint; // data valid up to and including this block
  blockHash: Hex;
  blockTimestamp: bigint;
  finality: 'finalized';
  lastProgressAt: number;
  stale: boolean;
  lagBlocks: bigint;
  holdings: Holding[];
  nftItems: NftItem[];
  scan: { newBlocks: bigint; rpcCalls: number; ms: number };
}

export interface DiscoveryService {
  /** Validate network and address, bring the account up to finalized head (single-flight per account), then read. */
  discover(network: string, account: string): Promise<DiscoveryResult>;
}
