// Shared contracts. Every module codes against these; change them only in coordination.
// Runs on Node's native type stripping: no enums, no parameter properties, `import type` for types.

export type Hex = `0x${string}`;
/** Always lowercase 0x-prefixed 20-byte address. */
export type Address = `0x${string}`;

export type TokenStandard = 'erc20' | 'erc721';

export interface BlockRef {
  number: bigint;
  hash: Hex;
  parentHash: Hex;
  /** Block header timestamp, in Unix seconds. */
  timestamp: bigint;
}

export interface HeadObservation {
  block: BlockRef;
  /** Epoch milliseconds of the last successful RPC response for this head kind. */
  lastResponseAt: number;
  /** Epoch milliseconds when this head kind last advanced to a higher block number. */
  lastProgressAt: number;
  stale: boolean;
}

export interface BlockRange {
  fromBlock: bigint;
  toBlock: bigint; // inclusive
}

/** Raw eth_getLogs entry, as returned by the node. */
export interface RawLog {
  address: Address;
  topics: Hex[];
  data: Hex;
  blockNumber: bigint;
  blockHash: Hex;
  transactionHash: Hex;
  logIndex: number;
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
  /** Confirmation-based head retained for the later realtime tail. */
  safeHead(): Promise<HeadObservation>;
  /** Raw latest head if safeHead() has already populated it; never triggers RPC. */
  cachedLatestHead?(): HeadObservation | undefined;
  /**
   * Aggregate every Transfer with `account` on either side within range (both directions).
   * Must handle provider range/result limits internally (adaptive splitting); rejects if either direction fails.
   */
  scanChunk(account: Address, range: BlockRange): Promise<ChunkScan>;
  /** balanceOf(account) for each token, pinned to one block. Never throws per token. */
  erc20Balances(account: Address, tokens: Address[], at: BlockRef): Promise<BalanceResult[]>;
  /** ownerOf(tokenId) for each (token, tokenId), pinned to one block. Never throws per item. */
  erc721Owners(items: { token: Address; tokenId: bigint }[], at: BlockRef): Promise<OwnerResult[]>;
}

export interface BalanceResult {
  token: Address;
  status: 'ok' | 'error';
  balance?: bigint;
  error?: string;
}

export interface OwnerResult {
  token: Address;
  tokenId: bigint;
  status: 'ok' | 'error';
  owner?: Address;
  error?: string;
}

// ---------------------------------------------------------------------------
// Low-level JSON-RPC client (src/rpc). Resilience lives here.
// ---------------------------------------------------------------------------

export interface RpcClient {
  /** Single JSON-RPC call with retries, backoff, endpoint failover and circuit breaking. */
  request<T>(method: string, params: unknown[], opts?: RpcRequestOptions): Promise<T>;
  stats(): RpcStats;
}

export interface RpcRequestOptions {
  /** Abort after this many ms per attempt (default from config). */
  timeoutMs?: number;
  /** Override max attempts across all endpoints. */
  maxAttempts?: number;
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

export interface RpcErrorShape extends Error {
  kind: RpcErrorKind;
  code?: number;
  /** Suggested sub-range parsed from provider messages like "Try with this block range [0x.., 0x..]". */
  suggestedRange?: BlockRange;
  /** Set on transient errors caused by a client, gateway, or provider-side timeout. */
  timeout?: boolean;
}

// ---------------------------------------------------------------------------
// Storage (src/store). SQLite (node:sqlite) for the PoC; Postgres later behind the same interface.
// ---------------------------------------------------------------------------

export interface Holding {
  network: string;
  holder: Address;
  token: Address;
  standard: TokenStandard;
  firstBlock: bigint;
  lastBlock: bigint;
  sentCount: number;
  receivedCount: number;
}

export interface NftItem {
  network: string;
  holder: Address;
  token: Address;
  tokenId: bigint;
  /** true if the latest seen Transfer of this tokenId (by block, logIndex) was TO holder. */
  heldBySeenLogs: boolean;
  lastBlock: bigint;
}

export interface AccountCursor {
  network: string;
  account: Address;
  /** Highest block fully scanned for this account; startBlock - 1 if never scanned. */
  scannedTo: bigint;
  scannedToHash: Hex;
  scannedToParentHash: Hex;
  scannedToTimestamp: bigint;
  updatedAt: number; // epoch ms
}

export interface Store {
  getCursor(network: string, account: Address): AccountCursor | undefined;
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
  /** Publish a consistent finalized height/hash pair after every chunk through it was applied. */
  finalizeScan(network: string, account: Address, scannedTo: BlockRef): void;
  holdings(network: string, account: Address, standard: TokenStandard): Holding[];
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
  blockParentHash: Hex;
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
  /** Bring the account up to finalized head (single-flight per account), then read from store. */
  discover(network: string, account: string): Promise<DiscoveryResult>;
}
