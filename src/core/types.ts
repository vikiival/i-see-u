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

export interface BlockHeader extends BlockRef {
  parentHash: Hex;
}

export interface HeadObservation {
  block: BlockRef;
  /** Epoch milliseconds when the head last advanced to a higher block number. */
  lastProgressAt: number;
  stale: boolean;
  /** True when the latest refresh failed and this is the cached observation (then also stale). */
  refreshFailed?: boolean;
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
  blockHash: Hex;
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
  /** Logical upstream requests used for this chunk (both RPC directions, or one HyperSync stream query). */
  calls: number;
  /** Distinct matching transfers folded into the aggregate (self-transfers count once). */
  logs: number;
  /** The chunk had to be split, or a response was large or slow: use a smaller range next. */
  heavy: boolean;
}

export interface AccountLogSourceStats {
  requests: number;
  chunks: number;
  logs: number;
  failures: number;
  /** Chunks redirected to a fallback after a failure or while the breaker is open. */
  fallbacks: number;
  lastError?: string;
  cooldownUntil?: number;
}

/** Standalone source comparison diagnostics; deliberately absent from ChainAdapter and runtime discovery. */
export interface AccountLogSource {
  readonly name: string;
  /** Preferred inclusive block span per diagnostic chunk. */
  readonly preferredMaxRange: bigint;
  scanAccount(account: Address, range: BlockRange): Promise<ChunkScan>;
  stats(): Record<string, AccountLogSourceStats>;
  close?(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Chain adapter boundary (one implementation per network family; EVM first).
// ---------------------------------------------------------------------------

export interface ChainAdapter {
  readonly network: string;
  readonly startBlock: bigint;
  readonly followerMaxRange: bigint;
  normalizeAddress(input: string): Address | undefined;
  finalizedHead(): Promise<HeadObservation>;
  cachedLatestBlockNumber?(): bigint | undefined;
  /** Uncached `latest` header, for the tail. */
  latestHeader(): Promise<BlockHeader>;
  headerByNumber(number: bigint): Promise<BlockHeader>;
  /** EIP-234 block-hash query: the logs of exactly this block, never a same-height block of another fork. */
  transfersInBlock(blockHash: Hex): Promise<TransferEvent[]>;
  /** All Transfer logs, verified against the supplied contiguous header chain. */
  transfersInRange(range: BlockRange, headers: BlockHeader[]): Promise<TransferEvent[]>;
  erc20BalancePairs(pairs: BalancePair[], blockHash: Hex, signal?: AbortSignal): Promise<BalanceResult[]>;
  erc721Owners(items: { token: Address; tokenId: bigint }[], blockHash: Hex, signal?: AbortSignal): Promise<OwnerResult[]>;
}

export interface BalancePair {
  account: Address;
  token: Address;
}

export type BalanceResult = { status: 'ok'; balance: bigint } | { status: 'error'; error: string; transient?: true };

export type OwnerResult = { status: 'ok'; owner: Address } | { status: 'error'; error: string; transient?: true };

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
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: validate inspects the raw JSON-RPC result before it is typed
  validate?: (result: unknown) => boolean;
  /** Throw a timeout (error.timeout) at once instead of retrying it, e.g. so the caller can split a log range. */
  failFastOnTimeout?: boolean;
  /** Called after every HTTP attempt; bytes is the uncompressed Content-Length when the provider sends one. */
  onAttempt?: (attempt: RpcAttemptInfo) => void;
  /** Stops the request: no further attempt or backoff, and the in-flight fetch is aborted. Rejects with signal.reason. */
  signal?: AbortSignal;
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
// Storage (src/store). PostgreSQL in production, PGlite in tests.
// ---------------------------------------------------------------------------

export interface Holding {
  token: Address;
  standard: TokenStandard;
  firstBlock: bigint;
  lastBlock: bigint;
  sentCount: bigint;
  receivedCount: bigint;
  /** Latest view only: the part of the counts from unfinalized tail blocks. */
  unfinalizedSentCount?: bigint;
  unfinalizedReceivedCount?: bigint;
}

/** Only currently owned items; historical movements are in transfers. Burns have the zero owner. */
export interface NftItem {
  token: Address;
  tokenId: bigint;
  heldBySeenLogs: boolean;
  lastBlock: bigint;
  /** Latest view only: ownership comes from an unfinalized tail block. */
  unfinalized?: true;
}

/** One unfinalized block of the tail journal with all its decoded Transfers. */
export interface TailBlock {
  header: BlockHeader;
  transfers: TransferEvent[];
}

export interface FollowerCursor extends BlockRef {
  updatedAt: number;
  /** Changes on repair; fences cache writes and history pagination across repairs. */
  revision: bigint;
}

export interface IndexState {
  startBlock: bigint;
  cursor: FollowerCursor;
  head?: BlockRef;
  checkedAt?: number;
  pendingRepair: boolean;
}

export interface BalanceSnapshot {
  token: Address;
  result: BalanceResult;
  blockNumber: bigint;
  blockHash: Hex;
  readAt: number;
  /** Repair revision the read was fenced against; a snapshot is only reused under the same revision. */
  revision: bigint;
}

/** A stored snapshot plus whether its block hash is still canonical (finalized header or current tail block). */
export interface StoredBalance extends BalanceSnapshot {
  canonical: boolean;
}

/** Applied segments only; target/progress are unknown before a bootstrap run exists. */
export interface BootstrapCoverage {
  startBlock: bigint;
  targetBlock?: bigint;
  targetHash?: Hex;
  coveredRanges: BlockRange[];
  coveredBlocks: bigint;
  totalBlocks?: bigint;
  lastAppliedAt?: number;
}

export type IndexSnapshot = { state: IndexState; bootstrap?: never } | { state?: never; bootstrap: BootstrapCoverage };

/** Everything one request needs, read in one repeatable-read snapshot. */
export type AccountView = IndexSnapshot & {
  holdings: Holding[];
  nftItems: NftItem[];
  /** With `tail`: the account's tail Transfers strictly above the cursor, ascending, and the tail tip. */
  tailTransfers: TransferEvent[];
  tailTip?: BlockHeader;
  /** With `balances`: the account's snapshots. */
  balances?: StoredBalance[];
};

export type HistoryTransfer = TransferEvent & { unfinalized?: true };

/** Descending (block, log index); the original snapshot bound and revision survive every page. */
export interface TransferCursor {
  blockNumber: bigint;
  logIndex: number;
  throughBlock: bigint;
  revision: bigint;
  /** Partial pages restart whenever coverage changes, including publication. */
  partial?: true;
}

export type TransferPage = IndexSnapshot & {
  throughBlock: bigint;
  /** Tail rows (above the cursor) are flagged unfinalized; only present when the tail was read. */
  transfers: HistoryTransfer[];
  next?: TransferCursor;
  tailTip?: BlockHeader;
};

export interface RepairRange extends BlockRange {
  reason: string;
  createdAt: number;
}

export interface Store {
  /** Initialize ONLY at startBlock - 1, never at the head. Registry/config owns the start block. */
  initialize(network: string, startBlock: bigint, anchor: BlockHeader, at: number): Promise<IndexState>;
  indexState(network: string): Promise<IndexState | undefined>;
  indexSnapshot(network: string): Promise<IndexSnapshot | undefined>;
  observeHead(network: string, head: BlockRef, at: number): Promise<void>;
  /** Bulk append for bootstrap and follower. Complete headers include empty blocks. Atomic cursor + raw + derived. */
  appendRange(network: string, range: BlockRange, headers: BlockHeader[], transfers: TransferEvent[], at: number): Promise<boolean>;
  /** Replace committed raw rows/headers and rebuild affected pairs/items from ALL remaining history atomically. */
  replaceRange(network: string, range: BlockRange, headers: BlockHeader[], transfers: TransferEvent[]): Promise<void>;
  /** Rebuild accounts/items touched by an existing raw range, without refetching or changing raw rows. */
  rebuildRange(network: string, range: BlockRange): Promise<void>;
  enqueueRepair(network: string, range: BlockRange, reason: string, at: number): Promise<void>;
  repairs(network: string): Promise<RepairRange[]>;
  readAccount(network: string, account: Address, options?: { tail?: boolean; balances?: boolean }): Promise<AccountView | undefined>;
  holdings(network: string, account: Address): Promise<Holding[]>;
  nftItems(network: string, account: Address): Promise<NftItem[]>;
  /** Interaction holders, not a claim of positive balance. Keyset by holder. */
  tokenHolders(network: string, token: Address, limit: number, after?: Address): Promise<Address[]>;
  /** With `tail`, merges tail rows above the cursor up to the tail tip; a cursor above the finalized cursor needs the tail. */
  transferHistory(network: string, account: Address, limit: number, before?: TransferCursor, tail?: boolean): Promise<TransferPage | undefined>;
  /**
   * Validates the expected repair revision and that the block is canonical (finalized header, or the tail block at
   * that height); returns false for a read invalidated by repair or reorg.
   */
  saveBalances(network: string, at: BlockRef, rows: (BalancePair & { result: BalanceResult })[], readAt: number, revision: bigint): Promise<boolean>;
  balances(network: string, account: Address): Promise<StoredBalance[]>;
  tailTip(network: string): Promise<BlockHeader | undefined>;
  tailBlocks(network: string): Promise<BlockHeader[]>;
  /** Append contiguous blocks on top of the tail tip (or the cursor when empty), fenced on `expectedParentHash`. */
  appendTailBlocks(network: string, expectedParentHash: Hex, blocks: TailBlock[]): Promise<void>;
  /** Delete tail rows above `aboveNumber`, fenced on the tip hash the caller observed. */
  rewindTail(network: string, aboveNumber: bigint, expectedTipHash: Hex): Promise<void>;
  wipeTail(network: string, expectedTipHash?: Hex): Promise<void>;
  close(): Promise<void>;
}

export interface FollowerStatus {
  ready: boolean;
  leader: boolean;
  cursor?: FollowerCursor;
  headNumber?: bigint;
  lagBlocks: bigint;
  lastAdvanceAt: number;
  lastError?: string;
  stale: boolean;
  unhealthy?: string;
}

export interface TailStatus {
  enabled: boolean;
  tipNumber?: bigint;
  tipHash?: Hex;
  latestNumber?: bigint;
  lagBlocks: bigint;
  reorgs: number;
  lastReorgDepth?: bigint;
  lastAdvanceAt: number;
  lastError?: string;
  healthy: boolean;
  tailError?: string;
}

export interface Tail {
  load(): Promise<void>;
  start(): void;
  stop(): Promise<void>;
  tick(): Promise<void>;
  status(): TailStatus;
}

export type Finality = 'latest' | 'finalized';

export interface Follower {
  load(): Promise<void>;
  start(): void;
  stop(): Promise<void>;
  tick(): Promise<void>;
  status(): FollowerStatus;
  kick(): void;
}

export type ServingMode = 'full' | 'stale' | 'partial';

export interface ServingStatus {
  ready: boolean;
  serving: ServingMode;
  reason?: string;
}

export type ServingMetadata = {
  status: 'ready';
  complete: true;
  blockHeight: bigint;
  blockHash: Hex;
  blockTimestamp: bigint;
  revision: bigint;
  lastProgressAt: number;
  stale: boolean;
  staleReason?: string;
  repairPending?: true;
  headHashMismatch?: true;
  note?: string;
  lagBlocks: bigint;
  followerLagBlocks: bigint;
} | (BootstrapCoverage & {
  status: 'partial';
  complete: false;
  note: string;
});

export type DiscoveryResult = ServingMetadata & {
  network: string;
  account: Address;
  fromBlock: bigint;
  finality: Finality;
  finalizedBlockHeight?: bigint;
  finalizedBlockHash?: Hex;
  tailError?: string;
  holdings: Holding[];
  nftItems: NftItem[];
  balances?: StoredBalance[];
};

export type HistoryResult = ServingMetadata & {
  network: string;
  account: Address;
  fromBlock: bigint;
  page: TransferPage;
  finality: Finality;
  finalizedBlockHeight?: bigint;
  finalizedBlockHash?: Hex;
  tailError?: string;
};

export interface DiscoveryService {
  status(network: string): Promise<ServingStatus>;
  /** Pure DB reads. Readiness is per network, never per account. */
  discover(network: string, account: string, options?: { finality?: Finality; balances?: boolean }): Promise<DiscoveryResult>;
  history(network: string, account: string, limit: number, before?: TransferCursor, finality?: Finality): Promise<HistoryResult>;
}
