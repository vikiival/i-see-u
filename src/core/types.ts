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

/** Chain-agnostic, aggregate-while-decoding source for one account's resumable historical backfill. */
export interface AccountLogSource {
  readonly name: string;
  /** Preferred inclusive block span per independently committed backfill chunk. */
  readonly preferredMaxRange: bigint;
  /** Present only for sources whose successful/heavy responses should adapt and persist a per-account range. */
  readonly adaptiveRange?: { minRange: bigint };
  scanAccount(account: Address, range: BlockRange): Promise<ChunkScan>;
  stats(): Record<string, AccountLogSourceStats>;
  close?(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Chain adapter boundary (one implementation per network family; EVM first).
// ---------------------------------------------------------------------------

export interface ChainAdapter {
  readonly network: string; // e.g. "ethereum"
  readonly startBlock: bigint;
  /** Pluggable source used only for per-account historical backfills. */
  readonly accountLogs: AccountLogSource;
  readonly scanConcurrency: number;
  /** Max block span of one unfiltered (all-Transfer) getLogs call made by the follower. */
  readonly followerMaxRange: bigint;
  normalizeAddress(input: string): Address | undefined;
  /** Protocol-finalized head used for durable discovery. */
  finalizedHead(): Promise<HeadObservation>;
  /** Uncached latest header; invalid/null results fail over inside RpcClient. */
  latestHeader(): Promise<BlockHeader>;
  /** Highest latest-block number seen by finalizedHead()'s background refresh; never triggers RPC. */
  cachedLatestBlockNumber?(): bigint | undefined;
  /** Header of a (finalized) block by number, for cursors that end below the head. */
  blockByNumber(number: bigint): Promise<BlockRef>;
  headerByNumber(number: bigint): Promise<BlockHeader>;
  /** Every Transfer in one canonical block, selected by hash (EIP-234). */
  transfersInBlock(blockHash: Hex): Promise<TransferEvent[]>;
  /** One by-number catch-up query, rejected if a log does not match the supplied header chain. */
  transfersInRange(range: BlockRange, headers: BlockHeader[]): Promise<TransferEvent[]>;
  /**
   * Every Transfer in range, unfiltered by address (the follower's source); callers keep ranges <= followerMaxRange.
   * `accept` pre-filters by the from/to address before decoding. Splits on too_large/timeout like scanChunk.
   */
  scanAllTransfers(
    range: BlockRange,
    onTransfer: (event: TransferEvent) => void,
    accept?: (address: Address) => boolean,
  ): Promise<{ calls: number }>;
  /** balanceOf for each (account, token) pair, in input order, batched across accounts, pinned to one block hash. */
  erc20BalancePairs(pairs: BalancePair[], blockHash: Hex): Promise<BalanceResult[]>;
  /** ownerOf(tokenId) for each item, in input order, pinned to one block hash. Never throws per item. */
  erc721Owners(items: { token: Address; tokenId: bigint }[], blockHash: Hex): Promise<OwnerResult[]>;
}

export interface BalancePair {
  account: Address;
  token: Address;
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
// Storage (src/store). PostgreSQL in production, PGlite in tests.
// ---------------------------------------------------------------------------

export interface Holding {
  token: Address;
  standard: TokenStandard;
  firstBlock: bigint;
  lastBlock: bigint;
  sentCount: number;
  receivedCount: number;
  /** Counts contributed by the unfinalized tail; absent on finalized reads. */
  unfinalizedSentCount?: number;
  unfinalizedReceivedCount?: number;
}

export interface NftItem {
  token: Address;
  tokenId: bigint;
  /** true if the latest seen Transfer of this tokenId (by block, logIndex) was TO the account. */
  heldBySeenLogs: boolean;
  lastBlock: bigint;
  /** The winning latest Transfer is still in the reorgable tail. */
  unfinalized?: boolean;
}

export interface TailBlock {
  header: BlockHeader;
  transfers: TransferEvent[];
}

/** The follower's global cursor; updatedAt is when it last advanced (epoch ms). */
export interface FollowerCursor extends BlockRef {
  updatedAt: number;
}

/** Backfill handoff state: the backfill owns blocks <= target, the follower everything above. */
export interface Tracking {
  target: bigint;
  /** Highest backfilled block, undefined before the first chunk. */
  progress?: bigint;
}

export interface BalanceSnapshot {
  token: Address;
  result: BalanceResult;
  blockNumber: bigint;
  blockHash: Hex;
}

/** Per-account data read in one snapshot, consistent with the follower cursor. */
export interface AccountView {
  cursor: FollowerCursor;
  holdings: Holding[];
  nftItems: NftItem[];
  /** Matching rows strictly above cursor, read in the same snapshot as the finalized base. */
  tailTransfers: TransferEvent[];
  tailTip?: BlockHeader;
}

export type FollowerApplyResult = { applied: true } | { applied: false; missing: Address[] };

export interface Store {
  getFollowerCursor(network: string): Promise<FollowerCursor | undefined>;
  /** Create the cursor at `block` unless one exists; returns the stored cursor. */
  initFollowerCursor(network: string, block: BlockRef, at: number): Promise<FollowerCursor>;
  /**
   * Mark an account tracked (idempotent). The insert and the read of the follower cursor (= backfill target) happen in
   * one transaction under a share lock on the cursor row, so a follower commit is either fully before it (and its blocks
   * are <= target) or fully after it (and applies to the account). Legacy progress above the cursor is discarded.
   */
  trackAccount(network: string, account: Address, at: number): Promise<Tracking>;
  getTracking(network: string, account: Address): Promise<Tracking | undefined>;
  trackedAccounts(network: string): Promise<Address[]>;
  /**
   * Apply one follower range for the accounts in `aggregates` and advance the cursor to `to`, in one transaction that
   * locks the cursor row. Rejects unless the cursor is at range.fromBlock - 1. Applies nothing (applied: false) when an
   * account is tracked in the database but missing from `known`, the set the range was decoded against.
   */
  applyFollowerRange(
    network: string,
    range: BlockRange,
    to: BlockRef,
    at: number,
    aggregates: Map<Address, ChunkAggregate>,
    known: ReadonlySet<Address>,
  ): Promise<FollowerApplyResult>;
  getTailBlocks(network: string): Promise<BlockHeader[]>;
  getTailTip(network: string): Promise<BlockHeader | undefined>;
  /** Compare-and-append a contiguous hash chain in one transaction. */
  appendTailBlocks(network: string, expectedParentHash: Hex, blocks: TailBlock[]): Promise<void>;
  rewindTail(network: string, aboveNumber: bigint): Promise<void>;
  wipeTail(network: string): Promise<void>;
  /** Highest backfilled block, even if the backfill has not reached its target yet. */
  getProgress(network: string, account: Address): Promise<bigint | undefined>;
  /**
   * Atomically apply one backfill chunk's aggregate and advance progress to range.toBlock.
   * Idempotent: a chunk at or below applied progress is a no-op; one straddling it, or one beyond the account's
   * backfill target (the follower's side), is rejected.
   */
  applyScanChunk(network: string, account: Address, range: BlockRange, chunk: ChunkAggregate): Promise<void>;
  /** Last adaptive getLogs range used for this account, so the next scan of a heavy account starts small. */
  getLogsRange(network: string, account: Address): Promise<bigint | undefined>;
  setLogsRange(network: string, account: Address, range: bigint): Promise<void>;
  /** Cursor, holdings and NFT items read in one repeatable-read transaction. Undefined without a follower cursor. */
  readAccount(network: string, account: Address): Promise<AccountView | undefined>;
  /** ERC-20 and ERC-721 summaries, newest first. */
  holdings(network: string, account: Address): Promise<Holding[]>;
  nftItems(network: string, account: Address): Promise<NftItem[]>;
  /** Every ERC-20 (account, token) pair of these accounts. */
  erc20Pairs(network: string, accounts: Address[]): Promise<BalancePair[]>;
  /** Upsert balance snapshots read at `block`; a row never moves to an older block. */
  saveBalances(network: string, block: BlockRef, rows: (BalancePair & { result: BalanceResult })[]): Promise<void>;
  balances(network: string, account: Address): Promise<BalanceSnapshot[]>;
  close(): Promise<void>;
}

/** Health of one network's follower, computed in memory (never triggers RPC). */
export interface FollowerStatus {
  /** A cursor exists, so accounts can be tracked and served. */
  ready: boolean;
  /** This process holds the follower lock and advances the cursor. */
  leader: boolean;
  cursor?: FollowerCursor;
  /** Highest finalized head number the follower has seen. */
  headNumber?: bigint;
  /** Finalized head minus cursor, from the follower's last head poll. */
  lagBlocks: bigint;
  /** When the cursor last advanced (epoch ms); 0 before the first cursor read. */
  lastAdvanceAt: number;
  lastError?: string;
  stale: boolean;
  /** Why discovery must refuse (503), if it must. */
  unhealthy?: string;
}

export interface Follower {
  /** Load the persisted cursor so a restarted process serves immediately, before its first tick reaches the RPC. */
  load(): Promise<void>;
  start(): void;
  stop(): Promise<void>;
  /** One poll: read the finalized head, apply every range up to it (when leader). */
  tick(): Promise<void>;
  status(): FollowerStatus;
  /** Add a just-tracked account to the in-memory set (the store re-checks at commit). */
  track(account: Address): void;
  /** Tick now instead of waiting for pollMs (the tail saw a new epoch). */
  kick(): void;
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

// ---------------------------------------------------------------------------
// Discovery service (src/discovery).
// ---------------------------------------------------------------------------

export interface DiscoveryResult {
  status: 'ready';
  network: string;
  account: Address;
  fromBlock: bigint; // network start block
  blockHeight: bigint; // follower cursor: data valid up to and including this block
  blockHash: Hex;
  blockTimestamp: bigint;
  finality: 'latest' | 'finalized';
  finalizedBlockHeight?: bigint;
  finalizedBlockHash?: Hex;
  tailError?: string;
  /** When the follower cursor last advanced (epoch ms). */
  lastProgressAt: number;
  stale: boolean;
  /** Latest head minus blockHeight. */
  lagBlocks: bigint;
  /** Finalized head minus blockHeight. */
  followerLagBlocks: bigint;
  holdings: Holding[];
  nftItems: NftItem[];
  /** Backfill work this request waited for (zero when served from the database). */
  scan: { newBlocks: bigint; rpcCalls: number; ms: number };
}

/** The account's backfill has not reached its target yet. */
export interface SyncingResult {
  status: 'syncing';
  network: string;
  account: Address;
  fromBlock: bigint;
  progressBlocks: bigint;
  targetBlocks: bigint;
  followerLagBlocks: bigint;
}

export interface DiscoveryService {
  /**
   * Validate network and address, track the account, wait up to the request deadline for its backfill
   * (single-flight per account), then read from the database. Never calls RPC once the account is backfilled.
   */
  discover(network: string, account: string, finality?: 'latest' | 'finalized'): Promise<DiscoveryResult | SyncingResult>;
}
