# i-see-u: token discovery over JSON-RPC

A TypeScript PoC for ERC-20 discovery and ERC-721 current ownership, starting at Ethereum block **25,933,634** and running to the **current chain tip**. The service is **near-realtime**: by default responses are valid at the latest block (~12 s slots, typically served within a few seconds of the block) with **finalized durability** underneath. Every finalized `Transfer` is indexed once for every account; a bounded, reorg-aware tail journal holds every `Transfer` above the finalized cursor. Discovery and transfer history read PostgreSQL; optional balances and ownership verification use hash-pinned Multicall3.

The architecture targets approximately one million accounts. There are no tracked accounts, account backfills, or account-level syncing responses. See [REPORT.md](REPORT.md) for schema choices, limits and agent handoffs.

## Run and validate

Node 26, PostgreSQL 18, Hono, viem over our `RpcClient`, Drizzle and node-postgres. Native TypeScript stripping, `.ts` imports, no build step.

```sh
npm install
cp .env.example .env                 # configure RPC_URL and DATABASE_URL
docker compose up -d postgres
npm start                             # migrate, start finalized follower and tail, serve :3000
npx tsc --noEmit -p .
npm test                              # real migrations on PGlite; no network or Docker
npm run lint
```

**A fresh database is not ready until the bootstrap has run.** `/health` and account reads return **503 `not_ready`** (with bootstrap progress in `/health`) until `npm run bootstrap` (or `BOOTSTRAP_ON_START=true`) has loaded every Transfer from the start block to a pinned finalized block and published the index. Startup never creates a cursor at the head. The follower and tail then continue from that block. See [Bootstrap](#bootstrap).

The migration directory is a **fresh baseline for an empty PoC database**, not an upgrade of the old aggregate-only database. Use a new database/schema when moving from that version; preserve an old database separately if needed. After schema changes run `npm run db:generate`. Generated SQL is not hand-edited. `0001_balance_revision` adds the snapshot repair revision; `0002_tail_journal` adds `tail_blocks`/`tail_transfers`; `0003_bootstrap` adds the bootstrap staging tables and `0004_bootstrap_unlogged` (custom SQL) makes `bootstrap_transfers` UNLOGGED. `npm run db:migrate` applies migrations without starting the server.

Runtime config loads `.env` with Node; already exported environment variables take precedence. The old `.env.example` may contain obsolete backfill/worker knobs; these no longer configure runtime behavior.

## API

| Endpoint | Behavior |
|---|---|
| `GET /v1/ethereum/accounts/{address}/erc20` | All ERC-20 contracts with a Transfer in or out from the start block to the served block |
| `…/erc20?balances=true` | Same discovery plus balances pinned to the served block hash (tail tip, or finalized cursor) |
| `…?finality=latest` (default) / `…?finality=finalized` | Any account endpoint: tip view with unfinalized overlay, or the finalized index only |
| `GET /v1/ethereum/accounts/{address}/erc721` | Interacted collections and currently owned token IDs according to indexed logs |
| `…/erc721?verifyOwnership=true` | Optional pinned `ownerOf` checks for held items, subject to the configured cap |
| `GET /v1/ethereum/accounts/{address}/transfers?limit=50` | Raw ERC-20/ERC-721 transfers newest first; `limit` is 1–200 |
| `…/transfers?limit=50&before={next}` | Next keyset page; use the previous response's `next` unchanged |
| `GET /health` | Shared database readiness per network, local follower and tail diagnostics (tip, lag, reorgs, errors) and redacted RPC stats; 503 until ready |

Discovery responses identify `blockHeight`, `blockHash`, `blockTimestamp`, `finality` and the network start block. **`finality=latest` (default)** serves the tail tip: `blockHeight`/`blockHash` are the tip, `finalizedBlockHeight`/`finalizedBlockHash` the finalized cursor below it. Counts include unfinalized Transfers and name that part as `unfinalizedSentCount`/`unfinalizedReceivedCount`; NFTs received in tail blocks carry `unfinalized: true`, NFTs sent in tail blocks disappear. If the tail is disabled, unhealthy, empty, or its tip is older than `TAIL_MAX_TIP_AGE_MS`, the response is the finalized view with `finality: "finalized"` and a `tailError` explaining why; it is never a 503 as long as the finalized index is ready. **`finality=finalized`** always serves the cursor without overlay and without `tailError`. `lastProgressAt` is the tip's block time (latest) or the cursor commit time (finalized); `lagBlocks` is the latest-head distance. Blocks, uint256 values, token IDs and **sent/received counts are decimal strings**. Historical NFT movements live in `/transfers`; the NFT endpoint no longer stores one historical held flag per account/item. Burned NFTs have no current holder.

History merges two bounded indexed scans per table (finalized `transfers`, and with latest `tail_transfers` above the cursor), excludes the duplicate side of self-transfers, and never uses OFFSET. Tail rows carry `unfinalized: true`. `next: null` means the end. A cursor carries the original `throughBlock` bound and repair revision; appends do not introduce new rows into later pages. Responses expose `throughBlock` separately from the served block. A repair/rebuild invalidates old cursors with **409**; so does a latest cursor whose `throughBlock` is above both the finalized cursor and the current tail tip (or that is continued with `finality=finalized`). A reorg between pages can change unfinalized rows of a later page; restart from the first page after a `tailError` or reorg if exactness matters.

Readiness is shared by every replica: a recent finalized-head observation must exist, the contiguous cursor must have reached it, and no repair may be pending. Incomplete, stale or suspect indexes return **503 `{ status: "not_ready", error }`** with `Retry-After`; there is no 202. The tail never makes the service not ready; it only degrades latest to finalized. Other errors: 400 invalid input (including `finality` other than `latest`/`finalized`), 404 unknown network, 503 RPC/queue/deadline/database acquisition failures.

## Bootstrap

```sh
docker compose up -d --wait postgres
START_BLOCK=26198554 npm run bootstrap   # optional override; default 25,933,634
npm start                                # follower continues at B + 1, tail serves the tip
```

`npm run bootstrap` (`src/bootstrap/`) is a one-time, resumable load of every ERC-20/721 `Transfer` in **[S, B]**: S is `START_BLOCK` (a plain block number; default is the registry's 25,933,634), B is the finalized block read once when the run is created and persisted in `bootstrap_runs`. B never moves during a run. After any failure or kill, rerun the same command: it resumes. After success, a rerun is a no-op. `BOOTSTRAP_ON_START=true` runs the same loader inside `npm start` while the service stays `not_ready`. A session advisory lock allows one loader per network.

1. **Headers.** The chain S-1..B (anchor S-1 from RPC) is staged in `bootstrap_headers` in 5,000-block chunks from HyperSync (`includeAllBlocks`, about 1 request per chunk). Each chunk must be contiguous, link its parent hashes, join the previous chunk, and (the last one) end at B's hash. A chunk that fails is refetched from RPC (one `eth_getBlockByNumber` per block). Each header keeps whether its `logsBloom` may contain the Transfer topic.
2. **Segments** of 1,000 blocks (`bootstrap_segments`) are the unit of commit, retry and resume. One HyperSync worker takes segments from the low end; `BOOTSTRAP_RPC_WORKERS` Quicknode workers take them from the high end with unfiltered topic0 `eth_getLogs` over 100-block calls. A 500-block call reached about 370 MB of JSON. The existing split-on-too-large/timeout still applies. Each segment runs in one transaction. Verified pages stream into the UNLOGGED, index-free `bootstrap_transfers`, and the segment is marked done in the same commit. A crash leaves neither rows nor the done marker. Verification checks three things. Every log's block hash must match the staged header. `(block, logIndex)` must strictly increase. A block with no decoded Transfer whose bloom may contain the topic is asked again on the other source (`eth_getLogs({blockHash})` or a one-block HyperSync query). Any failure or source error refetches the whole segment from the other source. A segment that fails on both stays pending; the run then exits non-zero after loading everything else.
3. **Finalize** is one transaction. It checks that every segment is done, the staged chain is complete and linked from S-1 to the pinned hash of B, and staged row count equals the sum of segment counts. It also checks that `ingestion_state` and `transfers` are empty for the network. It drops the five secondary indexes and inserts `transfers` (the primary key rejects duplicates) and `block_headers`. It builds `holdings` and `nft_ownership` with the follower's own SQL in one set-based pass. It recreates the indexes (`BOOTSTRAP_MAINTENANCE_WORK_MEM`), sets `ingestion_state` (cursor and observed head = B), and empties staging. `VACUUM (ANALYZE)` follows. The follower's next tick appends B+1 onward with its usual parent-hash check against B.

**Sources.** HyperSync is optional (`HYPERSYNC_API_KEY`). Without it the bootstrap is RPC-only and pays one call per header. The free tier allows **15 requests/min**, and responses are truncated at about 28k-150k logs. HyperSync is therefore paged with `getWithRateLimit` (`src/sources/hypersync-pages.ts`). On a 429 the pager sleeps for `resetSecs`, and it uses a per-request idle timeout (`HYPERSYNC_TIMEOUT_MS`). A request that times out is abandoned, not awaited. At most 2 requests are in flight. `stream()` and `collect()` are not used. With the client's default stream settings, many small requests burned the quota and then waited silently in `recv()`; that was the old "hang". `stream.close()` blocks while rate-limited, and `StreamConfig.maxNumLogs` ends a stream early as if it were complete. The `scripts/compare-sources.ts` diagnostic source (`src/sources/hypersync.ts`) uses the same pager. Bulk decoding uses a hand-written decoder for the canonical log shape, about 550k logs/s versus 38k/s for viem. Every other shape falls back to viem, and a fuzz test asserts that the two always agree.

**Progress.** The loader logs every 10 s: headers, segments done/total, logs, logs/s and ETA since the last (re)start. `/health` shows the same from the database under `bootstrap` (any process can run the loader) and stays `not_ready` until finalize commits.

**`START_BLOCK`.** A stored index or bootstrap run with a different start block makes both `npm start` and `npm run bootstrap` fail with `START_BLOCK is X but the … starts at Y; a different start block needs a fresh database`. To change S, use a new database.

**Measured (live smoke, 2026-10-15).** `START_BLOCK=26198554` gives 2,001 blocks to B = 26,200,554. That is 876,614 Transfers (436k from HyperSync in 7 requests including headers, 440k from Quicknode in 11 calls), 386,664 holdings rows and 15,028 NFTs. The run took **33 s**: 24 s loading (about 36k logs/s combined) and about 9 s finalize plus analyze. `transfers` with indexes is about 340 B/row. 15 Quicknode calls in total. `npm start` was then `ready` at once. The tail caught up from B to the tip (about 80 blocks: 1 header call per block plus 1-2 range `getLogs`), and latest served the tip.

**Projected** (from the measurements above and in the design: HyperSync quota-bound at about 21k logs/s, four RPC workers at about 20k logs/s, finalize at roughly 0.15M rows/s). **100k blocks** (53.8M logs): about 22-25 min loading plus about 6-10 min finalize, so about 30-35 min, about 500-550 Quicknode calls (about 11k credits) and about 20 HyperSync header requests. **Full window from 25,933,634** (about 266k blocks, about 128M logs, estimated): about 55 min loading plus 15-25 min finalize, so about 1.2-1.5 h and about 1,300 calls (about 27k credits). Disk is about 40 GB for `transfers` with indexes plus up to about 19 GB of transient staging. The follower's catch-up after the handover is 1-2 ranges.

**Limits.** The two-source check covers suspicious zero-log blocks, not a full second fetch. A source that drops some (not all) logs of a block whose other logs it returns is not detected. A PostgreSQL crash truncates the UNLOGGED staging table; the next run detects that and reloads every segment. A deep header inconsistency between two stored chunks fails finalize with a message, without automatic repair. Memory is bounded by one page per worker: a 100-block RPC response or one HyperSync page.

## Balances (`balances=true`)

Balances are read **only when a request asks for them**; nothing refreshes accounts in the background. Every read is a Multicall3 `aggregate3` of `balanceOf` calls at the served block (the tail tip for latest, the finalized cursor otherwise), pinned by block hash (EIP-1898), and is stored in `balance_snapshots` as value-or-error with block number/hash, `readAt` and the repair revision. A save is accepted only when the block is still canonical (the finalized header, or the tail block at that height).

**Reuse rule.** A snapshot read at block R serves block S (the response's `blockHeight`) without RPC when its revision equals the current repair revision and either R = S with the same hash, or R < S, R's hash is **still canonical** (finalized header at R, or the current tail block at R), the token is not in the always-refresh list, and no Transfer of that token touching the account exists in (R, S]: the finalized `holdings.last_block` ≤ R **and** no matching tail Transfer above R (the latest overlay folds tail Transfers into `lastBlock`). Canonicality is computed by joining `block_headers`/`tail_blocks` in the same repeatable-read snapshot as holdings and tail rows, so a tail reorg of R (seen by any replica's tail leader) makes the snapshot non-reusable, and a newer snapshot on an orphaned block may be overwritten by an older canonical read. If the served tail block itself is reorged out while the Multicall runs, the values are returned (labelled with that block) but not cached. Any repair or rebuild bumps the revision and so invalidates every snapshot; repairs also delete touched accounts' snapshots and snapshots inside the repaired range. A snapshot newer than S (e.g. a latest read, then a `finality=finalized` request) is not reused and the finalized read is not cached over it.

**Response fields per token:** `balance` or `balanceError`, `balanceStatus`, `balanceBlock`/`balanceBlockHash` (always the block actually read; never relabelled), `balanceReadAt`, `balanceCached` (no RPC in this request), and `balanceValidAt` (= `blockHeight`) only when the value provably holds at the served block. Top-level `balancesCurrent` is true when every token has `balanceValidAt`. A per-token revert or bad return data is stored as an error at its block and is reused under the same rule, but is never given `balanceValidAt`. Outage, rate limit or auth failure fails the request with **503** and caches nothing.

**Always-refresh tokens** (balance changes without a Transfer: WETH deposits, stETH/AMPL rebases, Aave aToken interest) are re-read once per new served block: with the tail that is every new tip (~12 s), not every finalized epoch. Mainnet defaults live in `src/networks.ts`; `BALANCE_ALWAYS_REFRESH_TOKENS` replaces the list. It is a starter list, not exhaustive, and the addresses were not re-verified on chain in this PoC; any unlisted token of that kind can be served stale under the Transfer rule.

**Coalescing.** Uncached (account, token) pairs at the same block hash and revision are single-flight and join one open batch. With latest, requests in different slots read different tip hashes, so batches split at every new block. A batch is sent when it reaches `ETH_MULTICALL_BATCH` pairs or `BALANCE_BATCH_WINDOW_MS` after its first pair, so concurrent requests for different accounts share Multicalls. Batches run through the bounded request-RPC limiter (`REQUEST_RPC_CONCURRENCY`/`REQUEST_RPC_QUEUE`); a full queue is 503 `Retry-After`, and each request waits at most `REQUEST_DEADLINE_MS`. Late reads still save, but `saveBalances` rejects a read crossing a repair (503, retry) and never replaces a snapshot from a newer block.

**Cost at 1M accounts** (assumptions: every account polls once a minute, 20 ERC-20 holdings each, 1,000 pairs per call, 20 credits per `eth_call`, finalized cursor advancing once per 6.4-minute epoch, ~5M Transfers/day, 10% of accounts holding one always-refresh token). The table is for `finality=finalized`; with the default latest view, Transfer-touched pairs are the same, but always-refresh pairs are re-read on every poll (each minute sees a new tip): 100k × 1,440 = 144M pairs/day (~144k full calls, ~2.9M credits), and a reorg adds re-reads of pairs whose snapshot block was orphaned. The tail itself costs one `latest` header poll + one `eth_getLogs({ blockHash })` per block (~7,200 of each per day, independent of users), plus at most two extra polls in a slot whose block is late.

| | Pairs/day | Calls/day | Credits/day |
|---|---|---|---|
| Re-read on every request, perfectly batched | 28.8B | 28.8M | 576M |
| Reuse: Transfer-touched pairs (≤ 2 × 5M) | ≤ 10M | | |
| Reuse: always-refresh (100k × 225 epochs) | 22.5M | | |
| Reuse total, batches full | ~32.5M | ~33k | ~0.65M |
| Reuse total, 25 ms window (~9 pairs per batch at ~376 pairs/s) | ~32.5M | ≤ 3.5M | ≤ 69M |

Unchanged polls cost no RPC, so cost follows real balance changes. At this load the window, not the pair count, bounds calls: 250 ms gives ≤ 346k calls (≤ 7M credits) per day for ≤ 250 ms extra latency. One-time first reads add 20M pairs (≈ 0.4M credits batched). Each replica batches independently, so multiply the window bound by the replica count.

## Near-realtime tail (liveness)

The durable index stays finalized-only (~13 min behind the tip). Liveness comes from a replaceable journal above it, `tail_blocks` (number/hash/parent hash/timestamp) and `tail_transfers` (same column types as `transfers`), which is never folded into `holdings`/`nft_ownership`. Because the index is global, the tail stores every `Transfer` of every block; there is no tracked-account logic.

1. **Leader only, slot aligned.** Only the process holding the follower's advisory lock polls `latest`, at slot +2.5/+5/+9 s (`TAIL_POLL_OFFSETS_MS`); once the slot's block is in, the remaining offsets are skipped. A direct successor costs one header and one `eth_getLogs({ blockHash })`; catch-up fetches headers by number, verifies the parent-hash chain and accepts its range query (split on too-large/timeout) only if every log hash matches its header.
2. **Readiness first.** The tail runs only once the global index exists and its cursor is within an epoch of the observed finalized head; while bootstrap or catch-up is pending it waits (the service stays `not_ready`) and wakes the follower instead of fetching headers the next commit would prune.
3. **Lagging nodes.** A head at or below the finalized cursor, or a lower head whose hash the tail already stores, is a lagging endpoint and ignored. A header that changes mid catch-up (load-balanced nodes) is retried, not counted as a reorg.
4. **Reorgs.** A parent mismatch walks stored vs canonical hashes back to the common ancestor, rewinds only tail rows above it and re-ingests. No ancestor within `TAIL_MAX_DEPTH`, or only the finalized cursor, wipes the tail: responses fall back to finalized with `tailError` until the next tick rebuilds it. Rewind and wipe are fenced on the tip hash the leader observed, and appends on the expected parent, so a failover writer cannot lose rows.
5. **Finalization.** `appendRange` deletes tail rows ≤ the new cursor in the same transaction that advances it; a different tail hash at the new cursor wipes the tail. A repair that replaces the cursor block also wipes it. There is never a moment where a Transfer is counted twice or missing between the two tables.
6. **Finality stalls.** The tail stops `TAIL_MAX_AHEAD_BLOCKS` above the cursor and degrades to finalized with `tailError`, bounding rows and overlay cost. At each epoch boundary (and every 4 slots until finalized moves) it wakes the follower.
7. **Shared freshness.** Every replica serves latest from the database tip; latest is served only while the tip's own block timestamp is at most `TAIL_MAX_TIP_AGE_MS` old, so a stalled leader degrades all replicas, not just itself. `/health` shows each process's tail status; non-leader processes report the tip loaded at startup.

## Ingestion and repair

1. The bootstrap (above) publishes [S, B] and `ingestion_state` at B in one transaction; `initialize(network, startBlock, anchorHeader, at)` with the header at `startBlock - 1` remains for small and test setups.
2. `appendRange(network, range, headers, transfers, at)` receives all headers, including empty blocks, and all decoded transfers. It checks range coverage, parent linkage and log hashes, locks the network cursor and atomically commits raw rows, derived rows and progress. A fully committed replay is a no-op; corrections require replacement.
3. The finalized follower holds a per-network session advisory lock, fetches complete header chains with bounded concurrency, then unfiltered `getLogs`. It has no account set. Each range uses the same append API; cursor locking also protects against competing writers.
4. Gaps/hash disagreements enter `repair_queue`. An alternate-source repair runner (pending) refetches the suspect range and calls `replaceRange`. The transaction checks both neighbors, remembers old and new participants, replaces raw rows/headers, rebuilds touched accounts from their complete retained histories, and restores the latest owner for touched NFTs. It invalidates affected caches and increments the repair revision. Fully covered queue entries are removed only with a successful replacement/append.
5. `rebuildRange` reconstructs derived data for accounts/items touched by an existing raw range, without changing raw transfers. It deliberately does not resolve a suspected-source repair.

All database conversions are in `src/store/columns.ts` plus the name-to-ID registry lookup in `createStore`. Public contracts keep `Address`, `Hex` and `bigint`. A new EVM network gets an immutable smallint database ID and adapter configuration in `src/networks.ts`; non-EVM networks can implement `ChainAdapter`. Separate RPC endpoint wiring is still required when networks use different providers.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `RPC_URL`, `RPC_FALLBACK_URLS` | required, empty | Primary and comma-separated fallback RPC endpoints |
| `DATABASE_URL` | local `iseeu` PostgreSQL database | Connection string |
| `START_BLOCK` | 25933634 | Network start block S (plain number); must match the stored index or bootstrap run |
| `BOOTSTRAP_ON_START` | false | Run the bootstrap inside `npm start` when the index is missing |
| `BOOTSTRAP_RPC_WORKERS`, `BOOTSTRAP_RPC_LOG_RANGE`, `BOOTSTRAP_RPC_TIMEOUT_MS` | 4, 100, 60000 | Quicknode segment workers, blocks per `eth_getLogs`, bulk call timeout |
| `BOOTSTRAP_SEGMENT_BLOCKS`, `BOOTSTRAP_HEADER_CHUNK_BLOCKS` | 1000, 5000 | Segment (commit/resume unit) and header-request sizes |
| `BOOTSTRAP_MAINTENANCE_WORK_MEM` | 1GB | `maintenance_work_mem` for the finalize index builds |
| `HYPERSYNC_API_KEY`, `HYPERSYNC_URL`, `HYPERSYNC_TIMEOUT_MS` | unset, `https://eth.hypersync.xyz`, 120000 | Optional bootstrap source; per-request idle timeout |
| `PORT` | 3000 | HTTP port |
| `RPC_TIMEOUT_MS`, `RPC_MAX_ATTEMPTS`, `RPC_MAX_RPS` | 15000, 6, 20 | Timeout, retry budget, shared outgoing rate cap |
| `ETH_FOLLOWER_MAX_RANGE` | 500 (maximum 1000) | Unfiltered log range; large responses require bounded memory |
| `FOLLOWER_POLL_MS` | 30000 | Finalized-head poll interval |
| `FOLLOWER_MAX_RANGES_PER_TICK` | 8 | Bounded catch-up per tick |
| `FOLLOWER_HEADER_CONCURRENCY` | 8 | Concurrent block-header reads |
| `MAX_HEAD_OBSERVATION_AGE_MS` | 120000 | Shared head observation expiry |
| `TAIL_ENABLED` | true | Run the tail and serve `finality=latest` by default; false serves finalized only |
| `TAIL_MAX_DEPTH` | 64 | Deepest reorg rewound in place; deeper ones wipe the tail |
| `TAIL_POLL_OFFSETS_MS` | 2500,5000,9000 | `latest` poll offsets within each 12 s slot |
| `TAIL_MAX_AHEAD_BLOCKS` | 1024 | Stop appending this far above the finalized cursor (finality stall, ~3.4 h) |
| `TAIL_MAX_TIP_AGE_MS` | 60000 | Serve finalized with `tailError` when the shared tail tip block is older |
| `FINALIZED_MAX_NO_PROGRESS_MS` | 1200000 | Head tracker stops trusting a non-advancing head |
| `MAX_HEAD_AGE_MS` | 1800000 | Refuse reads after index progress stops this long |
| `ETH_MULTICALL_BATCH` | 1000 | Calls per Multicall3 batch; also the shared balance batch size |
| `BALANCE_ALWAYS_REFRESH_TOKENS` | `src/networks.ts` list | Comma-separated tokens re-read at every new cursor; replaces the defaults |
| `BALANCE_BATCH_WINDOW_MS` | 25 | Wait for other requests' pairs before sending a partial Multicall |
| `REQUEST_DEADLINE_MS` | 10000 | On-request RPC wait budget |
| `REQUEST_RPC_CONCURRENCY`, `REQUEST_RPC_QUEUE` | 16, 256 | Concurrent and queued request-path Multicalls (balance batches, ownership) |
| `MAX_VERIFY_OWNERSHIP_ITEMS` | 2000 | Cap on optional ownership verification |
| `DB_POOL_MAX`, `DB_CONNECT_TIMEOUT_MS` | 20, 5000 | Pool size and acquisition timeout; one connection per follower leader lock |

`scripts/compare-sources.ts` with `src/sources/hypersync.ts`, `rpc-logs.ts` and `fallback.ts` remains a per-account RPC/HyperSync comparison diagnostic, not wired into discovery or ingestion. The bootstrap has its own global sources in `src/bootstrap/sources.ts` and shares the HyperSync pager.

## Limits

Only standard three-topic ERC-20 and four-topic ERC-721 Transfer logs are included. Pre-start untouched holdings, native ETH, ERC-1155, non-Transfer balance changes and spam filtering remain outside discovery. Malformed and removed logs are skipped.

RPC retains bounded retries, rate limiting, endpoint breakers/failover, unsupported-method memory, result validation and split-on-size/timeout log queries. A linked header chain detects fork/gap inconsistencies, **not silently omitted logs**. Alternate-source comparison and queue processing remain operational work.

Latest data is unfinalized: rows above `finalizedBlockHeight` and balances read at the tip can be reorged out (the tail repairs itself within one slot; deep reorgs degrade to finalized). History pages are not reorg-stable across the tail. The tail stores every Transfer of up to `TAIL_MAX_AHEAD_BLOCKS` blocks (normally 64–96 blocks, roughly 100k rows; a stalled finality grows this toward ~1M rows). Each latest request adds one indexed `tail_transfers` lookup. Full-window bootstrap throughput (projected above, measured only on 2,001 blocks), 180M-row query plans, vacuum/WAL behavior and one-million-account load have not been benchmarked. Large repairs serialize that network's writer and may scan substantial account history. See the report's partitioning tradeoff before the full bootstrap.
