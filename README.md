# i-see-u: token discovery over plain JSON-RPC (PoC)

For an Ethereum account, returns every ERC-20 contract it sent or received (and, as a bonus, ERC-721 collections and token IDs) from block 25,933,634 to a near-realtime, reorg-aware latest head, optionally with balances labelled with the exact block hash they were read at. A finalized aggregate remains the immutable base; a short journal of unfinalized Transfers is overlaid at read time. It works with only JSON-RPC; an experimental, opt-in Envio HyperSync source (`LOG_SOURCE=auto|hypersync`) can accelerate cold per-account backfills and falls back to RPC per chunk. No Blockbook or full indexer is required.

Design notes and measurements: [REPORT.md](REPORT.md).

## Run

```sh
npm install
cp .env.example .env                # replace RPC_URL with your endpoint
docker compose up -d postgres       # PostgreSQL 18 on 127.0.0.1:5432
npm start                            # migrates the database, starts the follower and balance worker, serves :3000
npm test                             # PGlite tests; no Docker or network needed
npm run test:watch                   # watch mode
```

The project-root `.env` file is loaded with Node's `process.loadEnvFile()`; existing environment variables take precedence. The development `DATABASE_URL` default matches `docker-compose.yml`, so copying `.env.example` is enough. The database is a derived cache, but migrations preserve it across schema-compatible releases.

After changing `src/store/schema.ts`, run `npm run db:generate` and commit the generated SQL and metadata in `drizzle/`. `npm run db:migrate` applies pending migrations without starting the API; `npm start` does the same automatically.

Optional env (numeric values must be positive integers; invalid values fail at startup):

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://iseeu:iseeu@localhost:5432/iseeu` | PostgreSQL connection |
| `RPC_FALLBACK_URLS` | | Comma-separated endpoints tried after `RPC_URL` |
| `LOG_SOURCE` | `rpc` | `rpc`, `auto`, or `hypersync`. HyperSync is experimental and off by default; `auto` enables it only when its key is set |
| `HYPERSYNC_API_KEY` | | Optional token. When absent, auto mode never imports the native addon and uses RPC only |
| `HYPERSYNC_URL` | network default (`https://eth.hypersync.xyz`) | HyperSync endpoint override |
| `HYPERSYNC_MAX_RANGE` / `HYPERSYNC_TIMEOUT_MS` | 100000 / 30000 | Blocks per committed HyperSync chunk and whole-stream timeout |
| `PORT` | 3000 | HTTP port |
| `RPC_TIMEOUT_MS` / `RPC_MAX_ATTEMPTS` / `RPC_MAX_RPS` | 15000 / 6 / 20 | Per attempt timeout, attempts per call, client-side rate limit |
| `ETH_FOLLOWER_MAX_RANGE` | 500 | Blocks per unfiltered follower `getLogs` (about 450 KB of JSON per block; must be at most 1000) |
| `FOLLOWER_POLL_MS` | 390000 (30000 with `TAIL_ENABLED=false`) | Finalized-head poll interval. With the tail, the follower is woken whenever the tail tip enters a new epoch, so this is only a fallback (finalized advances about every 6.4 min) |
| `FOLLOWER_MAX_LAG_BLOCKS` | 96 | Discovery returns 503 when the follower is further behind finalized (about 3 epochs) |
| `TAIL_ENABLED` | true | Persist and serve the reorg-aware near-realtime Transfer tail |
| `TAIL_MAX_DEPTH` | 64 | Maximum rollback search above the finalized cursor before wiping and rebuilding the tail |
| `TAIL_POLL_OFFSETS_MS` | `2500,5000,9000` | Poll offsets within each 12 s Ethereum slot |
| `TAIL_MAX_AHEAD_BLOCKS` | 1024 | Stop appending this many blocks above the finalized cursor (~3.4 h; normally 64-95). If finality stalls the tail stops growing and reads fall back to finalized with `tailError` |
| `TAIL_MAX_TIP_AGE_MS` | 60000 | Serve `latest` only while the shared tail tip's block timestamp is at most this old (5 slots); otherwise finalized with `tailError`. Checked in every process, so a stalled leader degrades non-leaders too |
| `FINALIZED_MAX_NO_PROGRESS_MS` | 1200000 | Responses are `stale` after the follower has not advanced for this long |
| `MAX_HEAD_AGE_MS` | 1800000 | Discovery returns 503 after the follower has not advanced for this long |
| `ACTIVE_ACCOUNT_TTL_MS` | 300000 | Accounts that requested balances within this window are refreshed by the balance worker |
| `BALANCE_REFRESH_MS` | 60000 | Balance worker interval (it also runs whenever the follower advances) |
| `ETH_MULTICALL_BATCH` | 1000 | `balanceOf` calls per Multicall3 `aggregate3` |
| `ETH_LOGS_MAX_RANGE` / `ETH_LOGS_MIN_RANGE` | 3000 / 500 | Adaptive range of address-filtered backfill `getLogs` |
| `ETH_SCAN_CONCURRENCY` | 8 | Backfill chunks in flight per account |
| `REQUEST_DEADLINE_MS` | 10000 | How long a request waits for RPC work (cold backfill, first balance read, ownership check) |
| `BACKFILL_CONCURRENCY` / `BACKFILL_QUEUE` | 4 / 64 | Concurrent account backfills, and how many more may wait before requests get 503 |
| `REQUEST_RPC_CONCURRENCY` / `REQUEST_RPC_QUEUE` | 16 / 256 | Same for first balance reads and `verifyOwnership` |

## Data sources

The finalized follower, head reads, Multicall balances and ownership checks always use JSON-RPC. Only the per-account historical backfill is pluggable. The default `LOG_SOURCE=rpc` is the zero-extra-service path. HyperSync is experimental and opt-in: `auto` chooses streaming HyperSync when `HYPERSYNC_API_KEY` is present, and `hypersync` requires a key. HyperSync ORs sent/received filters in one query and RPC reruns a complete chunk after any primary failure, so the store never sees partial vendor data.

HyperSync serves through the unfinalized chain head, but this service queries only through the already pinned finalized backfill target and sends `toBlock = target + 1` because its end is exclusive. The free tier exposed no limit headers in the 2026-10-14 measurements, but 429s, auth failures and vendor outages remain possible and trigger fallback/cooldown. The client is a N-API native addon; it is loaded lazily only when selected, and its streams are closed on completion, timeout and shutdown.

## API

| Endpoint | Returns |
|---|---|
| `GET /v1/ethereum/accounts/{address}/erc20?balances=true&finality=latest` | `{ status: "ready", blockHeight, blockHash, finality, finalizedBlockHeight?, finalizedBlockHash?, tailError?, …, tokens: [{ contract, …, sentCount, receivedCount, unfinalizedSentCount?, unfinalizedReceivedCount?, balance?, balanceBlock?, balanceBlockHash? }] }` |
| `GET /v1/ethereum/accounts/{address}/erc721?verifyOwnership=true&finality=latest` | `{ status: "ready", blockHeight, blockHash, …, collections: [{ contract, tokens: [{ tokenId, heldBySeenLogs, unfinalized?, owner?, ownedByAccount? }] }] }` |
| either, while the account's backfill runs | **HTTP 202** `{ status: "syncing", progressBlocks, targetBlocks, followerLagBlocks }` with `Retry-After: 2` |
| `GET /health` | uptime; follower, tail (tip/lag/reorg depth/health), and balance status; source and RPC stats |

Chain quantities are decimal strings; `dataAgeS` is an integer. `finality` defaults to `latest` while the tail is enabled and healthy. Then `blockHeight`/`blockHash` identify the tail tip and `finalizedBlockHeight`/`finalizedBlockHash` identify its durable base. `finality=finalized` returns the original finalized discovery shape (only `balanceBlockHash` is added per balance while the tail is enabled; with `TAIL_ENABLED=false` every response is exactly the pre-tail shape). An unhealthy tail safely falls back to finalized and adds `tailError`. Cursor, holdings, matching tail Transfers, and tail tip are read in one repeatable-read transaction, so a concurrent finalization prune cannot double-count or omit a Transfer.

Each balance carries `balanceBlock`, and with the tail enabled also `balanceBlockHash` (EIP-1898; a tail block can be reorged out). With the tail enabled, reads target the tail tip hash (also for `finality=finalized`, so `balanceBlock` can be above `blockHeight`); otherwise they target the finalized cursor. A snapshot is never relabelled at a later block, and `balancesAtBlockHeight` is true only when both number and hash match the served block. Balances refresh every `BALANCE_REFRESH_MS`, not every block, so with the tail they usually trail `blockHeight` by a few blocks and `balancesAtBlockHeight` is usually false.

Errors: 400 (address/flags), 404 (network), 503 with `Retry-After` when the follower is unhealthy (too far behind or not advancing), the RPC is down, or request-path RPC is saturated or past `REQUEST_DEADLINE_MS`.

## How it works

`RpcClient` is the only component that performs JSON-RPC HTTP. It owns error classification, retries, per-endpoint circuit breakers and unsupported-method memory, result validation with failover, rate limiting, and per-attempt statistics. A viem `PublicClient` sits on top through a custom transport with viem retries and call batching disabled. viem supplies RPC/action types, standard ABIs, calldata and topic encoding, strict event/result decoding, and the block/call actions. Inside `RpcClient`, one viem `http()` transport per endpoint (retries and batching off) does the HTTP/JSON-RPC mechanics; viem's retry, `shouldRetry` and `fallback` never run, so every attempt is billed and recorded once. The optional HyperSync source uses its own streaming client only for backfill.

Durable aggregates are **finalized-only**. Unfinalized data lives only in a replaceable Transfer journal and is never folded into `holdings`, `nft_items`, the follower cursor, or backfill state.

1. **Shared finalized follower (one per network).** A background loop polls the finalized head every `FOLLOWER_POLL_MS`, and immediately when the tail tip enters a new beacon epoch (the finalized checkpoint only moves at epoch boundaries), then every 4 slots of that epoch until finalized has moved. When it is ahead of the global cursor (`follower_cursor`: number, hash, timestamp, updated_at), the follower fetches every `Transfer` log of (cursor, head] **without an address filter**, in ranges of at most `ETH_FOLLOWER_MAX_RANGE` blocks, with the same strict decoding and split-on-too-large/timeout as the backfill. Logs whose from/to are not tracked are dropped before decoding; the rest fold into the same per-account aggregates the backfill uses. Each range commits in one transaction together with the cursor advance (a range that ends below the head costs one extra `eth_getBlockByNumber` for its hash). At the head, one epoch (32 blocks, about 22k logs, about 14 MB) is one `getLogs` call.
   - **Exactly one follower per network.** The loop runs only while it holds a PostgreSQL **session** advisory lock (`pg_try_advisory_lock`) on a dedicated pool connection, held for the follower's lifetime and re-verified every tick. Other processes retry acquisition each poll and meanwhile only read the cursor. Independently, each commit locks the cursor row and refuses a range that does not continue it, so a lost lock can never double-apply a range.
   - **Tracked accounts** are kept in an in-memory set loaded from `tracked_accounts`. At commit, the store compares the set the range was decoded against with the table; if an account was tracked in the meantime, nothing is applied and the range is refetched with it included.
   - **Health.** The follower reports its lag (finalized head minus cursor) and when the cursor last advanced. Responses are `stale` after an RPC failure on the last poll, a lag above one epoch, or no advance for `FINALIZED_MAX_NO_PROGRESS_MS`; discovery returns 503 above `FOLLOWER_MAX_LAG_BLOCKS` or after `MAX_HEAD_AGE_MS` without an advance.
2. **Near-realtime tail.** Only the follower leader polls `latest`, aligned to slot +2.5/+5/+9 s; once the slot's block is in, the remaining offsets are skipped (a late or missed block is retried at +5/+9 s). A direct successor costs one header and one `eth_getLogs({ blockHash })`; catch-up (startup, gaps) fetches headers by number, verifies their parent-hash chain, and accepts its range query (split on too-large/timeout like the follower) only if every log hash matches its header. A head at or below the finalized cursor, or a lower head whose hash we already store, is a lagging node and ignored; a header that changes mid catch-up is retried, not counted as a reorg. While the follower is more than an epoch behind finalized (startup, outage), the tail waits and wakes it rather than fetching headers the follower's commits would prune. Parent mismatch walks stored/canonical hashes back to a common ancestor, rewinds only tail rows, then re-ingests. Reaching the finalized cursor or exceeding `TAIL_MAX_DEPTH` wipes the tail, serves finalized with `tailError`, and rebuilds safely on the next tick. Finalization fetches the block again through the existing follower, then prunes tail rows ≤ cursor in the same transaction (or wipes on a hash mismatch). On restart the persisted tip is re-validated (in `load()` when already leader, otherwise on the first tick, which compares the stored tip against `latest`'s parent chain).
3. **Backfill → follower handoff.** A new account is tracked in one transaction that takes a share lock on the cursor row, inserts `tracked_accounts`, and records the cursor C as its `backfill_target`. A follower commit therefore happens entirely before (its blocks are ≤ C) or entirely after (it applies to the account, blocks > C). Because the tail stores every Transfer, accounts tracked mid-tail need no second handoff and are complete immediately after their finalized backfill.
   - Backfill uses the network's `AccountLogSource`. With a key, `auto` streams both account directions in one HyperSync query, folds pages immediately (never `collect()`), and commits 100k-block chunks in order. Any auth, rate-limit, server, timeout, lagging-height, or malformed-response failure reruns that entire uncommitted chunk through RPC. After two consecutive failures a 60 s breaker skips HyperSync. Without a key, RPC is used directly: two `eth_getLogs` directions per chunk, strict shared decoding, and self-transfer dedupe.
   - Up to `ETH_SCAN_CONCURRENCY` chunks are in flight; completed chunks commit strictly in order with `progress_to`. A partial stream is never exposed or committed. Direct RPC alone persists its adaptive per-account range (halve on heavy/split, grow after three light chunks). HyperSync uses its fixed large range; an RPC fallback recursively splits provider-limited requests but the composite chunk remains the atomic resume unit.
   - A request waits up to `REQUEST_DEADLINE_MS` for the backfill and otherwise answers **202 `syncing`** with `progressBlocks`/`targetBlocks`; the backfill continues in the background. Concurrent requests share one backfill. Backfills run through a bounded limiter (`BACKFILL_CONCURRENCY`, `BACKFILL_QUEUE`); beyond it requests get 503 with `Retry-After`.
   - Scan progress stored by the pre-follower version is resumed when it is at or below C, and discarded (the account is rebuilt) when it is above C.
4. **Shared balance worker.** Accounts that requested balances within `ACTIVE_ACCOUNT_TTL_MS` are active. Every `BALANCE_REFRESH_MS`, and immediately after a tail reorg (or after a follower advance when the tail is disabled), the worker reads at one hash: tail tip when enabled, otherwise finalized cursor. A read at a hash that was just reorged out (`unknown block`/`header for hash not found`) is transient; the next refresh uses the new tip. Tail-only ERC-20 pairs are included. Results are packed into Multicall3 and retain their number and hash.
5. **Request path.** With the account backfilled, a request reads cursor, holdings, NFT items, matching tail rows, and tail tip in one repeatable-read snapshot, plus balance snapshots. Only a missing balance snapshot, `verifyOwnership`, or cold backfill uses request-path RPC.
6. **Classification.** A `Transfer` log with 3 topics is decoded strictly with the ERC-20 event ABI. With 4 topics it is decoded with the ERC-721 variant, where `tokenId` is indexed. Malformed and `removed: true` logs are skipped.
7. **Storage.** PostgreSQL 18 stores finalized aggregates plus `tail_blocks` and `tail_transfers`. Individual Transfers are stored only for the bounded unfinalized tail (roughly 30–60k rows), then pruned on finalization. Drizzle migrations run before startup.

`lagBlocks` uses the highest `latest` block number cached by head tracking, which only the follower's poll refreshes.

## Code layout

```
src/core/types.ts         shared contracts (ChainAdapter, RpcClient, Store, DiscoveryService)
src/core/aggregate.ts     per-chunk aggregate folded from decoded Transfers (self-transfer dedupe, ERC-721 latest wins)
src/rpc/                  JSON-RPC client: viem http() per endpoint for transport mechanics; our policy on top: failover,
                          backoff with jitter, capped Retry-After, circuit breaker, token-bucket limiter, error classification
                          (rate_limited / transient / too_large / unsupported / fatal); rpcErrorOf() unwraps viem-wrapped errors
src/chains/evm/           EVM adapter: logs.ts (typed topics/strict decoding and range splitting), head.ts (finalized head via a
                          viem PublicClient over RpcClient, retries and batching disabled), multicall.ts, adapter.ts
src/sources/              per-account backfill sources: RPC wrapper, streaming HyperSync, and generic fallback/breaker
src/networks.ts           network registry: chain/id, start block, HyperSync default and Multicall3 address
src/store/schema.ts       Drizzle PostgreSQL schema
src/store/store.ts        async Store queries and transactional upserts, shared by pg and PGlite
src/store/postgres.ts     pg Pool startup and automatic Drizzle migrations
src/store/advisory-lock.ts advisory locks: transaction-scoped helper, and the session-level follower leader lock
drizzle/                  generated, commit-ready SQL migrations and metadata
src/follower/follower.ts  shared finalized follower: leader lock, unfiltered getLogs per range, health
src/tail/tail.ts          slot-aligned latest follower: hash-pinned logs, catch-up, reorg rewind/rebuild, health
src/balances/worker.ts    shared balance worker for active accounts; first-request reads (single-flight)
src/discovery/service.ts  tracking, backfill to the handoff target (resumable, single-flight), database reads
src/core/limiter.ts       bounded concurrency and per-request deadlines for request-path RPC
src/api/routes.ts         Hono routes
```

A new EVM network is one entry in `src/networks.ts`; the generic config/server construction supplies its source and `createEvmAdapter` (give it separate RPC/env wiring when endpoints differ). A non-EVM family implements `ChainAdapter`. A new backfill provider implements the small `AccountLogSource` contract (`name`, preferred chunk range, aggregate-returning `scanAccount`, stats/optional close) and can be composed with `withFallback`; discovery contains no provider switch.

For a live, finalized comparison run `npm run compare:sources -- <address> [fromBlock] [toBlock]`. It loads `.env` without printing secrets and reports chunks, upstream requests, time, logs, ERC-20 tokens, NFT items and aggregate equality. The default full window keeps RPC at its normal 3k-block ranges (about 172 `eth_getLogs` calls); pass a smaller range for a cheaper smoke check.

## Unreliable RPC

viem's `http()` transport builds requests, enforces the timeout and parses responses; `RpcClient` decides everything else. viem's built-in policy would get this wrong for us: it retries `-32005` even when the message says the range is too large (we must split instead), does not treat Quicknode `-32010` as a rate limit, cannot fail over on a null result, has no breaker, and would wait a full hour-long `Retry-After`; it would also hide retried attempts from the billing stats.

| Failure | Handling |
|---|---|
| 429, `-32007`, `-32010`, "limit reached" | Exponential backoff with full jitter, honouring `Retry-After`. Client-side limiter at `RPC_MAX_RPS` |
| 5xx, timeouts, network errors, "header not found" | Retry, then fail over to the next endpoint. The circuit breaker opens after 5 consecutive failures |
| Timeout on a multi-block `eth_getLogs` | Not retried with the same range: the error carries `timeout: true` and the range is split in half, like "range too large". It does not count against endpoint health. A single-block range retries normally |
| "more than N results", "range too large" | Split the block range. A range suggested in the error message is used when the provider gives one |
| "method not available", archive refused | Skip that endpoint for that method for 10 minutes |
| Null or invalid block result | Count it as a transient endpoint failure and retry the next endpoint in the same request |
| Null or non-array log result | Count it as a transient endpoint failure and retry the next endpoint in the same request |
| Unknown/reorged block hash | Transient retry/failover; no tail row is committed until the complete block/range validates |
| By-number catch-up returns a log from another fork | Reject the whole batch unless `log.blockHash` equals the fetched header hash |
| Latest parent mismatch | Find a common stored/canonical ancestor, rewind tail rows above it, and re-ingest by hash |
| Node behind the cached head | Keep the newer cached finalized block; the follower does not move its cursor backwards |
| Follower poll fails (head or `getLogs`) | The cursor stays put and the range is retried next poll; responses are `stale: true` until a poll succeeds |
| Follower falls behind finalized | `followerLagBlocks` grows; `stale` above one epoch, 503 with `Retry-After` above `FOLLOWER_MAX_LAG_BLOCKS` |
| Finalized (or the follower) stops advancing | `stale` after `FINALIZED_MAX_NO_PROGRESS_MS`, 503 after `MAX_HEAD_AGE_MS`; clears on the next advance |
| Request-path RPC saturated or slow | Bounded limiters and `REQUEST_DEADLINE_MS`: 202 `syncing` for backfills, 503 with `Retry-After` otherwise |
| HyperSync failure or timeout | Discard the partial aggregate, close the stream, rerun the whole chunk through RPC; breaker cooldown avoids retrying a dead primary on every later chunk |

## Limits (PoC)

- **Pre-start tokens.** Tokens held before block 25,933,634 and never moved since are not discovered.
- **Balances without `Transfer`.** Tokens that change balances without emitting `Transfer` are not discovered or tracked. Examples: WETH deposits, rebasing tokens. ERC-1155 is not supported.
- **Spam.** Spam and address-poisoning tokens are returned. Many of them fail `balanceOf` and show `balanceStatus: "error"`.
- **Polling cost.** Warm requests cost no RPC. The tail adds about 1.2–1.4 header calls (measured) and one `getLogs` per block (about 9.5–10.4M credits/month); `finalized` is polled once per epoch instead of every 30 s (saving about 3.2M), so steady state at 5k users is about 53–54M credits/month, dominated by the 60 s balance sweep (REPORT §4).
- **Single process.** The active-account set, single-flight maps and limiters are in process memory, and the balance worker runs in every process. Only the follower is protected against concurrent processes (advisory lock). Several API replicas would need the active set in the database (or Redis) and one balance-worker leader.
- **Freshness of balances.** Balances are at most `BALANCE_REFRESH_MS` (plus one refresh duration) older than the cursor, and every token says which block it was read at. Accounts idle for `ACTIVE_ACCOUNT_TTL_MS` are no longer refreshed; their next request is served from the old snapshot (labelled with its block) and re-activates them.
- **Tracked accounts are never untracked.** The follower applies logs for every account ever requested; the per-tick cost is independent of their number, but the set and the commit-time check grow with it.
- **Tail degradation.** Normal discovery trails the tip only by its slot polling delay. A reorg deeper than `TAIL_MAX_DEPTH`, or one reaching the finalized cursor, temporarily serves the roughly 13-minute-old finalized base with `tailError` while the tail rebuilds. The same fallback applies when the tail tip block is older than `TAIL_MAX_TIP_AGE_MS` (checked in every process from the shared tip), and when the tail reaches `TAIL_MAX_AHEAD_BLOCKS` above the finalized cursor because finality stalled (it stops appending until finality resumes).
- **Balance cadence.** Only tail reorgs kick the shared worker; otherwise the 60 s sweep follows the tip. A per-block refresh of only the pairs touched by that block's Transfers is the next step (not implemented).
- **Tail on non-leader replicas.** Only the follower leader runs the tail. Another replica serves the leader's tail rows from Postgres, but its own `/health` tail status (and the health check gating `finality=latest`) is not live.
- **HyperSync boundary.** HyperSync itself serves through the unfinalized chain head, but backfill targets are pinned to the follower's finalized cursor and queries always end exactly at `target + 1` (exclusive). A source below the requested height fails to RPC.
- **HyperSync is experimental (off by default).** Normal and NFT accounts matched RPC exactly (1 request vs 172 calls), but the stream for a 100k-block chunk of the Binance hot wallet (`0x28c6…`, about 1M logs per chunk) hung and never finished, even with a 600 s timeout. The chunk then fails over to RPC after the timeout. Undiagnosed; until it is fixed, use `LOG_SOURCE=rpc`.
- **Vendor/native dependency.** HyperSync is an optional hosted vendor/free-tier risk (auth, throttling and availability). Its package is a N-API native addon: it is dynamically imported only when configured; shutdown closes streams and drops the client reference. RPC remains the zero-extra-service path.
- **Heavy accounts.** An exchange hot wallet can mean about 2.5M logs (about 1.6 GB of JSON) for the window. The scan is then bound by download throughput (about 75 s measured for Binance's hot wallet), and the adaptive range trades RPC calls for smaller responses: about 320-450 calls instead of 172. Every response is still parsed whole by viem, so peak RSS is about 1 GB at concurrency 8, mostly in-flight responses plus GC slack. Each cold scan commits per chunk and resumes after the last committed chunk following a failure.
- **Concurrency and rate limit.** At the default `RPC_MAX_RPS` of 20, concurrency 8 (16 requests in flight) already saturates the limiter for a normal account: 172 calls take about 8.6 s, and concurrency 16 measured no faster. The limiter is shared with the follower and the balance worker; a live cold backfill on 2026-10-14 took about 26 s for the same 172 calls (so with the default 10 s deadline, the first requests answer 202).
- **Reorgs.** Durable summaries contain finalized blocks only. Ordinary latest-chain reorgs delete and re-ingest only tail rows; a consensus-level finality failure is still out of scope.
- **PostgreSQL required at runtime.** The API does not fall back to embedded storage. Tests use PGlite and exercise the same generated migrations without Docker.
