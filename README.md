# i-see-u: token discovery over plain JSON-RPC (PoC)

For an Ethereum account, returns every ERC-20 contract it sent or received (and, as a bonus, ERC-721 collections and token IDs) from block 25,933,634 to Ethereum's finalized head, optionally with balances labelled with the block they were read at. It needs nothing but a JSON-RPC endpoint: no Blockbook, no indexer service. RPC cost is driven by the block rate and the number of active (account, token) pairs, not by request volume: once an account is backfilled, its requests are pure database reads.

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
| `PORT` | 3000 | HTTP port |
| `RPC_TIMEOUT_MS` / `RPC_MAX_ATTEMPTS` / `RPC_MAX_RPS` | 15000 / 6 / 20 | Per attempt timeout, attempts per call, client-side rate limit |
| `ETH_FOLLOWER_MAX_RANGE` | 500 | Blocks per unfiltered follower `getLogs` (about 450 KB of JSON per block; must be at most 1000) |
| `FOLLOWER_POLL_MS` | 30000 | Finalized-head poll interval (finalized advances about every 6.4 min) |
| `FOLLOWER_MAX_LAG_BLOCKS` | 96 | Discovery returns 503 when the follower is further behind finalized (about 3 epochs) |
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

## API

| Endpoint | Returns |
|---|---|
| `GET /v1/ethereum/accounts/{address}/erc20?balances=true` | `{ status: "ready", blockHeight, blockHash, finality, lastProgressAt, blockTimestamp, dataAgeS, stale, lagBlocks, followerLagBlocks, fromBlock, balancesAtBlockHeight?, tokens: [{ contract, firstSeenBlock, lastSeenBlock, sentCount, receivedCount, balance?, balanceStatus?, balanceBlock? }], scan }` |
| `GET /v1/ethereum/accounts/{address}/erc721?verifyOwnership=true` | `{ status: "ready", blockHeight, blockHash, …, followerLagBlocks, collections: [{ contract, tokens: [{ tokenId, heldBySeenLogs, owner?, ownedByAccount? }] }] }` |
| either, while the account's backfill runs | **HTTP 202** `{ status: "syncing", progressBlocks, targetBlocks, followerLagBlocks }` with `Retry-After: 2` |
| `GET /health` | uptime, follower status (cursor, head, lag, last advance, error), balance worker (active accounts, last refresh), RPC call counts per method, error classes, circuit-breaker state (API key redacted) |

Chain quantities are decimal strings; `dataAgeS` is an integer. `blockHeight`/`blockHash` are the follower's cursor: discovery data is complete through that finalized block, and the pair is read in the same repeatable-read transaction as the holdings. `lastProgressAt` is when the cursor last advanced. `lagBlocks` is the latest head minus `blockHeight`; `followerLagBlocks` is the finalized head minus `blockHeight` (normally 0).

Each balance carries `balanceBlock`, the block it was read at (by hash, EIP-1898). A snapshot read at block N is never reported as valid at a later block: after the cursor advances, the old snapshot is served with its own `balanceBlock` until the next refresh, and `balancesAtBlockHeight` says whether every balance in the response was read at `blockHeight`.

Errors: 400 (address/flags), 404 (network), 503 with `Retry-After` when the follower is unhealthy (too far behind or not advancing), the RPC is down, or request-path RPC is saturated or past `REQUEST_DEADLINE_MS`.

## How it works

`RpcClient` is the only component that performs HTTP. It owns error classification, retries, per-endpoint circuit breakers and unsupported-method memory, result validation with failover, rate limiting, and per-attempt statistics. A viem `PublicClient` sits on top through a custom transport with viem retries and call batching disabled. viem supplies RPC/action types, standard ABIs, calldata and topic encoding, strict event/result decoding, and the block/call actions. Inside `RpcClient`, one viem `http()` transport per endpoint (retries and batching off) does the HTTP/JSON-RPC mechanics; viem's retry, `shouldRetry` and `fallback` never run, so every attempt is billed and recorded once.

Data is **finalized-only**: nothing above Ethereum's finalized tag is stored, so there is no reorg tail and no rollback.

1. **Shared finalized follower (one per network).** A background loop polls the finalized head every `FOLLOWER_POLL_MS`. When it is ahead of the global cursor (`follower_cursor`: number, hash, timestamp, updated_at), the follower fetches every `Transfer` log of (cursor, head] **without an address filter**, in ranges of at most `ETH_FOLLOWER_MAX_RANGE` blocks, with the same strict decoding and split-on-too-large/timeout as the backfill. Logs whose from/to are not tracked are dropped before decoding; the rest fold into the same per-account aggregates the backfill uses. Each range commits in one transaction together with the cursor advance (a range that ends below the head costs one extra `eth_getBlockByNumber` for its hash). At the head, one epoch (32 blocks, about 22k logs, about 14 MB) is one `getLogs` call.
   - **Exactly one follower per network.** The loop runs only while it holds a PostgreSQL **session** advisory lock (`pg_try_advisory_lock`) on a dedicated pool connection, held for the follower's lifetime and re-verified every tick. Other processes retry acquisition each poll and meanwhile only read the cursor. Independently, each commit locks the cursor row and refuses a range that does not continue it, so a lost lock can never double-apply a range.
   - **Tracked accounts** are kept in an in-memory set loaded from `tracked_accounts`. At commit, the store compares the set the range was decoded against with the table; if an account was tracked in the meantime, nothing is applied and the range is refetched with it included.
   - **Health.** The follower reports its lag (finalized head minus cursor) and when the cursor last advanced. Responses are `stale` after an RPC failure on the last poll, a lag above one epoch, or no advance for `FINALIZED_MAX_NO_PROGRESS_MS`; discovery returns 503 above `FOLLOWER_MAX_LAG_BLOCKS` or after `MAX_HEAD_AGE_MS` without an advance.
2. **Backfill → follower handoff.** A new account is tracked in one transaction that takes a share lock on the cursor row, inserts the `tracked_accounts` row, and records the cursor C as its `backfill_target`. A follower commit therefore happens entirely before (its blocks are ≤ C) or entirely after (it applies to the account, blocks > C). The backfill then scans [startBlock, C] with the resumable address-filtered scan; the store rejects backfill chunks above C. Counts add, first/last block widen, and the newest ERC-721 transfer wins by (block, logIndex), so the two writers commute and nothing is missed or counted twice across C.
   - The backfill is `eth_getLogs` on `Transfer` with viem-encoded account topics, both directions per chunk in parallel. Up to `ETH_SCAN_CONCURRENCY` chunks are in flight; completed chunks commit strictly in order, each with its aggregate and `progress_to` in one transaction. After a failure the earlier chunks still commit and the next request resumes at the first incomplete chunk. Each decoded log is folded straight into the chunk aggregate (only self-transfers are deduped, because both directions return them). The chunk range adapts per account (halves after a split, a > 50 MB response, or a slow > 5 MB one; doubles after three light chunks) and is persisted, so an exchange wallet's next scan starts small.
   - A request waits up to `REQUEST_DEADLINE_MS` for the backfill and otherwise answers **202 `syncing`** with `progressBlocks`/`targetBlocks`; the backfill continues in the background. Concurrent requests share one backfill. Backfills run through a bounded limiter (`BACKFILL_CONCURRENCY`, `BACKFILL_QUEUE`); beyond it requests get 503 with `Retry-After`.
   - Scan progress stored by the pre-follower version is resumed when it is at or below C, and discarded (the account is rebuilt) when it is above C.
3. **Shared balance worker.** Accounts that requested balances within `ACTIVE_ACCOUNT_TTL_MS` are active. Every `BALANCE_REFRESH_MS` and whenever the follower advances, the worker reads `balanceOf` for every active (account, token) pair at **one block, the follower cursor, pinned by hash** (EIP-1898), packed across accounts into Multicall3 `aggregate3` calls of `ETH_MULTICALL_BATCH`. Results (balance or error) are stored in `balance_snapshots` with the block number and hash they were read at; a snapshot never moves to an older block. "No Transfer, so unchanged" is never assumed (WETH deposits, rebasing tokens). Batches split only on size/gas/revert failures; authentication errors, rate limits and outages propagate.
4. **Request path.** With the account backfilled, a request is pure database reads: cursor, holdings and NFT items in one repeatable-read transaction, plus the account's balance snapshots. No `getLogs`, no head fetch, no `eth_call`. Only when a token has no snapshot yet (first `balances=true` request, or a token first seen since the last refresh) is the account read immediately at the cursor and stored; concurrent requests share that read. That read, `verifyOwnership` and cold backfills are the only request-path RPC; they run through bounded limiters with the per-request deadline and return 503 with `Retry-After` instead of queueing without bound.
5. **Classification.** A `Transfer` log with 3 topics is decoded strictly with the ERC-20 event ABI. With 4 topics it is decoded with the ERC-721 variant, where `tokenId` is indexed. Malformed logs are skipped.
6. **Storage.** PostgreSQL 18: one row per `(account, token)` with first/last block and sent/received counts, ERC-721 token IDs with the latest seen holder side, the follower cursor, tracked accounts with their backfill target, backfill progress, and balance snapshots. Individual transfers are never stored. Drizzle migrations run before the server starts.

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
src/store/schema.ts       Drizzle PostgreSQL schema
src/store/store.ts        async Store queries and transactional upserts, shared by pg and PGlite
src/store/postgres.ts     pg Pool startup and automatic Drizzle migrations
src/store/advisory-lock.ts advisory locks: transaction-scoped helper, and the session-level follower leader lock
drizzle/                  generated, commit-ready SQL migrations and metadata
src/follower/follower.ts  shared finalized follower: leader lock, unfiltered getLogs per range, health
src/balances/worker.ts    shared balance worker for active accounts; first-request reads (single-flight)
src/discovery/service.ts  tracking, backfill to the handoff target (resumable, single-flight), database reads
src/core/limiter.ts       bounded concurrency and per-request deadlines for request-path RPC
src/api/routes.ts         Hono routes
```

A new EVM chain is a config entry plus `createEvmAdapter`. A non-EVM chain implements `ChainAdapter`.

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
| Node behind the cached head | Keep the newer cached finalized block; the follower does not move its cursor backwards |
| Follower poll fails (head or `getLogs`) | The cursor stays put and the range is retried next poll; responses are `stale: true` until a poll succeeds |
| Follower falls behind finalized | `followerLagBlocks` grows; `stale` above one epoch, 503 with `Retry-After` above `FOLLOWER_MAX_LAG_BLOCKS` |
| Finalized (or the follower) stops advancing | `stale` after `FINALIZED_MAX_NO_PROGRESS_MS`, 503 after `MAX_HEAD_AGE_MS`; clears on the next advance |
| Request-path RPC saturated or slow | Bounded limiters and `REQUEST_DEADLINE_MS`: 202 `syncing` for backfills, 503 with `Retry-After` otherwise |

## Limits (PoC)

- **Pre-start tokens.** Tokens held before block 25,933,634 and never moved since are not discovered.
- **Balances without `Transfer`.** Tokens that change balances without emitting `Transfer` are not discovered or tracked. Examples: WETH deposits, rebasing tokens. ERC-1155 is not supported.
- **Spam.** Spam and address-poisoning tokens are returned. Many of them fail `balanceOf` and show `balanceStatus: "error"`.
- **Polling cost.** Warm requests cost no RPC. Steady-state cost is the follower (1 `getLogs` per finalized epoch plus 2 head calls per poll) and the balance worker (one `eth_call` per 1,000 active pairs per refresh): for 5k active users with about 10 tokens each, about 50 calls per minute, about 51M Quicknode credits per month (REPORT.md section 4). New accounts cost a cold backfill (about 172 calls for the full window) and one first balance read.
- **Single process.** The active-account set, single-flight maps and limiters are in process memory, and the balance worker runs in every process. Only the follower is protected against concurrent processes (advisory lock). Several API replicas would need the active set in the database (or Redis) and one balance-worker leader.
- **Freshness of balances.** Balances are at most `BALANCE_REFRESH_MS` (plus one refresh duration) older than the cursor, and every token says which block it was read at. Accounts idle for `ACTIVE_ACCOUNT_TTL_MS` are no longer refreshed; their next request is served from the old snapshot (labelled with its block) and re-activates them.
- **Tracked accounts are never untracked.** The follower applies logs for every account ever requested; the per-tick cost is independent of their number, but the set and the commit-time check grow with it.
- **Finality latency.** Durable discovery follows Ethereum's finalized tag, so responses trail the chain tip by roughly 64 blocks (about 13 minutes) plus up to one `FOLLOWER_POLL_MS`. There is no realtime tail (decided: finalized only).
- **Heavy accounts.** An exchange hot wallet can mean about 2.5M logs (about 1.6 GB of JSON) for the window. The scan is then bound by download throughput (about 75 s measured for Binance's hot wallet), and the adaptive range trades RPC calls for smaller responses: about 320-450 calls instead of 172. Every response is still parsed whole by viem, so peak RSS is about 1 GB at concurrency 8, mostly in-flight responses plus GC slack. Each cold scan commits per chunk and resumes after the last committed chunk following a failure.
- **Concurrency and rate limit.** At the default `RPC_MAX_RPS` of 20, concurrency 8 (16 requests in flight) already saturates the limiter for a normal account: 172 calls take about 8.6 s, and concurrency 16 measured no faster. The limiter is shared with the follower and the balance worker; a live cold backfill on 2026-10-14 took about 26 s for the same 172 calls (so with the default 10 s deadline, the first requests answer 202).
- **Reorgs.** Durable summaries contain finalized blocks only, making them reorg-safe by construction under Ethereum's finality guarantees. The service does not implement rollback for a consensus-level finality failure.
- **PostgreSQL required at runtime.** The API does not fall back to embedded storage. Tests use PGlite and exercise the same generated migrations without Docker.
