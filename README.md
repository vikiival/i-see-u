# i-see-u: token discovery over plain JSON-RPC (PoC)

For an Ethereum account, returns every ERC-20 contract it sent or received (and, as a bonus, ERC-721 collections and token IDs) from block 25,933,634 to Ethereum's finalized head, optionally with balances pinned to the same block. It needs nothing but a JSON-RPC endpoint: no Blockbook, no indexer service.

Design notes and measurements: [REPORT.md](REPORT.md).

## Run

```sh
npm install
cp .env.example .env                # replace RPC_URL with your endpoint
docker compose up -d postgres       # PostgreSQL 18 on 127.0.0.1:5432
npm start                            # migrates the database, then serves http://localhost:3000
npm test                             # PGlite tests; no Docker or network needed
npm run test:watch                   # watch mode
```

The project-root `.env` file is loaded with Node's `process.loadEnvFile()`; existing environment variables take precedence. The development `DATABASE_URL` default matches `docker-compose.yml`, so copying `.env.example` is enough. The database is a derived cache, but migrations preserve it across schema-compatible releases.

After changing `src/store/schema.ts`, run `npm run db:generate` and commit the generated SQL and metadata in `drizzle/`. `npm run db:migrate` applies pending migrations without starting the API; `npm start` does the same automatically.

Optional env: `DATABASE_URL` (default `postgresql://iseeu:iseeu@localhost:5432/iseeu`), `RPC_FALLBACK_URLS` (comma-separated), `PORT`, `FINALIZED_MAX_NO_PROGRESS_MS` (finalized head is marked stale after this long without a higher block, default 1200000), `MAX_HEAD_AGE_MS` (maximum finalized no-progress age accepted by discovery, default 1800000), `ETH_LOGS_MAX_RANGE` (default 3000), `ETH_LOGS_MIN_RANGE` (floor of the adaptive range, default 500), `ETH_SCAN_CONCURRENCY` (chunks in flight per scan, default 8), `RPC_TIMEOUT_MS` (per attempt, default 15000), `RPC_MAX_ATTEMPTS` (default 6), `ETH_MULTICALL_BATCH` (default 1000), `RPC_MAX_RPS` (default 20). Numeric values must be positive integers; invalid values fail at startup.

## API

| Endpoint | Returns |
|---|---|
| `GET /v1/ethereum/accounts/{address}/erc20?balances=true` | `{ blockHeight, blockHash, finality, lastProgressAt, blockTimestamp, dataAgeS, stale, lagBlocks, fromBlock, tokens: [{ contract, firstSeenBlock, lastSeenBlock, sentCount, receivedCount, balance?, balanceStatus? }], scan }` |
| `GET /v1/ethereum/accounts/{address}/erc721?verifyOwnership=true` | `{ blockHeight, blockHash, finality, lastProgressAt, blockTimestamp, dataAgeS, stale, lagBlocks, collections: [{ contract, tokens: [{ tokenId, heldBySeenLogs, owner?, ownedByAccount? }] }] }` |
| `GET /health` | uptime, RPC call counts per method, error classes, circuit-breaker state (API key redacted) |

Chain quantities are decimal strings; `dataAgeS` is an integer. `blockHeight` and `blockHash` identify the block the whole response is valid for: discovery is scanned through it, and balances use an EIP-1898 `eth_call` pinned to that exact hash. `lastProgressAt` is when the finalized block number last increased; `blockTimestamp` is the block header time, while `dataAgeS` is its age at response time.

## How it works

`RpcClient` is the only component that performs HTTP. It owns error classification, retries, per-endpoint circuit breakers and unsupported-method memory, result validation with failover, rate limiting, and per-attempt statistics. A viem `PublicClient` sits on top through a custom transport with viem retries and call batching disabled. viem supplies RPC/action types, standard ABIs, calldata and topic encoding, strict event/result decoding, and the block/call actions. Inside `RpcClient`, one viem `http()` transport per endpoint (retries and batching off) does the HTTP/JSON-RPC mechanics; viem's retry, `shouldRetry` and `fallback` never run, so every attempt is billed and recorded once.

1. **Finalized head.** viem's `getBlock({ blockTag: 'finalized' })` action, cached for 4 s and shared by all requests. Because the action cannot carry our `validate` callback, its custom transport injects validation into `RpcClient` before viem formats the block; null or invalid results therefore fail over. Repeated successful answers for the same block become stale after `FINALIZED_MAX_NO_PROGRESS_MS`, and fetch failures and lagging lower blocks are served stale from the newer cached block; only a higher block resets the no-progress clock. Discovery returns 503 once finalized progress is older than `MAX_HEAD_AGE_MS`. Each refresh also fetches `latest` in the background, only to report `lagBlocks`.
2. **Discovery, per account, on demand.** `eth_getLogs` on `Transfer` with viem-encoded account topics, in block-range chunks; both directions of a chunk run in parallel. Up to `ETH_SCAN_CONCURRENCY` chunks are in flight, and a new one starts as soon as any of them finishes. Completed chunks wait in a buffer until every earlier chunk is done (completed plus in flight is at most twice the concurrency, so memory stays bounded), then commit strictly in order, each with its summaries and `progress_to` in one transaction. A failed direction commits none of that chunk; after a failure nothing new starts, the chunks before it still commit, and a later request resumes at the first incomplete chunk. Once every chunk reaches the target finalized block, the public `scanned_to`/hash cursor is advanced as one consistent pair. Concurrent requests for the same account share one scan.
   - **Aggregated while decoding.** Each decoded log of either direction is folded straight into a per-chunk aggregate: per token the standard, first/last block and sent/received counts; per ERC-721 `(token, tokenId)` the latest `(block, logIndex, held)`. No per-transfer arrays are built, sorted, or concatenated. Only a self-transfer (`from == to == account`) is returned by both queries, so only those are deduped by `(txHash, logIndex)`.
   - **Adaptive range per account.** A scan starts at the account's persisted range, else `ETH_LOGS_MAX_RANGE`. A chunk is heavy if it had to be split, a response exceeded 50 MB, or it was slower than 3 s while larger than 5 MB (a slow small response is provider latency, which a smaller range would only multiply). A heavy chunk sets the range to half its own size (floor `ETH_LOGS_MIN_RANGE`), so a window of heavy results shrinks once, not once per result. Three consecutive light chunks at the current range double it again, up to the maximum. The last range is stored per account, so the next scan of an exchange wallet starts small. Response size is the uncompressed `Content-Length` from `RpcClient`'s per-attempt hook; Quicknode compresses large responses without one, so it is then estimated at 640 bytes per log, without re-serializing.
3. **Classification.** A `Transfer` log with 3 topics is decoded strictly with the ERC-20 event ABI. With 4 topics it is decoded with the ERC-721 variant, where `tokenId` is indexed. Malformed logs are skipped.
4. **Storage.** PostgreSQL 18 stores one row per `(account, token)` with first and last block and sent/received counts, plus ERC-721 token IDs. Individual transfers are never stored. Resumable `progress_to` is separate from the published finalized cursor, whose height, hash, and timestamp stay consistent; both only move forward. Every chunk is one transaction, and Drizzle migrations run before the server starts.
5. **Balances.** Multicall3 `aggregate3` with `allowFailure`, using viem's exported ABIs for encoding and decoding. The outer `eth_call` uses the EIP-1898 `{ blockHash }` parameter from the finalized cursor, so a provider cannot resolve the read against a different block at the same height. Batches contain up to 1,000 inner calls and split only size/gas/revert failures; authentication, malformed responses, rate limits, outages, and unsupported calls propagate immediately.

`lagBlocks` uses the highest `latest` block number cached by head tracking. Reading it from discovery never triggers another RPC; when none is available, lag is computed against the finalized observation (normally zero).

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
src/store/advisory-lock.ts transaction-scoped PostgreSQL advisory-lock helper for the future follower
drizzle/                  generated, commit-ready SQL migrations and metadata
src/discovery/service.ts  cursor-based incremental scan, single-flight
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
| Node behind the cached head | Keep the newer cached finalized block, mark the observation stale, and negative-cache the failure for 4 s |
| Head repeatedly returns the same block | Mark stale after `FINALIZED_MAX_NO_PROGRESS_MS` without a higher block |
| Prolonged finalized no-progress period | Serve a recent cached observation as `stale: true`; return 503 once progress is older than `MAX_HEAD_AGE_MS` |

## Limits (PoC)

- **Pre-start tokens.** Tokens held before block 25,933,634 and never moved since are not discovered.
- **Balances without `Transfer`.** Tokens that change balances without emitting `Transfer` are not discovered or tracked. Examples: WETH deposits, rebasing tokens. ERC-1155 is not supported.
- **Spam.** Spam and address-poisoning tokens are returned. Many of them fail `balanceOf` and show `balanceStatus: "error"`.
- **Polling cost.** Each request that finds a newer head costs about 2 `getLogs` calls, plus 1 `eth_call` when balances are requested. For 5k users polling every minute that is roughly 15k calls per minute. The next step is a shared follower, which reads each block's `Transfer` logs once and invalidates a cached balance only when that pair was touched. That makes cost depend on block rate instead of users. See REPORT.md.
- **Finality latency.** Durable discovery follows Ethereum's finalized tag, so responses trail the chain tip by roughly 64 blocks, about 13 minutes. A later realtime tail can overlay safe/unfinalized data without weakening the finalized cursor.
- **Heavy accounts.** An exchange hot wallet can mean about 2.5M logs (about 1.6 GB of JSON) for the window. The scan is then bound by download throughput (about 75 s measured for Binance's hot wallet), and the adaptive range trades RPC calls for smaller responses: about 320-450 calls instead of 172. Every response is still parsed whole by viem, so peak RSS is about 1 GB at concurrency 8, mostly in-flight responses plus GC slack. Each cold scan commits per chunk and resumes after the last committed chunk following a failure.
- **Concurrency and rate limit.** At the default `RPC_MAX_RPS` of 20, concurrency 8 (16 requests in flight) already saturates the limiter for a normal account: 172 calls take about 8.6 s, and concurrency 16 measured no faster.
- **Reorgs.** Durable summaries contain finalized blocks only, making them reorg-safe by construction under Ethereum's finality guarantees. The service does not implement rollback for a consensus-level finality failure.
- **No balance cache yet.** Balances are read on every request. Request coalescing is in-process, so multiple replicas would need Redis.
- **PostgreSQL required at runtime.** The API does not fall back to embedded storage. Tests use PGlite and exercise the same generated migrations without Docker.
