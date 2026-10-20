# Benchmark: Subsquid reimplementation (`../disquid`) against i-see-u

The Subsquid reimplementation in `../disquid` was run live against Ethereum mainnet over JSON-RPC only, from block **26,104,555** to the head, and kept following it for 34.6 hours. Everything below is **measured**, unless it is marked *derived* (computed from code or from other measurements). The design is in `../disquid/DESIGN.md`; how it came about is in [PROGRESS.md §18](PROGRESS.md).

> **Update after these runs.** i-see-u no longer answers 503 `not_ready` because of index state (commits `2268cbd`, `5cccd13`). The finalized-epoch window and any other lag now return 200 with `stale: true` and a `staleReason`, holding exact data at the follower cursor. A running bootstrap returns 200 `status: "partial"` with the covered ranges. 503 remains only for database, RPC, queue or deadline failures. The 503 figures below were measured before that change. The load tests were not re-run.

## Setup

| | |
|---|---|
| Machine | Apple M3 Pro (11 cores, 36 GB), macOS. PostgreSQL in local Docker (disquid: postgres 15 container; i-see-u: postgres 18) |
| RPC | One Quicknode Ethereum mainnet endpoint. Credits assume ~20 per call, as in the rest of this repo |
| Stack | `@subsquid/squid-sdk` 0.6.2 (`EvmRpcDataSourceBuilder`, the latest release), `batch-processor` 1.1.0, `typeorm-store` 1.9.2 with hot blocks, `graphql-server` 4.12.1, Node 26.10 |
| Indexed | ERC-20 and ERC-721 `Transfer` logs, folded into one row per (account, token, standard), plus ERC-721 current owner per item, token metadata and recent block headers. Zero-value ERC-20 Transfers are skipped |
| RPC layer | `DiscoveryRpcSourceBuilder`, a subclass of `EvmRpcDataSourceBuilder` with the same API. It adds header-only blocks, topic0 pushed into `eth_getLogs` with range splitting, a log-vs-header block-hash check, retries with timeouts, slot-aligned head polling and a reorg fix. Without these changes ("stock") the SDK fetches full blocks and unfiltered logs, polls the head every 100 ms, exits on the first 429, and crashes on the first head reorg |

## Fair comparison: same range, same data (2026-10-18/19)

Both systems index the same blocks from the same start block, 26,104,555. Every figure in this section is measured, unless it is marked *derived*. This section supersedes the earlier squid-against-i-see-u numbers further down, which came from different ranges and databases.

**Method.**
- **i-see-u:** loaded into a fresh database (`iseeu_bench`) with its current bootstrap path, pinned at finalized B = **26,221,956**. That is 117,402 blocks: HyperSync for the headers and part of the logs, Quicknode workers with 100-block topic0 `eth_getLogs`, and raw Transfers plus aggregates committed per 1,000-block segment.
- **Squid:** its index already started at 26,104,555.
- **Same height:** both were brought to the head (i-see-u's follower and tail, the squid's processor) and tested at about **26,225,700**.
- **RPC counting:** every Quicknode call went through a counting proxy per process, with a separate one for the squid's balance reads.
- **Same data:** both databases give the same ERC-20 token counts for the test accounts (average 38.7, max 13,386 against 13,385). The one difference is that the squid skips zero-value ERC-20 Transfers, which leaves it about 7% fewer pairs on this range.
- **Load method:** the same closed-loop generators as §5, on one M3 Pro, with each system's processor following the head during its tests. i-see-u ran first, then the squid.
- **Interruptions:** the bootstrap was interrupted twice and resumed both times (it is resumable).
  - The first stop was a full disk, fixed by dropping the old 60 GB database.
  - The second was the Mac sleeping for about 72 min; HyperSync hit DNS errors and 4 segments needed a retry pass.
  - Times below are active time, with sleep gaps taken out of the logs.

### Catch-up

| | i-see-u bootstrap | Squid re-index (§1) |
|---|---|---|
| Blocks | 117,402 (26,104,555–26,221,956) | 103,617 (26,104,555–26,208,172) |
| Active time | **about 109 min** (three passes: 16 + 87 + 6 min) | **2 h 59 min** |
| Speed | **about 18 blocks/s**: 34 on cold indexes, falling as `transfers` and `holdings` grew | **9.7 blocks/s** |
| Quicknode calls | **982**, 0.008 per block (headers from HyperSync, logs in 100-block ranges) | **106,084**, 1.02 per block (RPC only) |
| Rows written | 65.66M raw Transfers plus 30.1M `holdings` | 28.1M `account_token` (aggregates only) |
| Afterwards, to the head | 3,667 blocks in about 19 min by the follower (about 3.2 blocks/s). At the time the API answered 503 until this finished; it now serves stale data at the cursor | 6.4k blocks in about 20 min |

### Squid catch-up without per-block headers (2026-10-19, after the fair run)

The squid's 1.02 calls per block were almost all `eth_getBlockByNumber`, one per block. Since disquid `95851d9`, blocks below the finalized head are built from the filtered `eth_getLogs` alone: each log carries `blockNumber`, `blockHash` and `blockTimestamp`.
- **When it still reads a header:** only for a block with no matching log (checked against the bloom), for logs without a timestamp, or for logs that disagree on the hash.
- **Near the head:** it hands over to the normal stream, after checking the first block's real parent hash. A hot head replaced during downtime still shows up as a fork.
- **Not changed:** the write path and the database.

| | Before (header + logs) | After (logs only) |
|---|---|---|
| Same 2,000 blocks from 26,104,555, fetch only | 2,041 calls (2,000 headers, 40 `eth_getLogs`, 1 `eth_chainId`) in 241 HTTP requests, 63 s | **20 calls** (20 `eth_getLogs` of 100 blocks) in 20 HTTP requests, **17 s** |
| Same data? | — | **Yes:** 838,808 logs; 0 mismatches in block hash, timestamp or logs |
| Live catch-up, 3,201 blocks (26,225,769–26,228,969), through the counting proxy | 1.02 per block, so about 3,270 (*derived*) | **83 calls, about 0.026 per block**: 33 `eth_getLogs` plus 50 head and finalized polls (time-driven, about 4 per minute). 0 errors, then a clean hand-off to the head |
| Full re-index, 103,617 blocks (*derived*) | 106,084 (measured, §1) | **about 1.7k**: about 1,040 `eth_getLogs` plus about 4 polls per minute for the run. About 60× fewer, against i-see-u's 982 with HyperSync |

- **Not counted above:** token metadata `eth_call`s (73 in the live run), which are unchanged.
- **Speed is unchanged:** the live run did about 4 blocks/s on a just-started database container, so writes are the limit. Fetching alone runs at about 118 blocks/s, sequentially.

### API without balances

The accounts are the 2,844 test accounts with 100 or fewer ERC-20 tokens. Both systems returned the same tokens, **13.0 per response** on average. i-see-u has no page limit and the squid caps a page at 100, so the 116 heavier accounts were left out.

| Connections | i-see-u REST: req/s | p50 | p99 | Squid GraphQL: req/s | p50 | p99 |
|---|---|---|---|---|---|---|
| 1 | 620 | 1.5 ms | 2.7 ms | 646 | 1.2 ms | 6.1 ms |
| 10 | 2,365 | 4.2 ms | 6.0 ms | **3,816** | 2.5 ms | 4.5 ms |
| 50 | 2,299 (**28% 503**: a finalized-epoch window) | 20.5 ms | 45.6 ms | **4,091** | 12.1 ms | 15.8 ms |
| 100 | 2,267 | 43.3 ms | 54.9 ms | **3,795** | 25.2 ms | 35.8 ms |

On all 3,000 accounts, where heavy accounts make i-see-u responses about 2× larger (36.8 against 16.6 tokens), the gap was the same: 2,107 against 3,499 req/s at 10 connections, both with 0 errors.

### API with balances, cold caches

All 3,000 accounts, 5 connections for 20 s. Both caches were empty: the squid API had just started, and the i-see-u database was new, so it held no balance snapshots.

| | i-see-u | Squid |
|---|---|---|
| Req/s | 52 | **72** |
| p50 / p95 / p99 | 89 / 176 / 545 ms | **69 / 100 / 128 ms** |
| Tokens per response | 35.4 (all) | 16.4 (first page of up to 100) |
| `eth_call`s | **198** for 1,036 requests, about 185 balance reads per call | 276 for 1,446 requests, about 86 balance reads per call |
| Errors | 0 | 0 |

Both need about 0.19 `eth_call` per request. i-see-u packs about 2× more balance reads into each Multicall, because its batches are larger (1,000 pairs against 500) and its window longer (25 ms against 20 ms).

### Storage, same range, at about 26,225,700

| | i-see-u | Squid |
|---|---|---|
| Whole database | **37 GB** | **16 GB** |
| Per-pair aggregate | `holdings`: 30.12M rows, 9.0 GB, **309 B/row** | `account_token`: 28.10M rows, 16 GB, **593 B/row** |
| Raw Transfers | `transfers`: 65.58M rows, 28 GB, 453 B/row (16 GB of it indexes: the from/to history indexes and the primary key) | none |
| NFT items | `nft_ownership`: 611k rows, 169 MB | `nft_item`: 630k rows, 259 MB |

### Head RPC (from §2; different windows)

| i-see-u | Squid |
|---|---|
| 29.5k calls/day, 4.1 per block (45 min) | **18.2k calls/day**, about 2.5 per block (34.6 h) |

### Verdict

| Dimension | Winner | Why |
|---|---|---|
| Catch-up speed | **i-see-u**, about 1.9× | Bulk segment writes. The squid reads, merges and upserts per batch |
| Catch-up RPC cost | **i-see-u**, about 2× fewer calls per block since the squid's header-less catch-up (0.008 against about 0.016, *derived*); about 120× before it | i-see-u takes its headers from HyperSync. Below the finalized head the squid now reads none; it makes about one `eth_getLogs` per 100 blocks, plus head polls |
| API throughput and latency | **Squid**, 1.6–1.8× the req/s and about 40% lower p50 | One indexed lookup in one GraphQL process. i-see-u also overlays the tail. At the time it also returned 503 for 13–18 s every 6.4 min; that window now serves stale 200s |
| Balance reads | Mixed | Squid has lower latency. i-see-u makes about 2× fewer `eth_call`s per balance read |
| Storage | **Squid** in total (2.3× smaller); **i-see-u** per aggregate row (about 2× leaner) | i-see-u keeps raw history (history endpoint, repair). Squid entities need string ids |
| Head RPC cost | **Squid**, about 1.6× cheaper | i-see-u's finalized follower re-fetches headers the tail already has |

## Headline numbers

| Metric | Result |
|---|---|
| Catch-up, 103,617 blocks | **2 h 59 min**, 9.7 blocks/s on average (30 at the start, 17–18 on busy blocks) |
| RPC calls for catch-up | **106,084** (1.02 per block) in 13,263 HTTP requests, about **2.1M credits**. Since disquid `95851d9` (header-less catch-up): **about 0.026 per block**, measured live on 3,201 blocks. That is about 1.7k calls (about 34k credits) for the same range, *derived* |
| i-see-u bootstrap, same range (fair run) | 117,402 blocks in about **109 min** of active time, **about 18 blocks/s**, **982 Quicknode calls** (0.008 per block, against the squid's 1.02) |
| RPC calls at the head | **18.2k/day** (p50 12/min, p95 17/min), about **11M credits/month** |
| i-see-u, RPC calls at the head (global-index tail plus finalized follower, 45 min on 2026-10-18) | **29.5k/day** (p50 15/min, p95 50/min), about **17.7M credits/month**, 4.1 calls per block, 0 errors |
| Head lag | Indexed block about 9 s behind wall-clock time |
| Live reorgs | **8 in 34.6 h**, all 1 block deep, all rolled back and resumed. The stock SDK crashes on the first one |
| Parity with i-see-u | **0 mismatches**: ERC-20 386,213 pairs, ERC-721 5,627 pairs, 12,381 NFT owners |
| API without balances, one process, same data (fair run) | **3,816 req/s** at 10 connections (p50 2.5 ms, p99 4.5 ms), up to 4,091 at 50, 0 errors |
| API with balances, cold cache (fair run) | 72 req/s at 5 connections, p50 69 ms, 276 `eth_call`s for 1,446 requests, 0 errors |
| i-see-u API without balances, same data (fair run) | **2,365 req/s** at 10 connections (p50 4.2 ms, p99 6.0 ms), about 2,300 at 50–100. A **503 `not_ready` window of 13–18 s** at every finalized epoch (6.4 min); one 50-connection run had 28% 503s. That was before the always-serve change; the window now serves stale 200s |
| i-see-u API with balances, cold cache (fair run) | 52 req/s at 5 connections, p50 89 ms, **198 `eth_call`s** for 1,036 requests (about 2× more balance reads per call than the squid) |
| Storage, same range (fair run, about 121k blocks) | Squid **16 GB** (`account_token` 28.10M rows, 593 B/row). i-see-u **37 GB** (`holdings` 30.12M rows at 309 B/row, plus 65.58M raw Transfers, 28 GB) |

## 1. Ingestion (catch-up)

> **Earlier, unequal data.** The i-see-u figures in this section come from earlier runs on different ranges or databases. For squid against i-see-u, use the [fair comparison](#fair-comparison-same-range-same-data-2026-101819) above.


| Run | Source | Speed | What limited it |
|---|---|---|---|
| First run, ~5k blocks | Stock `EvmRpcDataSourceBuilder` (full blocks, unfiltered `eth_getLogs`, stride 10, rate limit 40/s) | 12 → 8 blocks/s | Database writes. Handler throughput (mapping rate) was no higher than the overall rate |
| Restart on the same data | `DiscoveryRpcSourceBuilder` (stride 50, 50 headers per batch, 4 strides in flight) | 14–15 blocks/s, mapping 20+ | Mostly the RPC and the database together |
| Full re-index (ERC-20 + ERC-721 + metadata + zero-value skip) | `DiscoveryRpcSourceBuilder` | 9.7 blocks/s average over 2 h 59 min | Database writes on busy blocks; the `account_token` table grew to 15 GB |

**Equivalence check (live, 100 blocks from 26,104,555).** The optimized source made 101 `eth_getBlockByNumber`, 5 `eth_getLogs` and 2 `eth_chainId` calls in 13 HTTP requests and 1.4 s. It returned **40,788 logs**, exactly what a direct topic-filtered `eth_getLogs` returned for the same range.

**Catch-up cost per block.** It is one header call per block plus one `eth_getLogs` per 50 blocks, so 1.02 calls/block. The stock SDK, *derived*, makes one full-block call per block plus one unfiltered `eth_getLogs` per 5–20 blocks, and moves 50–100× more bytes.

**i-see-u's bootstrap, for comparison.** i-see-u loads the same kind of global index with `npm run bootstrap`. It takes 1,000-block segments; HyperSync takes segments from the low end, while Quicknode workers take them from the high end with 100-block topic0 `eth_getLogs` calls. Headers come from HyperSync in 5,000-block chunks.
- **RPC-only, *derived*:** about 1.01 calls/block, the same as here. Most of it is one header per block. The live follower catch-up below measured 1.04.
- **Follower catch-up, RPC only, measured live on 2026-10-18** (the finalized follower on the existing 60 GB index, not the bootstrap; `RPC_URL` pointed at a counting proxy, no HyperSync, no fallback endpoint):
  - **Range:** cursor 26,206,043 (about 1.9 days behind) to 26,219,852, so **13,809 blocks** in **104.6 min** wall clock (14:17:31 to 16:02:05 CEST), which is 2.2 blocks/s. That wall clock includes about **42 min with no progress**: the follower last advanced at 15:14 and resumed at 15:56. The host Mac idle-slept on battery from about 15:24 to 15:55, and the DB connection timeout came with it. Excluding that gap, it ran at about **62 min, so about 3.7 blocks/s**. The Transfers of 26,206,044-26,219,788 (13,745 blocks) number **9.33M**, so about 680 per block and about 1.5k Transfers/s.
  - **Calls:** for those 13,745 blocks the proxy counted **14,255 `eth_getBlockByNumber` + 33 `eth_getLogs` = 14,288 calls** (**1.04 per block**, about 0.29M credits, *derived* about 2.1M per 100k blocks). 500-block ranges. The proxy saw no non-2xx response and no JSON-RPC error; the service's own RPC stats show 2 transient errors (client-side, retried).
  - **What limited it:** not the RPC. Quicknode answered each 500-block range in seconds, then the follower spent about 2-5 min per range writing it. `pg_stat_activity` showed the `holdings` upsert waiting on `DataFileRead` and an autovacuum of `transfers` running; this was not profiled further.
  - **Side effects seen:** `/health` answered 503 after up to 306 s while the pool was busy, the follower logged one `Connection terminated due to connection timeout` and recovered on the next tick, and the tail logged one `Tail parent changed` as it started (it retried).
- **With HyperSync, measured on a 2,001-block smoke run:** 15 Quicknode calls in total (about 0.0075 per block, roughly 130× fewer credits), in **33 s**.
- **Full window, live run on 2026-10-16 (old staging path):** 25,933,634 → 26,204,062, so **270,429 blocks** in 271 segments.
  - **Load:** 138.5M Transfers in **about 43 min**, about 53k logs/s or about **105 blocks/s**. HyperSync quota pauses of up to 57 s were visible during the header phase.
  - **Finalize:** the old single-transaction finalize then took **about 60 min**. Its `holdings` `GROUP BY` over 277M rows spilled 15.6 GB to temporary files.
  - **End to end:** about **1 h 45 min**, ending in a 58 GB database. Quicknode calls for this run were not counted.
  - **Rejected alternative, *derived*:** RPC-only would have needed about 185k header calls, about 3.7M credits and about 2.6 h, against about 37 HyperSync header requests.
- **Fetch only, last 100k finalized blocks (HyperSync free tier, one paginated loop):** 53.78M Transfer logs in **41.5 min** with **660 requests**, about 21.6k logs/s and about 10 GB transferred.
  - **Logs per block:** average 538, p50 392, p99 2,563, max 21,069.
  - **Same 100-block range:** RPC and HyperSync returned the same count (33,487 logs).
- **Building blocks, measured:**
  - Quicknode unfiltered `eth_getLogs`: 100 blocks is 21–38 MB in 3.8–5.0 s; 500 blocks is up to 367 MB, close to Node's string limit.
  - Postgres `COPY`: 724k rows/s into an unindexed unlogged table, 126k rows/s with indexes.
- **Not yet run live:** the current indexed-segment path (a Codex rework that commits raw rows and aggregates per segment and publishes in minutes) has **not** been benchmarked end to end. At 126k rows/s, raw inserts alone would take about 18 min for 138.5M rows (*derived*).

**Squid against i-see-u on catch-up.** On the same range (fair comparison above), i-see-u's current bootstrap ran at about 18 blocks/s with 982 Quicknode calls, against the squid's 9.7 blocks/s and 106,084 calls. The speed gap comes from the write path: i-see-u bulk-writes per segment, while the squid reads, merges and upserts per batch. The cost gap came from the squid's one header call per block, which HyperSync spared i-see-u. i-see-u's **follower**, the path after the bootstrap, is slower: about 3.2–3.7 blocks/s, because it writes every raw Transfer plus the aggregate upserts. Since disquid `95851d9` the squid reads no headers below the finalized head ([details](#squid-catch-up-without-per-block-headers-2026-10-19-after-the-fair-run)). That brings it to about 1.7k calls for this range (*derived*), within about 2× of i-see-u's 982, and it stays RPC-only.

## 2. Following the head

The squid's window is 34.6 hours, from 2026-10-17 00:45 to 2026-10-18 11:19 CEST. i-see-u's window is **45 minutes (225 blocks)**, from 2026-10-18 16:05:48 to 16:50:51 CEST.

| | Calls/day | Credits/month (×20) |
|---|---|---|
| Measured (`DiscoveryRpcSourceBuilder`, head poll aligned to 12 s slots, finalized head cached for 60 s) | **18,249** | **about 11M** |
| Stock SDK, *derived* (polls `latest` every 100 ms, finalizer probes every 500 ms, full blocks) | about 1.07M | about 640M |
| **i-see-u, global-index tail plus finalized follower**, measured through a counting proxy (README, Near-realtime tail; `finality=latest` default, one Quicknode endpoint, no fallback). 45 one-minute samples, 225 blocks | **29,536** (22,144 `eth_getBlockByNumber` + 7,392 `eth_getLogs`), *derived* from 923 calls | **about 17.7M** (*derived*) |

- **Squid errors:** 4 failed calls (2 `eth_getBlockByNumber`, 2 `eth_getLogs`), all retried; none surfaced.
- **Squid latency:** at each check, the indexed head was within one block of the chain, about 9 s behind wall clock.

**i-see-u tail, 45-minute window (225 blocks, 2026-10-18 16:05:48-16:50:51 CEST).** This replaces the figure measured on the earlier per-account version.

| | Result |
|---|---|
| Calls | **923**: 692 `eth_getBlockByNumber`, 231 `eth_getLogs`, 0 other methods. 923 JSON-RPC calls in 923 HTTP requests (no batching) |
| Per block | **4.10 calls**: 3.08 headers + 1.03 `eth_getLogs` |
| Per minute (45 samples) | p50 **15**, p95 **50**, max 52, min 13, mean 20.5 |
| Per day, *derived* | **29.5k** (22.1k headers + 7.4k `eth_getLogs`); about 17.7M credits/month (×20 ×30) |
| Errors | **0**: no non-2xx response, no JSON-RPC error member |
| Reorgs, tail events | `/health` showed **0 reorgs**, tail healthy and tail error empty in all 46 one-minute `/health` samples; tail lag was 0 except 1 block in two samples. No tail failure in the window |
| Readiness | One of the 46 samples (16:21:49 CEST) answered `not_ready`, consistent with the epoch window below |

- **Shape of the cost.** Most minutes make about 10 headers and 5 `eth_getLogs` (about 2 header calls per block, plus the one `eth_getLogs`). The 2 header calls per block are *derived*: the README polls `latest` at slot +2.5/+5/+9 s, and the first poll often still sees the previous block. Every **6.4 min** (7 times in the window) one minute shows about +33 headers and +1 `eth_getLogs`: the finalized follower fetches the 32 new finalized blocks as headers again, though the tail has them. That is about **25%** of the calls (235 of 923).
- **Epoch window.** At each finalized epoch the observed finalized head moves 32 blocks ahead of the cursor, and the service answers **503 `not_ready`** until the follower has appended them: **13.5 s** unloaded (16:59:40 CEST) and **18.3 s** with 100 connections hitting it (17:06:04 CEST). That is 3.5-4.8% of the time, every 6.4 min. The README says the tail never makes the service not ready, and that holds, but the follower does (§5). *Since `2268cbd`/`5cccd13` the window answers 200 with `stale: true` and `staleReason: "Global index has not reached the finalized head"` instead; not re-measured.*
- **Squid, for comparison:** 18.2k calls per day, p50 12 and p95 17 per minute. i-see-u makes about 1.6× the calls: it polls `latest` about twice per block, and its follower re-fetches finalized headers.

## 3. Reorgs and robustness

**Reorgs.** There were 8 live forks in 34.6 h, at blocks 26,208,183, 26,211,085, 26,212,487, 26,213,670, 26,214,990, 26,216,633, 26,217,812 and 26,218,129.
- **Depth:** each rolled back to the fork block − 2, so one orphaned block.
- **Extra RPC:** none, or a single `eth_getBlockByHash`. The ancestry the processor needs (the "refs" in the log) came from blocks already streamed.
- **Stock SDK comparison:** `ForkException` carries one ref, so `findRollbackIndex` returns −1 and the processor exits with "Unable to process fork". This is reproduced in `../disquid/test/source/fork.test.ts`.

**RPC outage.** During the first run the provider's hostname stopped resolving (`ENOTFOUND`) for a while. 1,376 calls failed and were retried with back-off. The processor never exited and resumed when DNS came back.

**For comparison, i-see-u on the same single endpoint.** A burst of 17 transient Quicknode errors opened i-see-u's circuit breaker ("No RPC endpoint is currently available") until its cooldown, which paused the tail. A second `RPC_FALLBACK_URLS` endpoint would have avoided it. The squid retries without a breaker, so it rides out such bursts but has no failover target either.

**Process loss.** The detached processor and API were killed from outside after 34.6 h. There was no crash log; they were most likely cleaned up together with the agent session that started them.
- **Recovery:** on restart the processor resumed from its database state at 26,218,506 and caught up the ~550 missed blocks in about 2 minutes.
- **Lesson:** production needs a supervisor, such as systemd, Docker `restart: always` or SQD Cloud.

## 4. Correctness: parity with i-see-u

i-see-u stores every raw Transfer (139.8M rows from 25,933,634), so it is an independent reference. The checks compare pairs, first-seen block and full rows (first/last block, sent and received counts). Full rows are compared only for pairs with no activity after the cut-off block.

| Check | Range | Compared | Mismatches |
|---|---|---|---|
| ERC-20 pairs, first run (no zero-value skip) | 26,104,555–26,106,500 | 393,516 pairs, 372,611 full rows | 0 |
| ERC-20 pairs, re-index (i-see-u filtered to `value > 0`) | same | 386,213 pairs, 334,711 full rows | 0 |
| ERC-721 pairs | same | 5,627 pairs, 5,181 full rows | 0 |
| ERC-721 current owner and last transfer block (`nft_item`) | same | 12,381 items | 0 |

## 5. API load

> **Earlier, unequal data.** The i-see-u figures in this section come from earlier runs on different ranges or databases. For squid against i-see-u, use the [fair comparison](#fair-comparison-same-range-same-data-2026-101819) above.


**Method.**
- **Load:** closed loop, with N connections each sending the next request as soon as the previous one returns. Each run lasted 30 s, against `tokenDiscovery(account, first: 100)`.
- **Accounts:** 3,000 random accounts sampled from `account_token`. A response carried 18.4 tokens on average, which is heavier than a typical account.
- **Conditions:** one `squid-graphql-server` process with a pool of 10. The processor was writing at the head at the same time, and the load generator ran on the same machine.
- **i-see-u:** the same 3,000 accounts (`accounts.txt`, sampled from the squid's `account_token` with the recipe under Reproduce), the same closed loop and durations, `GET /v1/ethereum/accounts/{addr}/erc20` with the default `finality=latest`, one `npm start` process (pool 20) with its finalized follower and tail running. The endpoint has no `limit` parameter, so it returns every token (not `first: 100`). It was run on 2026-10-18 16:50-17:07 CEST.

| Connections | Req/s | p50 | p95 | p99 | max | Errors |
|---|---|---|---|---|---|---|
| 1 | 456 | 1.3 ms | 6.2 ms | 18.1 ms | 86.6 ms | 0 |
| 10 | 3,027 | 3.2 ms | 4.9 ms | 5.9 ms | 52.2 ms | 0 |
| 50 | 3,230 | 15.3 ms | 18.7 ms | 21.5 ms | 37.3 ms | 0 |
| 100 | 3,055 | 32.2 ms | 39.0 ms | 46.5 ms | 94.9 ms | 0 |
| 5, with balances | 44 | 104 ms | 182 ms | 273 ms | 766 ms | 0 |

**i-see-u, same method** (REST, tokens per response is the average in the run).

| Connections | Req/s | p50 | p95 | p99 | max | Errors | Tokens per response |
|---|---|---|---|---|---|---|---|
| 1 | 160 | 2.3 ms | 13.2 ms | 40.6 ms | 3,141 ms | 0 | 66.4 |
| 10 | 1,820 | 4.4 ms | 10.5 ms | 24.2 ms | 779 ms | 0 | 59.1 |
| 50 | 2,002 | 22.7 ms | 38.7 ms | 62.6 ms | 154 ms | 0 | 57.2 |
| 100 | 1,950 | 47.6 ms | 72.2 ms | 90.3 ms | 191 ms | 0 | 58.0 |
| 5, with balances (20 s) | 50 | 87.3 ms | 178.8 ms | 479.9 ms | 1,895 ms | 0 | 91.2 |
| *Extra:* 10, `finality=finalized` | 2,640 | 3.1 ms | 6.6 ms | 16.4 ms | 148 ms | 0 | 57.3 |

- **The 100-connection row is the clean rerun.** Two other 100-connection runs landed on an epoch boundary and returned **503 `not_ready`**: the first had 20,275 of 53,805 requests (38%) fail, the second 10,425 of 56,498 (18%). Their 200-only latencies and rps were similar (1,790 and 1,881 req/s). 503s are fast, so a closed loop sends more of them. The 1, 10 and 50-connection runs, and the `finalized` run, did not hit a boundary (0 errors).
- **Responses:** about 9-10 KB each (about 36 KB with balances).
- **Balances run:** 1,005 requests (about 91.7k token pairs, *derived*) made **210 `eth_call`s** (about 4.2k credits) at the counting proxy, so about 437 pairs per Multicall. Accounts and blocks were cold. The run did not repeat accounts enough to test the snapshot cache (1,005 draws from 3,000 accounts).
- **Max latency:** the 1-connection max (3.1 s) and the 10-connection max (0.8 s) are single outliers, not a trend. Their cause was not investigated.

**Reading it.**
- **Saturation:** one Node process tops out at about 3,000 req/s. Past 10 connections, more concurrency only adds queueing.
- **Assignment target:** 5k users polling every minute is about 83 req/s, so about 3% of one process.
- **1M users each polling every minute** (the worst case, not the expected load) is about 16.7k req/s, roughly 6 API processes. Database read capacity at that rate was **not tested**.
- **i-see-u, earlier single requests (after its full-window bootstrap, not a load test):** vitalik.eth with 163 tokens in 77 ms, the Binance hot wallet with 393 tokens in 56 ms, and vitalik.eth with all 163 balances in 0.3 s.
- **i-see-u against the squid:** one process serves about 1,800-2,000 req/s against the squid's 3,000-3,200, but each i-see-u response carries about 3× the tokens (57-66 against 18.4; the history starts earlier, zero-value Transfers are kept and there is no `first` limit, *derived* reasons), so per token it moves about 2× more, about 107k tokens/s at 10 connections against about 56k. At 1 connection it is slower (160 against 456 req/s, p50 2.3 ms against 1.3 ms). Its p99 at 100 connections (90 ms) is twice the squid's (46 ms). The 503 window at each epoch (§2, since replaced by stale 200s) had no squid counterpart in this data.
- **With balances:** the squid run is RPC-bound, because every pair was a cache miss (random accounts, cold cache). Its number of `eth_call`s was not recorded. i-see-u's was (above): 210 for 1,005 requests. With the change-driven cache, repeat polls of the same account make no RPC calls until a Transfer touches it.

## 6. Storage

> **Earlier, unequal data.** The i-see-u figures in this section come from earlier runs on different ranges or databases. For squid against i-see-u, use the [fair comparison](#fair-comparison-same-range-same-data-2026-101819) above.


| | Subsquid (`../disquid`) | i-see-u |
|---|---|---|
| Blocks covered | 114.6k (from 26,104,555) | 272.4k (from 25,933,634) |
| Per-pair aggregate | `account_token`: 27.07M rows, 15 GB, about **550 B/row** (string ids `account:token:std`, 3 indexes) | `holdings`: 54.26M rows, 13 GB, about **240 B/row** (bytea keys) |
| Raw transfers | none (not stored) | `transfers`: 139.8M rows, 46 GB |
| NFTs | `nft_item`: 601,655 rows, 246 MB | `nft_ownership`: 1.85M rows, 408 MB |
| Tokens | 69,752, all with fetched metadata | — |
| Distinct accounts | 7.71M | — |
| Whole database | **15 GB** | **60 GB** |

- **Cross-check on pair counts:** i-see-u's HyperSync investigation counted 25.78M (holder, token) pairs, 8.18M accounts and 59,788 tokens over the last 100k finalized blocks. The squid has 27.07M pairs over 114.6k blocks, with zero-value Transfers skipped. Both are about 0.24–0.26M new pairs per thousand blocks.
- **Per row:** Subsquid needs about 2.3× more bytes per aggregate row, because squid entities require string ids and the schema has three indexes on long strings.
- **In total:** it stores 4× less, because it keeps no raw history.
- **Compaction (not done):** binary addresses and a single account-first serving index (`COLLATE "C"` on the id) would roughly halve the 15 GB.

## 7. Spam: zero-value ERC-20 Transfers

Measured on i-see-u's raw data for 26,104,555–26,173,814 (69k blocks).

| | All | Only from zero-value Transfers | Share removed by the skip |
|---|---|---|---|
| Interactions | 69.1M | 4.03M | 5.8% |
| (account, token) pairs | 17.72M | 1.49M | **8.4%** |
| Accounts | 6.43M | 0.81M | **12.6%** |

Most spam is fake tokens moving non-zero amounts, which this skip doesn't catch. A verified-token list and read-time spam flags are still needed. With metadata, a fake "USDT" now displays `USDT`.

## 8. Token metadata

- **Live check:** one Multicall3 `eth_call` (236 ms) read name, symbol and decimals for USDT, USDC, MKR (a `bytes32` symbol, decoded) and an address with no code (all null).
- **Backfill:** 18,169 tokens that existed before the feature were filled in by the processor's sweep, 200 per batch.
- **Now:** all 69,752 tokens are `FETCHED`. New tokens get metadata in the same insert, with one `eth_call` per 50 new tokens per batch.

## 9. Monthly RPC budget (Quicknode Build, 80M credits)

> **Earlier, unequal data.** The i-see-u figures in this section come from earlier runs on different ranges or databases. For squid against i-see-u, use the [fair comparison](#fair-comparison-same-range-same-data-2026-101819) above.


| Item | Subsquid (`../disquid`) | i-see-u (current branch) |
|---|---|---|
| Following the head | about 11M (measured, 34.6 h) | about **17.7M** (measured 45 min, tail plus finalized follower together, *derived* to a month) |
| Metadata | about 1 call per batch with new tokens, *derived*: under 0.1M | — (not fetched) |
| Balances | On request only. A pair is re-read only after its own Transfer or once its cache entry ages out. Not measured live | On request only, same reuse rule (README, Balances). One cold run measured: 210 `eth_call`s (about 4.2k credits) for 1,005 requests, about 91.7k pairs (*derived*) |
| **Steady state** | **about 11M + balances** | **about 17.7M + balances** |
| One-off catch-up (global index) | about 2.1M credits per 100k blocks, measured RPC-only before `95851d9`. About **33k** per 100k with the header-less catch-up (*derived* from the live 0.026 calls per block) | RPC only: about 2.1M per 100k blocks (measured on the follower, 1.04 calls per block; 2.0M *derived* for the bootstrap). With HyperSync: about 15k per 100k blocks (from the 2,001-block smoke run) |

Balance cost follows account activity in both implementations; the README's Balances section has the math for 1M accounts. At the head, i-see-u costs about 1.6× the squid (§2). The real gap used to be catch-up, where i-see-u with HyperSync used about 130× fewer Quicknode credits. With the squid's header-less catch-up, both now spend tens of thousands of credits per 100k blocks: the squid about 33k (*derived*, RPC only), i-see-u about 15k (with HyperSync).

## 10. Speculation: HyperSync as the squid's history source

Nothing in this section has been built for the squid. The HyperSync fetch figures are measured by i-see-u (§1); everything else is *derived*.

**It fits without patching the SDK.** A squid source is any object with `getHead`, `getFinalizedHead`, `getStream` and `getFinalizedStream`, yielding batches of `{header, logs}` blocks. There are two ways to plug HyperSync in:

- **Hybrid source (preferred), roughly 150–250 lines.**
  - **History:** below the finalized head, it streams from HyperSync. That means topic0-filtered log pages plus `includeAllBlocks` headers (number, hash, parentHash, timestamp), merged into squid's block shape.
  - **Head:** at the finalized head it hands off to the existing `DiscoveryRpcSourceBuilder` source with `{from: f + 1, parentHash: hash(f)}`. RPC keeps the head, finality and reorgs; finalized blocks never fork, so the HyperSync side needs no fork logic.
  - **Same split as i-see-u:** this is i-see-u's own split (bulk bootstrap, RPC tail) expressed as a squid source.
  - **Reusable code:** i-see-u's `src/sources/hypersync-pages.ts` (paging, 429 sleeps, idle timeout, no `stream()` hang), `hypersync-wire.ts` (fast decoding) and the header query in `src/bootstrap/sources.ts`.
- **`EvmFallbackDataSourceBuilder` with `{type: 'custom'}`.** HyperSync becomes one ranked source, with the RPC source as backup. The fallback switches on health, not on block range, so HyperSync would also have to follow the head and detect forks. A HyperSync capped at the finalized block would only hand over after the 180 s staleness timeout. This is useful for failover; the hybrid can be its primary source.

**Expected impact:**

| | RPC-only (measured) | HyperSync for history (*derived*) |
|---|---|---|
| Quicknode credits, 100k-block catch-up | about 2.1M with per-block headers; about 33k (*derived*) logs-only since `95851d9` | about 0 (head and finalized lookups only) |
| HyperSync requests, 100k blocks | — | **660 requests, 41.5 min, 53.78M logs** (measured by i-see-u, free tier, fetch only) |
| Time, 100k blocks | 2 h 59 min | **Still about 1.5–2 h.** Fetching takes about 42 min (measured), but the squid handler writes 15–20 blocks/s |
| Full history from genesis (about 24M blocks) | With per-block headers: about 24M header calls ≈ 480M credits, not feasible on 80M/month. Logs-only (*derived*): about 240k `eth_getLogs` (about 4.8M credits), plus one header for every block without a Transfer, which is common in early history (not measured) | Feasible on credits, but about **2–3 weeks** of writes at 15–20 blocks/s |

**HyperSync no longer changes much for the squid.** The header-less catch-up already brought its catch-up cost down to about 33k credits per 100k blocks, and HyperSync does not fix its speed. The speed limit is the write path, so it needs:
- insert-only writes for finalized ranges;
- a larger fold per batch;
- possibly `COPY`.

i-see-u's bootstrap loaded about 105 blocks/s on its old staging path, measured over the full window, because it wrote in bulk.

**Alternatives:**

| Source | Integration | What we know |
|---|---|---|
| Envio HyperSync | Custom source, about 150–250 lines | Fast in our tests: 2.55M logs in 13 s, and 436k logs in 7 requests. Free tier 15 requests/min; pages truncated at about 28k–150k logs; native addon checked on darwin-arm64 only |
| SQD Portal | Built in, `{type: 'portal'}`, no code | Free tier returned 529s at about 0.1 MB/s in our test (PROGRESS §11) |
| Envio HyperRPC | Just another RPC URL with fast `eth_getLogs` | Untested: our token got 403. The cheapest experiment if it works |

**Caveats.**
- **Not RPC-only any more:** this adds a vendor dependency, so keep RPC as the per-range fallback, as i-see-u does.
- **Coverage:** HyperSync covers about 70+ EVM chains. The assignment's long-tail networks may not be among them, so RPC-only stays the general path, and HyperSync is an accelerator where it exists.
- **To validate before trusting it:** re-index a range with the hybrid source, run `scripts/parity.sh` against the RPC-built index, and time a 100k-block run.

## Caveats

- **Fair run, remaining differences:**
  - The squid skips zero-value ERC-20 Transfers, leaving about 7% fewer pairs on the same range.
  - REST against GraphQL.
  - i-see-u returns all tokens, while the squid pages at 100. The no-balance tables therefore use accounts with 100 or fewer tokens; the balance table uses all accounts, so its tokens per response differ.
  - i-see-u was tested first and the squid second, minutes apart, each with its own processor following the head.
  - Each figure comes from a single run.
- **The bootstrap was interrupted twice:** once by a full disk and once by about 72 min of host sleep, which caused DNS errors and a 4-segment retry pass. Its time is active time, computed from the log timestamps with gaps over 60 s removed. Without the sleep, HyperSync would probably have loaded more of the logs; in the resumed passes Quicknode workers loaded most of them.
- **The old 60 GB i-see-u database (from 25,933,634) was dropped** to make room. Sections that cite it (§1 full window, §4 parity, §6) are historical. The fair-run database `iseeu_bench` (37 GB) is still there; drop it with `docker exec i-see-u-postgres-1 psql -U iseeu -d postgres -c 'DROP DATABASE iseeu_bench'`.

- **One machine, one provider, one run.** Speeds depend on Quicknode's latency on the day. During i-see-u's earlier per-account work (PROGRESS §7), per-call latency varied 5–10× within one day (0.2–0.4 s in the morning, 1.7–2.0 s later).
- **API numbers are from localhost**, so there is no network RTT, and the load generator competed with the server for CPU.
- **History starts at 26,104,555 on purpose.** Holdings untouched since then are invisible.
- **Balance RPC cost under load** and **the database at 16.7k req/s** were not measured.
- **Head cost is a 34.6 h sample.** A day with more reorgs or a lagging provider would cost a little more.
- **i-see-u head cost is a 45-minute sample** (225 blocks, 7 epoch bursts, 0 reorgs), against the squid's 34.6 h. Per-day and per-month figures are extrapolated from it. A longer run would add reorg re-reads and finality-stall behavior.
- **i-see-u was measured once, through a counting proxy** on `localhost:8546` that forwards unchanged, so latency to Quicknode includes one extra local hop. The proxy counts JSON-RPC items, not credits; 20 credits per call is an assumption.
- **The API comparison is not like for like.**
  - REST against GraphQL; i-see-u has no `first` limit and returned 57-66 tokens per response, the squid 18.4.
  - The fields differ: contract, first and last seen block, sent and received counts against token, symbol, decimals and last seen block.
  - The default `finality=latest` adds the unfinalized overlay (one extra `tail_transfers` lookup). The `finalized` view at 10 connections was 2,640 req/s against 1,820.
  - Two different databases: 60 GB with 140M raw rows against 15 GB. The squid's pool is 10, i-see-u's is 20.
  - Both runs share the host with the load generator, the processor or follower, and the tail. The i-see-u run also overlapped with a busy macOS (load average about 10 at one check).
  - Single 30 s runs, no repeats except the 100-connection one. Run-to-run noise was not measured.
- **Epoch 503 windows** hit the 100-connection runs twice. A load test that avoids them overstates availability; one that includes them overstates the error rate (503s are cheap, so they pile up in a closed loop). The window is 13.5-18.3 s per 6.4 min. It has since been replaced by stale 200 responses, so a rerun would show neither effect.
- **The follower catch-up is one run** on a database that was 1.9 days behind and had a concurrent autovacuum, and the host slept for part of it (about 42 min without progress). The 3.7 blocks/s, excluding that gap, is not a tuned figure and was not repeated.

## Reproduce

```bash
cd ../disquid && pnpm install && pnpm run build
docker compose up -d && pnpm exec squid-typeorm-migration apply
node lib/main.js                                   # processor; RPC calls per minute are logged under sqd:rpc-cost
pnpm exec squid-graphql-server --max-root-fields 10
```

```bash
scripts/parity.sh 26106500                         # needs both Postgres containers; prints mismatches per standard
docker exec disquid-db-1 psql -U postgres -d squid -tAc \
  "select account from account_token tablesample system (0.5) where standard='ERC20' limit 3000" > accounts.txt
node scripts/loadtest.mjs accounts.txt 10 30       # <connections> <seconds> [includeBalances]
```

i-see-u side (the proxy and load-test scripts were run from a scratch directory and are not in the repo):

```bash
# counting proxy on 127.0.0.1:8546 (forwards POST bodies, counts JSON-RPC items per method, one JSON line per minute)
UPSTREAM_RPC_URL=<real endpoint> node rpc-proxy.mjs
RPC_URL=http://127.0.0.1:8546 npm start            # RPC_FALLBACK_URLS unset; /health shows ready and tail lag 0
node loadtest-rest.mjs accounts.txt 10 30          # <connections> <seconds> [balances=true|false] [finality]; GET /v1/ethereum/accounts/{addr}/erc20
```

Re-running `scripts/parity.sh 26106500` on 2026-10-18 again gave 0 mismatches: 386,213 ERC-20 pairs (189,159 full rows) and 5,627 ERC-721 pairs (2,670 full rows). Fewer full rows qualify now because more pairs have activity after the cut-off.

**Fair run (same range).** These run from the i-see-u directory unless noted. The counting proxy (`../disquid/scripts/rpc-proxy.mjs`) forwards to `UPSTREAM_RPC_URL` and logs calls per method to `PROXY_OUT`.

```bash
docker exec i-see-u-postgres-1 psql -U iseeu -d postgres -c 'CREATE DATABASE iseeu_bench'   # then apply i-see-u's migrations with DATABASE_URL pointing at it
START_BLOCK=26104555 DATABASE_URL=postgresql://iseeu:iseeu@localhost:5432/iseeu_bench RPC_URL=http://127.0.0.1:8546 npm run bootstrap   # rerun to resume
START_BLOCK=26104555 DATABASE_URL=postgresql://iseeu:iseeu@localhost:5432/iseeu_bench RPC_URL=http://127.0.0.1:8546 npm start
node ../disquid/scripts/loadtest-rest.mjs accounts-le100.txt 10 30   # i-see-u REST
node ../disquid/scripts/loadtest.mjs accounts-le100.txt 10 30          # squid GraphQL (processor and API started from ../disquid)
```
