# Progress log: token discovery PoC (onsite day, 2026-10-14)

How the PoC got to its current shape: what we tried, what we measured, what changed and why. Mistakes and reversals are included on purpose; most of the design came out of them. The design as built is in [REPORT.md](REPORT.md), and running it is covered in [README.md](README.md).

Times are local (CEST). "We" is Viktor plus AI agents: a Claude orchestrator handed implementation to codex workers (gpt-5.6-sol) and reviews and research to Opus and Sonnet subagents. Viktor made the decisions, and Jakub and Tali answered scope questions.

## The arc in one table

| Time | Phase | Outcome |
|---|---|---|
| 10:15–11:06 | Research: indexer frameworks or plain RPC | Plain JSON-RPC (`eth_getLogs`) on Quicknode is the primary source. SQD and Envio are backfill or backup sources |
| 11:06–11:52 | Scope questions and verification | Six scope answers. Quicknode probed live. REPORT rev 2 argued for global ingestion |
| 11:52–12:10 | First build: per-account, on demand | Works end to end. Live runs exposed a stack overflow and a 720 MB SQLite file |
| 12:58–13:10 | External review, then finalized-only | Only finalized blocks are stored. The cursor stores the block hash |
| 13:18–14:00 | Stack swaps | Hono, vitest, `process.loadEnvFile`, viem on top of our own RpcClient |
| 13:56–14:28 | Second review and quick fixes | Stuck-provider detection, resumable cold scans. The cost model was corrected (balances dominate) |
| 14:23–15:23 | Chunk-size experiments (10k, 1k, 3k) | 3k blocks, a pipelined scan, aggregation during decoding, an adaptive range |
| 15:31–15:44 | First commits and a simplify pass | 9 commits, then −636 lines with no behaviour change |
| 15:42–15:54 | Postgres 18 and Drizzle | SQLite removed. PGlite in tests |
| 15:54–16:21+ | Squid SDK, Envio and Ponder research | Squid SDK and Ponder rejected. HyperSync benchmarked live and is very promising for backfill |
| 15:56–16:52 | Shared finalized follower and balance worker | Warm requests make 0 RPC calls. About 51M credits/month for 5k users |
| 17:00– | HyperSync `LogSource` for backfill | Codex, started from Viktor's main session. Landed as an opt-in, experimental source with per-chunk RPC fallback (76 tests) |
| 18:00–21:00 | Realtime tail with reorg handling | Jakub: realtime is part of the brief. Opus design, Codex build, Opus audit (9 bugs fixed). Tail lag 0 blocks live; 93 tests |
| 23:50–01:15 | `/improve` audit and fixes (branch `feat/improve`) | Three parallel audits (Codex sol, 2× Opus), 5 fix plans run by Sonnet, sol and Opus in isolated worktrees, each reviewed. 115 tests; live run passed every check |
| 11:00–13:30 | Budget: change-driven balance refresh (plan 006) | Opus built levers 1+3, 1 revision (orphaned snapshots on non-leaders). Balance sweep ~43M → ~1.6–6M credits/month; total ≈53M → ≈12–17M (estimates). 130 tests |
| evening | SQD squid-sdk RPC source, revisited (`../disquid`) | Sonnet, source-level. Not adopted. Full blocks and unfiltered `getLogs` are hardcoded; a patch to three packages reaches our call count. Patch specified as plan 007 for Codex |

---

## 1. Research: which technology? (10:15–11:06)

**Question:** use SQD, Envio or Goldsky, or build on plain RPC? The brief says "není full indexer" ("not a full indexer"), and we have Quicknode.

- First take: no managed indexer at the core. The brief is about new networks that only have JSON-RPC, and a hosted indexer exists only where the vendor already supports the chain. SQD Portal and Envio HyperSync remain useful as optional data sources.
- An Opus agent produced an independent architecture concept: TypeScript, viem, Fastify and Postgres. It proposed a "hybrid": lazy per-address backfill plus a shared follower for watched addresses. A second opinion (another tool, pasted in by Viktor) suggested a global Transfer worker for all addresses with Fastify and Kysely. We merged the two into REPORT revision 1 (watched-only).
- **Clarification with Jakub and Tali (10:50):** "full indexer" means full discovery from blocks, transactions, native transfers and so on (Blockbook-style). They had searched for an off-the-shelf service that does token discovery over plain RPC and found none, which is the reason for the PoC.
- **Agreed (11:06):** `eth_getLogs` on Quicknode is primary for the PoC, with SQD and Envio as backfill or backup.

**What we learned:** the "not a full indexer" line rules out walking blocks and transactions, not keeping a narrow index of `Transfer` logs. That reopened the global-ingestion option (section 2).

## 2. Scope questions and verification (11:06–11:52)

Viktor asked for one more round of checks before any code. **Scope answers from Viktor:**

| # | Question | Answer |
|---|---|---|
| 1 | Tokens held before block 25,933,634 and never moved since | Skip in the PoC (documented limit) |
| 2 | Spam tokens | Return everything for now |
| 3 | Lag behind the chain tip | "A few seconds is OK." This was later overridden by the finalized-only decision (section 4) |
| 4 | What "interacted" means | `Transfer` in or out |
| 5 | Native ETH balance | Skip |
| 6 | ERC-721 response shape | Not specified. We prepared contracts plus token IDs, with optional `ownerOf` verification |

Our Quicknode endpoint (shared by Tali) is on a **premium plan, so 10k-block `getLogs` ranges work**.

**Measurements in this phase:**

| What | Result |
|---|---|
| Window | Block 25,933,634 was mined on 2026-09-08, so the window was about 256k blocks and grows by about 7,200 per day |
| Unfiltered `Transfer` logs, Quicknode demo endpoint | 10 blocks: 15k logs, 9.7 MB, 0.17 s. 100 blocks: 48 MB, 0.5 s. **1,000 blocks: 701,764 logs, 446 MB, 4.0 s.** No result cap |
| Address-filtered, 10k blocks (Binance hot wallet) | received side: 31k logs, 20 MB, 0.37 s |
| Logs per block (dRPC samples) | 305–999, typically about 450 (about 280 KB). Quicknode samples were closer to about 700 |
| Public RPCs | archive refused, "specify an address", "method not available", rate limited, HTTP 525. Result caps of 10k (mevblocker) and 20k (tenderly), both with a suggested sub-range in the error |
| Multicall3 `aggregate3` with `balanceOf` | 2,000 calls are safe (about 6.8k gas each). HTTP 413 above about 1.3–1.7 MB of calldata. `eth_estimateGas` is capped at 16.7M |
| SQD Portal | 0 head lag. Topics must be lowercase and 32-byte padded, otherwise it **silently returns 0 logs**. Frequent 529 errors on the free tier, about 0.1 MB/s |
| HyperSync | API token required (401) |
| Distinct (holder, token) pairs if we indexed everyone | about 15–40M, about 1.5–6 GB *(extrapolated from 300-block samples)* |

**Doc review corrections (Opus):** Quicknode bills **every call inside a JSON-RPC batch**, so batching saves nothing. GoldRush and Ankr do serve historical holdings, but they are proprietary and cover major chains only. That still leaves the "new network" gap open.

**Decision, later reversed:** REPORT rev 2 argued for **global ingestion**, meaning every `Transfer` log for all accounts. The argument was that the follower downloads every log at the head anyway. We built per-account discovery first because it was faster to get working. The final design keeps per-account backfill (REPORT §1.1 explains why).

## 3. First build: per-account, on demand (11:52–12:10)

The orchestrator wrote the shared contracts (`ChainAdapter`, `RpcClient`, `Store`). Three codex workers built the rest in parallel: (A) a resilient RpcClient plus `getLogs` with range splitting, (B) a SQLite store (`node:sqlite`) plus the discovery service, (C) Multicall3 plus a Fastify API.

A request for a new account scanned [start, head] with two `getLogs` calls per 10k-block chunk (sent, received). Later requests scanned only the new blocks.

| Account | Cold | Warm |
|---|---|---|
| Normal (`0x6752…`, 9 tokens) | 2.4 s, 52 calls | 13 ms, 0 calls |
| Active (`0x5a52…`, 109 tokens) | 2.3–2.6 s, 52 calls | 19 ms |
| Binance hot wallet (`0x28c6…`, about 2.5M transfers, 385 tokens) | 81 s, 52 calls, mostly client-side JSON parsing | 77 ms |

**Bugs the live run found that unit tests did not:**
- `push(...bigArray)` overflowed the call stack on Binance.
- The store journalled every transfer, which made **720 MB of SQLite for one wallet**. It changed to one aggregate row per (account, token), under 200 KB.
- `rpcCalls` in the response always said 2.

**Opus code review, 7 fixes, 22 → 27 tests:**
- `isTooLarge` was true for any RPC error. One failed chunk became 15 calls.
- Multicall halved its batch on rate limits. One request made 1,999 `eth_call`s.
- `Retry-After` was uncapped, so a request could wait up to an hour. Capped at 10 s.
- The head could move backwards on a lagging fallback endpoint.
- A `null` block threw a plain error.
- The circuit breaker could stay stuck half-open.
- Workers kept fetching after a request had already failed.

## 4. External review, then finalized-only (12:58–13:10)

Viktor pasted an external review with six findings, all valid:
1. Polling load: 5k users need about 250 RPC calls/s, but the client limit is 20/s.
2. A reorg corrupts stored results permanently.
3. `blockHeight` and `blockHash` could describe different blocks.
4. Stale data was served as a normal 200.
5. A `null` block skipped failover.
6. Multicall still split on 401.

**Decision (Viktor, 13:01): "let's try the finalized first".** Only finalized blocks are stored, the cursor saves that block's hash, and `blockHeight`/`blockHash` come from the same cursor. Responses gained `stale`/`lagBlocks`, with a 503 past a maximum age. A new `validate` option makes `null` results fail over inside the client. Multicall now splits only on size, gas or revert errors.
- Bug found afterwards: `lagBlocks` was always 0, because `latest` was never fetched in finalized mode.
- Later (14:23, section 7), Viktor confirmed: "We do not need roll-back… only finalized blocks". Responses therefore trail the tip by about 64 blocks (about 13 min). That is well beyond the "few seconds" from scope answer 3; see "Where we stand".

## 5. Stack swaps (13:18–14:00)

| Change | Why |
|---|---|
| Fastify → **Hono** | Viktor prefers Hono. Only `routes.ts`, `server.ts` and the API tests depended on the framework. Live testing caught a bug the tests missed: the logger wrapper app made 404s plain text |
| Hand-written `.env` parser → **`process.loadEnvFile()`** | Viktor asked why not dotenv. Node has a built-in loader, so we need neither a dependency nor our own parser |
| `node:test` → **vitest** | Viktor asked for it. Watch mode, `vi.fn`, fake timers and `stubGlobal` replaced hand-rolled stubs |
| viem used only for ABI encoding → **viem on top of our RpcClient** | Viktor: "Why didn't we use viem? It was literally mentioned by you." He was right; the docs implied usage the code didn't have. viem now provides types, ABIs, topic encoding, strict `decodeEventLog`, and `getBlock`/`call` |
| Our `fetch` code → **viem `http()` transport inside RpcClient** | Viktor: "why are we handling that on our own?" We checked viem 2.57.6's source. Its retry and fallback would resend `-32005` "more than 10000 results" instead of splitting the range, miss Quicknode `-32010`, treat `null` as success, and lack a breaker and a Retry-After cap. So viem `http()` does only the transport (`retryCount: 0`, no batching), and our policy stays |

Catches from the transport swap: viem caps responses at **10 MB** by default (our logs reach 446 MB), so the cap is disabled. viem's error messages contain the full URL with the API key, so they are never passed through as `cause`. Tests went from 35 to 42.

## 6. Second review, quick fixes, cost correction (13:56–14:28)

A second pasted review: "tests show component correctness, not assignment completion". The new valid findings:
- A stuck provider looked healthy, because `observedAt` kept resetting.
- `null` from `getLogs` did not fail over.
- Cold scans were all-or-nothing in memory.
- A test assertion inside a mock was swallowed.

It also corrected two of our own assumptions:
- **Rescanning the tail per request does not scale.** It still costs 2 calls per request. One shared follower has to do it.
- **"No Transfer, so the balance is unchanged" is false.** WETH `deposit`, rebasing tokens and admin actions break it. Every balance must carry the block it was read at.

Quick fixes (codex):
- Three separate timestamps: last success, last progress, block age.
- `null` validation on logs.
- Per-chunk commits that resume after a failure.
- Balance reads pinned by hash (EIP-1898).

Tests went to 47. Codex ran out of usage partway through, so the rest went to Opus agents.

**The cost model was corrected in REPORT:** the earlier 19M credits/month assumed only "dirty" pairs need re-reading. Re-reading balances on a cadence costs about 43M/month, which makes balances the dominant cost.

## 7. Chunk-size experiments (14:23–15:23)

**Viktor: "why not 10k always? It costs 20 credits either way."** That holds for address-filtered queries. **Unfiltered** queries are bounded by payload instead: about 450 KB per block, so 10k blocks would be about 4.5 GB in one response, beyond a JS string. That is why follower ranges are capped at 500 blocks.

Viktor wanted to see raw responses, so we wrote `scripts/call.sh`. The probes of 10k-block chunks filled `out/` with 90 MB.

| 10k-block call | Time | Size | Logs |
|---|---|---|---|
| Normal account, sent / received | 0.27 / 0.21 s | 3.1 KB / 37 B | 5 / 0 |
| Binance, received | 2.6 s | **49 MB** | 80,726 |
| Binance, sent | 6.3 s | 7.7 MB | 12,716 |
| Unfiltered, 10 blocks | 0.10 s | 1.6 MB | 2,613 |

By then the normal account had slowed from about 2.5 s to about 5.5 s. A plain curl showed Quicknode at 1.7–2.0 s per call, against 0.2–0.4 s in the morning. Our in-order commit window made it worse: one slow chunk at the front idled the rest. Binance hit 1.9 GB RSS and 9–12 transient errors, from 8 parallel 20–50 MB downloads against a 15 s timeout.

**Jakub suggested 1k-block chunks** (smaller responses, cheap retries, under other providers' result caps). We measured before changing anything:

| Chunk | Normal, cold | Binance, cold | Peak RSS | Calls/scan | Errors | Provider |
|---|---|---|---|---|---|---|
| 10k | 5.5–11.7 s | 64–98 s, some attempts failed | 1.9 GB | 52 | 9–12 transient | slow |
| 1k | 36.7 s | 305 s | 733 MB | 516 | 12 transient | slow |
| 1k, `RPC_MAX_RPS=40` | 43.8 s | stopped | | 516 | | slow |
| **3k** | **7.8 s** | **73 s** | about 1.2 GB | **172** | **0** | fast again |

Raising the RPS cap did not help 1k, so the bottleneck was per-request latency at Quicknode. The rows were not measured under equal provider conditions. **Decision: 3k blocks** (`ETH_LOGS_MAX_RANGE=3000`). It is Jakub's "smaller is safer" at 3.4k credits per new account.

Then an Opus agent rebuilt the scan (47 → 58 tests):
- **Pipelining:** a new chunk starts as soon as any chunk finishes, and commits stay strictly in order. Buffered chunks are capped at 2× concurrency, and the default concurrency is 8.
- **Aggregation while decoding:** each log goes straight into per-token totals. There are no event arrays to concatenate, sort and dedupe.
- **Split on timeout:** a timed-out multi-block `getLogs` is halved, like "too large", instead of re-downloading the same response.
- **Adaptive per-account range,** persisted in `account_scan_range`.

| Run (3k, pipelined) | Time | Calls | Peak RSS |
|---|---|---|---|
| Normal, concurrency 4 (provider 4–9 s per call) | 15.3 s | 172 | 152 MB |
| Normal, concurrency 8 / 16 | 8.1 / 8.0 s | 172 | 153 MB |
| Binance, adaptive at a 5 MB threshold | 76.2 s | 446 | 1,045 MB |
| Binance, adaptive at a 50 MB threshold | 71.6 s | 320 | 1,107 MB |

**Learned:**
- Normal accounts are floored by our own `RPC_MAX_RPS=20`: 172 calls take at least 8.6 s.
- Heavy accounts are bound by download (about 1.6 GB of JSON). More scan tuning won't fix that; a faster backfill source will.
- The 5 MB threshold cost 2.6× the calls and saved no time, so we raised it to 50 MB.

## 8. Commits and simplify (15:31–15:44)

The work was committed as 9 logical commits on `poc/token-discovery`, at 15:32. They group the final state by area rather than replaying the order of the day. An Opus simplify pass then removed realtime leftovers, dead knobs and duplicated helpers, and replaced the legacy SQLite migration. The result was **net −636 lines across 21 files, 58 → 52 tests, no behaviour change**. Its value was a codebase small enough to review in an interview.

## 9. Postgres 18 and Drizzle (15:42–15:54)

Viktor: "We should also now set up Postgres. We agreed on that in the report." The follower needs an advisory lock and transactional cursor/upserts, so we switched before building it rather than writing the store twice.
- `docker-compose.yml` with `postgres:18-alpine`. A Drizzle schema and generated SQL migrations in `drizzle/` run at startup. The `Store` became async.
- **PGlite** (Postgres compiled to WASM) runs the same migrations in `npm test`, with no Docker needed.
- Live: the cold backfill took 172 calls and 9.6 s, then 0 calls and 60 ms. **Port 5432 clashed with another local container**, so the host port became configurable (`POSTGRES_PORT`).
- The codex lock helper held a transaction open for the lock's lifetime, which is wrong for a follower that runs indefinitely. The follower brief was changed to use a session lock on a dedicated connection.

## 10. Shared finalized follower and balance worker (15:36–16:52)

**Why:** a parallel session (Viktor testing the API himself) proposed the shared follower, and we corrected its numbers before building:
- **Our own overstatement:** it said "≈ 10–15k RPC calls/min". Under the finalized model, per-account rescans happen once per epoch, so discovery was about **1.6k calls/min**.
- The bigger cost was **balances: one Multicall per request, about 5k calls/min** (about 4.3B credits/month).
- So the follower alone was not enough. It had to come with a **cross-account balance worker**.

**The handoff rule:** track the account first, read the follower cursor C, backfill [start, C]. The follower owns every block above C.

**Built (Opus):**
- **Follower:** a session advisory lock elects the leader. It makes one unfiltered `getLogs` per finalized epoch (≤ 500-block ranges) and applies it to tracked accounts only.
- **Handoff:** while the backfill runs, requests get `202 syncing`.
- **Balance worker:** for accounts active in the last 5 min, it refreshes every 60 s and on each follower advance, with Multicall at the follower block pinned by hash. Every token carries its `balanceBlock`.
- **Request path:** bounded limiters with a per-request deadline (503 instead of unbounded queues).
- **Acceptance tests:** 5k tracked accounts, both handoff races, a frozen follower, single-flight balance reads, a backfill that fails midway.

| Live check | Result |
|---|---|
| One follower advance (26,191,174 → 26,191,206) | **1 `getLogs`**, plus 2 head calls per 30 s poll |
| Balance refresh, 109 pairs | **1 `eth_call`** |
| 10 repeat requests | **0 RPC calls**, 7–12 ms |
| Cold backfill under the shared 20 RPS limiter | 172 calls, about 26 s, two 202s |
| Fake RPC: 5k accounts × 10 tokens | 1 `getLogs` per epoch, 50 `eth_call` for 50k pairs, 0 calls for 5k requests |
| Vitalik (`0xd8dA6BF2…`) | 157 tokens after two 202s |

- The agent's "Vitalik returned 0 tokens" came from a mistyped address, not a bug.
- **Startup 503 (fixed, `2b7779c`):** the follower loaded its cursor only on its first RPC poll. `follower.load()` now reads it from Postgres before the server listens, and the server answered 200 50 ms after start. That made 66 tests.
- **Process hygiene mistake:** benchmark scripts used `pkill -f "src/server.ts"`. That killed Viktor's own dev server at least once, and possibly another project's server. Since then, only narrow patterns.

## 11. Squid SDK, Envio and Ponder (15:54–16:21, Ponder afterwards)

| Option | Verdict | Deciding facts |
|---|---|---|
| `@subsquid/evm-processor` 1.30.1 (RPC mode) | **Rejected** (Sonnet, source-level) | `eth_getBlockByNumber` per block plus unfiltered `getLogs` in 10-block slices is about 36 calls per epoch against our about 2 (15–20× the credits). Fixed filters before `run()`, so no dynamic tracked set or handoff. TypeORM store, owns the main loop, GPL-3.0 |
| SQD Pipes SDK 1.0.0-beta.8 | Rejected for now | Beta, Portal-only. Portal's free tier gave 529s at about 0.1 MB/s |
| Envio HyperIndex | Rejected | Same reasons as the Squid SDK: a full framework with a fixed config, its own DB and its own loop |
| **Envio HyperSync** | **Recommended backfill `LogSource`** | Benchmarked live with a free token (below) |
| Envio HyperRPC | Untested | 403: our token is not enabled for it |
| Ponder 0.18.1 | Rejected (no spike) | Static addresses force global ingestion. Per-block `eth_getBlockByNumber`, so 32+ calls per epoch. Reorg-aware only. Owns schema and process. See below |

**HyperSync live** (window 25,933,634 to finalized; `@envio-dev/hypersync-client` 1.4.1, napi addon, loads on Node 26 darwin-arm64 with ESM):

| Case | HyperSync | Quicknode today |
|---|---|---|
| Normal account, full window | **1 request, 0.6 s**, 219 logs, identical to Quicknode (0 diffs) | 172 calls, about 9.6 s |
| Binance, client `stream()` | **13.0 s**, 2,548,175 logs, peak RSS 580 MB | about 73 s, about 1 GB |
| Binance, `collect()` / sequential JSON | 19.3 s at 2.6 GB / 76.7 s | |
| Binance, one 3k-block slice | 26,902 logs, identical to Quicknode | |
| Follower epoch, 32 blocks unfiltered | 1 request, 150 ms, 16,777 logs (equal) | 1 call, 112 ms |

- **One OR query covers sent and received.** Two selections in one request return the union, with self-transfers deduplicated. Quicknode needs 2 calls.
- **HyperSync serves the chain head (0 lag vs `latest`).** A HyperSync source must cap `to_block` at our RPC finalized block, which ran about 75 blocks behind `latest`.
- No rate limit showed at 8 parallel requests. The client keeps the process alive until closed, and only the darwin-arm64 build was checked.

**Ponder (after the HyperSync benchmark).** Viktor asked to evaluate Ponder too (`ponder` 0.18.1, MIT, Node ≥ 22.12, Postgres 14–17 only). **Rejected from its docs and source (`sync-historical`), with no spike.** The log, row and credit numbers are estimates:
1. **Static addresses.** They are fixed in `ponder.config.ts`; only `factory()` is dynamic. Runtime-tracked accounts would force global ingestion: about 177M logs and 15–40M rows. There is no per-account backfill handoff.
2. **Per-block calls.** It calls `eth_getBlockByNumber(n, true)` for every block with a matching log, and realtime fetches every block. That is ≥ 257k block calls for the backfill (about 5–10M credits) and 32+ calls per epoch against our 1–2. This is the same reason the Squid SDK was rejected.
3. **Reorg-aware realtime** (`reorgWindow`), with no finalized-only mode.
4. **It owns the app:** its own schema, process and codegen. Balances via `publicClients` would cost one call per request.
5. **A backfill-only role** is redundant with HyperSync. SQD's fork `@subsquid/ponder` adds a Portal historical source (beta).

## 12. HyperSync backfill source (from 17:00, landed)

After the benchmark, Viktor started a Codex worker from his main session to build the HyperSync backfill. The parallel docs work stayed out of its files. **Planned shape** (taken from the work in progress, not yet reviewed or verified):
- **A new per-account `AccountLogSource` interface** in `src/core/types.ts`, with implementations in `src/sources/`:
  - `rpc-logs.ts`: today's Quicknode chunked scan.
  - `hypersync.ts`: `@envio-dev/hypersync-client` `stream()`, using the sent-or-received OR query. The native addon is loaded only on first use, so tests inject a fake.
  - `fallback.ts`: per-chunk fallback to RPC with a small breaker. A partial HyperSync result never leaks through.
- **Config:** `LOG_SOURCE=auto|hypersync|rpc` (default `auto`), `HYPERSYNC_API_KEY`, `HYPERSYNC_URL`, `HYPERSYNC_MAX_RANGE` (100k blocks), `HYPERSYNC_TIMEOUT_MS`.
- **Comparison:** `scripts/compare-sources.ts` compares sources side by side. `test/hypersync.test.ts` holds the unit tests.
- **Unchanged:** the follower and balances stay on Quicknode. HyperSync serves the chain head, so its ranges must be capped at our RPC-finalized block.

**Landed** (`9286c4f`…`6f8a812`, 76 tests):
- **Source selection:** `LOG_SOURCE` defaults to `rpc`. `auto` picks HyperSync only when `HYPERSYNC_API_KEY` is set, and the native addon is imported only when it is selected.
- **Query:** HyperSync sends one query for both directions and folds pages as they stream in (never `collect()`). It commits 100k-block chunks in order, always ending at the pinned finalized backfill target (exclusive `target + 1`).
- **Fallback:** any HyperSync failure reruns the whole uncommitted chunk through RPC, so partial vendor data never reaches the store. After two consecutive failures, a 60 s breaker skips HyperSync.
- **Results:** normal and NFT accounts matched RPC exactly (1 request vs 172 calls).
- **Open problem:** on the Binance hot wallet, the integrated stream for a 100k-block chunk (about 1M logs) **hung even with a 600 s timeout**, then fell back to RPC. The standalone benchmark had streamed the same wallet in 13 s. The cause is undiagnosed, so HyperSync ships off by default.

## 13. Realtime tail with reorg handling (18:00–21:00, landed)

**Trigger:** Jakub said realtime is part of the brief ("skoro realtime"), so reorgs must be handled. The finalized-only answer from section 4 was no longer enough.

**Corrected premise:** "finalized is about 5 min behind" is wrong. Live, `latest − finalized` was **90 blocks**, and the finalized block was **about 18 min old**. Finality trails by two epochs, which is 64–95 slots (about 13–19 min).

**Research (Opus, about 125 Quicknode calls in a live probe):**

| Option | Verdict |
|---|---|
| **Tail overlay**: store every decoded Transfer of the unfinalized blocks (finalized, tip] and overlay them at read time | **Chosen.** The finalized base is never touched, so a reorg only deletes and re-ingests tail rows. Accounts tracked mid-tail are covered, and the follower, backfill and handoff are unchanged |
| Journal plus rollback of the base tables | Rejected. Counts can be subtracted, but `LEAST`/`GREATEST` first/last block and NFT latest-wins cannot be undone without per-row before-images |
| Quicknode Streams, WebSocket `newHeads` (billed per notification), HyperSync head | Not now. Each adds a vendor to the hot path and conflicts with "new networks with only JSON-RPC". The head source stays pluggable |

**Measured:**
- **Logs by block hash:** `eth_getLogs({blockHash})` (EIP-234) works on Quicknode, returning the same logs as by-number in 14–84 ms.
- **Load balancing:** responses carry varying `x-node-id`. By-number queries can therefore return logs from a different fork than the header we just read, so tail logs are fetched by hash, or every `log.blockHash` is verified.
- **Visibility:** new blocks became visible **2.0–3.8 s** (median 2.6 s) after their timestamp.

**From literature:** about 4–8 reorgs per day post-merge, about 99.7% of them depth 1.

**Reorg algorithm:**
- **Steady state:** a new head whose parent is our tip is fetched by hash and appended.
- **Gap:** fetch the missing headers, check that the hash chain links up, then fetch the range and verify each log's block hash.
- **Parent mismatch:** walk back to the common ancestor (capped at 64 blocks and never below the finalized cursor), rewind, and re-ingest. If no ancestor is found, wipe the tail and serve finalized.
- **Lagging head:** a lower head from a lagging node is ignored.
- **Finalization:** when the follower finalizes blocks, it prunes the tail rows at or below the new cursor in the same transaction, so nothing is double counted.

**Cost:** polling aligned to slots (`latest` at +2.5 s, with retries) costs about 1.2 header calls plus 1 `getLogs` per block. That is about 15.8k calls per day, about **+9.5M credits/month**. Polling `finalized` only at epoch boundaries saves about 3.5M. The total becomes **about 57M/month (about 61M with per-block balances)**, within the 80M Build plan.

**API:**
- `?finality=latest|finalized`, defaulting to `latest`.
- `blockHeight` becomes the tail tip; `finalizedBlockHeight`/`finalizedBlockHash` are added.
- Per token, unfinalized sent and received counts are reported.
- An unhealthy tail degrades to finalized and reports `tailError`.
- Balances are read at the tip hash.

**Implementation:**
- **Codex (gpt-5.6-sol):** built most of the brief: schema and migration 0003, compare-and-append, prune plus hash check inside the follower commit, a single repeatable-read snapshot for base and tail, the merge in the service, the reorg walk, and 10 tests. It **ran out of usage** before reporting.
- **Opus audit:** checked the work item by item against the brief and **fixed 9 bugs**, each with a test:
  1. A lagging head exactly at the cursor height wiped the whole tail (`<` instead of `<=`).
  2. A header that changed mid catch-up, caused by Quicknode's load-balanced nodes, was counted as a depth-0 reorg. It is now retried.
  3. All three poll offsets fired every slot (about 3 header calls per block live). The remaining offsets are now skipped once the slot's block is in.
  4. Epoch-boundary finalized polling was missing. `follower.kick()` was added; the 30 s poll became a 390 s fallback.
  5. Balances were refreshed on every tail block. Now only on a reorg, plus the 60 s sweep.
  6. Range catch-up did not split on too-large or timeout.
  7. `eth_call` on a reorged hash ("header not found") was treated as fatal. It is now transient.
  8. `balanceBlockHash` appeared even with the tail disabled.
  9. Found live: after an outage the tail fetched up to 500 headers that the follower then pruned. The tail now waits and wakes the follower first.
- Tests went from 86 to **93**, 16 of them in `tail.test.ts`. Commits: `e27e32d`…`e7e6ede`.

**Live check (Quicknode, Docker Postgres):**

| What | Result |
|---|---|
| Tail lag vs `latest` | **0 blocks** in every sample, one block per slot |
| Rebuild on start | 72 headers plus 1 range `getLogs` for 73 blocks (37.8k transfer rows) |
| Steady state | about 1.26 header calls and exactly 1 `getLogs` per block |
| Follower | advanced on the epoch kick (one epoch, 32 blocks) |
| `?balances=true` (latest) | `blockHeight` = tip, `finalizedBlockHeight` present, 109 tokens, 30 ms. Repeats cost 0 RPC calls |
| `?finality=finalized` | the old response shape. An invalid value gets 400 |
| Network outage mid-run (ENOTFOUND) | the tail went unhealthy (lag 487) and served finalized with `tailError`, as designed |
| Real reorg | none in the window (expected about 4–8 per day) |

**Cost, re-measured:** about **53–54M credits/month** steady state for 5k users, within the Build plan's 80M.

**Open items:**
- No real reorg observed live yet; reorgs are covered by tests only.
- The 5k-account acceptance test took 130 s on a loaded machine (load average 15). The same test on the pre-tail commit was even slower, so this is the environment, not a regression.
- Balances are not refreshed per block for pairs a new block touched; the 60 s sweep remains.
- A non-leader replica doesn't report live tail health. *(Fixed in §14: freshness now comes from the shared tip.)*

---

## 14. Audit and hardening pass (`feat/improve`, 2026-10-15 00:00–01:15)

**How:**
- **Audit:** the `/improve` skill ran three read-only audits in parallel:
  - Codex gpt-5.6-sol on correctness and performance.
  - Opus on correctness, concurrency and reorgs.
  - Opus on performance at 5k users, security and test gaps.
- **Vetting:** every finding was re-checked against the code. The confirmed ones became five self-contained plans in `plans/` (index: `plans/README.md`).
- **Execution:** each plan ran in its own git worktree, with the executor picked by complexity: Sonnet for the small ones, sol for the mechanical ones, Opus for the riskiest.
- **Review:** every diff was reviewed before merging (done criteria re-run, scope checked, tests read). Two plans needed one revision round each.
- **Merge:** all five were merged on an integration branch, then fast-forwarded into `feat/improve` (`850f3ed`).

| Plan | Executor | Problem | Fix |
|---|---|---|---|
| 001 | Sonnet | Two simultaneous first requests for one address → **500** (duplicate key on `tracked_accounts`). `/v1/constructor/…` → 500 (prototype lookup). The pg pool waited **forever** when exhausted | `ON CONFLICT DO NOTHING` + re-read the winner's target (the handoff is unchanged). `Object.hasOwn` network lookup → 404. `DB_POOL_MAX` / `DB_CONNECT_TIMEOUT_MS` → 503 "Database busy" |
| 002 | sol | A sign-up during a follower range made it **re-download the whole range** (up to 500 blocks, billed). Steady sign-ups could stall the cursor and 503 everyone. Logs outside the requested range were accepted. The leader flag stayed `true` after the lock's DB session died | Skipped logs are kept and re-folded for newly tracked accounts (no refetch, up to 8 refolds). Out-of-range logs are a transient error. `leader` = flag ∧ `lock.held()`, false when acquire throws |
| 003 | Opus (1 revision) | Non-leader processes always reported the tail **healthy**, so a frozen tail was served as fresh `latest`. Rewind/wipe were unfenced, so two leaders during failover could delete each other's rows. The tail grew **without bound** if finality stalled. The restart re-validation never ran (leader unknown at `load()`) | `TAIL_MAX_TIP_AGE_MS` (60 s): every process judges freshness by the shared tip block's age. `lastProgressAt` = tip block time. Rewind/wipe compare the locked tip hash (`TailConflictError`). `TAIL_MAX_AHEAD_BLOCKS` (1024) caps the tail and degrades to finalized. The first leader tick validates the stored tip |
| 004 | sol (1 revision) | After a reorg to a **shorter** chain, balances read at the orphaned block were never replaced. One failed multicall batch **discarded the whole sweep**. A first read re-fetched **all** tokens when one was missing. A malformed outer `aggregate3` result → 500, no failover | A reorg-kicked refresh may overwrite newer snapshots (`replaceNewer`). Per-batch results; failed-batch rows are marked `transient` and not saved, so old good balances survive. Revision: the first version overwrote them with errors. Only missing tokens are read. Outer decode moved into the RPC client's `validate`, so it retries and fails over |
| 005 | Sonnet | `verifyOwnership` had no cap, cache or single-flight (~83 calls/s at 5k users, 4× the RPS cap). Queued RPC work still ran after its request had timed out. NFT grouping was O(collections × items) | `MAX_VERIFY_OWNERSHIP_ITEMS` (2000, then `ownerStatus: skipped`). Shared lookup per (account, block hash). Limiter `deadlineAt` drops expired queued jobs. Grouping via a Map |

**Tests:** 93 → **115**, all passing; typecheck clean. One caveat: PGlite has one connection, so the race fixes are covered by contract tests and reasoning, not a reproduced race.

**Live run** (Sonnet agent, Quicknode + Docker Postgres, about 450 RPC calls in total):

| Check | Result |
|---|---|
| Startup on a 17 h-old cursor | Listening in < 12 s. The follower caught up 2,069 blocks in about 4 min (175 calls); 503 meanwhile, as designed |
| `/erc20?balances=true` (Vitalik) | 158 tokens, all balances ok, `finality: latest`, lag 0, `lastProgressAt` = tip time (9 s old). A repeat costs 0 calls |
| `?finality=finalized` | Old shape, 85 blocks behind |
| `/v1/constructor/…`, `?finality=bogus` | 404, 400 |
| `/erc721?verifyOwnership=true` | 68 items with 1 `eth_call`. A second request 2 s later: 0 calls (shared lookup) |
| 3 simultaneous first requests (new address) | 202 ×3, no 500. Backfill done in about 3 min, then 200 with 0 calls |
| 5 min watch | Tail lag 0 in 11/11 samples, healthy, 0 reorgs. The follower advanced one epoch on schedule. About 1.5 header calls + 1 `getLogs` per block (was 1.26; small sample, to re-measure) |

**Second review round (Codex, verified by Opus).** Codex found three more correctness bugs after the merge. Opus confirmed each one with a failing test (`test/regressions-audit.test.ts`) and then fixed it:

| Bug | Origin | Fix |
|---|---|---|
| A is tracked; B registers while the follower processes a range that contains an A→B transfer. That log is not "skipped" (A is known), so plan 002's refold never revisited it. The cursor advances and B **permanently misses** the transfer | **Introduced by plan 002** (missed in review); the old refetch handled it | The follower keeps every log it already folded and replays it for newly tracked endpoints (no double count: they were unknown at first fold). Tests: B sends and receives, a B self-transfer, two accounts tracked in different refold rounds; still one `getLogs` |
| A late request-path balance read from an orphaned fork overwrites the corrected balance **at the same height** (writes compared only block number) | Pre-existing | A same-height write must carry the same block hash. Only the reorg-kicked refresh at the canonical tip overwrites unconditionally |
| A failed finalized-head poll returned the cached head with `stale: true`, but the follower ignored it and cleared `lastError`, so it reported **healthy for up to ~20 min** | Pre-existing | `refreshFailed` is set on the observation; the follower ORs `head.stale` into `stale` and sets `lastError` ("Finalized head refresh failed; serving cached head") |

Tests: 115 → **120**, all passing. Lesson: a refold optimisation must replay *every* retained event for new accounts, not only the ones it skipped. A test with a transfer **between** a known and a newly tracked account would have caught it in the first review.

**Deliberately not planned** (real, but needs a product decision or is larger; listed in `plans/README.md`):
- **Backfill credit budget or denylist:** a flood of random addresses could burn the month's credits in about 2 days, and the zero address is the worst case.
- **Account lifecycle:** untracking idle accounts.
- **Cross-replica backfills:** a per-account lease, so two processes don't backfill the same account.
- **Follower catch-up memory:** 500-block unfiltered responses ≈ 225 MB of JSON.
- **Response size:** pagination for heavy accounts.
- ~~**Balance sweep cost:** refreshing only touched pairs.~~ Done in §15.
- **Real concurrency tests:** a multi-connection Postgres integration suite.

---

## 15. Budget: change-driven balance refresh (2026-10-15)

**Problem.** The steady-state bill for 5k users × 10 tokens was about 53M of the 80M credits/month, and the balance worker was most of it (the 20-credit call rate is the only lever, so the number of calls had to drop):

| Item (before) | Credits/month (estimate) |
|---|---|
| Balances: every pair every 60 s (50k pairs / 1000 per Multicall = 50 calls/min) | about 43M |
| Tail (`latest` header + `getLogs` per block) | about 9–11M |
| Follower, head polling | under 0.5M |
| **Total** | **about 53–54M** |

**Levers considered:**
1. **Change-driven refresh.** Re-read a pair only when something may have changed. Built.
2. **Bigger or compact batches.** 2000 per batch, or `aggregate3` replaced by a state-override balance-checker contract. Not done: needs a measurement of the Quicknode `eth_call` gas cap first.
3. **Skip zero balances.** A zero balance only becomes non-zero through a Transfer, which lever 1 already catches. Built with lever 1.
4. **HyperSync backfills.** Would cut the one-off cold backfill cost, but blocked on the Binance stream hang (§12).
5. **WebSocket heads.** Would replace header polling, but how Quicknode bills subscriptions is unverified.
6. **Abuse or admission cap.** A cap on newly tracked accounts and backfills; a product decision, not done.
7. **Self-hosted node.** Comparison point only: about $200–400/month flat (estimate), against Quicknode Build at $49 for 80M credits.

**What was built (plan 006, levers 1+3; Opus, `873f91e`, merged in `a4e98d6`).** Each 60 s cycle the worker re-reads only pairs with no snapshot, pairs with a Transfer after their snapshot's block (`store.erc20RefreshState`), `BALANCE_ALWAYS_REFRESH_TOKENS` (WETH, stETH), pairs whose last read failed transiently, and pairs whose snapshot block is no longer canonical. Every `BALANCE_FULL_SWEEP_MS` (1 h) it re-reads all non-zero pairs; a reorg kick re-reads everything. Responses gained per-token `balanceCurrent` and a top-level `balancesCurrent`; `/health` reports `candidates`, `read` and `fullSweep`.

**Review revision (1 round).** Processes that never received a reorg kick (non-leader replicas, whose tail does not run) kept snapshots of an orphaned block, because the change test looked only at Transfers. Orphaned snapshots are now detected (block no longer canonical), re-read and saved with `replaceNewer`.

**Soundness assumption.** Balances change only through `Transfer`, except for the always-refresh list. Anything else (unlisted rebasing tokens, admin mints without events) can be stale, bounded by the hourly sweep, and never fixed for an `ok` zero balance. `balanceCurrent` says "no Transfer since the snapshot", not "verified on chain", so it can be true for such a balance.

**Cost, before vs after** (all estimates; 5k users × 10 tokens, about 1% of pairs touched per minute):

| Item | Before | After |
|---|---|---|
| Balances | about 43M (50 calls/min) | about 1.6M (about 1.8 calls/min); about 6M if every user holds WETH/stETH |
| Tail | about 9–11M | about 9–11M |
| Follower and heads | under 0.5M | under 0.5M |
| **Total steady state** | **about 53–54M** | **about 12–17M** |

Cold backfill is unchanged: about 172 calls per new account, one-off.

**Tests:** 120 → **130**. The call rate is modelled, not measured live (next items: lever 2 gas cap, live re-measure).

---

## 16. SQD squid-sdk RPC source, revisited (2026-10-15)

**Trigger.** Viktor found a way to run the new Squid SDK on plain RPC, with no Portal (`../disquid`, uncommitted working tree on top of `207fd0b`): `EvmRpcDataSourceBuilder` from `@subsquid/squid-sdk/evm/rpc` 0.6.2, `.setRpc({url, network: 'ethereum-mainnet'})`, `addLog({address: [USDC], topic0: [Transfer]})`, `run()` from `@subsquid/batch-processor` into `TypeormDatabase({supportHotBlocks: true})`. The handler only logs. §11 had rejected the older `@subsquid/evm-processor` 1.30.1 RPC mode; this is a rewritten stack (`evm-rpc` 0.2.0, `evm-normalization` 0.1.0), so Sonnet re-checked it against the installed source. Nothing was run.

**Round 1: fit (Sonnet).** Not adopted, not even partially:

| Point | Finding |
|---|---|
| Data coverage | Transfer logs: yes (wildcard address plus topic0, split ERC-20/721 by topic count, as we do). Balances: no block-pinned state reads, so `src/balances/reader.ts` stays. Native transfers need traces, which are out of scope |
| Discovery | We have no address set (global index), so SQD's static `where` filters do not matter |
| RPC cost | `eth_getBlockByNumber(n, true)` for every block, plus `eth_getLogs({fromBlock, toBlock})` with no topics per stride of 5 blocks |
| Reorgs | `hot_change_log` rollback is roughly our tail journal. It does not give the fenced leader, prune-on-finalize, repair revision or 503 readiness |
| Storage | TypeORM plus its own status tables beside drizzle. Using SQD only as a `DataSource` gives up the hot-block store, its main addition |
| Validation | Stronger than ours on Ethereum: block hash, tx root, tx sender, receipts root and **logs bloom**. The bloom check is what would catch silently omitted logs (REPORT "Repair runner") |

**Round 2: can a topic filter stop the full-block fetch?** Viktor: the stub does not matter, we can iterate on it. Not by configuration:
- `squid-sdk/src/evm/rpc/source/data-source.ts` hardcodes `transactions: true`, because `evm-normalization` `mapRpcBlock` asserts every transaction is a full object. `evm-rpc`'s `Rpc.getBlockBatch` already honours `transactions: false` (header-only `eth_getBlockByNumber(n, false)`).
- `evm-rpc/src/rpc.ts` `addLogs` sends only `fromBlock`/`toBlock`. `DataRequest` carries booleans only, and filtering is client-side (`squid-sdk/src/evm/rpc/filter/filter.ts`).
- The `eth_getLogs` range is one stride. `strideSize` is configurable, but an unfiltered 500-block call will hit provider limits, and `addLogs` has no split on too-large.
- `verifyLogsBloom` needs every log of the block, so it cannot be combined with a filter. It can be turned off with `setRpc({rpc: {verifyLogsBloom: false}})`; the overrides are spread over the preset.
- `includeAllBlocks` only affects output. Every block number is fetched either way.

**Found while writing the plan (Opus).** `addLogs` groups the returned logs by `blockHash` and silently drops any log whose hash is not one of the batch's headers. Behind a load-balanced provider (§13: varying `x-node-id`), a by-number `getLogs` can return another fork's logs, and with the bloom check off that block would just look empty. Our follower rejects such a response instead. The patch must add the same membership check.

**Call counts for the ~265k-block window** (derived from code, not measured):

| Configuration | Block calls | `eth_getLogs` |
|---|---|---|
| SQD unpatched (stride 5–20) | about 265k, full transactions | about 13–53k, no topic filter |
| SQD patched (header-only, topic0, stride 500) | about 265k headers | about 530 plus splits |
| i-see-u follower | about 265k headers (concurrency 8) | about 530 (`ETH_FOLLOWER_MAX_RANGE=500`) plus splits |

**Decision.** i-see-u keeps its own follower and tail. The patch is specified as [plans/007](plans/007-sqd-rpc-log-filter-pushdown.md) for a Codex worker in `../disquid`, with a counting proxy to measure calls and bytes and a parity check against the unpatched source. If it measures at parity, the patched source (or the Portal, if RPC-only is not a hard rule) is a candidate for the pending bootstrap, appended through `appendRange`. The logs-bloom idea can also be ported into `src/chains/evm/logs.ts` without the framework, but only with unfiltered per-block logs or receipts.

---

## Mistakes and reversals (the interesting part)

| What happened | What we changed |
|---|---|
| Recommended viem, then didn't use it, and the docs implied we did | Integrated viem honestly. Our policy stays where viem's would cost money (`-32005` retries) |
| Hand-wrote an `.env` parser | `process.loadEnvFile()` |
| Global ingestion in REPORT rev 2, then built per-account | Final design: tracked accounts plus a follower. Global ingestion is documented as the alternative (REPORT §1.1) |
| "Rescan the tail per request" plan | Wrong for scale (2 calls per request). Replaced by the shared follower |
| "No Transfer means the balance is unchanged" | False (WETH, rebasing). Balances are re-read on a cadence and carry `balanceBlock`. The cost estimate went from 19M to about 51M/month |
| Reorg rollback by deleting pairs above the fork | Invalid (it cannot restore counts or NFT ownership). Replaced by finalized-only |
| Overstated discovery cost ("10–15k calls/min") | Corrected to about 1.6k/min. Balances (about 5k/min) were the real problem |
| Per-transfer journal (720 MB for one wallet) | Per-token aggregates |
| 5 MB adaptive threshold | It cost 2.6× the calls for no gain, so 50 MB |
| Benchmarks under a changing provider | Re-measured, and every table states the provider conditions |
| `pkill -f src/server.ts` in benchmarks | Killed Viktor's server once. Narrow patterns only since |
| A 46 s cold request in Viktor's own test | Contention with a parallel benchmark on the same endpoint, plus a wrong test address. Re-measured at 3.3 s |

---

## Where we stand (against ASSIGNMENT.md)

| Requirement | Status | Notes |
|---|---|---|
| ETH mainnet from block 25,933,634 to now, "almost realtime" | ✅ | Realtime tail with reorg handling (§13): 0 blocks behind `latest` live, about 3 s after block time. `?finality=finalized` remains available |
| Technology choice with reasons | ✅ | REPORT §6 |
| REST endpoint: ERC-20 tokens the address interacted with | ✅ | `GET /v1/ethereum/accounts/{addr}/erc20` |
| Balance parameter | ✅ | `?balances=true`. Each token has its own `balanceBlock` |
| Block height the data is valid for | ✅ | `blockHeight` + `blockHash` (the tail tip with `finality=latest`, the follower cursor with `finalized`), `finalizedBlockHeight`, `balancesAtBlockHeight` |
| Modular (adapter-style) | ✅ / ⚠️ | `ChainAdapter`, `Store` and `RpcClient` are interfaces, and a new EVM chain is a config entry. A per-account `AccountLogSource` (RPC, HyperSync, with fallback) is in progress (§12) |
| Scales to 5k users polling every minute | ✅ / ⚠️ | 0 RPC calls per warm request. A fake-RPC acceptance test covers 5k accounts. No live HTTP load test. Multi-replica hardened in §14 (race-safe tracking, truthful leadership, shared tail freshness, fenced tail writes); still no cross-replica backfill dedupe |
| Unreliable RPC: slow, errors, rate limits, lagging | ✅ | RpcClient classification, backoff, breaker, failover, validate. Follower lag, `stale` and 503 |
| README and documented limits | ✅ | README "Limits", REPORT §7 |
| Not a full indexer | ✅ | Only `Transfer` logs, only for tracked accounts, aggregated per (account, token) |
| RPC-efficient (paid per request) | ✅ | About 12–17M credits/month steady state for 5k users at 10 tokens each (estimate; Build plan has 80M, was about 53–54M before the change-driven balance refresh, §15). Balance cost now scales with Transfer activity. Cold backfill is 172 calls per account |
| BONUS: ERC-721 discovery and endpoint | ✅ | `/erc721`: collections plus token IDs, `?verifyOwnership=true` via `ownerOf` |
| Own repository, shared | ⚠️ | Pushed (private) to github.com/vikiival/i-see-u, including `feat/improve`; collaborators not added yet |
| Presentation (30 min + Q&A, Thursday) | ❌ | To do. This log and REPORT are the basis |

## Next

1. **Share the repo** (pushed to github.com/vikiival/i-see-u, private: add Jakub and Tali as collaborators).
2. **Observe a real reorg live** (about 4–8 per day), or replay one against a fork, to back the tail tests with a real event.
3. **Diagnose the HyperSync Binance stream hang (§12)** before turning `LOG_SOURCE=auto` on by default. Also check the Linux napi build.
4. **Presentation:** the arc above, the chunk-size table, the cost table, and the path from finalized-only to the realtime tail with reorgs.
5. **Tuning:** raise `RPC_MAX_RPS` toward the plan's 50 (normal cold scan about 3.5 s). Verify credits per call and per batch on the Quicknode dashboard.
6. **Known limits to decide on:** multi-replica state (active set, balance-worker leader), untracking idle accounts, holdings from before the start block, spam flags, and native ETH.
7. **Backfill admission and credit budget** for arbitrary addresses: the largest remaining cost and DoS lever (§14).
8. **Re-measure tail cost per block** over a longer window (1.5 vs 1.26 header calls).
9. **Lever 2:** measure the Quicknode `eth_call` gas cap for a state-override balance checker / 2000 per batch.
10. **Live re-measure of the balance refresh call rate** (the 1.8 calls/min figure is an estimate).
11. **Run plan 007 (Codex, `../disquid`)**: header-only blocks plus topic-filtered `eth_getLogs` in the SQD RPC source; measure calls and bytes against the follower over the same range (§16).
