# Token discovery PoC: technology and architecture proposal

Status: proposal, revision 2. What is implemented is in section 0.1.

What changed since revision 1:
- The authors clarified that "full indexer" means walking blocks, transactions and native transfers (Blockbook-style account discovery). An index of ERC-20/721 `Transfer` logs only is therefore in scope.
- They looked for an off-the-shelf service for token discovery over plain JSON-RPC and found none, which is why this is a PoC.
- Decision: `eth_getLogs` over Quicknode is the primary source. SQD and Envio are backup sources behind the same adapter interface.
- Numbers below were re-measured on mainnet on 2026-10-14. Values marked *(estimate)* are derived, not measured.

## 0. Scope decisions for the PoC (2026-10-14)

| Question | Decision |
|---|---|
| Tokens held before block 25,933,634 and untouched since | Out of scope. Documented as a limit |
| Spam tokens | Return everything. No filtering yet |
| Freshness | A few seconds behind head is acceptable, so `confirmationDepth` of about 2 blocks is fine |
| What counts as "interacted" | `Transfer` in or out. Tokens that change balances without `Transfer` are a documented limit |
| Native ETH balance | Out of scope |
| ERC-721 response (bonus) | Not specified by the authors. Proposal: return contracts plus token IDs seen in logs, with optional `ownerOf` verification through Multicall (`?verifyOwnership=true`) |

## 0.1 Implementation status (what the code does today)

The PoC was built in phases, so it differs from the target design in sections 1–5:

| Area | Target design (below) | Implemented now |
|---|---|---|
| Discovery | Global ingestion of all Transfer logs | **Hybrid:** a shared finalized follower reads every `Transfer` log once (unfiltered `eth_getLogs`, ≤ 500-block ranges, one call per 32-block epoch at the head) and applies it for **tracked** accounts only. A new account is backfilled once with address-filtered `getLogs` up to the follower cursor at tracking time (handoff target C); the follower owns every block above C. Not a full global index of all holders |
| Storage | Postgres | **PostgreSQL 18** via Drizzle ORM; one row per `(account, token)`, no per-transfer rows; global `follower_cursor`, `tracked_accounts(backfill_target)`, `balance_snapshots`; generated migrations run at startup |
| Consistency | Confirmation depth plus reorg rollback | **Finalized only (decided):** only finalized blocks are persisted, cursors store the block hash. Reorg-safe by construction, no realtime tail, about 13 min behind the tip |
| Single writer | Advisory lock | **Session advisory lock** on a dedicated connection for the follower's lifetime; commits also lock the cursor row and refuse non-contiguous ranges |
| Balances | Cached and invalidated by the follower | **Shared balance worker:** every minute and on each follower advance, all active (account, token) pairs are re-read at the follower cursor (by hash) in 1,000-call Multicall3 batches and stored with their block. A request serves the snapshot (`balanceBlock` per token); only an account without a snapshot is read on the request path (single-flight) |
| Request path | DB only | **DB only once backfilled:** zero RPC per request; cold backfills (202 `syncing` past a 10 s deadline), first balance reads and `verifyOwnership` go through bounded limiters (503 + `Retry-After` on overload) |
| viem / RPC boundary | viem plus a resilient RPC pool | **viem on top of `RpcClient`:** viem provides types, standard ABIs, topic/calldata encoding, strict decoding, and block/call actions. `RpcClient` alone performs HTTP and owns retries, endpoint failover, circuit breakers, unsupported-method memory, error classification, result validation, rate limiting, and per-attempt stats. viem retries and call batching are disabled. Hybrid transport: viem `http()` (one per endpoint, `retryCount: 0`, no batching) does request building, timeouts and response parsing, while `RpcClient` keeps all retry/failover/classification policy, because viem's would retry `-32005` range errors instead of splitting, miss Quicknode `-32010`, not fail over on null results, lack a breaker, ignore a Retry-After cap and hide retried attempts from billing stats; viem's `fallback` is not used |

Measured live with the follower (Quicknode, 2026-10-14):
- Follower advance of one epoch (32 blocks): **1 `eth_getLogs`**, plus 2 `eth_getBlockByNumber` (finalized + latest) per 30 s poll.
- Balance refresh of 109 active pairs: **1 `eth_call`**, at the new cursor right after the advance (and every 60 s).
- 10 warm requests (ERC-20 with balances, ERC-721): **0 RPC calls**, 7–12 ms each.
- Cold backfill of an active account over 257,541 blocks: 172 calls, about 26 s (two 202 `syncing` responses first); 109 tokens.
- Acceptance test (fake RPC): 5k tracked accounts × 10 tokens → 1 `getLogs` for the epoch, 50 `eth_call` for 50k pairs, then 0 calls for 5k requests.

Measured live on Quicknode before the follower (2026-10-14):
- Cold scan of an active account: 2.4–2.6 s and 52 calls. Warm scan: 13–19 ms.
- Binance hot wallet (about 2.5M transfers in the window): 81 s cold, which is mostly client-side JSON parsing.
- A topic0-only `getLogs` over 1,000 blocks: 701,764 logs, 446 MB, 4 s. There was no result cap on the demo endpoint.

Other measurements (public RPCs, SQD, HyperSync):
- **Multicall3:** 2,000 `balanceOf` calls in one call is safe, at about 6.8k gas each. `eth_estimateGas` is capped at 16.7M on public nodes. Bodies over about 1.3–1.7 MB get HTTP 413.
- **Provider result caps:** 10k (mevblocker) and 20k (tenderly). Both return a suggested sub-range, which the code parses.
- **SQD Portal:** head lag 0 blocks. Topic values must be lowercase and 32-byte padded; other formats return 0 logs silently. HTTP 529 "overloaded" is frequent on the free tier.
- **HyperSync:** an API token is required (HTTP 401 without one).
- **Distinct `(holder, token)` pairs for global ingestion:** about 15–40M over the window, roughly 1.5–6 GB in Postgres *(extrapolated from 300-block samples)*.

Corrections from a documentation review:
- Quicknode bills **each call inside a JSON-RPC batch**, and every WebSocket notification.
- Quicknode Streams costs about 4.3M credits per month on mainnet, about the same as our follower.
- Historical-balance APIs exist for major chains (GoldRush, Ankr), but they are proprietary indexers that new networks lack. That is the gap this PoC targets.

## 1. Recommendation

Build a **log-only Transfer indexer** in **TypeScript (Node 22) + viem + Fastify + Postgres**, with an in-process cache.

1. **Global ingestion.** One worker per network reads every `Transfer` log (topic0 only) from block 25,933,634 to head and upserts a compact `(holder, token)` set for **all** accounts. No blocks, transactions, receipts, traces or native transfers are stored.
2. **Near-realtime follower.** After backfill, the same worker follows the head block by block, with reorg handling.
3. **Balances via Multicall3**, pinned to the indexed block. Balances are cached per `(holder, token)` and invalidated by the Transfer logs the follower already sees, so a balance is refetched only when it can have changed.
4. **Fallback sources.** SQD Portal and Envio HyperSync implement the same `LogSource` interface and take over log ingestion (fast backfill, or failover when Quicknode is down). Balances always come from JSON-RPC.

Key point for the presentation: **RPC cost depends on the block rate, not on the number of users or how often they poll.**

### Why global ingestion instead of the watched-only design in revision 1

- **The follower downloads every Transfer log either way.** Filtering by 5k watched addresses in `topics` is not practical (providers cap OR-list sizes), so even the watched-only design pays for the full log stream at the head.
- **The only savings of watched-only were backfill and storage.** Global backfill is about 0.5M credits once (section 4), well under 1% of the cheapest paid plan, and storage is a few GB.
- **Global is simpler and gives better answers.** Every address is answerable immediately: no 52-call backfill per new address, no `sync: in_progress` state, and no race between the backfill and the follower.
- **It stays clear of "full indexer".** It never touches blocks, transactions or native transfers, and it stores one row per `(holder, token)` pair, not per transfer.

## 2. Measured facts

Mainnet measurements on 2026-10-14 via public RPC (dRPC):

- **Window:** head was 26,189,8xx. Block 25,933,634 was mined on 2026-09-08. That is **about 256k blocks**, growing by about 7,200 per day.
- **Transfer log volume:** 10 samples of 10 blocks across the window gave **305–999 logs per block, typically about 450**, and **220–620 KB of JSON per block, typically about 280 KB**.
- **ERC-20 vs ERC-721:** same topic0, `Transfer(address,address,uint256)`. ERC-20 logs have 3 topics, ERC-721 logs have 4 (tokenId is indexed). Every sampled log was one of the two. The ERC-721 share was 0.3–9%.
- **Distinct `(account, token)` pairs:** 380–890 per block within a 10-block window. The total across the window is not measured; repeat pairs dedupe heavily.
- **Public RPCs are unreliable in practice:**
  - publicnode rejected `eth_getLogs` at the start block ("Archive requests require a personal token").
  - It also rejected topic0-only queries at head ("Please specify an address").
  - 1rpc returned "method not available" for recent blocks.
  - merkle.io returned -32005 "Rate limit exceeded".
  - llamarpc returned HTTP 525.

  This is useful evidence for the resilience section of the presentation.

Quicknode, verified from their pages:

- **Credits:** Ethereum standard methods cost **20 credits each**, including `eth_getLogs` and `eth_call`. Trace/debug calls are 2x, and specialised calls are 4x. ([credits](https://www.quicknode.com/api-credits))
- **Not stated on that page:** whether a JSON-RPC batch is billed per inner call, whether WebSocket messages are billed, and whether response size affects cost.
- **Plans:** Build $49, 80M credits, 50 RPS. Accelerate $249, 450M credits, 125 RPS. Overage is $0.50–0.62 per 1M credits. Archive data is included on every plan. ([pricing](https://www.quicknode.com/pricing))
- **getLogs range:** up to 10,000 blocks per call on paid plans, 5 on the Free Trial. The limit is on range, not result count. ([support](https://support.quicknode.com/hc/en-us/articles/10258449939473-Understanding-the-10-000-Block-Range-Limit-for-querying-Logs-and-Events))

Implication: topic0-only queries must use **small ranges**. 10 blocks is about 4,500 logs and 2.8 MB. A 10k-block range would be about 4.5M logs, so it is useless here.

## 3. Third-party data sources

No source fully replaces this service. The authors' finding holds: none returns "tokens this address interacted with, plus balances at block N" over plain JSON-RPC.

| Source | What it gives | Role |
|---|---|---|
| Quicknode JSON-RPC | `eth_getLogs`, `eth_call`, archive data | **Primary** for logs and balances |
| SQD Portal | HTTP archive stream. `logs[]` filters on `address` and `topic0..3` (AND within an object, OR across objects). `/stream` includes unfinalized blocks, `/finalized-stream` is finalized only. ([docs](https://docs.sqd.dev/en/portal/evm/api)) | **Backup `LogSource`**: fast bulk backfill, failover |
| Envio HyperSync / HyperRPC | High-throughput log queries. HyperRPC exposes JSON-RPC, so it could join the RPC fallback pool. API token reportedly required *(not verified)* | **Backup `LogSource`** |
| Quicknode Streams | Managed push of block data with backfill and reorg handling | Production alternative. Mention only |
| Goldsky Turbo | Managed pipelines into Postgres or Kafka | Not used. A hosted indexer, outside the premise |

Open on SQD: whether topic filters need lowercase, 32-byte-padded values, and the real head lag for `ethereum-mainnet`.

## 4. Cost (Quicknode credits)

Assumptions: 5k users, about 10 tokens each, 20 credits per call, 7,200 blocks per day. Steady-state rows are what the implemented follower and balance worker do (measured call counts in section 0.1).

| Item | Calls | Credits |
|---|---|---|
| Cold backfill per new account (address-filtered, 3k-block chunks over 257k blocks; measured) | about 172 once per account; 5k accounts ≈ 860k once | **about 17M once** |
| Follower, 1 unfiltered `getLogs` per finalized epoch (32 blocks; measured) | about 225/day | about 0.14M/month |
| Head polling, `finalized` plus `latest` every 30 s (`FOLLOWER_POLL_MS`) | about 5.8k/day | about 3.5M/month |
| Balance worker: 50k active pairs at 1,000 per Multicall, so 50 calls per refresh, every 60 s plus once per follower advance (the timer restarts at each advance, about +10%) | about 79k/day | about 47M/month |
| First balance read per newly active account | 1 per account activation | small |
| Request path for backfilled accounts (measured) | 0 | 0 |
| **Total steady state** | | **about 51M/month, fits Build (80M)** |

*(Correction to revision 2:* the earlier 19M total assumed only "dirty" pairs need re-reading. That assumption is invalid (section 5, balance path), so balances are re-read on a cadence. The balance worker is the dominant cost. Refreshing only accounts polled in the last few minutes (`ACTIVE_ACCOUNT_TTL_MS`, implemented), or every 2 minutes, scales it down linearly. Refreshing every block instead (12 s) would cost about 215M/month, which needs Accelerate.)

For comparison, one Multicall per user request costs 5k × 60 × 24 × 30 × 20 = **4.3B credits/month**, and the pre-follower per-account rescan on each finalized advance added about 1.6k `getLogs` per minute. Batching across accounts is what makes the difference: 5,000 requests a minute become 50 calls a minute.

Bandwidth: about 14 MB per epoch at head, about 3 GB/day *(estimate)*. Credits don't depend on response size per Quicknode's page, but this is worth confirming.

Storage: one row per (tracked account, token) plus one balance snapshot per active pair; for 5k accounts × 10 tokens, about 100k rows *(estimate)*.

## 5. Design

### Ingestion worker

- **Backfill** from `start_block` in 10-block chunks, adapting the size: grow when a response is small, halve on a timeout or "too large".
- **Concurrency** of about 8–16 to stay under the plan's RPS. At 1–2 s per call, the backfill takes roughly one hour *(estimate)*.
- **Each chunk is one Postgres transaction:** upsert pairs, then advance the cursor. A crash replays the chunk, and upserts make that idempotent.
- **At the head:** poll `eth_blockNumber` every 12 s. For each new block, call `getLogs({ blockHash })` (EIP-234) so logs are tied to a specific block hash.
- **Finalized-only, no rollback (decision 2026-10-14).** Only finalized blocks are ingested. They cannot reorg, so no rollback machinery is needed. Responses lag the tip by about 64 blocks (about 13 min), which the authors accepted for the PoC.
- *(Correction to revision 2:* the earlier "delete pairs with `first_block > F`" rollback was not valid anyway. It cannot restore counts, last-seen blocks or NFT ownership on rows that already existed. Finalized-only removes the problem.)
- **The follower steps by epoch.** `finalized` advances by 32 blocks about every 6.4 min, so one ranged `getLogs` per epoch (about 22k logs, about 14 MB) replaces one call per block. That is about 225 calls a day instead of 7,200.
- **Range size is bounded by payload, not credits.** Credits are 20 per call at any range. Address-filtered queries use the full 10,000 blocks. Unfiltered Transfer queries return about 450 KB per block, so 10k blocks would be about 4.5 GB in one response, beyond a single JS string (about 0.5–1 GB). Unfiltered ranges are therefore capped at about 500–1,000 blocks (1,000 measured at 446 MB in 4 s).
- **Single worker per network,** enforced with a Postgres advisory lock.
- **Classification by topic count:** 3 topics is ERC-20, 4 is ERC-721. Skip the zero address as a *holder* (never skip the log, or every mint is lost). Optionally confirm ERC-721 contracts with ERC-165 `supportsInterface(0x80ac58cd)`.

### Balance path

- When `balances=true`, read the address's tokens from Postgres and pin to `blockHeight = indexed cursor`.
- **Cache per `(holder, token)` keyed by the block hash it was read at, and report that block.** *(Correction to revision 2:* "no Transfer touched the pair, so a balance read at N is still valid at M" is **false**. WETH `deposit`/`withdraw`, rebasing tokens and admin actions change balances without a Transfer. A cached balance is never relabelled as valid at a newer block. Each token carries its own `balanceBlock`, or the whole snapshot is re-read at one block.)
- **Shared balance worker for active accounts.** It batches `(account, token)` reads across all accounts at one explicit block, e.g. 5k accounts × 10 tokens = 50k reads at 500 per Multicall, so about 100 calls per full refresh. Transfers seen by the follower prioritise which pairs refresh first; they don't prove the others are unchanged. *(Capacity hypothesis, to be measured.)*
- **Batch misses across concurrent requests** into Multicall3 `aggregate3` with `allowFailure = true`, about 500 calls per batch. Native ETH can be added via `getEthBalance`.
- **Single-flight per address** so concurrent requests share a fetch.
- **Return per-token partial results** (`balanceStatus: ok | error`) instead of failing the whole response.
- **Refresh cadence** bounds staleness for tokens that change without events (stETH, aTokens, WETH). The API discloses the block every balance was read at.

### RPC resilience

- **`RpcPool`:** Quicknode first, then a second provider. Each endpoint has a circuit breaker and exponential backoff with jitter.
- **Error classes:**
  - 429, 5xx and timeouts: retry with backoff.
  - -32005 or "too many results": split the range in half.
  - "Method not available" or archive refusal: mark that endpoint unfit for the method.
  - Anything else: fail.
- **Head-lag check:** compare `eth_blockNumber` across endpoints and skip any endpoint more than N blocks behind.
- **If the worker falls more than X blocks behind the head,** the API keeps serving the last snapshot with `stale: true` and `lagBlocks`.
- **Failover of log ingestion to SQD or Envio** if Quicknode is down for longer than a threshold. The same cursor and upserts apply, so switching sources is transparent.

### Modular interfaces

```ts
interface LogSource {             // RpcLogSource (default) | SqdPortalLogSource | HyperSyncLogSource
  head(): Promise<BlockRef>;
  transferLogs(range: BlockRange | BlockRef): Promise<RawLog[]>;
}
interface StateReader {           // always JSON-RPC
  balances(pairs: HolderToken[], at: BlockRef): Promise<BalanceResult[]>;
}
interface TokenStandard {         // erc20 | erc721
  id: 'erc20' | 'erc721';
  matches(log: RawLog): boolean;
  decode(log: RawLog): TransferEvent;
}
interface ChainAdapter {          // one per network family; EVM chains differ mostly by config
  logSources: LogSource[];
  state: StateReader;
  standards: TokenStandard[];
}
```

### Tables

- `networks(id, start_block, confirmation_depth)`
- `cursor(net, block_number, block_hash, source, updated_at)`
- `recent_blocks(net, number, hash, parent)`: the last 128 blocks, for reorg detection
- `holdings(net, holder, token, standard, first_block, last_block)`, primary key `(net, holder, token)`, index on `(net, holder)`
- Bonus: `nft_tokens(net, holder, token, token_id, block)`, verified with `ownerOf` through Multicall
- Optional: `balance_cache(net, holder, token, balance, at_block, dirty)` if the in-process cache is moved to Postgres or Redis

### API

- `GET /v1/{net}/accounts/{addr}/erc20?balances=true`
  returns `{ network, account, fromBlock, blockHeight, blockHash, chainHeadBlock, stale, lagBlocks, tokens: [{ contract, firstSeenBlock, lastSeenBlock, balance?, balanceStatus? }] }`
- `GET /v1/{net}/accounts/{addr}/erc721` is the bonus endpoint.
- `GET /health` reports the cursor, the head, lag and the active source.

## 6. Why this stack

- viem ships Multicall, a ranked fallback transport and typed ABIs.
- TypeScript matches the Trezor Suite ecosystem, and fits a short PoC.
- 5k users once a minute is about 83 requests per second, which is trivial for one Node process plus an indexed Postgres lookup.
- Postgres gives transactional cursor and upserts, the advisory lock, and enough capacity for tens of millions of pairs.
- Go is the natural production choice (Blockbook is Go). Say so in the presentation.

## 7. Limits and risks (for the README)

- **Only tokens that emit standard `Transfer` are discovered.** WETH `deposit` emits `Deposit`, and mints that don't emit `Transfer` from the zero address are missed. Some legacy NFTs (CryptoKitties) emit non-indexed Transfer. ERC-1155 is out of scope.
- **Tokens held before block 25,933,634 and untouched since are invisible** by definition of the source of truth.
- **Rebasing, reflection and WETH-style tokens:** balances can change without Transfer events. Balances are therefore always reported with the block they were read at, never inferred from the absence of transfers.
- **Spam airdrops inflate token lists.** Consider a "received only, never sent" flag or a deny list.
- **Not measured yet:**
  - Total pair count and storage for the window.
  - Quicknode response time for 10-block topic0 queries.
  - The `eth_call` gas cap for large Multicalls.
  - Whether JSON-RPC batches are billed per inner call. The `credits` page doesn't say.
- **Bandwidth:** at about 2 GB/day, the worker should run close to the provider, or use compression where supported.

## 8. Cheapest experiments to de-risk

1. **Quicknode, one topic0-only `getLogs` over 10 blocks.** Measure latency and size, and check the dashboard credit delta. This confirms 20 credits regardless of size.
2. **Send a 3-call JSON-RPC batch** and check whether it costs 20 or 60 credits.
3. **Run Multicall3 `aggregate3` with 500 and with 1,000 `balanceOf` calls** to find the gas or response ceiling.
4. **Backfill 10k blocks into Postgres** and extrapolate rows, storage and time to the full window.
5. **SQD Portal: stream `Transfer` logs for the same 10k blocks.** Compare speed and output with RPC, and confirm the topic padding rules.
6. **Ask Jakub:**
   - Does the start-block gap matter?
   - Should spam tokens be filtered?
   - Is a few seconds of lag behind head acceptable?

## Sources

- [Quicknode API credits](https://www.quicknode.com/api-credits)
- [Quicknode pricing](https://www.quicknode.com/pricing)
- [Quicknode eth_getLogs](https://www.quicknode.com/docs/ethereum/eth_getLogs)
- [Quicknode block range limit](https://support.quicknode.com/hc/en-us/articles/10258449939473-Understanding-the-10-000-Block-Range-Limit-for-querying-Logs-and-Events)
- [SQD Portal EVM API](https://docs.sqd.dev/en/portal/evm/api)
- [Envio HyperSync](https://docs.envio.dev/docs/HyperSync/overview)
- [Goldsky Turbo](https://docs.goldsky.com/turbo-pipelines/quickstart)
