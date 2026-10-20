import type { Address } from './core/types.ts';
import { networkRegistry } from './networks.ts';
import { readyMode } from './api/ops.ts';

try {
  process.loadEnvFile(new URL('../.env', import.meta.url));
} catch (error) {
  if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
    throw error;
  }
}

const env = (k: string, d?: string): string => {
  const v = process.env[k] ?? d;

  if (v === undefined) {
    throw new Error(`Missing env ${k}`);
  }

  return v;
};

/** Every numeric knob is a positive integer; fail at startup rather than mid-scan. */
const int = (k: string, d: string): number => {
  const v = Number(env(k, d));

  if (!Number.isSafeInteger(v) || v < 1) {
    throw new Error(`${k} must be a positive integer`);
  }

  return v;
};

const bool = (k: string, d: boolean): boolean => {
  const value = env(k, d ? 'true' : 'false');

  if (value === 'true' || value === '1') {
    return true;
  }

  if (value === 'false' || value === '0') {
    return false;
  }

  throw new Error(`${k} must be true or false`);
};

const intList = (k: string, d: string): number[] =>
  env(k, d)
    .split(',')
    .map((part) => {
      const value = Number(part.trim());

      if (!Number.isSafeInteger(value) || value < 0) {
        throw new Error(`${k} must contain non-negative integers`);
      }

      return value;
    });

const isLowercaseAddress = (value: string): value is Address => /^0x[0-9a-f]{40}$/.test(value);

const evmTuning = {
  /** Mark the finalized head stale after this long without a higher block number. */
  finalizedMaxNoProgressMs: int('FINALIZED_MAX_NO_PROGRESS_MS', '1200000'),
  /** balanceOf calls per Multicall3 aggregate3 request (measured safe up to ~2000). */
  multicallBatch: int('ETH_MULTICALL_BATCH', '1000'),
  /** Max blocks per unfiltered follower getLogs (~450 KB of JSON per block; must stay <= 1000). */
  followerMaxRange: BigInt(int('ETH_FOLLOWER_MAX_RANGE', '500')),
};

const addresses = (k: string, value: string): Address[] =>
  value
    .split(',')
    .map((token) => token.trim().toLowerCase())
    .filter(Boolean)
    .map((token) => {
      if (!isLowercaseAddress(token)) {
        throw new Error(`${k} must contain addresses`);
      }

      return token;
    });

/** When set, replaces (not extends) each network's default list from src/networks.ts. */
const refreshOverride = process.env.BALANCE_ALWAYS_REFRESH_TOKENS;

/** START_BLOCK replaces the registry start block (a plain block number). A stored index or bootstrap must match it. */
const startBlockOverride = (() => {
  const value = process.env.START_BLOCK;

  if (value === undefined || value === '') {
    return undefined;
  }

  if (!/^\d+$/.test(value) || BigInt(value) < 1n) {
    throw new Error('START_BLOCK must be a positive block number');
  }

  return BigInt(value);
})();

const networks = Object.fromEntries(
  Object.values(networkRegistry).map((network) => [
    network.name,
    {
      ...network,
      ...evmTuning,
      startBlock: startBlockOverride ?? network.startBlock,
      alwaysRefreshTokens:
        refreshOverride === undefined
          ? network.alwaysRefreshTokens
          : addresses('BALANCE_ALWAYS_REFRESH_TOKENS', refreshOverride),
    },
  ]),
);

const ethereum = networks.ethereum;

export const config = {
  serveOnly: bool('SERVE_ONLY', false),
  readyzAllow: readyMode(process.env.READYZ_ALLOW),
  headStallMs: int('HEAD_STALL_MS', '60000'),
  port: int('PORT', '3000'),
  databaseUrl: env('DATABASE_URL', 'postgresql://iseeu:iseeu@localhost:5432/iseeu'),
  rpc: {
    /** Primary first; comma-separated RPC_FALLBACK_URLS are tried after it. */
    urls: [env('RPC_URL'), ...env('RPC_FALLBACK_URLS', '').split(',').filter(Boolean)],
    timeoutMs: int('RPC_TIMEOUT_MS', '15000'),
    maxAttempts: int('RPC_MAX_ATTEMPTS', '6'),
    /** Client-side cap on outgoing requests per second across all endpoints. */
    maxRps: int('RPC_MAX_RPS', '20'),
  },
  networks,
  /** Kept as a compact single-network convenience for the PoC. */
  ethereum,
  follower: {
    pollMs: int('FOLLOWER_POLL_MS', '30000'),
    maxRangesPerTick: int('FOLLOWER_MAX_RANGES_PER_TICK', '8'),
    headerConcurrency: int('FOLLOWER_HEADER_CONCURRENCY', '8'),
  },
  maxHeadAgeMs: int('MAX_HEAD_OBSERVATION_AGE_MS', '120000'),
  /** Unfinalized journal above the finalized cursor: `finality=latest` (the default) serves the tip. */
  tail: {
    enabled: bool('TAIL_ENABLED', true),
    /** Deeper reorgs (or one reaching the finalized cursor) wipe the tail and serve finalized until it rebuilds. */
    maxDepth: BigInt(int('TAIL_MAX_DEPTH', '64')),
    /** Poll `latest` at these offsets into each 12 s slot; the rest of a slot is skipped once its block is in. */
    pollOffsetsMs: intList('TAIL_POLL_OFFSETS_MS', '2500,5000,9000'),
    /** Serve `latest` only while the tail tip block is at most this old (5 slots: up to 4 missed slots). */
    maxTipAgeMs: int('TAIL_MAX_TIP_AGE_MS', '60000'),
    /** Stop appending this far above the finalized cursor (~3.4 h; normally 64-95): bounds the tail if finality stalls. */
    maxAheadBlocks: BigInt(int('TAIL_MAX_AHEAD_BLOCKS', '1024')),
  },
  balances: {
    /** How long the first uncached (account, token) pair waits for others to share its Multicall. */
    batchWindowMs: int('BALANCE_BATCH_WINDOW_MS', '25'),
  },
  db: {
    /** Postgres pool size (one connection is held permanently by the follower leader lock). */
    poolMax: int('DB_POOL_MAX', '20'),
    /** Fail pool acquisition after this long (surfaces as 503) instead of queueing forever. */
    connectTimeoutMs: int('DB_CONNECT_TIMEOUT_MS', '5000'),
  },
  request: {
    /** Per-request deadline for RPC work left on the request path (balances and ownership). */
    deadlineMs: int('REQUEST_DEADLINE_MS', '10000'),
    /** Concurrent request-path RPC reads (shared balance Multicalls, ownership), and how many more may wait. */
    rpcConcurrency: int('REQUEST_RPC_CONCURRENCY', '16'),
    rpcQueue: int('REQUEST_RPC_QUEUE', '256'),
    /** verifyOwnership is skipped (not an error) when an account holds more NFT items than this. */
    maxVerifyOwnershipItems: int('MAX_VERIFY_OWNERSHIP_ITEMS', '2000'),
  },
  /** One-time global load (`npm run bootstrap`, or BOOTSTRAP_ON_START in the server). */
  bootstrap: {
    onStart: bool('BOOTSTRAP_ON_START', false),
    segmentBlocks: BigInt(int('BOOTSTRAP_SEGMENT_BLOCKS', '1000')),
    headerChunkBlocks: BigInt(int('BOOTSTRAP_HEADER_CHUNK_BLOCKS', '5000')),
    /** Concurrent Quicknode segment workers (unfiltered 100-block getLogs). */
    rpcWorkers: int('BOOTSTRAP_RPC_WORKERS', '4'),
    rpcLogRange: BigInt(int('BOOTSTRAP_RPC_LOG_RANGE', '100')),
    /** Bulk getLogs answers take 4-35 s under concurrency; the request-path timeout is too short for them. */
    rpcTimeoutMs: int('BOOTSTRAP_RPC_TIMEOUT_MS', '60000'),
    /** Optional: without it the bootstrap is RPC-only (and pays one call per header). Never logged. */
    hypersyncApiKey: process.env.HYPERSYNC_API_KEY || undefined,
    hypersyncUrl: process.env.HYPERSYNC_URL || ethereum.hypersyncUrl,
    /** Per-request idle timeout for HyperSync (not a whole-range timeout). */
    hypersyncIdleMs: int('HYPERSYNC_TIMEOUT_MS', '120000'),
    /** Per legacy segment GROUP BY, not a global server setting. */
    workMem: env('BOOTSTRAP_WORK_MEM', '64MB'),
    maintenanceWorkMem: env('BOOTSTRAP_MAINTENANCE_WORK_MEM', '1GB'),
  },
  /** Mark reads stale when the follower has not advanced for longer than this. */
  maxHeadProgressAgeMs: int('MAX_HEAD_AGE_MS', '1800000'),
};

if (config.tail.pollOffsetsMs.length === 0 || config.tail.pollOffsetsMs.some((offset) => offset >= 12_000)) {
  throw new Error('TAIL_POLL_OFFSETS_MS must contain offsets within a 12-second slot');
}

for (const network of Object.values(config.networks)) {
  if (network.followerMaxRange > 1_000n) {
    throw new Error('ETH_FOLLOWER_MAX_RANGE must not exceed 1000 (unfiltered responses are ~450 KB per block)');
  }
}
