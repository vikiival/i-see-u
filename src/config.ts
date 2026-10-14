import { networkRegistry } from './networks.ts';

try {
  process.loadEnvFile(new URL('../.env', import.meta.url));
} catch (error) {
  if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
}

const env = (k: string, d?: string): string => {
  const v = process.env[k] ?? d;
  if (v === undefined) throw new Error(`Missing env ${k}`);
  return v;
};

/** Every numeric knob is a positive integer; fail at startup rather than mid-scan. */
const int = (k: string, d: string): number => {
  const v = Number(env(k, d));
  if (!Number.isSafeInteger(v) || v < 1) throw new Error(`${k} must be a positive integer`);
  return v;
};

const choice = <T extends string>(k: string, values: readonly T[], d: T): T => {
  const value = env(k, d);
  if (!values.includes(value as T)) throw new Error(`${k} must be one of ${values.join(', ')}`);
  return value as T;
};

const evmTuning = {
  /** Mark the finalized head stale after this long without a higher block number. */
  finalizedMaxNoProgressMs: int('FINALIZED_MAX_NO_PROGRESS_MS', '1200000'),
  /** Max block span per address-filtered eth_getLogs call (Quicknode paid: 10k). */
  logsMaxRange: BigInt(int('ETH_LOGS_MAX_RANGE', '3000')),
  /** Floor of the adaptive per-account RPC range; heavy accounts shrink towards it. */
  logsMinRange: BigInt(int('ETH_LOGS_MIN_RANGE', '500')),
  /** Backfill chunks in flight per account; completed + in flight <= 2x this. */
  scanConcurrency: int('ETH_SCAN_CONCURRENCY', '8'),
  /** balanceOf calls per Multicall3 aggregate3 request (measured safe up to ~2000). */
  multicallBatch: int('ETH_MULTICALL_BATCH', '1000'),
  /** Max blocks per unfiltered follower getLogs (~450 KB of JSON per block; must stay <= 1000). */
  followerMaxRange: BigInt(int('ETH_FOLLOWER_MAX_RANGE', '500')),
};
const networks = Object.fromEntries(Object.values(networkRegistry)
  .map((network) => [network.name, { ...network, ...evmTuning }]));
const ethereum = networks.ethereum;

export const config = {
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
  logSource: choice('LOG_SOURCE', ['auto', 'hypersync', 'rpc'] as const, 'rpc'),
  hypersync: {
    apiToken: process.env.HYPERSYNC_API_KEY || undefined,
    url: process.env.HYPERSYNC_URL || undefined,
    maxRange: BigInt(int('HYPERSYNC_MAX_RANGE', '100000')),
    timeoutMs: int('HYPERSYNC_TIMEOUT_MS', '30000'),
  },
  follower: {
    /** How often the follower polls the finalized head (finalized advances ~every 6.4 min). */
    pollMs: int('FOLLOWER_POLL_MS', '30000'),
    /** Discovery returns 503 when the follower is more than this many blocks behind finalized (~3 epochs). */
    maxLagBlocks: BigInt(int('FOLLOWER_MAX_LAG_BLOCKS', '96')),
  },
  balances: {
    /** Accounts whose balances were requested within this window are refreshed by the shared worker. */
    activeAccountTtlMs: int('ACTIVE_ACCOUNT_TTL_MS', '300000'),
    refreshMs: int('BALANCE_REFRESH_MS', '60000'),
  },
  request: {
    /** Per-request deadline for RPC work left on the request path (cold backfill wait, first balance read). */
    deadlineMs: int('REQUEST_DEADLINE_MS', '10000'),
    /** Concurrent account backfills, and how many more may wait before requests get 503. */
    backfillConcurrency: int('BACKFILL_CONCURRENCY', '4'),
    backfillQueue: int('BACKFILL_QUEUE', '64'),
    /** Concurrent request-path RPC reads (first balance read, ownership), and how many more may wait. */
    rpcConcurrency: int('REQUEST_RPC_CONCURRENCY', '16'),
    rpcQueue: int('REQUEST_RPC_QUEUE', '256'),
  },
  /** Refuse discovery when the follower has not advanced for longer than this. */
  maxHeadProgressAgeMs: int('MAX_HEAD_AGE_MS', '1800000'),
};

for (const network of Object.values(config.networks)) {
  if (network.logsMinRange > network.logsMaxRange) {
    throw new Error('ETH_LOGS_MIN_RANGE must not exceed ETH_LOGS_MAX_RANGE');
  }
  if (network.followerMaxRange > 1_000n) {
    throw new Error('ETH_FOLLOWER_MAX_RANGE must not exceed 1000 (unfiltered responses are ~450 KB per block)');
  }
}
if (config.logSource === 'hypersync' && !config.hypersync.apiToken) {
  throw new Error('LOG_SOURCE=hypersync requires HYPERSYNC_API_KEY');
}
