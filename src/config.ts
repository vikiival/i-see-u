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

export const config = {
  port: Number(env('PORT', '3000')),
  dbPath: env('DB_PATH', 'data/i-see-u.sqlite'),
  rpc: {
    /** Primary first; comma-separated RPC_FALLBACK_URLS are tried after it. */
    urls: [env('RPC_URL'), ...env('RPC_FALLBACK_URLS', '').split(',').filter(Boolean)],
    timeoutMs: Number(env('RPC_TIMEOUT_MS', '15000')),
    maxAttempts: Number(env('RPC_MAX_ATTEMPTS', '6')),
    /** Client-side cap on outgoing requests per second across all endpoints. */
    maxRps: Number(env('RPC_MAX_RPS', '20')),
  },
  ethereum: {
    network: 'ethereum',
    chainId: 1,
    startBlock: BigInt(env('ETH_START_BLOCK', '25933634')),
    confirmations: BigInt(env('ETH_CONFIRMATIONS', '2')),
    /** Max block span per address-filtered eth_getLogs call (Quicknode paid: 10k). */
    logsMaxRange: BigInt(env('ETH_LOGS_MAX_RANGE', '3000')),
    /** Floor of the adaptive per-account range; heavy accounts shrink towards it. */
    logsMinRange: BigInt(env('ETH_LOGS_MIN_RANGE', '500')),
    /** A getLogs response above this size or slower than this marks a chunk heavy (halve the range). */
    logsHeavyBytes: Number(env('ETH_LOGS_HEAVY_BYTES', '50000000')),
    logsHeavyMs: Number(env('ETH_LOGS_HEAVY_MS', '3000')),
    /** Chunks (each 2 getLogs calls) in flight per account scan; completed + in flight <= 2x this. */
    scanConcurrency: Number(env('ETH_SCAN_CONCURRENCY', '8')),
    multicall3: '0xca11bde05977b3631167028862be2a173976ca11' as const,
    /** balanceOf calls per Multicall3 aggregate3 request (measured safe up to ~2000). */
    multicallBatch: Number(env('ETH_MULTICALL_BATCH', '1000')),
  },
  /** How long cached head observations are reused before asking the node again. */
  headCacheMs: Number(env('HEAD_CACHE_MS', '4000')),
  /** Mark finalized/latest stale after this long without a higher block number. */
  finalizedMaxNoProgressMs: Number(env('FINALIZED_MAX_NO_PROGRESS_MS', '1200000')),
  latestMaxNoProgressMs: Number(env('LATEST_MAX_NO_PROGRESS_MS', '120000')),
  /** Refuse discovery when finalized progress is older than this. */
  maxHeadProgressAgeMs: Number(env('MAX_HEAD_AGE_MS', '1800000')),
};

export type Config = typeof config;
