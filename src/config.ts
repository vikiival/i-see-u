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
  ethereum: {
    network: 'ethereum',
    startBlock: 25_933_634n,
    /** Mark the finalized head stale after this long without a higher block number. */
    finalizedMaxNoProgressMs: int('FINALIZED_MAX_NO_PROGRESS_MS', '1200000'),
    /** Max block span per address-filtered eth_getLogs call (Quicknode paid: 10k). */
    logsMaxRange: BigInt(int('ETH_LOGS_MAX_RANGE', '3000')),
    /** Floor of the adaptive per-account range; heavy accounts shrink towards it. */
    logsMinRange: BigInt(int('ETH_LOGS_MIN_RANGE', '500')),
    /** Chunks (each 2 getLogs calls) in flight per account scan; completed + in flight <= 2x this. */
    scanConcurrency: int('ETH_SCAN_CONCURRENCY', '8'),
    /** balanceOf calls per Multicall3 aggregate3 request (measured safe up to ~2000). */
    multicallBatch: int('ETH_MULTICALL_BATCH', '1000'),
  },
  /** Refuse discovery when finalized progress is older than this. */
  maxHeadProgressAgeMs: int('MAX_HEAD_AGE_MS', '1800000'),
};

if (config.ethereum.logsMinRange > config.ethereum.logsMaxRange) {
  throw new Error('ETH_LOGS_MIN_RANGE must not exceed ETH_LOGS_MAX_RANGE');
}
