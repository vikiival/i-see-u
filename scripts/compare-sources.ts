// Live diagnostic only: compares finalized per-account aggregates without printing endpoint URLs or API tokens.
import type { AccountLogSource, ChunkAggregate } from '../src/core/types.ts';

try {
  process.loadEnvFile(new URL('../.env', import.meta.url));
} catch (error) {
  if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
    throw error;
  }
}

const { createEvmAdapter } = await import('../src/chains/evm/adapter.ts');
const { emptyAggregate, mergeAggregate } = await import('../src/core/aggregate.ts');
const { networkRegistry } = await import('../src/networks.ts');
const { createRpcClient } = await import('../src/rpc/client.ts');
const { createHypersyncLogSource } = await import('../src/sources/hypersync.ts');
const { createRpcLogSource } = await import('../src/sources/rpc-logs.ts');

const integer = (name: string, fallback: number): number => {
  const value = Number(process.env[name] ?? fallback);

  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }

  return value;
};

const block = (value: string | undefined, fallback: bigint): bigint => (value === undefined ? fallback : BigInt(value));

const [input, fromArg, toArg] = process.argv.slice(2);

if (!input) {
  throw new Error('Usage: node scripts/compare-sources.ts <address> [fromBlock] [toBlock]');
}

const apiToken = process.env.HYPERSYNC_API_KEY;

if (!apiToken) {
  throw new Error('HYPERSYNC_API_KEY is required for source comparison');
}

const rpcUrl = process.env.RPC_URL;

if (!rpcUrl) {
  throw new Error('RPC_URL is required for source comparison');
}

const network = networkRegistry.ethereum;

const rpc = createRpcClient({
  urls: [rpcUrl, ...(process.env.RPC_FALLBACK_URLS ?? '').split(',').filter(Boolean)],
  timeoutMs: integer('RPC_TIMEOUT_MS', 15_000),
  maxAttempts: integer('RPC_MAX_ATTEMPTS', 6),
  maxRps: integer('RPC_MAX_RPS', 20),
});

const rpcSource = createRpcLogSource(rpc, {
  preferredMaxRange: BigInt(integer('ETH_LOGS_MAX_RANGE', 3_000)),
});

const hypersyncSource = createHypersyncLogSource({
  url: process.env.HYPERSYNC_URL || network.hypersyncUrl,
  apiToken,
  preferredMaxRange: BigInt(integer('HYPERSYNC_MAX_RANGE', 100_000)),
  timeoutMs: integer('HYPERSYNC_TIMEOUT_MS', 30_000),
});

const adapter = createEvmAdapter(rpc, {
  ...network,
  network: network.name,
  finalizedMaxNoProgressMs: 1_200_000,
  followerMaxRange: 500n,
  multicallBatch: 1_000,
});

const account = adapter.normalizeAddress(input);

if (!account) {
  throw new Error('Invalid account address');
}

const finalized = await adapter.finalizedHead();

const range = {
  fromBlock: block(fromArg, network.startBlock),
  toBlock: block(toArg, finalized.block.number),
};

if (range.fromBlock > range.toBlock || range.toBlock > finalized.block.number) {
  throw new Error(`Range must be ordered and end at or below finalized block ${finalized.block.number}`);
}

const canonical = (aggregate: ChunkAggregate): string =>
  JSON.stringify({
    tokens: [...aggregate.tokens].sort().map(([token, value]) => [
      token,
      {
        ...value,
        firstBlock: value.firstBlock.toString(),
        lastBlock: value.lastBlock.toString(),
      },
    ]),
    nfts: [...aggregate.nfts].sort().map(([key, value]) => [
      key,
      {
        ...value,
        tokenId: value.tokenId.toString(),
        block: value.block.toString(),
      },
    ]),
  });

const run = async (source: AccountLogSource) => {
  const aggregate = emptyAggregate();
  let chunks = 0;
  let calls = 0;
  let logs = 0;
  const started = performance.now();

  for (let fromBlock = range.fromBlock; fromBlock <= range.toBlock;) {
    const end = fromBlock + source.preferredMaxRange - 1n;
    const toBlock = end < range.toBlock ? end : range.toBlock;
    const scanned = await source.scanAccount(account, { fromBlock, toBlock });
    mergeAggregate(aggregate, scanned.aggregate);
    chunks += 1;
    calls += scanned.calls;
    logs += scanned.logs;
    fromBlock = toBlock + 1n;
  }

  return {
    aggregate,
    summary: {
      source: source.name,
      chunks,
      requests: calls,
      ms: Math.round(performance.now() - started),
      logs,
      tokens: [...aggregate.tokens.values()].filter((token) => token.standard === 'erc20').length,
      nftItems: aggregate.nfts.size,
    },
  };
};

try {
  // RPC deliberately runs at its normal modest 3k-block chunks; the default full window is about 172 getLogs calls.
  const rpcResult = await run(rpcSource);
  const hypersyncResult = await run(hypersyncSource);
  const equal = canonical(rpcResult.aggregate) === canonical(hypersyncResult.aggregate);
  console.log(
    JSON.stringify({
      range: { fromBlock: range.fromBlock.toString(), toBlock: range.toBlock.toString() },
      ...rpcResult.summary,
      equal,
    }),
  );
  console.log(
    JSON.stringify({
      range: { fromBlock: range.fromBlock.toString(), toBlock: range.toBlock.toString() },
      ...hypersyncResult.summary,
      equal,
    }),
  );

  if (!equal) {
    process.exitCode = 1;
  }
} finally {
  await Promise.all([rpcSource.close?.(), hypersyncSource.close?.()]);
}
