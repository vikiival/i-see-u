// config loads .env via process.loadEnvFile; exported environment values take precedence.
import { config } from '../src/config.ts';
import { createEvmAdapter } from '../src/chains/evm/adapter.ts';
import { createDiscoveryService } from '../src/discovery/service.ts';
import { createRpcClient } from '../src/rpc/client.ts';
import { openPostgresStore } from '../src/store/postgres.ts';
import type { Store } from '../src/core/types.ts';

let store: Store | undefined;

try {
  ({ store } = await openPostgresStore(config.databaseUrl, {
    max: 1,
    connectionTimeoutMillis: config.db.connectTimeoutMs,
    migrate: false,
  }));
  const rpc = createRpcClient(config.rpc);

  const adapters = Object.fromEntries(
    Object.values(config.networks).map((network) => [
      network.name,
      createEvmAdapter(rpc, { ...network, network: network.name }),
    ]),
  );

  const discovery = createDiscoveryService({
    adapters,
    store,
    maxHeadAgeMs: config.maxHeadAgeMs,
    maxNoAdvanceMs: config.maxHeadProgressAgeMs,
  });

  for (const network of Object.keys(adapters)) {
    const snapshot = await store.opsSnapshot(network);
    const status = await discovery.status(network, snapshot);
    console.log(
      JSON.stringify({
        network,
        cursor: snapshot.state?.cursor.number.toString() ?? null,
        head: snapshot.state?.head?.number.toString() ?? null,
        tailTip: snapshot.tipNumber?.toString() ?? null,
        serving: status.serving,
        reason: status.reason,
        bootstrap: snapshot.bootstrap
          ? {
              ...snapshot.bootstrap,
              startBlock: snapshot.bootstrap.startBlock.toString(),
              targetBlock: snapshot.bootstrap.targetBlock.toString(),
            }
          : null,
      }),
    );
  }
} catch {
  // Drivers and invalid URL errors can embed credentials; never print the raw failure.
  console.error('Status read failed; check database connectivity and configuration');
  process.exitCode = 1;
} finally {
  await store?.close();
}
