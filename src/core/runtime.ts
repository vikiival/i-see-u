/** Loads shared read state in every replica; only indexers start loops or acquire bootstrap leadership. */
export async function startBackground(
  serveOnly: boolean,
  workers: { load(): Promise<void>; start(): void }[],
  bootstrap: () => Promise<void>,
): Promise<void> {
  await Promise.all(workers.map((worker) => worker.load()));

  if (serveOnly) {
    return;
  }

  for (const worker of workers) {
    worker.start();
  }

  await bootstrap();
}
