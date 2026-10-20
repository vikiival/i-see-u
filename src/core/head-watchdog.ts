/** Only a strictly higher block resets the clock; old/load-balanced heads do not count as progress. */
export function createHeadWatchdog(stallMs: number, now = () => Date.now()) {
  let highest: bigint | undefined;
  let progressedAt = now();
  let stalled = false;

  return {
    observe(number: bigint | undefined, penalize?: () => void) {
      if (number !== undefined && (highest === undefined || number > highest)) {
        highest = number;
        progressedAt = now();
        stalled = false;
      } else if (now() - progressedAt >= stallMs) {
        stalled = true;
        penalize?.();
      }
    },
    stalled: () => stalled,
  };
}
