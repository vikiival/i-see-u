/** A source range does not join the stored canonical chain; retain it as suspect work. */
export class ChainGapError extends Error {}

/** A tail write raced another writer or a finalization (its fence no longer holds); retried on the next tick. */
export class TailConflictError extends Error {}
