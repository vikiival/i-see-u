import type { AccountLogSource, Address, BlockRange } from '../core/types.ts';

export interface FallbackOptions {
  /** Open the breaker after this many consecutive primary failures. */
  failureThreshold?: number;
  cooldownMs?: number;
  now?: () => number;
}

/** Per-chunk fallback with a small breaker; no partial primary result can escape this boundary. */
export function withFallback(
  primary: AccountLogSource,
  fallback: AccountLogSource,
  options: FallbackOptions = {},
): AccountLogSource {
  const threshold = options.failureThreshold ?? 2;
  const cooldownMs = options.cooldownMs ?? 60_000;
  const now = options.now ?? Date.now;
  let consecutiveFailures = 0;
  let cooldownUntil = 0;
  let fallbacks = 0;

  const useFallback = async (account: Address, range: BlockRange) => {
    fallbacks += 1;
    return fallback.scanAccount(account, range);
  };

  return {
    name: `${primary.name}+${fallback.name}`,
    preferredMaxRange: primary.preferredMaxRange,
    // Fixed large primary chunks stay independent of RPC's persisted adaptive range. If fallback is needed,
    // RpcLogSource still splits provider-limited ranges internally and the whole composite chunk commits atomically.
    adaptiveRange: primary.adaptiveRange,
    async scanAccount(account, range) {
      if (now() < cooldownUntil) return useFallback(account, range);
      try {
        const result = await primary.scanAccount(account, range);
        consecutiveFailures = 0;
        cooldownUntil = 0;
        return result;
      } catch {
        consecutiveFailures += 1;
        if (consecutiveFailures >= threshold) cooldownUntil = now() + cooldownMs;
        return useFallback(account, range);
      }
    },
    stats() {
      const stats = { ...primary.stats(), ...fallback.stats() };
      const primaryState = stats[primary.name];
      if (primaryState) stats[primary.name] = {
        ...primaryState,
        fallbacks: primaryState.fallbacks + fallbacks,
        cooldownUntil: cooldownUntil > now() ? cooldownUntil : undefined,
      };
      return stats;
    },
    async close() {
      await Promise.all([primary.close?.(), fallback.close?.()]);
    },
  };
}
