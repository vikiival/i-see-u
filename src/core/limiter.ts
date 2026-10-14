/** Error the API maps to 503 with a Retry-After header. */
export const unavailable = (message: string, retryAfterS: number): Error =>
  Object.assign(new Error(message), { statusCode: 503, retryAfterS });

export interface Limiter {
  /** Run fn when a slot is free; rejects with 503 at once when maxActive run and maxQueued already wait. */
  run<T>(fn: () => Promise<T>): Promise<T>;
  readonly active: number;
  readonly queued: number;
}

export function createLimiter(maxActive: number, maxQueued: number, retryAfterS = 5): Limiter {
  let active = 0;
  const queue: (() => void)[] = [];
  const release = (): void => {
    active -= 1;
    const next = queue.shift();
    if (next) {
      active += 1;
      next();
    }
  };
  return {
    async run<T>(fn: () => Promise<T>): Promise<T> {
      if (active >= maxActive) {
        if (queue.length >= maxQueued) throw unavailable('Server busy: too many RPC-bound requests in flight', retryAfterS);
        await new Promise<void>((resolve) => queue.push(resolve));
      } else {
        active += 1;
      }
      try {
        return await fn();
      } finally {
        release();
      }
    },
    get active() { return active; },
    get queued() { return queue.length; },
  };
}

/** Settle with the promise's outcome, or with `onTimeout()` (which may throw) after ms; the promise keeps running. */
export function withDeadline<T, U>(promise: Promise<T>, ms: number, onTimeout: () => U): Promise<T | U> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<U>((resolve, reject) => {
    timer = setTimeout(() => {
      try {
        resolve(onTimeout());
      } catch (error) {
        reject(error);
      }
    }, ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
