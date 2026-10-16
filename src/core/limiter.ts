/** Error the API maps to 503 with a Retry-After header. */
export const unavailable = (message: string, retryAfterS: number): Error =>
  Object.assign(new Error(message), { statusCode: 503, retryAfterS });

export interface RunOptions {
  /** Cancels the job: a queued job leaves the queue at once, a running one is aborted through the signal fn gets. */
  signal?: AbortSignal;
  /** Epoch ms; past it the job is cancelled the same way. */
  deadlineAt?: number;
}

export interface Limiter {
  /**
   * Run fn when a slot is free; rejects with 503 at once when maxActive run and maxQueued already wait, and with 503
   * when cancelled. fn gets a signal that aborts on cancellation so it stops spending RPC credits.
   */
  run<T>(fn: (signal: AbortSignal) => Promise<T>, opts?: RunOptions): Promise<T>;
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
    async run<T>(fn: (signal: AbortSignal) => Promise<T>, opts: RunOptions = {}): Promise<T> {
      const { deadlineAt } = opts;
      const expired = (): boolean => deadlineAt !== undefined && Date.now() >= deadlineAt;
      // One clock read: a second one could see the deadline pass and hand AbortSignal.timeout a negative delay.
      const remainingMs = deadlineAt === undefined ? Infinity : deadlineAt - Date.now();

      const cancelled = (when: string, cause?: unknown): Error => Object.assign(
        unavailable(`Request ${expired() ? 'deadline passed' : 'cancelled'} ${when}`, retryAfterS),
        { cause: cause ?? opts.signal?.reason },
      );

      if (opts.signal?.aborted || remainingMs <= 0) throw cancelled('before it ran');
      const signals = opts.signal ? [opts.signal] : [];

      if (remainingMs !== Infinity) signals.push(AbortSignal.timeout(remainingMs));
      const signal = AbortSignal.any(signals);

      if (active >= maxActive) {
        if (queue.length >= maxQueued) throw unavailable('Server busy: too many RPC-bound requests in flight', retryAfterS);

        await new Promise<void>((resolve, reject) => {
          const leave = (): void => {
            queue.splice(queue.indexOf(take), 1);
            reject(cancelled('while queued'));
          };

          const take = (): void => {
            signal.removeEventListener('abort', leave);
            resolve();
          };

          queue.push(take);
          signal.addEventListener('abort', leave, { once: true });
        });
      } else {
        active += 1;
      }

      try {
        return await fn(signal);
      } catch (error) {
        // Whatever fn threw after the abort is a consequence of it.
        throw signal.aborted ? cancelled('while running', error) : error;
      } finally {
        release();
      }
    },
    get active() { return active; },
    get queued() { return queue.length; },
  };
}

/**
 * Run work with a signal that aborts after ms or when `parent` aborts. On the deadline it settles with `onTimeout()`
 * (which may throw), on a parent abort with a 503 "Request cancelled"; either way at once, without waiting for the
 * aborted work to stop.
 */
export function withDeadline<T, U>(
  work: (signal: AbortSignal) => Promise<T>,
  ms: number,
  onTimeout: () => U,
  parent?: AbortSignal,
): Promise<T | U> {
  const controller = new AbortController();
  const signal = parent ? AbortSignal.any([parent, controller.signal]) : controller.signal;
  const cut = Promise.withResolvers<U>();
  const cancel = (): void => cut.reject(Object.assign(unavailable('Request cancelled', 1), { cause: parent?.reason }));

  const timer = setTimeout(() => {
    // Settle first so the race reports the deadline, not the abort error the work rejects with.
    try {
      cut.resolve(onTimeout());
    } catch (error) {
      cut.reject(error);
    }

    controller.abort(new DOMException('Request deadline passed', 'TimeoutError'));
  }, ms);

  if (parent?.aborted) cancel();
  else parent?.addEventListener('abort', cancel, { once: true });

  // The executor turns a synchronous throw into a rejection, so the cleanup below always runs.
  const running = new Promise<T>((resolve) => resolve(work(signal)));

  return Promise.race([running, cut.promise]).finally(() => {
    clearTimeout(timer);
    parent?.removeEventListener('abort', cancel);
  });
}
