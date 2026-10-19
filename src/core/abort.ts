/** Abandon a read-only wait on cancellation; callers must still await database transactions normally. */
export async function abortable<T>(pending: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return pending;
  const aborted = Promise.withResolvers<never>();
  const abort = (): void => aborted.reject(signal.reason);
  signal.addEventListener('abort', abort, { once: true });

  if (signal.aborted) abort();

  try {
    return await Promise.race([pending, aborted.promise]);
  } finally {
    signal.removeEventListener('abort', abort);
  }
}
