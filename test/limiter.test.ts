import { expect, test, vi } from 'vitest';

import { createLimiter, withDeadline } from '../src/core/limiter.ts';

const gate = () => {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => { open = resolve; });

  return { promise, open };
};

test('drops a queued job whose deadline passed and still runs the next one', async () => {
  const limiter = createLimiter(1, 4);
  const g = gate();
  const first = limiter.run(() => g.promise);
  const expired = vi.fn(async () => 'expired');
  const expiredRun = limiter.run(expired, { deadlineAt: Date.now() - 1 });
  const expiredResult = expect(expiredRun).rejects.toMatchObject({ statusCode: 503, retryAfterS: 5 });
  const next = limiter.run(async () => 'next');

  g.open();
  await first;
  await expiredResult;
  expect(await next).toBe('next');
  expect(expired).not.toHaveBeenCalled();
  expect(limiter.active).toBe(0);
  expect(limiter.queued).toBe(0);
});

test('runs queued jobs without deadlineAt, or with a future one, as before', async () => {
  const limiter = createLimiter(1, 4);
  const g = gate();
  const first = limiter.run(() => g.promise);
  const plain = limiter.run(async () => 'plain');
  const future = limiter.run(async () => 'future', { deadlineAt: Date.now() + 60_000 });
  g.open();
  await first;
  expect(await plain).toBe('plain');
  expect(await future).toBe('future');
});

test('a queued job whose signal aborts leaves the queue at once, before a slot frees', async () => {
  const limiter = createLimiter(1, 4);
  const g = gate();
  const first = limiter.run(() => g.promise);
  const controller = new AbortController();
  const queued = vi.fn(async () => 'queued');
  const run = limiter.run(queued, { signal: controller.signal });
  expect(limiter.queued).toBe(1);

  controller.abort();
  await expect(run).rejects.toMatchObject({ statusCode: 503, message: 'Request cancelled while queued' });
  expect(limiter.queued).toBe(0);
  expect(limiter.active).toBe(1);

  g.open();
  await first;
  expect(queued).not.toHaveBeenCalled();
  expect(limiter.active).toBe(0);
});

test('a queued job leaves the queue when its deadline passes, without waiting for the slot', async () => {
  const limiter = createLimiter(1, 4);
  const g = gate();
  const first = limiter.run(() => g.promise);
  const queued = vi.fn(async () => 'queued');
  await expect(limiter.run(queued, { deadlineAt: Date.now() + 20 })).rejects.toMatchObject({ statusCode: 503, message: 'Request deadline passed while queued' });
  expect(limiter.queued).toBe(0);
  g.open();
  await first;
  expect(queued).not.toHaveBeenCalled();
});

test('a running job past its deadline is aborted through its signal and rejects with 503', async () => {
  const limiter = createLimiter(1, 4);
  let seen: AbortSignal | undefined;

  const run = limiter.run((signal) => {
    seen = signal;

    return new Promise<never>((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
  }, { deadlineAt: Date.now() + 20 });

  await expect(run).rejects.toMatchObject({ statusCode: 503, retryAfterS: 5, message: 'Request deadline passed while running' });
  expect(seen?.aborted).toBe(true);
  expect(limiter.active).toBe(0);
});

test('withDeadline settles with onTimeout and then aborts the work', async () => {
  let seen: AbortSignal | undefined;

  const work = (signal: AbortSignal) => {
    seen = signal;

    return new Promise<string>((_resolve, reject) => signal.addEventListener('abort', () => reject(new Error('work saw the abort'))));
  };

  await expect(withDeadline(work, 20, () => { throw new Error('deadline'); })).rejects.toThrow('deadline');
  expect(seen?.aborted).toBe(true);
  // Finishing in time never aborts the signal.
  expect(await withDeadline(async (signal) => signal.aborted, 1_000, () => true)).toBe(false);
});

test('withDeadline aborts the work when the parent signal does', async () => {
  const parent = new AbortController();
  let seen: AbortSignal | undefined;

  const pending = withDeadline((signal) => {
    seen = signal;

    return new Promise<string>((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
  }, 60_000, () => 'timeout', parent.signal);

  parent.abort(new Error('client went away'));
  await expect(pending).rejects.toMatchObject({ statusCode: 503, message: 'Request cancelled', cause: expect.objectContaining({ message: 'client went away' }) });
  expect(seen?.aborted).toBe(true);
});

test('a deadline that passes between clock reads still runs the job instead of throwing a RangeError', async () => {
  // The expiry check sees 1 ms left; any later read is already past the deadline.
  vi.spyOn(Date, 'now').mockReturnValueOnce(999).mockReturnValue(1_001);
  const limiter = createLimiter(1, 1);

  try {
    expect(await limiter.run(async () => 'ran', { deadlineAt: 1_000 })).toBe('ran');
  } finally { vi.restoreAllMocks(); }
});

test('withDeadline turns a synchronous throw into a rejection and clears its timer', async () => {
  vi.useFakeTimers();

  try {
    const pending = withDeadline(() => { throw new Error('sync'); }, 60_000, () => 'timeout');
    await expect(pending).rejects.toThrow('sync');
    expect(vi.getTimerCount()).toBe(0);
  } finally { vi.useRealTimers(); }
});

test('withDeadline with an already aborted parent rejects at once and hands the work an aborted signal', async () => {
  let seen: AbortSignal | undefined;

  const pending = withDeadline((signal) => {
    seen = signal;

    return new Promise<string>(() => {});
  }, 60_000, () => 'timeout', AbortSignal.abort());

  await expect(pending).rejects.toMatchObject({ statusCode: 503, message: 'Request cancelled' });
  expect(seen?.aborted).toBe(true);
});
