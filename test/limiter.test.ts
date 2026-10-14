import { expect, test, vi } from 'vitest';

import { createLimiter } from '../src/core/limiter.ts';

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
