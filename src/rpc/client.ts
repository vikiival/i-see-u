import { AsyncLocalStorage } from 'node:async_hooks';
import { HttpRequestError, RpcRequestError, TimeoutError, http } from 'viem';
import type { EIP1193RequestFn } from 'viem';

import type { RpcClient, RpcErrorKind, RpcRequestOptions, RpcStats } from '../core/types.ts';
import { isObject, isString } from '../core/parse.ts';
import { RpcError, classify } from './errors.ts';

export interface RpcClientOptions {
  urls: string[];
  timeoutMs: number;
  maxAttempts: number;
  maxRps: number;
}

interface Endpoint {
  send: EIP1193RequestFn;
  displayUrl: string;
  failures: number;
  openUntil: number;
  probeInFlight: boolean;
  unsupportedUntil: Map<string, number>;
}

const OPEN_MS = 15_000;
const UNSUPPORTED_MS = 10 * 60_000;
const BREAKER_FAILURES = 5;
const BACKOFF_BASE_MS = 250;
const BACKOFF_CAP_MS = 8_000;
const RETRY_AFTER_CAP_MS = 10_000;

// Hands each attempt the raw Response (status, Retry-After) that viem's errors do not always carry.
const attemptResponse = new AsyncLocalStorage<(response: Response) => void>();

const sleep = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

const redactedUrl = (url: URL): string => `${url.origin}/***`;

const retryAfterMs = (value: string | null | undefined): number => {
  if (!value) return 0;
  const seconds = Number(value);

  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1_000;
  const at = Date.parse(value);

  return Number.isNaN(at) ? 0 : Math.max(0, at - Date.now());
};

/** The first string `code` (e.g. ECONNRESET) along a thrown value's cause chain. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: walks the cause chain of an arbitrary thrown value
const networkMessage = (error: unknown): string => {
  const seen = new Set<object>();

  for (let current = error; isObject(current) && !seen.has(current); current = 'cause' in current ? current.cause : undefined) {
    seen.add(current);
    const code = 'code' in current ? current.code : undefined;

    if (isString(code)) return `RPC network error: ${code}`;
  }

  return 'RPC network error: fetch failed';
};

/*
 * What viem's http() transport (called via transport.config.request, i.e. without viem's buildRequest/withRetry)
 * gives classify():
 * - RpcRequestError: JSON-RPC error body, whatever the HTTP status. .code + .details (provider message); .cause is
 *   the raw { code, message, data }. No status/headers, so those come from the onFetchResponse hook.
 * - HttpRequestError with .status: non-2xx without a JSON-RPC error body; .headers, .details (body or statusText).
 * - HttpRequestError without .status: fetch/body-read failure (.cause chain carries e.g. ECONNRESET) or, on a 2xx,
 *   an unparsable body (SyntaxError cause).
 * - TimeoutError: per-transport timeout; AbortError never occurs since we pass no signal.
 * viem's messages embed the full endpoint URL (key in path), so only .details/.code/.status are ever surfaced.
 */
const timeoutMessage = /timed? ?out|timeout/i;

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: maps whatever the transport threw into an RpcError
const failureOf = (error: unknown, response: Response | undefined): RpcError => {
  if (error instanceof RpcError) return error;

  if (error instanceof RpcRequestError) {
    const rpc = { code: error.code, message: error.details };
    const kind = classify(response?.status, rpc);
    const timeout = kind === 'transient' && timeoutMessage.test(rpc.message);

    return new RpcError(kind, rpc.message, { code: rpc.code, cause: error.cause, timeout });
  }

  if (error instanceof TimeoutError) {
    return new RpcError('transient', 'RPC request timed out', { cause: { name: error.name, details: error.details }, timeout: true });
  }

  if (error instanceof HttpRequestError) {
    const status = error.status ?? response?.status;
    const cause = { name: error.name, status, details: error.details, cause: error.cause };

    if (error.status !== undefined) {
      return new RpcError(classify(status, error.details), `HTTP ${status}`, { cause, timeout: status === 408 || status === 504 });
    }

    if (response?.ok && error.cause instanceof SyntaxError) {
      return new RpcError('fatal', 'Malformed JSON-RPC response', { cause });
    }

    const message = networkMessage(error.cause);

    return new RpcError(classify(status, message), message, { cause });
  }

  const message = networkMessage(error);

  return new RpcError(classify(response?.status, message), message, { cause: error });
};

export function createRpcClient(options: RpcClientOptions): RpcClient {
  if (options.urls.length === 0) throw new Error('At least one RPC URL is required');

  const endpoints: Endpoint[] = options.urls.map(value => {
    const url = new URL(value);

    return {
      // viem does HTTP/JSON-RPC mechanics only: no retries, no batching, no body-size cap (wide getLogs can be 100s of MB).
      // config.request is the bare transport function, so viem's buildRequest retry/shouldRetry/error remapping never runs.
      send: http(url.href, {
        batch: false,
        retryCount: 0,
        timeout: options.timeoutMs,
        maxResponseBodySize: false,
        onFetchResponse: response => attemptResponse.getStore()?.(response),
      })({ retryCount: 0 }).config.request,
      displayUrl: redactedUrl(url),
      failures: 0,
      openUntil: 0,
      probeInFlight: false,
      unsupportedUntil: new Map(),
    };
  });

  const counters: Pick<RpcStats, 'requests' | 'attempts' | 'byMethod' | 'errors'> = {
    requests: 0,
    attempts: 0,
    byMethod: {},
    errors: {},
  };

  const bucketCapacity = Math.max(1, options.maxRps);
  let tokens = bucketCapacity;
  let refilledAt = performance.now();

  const takeToken = async (): Promise<void> => {
    while (true) {
      const now = performance.now();
      tokens = Math.min(bucketCapacity, tokens + (now - refilledAt) * options.maxRps / 1_000);
      refilledAt = now;

      if (tokens >= 1) {
        tokens -= 1;

        return;
      }

      await sleep(Math.max(1, Math.ceil((1 - tokens) * 1_000 / options.maxRps)));
    }
  };

  const stateOf = (endpoint: Endpoint, now = Date.now()): 'closed' | 'open' | 'half-open' => {
    if (endpoint.openUntil === 0) return 'closed';

    return now < endpoint.openUntil ? 'open' : 'half-open';
  };

  const chooseEndpoint = (method: string, offset: number): Endpoint | undefined => {
    const now = Date.now();

    for (let step = 0; step < endpoints.length; step += 1) {
      const endpoint = endpoints[(offset + step) % endpoints.length];
      const unsupportedUntil = endpoint.unsupportedUntil.get(method) ?? 0;

      if (unsupportedUntil > now) continue;

      if (unsupportedUntil !== 0) endpoint.unsupportedUntil.delete(method);
      const state = stateOf(endpoint, now);

      if (state === 'open' || (state === 'half-open' && endpoint.probeInFlight)) continue;

      if (state === 'half-open') endpoint.probeInFlight = true;

      return endpoint;
    }

    return undefined;
  };

  const recordFailure = (endpoint: Endpoint, kind: RpcErrorKind): void => {
    counters.errors[kind] = (counters.errors[kind] ?? 0) + 1;

    if (kind !== 'transient' && kind !== 'rate_limited') {
      // The endpoint answered; the error is about the request, not endpoint health.
      recordSuccess(endpoint);

      return;
    }

    endpoint.failures += 1;
    endpoint.probeInFlight = false;

    if (endpoint.failures >= BREAKER_FAILURES) endpoint.openUntil = Date.now() + OPEN_MS;
  };

  const recordSuccess = (endpoint: Endpoint): void => {
    endpoint.failures = 0;
    endpoint.openUntil = 0;
    endpoint.probeInFlight = false;
  };

  const request = async <T>(method: string, params: unknown[], requestOptions: RpcRequestOptions = {}): Promise<T> => {
    counters.requests += 1;
    const { maxAttempts } = options;

    let lastError: RpcError | undefined;
    let endpointOffset = 0;

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const endpoint = chooseEndpoint(method, endpointOffset);

      if (!endpoint) {
        throw lastError ?? new RpcError('transient', 'No RPC endpoint is currently available');
      }

      endpointOffset = (endpoints.indexOf(endpoint) + 1) % endpoints.length;
      await takeToken();
      counters.attempts += 1;
      counters.byMethod[method] = (counters.byMethod[method] ?? 0) + 1;

      let response: Response | undefined;
      const startedAt = performance.now();

      const report = (ok: boolean): void => {
        if (!requestOptions.onAttempt) return;
        const length = response && !response.headers.get('content-encoding') ? Number(response.headers.get('content-length') ?? NaN) : NaN;
        requestOptions.onAttempt({ ok, durationMs: performance.now() - startedAt, bytes: Number.isFinite(length) ? length : undefined });
      };

      try {
        const result = await attemptResponse.run(
          value => { response = value; },
          () => endpoint.send({ method, params }),
        );

        if (result === undefined) throw new RpcError('fatal', 'Malformed JSON-RPC response');

        if (requestOptions.validate && !requestOptions.validate(result)) {
          throw new RpcError('transient', 'RPC endpoint returned an invalid result');
        }

        recordSuccess(endpoint);
        report(true);

        // SAFETY: T is the caller's declared result type for `method`: the transport returns the JSON-RPC `result`
        // parsed from the response body, checked by the caller's `validate` above where the shape is not guaranteed.
        return result as T;
      } catch (cause) {
        const error = failureOf(cause, response);
        lastError = error;
        report(false);

        if (error.timeout && requestOptions.failFastOnTimeout) {
          // The caller will shrink the request: count the error, but not against endpoint health.
          counters.errors[error.kind] = (counters.errors[error.kind] ?? 0) + 1;
          endpoint.probeInFlight = false;
          throw error;
        }

        recordFailure(endpoint, error.kind);

        if (error.kind === 'unsupported') {
          endpoint.unsupportedUntil.set(method, Date.now() + UNSUPPORTED_MS);
          continue;
        } else if (error.kind === 'too_large' || error.kind === 'fatal') {
          throw error;
        }

        if (attempt + 1 < maxAttempts) {
          const cap = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempt);
          const headers = response?.headers ?? (cause instanceof HttpRequestError ? cause.headers : undefined);
          // Honour Retry-After, but never hold an API request longer than RETRY_AFTER_CAP_MS per attempt.
          await sleep(Math.max(Math.min(retryAfterMs(headers?.get('retry-after')), RETRY_AFTER_CAP_MS), Math.random() * cap));
        }
      }
    }

    throw lastError ?? new RpcError('transient', 'RPC request failed');
  };

  return {
    request,
    stats: (): RpcStats => ({
      requests: counters.requests,
      attempts: counters.attempts,
      byMethod: { ...counters.byMethod },
      errors: { ...counters.errors },
      endpoints: endpoints.map(endpoint => ({
        url: endpoint.displayUrl,
        state: stateOf(endpoint),
        failures: endpoint.failures,
      })),
    }),
  };
}
