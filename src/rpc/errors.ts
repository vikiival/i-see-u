import type { BlockRange, RpcErrorKind } from '../core/types.ts';
import { isNumber, isObject, isString } from '../core/parse.ts';

/** What classification reads from a failure: a numeric JSON-RPC code and the provider's message. */
interface Failure {
  readonly code: number | undefined;
  readonly message: string;
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: thrown values and JSON-RPC error bodies are untyped
const parseFailure = (error: unknown): Failure => {
  if (isString(error)) {
    return { code: undefined, message: error };
  }

  if (isObject(error)) {
    const message = 'message' in error ? error.message : undefined;
    const code = 'code' in error ? error.code : undefined;

    return {
      code: isNumber(code) ? code : undefined,
      message: isString(message) ? message : isString(code) ? code : String(error),
    };
  }

  return { code: undefined, message: String(error ?? 'Unknown RPC error') };
};

const parseSuggestedRange = (message: string): BlockRange | undefined => {
  const match = message.match(/block range\s*\[\s*(0x[0-9a-f]+)\s*,\s*(0x[0-9a-f]+)\s*\]/i);

  if (!match) {
    return undefined;
  }

  const fromBlock = BigInt(match[1]);
  const toBlock = BigInt(match[2]);

  return fromBlock <= toBlock ? { fromBlock, toBlock } : undefined;
};

/** Classify an HTTP, JSON-RPC, or network failure into caller-facing policy. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: takes any raw failure and parses it with parseFailure
export function classify(httpStatus: number | undefined, error: unknown): RpcErrorKind {
  if (httpStatus === 429) {
    return 'rate_limited';
  }

  if (httpStatus === 413) {
    return 'too_large';
  }

  if (httpStatus === 401 || httpStatus === 403) {
    return 'fatal';
  }

  if (httpStatus !== undefined && httpStatus >= 500) {
    return 'transient';
  }

  const failure = parseFailure(error);
  const { code } = failure;
  const message = failure.message.toLowerCase();

  if (
    /query returned more than \d+ results|ranges? over \d+ blocks? (?:are|is) not supported|max range\s*:|block range too large|response size exceeded/.test(
      message,
    )
  ) {
    return 'too_large';
  }

  if (
    code === -32010 ||
    code === -32007 ||
    /rate[ -]?limit|request limit reached|requests? limited to \d+\s*\/\s*(?:sec|second)/.test(message)
  ) {
    return 'rate_limited';
  }

  if (/method not (?:available|found)|archive requests? require|please specify an address/.test(message)) {
    return 'unsupported';
  }

  if (
    /timed? ?out|timeout|econnreset|econnrefused|socket hang up|network error|fetch failed|header not found|header for hash not found|unknown block|block not found/.test(
      message,
    )
  ) {
    return 'transient';
  }

  return 'fatal';
}

export class RpcError extends Error {
  readonly kind: RpcErrorKind;
  readonly code?: number;
  /** Sub-range parsed from provider messages like "Try with this block range [0x.., 0x..]". */
  readonly suggestedRange?: BlockRange;
  /** Set on transient errors caused by a client, gateway, or provider-side timeout. */
  readonly timeout?: boolean;

  constructor(
    kind: RpcErrorKind,
    message: string,
    options: { code?: number; timeout?: boolean; cause?: unknown } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = 'RpcError';
    this.kind = kind;
    this.code = options.code;
    this.suggestedRange = parseSuggestedRange(message);

    if (options.timeout) {
      this.timeout = true;
    }
  }
}

/** The RpcError inside a (possibly viem-wrapped) error's cause chain. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: walks the cause chain of an arbitrary thrown value
export function rpcErrorOf(error: unknown): RpcError | undefined {
  for (let current = error, depth = 0; isObject(current) && depth < 16; depth += 1) {
    if (current instanceof RpcError) {
      return current;
    }

    current = 'cause' in current ? current.cause : undefined;
  }

  return undefined;
}
