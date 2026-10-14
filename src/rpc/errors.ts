import type { BlockRange, RpcErrorKind, RpcErrorShape } from '../core/types.ts';

interface JsonRpcErrorLike {
  code?: number;
  message?: string;
}

export interface RpcErrorOptions {
  code?: number;
  suggestedRange?: BlockRange;
  timeout?: boolean;
  cause?: unknown;
}

const messageOf = (error: unknown): string => {
  if (typeof error === 'string') return error;
  if (error && typeof error === 'object') {
    const message = (error as { message?: unknown }).message;
    if (typeof message === 'string') return message;
    const code = (error as { code?: unknown }).code;
    if (typeof code === 'string') return code;
  }
  return String(error ?? 'Unknown RPC error');
};

export function parseSuggestedRange(message: string): BlockRange | undefined {
  const match = message.match(/block range\s*\[\s*(0x[0-9a-f]+)\s*,\s*(0x[0-9a-f]+)\s*\]/i);
  if (!match) return undefined;
  try {
    const fromBlock = BigInt(match[1]);
    const toBlock = BigInt(match[2]);
    return fromBlock <= toBlock ? { fromBlock, toBlock } : undefined;
  } catch {
    return undefined;
  }
}

/** Classify an HTTP, JSON-RPC, or network failure into caller-facing policy. */
export function classify(httpStatus: number | undefined, error: unknown): RpcErrorKind {
  if (httpStatus === 429) return 'rate_limited';
  if (httpStatus === 413) return 'too_large';
  if (httpStatus === 401 || httpStatus === 403) return 'fatal';
  if (httpStatus !== undefined && (httpStatus >= 500 || httpStatus === 525)) return 'transient';

  const rpc = error && typeof error === 'object' ? error as JsonRpcErrorLike : undefined;
  const code = rpc?.code;
  const message = messageOf(error).toLowerCase();

  if (
    /query returned more than \d+ results|ranges? over \d+ blocks? (?:are|is) not supported|max range\s*:|block range too large|response size exceeded/.test(message)
  ) return 'too_large';

  if (
    code === -32010 || code === -32007 ||
    /rate[ -]?limit|request limit reached|requests? limited to \d+\s*\/\s*(?:sec|second)/.test(message)
  ) return 'rate_limited';

  if (
    /method not (?:available|found)|archive requests? require|please specify an address/.test(message)
  ) return 'unsupported';

  if (
    /timed? ?out|timeout|econnreset|econnrefused|socket hang up|network error|fetch failed|header not found|unknown block/.test(message)
  ) return 'transient';

  return 'fatal';
}

export class RpcError extends Error implements RpcErrorShape {
  readonly kind: RpcErrorKind;
  readonly code?: number;
  readonly suggestedRange?: BlockRange;
  readonly timeout?: boolean;

  constructor(kind: RpcErrorKind, message: string, options: RpcErrorOptions = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = 'RpcError';
    this.kind = kind;
    this.code = options.code;
    this.suggestedRange = options.suggestedRange ?? parseSuggestedRange(message);
    if (options.timeout) this.timeout = true;
  }
}
