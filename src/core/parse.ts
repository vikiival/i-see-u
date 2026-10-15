// Primitive guards for boundary parsers (JSON-RPC results, HyperSync responses, thrown values). Code past a
// boundary works with the domain types those parsers return and never needs these.

// oxlint-disable-next-line anti-slop/no-runtime-typeof -- boundary parser: the primitive check every wire parser builds on
export const isString = (value: unknown): value is string => typeof value === 'string';

// oxlint-disable-next-line anti-slop/no-runtime-typeof -- boundary parser: the primitive check every wire parser builds on
export const isNumber = (value: unknown): value is number => typeof value === 'number';

/** A non-null object (arrays included, functions excluded), whose fields can then be read with `in`. */
// oxlint-disable-next-line anti-slop/no-runtime-typeof -- boundary parser: the primitive check every wire parser builds on
export const isObject = (value: unknown): value is object => typeof value === 'object' && value !== null;

/** The message of a thrown value: an Error's message, otherwise the value's string form. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary parser: `catch` and promise rejections bind unknown
export const thrownMessage = (error: unknown): string => error instanceof Error ? error.message : String(error);

/** Lowercase a 0x-prefixed value. Equal to `value.toLowerCase()`, since the prefix lowercases to `0x`. */
export const toLowerHex = (value: `0x${string}`): `0x${string}` => `0x${value.slice(2).toLowerCase()}`;
