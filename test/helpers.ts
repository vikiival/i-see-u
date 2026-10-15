import type { Address, Hex } from '../src/core/types.ts';

/** Narrows a string to a 0x-prefixed hex string without an assertion. */
export const isHex = (value: string): value is Hex => value.startsWith('0x');

/** Lowercases an address; the 0x prefix is rebuilt so no type assertion is needed. */
export const lowerAddress = (value: string): Address => `0x${value.slice(2).toLowerCase()}`;

/** Test-double version of an adapter's normalizeAddress: accepts any-case 20-byte hex, returns it lowercased. */
export const normalizeTestAddress = (input: string): Address | undefined =>
  /^0x[0-9a-f]{40}$/i.test(input) ? lowerAddress(input) : undefined;

/**
 * A fake RpcClient.request<T>() cannot know the caller's T, and malformed-response tests need to hand a deliberately
 * wrong shape to typed code. Either way the test, not the compiler, owns the shape.
 */
export const staged = <T>(value: NonNullable<unknown>): T =>
  // SAFETY: the test stages this payload for the method or boundary under test, so it controls the shape that T claims.
  value as T;

/** Reads positional JSON-RPC param `index`; the test fake knows which method it is answering and thus the shape. */
export const rpcParam = <T>(params: readonly unknown[], index: number): T =>
  // SAFETY: the fake only reads params for the method it is staged to answer, where the test controls the shape.
  params[index] as T;
