import type { Address } from '../src/core/types.ts';

/** Lowercases an address; the 0x prefix is rebuilt so no type assertion is needed. */
export const lowerAddress = (value: string): Address => `0x${value.slice(2).toLowerCase()}`;

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
