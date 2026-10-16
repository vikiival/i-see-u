import { customType } from 'drizzle-orm/pg-core';

import type { Hex, TokenStandard } from '../core/types.ts';

/** All wire/domain conversions live here. PostgreSQL bytea has no length modifier: schema checks enforce widths. */
export const binaryHex = customType<{ data: Hex; driverData: Buffer; config: { bytes: number } }>({
  dataType: () => 'bytea',
  toDriver(value) {
    if (!/^0x(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(value)) throw new Error('Invalid normalized binary hex');

    return Buffer.from(value.slice(2), 'hex');
  },
  fromDriver(value) { return `0x${Buffer.from(value).toString('hex')}`; },
});

export const uint256 = customType<{ data: bigint; driverData: string }>({
  dataType: () => 'numeric(78,0)',
  toDriver: (value) => value.toString(),
  fromDriver: (value) => BigInt(value),
});

export const standard = customType<{ data: TokenStandard; driverData: number }>({
  dataType: () => 'smallint',
  toDriver: (value) => value === 'erc20' ? 20 : 721,
  fromDriver: (value) => value === 20 ? 'erc20' : 'erc721',
});
