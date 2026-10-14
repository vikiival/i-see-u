import type { Address, ChunkAggregate, TransferEvent } from './types.ts';

export const emptyAggregate = (): ChunkAggregate => ({ tokens: new Map(), nfts: new Map(), selfSeen: new Set() });

/** Number of distinct matching Transfers represented by an aggregate; a self-transfer contributes both sides once. */
export const aggregateLogCount = (aggregate: ChunkAggregate): number =>
  [...aggregate.tokens.values()].reduce((sum, token) => sum + token.sentCount + token.receivedCount, 0)
  - aggregate.selfSeen.size;

/** Merge disjoint block chunks for diagnostics/scripts; the database performs the durable equivalent. */
export function mergeAggregate(into: ChunkAggregate, from: ChunkAggregate): void {
  for (const [token, incoming] of from.tokens) {
    const current = into.tokens.get(token);
    if (!current) into.tokens.set(token, { ...incoming });
    else {
      if (incoming.firstBlock < current.firstBlock) current.firstBlock = incoming.firstBlock;
      if (incoming.lastBlock > current.lastBlock) current.lastBlock = incoming.lastBlock;
      current.sentCount += incoming.sentCount;
      current.receivedCount += incoming.receivedCount;
    }
  }
  for (const [key, incoming] of from.nfts) {
    const current = into.nfts.get(key);
    if (!current || incoming.block > current.block || (incoming.block === current.block && incoming.logIndex > current.logIndex)) {
      into.nfts.set(key, { ...incoming });
    }
  }
  for (const key of from.selfSeen) into.selfSeen.add(key);
}

/**
 * Fold one decoded Transfer into a chunk aggregate. Only a self-transfer (from == to == account) is returned by
 * both the sent and the received query, so only those are deduped by (txHash, logIndex).
 */
export function addTransfer(agg: ChunkAggregate, account: Address, event: TransferEvent): void {
  const sent = event.from === account;
  const received = event.to === account;
  if (!sent && !received) return;
  if (sent && received) {
    const key = `${event.transactionHash}:${event.logIndex}`;
    if (agg.selfSeen.has(key)) return;
    agg.selfSeen.add(key);
  }

  const h = agg.tokens.get(event.token);
  if (h) {
    if (event.blockNumber < h.firstBlock) h.firstBlock = event.blockNumber;
    if (event.blockNumber > h.lastBlock) h.lastBlock = event.blockNumber;
    if (sent) h.sentCount += 1;
    if (received) h.receivedCount += 1;
  } else {
    agg.tokens.set(event.token, {
      standard: event.standard,
      firstBlock: event.blockNumber,
      lastBlock: event.blockNumber,
      sentCount: sent ? 1 : 0,
      receivedCount: received ? 1 : 0,
    });
  }

  if (event.standard === 'erc721') {
    const key = `${event.token}:${event.value}`;
    const n = agg.nfts.get(key);
    if (!n || event.blockNumber > n.block || (event.blockNumber === n.block && event.logIndex > n.logIndex)) {
      agg.nfts.set(key, { token: event.token, tokenId: event.value, held: received, block: event.blockNumber, logIndex: event.logIndex });
    }
  }
}
