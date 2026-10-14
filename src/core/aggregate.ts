import type { Address, ChunkAggregate, TransferEvent } from './types.ts';

export const emptyAggregate = (): ChunkAggregate => ({ tokens: new Map(), nfts: new Map(), selfSeen: new Set() });

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
