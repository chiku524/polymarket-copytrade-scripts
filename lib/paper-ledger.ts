import {
  getPaperLedger,
  setPaperLedger,
  type PaperLedger,
  type PaperLedgerLot,
} from "@/lib/kv";
import { getMarketsByConditionIds, type ClobMarketStatus } from "@/lib/polymarket";

export interface PaperLedgerSummary {
  totalLots: number;
  openLots: number;
  settledLots: number;
  totalStakedUsd: number;
  openCostUsd: number;
  openMarkValueUsd: number;
  unrealizedPnlUsd: number;
  realizedPnlUsd: number;
  totalPnlUsd: number;
  lastEntryAt?: number;
  lastSettledAt?: number;
}

export interface PaperLedgerSnapshot {
  summary: PaperLedgerSummary;
  recentLots: PaperLedgerLot[];
}

function toNum(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function tokenMarkPrice(
  lot: Pick<PaperLedgerLot, "asset">,
  market: ClobMarketStatus | undefined
): number | undefined {
  if (!market || !Array.isArray(market.tokens)) return undefined;
  const token = market.tokens.find((t) => t.token_id === lot.asset);
  if (!token) return undefined;
  if (token.winner === true) return 1;
  if (token.winner === false) return 0;
  const price = toNum(token.price);
  if (price >= 0 && price <= 1) return price;
  return undefined;
}

function tokenSettlementPrice(
  lot: Pick<PaperLedgerLot, "asset">,
  market: ClobMarketStatus | undefined
): number | undefined {
  if (!market || market.closed !== true) return undefined;
  const token = market.tokens?.find((t) => t.token_id === lot.asset);
  if (!token) return undefined;
  if (token.winner === true) return 1;
  if (token.winner === false) return 0;
  const price = toNum(token.price);
  if (price === 0 || price === 1) return price;
  return undefined;
}

function buildSummary(ledger: PaperLedger, marketsByCondition: Map<string, ClobMarketStatus>): PaperLedgerSummary {
  let openLots = 0;
  let settledLots = 0;
  let totalStakedUsd = 0;
  let openCostUsd = 0;
  let openMarkValueUsd = 0;
  let realizedPnlUsd = 0;
  let lastEntryAt = 0;
  let lastSettledAt = ledger.lastSettledAt ?? 0;

  for (const lot of ledger.lots) {
    totalStakedUsd += lot.amountUsd;
    if (lot.timestamp > lastEntryAt) lastEntryAt = lot.timestamp;
    if (lot.settledAt && lot.settledAt > lastSettledAt) lastSettledAt = lot.settledAt;
    if (lot.settledAt) {
      settledLots += 1;
      realizedPnlUsd += lot.realizedPnlUsd ?? 0;
      continue;
    }
    openLots += 1;
    openCostUsd += lot.amountUsd;
    const market = marketsByCondition.get(lot.conditionId);
    const markPrice = tokenMarkPrice(lot, market);
    const effectiveMarkPrice = typeof markPrice === "number" ? markPrice : lot.price;
    openMarkValueUsd += lot.shares * effectiveMarkPrice;
  }

  const unrealizedPnlUsd = openMarkValueUsd - openCostUsd;
  return {
    totalLots: ledger.lots.length,
    openLots,
    settledLots,
    totalStakedUsd,
    openCostUsd,
    openMarkValueUsd,
    unrealizedPnlUsd,
    realizedPnlUsd,
    totalPnlUsd: realizedPnlUsd + unrealizedPnlUsd,
    lastEntryAt: lastEntryAt > 0 ? lastEntryAt : undefined,
    lastSettledAt: lastSettledAt > 0 ? lastSettledAt : undefined,
  };
}

export async function getPaperLedgerSnapshot(): Promise<PaperLedgerSnapshot> {
  const ledger = await getPaperLedger();
  if (ledger.lots.length === 0) {
    return {
      summary: {
        totalLots: 0,
        openLots: 0,
        settledLots: 0,
        totalStakedUsd: 0,
        openCostUsd: 0,
        openMarkValueUsd: 0,
        unrealizedPnlUsd: 0,
        realizedPnlUsd: 0,
        totalPnlUsd: 0,
      },
      recentLots: [],
    };
  }

  const unresolvedConditionIds = Array.from(
    new Set(ledger.lots.filter((lot) => !lot.settledAt).map((lot) => lot.conditionId))
  );
  const marketsByCondition =
    unresolvedConditionIds.length > 0
      ? await getMarketsByConditionIds(unresolvedConditionIds).catch(() => new Map<string, ClobMarketStatus>())
      : new Map<string, ClobMarketStatus>();

  let changed = false;
  let newestSettlementTs = ledger.lastSettledAt ?? 0;
  const settledLots = ledger.lots.map((lot) => {
    if (lot.settledAt) return lot;
    const settlementPrice = tokenSettlementPrice(lot, marketsByCondition.get(lot.conditionId));
    if (settlementPrice == null) return lot;
    changed = true;
    const settledAt = Date.now();
    if (settledAt > newestSettlementTs) newestSettlementTs = settledAt;
    const realizedPnlUsd = lot.shares * settlementPrice - lot.amountUsd;
    return {
      ...lot,
      settledAt,
      settledPrice: settlementPrice,
      settledWinner: settlementPrice >= 0.5,
      realizedPnlUsd,
    };
  });

  const nextLedger =
    changed || newestSettlementTs !== (ledger.lastSettledAt ?? 0)
      ? {
          ...ledger,
          lots: settledLots,
          lastSettledAt: newestSettlementTs > 0 ? newestSettlementTs : undefined,
          lastUpdatedAt: Date.now(),
        }
      : ledger;
  if (changed) {
    await setPaperLedger(nextLedger);
  }

  const summary = buildSummary(nextLedger, marketsByCondition);
  return {
    summary,
    recentLots: nextLedger.lots.slice(0, 80),
  };
}
