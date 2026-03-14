import { ClobClient, OrderType, Side } from "@polymarket/clob-client";
import { Wallet } from "ethers";
import { getCashBalance, type CopiedTrade } from "@/lib/copy-trade";
import { getPositions } from "@/lib/polymarket";

const DATA_API = "https://data-api.polymarket.com";
const CLOB_HOST = "https://clob.polymarket.com";
const CHAIN_ID = 137;
const POLYMARKET_MIN_ORDER_USD = 1;
const DEFAULT_MAX_UNRESOLVED_IMBALANCES_PER_RUN = 1;
const DEFAULT_UNWIND_SELL_SLIPPAGE = 0.03;
const DEFAULT_UNWIND_SHARE_BUFFER = 0.99;
const MIN_LIVE_BUY_PRICE_BUFFER = 0.01;

type TradingMode = "off" | "paper" | "live";
type PairCoin = "BTC" | "ETH";
type PairCadence = "5m" | "15m" | "hourly" | "other";

interface GlobalTrade {
  conditionId: string;
  title?: string;
  slug?: string;
  asset: string;
  outcome: string;
  size?: number | string;
  price: number | string;
  timestamp: number | string;
}

interface OutcomeSnapshot {
  asset: string;
  outcome: string;
  price: number;
  timestamp: number;
}

interface PairSignal {
  conditionId: string;
  title: string;
  slug?: string;
  coin: PairCoin;
  cadence: PairCadence;
  latestTimestamp: number;
  pairSum: number;
  edge: number;
  recentTradeCount: number;
  recentNotionalUsd: number;
  outcomes: [OutcomeSnapshot, OutcomeSnapshot];
}

export interface SignalBreakdown {
  byCoin: Record<PairCoin, number>;
  byCadence: Record<PairCadence, number>;
}

interface TradeConditionSnapshot {
  latestTimestamp: number;
  byOutcome: Map<string, OutcomeSnapshot>;
  recentTradeCount: number;
  recentNotionalUsd: number;
  identityHint?: { coin: PairCoin; cadence: PairCadence };
}

interface ClobMarketToken {
  token_id?: string;
  outcome?: string;
  price?: number | string;
}

interface ClobMarket {
  question?: string;
  market_slug?: string;
  active?: boolean;
  closed?: boolean;
  accepting_orders?: boolean;
  enable_order_book?: boolean;
  tokens?: ClobMarketToken[];
}

interface SignalBuildResult {
  signals: PairSignal[];
  diagnostics: Record<string, number>;
}

export interface PairedStrategyResult {
  copied: number;
  failed: number;
  paper: number;
  simulatedVolumeUsd: number;
  mode: TradingMode;
  budgetCapUsd: number;
  budgetUsedUsd: number;
  evaluatedSignals: number;
  eligibleSignals: number;
  rejectedReasons: Record<string, number>;
  evaluatedBreakdown: SignalBreakdown;
  eligibleBreakdown: SignalBreakdown;
  executedBreakdown: SignalBreakdown;
  error?: string;
  lastTimestamp?: number;
  unresolvedExposureAssets: string[];
  copiedKeys: string[];
  copiedTrades: CopiedTrade[];
  /** Sum of edge cents across executed pair entries (for averaging) */
  executedEdgeCentsSum: number;
  /** Average edge cents across executed pair entries */
  avgExecutedEdgeCents?: number;
  /** Sum of net edge cents after fee/slippage penalties */
  executedNetEdgeCentsSum: number;
  /** Average net edge cents after fee/slippage penalties */
  avgExecutedNetEdgeCents?: number;
  /** Best edge among evaluated signals (cents), for diagnostics */
  _maxEdgeCents?: number;
  /** Best net edge after penalties among evaluated signals (cents) */
  _maxNetEdgeCents?: number;
  /** Lowest pairSum among evaluated signals, for diagnostics */
  _minPairSum?: number;
}

function toNum(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function toEdge(value: unknown, fallbackCents = 0): number {
  const centsRaw = Number(value);
  const cents = Number.isFinite(centsRaw) ? centsRaw : fallbackCents;
  return Math.max(0, Math.min(50, cents)) / 100;
}

function toSignedEdge(value: unknown, fallbackCents = 0): number {
  const centsRaw = Number(value);
  const cents = Number.isFinite(centsRaw) ? centsRaw : fallbackCents;
  return Math.max(-50, Math.min(50, cents)) / 100;
}

function clipError(current: string | undefined, message: string): string {
  const next = current ? `${current}; ${message}` : message;
  return next.length > 500 ? `${next.slice(0, 497)}...` : next;
}

function responseOk(resp: unknown): boolean {
  return !!(resp && typeof resp === "object" && "success" in resp && (resp as { success?: unknown }).success);
}

function responseError(resp: unknown, fallback: string): string {
  if (!resp || typeof resp !== "object") return fallback;
  const r = resp as { errorMsg?: unknown; message?: unknown; error?: unknown; status?: unknown };
  if (typeof r.errorMsg === "string" && r.errorMsg.trim()) return r.errorMsg;
  if (typeof r.message === "string" && r.message.trim()) return r.message;
  if (typeof r.error === "string" && r.error.trim()) return r.error;
  if (r.error && typeof r.error === "object") {
    const serial = JSON.stringify(r.error);
    if (serial) return serial.slice(0, 160);
  }
  if (typeof r.status === "number") return `HTTP ${r.status}`;
  return fallback;
}

function bumpReason(map: Record<string, number>, reason: string) {
  map[reason] = (map[reason] ?? 0) + 1;
}

function emptyBreakdown(): SignalBreakdown {
  return {
    byCoin: { BTC: 0, ETH: 0 },
    byCadence: { "5m": 0, "15m": 0, hourly: 0, other: 0 },
  };
}

function bumpBreakdown(target: SignalBreakdown, signal: Pick<PairSignal, "coin" | "cadence">) {
  target.byCoin[signal.coin] = (target.byCoin[signal.coin] ?? 0) + 1;
  target.byCadence[signal.cadence] = (target.byCadence[signal.cadence] ?? 0) + 1;
}

function estimateNetEdgeCents(params: {
  edge: number;
  pairSum: number;
  pairFeeBps: number;
  pairSlippageCents: number;
}): number {
  const feePenaltyCents = Math.max(0, params.pairSum) * (Math.max(0, params.pairFeeBps) / 10_000) * 100;
  const slippagePenaltyCents = Math.max(0, params.pairSlippageCents) * 2;
  return params.edge * 100 - feePenaltyCents - slippagePenaltyCents;
}

function freshnessDecayWeight(latestTimestampSec: number, nowSec: number, halfLifeSec: number): number {
  const effectiveHalfLife = Math.max(1, halfLifeSec);
  const ageSec = Math.max(0, nowSec - latestTimestampSec);
  return Math.exp((-Math.log(2) * ageSec) / effectiveHalfLife);
}

function detectUpDownIdentity(
  question: string,
  slug?: string
): { coin: PairCoin; cadence: PairCadence } | null {
  const q = question.toLowerCase();
  const s = String(slug ?? "").toLowerCase();
  const hay = `${q} ${s}`;

  let coin: PairCoin | null = null;
  if (
    hay.includes("bitcoin up or down") ||
    hay.includes("btc up or down") ||
    hay.includes("bitcoin-up-or-down") ||
    hay.includes("btc-updown")
  ) {
    coin = "BTC";
  } else if (
    hay.includes("ethereum up or down") ||
    hay.includes("eth up or down") ||
    hay.includes("ethereum-up-or-down") ||
    hay.includes("eth-updown")
  ) {
    coin = "ETH";
  }
  if (!coin) return null;

  let cadence: PairCadence = "other";
  if (s.includes("updown-5m")) cadence = "5m";
  else if (s.includes("updown-15m")) cadence = "15m";
  else if (
    s.includes("up-or-down") &&
    /(?:\d{1,2}(?:am|pm)-et)\b/.test(s) &&
    !s.includes("updown-5m") &&
    !s.includes("updown-15m")
  ) {
    cadence = "hourly";
  }

  return { coin, cadence };
}

const MARKET_CACHE_TTL_MS = 60_000;
const marketCache = new Map<string, { fetchedAt: number; market: ClobMarket | null }>();

async function getMarketCached(client: ClobClient, conditionId: string): Promise<ClobMarket | null> {
  const now = Date.now();
  const cached = marketCache.get(conditionId);
  if (cached && now - cached.fetchedAt < MARKET_CACHE_TTL_MS) {
    return cached.market;
  }
  try {
    const market = (await client.getMarket(conditionId)) as ClobMarket;
    marketCache.set(conditionId, { fetchedAt: now, market });
    return market;
  } catch {
    marketCache.set(conditionId, { fetchedAt: now, market: null });
    return null;
  }
}

function isCadenceEnabled(
  cadence: PairCadence,
  toggles: { cadence5m: boolean; cadence15m: boolean; cadenceHourly: boolean }
): boolean {
  if (cadence === "5m") return toggles.cadence5m;
  if (cadence === "15m") return toggles.cadence15m;
  if (cadence === "hourly") return toggles.cadenceHourly;
  return false;
}

async function getRecentPairSignals(params: {
  lookbackSeconds: number;
  includeBtc: boolean;
  includeEth: boolean;
  cadence5m: boolean;
  cadence15m: boolean;
  cadenceHourly: boolean;
  tradeLimit?: number;
  maxConditionsToInspect?: number;
}): Promise<SignalBuildResult> {
  const {
    lookbackSeconds,
    includeBtc,
    includeEth,
    cadence5m,
    cadence15m,
    cadenceHourly,
    tradeLimit = 5000,
    maxConditionsToInspect = 120,
  } = params;
  const nowSec = Math.floor(Date.now() / 1000);
  const res = await fetch(`${DATA_API}/trades?limit=${tradeLimit}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Trades fetch failed: ${res.status}`);
  const trades = (await res.json()) as GlobalTrade[];
  const grouped = new Map<string, TradeConditionSnapshot>();
  const diagnostics: Record<string, number> = {};

  for (const t of Array.isArray(trades) ? trades : []) {
    const conditionId = String(t.conditionId ?? "");
    if (!conditionId) continue;
    const ts = toNum(t.timestamp);
    if (!ts || nowSec - ts > lookbackSeconds) continue;
    const price = toNum(t.price);
    if (price <= 0 || price >= 1) continue;
    const outcome = String(t.outcome ?? "");
    const asset = String(t.asset ?? "");
    if (!outcome || !asset) continue;

    // Cheap pre-filter: most global trades are not BTC/ETH Up-Down.
    // Use trade-level title/slug hints to avoid unnecessary market lookups.
    const tradeTitle = String(t.title ?? "");
    const tradeSlug = String(t.slug ?? "");
    const hasIdentityHints = tradeTitle.length > 0 || tradeSlug.length > 0;
    const hintedIdentity = detectUpDownIdentity(tradeTitle, tradeSlug);
    if (hasIdentityHints && !hintedIdentity) {
      continue;
    }
    if (hintedIdentity) {
      if ((hintedIdentity.coin === "BTC" && !includeBtc) || (hintedIdentity.coin === "ETH" && !includeEth)) {
        bumpReason(diagnostics, "coin_disabled");
        continue;
      }
      if (!isCadenceEnabled(hintedIdentity.cadence, { cadence5m, cadence15m, cadenceHourly })) {
        bumpReason(diagnostics, "cadence_disabled");
        continue;
      }
    }

    const bucket = grouped.get(conditionId) ?? {
      latestTimestamp: 0,
      byOutcome: new Map<string, OutcomeSnapshot>(),
      recentTradeCount: 0,
      recentNotionalUsd: 0,
    };
    const current = bucket.byOutcome.get(outcome);
    if (!current || ts > current.timestamp) {
      bucket.byOutcome.set(outcome, {
        asset,
        outcome,
        price,
        timestamp: ts,
      });
    }
    bucket.latestTimestamp = Math.max(bucket.latestTimestamp, ts);
    bucket.recentTradeCount += 1;
    const tradeSize = Math.max(0, toNum(t.size));
    bucket.recentNotionalUsd += tradeSize > 0 ? tradeSize * price : price;
    if (hintedIdentity) {
      bucket.identityHint = hintedIdentity;
    }
    grouped.set(conditionId, bucket);
  }

  const marketClient = new ClobClient(CLOB_HOST, CHAIN_ID);
  const conditionIds = Array.from(grouped.entries())
    .sort((a, b) => b[1].latestTimestamp - a[1].latestTimestamp)
    .map(([conditionId]) => conditionId)
    .slice(0, maxConditionsToInspect);
  const marketsByCondition = new Map<string, ClobMarket | null>();
  const LOOKUP_BATCH_SIZE = 15;
  for (let i = 0; i < conditionIds.length; i += LOOKUP_BATCH_SIZE) {
    const batchIds = conditionIds.slice(i, i + LOOKUP_BATCH_SIZE);
    const batchMarkets = await Promise.all(batchIds.map((conditionId) => getMarketCached(marketClient, conditionId)));
    batchIds.forEach((conditionId, idx) => {
      marketsByCondition.set(conditionId, batchMarkets[idx] ?? null);
    });
  }

  const signals: PairSignal[] = [];
  for (const conditionId of conditionIds) {
    const groupedSnapshot = grouped.get(conditionId);
    if (!groupedSnapshot) continue;
    // Require at least 1 outcome from trades (we can use market token prices for the other)
    if (groupedSnapshot.byOutcome.size < 1) {
      bumpReason(diagnostics, "missing_any_outcome");
      continue;
    }

    const market = marketsByCondition.get(conditionId) ?? null;
    if (!market) {
      bumpReason(diagnostics, "market_lookup_failed");
      continue;
    }
    if (market.closed) {
      bumpReason(diagnostics, "market_closed");
      continue;
    }
    if (!market.active) {
      bumpReason(diagnostics, "market_inactive");
      continue;
    }
    if (!market.accepting_orders) {
      bumpReason(diagnostics, "market_not_accepting_orders");
      continue;
    }
    if (!market.enable_order_book) {
      bumpReason(diagnostics, "market_orderbook_disabled");
      continue;
    }
    const title = String(market.question ?? "");
    const slug = String(market.market_slug ?? "");
    const identity = groupedSnapshot.identityHint ?? detectUpDownIdentity(title, slug);
    if (!identity) {
      bumpReason(diagnostics, "market_not_btc_eth_updown");
      continue;
    }
    if ((identity.coin === "BTC" && !includeBtc) || (identity.coin === "ETH" && !includeEth)) {
      bumpReason(diagnostics, "coin_disabled");
      continue;
    }
    if (!isCadenceEnabled(identity.cadence, { cadence5m, cadence15m, cadenceHourly })) {
      bumpReason(diagnostics, "cadence_disabled");
      continue;
    }

    const tokens = Array.isArray(market.tokens) ? market.tokens.slice(0, 2) : [];
    if (tokens.length < 2) {
      bumpReason(diagnostics, "market_missing_tokens");
      continue;
    }

    const resolvedOutcomes = tokens
      .map((token) => {
        const outcome = String(token.outcome ?? "");
        const tokenId = String(token.token_id ?? "");
        const snapshot = groupedSnapshot.byOutcome.get(outcome);
        const price = snapshot?.price ?? toNum(token.price);
        const timestamp = snapshot?.timestamp ?? groupedSnapshot.latestTimestamp;
        if (!outcome || !tokenId || price <= 0 || price >= 1) return null;
        return {
          asset: tokenId,
          outcome,
          price,
          timestamp,
        } as OutcomeSnapshot;
      })
      .filter(Boolean) as OutcomeSnapshot[];

    if (resolvedOutcomes.length < 2) {
      bumpReason(diagnostics, "missing_valid_token_snapshot");
      continue;
    }
    const [first, second] = resolvedOutcomes as [OutcomeSnapshot, OutcomeSnapshot];
    if (first.asset === second.asset) {
      bumpReason(diagnostics, "duplicate_token_assets");
      continue;
    }

    const outcomes: [OutcomeSnapshot, OutcomeSnapshot] = [first, second];
    const pairSum = outcomes[0].price + outcomes[1].price;
    const edge = 1 - pairSum;
    signals.push({
      conditionId,
      title,
      slug,
      coin: identity.coin,
      cadence: identity.cadence,
      latestTimestamp: Math.max(outcomes[0].timestamp, outcomes[1].timestamp),
      pairSum,
      edge,
      recentTradeCount: groupedSnapshot.recentTradeCount,
      recentNotionalUsd: groupedSnapshot.recentNotionalUsd,
      outcomes,
    });
  }

  return {
    diagnostics,
    signals: signals.sort((a, b) => {
      if (b.edge !== a.edge) return b.edge - a.edge;
      return b.latestTimestamp - a.latestTimestamp;
    }),
  };
}

export async function runPairedStrategy(
  privateKey: string,
  myAddress: string,
  signatureType: number,
  config: {
    mode: TradingMode;
    walletUsagePercent: number;
    pairChunkUsd: number;
    maxRunBudgetUsd: number;
    minBetUsd: number;
    stopLossBalance: number;
    floorToPolymarketMin: boolean;
    pairMinEdgeCents: number;
    paperAllowNegativeEdge: boolean;
    paperMinEdgeCents: number;
    pairMinEdgeCents5m: number;
    pairMinEdgeCents15m: number;
    pairMinEdgeCentsHourly: number;
    pairFeeBps: number;
    pairSlippageCents: number;
    liveMinNetEdgeSurplusCents: number;
    adaptiveEdgeEnabled: boolean;
    adaptiveEdgeLowActivityTradeCount: number;
    adaptiveEdgeMaxPenaltyCents: number;
    adaptiveEdgeStalePenaltyCents: number;
    adaptiveEdgeStalePenaltyCents5m: number;
    adaptiveEdgeStalePenaltyCents15m: number;
    adaptiveEdgeStalePenaltyCentsHourly: number;
    freshnessMaxSignalAgeSec5m: number;
    freshnessMaxSignalAgeSec15m: number;
    freshnessMaxSignalAgeSecHourly: number;
    freshnessMaxExecutionQuoteAgeSec5m: number;
    freshnessMaxExecutionQuoteAgeSec15m: number;
    freshnessMaxExecutionQuoteAgeSecHourly: number;
    paperRelaxFreshness: boolean;
    paperFreshnessAgeMultiplier: number;
    dynamicSizingEnabled: boolean;
    dynamicSizingMinScalePct: number;
    dynamicSizingMaxScalePct: number;
    dynamicSizingEdgeTargetCents: number;
    dynamicSizingLiquidityTradeCount: number;
    pairLookbackSeconds: number;
    pairMaxMarketsPerRun: number;
    reentryMaxEntriesPerSignal: number;
    reentryEdgeStepCents: number;
    maxConditionExposureUsd: number;
    maxCoinExposureSharePct: number;
    maxCadenceExposureSharePct: number;
    autoExitResidualPositions: boolean;
    residualPositionMinUsd: number;
    residualPositionMaxPerRun: number;
    residualPositionSellDiscountCents: number;
    enableBtc: boolean;
    enableEth: boolean;
    enableCadence5m: boolean;
    enableCadence15m: boolean;
    enableCadenceHourly: boolean;
    maxUnresolvedImbalancesPerRun: number;
    unwindSellSlippageCents: number;
    unwindShareBufferPct: number;
  },
  state: { lastTimestamp: number; copiedKeys: string[] }
): Promise<PairedStrategyResult> {
  const mode = config.mode;
  const result: PairedStrategyResult = {
    copied: 0,
    failed: 0,
    paper: 0,
    simulatedVolumeUsd: 0,
    mode,
    budgetCapUsd: 0,
    budgetUsedUsd: 0,
    evaluatedSignals: 0,
    eligibleSignals: 0,
    rejectedReasons: {},
    evaluatedBreakdown: emptyBreakdown(),
    eligibleBreakdown: emptyBreakdown(),
    executedBreakdown: emptyBreakdown(),
    unresolvedExposureAssets: [],
    copiedKeys: [],
    copiedTrades: [],
    executedEdgeCentsSum: 0,
    executedNetEdgeCentsSum: 0,
  };
  const reject = (reason: string) => {
    result.rejectedReasons[reason] = (result.rejectedReasons[reason] ?? 0) + 1;
  };
  if (mode === "off") {
    result.error = "Trading mode is off";
    return result;
  }

  const cashBalance = await getCashBalance(myAddress);
  const walletUsagePercent = Math.max(1, Math.min(100, Number(config.walletUsagePercent) || 100));
  const walletRunCapUsd = (cashBalance * walletUsagePercent) / 100;
  const configuredRunBudgetUsd = Math.max(0, Number(config.maxRunBudgetUsd) || 0);
  const runBudgetCapUsd =
    configuredRunBudgetUsd > 0 ? Math.min(walletRunCapUsd, configuredRunBudgetUsd) : walletRunCapUsd;
  let remainingBudgetUsd = runBudgetCapUsd;
  result.budgetCapUsd = runBudgetCapUsd;

  if (mode === "live" && cashBalance < 1) {
    result.error = "Low balance";
    return result;
  }
  if (mode === "live" && config.stopLossBalance > 0 && cashBalance < config.stopLossBalance) {
    result.error = `Stop-loss: balance $${cashBalance.toFixed(2)} below threshold $${config.stopLossBalance}`;
    return result;
  }
  if (mode === "live" && runBudgetCapUsd < POLYMARKET_MIN_ORDER_USD * 2) {
    const capReason =
      configuredRunBudgetUsd > 0
        ? `effective run cap $${runBudgetCapUsd.toFixed(2)} (wallet cap $${walletRunCapUsd.toFixed(2)}, fixed cap $${configuredRunBudgetUsd.toFixed(2)})`
        : `${walletUsagePercent.toFixed(1)}% wallet cap of $${cashBalance.toFixed(2)} is $${runBudgetCapUsd.toFixed(2)}`;
    result.error = `Run budget too low: ${capReason} (< $2 for paired leg minimums)`;
    return result;
  }

  const defaultMinEdge = toEdge(config.pairMinEdgeCents, 0);
  const minEdgeByCadence: Record<PairCadence, number> = {
    "5m": toEdge(config.pairMinEdgeCents5m, defaultMinEdge * 100),
    "15m": toEdge(config.pairMinEdgeCents15m, defaultMinEdge * 100),
    hourly: toEdge(config.pairMinEdgeCentsHourly, defaultMinEdge * 100),
    other: defaultMinEdge,
  };
  const paperMinEdgeOverride =
    mode === "paper" && config.paperAllowNegativeEdge
      ? toSignedEdge(config.paperMinEdgeCents, -0.2)
      : null;
  const lookbackSeconds = Math.max(20, Number(config.pairLookbackSeconds) || 120);
  const maxMarketsPerRun = Math.max(1, Math.min(20, Number(config.pairMaxMarketsPerRun) || 4));
  const reentryMaxEntriesPerSignal = Math.max(
    1,
    Math.min(6, Math.floor(Number(config.reentryMaxEntriesPerSignal) || 1))
  );
  const reentryEdgeStepCents = Math.max(
    0.01,
    Math.min(10, Number(config.reentryEdgeStepCents) || 0.1)
  );
  const maxConditionExposureUsd = Math.max(0, Number(config.maxConditionExposureUsd) || 0);
  const pairChunkUsd = Math.max(1, Number(config.pairChunkUsd) || 3);
  const pairFeeBps = Math.max(0, Math.min(200, Number(config.pairFeeBps) || 0));
  const pairSlippageCents = Math.max(0, Math.min(25, Number(config.pairSlippageCents) || 0));
  const liveMinNetEdgeSurplusCents = Math.max(
    0,
    Math.min(10, Number(config.liveMinNetEdgeSurplusCents) || 0)
  );
  const adaptiveEdgeEnabled = config.adaptiveEdgeEnabled !== false;
  const adaptiveEdgeLowActivityTradeCount = Math.max(
    1,
    Math.min(200, Math.floor(Number(config.adaptiveEdgeLowActivityTradeCount) || 1))
  );
  const adaptiveEdgeMaxPenaltyCents = Math.max(
    0,
    Math.min(10, Number(config.adaptiveEdgeMaxPenaltyCents) || 0)
  );
  const adaptiveEdgeStalePenaltyCents = Math.max(
    0,
    Math.min(10, Number(config.adaptiveEdgeStalePenaltyCents) || 0)
  );
  const adaptiveEdgeStalePenaltyByCadenceCents: Record<PairCadence, number> = {
    "5m": (() => {
      const raw = Number(config.adaptiveEdgeStalePenaltyCents5m);
      const value = Number.isFinite(raw) ? raw : adaptiveEdgeStalePenaltyCents;
      return Math.max(0, Math.min(10, value));
    })(),
    "15m": (() => {
      const raw = Number(config.adaptiveEdgeStalePenaltyCents15m);
      const value = Number.isFinite(raw) ? raw : adaptiveEdgeStalePenaltyCents;
      return Math.max(0, Math.min(10, value));
    })(),
    hourly: (() => {
      const raw = Number(config.adaptiveEdgeStalePenaltyCentsHourly);
      const value = Number.isFinite(raw) ? raw : adaptiveEdgeStalePenaltyCents;
      return Math.max(0, Math.min(10, value));
    })(),
    other: adaptiveEdgeStalePenaltyCents,
  };
  const paperRelaxFreshness = mode === "paper" && config.paperRelaxFreshness === true;
  const paperFreshnessAgeMultiplier = paperRelaxFreshness
    ? Math.max(1, Math.min(4, Number(config.paperFreshnessAgeMultiplier) || 1.5))
    : 1;
  const dynamicSizingEnabled = config.dynamicSizingEnabled !== false;
  const dynamicSizingMinScale = Math.max(
    0.1,
    Math.min(2, (Number(config.dynamicSizingMinScalePct) || 100) / 100)
  );
  const dynamicSizingMaxScale = Math.max(
    dynamicSizingMinScale,
    Math.min(3, (Number(config.dynamicSizingMaxScalePct) || 100) / 100)
  );
  const dynamicSizingEdgeTargetCents = Math.max(
    0.05,
    Math.min(20, Number(config.dynamicSizingEdgeTargetCents) || 1)
  );
  const dynamicSizingLiquidityTradeCount = Math.max(
    1,
    Math.min(200, Math.floor(Number(config.dynamicSizingLiquidityTradeCount) || 1))
  );
  const maxCoinExposureSharePct = Math.max(
    0,
    Math.min(100, Number(config.maxCoinExposureSharePct) || 0)
  );
  const maxCadenceExposureSharePct = Math.max(
    0,
    Math.min(100, Number(config.maxCadenceExposureSharePct) || 0)
  );
  const autoExitResidualPositions = config.autoExitResidualPositions === true;
  const residualPositionMinUsd = Math.max(0.1, Number(config.residualPositionMinUsd) || 1);
  const residualPositionMaxPerRun = Math.max(
    1,
    Math.min(20, Math.floor(Number(config.residualPositionMaxPerRun) || 1))
  );
  const residualPositionSellDiscount = Math.max(
    0,
    Math.min(0.25, (Number(config.residualPositionSellDiscountCents) || 0) / 100)
  );
  const liveBuyPriceBuffer = Math.max(
    MIN_LIVE_BUY_PRICE_BUFFER,
    Math.min(0.05, pairSlippageCents / 100 + 0.005)
  );
  const minLegUsd = Math.max(0.1, Number(config.minBetUsd) || 0.1);
  const includeBtc = config.enableBtc !== false;
  const includeEth = config.enableEth !== false;
  const cadence5m = config.enableCadence5m !== false;
  const cadence15m = config.enableCadence15m !== false;
  const cadenceHourly = config.enableCadenceHourly !== false;
  const maxUnresolvedImbalancesPerRun = Math.max(
    1,
    Math.min(
      10,
      Math.floor(
        Number(config.maxUnresolvedImbalancesPerRun) ||
          DEFAULT_MAX_UNRESOLVED_IMBALANCES_PER_RUN
      )
    )
  );
  const unwindSellSlippage = Math.max(
    0,
    Math.min(
      0.2,
      (Number(config.unwindSellSlippageCents) ||
        DEFAULT_UNWIND_SELL_SLIPPAGE * 100) / 100
    )
  );
  const unwindShareBuffer = Math.max(
    0.5,
    Math.min(
      1,
      (Number(config.unwindShareBufferPct) ||
        DEFAULT_UNWIND_SHARE_BUFFER * 100) / 100
    )
  );

  if (!includeBtc && !includeEth) {
    result.error = "Both BTC and ETH are disabled";
    reject("all_coins_disabled");
    return result;
  }
  if (!cadence5m && !cadence15m && !cadenceHourly) {
    result.error = "All cadences are disabled";
    reject("all_cadences_disabled");
    return result;
  }

  const signalBuild = await getRecentPairSignals({
    lookbackSeconds,
    includeBtc,
    includeEth,
    cadence5m,
    cadence15m,
    cadenceHourly,
  });
  for (const [reason, count] of Object.entries(signalBuild.diagnostics)) {
    result.rejectedReasons[reason] = (result.rejectedReasons[reason] ?? 0) + count;
  }
  const signals = signalBuild.signals;
  result.evaluatedSignals = signals.length;
  let maxEdgeCentsSeen = -Infinity;
  let maxNetEdgeCentsSeen = -Infinity;
  let minPairSumSeen = Infinity;
  for (const signal of signals) {
    bumpBreakdown(result.evaluatedBreakdown, signal);
    const edgeCents = signal.edge * 100;
    const netEdgeCents = estimateNetEdgeCents({
      edge: signal.edge,
      pairSum: signal.pairSum,
      pairFeeBps,
      pairSlippageCents,
    });
    if (edgeCents > maxEdgeCentsSeen) maxEdgeCentsSeen = edgeCents;
    if (netEdgeCents > maxNetEdgeCentsSeen) maxNetEdgeCentsSeen = netEdgeCents;
    if (signal.pairSum < minPairSumSeen) minPairSumSeen = signal.pairSum;
  }
  if (result.evaluatedSignals === 0) {
    reject("no_recent_signals");
  }
  if (signals.length > 0) {
    result._maxEdgeCents = maxEdgeCentsSeen;
    result._maxNetEdgeCents = maxNetEdgeCentsSeen;
    result._minPairSum = minPairSumSeen;
  }

  const copiedSet = new Set(state.copiedKeys);
  let client: ClobClient | null = null;
  if (mode === "live") {
    const signer = new Wallet(privateKey);
    const rawClient = new ClobClient(CLOB_HOST, CHAIN_ID, signer);
    const creds = await rawClient.createOrDeriveApiKey();
    client = new ClobClient(
      CLOB_HOST,
      CHAIN_ID,
      signer,
      creds,
      signatureType,
      myAddress
    );
  }
  if (mode === "live" && client && autoExitResidualPositions) {
    try {
      const positions = await getPositions(myAddress, 200);
      const candidates = positions
        .filter((p) => !p.redeemable && Number(p.size) > 0 && Number(p.currentValue) >= residualPositionMinUsd)
        .map((p) => {
          const identity = detectUpDownIdentity(String(p.title ?? ""), String(p.slug ?? ""));
          return { p, identity };
        })
        .filter(
          (x) =>
            x.identity &&
            ((x.identity.coin === "BTC" && includeBtc) || (x.identity.coin === "ETH" && includeEth)) &&
            isCadenceEnabled(x.identity.cadence, { cadence5m, cadence15m, cadenceHourly })
        )
        .sort((a, b) => Number(b.p.currentValue) - Number(a.p.currentValue))
        .slice(0, residualPositionMaxPerRun);

      for (const { p } of candidates) {
        result.rejectedReasons.residual_exit_attempted =
          (result.rejectedReasons.residual_exit_attempted ?? 0) + 1;
        const shareAmount = Math.max(0.001, Number(p.size) * unwindShareBuffer);
        const minPrice = Math.max(
          0.01,
          Math.min(0.99, Number(p.curPrice) - residualPositionSellDiscount)
        );
        try {
          const resp = await client.createAndPostMarketOrder(
            {
              tokenID: p.asset,
              amount: shareAmount,
              side: Side.SELL,
              price: minPrice,
              orderType: OrderType.FOK,
            },
            undefined,
            OrderType.FOK
          );
          if (responseOk(resp)) {
            result.rejectedReasons.residual_exit_filled =
              (result.rejectedReasons.residual_exit_filled ?? 0) + 1;
            result.copiedTrades.push({
              title: p.title,
              outcome: p.outcome,
              side: "AUTO SELL (residual lifecycle)",
              amountUsd: shareAmount * Math.max(0.01, Number(p.curPrice)),
              price: Math.max(0.01, Number(p.curPrice)),
              asset: p.asset,
              timestamp: Date.now(),
            });
          } else {
            result.rejectedReasons.residual_exit_failed =
              (result.rejectedReasons.residual_exit_failed ?? 0) + 1;
            result.error = clipError(
              result.error,
              `Residual exit failed for ${p.asset}: ${responseError(resp, "sell rejected")}`
            );
          }
        } catch (e) {
          result.rejectedReasons.residual_exit_failed =
            (result.rejectedReasons.residual_exit_failed ?? 0) + 1;
          const msg = e instanceof Error ? e.message : String(e);
          result.error = clipError(result.error, `Residual exit exception for ${p.asset}: ${msg}`);
        }
      }
    } catch (e) {
      result.rejectedReasons.residual_exit_lookup_failed =
        (result.rejectedReasons.residual_exit_lookup_failed ?? 0) + 1;
      const msg = e instanceof Error ? e.message : String(e);
      result.error = clipError(result.error, `Residual exit lookup failed: ${msg}`);
    }
  }

  let lastTimestamp = state.lastTimestamp;
  let unresolvedImbalances = 0;
  const conditionExposureUsd = new Map<string, number>();
  const coinExposureUsd: Record<PairCoin, number> = { BTC: 0, ETH: 0 };
  const cadenceExposureUsd: Record<PairCadence, number> = {
    "5m": 0,
    "15m": 0,
    hourly: 0,
    other: 0,
  };
  const conditionExposureUsed = (conditionId: string): number =>
    Math.max(0, conditionExposureUsd.get(conditionId) ?? 0);
  const addConditionExposure = (conditionId: string, exposureUsd: number) => {
    if (exposureUsd <= 0) return;
    conditionExposureUsd.set(conditionId, conditionExposureUsed(conditionId) + exposureUsd);
  };
  const addSignalExposure = (signal: Pick<PairSignal, "conditionId" | "coin" | "cadence">, exposureUsd: number) => {
    if (exposureUsd <= 0) return;
    addConditionExposure(signal.conditionId, exposureUsd);
    coinExposureUsd[signal.coin] = Math.max(0, (coinExposureUsd[signal.coin] ?? 0) + exposureUsd);
    cadenceExposureUsd[signal.cadence] = Math.max(
      0,
      (cadenceExposureUsd[signal.cadence] ?? 0) + exposureUsd
    );
  };
  const nowSec = Math.floor(Date.now() / 1000);
  const cadencePriorityWeight: Record<PairCadence, number> = {
    "5m": 1.0,
    "15m": 1.05,
    hourly: 1.1,
    other: 0.9,
  };
  const cadenceFreshnessHalfLifeSec: Record<PairCadence, number> = {
    "5m": 150,
    "15m": 420,
    hourly: 1800,
    other: 300,
  };
  const cadenceMaxSignalAgeSec: Record<PairCadence, number> = {
    "5m": Math.max(20, Math.min(3600, Number(config.freshnessMaxSignalAgeSec5m) || 180)),
    "15m": Math.max(20, Math.min(7200, Number(config.freshnessMaxSignalAgeSec15m) || 540)),
    hourly: Math.max(20, Math.min(14400, Number(config.freshnessMaxSignalAgeSecHourly) || 2100)),
    other: 600,
  };
  if (paperRelaxFreshness) {
    cadenceMaxSignalAgeSec["5m"] = Math.floor(cadenceMaxSignalAgeSec["5m"] * paperFreshnessAgeMultiplier);
    cadenceMaxSignalAgeSec["15m"] = Math.floor(cadenceMaxSignalAgeSec["15m"] * paperFreshnessAgeMultiplier);
    cadenceMaxSignalAgeSec.hourly = Math.floor(
      cadenceMaxSignalAgeSec.hourly * paperFreshnessAgeMultiplier
    );
    cadenceMaxSignalAgeSec.other = Math.floor(cadenceMaxSignalAgeSec.other * paperFreshnessAgeMultiplier);
  }
  const cadenceMaxOutcomeSkewSec: Record<PairCadence, number> = {
    "5m": 75,
    "15m": 180,
    hourly: 600,
    other: 120,
  };
  const cadenceMaxExecutionQuoteAgeSecConfigured: Record<PairCadence, number> = {
    "5m": Math.max(
      0,
      Math.min(3600, Math.floor(Number(config.freshnessMaxExecutionQuoteAgeSec5m) || 0))
    ),
    "15m": Math.max(
      0,
      Math.min(7200, Math.floor(Number(config.freshnessMaxExecutionQuoteAgeSec15m) || 0))
    ),
    hourly: Math.max(
      0,
      Math.min(14400, Math.floor(Number(config.freshnessMaxExecutionQuoteAgeSecHourly) || 0))
    ),
    other: 0,
  };
  const rankedSignals = signals
    .map((signal) => {
      const signalMinEdge = minEdgeByCadence[signal.cadence] ?? defaultMinEdge;
      const effectiveMinEdge =
        paperMinEdgeOverride != null
          ? Math.min(signalMinEdge, paperMinEdgeOverride)
          : signalMinEdge;
      const baseSignalMinEdgeCents = effectiveMinEdge * 100;
      const netEdgeCents = estimateNetEdgeCents({
        edge: signal.edge,
        pairSum: signal.pairSum,
        pairFeeBps,
        pairSlippageCents,
      });
      const activityRatio = Math.max(
        0,
        Math.min(1, signal.recentTradeCount / adaptiveEdgeLowActivityTradeCount)
      );
      const freshness = freshnessDecayWeight(
        signal.latestTimestamp,
        nowSec,
        cadenceFreshnessHalfLifeSec[signal.cadence] ?? 300
      );
      const adaptivePenaltyCents =
        adaptiveEdgeEnabled
          ? (1 - activityRatio) * adaptiveEdgeMaxPenaltyCents +
            (1 - freshness) * (adaptiveEdgeStalePenaltyByCadenceCents[signal.cadence] ?? adaptiveEdgeStalePenaltyCents)
          : 0;
      const signalMinEdgeCents = baseSignalMinEdgeCents + adaptivePenaltyCents;
      const netSurplusCents = netEdgeCents - signalMinEdgeCents;
      const cadenceWeight = cadencePriorityWeight[signal.cadence] ?? 1;
      // Cadence-aware ranking: prioritize surplus net edge, then freshness.
      const priorityScore = netSurplusCents * cadenceWeight + freshness * 0.25;
      return {
        signal,
        netEdgeCents,
        signalMinEdgeCents,
        baseSignalMinEdgeCents,
        adaptivePenaltyCents,
        priorityScore,
      };
    })
    .sort((a, b) => {
      if (b.priorityScore !== a.priorityScore) return b.priorityScore - a.priorityScore;
      if (b.netEdgeCents !== a.netEdgeCents) return b.netEdgeCents - a.netEdgeCents;
      return b.signal.latestTimestamp - a.signal.latestTimestamp;
    });

  const buildSignalKey = (conditionId: string, latestTimestamp: number, entryIndex: number): string =>
    entryIndex <= 0
      ? `${conditionId}|${latestTimestamp}`
      : `${conditionId}|${latestTimestamp}|reentry:${entryIndex}`;

  signalsLoop: for (const rankedSignal of rankedSignals) {
    const signal = rankedSignal.signal;
    const netEdgeCents = rankedSignal.netEdgeCents;
    const signalMinEdgeCents = rankedSignal.signalMinEdgeCents;
    const baseSignalMinEdgeCents = rankedSignal.baseSignalMinEdgeCents;
    const adaptivePenaltyCents = rankedSignal.adaptivePenaltyCents;
    const maxSignalAgeSec = Math.max(30, cadenceMaxSignalAgeSec[signal.cadence] ?? lookbackSeconds);
    const signalAgeSec = Math.max(0, Math.floor(Date.now() / 1000) - signal.latestTimestamp);
    if (signalAgeSec > maxSignalAgeSec) {
      reject(`signal_stale_${signal.cadence}`);
      continue;
    }
    const [outcomeA, outcomeB] = signal.outcomes;
    const outcomeTimestampSkewSec = Math.abs(outcomeA.timestamp - outcomeB.timestamp);
    if (outcomeTimestampSkewSec > (cadenceMaxOutcomeSkewSec[signal.cadence] ?? 120)) {
      reject(`outcome_timestamp_skew_${signal.cadence}`);
      continue;
    }
    const conditionExposureRemaining =
      maxConditionExposureUsd > 0
        ? maxConditionExposureUsd - conditionExposureUsed(signal.conditionId)
        : Infinity;
    if (conditionExposureRemaining <= 0) {
      reject("condition_exposure_cap_reached");
      continue;
    }
    if (netEdgeCents < signalMinEdgeCents) {
      const baseReason =
        signal.cadence === "other" ? "edge_below_threshold" : `edge_below_threshold_${signal.cadence}`;
      const adaptiveSuffix = adaptivePenaltyCents > 0.001 ? "_adaptive" : "";
      reject(
        signal.edge * 100 >= baseSignalMinEdgeCents
          ? `${baseReason}_after_costs${adaptiveSuffix}`
          : `${baseReason}${adaptiveSuffix}`
      );
      continue;
    }
    const netEdgeSurplusCents = netEdgeCents - signalMinEdgeCents;
    if (mode === "live" && netEdgeSurplusCents < liveMinNetEdgeSurplusCents) {
      reject("live_edge_surplus_below_min");
      continue;
    }

    const pairSum = signal.pairSum;
    if (pairSum <= 0 || pairSum >= 2) {
      reject("invalid_pair_sum");
      continue;
    }
    const extraEntriesByEdge = Math.max(
      0,
      Math.floor((netEdgeCents - signalMinEdgeCents) / reentryEdgeStepCents)
    );
    const maxEntriesForSignal = Math.max(
      1,
      Math.min(reentryMaxEntriesPerSignal, 1 + extraEntriesByEdge)
    );
    const configuredQuoteAge = cadenceMaxExecutionQuoteAgeSecConfigured[signal.cadence] ?? 0;
    let maxExecutionQuoteAgeSec =
      configuredQuoteAge > 0 ? configuredQuoteAge : Math.max(20, Math.floor(maxSignalAgeSec * 0.7));
    if (paperRelaxFreshness) {
      maxExecutionQuoteAgeSec = Math.floor(maxExecutionQuoteAgeSec * paperFreshnessAgeMultiplier);
    }
    let acceptedEntryForSignal = false;
    for (let entryIndex = 0; entryIndex < maxEntriesForSignal; entryIndex++) {
      if (result.eligibleSignals >= maxMarketsPerRun) {
        reject("max_markets_per_run_reached");
        break signalsLoop;
      }
      if (remainingBudgetUsd < (mode === "live" ? 2 : 0.2)) {
        reject("insufficient_remaining_budget");
        break signalsLoop;
      }
      const quoteAgeSec = Math.max(0, Math.floor(Date.now() / 1000) - signal.latestTimestamp);
      if (quoteAgeSec > maxExecutionQuoteAgeSec) {
        reject(`quote_stale_before_execution_${signal.cadence}`);
        break;
      }

      const conditionExposureRemaining =
        maxConditionExposureUsd > 0
          ? maxConditionExposureUsd - conditionExposureUsed(signal.conditionId)
          : Infinity;
      if (conditionExposureRemaining <= 0) {
        if (!acceptedEntryForSignal) {
          reject("condition_exposure_cap_reached");
        }
        break;
      }
      const signalKey = buildSignalKey(signal.conditionId, signal.latestTimestamp, entryIndex);
      if (copiedSet.has(signalKey)) {
        if (!acceptedEntryForSignal && entryIndex === 0) {
          reject("signal_not_new");
        }
        continue;
      }

      const dynamicSizingEdgeRatio = Math.max(
        0,
        Math.min(1, netEdgeSurplusCents / dynamicSizingEdgeTargetCents)
      );
      const dynamicSizingLiquidityRatio = Math.max(
        0,
        Math.min(1, signal.recentTradeCount / dynamicSizingLiquidityTradeCount)
      );
      const dynamicSizingScore = dynamicSizingEdgeRatio * 0.7 + dynamicSizingLiquidityRatio * 0.3;
      const dynamicSizingScale = dynamicSizingEnabled
        ? dynamicSizingMinScale + (dynamicSizingMaxScale - dynamicSizingMinScale) * dynamicSizingScore
        : 1;
      const targetPairChunkUsd = pairChunkUsd * dynamicSizingScale;
      const coinExposureRemaining =
        maxCoinExposureSharePct > 0
          ? (runBudgetCapUsd * maxCoinExposureSharePct) / 100 - (coinExposureUsd[signal.coin] ?? 0)
          : Infinity;
      const cadenceExposureRemaining =
        maxCadenceExposureSharePct > 0
          ? (runBudgetCapUsd * maxCadenceExposureSharePct) / 100 -
            (cadenceExposureUsd[signal.cadence] ?? 0)
          : Infinity;
      let pairSpend = Math.min(
        targetPairChunkUsd,
        remainingBudgetUsd,
        conditionExposureRemaining,
        coinExposureRemaining,
        cadenceExposureRemaining
      );
      if (pairSpend <= 0) {
        if (!acceptedEntryForSignal) {
          if (coinExposureRemaining <= 0) {
            reject(`coin_exposure_share_cap_reached_${signal.coin.toLowerCase()}`);
          } else if (cadenceExposureRemaining <= 0) {
            reject(`cadence_exposure_share_cap_reached_${signal.cadence}`);
          } else {
            reject("pair_spend_non_positive");
          }
        }
        break;
      }
      const shares = pairSpend / pairSum;
      let legAUsd = shares * outcomeA.price;
      let legBUsd = shares * outcomeB.price;

      if (legAUsd < minLegUsd || legBUsd < minLegUsd) {
        if (!acceptedEntryForSignal) {
          reject("leg_below_min_bet");
        }
        break;
      }
      if (mode === "live") {
        if (legAUsd < POLYMARKET_MIN_ORDER_USD || legBUsd < POLYMARKET_MIN_ORDER_USD) {
          if (!config.floorToPolymarketMin) {
            if (!acceptedEntryForSignal) {
              reject("leg_below_polymarket_min_no_floor");
            }
            break;
          }
          legAUsd = Math.max(POLYMARKET_MIN_ORDER_USD, legAUsd);
          legBUsd = Math.max(POLYMARKET_MIN_ORDER_USD, legBUsd);
        }
        pairSpend = legAUsd + legBUsd;
        if (pairSpend > conditionExposureRemaining) {
          if (!acceptedEntryForSignal) {
            reject("condition_exposure_cap_reached");
          }
          break;
        }
        if (
          maxCoinExposureSharePct > 0 &&
          (coinExposureUsd[signal.coin] ?? 0) + pairSpend >
            (runBudgetCapUsd * maxCoinExposureSharePct) / 100
        ) {
          if (!acceptedEntryForSignal) {
            reject(`coin_exposure_share_cap_reached_${signal.coin.toLowerCase()}`);
          }
          break;
        }
        if (
          maxCadenceExposureSharePct > 0 &&
          (cadenceExposureUsd[signal.cadence] ?? 0) + pairSpend >
            (runBudgetCapUsd * maxCadenceExposureSharePct) / 100
        ) {
          if (!acceptedEntryForSignal) {
            reject(`cadence_exposure_share_cap_reached_${signal.cadence}`);
          }
          break;
        }
        if (pairSpend > remainingBudgetUsd) {
          if (!acceptedEntryForSignal) {
            reject("pair_exceeds_remaining_budget");
          }
          break;
        }
      }

      result.eligibleSignals++;
      acceptedEntryForSignal = true;
      bumpBreakdown(result.eligibleBreakdown, signal);

      if (mode === "paper") {
        copiedSet.add(signalKey);
        result.copied++;
        result.paper++;
        result.executedEdgeCentsSum += signal.edge * 100;
        result.executedNetEdgeCentsSum += netEdgeCents;
        bumpBreakdown(result.executedBreakdown, signal);
        result.simulatedVolumeUsd += legAUsd + legBUsd;
        addSignalExposure(signal, legAUsd + legBUsd);
        remainingBudgetUsd = Math.max(0, remainingBudgetUsd - (legAUsd + legBUsd));
        lastTimestamp = Math.max(lastTimestamp ?? 0, signal.latestTimestamp);
        result.copiedTrades.push({
          title: signal.title,
          outcome: outcomeA.outcome,
          side: `PAPER BUY (${signal.coin} pair)`,
          amountUsd: legAUsd,
          price: outcomeA.price,
          asset: outcomeA.asset,
          timestamp: Date.now(),
        });
        result.copiedTrades.push({
          title: signal.title,
          outcome: outcomeB.outcome,
          side: `PAPER BUY (${signal.coin} pair)`,
          amountUsd: legBUsd,
          price: outcomeB.price,
          asset: outcomeB.asset,
          timestamp: Date.now(),
        });
        continue;
      }

      if (!client) {
        result.failed++;
        reject("missing_clob_client_live");
        result.error = clipError(result.error, "Missing CLOB client in live mode");
        break;
      }
      const dynamicBuyPriceBuffer = Math.max(
        MIN_LIVE_BUY_PRICE_BUFFER,
        Math.min(liveBuyPriceBuffer, Math.max(MIN_LIVE_BUY_PRICE_BUFFER, netEdgeSurplusCents / 100))
      );

      const recordLivePair = () => {
        copiedSet.add(signalKey);
        result.copied++;
        result.executedEdgeCentsSum += signal.edge * 100;
        result.executedNetEdgeCentsSum += netEdgeCents;
        bumpBreakdown(result.executedBreakdown, signal);
        addSignalExposure(signal, legAUsd + legBUsd);
        remainingBudgetUsd = Math.max(0, remainingBudgetUsd - (legAUsd + legBUsd));
        lastTimestamp = Math.max(lastTimestamp ?? 0, signal.latestTimestamp);
        result.copiedTrades.push({
          title: signal.title,
          outcome: outcomeA.outcome,
          side: `BUY (${signal.coin} pair)`,
          amountUsd: legAUsd,
          price: outcomeA.price,
          asset: outcomeA.asset,
          timestamp: Date.now(),
        });
        result.copiedTrades.push({
          title: signal.title,
          outcome: outcomeB.outcome,
          side: `BUY (${signal.coin} pair)`,
          amountUsd: legBUsd,
          price: outcomeB.price,
          asset: outcomeB.asset,
          timestamp: Date.now(),
        });
      };

      const placeBuyLeg = async (
        tokenID: string,
        amountUsd: number,
        quotePrice: number
      ): Promise<{ ok: boolean; error: string }> => {
        try {
          const buyPrice = Math.max(
            0.001,
            Math.min(0.999, quotePrice + dynamicBuyPriceBuffer)
          );
          const resp = await client.createAndPostMarketOrder(
            {
              tokenID,
              amount: amountUsd,
              side: Side.BUY,
              orderType: OrderType.FOK,
              price: buyPrice,
            },
            undefined,
            OrderType.FOK
          );
          if (responseOk(resp)) return { ok: true, error: "" };
          return { ok: false, error: responseError(resp, "BUY leg rejected") };
        } catch (e) {
          return { ok: false, error: e instanceof Error ? e.message : String(e) };
        }
      };

      const placeUnwindSell = async (
        tokenID: string,
        shareAmount: number,
        minPrice: number
      ): Promise<{ ok: boolean; error: string }> => {
        try {
          const resp = await client.createAndPostMarketOrder(
            {
              tokenID,
              amount: shareAmount,
              side: Side.SELL,
              price: Math.max(0.01, Math.min(0.99, minPrice)),
              orderType: OrderType.FOK,
            },
            undefined,
            OrderType.FOK
          );
          if (responseOk(resp)) return { ok: true, error: "" };
          return { ok: false, error: responseError(resp, "Unwind sell rejected") };
        } catch (e) {
          return { ok: false, error: e instanceof Error ? e.message : String(e) };
        }
      };

      const legA = await placeBuyLeg(outcomeA.asset, legAUsd, outcomeA.price);
      if (!legA.ok) {
        result.failed++;
        reject("live_leg_a_rejected");
        result.error = clipError(result.error, legA.error || "Leg A rejected");
        break;
      }

      const legB = await placeBuyLeg(outcomeB.asset, legBUsd, outcomeB.price);
      if (legB.ok) {
        recordLivePair();
        continue;
      }

      reject("live_partial_fill_detected");
      const retryB = await placeBuyLeg(outcomeB.asset, legBUsd, outcomeB.price);
      if (retryB.ok) {
        reject("live_partial_recovered_leg_b_retry");
        recordLivePair();
        continue;
      }

      let unwindShareAmount =
        (legAUsd / Math.max(0.001, outcomeA.price)) * unwindShareBuffer;
      try {
        const positions = await getPositions(myAddress, 200);
        const legAPosition = positions.find(
          (p) => !p.redeemable && p.asset === outcomeA.asset && Number(p.size) > 0
        );
        if (legAPosition) {
          // Use the current held shares for unwind sizing to avoid oversell rejections.
          unwindShareAmount = Math.max(
            0.001,
            Number(legAPosition.size) * unwindShareBuffer
          );
        } else {
          reject("live_partial_unwind_position_not_found");
        }
      } catch {
        reject("live_partial_unwind_position_lookup_failed");
      }
      const unwindMinPrice = Math.max(0.01, outcomeA.price - unwindSellSlippage);
      const unwind = await placeUnwindSell(
        outcomeA.asset,
        Math.max(0.001, unwindShareAmount),
        unwindMinPrice
      );
      if (unwind.ok) {
        result.failed++;
        reject("live_partial_unwound_leg_a");
        result.error = clipError(
          result.error,
          `Leg B failed and retry failed (${legB.error}; ${retryB.error}); unwind of leg A succeeded`
        );
        break;
      }

      unresolvedImbalances++;
      result.failed++;
      reject("live_partial_unwind_failed");
      if (!result.unresolvedExposureAssets.includes(outcomeA.asset)) {
        result.unresolvedExposureAssets.push(outcomeA.asset);
      }
      result.error = clipError(
        result.error,
        `CRITICAL unresolved one-leg exposure (${unresolvedImbalances}/${maxUnresolvedImbalancesPerRun}): leg B failed (${legB.error}); retry failed (${retryB.error}); unwind failed (${unwind.error})`
      );
      if (unresolvedImbalances >= maxUnresolvedImbalancesPerRun) {
        reject("circuit_breaker_unresolved_imbalance");
        result.error = clipError(result.error, "Circuit breaker tripped due to unresolved imbalance");
        break signalsLoop;
      }
      break;
    }
  }

  result.lastTimestamp = lastTimestamp;
  result.copiedKeys = Array.from(copiedSet).slice(-5000);
  result.budgetUsedUsd = Math.max(0, result.budgetCapUsd - remainingBudgetUsd);
  const executedPairs = mode === "paper" ? result.paper : result.copied;
  if (executedPairs > 0) {
    result.avgExecutedEdgeCents = result.executedEdgeCentsSum / executedPairs;
    result.avgExecutedNetEdgeCents = result.executedNetEdgeCentsSum / executedPairs;
  }
  return result;
}
