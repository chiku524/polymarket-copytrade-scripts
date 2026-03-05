import { ClobClient, OrderType, Side } from "@polymarket/clob-client";
import { Wallet } from "ethers";
import { getCashBalance, type CopiedTrade } from "@/lib/copy-trade";

const DATA_API = "https://data-api.polymarket.com";
const CLOB_HOST = "https://clob.polymarket.com";
const CHAIN_ID = 137;
const POLYMARKET_MIN_ORDER_USD = 1;
const DEFAULT_MAX_UNRESOLVED_IMBALANCES_PER_RUN = 1;
const DEFAULT_UNWIND_SELL_SLIPPAGE = 0.03;
const DEFAULT_UNWIND_SHARE_BUFFER = 0.99;

type TradingMode = "off" | "paper" | "live";
type PairCoin = "BTC" | "ETH";
type PairCadence = "5m" | "15m" | "hourly" | "other";

interface GlobalTrade {
  conditionId: string;
  title?: string;
  slug?: string;
  asset: string;
  outcome: string;
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
  outcomes: [OutcomeSnapshot, OutcomeSnapshot];
}

export interface SignalBreakdown {
  byCoin: Record<PairCoin, number>;
  byCadence: Record<PairCadence, number>;
}

interface TradeConditionSnapshot {
  latestTimestamp: number;
  byOutcome: Map<string, OutcomeSnapshot>;
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
  /** Best edge among evaluated signals (cents), for diagnostics */
  _maxEdgeCents?: number;
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

    const bucket = grouped.get(conditionId) ?? { latestTimestamp: 0, byOutcome: new Map<string, OutcomeSnapshot>() };
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
    pairMinEdgeCents5m: number;
    pairMinEdgeCents15m: number;
    pairMinEdgeCentsHourly: number;
    pairLookbackSeconds: number;
    pairMaxMarketsPerRun: number;
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
  const lookbackSeconds = Math.max(20, Number(config.pairLookbackSeconds) || 120);
  const maxMarketsPerRun = Math.max(1, Math.min(20, Number(config.pairMaxMarketsPerRun) || 4));
  const pairChunkUsd = Math.max(1, Number(config.pairChunkUsd) || 3);
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
  let minPairSumSeen = Infinity;
  for (const signal of signals) {
    bumpBreakdown(result.evaluatedBreakdown, signal);
    const edgeCents = signal.edge * 100;
    if (edgeCents > maxEdgeCentsSeen) maxEdgeCentsSeen = edgeCents;
    if (signal.pairSum < minPairSumSeen) minPairSumSeen = signal.pairSum;
  }
  if (result.evaluatedSignals === 0) {
    reject("no_recent_signals");
  }
  if (signals.length > 0) {
    result._maxEdgeCents = maxEdgeCentsSeen;
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

  let lastTimestamp = state.lastTimestamp;
  let unresolvedImbalances = 0;

  for (const signal of signals) {
    if (result.eligibleSignals >= maxMarketsPerRun) {
      reject("max_markets_per_run_reached");
      break;
    }
    if (remainingBudgetUsd < (mode === "live" ? 2 : 0.2)) {
      reject("insufficient_remaining_budget");
      break;
    }
    const signalMinEdge = minEdgeByCadence[signal.cadence] ?? defaultMinEdge;
    if (signal.edge < signalMinEdge) {
      reject(
        signal.cadence === "other"
          ? "edge_below_threshold"
          : `edge_below_threshold_${signal.cadence}`
      );
      continue;
    }
    if (signal.latestTimestamp <= state.lastTimestamp) {
      reject("signal_not_new");
      continue;
    }

    const signalKey = `${signal.conditionId}|${signal.latestTimestamp}`;
    if (copiedSet.has(signalKey)) {
      reject("already_processed_signal");
      continue;
    }

    const [outcomeA, outcomeB] = signal.outcomes;
    const pairSum = signal.pairSum;
    if (pairSum <= 0 || pairSum >= 2) {
      reject("invalid_pair_sum");
      continue;
    }

    let pairSpend = Math.min(pairChunkUsd, remainingBudgetUsd);
    if (pairSpend <= 0) {
      reject("pair_spend_non_positive");
      continue;
    }
    const shares = pairSpend / pairSum;
    let legAUsd = shares * outcomeA.price;
    let legBUsd = shares * outcomeB.price;

    if (legAUsd < minLegUsd || legBUsd < minLegUsd) {
      reject("leg_below_min_bet");
      continue;
    }
    if (mode === "live") {
      if (legAUsd < POLYMARKET_MIN_ORDER_USD || legBUsd < POLYMARKET_MIN_ORDER_USD) {
        if (!config.floorToPolymarketMin) {
          reject("leg_below_polymarket_min_no_floor");
          continue;
        }
        legAUsd = Math.max(POLYMARKET_MIN_ORDER_USD, legAUsd);
        legBUsd = Math.max(POLYMARKET_MIN_ORDER_USD, legBUsd);
      }
      pairSpend = legAUsd + legBUsd;
      if (pairSpend > remainingBudgetUsd) {
        reject("pair_exceeds_remaining_budget");
        continue;
      }
    }

    result.eligibleSignals++;
    bumpBreakdown(result.eligibleBreakdown, signal);

    if (mode === "paper") {
      copiedSet.add(signalKey);
      result.copied++;
      result.paper++;
      bumpBreakdown(result.executedBreakdown, signal);
      result.simulatedVolumeUsd += legAUsd + legBUsd;
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
      continue;
    }

    const recordLivePair = () => {
      copiedSet.add(signalKey);
      result.copied++;
      bumpBreakdown(result.executedBreakdown, signal);
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
        const resp = await client.createAndPostMarketOrder(
          {
            tokenID,
            amount: amountUsd,
            side: Side.BUY,
            orderType: OrderType.FOK,
            price: Math.max(0.001, Math.min(0.999, quotePrice)),
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
      continue;
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

    const estimatedLegAShares =
      (legAUsd / Math.max(0.001, outcomeA.price)) * unwindShareBuffer;
    const unwindMinPrice = Math.max(0.01, outcomeA.price - unwindSellSlippage);
    const unwind = await placeUnwindSell(
      outcomeA.asset,
      Math.max(0.1, estimatedLegAShares),
      unwindMinPrice
    );
    if (unwind.ok) {
      result.failed++;
      reject("live_partial_unwound_leg_a");
      result.error = clipError(
        result.error,
        `Leg B failed and retry failed (${legB.error}; ${retryB.error}); unwind of leg A succeeded`
      );
      continue;
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
      break;
    }
  }

  result.lastTimestamp = lastTimestamp;
  result.copiedKeys = Array.from(copiedSet).slice(-5000);
  result.budgetUsedUsd = Math.max(0, result.budgetCapUsd - remainingBudgetUsd);
  return result;
}
