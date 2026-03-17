import Redis from "ioredis";
import { randomUUID } from "crypto";
import type { CopiedTrade } from "@/lib/copy-trade";

interface KvSetOptions {
  nx?: boolean;
  ex?: number;
}

const REDIS_URL =
  process.env.REDIS_URL?.trim() ||
  process.env.REDIS_PRIVATE_URL?.trim() ||
  process.env.REDIS_PUBLIC_URL?.trim();
let redis: Redis | null = null;
let warnedMemoryFallback = false;
let warnedRedisFailure = false;
let redisBackoffUntil = 0;
const memoryStore = new Map<string, { value: string; expiresAt?: number }>();

function maybeUpgradeRedisUrl(url: string): string {
  // Upstash URLs typically require TLS. If a plain redis:// URL is provided
  // for an Upstash host, transparently upgrade it to rediss://.
  if (url.startsWith("redis://") && url.includes(".upstash.io")) {
    return `rediss://${url.slice("redis://".length)}`;
  }
  return url;
}

function serializeValue(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function parseValue<T>(raw: string | null): T | null {
  if (raw == null) return null;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return raw as T;
  }
}

function ensureMemoryFresh(key: string): void {
  const entry = memoryStore.get(key);
  if (!entry) return;
  if (entry.expiresAt && entry.expiresAt <= Date.now()) {
    memoryStore.delete(key);
  }
}

function getRedis(): Redis | null {
  if (!REDIS_URL) {
    if (!warnedMemoryFallback) {
      console.warn(
        "REDIS_URL not configured; using in-memory KV fallback (non-persistent, single-instance only)."
      );
      warnedMemoryFallback = true;
    }
    return null;
  }
  if (Date.now() < redisBackoffUntil) {
    return null;
  }
  if (redis && (redis.status === "end" || redis.status === "close")) {
    redis = null;
  }
  if (!redis) {
    const effectiveRedisUrl = maybeUpgradeRedisUrl(REDIS_URL);
    const client = new Redis(effectiveRedisUrl, {
      maxRetriesPerRequest: 5,
      enableAutoPipelining: true,
      lazyConnect: false,
      connectTimeout: 10000,
      retryStrategy(times) {
        if (times > 5) return null;
        return Math.min(times * 200, 2000);
      },
    });
    client.on("error", () => {
      // Prevent ioredis "Unhandled error event" noise; command paths
      // handle fallback and structured logging via handleRedisFailure.
    });
    redis = client;
  }
  return redis;
}

function handleRedisFailure(error: unknown): void {
  const msg = error instanceof Error ? error.message : String(error);
  if (!warnedRedisFailure) {
    console.error(`Redis unavailable; temporarily falling back to in-memory KV. Error: ${msg}`);
    warnedRedisFailure = true;
  }
  redisBackoffUntil = Date.now() + 30_000;
  try {
    redis?.disconnect(false);
  } catch {
    // no-op
  }
  redis = null;
}

const kv = {
  async get<T>(key: string): Promise<T | null> {
    const client = getRedis();
    if (client) {
      try {
        const raw = await client.get(key);
        warnedRedisFailure = false;
        return parseValue<T>(raw);
      } catch (e) {
        handleRedisFailure(e);
      }
    }
    ensureMemoryFresh(key);
    return parseValue<T>(memoryStore.get(key)?.value ?? null);
  },
  async set(key: string, value: unknown, options?: KvSetOptions): Promise<"OK" | null> {
    const payload = serializeValue(value);
    const ttlSeconds =
      options?.ex && Number.isFinite(options.ex) ? Math.max(1, Math.floor(options.ex)) : undefined;

    const client = getRedis();
    if (client) {
      try {
        if (options?.nx && ttlSeconds) {
          const result = await client.set(key, payload, "EX", ttlSeconds, "NX");
          warnedRedisFailure = false;
          return result;
        }
        if (options?.nx) {
          const result = await client.set(key, payload, "NX");
          warnedRedisFailure = false;
          return result;
        }
        if (ttlSeconds) {
          const result = await client.set(key, payload, "EX", ttlSeconds);
          warnedRedisFailure = false;
          return result;
        }
        const result = await client.set(key, payload);
        warnedRedisFailure = false;
        return result;
      } catch (e) {
        handleRedisFailure(e);
      }
    }

    ensureMemoryFresh(key);
    if (options?.nx && memoryStore.has(key)) return null;
    memoryStore.set(key, {
      value: payload,
      expiresAt: ttlSeconds ? Date.now() + ttlSeconds * 1000 : undefined,
    });
    return "OK";
  },
  async del(key: string): Promise<number> {
    const client = getRedis();
    if (client) {
      try {
        const result = await client.del(key);
        warnedRedisFailure = false;
        return result;
      } catch (e) {
        handleRedisFailure(e);
      }
    }
    ensureMemoryFresh(key);
    return memoryStore.delete(key) ? 1 : 0;
  },
};

const CONFIG_KEY = "copy_trader_config";
const STATE_KEY = "copy_trader_state";
const ACTIVITY_KEY = "copy_trader_activity";
const RUN_LOCK_KEY = "copy_trader_run_lock";
const PAPER_STATS_KEY = "copy_trader_paper_stats";
const PAPER_LEDGER_KEY = "copy_trader_paper_ledger";
const STRATEGY_DIAGNOSTICS_HISTORY_KEY = "copy_trader_strategy_diagnostics_history";

export type TradingMode = "off" | "paper" | "live";

export interface CopyTraderConfig {
  /** Legacy toggle; derived from mode (mode !== "off") */
  enabled: boolean;
  /** Trading mode: off (paused), paper (simulate), live (place real orders) */
  mode: TradingMode;
  /** Max % of wallet balance this bot can allocate per run */
  walletUsagePercent: number;
  /** Legacy max bet (kept for backward compatibility) */
  maxBetUsd: number;
  /** Paired strategy chunk size per signal (USD) */
  pairChunkUsd: number;
  /** Hard cap per run in USD (0 = disabled; uses wallet % cap only) */
  maxRunBudgetUsd: number;
  /** Paper-only virtual wallet balance override in USD (0 = disabled; use real wallet balance) */
  paperVirtualWalletUsd: number;
  /** Minimum required edge in cents (1 - (pA + pB)) */
  pairMinEdgeCents: number;
  /** Paper-only override toggle allowing negative-edge simulations */
  paperAllowNegativeEdge: boolean;
  /** Paper-only minimum edge in cents (can be negative when override enabled) */
  paperMinEdgeCents: number;
  /** Min edge for 5m cadence signals */
  pairMinEdgeCents5m: number;
  /** Min edge for 15m cadence signals */
  pairMinEdgeCents15m: number;
  /** Min edge for hourly cadence signals */
  pairMinEdgeCentsHourly: number;
  /** Estimated entry fee in basis points used for net-edge gating */
  pairFeeBps: number;
  /** Estimated per-leg slippage in cents used for net-edge gating */
  pairSlippageCents: number;
  /** Live-only minimum net-edge surplus above threshold before entry */
  liveMinNetEdgeSurplusCents: number;
  /** Enable adaptive edge thresholds based on freshness + activity */
  adaptiveEdgeEnabled: boolean;
  /** Trade-count target used by adaptive activity penalty */
  adaptiveEdgeLowActivityTradeCount: number;
  /** Maximum adaptive penalty for low-activity signals (cents) */
  adaptiveEdgeMaxPenaltyCents: number;
  /** Maximum adaptive penalty for staler signals (cents) */
  adaptiveEdgeStalePenaltyCents: number;
  /** 5m-specific stale penalty override (cents) */
  adaptiveEdgeStalePenaltyCents5m: number;
  /** 15m-specific stale penalty override (cents) */
  adaptiveEdgeStalePenaltyCents15m: number;
  /** Hourly-specific stale penalty override (cents) */
  adaptiveEdgeStalePenaltyCentsHourly: number;
  /** Maximum signal age for 5m cadence (seconds) */
  freshnessMaxSignalAgeSec5m: number;
  /** Maximum signal age for 15m cadence (seconds) */
  freshnessMaxSignalAgeSec15m: number;
  /** Maximum signal age for hourly cadence (seconds) */
  freshnessMaxSignalAgeSecHourly: number;
  /** Maximum quote age before execution for 5m cadence (seconds, 0 = auto) */
  freshnessMaxExecutionQuoteAgeSec5m: number;
  /** Maximum quote age before execution for 15m cadence (seconds, 0 = auto) */
  freshnessMaxExecutionQuoteAgeSec15m: number;
  /** Maximum quote age before execution for hourly cadence (seconds, 0 = auto) */
  freshnessMaxExecutionQuoteAgeSecHourly: number;
  /** In paper mode, relax freshness age windows */
  paperRelaxFreshness: boolean;
  /** Multiplier used when paper freshness relaxation is enabled */
  paperFreshnessAgeMultiplier: number;
  /** Enable dynamic pair sizing by edge/activity quality */
  dynamicSizingEnabled: boolean;
  /** Minimum dynamic sizing scale (percent of pair chunk) */
  dynamicSizingMinScalePct: number;
  /** Maximum dynamic sizing scale (percent of pair chunk) */
  dynamicSizingMaxScalePct: number;
  /** Net-edge surplus target (cents) for dynamic sizing to hit max scale */
  dynamicSizingEdgeTargetCents: number;
  /** Trade-count target used for dynamic sizing liquidity scaling */
  dynamicSizingLiquidityTradeCount: number;
  /** Enable extra size scaling for high net-edge opportunities */
  edgeBoostEnabled: boolean;
  /** Net-edge threshold in cents where edge boost begins */
  edgeBoostThresholdCents: number;
  /** Net-edge threshold in cents where max edge boost is reached */
  edgeBoostHighThresholdCents: number;
  /** Size scale at lower edge-boost threshold (percent of pair chunk) */
  edgeBoostScalePct: number;
  /** Size scale at high edge-boost threshold (percent of pair chunk) */
  edgeBoostHighScalePct: number;
  /** Recency window for global market signal discovery */
  pairLookbackSeconds: number;
  /** Maximum number of paired signals to execute per run */
  pairMaxMarketsPerRun: number;
  /** Max entries allowed for the same signal snapshot in a run (>=1) */
  reentryMaxEntriesPerSignal: number;
  /** Net-edge step (cents) required for each additional same-signal entry */
  reentryEdgeStepCents: number;
  /** Max exposure per condition within a run (0 = disabled) */
  maxConditionExposureUsd: number;
  /** Max share of run budget allocated to one coin (0 = disabled) */
  maxCoinExposureSharePct: number;
  /** Max share of run budget allocated to one cadence bucket (0 = disabled) */
  maxCadenceExposureSharePct: number;
  /** Enable residual position lifecycle exits before new entries (live only) */
  autoExitResidualPositions: boolean;
  /** Minimum residual position value to attempt auto-exit */
  residualPositionMinUsd: number;
  /** Max residual positions to auto-exit per run */
  residualPositionMaxPerRun: number;
  /** SELL price discount from current price for residual exits (cents) */
  residualPositionSellDiscountCents: number;
  /** Include BTC Up/Down markets in strategy */
  enableBtc: boolean;
  /** Include ETH Up/Down markets in strategy */
  enableEth: boolean;
  /** Include 5-minute cadence markets */
  enableCadence5m: boolean;
  /** Include 15-minute cadence markets */
  enableCadence15m: boolean;
  /** Include hourly cadence markets */
  enableCadenceHourly: boolean;
  /** Min bet to place - skip if below (default 0.10) */
  minBetUsd: number;
  /** Stop placing orders when cash balance falls below this (0 = disabled) */
  stopLossBalance: number;
  /** When true, round small orders up to $1 (Polymarket minimum) */
  floorToPolymarketMin: boolean;
  /** Max unresolved one-leg imbalances allowed before circuit breaker halts run */
  maxUnresolvedImbalancesPerRun: number;
  /** SELL unwind price slippage tolerance in cents (probability points) */
  unwindSellSlippageCents: number;
  /** Fraction of estimated shares to unwind (percent) */
  unwindShareBufferPct: number;
  /** Max total live notional per UTC day (0 = disabled) */
  maxDailyLiveNotionalUsd: number;
  /** Max drawdown from UTC-day starting balance (0 = disabled) */
  maxDailyDrawdownUsd: number;
  /** Auto-stop timestamp in ms epoch (0 = disabled) */
  autoStopAt: number;
  /** Target executed pairs per hour for session pacing (0 = disabled) */
  sessionTargetPairsPerHour: number;
  /** Target minimum session average net edge in cents (0 = disabled) */
  sessionMinAvgNetEdgeCents: number;
}

export interface SafetyLatchState {
  active: boolean;
  reason: string;
  triggeredAt: number;
  unresolvedAssets: string[];
  attempts: number;
  lastAttemptAt?: number;
  lastAlertAt?: number;
}

export interface DailyRiskState {
  dayKey: string;
  dayStartBalanceUsd: number;
  liveNotionalUsd: number;
  liveRuns: number;
  lastRunAt?: number;
  alertedNotionalCap?: boolean;
  alertedDrawdownCap?: boolean;
}

export interface CopyTraderState {
  lastTimestamp: number;
  copiedKeys: string[];
  lastRunAt?: number;
  lastCopiedAt?: number;
  lastError?: string;
  lastStrategyDiagnostics?: StrategyDiagnostics;
  /** Incremented each strategy run; claim runs when this reaches CLAIM_EVERY_N_RUNS */
  runsSinceLastClaim?: number;
  lastClaimAt?: number;
  lastClaimResult?: { claimed: number; failed: number };
  safetyLatch?: SafetyLatchState;
  dailyRisk?: DailyRiskState;
}

export interface StrategyDiagnostics {
  mode: TradingMode;
  evaluatedSignals: number;
  eligibleSignals: number;
  rejectedReasons: Record<string, number>;
  evaluatedBreakdown?: StrategyBreakdown;
  eligibleBreakdown?: StrategyBreakdown;
  executedBreakdown?: StrategyBreakdown;
  copied: number;
  paper: number;
  failed: number;
  budgetCapUsd: number;
  budgetUsedUsd: number;
  avgExecutedEdgeCents?: number;
  avgExecutedNetEdgeCents?: number;
  error?: string;
  timestamp: number;
  maxEdgeCentsSeen?: number;
  maxNetEdgeCentsSeen?: number;
  minPairSumSeen?: number;
}

export interface StrategyBreakdown {
  byCoin: {
    BTC: number;
    ETH: number;
  };
  byCadence: {
    "5m": number;
    "15m": number;
    hourly: number;
    other: number;
  };
}

export interface StrategyDiagnosticsHistory {
  totalRuns: number;
  lastRunAt?: number;
  lastError?: string;
  recentRuns: StrategyDiagnostics[];
}

export interface RecentActivity {
  title: string;
  outcome: string;
  side: string;
  amountUsd: number;
  price: number;
  timestamp: number;
  asset?: string;
  conditionId?: string;
  slug?: string;
  coin?: "BTC" | "ETH";
  cadence?: "5m" | "15m" | "hourly" | "other";
  edgeCentsAtEntry?: number;
  netEdgeCentsAtEntry?: number;
  dynamicSizingScalePct?: number;
  edgeBoostScalePct?: number;
}

export interface PaperRunStat {
  timestamp: number;
  simulatedTrades: number;
  simulatedVolumeUsd: number;
  failed: number;
  budgetCapUsd: number;
  budgetUsedUsd: number;
  executedEdgeCentsSum?: number;
  executedNetEdgeCentsSum?: number;
  avgExecutedEdgeCents?: number;
  avgExecutedNetEdgeCents?: number;
  error?: string;
}

export interface PaperStats {
  totalRuns: number;
  totalSimulatedTrades: number;
  totalSimulatedVolumeUsd: number;
  totalFailed: number;
  totalBudgetCapUsd: number;
  totalBudgetUsedUsd: number;
  totalExecutedEdgeCents: number;
  totalExecutedNetEdgeCents: number;
  avgExecutedEdgeCents: number;
  avgExecutedNetEdgeCents: number;
  lastRunAt?: number;
  lastError?: string;
  recentRuns: PaperRunStat[];
}

export interface PaperLedgerLot {
  id: string;
  title: string;
  outcome: string;
  side: string;
  amountUsd: number;
  price: number;
  shares: number;
  asset: string;
  conditionId: string;
  timestamp: number;
  slug?: string;
  coin?: "BTC" | "ETH";
  cadence?: "5m" | "15m" | "hourly" | "other";
  edgeCentsAtEntry?: number;
  netEdgeCentsAtEntry?: number;
  dynamicSizingScalePct?: number;
  edgeBoostScalePct?: number;
  settledAt?: number;
  settledPrice?: number;
  settledWinner?: boolean;
  realizedPnlUsd?: number;
}

export interface PaperLedger {
  lots: PaperLedgerLot[];
  lastUpdatedAt?: number;
  lastSettledAt?: number;
}

const DEFAULT_CONFIG: CopyTraderConfig = {
  enabled: false,
  mode: "off",
  walletUsagePercent: 25,
  maxBetUsd: 3,
  pairChunkUsd: 3,
  maxRunBudgetUsd: 0,
  paperVirtualWalletUsd: 0,
  pairMinEdgeCents: 0.5,
  paperAllowNegativeEdge: false,
  paperMinEdgeCents: -0.2,
  pairMinEdgeCents5m: 0.5,
  pairMinEdgeCents15m: 0.5,
  pairMinEdgeCentsHourly: 0.5,
  pairFeeBps: 2,
  pairSlippageCents: 0.05,
  liveMinNetEdgeSurplusCents: 0.1,
  adaptiveEdgeEnabled: true,
  adaptiveEdgeLowActivityTradeCount: 8,
  adaptiveEdgeMaxPenaltyCents: 0.2,
  adaptiveEdgeStalePenaltyCents: 0.2,
  adaptiveEdgeStalePenaltyCents5m: 0.2,
  adaptiveEdgeStalePenaltyCents15m: 0.2,
  adaptiveEdgeStalePenaltyCentsHourly: 0.2,
  freshnessMaxSignalAgeSec5m: 180,
  freshnessMaxSignalAgeSec15m: 540,
  freshnessMaxSignalAgeSecHourly: 2100,
  freshnessMaxExecutionQuoteAgeSec5m: 0,
  freshnessMaxExecutionQuoteAgeSec15m: 0,
  freshnessMaxExecutionQuoteAgeSecHourly: 0,
  paperRelaxFreshness: false,
  paperFreshnessAgeMultiplier: 1.5,
  dynamicSizingEnabled: true,
  dynamicSizingMinScalePct: 70,
  dynamicSizingMaxScalePct: 140,
  dynamicSizingEdgeTargetCents: 1,
  dynamicSizingLiquidityTradeCount: 12,
  edgeBoostEnabled: false,
  edgeBoostThresholdCents: 5,
  edgeBoostHighThresholdCents: 10,
  edgeBoostScalePct: 200,
  edgeBoostHighScalePct: 500,
  pairLookbackSeconds: 600,
  pairMaxMarketsPerRun: 4,
  reentryMaxEntriesPerSignal: 2,
  reentryEdgeStepCents: 0.15,
  maxConditionExposureUsd: 0,
  maxCoinExposureSharePct: 0,
  maxCadenceExposureSharePct: 0,
  autoExitResidualPositions: false,
  residualPositionMinUsd: 1,
  residualPositionMaxPerRun: 2,
  residualPositionSellDiscountCents: 3,
  enableBtc: true,
  enableEth: true,
  enableCadence5m: true,
  enableCadence15m: true,
  enableCadenceHourly: true,
  minBetUsd: 0.1,
  stopLossBalance: 0,
  floorToPolymarketMin: true,
  maxUnresolvedImbalancesPerRun: 1,
  unwindSellSlippageCents: 3,
  unwindShareBufferPct: 99,
  maxDailyLiveNotionalUsd: 0,
  maxDailyDrawdownUsd: 0,
  autoStopAt: 0,
  sessionTargetPairsPerHour: 0,
  sessionMinAvgNetEdgeCents: 0,
};

const DEFAULT_PAPER_STATS: PaperStats = {
  totalRuns: 0,
  totalSimulatedTrades: 0,
  totalSimulatedVolumeUsd: 0,
  totalFailed: 0,
  totalBudgetCapUsd: 0,
  totalBudgetUsedUsd: 0,
  totalExecutedEdgeCents: 0,
  totalExecutedNetEdgeCents: 0,
  avgExecutedEdgeCents: 0,
  avgExecutedNetEdgeCents: 0,
  recentRuns: [],
};

const DEFAULT_PAPER_LEDGER: PaperLedger = {
  lots: [],
};
const MAX_ACTIVITY_ITEMS = 50;
const MAX_PAPER_LEDGER_LOTS = 4000;

const DEFAULT_STRATEGY_DIAGNOSTICS_HISTORY: StrategyDiagnosticsHistory = {
  totalRuns: 0,
  recentRuns: [],
};

function toFiniteNumber(value: unknown, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function normalizeMode(value: unknown): TradingMode | undefined {
  if (value === "off" || value === "paper" || value === "live") return value;
  return undefined;
}

function normalizeBreakdown(value: unknown): StrategyBreakdown {
  const root =
    value && typeof value === "object"
      ? (value as { byCoin?: Record<string, unknown>; byCadence?: Record<string, unknown> })
      : {};
  const byCoin = root.byCoin ?? {};
  const byCadence = root.byCadence ?? {};
  return {
    byCoin: {
      BTC: Math.max(0, toFiniteNumber(byCoin.BTC, 0)),
      ETH: Math.max(0, toFiniteNumber(byCoin.ETH, 0)),
    },
    byCadence: {
      "5m": Math.max(0, toFiniteNumber(byCadence["5m"], 0)),
      "15m": Math.max(0, toFiniteNumber(byCadence["15m"], 0)),
      hourly: Math.max(0, toFiniteNumber(byCadence.hourly, 0)),
      other: Math.max(0, toFiniteNumber(byCadence.other, 0)),
    },
  };
}

function normalizeRejectedReasons(value: unknown): Record<string, number> {
  if (!value || typeof value !== "object") return {};
  const result: Record<string, number> = {};
  for (const [key, rawCount] of Object.entries(value as Record<string, unknown>)) {
    const count = Math.max(0, toFiniteNumber(rawCount, 0));
    if (!key || count <= 0) continue;
    result[key] = count;
  }
  return result;
}

function normalizeCadence(value: unknown): "5m" | "15m" | "hourly" | "other" | undefined {
  return value === "5m" || value === "15m" || value === "hourly" || value === "other"
    ? value
    : undefined;
}

function normalizeRecentActivity(value: unknown): RecentActivity | null {
  if (!value || typeof value !== "object") return null;
  const raw = value as Record<string, unknown>;
  const title = String(raw.title ?? "").trim();
  const outcome = String(raw.outcome ?? "").trim();
  const side = String(raw.side ?? "").trim();
  const amountUsd = Math.max(0, toFiniteNumber(raw.amountUsd, 0));
  const price = Math.max(0, toFiniteNumber(raw.price, 0));
  const timestamp = Math.max(0, toFiniteNumber(raw.timestamp, Date.now()));
  if (!title || !outcome || !side || amountUsd <= 0 || price <= 0) return null;
  const conditionId = String(raw.conditionId ?? "").trim() || undefined;
  const slug = String(raw.slug ?? "").trim() || undefined;
  const asset = String(raw.asset ?? "").trim() || undefined;
  const coinRaw = String(raw.coin ?? "").toUpperCase();
  const coin = coinRaw === "BTC" || coinRaw === "ETH" ? coinRaw : undefined;
  return {
    title,
    outcome,
    side,
    amountUsd,
    price,
    timestamp,
    asset,
    conditionId,
    slug,
    coin,
    cadence: normalizeCadence(raw.cadence),
    edgeCentsAtEntry:
      typeof raw.edgeCentsAtEntry === "number" ? toFiniteNumber(raw.edgeCentsAtEntry, 0) : undefined,
    netEdgeCentsAtEntry:
      typeof raw.netEdgeCentsAtEntry === "number"
        ? toFiniteNumber(raw.netEdgeCentsAtEntry, 0)
        : undefined,
    dynamicSizingScalePct:
      typeof raw.dynamicSizingScalePct === "number"
        ? toFiniteNumber(raw.dynamicSizingScalePct, 0)
        : undefined,
    edgeBoostScalePct:
      typeof raw.edgeBoostScalePct === "number"
        ? toFiniteNumber(raw.edgeBoostScalePct, 0)
        : undefined,
  };
}

function normalizePaperLedgerLot(value: unknown): PaperLedgerLot | null {
  if (!value || typeof value !== "object") return null;
  const raw = value as Record<string, unknown>;
  const id = String(raw.id ?? "").trim();
  const title = String(raw.title ?? "").trim();
  const outcome = String(raw.outcome ?? "").trim();
  const side = String(raw.side ?? "").trim();
  const asset = String(raw.asset ?? "").trim();
  const conditionId = String(raw.conditionId ?? "").trim();
  const timestamp = Math.max(0, toFiniteNumber(raw.timestamp, Date.now()));
  const amountUsd = Math.max(0, toFiniteNumber(raw.amountUsd, 0));
  const price = Math.max(0, toFiniteNumber(raw.price, 0));
  const shares = Math.max(0, toFiniteNumber(raw.shares, amountUsd > 0 && price > 0 ? amountUsd / price : 0));
  if (!id || !title || !outcome || !side || !asset || !conditionId || amountUsd <= 0 || price <= 0) {
    return null;
  }
  const coinRaw = String(raw.coin ?? "").toUpperCase();
  const coin = coinRaw === "BTC" || coinRaw === "ETH" ? coinRaw : undefined;
  const settledAt =
    typeof raw.settledAt === "number" ? Math.max(0, toFiniteNumber(raw.settledAt, Date.now())) : undefined;
  const settledPrice =
    typeof raw.settledPrice === "number" ? Math.max(0, toFiniteNumber(raw.settledPrice, 0)) : undefined;
  return {
    id,
    title,
    outcome,
    side,
    amountUsd,
    price,
    shares,
    asset,
    conditionId,
    timestamp,
    slug: String(raw.slug ?? "").trim() || undefined,
    coin,
    cadence: normalizeCadence(raw.cadence),
    edgeCentsAtEntry:
      typeof raw.edgeCentsAtEntry === "number" ? toFiniteNumber(raw.edgeCentsAtEntry, 0) : undefined,
    netEdgeCentsAtEntry:
      typeof raw.netEdgeCentsAtEntry === "number"
        ? toFiniteNumber(raw.netEdgeCentsAtEntry, 0)
        : undefined,
    dynamicSizingScalePct:
      typeof raw.dynamicSizingScalePct === "number"
        ? toFiniteNumber(raw.dynamicSizingScalePct, 0)
        : undefined,
    edgeBoostScalePct:
      typeof raw.edgeBoostScalePct === "number"
        ? toFiniteNumber(raw.edgeBoostScalePct, 0)
        : undefined,
    settledAt,
    settledPrice,
    settledWinner:
      typeof raw.settledWinner === "boolean"
        ? raw.settledWinner
        : typeof settledPrice === "number"
          ? settledPrice >= 0.5
          : undefined,
    realizedPnlUsd:
      typeof raw.realizedPnlUsd === "number" ? toFiniteNumber(raw.realizedPnlUsd, 0) : undefined,
  };
}

function normalizeSafetyLatch(value: unknown): SafetyLatchState | undefined {
  if (!value || typeof value !== "object") return undefined;
  const raw = value as Record<string, unknown>;
  const active = raw.active !== false;
  const unresolvedAssets = Array.isArray(raw.unresolvedAssets)
    ? raw.unresolvedAssets
        .map((a) => String(a ?? "").trim())
        .filter(Boolean)
        .slice(0, 20)
    : [];
  return {
    active,
    reason: typeof raw.reason === "string" ? raw.reason : "Safety latch active",
    triggeredAt: Math.max(0, toFiniteNumber(raw.triggeredAt, Date.now())),
    unresolvedAssets,
    attempts: Math.max(0, Math.floor(toFiniteNumber(raw.attempts, 0))),
    lastAttemptAt: raw.lastAttemptAt ? toFiniteNumber(raw.lastAttemptAt, Date.now()) : undefined,
    lastAlertAt: raw.lastAlertAt ? toFiniteNumber(raw.lastAlertAt, Date.now()) : undefined,
  };
}

function normalizeDailyRisk(value: unknown): DailyRiskState | undefined {
  if (!value || typeof value !== "object") return undefined;
  const raw = value as Record<string, unknown>;
  const dayKey = typeof raw.dayKey === "string" ? raw.dayKey : "";
  if (!dayKey) return undefined;
  return {
    dayKey,
    dayStartBalanceUsd: Math.max(0, toFiniteNumber(raw.dayStartBalanceUsd, 0)),
    liveNotionalUsd: Math.max(0, toFiniteNumber(raw.liveNotionalUsd, 0)),
    liveRuns: Math.max(0, Math.floor(toFiniteNumber(raw.liveRuns, 0))),
    lastRunAt: raw.lastRunAt ? toFiniteNumber(raw.lastRunAt, Date.now()) : undefined,
    alertedNotionalCap: raw.alertedNotionalCap === true,
    alertedDrawdownCap: raw.alertedDrawdownCap === true,
  };
}

function normalizeStrategyDiagnostics(value: unknown): StrategyDiagnostics {
  const raw = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  return {
    mode: normalizeMode(raw.mode) ?? "off",
    evaluatedSignals: Math.max(0, toFiniteNumber(raw.evaluatedSignals, 0)),
    eligibleSignals: Math.max(0, toFiniteNumber(raw.eligibleSignals, 0)),
    rejectedReasons: normalizeRejectedReasons(raw.rejectedReasons),
    evaluatedBreakdown: normalizeBreakdown(raw.evaluatedBreakdown),
    eligibleBreakdown: normalizeBreakdown(raw.eligibleBreakdown),
    executedBreakdown: normalizeBreakdown(raw.executedBreakdown),
    copied: Math.max(0, toFiniteNumber(raw.copied, 0)),
    paper: Math.max(0, toFiniteNumber(raw.paper, 0)),
    failed: Math.max(0, toFiniteNumber(raw.failed, 0)),
    budgetCapUsd: Math.max(0, toFiniteNumber(raw.budgetCapUsd, 0)),
    budgetUsedUsd: Math.max(0, toFiniteNumber(raw.budgetUsedUsd, 0)),
    avgExecutedEdgeCents:
      typeof raw.avgExecutedEdgeCents === "number"
        ? toFiniteNumber(raw.avgExecutedEdgeCents, 0)
        : undefined,
    avgExecutedNetEdgeCents:
      typeof raw.avgExecutedNetEdgeCents === "number"
        ? toFiniteNumber(raw.avgExecutedNetEdgeCents, 0)
        : undefined,
    error: typeof raw.error === "string" ? raw.error : undefined,
    timestamp: Math.max(0, toFiniteNumber(raw.timestamp, Date.now())),
    maxEdgeCentsSeen: typeof raw.maxEdgeCentsSeen === "number" ? raw.maxEdgeCentsSeen : undefined,
    maxNetEdgeCentsSeen:
      typeof raw.maxNetEdgeCentsSeen === "number" ? raw.maxNetEdgeCentsSeen : undefined,
    minPairSumSeen: typeof raw.minPairSumSeen === "number" ? raw.minPairSumSeen : undefined,
  };
}

function sanitizeConfig(
  raw: Partial<CopyTraderConfig> & Record<string, unknown>,
  current: CopyTraderConfig
): CopyTraderConfig {
  let mode = normalizeMode(raw.mode) ?? current.mode;
  if (raw.enabled !== undefined && raw.mode === undefined) {
    mode = Boolean(raw.enabled) ? (current.mode === "off" ? "live" : current.mode) : "off";
  }

  return {
    enabled: mode !== "off",
    mode,
    walletUsagePercent: clamp(
      toFiniteNumber(raw.walletUsagePercent, current.walletUsagePercent),
      1,
      100
    ),
    maxBetUsd: clamp(toFiniteNumber(raw.maxBetUsd, current.maxBetUsd), 1, 10000),
    pairChunkUsd: clamp(toFiniteNumber(raw.pairChunkUsd, current.pairChunkUsd), 1, 10000),
    maxRunBudgetUsd: clamp(
      toFiniteNumber(raw.maxRunBudgetUsd, current.maxRunBudgetUsd),
      0,
      10000000
    ),
    paperVirtualWalletUsd: clamp(
      toFiniteNumber(raw.paperVirtualWalletUsd, current.paperVirtualWalletUsd),
      0,
      10000000
    ),
    pairMinEdgeCents: clamp(
      toFiniteNumber(raw.pairMinEdgeCents, current.pairMinEdgeCents),
      0,
      50
    ),
    paperAllowNegativeEdge: raw.paperAllowNegativeEdge === true,
    paperMinEdgeCents: clamp(
      toFiniteNumber(raw.paperMinEdgeCents, current.paperMinEdgeCents),
      -10,
      50
    ),
    pairMinEdgeCents5m: clamp(
      toFiniteNumber(raw.pairMinEdgeCents5m, current.pairMinEdgeCents5m),
      0,
      50
    ),
    pairMinEdgeCents15m: clamp(
      toFiniteNumber(raw.pairMinEdgeCents15m, current.pairMinEdgeCents15m),
      0,
      50
    ),
    pairMinEdgeCentsHourly: clamp(
      toFiniteNumber(raw.pairMinEdgeCentsHourly, current.pairMinEdgeCentsHourly),
      0,
      50
    ),
    pairFeeBps: clamp(toFiniteNumber(raw.pairFeeBps, current.pairFeeBps), 0, 200),
    pairSlippageCents: clamp(
      toFiniteNumber(raw.pairSlippageCents, current.pairSlippageCents),
      0,
      25
    ),
    liveMinNetEdgeSurplusCents: clamp(
      toFiniteNumber(raw.liveMinNetEdgeSurplusCents, current.liveMinNetEdgeSurplusCents),
      0,
      10
    ),
    adaptiveEdgeEnabled: raw.adaptiveEdgeEnabled !== false,
    adaptiveEdgeLowActivityTradeCount: clamp(
      Math.floor(
        toFiniteNumber(raw.adaptiveEdgeLowActivityTradeCount, current.adaptiveEdgeLowActivityTradeCount)
      ),
      1,
      200
    ),
    adaptiveEdgeMaxPenaltyCents: clamp(
      toFiniteNumber(raw.adaptiveEdgeMaxPenaltyCents, current.adaptiveEdgeMaxPenaltyCents),
      0,
      10
    ),
    adaptiveEdgeStalePenaltyCents: clamp(
      toFiniteNumber(raw.adaptiveEdgeStalePenaltyCents, current.adaptiveEdgeStalePenaltyCents),
      0,
      10
    ),
    adaptiveEdgeStalePenaltyCents5m: clamp(
      toFiniteNumber(raw.adaptiveEdgeStalePenaltyCents5m, current.adaptiveEdgeStalePenaltyCents5m),
      0,
      10
    ),
    adaptiveEdgeStalePenaltyCents15m: clamp(
      toFiniteNumber(raw.adaptiveEdgeStalePenaltyCents15m, current.adaptiveEdgeStalePenaltyCents15m),
      0,
      10
    ),
    adaptiveEdgeStalePenaltyCentsHourly: clamp(
      toFiniteNumber(
        raw.adaptiveEdgeStalePenaltyCentsHourly,
        current.adaptiveEdgeStalePenaltyCentsHourly
      ),
      0,
      10
    ),
    freshnessMaxSignalAgeSec5m: clamp(
      Math.floor(toFiniteNumber(raw.freshnessMaxSignalAgeSec5m, current.freshnessMaxSignalAgeSec5m)),
      20,
      3600
    ),
    freshnessMaxSignalAgeSec15m: clamp(
      Math.floor(
        toFiniteNumber(raw.freshnessMaxSignalAgeSec15m, current.freshnessMaxSignalAgeSec15m)
      ),
      20,
      7200
    ),
    freshnessMaxSignalAgeSecHourly: clamp(
      Math.floor(
        toFiniteNumber(raw.freshnessMaxSignalAgeSecHourly, current.freshnessMaxSignalAgeSecHourly)
      ),
      20,
      14400
    ),
    freshnessMaxExecutionQuoteAgeSec5m: clamp(
      Math.floor(
        toFiniteNumber(
          raw.freshnessMaxExecutionQuoteAgeSec5m,
          current.freshnessMaxExecutionQuoteAgeSec5m
        )
      ),
      0,
      3600
    ),
    freshnessMaxExecutionQuoteAgeSec15m: clamp(
      Math.floor(
        toFiniteNumber(
          raw.freshnessMaxExecutionQuoteAgeSec15m,
          current.freshnessMaxExecutionQuoteAgeSec15m
        )
      ),
      0,
      7200
    ),
    freshnessMaxExecutionQuoteAgeSecHourly: clamp(
      Math.floor(
        toFiniteNumber(
          raw.freshnessMaxExecutionQuoteAgeSecHourly,
          current.freshnessMaxExecutionQuoteAgeSecHourly
        )
      ),
      0,
      14400
    ),
    paperRelaxFreshness: raw.paperRelaxFreshness === true,
    paperFreshnessAgeMultiplier: clamp(
      toFiniteNumber(raw.paperFreshnessAgeMultiplier, current.paperFreshnessAgeMultiplier),
      1,
      4
    ),
    dynamicSizingEnabled: raw.dynamicSizingEnabled !== false,
    dynamicSizingMinScalePct: clamp(
      toFiniteNumber(raw.dynamicSizingMinScalePct, current.dynamicSizingMinScalePct),
      10,
      200
    ),
    dynamicSizingMaxScalePct: clamp(
      toFiniteNumber(raw.dynamicSizingMaxScalePct, current.dynamicSizingMaxScalePct),
      20,
      300
    ),
    dynamicSizingEdgeTargetCents: clamp(
      toFiniteNumber(raw.dynamicSizingEdgeTargetCents, current.dynamicSizingEdgeTargetCents),
      0.05,
      20
    ),
    dynamicSizingLiquidityTradeCount: clamp(
      Math.floor(
        toFiniteNumber(raw.dynamicSizingLiquidityTradeCount, current.dynamicSizingLiquidityTradeCount)
      ),
      1,
      200
    ),
    edgeBoostEnabled: raw.edgeBoostEnabled === true,
    edgeBoostThresholdCents: clamp(
      toFiniteNumber(raw.edgeBoostThresholdCents, current.edgeBoostThresholdCents),
      0,
      50
    ),
    edgeBoostHighThresholdCents: clamp(
      toFiniteNumber(raw.edgeBoostHighThresholdCents, current.edgeBoostHighThresholdCents),
      Math.min(
        100,
        Math.max(0, toFiniteNumber(raw.edgeBoostThresholdCents, current.edgeBoostThresholdCents))
      ),
      100
    ),
    edgeBoostScalePct: clamp(
      toFiniteNumber(raw.edgeBoostScalePct, current.edgeBoostScalePct),
      100,
      5000
    ),
    edgeBoostHighScalePct: clamp(
      toFiniteNumber(raw.edgeBoostHighScalePct, current.edgeBoostHighScalePct),
      Math.min(5000, Math.max(100, toFiniteNumber(raw.edgeBoostScalePct, current.edgeBoostScalePct))),
      5000
    ),
    pairLookbackSeconds: clamp(
      toFiniteNumber(raw.pairLookbackSeconds, current.pairLookbackSeconds),
      20,
      900
    ),
    pairMaxMarketsPerRun: clamp(
      toFiniteNumber(raw.pairMaxMarketsPerRun, current.pairMaxMarketsPerRun),
      1,
      20
    ),
    reentryMaxEntriesPerSignal: clamp(
      Math.floor(
        toFiniteNumber(raw.reentryMaxEntriesPerSignal, current.reentryMaxEntriesPerSignal)
      ),
      1,
      6
    ),
    reentryEdgeStepCents: clamp(
      toFiniteNumber(raw.reentryEdgeStepCents, current.reentryEdgeStepCents),
      0.01,
      10
    ),
    maxConditionExposureUsd: clamp(
      toFiniteNumber(raw.maxConditionExposureUsd, current.maxConditionExposureUsd),
      0,
      10000000
    ),
    maxCoinExposureSharePct: clamp(
      toFiniteNumber(raw.maxCoinExposureSharePct, current.maxCoinExposureSharePct),
      0,
      100
    ),
    maxCadenceExposureSharePct: clamp(
      toFiniteNumber(raw.maxCadenceExposureSharePct, current.maxCadenceExposureSharePct),
      0,
      100
    ),
    autoExitResidualPositions: raw.autoExitResidualPositions === true,
    residualPositionMinUsd: clamp(
      toFiniteNumber(raw.residualPositionMinUsd, current.residualPositionMinUsd),
      0.1,
      10000
    ),
    residualPositionMaxPerRun: clamp(
      Math.floor(toFiniteNumber(raw.residualPositionMaxPerRun, current.residualPositionMaxPerRun)),
      1,
      20
    ),
    residualPositionSellDiscountCents: clamp(
      toFiniteNumber(raw.residualPositionSellDiscountCents, current.residualPositionSellDiscountCents),
      0,
      25
    ),
    enableBtc: raw.enableBtc !== false,
    enableEth: raw.enableEth !== false,
    enableCadence5m: raw.enableCadence5m !== false,
    enableCadence15m: raw.enableCadence15m !== false,
    enableCadenceHourly: raw.enableCadenceHourly !== false,
    minBetUsd: clamp(toFiniteNumber(raw.minBetUsd, current.minBetUsd), 0.1, 10000),
    stopLossBalance: clamp(
      toFiniteNumber(raw.stopLossBalance, current.stopLossBalance),
      0,
      10000000
    ),
    floorToPolymarketMin: raw.floorToPolymarketMin !== false,
    maxUnresolvedImbalancesPerRun: clamp(
      Math.floor(
        toFiniteNumber(raw.maxUnresolvedImbalancesPerRun, current.maxUnresolvedImbalancesPerRun)
      ),
      1,
      10
    ),
    unwindSellSlippageCents: clamp(
      toFiniteNumber(raw.unwindSellSlippageCents, current.unwindSellSlippageCents),
      0,
      20
    ),
    unwindShareBufferPct: clamp(
      toFiniteNumber(raw.unwindShareBufferPct, current.unwindShareBufferPct),
      50,
      100
    ),
    maxDailyLiveNotionalUsd: clamp(
      toFiniteNumber(raw.maxDailyLiveNotionalUsd, current.maxDailyLiveNotionalUsd),
      0,
      10000000
    ),
    maxDailyDrawdownUsd: clamp(
      toFiniteNumber(raw.maxDailyDrawdownUsd, current.maxDailyDrawdownUsd),
      0,
      10000000
    ),
    autoStopAt: clamp(
      Math.floor(toFiniteNumber(raw.autoStopAt, current.autoStopAt)),
      0,
      4102444800000 // year 2100-01-01 UTC
    ),
    sessionTargetPairsPerHour: clamp(
      toFiniteNumber(raw.sessionTargetPairsPerHour, current.sessionTargetPairsPerHour),
      0,
      2000
    ),
    sessionMinAvgNetEdgeCents: clamp(
      toFiniteNumber(raw.sessionMinAvgNetEdgeCents, current.sessionMinAvgNetEdgeCents),
      -10,
      50
    ),
  };
}

export async function getConfig(): Promise<CopyTraderConfig> {
  const c = await kv.get<Record<string, unknown>>(CONFIG_KEY);
  if (!c) return { ...DEFAULT_CONFIG };

  // Migration path for older config keys.
  const legacyMode =
    normalizeMode(c.mode) ??
    normalizeMode(c.tradingMode) ??
    normalizeMode(c.operatingMode);
  const legacyEnabled =
    c.enabled == null ? undefined : Boolean(c.enabled ?? DEFAULT_CONFIG.enabled);
  const mode = legacyMode ?? (legacyEnabled ? "live" : "off");

  const migratedRaw: Record<string, unknown> = {
    ...c,
    mode,
    enabled: mode !== "off",
    walletUsagePercent:
      c.walletUsagePercent ?? c.walletPercent ?? DEFAULT_CONFIG.walletUsagePercent,
    maxBetUsd: c.maxBetUsd ?? c.minBetUsd ?? DEFAULT_CONFIG.maxBetUsd,
    pairChunkUsd: c.pairChunkUsd ?? c.maxBetUsd ?? DEFAULT_CONFIG.pairChunkUsd,
    maxRunBudgetUsd:
      c.maxRunBudgetUsd ?? c.runBudgetUsd ?? c.maxRunUsd ?? c.fixedRunBudgetUsd ?? 0,
    paperVirtualWalletUsd:
      c.paperVirtualWalletUsd ??
      c.paperWalletUsd ??
      c.paperWalletBalanceUsd ??
      c.paperVirtualBalanceUsd ??
      c.simulatedWalletUsd ??
      0,
    pairMinEdgeCents: c.pairMinEdgeCents ?? DEFAULT_CONFIG.pairMinEdgeCents,
    paperAllowNegativeEdge:
      c.paperAllowNegativeEdge ?? c.allowNegativeEdgeInPaper ?? c.paperAllowNegEdge ?? false,
    paperMinEdgeCents:
      c.paperMinEdgeCents ?? c.paperEdgeMinCents ?? c.minPaperEdgeCents ?? DEFAULT_CONFIG.paperMinEdgeCents,
    pairMinEdgeCents5m:
      c.pairMinEdgeCents5m ??
      c.pairMinEdge5m ??
      c.pairMinEdgeCents ??
      DEFAULT_CONFIG.pairMinEdgeCents5m,
    pairMinEdgeCents15m:
      c.pairMinEdgeCents15m ??
      c.pairMinEdge15m ??
      c.pairMinEdgeCents ??
      DEFAULT_CONFIG.pairMinEdgeCents15m,
    pairMinEdgeCentsHourly:
      c.pairMinEdgeCentsHourly ??
      c.pairMinEdgeHourly ??
      c.pairMinEdgeCents ??
      DEFAULT_CONFIG.pairMinEdgeCentsHourly,
    pairFeeBps:
      c.pairFeeBps ??
      c.pairEstimatedFeeBps ??
      c.estimatedFeeBps ??
      DEFAULT_CONFIG.pairFeeBps,
    pairSlippageCents:
      c.pairSlippageCents ??
      c.pairEstimatedSlippageCents ??
      c.estimatedSlippageCents ??
      DEFAULT_CONFIG.pairSlippageCents,
    liveMinNetEdgeSurplusCents:
      c.liveMinNetEdgeSurplusCents ??
      c.liveMinEdgeSurplusCents ??
      c.liveNetEdgeSurplusMinCents ??
      DEFAULT_CONFIG.liveMinNetEdgeSurplusCents,
    adaptiveEdgeEnabled: c.adaptiveEdgeEnabled,
    adaptiveEdgeLowActivityTradeCount:
      c.adaptiveEdgeLowActivityTradeCount ??
      c.adaptiveEdgeMinTradeCount ??
      c.adaptiveLowActivityTradeCount ??
      DEFAULT_CONFIG.adaptiveEdgeLowActivityTradeCount,
    adaptiveEdgeMaxPenaltyCents:
      c.adaptiveEdgeMaxPenaltyCents ??
      c.adaptiveEdgePenaltyCents ??
      c.adaptiveLowLiquidityPenaltyCents ??
      DEFAULT_CONFIG.adaptiveEdgeMaxPenaltyCents,
    adaptiveEdgeStalePenaltyCents:
      c.adaptiveEdgeStalePenaltyCents ??
      c.adaptiveStalePenaltyCents ??
      DEFAULT_CONFIG.adaptiveEdgeStalePenaltyCents,
    adaptiveEdgeStalePenaltyCents5m:
      c.adaptiveEdgeStalePenaltyCents5m ??
      c.adaptiveStalePenaltyCents5m ??
      c.adaptiveEdgeStalePenaltyCents ??
      DEFAULT_CONFIG.adaptiveEdgeStalePenaltyCents5m,
    adaptiveEdgeStalePenaltyCents15m:
      c.adaptiveEdgeStalePenaltyCents15m ??
      c.adaptiveStalePenaltyCents15m ??
      c.adaptiveEdgeStalePenaltyCents ??
      DEFAULT_CONFIG.adaptiveEdgeStalePenaltyCents15m,
    adaptiveEdgeStalePenaltyCentsHourly:
      c.adaptiveEdgeStalePenaltyCentsHourly ??
      c.adaptiveStalePenaltyCentsHourly ??
      c.adaptiveEdgeStalePenaltyCents ??
      DEFAULT_CONFIG.adaptiveEdgeStalePenaltyCentsHourly,
    freshnessMaxSignalAgeSec5m:
      c.freshnessMaxSignalAgeSec5m ??
      c.maxSignalAgeSec5m ??
      DEFAULT_CONFIG.freshnessMaxSignalAgeSec5m,
    freshnessMaxSignalAgeSec15m:
      c.freshnessMaxSignalAgeSec15m ??
      c.maxSignalAgeSec15m ??
      DEFAULT_CONFIG.freshnessMaxSignalAgeSec15m,
    freshnessMaxSignalAgeSecHourly:
      c.freshnessMaxSignalAgeSecHourly ??
      c.maxSignalAgeSecHourly ??
      DEFAULT_CONFIG.freshnessMaxSignalAgeSecHourly,
    freshnessMaxExecutionQuoteAgeSec5m:
      c.freshnessMaxExecutionQuoteAgeSec5m ??
      c.maxExecutionQuoteAgeSec5m ??
      DEFAULT_CONFIG.freshnessMaxExecutionQuoteAgeSec5m,
    freshnessMaxExecutionQuoteAgeSec15m:
      c.freshnessMaxExecutionQuoteAgeSec15m ??
      c.maxExecutionQuoteAgeSec15m ??
      DEFAULT_CONFIG.freshnessMaxExecutionQuoteAgeSec15m,
    freshnessMaxExecutionQuoteAgeSecHourly:
      c.freshnessMaxExecutionQuoteAgeSecHourly ??
      c.maxExecutionQuoteAgeSecHourly ??
      DEFAULT_CONFIG.freshnessMaxExecutionQuoteAgeSecHourly,
    paperRelaxFreshness:
      c.paperRelaxFreshness ??
      c.relaxFreshnessInPaper ??
      DEFAULT_CONFIG.paperRelaxFreshness,
    paperFreshnessAgeMultiplier:
      c.paperFreshnessAgeMultiplier ??
      c.freshnessPaperMultiplier ??
      DEFAULT_CONFIG.paperFreshnessAgeMultiplier,
    dynamicSizingEnabled: c.dynamicSizingEnabled,
    dynamicSizingMinScalePct:
      c.dynamicSizingMinScalePct ??
      c.dynamicSizeMinPct ??
      DEFAULT_CONFIG.dynamicSizingMinScalePct,
    dynamicSizingMaxScalePct:
      c.dynamicSizingMaxScalePct ??
      c.dynamicSizeMaxPct ??
      DEFAULT_CONFIG.dynamicSizingMaxScalePct,
    dynamicSizingEdgeTargetCents:
      c.dynamicSizingEdgeTargetCents ??
      c.dynamicSizingTargetEdgeCents ??
      DEFAULT_CONFIG.dynamicSizingEdgeTargetCents,
    dynamicSizingLiquidityTradeCount:
      c.dynamicSizingLiquidityTradeCount ??
      c.dynamicSizingLiquidityTargetTrades ??
      DEFAULT_CONFIG.dynamicSizingLiquidityTradeCount,
    edgeBoostEnabled: c.edgeBoostEnabled ?? c.edgeSurgeEnabled ?? c.highEdgeSizingEnabled ?? false,
    edgeBoostThresholdCents:
      c.edgeBoostThresholdCents ??
      c.edgeSurgeThresholdCents ??
      c.highEdgeThresholdCents ??
      DEFAULT_CONFIG.edgeBoostThresholdCents,
    edgeBoostHighThresholdCents:
      c.edgeBoostHighThresholdCents ??
      c.edgeSurgeHighThresholdCents ??
      c.highEdgeHighThresholdCents ??
      DEFAULT_CONFIG.edgeBoostHighThresholdCents,
    edgeBoostScalePct:
      c.edgeBoostScalePct ??
      c.edgeSurgeScalePct ??
      c.highEdgeScalePct ??
      DEFAULT_CONFIG.edgeBoostScalePct,
    edgeBoostHighScalePct:
      c.edgeBoostHighScalePct ??
      c.edgeSurgeHighScalePct ??
      c.highEdgeHighScalePct ??
      DEFAULT_CONFIG.edgeBoostHighScalePct,
    pairLookbackSeconds:
      c.pairLookbackSeconds ?? c.signalLookbackSeconds ?? DEFAULT_CONFIG.pairLookbackSeconds,
    pairMaxMarketsPerRun:
      c.pairMaxMarketsPerRun ?? c.maxSignalsPerRun ?? DEFAULT_CONFIG.pairMaxMarketsPerRun,
    reentryMaxEntriesPerSignal:
      c.reentryMaxEntriesPerSignal ??
      c.maxEntriesPerSignal ??
      c.reentrySlotsPerSignal ??
      DEFAULT_CONFIG.reentryMaxEntriesPerSignal,
    reentryEdgeStepCents:
      c.reentryEdgeStepCents ??
      c.reentryStepCents ??
      c.reentryMinEdgeStepCents ??
      DEFAULT_CONFIG.reentryEdgeStepCents,
    maxConditionExposureUsd:
      c.maxConditionExposureUsd ??
      c.maxConditionNotionalUsd ??
      c.maxPerConditionUsd ??
      DEFAULT_CONFIG.maxConditionExposureUsd,
    maxCoinExposureSharePct:
      c.maxCoinExposureSharePct ??
      c.maxCoinBudgetSharePct ??
      c.maxCoinSharePct ??
      DEFAULT_CONFIG.maxCoinExposureSharePct,
    maxCadenceExposureSharePct:
      c.maxCadenceExposureSharePct ??
      c.maxCadenceBudgetSharePct ??
      c.maxCadenceSharePct ??
      DEFAULT_CONFIG.maxCadenceExposureSharePct,
    autoExitResidualPositions:
      c.autoExitResidualPositions ??
      c.enableAutoResidualExit ??
      c.autoResidualExit ??
      DEFAULT_CONFIG.autoExitResidualPositions,
    residualPositionMinUsd:
      c.residualPositionMinUsd ??
      c.autoResidualExitMinUsd ??
      DEFAULT_CONFIG.residualPositionMinUsd,
    residualPositionMaxPerRun:
      c.residualPositionMaxPerRun ??
      c.autoResidualExitMaxPerRun ??
      DEFAULT_CONFIG.residualPositionMaxPerRun,
    residualPositionSellDiscountCents:
      c.residualPositionSellDiscountCents ??
      c.autoResidualExitSellDiscountCents ??
      DEFAULT_CONFIG.residualPositionSellDiscountCents,
    enableBtc: c.enableBtc,
    enableEth: c.enableEth,
    enableCadence5m: c.enableCadence5m,
    enableCadence15m: c.enableCadence15m,
    enableCadenceHourly: c.enableCadenceHourly,
    minBetUsd: c.minBetUsd ?? DEFAULT_CONFIG.minBetUsd,
    stopLossBalance: c.stopLossBalance ?? DEFAULT_CONFIG.stopLossBalance,
    floorToPolymarketMin: c.floorToPolymarketMin,
    maxUnresolvedImbalancesPerRun:
      c.maxUnresolvedImbalancesPerRun ??
      c.maxImbalancesPerRun ??
      DEFAULT_CONFIG.maxUnresolvedImbalancesPerRun,
    unwindSellSlippageCents:
      c.unwindSellSlippageCents ??
      c.unwindSlippageCents ??
      c.unwindSlippage ??
      DEFAULT_CONFIG.unwindSellSlippageCents,
    unwindShareBufferPct:
      c.unwindShareBufferPct ??
      c.unwindBufferPct ??
      c.unwindBufferPercent ??
      DEFAULT_CONFIG.unwindShareBufferPct,
    maxDailyLiveNotionalUsd:
      c.maxDailyLiveNotionalUsd ??
      c.maxDailyNotionalUsd ??
      c.dailyNotionalCapUsd ??
      DEFAULT_CONFIG.maxDailyLiveNotionalUsd,
    maxDailyDrawdownUsd:
      c.maxDailyDrawdownUsd ??
      c.dailyDrawdownCapUsd ??
      c.maxDailyLossUsd ??
      DEFAULT_CONFIG.maxDailyDrawdownUsd,
    autoStopAt: c.autoStopAt ?? c.runUntilTs ?? c.runUntilAt ?? c.autoPauseAt ?? 0,
    sessionTargetPairsPerHour:
      c.sessionTargetPairsPerHour ??
      c.sessionPairsPerHourTarget ??
      c.targetPairsPerHour ??
      0,
    sessionMinAvgNetEdgeCents:
      c.sessionMinAvgNetEdgeCents ??
      c.sessionNetEdgeTargetCents ??
      c.targetAvgNetEdgeCents ??
      0,
  };

  return sanitizeConfig(
    migratedRaw as Partial<CopyTraderConfig> & Record<string, unknown>,
    { ...DEFAULT_CONFIG }
  );
}

export async function setConfig(config: Partial<CopyTraderConfig>): Promise<CopyTraderConfig> {
  const current = await getConfig();
  const updated = sanitizeConfig({ ...current, ...config }, current);
  await kv.set(CONFIG_KEY, updated);
  return updated;
}

export async function getState(): Promise<CopyTraderState> {
  const s = await kv.get<CopyTraderState>(STATE_KEY);
  return s
    ? {
        lastTimestamp: s.lastTimestamp ?? 0,
        copiedKeys: s.copiedKeys ?? [],
        lastRunAt: s.lastRunAt,
        lastCopiedAt: s.lastCopiedAt,
        lastError: s.lastError,
        lastStrategyDiagnostics: s.lastStrategyDiagnostics
          ? normalizeStrategyDiagnostics(s.lastStrategyDiagnostics)
          : undefined,
        runsSinceLastClaim: s.runsSinceLastClaim ?? 0,
        lastClaimAt: s.lastClaimAt,
        lastClaimResult: s.lastClaimResult,
        safetyLatch: normalizeSafetyLatch(s.safetyLatch),
        dailyRisk: normalizeDailyRisk(s.dailyRisk),
      }
    : { lastTimestamp: 0, copiedKeys: [] };
}

export async function setState(state: Partial<CopyTraderState>): Promise<void> {
  const current = await getState();
  const updated = { ...current, ...state };
  await kv.set(STATE_KEY, updated);
}

export async function resetSyncState(): Promise<void> {
  const current = await getState();
  await kv.set(STATE_KEY, {
    ...current,
    lastTimestamp: 0,
    copiedKeys: [],
    lastStrategyDiagnostics: undefined,
    safetyLatch: undefined,
  });
}

export async function getRecentActivity(): Promise<RecentActivity[]> {
  const a = await kv.get<RecentActivity[]>(ACTIVITY_KEY);
  if (!Array.isArray(a)) return [];
  return a
    .map((entry) => normalizeRecentActivity(entry))
    .filter((entry): entry is RecentActivity => Boolean(entry))
    .slice(0, MAX_ACTIVITY_ITEMS);
}

export async function appendActivity(trades: RecentActivity[]): Promise<void> {
  if (trades.length === 0) return;
  const normalizedIncoming = trades
    .map((trade) => normalizeRecentActivity(trade))
    .filter((trade): trade is RecentActivity => Boolean(trade));
  if (normalizedIncoming.length === 0) return;
  const current = await getRecentActivity();
  const updated = [...normalizedIncoming, ...current].slice(0, MAX_ACTIVITY_ITEMS);
  await kv.set(ACTIVITY_KEY, updated);
}

export async function getPaperLedger(): Promise<PaperLedger> {
  const stored = await kv.get<PaperLedger>(PAPER_LEDGER_KEY);
  if (!stored || !Array.isArray(stored.lots)) return { ...DEFAULT_PAPER_LEDGER };
  return {
    lastUpdatedAt:
      typeof stored.lastUpdatedAt === "number" ? toFiniteNumber(stored.lastUpdatedAt, Date.now()) : undefined,
    lastSettledAt:
      typeof stored.lastSettledAt === "number" ? toFiniteNumber(stored.lastSettledAt, Date.now()) : undefined,
    lots: stored.lots
      .map((lot) => normalizePaperLedgerLot(lot))
      .filter((lot): lot is PaperLedgerLot => Boolean(lot))
      .slice(0, MAX_PAPER_LEDGER_LOTS),
  };
}

export async function setPaperLedger(ledger: PaperLedger): Promise<void> {
  const normalizedLots = (Array.isArray(ledger.lots) ? ledger.lots : [])
    .map((lot) => normalizePaperLedgerLot(lot))
    .filter((lot): lot is PaperLedgerLot => Boolean(lot))
    .slice(0, MAX_PAPER_LEDGER_LOTS);
  await kv.set(PAPER_LEDGER_KEY, {
    lots: normalizedLots,
    lastUpdatedAt:
      typeof ledger.lastUpdatedAt === "number" ? toFiniteNumber(ledger.lastUpdatedAt, Date.now()) : Date.now(),
    lastSettledAt:
      typeof ledger.lastSettledAt === "number" ? toFiniteNumber(ledger.lastSettledAt, Date.now()) : undefined,
  });
}

export async function recordPaperTrades(trades: CopiedTrade[]): Promise<PaperLedger> {
  if (!Array.isArray(trades) || trades.length === 0) return getPaperLedger();
  const lots = trades
    .filter((trade) => typeof trade.side === "string" && trade.side.startsWith("PAPER BUY"))
    .map((trade): PaperLedgerLot | null => {
      const amountUsd = Math.max(0, toFiniteNumber(trade.amountUsd, 0));
      const price = Math.max(0, toFiniteNumber(trade.price, 0));
      const conditionId = String(trade.conditionId ?? "").trim();
      const asset = String(trade.asset ?? "").trim();
      const title = String(trade.title ?? "").trim();
      const outcome = String(trade.outcome ?? "").trim();
      if (amountUsd <= 0 || price <= 0 || !conditionId || !asset || !title || !outcome) return null;
      const shares = amountUsd / price;
      const coin = trade.coin === "BTC" || trade.coin === "ETH" ? trade.coin : undefined;
      return {
        id: randomUUID(),
        title,
        outcome,
        side: trade.side,
        amountUsd,
        price,
        shares,
        asset,
        conditionId,
        timestamp: Math.max(0, toFiniteNumber(trade.timestamp, Date.now())),
        slug: String(trade.slug ?? "").trim() || undefined,
        coin,
        cadence: normalizeCadence(trade.cadence),
        edgeCentsAtEntry:
          typeof trade.edgeCentsAtEntry === "number" ? toFiniteNumber(trade.edgeCentsAtEntry, 0) : undefined,
        netEdgeCentsAtEntry:
          typeof trade.netEdgeCentsAtEntry === "number"
            ? toFiniteNumber(trade.netEdgeCentsAtEntry, 0)
            : undefined,
        dynamicSizingScalePct:
          typeof trade.dynamicSizingScalePct === "number"
            ? toFiniteNumber(trade.dynamicSizingScalePct, 0)
            : undefined,
        edgeBoostScalePct:
          typeof trade.edgeBoostScalePct === "number"
            ? toFiniteNumber(trade.edgeBoostScalePct, 0)
            : undefined,
      };
    })
    .filter((lot): lot is PaperLedgerLot => Boolean(lot));
  if (lots.length === 0) return getPaperLedger();
  const current = await getPaperLedger();
  const updated: PaperLedger = {
    lots: [...lots, ...current.lots].slice(0, MAX_PAPER_LEDGER_LOTS),
    lastUpdatedAt: Date.now(),
    lastSettledAt: current.lastSettledAt,
  };
  await kv.set(PAPER_LEDGER_KEY, updated);
  return updated;
}

export async function resetPaperLedger(): Promise<void> {
  await kv.set(PAPER_LEDGER_KEY, { ...DEFAULT_PAPER_LEDGER });
}

export async function getPaperStats(): Promise<PaperStats> {
  const s = await kv.get<PaperStats>(PAPER_STATS_KEY);
  if (!s) return { ...DEFAULT_PAPER_STATS };
  const totalSimulatedTrades = toFiniteNumber(s.totalSimulatedTrades, 0);
  const totalExecutedEdgeCents = toFiniteNumber(s.totalExecutedEdgeCents, 0);
  const totalExecutedNetEdgeCents = toFiniteNumber(s.totalExecutedNetEdgeCents, 0);
  const avgExecutedEdgeCents =
    totalSimulatedTrades > 0 ? totalExecutedEdgeCents / totalSimulatedTrades : 0;
  const avgExecutedNetEdgeCents =
    totalSimulatedTrades > 0 ? totalExecutedNetEdgeCents / totalSimulatedTrades : 0;
  return {
    totalRuns: toFiniteNumber(s.totalRuns, 0),
    totalSimulatedTrades,
    totalSimulatedVolumeUsd: toFiniteNumber(s.totalSimulatedVolumeUsd, 0),
    totalFailed: toFiniteNumber(s.totalFailed, 0),
    totalBudgetCapUsd: toFiniteNumber(s.totalBudgetCapUsd, 0),
    totalBudgetUsedUsd: toFiniteNumber(s.totalBudgetUsedUsd, 0),
    totalExecutedEdgeCents,
    totalExecutedNetEdgeCents,
    avgExecutedEdgeCents,
    avgExecutedNetEdgeCents,
    lastRunAt: s.lastRunAt,
    lastError: s.lastError,
    recentRuns: Array.isArray(s.recentRuns)
      ? s.recentRuns
          .map((r) => ({
            timestamp: toFiniteNumber(r.timestamp, Date.now()),
            simulatedTrades: toFiniteNumber(r.simulatedTrades, 0),
            simulatedVolumeUsd: toFiniteNumber(r.simulatedVolumeUsd, 0),
            failed: toFiniteNumber(r.failed, 0),
            budgetCapUsd: toFiniteNumber(r.budgetCapUsd, 0),
            budgetUsedUsd: toFiniteNumber(r.budgetUsedUsd, 0),
            executedEdgeCentsSum: toFiniteNumber(r.executedEdgeCentsSum, 0),
            executedNetEdgeCentsSum: toFiniteNumber(r.executedNetEdgeCentsSum, 0),
            avgExecutedEdgeCents:
              typeof r.avgExecutedEdgeCents === "number"
                ? toFiniteNumber(r.avgExecutedEdgeCents, 0)
                : undefined,
            avgExecutedNetEdgeCents:
              typeof r.avgExecutedNetEdgeCents === "number"
                ? toFiniteNumber(r.avgExecutedNetEdgeCents, 0)
                : undefined,
            error: typeof r.error === "string" ? r.error : undefined,
          }))
          .slice(0, 100)
      : [],
  };
}

export async function recordPaperRun(run: PaperRunStat): Promise<PaperStats> {
  const current = await getPaperStats();
  const normalizedExecutedEdgeCentsSum = toFiniteNumber(run.executedEdgeCentsSum, 0);
  const normalizedExecutedNetEdgeCentsSum = toFiniteNumber(run.executedNetEdgeCentsSum, 0);
  const normalizedSimulatedTrades = toFiniteNumber(run.simulatedTrades, 0);
  const normalizedRun: PaperRunStat = {
    timestamp: toFiniteNumber(run.timestamp, Date.now()),
    simulatedTrades: normalizedSimulatedTrades,
    simulatedVolumeUsd: toFiniteNumber(run.simulatedVolumeUsd, 0),
    failed: toFiniteNumber(run.failed, 0),
    budgetCapUsd: toFiniteNumber(run.budgetCapUsd, 0),
    budgetUsedUsd: toFiniteNumber(run.budgetUsedUsd, 0),
    executedEdgeCentsSum: normalizedExecutedEdgeCentsSum,
    executedNetEdgeCentsSum: normalizedExecutedNetEdgeCentsSum,
    avgExecutedEdgeCents:
      normalizedSimulatedTrades > 0
        ? normalizedExecutedEdgeCentsSum / normalizedSimulatedTrades
        : undefined,
    avgExecutedNetEdgeCents:
      normalizedSimulatedTrades > 0
        ? normalizedExecutedNetEdgeCentsSum / normalizedSimulatedTrades
        : undefined,
    error: run.error,
  };
  const totalExecutedEdgeCents = current.totalExecutedEdgeCents + normalizedExecutedEdgeCentsSum;
  const totalExecutedNetEdgeCents =
    current.totalExecutedNetEdgeCents + normalizedExecutedNetEdgeCentsSum;
  const totalSimulatedTrades = current.totalSimulatedTrades + normalizedRun.simulatedTrades;
  const updated: PaperStats = {
    totalRuns: current.totalRuns + 1,
    totalSimulatedTrades,
    totalSimulatedVolumeUsd: current.totalSimulatedVolumeUsd + normalizedRun.simulatedVolumeUsd,
    totalFailed: current.totalFailed + normalizedRun.failed,
    totalBudgetCapUsd: current.totalBudgetCapUsd + normalizedRun.budgetCapUsd,
    totalBudgetUsedUsd: current.totalBudgetUsedUsd + normalizedRun.budgetUsedUsd,
    totalExecutedEdgeCents,
    totalExecutedNetEdgeCents,
    avgExecutedEdgeCents: totalSimulatedTrades > 0 ? totalExecutedEdgeCents / totalSimulatedTrades : 0,
    avgExecutedNetEdgeCents:
      totalSimulatedTrades > 0 ? totalExecutedNetEdgeCents / totalSimulatedTrades : 0,
    lastRunAt: normalizedRun.timestamp,
    lastError: normalizedRun.error,
    recentRuns: [normalizedRun, ...current.recentRuns].slice(0, 100),
  };
  await kv.set(PAPER_STATS_KEY, updated);
  return updated;
}

export async function resetPaperStats(): Promise<void> {
  await kv.set(PAPER_STATS_KEY, { ...DEFAULT_PAPER_STATS });
}

export async function getStrategyDiagnosticsHistory(): Promise<StrategyDiagnosticsHistory> {
  const stored = await kv.get<StrategyDiagnosticsHistory>(STRATEGY_DIAGNOSTICS_HISTORY_KEY);
  if (!stored) return { ...DEFAULT_STRATEGY_DIAGNOSTICS_HISTORY };
  return {
    totalRuns: Math.max(0, toFiniteNumber(stored.totalRuns, 0)),
    lastRunAt: stored.lastRunAt ? toFiniteNumber(stored.lastRunAt, Date.now()) : undefined,
    lastError: typeof stored.lastError === "string" ? stored.lastError : undefined,
    recentRuns: Array.isArray(stored.recentRuns)
      ? stored.recentRuns.map((run) => normalizeStrategyDiagnostics(run)).slice(0, 200)
      : [],
  };
}

export async function recordStrategyDiagnostics(
  diagnostics: StrategyDiagnostics
): Promise<StrategyDiagnosticsHistory> {
  const current = await getStrategyDiagnosticsHistory();
  const normalized = normalizeStrategyDiagnostics(diagnostics);
  const updated: StrategyDiagnosticsHistory = {
    totalRuns: current.totalRuns + 1,
    lastRunAt: normalized.timestamp,
    lastError: normalized.error,
    recentRuns: [normalized, ...current.recentRuns].slice(0, 200),
  };
  await kv.set(STRATEGY_DIAGNOSTICS_HISTORY_KEY, updated);
  return updated;
}

export async function resetStrategyDiagnosticsHistory(): Promise<void> {
  await kv.set(STRATEGY_DIAGNOSTICS_HISTORY_KEY, { ...DEFAULT_STRATEGY_DIAGNOSTICS_HISTORY });
}

export async function acquireRunLock(ttlSeconds = 120): Promise<string | null> {
  const token = randomUUID();
  const res = await kv.set(RUN_LOCK_KEY, token, { nx: true, ex: ttlSeconds });
  if (res !== "OK") return null;
  return token;
}

export async function releaseRunLock(token: string): Promise<void> {
  const current = await kv.get<string>(RUN_LOCK_KEY);
  if (current === token) {
    await kv.del(RUN_LOCK_KEY);
  }
}
