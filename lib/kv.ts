import Redis from "ioredis";
import { randomUUID } from "crypto";

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
  /** Minimum required edge in cents (1 - (pA + pB)) */
  pairMinEdgeCents: number;
  /** Min edge for 5m cadence signals */
  pairMinEdgeCents5m: number;
  /** Min edge for 15m cadence signals */
  pairMinEdgeCents15m: number;
  /** Min edge for hourly cadence signals */
  pairMinEdgeCentsHourly: number;
  /** Recency window for global market signal discovery */
  pairLookbackSeconds: number;
  /** Maximum number of paired signals to execute per run */
  pairMaxMarketsPerRun: number;
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
  error?: string;
  timestamp: number;
  maxEdgeCentsSeen?: number;
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
}

export interface PaperRunStat {
  timestamp: number;
  simulatedTrades: number;
  simulatedVolumeUsd: number;
  failed: number;
  budgetCapUsd: number;
  budgetUsedUsd: number;
  error?: string;
}

export interface PaperStats {
  totalRuns: number;
  totalSimulatedTrades: number;
  totalSimulatedVolumeUsd: number;
  totalFailed: number;
  totalBudgetCapUsd: number;
  totalBudgetUsedUsd: number;
  lastRunAt?: number;
  lastError?: string;
  recentRuns: PaperRunStat[];
}

const DEFAULT_CONFIG: CopyTraderConfig = {
  enabled: false,
  mode: "off",
  walletUsagePercent: 25,
  maxBetUsd: 3,
  pairChunkUsd: 3,
  pairMinEdgeCents: 0.5,
  pairMinEdgeCents5m: 0.5,
  pairMinEdgeCents15m: 0.5,
  pairMinEdgeCentsHourly: 0.5,
  pairLookbackSeconds: 600,
  pairMaxMarketsPerRun: 4,
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
};

const DEFAULT_PAPER_STATS: PaperStats = {
  totalRuns: 0,
  totalSimulatedTrades: 0,
  totalSimulatedVolumeUsd: 0,
  totalFailed: 0,
  totalBudgetCapUsd: 0,
  totalBudgetUsedUsd: 0,
  recentRuns: [],
};

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
    error: typeof raw.error === "string" ? raw.error : undefined,
    timestamp: Math.max(0, toFiniteNumber(raw.timestamp, Date.now())),
    maxEdgeCentsSeen: typeof raw.maxEdgeCentsSeen === "number" ? raw.maxEdgeCentsSeen : undefined,
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
    pairMinEdgeCents: clamp(
      toFiniteNumber(raw.pairMinEdgeCents, current.pairMinEdgeCents),
      0,
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
    pairMinEdgeCents: c.pairMinEdgeCents ?? DEFAULT_CONFIG.pairMinEdgeCents,
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
    pairLookbackSeconds:
      c.pairLookbackSeconds ?? c.signalLookbackSeconds ?? DEFAULT_CONFIG.pairLookbackSeconds,
    pairMaxMarketsPerRun:
      c.pairMaxMarketsPerRun ?? c.maxSignalsPerRun ?? DEFAULT_CONFIG.pairMaxMarketsPerRun,
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
  return Array.isArray(a) ? a : [];
}

export async function appendActivity(trades: RecentActivity[]): Promise<void> {
  if (trades.length === 0) return;
  const current = await getRecentActivity();
  const updated = [...trades, ...current].slice(0, 50);
  await kv.set(ACTIVITY_KEY, updated);
}

export async function getPaperStats(): Promise<PaperStats> {
  const s = await kv.get<PaperStats>(PAPER_STATS_KEY);
  if (!s) return { ...DEFAULT_PAPER_STATS };
  return {
    totalRuns: toFiniteNumber(s.totalRuns, 0),
    totalSimulatedTrades: toFiniteNumber(s.totalSimulatedTrades, 0),
    totalSimulatedVolumeUsd: toFiniteNumber(s.totalSimulatedVolumeUsd, 0),
    totalFailed: toFiniteNumber(s.totalFailed, 0),
    totalBudgetCapUsd: toFiniteNumber(s.totalBudgetCapUsd, 0),
    totalBudgetUsedUsd: toFiniteNumber(s.totalBudgetUsedUsd, 0),
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
            error: typeof r.error === "string" ? r.error : undefined,
          }))
          .slice(0, 100)
      : [],
  };
}

export async function recordPaperRun(run: PaperRunStat): Promise<PaperStats> {
  const current = await getPaperStats();
  const normalizedRun: PaperRunStat = {
    timestamp: toFiniteNumber(run.timestamp, Date.now()),
    simulatedTrades: toFiniteNumber(run.simulatedTrades, 0),
    simulatedVolumeUsd: toFiniteNumber(run.simulatedVolumeUsd, 0),
    failed: toFiniteNumber(run.failed, 0),
    budgetCapUsd: toFiniteNumber(run.budgetCapUsd, 0),
    budgetUsedUsd: toFiniteNumber(run.budgetUsedUsd, 0),
    error: run.error,
  };
  const updated: PaperStats = {
    totalRuns: current.totalRuns + 1,
    totalSimulatedTrades: current.totalSimulatedTrades + normalizedRun.simulatedTrades,
    totalSimulatedVolumeUsd: current.totalSimulatedVolumeUsd + normalizedRun.simulatedVolumeUsd,
    totalFailed: current.totalFailed + normalizedRun.failed,
    totalBudgetCapUsd: current.totalBudgetCapUsd + normalizedRun.budgetCapUsd,
    totalBudgetUsedUsd: current.totalBudgetUsedUsd + normalizedRun.budgetUsedUsd,
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
