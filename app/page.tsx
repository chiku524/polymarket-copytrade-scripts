"use client";

import { useEffect, useState, useRef, useCallback } from "react";

const PAGE_SIZE = 10;
const FETCH_INTERVAL_MS = 15000;
const CONFIG_COOLDOWN_MS = 35000;
const API_TIMEOUT_MS = 90000;

async function fetchWithTimeout(url: string, opts: RequestInit = {}): Promise<Response> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), API_TIMEOUT_MS);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

async function safeJson<T = unknown>(res: Response): Promise<T> {
  const text = await res.text();
  if (!text || !text.trim()) {
    throw new Error(res.ok ? "Empty response" : `Request failed (${res.status})`);
  }
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new Error(res.ok ? "Invalid response format" : `Request failed (${res.status}): ${text.slice(0, 80)}`);
  }
}

function useDebouncedCallback<A extends unknown[]>(
  fn: (...args: A) => void,
  delay: number
): (...args: A) => void {
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  return useCallback(
    (...args: A) => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      timeoutRef.current = setTimeout(() => fnRef.current(...args), delay);
    },
    [delay]
  );
}

type CadenceKey = "5m" | "15m" | "hourly";
type CadenceConfigField = "pairMinEdgeCents5m" | "pairMinEdgeCents15m" | "pairMinEdgeCentsHourly";

interface CadenceAutoTuneSuggestion {
  key: CadenceKey;
  label: string;
  field: CadenceConfigField;
  enabled: boolean;
  current: number;
  suggested: number;
  delta: number;
  confidence: "low" | "medium" | "high";
  evaluated: number;
  eligible: number;
  executed: number;
  edgeRejects: number;
  rationale: string;
}

function clampEdgeCents(value: number): number {
  return Math.round(Math.max(0, Math.min(50, value)) * 10) / 10;
}

function getConfidenceLabel(evaluated: number): "low" | "medium" | "high" {
  if (evaluated >= 40) return "high";
  if (evaluated >= 20) return "medium";
  return "low";
}

interface Config {
  enabled: boolean;
  mode: "off" | "paper" | "live";
  walletUsagePercent: number;
  pairChunkUsd: number;
  maxRunBudgetUsd: number;
  pairMinEdgeCents: number;
  paperAllowNegativeEdge: boolean;
  paperMinEdgeCents: number;
  pairMinEdgeCents5m: number;
  pairMinEdgeCents15m: number;
  pairMinEdgeCentsHourly: number;
  pairFeeBps: number;
  pairSlippageCents: number;
  pairLookbackSeconds: number;
  pairMaxMarketsPerRun: number;
  reentryMaxEntriesPerSignal: number;
  reentryEdgeStepCents: number;
  maxConditionExposureUsd: number;
  enableBtc: boolean;
  enableEth: boolean;
  enableCadence5m: boolean;
  enableCadence15m: boolean;
  enableCadenceHourly: boolean;
  maxBetUsd: number;
  minBetUsd: number;
  stopLossBalance: number;
  floorToPolymarketMin?: boolean;
  maxUnresolvedImbalancesPerRun: number;
  unwindSellSlippageCents: number;
  unwindShareBufferPct: number;
  maxDailyLiveNotionalUsd: number;
  maxDailyDrawdownUsd: number;
  autoStopAt: number;
  sessionTargetPairsPerHour: number;
  sessionMinAvgNetEdgeCents: number;
}

const PAPER_HIGH_DATA_PRESET: Partial<Config> = {
  mode: "paper",
  walletUsagePercent: 100,
  pairChunkUsd: 1,
  maxRunBudgetUsd: 0,
  pairLookbackSeconds: 300,
  pairMaxMarketsPerRun: 20,
  reentryMaxEntriesPerSignal: 4,
  reentryEdgeStepCents: 0.1,
  maxConditionExposureUsd: 0,
  pairMinEdgeCents: 0.1,
  paperAllowNegativeEdge: true,
  paperMinEdgeCents: -0.2,
  pairMinEdgeCents5m: 0.1,
  pairMinEdgeCents15m: 0.1,
  pairMinEdgeCentsHourly: 0.2,
  pairFeeBps: 2,
  pairSlippageCents: 0.05,
  enableBtc: true,
  enableEth: true,
  enableCadence5m: true,
  enableCadence15m: true,
  enableCadenceHourly: false,
  minBetUsd: 0.1,
  floorToPolymarketMin: true,
};

const LOW_LEVEL_DEFAULT_PRESET: Config = {
  enabled: false,
  mode: "off",
  walletUsagePercent: 25,
  pairChunkUsd: 3,
  maxRunBudgetUsd: 0,
  pairMinEdgeCents: 0.5,
  paperAllowNegativeEdge: false,
  paperMinEdgeCents: -0.2,
  pairMinEdgeCents5m: 0.5,
  pairMinEdgeCents15m: 0.5,
  pairMinEdgeCentsHourly: 0.5,
  pairFeeBps: 2,
  pairSlippageCents: 0.05,
  pairLookbackSeconds: 600,
  pairMaxMarketsPerRun: 4,
  reentryMaxEntriesPerSignal: 2,
  reentryEdgeStepCents: 0.15,
  maxConditionExposureUsd: 0,
  enableBtc: true,
  enableEth: true,
  enableCadence5m: true,
  enableCadence15m: true,
  enableCadenceHourly: true,
  maxBetUsd: 3,
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

interface StrategyBreakdown {
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

interface StrategyDiagnostics {
  mode: "off" | "paper" | "live";
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

interface Status {
  config: Config;
  state: {
    lastTimestamp: number;
    lastRunAt?: number;
    lastCopiedAt?: number;
    lastError?: string;
    lastStrategyDiagnostics?: StrategyDiagnostics;
    runsSinceLastClaim?: number;
    lastClaimAt?: number;
    lastClaimResult?: { claimed: number; failed: number };
    safetyLatch?: {
      active: boolean;
      reason: string;
      triggeredAt: number;
      unresolvedAssets: string[];
      attempts: number;
      lastAttemptAt?: number;
      lastAlertAt?: number;
    };
    dailyRisk?: {
      dayKey: string;
      dayStartBalanceUsd: number;
      liveNotionalUsd: number;
      liveRuns: number;
      lastRunAt?: number;
      alertedNotionalCap?: boolean;
      alertedDrawdownCap?: boolean;
    };
  };
  cashBalance: number;
  recentActivity: { title: string; outcome: string; side: string; amountUsd: number; price: number; timestamp: number }[];
  paperStats?: {
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
    recentRuns: {
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
    }[];
  };
  strategyDiagnosticsHistory?: {
    totalRuns: number;
    lastRunAt?: number;
    lastError?: string;
    recentRuns: StrategyDiagnostics[];
  };
}

interface Position {
  asset: string;
  title: string;
  outcome: string;
  size: number;
  initialValue: number;
  currentValue: number;
  cashPnl: number;
  percentPnl: number;
  curPrice: number;
  icon?: string;
  slug: string;
  eventSlug: string;
  redeemable: boolean;
}

type PositionTab = "active" | "resolved";

export default function Home() {
  const [status, setStatus] = useState<Status | null>(null);
  const [activePositions, setActivePositions] = useState<Position[]>([]);
  const [resolvedPositions, setResolvedPositions] = useState<Position[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [running, setRunning] = useState(false);
  const [runResult, setRunResult] = useState<string | null>(null);
  const [cashingOut, setCashingOut] = useState<string | null>(null);
  const [resetting, setResetting] = useState(false);
  const [resettingPaperStats, setResettingPaperStats] = useState(false);
  const [claiming, setClaiming] = useState(false);
  const [claimResult, setClaimResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [positionTab, setPositionTab] = useState<PositionTab>("active");
  const [activePage, setActivePage] = useState(0);
  const [resolvedPage, setResolvedPage] = useState(0);
  const [trendRuns, setTrendRuns] = useState(20);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showDiagnosticsTrend, setShowDiagnosticsTrend] = useState(false);
  const [mainTab, setMainTab] = useState<"betting" | "positions" | "analytics">("betting");
  const [pairChunkInput, setPairChunkInput] = useState<string | null>(null);
  const configUpdatedAtRef = useRef<number>(0);
  const configRef = useRef<Config | null>(null);

  const fetchAll = useCallback(async (forceConfig = false) => {
    try {
      const base = typeof window !== "undefined" ? window.location.origin : "";
      const [statusRes, positionsRes] = await Promise.all([
        fetchWithTimeout(`${base}/api/status`),
        fetchWithTimeout(`${base}/api/positions`),
      ]);
      if (!statusRes.ok) throw new Error("Failed to load status");
      if (!positionsRes.ok) throw new Error("Failed to load positions");
      const statusData = (await safeJson(statusRes)) as Status;
      const positionsData = (await safeJson(positionsRes)) as { active?: Position[]; resolved?: Position[] };
      setStatus((prev) => {
        if (!prev) {
          if (statusData.config) configRef.current = statusData.config;
          return statusData;
        }
        // Background poll (cron triggers etc): never overwrite config from server.
        // Config only updates from user actions or explicit refresh (Run now, Reset, Cashout).
        if (!forceConfig) {
          return { ...statusData, config: prev.config };
        }
        if (statusData.config) configRef.current = statusData.config;
        return statusData;
      });
      setActivePositions(positionsData.active ?? []);
      setResolvedPositions(positionsData.resolved ?? []);
      setError(null);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      const isAbort = e instanceof Error && e.name === "AbortError";
      setError(isAbort ? "Request timed out. Your hosting provider may be cold starting." : msg);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAll(true);
    const id = setInterval(() => fetchAll(false), FETCH_INTERVAL_MS);
    return () => clearInterval(id);
  }, [fetchAll]);

  const updateConfig = useCallback(async (updates: Partial<Config>, optimistic?: boolean) => {
    if (!status) return;
    const previousConfig = status.config;
    if (optimistic) {
      const nextConfig = {
        ...status.config,
        ...updates,
        ...(updates.mode ? { enabled: updates.mode !== "off" } : {}),
      };
      configRef.current = nextConfig;
      configUpdatedAtRef.current = Date.now();
      setStatus((s) => (s ? { ...s, config: nextConfig } : null));
    }
    setSaving(true);
    setError(null);
    try {
      const base = typeof window !== "undefined" ? window.location.origin : "";
      const res = await fetchWithTimeout(`${base}/api/config`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(updates),
      });
      const data = (await safeJson(res)) as Config;
      if (!res.ok) throw new Error((data as { error?: string }).error ?? "Failed to save");
      configRef.current = data;
      configUpdatedAtRef.current = Date.now();
      setStatus((s) => (s ? { ...s, config: data } : null));
    } catch (e) {
      if (optimistic) {
        configRef.current = previousConfig;
        setStatus((s) => (s ? { ...s, config: previousConfig } : null));
      }
      setError(e instanceof Error ? e.message : "Failed to save");
    } finally {
      setSaving(false);
    }
  }, [status]);

  const runNow = async () => {
    setRunning(true);
    setRunResult(null);
    setError(null);
    try {
      const base = typeof window !== "undefined" ? window.location.origin : "";
      const res = await fetchWithTimeout(`${base}/api/run-now`, { method: "POST" });
      const data = (await safeJson(res)) as {
        error?: string;
        skipped?: boolean;
        reason?: string;
        mode?: string;
        paper?: number;
        copied?: number;
        simulatedVolumeUsd?: number;
        eligibleSignals?: number;
        evaluatedSignals?: number;
      };
      if (!res.ok) throw new Error(data.error ?? "Failed");
      await fetchAll(true);
      if (data.skipped) {
        if (data.reason === "mode_off") {
          setRunResult("Skipped (mode is Off)");
        } else if (data.reason === "busy") {
          setRunResult("Skipped (another run is in progress)");
        } else if (data.reason === "safety_latch_active") {
          setRunResult("Skipped (safety latch active; resolve residual exposure first)");
        } else if (data.reason === "daily_notional_cap") {
          setRunResult("Skipped (daily live notional cap reached)");
        } else if (data.reason === "daily_drawdown_cap") {
          setRunResult("Skipped (daily drawdown cap reached)");
        } else if (data.reason === "timer_expired") {
          setRunResult("Skipped (run timer expired; mode switched to Off)");
        } else {
          setRunResult("Skipped");
        }
      } else if (data.mode === "paper") {
        const simVolume = Number(data.simulatedVolumeUsd ?? 0);
        const paperCount = data.paper ?? 0;
        if (paperCount > 0) {
          setRunResult(
            `Paper simulated ${paperCount} pair${paperCount === 1 ? "" : "s"} · $${simVolume.toFixed(2)}`
          );
        } else {
          const eligible = Number(data.eligibleSignals ?? 0);
          const evaluated = Number(data.evaluatedSignals ?? 0);
          setRunResult(`Paper mode: no fills (${eligible}/${evaluated} eligible/evaluated)`);
        }
      } else if (data.error && (data.error.startsWith("Stop-loss") || data.error.startsWith("Low balance"))) {
        setRunResult(data.error);
      } else if ((data.copied ?? 0) > 0) {
        setRunResult(`Executed ${data.copied} paired signal${data.copied === 1 ? "" : "s"}`);
      } else {
        const eligible = Number(data.eligibleSignals ?? 0);
        const evaluated = Number(data.evaluatedSignals ?? 0);
        setRunResult(`No paired entries (${eligible}/${evaluated} eligible/evaluated)`);
      }
      setTimeout(() => setRunResult(null), 5000);
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Run failed";
      const isAbort = e instanceof Error && e.name === "AbortError";
      setError(isAbort ? "Run now timed out. Try again." : msg);
    } finally {
      setRunning(false);
    }
  };

  const resetSync = async () => {
    setResetting(true);
    setError(null);
    try {
      const base = typeof window !== "undefined" ? window.location.origin : "";
      const res = await fetchWithTimeout(`${base}/api/reset-sync`, { method: "POST" });
      const data = (await safeJson(res)) as Record<string, unknown>;
      if (!res.ok) throw new Error((data.error as string) ?? "Reset failed");
      await fetchAll(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reset failed");
    } finally {
      setResetting(false);
    }
  };

  const resetPaperAnalytics = async () => {
    setResettingPaperStats(true);
    setError(null);
    try {
      const base = typeof window !== "undefined" ? window.location.origin : "";
      const res = await fetchWithTimeout(`${base}/api/paper-stats`, { method: "DELETE" });
      const data = (await safeJson(res)) as Record<string, unknown>;
      if (!res.ok) throw new Error((data.error as string) ?? "Reset paper stats failed");
      await fetchAll(true);
      setRunResult("Paper analytics reset");
      setTimeout(() => setRunResult(null), 4000);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reset paper stats failed");
    } finally {
      setResettingPaperStats(false);
    }
  };

  const claimNow = async () => {
    setClaiming(true);
    setClaimResult(null);
    setError(null);
    try {
      const base = typeof window !== "undefined" ? window.location.origin : "";
      const res = await fetchWithTimeout(`${base}/api/claim-now`, { method: "POST" });
      const data = (await safeJson(res)) as { error?: string; claimed?: number; errors?: string[] };
      if (!res.ok) throw new Error(data.error ?? "Claim failed");
      await fetchAll(true);
      const claimed = data.claimed ?? 0;
      if (claimed > 0) {
        setClaimResult(`Claimed ${claimed} position${claimed === 1 ? "" : "s"}`);
      } else if (claimed === 0 && !data.error) {
        setClaimResult("No redeemable positions to claim");
      }
      if (data.errors?.length) {
        setClaimResult((prev) => (prev ? `${prev}. ${data.errors![0]}` : data.errors![0]));
      }
      setTimeout(() => setClaimResult(null), 5000);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Claim failed");
    } finally {
      setClaiming(false);
    }
  };

  const cashout = async (pos: Position) => {
    setCashingOut(pos.asset);
    try {
      const base = typeof window !== "undefined" ? window.location.origin : "";
      const res = await fetchWithTimeout(`${base}/api/cashout`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          asset: pos.asset,
          size: pos.size,
          price: pos.curPrice > 0 ? pos.curPrice : 0.5,
        }),
      });
      const data = (await safeJson(res)) as Record<string, unknown>;
      if (!res.ok) throw new Error((data.error as string) ?? "Cashout failed");
      await fetchAll(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Cashout failed");
    } finally {
      setCashingOut(null);
    }
  };

  const setMode = (mode: Config["mode"]) => updateConfig({ mode }, true);

  const debouncedUpdateConfig = useDebouncedCallback(
    (updates: Partial<Config>) => updateConfig(updates, false),
    600
  );

  const handleNumericConfigChange = useCallback(
    (field: keyof Config, value: number, min: number, max: number) => {
      const clamped = Math.max(min, Math.min(max, value));
      const base = status?.config ?? {
        enabled: false,
        mode: "off" as const,
        walletUsagePercent: 25,
        pairChunkUsd: 3,
        maxRunBudgetUsd: 0,
        pairMinEdgeCents: 0.5,
        paperAllowNegativeEdge: false,
        paperMinEdgeCents: -0.2,
        pairMinEdgeCents5m: 0.5,
        pairMinEdgeCents15m: 0.5,
        pairMinEdgeCentsHourly: 0.5,
        pairFeeBps: 2,
        pairSlippageCents: 0.05,
        pairLookbackSeconds: 120,
        pairMaxMarketsPerRun: 4,
        reentryMaxEntriesPerSignal: 2,
        reentryEdgeStepCents: 0.15,
        maxConditionExposureUsd: 0,
        enableBtc: true,
        enableEth: true,
        enableCadence5m: true,
        enableCadence15m: true,
        enableCadenceHourly: true,
        maxBetUsd: 3,
        minBetUsd: 0.1,
        stopLossBalance: 0,
        maxUnresolvedImbalancesPerRun: 1,
        unwindSellSlippageCents: 3,
        unwindShareBufferPct: 99,
        maxDailyLiveNotionalUsd: 0,
        maxDailyDrawdownUsd: 0,
        autoStopAt: 0,
        sessionTargetPairsPerHour: 0,
        sessionMinAvgNetEdgeCents: 0,
      };
      const updates: Partial<Config> = { [field]: clamped };
      if (field === "pairMinEdgeCents") {
        updates.pairMinEdgeCents5m = clamped;
        updates.pairMinEdgeCents15m = clamped;
        updates.pairMinEdgeCentsHourly = clamped;
      }
      const nextConfig = { ...base, ...updates };
      configRef.current = nextConfig;
      configUpdatedAtRef.current = Date.now();
      setStatus((s) => (s ? { ...s, config: nextConfig } : null));
      debouncedUpdateConfig(updates);
    },
    [debouncedUpdateConfig, status?.config]
  );

  if (loading) {
    return (
      <main className="min-h-screen flex items-center justify-center bg-zinc-950">
        <div className="animate-pulse text-zinc-500">Loading...</div>
      </main>
    );
  }

  if (error && !status) {
    return (
      <main className="min-h-screen flex items-center justify-center p-6 bg-zinc-950">
        <div className="text-center">
          <p className="text-red-400 mb-4">{error}</p>
          <button onClick={() => fetchAll(true)} className="px-4 py-2 rounded-lg bg-zinc-800 hover:bg-zinc-700 text-sm">
            Retry
          </button>
        </div>
      </main>
    );
  }

  const cfg = status?.config ?? {
    enabled: false,
    mode: "off" as const,
    walletUsagePercent: 25,
    pairChunkUsd: 3,
    maxRunBudgetUsd: 0,
    pairMinEdgeCents: 0.5,
    paperAllowNegativeEdge: false,
    paperMinEdgeCents: -0.2,
    pairMinEdgeCents5m: 0.5,
    pairMinEdgeCents15m: 0.5,
    pairMinEdgeCentsHourly: 0.5,
    pairFeeBps: 2,
    pairSlippageCents: 0.05,
    pairLookbackSeconds: 120,
    pairMaxMarketsPerRun: 4,
    reentryMaxEntriesPerSignal: 2,
    reentryEdgeStepCents: 0.15,
    maxConditionExposureUsd: 0,
    enableBtc: true,
    enableEth: true,
    enableCadence5m: true,
    enableCadence15m: true,
    enableCadenceHourly: true,
    maxBetUsd: 3,
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
  const activity = status?.recentActivity ?? [];
  const paperStats = status?.paperStats ?? {
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
  const avgTradesPerRun =
    paperStats.totalRuns > 0 ? paperStats.totalSimulatedTrades / paperStats.totalRuns : 0;
  const avgBudgetUsagePct =
    paperStats.totalBudgetCapUsd > 0
      ? (paperStats.totalBudgetUsedUsd / paperStats.totalBudgetCapUsd) * 100
      : 0;
  const avgExecutedEdgeCents = Number(paperStats.avgExecutedEdgeCents ?? 0);
  const avgExecutedNetEdgeCents = Number(paperStats.avgExecutedNetEdgeCents ?? 0);
  const lastDiag = status?.state.lastStrategyDiagnostics;
  const rejectedEntries = Object.entries(lastDiag?.rejectedReasons ?? {}).sort(
    (a, b) => b[1] - a[1]
  );
  const rejectionTotal = rejectedEntries.reduce((sum, [, count]) => sum + count, 0);
  const diagnosticsHistory = status?.strategyDiagnosticsHistory ?? {
    totalRuns: 0,
    recentRuns: [] as StrategyDiagnostics[],
  };
  const sessionWindowMs = 60 * 60 * 1000;
  const sessionSample = diagnosticsHistory.recentRuns.filter(
    (run) => run.timestamp >= Date.now() - sessionWindowMs
  );
  const sessionExecutedPairs = sessionSample.reduce(
    (sum, run) => sum + (run.mode === "paper" ? run.paper : run.copied),
    0
  );
  const sessionNetEdgeWeighted = sessionSample.reduce((sum, run) => {
    const executedPairs = run.mode === "paper" ? run.paper : run.copied;
    const avgNetEdge = run.avgExecutedNetEdgeCents ?? run.avgExecutedEdgeCents;
    if (executedPairs <= 0 || avgNetEdge == null) return sum;
    return sum + avgNetEdge * executedPairs;
  }, 0);
  const sessionAvgNetEdgeCents =
    sessionExecutedPairs > 0 ? sessionNetEdgeWeighted / sessionExecutedPairs : 0;
  const sessionTargetPairsPerHour = Math.max(0, Number(cfg.sessionTargetPairsPerHour) || 0);
  const sessionMinAvgNetEdgeCents = Number(cfg.sessionMinAvgNetEdgeCents) || 0;
  const sessionPaceOnTrack =
    sessionTargetPairsPerHour <= 0 || sessionExecutedPairs >= sessionTargetPairsPerHour;
  const sessionQualityOnTrack =
    sessionMinAvgNetEdgeCents <= 0 ||
    (sessionExecutedPairs > 0 && sessionAvgNetEdgeCents >= sessionMinAvgNetEdgeCents);
  const sessionKpiStatus =
    sessionTargetPairsPerHour <= 0 && sessionMinAvgNetEdgeCents <= 0
      ? "Targets off"
      : sessionPaceOnTrack && sessionQualityOnTrack
        ? "On track"
        : "Needs adjustment";
  const trendWindow = Math.max(1, trendRuns);
  const trendSample = diagnosticsHistory.recentRuns.slice(0, trendWindow);
  const trendCount = trendSample.length;
  const trendExecutedTotal = trendSample.reduce(
    (sum, run) => sum + (run.mode === "paper" ? run.paper : run.copied),
    0
  );
  const trendAvgEvaluated =
    trendCount > 0 ? trendSample.reduce((sum, run) => sum + run.evaluatedSignals, 0) / trendCount : 0;
  const trendAvgEligible =
    trendCount > 0 ? trendSample.reduce((sum, run) => sum + run.eligibleSignals, 0) / trendCount : 0;
  const trendAvgExecuted = trendCount > 0 ? trendExecutedTotal / trendCount : 0;
  const trendExecutedEdgeCentsWeighted = trendSample.reduce((sum, run) => {
    const executedPairs = run.mode === "paper" ? run.paper : run.copied;
    if (executedPairs <= 0 || run.avgExecutedEdgeCents == null) return sum;
    return sum + run.avgExecutedEdgeCents * executedPairs;
  }, 0);
  const trendExecutedPairs = trendSample.reduce(
    (sum, run) => sum + (run.mode === "paper" ? run.paper : run.copied),
    0
  );
  const trendAvgExecutedEdgeCents =
    trendExecutedPairs > 0 ? trendExecutedEdgeCentsWeighted / trendExecutedPairs : 0;
  const trendExecutedNetEdgeCentsWeighted = trendSample.reduce((sum, run) => {
    const executedPairs = run.mode === "paper" ? run.paper : run.copied;
    if (executedPairs <= 0 || run.avgExecutedNetEdgeCents == null) return sum;
    return sum + run.avgExecutedNetEdgeCents * executedPairs;
  }, 0);
  const trendAvgExecutedNetEdgeCents =
    trendExecutedPairs > 0 ? trendExecutedNetEdgeCentsWeighted / trendExecutedPairs : 0;
  const trendErrorRuns = trendSample.filter((run) => Boolean(run.error) || run.failed > 0).length;
  const trendBudgetUsed = trendSample.reduce((sum, run) => sum + run.budgetUsedUsd, 0);
  const trendBudgetCap = trendSample.reduce((sum, run) => sum + run.budgetCapUsd, 0);
  const trendAvgBudgetUsagePct = trendBudgetCap > 0 ? (trendBudgetUsed / trendBudgetCap) * 100 : 0;
  const trendRejectedReasons = trendSample.reduce<Record<string, number>>((acc, run) => {
    for (const [reason, count] of Object.entries(run.rejectedReasons ?? {})) {
      acc[reason] = (acc[reason] ?? 0) + Number(count || 0);
    }
    return acc;
  }, {});
  const trendRejectedEntries = Object.entries(trendRejectedReasons).sort((a, b) => b[1] - a[1]);
  const trendRejectionTotal = trendRejectedEntries.reduce((sum, [, count]) => sum + count, 0);
  const makeBreakdown = () =>
    ({
      byCoin: { BTC: 0, ETH: 0 },
      byCadence: { "5m": 0, "15m": 0, hourly: 0, other: 0 },
    }) satisfies StrategyBreakdown;
  const trendEvaluatedBreakdown = trendSample.reduce((acc, run) => {
    const breakdown = run.evaluatedBreakdown;
    acc.byCoin.BTC += breakdown?.byCoin.BTC ?? 0;
    acc.byCoin.ETH += breakdown?.byCoin.ETH ?? 0;
    acc.byCadence["5m"] += breakdown?.byCadence["5m"] ?? 0;
    acc.byCadence["15m"] += breakdown?.byCadence["15m"] ?? 0;
    acc.byCadence.hourly += breakdown?.byCadence.hourly ?? 0;
    acc.byCadence.other += breakdown?.byCadence.other ?? 0;
    return acc;
  }, makeBreakdown());
  const trendEligibleBreakdown = trendSample.reduce((acc, run) => {
    const breakdown = run.eligibleBreakdown;
    acc.byCoin.BTC += breakdown?.byCoin.BTC ?? 0;
    acc.byCoin.ETH += breakdown?.byCoin.ETH ?? 0;
    acc.byCadence["5m"] += breakdown?.byCadence["5m"] ?? 0;
    acc.byCadence["15m"] += breakdown?.byCadence["15m"] ?? 0;
    acc.byCadence.hourly += breakdown?.byCadence.hourly ?? 0;
    acc.byCadence.other += breakdown?.byCadence.other ?? 0;
    return acc;
  }, makeBreakdown());
  const trendExecutedBreakdown = trendSample.reduce((acc, run) => {
    const breakdown = run.executedBreakdown;
    acc.byCoin.BTC += breakdown?.byCoin.BTC ?? 0;
    acc.byCoin.ETH += breakdown?.byCoin.ETH ?? 0;
    acc.byCadence["5m"] += breakdown?.byCadence["5m"] ?? 0;
    acc.byCadence["15m"] += breakdown?.byCadence["15m"] ?? 0;
    acc.byCadence.hourly += breakdown?.byCadence.hourly ?? 0;
    acc.byCadence.other += breakdown?.byCadence.other ?? 0;
    return acc;
  }, makeBreakdown());
  const lastDiagEvaluatedBreakdown =
    lastDiag?.evaluatedBreakdown ??
    ({
      byCoin: { BTC: 0, ETH: 0 },
      byCadence: { "5m": 0, "15m": 0, hourly: 0, other: 0 },
    } satisfies StrategyBreakdown);
  const lastDiagEligibleBreakdown =
    lastDiag?.eligibleBreakdown ??
    ({
      byCoin: { BTC: 0, ETH: 0 },
      byCadence: { "5m": 0, "15m": 0, hourly: 0, other: 0 },
    } satisfies StrategyBreakdown);
  const lastDiagExecutedBreakdown =
    lastDiag?.executedBreakdown ??
    ({
      byCoin: { BTC: 0, ETH: 0 },
      byCadence: { "5m": 0, "15m": 0, hourly: 0, other: 0 },
    } satisfies StrategyBreakdown);
  const selectedCoins = [cfg.enableBtc ? "BTC" : null, cfg.enableEth ? "ETH" : null]
    .filter(Boolean)
    .join(", ") || "None";
  const selectedCadences = [
    cfg.enableCadence5m ? "5m" : null,
    cfg.enableCadence15m ? "15m" : null,
    cfg.enableCadenceHourly ? "Hourly" : null,
  ]
    .filter(Boolean)
    .join(", ") || "None";
  const cadenceEdgeSummary = `5m ${cfg.pairMinEdgeCents5m.toFixed(1)}¢ · 15m ${cfg.pairMinEdgeCents15m.toFixed(1)}¢ · Hourly ${cfg.pairMinEdgeCentsHourly.toFixed(1)}¢`;
  const edgeQualitySummary = `fee ${cfg.pairFeeBps.toFixed(1)} bps · slippage ${cfg.pairSlippageCents.toFixed(2)}¢/leg`;
  const guardrailSummary = `max imbalances ${cfg.maxUnresolvedImbalancesPerRun} · unwind slippage ${cfg.unwindSellSlippageCents.toFixed(1)}¢ · unwind buffer ${cfg.unwindShareBufferPct.toFixed(0)}%`;
  const dailyCapSummary = `notional cap ${
    cfg.maxDailyLiveNotionalUsd > 0 ? `$${cfg.maxDailyLiveNotionalUsd.toFixed(0)}` : "off"
  } · drawdown cap ${
    cfg.maxDailyDrawdownUsd > 0 ? `$${cfg.maxDailyDrawdownUsd.toFixed(0)}` : "off"
  }`;
  const timerRemainingMs = cfg.autoStopAt > 0 ? cfg.autoStopAt - Date.now() : 0;
  const timerRemainingMinutes = Math.max(0, Math.ceil(timerRemainingMs / 60000));
  const runTimerSummary =
    cfg.autoStopAt > 0
      ? timerRemainingMs > 0
        ? `auto-stop in ~${timerRemainingMinutes}m`
        : "timer expired (next cycle will switch off)"
      : "auto-stop off";
  const runBudgetSummary =
    cfg.maxRunBudgetUsd > 0 ? `$${cfg.maxRunBudgetUsd.toFixed(2)} fixed cap` : `${cfg.walletUsagePercent}% wallet cap`;
  const conditionExposureSummary =
    cfg.maxConditionExposureUsd > 0
      ? `$${cfg.maxConditionExposureUsd.toFixed(2)} / condition`
      : "condition cap off";
  const reentrySummary =
    cfg.reentryMaxEntriesPerSignal > 1
      ? `${cfg.reentryMaxEntriesPerSignal}x/signal @ ${cfg.reentryEdgeStepCents.toFixed(2)}¢ step`
      : "re-entry off";
  const sessionTargetSummary =
    cfg.sessionTargetPairsPerHour > 0 || cfg.sessionMinAvgNetEdgeCents > 0
      ? `session target ${cfg.sessionTargetPairsPerHour.toFixed(0)} pairs/h @ ${cfg.sessionMinAvgNetEdgeCents.toFixed(2)}¢+`
      : "session targets off";
  const paperEdgeSummary =
    cfg.mode === "paper" && cfg.paperAllowNegativeEdge
      ? `paper edge override ${cfg.paperMinEdgeCents.toFixed(1)}¢`
      : "paper edge override off";
  const safetyLatch = status?.state.safetyLatch;
  const dailyRisk = status?.state.dailyRisk;
  const currentBalanceUsd = status?.cashBalance ?? 0;
  const dailyDrawdownUsd = dailyRisk
    ? Math.max(0, (Number(dailyRisk.dayStartBalanceUsd) || 0) - currentBalanceUsd)
    : 0;
  const minSamplesForSuggestion = Math.max(8, Math.ceil(trendCount * 0.8));
  const cadenceSuggestionInputs: Array<
    Pick<CadenceAutoTuneSuggestion, "key" | "label" | "field" | "enabled" | "current">
  > = [
    {
      key: "5m",
      label: "5m",
      field: "pairMinEdgeCents5m",
      enabled: cfg.enableCadence5m,
      current: cfg.pairMinEdgeCents5m,
    },
    {
      key: "15m",
      label: "15m",
      field: "pairMinEdgeCents15m",
      enabled: cfg.enableCadence15m,
      current: cfg.pairMinEdgeCents15m,
    },
    {
      key: "hourly",
      label: "Hourly",
      field: "pairMinEdgeCentsHourly",
      enabled: cfg.enableCadenceHourly,
      current: cfg.pairMinEdgeCentsHourly,
    },
  ];
  const cadenceAutoSuggestions: CadenceAutoTuneSuggestion[] = cadenceSuggestionInputs.map((entry) => {
    const evaluated = trendEvaluatedBreakdown.byCadence[entry.key];
    const eligible = trendEligibleBreakdown.byCadence[entry.key];
    const executed = trendExecutedBreakdown.byCadence[entry.key];
    const edgeRejectKey = `edge_below_threshold_${entry.key}`;
    const edgeRejects = Number(trendRejectedReasons[edgeRejectKey] ?? 0);
    const passRate = evaluated > 0 ? eligible / evaluated : 0;
    const execRate = evaluated > 0 ? executed / evaluated : 0;
    const edgeRejectRate = evaluated > 0 ? edgeRejects / evaluated : 0;

    let delta = 0;
    let rationale = "Threshold looks balanced for current trend window.";

    if (!entry.enabled) {
      rationale = "Cadence is disabled in Trade controls.";
    } else if (evaluated < minSamplesForSuggestion) {
      rationale = `Need more samples for this cadence (${evaluated}/${minSamplesForSuggestion}).`;
    } else if (edgeRejectRate >= 0.55 && execRate < 0.18) {
      delta = -0.2;
      rationale = "High edge-threshold rejections with low execution rate; loosen slightly.";
    } else if (edgeRejectRate >= 0.35 && execRate < 0.28) {
      delta = -0.1;
      rationale = "Moderate threshold pressure; loosen a bit to admit more entries.";
    } else if (edgeRejectRate <= 0.08 && execRate > 0.65 && trendAvgBudgetUsagePct > 92) {
      delta = 0.2;
      rationale = "Very high fill rate and budget pressure; tighten to improve selectivity.";
    } else if (edgeRejectRate <= 0.15 && passRate > 0.5 && trendAvgBudgetUsagePct > 82) {
      delta = 0.1;
      rationale = "Healthy pass rate with elevated budget use; tighten modestly.";
    }

    const suggested = clampEdgeCents(entry.current + delta);
    const normalizedDelta = clampEdgeCents(suggested - entry.current);
    const confidence = entry.enabled ? getConfidenceLabel(evaluated) : "low";
    return {
      ...entry,
      suggested,
      delta: normalizedDelta,
      confidence,
      evaluated,
      eligible,
      executed,
      edgeRejects,
      rationale,
    };
  });
  const suggestedEdgePatch = cadenceAutoSuggestions.reduce((acc, suggestion) => {
    if (suggestion.enabled && Math.abs(suggestion.delta) >= 0.1) {
      acc[suggestion.field] = suggestion.suggested;
    }
    return acc;
  }, {} as Partial<Config>);
  const suggestedEdgeChangeCount = Object.keys(suggestedEdgePatch).length;
  const applySuggestedEdges = async () => {
    if (suggestedEdgeChangeCount === 0) {
      setRunResult("No cadence edge updates suggested for current trend window");
      setTimeout(() => setRunResult(null), 4500);
      return;
    }
    await updateConfig(suggestedEdgePatch, true);
    setRunResult(
      `Applied ${suggestedEdgeChangeCount} auto-tuned cadence edge threshold${
        suggestedEdgeChangeCount === 1 ? "" : "s"
      }`
    );
    setTimeout(() => setRunResult(null), 4500);
  };

  const applyPaperBaselinePreset = async () => {
    await updateConfig(PAPER_HIGH_DATA_PRESET, true);
    setRunResult(
      "Applied Paper high-data preset (0.1c edge, max markets/run, 5m+15m focus)"
    );
    setTimeout(() => setRunResult(null), 4500);
  };

  const setRunTimerPreset = async (hours: number) => {
    const stopAt = Date.now() + hours * 60 * 60 * 1000;
    await updateConfig({ autoStopAt: stopAt }, true);
    setRunResult(`Auto-stop timer set for ${hours}h`);
    setTimeout(() => setRunResult(null), 4500);
  };

  const clearRunTimer = async () => {
    await updateConfig({ autoStopAt: 0 }, true);
    setRunResult("Auto-stop timer cleared");
    setTimeout(() => setRunResult(null), 4500);
  };

  const resetAllToDefaults = async () => {
    if (!window.confirm("Reset all parameters to low-level defaults and switch mode to Off?")) return;
    await updateConfig(LOW_LEVEL_DEFAULT_PRESET, true);
    setRunResult("Reset all parameters to low-level defaults");
    setTimeout(() => setRunResult(null), 4500);
  };
  const pairChunkDisplayValue = pairChunkInput ?? String(cfg.pairChunkUsd);
  const handlePairChunkChange = (raw: string) => {
    setPairChunkInput(raw);
    if (raw.trim() === "") return;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) return;
    handleNumericConfigChange("pairChunkUsd", parsed, 1, 10000);
  };
  const commitPairChunkInput = () => {
    if (pairChunkInput === null) return;
    const raw = pairChunkInput.trim();
    setPairChunkInput(null);
    if (raw === "") return;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) return;
    handleNumericConfigChange("pairChunkUsd", parsed, 1, 10000);
  };

  const lastRunAgo = status?.state.lastRunAt
    ? Math.floor((Date.now() - status.state.lastRunAt) / 1000)
    : null;
  const workerStatusText =
    lastRunAgo === null
      ? "No runs yet"
      : lastRunAgo < 60
        ? `Last run ${lastRunAgo}s ago`
        : lastRunAgo < 3600
          ? `Last run ${Math.floor(lastRunAgo / 60)}m ago`
          : `Last run ${Math.floor(lastRunAgo / 3600)}h ago`;

  const tabs = [
    { id: "betting" as const, label: "Betting" },
    { id: "positions" as const, label: "Positions" },
    { id: "analytics" as const, label: "Analytics" },
  ] as const;

  return (
    <main className="min-h-screen bg-zinc-950 text-zinc-100">
      <div className="max-w-2xl mx-auto p-4 sm:p-6 md:p-8">
        {/* Header */}
        <header className="mb-4">
          <h1 className="text-lg sm:text-xl font-semibold tracking-tight text-zinc-100">Polymarket Paired Trader</h1>
          <p className="mt-0.5 text-xs text-zinc-500">BTC/ETH Up–Down · {cfg.mode}</p>
        </header>

        {/* Tab nav */}
        <nav className="mb-6 flex gap-0.5 p-0.5 rounded-lg bg-zinc-900/60 border border-zinc-800/50">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setMainTab(tab.id)}
              className={`flex-1 py-2 px-3 rounded-md text-sm font-medium transition-colors ${
                mainTab === tab.id
                  ? "bg-zinc-800 text-zinc-100"
                  : "text-zinc-500 hover:text-zinc-300"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>

        {error && (
          <div className="mb-4 p-3 rounded-lg bg-red-500/10 border border-red-500/30 flex items-center justify-between gap-3">
            <p className="text-sm text-red-400 flex-1 min-w-0">{error}</p>
            <button
              onClick={() => setError(null)}
              className="text-xs text-red-400 hover:text-red-300 shrink-0"
            >
              Dismiss
            </button>
          </div>
        )}

        {safetyLatch?.active && (
          <div className="mb-4 p-3 rounded-lg bg-red-500/10 border border-red-500/30">
            <p className="text-sm text-red-300 font-medium">Safety latch active</p>
            <p className="text-xs text-red-300/90 mt-0.5">{safetyLatch.reason}</p>
            <p className="text-xs text-zinc-500 mt-1">
              Resolve exposure first, then use <strong>Reset sync</strong>.
            </p>
          </div>
        )}

        {/* Control bar - always visible */}
        <section className="mb-5 p-3 sm:p-4 rounded-xl bg-zinc-900/50 border border-zinc-800/40">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-3">
              <div className="flex gap-0.5 p-0.5 rounded-md bg-zinc-800/80">
                {(["off", "paper", "live"] as const).map((mode) => (
                  <button
                    key={mode}
                    onClick={() => setMode(mode)}
                    disabled={saving}
                    className={`px-3 py-1.5 rounded text-xs font-medium uppercase transition-all disabled:opacity-50 ${
                      cfg.mode === mode
                        ? mode === "live"
                          ? "bg-emerald-500/50 text-emerald-100"
                          : mode === "paper"
                            ? "bg-sky-500/50 text-sky-100"
                            : "bg-zinc-700 text-zinc-200"
                        : "text-zinc-500 hover:text-zinc-300"
                    }`}
                  >
                    {mode}
                  </button>
                ))}
              </div>
              <span className="text-sm text-zinc-400">
                Balance <span className="font-medium text-emerald-400">${(status?.cashBalance ?? 0).toFixed(2)}</span>
              </span>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={runNow}
                disabled={running}
                className="px-3 py-1.5 rounded-lg bg-zinc-700 hover:bg-zinc-600 text-sm font-medium disabled:opacity-50"
              >
                {running ? "Running…" : "Run now"}
              </button>
              <button onClick={claimNow} disabled={claiming} className="px-2.5 py-1.5 rounded-lg bg-sky-500/20 hover:bg-sky-500/30 text-sky-400 text-sm disabled:opacity-50">
                {claiming ? "…" : "Claim"}
              </button>
              <button onClick={resetSync} disabled={resetting} className="px-2.5 py-1.5 rounded-lg bg-amber-500/20 hover:bg-amber-500/30 text-amber-400 text-sm disabled:opacity-50">
                {resetting ? "…" : "Reset"}
              </button>
            </div>
          </div>
          <p className="text-[11px] text-zinc-500 mt-2">{workerStatusText}{(runResult || claimResult) && ` · ${runResult || claimResult}`}</p>
        </section>

        {/* Betting tab */}
        {mainTab === "betting" && (
        <>
        {/* Trade controls */}
        <section className="mb-6 p-5 rounded-xl bg-zinc-900/40 border border-zinc-800/40">
          <div className="mb-4 flex items-center justify-between">
            <h2 className="text-sm font-medium text-zinc-300">Betting parameters</h2>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => setShowAdvanced((v) => !v)}
                className="text-xs text-zinc-500 hover:text-zinc-300"
              >
                {showAdvanced ? "−" : "+"} Advanced
              </button>
              <button
                onClick={applyPaperBaselinePreset}
                disabled={saving}
                className="px-2.5 py-1 rounded-md bg-sky-500/20 hover:bg-sky-500/30 text-sky-300 text-xs disabled:opacity-40"
              >
                Paper high-data preset
              </button>
              <button
                onClick={resetAllToDefaults}
                disabled={saving}
                className="px-2.5 py-1 rounded-md bg-amber-500/20 hover:bg-amber-500/30 text-amber-300 text-xs disabled:opacity-40"
              >
                Reset defaults
              </button>
            </div>
          </div>
          <p className="text-xs text-zinc-500 mb-5">
                {selectedCoins} · {selectedCadences} · ${cfg.pairChunkUsd}/pair · {runBudgetSummary} · {conditionExposureSummary} · {reentrySummary} · {edgeQualitySummary} · {sessionTargetSummary} · {paperEdgeSummary} · {runTimerSummary}
          </p>
          <div className="mb-5 rounded-lg bg-zinc-900/70 border border-zinc-800 p-3">
            <p className="text-[11px] text-zinc-500 uppercase mb-2">Run timer (auto-stop)</p>
            <div className="flex flex-wrap items-center gap-2">
              {[1, 2, 4, 8, 12, 24].map((hours) => (
                <button
                  key={hours}
                  onClick={() => setRunTimerPreset(hours)}
                  disabled={saving}
                  className="px-2.5 py-1 rounded-md bg-zinc-800 hover:bg-zinc-700 text-zinc-300 text-xs disabled:opacity-40"
                >
                  {hours}h
                </button>
              ))}
              <button
                onClick={clearRunTimer}
                disabled={saving || cfg.autoStopAt <= 0}
                className="px-2.5 py-1 rounded-md bg-zinc-800 hover:bg-zinc-700 text-zinc-400 text-xs disabled:opacity-30"
              >
                Clear
              </button>
            </div>
            <p className="text-xs text-zinc-500 mt-2">
              {cfg.autoStopAt > 0
                ? `Auto-stop at ${new Date(cfg.autoStopAt).toLocaleString()}`
                : "No timer active"}
            </p>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
            <div>
              <p className="text-xs text-zinc-500 mb-1">Min edge (¢)</p>
              <input
                type="number"
                min={0}
                max={50}
                step={0.1}
                value={cfg.pairMinEdgeCents}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairMinEdgeCents",
                    parseFloat(e.target.value) || 0,
                    0,
                    50
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            {showAdvanced && (
            <>
            <div>
              <p className="text-xs text-zinc-500 mb-1">5m edge (¢)</p>
              <input
                type="number"
                min={0}
                max={50}
                step={0.1}
                value={cfg.pairMinEdgeCents5m}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairMinEdgeCents5m",
                    parseFloat(e.target.value) || 0,
                    0,
                    50
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">15m edge (¢)</p>
              <input
                type="number"
                min={0}
                max={50}
                step={0.1}
                value={cfg.pairMinEdgeCents15m}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairMinEdgeCents15m",
                    parseFloat(e.target.value) || 0,
                    0,
                    50
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Hourly edge (¢)</p>
              <input
                type="number"
                min={0}
                max={50}
                step={0.1}
                value={cfg.pairMinEdgeCentsHourly}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairMinEdgeCentsHourly",
                    parseFloat(e.target.value) || 0,
                    0,
                    50
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Fee buffer (bps)</p>
              <input
                type="number"
                min={0}
                max={200}
                step={0.1}
                value={cfg.pairFeeBps}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairFeeBps",
                    parseFloat(e.target.value) || 0,
                    0,
                    200
                  )
                }
                disabled={saving}
                className="w-24 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Slippage/leg (¢)</p>
              <input
                type="number"
                min={0}
                max={25}
                step={0.01}
                value={cfg.pairSlippageCents}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairSlippageCents",
                    parseFloat(e.target.value) || 0,
                    0,
                    25
                  )
                }
                disabled={saving}
                className="w-24 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            </>
            )}
            <div>
              <p className="text-xs text-zinc-500 mb-1">Pair chunk ($)</p>
              <input
                type="number"
                min={1}
                max={10000}
                step="any"
                value={pairChunkDisplayValue}
                onChange={(e) => handlePairChunkChange(e.target.value)}
                onBlur={commitPairChunkInput}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    (e.currentTarget as HTMLInputElement).blur();
                  }
                }}
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Wallet %</p>
              <input
                type="number"
                min={1}
                max={100}
                step={1}
                value={cfg.walletUsagePercent}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "walletUsagePercent",
                    parseInt(e.target.value, 10) || 25,
                    1,
                    100
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Run budget cap ($)</p>
              <input
                type="number"
                min={0}
                step={1}
                value={cfg.maxRunBudgetUsd || ""}
                placeholder="0 = wallet % only"
                onChange={(e) =>
                  handleNumericConfigChange(
                    "maxRunBudgetUsd",
                    parseFloat(e.target.value) || 0,
                    0,
                    1000000
                  )
                }
                disabled={saving}
                className="w-28 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60 placeholder:text-zinc-500"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Condition cap ($)</p>
              <input
                type="number"
                min={0}
                step={1}
                value={cfg.maxConditionExposureUsd || ""}
                placeholder="0 = disabled"
                onChange={(e) =>
                  handleNumericConfigChange(
                    "maxConditionExposureUsd",
                    parseFloat(e.target.value) || 0,
                    0,
                    1000000
                  )
                }
                disabled={saving}
                className="w-28 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60 placeholder:text-zinc-500"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Target pairs/hour</p>
              <input
                type="number"
                min={0}
                step={1}
                value={cfg.sessionTargetPairsPerHour || ""}
                placeholder="0 = off"
                onChange={(e) =>
                  handleNumericConfigChange(
                    "sessionTargetPairsPerHour",
                    parseFloat(e.target.value) || 0,
                    0,
                    2000
                  )
                }
                disabled={saving}
                className="w-28 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60 placeholder:text-zinc-500"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Target net edge (¢)</p>
              <input
                type="number"
                min={-10}
                max={50}
                step={0.1}
                value={cfg.sessionMinAvgNetEdgeCents || ""}
                placeholder="0 = off"
                onChange={(e) =>
                  handleNumericConfigChange(
                    "sessionMinAvgNetEdgeCents",
                    parseFloat(e.target.value) || 0,
                    -10,
                    50
                  )
                }
                disabled={saving}
                className="w-28 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60 placeholder:text-zinc-500"
              />
            </div>
            <div className="col-span-2 sm:col-span-3 lg:col-span-4 rounded-lg bg-zinc-900/70 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase mb-2">Paper-only edge override</p>
              <div className="flex flex-wrap items-center gap-3">
                <button
                  role="switch"
                  aria-checked={cfg.paperAllowNegativeEdge}
                  onClick={() =>
                    updateConfig({ paperAllowNegativeEdge: !cfg.paperAllowNegativeEdge }, true)
                  }
                  disabled={saving}
                  className={`
                    relative w-11 h-6 rounded-full transition-colors flex-shrink-0
                    ${cfg.paperAllowNegativeEdge ? "bg-sky-500" : "bg-zinc-700"}
                    ${saving ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}
                  `}
                >
                  <span
                    className={`
                      absolute top-1 w-4 h-4 rounded-full bg-white shadow transition-transform
                      ${cfg.paperAllowNegativeEdge ? "left-6 translate-x-[-2px]" : "left-1"}
                    `}
                  />
                </button>
                <div>
                  <p className="text-xs text-zinc-300">Allow slight negative edge in paper mode only</p>
                  <p className="text-[11px] text-zinc-500">
                    Live mode still enforces non-negative edge thresholds.
                  </p>
                </div>
                <div>
                  <p className="text-xs text-zinc-500 mb-1">Paper min edge (¢)</p>
                  <input
                    type="number"
                    min={-10}
                    max={50}
                    step={0.1}
                    value={cfg.paperMinEdgeCents}
                    onChange={(e) =>
                      handleNumericConfigChange(
                        "paperMinEdgeCents",
                        parseFloat(e.target.value) || 0,
                        -10,
                        50
                      )
                    }
                    disabled={saving}
                    className="w-24 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
                  />
                </div>
              </div>
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Lookback (s)</p>
              <input
                type="number"
                min={20}
                max={1800}
                step={5}
                value={cfg.pairLookbackSeconds}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairLookbackSeconds",
                    parseInt(e.target.value, 10) || 600,
                    20,
                    1800
                  )
                }
                disabled={saving}
                className="w-24 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Max pairs</p>
              <input
                type="number"
                min={1}
                max={20}
                step={1}
                value={cfg.pairMaxMarketsPerRun}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "pairMaxMarketsPerRun",
                    parseInt(e.target.value, 10) || 4,
                    1,
                    20
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Re-entries/signal</p>
              <input
                type="number"
                min={1}
                max={6}
                step={1}
                value={cfg.reentryMaxEntriesPerSignal}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "reentryMaxEntriesPerSignal",
                    parseInt(e.target.value, 10) || 1,
                    1,
                    6
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Re-entry step (¢)</p>
              <input
                type="number"
                min={0.01}
                max={10}
                step={0.01}
                value={cfg.reentryEdgeStepCents}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "reentryEdgeStepCents",
                    parseFloat(e.target.value) || 0.1,
                    0.01,
                    10
                  )
                }
                disabled={saving}
                className="w-24 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div className="flex items-center gap-2 col-span-2">
              <button
                role="switch"
                aria-checked={cfg.floorToPolymarketMin !== false}
                onClick={() => updateConfig({ floorToPolymarketMin: !(cfg.floorToPolymarketMin !== false) })}
                disabled={saving}
                className={`
                  relative w-11 h-6 rounded-full transition-colors flex-shrink-0
                  ${cfg.floorToPolymarketMin !== false ? "bg-emerald-500" : "bg-zinc-700"}
                  ${saving ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}
                `}
              >
                <span
                  className={`
                    absolute top-1 w-4 h-4 rounded-full bg-white shadow transition-transform
                    ${cfg.floorToPolymarketMin !== false ? "left-6 translate-x-[-2px]" : "left-1"}
                  `}
                />
              </button>
              <div>
                <p className="text-xs text-zinc-500">Floor to $1</p>
              </div>
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Min bet ($)</p>
              <input
                type="number"
                min={0.1}
                step={0.1}
                value={cfg.minBetUsd}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "minBetUsd",
                    parseFloat(e.target.value) || 0.1,
                    0.1,
                    100
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Stop-loss ($)</p>
              <input
                type="number"
                min={0}
                step={1}
                value={cfg.stopLossBalance || ""}
                placeholder="0 = disabled"
                onChange={(e) =>
                  handleNumericConfigChange(
                    "stopLossBalance",
                    parseFloat(e.target.value) || 0,
                    0,
                    10000
                  )
                }
                disabled={saving}
                className="w-24 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60 placeholder:text-zinc-500"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Daily notional cap ($)</p>
              <input
                type="number"
                min={0}
                step={1}
                value={cfg.maxDailyLiveNotionalUsd || ""}
                placeholder="0 = disabled"
                onChange={(e) =>
                  handleNumericConfigChange(
                    "maxDailyLiveNotionalUsd",
                    parseFloat(e.target.value) || 0,
                    0,
                    1000000
                  )
                }
                disabled={saving}
                className="w-28 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60 placeholder:text-zinc-500"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Daily drawdown cap ($)</p>
              <input
                type="number"
                min={0}
                step={1}
                value={cfg.maxDailyDrawdownUsd || ""}
                placeholder="0 = disabled"
                onChange={(e) =>
                  handleNumericConfigChange(
                    "maxDailyDrawdownUsd",
                    parseFloat(e.target.value) || 0,
                    0,
                    1000000
                  )
                }
                disabled={saving}
                className="w-28 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60 placeholder:text-zinc-500"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Max imbalances/run</p>
              <input
                type="number"
                min={1}
                max={10}
                step={1}
                value={cfg.maxUnresolvedImbalancesPerRun}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "maxUnresolvedImbalancesPerRun",
                    parseInt(e.target.value, 10) || 1,
                    1,
                    10
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Unwind slippage (¢)</p>
              <input
                type="number"
                min={0}
                max={20}
                step={0.1}
                value={cfg.unwindSellSlippageCents}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "unwindSellSlippageCents",
                    parseFloat(e.target.value) || 0,
                    0,
                    20
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Unwind buffer %</p>
              <input
                type="number"
                min={50}
                max={100}
                step={1}
                value={cfg.unwindShareBufferPct}
                onChange={(e) =>
                  handleNumericConfigChange(
                    "unwindShareBufferPct",
                    parseInt(e.target.value, 10) || 99,
                    50,
                    100
                  )
                }
                disabled={saving}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm disabled:opacity-60"
              />
            </div>
            <div className="col-span-2 sm:col-span-3 lg:col-span-4">
              <p className="text-xs text-zinc-500 mb-1">Coins</p>
              <div className="flex items-center gap-2 rounded-lg bg-zinc-900/70 border border-zinc-800 p-1">
                <button
                  onClick={() => updateConfig({ enableBtc: !cfg.enableBtc }, true)}
                  disabled={saving}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors disabled:opacity-50 ${
                    cfg.enableBtc
                      ? "bg-emerald-500/25 text-emerald-300"
                      : "bg-zinc-800 text-zinc-500 hover:text-zinc-300"
                  }`}
                >
                  BTC
                </button>
                <button
                  onClick={() => updateConfig({ enableEth: !cfg.enableEth }, true)}
                  disabled={saving}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors disabled:opacity-50 ${
                    cfg.enableEth
                      ? "bg-emerald-500/25 text-emerald-300"
                      : "bg-zinc-800 text-zinc-500 hover:text-zinc-300"
                  }`}
                >
                  ETH
                </button>
              </div>
            </div>
            <div className="col-span-2 sm:col-span-3 lg:col-span-4">
              <p className="text-xs text-zinc-500 mb-1">Cadence filters</p>
              <div className="flex items-center gap-2 rounded-lg bg-zinc-900/70 border border-zinc-800 p-1">
                <button
                  onClick={() => updateConfig({ enableCadence5m: !cfg.enableCadence5m }, true)}
                  disabled={saving}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors disabled:opacity-50 ${
                    cfg.enableCadence5m
                      ? "bg-sky-500/25 text-sky-300"
                      : "bg-zinc-800 text-zinc-500 hover:text-zinc-300"
                  }`}
                >
                  5m
                </button>
                <button
                  onClick={() => updateConfig({ enableCadence15m: !cfg.enableCadence15m }, true)}
                  disabled={saving}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors disabled:opacity-50 ${
                    cfg.enableCadence15m
                      ? "bg-sky-500/25 text-sky-300"
                      : "bg-zinc-800 text-zinc-500 hover:text-zinc-300"
                  }`}
                >
                  15m
                </button>
                <button
                  onClick={() => updateConfig({ enableCadenceHourly: !cfg.enableCadenceHourly }, true)}
                  disabled={saving}
                  className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors disabled:opacity-50 ${
                    cfg.enableCadenceHourly
                      ? "bg-sky-500/25 text-sky-300"
                      : "bg-zinc-800 text-zinc-500 hover:text-zinc-300"
                  }`}
                >
                  Hourly
                </button>
              </div>
            </div>
          </div>
          {dailyRisk && (
            <div className="mt-4 rounded-lg bg-zinc-900/70 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase mb-1">Daily live risk window ({dailyRisk.dayKey})</p>
              <p className="text-xs text-zinc-300">
                Notional: ${dailyRisk.liveNotionalUsd.toFixed(2)}
                {cfg.maxDailyLiveNotionalUsd > 0 ? ` / $${cfg.maxDailyLiveNotionalUsd.toFixed(2)}` : " (cap off)"} ·
                Drawdown: ${dailyDrawdownUsd.toFixed(2)}
                {cfg.maxDailyDrawdownUsd > 0 ? ` / $${cfg.maxDailyDrawdownUsd.toFixed(2)}` : " (cap off)"} ·
                Live runs: {dailyRisk.liveRuns}
              </p>
            </div>
          )}
        </section>
        </>
        )}

        {/* Analytics tab */}
        {mainTab === "analytics" && (
        <>
        {/* Paper analytics */}
        <section className="mb-6 p-5 rounded-xl bg-zinc-900/40 border border-zinc-800/40">
          <div className="flex items-center justify-between gap-3 mb-4">
            <div>
              <h2 className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                Paper analytics
              </h2>
              <p className="text-xs text-zinc-600 mt-1">
                Tracks simulated runs to validate behavior before Live mode.
              </p>
            </div>
            <button
              onClick={resetPaperAnalytics}
              disabled={resettingPaperStats}
              className="px-3 py-1.5 rounded-lg bg-zinc-800 hover:bg-zinc-700 text-xs disabled:opacity-50"
            >
              {resettingPaperStats ? "Resetting…" : "Reset paper stats"}
            </button>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-4">
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Runs</p>
              <p className="text-lg font-semibold text-zinc-200">{paperStats.totalRuns}</p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Simulated pairs</p>
              <p className="text-lg font-semibold text-zinc-200">{paperStats.totalSimulatedTrades}</p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Sim volume</p>
              <p className="text-lg font-semibold text-zinc-200">${paperStats.totalSimulatedVolumeUsd.toFixed(2)}</p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Avg budget used</p>
              <p className="text-lg font-semibold text-zinc-200">{avgBudgetUsagePct.toFixed(1)}%</p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Avg executed edge</p>
              <p className="text-lg font-semibold text-zinc-200">
                {paperStats.totalSimulatedTrades > 0 ? `${avgExecutedEdgeCents.toFixed(2)}¢` : "—"}
              </p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Avg net edge</p>
              <p className="text-lg font-semibold text-zinc-200">
                {paperStats.totalSimulatedTrades > 0 ? `${avgExecutedNetEdgeCents.toFixed(2)}¢` : "—"}
              </p>
            </div>
          </div>
          <p className="text-xs text-zinc-500">
            Avg pairs/run: {avgTradesPerRun.toFixed(2)} · Failed runs: {paperStats.totalFailed}
            {paperStats.lastRunAt ? ` · Last paper run: ${new Date(paperStats.lastRunAt).toLocaleString()}` : ""}
          </p>
          {paperStats.lastError && (
            <p className="text-xs text-red-400 mt-1">{paperStats.lastError}</p>
          )}
        </section>

        {/* Session KPI targeting */}
        <section className="mb-6 p-5 rounded-xl bg-zinc-900/40 border border-zinc-800/40">
          <h2 className="text-xs font-medium uppercase tracking-wider text-zinc-500 mb-3">
            Session KPI targeting (last 60m)
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Executed pairs</p>
              <p className="text-lg font-semibold text-zinc-200">{sessionExecutedPairs}</p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Avg net edge</p>
              <p className="text-lg font-semibold text-zinc-200">
                {sessionExecutedPairs > 0 ? `${sessionAvgNetEdgeCents.toFixed(2)}¢` : "—"}
              </p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Pace target</p>
              <p className="text-lg font-semibold text-zinc-200">
                {sessionTargetPairsPerHour > 0 ? `${sessionTargetPairsPerHour.toFixed(0)} / h` : "off"}
              </p>
            </div>
            <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
              <p className="text-[11px] text-zinc-500 uppercase">Quality target</p>
              <p className="text-lg font-semibold text-zinc-200">
                {sessionMinAvgNetEdgeCents > 0 ? `${sessionMinAvgNetEdgeCents.toFixed(2)}¢+` : "off"}
              </p>
            </div>
          </div>
          <p className="text-xs text-zinc-500">
            Status:{" "}
            <span
              className={
                sessionKpiStatus === "On track"
                  ? "text-emerald-300"
                  : sessionKpiStatus === "Needs adjustment"
                    ? "text-amber-300"
                    : "text-zinc-300"
              }
            >
              {sessionKpiStatus}
            </span>
            {" · "}
            Pace {sessionPaceOnTrack ? "ok" : "below target"}
            {" · "}
            Quality {sessionQualityOnTrack ? "ok" : "below target"}
          </p>
        </section>

        {/* Strategy diagnostics */}
        <section className="mb-6 p-5 rounded-xl bg-zinc-900/40 border border-zinc-800/40">
          <h2 className="text-xs font-medium uppercase tracking-wider text-zinc-500 mb-3">
            Strategy diagnostics (last run)
          </h2>
          {lastDiag ? (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Evaluated</p>
                  <p className="text-lg font-semibold text-zinc-200">{lastDiag.evaluatedSignals}</p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Eligible</p>
                  <p className="text-lg font-semibold text-zinc-200">{lastDiag.eligibleSignals}</p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Executed/Paper</p>
                  <p className="text-lg font-semibold text-zinc-200">
                    {lastDiag.mode === "paper" ? lastDiag.paper : lastDiag.copied}
                  </p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Budget used</p>
                  <p className="text-lg font-semibold text-zinc-200">
                    ${lastDiag.budgetUsedUsd.toFixed(2)}
                  </p>
                </div>
              </div>
              <p className="text-xs text-zinc-500 mb-3">
                Mode: <span className="uppercase text-zinc-300">{lastDiag.mode}</span> · Rejections tracked: {rejectionTotal} ·
                {lastDiag.avgExecutedEdgeCents != null && (lastDiag.mode === "paper" ? lastDiag.paper : lastDiag.copied) > 0 && (
                  <> Avg executed edge: <span className="text-zinc-300">{lastDiag.avgExecutedEdgeCents.toFixed(2)}¢</span> ·</>
                )}
                {lastDiag.avgExecutedNetEdgeCents != null && (lastDiag.mode === "paper" ? lastDiag.paper : lastDiag.copied) > 0 && (
                  <> Avg net edge: <span className={lastDiag.avgExecutedNetEdgeCents < 0 ? "text-amber-400" : "text-zinc-300"}>{lastDiag.avgExecutedNetEdgeCents.toFixed(2)}¢</span> ·</>
                )}
                {lastDiag.maxEdgeCentsSeen != null && (
                  <> Best edge seen: <span className={lastDiag.maxEdgeCentsSeen < 0 ? "text-amber-400" : "text-zinc-300"}>{lastDiag.maxEdgeCentsSeen.toFixed(2)}¢</span> ·</>
                )}
                {lastDiag.maxNetEdgeCentsSeen != null && (
                  <> Best net edge: <span className={lastDiag.maxNetEdgeCentsSeen < 0 ? "text-amber-400" : "text-zinc-300"}>{lastDiag.maxNetEdgeCentsSeen.toFixed(2)}¢</span> ·</>
                )}
                {" "}Updated: {new Date(lastDiag.timestamp).toLocaleString()}
              </p>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-2 mb-3">
                <div className="rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-2">
                  <p className="text-[11px] text-zinc-500 uppercase mb-1">Evaluated mix</p>
                  <p className="text-xs text-zinc-300">
                    BTC {lastDiagEvaluatedBreakdown.byCoin.BTC} · ETH {lastDiagEvaluatedBreakdown.byCoin.ETH}
                  </p>
                  <p className="text-xs text-zinc-500">
                    5m {lastDiagEvaluatedBreakdown.byCadence["5m"]} · 15m {lastDiagEvaluatedBreakdown.byCadence["15m"]} ·
                    Hourly {lastDiagEvaluatedBreakdown.byCadence.hourly}
                  </p>
                </div>
                <div className="rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-2">
                  <p className="text-[11px] text-zinc-500 uppercase mb-1">Eligible mix</p>
                  <p className="text-xs text-zinc-300">
                    BTC {lastDiagEligibleBreakdown.byCoin.BTC} · ETH {lastDiagEligibleBreakdown.byCoin.ETH}
                  </p>
                  <p className="text-xs text-zinc-500">
                    5m {lastDiagEligibleBreakdown.byCadence["5m"]} · 15m {lastDiagEligibleBreakdown.byCadence["15m"]} ·
                    Hourly {lastDiagEligibleBreakdown.byCadence.hourly}
                  </p>
                </div>
                <div className="rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-2">
                  <p className="text-[11px] text-zinc-500 uppercase mb-1">Executed/Paper mix</p>
                  <p className="text-xs text-zinc-300">
                    BTC {lastDiagExecutedBreakdown.byCoin.BTC} · ETH {lastDiagExecutedBreakdown.byCoin.ETH}
                  </p>
                  <p className="text-xs text-zinc-500">
                    5m {lastDiagExecutedBreakdown.byCadence["5m"]} · 15m {lastDiagExecutedBreakdown.byCadence["15m"]} ·
                    Hourly {lastDiagExecutedBreakdown.byCadence.hourly}
                  </p>
                </div>
              </div>
              {rejectedEntries.length > 0 ? (
                <div className="space-y-1">
                  {rejectedEntries.slice(0, 10).map(([reason, count]) => (
                    <div
                      key={reason}
                      className="flex items-center justify-between text-xs rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-1.5"
                    >
                      <span className="text-zinc-300">{reason}</span>
                      <span className="text-zinc-500">
                        {count} ({rejectionTotal > 0 ? ((count / rejectionTotal) * 100).toFixed(1) : "0.0"}%)
                      </span>
                    </div>
                  ))}
                  {rejectedEntries.some(([r]) => r === "no_recent_signals") && (
                    <p className="text-xs text-amber-400/90 mt-2 px-2">
                      Tip: No BTC/ETH Up-Down trades in the lookback window. Increase &quot;Lookback (s)&quot; to 600 in settings.
                    </p>
                  )}
                  {rejectedEntries.some(([r]) => r.startsWith("edge_below_threshold")) && (
                    <p className="text-xs text-amber-400/90 mt-2 px-2">
                      {lastDiag?.maxEdgeCentsSeen != null && lastDiag.maxEdgeCentsSeen < 0
                        ? `All signals have negative edge (best: ${lastDiag.maxEdgeCentsSeen.toFixed(2)}¢, pairSum ${(lastDiag.minPairSumSeen ?? 0).toFixed(4)}). No profitable opportunities in current market—prices sum above $1.`
                        : "Lower \"Min edge (¢)\" for 5m, 15m, and Hourly to match the base (e.g. 0.3) so more signals qualify."}
                    </p>
                  )}
                </div>
              ) : (
                <p className="text-xs text-zinc-600">No rejections recorded in last run.</p>
              )}
            </>
          ) : (
            <p className="text-xs text-zinc-600">No run diagnostics yet. Trigger Run now or wait for worker cycle.</p>
          )}
        </section>

        {/* Strategy diagnostics trend */}
        <section className="mb-6 p-5 rounded-xl bg-zinc-900/40 border border-zinc-800/40">
          <button
            type="button"
            onClick={() => setShowDiagnosticsTrend((v) => !v)}
            className="w-full flex flex-wrap items-center justify-between gap-3 mb-3 text-left"
          >
            <div>
              <h2 className="text-xs font-medium uppercase tracking-wider text-zinc-500">
                Strategy diagnostics trend
              </h2>
              <p className="text-xs text-zinc-600 mt-0.5">
                {showDiagnosticsTrend ? "Last-N run rollup for tuning" : `Last ${trendRuns} runs · Click to expand`}
              </p>
            </div>
            <span className="text-zinc-500 text-sm">{showDiagnosticsTrend ? "−" : "+"}</span>
          </button>
          {showDiagnosticsTrend && (
          <>
          <div className="flex flex-wrap items-center justify-end gap-3 mb-3">
            <label className="text-xs text-zinc-500 flex items-center gap-2">
              Last
              <select
                value={trendRuns}
                onChange={(e) => setTrendRuns(Math.max(1, parseInt(e.target.value, 10) || 20))}
                className="px-2 py-1 rounded-md bg-zinc-800 border border-zinc-700 text-zinc-300"
              >
                {[5, 10, 20, 50, 100].map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
              runs
            </label>
          </div>
          {trendCount > 0 ? (
            <>
              <div className="grid grid-cols-2 md:grid-cols-7 gap-3 mb-4">
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Runs used</p>
                  <p className="text-lg font-semibold text-zinc-200">{trendCount}</p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Avg evaluated</p>
                  <p className="text-lg font-semibold text-zinc-200">{trendAvgEvaluated.toFixed(1)}</p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Avg eligible</p>
                  <p className="text-lg font-semibold text-zinc-200">{trendAvgEligible.toFixed(1)}</p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Avg executed/paper</p>
                  <p className="text-lg font-semibold text-zinc-200">{trendAvgExecuted.toFixed(2)}</p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Avg budget used</p>
                  <p className="text-lg font-semibold text-zinc-200">{trendAvgBudgetUsagePct.toFixed(1)}%</p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Avg executed edge</p>
                  <p className="text-lg font-semibold text-zinc-200">
                    {trendExecutedPairs > 0 ? `${trendAvgExecutedEdgeCents.toFixed(2)}¢` : "—"}
                  </p>
                </div>
                <div className="rounded-lg bg-zinc-900/80 border border-zinc-800 p-3">
                  <p className="text-[11px] text-zinc-500 uppercase">Avg net edge</p>
                  <p className="text-lg font-semibold text-zinc-200">
                    {trendExecutedPairs > 0 ? `${trendAvgExecutedNetEdgeCents.toFixed(2)}¢` : "—"}
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
                <div className="rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-2">
                  <p className="text-[11px] text-zinc-500 uppercase mb-1">Evaluated mix (Phase 2)</p>
                  <p className="text-xs text-zinc-300">
                    BTC {trendEvaluatedBreakdown.byCoin.BTC} · ETH {trendEvaluatedBreakdown.byCoin.ETH}
                  </p>
                  <p className="text-xs text-zinc-500">
                    5m {trendEvaluatedBreakdown.byCadence["5m"]} · 15m {trendEvaluatedBreakdown.byCadence["15m"]} ·
                    Hourly {trendEvaluatedBreakdown.byCadence.hourly}
                  </p>
                </div>
                <div className="rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-2">
                  <p className="text-[11px] text-zinc-500 uppercase mb-1">Eligible mix (Phase 2)</p>
                  <p className="text-xs text-zinc-300">
                    BTC {trendEligibleBreakdown.byCoin.BTC} · ETH {trendEligibleBreakdown.byCoin.ETH}
                  </p>
                  <p className="text-xs text-zinc-500">
                    5m {trendEligibleBreakdown.byCadence["5m"]} · 15m {trendEligibleBreakdown.byCadence["15m"]} ·
                    Hourly {trendEligibleBreakdown.byCadence.hourly}
                  </p>
                </div>
                <div className="rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-2">
                  <p className="text-[11px] text-zinc-500 uppercase mb-1">Executed/Paper mix (Phase 2)</p>
                  <p className="text-xs text-zinc-300">
                    BTC {trendExecutedBreakdown.byCoin.BTC} · ETH {trendExecutedBreakdown.byCoin.ETH}
                  </p>
                  <p className="text-xs text-zinc-500">
                    5m {trendExecutedBreakdown.byCadence["5m"]} · 15m {trendExecutedBreakdown.byCadence["15m"]} ·
                    Hourly {trendExecutedBreakdown.byCadence.hourly}
                  </p>
                </div>
              </div>

              <div className="rounded-lg bg-zinc-900/70 border border-zinc-800 p-3 mb-4">
                <div className="flex flex-wrap items-center justify-between gap-3 mb-2">
                  <div>
                    <p className="text-[11px] text-zinc-500 uppercase">Auto-tune suggestions</p>
                    <p className="text-xs text-zinc-600">
                      Suggests cadence thresholds from the selected trend window.
                    </p>
                  </div>
                  <button
                    onClick={applySuggestedEdges}
                    disabled={saving || suggestedEdgeChangeCount === 0}
                    className="px-3 py-1.5 rounded-md bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-300 text-xs disabled:opacity-40"
                  >
                    {suggestedEdgeChangeCount > 0
                      ? `Apply suggested edges (${suggestedEdgeChangeCount})`
                      : "No changes suggested"}
                  </button>
                </div>
                <div className="space-y-1.5">
                  {cadenceAutoSuggestions.map((suggestion) => (
                    <div
                      key={suggestion.key}
                      className="rounded-md bg-zinc-900/80 border border-zinc-800 px-2 py-2"
                    >
                      <div className="flex items-center justify-between gap-2 text-xs">
                        <span className="text-zinc-300">
                          {suggestion.label}: {suggestion.current.toFixed(1)}¢ {"->"} {suggestion.suggested.toFixed(1)}¢{" "}
                          <span
                            className={
                              suggestion.delta > 0
                                ? "text-amber-300"
                                : suggestion.delta < 0
                                  ? "text-sky-300"
                                  : "text-zinc-500"
                            }
                          >
                            ({suggestion.delta > 0 ? "+" : ""}
                            {suggestion.delta.toFixed(1)}¢)
                          </span>
                        </span>
                        <span
                          className={
                            suggestion.confidence === "high"
                              ? "text-emerald-300"
                              : suggestion.confidence === "medium"
                                ? "text-sky-300"
                                : "text-zinc-500"
                          }
                        >
                          {suggestion.confidence} confidence
                        </span>
                      </div>
                      <p className="text-xs text-zinc-500 mt-1">
                        Eval {suggestion.evaluated} · Eligible {suggestion.eligible} · Executed{" "}
                        {suggestion.executed} · Edge rejects {suggestion.edgeRejects}
                      </p>
                      <p className="text-xs text-zinc-600 mt-0.5">{suggestion.rationale}</p>
                    </div>
                  ))}
                </div>
              </div>

              <p className="text-xs text-zinc-500 mb-3">
                Error/failure runs: {trendErrorRuns} of {trendCount}
                {diagnosticsHistory.lastRunAt
                  ? ` · Last trend update: ${new Date(diagnosticsHistory.lastRunAt).toLocaleString()}`
                  : ""}
              </p>
              {trendRejectedEntries.length > 0 ? (
                <div className="space-y-1 mb-3">
                  {trendRejectedEntries.slice(0, 8).map(([reason, count]) => (
                    <div
                      key={reason}
                      className="flex items-center justify-between text-xs rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-1.5"
                    >
                      <span className="text-zinc-300">{reason}</span>
                      <span className="text-zinc-500">
                        {count} ({trendRejectionTotal > 0 ? ((count / trendRejectionTotal) * 100).toFixed(1) : "0.0"}
                        %)
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-xs text-zinc-600 mb-3">No aggregated rejections for selected runs.</p>
              )}

              <div className="space-y-1">
                {trendSample.slice(0, 10).map((run, idx) => (
                  <div
                    key={`${run.timestamp}-${idx}`}
                    className="flex items-center justify-between text-xs rounded-md bg-zinc-900/70 border border-zinc-800 px-2 py-1.5"
                  >
                    <span className="text-zinc-400">{new Date(run.timestamp).toLocaleString()}</span>
                    <span className="text-zinc-500">
                      {run.mode.toUpperCase()} · Eval {run.evaluatedSignals} · Elig {run.eligibleSignals} · Exec{" "}
                      {run.mode === "paper" ? run.paper : run.copied}
                    </span>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <p className="text-xs text-zinc-600">
              No diagnostics history yet. Run the strategy a few cycles to populate trends.
            </p>
          )}
          </>
          )}
        </section>
        </>
        )}

        {/* Positions tab */}
        {mainTab === "positions" && (
        <>
        {/* Recent activity */}
        {activity.length > 0 && (
          <section className="mb-8">
            <h2 className="text-sm font-medium text-zinc-300 mb-3">
              {cfg.mode === "paper" ? "Recent (simulated)" : "Recent activity"}
            </h2>
            <div className="space-y-2">
              {activity.slice(0, 8).map((a, i) => (
                <div
                  key={i}
                  className="flex items-center gap-3 p-3 rounded-lg bg-zinc-900/60 border border-zinc-800/40"
                >
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-zinc-200 truncate">{a.title}</p>
                    <p className="text-xs text-zinc-500">
                      {a.side} {a.outcome} · ${a.amountUsd.toFixed(2)} @ {(a.price * 100).toFixed(0)}¢
                    </p>
                  </div>
                  <span className="text-xs text-zinc-500">
                    {new Date(a.timestamp).toLocaleTimeString()}
                  </span>
                </div>
              ))}
            </div>
          </section>
        )}

        {/* Positions */}
        <section className="space-y-6">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-medium text-zinc-300">Positions</h2>
            <div className="flex rounded-lg bg-zinc-800/60 p-0.5">
              <button
                onClick={() => { setPositionTab("active"); setActivePage(0); }}
                className={`px-3 py-1.5 rounded-md text-sm transition-colors ${
                  positionTab === "active" ? "bg-zinc-700 text-zinc-100" : "text-zinc-500 hover:text-zinc-300"
                }`}
              >
                Active
              </button>
              <button
                onClick={() => { setPositionTab("resolved"); setResolvedPage(0); }}
                className={`px-3 py-1.5 rounded-md text-sm transition-colors ${
                  positionTab === "resolved" ? "bg-zinc-700 text-zinc-100" : "text-zinc-500 hover:text-zinc-300"
                }`}
              >
                Resolved
              </button>
            </div>
          </div>

          {(() => {
            const displayed = positionTab === "active" ? activePositions : resolvedPositions;
            const page = positionTab === "active" ? activePage : resolvedPage;
            const totalPages = Math.ceil(displayed.length / PAGE_SIZE) || 1;
            const paginated = displayed.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

            if (displayed.length === 0) {
              return (
                <p className="text-zinc-500 text-sm py-8 text-center">
                  {positionTab === "active" ? "No active positions" : "No resolved positions"}
                </p>
              );
            }

            return (
              <>
                <div className="space-y-4">
                  {paginated.map((pos) => {
                    const marketUrl = `https://polymarket.com/event/${pos.eventSlug || pos.slug}`;
                    const canSell = !pos.redeemable && pos.curPrice > 0;
                    const pnlPositive = pos.cashPnl >= 0;
                    return (
                      <div
                        key={pos.asset}
                        className="p-4 rounded-xl bg-zinc-900/80 border border-zinc-800/60 hover:border-zinc-700/60 transition-colors"
                      >
                        <a
                          href={marketUrl}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="block group"
                        >
                          <div className="flex gap-3">
                            {pos.icon && (
                              <img
                                src={pos.icon}
                                alt=""
                                className="w-12 h-12 rounded-lg object-cover flex-shrink-0"
                              />
                            )}
                            <div className="flex-1 min-w-0">
                              <p className="text-sm font-medium text-zinc-100 group-hover:text-emerald-400 transition-colors line-clamp-2">
                                {pos.title}
                              </p>
                              <div className="flex items-center gap-3 mt-1.5 flex-wrap">
                                <span className="text-xs px-2 py-0.5 rounded bg-zinc-800 text-zinc-400">
                                  {pos.outcome}
                                </span>
                                <span className="text-xs text-zinc-500">
                                  ${pos.initialValue.toFixed(2)} → ${pos.currentValue.toFixed(2)}
                                </span>
                                <span
                                  className={`text-xs font-medium ${
                                    pnlPositive ? "text-emerald-400" : "text-red-400"
                                  }`}
                                >
                                  {pnlPositive ? "+" : ""}{pos.cashPnl.toFixed(2)} ({pos.percentPnl.toFixed(1)}%)
                                </span>
                              </div>
                            </div>
                          </div>
                        </a>
                        {canSell && (
                          <div className="mt-3 pt-3 border-t border-zinc-800/60">
                            <button
                              onClick={(e) => {
                                e.preventDefault();
                                cashout(pos);
                              }}
                              disabled={cashingOut === pos.asset}
                              className="w-full py-2 rounded-lg bg-amber-500/20 hover:bg-amber-500/30 text-amber-400 text-sm font-medium disabled:opacity-50 transition-colors"
                            >
                              {cashingOut === pos.asset ? "Selling…" : "Cash out"}
                            </button>
                          </div>
                        )}
                        {pos.redeemable && (
                          <p className="mt-2 text-xs text-zinc-500">Resolved · Redeem on Polymarket</p>
                        )}
                      </div>
                    );
                  })}
                </div>

                {totalPages > 1 && (
                  <div className="flex items-center justify-between mt-4">
                    <button
                      onClick={() =>
                        positionTab === "active"
                          ? setActivePage((p) => Math.max(0, p - 1))
                          : setResolvedPage((p) => Math.max(0, p - 1))
                      }
                      disabled={page === 0}
                      className="px-3 py-1.5 rounded-lg bg-zinc-800 text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-zinc-700"
                    >
                      Previous
                    </button>
                    <span className="text-sm text-zinc-500">
                      Page {page + 1} of {totalPages}
                    </span>
                    <button
                      onClick={() =>
                        positionTab === "active"
                          ? setActivePage((p) => Math.min(totalPages - 1, p + 1))
                          : setResolvedPage((p) => Math.min(totalPages - 1, p + 1))
                      }
                      disabled={page >= totalPages - 1}
                      className="px-3 py-1.5 rounded-lg bg-zinc-800 text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-zinc-700"
                    >
                      Next
                    </button>
                  </div>
                )}
              </>
            );
          })()}
        </section>
        </>
        )}

        {/* Status footer */}
        <footer className="mt-8 pt-6 border-t border-zinc-800/60 text-xs text-zinc-500 space-y-0.5">
          <p>
          Last run {status?.state.lastRunAt ? new Date(status.state.lastRunAt).toLocaleString() : "—"} ·{" "}
          Last execution: {status?.state.lastCopiedAt ? new Date(status.state.lastCopiedAt).toLocaleString() : "—"}
          {" · "}
          Last claim: {status?.state.lastClaimAt ? new Date(status.state.lastClaimAt).toLocaleString() : "—"}
          {status?.state.lastClaimResult && (
            <span> ({status.state.lastClaimResult.claimed} claimed)</span>
          )}
          </p>
          {status?.state.runsSinceLastClaim != null && (
            <span className="block mt-0.5 text-zinc-600">Claim runs every 10 strategy runs ({status.state.runsSinceLastClaim}/10)</span>
          )}
          {status?.state.lastError && (
            <span className="block mt-1 text-red-400">{status.state.lastError}</span>
          )}
          <a
            href={typeof window !== "undefined" ? `${window.location.origin}/api/debug` : "/api/debug"}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block mt-2 text-zinc-500 hover:text-zinc-300"
          >
            Diagnostics & debug →
          </a>
        </footer>

      </div>
    </main>
  );
}
