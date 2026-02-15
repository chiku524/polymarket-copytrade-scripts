import { kv } from "@vercel/kv";

const CONFIG_KEY = "copy_trader_config";
const STATE_KEY = "copy_trader_state";
const ACTIVITY_KEY = "copy_trader_activity";

export interface CopyTraderConfig {
  enabled: boolean;
  /** Copy at this % of target's bet (default 5) */
  copyPercent: number;
  /** Max USDC per bet (default 3) */
  maxBetUsd: number;
  /** Min bet to place - skip if below (default 0.10) */
  minBetUsd: number;
}

export interface CopyTraderState {
  lastTimestamp: number;
  copiedKeys: string[];
  lastRunAt?: number;
  lastCopiedAt?: number;
  lastError?: string;
}

export interface RecentActivity {
  title: string;
  outcome: string;
  side: string;
  amountUsd: number;
  price: number;
  timestamp: number;
}

const DEFAULT_CONFIG: CopyTraderConfig = {
  enabled: false,
  copyPercent: 5,
  maxBetUsd: 3,
  minBetUsd: 0.1,
};

export async function getConfig(): Promise<CopyTraderConfig> {
  const c = await kv.get<Record<string, unknown>>(CONFIG_KEY);
  if (!c) return { ...DEFAULT_CONFIG };
  const migrated: CopyTraderConfig = {
    enabled: Boolean(c.enabled ?? DEFAULT_CONFIG.enabled),
    copyPercent: Number(c.copyPercent ?? c.minPercent ?? DEFAULT_CONFIG.copyPercent),
    maxBetUsd: Number(c.maxBetUsd ?? c.minBetUsd ?? DEFAULT_CONFIG.maxBetUsd),
    minBetUsd: Number(c.minBetUsd ?? DEFAULT_CONFIG.minBetUsd),
  };
  return migrated;
}

export async function setConfig(config: Partial<CopyTraderConfig>): Promise<CopyTraderConfig> {
  const current = await getConfig();
  const updated = { ...current, ...config };
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
      }
    : { lastTimestamp: 0, copiedKeys: [] };
}

export async function setState(state: Partial<CopyTraderState>): Promise<void> {
  const current = await getState();
  const updated = { ...current, ...state };
  await kv.set(STATE_KEY, updated);
}

export async function resetSyncState(): Promise<void> {
  await kv.set(STATE_KEY, { lastTimestamp: 0, copiedKeys: [] });
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
