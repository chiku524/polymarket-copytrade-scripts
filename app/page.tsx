"use client";

import { useEffect, useState } from "react";

const PAGE_SIZE = 10;

interface Config {
  enabled: boolean;
  copyPercent: number;
  maxBetUsd: number;
  minBetUsd: number;
}

interface Status {
  config: Config;
  state: {
    lastTimestamp: number;
    lastRunAt?: number;
    lastCopiedAt?: number;
    lastError?: string;
  };
  cashBalance: number;
  recentActivity: { title: string; outcome: string; side: string; amountUsd: number; price: number; timestamp: number }[];
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
  const [error, setError] = useState<string | null>(null);
  const [positionTab, setPositionTab] = useState<PositionTab>("active");
  const [activePage, setActivePage] = useState(0);
  const [resolvedPage, setResolvedPage] = useState(0);

  const fetchAll = async () => {
    try {
      const [statusRes, positionsRes] = await Promise.all([
        fetch("/api/status"),
        fetch("/api/positions"),
      ]);
      if (!statusRes.ok) throw new Error("Failed to load status");
      if (!positionsRes.ok) throw new Error("Failed to load positions");
      const statusData = await statusRes.json();
      const positionsData = await positionsRes.json();
      setStatus(statusData);
      setActivePositions(positionsData.active ?? []);
      setResolvedPositions(positionsData.resolved ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAll();
    const id = setInterval(fetchAll, 10000);
    return () => clearInterval(id);
  }, []);

  const updateConfig = async (updates: Partial<Config>) => {
    if (!status) return;
    setSaving(true);
    try {
      const res = await fetch("/api/config", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(updates),
      });
      if (!res.ok) throw new Error("Failed to save");
      const data = await res.json();
      setStatus((s) => (s ? { ...s, config: data } : null));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save");
    } finally {
      setSaving(false);
    }
  };

  const runNow = async () => {
    setRunning(true);
    setRunResult(null);
    setError(null);
    try {
      const res = await fetch("/api/run-now", { method: "POST" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? "Failed");
      await fetchAll();
      if (data.skipped) {
        setRunResult("Skipped (copy trading is disabled)");
      } else if (data.copied > 0) {
        setRunResult(`Copied ${data.copied} trade${data.copied === 1 ? "" : "s"}`);
      } else {
        setRunResult("No new trades to copy");
      }
      setTimeout(() => setRunResult(null), 5000);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Run failed");
    } finally {
      setRunning(false);
    }
  };

  const resetSync = async () => {
    setResetting(true);
    setError(null);
    try {
      const res = await fetch("/api/reset-sync", { method: "POST" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? "Reset failed");
      await fetchAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Reset failed");
    } finally {
      setResetting(false);
    }
  };

  const cashout = async (pos: Position) => {
    setCashingOut(pos.asset);
    try {
      const res = await fetch("/api/cashout", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          asset: pos.asset,
          size: pos.size,
          price: pos.curPrice > 0 ? pos.curPrice : 0.5,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? "Cashout failed");
      await fetchAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Cashout failed");
    } finally {
      setCashingOut(null);
    }
  };

  const toggleEnabled = () => updateConfig({ enabled: !status?.config.enabled });

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
          <button onClick={fetchAll} className="px-4 py-2 rounded-lg bg-zinc-800 hover:bg-zinc-700 text-sm">
            Retry
          </button>
        </div>
      </main>
    );
  }

  const cfg = status?.config ?? { enabled: false, copyPercent: 5, maxBetUsd: 3, minBetUsd: 0.1 };
  const activity = status?.recentActivity ?? [];

  return (
    <main className="min-h-screen bg-zinc-950 text-zinc-100">
      <div className="max-w-2xl mx-auto p-6 md:p-8">
        {/* Header */}
        <header className="mb-8">
          <h1 className="text-2xl font-bold tracking-tight">Polymarket Copy Trader</h1>
          <p className="mt-1 text-zinc-500">
            Copying <span className="text-emerald-400">gabagool22</span> → your account
          </p>
        </header>

        {/* Note: no need to keep UI open */}
        <p className="mb-4 text-xs text-zinc-500">
          You don&apos;t need to keep this page open. When the toggle is on, set up a cron (e.g. cron-job.org) to GET your-app-url/api/copy-trade every minute with <code className="bg-zinc-800 px-1 rounded">Authorization: Bearer YOUR_CRON_SECRET</code>.
        </p>

        {/* Balance + Control bar */}
        <div className="flex flex-wrap items-center justify-between gap-4 mb-8 p-4 rounded-xl bg-zinc-900/80 border border-zinc-800/60">
          <div>
            <p className="text-xs text-zinc-500 uppercase tracking-wider">Cash balance</p>
            <p className="text-2xl font-semibold text-emerald-400">
              ${(status?.cashBalance ?? 0).toFixed(2)}
            </p>
          </div>
          <div className="flex items-center gap-4">
            <button
              role="switch"
              aria-checked={cfg.enabled}
              onClick={toggleEnabled}
              disabled={saving}
              className={`
                relative w-14 h-8 rounded-full transition-colors flex-shrink-0
                ${cfg.enabled ? "bg-emerald-500" : "bg-zinc-700"}
                ${saving ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}
              `}
            >
              <span
                className={`
                  absolute top-1 w-6 h-6 rounded-full bg-white shadow transition-transform
                  ${cfg.enabled ? "left-7 translate-x-[-2px]" : "left-1"}
                `}
              />
            </button>
            <div className="flex flex-col items-end gap-1">
              <div className="flex gap-2">
                <button
                  onClick={runNow}
                  disabled={running}
                  className="px-4 py-2 rounded-lg bg-zinc-700 hover:bg-zinc-600 text-sm font-medium disabled:opacity-50 transition-colors"
                >
                  {running ? "Running…" : "Run now"}
                </button>
                <button
                  onClick={resetSync}
                  disabled={resetting}
                  className="px-3 py-2 rounded-lg bg-amber-500/20 hover:bg-amber-500/30 text-amber-400 text-sm disabled:opacity-50 transition-colors"
                >
                  {resetting ? "Resetting…" : "Reset sync"}
                </button>
              </div>
              {runResult && (
                <span className="text-xs text-emerald-400/90">{runResult}</span>
              )}
            </div>
          </div>
        </div>

        {/* Settings */}
        <section className="mb-8 p-4 rounded-xl bg-zinc-900/50 border border-zinc-800/60">
          <h2 className="text-xs font-medium uppercase tracking-wider text-zinc-500 mb-4">Bet size</h2>
          <p className="text-sm text-zinc-400 mb-4">
            Copying at <strong>{cfg.copyPercent}%</strong> of gabagool22&apos;s bet, max <strong>${cfg.maxBetUsd}</strong> per trade
          </p>
          <div className="flex flex-wrap gap-6">
            <div>
              <p className="text-xs text-zinc-500 mb-1">Copy %</p>
              <input
                type="number"
                min={1}
                max={100}
                step={1}
                value={cfg.copyPercent}
                onChange={(e) => updateConfig({ copyPercent: Math.max(1, Math.min(100, parseInt(e.target.value, 10) || 5)) })}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Max bet (USDC)</p>
              <input
                type="number"
                min={0.5}
                max={100}
                step={0.5}
                value={cfg.maxBetUsd}
                onChange={(e) => updateConfig({ maxBetUsd: Math.max(0.5, parseFloat(e.target.value) || 3) })}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm"
              />
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Min bet (USDC)</p>
              <input
                type="number"
                min={0.1}
                step={0.1}
                value={cfg.minBetUsd}
                onChange={(e) => updateConfig({ minBetUsd: Math.max(0.1, parseFloat(e.target.value) || 0.1) })}
                className="w-20 px-2 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-sm"
              />
            </div>
          </div>
        </section>

        {/* Recent activity */}
        {activity.length > 0 && (
          <section className="mb-8">
            <h2 className="text-sm font-medium text-zinc-400 mb-3">Recently copied</h2>
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
        <section>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-medium text-zinc-400">Your positions</h2>
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

        {/* Status footer */}
        <footer className="mt-8 pt-6 border-t border-zinc-800/60 text-xs text-zinc-500">
          Last run: {status?.state.lastRunAt ? new Date(status.state.lastRunAt).toLocaleString() : "—"} ·{" "}
          Last copied: {status?.state.lastCopiedAt ? new Date(status.state.lastCopiedAt).toLocaleString() : "—"}
          {status?.state.lastError && (
            <span className="block mt-1 text-red-400">{status.state.lastError}</span>
          )}
          <a href="/api/debug" target="_blank" rel="noopener noreferrer" className="block mt-2 text-zinc-500 hover:text-zinc-400">
            Debug
          </a>
        </footer>

        {error && <p className="mt-4 text-sm text-red-400">{error}</p>}
      </div>
    </main>
  );
}
