import { NextResponse } from "next/server";
import { getConfig, getState } from "@/lib/kv";
import { getCashBalance, getTargetActivity } from "@/lib/copy-trade";

const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";
const TARGET_ADDRESS = process.env.TARGET_ADDRESS ?? "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d";
const CRON_SECRET = process.env.CRON_SECRET;
const POLYMARKET_MIN_USD = 1;

export async function GET() {
  try {
    const [config, state, cashBalance, targetActivity, geoblock] = await Promise.all([
      getConfig(),
      getState(),
      getCashBalance(MY_ADDRESS).catch(() => 0),
      getTargetActivity(TARGET_ADDRESS, 15).catch(() => []),
      fetch("https://polymarket.com/api/geoblock").then((r) => r.json()).catch(() => ({ blocked: null, ip: null, country: null, region: null })),
    ]);

    const nowSec = Math.floor(Date.now() / 1000);
    const latestTrade = targetActivity[0];
    const latestTs = latestTrade?.timestamp ?? 0;
    const tradeAgeSec = latestTs > 0 ? nowSec - latestTs : null;
    const copiedSet = new Set(state.copiedKeys ?? []);
    const lastTs = state.lastTimestamp ?? 0;
    const isFirstRun = lastTs === 0 && copiedSet.size === 0;
    const fiveMinAgo = nowSec - 300;
    const walletRunCapUsd = (cashBalance * config.walletUsagePercent) / 100;
    let budgetRemainingUsd = walletRunCapUsd;

    // Per-trade copy analysis (dry run)
    const tradeAnalysis: Array<{
      title: string;
      ts: number;
      targetBetUsd: number;
      ourBetUsd: number;
      wouldCopy: boolean;
      reason: string;
    }> = [];

    const floorToPolymarketMin = config.floorToPolymarketMin !== false;
    for (const act of targetActivity) {
      if (act.type !== "TRADE") continue;
      const ts = act.timestamp;
      const txHash = act.transactionHash ?? "";
      const asset = act.asset ?? "";
      const sideStr = (act.side ?? "BUY").toUpperCase();
      const price = act.price;
      const targetBetUsd = act.usdcSize ?? (act.size ?? 0) * price;
      const rawAmount = Math.min(
        (targetBetUsd * config.copyPercent) / 100,
        config.maxBetUsd,
        Math.max(0, budgetRemainingUsd)
      );
      let ourBetUsd = rawAmount >= config.minBetUsd ? rawAmount : 0;
      if (ourBetUsd === 0 && floorToPolymarketMin && rawAmount > 0 && rawAmount < POLYMARKET_MIN_USD) {
        ourBetUsd = POLYMARKET_MIN_USD;
      } else if (ourBetUsd > 0 && ourBetUsd < POLYMARKET_MIN_USD && floorToPolymarketMin) {
        ourBetUsd = POLYMARKET_MIN_USD;
      }
      const key = `${txHash}|${asset}|${sideStr}`;

      let wouldCopy = false;
      let reason = "";

      if (ts <= lastTs) {
        reason = "already synced (ts <= lastTimestamp)";
      } else if (isFirstRun && ts < fiveMinAgo) {
        reason = "first run: skipped (older than 5 min)";
      } else if (copiedSet.has(key)) {
        reason = "already copied";
      } else if (!asset || price <= 0) {
        reason = "invalid asset/price";
      } else if (rawAmount < config.minBetUsd && !(floorToPolymarketMin && rawAmount > 0 && rawAmount < POLYMARKET_MIN_USD)) {
        reason = `bet $${rawAmount.toFixed(2)} < minBetUsd $${config.minBetUsd}`;
      } else if (ourBetUsd < POLYMARKET_MIN_USD && !floorToPolymarketMin) {
        reason = `bet $${rawAmount.toFixed(2)} < Polymarket min $1`;
      } else {
        wouldCopy = true;
        reason = ourBetUsd > rawAmount ? `would copy (floor to $1, raw $${rawAmount.toFixed(2)})` : "would copy";
        budgetRemainingUsd = Math.max(0, budgetRemainingUsd - ourBetUsd);
      }

      tradeAnalysis.push({
        title: (act.title ?? "Unknown").slice(0, 50),
        ts,
        targetBetUsd,
        ourBetUsd,
        wouldCopy,
        reason,
      });
    }

    const wouldCopyCount = tradeAnalysis.filter((t) => t.wouldCopy).length;
    const targetBetSizes = tradeAnalysis.map((t) => t.targetBetUsd).filter((v) => v > 0);
    const minTargetForUsd1 = config.copyPercent > 0 ? (100 / config.copyPercent) * POLYMARKET_MIN_USD : Infinity;

    const lastRunAgeSec = state.lastRunAt ? Math.floor((Date.now() - state.lastRunAt) / 1000) : null;
    const cronHint =
      lastRunAgeSec != null && lastRunAgeSec > 120
        ? `Cron may not be hitting: last run ${lastRunAgeSec}s ago. Check cron-job.org URL (must be Railway URL) and Authorization: Bearer CRON_SECRET.`
        : null;

    const copyPercentHint =
      wouldCopyCount === 0 &&
      targetBetSizes.length > 0 &&
      config.copyPercent <= 10
        ? `With ${config.copyPercent}% copy, target must bet ≥$${minTargetForUsd1.toFixed(0)} for a $1 order. Recent target bets: $${targetBetSizes.slice(0, 5).map((v) => v.toFixed(1)).join(", ")}. Consider 10-15% copy % to copy smaller bets.`
        : null;

    return NextResponse.json({
      config: { ...config, enabled: config.enabled },
      state: {
        lastTimestamp: state.lastTimestamp,
        copiedKeysCount: state.copiedKeys?.length ?? 0,
        lastRunAt: state.lastRunAt,
        lastCopiedAt: state.lastCopiedAt,
        lastError: state.lastError,
      },
      cashBalance,
      cronSecretSet: !!CRON_SECRET,
      target: {
        latestTradeTitle: latestTrade?.title,
        latestTradeTimestamp: latestTs,
        latestTradeAgeSec: tradeAgeSec,
        activityCount: targetActivity.length,
      },
      diagnosis: {
        willCopyNewTrades: config.mode !== "off" && latestTs > lastTs,
        reason: config.mode === "off"
          ? "Trading mode is off"
          : latestTs <= lastTs
            ? `Latest trade (${latestTs}) is older than lastTimestamp (${lastTs}) - already synced`
            : wouldCopyCount > 0
              ? `${wouldCopyCount} trade(s) would copy on next run`
              : "No new trades pass filters (check tradeAnalysis)",
      },
      tradeAnalysis,
      hints: [cronHint, copyPercentHint].filter(Boolean),
      walletBudget: {
        walletUsagePercent: config.walletUsagePercent,
        runCapUsd: walletRunCapUsd,
        remainingUsd: budgetRemainingUsd,
      },
      geoblock: {
        blocked: geoblock.blocked,
        country: geoblock.country,
        region: geoblock.region,
        ip: geoblock.ip,
        note: geoblock.blocked ? `Server IP is in restricted region (${geoblock.country}). Use Railway EU West (Amsterdam) - ensure cron & app use Railway URL, not Vercel.` : null,
      },
    });
  } catch (e) {
    console.error("Debug error:", e);
    return NextResponse.json(
      { error: e instanceof Error ? e.message : String(e) },
      { status: 500 }
    );
  }
}
