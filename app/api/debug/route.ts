import { NextResponse } from "next/server";
import { getConfig, getState } from "@/lib/kv";
import { getCashBalance } from "@/lib/copy-trade";
import { getTargetActivity } from "@/lib/copy-trade";

const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";
const TARGET_ADDRESS = process.env.TARGET_ADDRESS ?? "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d";
const CRON_SECRET = process.env.CRON_SECRET;

export async function GET() {
  try {
    const [config, state, cashBalance, targetActivity, geoblock] = await Promise.all([
      getConfig(),
      getState(),
      getCashBalance(MY_ADDRESS).catch(() => 0),
      getTargetActivity(TARGET_ADDRESS, 5).catch(() => []),
      fetch("https://polymarket.com/api/geoblock").then((r) => r.json()).catch(() => ({ blocked: null, ip: null, country: null, region: null })),
    ]);

    const nowSec = Math.floor(Date.now() / 1000);
    const latestTrade = targetActivity[0];
    const latestTs = latestTrade?.timestamp ?? 0;
    const tradeAgeSec = latestTs > 0 ? nowSec - latestTs : null;

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
        willCopyNewTrades: config.enabled && latestTs > state.lastTimestamp,
        reason: !config.enabled
          ? "Copy trading is disabled"
          : latestTs <= state.lastTimestamp
            ? `Latest trade (${latestTs}) is older than lastTimestamp (${state.lastTimestamp}) - already synced`
            : "Should copy on next run",
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
