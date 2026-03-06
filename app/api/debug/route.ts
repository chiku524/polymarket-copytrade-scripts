import { NextResponse } from "next/server";
import { getConfig, getState } from "@/lib/kv";
import { getCashBalance } from "@/lib/copy-trade";

const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";
const CRON_SECRET = process.env.CRON_SECRET;

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(request: Request) {
  try {
    const [configResult, stateResult, cashBalance, geoblock] = await Promise.all([
      getConfig()
        .then((value) => ({ value, error: null as string | null }))
        .catch((e) => ({
          value: null,
          error: e instanceof Error ? e.message : String(e),
        })),
      getState()
        .then((value) => ({ value, error: null as string | null }))
        .catch((e) => ({
          value: null,
          error: e instanceof Error ? e.message : String(e),
        })),
      getCashBalance(MY_ADDRESS).catch(() => 0),
      fetch("https://polymarket.com/api/geoblock", { cache: "no-store" })
        .then((r) => r.json())
        .catch(() => ({ blocked: null, ip: null, country: null, region: null })),
    ]);
    const config = configResult.value;
    const state = stateResult.value;

    const walletUsagePercent = config?.walletUsagePercent ?? 0;
    const walletRunCapUsd = (cashBalance * walletUsagePercent) / 100;
    const maxRunBudgetUsd = Math.max(0, Number(config?.maxRunBudgetUsd ?? 0));
    const effectiveRunCapUsd =
      maxRunBudgetUsd > 0 ? Math.min(walletRunCapUsd, maxRunBudgetUsd) : walletRunCapUsd;
    const lastRunAgeSec = state?.lastRunAt
      ? Math.floor((Date.now() - state.lastRunAt) / 1000)
      : null;
    const cronHint =
      lastRunAgeSec != null && lastRunAgeSec > 120
        ? `Worker/cron may not be hitting: last run ${lastRunAgeSec}s ago. Check worker APP_BASE_URL (your web app URL, e.g. https://polymarket-trader.fly.dev) and Authorization: Bearer CRON_SECRET.`
        : null;

    const diag = state?.lastStrategyDiagnostics;

    return NextResponse.json(
      {
        config: config ? { ...config, enabled: config.enabled } : null,
        state: state
          ? {
              lastTimestamp: state.lastTimestamp,
              copiedKeysCount: state.copiedKeys?.length ?? 0,
              lastRunAt: state.lastRunAt,
              lastCopiedAt: state.lastCopiedAt,
              lastError: state.lastError,
            }
          : null,
        kvErrors: {
          config: configResult.error,
          state: stateResult.error,
        },
        lastRunDiagnostics: diag
          ? {
              evaluatedSignals: diag.evaluatedSignals,
              eligibleSignals: diag.eligibleSignals ?? (diag.copied ?? 0) + (diag.paper ?? 0),
              rejectedReasons: diag.rejectedReasons ?? {},
              avgExecutedEdgeCents: diag.avgExecutedEdgeCents,
              avgExecutedNetEdgeCents: diag.avgExecutedNetEdgeCents,
              maxNetEdgeCentsSeen: diag.maxNetEdgeCentsSeen,
            }
          : null,
        cashBalance,
        cronSecretSet: !!CRON_SECRET,
        diagnosis: {
          status: config
            ? config.mode === "off"
              ? "Trading mode is off"
              : "Paired strategy runs on worker/cron cycles"
            : "Config unavailable (see kvErrors)",
        },
        hints: [cronHint].filter(Boolean),
        walletBudget: {
          walletUsagePercent,
          runCapUsd: walletRunCapUsd,
          maxRunBudgetUsd,
          effectiveRunCapUsd,
        },
        geoblock: {
          blocked: geoblock.blocked,
          country: geoblock.country,
          region: geoblock.region,
          ip: geoblock.ip,
          note: geoblock.blocked
            ? `Server IP is in restricted region (${geoblock.country}). Use a server in an allowed region (e.g. Fly.io Mumbai/bom).`
            : null,
        },
        _requestHost: new URL(request.url).host,
        _flyRegion: process.env.FLY_REGION ?? null,
        _debugHint:
          geoblock.blocked && geoblock.country === "US"
            ? "If _requestHost is localhost, Polymarket sees YOUR IP. Open https://polymarket-trader.fly.dev (Fly URL) and use Diagnostics there instead."
            : undefined,
      },
      { headers: { "Cache-Control": "no-store, no-cache, max-age=0" } }
    );
  } catch (e) {
    console.error("Debug error:", e);
    return NextResponse.json(
      { error: e instanceof Error ? e.message : String(e) },
      { status: 500 }
    );
  }
}
