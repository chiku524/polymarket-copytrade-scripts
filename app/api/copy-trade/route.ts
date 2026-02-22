import { NextRequest, NextResponse } from "next/server";
import {
  getConfig,
  getState,
  setState,
  appendActivity,
  acquireRunLock,
  releaseRunLock,
  recordPaperRun,
} from "@/lib/kv";
import { runPairedStrategy } from "@/lib/paired-strategy";
import { claimWinnings } from "@/lib/claim";

const PRIVATE_KEY = process.env.PRIVATE_KEY;
const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";
const SIGNATURE_TYPE = parseInt(process.env.SIGNATURE_TYPE ?? "1", 10);
const CRON_SECRET = process.env.CRON_SECRET;
const CLAIM_EVERY_N_RUNS = Math.max(1, parseInt(process.env.CLAIM_EVERY_N_RUNS ?? "10", 10));

export const maxDuration = 90;

async function runCopyTradeHandler() {
  const lockToken = await acquireRunLock(120);
  if (!lockToken) {
    return NextResponse.json({ ok: true, skipped: true, reason: "busy" });
  }

  try {
    const config = await getConfig();
    const livePrivateKey = PRIVATE_KEY ?? "";
    if (config.mode === "off" || !config.enabled) {
      await setState({ lastRunAt: Date.now(), lastError: undefined });
      return NextResponse.json({ ok: true, skipped: true, reason: "mode_off" });
    }
    if (config.mode === "live" && !livePrivateKey) {
      return NextResponse.json({ error: "PRIVATE_KEY not configured for Live mode" }, { status: 500 });
    }

    const state = await getState();
    const result = await runPairedStrategy(
      livePrivateKey,
      MY_ADDRESS,
      SIGNATURE_TYPE,
      {
        mode: config.mode,
        walletUsagePercent: config.walletUsagePercent,
        pairChunkUsd: config.pairChunkUsd,
        minBetUsd: config.minBetUsd,
        stopLossBalance: config.stopLossBalance ?? 0,
        floorToPolymarketMin: config.floorToPolymarketMin !== false,
        pairMinEdgeCents: config.pairMinEdgeCents,
        pairLookbackSeconds: config.pairLookbackSeconds,
        pairMaxMarketsPerRun: config.pairMaxMarketsPerRun,
      },
      { lastTimestamp: state.lastTimestamp, copiedKeys: state.copiedKeys }
    );

    const isLive = config.mode === "live";
    const runsSinceLastClaim = isLive ? (state.runsSinceLastClaim ?? 0) + 1 : state.runsSinceLastClaim ?? 0;
    const shouldClaim = isLive && runsSinceLastClaim >= CLAIM_EVERY_N_RUNS;

    const stateUpdate: Parameters<typeof setState>[0] = {
      lastTimestamp: result.lastTimestamp ?? state.lastTimestamp,
      copiedKeys: result.copiedKeys.length > 0 ? result.copiedKeys : state.copiedKeys,
      lastRunAt: Date.now(),
      lastCopiedAt: result.copied > 0 ? Date.now() : state.lastCopiedAt,
      lastError: result.error,
      lastStrategyDiagnostics: {
        mode: result.mode,
        evaluatedSignals: result.evaluatedSignals,
        eligibleSignals: result.eligibleSignals,
        rejectedReasons: result.rejectedReasons,
        copied: result.copied,
        paper: result.paper,
        failed: result.failed,
        budgetCapUsd: result.budgetCapUsd,
        budgetUsedUsd: result.budgetUsedUsd,
        timestamp: Date.now(),
      },
      runsSinceLastClaim: shouldClaim ? 0 : runsSinceLastClaim,
    };

    let claimResult: { claimed: number; failed: number } | undefined;
    if (shouldClaim) {
      try {
        const res = await claimWinnings(livePrivateKey, MY_ADDRESS);
        stateUpdate.lastClaimAt = Date.now();
        stateUpdate.lastClaimResult = { claimed: res.claimed, failed: res.failed };
        claimResult = stateUpdate.lastClaimResult;
      } catch (claimErr) {
        console.error("Claim after run failed:", claimErr);
        stateUpdate.runsSinceLastClaim = 0;
      }
    }

    await setState(stateUpdate);
    if (result.copiedTrades?.length) {
      await appendActivity(result.copiedTrades);
    }
    if (result.mode === "paper") {
      await recordPaperRun({
        timestamp: Date.now(),
        simulatedTrades: result.paper,
        simulatedVolumeUsd: result.simulatedVolumeUsd,
        failed: result.failed,
        budgetCapUsd: result.budgetCapUsd,
        budgetUsedUsd: result.budgetUsedUsd,
        error: result.error,
      });
    }

    return NextResponse.json({
      ok: true,
      mode: result.mode,
      copied: result.copied,
      paper: result.paper,
      simulatedVolumeUsd: result.simulatedVolumeUsd,
      failed: result.failed,
      evaluatedSignals: result.evaluatedSignals,
      eligibleSignals: result.eligibleSignals,
      rejectedReasons: result.rejectedReasons,
      budgetCapUsd: result.budgetCapUsd,
      budgetUsedUsd: result.budgetUsedUsd,
      error: result.error,
      claimed: claimResult?.claimed,
    });
  } catch (e) {
    const err = e instanceof Error ? e.message : String(e);
    console.error("Copy trade error:", e);
    return NextResponse.json({ ok: false, error: err }, { status: 500 });
  } finally {
    await releaseRunLock(lockToken).catch((e) => {
      console.error("Failed releasing run lock:", e);
    });
  }
}

export async function GET(request: NextRequest) {
  if (CRON_SECRET) {
    const auth = request.headers.get("authorization");
    if (auth !== `Bearer ${CRON_SECRET}`) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }
  }
  return runCopyTradeHandler();
}

export async function POST() {
  // Cron uses GET; POST reserved for future use
  return NextResponse.json({ error: "Use GET for cron" }, { status: 405 });
}
