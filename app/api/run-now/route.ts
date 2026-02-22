import { NextResponse } from "next/server";
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

const PRIVATE_KEY = process.env.PRIVATE_KEY;
const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";
const SIGNATURE_TYPE = parseInt(process.env.SIGNATURE_TYPE ?? "1", 10);

export const maxDuration = 60;

/** GET returns instructions - use POST from the Run now button */
export async function GET() {
  return NextResponse.json(
    { message: "Use POST to trigger copy trade (Run now button)" },
    { status: 200 }
  );
}

/**
 * Manual trigger - no auth required (same-origin only in production).
 * Use for "Run now" button in the UI.
 */
export async function POST() {
  const lockToken = await acquireRunLock(120);
  if (!lockToken) {
    return NextResponse.json({ ok: true, skipped: true, reason: "busy" });
  }

  try {
    const config = await getConfig();
    if (config.mode === "off" || !config.enabled) {
      await setState({ lastRunAt: Date.now(), lastError: undefined });
      return NextResponse.json({ ok: true, skipped: true, reason: "mode_off" });
    }
    if (config.mode === "live" && !PRIVATE_KEY) {
      return NextResponse.json({ error: "PRIVATE_KEY not configured for Live mode" }, { status: 500 });
    }
    const state = await getState();
    const result = await runPairedStrategy(
      PRIVATE_KEY ?? "",
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

    await setState({
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
    });
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
