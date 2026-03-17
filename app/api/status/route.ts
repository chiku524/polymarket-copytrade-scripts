import { NextResponse } from "next/server";
import {
  getConfig,
  getState,
  getRecentActivity,
  getPaperStats,
  getStrategyDiagnosticsHistory,
} from "@/lib/kv";
import { getCashBalance } from "@/lib/copy-trade";
import { getPaperLedgerSnapshot } from "@/lib/paper-ledger";

const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET() {
  try {
    const paperLedgerPromise = getPaperLedgerSnapshot().catch((e) => {
      console.error("Paper ledger snapshot error:", e);
      return undefined;
    });
    const [config, state, cashBalance, recentActivity, paperStats, strategyDiagnosticsHistory, paperLedger] =
      await Promise.all([
      getConfig(),
      getState(),
      getCashBalance(MY_ADDRESS).catch(() => 0),
      getRecentActivity(),
      getPaperStats(),
      getStrategyDiagnosticsHistory(),
      paperLedgerPromise,
    ]);
    return NextResponse.json(
      {
        config,
        state: {
          lastTimestamp: state.lastTimestamp,
          lastRunAt: state.lastRunAt,
          lastCopiedAt: state.lastCopiedAt,
          lastError: state.lastError,
          lastStrategyDiagnostics: state.lastStrategyDiagnostics,
          runsSinceLastClaim: state.runsSinceLastClaim,
          lastClaimAt: state.lastClaimAt,
          lastClaimResult: state.lastClaimResult,
          safetyLatch: state.safetyLatch,
          dailyRisk: state.dailyRisk,
        },
        cashBalance,
        recentActivity,
        paperStats,
        strategyDiagnosticsHistory,
        paperLedger,
      },
      { headers: { "Cache-Control": "no-store, no-cache, max-age=0" } }
    );
  } catch (e) {
    console.error("Status error:", e);
    return NextResponse.json(
      { error: "Failed to load status" },
      { status: 500 }
    );
  }
}
