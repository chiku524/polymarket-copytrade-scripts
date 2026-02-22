import { NextResponse } from "next/server";
import { getConfig, getState, getRecentActivity, getPaperStats } from "@/lib/kv";
import { getCashBalance } from "@/lib/copy-trade";

const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";

export async function GET() {
  try {
    const [config, state, cashBalance, recentActivity, paperStats] = await Promise.all([
      getConfig(),
      getState(),
      getCashBalance(MY_ADDRESS).catch(() => 0),
      getRecentActivity(),
      getPaperStats(),
    ]);
    return NextResponse.json({
      config,
      state: {
        lastTimestamp: state.lastTimestamp,
        lastRunAt: state.lastRunAt,
        lastCopiedAt: state.lastCopiedAt,
        lastError: state.lastError,
        runsSinceLastClaim: state.runsSinceLastClaim,
        lastClaimAt: state.lastClaimAt,
        lastClaimResult: state.lastClaimResult,
      },
      cashBalance,
      recentActivity,
      paperStats,
    });
  } catch (e) {
    console.error("Status error:", e);
    return NextResponse.json(
      { error: "Failed to load status" },
      { status: 500 }
    );
  }
}
