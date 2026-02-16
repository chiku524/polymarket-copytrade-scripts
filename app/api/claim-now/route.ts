import { NextResponse } from "next/server";
import { getState, setState } from "@/lib/kv";
import { claimWinnings } from "@/lib/claim";

const PRIVATE_KEY = process.env.PRIVATE_KEY;
const MY_ADDRESS = process.env.MY_ADDRESS ?? "0x370e81c93aa113274321339e69049187cce03bb9";

export const maxDuration = 60;

/** POST: Manually trigger claim winnings (redeem resolved positions to cash). */
export async function POST() {
  if (!PRIVATE_KEY) {
    return NextResponse.json({ error: "PRIVATE_KEY not configured" }, { status: 500 });
  }

  try {
    const result = await claimWinnings(PRIVATE_KEY, MY_ADDRESS);
    await setState({
      lastClaimAt: Date.now(),
      lastClaimResult: { claimed: result.claimed, failed: result.failed },
    });
    return NextResponse.json({
      ok: true,
      claimed: result.claimed,
      failed: result.failed,
      errors: result.errors,
      txHashes: result.txHashes,
    });
  } catch (e) {
    const err = e instanceof Error ? e.message : String(e);
    console.error("Claim now error:", e);
    return NextResponse.json({ ok: false, error: err }, { status: 500 });
  }
}

export async function GET() {
  return NextResponse.json(
    { message: "Use POST to trigger claim winnings (Claim now button)" },
    { status: 200 }
  );
}
