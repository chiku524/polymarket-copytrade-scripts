import { NextResponse } from "next/server";
import { resetSyncState } from "@/lib/kv";

/** GET returns instructions - use POST from the Reset sync button */
export async function GET() {
  return NextResponse.json(
    { message: "Use POST to reset sync state (Reset sync button)" },
    { status: 200 }
  );
}

export async function POST() {
  try {
    await resetSyncState();
    return NextResponse.json({
      ok: true,
      message: "Strategy sync state reset. Next run will treat signals as fresh.",
    });
  } catch (e) {
    console.error("Reset error:", e);
    return NextResponse.json(
      { error: e instanceof Error ? e.message : String(e) },
      { status: 500 }
    );
  }
}
