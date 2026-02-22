import { NextResponse } from "next/server";
import { getPaperStats, resetPaperStats } from "@/lib/kv";

export async function GET() {
  try {
    const stats = await getPaperStats();
    return NextResponse.json(stats);
  } catch (e) {
    console.error("Paper stats GET error:", e);
    return NextResponse.json(
      { error: "Failed to load paper stats" },
      { status: 500 }
    );
  }
}

export async function DELETE() {
  try {
    await resetPaperStats();
    return NextResponse.json({ ok: true });
  } catch (e) {
    console.error("Paper stats reset error:", e);
    return NextResponse.json(
      { error: "Failed to reset paper stats" },
      { status: 500 }
    );
  }
}
